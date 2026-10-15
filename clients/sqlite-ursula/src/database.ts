// The database `open` returns: a node:sqlite DatabaseSync, a subclass of it, so drizzle, kysely and
// Pi's node driver take it. Its methods that run SQL throw what `classify` (errors.ts) makes of their
// errors: an UrsulaError for a failure of the extension, and every other error unchanged. Its
// statements are node:sqlite's own StatementSync objects, whose run, get, all and iterate (and the
// iterator's next) do the same. Every argument goes through to node:sqlite as given. On top of
// DatabaseSync it has `name`, `synced()`, `status()` and the `reset` listeners.
//
// A refused write or a reset is one of SQLite's I/O errors, on which SQLite rolls back the whole
// transaction. A transaction helper (a library's, or `catch { ROLLBACK }`) then runs a ROLLBACK that
// fails with "cannot rollback - no transaction is active", which would replace the UrsulaError the
// caller needs. So the database remembers an UrsulaError after which no transaction is open, until a
// statement succeeds. A prepared ROLLBACK that finds no transaction then succeeds, since nothing is
// left to roll back. Through `exec`, which may hold more statements after it, it throws the
// remembered error instead, and so does a ROLLBACK TO or RELEASE that finds no savepoint, since the
// transaction around the savepoint is gone too.
import { DatabaseSync, type DatabaseSyncOptions, type StatementSync } from "node:sqlite";
import { call } from "./control.ts";
import { classify, errcode, type FailureStatus, SQLITE_ERROR, UrsulaError, UrsulaResetError } from "./errors.ts";
import { pump, type ResetTarget, resetError, track, untrack, waitTicket } from "./pump.ts";

/** SQLite's message for a ROLLBACK, and the start of its message for a ROLLBACK TO or RELEASE, that finds nothing to undo. */
const NO_TRANSACTION = "cannot rollback - no transaction is active";
const NO_SAVEPOINT = "no such savepoint: ";

/**
 * A local cache moved aside: a history change of the stream that does not keep it (`history_changed`),
 * a db file that is not the cache's own (`replaced`), or a stream created again over it with
 * `recreate` (`recreated`).
 */
export interface UrsulaSalvage {
	readonly dir: string;
	readonly reason: "history_changed" | "replaced" | "recreated";
	/** When it was moved aside, RFC 3339 in UTC. */
	readonly time: string;
	/** The stream token the cache was built under, `null` for a replaced one. */
	readonly incarnation: string | null;
	/** The stream's token when it was moved aside. */
	readonly stream_incarnation: string | null;
	/** Every commit up to this position was in the stream under `incarnation`. It is `null` for a replaced one. */
	readonly acknowledged_position: number | null;
	/** The cache's commits after `acknowledged_position`, which the stream may never have held. */
	readonly unacknowledged: number | null;
	/** Whether `db` opens as a plain SQLite database. */
	readonly plain: boolean;
}

/**
 * `ursula_status` (design §9, the part built so far): one snake_case JSON for every host.
 */
export interface UrsulaStatus {
	/** The stream's path, such as `/acme/notes`. */
	readonly name: string;
	/** The local database file the extension bound. */
	readonly path: string;
	readonly role: "owner";
	readonly state: "live" | "retiring" | "retired" | "reset";
	/** The epoch this owner claimed, `null` before its claim is acknowledged. */
	readonly epoch: number | null;
	readonly cluster_id: string;
	readonly incarnation: string;
	readonly lineage: string;
	readonly commits: {
		/** The position of the last commit logged locally. */
		readonly logged: number;
		/** The position of the last commit SQLite published. */
		readonly confirmed: number;
		/** Every commit up to this position is in the stream. */
		readonly acknowledged: number;
		readonly pending: number;
		readonly pending_bytes: number;
	};
	readonly replication: { readonly state: string; readonly condition: string | null };
	/**
	 * Where the open got the local database: a trusted local cache (`resumed`), a trusted local cache
	 * kept across a history change of the stream that inherits it (`rekeyed`), or a build from the
	 * stream (`built`), from the snapshot at `snapshot_offset` and the tail after it, or from the
	 * stream's start when it is `null`. A build that moved the local cache aside into `salvage` first
	 * is `salvaged`.
	 */
	readonly attach: { readonly local: "resumed" | "rekeyed" | "built" | "salvaged"; readonly snapshot_offset: number | null };
	/** The last write refused with nothing committed. */
	readonly refused: { readonly kind: string; readonly action: string; readonly message: string } | null;
	/**
	 * Local caches of this stream moved aside, oldest first, never read or deleted by the extension:
	 * each `dir` holds a plain SQLite database `db` (a replaced file as it was found), its log and a
	 * `manifest.json`.
	 */
	readonly salvage: readonly UrsulaSalvage[];
	readonly reset: { readonly kind: string; readonly action: string } | null;
}

/**
 * Runs `f` with `args` on `self`, a method that runs a statement, and throws what its error maps to.
 * `rolledBack` is what it returns for a ROLLBACK that found no transaction after an UrsulaError, when
 * it can report one: it is not given for `exec`.
 */
type Call = <A extends unknown[], R>(self: object, f: (this: object, ...args: A) => R, args: A, rolledBack?: () => R) => R;

/** `statement`, its methods that step it shadowed by ones that run through `call`. */
function classifying(statement: StatementSync, run: Call, classify: (error: unknown) => unknown): StatementSync {
	const { run: step, get, all, iterate } = statement;
	statement.run = ((...args: Parameters<StatementSync["run"]>) => run(statement, step, args, () => ({ changes: 0, lastInsertRowid: 0 }))) as StatementSync["run"];
	statement.get = ((...args: Parameters<StatementSync["get"]>) => run(statement, get, args, () => undefined)) as StatementSync["get"];
	statement.all = ((...args: Parameters<StatementSync["all"]>) => run(statement, all, args, () => [])) as StatementSync["all"];
	statement.iterate = ((...args: Parameters<StatementSync["iterate"]>) => {
		// Making the iterator runs nothing (it binds the parameters): its next steps the statement.
		let iterator: ReturnType<StatementSync["iterate"]>;
		try {
			iterator = iterate.apply(statement, args);
		} catch (error) {
			throw classify(error);
		}
		const { next } = iterator;
		iterator.next = ((...more: Parameters<typeof next>) => run(iterator, next, more, () => ({ done: true, value: undefined }))) as typeof next;
		return iterator;
	}) as StatementSync["iterate"];
	return statement;
}

/** What `open` bound: the stream's path and the local file. */
export interface Binding {
	readonly name: string;
	readonly path: string;
}

export type ResetListener = (error: UrsulaResetError) => void;

export class UrsulaDatabase extends DatabaseSync implements ResetTarget {
	/** The stream's path, such as `/acme/notes`. */
	readonly name: string;
	readonly boundPath: string;
	readonly #bound: boolean;
	readonly #listeners = new Set<ResetListener>();
	/** The reset this database knows of, from an event, a statement or `synced()`. */
	#reset: UrsulaResetError | undefined;
	#fired = false;
	/** An UrsulaError after which no transaction was open, until a statement succeeds. */
	#rolledBack: UrsulaError | undefined;

	/**
	 * DatabaseSync's arguments, and what `open` bound. Without `binding` (a connection made through
	 * `db.constructor`) it classifies nothing and works as a DatabaseSync.
	 */
	constructor(path: ConstructorParameters<typeof DatabaseSync>[0], options?: DatabaseSyncOptions, binding?: Binding) {
		if (options === undefined) super(path);
		else super(path, options);
		this.name = binding?.name ?? "";
		this.boundPath = binding?.path ?? String(path);
		this.#bound = binding !== undefined;
		if (this.#bound) track(this);
	}

	/**
	 * Resolves once the stream holds every commit this process logged before the call, and a round
	 * trip that started after the call found this process still the owner (design §8.2). Await it
	 * before anything outside the process acts on what you wrote or read. It does not block the event
	 * loop, and it keeps the process alive while it waits. It rejects with an {@link UrsulaResetError}
	 * on a reset, and with `signal.reason` when `signal` fires (the commits keep replicating).
	 * Network failures never reject it: it waits.
	 */
	synced(options: { signal?: AbortSignal | undefined } = {}): Promise<void> {
		const { signal } = options;
		if (this.#reset !== undefined) return Promise.reject(this.#reset);
		if (signal?.aborted === true) return Promise.reject(signal.reason);
		// Started before the call, so its events cursor precedes the ticket.
		pump();
		let answer: { synced: boolean; ticket: number };
		try {
			answer = call("SELECT ursula_synced(?, 0)", this.boundPath);
		} catch (error) {
			if (error instanceof UrsulaResetError) this.#reset ??= error;
			return Promise.reject(error);
		}
		if (answer.synced) return Promise.resolve();
		return waitTicket(this.boundPath, answer.ticket, signal).catch((error: unknown) => {
			if (error instanceof UrsulaResetError) this.#reset ??= error;
			throw error;
		});
	}

	/** `ursula_status` of the database (design §9). */
	status(): UrsulaStatus {
		return call<UrsulaStatus>("SELECT ursula_status(?)", this.boundPath);
	}

	/** Runs `listener` once when the database is reset, after its pending `synced()` calls rejected. */
	on(event: "reset", listener: ResetListener): this {
		if (event !== "reset") throw new TypeError(`UrsulaDatabase has no ${String(event)} event`);
		this.#listeners.add(listener);
		return this;
	}

	off(event: "reset", listener: ResetListener): this {
		if (event !== "reset") throw new TypeError(`UrsulaDatabase has no ${String(event)} event`);
		this.#listeners.delete(listener);
		return this;
	}

	/** @internal The pump: the database was reset. Its listeners run once. */
	resetBy(error: UrsulaResetError): void {
		this.#reset ??= error;
		if (this.#fired || !this.isOpen) return;
		this.#fired = true;
		for (const listener of [...this.#listeners]) {
			try {
				listener(this.#reset);
			} catch (thrown) {
				// As an EventEmitter's listener would: the error is the process's.
				queueMicrotask(() => {
					throw thrown;
				});
			}
		}
	}

	/** @internal The pump: the reset `ursula_status` reports, if any. */
	currentReset(): UrsulaResetError | undefined {
		try {
			const reset = this.status().reset;
			return reset === null ? undefined : resetError(reset.kind);
		} catch {
			return undefined;
		}
	}

	/** What classifies a statement's I/O error: the reset this database knows of, else its status. */
	readonly #failure = (): FailureStatus => {
		if (this.#reset !== undefined) return { reset: { kind: this.#reset.kind, action: this.#reset.action }, refused: null };
		return this.status();
	};

	readonly #classify = (error: unknown): unknown => {
		if (!this.#bound) return error;
		const classified = classify(error, this.#failure);
		if (classified instanceof UrsulaResetError) this.#reset ??= classified;
		return classified;
	};

	readonly #call: Call = (self, f, args, rolledBack) => {
		try {
			const result = f.apply(self, args);
			this.#rolledBack = undefined;
			return result;
		} catch (error) {
			const remembered = this.#rolledBack;
			// SQLite tells these errors from others by their message alone: they are SQLITE_ERROR.
			if (remembered !== undefined && errcode(error) === SQLITE_ERROR && error instanceof Error) {
				if (error.message === NO_TRANSACTION && rolledBack !== undefined) {
					this.#rolledBack = undefined;
					return rolledBack();
				}
				if (error.message === NO_TRANSACTION || error.message.startsWith(NO_SAVEPOINT)) throw remembered;
			}
			const classified = this.#classify(error);
			if (classified instanceof UrsulaError && this.isOpen && !this.isTransaction) this.#rolledBack = classified;
			throw classified;
		}
	};

	override exec(...args: Parameters<DatabaseSync["exec"]>): void {
		this.#call(this, super.exec, args);
	}

	// Preparing runs nothing, so it neither clears nor remembers an error. Every argument goes on
	// (node:sqlite from 24.14 takes the statement's options).
	override prepare(...args: Parameters<DatabaseSync["prepare"]>): StatementSync {
		let statement: StatementSync;
		try {
			statement = super.prepare(...args);
		} catch (error) {
			throw this.#classify(error);
		}
		return classifying(statement, this.#call, this.#classify);
	}

	override applyChangeset(...args: Parameters<DatabaseSync["applyChangeset"]>): boolean {
		return this.#call(this, super.applyChangeset, args);
	}

	/**
	 * Closes the connection, synchronously. Once a binding's last connection closes, replication
	 * drains in the background and the binding retires. To use the database again, call the
	 * package's `open()`: this database's own `open()` method binds nothing.
	 */
	override close(): void {
		super.close();
		untrack(this);
	}
}
