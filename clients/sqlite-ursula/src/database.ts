// The database `open` returns: a node:sqlite DatabaseSync, a subclass of it, whose methods that run
// SQL throw what `classify` (errors.ts) makes of their errors: an UrsulaError for a failure of the
// extension, and every other error unchanged. Its statements are node:sqlite's own StatementSync
// objects, whose run, get, all and iterate (and the iterator's next) do the same. Every argument goes
// through to node:sqlite as given.
//
// A failure of the extension is one of SQLite's I/O errors, on which SQLite rolls back the whole
// transaction. A transaction helper (a library's, or `catch { ROLLBACK }`) then runs a ROLLBACK that
// fails with "cannot rollback - no transaction is active", which would replace the UrsulaError the
// caller needs. So the database remembers an UrsulaError after which no transaction is open, until a
// statement succeeds. A prepared ROLLBACK that finds no transaction then succeeds, since nothing is
// left to roll back. Through `exec`, which may hold more statements after it, it throws the
// remembered error instead, and so does a ROLLBACK TO or RELEASE that finds no savepoint, since the
// transaction around the savepoint is gone too.
import { DatabaseSync, type DatabaseSyncOptions, type StatementSync } from "node:sqlite";
import { errcode, SQLITE_ERROR, UrsulaError } from "./errors.ts";

/** SQLite's message for a ROLLBACK, and the start of its message for a ROLLBACK TO or RELEASE, that finds nothing to undo. */
const NO_TRANSACTION = "cannot rollback - no transaction is active";
const NO_SAVEPOINT = "no such savepoint: ";

/**
 * Runs `f` with `args` on `self`, a method that runs a statement, and throws what its error maps to.
 * `rolledBack` is what it returns for a ROLLBACK that found no transaction after an UrsulaError, when
 * it can report one: it is not given for `exec`.
 */
type Call = <A extends unknown[], R>(self: object, f: (this: object, ...args: A) => R, args: A, rolledBack?: () => R) => R;

/** `statement`, its methods that step it shadowed by ones that run through `call`. */
function classifying(statement: StatementSync, call: Call, classify: (error: unknown) => unknown): StatementSync {
	const { run, get, all, iterate } = statement;
	statement.run = ((...args: Parameters<StatementSync["run"]>) => call(statement, run, args, () => ({ changes: 0, lastInsertRowid: 0 }))) as StatementSync["run"];
	statement.get = ((...args: Parameters<StatementSync["get"]>) => call(statement, get, args, () => undefined)) as StatementSync["get"];
	statement.all = ((...args: Parameters<StatementSync["all"]>) => call(statement, all, args, () => [])) as StatementSync["all"];
	statement.iterate = ((...args: Parameters<StatementSync["iterate"]>) => {
		// Making the iterator runs nothing (it binds the parameters): its next steps the statement.
		let iterator: ReturnType<StatementSync["iterate"]>;
		try {
			iterator = iterate.apply(statement, args);
		} catch (error) {
			throw classify(error);
		}
		const { next } = iterator;
		iterator.next = ((...step: Parameters<typeof next>) => call(iterator, next, step, () => ({ done: true, value: undefined }))) as typeof next;
		return iterator;
	}) as StatementSync["iterate"];
	return statement;
}

export class UrsulaDatabase extends DatabaseSync {
	readonly #classify: (error: unknown) => unknown;
	/** An UrsulaError after which no transaction was open, until a statement succeeds. */
	#rolledBack: UrsulaError | undefined;

	/**
	 * DatabaseSync's arguments, and what to throw for an error: by default the error itself, so a
	 * connection made through `db.constructor` classifies nothing, and works as a DatabaseSync.
	 */
	constructor(path: ConstructorParameters<typeof DatabaseSync>[0], options?: DatabaseSyncOptions, classify: (error: unknown) => unknown = (error) => error) {
		if (options === undefined) super(path);
		else super(path, options);
		this.#classify = classify;
	}

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
	// (node:sqlite 24 takes the statement's options).
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
}
