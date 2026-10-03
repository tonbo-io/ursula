// ReplicatedSqlite: one local SQLite file whose write-ahead log of record is an Ursula JSON stream.
//
// Every write transaction runs locally under BEGIN IMMEDIATE with a SQLite session attached; at the
// end its row-level changeset (plus any DDL text the transaction executed, which sessions do not
// capture) becomes one stream record `{"ddl"?:[...],"cs":"<base64>"}`, appended with
// `Stream-Record-Match = watermark`. Only after the append is acknowledged does the local COMMIT
// happen, in the same local transaction that advances the watermark (`_ursula_wal.next_record`).
// So the local file is always a prefix of the stream, and open() replays the suffix it lacks.
//
// Fencing: there is no claim protocol. Whoever opens catches up to the tail and becomes the writer;
// a stale writer's next append fails the record match (412), its transaction is rolled back and the
// instance is poisoned. An append with an unknown outcome (no response) also rolls back and poisons:
// a reopen replays the log and reveals whether it landed.
//
// The class implements Pi Durable's `SqliteDatabase` facade, so `SqliteStorage.open(db)` runs on it.
import { DatabaseSync, type StatementSync } from "node:sqlite";
import type { SqliteDatabase, SqliteExecutor, SqliteValue } from "@earendil-works/pi-durable/storage/sqlite";
import type { WalStream } from "./stream.ts";

export class FencedError extends Error {
	override readonly name = "FencedError";
}
export class PoisonedError extends Error {
	override readonly name = "PoisonedError";
}

/** Test crash points: returning true from the hook abandons the connection there (no COMMIT, no ROLLBACK). */
export type CrashPoint = "before-append" | "before-commit";

export interface CommitInfo {
	/** Bytes of the appended record (JSON text). */
	readonly bytes: number;
	/** Changeset bytes before base64. */
	readonly changesetBytes: number;
	/** Wall time of the whole transaction, callback included. */
	readonly latencyMs: number;
	/** Wall time of the append request alone. */
	readonly appendMs: number;
	/** Tables written (from the SQL text). */
	readonly tables: readonly string[];
}

export interface ReplicatedSqliteOptions {
	readonly crash?: (point: CrashPoint) => boolean;
	readonly onCommit?: (info: CommitInfo) => void;
}

const WRITE_TABLE = /\b(?:INSERT\s+(?:OR\s+\w+\s+)?INTO|UPDATE|DELETE\s+FROM)\s+(\w+)/gi;
const DDL = /^\s*(CREATE|ALTER|DROP)\b/i;

interface TxState {
	active: boolean;
	readonly ddl: string[];
	readonly tables: Set<string>;
}

class Executor implements SqliteExecutor {
	constructor(
		private readonly owner: ReplicatedSqlite,
		private readonly tx: TxState,
	) {}
	private check(sql: string): void {
		if (!this.tx.active) throw new Error("SQLite transaction handle is no longer active");
		if (DDL.test(sql)) this.tx.ddl.push(sql);
		for (const m of sql.matchAll(WRITE_TABLE)) this.tx.tables.add(m[1] as string);
	}
	async exec(sql: string): Promise<void> {
		this.check(sql);
		this.owner.raw.exec(sql);
	}
	async run(sql: string, ...params: SqliteValue[]): Promise<void> {
		this.check(sql);
		this.owner.statement(sql).run(...params);
	}
	async get<T extends object>(sql: string, ...params: SqliteValue[]): Promise<T | undefined> {
		this.check(sql);
		return this.owner.statement(sql).get(...params) as T | undefined;
	}
	async all<T extends object>(sql: string, ...params: SqliteValue[]): Promise<T[]> {
		this.check(sql);
		return this.owner.statement(sql).all(...params) as T[];
	}
}

export class ReplicatedSqlite implements SqliteDatabase {
	readonly raw: DatabaseSync;
	private readonly stream: WalStream;
	private readonly options: ReplicatedSqliteOptions;
	private readonly statements = new Map<string, StatementSync>();
	private queue: Promise<unknown> = Promise.resolve();
	/** The next stream record ordinal; everything before it is applied locally. */
	private watermark = 0;
	private poisoned: Error | undefined;
	private closed = false;

	private constructor(raw: DatabaseSync, stream: WalStream, options: ReplicatedSqliteOptions) {
		this.raw = raw;
		this.stream = stream;
		this.options = options;
	}

	/** Open `path` (created if missing), create the stream if missing, and replay it to the tail. */
	static async open(path: string, stream: WalStream, options: ReplicatedSqliteOptions = {}): Promise<ReplicatedSqlite> {
		const raw = new DatabaseSync(path);
		raw.exec("PRAGMA journal_mode = WAL");
		raw.exec("PRAGMA synchronous = NORMAL");
		raw.exec("CREATE TABLE IF NOT EXISTS _ursula_wal (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), next_record INTEGER NOT NULL)");
		raw.exec("INSERT OR IGNORE INTO _ursula_wal VALUES (1, 0)");
		const db = new ReplicatedSqlite(raw, stream, options);
		try {
			await stream.create();
			db.watermark = (raw.prepare("SELECT next_record FROM _ursula_wal").get() as { next_record: number }).next_record;
			await db.catchUp();
		} catch (error) {
			raw.close();
			throw error;
		}
		return db;
	}

	/** The next stream record this file expects. */
	get nextRecord(): number {
		return this.watermark;
	}

	/** Replay records from the watermark to the tail, one local transaction per page. */
	private async catchUp(): Promise<void> {
		for (;;) {
			const page = await this.stream.read(this.watermark);
			if (page.length === 0) return;
			this.raw.exec("BEGIN IMMEDIATE");
			try {
				for (const line of page) {
					const record = JSON.parse(line) as { ddl?: string[]; cs?: string };
					for (const sql of record.ddl ?? []) this.raw.exec(sql);
					if (record.cs !== undefined && !this.raw.applyChangeset(Buffer.from(record.cs, "base64"))) {
						throw new Error(`changeset of record ${this.watermark} conflicted on replay`);
					}
				}
				this.statement("UPDATE _ursula_wal SET next_record = ? WHERE singleton = 1").run(this.watermark + page.length);
				this.raw.exec("COMMIT");
			} catch (error) {
				this.raw.exec("ROLLBACK");
				throw error;
			}
			this.watermark += page.length;
		}
	}

	statement(sql: string): StatementSync {
		let s = this.statements.get(sql);
		if (s === undefined) {
			s = this.raw.prepare(sql);
			this.statements.set(sql, s);
		}
		return s;
	}

	private serial<T>(operation: () => Promise<T>): Promise<T> {
		const run = this.queue.then(operation, operation);
		this.queue = run.catch(() => undefined);
		return run;
	}

	private assertUsable(): void {
		if (this.closed) throw new Error("ReplicatedSqlite is closed");
		if (this.poisoned !== undefined) throw new PoisonedError("ReplicatedSqlite is poisoned", { cause: this.poisoned });
	}

	transaction<T>(callback: (transaction: SqliteExecutor) => Promise<T>): Promise<T> {
		return this.serial(() => this.runTransaction(callback));
	}

	private async runTransaction<T>(callback: (transaction: SqliteExecutor) => Promise<T>): Promise<T> {
		this.assertUsable();
		const started = performance.now();
		this.raw.exec("BEGIN IMMEDIATE");
		const session = this.raw.createSession();
		const tx: TxState = { active: true, ddl: [], tables: new Set() };
		let open = true;
		const rollback = (): void => {
			if (!open) return;
			open = false;
			session.close();
			this.raw.exec("ROLLBACK");
		};
		const crash = (point: CrashPoint): void => {
			if (this.options.crash?.(point) !== true) return;
			open = false;
			session.close();
			this.closed = true;
			this.statements.clear();
			this.raw.close(); // an open transaction is discarded, as in a process crash
			throw new Error(`crashed at ${point}`);
		};
		try {
			const result = await callback(new Executor(this, tx));
			tx.active = false;
			const changeset = session.changeset();
			if (changeset.length === 0 && tx.ddl.length === 0) {
				open = false;
				session.close();
				this.raw.exec("COMMIT");
				return result;
			}
			const record = JSON.stringify({ ...(tx.ddl.length > 0 ? { ddl: tx.ddl } : {}), cs: Buffer.from(changeset).toString("base64") });
			crash("before-append");
			const appendStarted = performance.now();
			let outcome: Awaited<ReturnType<WalStream["append"]>>;
			try {
				outcome = await this.stream.append(record, this.watermark);
			} catch (error) {
				rollback();
				this.poisoned = error as Error;
				throw error;
			}
			const appendMs = performance.now() - appendStarted;
			if (!outcome.ok) {
				rollback();
				this.poisoned = new FencedError(`record match ${this.watermark} failed: the stream is at ${outcome.next}`);
				throw this.poisoned;
			}
			crash("before-commit");
			open = false;
			session.close();
			this.statement("UPDATE _ursula_wal SET next_record = ? WHERE singleton = 1").run(this.watermark + 1);
			this.raw.exec("COMMIT");
			this.watermark++;
			this.options.onCommit?.({
				bytes: Buffer.byteLength(record),
				changesetBytes: changeset.length,
				latencyMs: performance.now() - started,
				appendMs,
				tables: [...tx.tables],
			});
			return result;
		} catch (error) {
			tx.active = false;
			if (open) rollback();
			throw error;
		}
	}

	// Writes outside an explicit transaction are replicated too: each runs as its own transaction.
	exec(sql: string): Promise<void> {
		return this.transaction((tx) => tx.exec(sql));
	}
	run(sql: string, ...params: SqliteValue[]): Promise<void> {
		return this.transaction((tx) => tx.run(sql, ...params));
	}
	get<T extends object>(sql: string, ...params: SqliteValue[]): Promise<T | undefined> {
		return this.serial(async () => {
			this.assertUsable();
			return this.statement(sql).get(...params) as T | undefined;
		});
	}
	all<T extends object>(sql: string, ...params: SqliteValue[]): Promise<T[]> {
		return this.serial(async () => {
			this.assertUsable();
			return this.statement(sql).all(...params) as T[];
		});
	}

	close(): Promise<void> {
		return this.serial(async () => {
			if (this.closed) return;
			this.closed = true;
			this.statements.clear();
			this.raw.close();
		});
	}
}
