// SQLite replicated through an Ursula stream by the sqlite-ursula-vfs loadable extension
// (clients/sqlite-vfs). The extension registers the "ursula" VFS as SQLite's default; after
// `attach(path, streamUrl)` every connection that opens `path` (plain node:sqlite, Pi's official node
// driver) is replicated: each commit is appended to the stream before it reaches the local WAL.
import { DatabaseSync } from "node:sqlite";
import { type SqliteExecutor, SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { type NodeSqliteStorageOptions, openNodeSqliteDatabase } from "@earendil-works/pi-durable/storage/sqlite/node";

let control: DatabaseSync | undefined;

/** Loads the extension once per process (path from SQLITE_URSULA_VFS) and returns the control connection. */
export function loadUrsulaVfs(path = process.env.SQLITE_URSULA_VFS): DatabaseSync {
	if (control !== undefined) return control;
	if (path === undefined || path.length === 0) throw new Error("set SQLITE_URSULA_VFS to the built extension");
	const db = new DatabaseSync(":memory:", { allowExtension: true });
	db.loadExtension(path);
	control = db;
	return db;
}

/**
 * Catches `file` up from the stream (installing the stream's latest snapshot first when the file is
 * missing or behind it), claims the stream for this process (fencing every earlier owner)
 * and attaches the file. No connection to `file` may be open; the process keeps a host lock on the file
 * for its lifetime. Returns the stream offset the file reflects. A file that has a sidecar (`<file>-ursula`,
 * written by its first attach) opens in this process only while attached here: not before an attach
 * of it succeeds, nor after one fails (unless refused up front for open connections, another thread
 * attaching it, or another process holding it, which leaves the file as it was).
 */
export function attach(file: string, streamUrl: string): number {
	const row = loadUrsulaVfs().prepare("SELECT ursula_attach(?, ?) AS n").get(file, streamUrl) as { n: number | bigint };
	return Number(row.n);
}

export interface AttachStatus {
	/** Stream offset after the last acknowledged commit. */
	readonly offset: number;
	/** This owner's producer epoch. */
	readonly epoch: number;
	/** Every later commit fails until the file is re-attached. */
	readonly poisoned: boolean;
	/** Poisoned because a newer owner claimed the stream, or the stream was deleted and recreated. */
	readonly fenced: boolean;
	readonly reason: string | null;
	/** Offset of the latest snapshot known readable (published and read back, or found at attach); 0 for none. */
	readonly snapshot: number;
	/** Retention this owner advanced the stream to (0: none yet). */
	readonly retained: number;
	/** Stream offset of the local state attach started from; 0 when it rebuilt the file from nothing (a fresh host, or local files it could not trust and discarded: another boot, a replaced file, a sidecar ahead of its WAL, another incarnation of the stream: deleted and recreated). */
	readonly local: number;
	/** Offset of the snapshot attach installed (0: none). */
	readonly installed: number;
}

export function status(file: string): AttachStatus {
	const row = loadUrsulaVfs().prepare("SELECT ursula_status(?) AS s").get(file) as { s: string };
	return JSON.parse(row.s) as AttachStatus;
}

/** One replicated commit, as measured inside the VFS. */
export interface VfsCommitStat {
	/** Frame bytes on the stream (compressed). */
	readonly bytes: number;
	/** Uncompressed record bytes. */
	readonly raw: number;
	/** Page images in the record. */
	readonly pages: number;
	/** Append requests (more than one after an unknown outcome). */
	readonly attempts: number;
	/** The append requests alone. */
	readonly append_us: number;
	/** Commit hook: record build + append + local WAL write. */
	readonly vfs_us: number;
}

/** One published (and read back) snapshot. */
export interface VfsSnapshotStat {
	/** Stream offset it reflects. */
	readonly offset: number;
	/** Body bytes (compressed). */
	readonly bytes: number;
	/** Database bytes. */
	readonly raw: number;
	/** Page copy under the read transaction. */
	readonly copy_us: number;
	/** Checkpoint, copy, compression, publish and read-back. */
	readonly total_us: number;
}

export interface VfsStats {
	readonly commits: VfsCommitStat[];
	/** Checkpoints of the file since the last drain (time holding the checkpoint lock). */
	readonly checkpoints_us: number[];
	readonly snapshots: VfsSnapshotStat[];
}

/** Drains the per-commit and per-checkpoint stats of an attached file. */
export function drainStats(file: string): VfsStats {
	const row = loadUrsulaVfs().prepare("SELECT ursula_stats(?) AS s").get(file) as { s: string };
	return JSON.parse(row.s) as VfsStats;
}

/** A commit the VFS could not replicate: fenced by a newer owner, rejected, or with no answer in time. */
export class UrsulaReplicationError extends Error {
	readonly fenced: boolean;
	constructor(message: string, fenced: boolean, cause: unknown) {
		super(message, { cause });
		this.name = "UrsulaReplicationError";
		this.fenced = fenced;
	}
}

/**
 * Pi Durable's official node SqliteStorage on a replicated file: attaches `file` to `streamUrl`, then
 * opens it with Pi's node driver, unmodified. A commit the VFS fails surfaces as an
 * `UrsulaReplicationError` (SQLite has already rolled the transaction back, so the driver's own
 * rollback attempt would otherwise turn it into an AggregateError). The storage is unusable for
 * writes after that; re-open it to take the stream over again.
 */
export async function openUrsulaPiStorage(file: string, streamUrl: string, options: NodeSqliteStorageOptions = {}): Promise<SqliteStorage> {
	attach(file, streamUrl);
	const db = await openNodeSqliteDatabase(file, options);
	const transaction = db.transaction.bind(db);
	db.transaction = <T>(callback: (transaction: SqliteExecutor) => Promise<T>): Promise<T> =>
		transaction(callback).catch((error: unknown) => {
			const s = status(file);
			if (!s.poisoned) throw error;
			throw new UrsulaReplicationError(`commit not replicated: ${s.reason ?? "unknown"}`, s.fenced, error instanceof AggregateError ? error.errors[0] : error);
		});
	return SqliteStorage.open(db);
}
