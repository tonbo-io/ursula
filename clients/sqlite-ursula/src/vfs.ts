// The VFS variant (clients/sqlite-ursula-vfs): a loadable extension that registers the "ursula" VFS as
// SQLite's default and replicates every WAL commit of an attached file to an Ursula JSON stream.
// Applications (Pi's official node driver included) open the file with plain node:sqlite after
// `attach(path, streamUrl)`; nothing else changes.
import { DatabaseSync } from "node:sqlite";

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

/** Catches `file` up from the stream and attaches it; returns the stream tail (records). */
export function attach(file: string, streamUrl: string): number {
	const row = loadUrsulaVfs().prepare("SELECT ursula_attach(?, ?) AS n").get(file, streamUrl) as { n: number | bigint };
	return Number(row.n);
}

/** One replicated commit, as measured inside the VFS. */
export interface VfsCommitStat {
	/** Record bytes (JSON). */
	readonly bytes: number;
	/** Page images in the record. */
	readonly pages: number;
	/** The append request alone. */
	readonly append_us: number;
	/** Commit hook: record build + append + local WAL write + sync + sidecar. */
	readonly vfs_us: number;
}

/** Drains the per-commit stats of an attached file. */
export function drainStats(file: string): VfsCommitStat[] {
	const row = loadUrsulaVfs().prepare("SELECT ursula_stats(?) AS s").get(file) as { s: string };
	return JSON.parse(row.s) as VfsCommitStat[];
}
