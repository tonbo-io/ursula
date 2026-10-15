// `@tonbo/sqlite-ursula/pi` (design §8.5): Pi Durable's official node SqliteStorage on a replicated
// database. The only module that imports `@earendil-works/pi-durable`, an optional peer dependency of
// the package, so an application that uses the root entry alone never installs it.
import { StorageRejected } from "@earendil-works/pi-durable";
import { type SqliteExecutor, SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { type NodeSqliteStorageOptions, openNodeSqliteDatabase } from "@earendil-works/pi-durable/storage/sqlite/node";
import { classifyWrapped, UrsulaResetError, UrsulaWriteRefusedError } from "./errors.ts";
import { type OpenOptions, open } from "./index.ts";

/** A statement's parameters, as Pi's executor takes them. */
type Params = SqliteExecutor["run"] extends (sql: string, ...params: infer P) => unknown ? P : never;

/** {@link openUrsulaPiStorage}'s options: how to open the stream, Pi's node driver settings, and how long `close` waits. */
export type UrsulaPiStorageOptions = NodeSqliteStorageOptions &
	Pick<OpenOptions, "dir" | "token" | "signal" | "maxPendingBytes"> & {
		/** How long the storage's `close` waits for `synced()` before it closes anyway: 10 s by default. */
		readonly closeTimeoutMs?: number | undefined;
	};

/**
 * Pi Durable's official node SqliteStorage on the database the stream at `url` holds: opens it with
 * the root entry's `open`, then runs Pi's node driver, unmodified, on `db.location()`.
 *
 * Every transaction and every statement outside one resolves only once `synced()` did: a Pi commit
 * resolves once the stream holds it, which is Pi's `Storage.commit` contract, and a read once what it
 * read is in the stream. The waits do not block the event loop.
 *
 * A commit refused with `backlog_full` (nothing committed) throws Pi's `StorageRejected`, whose
 * `cause` is the `UrsulaWriteRefusedError`, so the Session continues. A reset throws the
 * `UrsulaResetError` itself, unwrapped, so Pi's Session poisons itself: open the storage again, or
 * stop when its `action` is `stop`. Other failures of the extension throw the root entry's errors.
 * `close` waits up to `closeTimeoutMs` for `synced()`, then closes.
 */
export async function openUrsulaPiStorage(url: string | URL, options: UrsulaPiStorageOptions = {}): Promise<SqliteStorage> {
	const { walAutoCheckpointPages, busyTimeoutMs, closeTimeoutMs = 10_000, ...openOptions } = options;
	const db = await open(url, openOptions);
	const location = db.location();
	let pi: Awaited<ReturnType<typeof openNodeSqliteDatabase>>;
	try {
		pi = await openNodeSqliteDatabase(location as string, {
			...(walAutoCheckpointPages === undefined ? {} : { walAutoCheckpointPages }),
			...(busyTimeoutMs === undefined ? {} : { busyTimeoutMs }),
		});
	} catch (error) {
		db.close();
		throw failure(error);
	}
	const { transaction, run, exec, get, all, close } = {
		transaction: pi.transaction.bind(pi),
		run: pi.run.bind(pi),
		exec: pi.exec.bind(pi),
		get: pi.get.bind(pi),
		all: pi.all.bind(pi),
		close: pi.close.bind(pi),
	};
	function failure(error: unknown): unknown {
		const classified = classifyWrapped(error, () => db.status());
		if (classified instanceof UrsulaWriteRefusedError && classified.kind === "backlog_full") {
			return new StorageRejected(classified.message, { cause: classified });
		}
		return classified;
	}
	/** `work`'s result once `synced()` resolved, and its failure as {@link failure} maps it. */
	async function gated<T>(work: Promise<T>): Promise<T> {
		let result: T;
		try {
			result = await work;
		} catch (error) {
			throw failure(error);
		}
		await db.synced();
		return result;
	}
	pi.transaction = <T>(callback: (transaction: SqliteExecutor) => Promise<T>): Promise<T> => gated(transaction(callback));
	// Statements outside a transaction: a write commits on its own, and any statement reads.
	pi.run = (sql, ...params) => gated(run(sql, ...params));
	pi.exec = (sql) => gated(exec(sql));
	pi.get = <T extends object>(sql: string, ...params: Params): Promise<T | undefined> => gated(get<T>(sql, ...params));
	pi.all = <T extends object>(sql: string, ...params: Params): Promise<T[]> => gated(all<T>(sql, ...params));
	pi.close = async () => {
		try {
			await db.synced({ signal: AbortSignal.timeout(closeTimeoutMs) });
		} catch {
			// Timed out, or reset: the log keeps what the stream lacks for the next open.
		}
		try {
			await close();
		} catch (error) {
			// A reset database refuses the driver's closing checkpoint: the connection closed anyway.
			const mapped = failure(error);
			if (!(mapped instanceof UrsulaResetError)) throw mapped;
		} finally {
			db.close();
		}
	};
	return SqliteStorage.open(pi);
}
