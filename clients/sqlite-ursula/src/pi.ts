// `@tonbo/sqlite-ursula/pi` (design: Pi): Pi Durable's official node SqliteStorage on a replicated
// database. The only module that imports `@earendil-works/pi-durable`, an optional peer dependency of
// the package, so an application that uses the root entry alone never installs it.
import * as durable from "@earendil-works/pi-durable";
import { type SqliteExecutor, SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { type NodeSqliteStorageOptions, openNodeSqliteDatabase } from "@earendil-works/pi-durable/storage/sqlite/node";
import { classifyWrapped } from "./control.ts";
import { UrsulaResetError, UrsulaWriteRefusedError } from "./errors.ts";
import { type OpenOptions, open } from "./index.ts";

/**
 * Pi's `StorageRejected` up to 1.1: a commit refused before any durable effect, after which the
 * Session continues. Pi 1.2 removed it, and a storage retries its own transient failures instead.
 */
const StorageRejected = (durable as Record<string, unknown>).StorageRejected as (new (message: string, options?: ErrorOptions) => Error) | undefined;

/** A statement's parameters, as Pi's executor takes them. */
type Params = SqliteExecutor["run"] extends (sql: string, ...params: infer P) => unknown ? P : never;

/**
 * Whether Pi's `Storage.commit` stores a group of commits in one call, `commit(batches, receive, context)`,
 * as from Pi 1.5, rather than one, `commit(writes, context)`, up to 1.4.
 */
const groupedCommits = SqliteStorage.prototype.commit.length >= 3;

/** {@link openUrsulaPiStorage}'s options: how to open the stream, Pi's node driver settings, and how long `close` waits. */
export type UrsulaPiStorageOptions = Pick<NodeSqliteStorageOptions, "walAutoCheckpointPages" | "busyTimeoutMs"> &
	Pick<OpenOptions, "dir" | "token" | "signal" | "maxPendingBytes" | "recreate"> & {
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
 * A commit refused with `backlog_full` commits nothing. From Pi 1.2, whose storages retry their own
 * transient failures, it waits for `synced()` and runs again: the transaction up to Pi 1.4, and from
 * Pi 1.5 the whole `Storage.commit` call, with the same batches. Up to Pi 1.1 it throws Pi's
 * `StorageRejected`, whose `cause` is the `UrsulaWriteRefusedError`, so the Session continues. A
 * reset throws the `UrsulaResetError` itself, unwrapped, so Pi's Session poisons itself: open the
 * storage again, or stop when its `action` is `stop`. Other failures of the extension throw the root entry's errors.
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
	const counted = pi as unknown as { runCounted?: (sql: string, ...params: Params) => Promise<number> };
	const runCounted = counted.runCounted?.bind(pi);
	const { transaction, run, exec, get, all, close } = {
		transaction: pi.transaction.bind(pi),
		run: pi.run.bind(pi),
		exec: pi.exec.bind(pi),
		get: pi.get.bind(pi),
		all: pi.all.bind(pi),
		close: pi.close.bind(pi),
	};
	const backlogFull = (error: unknown): error is UrsulaWriteRefusedError => error instanceof UrsulaWriteRefusedError && error.kind === "backlog_full";
	function failure(error: unknown): unknown {
		const classified = classifyWrapped(error, db.boundPath);
		if (StorageRejected !== undefined && backlogFull(classified)) {
			return new StorageRejected(classified.message, { cause: classified });
		}
		return classified;
	}
	/**
	 * `work()`'s result once `synced()` resolved, and its failure as {@link failure} maps it. From Pi
	 * 1.2, a `backlog_full` refusal runs `work` again once `synced()` drained the backlog, unless
	 * `retry` is false: the refused attempt committed nothing, and the extension admits any commit
	 * while nothing is pending.
	 */
	async function gated<T>(work: () => Promise<T>, retry = true): Promise<T> {
		let result: T;
		for (;;) {
			try {
				result = await work();
				break;
			} catch (error) {
				const mapped = failure(error);
				if (!retry || !backlogFull(mapped)) throw mapped;
			}
			await db.synced();
		}
		await db.synced();
		return result;
	}
	/**
	 * Set while Pi 1.5's `Storage.commit` starts, which opens its transaction synchronously: that
	 * transaction is not run again on `backlog_full`, the commit call is. Its callback gathers the seqs
	 * it stored outside itself, so running it again would receive each batch twice.
	 */
	let committing = false;
	pi.transaction = <T>(callback: (transaction: SqliteExecutor) => Promise<T>): Promise<T> => {
		const retry = !committing;
		committing = false;
		return gated(() => transaction(callback), retry);
	};
	// Statements outside a transaction: a write commits on its own, and any statement reads.
	pi.run = (sql, ...params) => gated(() => run(sql, ...params));
	if (runCounted !== undefined) counted.runCounted = (sql, ...params) => gated(() => runCounted(sql, ...params));
	pi.exec = (sql) => gated(() => exec(sql));
	pi.get = <T extends object>(sql: string, ...params: Params): Promise<T | undefined> => gated(() => get<T>(sql, ...params));
	pi.all = <T extends object>(sql: string, ...params: Params): Promise<T[]> => gated(() => all<T>(sql, ...params));
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
	const storage = await SqliteStorage.open(pi);
	if (!groupedCommits) return storage;
	type Commit = (...args: unknown[]) => Promise<unknown>;
	const commit = (storage.commit as unknown as Commit).bind(storage);
	// Pi 1.5: a call refused with `backlog_full` stored none of its batches, and SqliteStorage keeps
	// what a call gathers in the call, advancing its IDs only once it committed, so the call runs
	// again with the same batches once `synced()` drained the backlog.
	(storage as unknown as { commit: Commit }).commit = async (...args) => {
		for (;;) {
			let attempt: Promise<unknown>;
			committing = true;
			try {
				attempt = commit(...args);
			} finally {
				committing = false;
			}
			try {
				return await attempt;
			} catch (error) {
				if (!backlogFull(error)) throw error;
			}
			await db.synced();
		}
	};
	return storage;
}
