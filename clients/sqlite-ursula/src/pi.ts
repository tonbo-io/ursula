// `@tonbo/sqlite-ursula/pi`: Pi Durable's official node SqliteStorage on a replicated file. The only
// module that imports `@earendil-works/pi-durable`, an optional peer dependency of the package, so an
// application that uses the root entry alone never installs it.
import { type SqliteExecutor, SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { type NodeSqliteStorageOptions, openNodeSqliteDatabase } from "@earendil-works/pi-durable/storage/sqlite/node";
import { classify, errcode, SQLITE_READONLY } from "./errors.ts";
import { type AttachOptions, attach, status } from "./index.ts";

/** A statement's parameters, as Pi's executor takes them. */
type Params = SqliteExecutor["run"] extends (sql: string, ...params: infer P) => unknown ? P : never;

/** {@link openUrsulaPiStorage}'s options: Pi's node driver settings, and how to attach. */
export type UrsulaPiStorageOptions = NodeSqliteStorageOptions & AttachOptions;

/**
 * Pi Durable's official node SqliteStorage on a replicated file: attaches `file` to `streamUrl`, then
 * opens it with Pi's node driver, unmodified. Its failures throw the root entry's errors, from the
 * mapping `open`'s database uses, each with a `kind` and an `action`.
 *
 * A commit the VFS fails surfaces as an `UrsulaReplicationError` (SQLite has already rolled the
 * transaction back, so the driver's own rollback attempt would otherwise turn it into an
 * AggregateError): an `UrsulaFencedError` (`fenced: true`, `action` `stop`) when another owner took
 * the stream over, and otherwise usually an `UrsulaReattachError` (`action` `reattach`). The storage
 * is unusable for writes after that; re-open it to take the stream over again (after a delete and
 * recreate, re-opening rebuilds the file from the new stream). Once the file is known to be fenced (a
 * newer owner claimed the stream, or it was deleted and recreated), its reads fail too, also with an
 * `UrsulaFencedError` whose message says the read was refused: the file no longer serves the state
 * the new owner moved past.
 *
 * A commit refused before it reached the stream surfaces as an `UrsulaWriteRefusedError` instead
 * (`UrsulaPaymentRequiredError` for a `402`, `action` `retry`), and the storage stays usable: the next
 * commit goes out as usual. A failed attach throws an `UrsulaError` as `attach` does. With
 * `{ readOnly: true }` the file is attached read-only: the storage opens on a database Pi has already
 * written, its reads work, and every commit that changes it fails with SQLITE_READONLY. The error
 * classes are exported by the root entry, `@tonbo/sqlite-ursula`.
 */
export async function openUrsulaPiStorage(file: string, streamUrl: string, options: UrsulaPiStorageOptions = {}): Promise<SqliteStorage> {
	const { readOnly, ...sqlite } = options;
	attach(file, streamUrl, readOnly === undefined ? {} : { readOnly });
	const db = await openNodeSqliteDatabase(file, sqlite);
	const { transaction, run, exec, get, all } = {
		transaction: db.transaction.bind(db),
		run: db.run.bind(db),
		exec: db.exec.bind(db),
		get: db.get.bind(db),
		all: db.all.bind(db),
	};
	const failed = (error: unknown): never => {
		throw failure(file, error);
	};
	db.transaction = <T>(callback: (transaction: SqliteExecutor) => Promise<T>): Promise<T> => transaction(callback).catch(failed);
	// Statements outside a transaction: a write commits on its own, and any statement reads.
	db.run = (sql, ...params) => run(sql, ...params).catch(failed);
	db.exec = (sql) => exec(sql).catch(failed);
	db.get = <T extends object>(sql: string, ...params: Params): Promise<T | undefined> => get<T>(sql, ...params).catch(failed);
	db.all = <T extends object>(sql: string, ...params: Params): Promise<T[]> => all<T>(sql, ...params).catch(failed);
	return SqliteStorage.open(db);
}

/** The error to throw for `error`, which a statement or a transaction of Pi's driver threw on `file`. */
function failure(file: string, error: unknown): unknown {
	// A COMMIT that SQLite rolled back fails the driver's ROLLBACK after it: an AggregateError of both.
	const cause = error instanceof AggregateError ? error.errors[0] : error;
	const classified = classify(cause, () => status(file));
	if (classified !== cause) return classified;
	// Read-only: SQLite's own error, without the driver's failed ROLLBACK. The transaction is rolled
	// back either way: by SQLite when COMMIT failed, by the driver's ROLLBACK when a statement failed
	// before it.
	if (errcode(cause) === SQLITE_READONLY) return cause;
	return error;
}
