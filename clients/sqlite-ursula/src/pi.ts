// `@tonbo/sqlite-ursula/pi`: Pi Durable's official node SqliteStorage on a replicated file. The only
// module that imports `@earendil-works/pi-durable`, an optional peer dependency of the package, so an
// application that uses the root entry alone never installs it.
import { type SqliteExecutor, SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { type NodeSqliteStorageOptions, openNodeSqliteDatabase } from "@earendil-works/pi-durable/storage/sqlite/node";
import { errcode, refusedCommitError, SQLITE_IOERR, SQLITE_IOERR_AUTH, SQLITE_IOERR_READ, SQLITE_READONLY, UrsulaReplicationError } from "./errors.ts";
import { type AttachOptions, type AttachStatus, attach, status } from "./index.ts";

/** A statement's parameters, as Pi's executor takes them. */
type Params = SqliteExecutor["run"] extends (sql: string, ...params: infer P) => unknown ? P : never;

/** {@link openUrsulaPiStorage}'s options: Pi's node driver settings, and how to attach. */
export type UrsulaPiStorageOptions = NodeSqliteStorageOptions & AttachOptions;

/**
 * Pi Durable's official node SqliteStorage on a replicated file: attaches `file` to `streamUrl`, then
 * opens it with Pi's node driver, unmodified. A commit the VFS fails surfaces as an
 * `UrsulaReplicationError` (SQLite has already rolled the transaction back, so the driver's own
 * rollback attempt would otherwise turn it into an AggregateError). The storage is unusable for
 * writes after that; re-open it to take the stream over again (after a delete and recreate, re-opening
 * rebuilds the file from the new stream). Once the file is known to be fenced (a newer owner claimed
 * the stream, or it was deleted and recreated), its reads fail too, also with an
 * `UrsulaReplicationError` whose `fenced` is `true` and whose message says the read was refused: the
 * file no longer serves the state the new owner moved past.
 *
 * A commit refused before it reached the stream surfaces as an `UrsulaWriteRefusedError` instead
 * (`UrsulaPaymentRequiredError` for a `402`), and the storage stays usable: the next commit goes
 * out as usual. With `{ readOnly: true }` the file is attached read-only: the storage opens on a
 * database Pi has already written, its reads work, and every commit that changes it fails with
 * SQLITE_READONLY. The error classes are exported by the root entry, `@tonbo/sqlite-ursula`.
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
	const committed = (error: unknown): never => {
		throw commitFailure(file, error);
	};
	const read = (error: unknown): never => {
		throw readFailure(file, error);
	};
	db.transaction = <T>(callback: (transaction: SqliteExecutor) => Promise<T>): Promise<T> => transaction(callback).catch(committed);
	// Statements outside a transaction: a write commits on its own, and any statement reads.
	db.run = (sql, ...params) => run(sql, ...params).catch(committed);
	db.exec = (sql) => exec(sql).catch(committed);
	db.get = <T extends object>(sql: string, ...params: Params): Promise<T | undefined> => get<T>(sql, ...params).catch(read);
	db.all = <T extends object>(sql: string, ...params: Params): Promise<T[]> => all<T>(sql, ...params).catch(read);
	return SqliteStorage.open(db);
}

/** `status(file)`, or `undefined` when the file is not attached (any more). */
function currentStatus(file: string): AttachStatus | undefined {
	try {
		return status(file);
	} catch {
		return undefined;
	}
}

/** The error to throw for `error`, which a statement that may commit (a transaction, or a write outside one) threw on `file`. */
function commitFailure(file: string, error: unknown): unknown {
	const cause = error instanceof AggregateError ? error.errors[0] : error;
	const s = currentStatus(file);
	if (s === undefined) return error;
	// Read-only: SQLite's own error, without the driver's failed ROLLBACK. The transaction is
	// rolled back either way: by SQLite when COMMIT failed, by the driver's ROLLBACK when a
	// statement failed before it.
	if (s.read_only && errcode(cause) === SQLITE_READONLY) return cause;
	// Poisoned (by this commit, or by the snapshot thread since a refusal): the storage needs a
	// new attach whatever the error, a refusal's code included. A transaction whose first read failed
	// on a fenced file (a read-only one too, such as Pi's `document()`) never started: a read refused.
	if (s.poisoned && s.fenced && errcode(cause) === SQLITE_IOERR_READ) return fencedRead(s, cause);
	if (s.poisoned) return new UrsulaReplicationError(`commit not replicated: ${s.reason ?? "unknown"}`, s.fenced, cause);
	if (errcode(cause) === SQLITE_IOERR_AUTH) return refusedCommitError(s, cause);
	return error;
}

/** The error to throw for `error`, which a read threw on `file`: once the file is known to be fenced, its reads fail with an I/O error. */
function readFailure(file: string, error: unknown): unknown {
	const code = errcode(error);
	if (code === undefined || (code & 0xff) !== SQLITE_IOERR) return error;
	const s = currentStatus(file);
	if (s?.poisoned === true && s.fenced) return fencedRead(s, error);
	return error;
}

/** A read the VFS refused on a fenced file (`s`), which threw `cause`. */
function fencedRead(s: AttachStatus, cause: unknown): UrsulaReplicationError {
	return new UrsulaReplicationError(`read refused, the file is fenced: ${s.reason ?? "unknown"}`, true, cause);
}
