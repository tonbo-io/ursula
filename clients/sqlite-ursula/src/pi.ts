// `@tonbo/sqlite-ursula/pi`: Pi Durable's official node SqliteStorage on a replicated file. The only
// module that imports `@earendil-works/pi-durable`, an optional peer dependency of the package, so an
// application that uses the root entry alone never installs it.
import { type SqliteExecutor, SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { type NodeSqliteStorageOptions, openNodeSqliteDatabase } from "@earendil-works/pi-durable/storage/sqlite/node";
import { errcode, refusedCommitError, SQLITE_IOERR_AUTH, SQLITE_READONLY, UrsulaReplicationError } from "./errors.ts";
import { type AttachOptions, attach, status } from "./index.ts";

/** {@link openUrsulaPiStorage}'s options: Pi's node driver settings, and how to attach. */
export type UrsulaPiStorageOptions = NodeSqliteStorageOptions & AttachOptions;

/**
 * Pi Durable's official node SqliteStorage on a replicated file: attaches `file` to `streamUrl`, then
 * opens it with Pi's node driver, unmodified. A commit the VFS fails surfaces as an
 * `UrsulaReplicationError` (SQLite has already rolled the transaction back, so the driver's own
 * rollback attempt would otherwise turn it into an AggregateError). The storage is unusable for
 * writes after that; re-open it to take the stream over again (after a delete and recreate, re-opening
 * rebuilds the file from the new stream).
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
	const transaction = db.transaction.bind(db);
	db.transaction = <T>(callback: (transaction: SqliteExecutor) => Promise<T>): Promise<T> =>
		transaction(callback).catch((error: unknown) => {
			const cause = error instanceof AggregateError ? error.errors[0] : error;
			const s = status(file);
			// Read-only: SQLite's own error, without the driver's failed ROLLBACK. The transaction is
			// rolled back either way: by SQLite when COMMIT failed, by the driver's ROLLBACK when a
			// statement failed before it.
			if (s.read_only && errcode(cause) === SQLITE_READONLY) throw cause;
			// Poisoned (by this commit, or by the snapshot thread since a refusal): the storage needs a
			// new attach whatever the error, a refusal's code included.
			if (s.poisoned) throw new UrsulaReplicationError(`commit not replicated: ${s.reason ?? "unknown"}`, s.fenced, cause);
			if (errcode(cause) === SQLITE_IOERR_AUTH) throw refusedCommitError(s, cause);
			throw error;
		});
	return SqliteStorage.open(db);
}
