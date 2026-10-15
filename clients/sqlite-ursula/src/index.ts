// SQLite replicated through an Ursula stream by the sqlite-ursula-vfs loadable extension
// (clients/sqlite-vfs), 0.8. The extension registers the "ursula" VFS as SQLite's default and the
// SQL functions `ursula_open`, `ursula_synced`, `ursula_events`, `ursula_cancel`, `ursula_status`,
// `ursula_stats` and `ursula_set_token`: commits are local, and replicate in the background.
//
// This release of the package only loads the extension. The asynchronous `open`, `synced()` and the
// reset listeners come next (the 0.8 TypeScript API); until then hosts call the SQL functions on the
// connection `loadUrsulaVfs` returns.
//
// Module map:
//
// - platform.ts: the prebuilt extension of this platform.
import { DatabaseSync } from "node:sqlite";
import { prebuiltExtension } from "./platform.ts";

let control: DatabaseSync | undefined;

/**
 * Loads the extension once per process and returns the control connection. The library is `path`,
 * else SQLITE_URSULA_VFS when set, else the prebuilt one for this platform
 * (`@tonbo/sqlite-ursula-<platform>`, an optional dependency).
 */
export function loadUrsulaVfs(path = process.env.SQLITE_URSULA_VFS): DatabaseSync {
	if (control !== undefined) return control;
	const library = path === undefined || path.length === 0 ? prebuiltExtension() : path;
	const db = new DatabaseSync(":memory:", { allowExtension: true });
	db.loadExtension(library);
	control = db;
	return db;
}
