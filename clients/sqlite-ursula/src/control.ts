// The extension's control connection on this JavaScript thread, and the one way the package calls the
// extension's SQL functions on it. Every thread that uses the package (the main thread, each worker
// thread, the pump's worker) loads the library the process already holds: the registry of bound
// databases and the replication driver are the process's, shared by all of them.
import { DatabaseSync } from "node:sqlite";
import { classify } from "./errors.ts";
import { prebuiltExtension } from "./platform.ts";

let control: DatabaseSync | undefined;
let library: string | undefined;

/**
 * Loads the extension once per thread and returns the thread's control connection. The library is
 * `path`, else SQLITE_URSULA_VFS when set, else the prebuilt one for this platform
 * (`@tonbo/sqlite-ursula-<platform>`, an optional dependency).
 */
export function loadUrsulaVfs(path = process.env.SQLITE_URSULA_VFS): DatabaseSync {
	if (control !== undefined) return control;
	const chosen = path === undefined || path.length === 0 ? prebuiltExtension() : path;
	const db = new DatabaseSync(":memory:", { allowExtension: true });
	db.loadExtension(chosen);
	control = db;
	library = chosen;
	return db;
}

/** The library this thread loaded, which the pump's worker loads too. */
export function loadedLibrary(): string {
	loadUrsulaVfs();
	return library as string;
}

/** One of the extension's SQL functions on the control connection: its JSON result, or its error as an UrsulaError. */
export function call<T>(sql: string, ...args: (string | number | null)[]): T {
	let row: Record<string, unknown>;
	try {
		row = loadUrsulaVfs().prepare(sql).get(...args) as Record<string, unknown>;
	} catch (error) {
		throw classify(error);
	}
	const text = Object.values(row)[0];
	return (typeof text === "string" && text.length > 0 ? JSON.parse(text) : undefined) as T;
}
