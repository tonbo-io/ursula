// The extension's control connection on this JavaScript thread, and the one way the package calls the
// extension's SQL functions on it. Every thread that uses the package (the main thread, each worker
// thread, the pump's worker) loads the library the process already holds: the registry of bound
// databases and the replication driver are the process's, shared by all of them.
import { DatabaseSync, type StatementSync } from "node:sqlite";
import { type Described, errcode, errorOf, findWrapped, UrsulaError, UrsulaSetupError } from "./errors.ts";
import { prebuiltExtension } from "./platform.ts";

let control: DatabaseSync | undefined;
let library: string | undefined;
/** `ursula_classify` on the control connection, prepared when the library loads. */
let classifier: StatementSync | undefined;

/**
 * Loads the extension once per thread and returns the thread's control connection. The library is
 * `path`, else SQLITE_URSULA_VFS when set, else the prebuilt one for this platform
 * (`@tonbo/sqlite-ursula-<platform>`, an optional dependency). A library without `ursula_classify`,
 * which says which failure is which error, is older than this package: it fails here, with an
 * {@link UrsulaSetupError} (`local_setup`), rather than leave every failure unclassified.
 */
export function loadUrsulaVfs(path = process.env.SQLITE_URSULA_VFS): DatabaseSync {
	if (control !== undefined) return control;
	const chosen = path === undefined || path.length === 0 ? prebuiltExtension() : path;
	const db = new DatabaseSync(":memory:", { allowExtension: true });
	let prepared: StatementSync;
	try {
		db.loadExtension(chosen);
		try {
			prepared = db.prepare("SELECT ursula_classify(?, ?, ?)");
		} catch (error) {
			throw new UrsulaSetupError(
				`local_setup: the sqlite-ursula extension at ${chosen} has no ursula_classify: it is older than this package, so load the library of the package's own version`,
				"local_setup",
				error,
			);
		}
	} catch (error) {
		db.close();
		throw error;
	}
	control = db;
	library = chosen;
	classifier = prepared;
	return db;
}

/** The library this thread loaded, which the pump's worker loads too. */
export function loadedLibrary(): string {
	loadUrsulaVfs();
	return library as string;
}

/** One of the extension's SQL functions on the control connection: its JSON result, or its error as an UrsulaError. */
export function call<T>(sql: string, ...args: (string | number | null)[]): T {
	const db = loadUrsulaVfs();
	let row: Record<string, unknown>;
	try {
		row = db.prepare(sql).get(...args) as Record<string, unknown>;
	} catch (error) {
		throw classify(error);
	}
	return parsed<T>(row);
}

/** A SQL function's JSON result, from its row. */
function parsed<T>(row: Record<string, unknown>): T {
	const text = Object.values(row)[0];
	return (typeof text === "string" && text.length > 0 ? JSON.parse(text) : undefined) as T;
}

/**
 * What to throw for `error`, which a statement or an Ursula SQL function threw: the
 * {@link UrsulaError} of what the extension's `ursula_classify` makes of it, and `error` itself,
 * unchanged, when the extension says it is not its failure (a constraint violation, a syntax
 * error, an I/O error of an attached database while the database at `path` has no reset and has
 * refused no write). `path` is the bound database the statement ran on, whose state names a
 * statement's I/O error, which says nothing.
 */
export function classify(error: unknown, path?: string): unknown {
	if (error instanceof UrsulaError) return error;
	const code = errcode(error);
	if (classifier === undefined || code === undefined || !(error instanceof Error)) return error;
	let named: Described | null | undefined;
	try {
		named = parsed<Described | null | undefined>(classifier.get(error.message, code, path ?? null) as Record<string, unknown>);
	} catch {
		return error;
	}
	return named === null || named === undefined ? error : errorOf(named, named.message, error);
}

/**
 * {@link classify} for an error a library may have wrapped: the first error of the chain
 * {@link findWrapped} walks that maps to an {@link UrsulaError} is the result, so a failure of the
 * extension never passes for the wrapper's own kind of error. `error` itself otherwise.
 */
export function classifyWrapped(error: unknown, path: string): unknown {
	return (
		findWrapped(error, (next) => {
			const classified = classify(next, path);
			return classified instanceof UrsulaError ? classified : undefined;
		}) ?? error
	);
}
