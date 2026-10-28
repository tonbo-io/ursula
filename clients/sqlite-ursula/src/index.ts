// SQLite replicated through an Ursula stream by the sqlite-ursula-vfs loadable extension
// (clients/sqlite-vfs), 0.8. A commit is local: the extension appends it to a log it owns and
// replicates the log to the stream in the background. `await db.synced()` is the output gate.
//
// The package's root entry. It depends on nothing outside Node.js: Pi Durable's storage is the
// `@tonbo/sqlite-ursula/pi` entry (pi.ts), whose `@earendil-works/pi-durable` is an optional peer
// dependency. Nothing reachable from here may import it.
//
// Module map:
//
// - control.ts: the extension's control connection on this thread, and calls of its SQL functions.
// - errors.ts: the four error classes, and `classify`, the one mapping from a failure of the
//   extension to them, which the root entry and the Pi adapter share.
// - database.ts: the database `open` returns, a node:sqlite DatabaseSync with `synced()`, `status()`
//   and the `reset` listeners, whose statements classify their errors.
// - gate.ts: `gate`, which hands a unit of work's outcome, and a returned Response's body chunk by
//   chunk, on only after `synced()`.
// - pump.ts and pump-worker.ts: one worker per JavaScript thread that waits for the extension's
//   events, so no call on the thread itself waits on the network.
// - token.ts: the bearer token, a string or the latest from a provider, refreshed ahead of expiry
//   and after a 401.
// - abort.ts: waiting under an AbortSignal.
// - platform.ts: the prebuilt extension of this platform.
// - pi.ts: the `/pi` entry.
// - tonbo.ts: the `/tonbo` entry, which opens a Tonbo database by name with a Tonbo API key, or
//   resolves it into the stream URL and token another opener, such as the `/pi` entry's, takes.
// - tonbo-pi.ts: the `/tonbo/pi` entry, which opens Pi storage on a Tonbo database by name: the
//   `/tonbo` entry's resolve, then the `/pi` entry's open.
import type { DatabaseSyncOptions } from "node:sqlite";
import { call } from "./control.ts";
import { type Binding, UrsulaDatabase } from "./database.ts";
import { DEFAULT_OPEN_MS, raced, reasonOf } from "./abort.ts";
import { errorOf, UrsulaError, UrsulaSetupError, UrsulaUnavailableError } from "./errors.ts";
import { pump } from "./pump.ts";
import { bound, openWithProvider, type TokenProvider, useStringToken } from "./token.ts";

export { loadUrsulaVfs } from "./control.ts";
export { type GateOptions, gate } from "./gate.ts";
export { type ResetListener, UrsulaDatabase, type UrsulaSalvage, type UrsulaStatus } from "./database.ts";
export {
	type ResetKind,
	type SetupKind,
	type UnavailableKind,
	UrsulaError,
	type UrsulaErrorAction,
	type UrsulaErrorKind,
	UrsulaResetError,
	UrsulaSetupError,
	UrsulaUnavailableError,
	UrsulaWriteRefusedError,
	type WriteRefusedKind,
} from "./errors.ts";
export type { BearerToken, TokenProvider } from "./token.ts";

/** {@link open}'s options: where the cache is, how to reach the stream, and node:sqlite's own settings. */
export interface OpenOptions extends Omit<DatabaseSyncOptions, "open" | "readOnly"> {
	/**
	 * The cache root, created when missing. By default (`undefined`) `<user cache>/sqlite-ursula/0.8`,
	 * also when the stream is open in this process under another root.
	 */
	readonly dir?: string | undefined;
	/**
	 * The bearer token requests carry, or a {@link TokenProvider} that hands out tokens and when they
	 * expire. The package calls a provider before the open's first request when it holds no valid
	 * token, again ahead of each token's expiry, and again at once after a request was refused with
	 * 401, and stops once no binding this thread opened is bound. One token per process: the last
	 * open's applies. A later open with another function names the function the next call runs, and
	 * shares the token held meanwhile. One with a string stops the provider on its thread.
	 */
	readonly token?: string | TokenProvider | undefined;
	/** Bounds the open: by default it gives up after 30 s. */
	readonly signal?: AbortSignal | undefined;
	/** How many bytes of compressed commits may wait for the stream before writes fail with `backlog_full`. By default 64 MiB. */
	readonly maxPendingBytes?: number | undefined;
	/**
	 * When the stream is missing and the local cache holds data: create the stream again, moving the
	 * cache aside into the cache root's `salvage/` (reason `recreated`), instead of failing with
	 * `not_found`. By default `false`.
	 */
	readonly recreate?: boolean | undefined;
	/**
	 * Opens the database read-only and claims nothing, so it never fences the owner, and its writes
	 * fail with SQLITE_READONLY (8). In a process that owns the stream it is a read-only connection on
	 * the owner's binding, which sees the owner's latest commits, including ones the stream has not
	 * acknowledged yet: its `synced()` waits for them, as the owner's does. Otherwise the extension
	 * builds or catches up a reader cache of its own (`role: "reader"` in `status()`, `reader-<n>` in
	 * the stream's cache directory, one per reader process) to the stream's tail at the open, and the
	 * connection reads the stream as of then: it does not follow later commits, and its `synced()`
	 * resolves at once. A closed stream opens too. A reader cache built on another deployment fails
	 * with `wrong_cluster`, as an owner's does. A later `readOnly` open in the process
	 * shares that cache while one of its connections is open, and once they all closed the next one
	 * catches up again. When the stream is unreachable it opens an earlier reader cache as it is, and
	 * without one waits until the signal fires. A missing stream fails with `not_found`. Cannot go
	 * with `recreate`. By default `false`.
	 */
	readonly readOnly?: boolean | undefined;
}

/** `ursula_open`'s answer. */
type Opened = ({ done: true } & Binding) | { done: false; request: number };

/** The extension's own deadline for an open: far past any signal, which governs the open. */
const EXTENSION_OPEN_MS = 24 * 60 * 60 * 1000;
/** node:sqlite's busy `timeout` unless the caller passes one: the connections of a binding share its files. */
const DEFAULT_BUSY_MS = 5_000;

/** The bind in flight on this thread, by stream URL and cache root: concurrent opens share it, its failure included. */
const inflight = new Map<string, Promise<Binding>>();
/** Errors of an open given up at its own caller's signal: an open that shared it opens anew. */
const byAbort = new WeakSet<object>();

/**
 * Opens the database a stream holds and makes this process its owner: the first open of a stream in
 * the process binds it, catches the local cache up and claims the stream (fencing every earlier
 * owner). Later opens of it in the process return another connection on the same binding, with no
 * network. Concurrent opens share one bind and its outcome. Worker threads call `open` too. After a
 * reset, an open binds anew at once, beside the reset database's connections: it works on a copy of
 * their local cache, and only another process gets `in_use`.
 *
 * A stream is open under one cache root at a time in the process, and a missing `dir` names the
 * default root, not the one it is open under. An open under another root waits for the database
 * there to retire once its connections closed, and fails `bound_elsewhere` (`fix`) while it has
 * connections open or an open of it that has not connected yet: close it first, or pass its
 * `dir`.
 *
 * Returns a node:sqlite `DatabaseSync` (a subclass, {@link UrsulaDatabase}), which any library that
 * takes one takes. Statements are local: commit, then `await db.synced()` before anything outside the
 * process acts on what you wrote or read. The connections of a binding share its files, so
 * node:sqlite's busy `timeout` is 5000 ms unless you pass one.
 *
 * When the stream is unreachable (its first request fails before connecting: refused at once, or
 * after the name lookup and the connect, each limited to 5 s), an open with a trusted local cache
 * that claimed the stream before resolves then, `provisional` in `status()`: reads come from the
 * cache, commits are local and pending, and `synced()` waits. Once the stream answers, the
 * extension checks it from the cache's acknowledged state to its tail, claims it only when no
 * other owner wrote or claimed there, and the database is `live`. If another owner took the
 * stream meanwhile, the database resets (`fenced` or `stale`) with nothing sent, and no
 * `synced()` ever resolved on the provisional state. A stream left idle past producer expiry,
 * whose claim the check could not see, resets `stale` too: reopen, and the new open takes over
 * with the pending commits. Commits made while provisional survive a history change that lost
 * nothing (such as a restart of a single node) and replicate once the stream answers. Without such
 * a cache the open waits for the stream until its signal fires.
 *
 * With `readOnly` the open claims nothing: see {@link OpenOptions.readOnly}.
 *
 * Errors say what to do through `action`: {@link UrsulaSetupError} (`fix`, or `retry` for `in_use` and
 * `claim_races`), and {@link UrsulaUnavailableError} when the signal fired first: the last condition
 * the open met, or `cancelled` when it met none. With a token provider, the open also rejects with
 * the provider's own {@link UrsulaError} when it fails for good, with an `UrsulaSetupError`
 * `invalid_argument` when it hands out a token that cannot be sent, and with an
 * `UrsulaUnavailableError` `unavailable` whose cause is the provider's last error when it handed out
 * no token before the signal fired. A bind refused with 401 after a failed call of the provider
 * fails `unauthorized` with that failure as its cause, and action `retry` unless it failed for good.
 */
export async function open(url: string | URL, options: OpenOptions = {}): Promise<UrsulaDatabase> {
	const { dir, token, signal, maxPendingBytes, recreate, readOnly, timeout, ...rest } = options;
	const stream = String(url);
	const settings: Record<string, unknown> = { timeout_ms: EXTENSION_OPEN_MS };
	if (token !== undefined && typeof token !== "string" && typeof token !== "function") {
		throw new UrsulaSetupError(`invalid_argument: token must be a string or a token provider function, not ${typeof token}`, "invalid_argument");
	}
	if (maxPendingBytes !== undefined) {
		if (!Number.isSafeInteger(maxPendingBytes) || maxPendingBytes < 0) {
			throw new UrsulaSetupError(`invalid_argument: maxPendingBytes must be a whole number of bytes, not ${maxPendingBytes}`, "invalid_argument");
		}
		settings.max_pending_bytes = maxPendingBytes;
	}
	if (recreate !== undefined) {
		if (typeof recreate !== "boolean") {
			throw new UrsulaSetupError(`invalid_argument: recreate must be a boolean, not ${String(recreate)}`, "invalid_argument");
		}
		settings.recreate = recreate;
	}
	if (readOnly !== undefined) {
		if (typeof readOnly !== "boolean") {
			throw new UrsulaSetupError(`invalid_argument: readOnly must be a boolean, not ${String(readOnly)}`, "invalid_argument");
		}
		// With `recreate` too, the extension refuses it: `invalid_argument`.
		settings.read_only = readOnly;
	}
	const json = JSON.stringify(settings);
	const sqlite = { ...rest, timeout: timeout ?? DEFAULT_BUSY_MS, readOnly: readOnly === true };
	const until = signal ?? AbortSignal.timeout(DEFAULT_OPEN_MS);
	// Either kind of token is applied once, here, with `ursula_set_token`, never in `ursula_open`'s
	// options, which a bind sends again on each of its retries, over a token applied meanwhile.
	if (typeof token === "function") return openWithProvider(token, stream, until, () => opened(stream, dir, json, sqlite, readOnly === true, until));
	if (typeof token === "string") useStringToken(token);
	return bound(await opened(stream, dir, json, sqlite, readOnly === true, until));
}

/** The open of `stream` once its token is set: a connection on its binding, binding it first unless another open does. */
async function opened(stream: string, dir: string | undefined, json: string, sqlite: Omit<DatabaseSyncOptions, "open">, readOnly: boolean, until: AbortSignal): Promise<UrsulaDatabase> {
	// An owner's and a reader's binds of one stream are different binds.
	const key = `${stream}\n${dir ?? ""}\n${readOnly ? "reader" : "owner"}`;
	for (;;) {
		if (until.aborted) throw marked(cancelled(stream, until));
		const shared = inflight.get(key);
		if (shared !== undefined) {
			try {
				await raced(shared, until);
			} catch (error) {
				if (until.aborted) throw marked(cancelled(stream, until));
				// The open that bound gave up at its own caller's signal, not this one's.
				if (byAbort.has(error as object)) continue;
				throw error;
			}
			// Bound: a connection of this call's own on the binding.
			const again = call<Opened>("SELECT ursula_open(?, ?, ?, 0)", stream, dir ?? null, json);
			if (again.done) return connect(again, sqlite);
			continue;
		}
		const binding = bind(stream, dir, json, until);
		inflight.set(key, binding);
		try {
			return connect(await binding, sqlite);
		} finally {
			if (inflight.get(key) === binding) inflight.delete(key);
		}
	}
}

/** Binds `stream`: `ursula_open` with timeout 0, waiting on the pump while it goes on. */
async function bind(stream: string, dir: string | undefined, json: string, until: AbortSignal): Promise<Binding> {
	for (;;) {
		// Started before the call, so its events cursor precedes the request.
		const events = pump();
		let opened: Opened;
		try {
			opened = call<Opened>("SELECT ursula_open(?, ?, ?, 0)", stream, dir ?? null, json);
		} catch (error) {
			// Another thread's open of the stream, which this one shared, was cancelled: open anew.
			if (error instanceof UrsulaUnavailableError && error.kind === "cancelled" && !until.aborted) continue;
			throw error;
		}
		if (opened.done) return opened;
		try {
			await events.opened(opened.request, until);
		} catch {
			throw giveUp(stream, opened.request, until);
		}
	}
}

/** The signal fired: cancels `request`, and the error is the last condition it met, or `cancelled`. */
function giveUp(stream: string, request: number, signal: AbortSignal): UrsulaError {
	let ended: { kind: string; message: string } | null = null;
	try {
		ended = call<{ kind: string; message: string } | null>("SELECT ursula_cancel(?)", request);
	} catch {
		// It resolved meanwhile and its result was taken.
	}
	if (ended === null || ended.kind === "cancelled") return marked(cancelled(stream, signal));
	return marked(errorOf(ended.kind, `${ended.message}: the open of ${stream} gave up when its signal fired`, signal.reason, false));
}

const marked = <E extends UrsulaError>(error: E): E => {
	byAbort.add(error);
	return error;
};

/** The open of `stream` gave up at `signal` before it met any condition. */
function cancelled(stream: string, signal: AbortSignal): UrsulaUnavailableError {
	return new UrsulaUnavailableError(`cancelled: the open of ${stream} gave up when its signal fired: ${reasonOf(signal.reason)}`, "cancelled", signal.reason);
}

/** A connection to what `ursula_open` bound, with node:sqlite's `settings`. */
function connect(binding: Binding, settings: Omit<DatabaseSyncOptions, "open">): UrsulaDatabase {
	try {
		return new UrsulaDatabase(binding.path, settings, { name: binding.name, path: binding.path });
	} catch (error) {
		if (error instanceof UrsulaError) throw error;
		const why = error instanceof Error ? error.message : String(error);
		throw new UrsulaSetupError(`invalid_argument: bound ${binding.name}, then node:sqlite refused to open ${binding.path} (${why})`, "invalid_argument", error);
	}
}

/** {@link importSqlite}'s options. */
export interface ImportOptions {
	/**
	 * The bearer token the import's requests carry, for the import alone. A string only: the import
	 * blocks its thread, so no provider could refresh it meanwhile. To import with a provider, pass
	 * `(await provider({ signal })).token`.
	 */
	readonly token?: string | undefined;
	/** How long the import may take. By default 10 minutes. */
	readonly timeoutMs?: number | undefined;
}

/** What {@link importSqlite} published: the stream's first snapshot. */
export interface Imported {
	/** The snapshot's stream offset, the end of the import's claim. */
	readonly offset: number;
	readonly digest: string;
	readonly pages: number;
	/** The compressed snapshot's size. */
	readonly bytes: number;
}

/** How long an import may take without `timeoutMs`. */
const DEFAULT_IMPORT_MS = 10 * 60 * 1000;

/**
 * Imports the plain SQLite database `file` into a new stream at `url`, as the stream's first
 * snapshot, page for page. The file must have 4096-byte pages (`PRAGMA page_size = 4096; VACUUM
 * INTO ...` makes such a copy), no `-wal` beside it and no hot journal. It is only read. The stream
 * must be missing or empty: anything else fails with `stream_state` and nothing is published. Then
 * {@link open} the stream as any other. While the import runs, and after an import that failed
 * once it claimed the stream, opens and imports of the stream fail with `stream_state` until the
 * stream is deleted or recreated.
 *
 * It blocks the calling thread until it is done, for at most `timeoutMs`. Errors are
 * {@link UrsulaSetupError} (such as `invalid_argument` for the file) and
 * {@link UrsulaUnavailableError}, whose action is `fix`: the stream may hold the import's claim.
 */
export function importSqlite(url: string | URL, file: string, options: ImportOptions = {}): Imported {
	const { token, timeoutMs } = options;
	const timeout = timeoutMs ?? DEFAULT_IMPORT_MS;
	if (!Number.isSafeInteger(timeout) || timeout <= 0) {
		throw new UrsulaSetupError(`invalid_argument: timeoutMs must be a positive whole number of milliseconds, not ${timeout}`, "invalid_argument");
	}
	const settings: Record<string, unknown> = {};
	if (token !== undefined) settings.token = token;
	return call<Imported>("SELECT ursula_import(?, ?, ?, ?)", String(url), file, JSON.stringify(settings), timeout);
}
