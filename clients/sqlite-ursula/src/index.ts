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
// - pump.ts and pump-worker.ts: one worker per JavaScript thread that waits for the extension's
//   events, so no call on the thread itself waits on the network.
// - platform.ts: the prebuilt extension of this platform.
// - pi.ts: the `/pi` entry.
import type { DatabaseSyncOptions } from "node:sqlite";
import { call } from "./control.ts";
import { type Binding, UrsulaDatabase } from "./database.ts";
import { errorOf, UrsulaError, UrsulaSetupError, UrsulaUnavailableError } from "./errors.ts";
import { pump } from "./pump.ts";

export { loadUrsulaVfs } from "./control.ts";
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

/** {@link open}'s options: where the cache is, how to reach the stream, and node:sqlite's own settings. */
export interface OpenOptions extends Omit<DatabaseSyncOptions, "open" | "readOnly"> {
	/** The cache root, created when missing. By default `<user cache>/sqlite-ursula/0.8`. */
	readonly dir?: string | undefined;
	/** The bearer token requests carry. One per process in this release: the last open's applies. */
	readonly token?: string | undefined;
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
}

/** `ursula_open`'s answer. */
type Opened = ({ done: true } & Binding) | { done: false; request: number };

/** How long an open may take without a signal. */
const DEFAULT_OPEN_MS = 30_000;
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
 * `synced()` ever resolved on the provisional state. Without such a cache the open waits for the
 * stream until its signal fires.
 *
 * Errors say what to do through `action`: {@link UrsulaSetupError} (`fix`, or `retry` for `in_use` and
 * `claim_races`), and {@link UrsulaUnavailableError} when the signal fired first: the last condition
 * the open met, or `cancelled` when it met none.
 */
export async function open(url: string | URL, options: OpenOptions = {}): Promise<UrsulaDatabase> {
	const { dir, token, signal, maxPendingBytes, recreate, timeout, ...rest } = options;
	const stream = String(url);
	const settings: Record<string, unknown> = { timeout_ms: EXTENSION_OPEN_MS };
	if (token !== undefined) settings.token = token;
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
	const json = JSON.stringify(settings);
	const sqlite = { ...rest, timeout: timeout ?? DEFAULT_BUSY_MS };
	const until = signal ?? AbortSignal.timeout(DEFAULT_OPEN_MS);
	const key = `${stream}\n${dir ?? ""}`;
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

/** `promise`, or `signal`'s reason when it fires first. */
function raced<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
	return new Promise((resolve, reject) => {
		const abort = (): void => reject(signal.reason);
		signal.addEventListener("abort", abort, { once: true });
		promise.then(resolve, reject).finally(() => signal.removeEventListener("abort", abort));
	});
}

/** The open of `stream` gave up at `signal` before it met any condition. */
function cancelled(stream: string, signal: AbortSignal): UrsulaUnavailableError {
	return new UrsulaUnavailableError(`cancelled: the open of ${stream} gave up when its signal fired: ${reasonOf(signal.reason)}`, "cancelled", signal.reason);
}

const reasonOf = (reason: unknown): string => (reason instanceof Error ? reason.message : String(reason));
/** A connection to what `ursula_open` bound, with node:sqlite's `settings`. */
function connect(binding: Binding, settings: Omit<DatabaseSyncOptions, "open" | "readOnly">): UrsulaDatabase {
	try {
		return new UrsulaDatabase(binding.path, settings, { name: binding.name, path: binding.path });
	} catch (error) {
		if (error instanceof UrsulaError) throw error;
		const why = error instanceof Error ? error.message : String(error);
		throw new UrsulaSetupError(`invalid_argument: bound ${binding.name}, then node:sqlite refused to open ${binding.path} (${why})`, "invalid_argument", error);
	}
}
