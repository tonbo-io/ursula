// `@tonbo/sqlite-ursula/tonbo` (design: Tonbo): open a Tonbo database by name with a Tonbo API
// key. The extension's `tonbo_resolve` does the work, in one implementation for every host: it
// finds the key, looks the name up in the Management API and, unless the open brings a token of its
// own, makes the key the process's source of Streams tokens, which it exchanges and refreshes
// itself, holds the first one before it answers, and clears the token set. This entry calls it
// without blocking (timeout 0, then its `resolved` event through the pump, as `open` waits for
// `ursula_open`), joins this thread's concurrent resolves of the same arguments, and turns the
// failure the extension reports as fields into the package's classes. `resolveDatabase` is that
// resolve on its own: the stream URL and the `token` to open it with, which `open` passes to the
// root entry's `open` and a caller of another opener, such as the `/pi` entry's
// `openUrsulaPiStorage`, passes to that.
//
// - The key: the `apiKey` option, else TONBO_API_KEY, else the file `tonbo login` writes (the
//   Tonbo CLI's credentials contract, version 1), as the extension reads them.
// - An explicit `token` serves the Streams requests instead of the key's tokens (`own_token`), and
//   so does URSULA_VFS_TOKEN_FILE, which the extension reads.
// - When the key's tokens serve (`key_tokens` in the answer), they serve every Streams request of
//   the process from the resolve on: the extension cleared the token an earlier open set, and this
//   thread's token provider stops (token.ts).
//
// Nothing here logs, and no error message carries a key or a token.
import { DEFAULT_OPEN_MS, raced, reasonOf } from "./abort.ts";
import { call } from "./control.ts";
import { type Described, errorOf, type TonboKind, UrsulaError, UrsulaSetupError, UrsulaUnavailableError } from "./errors.ts";
import { type OpenOptions, open as openStream, type UrsulaDatabase } from "./index.ts";
import { pump } from "./pump.ts";
import { type TokenProvider, useKeyTokens } from "./token.ts";

export { type GateOptions, gate } from "./gate.ts";
export { type ResetListener, UrsulaDatabase, type UrsulaSalvage, type UrsulaStatus } from "./database.ts";
export {
	type ResetKind,
	type SetupKind,
	type TonboKind,
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

/** The extension's own deadline for a resolve: far past any signal, which governs it. */
const EXTENSION_RESOLVE_MS = 24 * 60 * 60 * 1000;
/** The kinds a {@link TonboError} carries. */
const TONBO_KINDS: ReadonlySet<string> = new Set<TonboKind>(["missing_credentials", "credentials_refused", "invalid_api_key", "database_not_found"]);
/** The options that name a Tonbo API key or origin: each a string when given. */
const STRING_OPTIONS = ["apiKey", "apiOrigin", "authOrigin"] as const;

/**
 * A Tonbo API key or login the open cannot use, or a database the account does not have. `kind` names
 * what failed, `fix` what to do (the variable to set or the command to run), and `requestId` the
 * Tonbo API's id of the answer that failed, when one did. The action is always `fix`.
 *
 * - `missing_credentials`: no `apiKey`, no TONBO_API_KEY and no `tonbo login`.
 * - `credentials_refused`: the login file is a symlink, not a regular file, or open to group or others.
 * - `invalid_api_key`: the Tonbo API refused the key, the key cannot read the database (403), or the
 *   login expired or is not one this package reads.
 * - `database_not_found`: the account has no database of that name, or it is being deleted.
 */
export class TonboError extends UrsulaError {
	declare readonly kind: TonboKind;
	/** What to do, such as `Run: tonbo login`. The message ends with it. */
	readonly fix: string;
	/** The Tonbo API's request id of the answer that failed, for support. */
	readonly requestId: string | undefined;
	constructor(what: string, kind: TonboKind, fix: string, requestId?: string, cause?: unknown) {
		super(`${kind}: ${what}${requestId === undefined ? "" : ` (request ${requestId})`}. ${fix}`, kind, "fix", cause);
		this.name = "TonboError";
		this.fix = fix;
		this.requestId = requestId;
	}
}

/**
 * {@link resolveDatabase}'s options: a Tonbo API key, the Tonbo origins, and the root entry's `token`
 * and `signal`. A `token` serves the Streams requests instead of the key's, and the signal (30 s by
 * default) bounds the resolve.
 */
export interface TonboResolveOptions extends Pick<OpenOptions, "token" | "signal"> {
	/** The Tonbo API key. By default TONBO_API_KEY, else the key `tonbo login` saved. */
	readonly apiKey?: string | undefined;
	/** The Management API's origin. By default TONBO_API_ORIGIN, else `https://api.tonbo.dev`. */
	readonly apiOrigin?: string | undefined;
	/** The token endpoint's origin. By default TONBO_AUTH_ORIGIN, else `https://tonbo.dev`. */
	readonly authOrigin?: string | undefined;
}

/** {@link open}'s options: the root entry's, a Tonbo API key, and the Tonbo origins. */
export interface TonboOpenOptions extends OpenOptions, TonboResolveOptions {}

/** What {@link resolveDatabase} hands out: the stream to open, and the `token` option to open it with. */
export interface ResolvedDatabase {
	/** The database's stream URL. */
	readonly url: string;
	/**
	 * The `token` option of the open: the `token` the resolve was given, or `undefined` when the
	 * key's Streams tokens, which the extension holds and refreshes, or URSULA_VFS_TOKEN_FILE serve
	 * the Streams requests.
	 */
	readonly token: string | TokenProvider | undefined;
}

/** What a resolve failed with, as `tonbo_resolve` and `ursula_cancel` report it. */
interface Report extends Described {
	/** `<kind>: <what>[ (request <id>)][. <fix>]`. */
	readonly message: string;
	readonly fix: string | null;
	readonly request_id: string | null;
}

/** A resolve's answer: the stream URL, and whether the key's Streams tokens serve from now on. */
interface Answer {
	readonly url: string;
	readonly key_tokens: boolean;
}

/** `tonbo_resolve`'s answer. */
type Resolution = ({ done: true } & Answer) | { done: true; error: Report } | { done: false; request: number };

/** The resolves in flight on this thread, by their arguments: concurrent calls share one and its outcome. */
const inflight = new Map<string, Promise<Answer>>();
/** Errors of a resolve given up at its own caller's signal: a call that shared it resolves anew. */
const byAbort = new WeakSet<object>();

/**
 * Opens the Tonbo database `nameOrUrl` names, as the root entry's `open` does, and returns its
 * {@link UrsulaDatabase}. A name is looked up in the Management API with the account's API key. A
 * URL (`https://...`) skips the lookup and opens that stream.
 *
 * Streams requests carry tokens exchanged for the key, which the extension keeps fresh in the
 * background, unless `token` or URSULA_VFS_TOKEN_FILE gives one: then no key is needed for a URL.
 * The process holds one key for Streams tokens: once it held one, another key that would fetch
 * them fails `invalid_argument` while any database of the process may send requests, and for 60 s
 * after a resolve was handed its token. A database still being created is waited for. The signal
 * (30 s by default) bounds the whole open, the lookup included.
 *
 * Throws {@link TonboError} for a key or database to fix, {@link UrsulaUnavailableError} when the
 * signal fired while the Tonbo API did not answer (`unreachable`, `unavailable` or `rate_limited`,
 * action `retry`), {@link UrsulaSetupError} `invalid_argument` for an option it cannot use, an
 * answer of the Tonbo API it cannot read or a second key, `invalid_url` for a stream URL the open
 * cannot take or that would get the key's Streams tokens over plain http to another host, and the
 * root entry's errors from the open of the stream.
 */
export async function open(nameOrUrl: string | URL, options: TonboOpenOptions = {}): Promise<UrsulaDatabase> {
	const { apiKey, apiOrigin, authOrigin, ...rest } = options;
	const signal = rest.signal ?? AbortSignal.timeout(DEFAULT_OPEN_MS);
	const { url, token } = await resolveDatabase(nameOrUrl, { apiKey, apiOrigin, authOrigin, token: rest.token, signal });
	return openStream(url, { ...rest, token, signal });
}

/**
 * Resolves the Tonbo database `nameOrUrl` names into what opens it: its stream URL and the `token`
 * option, as {@link open} does before it opens the stream. Hand both to another opener, such as
 * the `/pi` entry's `openUrsulaPiStorage(url, { token })`. The `/tonbo/pi` entry's
 * `openUrsulaPiStorage` does both in one call.
 *
 * A name is looked up in the Management API with the account's API key, and a URL (`https://...`)
 * is taken as it is. Unless `token` or URSULA_VFS_TOKEN_FILE serves the Streams requests, the key
 * becomes the process's source of Streams tokens, which the extension exchanges and keeps fresh, and
 * the resolve returns once it holds the first one: a key the token endpoint refuses fails here, and
 * the `token` handed out is `undefined`, since the key's tokens serve the open. From then on they
 * serve every Streams request of the process: the extension cleared the token an earlier open set,
 * and this thread's token provider stops. A `token` is handed out as it was given, and then a URL
 * needs no key and the resolve sends nothing. Concurrent calls with the same arguments on a thread
 * share one resolve. The signal (30 s by default) bounds the resolve.
 *
 * Throws as {@link open} does before the open of the stream: {@link TonboError} for a key or
 * database to fix, {@link UrsulaUnavailableError} when the signal fired while the Tonbo API did not
 * answer, and {@link UrsulaSetupError} `invalid_argument` or `invalid_url`.
 */
export async function resolveDatabase(nameOrUrl: string | URL, options: TonboResolveOptions = {}): Promise<ResolvedDatabase> {
	const { apiKey, apiOrigin, authOrigin, token } = options;
	for (const name of STRING_OPTIONS) {
		const value: unknown = options[name];
		// Never the value itself: it may be a key.
		if (value !== undefined && typeof value !== "string") throw new UrsulaSetupError(`invalid_argument: ${name} must be a string, not ${value === null ? "null" : typeof value}`, "invalid_argument");
	}
	const until = options.signal ?? AbortSignal.timeout(DEFAULT_OPEN_MS);
	// `undefined` members are left out: the extension's defaults apply.
	const settings = JSON.stringify({ api_key: apiKey, api_origin: apiOrigin, auth_origin: authOrigin, own_token: token !== undefined, timeout_ms: EXTENSION_RESOLVE_MS });
	const { url, key_tokens } = await joined(String(nameOrUrl), settings, until);
	if (key_tokens) useKeyTokens();
	return { url, token };
}

/**
 * The resolve of `target` with `settings`: the one this thread has in flight for them, whose outcome
 * this call shares, or a new one. A call whose resolve was given up at another call's signal
 * resolves anew.
 */
async function joined(target: string, settings: string, until: AbortSignal): Promise<Answer> {
	const key = `${target}\n${settings}`;
	for (;;) {
		if (until.aborted) throw cancelled(target, until);
		const shared = inflight.get(key);
		if (shared !== undefined) {
			try {
				return await raced(shared, until);
			} catch (error) {
				if (until.aborted) throw cancelled(target, until);
				if (byAbort.has(error as object)) continue;
				throw error;
			}
		}
		const resolve = resolved(target, settings, until);
		inflight.set(key, resolve);
		try {
			return await resolve;
		} finally {
			if (inflight.get(key) === resolve) inflight.delete(key);
		}
	}
}

/**
 * `tonbo_resolve(target, settings, 0)` until it answers: the call returns at once, the pump wakes
 * this one at the resolve's `resolved` event, and a call with the same arguments then takes the
 * outcome, or names another resolve to wait for when the one it waited for was given up. `until`
 * firing cancels the resolve, and so does every other way out but its outcome, such as a pump that
 * failed for good: no resolve this call started outlives it.
 */
async function resolved(target: string, settings: string, until: AbortSignal): Promise<Answer> {
	/** The resolve this call waits for, which the call cancels unless it took the outcome. */
	let waiting: number | undefined;
	try {
		for (;;) {
			if (until.aborted) {
				const request = waiting;
				waiting = undefined;
				throw marked(request === undefined ? cancelled(target, until) : giveUp(target, request, until));
			}
			// Started before the call, so its events cursor precedes the request.
			const events = pump();
			const answer = call<Resolution>("SELECT tonbo_resolve(?, ?, 0)", target, settings);
			if (answer.done) {
				waiting = undefined;
				if ("error" in answer) throw errorOfReport(answer.error, undefined);
				return answer;
			}
			waiting = answer.request;
			try {
				await events.completed(answer.request, until);
			} catch {
				waiting = undefined;
				throw marked(giveUp(target, answer.request, until));
			}
		}
	} finally {
		if (waiting !== undefined) abandon(waiting);
	}
}

/** `error`, which a resolve met at its own caller's signal. */
function marked<E extends object>(error: E): E {
	byAbort.add(error);
	return error;
}

/** The package's error for what a resolve failed with: a {@link TonboError} for its kinds, the root entry's class otherwise. */
function errorOfReport(report: Report, cause: unknown): UrsulaError {
	const { kind, message, fix } = report;
	if (!TONBO_KINDS.has(kind)) return errorOf(report, message, cause);
	// The message is `<kind>: <what>[ (request <id>)][. <fix>]`, its fields given: `what` is what is left.
	const requestId = report.request_id ?? undefined;
	const request = requestId === undefined ? "" : ` (request ${requestId})`;
	const end = message.length - (fix === null ? 0 : `. ${fix}`.length) - request.length;
	return new TonboError(message.slice(`${kind}: `.length, end), kind as TonboKind, fix ?? "", requestId, cause);
}

/** The signal fired: cancels resolve `request`, and the error is what it came to, the last condition it met, or `cancelled`. */
function giveUp(target: string, request: number, signal: AbortSignal): UrsulaError {
	let ended: Report | null = null;
	try {
		ended = call<Report | null>("SELECT ursula_cancel(?)", request);
	} catch {
		// It ended and its outcome was taken meanwhile.
	}
	if (ended === null || ended.kind === "cancelled") return cancelled(target, signal);
	return errorOfReport(ended, signal.reason);
}

/** The call left its resolve `request` for another reason than the signal: it is cancelled, and what it came to is dropped. */
function abandon(request: number): void {
	try {
		call("SELECT ursula_cancel(?)", request);
	} catch {
		// It ended and its outcome was taken meanwhile.
	}
}

/** The resolve of `target` gave up at `signal` before it met any condition. */
function cancelled(target: string, signal: AbortSignal): UrsulaUnavailableError {
	return new UrsulaUnavailableError(`cancelled: the resolve of "${target}" gave up when its signal fired: ${reasonOf(signal.reason)}`, "cancelled", signal.reason);
}
