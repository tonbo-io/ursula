// `@tonbo/sqlite-ursula/tonbo` (design: Tonbo): open a Tonbo database by name with a Tonbo API
// key. It looks the name up in the Management API, then opens the database's stream with the root
// entry's `open`, whose token provider exchanges the key for short-lived Streams tokens.
// `resolveDatabase` is that first half on its own: the stream URL and the `token` to open it with,
// which `open` passes to the root entry's `open` and a caller of another opener, such as the `/pi`
// entry's `openUrsulaPiStorage`, passes to that.
//
// - The key: the `apiKey` option, else TONBO_API_KEY, else the file `tonbo login` writes (the
//   Tonbo CLI's credentials contract, version 1: `${XDG_CONFIG_HOME:-$HOME/.config}/tonbo/credentials.json`,
//   mode 0600, a personal key `tbo_...`). A symlink, a non-regular file or one with any group or
//   other permission bit is refused and never read.
// - The lookup: GET {api origin}/v1/databases/{name} with the key as the bearer token. A key that
//   must be exchanged first (401 `exchange_required`) is exchanged at the token endpoint with no
//   audience, and that token is kept until it expires. A Streams token never goes to the
//   Management API, and the key goes only to the two configured origins.
// - The Streams token: POST {auth origin}/api/iam/token with audience https://streams.tonbo.dev,
//   from the provider each open passes. The process holds one token, so later opens reuse it and
//   the next refresh runs the last open's provider (token.ts). It goes only to a stream URL that is
//   `https://`, or `http://` to a loopback host.
// - An explicit `token`, or URSULA_VFS_TOKEN_FILE, wins over the key for Streams requests.
// - Redirects are never followed: a 3xx is an answer the open cannot use.
//
// Nothing here logs, and no error message carries a key or a token.
import { constants } from "node:fs";
import { lstat, open as openFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { DEFAULT_OPEN_MS, reasonOf } from "./abort.ts";
import { actionOf, type TonboKind, type UnavailableKind, UrsulaError, UrsulaSetupError, UrsulaUnavailableError } from "./errors.ts";
import { type OpenOptions, open as openStream, type UrsulaDatabase } from "./index.ts";
import { MAX_TIMER_MS, type TokenProvider, USABLE } from "./token.ts";

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

/** The Management API, unless `apiOrigin` or TONBO_API_ORIGIN names another. */
const DEFAULT_API_ORIGIN = "https://api.tonbo.dev";
/** The token endpoint's origin, unless `authOrigin` or TONBO_AUTH_ORIGIN names another. */
const DEFAULT_AUTH_ORIGIN = "https://tonbo.dev";
/** The audience of a Streams token. */
const STREAMS_AUDIENCE = "https://streams.tonbo.dev";
/** A Management API token is exchanged again this long before it expires. */
const EXPIRY_MARGIN_MS = 30_000;
/** The first wait before looking a database up again (backoff, or a database still `creating`), doubled up to {@link MAX_RETRY_MS}. */
const FIRST_RETRY_MS = 250;
const MAX_RETRY_MS = 2_000;
/** An RFC 3339 date-time, as the credentials file's `expires_at` holds one. */
const DATE_TIME = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/i;
/** The fix for every problem of the login file. */
const LOGIN = "Run: tonbo login";
/** What the open calls the Management API and the token endpoint in messages, and the option and variable that name each origin. */
const MANAGEMENT = { what: "the Tonbo Management API", fix: "Check apiOrigin (or TONBO_API_ORIGIN)" } as const;
const TOKEN_ENDPOINT = { what: "the Tonbo token endpoint", fix: "Check authOrigin (or TONBO_AUTH_ORIGIN)" } as const;

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
		super(`${kind}: ${what}${requestId === undefined ? "" : ` (request ${requestId})`}. ${fix}`, kind, actionOf(kind), cause);
		this.name = "TonboError";
		this.fix = fix;
		this.requestId = requestId;
	}
}

/**
 * {@link resolveDatabase}'s options: a Tonbo API key, the Tonbo origins, and the root entry's `token`
 * and `signal`. A `token` serves the Streams requests instead of the key's, and the signal (30 s by
 * default) bounds the lookup.
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
	 * The `token` option of the open: the provider of Streams tokens for the key, the `token` the
	 * resolve was given, or `undefined` when URSULA_VFS_TOKEN_FILE serves the Streams requests.
	 */
	readonly token: string | TokenProvider | undefined;
}

/** A key and where it came from, which says how to fix it. */
interface ApiKey {
	readonly value: string;
	/** Where it came from, in words. */
	readonly from: string;
	readonly fix: string;
}

/** The Management API's database, the fields the open reads. */
interface Database {
	readonly url?: unknown;
	readonly state?: unknown;
}

/** A failure the open retries until its signal fires: the condition it reports then, and how long to wait first. */
class Transient extends Error {
	readonly condition: UnavailableKind;
	readonly waitMs: number | undefined;
	constructor(message: string, condition: UnavailableKind, waitMs?: number, cause?: unknown) {
		super(message, cause === undefined ? undefined : { cause });
		this.condition = condition;
		this.waitMs = waitMs;
	}
}

/**
 * Opens the Tonbo database `nameOrUrl` names, as the root entry's `open` does, and returns its
 * {@link UrsulaDatabase}. A name is looked up in the Management API with the account's API key. A
 * URL (`https://...`) skips the lookup and opens that stream.
 *
 * Streams requests carry tokens exchanged for the key, kept fresh in the background, unless
 * `token` or URSULA_VFS_TOKEN_FILE gives one: then no key is needed for a URL. A database still
 * being created is waited for. The signal (30 s by default) bounds the whole open, the lookup
 * included.
 *
 * Throws {@link TonboError} for a key or database to fix, {@link UrsulaUnavailableError} when the
 * signal fired while the Tonbo API did not answer (`unreachable`, `unavailable` or `rate_limited`,
 * action `retry`), {@link UrsulaSetupError} `invalid_argument` for an option it cannot use or an
 * answer of the Tonbo API it cannot read, `invalid_url` for a stream URL that would get the key's
 * Streams tokens over plain http to another host, and the root entry's errors from the open of the
 * stream.
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
 * the `/pi` entry's `openUrsulaPiStorage(url, { token })`, whose open then keeps the Streams tokens
 * fresh with no refresher of the caller's own.
 *
 * A name is looked up in the Management API with the account's API key, and a URL (`https://...`)
 * is taken as it is. The token is a provider of Streams tokens exchanged for the key, which the open
 * calls: the resolve exchanges nothing for the stream. A `token`, or URSULA_VFS_TOKEN_FILE, serves
 * the Streams requests instead, and then a URL needs no key. The signal (30 s by default) bounds the
 * lookup.
 *
 * Throws as {@link open} does before the open of the stream: {@link TonboError} for a key or
 * database to fix, {@link UrsulaUnavailableError} when the signal fired while the Tonbo API did not
 * answer, and {@link UrsulaSetupError} `invalid_argument` or `invalid_url`. A key the token endpoint
 * refuses a Streams token fails the open that calls the provider, with `invalid_api_key`.
 */
export async function resolveDatabase(nameOrUrl: string | URL, options: TonboResolveOptions = {}): Promise<ResolvedDatabase> {
	const { apiKey, apiOrigin, authOrigin, token: given } = options;
	const until = options.signal ?? AbortSignal.timeout(DEFAULT_OPEN_MS);
	const name = typeof nameOrUrl === "string" && !/^[a-z][a-z0-9+.-]*:\/\//i.test(nameOrUrl) ? nameOrUrl : undefined;
	if (name !== undefined && name.length === 0) throw new UrsulaSetupError("invalid_name: the database name is empty", "invalid_name");
	const api = originOf(apiOrigin, "apiOrigin", "TONBO_API_ORIGIN", DEFAULT_API_ORIGIN);
	const auth = originOf(authOrigin, "authOrigin", "TONBO_AUTH_ORIGIN", DEFAULT_AUTH_ORIGIN);
	// An explicit token, or the file the extension reads, serves the Streams requests.
	const streamsToken = given !== undefined || nonEmpty(process.env.URSULA_VFS_TOKEN_FILE) !== undefined;
	const key = name !== undefined || !streamsToken ? await apiKeyOf(apiKey) : undefined;
	const url = name !== undefined && key !== undefined ? await lookUp(name, key, api, auth, until) : String(nameOrUrl);
	if (!streamsToken && key !== undefined) refuseExposed(url, name);
	const token = given ?? (streamsToken || key === undefined ? undefined : streamsProvider(key, auth));
	return { url, token };
}

const nonEmpty = (value: string | undefined): string | undefined => (value === undefined || value.trim() === "" ? undefined : value.trim());

/** Whether requests to `url` may carry the key, or a token exchanged for it: `https://`, or `http://` to a loopback host. */
function confidential(url: URL): boolean {
	const loopback = url.hostname === "localhost" || url.hostname === "[::1]" || /^127\.\d+\.\d+\.\d+$/.test(url.hostname);
	return url.protocol === "https:" || (url.protocol === "http:" && loopback);
}

/** Refuses to send Streams tokens for the key to the stream at `url` (of database `name`) over plain http to another host. */
function refuseExposed(url: string, name: string | undefined): void {
	let parsed: URL;
	try {
		parsed = new URL(url);
	} catch {
		// The root entry's open fails it `invalid_url`, before any request.
		return;
	}
	if (confidential(parsed)) return;
	const of = name === undefined ? "" : ` of database "${name}"`;
	throw new UrsulaSetupError(
		`invalid_url: the stream URL ${url}${of} gets no Streams token for the API key, since it is not https:// (http:// only to a loopback host). Open an https:// URL, or pass token`,
		"invalid_url",
	);
}

/** `path` as one shell word: as it is when it needs no quoting, single-quoted otherwise. */
const shellWord = (path: string): string => (/^[\w@%+=:,./-]+$/.test(path) ? path : `'${path.replaceAll("'", "'\\''")}'`);

/** The origin `option`, else the variable `env`, else `fallback`: `https://`, or `http://` to a loopback host only, since requests carry the key. */
function originOf(option: string | undefined, name: string, env: string, fallback: string): string {
	const raw = option ?? nonEmpty(process.env[env]) ?? fallback;
	const wrong = (why: string): UrsulaSetupError =>
		new UrsulaSetupError(`invalid_argument: ${name} (or ${env}) must be an origin such as ${fallback}, not ${String(raw)}: ${why}`, "invalid_argument");
	if (typeof raw !== "string") throw wrong("not a string");
	let url: URL;
	try {
		url = new URL(raw);
	} catch (error) {
		throw wrong(reasonOf(error));
	}
	if (url.pathname !== "/" || url.search !== "" || url.hash !== "" || url.username !== "" || url.password !== "") throw wrong("an origin has no path, query or credentials");
	if (!confidential(url)) throw wrong("use https://, since requests carry the API key (http:// only to a loopback host)");
	return url.origin;
}

/** The key: the option, else TONBO_API_KEY, else the login file. */
async function apiKeyOf(option: string | undefined): Promise<ApiKey> {
	if (option !== undefined) {
		if (typeof option !== "string" || !USABLE.test(option)) {
			throw new UrsulaSetupError("invalid_argument: apiKey must be a non-empty string of printable ASCII without spaces", "invalid_argument");
		}
		return { value: option, from: "the apiKey option", fix: "Pass a valid apiKey" };
	}
	const env = nonEmpty(process.env.TONBO_API_KEY);
	const envFix = "Set TONBO_API_KEY to a valid key, or unset it and run: tonbo login";
	if (env !== undefined) {
		if (!USABLE.test(env)) throw new TonboError("TONBO_API_KEY holds no usable key", "invalid_api_key", envFix);
		return { value: env, from: "TONBO_API_KEY", fix: envFix };
	}
	const login = await loginKey();
	if (login !== undefined) return login;
	throw new TonboError("no Tonbo API key", "missing_credentials", "Set TONBO_API_KEY, or run: tonbo login");
}

/** Where `tonbo login` saves the login. */
function loginPath(): string {
	const base = nonEmpty(process.env.XDG_CONFIG_HOME) ?? join(nonEmpty(process.env.HOME) ?? homedir(), ".config");
	return join(base, "tonbo", "credentials.json");
}

/** The key `tonbo login` saved, `undefined` when there is no login file. */
async function loginKey(): Promise<ApiKey | undefined> {
	const path = loginPath();
	const posix = process.platform !== "win32";
	const refused = (why: string, fix: string): TonboError => new TonboError(`the tonbo login at ${path} ${why}, so it is not read`, "credentials_refused", fix);
	const notRegular = (): TonboError => refused("is not a regular file", `Remove it, then ${LOGIN.toLowerCase()}`);
	const symlink = (): TonboError => refused("is a symlink", `Replace it with the file itself, or remove it, then ${LOGIN.toLowerCase()}`);
	const unreadable = (error: unknown): UrsulaSetupError =>
		new UrsulaSetupError(`local_setup: cannot read the tonbo login at ${path}: ${reasonOf(error)}. Run: chmod 600 ${shellWord(path)}, or ${LOGIN.toLowerCase()}`, "local_setup", error);
	try {
		const entry = await lstat(path);
		if (entry.isSymbolicLink()) throw symlink();
		if (!entry.isFile()) throw notRegular();
	} catch (error) {
		if (error instanceof TonboError) throw error;
		const code = (error as { code?: unknown }).code;
		if (code === "ENOENT" || code === "ENOTDIR") return undefined;
		throw unreadable(error);
	}
	try {
		// O_NOFOLLOW: a symlink put in place since the check is refused, not followed.
		const file = await openFile(path, constants.O_RDONLY | (posix ? constants.O_NOFOLLOW | constants.O_NONBLOCK : 0));
		try {
			const stat = await file.stat();
			if (!stat.isFile()) throw notRegular();
			if (posix && (stat.mode & 0o077) !== 0) {
				throw refused(`has group or other permissions (mode ${(stat.mode & 0o777).toString(8)})`, `Run: chmod 600 ${shellWord(path)}`);
			}
			return keyOfLogin(path, await file.readFile("utf8"));
		} finally {
			await file.close();
		}
	} catch (error) {
		if (error instanceof TonboError) throw error;
		if ((error as { code?: unknown }).code === "ELOOP") throw symlink();
		throw unreadable(error);
	}
}

/** The key of the login file at `path`, whose content is `text`: unusable or expired logins fail `invalid_api_key`. */
function keyOfLogin(path: string, text: string): ApiKey {
	const login = usableLogin(text);
	if (login === undefined) throw new TonboError(`the tonbo login at ${path} is not one this package reads`, "invalid_api_key", LOGIN);
	if (login.expiresAt !== null && Date.parse(login.expiresAt) <= Date.now()) {
		throw new TonboError(`the tonbo login at ${path} expired at ${login.expiresAt}`, "invalid_api_key", LOGIN);
	}
	return { value: login.key, from: `the tonbo login at ${path}`, fix: LOGIN };
}

/** The login in `text` when it is version 1 and matches its schema: `undefined` otherwise. */
function usableLogin(text: string): { key: string; expiresAt: string | null } | undefined {
	let parsed: unknown;
	try {
		parsed = JSON.parse(text);
	} catch {
		return undefined;
	}
	if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) return undefined;
	const login = parsed as Record<string, unknown>;
	const id = (value: unknown): boolean => typeof value === "string" && value.length > 0;
	const { version, account_id, key_id, key, expires_at } = login;
	if (version !== 1 || !id(account_id) || !id(key_id)) return undefined;
	if (typeof key !== "string" || !key.startsWith("tbo_") || !USABLE.test(key)) return undefined;
	if (!("expires_at" in login)) return undefined;
	if (expires_at !== null && (typeof expires_at !== "string" || !DATE_TIME.test(expires_at) || Number.isNaN(Date.parse(expires_at)))) return undefined;
	return { key, expiresAt: expires_at };
}

/** An answer's problem details (`application/problem+json`), as far as the open reads them. */
interface Problem {
	readonly code: string | undefined;
	readonly title: string | undefined;
	readonly detail: string | undefined;
	readonly requestId: string | undefined;
}

async function problemOf(response: Response): Promise<Problem> {
	let body: Record<string, unknown> = {};
	try {
		const parsed: unknown = await response.json();
		if (parsed !== null && typeof parsed === "object") body = parsed as Record<string, unknown>;
	} catch {
		// Not JSON: the status says it all.
	}
	const text = (value: unknown): string | undefined => (typeof value === "string" && value.length > 0 ? value : undefined);
	return { code: text(body.code), title: text(body.title), detail: text(body.detail), requestId: text(body.request_id) ?? requestIdOf(response) };
}

/** The answer's `X-Request-Id`. */
const requestIdOf = (response: Response): string | undefined => response.headers.get("x-request-id") || undefined;

/**
 * An answer the open cannot use, from `api`: a status the contract does not have, a redirect (never
 * followed), or a body without what it must hold. The fix names the option and variable of `api`'s origin.
 */
function unexpected(api: { readonly what: string; readonly fix: string }, answer: string, problem: Problem | undefined, requestId: string | undefined): UrsulaSetupError {
	const detail = problem?.detail === undefined ? "" : `: ${problem.detail}`;
	const request = requestId === undefined ? "" : ` (request ${requestId})`;
	return new UrsulaSetupError(`invalid_argument: ${api.what} answered ${answer}${detail}${request}. ${api.fix}`, "invalid_argument");
}

/** The words for an unexpected `status`. */
const answered = (status: number): string => (status >= 300 && status < 400 ? `${status} (a redirect, never followed)` : String(status));

/** `Retry-After`, in ms: delta-seconds or an HTTP date. */
function retryAfterMs(response: Response): number | undefined {
	const raw = response.headers.get("retry-after");
	if (raw === null) return undefined;
	const seconds = Number(raw);
	if (raw.trim() !== "" && Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;
	const at = Date.parse(raw);
	return Number.isNaN(at) ? undefined : Math.max(0, at - Date.now());
}

/** The answer to a request of `what` that the open retries: 429, 408 and 5xx. Others return `undefined`. */
function transientOf(response: Response, what: string): Transient | undefined {
	if (response.status !== 429 && response.status !== 408 && response.status < 500) return undefined;
	response.body?.cancel().catch(() => {});
	if (response.status === 429) return new Transient(`${what} answered 429`, "rate_limited", retryAfterMs(response));
	return new Transient(`${what} answered ${response.status}`, "unavailable");
}

/** The key was refused (401), or (403) cannot do what was asked. */
function keyRefused(key: ApiKey, by: string, problem: Problem): TonboError {
	return new TonboError(`${by} refused the API key from ${key.from}${problem.code === undefined ? "" : ` (${problem.code})`}`, "invalid_api_key", key.fix, problem.requestId);
}

/** A token the token endpoint handed out, and when it expires (epoch ms). */
interface Exchanged {
	readonly token: string;
	readonly expiresAt: number;
}

/** Exchanges `key` at the token endpoint of `auth`: for the Management API with no audience, or for `audience`. */
async function exchange(key: ApiKey, auth: string, audience: string | undefined, signal: AbortSignal): Promise<Exchanged> {
	const what = TOKEN_ENDPOINT.what;
	let response: Response;
	try {
		response = await fetch(`${auth}/api/iam/token`, {
			method: "POST",
			headers: { authorization: `Bearer ${key.value}`, "content-type": "application/json", accept: "application/json" },
			body: JSON.stringify(audience === undefined ? {} : { audience }),
			redirect: "manual",
			signal,
		});
	} catch (error) {
		if (signal.aborted) throw error;
		throw new Transient(`${what} did not answer: ${reasonOf(error)}`, "unreachable", undefined, error);
	}
	if (response.status === 401 || response.status === 403) throw keyRefused(key, what, await problemOf(response));
	const transient = transientOf(response, what);
	if (transient !== undefined) throw transient;
	if (!response.ok) {
		const problem = await problemOf(response);
		throw unexpected(TOKEN_ENDPOINT, answered(response.status), problem, problem.requestId);
	}
	let parsed: unknown;
	try {
		parsed = await response.json();
	} catch {
		// Below.
	}
	const body = parsed as { access_token?: unknown; expires_in?: unknown } | null | undefined;
	const token = body?.access_token;
	const lifetime = body?.expires_in;
	if (typeof token !== "string" || !USABLE.test(token) || typeof lifetime !== "number" || !(lifetime > 0)) {
		throw new Transient(`${what} answered 200 without a token`, "unavailable");
	}
	return { token, expiresAt: Date.now() + lifetime * 1000 };
}

/**
 * The Management API tokens of keys that must be exchanged first, by auth origin and key: the
 * presence of an entry says the key needs the exchange, its value the token held, if any.
 */
const managementTokens = new Map<string, Exchanged | undefined>();

/** The bearer token of a Management API request with `key`: the key itself, or the token exchanged for it (anew with `fresh`). */
async function managementBearer(key: ApiKey, auth: string, signal: AbortSignal, fresh: boolean): Promise<string> {
	const id = `${auth}\n${key.value}`;
	if (!fresh && !managementTokens.has(id)) return key.value;
	const held = managementTokens.get(id);
	if (!fresh && held !== undefined && held.expiresAt - EXPIRY_MARGIN_MS > Date.now()) return held.token;
	managementTokens.set(id, undefined);
	const got = await exchange(key, auth, undefined, signal);
	managementTokens.set(id, got);
	return got.token;
}

/** One lookup of `name`: the database and the answer's request id, or a {@link TonboError}, or a {@link Transient}. */
async function lookUpOnce(name: string, key: ApiKey, api: string, auth: string, signal: AbortSignal): Promise<{ database: Database; requestId: string | undefined }> {
	const what = MANAGEMENT.what;
	const get = async (bearer: string): Promise<Response> => {
		try {
			return await fetch(`${api}/v1/databases/${encodeURIComponent(name)}`, {
				headers: { authorization: `Bearer ${bearer}`, accept: "application/json" },
				redirect: "manual",
				signal,
			});
		} catch (error) {
			if (signal.aborted) throw error;
			throw new Transient(`${what} did not answer: ${reasonOf(error)}`, "unreachable", undefined, error);
		}
	};
	let bearer = await managementBearer(key, auth, signal, false);
	let response = await get(bearer);
	if (response.status === 401) {
		const problem = await problemOf(response);
		// A key that must be exchanged, or an exchanged token that expired early: exchange, and retry once.
		if (problem.code !== "exchange_required" && bearer === key.value) throw keyRefused(key, what, problem);
		bearer = await managementBearer(key, auth, signal, true);
		response = await get(bearer);
	}
	if (response.ok) {
		let database: unknown;
		try {
			database = ((await response.json()) as { data?: unknown } | null)?.data;
		} catch {
			// Below.
		}
		if (database === null || typeof database !== "object" || typeof (database as Database).state !== "string") {
			throw new Transient(`${what} answered ${response.status} without a database`, "unavailable");
		}
		return { database: database as Database, requestId: requestIdOf(response) };
	}
	const transient = transientOf(response, what);
	if (transient !== undefined) throw transient;
	const problem = await problemOf(response);
	switch (response.status) {
		case 401:
			throw keyRefused(key, what, problem);
		case 403:
			throw new TonboError(`this key, from ${key.from}, can't read database "${name}"`, "invalid_api_key", `Use an API key that can read database "${name}"`, problem.requestId);
		case 404:
			throw notFound(name, problem.requestId);
		default:
			throw unexpected(MANAGEMENT, `${answered(response.status)} for database "${name}"`, problem, problem.requestId);
	}
}

const notFound = (name: string, requestId: string | undefined): TonboError => new TonboError(`database "${name}" not found`, "database_not_found", `Run: tonbo database create ${name}`, requestId);

/** The stream URL of database `name`: looked up until it is active, retrying failures that pass, until `until` fires. */
async function lookUp(name: string, key: ApiKey, api: string, auth: string, until: AbortSignal): Promise<string> {
	let last: Transient | undefined;
	for (let attempt = 0; ; attempt += 1) {
		const backoff = Math.min(MAX_RETRY_MS, FIRST_RETRY_MS * 2 ** Math.min(attempt, 8));
		let wait: number;
		try {
			const { database, requestId } = await lookUpOnce(name, key, api, auth, until);
			if (database.state === "active") {
				if (typeof database.url === "string") return database.url;
				throw unexpected(MANAGEMENT, `database "${name}" active without a url`, undefined, requestId);
			}
			if (database.state === "deleting" || database.state === "deleted") throw notFound(name, requestId);
			// `creating`, or a state this package does not know yet: look again.
			last = new Transient(`database "${name}" is ${String(database.state)}`, "unavailable");
			wait = backoff;
		} catch (error) {
			if (error instanceof UrsulaError) throw error;
			if (until.aborted) throw gaveUp(name, last, until);
			if (!(error instanceof Transient)) throw error;
			last = error;
			// Never sooner than the backoff, so `Retry-After: 0` or a past date does not spin, and never past a timer's range.
			wait = Math.min(MAX_TIMER_MS, Math.max(error.waitMs ?? 0, backoff));
		}
		try {
			await delay(wait, undefined, { signal: until });
		} catch {
			throw gaveUp(name, last, until);
		}
	}
}

/** The open's signal fired during the lookup: the last condition it met, or `cancelled`. */
function gaveUp(name: string, last: Transient | undefined, until: AbortSignal): UrsulaUnavailableError {
	const kind = last?.condition ?? "cancelled";
	const why = last === undefined ? reasonOf(until.reason) : last.message;
	return new UrsulaUnavailableError(`${kind}: the lookup of database "${name}" gave up when its signal fired: ${why}`, kind, last ?? until.reason);
}

/**
 * The provider of Streams tokens for `key`, whose errors name the source this open took the key
 * from. A key the token endpoint refuses fails for good: the open rejects with `invalid_api_key`.
 */
function streamsProvider(key: ApiKey, auth: string): TokenProvider {
	return async ({ signal }) => {
		const got = await exchange(key, auth, STREAMS_AUDIENCE, signal);
		return { token: got.token, expiresAt: got.expiresAt };
	};
}
