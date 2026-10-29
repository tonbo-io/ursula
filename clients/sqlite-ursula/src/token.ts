// The bearer token (design: API, Tokens): `open()`'s `token` is a string or a function that hands
// out a token and when it expires. Both kinds are applied with `ursula_set_token`, so the token stays
// one per process and the last open that names one wins.
//
// The package calls a provider before an open's first request when it holds no usable token, again
// ahead of each token's expiry, and again at once when a request was refused with 401: the
// replication condition `unauthorized` of a bound database, or an open that failed `unauthorized`
// while it was binding (the extension reports conditions of bound databases only). Which function
// minted the token held does not matter, since the process carries one token: an open with another
// function only names the function the next call runs. An open with a string stops the provider:
// the waiting opens go on under the string. Each call is handed the token it last handed out as
// `previous` (the refused one after a 401), and an answer equal to it counts as a failed call: the
// extension takes an equal token as no change, so a refused request would never go out again. A
// retry is due by the expiry of `previous` at the latest, when a provider's cache turns over.
//
// A Tonbo API key's tokens, which the extension exchanges and refreshes itself (the `/tonbo` entry),
// take over the same way: the extension clears the token set, and `useKeyTokens` stops the provider
// and withdraws a token it set since.
//
// One source per JavaScript thread, since a function lives on the thread that made it. Its timers
// are unref'd, so they never keep the process alive, except while an `open()` waits for a token,
// which the caller awaits. It stops once no binding the thread opened is bound, whatever token its
// open named.
import { raced, reasonOf } from "./abort.ts";
import { call } from "./control.ts";
import { UrsulaError, UrsulaSetupError, UrsulaUnavailableError } from "./errors.ts";
import { type PumpEvent, pump, watch } from "./pump.ts";

/** What a {@link TokenProvider} hands out: a bearer token and when it expires. */
export interface BearerToken {
	/** The token, printable ASCII without spaces. Never logged. */
	readonly token: string;
	/** When the token expires: a `Date`, or milliseconds (not seconds) since the epoch. */
	readonly expiresAt: Date | number;
}

/**
 * Hands out a fresh bearer token: `open()`'s `token` option. `signal` fires when the package no
 * longer waits for the answer: after 30 s, or once an open with a string token stopped the provider
 * or no database uses it.
 *
 * `previous` is the token the provider last handed out, exactly as it handed it out, when the
 * package will not take it again: the current one at a scheduled refresh, or the one the server
 * refused after a 401. It is `undefined` before the first call and after the provider stopped, but
 * a token the server refused stays `previous` until a string token replaces it. When `previous` is
 * set, hand out a different token, and refill a cache that holds it: the package compares tokens
 * without surrounding whitespace, and an equal one counts as a failed call, retried with backoff
 * and by the expiry of `previous` at the latest. A token that never changes belongs in a string
 * `token` or in `URSULA_VFS_TOKEN_FILE`.
 *
 * To fail for good, throw an error whose `action` is `"fix"`, an {@link UrsulaError} such as
 * `new UrsulaUnavailableError("unauthorized: the key was revoked", "unauthorized")` or any other
 * object with that property: the package stops calling the provider until the next `open()` with a
 * provider. It retries every other error with backoff.
 */
export type TokenProvider = (options: { readonly signal: AbortSignal; readonly previous?: string | undefined }) => Promise<BearerToken>;

/** A token as the package holds it: when it expires and when to ask for the next one, in epoch ms. */
interface Fresh {
	/** The token as requests carry it: what the provider handed out, without surrounding whitespace. */
	readonly token: string;
	/** What the provider handed out, which a later call gets back as `previous`. */
	readonly handed: string;
	readonly expiresAt: number;
	readonly refreshAt: number;
}

/** How long one call of the provider may take before it counts as failed. */
const PROVIDER_MS = 30_000;
/** The first retry after a failed call, doubled after each failure in a row up to {@link MAX_RETRY_MS}, then jittered down to half. */
const FIRST_RETRY_MS = 1_000;
const MAX_RETRY_MS = 30_000;
/** The shortest token lifetime the package takes, and the earliest refresh after a token arrived. */
const MIN_REFRESH_MS = 1_000;
/** The longest delay a timer takes. A refresh further out runs then. */
const MAX_TIMER_MS = 2_147_483_647;
/** The earliest `expiresAt` read as milliseconds since the epoch (2001-09-09): below it, it is seconds. */
const MIN_EPOCH_MS = 1e12;
/** A token as `Authorization: Bearer` carries it (clients/sqlite-vfs/src/auth.rs `usable`). */
const USABLE = /^[\x21-\x7e]+$/;

/** A refresh is due after 70 to 80% of the token's lifetime, at least {@link MIN_REFRESH_MS}: never after it expired, since a shorter lifetime is refused. */
const refreshAfter = (lifetime: number): number => Math.max(MIN_REFRESH_MS, lifetime * (0.7 + 0.1 * Math.random()));
const retryAfter = (failures: number): number => Math.min(MAX_RETRY_MS, FIRST_RETRY_MS * 2 ** Math.min(failures - 1, 16)) * (0.5 + 0.5 * Math.random());

/** A failure the package does not retry: an error of the provider whose `action` is `fix`, an UrsulaError or not, or an answer it cannot use. */
const permanent = (error: unknown): boolean => typeof error === "object" && error !== null && (error as { action?: unknown }).action === "fix";

/** What the provider returned, checked: an answer the package cannot use fails for good, a token that expired or expires within {@link MIN_REFRESH_MS} is retried. */
function checked(got: unknown, now: number): Fresh {
	const answer = got as { token?: unknown; expiresAt?: unknown } | null | undefined;
	const handed = typeof answer?.token === "string" ? answer.token : "";
	const token = handed.trim();
	if (!USABLE.test(token)) {
		throw new UrsulaSetupError("invalid_argument: the token provider returned no usable token: a non-empty string of printable ASCII without spaces", "invalid_argument");
	}
	const raw = answer?.expiresAt;
	const expiresAt = raw instanceof Date ? raw.getTime() : raw;
	if (typeof expiresAt !== "number" || !Number.isFinite(expiresAt)) {
		throw new UrsulaSetupError("invalid_argument: the token provider returned no expiresAt: a Date, or milliseconds since the epoch", "invalid_argument");
	}
	if (expiresAt < MIN_EPOCH_MS) {
		throw new UrsulaSetupError(
			`invalid_argument: the token provider returned expiresAt ${expiresAt}, which reads as seconds rather than milliseconds since the epoch: pass milliseconds (a JWT's exp * 1000) or a Date`,
			"invalid_argument",
		);
	}
	const lifetime = expiresAt - now;
	if (lifetime < MIN_REFRESH_MS) throw new Error(`the token provider returned a token that expires in ${lifetime} ms, under the ${MIN_REFRESH_MS} ms the package needs to refresh it in time`);
	return { token, handed, expiresAt, refreshAt: now + refreshAfter(lifetime) };
}

/** Requests carry `token` from now on. */
const apply = (token: string): void => {
	call("SELECT ursula_set_token('', ?)", token);
};

/** What an open waiting for a token learns: the token (none when a string token replaced the provider), or a failure for good. */
type Outcome = { token: string | undefined } | { error: unknown };

/** This thread's provider and its schedule. At most one call of it runs at a time: concurrent refreshes share it. */
class Source {
	/** The function the next call runs: the provider of the thread's last open that named one, until an open with a string or idleness stopped it. */
	#provider: TokenProvider | undefined;
	/** The call in flight: aborting it discards its answer. */
	#attempt: AbortController | undefined;
	#current: Fresh | undefined;
	/**
	 * The token the provider last handed out, also once a 401 dropped it, which its next call gets as
	 * `previous`. A stop forgets it, unless the server refused it and no string or key replaced it.
	 */
	#held: Fresh | undefined;
	/**
	 * The token a condition `unauthorized` of `paths` dropped. The event may predate the token, since
	 * the extension clears a condition only once the blocked request went out again under a new one:
	 * once none of `paths` is blocked any more, the provider's answer of that token is taken.
	 */
	#dropped: { readonly token: string; readonly paths: readonly string[] } | undefined;
	#timer: NodeJS.Timeout | undefined;
	/** Failed calls in a row. */
	#failures = 0;
	/** The last call's failure, one for good included, until a call succeeds. */
	#lastFailure: { readonly error: unknown } | undefined;
	/** It failed for good: no call until the next open. */
	#failed = false;
	/** The opens waiting for a token. */
	readonly #waiters = new Set<(outcome: Outcome) => void>();

	get provider(): TokenProvider | undefined {
		return this.#provider;
	}

	/** Whether an open waits for it. */
	get waited(): boolean {
		return this.#waiters.size > 0;
	}

	/** The last call's failure, until a call succeeds. */
	get lastFailure(): { readonly error: unknown } | undefined {
		return this.#lastFailure;
	}

	/** The next call runs `provider`. The token held and the call in flight serve on: the process carries one token, whichever function minted it. */
	use(provider: TokenProvider): void {
		this.#provider = provider;
	}

	/**
	 * The token for an open of `stream`, applied. A token held serves at once while it is valid, so a
	 * connection on a bound binding needs no network, and a refresh that is due runs in the
	 * background. An expired one serves only while the provider fails, since otherwise its next token
	 * is on the way. Else the open waits for the provider: when that fails for good it rejects with
	 * the provider's error if that is an {@link UrsulaError}, else with an
	 * {@link UrsulaUnavailableError} `unauthorized`, action `fix`, whose cause it is, and with an
	 * {@link UrsulaUnavailableError} `unavailable` when `signal` fires first.
	 */
	token(stream: string, signal: AbortSignal): Promise<string | undefined> {
		const failing = this.#failed || this.#failures > 0;
		// A new open asks a provider that failed for good again.
		this.#failed = false;
		const current = this.#current;
		if (current !== undefined && (failing || current.expiresAt > Date.now())) {
			apply(current.token);
			if (this.#attempt === undefined && this.#timer === undefined) this.#call();
			return Promise.resolve(current.token);
		}
		return new Promise((resolve, reject) => {
			const done = (): void => {
				this.#waiters.delete(waiter);
				signal.removeEventListener("abort", abort);
				if (this.#waiters.size === 0) this.#timer?.unref();
			};
			const waiter = (outcome: Outcome): void => {
				done();
				if ("token" in outcome) resolve(outcome.token);
				else reject(failedForGood(stream, outcome.error));
			};
			const abort = (): void => {
				done();
				reject(this.#gaveUp(stream, signal));
			};
			if (signal.aborted) {
				reject(this.#gaveUp(stream, signal));
				return;
			}
			this.#waiters.add(waiter);
			signal.addEventListener("abort", abort, { once: true });
			// A retry already scheduled runs on time: the open keeps the process alive meanwhile.
			if (this.#attempt !== undefined) return;
			if (this.#backingOff) this.#timer?.ref();
			else this.#call();
		});
	}

	/** A bind under `token` was refused with 401: the token is dropped unless a newer one is held, and a call starts unless one is in flight or a retry is scheduled. */
	refused(token: string): void {
		if (this.#provider === undefined || this.#failed) return;
		const current = this.#current;
		if (current !== undefined && current.token !== token) return;
		if (this.#dropped?.token === token) this.#dropped = undefined;
		this.#current = undefined;
		this.#ask();
	}

	/** The replication of `paths` blocked `unauthorized`: the token held is dropped, and a call starts unless one is in flight or a retry is scheduled. */
	blocked(paths: readonly string[]): void {
		if (this.#provider === undefined || this.#failed) return;
		const current = this.#current;
		if (current !== undefined) this.#dropped = { token: current.token, paths };
		else if (this.#dropped !== undefined) this.#dropped = { token: this.#dropped.token, paths: [...new Set([...this.#dropped.paths, ...paths])] };
		this.#current = undefined;
		this.#ask();
	}

	/**
	 * No provider from now on: the call in flight is abandoned, and the opens waiting go on with the
	 * process's token, which `why` set. `replaced`: a string or a key's tokens replaced the provider's.
	 * Otherwise a token the server refused is still the process's, so the next call gets it as `previous`.
	 */
	stop(why: string, replaced: boolean): void {
		this.#attempt?.abort(new UrsulaUnavailableError(`cancelled: the token provider stopped: ${why}`, "cancelled"));
		this.#attempt = undefined;
		clearTimeout(this.#timer);
		this.#timer = undefined;
		this.#provider = undefined;
		if (replaced || this.#current !== undefined) this.#held = undefined;
		this.#current = undefined;
		this.#dropped = undefined;
		this.#failures = 0;
		this.#lastFailure = undefined;
		this.#failed = false;
		for (const waiter of [...this.#waiters]) waiter({ token: undefined });
	}

	/** A retry after a failed call is scheduled. */
	get #backingOff(): boolean {
		return this.#failures > 0 && this.#timer !== undefined;
	}

	/** A call at once, unless one is in flight or a retry is scheduled. */
	#ask(): void {
		if (this.#attempt === undefined && !this.#backingOff) this.#call();
	}

	/**
	 * Whether the provider's answer `token`, equal to `previous`, is taken: only when a condition that
	 * may predate it dropped it, and none of the paths that reported the condition is blocked any more.
	 */
	#restored(token: string): boolean {
		const dropped = this.#dropped;
		return dropped !== undefined && dropped.token === token && cleared(dropped.paths);
	}

	#gaveUp(stream: string, signal: AbortSignal): UrsulaUnavailableError {
		const last = this.#failures > 0 ? this.#lastFailure : undefined;
		const why = last !== undefined ? `its last call failed: ${reasonOf(last.error)}` : `it did not answer: ${reasonOf(signal.reason)}`;
		return new UrsulaUnavailableError(`unavailable: the token provider handed out no token (${why}): the open of ${stream} gave up when its signal fired`, "unavailable", last !== undefined ? last.error : signal.reason);
	}

	#call(): void {
		const provider = this.#provider;
		if (provider === undefined) return;
		clearTimeout(this.#timer);
		this.#timer = undefined;
		const attempt = new AbortController();
		this.#attempt = attempt;
		const signal = AbortSignal.any([attempt.signal, AbortSignal.timeout(PROVIDER_MS)]);
		const previous = this.#held;
		// A retry is due by the expiry of `previous` at the latest, when a cache that hands it back turns over.
		const by = previous !== undefined && previous.expiresAt > Date.now() ? previous.expiresAt : undefined;
		raced(
			// A call abandoned before it started never reaches the provider.
			Promise.resolve().then(() => {
				signal.throwIfAborted();
				return provider({ signal, previous: previous?.handed });
			}),
			signal,
		)
			.then((got) => {
				const fresh = checked(got, Date.now());
				// The extension takes an equal token as no change, so it would not send a refused request again.
				if (fresh.token === previous?.token && !(this.#attempt === attempt && this.#restored(fresh.token))) {
					throw new Error("the token provider returned the token it was handed as previous, which the package holds already or the server refused");
				}
				if (this.#attempt === attempt) apply(fresh.token);
				return fresh;
			})
			.then(
				(fresh) => {
					if (this.#attempt === attempt) this.#succeeded(fresh);
				},
				(error: unknown) => {
					if (this.#attempt === attempt) this.#failedWith(error, by);
				},
			);
	}

	#succeeded(fresh: Fresh): void {
		this.#attempt = undefined;
		this.#current = fresh;
		this.#held = fresh;
		this.#dropped = undefined;
		this.#failures = 0;
		this.#lastFailure = undefined;
		for (const waiter of [...this.#waiters]) waiter({ token: fresh.token });
		this.#schedule(fresh.refreshAt - Date.now());
	}

	/** A failed call: retried with backoff, and by `by` at the latest, unless it failed for good. */
	#failedWith(error: unknown, by: number | undefined): void {
		this.#attempt = undefined;
		this.#lastFailure = { error };
		if (permanent(error)) {
			this.#failed = true;
			for (const waiter of [...this.#waiters]) waiter({ error });
			return;
		}
		this.#failures += 1;
		const backoff = retryAfter(this.#failures);
		this.#schedule(by === undefined ? backoff : Math.min(backoff, by - Date.now()));
	}

	/** The next call after `ms`. Its timer keeps the process alive only while an open waits. */
	#schedule(ms: number): void {
		clearTimeout(this.#timer);
		this.#timer = setTimeout(() => this.#due(), Math.min(MAX_TIMER_MS, Math.max(0, ms)));
		if (this.#waiters.size === 0) this.#timer.unref();
	}

	#due(): void {
		this.#timer = undefined;
		if (this.#waiters.size === 0 && !inUse()) {
			this.stop("no binding uses it", false);
			return;
		}
		this.#call();
	}
}

/** The error an open of `stream` rejects with when the provider failed for good with `error`: an UrsulaError as it is, any other error as the cause of one that says `fix`. */
function failedForGood(stream: string, error: unknown): unknown {
	if (error instanceof UrsulaError) return error;
	return new UrsulaUnavailableError(`unauthorized: the token provider failed for good (${reasonOf(error)}): the open of ${stream} has no token`, "unauthorized", error, "fix");
}

/** This thread's token source. */
const source = new Source();
/** The paths this thread bound, whatever token their opens named: the process's token serves them all. */
const paths = new Set<string>();
/** Opens that got a token from a provider and have not bound yet. */
let opening = 0;

/** Drops the paths that retired, then whether a path of the thread is bound or an open with a provider is binding. */
function inUse(): boolean {
	for (const path of [...paths]) {
		try {
			if (call<{ state: string }>("SELECT ursula_status(?)", path).state === "retired") paths.delete(path);
		} catch {
			paths.delete(path);
		}
	}
	return paths.size > 0 || opening > 0;
}

/** Whether the binding of `path` is blocked `unauthorized` now, or `undefined` once it retired. */
function blockedNow(path: string): boolean | undefined {
	try {
		const status = call<{ state: string; replication: { condition: string | null } }>("SELECT ursula_status(?)", path);
		return status.state === "retired" ? undefined : status.replication.condition === "unauthorized";
	} catch {
		return undefined;
	}
}

/** Whether each of `paths`, one at least, is still bound and no longer blocked `unauthorized`. */
function cleared(paths: readonly string[]): boolean {
	return paths.length > 0 && paths.every((path) => blockedNow(path) === false);
}

/** The extension's events: a 401 asks for a token at once, and a retired binding may stop the provider. */
function onEvent(event: PumpEvent): void {
	switch (event.event) {
		case "condition":
			if (event.condition === "unauthorized") source.blocked(event.path === null ? [] : [event.path]);
			break;
		case "retired": {
			// Pruned with or without a provider, so the paths of string opens never pile up.
			const used = inUse();
			if (!used && source.provider !== undefined && !source.waited) source.stop("no binding uses it", false);
			break;
		}
		case "gap": {
			// The events it dropped may have held a 401: status still shows it.
			if (source.provider === undefined) break;
			const blocked = [...paths].filter((path) => blockedNow(path) === true);
			if (blocked.length > 0) source.blocked(blocked);
			break;
		}
		default:
			break;
	}
}

// The pump keeps running for the provider while it serves a bound path.
watch(onEvent, () => source.provider !== undefined && inUse());

/** `db` is bound on this thread: the provider serves its path until it retires, whatever token its open named. */
export function bound<T extends { readonly boundPath: string }>(db: T): T {
	paths.add(db.boundPath);
	if (source.provider !== undefined) {
		// The pump delivers the 401s and the retirements the provider follows.
		try {
			pump();
		} catch {
			// The pump is down: the scheduled refreshes go on.
		}
	}
	return db;
}

/**
 * Opens with a token from `provider`, applied to the process before `open` runs, all bounded by
 * `signal`. `provider` becomes the function this thread's next call runs. An open refused with 401
 * drops its token and asks for another, and rejects with the provider's last failure as its cause.
 */
export async function openWithProvider<T extends { readonly boundPath: string }>(provider: TokenProvider, stream: string, signal: AbortSignal, open: () => Promise<T>): Promise<T> {
	source.use(provider);
	opening += 1;
	try {
		const token = await source.token(stream, signal);
		try {
			return bound(await open());
		} catch (error) {
			throw refusedWith(error, token);
		}
	} finally {
		opening -= 1;
	}
}

/** The error an open under `token` throws: a 401 drops the token, and says `retry` when the provider's last call failed but may succeed. */
function refusedWith(error: unknown, token: string | undefined): unknown {
	if (!(error instanceof UrsulaUnavailableError) || error.kind !== "unauthorized" || token === undefined) return error;
	const last = source.lastFailure;
	source.refused(token);
	// No failure recorded: the server refused a fresh token, which a new call will not fix.
	if (last === undefined) return error;
	return new UrsulaUnavailableError(`${error.message} (the token provider's last call failed: ${reasonOf(last.error)})`, "unauthorized", last.error, permanent(last.error) ? "fix" : "retry");
}

/** An open with a string token: requests carry `token` from now on, and this thread's provider stops. */
export function useStringToken(token: string): void {
	// Stopped already, it still forgets a token the server refused.
	source.stop("an open with a string token replaced it", true);
	apply(token);
}

/**
 * The `/tonbo` entry's resolve made a Tonbo API key the process's source of Streams tokens, which the
 * extension exchanges and refreshes, and cleared the token set: requests carry the key's tokens from
 * now on, so this thread's provider stops, and a token it set since the resolve is withdrawn.
 */
export function useKeyTokens(): void {
	const serving = source.provider !== undefined;
	source.stop("an open with a Tonbo API key replaced it", true);
	if (serving) call("SELECT ursula_set_token('', '')");
}
