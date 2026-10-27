// The bearer token from a provider (design: API, Tokens): `open()`'s `token` may be a function that
// hands out a token and when it expires. The package calls it before the open's first request,
// again ahead of each token's expiry, and again at once when the extension reports a request
// refused with 401 (the replication condition `unauthorized`). It applies each token with
// `ursula_set_token`, so the token stays one per process, and the last open's token source wins: a
// later open with another provider takes over the bindings this one served, and one with a string
// stops it.
//
// One source per JavaScript thread, since a function lives on the thread that made it. Its timers
// are unref'd, so they never keep the process alive, except while an `open()` waits for a token,
// which the caller awaits. It stops once no binding opened with a provider on the thread is bound.
import { call } from "./control.ts";
import { UrsulaError, UrsulaSetupError, UrsulaUnavailableError } from "./errors.ts";
import { type PumpEvent, pump, watch } from "./pump.ts";

/** What a {@link TokenProvider} hands out: a bearer token and when it expires. */
export interface BearerToken {
	/** The token, printable ASCII without spaces. Never logged. */
	readonly token: string;
	/** When the token expires: a `Date`, or milliseconds since the epoch. */
	readonly expiresAt: Date | number;
}

/**
 * Hands out a fresh bearer token: `open()`'s `token` option. `signal` fires when the package no
 * longer waits for the answer (after 30 s, or once another open replaced the provider).
 *
 * To fail for good, throw an {@link UrsulaError} whose action is `fix`, such as
 * `new UrsulaUnavailableError("unauthorized: the key was revoked", "unauthorized")`: the package
 * stops calling the provider until the next `open()` with one. It retries every other error with
 * backoff.
 */
export type TokenProvider = (options: { readonly signal: AbortSignal }) => Promise<BearerToken>;

/** A token as the package holds it: when it expires and when to ask for the next one, in epoch ms. */
interface Fresh {
	readonly token: string;
	readonly expiresAt: number;
	readonly refreshAt: number;
}

/** How long one call of the provider may take before it counts as failed. */
const PROVIDER_MS = 30_000;
/** The first retry after a failed call, doubled after each failure in a row up to {@link MAX_RETRY_MS}, then jittered down to half. */
const FIRST_RETRY_MS = 1_000;
const MAX_RETRY_MS = 30_000;
/** The earliest refresh after a token arrived, whatever its lifetime. */
const MIN_REFRESH_MS = 1_000;
/** The longest delay a timer takes. A refresh further out runs then. */
const MAX_TIMER_MS = 2_147_483_647;
/** A token as `Authorization: Bearer` carries it (clients/sqlite-vfs/src/auth.rs `usable`). */
const USABLE = /^[\x21-\x7e]+$/;

/** A refresh is due after 70 to 80% of the token's lifetime. */
const refreshAfter = (lifetime: number): number => Math.max(MIN_REFRESH_MS, lifetime * (0.7 + 0.1 * Math.random()));
const retryAfter = (failures: number): number => Math.min(MAX_RETRY_MS, FIRST_RETRY_MS * 2 ** Math.min(failures - 1, 16)) * (0.5 + 0.5 * Math.random());

/** A failure the package does not retry: the provider's own UrsulaError that says `fix`, or a token it cannot send. */
const permanent = (error: unknown): error is UrsulaError => error instanceof UrsulaError && error.action === "fix";

const reasonOf = (reason: unknown): string => (reason instanceof Error ? reason.message : String(reason));

/** What the provider returned, checked: a token it cannot send fails for good, one that expired already is retried. */
function checked(got: unknown, now: number): Fresh {
	const answer = got as { token?: unknown; expiresAt?: unknown } | null | undefined;
	const token = typeof answer?.token === "string" ? answer.token.trim() : "";
	if (!USABLE.test(token)) {
		throw new UrsulaSetupError("invalid_argument: the token provider returned no usable token: a non-empty string of printable ASCII without spaces", "invalid_argument");
	}
	const raw = answer?.expiresAt;
	const expiresAt = raw instanceof Date ? raw.getTime() : raw;
	if (typeof expiresAt !== "number" || !Number.isFinite(expiresAt)) {
		throw new UrsulaSetupError("invalid_argument: the token provider returned no expiresAt: a Date, or milliseconds since the epoch", "invalid_argument");
	}
	const lifetime = expiresAt - now;
	if (lifetime <= 0) throw new Error(`the token provider returned a token that expired ${-lifetime} ms ago`);
	return { token, expiresAt, refreshAt: now + refreshAfter(lifetime) };
}

/** Requests carry `token` from now on. */
const apply = (token: string): void => {
	call("SELECT ursula_set_token('', ?)", token);
};

/** `promise`, or `signal`'s reason when it fires first. */
function raced<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
	return new Promise((resolve, reject) => {
		const abort = (): void => reject(signal.reason);
		if (signal.aborted) abort();
		signal.addEventListener("abort", abort, { once: true });
		promise.then(resolve, reject).finally(() => signal.removeEventListener("abort", abort));
	});
}

/** What an open waiting for a token learns of an attempt: a token, a failure for good, or a failure to retry. */
type Outcome = { token: string } | { error: UrsulaError } | "retrying";

/** One provider and its schedule. At most one call of it runs at a time: concurrent refreshes share it. */
class Source {
	readonly provider: TokenProvider;
	readonly #stopped = new AbortController();
	#current: Fresh | undefined;
	#calling = false;
	#timer: NodeJS.Timeout | undefined;
	/** Failed calls in a row, and the last one's error. */
	#failures = 0;
	#lastError: unknown;
	/** It failed for good: no call until the next open. */
	#failed = false;
	/** The opens waiting for a token. */
	readonly #waiters = new Set<(outcome: Outcome) => void>();

	constructor(provider: TokenProvider) {
		this.provider = provider;
	}

	/**
	 * The token for an open of `stream`, applied: the current one until its refresh is due, else the
	 * next the provider hands out. While a refresh fails, an unexpired token serves. Rejects with the
	 * provider's error when it fails for good, and when `signal` fires first with an
	 * {@link UrsulaUnavailableError} `unauthorized` that says `retry`.
	 */
	token(stream: string, signal: AbortSignal): Promise<string> {
		// A new open asks a provider that failed for good again.
		const failed = this.#failed;
		this.#failed = false;
		const now = Date.now();
		const current = this.#current;
		if (current !== undefined && (now < current.refreshAt || ((failed || this.#failures > 0) && now < current.expiresAt))) {
			apply(current.token);
			if (failed && !this.#calling) this.#call();
			return Promise.resolve(current.token);
		}
		return new Promise((resolve, reject) => {
			const done = (): void => {
				this.#waiters.delete(waiter);
				signal.removeEventListener("abort", abort);
				if (this.#waiters.size === 0) this.#timer?.unref();
			};
			const waiter = (outcome: Outcome): void => {
				if (outcome === "retrying") {
					const c = this.#current;
					if (c === undefined || Date.now() >= c.expiresAt) return;
					outcome = { token: c.token };
				}
				done();
				if ("token" in outcome) resolve(outcome.token);
				else reject(outcome.error);
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
			// A retry already scheduled runs on time; the open keeps the process alive meanwhile.
			if (this.#failures > 0 && this.#timer !== undefined) this.#timer.ref();
			else if (!this.#calling) this.#call();
		});
	}

	/** A request was refused with 401: the current token is no good, ask for another at once. */
	refused(): void {
		if (this.#stopped.signal.aborted || this.#failed) return;
		this.#current = undefined;
		if (!this.#calling) this.#call();
	}

	/** Stops it: no further call, its timer cleared, and the opens waiting for a token fail. */
	stop(error: UrsulaError): void {
		this.#stopped.abort(error);
		clearTimeout(this.#timer);
		this.#timer = undefined;
		for (const waiter of [...this.#waiters]) waiter({ error });
	}

	#gaveUp(stream: string, signal: AbortSignal): UrsulaUnavailableError {
		const why = this.#failures > 0 ? `it failed: ${reasonOf(this.#lastError)}` : `it did not answer: ${reasonOf(signal.reason)}`;
		const cause = this.#failures > 0 ? this.#lastError : signal.reason;
		return new UrsulaUnavailableError(`unauthorized: the token provider handed out no token (${why}): the open of ${stream} gave up when its signal fired`, "unauthorized", cause, "retry");
	}

	#call(): void {
		clearTimeout(this.#timer);
		this.#timer = undefined;
		this.#calling = true;
		const signal = AbortSignal.any([this.#stopped.signal, AbortSignal.timeout(PROVIDER_MS)]);
		raced(
			Promise.resolve().then(() => this.provider({ signal })),
			signal,
		)
			.then((got) => {
				const fresh = checked(got, Date.now());
				if (!this.#stopped.signal.aborted) apply(fresh.token);
				return fresh;
			})
			.then(
				(fresh) => this.#succeeded(fresh),
				(error: unknown) => this.#failedWith(error),
			);
	}

	#succeeded(fresh: Fresh): void {
		this.#calling = false;
		if (this.#stopped.signal.aborted) return;
		this.#current = fresh;
		this.#failures = 0;
		this.#lastError = undefined;
		for (const waiter of [...this.#waiters]) waiter({ token: fresh.token });
		this.#schedule(fresh.refreshAt - Date.now());
	}

	#failedWith(error: unknown): void {
		this.#calling = false;
		if (this.#stopped.signal.aborted) return;
		if (permanent(error)) {
			this.#failed = true;
			for (const waiter of [...this.#waiters]) waiter({ error });
			return;
		}
		this.#failures += 1;
		this.#lastError = error;
		this.#schedule(retryAfter(this.#failures));
		for (const waiter of [...this.#waiters]) waiter("retrying");
	}

	/** The next call after `ms`. Its timer keeps the process alive only while an open waits. */
	#schedule(ms: number): void {
		clearTimeout(this.#timer);
		this.#timer = setTimeout(() => this.#due(), Math.min(MAX_TIMER_MS, Math.max(0, ms)));
		if (this.#waiters.size === 0) this.#timer.unref();
	}

	#due(): void {
		this.#timer = undefined;
		if (this.#stopped.signal.aborted) return;
		if (this.#waiters.size === 0 && !inUse()) {
			drop(this, "no binding uses it");
			return;
		}
		this.#call();
	}

	/** Whether an open waits for it. */
	get waited(): boolean {
		return this.#waiters.size > 0;
	}
}

/** This thread's token source, if its last open that named a token gave a provider. */
let source: Source | undefined;
/** The bound paths opened with a provider on this thread since the last open with a string token. */
const paths = new Set<string>();
/** Opens that got a token from a provider and have not bound yet. */
let opening = 0;
let watching = false;

/** Whether a binding opened with a provider is bound, or such an open is binding: retired paths are dropped. */
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

/** Stops `stopped`, this thread's source, because of `why`. */
function drop(stopped: Source, why: string): void {
	stopped.stop(new UrsulaUnavailableError(`cancelled: the token provider stopped: ${why}`, "cancelled"));
	if (source === stopped) source = undefined;
}

/** The extension's events: a 401 asks for a token at once, and a retired binding may end the source. */
function onEvent(event: PumpEvent): void {
	const s = source;
	if (s === undefined) return;
	switch (event.event) {
		case "condition":
			if (event.condition === "unauthorized") s.refused();
			break;
		case "retired":
			if (!s.waited && !inUse()) drop(s, "no binding uses it");
			break;
		case "gap":
			// The events it dropped may have held a 401: status still shows it.
			for (const path of paths) {
				try {
					if (call<{ replication: { condition: string | null } }>("SELECT ursula_status(?)", path).replication.condition === "unauthorized") {
						s.refused();
						break;
					}
				} catch {
					// Retired meanwhile.
				}
			}
			break;
		default:
			break;
	}
}

/** A token an open got from a provider: `done` once the open bound `path`, or failed (no path). */
export interface TokenLease {
	readonly token: string;
	done(path?: string): void;
}

/**
 * The token for an open of `stream` from `provider`, applied to the process, bounded by `signal`.
 * `provider` becomes this thread's source, taking over the bindings an earlier provider served.
 */
export async function leaseToken(provider: TokenProvider, stream: string, signal: AbortSignal): Promise<TokenLease> {
	if (source?.provider !== provider) {
		if (source !== undefined) drop(source, "an open replaced it");
		source = new Source(provider);
	}
	if (!watching) {
		watching = true;
		watch(onEvent);
	}
	const leased = source;
	opening += 1;
	let token: string;
	try {
		token = await leased.token(stream, signal);
	} catch (error) {
		opening -= 1;
		throw error;
	}
	let open = true;
	return {
		token,
		done: (path) => {
			if (!open) return;
			open = false;
			opening -= 1;
			if (path === undefined || source !== leased) return;
			paths.add(path);
			// The pump delivers the 401s and the retirements it waits for.
			try {
				pump();
			} catch {
				// The pump is down: the scheduled refreshes go on.
			}
		},
	};
}

/** An open with a string token: it applies to the process, so this thread's provider stops. */
export function dropTokenProvider(): void {
	if (source !== undefined) drop(source, "an open with a string token replaced it");
	paths.clear();
}
