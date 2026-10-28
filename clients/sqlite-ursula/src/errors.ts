// The errors the package throws (design: Errors), and the one constructor of the error of a failure
// the extension classified (`errorOf`). Which failure is which error is the extension's to say
// (`ursula_classify`, control.ts `classify`): its kind, action and class. Shared by the root entry
// and the Pi adapter (pi.ts). The error classes and the types of their fields are public. A
// constructor's default action, which its class's documentation lists, serves an error the
// application makes, such as a token provider's: the package passes the extension's.

/** SQLITE_ERROR: a SQL error, and every error of an Ursula SQL function. */
export const SQLITE_ERROR = 1;

/** The SQLite result code of an error node:sqlite threw (`errcode`), if it is one. */
export function errcode(error: unknown): number | undefined {
	const code = (error as { errcode?: unknown } | null | undefined)?.errcode;
	return typeof code === "number" ? code : undefined;
}

/**
 * What to do about a failure. Branch on it:
 *
 * - `retry`: nothing was lost and the database stays usable. For `backlog_full`, await `synced()`
 *   and run the transaction again. For a failed `open()`, call it again later.
 * - `reopen`: close every connection of the database and `open()` it again.
 * - `stop`: another owner took the stream over. Stop using the database, and never reopen it in a
 *   loop: opening it takes the stream back.
 * - `fix`: it cannot succeed as asked. Fix the URL, the setup, the stream or the transaction.
 */
export type UrsulaErrorAction = "retry" | "reopen" | "stop" | "fix";

/** The kinds of an {@link UrsulaWriteRefusedError}. */
export type WriteRefusedKind = "backlog_full" | "unsupported" | "too_large";
/** The kinds of an {@link UrsulaResetError} (design: Resets). */
export type ResetKind = "fenced" | "stale" | "rewound" | "deleted" | "local_damage" | "stream_state" | "internal";
/**
 * The kinds of an {@link UrsulaUnavailableError}: the last condition the open met, or `cancelled` when
 * its signal fired first. An open whose last condition was `wrong_cluster`, `no_server_identity` or
 * `protocol_mismatch` throws an {@link UrsulaSetupError} of that kind instead. A bound database
 * reports them as `status().replication.condition`.
 */
export type UnavailableKind = "unreachable" | "unavailable" | "rate_limited" | "unauthorized" | "payment_required" | "forbidden" | "cancelled";
/** The kinds of an {@link UrsulaSetupError}. `invalid_name` is the `/tonbo` entry's, for an empty database name. */
export type SetupKind =
	| "invalid_argument"
	| "invalid_url"
	| "invalid_name"
	| "not_found"
	| "in_use"
	| "bound_elsewhere"
	| "local_setup"
	| "stream_format"
	| "no_server_identity"
	| "protocol_mismatch"
	| "wrong_cluster"
	| "stream_state"
	| "claim_races"
	| "not_bound"
	| "internal";

/**
 * The kinds of the `/tonbo` entry's `TonboError` (tonbo.ts): a Tonbo API key that is missing, refused
 * or unusable, a login file the package refuses to read, or a database the account does not have.
 */
export type TonboKind = "missing_credentials" | "credentials_refused" | "invalid_api_key" | "database_not_found";

export type UrsulaErrorKind = WriteRefusedKind | ResetKind | UnavailableKind | SetupKind | TonboKind;

/**
 * A failure as the extension names it, in `ursula_classify`'s result, in events and in status:
 * its kind, what to do, and the error to throw (`write_refused`, `reset`, `unavailable` or `setup`).
 */
export interface Named {
	readonly kind: string;
	readonly action: string;
	readonly class: string;
}

/**
 * A failure of the extension or of an open: the base class of every error the package throws.
 * `kind` names what failed and `action` what to do ({@link UrsulaErrorAction}): branch on `action`.
 * `cause` is the error the failure surfaced as, usually SQLite's, and when it is SQLite's its
 * `code`, `errcode` and `errstr` are copied, so code that reads them keeps working.
 */
export class UrsulaError extends Error {
	readonly kind: UrsulaErrorKind;
	readonly action: UrsulaErrorAction;
	/** `cause`'s, when it is an error of node:sqlite. */
	declare readonly code?: string;
	/** `cause`'s SQLite result code, when it is an error of node:sqlite (7178, 266 or 778). */
	declare readonly errcode?: number;
	/** `cause`'s description of `errcode`, when it is an error of node:sqlite. */
	declare readonly errstr?: string;
	constructor(message: string, kind: UrsulaErrorKind, action: UrsulaErrorAction, cause: unknown) {
		super(message, cause === undefined ? undefined : { cause });
		this.name = "UrsulaError";
		this.kind = kind;
		this.action = action;
		const sqlite = cause as { code?: unknown; errcode?: unknown; errstr?: unknown } | null | undefined;
		if (typeof sqlite?.errcode === "number") {
			this.errcode = sqlite.errcode;
			if (typeof sqlite.code === "string") this.code = sqlite.code;
			if (typeof sqlite.errstr === "string") this.errstr = sqlite.errstr;
		}
	}
}

/**
 * A write or COMMIT refused with nothing committed (SQLITE_IOERR_AUTH): SQLite rolled the whole
 * transaction back, and the database stays usable. `backlog_full` (`retry`): more than
 * `maxPendingBytes` of commits wait for the stream, so await `synced()` and run the transaction
 * again. `unsupported` (`fix`): a write the extension does not capture, such as one in EXCLUSIVE
 * locking mode. `too_large` (`fix`): the transaction's commit holds more bytes of pages than the
 * commit limit (`URSULA_VFS_MAX_COMMIT_BYTES`, 128 MiB by default), so split the transaction or
 * raise the limit.
 */
export class UrsulaWriteRefusedError extends UrsulaError {
	declare readonly kind: WriteRefusedKind;
	constructor(message: string, kind: WriteRefusedKind, cause: unknown, action: UrsulaErrorAction = kind === "backlog_full" ? "retry" : "fix") {
		super(message, kind, action, cause);
		this.name = "UrsulaWriteRefusedError";
	}
}

/**
 * The database was reset (design: Resets): every statement of it throws this, reads included,
 * `synced()` rejects with it, and the `reset` listeners get it. `fenced` (`stop`): another owner took
 * the stream over. The other kinds say to `reopen` (a new `open()` of the URL) or to `fix` the stream
 * first.
 */
export class UrsulaResetError extends UrsulaError {
	declare readonly kind: ResetKind;
	constructor(message: string, kind: ResetKind, cause?: unknown, action: UrsulaErrorAction = kind === "fenced" ? "stop" : kind === "deleted" || kind === "stream_state" ? "fix" : "reopen") {
		super(message, kind, action, cause);
		this.name = "UrsulaResetError";
	}
}

/**
 * `open()` found no trusted state in time: its signal fired (by default after 30 s) while the stream
 * was not answering as it must. `kind` is the last condition the open met, or `cancelled` when it met
 * none yet.
 */
export class UrsulaUnavailableError extends UrsulaError {
	declare readonly kind: UnavailableKind;
	constructor(message: string, kind: UnavailableKind, cause?: unknown, action: UrsulaErrorAction = kind === "unauthorized" || kind === "payment_required" || kind === "forbidden" ? "fix" : "retry") {
		super(message, kind, action, cause);
		this.name = "UrsulaUnavailableError";
	}
}

/**
 * `open()` cannot succeed as asked: its message says what to change. Every kind says `fix`, except
 * `in_use` and `claim_races`, which say `retry`, and `internal`, which says `reopen`.
 */
export class UrsulaSetupError extends UrsulaError {
	declare readonly kind: SetupKind;
	constructor(message: string, kind: SetupKind, cause?: unknown, action: UrsulaErrorAction = kind === "in_use" || kind === "claim_races" ? "retry" : kind === "internal" ? "reopen" : "fix") {
		super(message, kind, action, cause);
		this.name = "UrsulaSetupError";
	}
}

/**
 * The error of a failure the extension named (`named`), with `message` and `cause`: its class picks
 * the error, and its action is the extension's.
 */
export function errorOf(named: Named, message: string, cause: unknown): UrsulaError {
	const action = named.action as UrsulaErrorAction;
	switch (named.class) {
		case "write_refused":
			return new UrsulaWriteRefusedError(message, named.kind as WriteRefusedKind, cause, action);
		case "reset":
			return new UrsulaResetError(message, named.kind as ResetKind, cause, action);
		case "unavailable":
			return new UrsulaUnavailableError(message, named.kind as UnavailableKind, cause, action);
		default:
			return new UrsulaSetupError(message, named.kind as SetupKind, cause, action);
	}
}

/** What to do, in words, for an error whose own message (SQLite's) says nothing. */
export function advice(action: UrsulaErrorAction): string {
	switch (action) {
		case "retry":
			return "try again later.";
		case "reopen":
			return "close every connection of the database and open() it again.";
		case "stop":
			return "another owner took the stream over: stop using this database, and never reopen it in a loop.";
		case "fix":
			return "fix the cause, then open() the database again.";
	}
}

/**
 * The first value `match` returns for an error in the chain of `error`, which a library may have
 * wrapped: `error` itself, the errors an AggregateError holds (a driver's failed ROLLBACK after a
 * COMMIT that SQLite rolled back), and the errors they wrap (`cause`, such as Pi's StorageRejected
 * around a read the extension failed, or drizzle's DrizzleQueryError), in that order. Each object is
 * visited once, and at most 16 are. `undefined` when `match` returns it for every one.
 */
export function findWrapped<T>(error: unknown, match: (error: object) => T | undefined): T | undefined {
	const pending: unknown[] = [error];
	const seen = new Set<unknown>();
	while (pending.length > 0 && seen.size < 16) {
		const next = pending.shift();
		if (next === null || typeof next !== "object" || seen.has(next)) continue;
		seen.add(next);
		const found = match(next);
		if (found !== undefined) return found;
		if (next instanceof AggregateError) pending.push(...(next.errors as unknown[]));
		if ("cause" in next) pending.push(next.cause);
	}
	return undefined;
}
