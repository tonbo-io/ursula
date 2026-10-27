// The errors the package throws (design: Errors), the SQLite result codes they map from, and the
// one mapping from a failure of the extension to them (`classify`). Shared by the root entry and
// the Pi adapter (pi.ts). The error classes and the types of their fields are public.

/** SQLITE_ERROR: a SQL error, and every error of an Ursula SQL function. */
export const SQLITE_ERROR = 1;
/** SQLITE_IOERR, the primary code (`errcode & 0xff`) of every I/O error. */
export const SQLITE_IOERR = 10;
/** SQLITE_IOERR_READ: a statement of a database that was reset, which reads. */
export const SQLITE_IOERR_READ = 266;
/** SQLITE_IOERR_WRITE: a statement of a database that was reset, which writes. */
export const SQLITE_IOERR_WRITE = 778;
/** SQLITE_IOERR_AUTH: a write refused with nothing committed (`backlog_full`, `unsupported`, `too_large`). */
export const SQLITE_IOERR_AUTH = 7178;

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
/** The kinds of an {@link UrsulaUnavailableError}: the last condition the open met, or `cancelled` when its signal fired first. */
export type UnavailableKind = "unreachable" | "unavailable" | "rate_limited" | "unauthorized" | "payment_required" | "forbidden" | "wrong_cluster" | "cancelled";
/** The kinds of an {@link UrsulaSetupError}. */
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

const WRITE_REFUSED_KINDS: ReadonlySet<string> = new Set<WriteRefusedKind>(["backlog_full", "unsupported", "too_large"]);
const RESET_KINDS: ReadonlySet<string> = new Set<ResetKind>(["fenced", "stale", "rewound", "deleted", "local_damage", "stream_state", "internal"]);
const UNAVAILABLE_KINDS: ReadonlySet<string> = new Set<UnavailableKind>(["unreachable", "unavailable", "rate_limited", "unauthorized", "payment_required", "forbidden", "cancelled"]);

/** Each kind's action: `ErrorKind::action` and `reset_action` in clients/sqlite-vfs/src/error.rs. */
const ACTIONS: ReadonlyMap<string, UrsulaErrorAction> = new Map<string, UrsulaErrorAction>([
	["backlog_full", "retry"],
	["unsupported", "fix"],
	["too_large", "fix"],
	["fenced", "stop"],
	["stale", "reopen"],
	["rewound", "reopen"],
	["deleted", "fix"],
	["local_damage", "reopen"],
	["stream_state", "fix"],
	["internal", "reopen"],
	["unreachable", "retry"],
	["unavailable", "retry"],
	["rate_limited", "retry"],
	["cancelled", "retry"],
	["claim_races", "retry"],
	["in_use", "retry"],
	["unauthorized", "fix"],
	["payment_required", "fix"],
	["forbidden", "fix"],
	["wrong_cluster", "fix"],
	["invalid_argument", "fix"],
	["invalid_url", "fix"],
	["invalid_name", "fix"],
	["not_found", "fix"],
	["local_setup", "fix"],
	["stream_format", "fix"],
	["no_server_identity", "fix"],
	["protocol_mismatch", "fix"],
	["not_bound", "fix"],
	["bound_elsewhere", "fix"],
	["missing_credentials", "fix"],
	["credentials_refused", "fix"],
	["invalid_api_key", "fix"],
	["database_not_found", "fix"],
]);

/** The action of `kind`, `fix` for a kind this package does not know (a newer extension's). */
export const actionOf = (kind: string): UrsulaErrorAction => ACTIONS.get(kind) ?? "fix";

/**
 * A failure of the extension: the base class of the four the package throws. `kind` names what
 * failed and `action` what to do ({@link UrsulaErrorAction}): branch on `action`. `cause` is the error
 * the failure surfaced as, usually SQLite's, and when it is SQLite's its `code`, `errcode` and
 * `errstr` are copied, so code that reads them keeps working.
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
	constructor(message: string, kind: WriteRefusedKind, cause: unknown) {
		super(message, kind, actionOf(kind), cause);
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
	constructor(message: string, kind: ResetKind, cause?: unknown) {
		super(message, kind, actionOf(kind), cause);
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
	constructor(message: string, kind: UnavailableKind, cause?: unknown, action: UrsulaErrorAction = actionOf(kind)) {
		super(message, kind, action, cause);
		this.name = "UrsulaUnavailableError";
	}
}

/** `open()` cannot succeed as asked: its message says what to change. `in_use` and `claim_races` say `retry`. */
export class UrsulaSetupError extends UrsulaError {
	declare readonly kind: SetupKind;
	constructor(message: string, kind: SetupKind, cause?: unknown) {
		super(message, kind, actionOf(kind), cause);
		this.name = "UrsulaSetupError";
	}
}

/**
 * The error of `kind` with `message`. `fromSync` says the failure is a statement's or `synced()`'s,
 * where `stream_state` and `internal` are resets; for `open()` they are setup errors.
 */
export function errorOf(kind: string, message: string, cause: unknown, fromSync: boolean): UrsulaError {
	if (WRITE_REFUSED_KINDS.has(kind)) return new UrsulaWriteRefusedError(message, kind as WriteRefusedKind, cause);
	if (RESET_KINDS.has(kind) && (fromSync || (kind !== "stream_state" && kind !== "internal"))) return new UrsulaResetError(message, kind as ResetKind, cause);
	if (UNAVAILABLE_KINDS.has(kind)) return new UrsulaUnavailableError(message, kind as UnavailableKind, cause);
	return new UrsulaSetupError(message, kind as SetupKind, cause);
}

/** An Ursula SQL function's error: `<function>: <kind>: <message>`. */
const FUNCTION_ERROR = /^(ursula_[a-z_]+): ([a-z_]+): /;

/** The error for `error` when it is an Ursula SQL function's, `undefined` otherwise. */
function functionError(error: unknown): UrsulaError | undefined {
	if (errcode(error) !== SQLITE_ERROR || !(error instanceof Error)) return undefined;
	const named = FUNCTION_ERROR.exec(error.message);
	if (named === null) return undefined;
	const [, fn, kind] = named as unknown as [string, string, string];
	// An import that got no definite answer may have claimed the stream, which then refuses opens
	// and imports: a retry into it fails, so fix (check the stream, delete or recreate it).
	if (fn === "ursula_import" && UNAVAILABLE_KINDS.has(kind)) return new UrsulaUnavailableError(error.message, kind as UnavailableKind, error, "fix");
	return errorOf(kind, error.message, error, fn !== "ursula_open" && fn !== "ursula_cancel" && fn !== "ursula_import");
}

/** The fields of `ursula_status` that {@link classify} reads. */
export interface FailureStatus {
	readonly refused: { readonly kind: string; readonly action: string; readonly message: string } | null;
	readonly reset: { readonly kind: string; readonly action: string } | null;
}

/**
 * What to throw for `error`, which a statement or an Ursula SQL function threw: an
 * {@link UrsulaError} when the extension failed it, and `error` itself, unchanged, otherwise (a
 * constraint violation, a syntax error). An Ursula SQL function's error names its kind. A statement's
 * failure surfaces as one of SQLite's I/O errors, which says nothing: then, and only then, `status`
 * reads the database's status, which names it. SQLITE_IOERR_AUTH is a refused write
 * (`status.refused`), 266 and 778 are a reset (`status.reset`). A status that throws, or that names
 * nothing, leaves `error` as it is: the failure was another database's, one used through SQL
 * `ATTACH DATABASE`.
 */
export function classify(error: unknown, status?: () => FailureStatus): unknown {
	if (error instanceof UrsulaError) return error;
	const named = functionError(error);
	if (named !== undefined) return named;
	const code = errcode(error);
	if (code === undefined || status === undefined || (code & 0xff) !== SQLITE_IOERR) return error;
	let s: FailureStatus;
	try {
		s = status();
	} catch {
		return error;
	}
	if (s.reset !== null) {
		const kind = s.reset.kind;
		const what = code === SQLITE_IOERR_WRITE ? "write" : code === SQLITE_IOERR_READ ? "read" : "statement";
		return errorOf(kind, `${what} refused, the database was reset (${kind}): ${advice(actionOf(kind))}`, error, true);
	}
	if (code === SQLITE_IOERR_AUTH && s.refused !== null) {
		const kind = s.refused.kind;
		return errorOf(kind, `${s.refused.message}. ${refusedAdvice(kind)}`, error, true);
	}
	return error;
}

/** What to do, in words, about a refused write. */
function refusedAdvice(kind: string): string {
	switch (kind) {
		case "backlog_full":
			return "Nothing was committed: await synced(), then run the transaction again.";
		case "too_large":
			return "Nothing was committed: split the transaction, or raise URSULA_VFS_MAX_COMMIT_BYTES.";
		default:
			return "Nothing was committed.";
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

/**
 * {@link classify} for an error a library may have wrapped: the first error of the chain
 * {@link findWrapped} walks that maps to an {@link UrsulaError} is the result, so a failure of the
 * extension never passes for the wrapper's own kind of error. `error` itself otherwise.
 */
export function classifyWrapped(error: unknown, status: () => FailureStatus): unknown {
	return (
		findWrapped(error, (next) => {
			const classified = classify(next, status);
			return classified instanceof UrsulaError ? classified : undefined;
		}) ?? error
	);
}
