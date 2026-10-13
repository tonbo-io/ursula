// The errors the package throws, the SQLite result codes they map from, and the one mapping from a
// failure of the extension to them (`classify`). Shared by the root entry (index.ts, database.ts) and
// the Pi adapter (pi.ts). The error classes and the types of their fields are public.

/** SQLITE_READONLY: a write to a file attached read-only. */
export const SQLITE_READONLY = 8;
/** SQLITE_FULL, a primary code: a local write that found no room, after the stream acknowledged its commit (the file is poisoned). */
export const SQLITE_FULL = 13;
/** SQLITE_AUTH: `ursula_attach` refused with 402 in front of the stream. */
export const SQLITE_AUTH = 23;
/** SQLITE_IOERR_AUTH: a commit, or the claim of an `ursula_attach`, refused before it reached the stream (an attach refused with 402 gets SQLITE_AUTH). */
export const SQLITE_IOERR_AUTH = 7178;
/** SQLITE_IOERR, the primary code (`errcode & 0xff`) of every I/O error: every failure of the VFS's own surfaces as one. */
export const SQLITE_IOERR = 10;
/** SQLITE_IOERR_READ: a read transaction refused on a fenced file, before it started. */
export const SQLITE_IOERR_READ = 266;

/** The SQLite result code of an error node:sqlite threw (`errcode`), if it is one. */
export function errcode(error: unknown): number | undefined {
	const code = (error as { errcode?: unknown } | null | undefined)?.errcode;
	return typeof code === "number" ? code : undefined;
}

/**
 * What to do about a failure (`UrsulaError.action`, and `action` in `status(file).error`):
 *
 * - `retry`: nothing reached the stream, and the file stays usable. Run the same statement, or the
 *   same attach, again later, on the same database.
 * - `reattach`: the local file may no longer match the stream. Close the database and open it again
 *   (attach it again), which replays the stream.
 * - `stop`: another owner took the stream over. Stop using the database. Opening it again takes the
 *   stream back, which only a deliberate takeover does, so never reopen it in a loop.
 * - `fix`: the request cannot succeed as made. Fix the URL, the setup, the files or the transaction.
 */
export type UrsulaErrorAction = "retry" | "reattach" | "stop" | "fix";

/**
 * What refused a write before it reached the stream (`refused_kind` in `status(file)`):
 *
 * - `payment_required`: a layer in front of the stream answered `402 Payment Required`;
 * - `unauthorized`: the token was refused (`401`) until the retry budget ran out;
 * - `forbidden`: an authorizer denied the write with a `403` marked `x-ursula-not-applied`, which a
 *   gateway vouched for with `x-ursula-not-applied-checked` (any other `403` fails the commit and
 *   poisons the file, since a filtering proxy can send one after the stream applied the write);
 * - `rate_limited`: `429` until the retry budget ran out, marked or not;
 * - `not_applied`: other answers marked `x-ursula-not-applied` and vouched for by a gateway (the server
 *   under write pressure or while it knows no leader for the stream, or a gateway that could not
 *   authorize the request) until the retry budget ran out, or another marked final answer;
 * - `too_large`: the transaction's commit frame is larger than one request may be, so it was never
 *   sent (over `URSULA_VFS_MAX_FRAME_BYTES`, 32 MiB by default), or a lower limit in front refused it
 *   with a marked and vouched `413`. The same transaction can never be sent: split it into smaller
 *   ones;
 * - `unreachable`: every attempt failed before any byte of it went out, or was refused as above,
 *   until the retry budget ran out, and the last one failed before it went out: its host did not
 *   resolve, or no connection to it was made (the TLS handshake included). Commit again once the
 *   endpoint is reachable. The reason is the last attempt's failure (such as `timeout: resolve`).
 *   From 0.7.4.
 *
 * The marker counts only with the vouch, which `ursula gateway` adds from 0.7.3 on. Without such a
 * gateway in front of the servers, only a `401`, `402` or `429`, or the extension's own frame limit,
 * refuses a write this way.
 */
export type WriteRefusedKind = "payment_required" | "unauthorized" | "forbidden" | "rate_limited" | "not_applied" | "too_large" | "unreachable";

/** The kinds of an {@link UrsulaWriteRefusedError}: what refused the write, or why nothing was sent. */
export type RefusedKind = WriteRefusedKind | "refused" | "unavailable";
/** The kinds of an {@link UrsulaReattachError}. */
export type ReattachKind = "outcome_unknown" | "local_damage" | "local_state" | "internal";
/** The kinds of an {@link UrsulaSetupError}. */
export type SetupKind =
	| "invalid_argument"
	| "invalid_url"
	| "invalid_name"
	| "not_found"
	| "in_use"
	| "unreplicated_file"
	| "other_stream"
	| "unsupported"
	| "stream_state"
	| "not_attached"
	| "local_setup";

/**
 * A stable name of what failed: the extension's kinds, which the design doc lists with what each
 * means (docs/architecture/sqlite-vfs.md §1, errors), except that a write refused before it reached
 * the stream names what refused it ({@link WriteRefusedKind}) where the extension's kind is `refused`,
 * whenever that is known. A path whose last attach failed reports that failure's kind.
 */
export type UrsulaErrorKind = RefusedKind | ReattachKind | SetupKind | "fenced";

/** Each kind of the extension's, and its action: `ErrorKind::action` in clients/sqlite-vfs/src/error.rs. */
const ACTIONS: ReadonlyMap<string, UrsulaErrorAction> = new Map<string, UrsulaErrorAction>([
	["invalid_argument", "fix"],
	["invalid_url", "fix"],
	["invalid_name", "fix"],
	["not_found", "fix"],
	["in_use", "fix"],
	["unreplicated_file", "fix"],
	["other_stream", "fix"],
	["too_large", "fix"],
	["unsupported", "fix"],
	["stream_state", "fix"],
	["not_attached", "fix"],
	["local_setup", "fix"],
	["unreachable", "retry"],
	["unavailable", "retry"],
	["unauthorized", "retry"],
	["refused", "retry"],
	["outcome_unknown", "reattach"],
	["local_damage", "reattach"],
	["local_state", "reattach"],
	["internal", "reattach"],
	["fenced", "stop"],
]);

/** A refusal's action: `retry`, except for a transaction too large for one append, which never fits. */
const refusedAction = (kind: RefusedKind | null): UrsulaErrorAction => (kind === "too_large" ? "fix" : "retry");

/**
 * A failure of the extension, which says what to do about it: the base class of every error the
 * package throws for one. `kind` names what failed, `action` what to do ({@link UrsulaErrorAction}):
 * branch on `action`. The subclasses group the failures by what happened. `cause` is the error the
 * failure surfaced as, usually SQLite's (`disk I/O error`, `SQL logic error`), and when it is
 * SQLite's, its `code`, `errcode` and `errstr` are copied, so code that reads them keeps working.
 */
export class UrsulaError extends Error {
	readonly kind: UrsulaErrorKind;
	readonly action: UrsulaErrorAction;
	/** `cause`'s, when it is an error of node:sqlite: `ERR_SQLITE_ERROR`. */
	declare readonly code?: string;
	/** `cause`'s SQLite result code, when it is an error of node:sqlite (such as 778 for a commit the stream did not take). */
	declare readonly errcode?: number;
	/** `cause`'s description of `errcode`, when it is an error of node:sqlite. */
	declare readonly errstr?: string;
	constructor(message: string, kind: UrsulaErrorKind, action: UrsulaErrorAction, cause: unknown) {
		super(message, { cause });
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
 * Nothing reached the stream: a write refused before it reached the stream, a commit or an attach's
 * claim, by a layer in front of the stream (an authorizer, a quota or billing service, a gateway or a
 * proxy), by the server before it proposed the write (under write pressure, or while it knows no
 * leader for the stream), or by the extension itself, which never sends a transaction over its frame
 * limit (kind `too_large`), or a write that never went out because the stream's endpoint could not
 * be reached (kind `unreachable`). An attach that failed before it sent its claim, because the
 * endpoint did not answer (`unreachable`), put the request off until the retry budget ran out
 * (`unavailable`) or refused the credentials (`unauthorized`), is one too.
 *
 * Unlike an {@link UrsulaReplicationError}, it leaves the file usable. A refused commit was rolled
 * back, but the file is not poisoned: reads go on from the local file, and later commits are sent as
 * usual and succeed once the refusals stop, with no new attach (`action` `retry`). The one exception
 * is `too_large`, whose transaction never fits in one append, whatever the layer does: split it into
 * smaller ones (`action` `fix`). A refused attach claimed nothing: attach again later, or with
 * `{ readOnly: true }` to read the database meanwhile.
 *
 * With plain node:sqlite, a refused commit throws SQLite's error with `errcode` 7178
 * (SQLITE_IOERR_AUTH), and `status(file)` names the refusal (`refused_kind`, `refused_reason`). A
 * refused `ursula_attach` throws one with `errcode` 7178 too, or 23 (SQLITE_AUTH) for a `402`.
 */
export class UrsulaWriteRefusedError extends UrsulaError {
	/**
	 * What refused it: for a refused commit its `refused_kind`, for an attach refused with a `402`
	 * `payment_required`, and for any other failed attach the extension's kind (`refused`,
	 * `unreachable`, `unavailable` or `unauthorized`), whose message names what refused it. `refused`
	 * also stands for a refusal that names nothing more, such as one from an extension before 0.7.3.
	 */
	declare readonly kind: RefusedKind;
	/** Why (`refused_reason`), for a refused commit: the layer's explanation, for `too_large` the frame's size and the limit, and for `unreachable` the last attempt's failure (such as `timeout: resolve`). `null` when the layer sent no explanation, and for a failed attach, whose message carries it. */
	readonly reason: string | null;
	constructor(message: string, kind: RefusedKind | null, reason: string | null, cause: unknown) {
		super(message, kind ?? "refused", refusedAction(kind), cause);
		this.name = "UrsulaWriteRefusedError";
		this.reason = reason;
	}
}

/**
 * A write refused with `402 Payment Required` by a layer in front of the stream, such as an authorizer
 * or a quota or billing service: the {@link UrsulaWriteRefusedError} of kind `payment_required`.
 * Nothing of it reached the stream, and the file stays usable. Its `reason` is the layer's explanation
 * for a refused commit, as `payment_reason` reports it (`null` when the layer sent none). It is always
 * `null` for a refused attach, whose message carries it.
 */
export class UrsulaPaymentRequiredError extends UrsulaWriteRefusedError {
	constructor(message: string, reason: string | null, cause: unknown) {
		super(message, "payment_required", reason, cause);
		this.name = "UrsulaPaymentRequiredError";
	}
}

/**
 * A commit the VFS could not replicate, which poisoned the file: fenced (by a newer owner, because the
 * stream was deleted and recreated, or by another writer's append with a higher `Stream-Seq`),
 * rejected, or with no answer in time. Every later commit fails until the file is attached again.
 * Also a read the VFS refused once the file is fenced (`fenced` is `true`, and the message starts with
 * `read refused`): the file no longer serves the state the new owner moved past.
 *
 * Its subclasses say what to do: {@link UrsulaFencedError} (`stop`) and {@link UrsulaReattachError}
 * (`reattach`). A poison whose cause must be fixed before the file is attached again (kind
 * `unsupported`, `stream_state` or `local_setup`) is an UrsulaReplicationError with `action` `fix`.
 */
export class UrsulaReplicationError extends UrsulaError {
	declare readonly kind: "fenced" | ReattachKind | SetupKind;
	readonly fenced: boolean;
	/** `kind` and `action` are those of a fence when `fenced`, and of an unknown outcome otherwise, unless given. */
	constructor(message: string, fenced: boolean, cause: unknown, kind: "fenced" | ReattachKind | SetupKind = fenced ? "fenced" : "outcome_unknown", action?: UrsulaErrorAction) {
		super(message, kind, action ?? ACTIONS.get(kind) ?? "reattach", cause);
		this.name = "UrsulaReplicationError";
		this.fenced = fenced;
	}
}

/**
 * Another owner took the stream over: a newer owner claimed it, it was deleted and recreated, or a
 * writer outside the protocol appended to it. Commits fail, and so do new reads, whose message starts
 * with `read refused`. Stop using the database (`action` `stop`). Opening it again takes the stream
 * back from that owner, which only a deliberate takeover does: exit and let the scheduler restart the
 * process, and never reopen in a loop.
 */
export class UrsulaFencedError extends UrsulaReplicationError {
	declare readonly kind: "fenced";
	constructor(message: string, cause: unknown) {
		super(message, true, cause, "fenced", "stop");
		this.name = "UrsulaFencedError";
	}
}

/**
 * The local file may no longer match the stream: a commit whose outcome is unknown (no answer within
 * the retry budget, or an answer that does not prove nothing was applied, so the stream may or may
 * not hold it), a local file damaged or failing, or a failure inside the extension. Close the database
 * and open it again (`action` `reattach`): the new attach replays what the stream holds. An attach
 * that failed after its claim, which may already have fenced the earlier owner, is one too.
 */
export class UrsulaReattachError extends UrsulaReplicationError {
	declare readonly kind: ReattachKind;
	constructor(message: string, kind: ReattachKind, cause: unknown) {
		super(message, false, cause, kind, "reattach");
		this.name = "UrsulaReattachError";
	}
}

/**
 * The request cannot succeed as made (`action` `fix`): a stream URL that does not parse or names a
 * bucket or stream the server refuses, a missing stream, a file in use or holding another database, a
 * local directory that refuses the files. Its message says what to change. No message tells you to
 * delete a file that may hold data.
 */
export class UrsulaSetupError extends UrsulaError {
	declare readonly kind: SetupKind;
	constructor(message: string, kind: SetupKind, cause: unknown) {
		super(message, kind, "fix", cause);
		this.name = "UrsulaSetupError";
	}
}

/** What to do, said in words, for the errors built from `status(file)`, whose own message (SQLite's) says nothing. */
function advice(kind: string, action: UrsulaErrorAction): string {
	switch (action) {
		case "retry":
			return "Nothing of it reached the stream, and the database stays usable: run it again later.";
		case "reattach":
			return "Close the database and open it again, which replays the stream.";
		case "stop":
			return "Another owner took the stream over: stop using this database, and never reopen it in a loop.";
		case "fix":
			return kind === "too_large" ? "The same transaction never fits in one append: split it into smaller ones." : "Fix the cause, then open the database again.";
	}
}

/** The fields of `status(file)` that name a refused commit's refusal ({@link refusedCommitError}). */
export interface CommitRefusalStatus {
	readonly payment_required: boolean;
	readonly payment_reason: string | null;
	readonly refused_kind?: WriteRefusedKind | null;
	readonly refused_reason?: string | null;
}

/** The fields of `status(file)` that {@link classify} reads. */
export interface FailureStatus extends CommitRefusalStatus {
	readonly poisoned: boolean;
	readonly fenced: boolean;
	readonly reason: string | null;
	/** Absent with an extension before 0.7.5. */
	readonly error?: { readonly kind: string; readonly action: UrsulaErrorAction; readonly message: string } | null;
}

/**
 * The error for a commit that failed with SQLITE_IOERR_AUTH, from `status(file)` right after it.
 * `refused_kind` names that commit's refusal: a transaction refused as `too_large` while a layer still
 * refuses with `402` is not the `402`'s. An extension before 0.7.3 reports no kind, and refuses with
 * `402` alone.
 */
export function refusedCommitError(s: CommitRefusalStatus, cause: unknown): UrsulaWriteRefusedError {
	if ((s.refused_kind ?? (s.payment_required ? "payment_required" : null)) === "payment_required") {
		const why = s.payment_reason === null ? "" : `: ${s.payment_reason}`;
		return new UrsulaPaymentRequiredError(`commit refused with 402 Payment Required in front of the stream${why}. ${advice("payment_required", "retry")}`, s.payment_reason, cause);
	}
	const kind = s.refused_kind ?? null;
	const reason = s.refused_reason ?? null;
	const why = `${kind === null ? "" : `: ${kind}`}${reason === null ? "" : ` (${reason})`}`;
	return new UrsulaWriteRefusedError(`commit refused before it reached the stream${why}. ${advice(kind ?? "refused", refusedAction(kind))}`, kind, reason, cause);
}

/** The error for an I/O error `cause` on a poisoned file (`s`): a read refused once fenced, or a commit not replicated. */
function poisonError(s: FailureStatus, cause: unknown): UrsulaReplicationError {
	const reason = s.reason ?? "unknown";
	const read = errcode(cause) === SQLITE_IOERR_READ;
	if (s.fenced) return new UrsulaFencedError(`${read ? "read refused, the file is fenced" : "commit not replicated"}: ${reason}. ${advice("fenced", "stop")}`, cause);
	// An extension before 0.7.5 names no kind: a poison that is no fence is an unknown outcome.
	const kind = (s.error?.kind ?? "outcome_unknown") as ReattachKind | SetupKind;
	const action = s.error?.action ?? "reattach";
	const message = `${read ? "read failed, the file is poisoned" : "commit not replicated"}: ${reason}. ${advice(kind, action)}`;
	return action === "reattach" ? new UrsulaReattachError(message, kind as ReattachKind, cause) : new UrsulaReplicationError(message, false, cause, kind, action);
}

/** An error of an Ursula SQL function names its kind: `<function>: <kind>: <message>`, and `<function>: attach_failed: <kind>: <message>` for a path whose last attach failed. */
const FUNCTION_ERROR = /^ursula_(?:attach|status|stats|set_token): (?:attach_failed: )?([a-z_]+): /;

/** The error for `error` when it is an Ursula SQL function's, `undefined` otherwise. */
function functionError(error: unknown): UrsulaError | undefined {
	const code = errcode(error);
	if (code === undefined || !(error instanceof Error)) return undefined;
	const kind = FUNCTION_ERROR.exec(error.message)?.[1];
	const action = kind === undefined ? undefined : ACTIONS.get(kind);
	if (kind === undefined || action === undefined) {
		// Before 0.7.5 the message names no kind, and only the code tells an attach refused before
		// anything reached the stream: SQLITE_AUTH for a 402, SQLITE_IOERR_AUTH otherwise.
		if (!error.message.startsWith("ursula_attach: ")) return undefined;
		if (code === SQLITE_AUTH) return new UrsulaPaymentRequiredError(error.message, null, error);
		if (code === SQLITE_IOERR_AUTH) return new UrsulaWriteRefusedError(error.message, null, null, error);
		return undefined;
	}
	if (code === SQLITE_AUTH) return new UrsulaPaymentRequiredError(error.message, null, error);
	if (code === SQLITE_IOERR_AUTH || action === "retry" || kind === "too_large") return new UrsulaWriteRefusedError(error.message, kind as RefusedKind, null, error);
	if (action === "stop") return new UrsulaFencedError(error.message, error);
	if (action === "reattach") return new UrsulaReattachError(error.message, kind as ReattachKind, error);
	return new UrsulaSetupError(error.message, kind as SetupKind, error);
}

/**
 * What to throw for `error`, which a statement, a transaction, an attach or another Ursula SQL
 * function threw: an {@link UrsulaError} when the extension failed it, and `error` itself, unchanged,
 * otherwise (a constraint violation, a syntax error, a write to a file attached read-only). An Ursula
 * SQL function's error names its kind. A failure of a statement on an attached file surfaces as one
 * of SQLite's I/O errors, which says nothing: then, and only then, `status` reads the file's status,
 * which names it. A status that throws stands for a file not attached (any more): `error` itself.
 */
export function classify(error: unknown, status?: () => FailureStatus): unknown {
	const named = functionError(error);
	if (named !== undefined) return named;
	const code = errcode(error);
	if (code === undefined || status === undefined) return error;
	const primary = code & 0xff;
	if (primary !== SQLITE_IOERR && primary !== SQLITE_FULL) return error;
	let s: FailureStatus;
	try {
		s = status();
	} catch {
		return error;
	}
	// Poisoned (by this commit, by an earlier one, or by the snapshot thread since a refusal): the file
	// needs a new attach whatever the error, a refusal's code included.
	if (s.poisoned) return poisonError(s, error);
	if (code === SQLITE_IOERR_AUTH) return refusedCommitError(s, error);
	return error;
}
