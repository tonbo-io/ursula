// The errors the package throws, the SQLite result codes they map from, and how a refused commit
// maps to one. Shared by the root entry (index.ts) and the Pi adapter (pi.ts); only the error classes
// and `WriteRefusedKind` are public.

/** SQLITE_READONLY: a write to a file attached read-only. */
export const SQLITE_READONLY = 8;
/** SQLITE_AUTH: `ursula_attach` refused with 402 in front of the stream. */
export const SQLITE_AUTH = 23;
/** SQLITE_IOERR_AUTH: a commit, or the claim of an `ursula_attach`, refused before it reached the stream (an attach refused with 402 gets SQLITE_AUTH). */
export const SQLITE_IOERR_AUTH = 7178;
/** SQLITE_IOERR, the primary code (`errcode & 0xff`) of every I/O error: a read of a fenced file fails with one (SQLITE_IOERR_READ). */
export const SQLITE_IOERR = 10;
/** SQLITE_IOERR_READ: a read transaction refused on a fenced file, before it started. */
export const SQLITE_IOERR_READ = 266;

/** The SQLite result code of an error node:sqlite threw (`errcode`), if it is one. */
export function errcode(error: unknown): number | undefined {
	const code = (error as { errcode?: unknown } | null | undefined)?.errcode;
	return typeof code === "number" ? code : undefined;
}

/**
 * A commit the VFS could not replicate: fenced (by a newer owner, because the stream was deleted and
 * recreated, or by another writer's append with a higher `Stream-Seq`), rejected, or with no answer in
 * time. Also a read the VFS refused once the file is fenced (`fenced` is `true`, and the message
 * starts with `read refused`): the file no longer serves the state the new owner moved past.
 */
export class UrsulaReplicationError extends Error {
	readonly fenced: boolean;
	constructor(message: string, fenced: boolean, cause: unknown) {
		super(message, { cause });
		this.name = "UrsulaReplicationError";
		this.fenced = fenced;
	}
}

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

/**
 * A write refused before it reached the stream, so that nothing of it was applied: by a layer in front
 * of the stream (an authorizer, a quota or billing service, a gateway or a proxy), by the server
 * before it proposed the write (under write pressure, or while it knows no leader for the stream),
 * or by the extension itself, which never sends a transaction over its frame limit (kind
 * `too_large`: only a smaller transaction succeeds), or a write that never went out because the
 * stream's endpoint could not be reached (kind `unreachable`). Unlike an {@link UrsulaReplicationError}, it
 * leaves the file usable. A refused commit was rolled back, but the file is not poisoned: reads go on
 * from the local file, and later commits are sent as usual and succeed once the refusals stop, with
 * no new attach. A refused attach claimed nothing: attach again later, or with `{ readOnly: true }`
 * to read the database meanwhile.
 *
 * With plain node:sqlite, a refused commit throws SQLite's error with `errcode` 7178
 * (SQLITE_IOERR_AUTH), and `status(file)` names the refusal (`refused_kind`, `refused_reason`). A
 * refused `ursula_attach` throws one with `errcode` 7178 too, or 23 (SQLITE_AUTH) for a `402`;
 * `attach` turns them into this error and {@link UrsulaPaymentRequiredError}.
 */
export class UrsulaWriteRefusedError extends Error {
	/** What refused it, for a refused commit. `null` for an attach refused other than with a `402`, whose message names it. */
	readonly kind: WriteRefusedKind | null;
	/** Why (`refused_reason`), for a refused commit: the layer's explanation, for `too_large` the frame's size and the limit, and for `unreachable` the last attempt's failure (such as `timeout: resolve`). `null` when the layer sent no explanation, and for a refused attach, whose message carries it. */
	readonly reason: string | null;
	constructor(message: string, kind: WriteRefusedKind | null, reason: string | null, cause: unknown) {
		super(message, { cause });
		this.name = "UrsulaWriteRefusedError";
		this.kind = kind;
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

/** The fields of `status(file)` that name a refused commit's refusal ({@link refusedCommitError}). */
export interface CommitRefusalStatus {
	readonly payment_required: boolean;
	readonly payment_reason: string | null;
	readonly refused_kind?: WriteRefusedKind | null;
	readonly refused_reason?: string | null;
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
		return new UrsulaPaymentRequiredError(`commit refused with 402 Payment Required in front of the stream${why}`, s.payment_reason, cause);
	}
	const kind = s.refused_kind ?? null;
	const reason = s.refused_reason ?? null;
	const why = `${kind === null ? "" : `: ${kind}`}${reason === null ? "" : ` (${reason})`}`;
	return new UrsulaWriteRefusedError(`commit refused before it reached the stream${why}`, kind, reason, cause);
}
