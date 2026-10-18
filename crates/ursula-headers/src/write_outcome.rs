//! What one answer says about a write: [`classify`], the single reading of
//! every write answer the node and the gateway send, for every client, such
//! as the SQLite VFS or a gateway's embedder.
//!
//! A write is every request that changes what a stream or a bucket holds
//! ([`WriteKind`]): an append or a close (with or without producer headers),
//! a create, a snapshot publish, a parts commit, a part staged, a discard of
//! the staged parts (`DELETE {stream}/snapshot/parts`), a retention advance
//! (`PUT {stream}/retention/{offset}`), a delete of a stream, and a create of
//! a bucket (`PUT /{bucket}`). Its answer is one of:
//!
//! - [`WriteOutcome::Applied`]: the write holds, and the answer does not say
//!   that an earlier request applied it. A keyless write, a create, a
//!   snapshot publish, a parts commit, a discard, a retention advance and a
//!   create of a bucket answer a repeat as they answer the first apply, so
//!   for them `Applied` means only that the write holds.
//! - [`WriteOutcome::Repeat`]: a typed producer receipt for a write that an
//!   earlier request with the same key applied: a `204` receipt, or
//!   `receipt-evicted`. A close-only producer write answers `204` even when
//!   it applies now, so its receipt reads `Repeat` either way. Both
//!   `Applied` and `Repeat` hold.
//! - [`WriteOutcome::Refused`]: this request applied nothing of the write
//!   and never will. Either the node refused it before proposing it, or it
//!   was proposed and its apply refused it deterministically. The code
//!   names why. It speaks for this request only. Whether an earlier attempt
//!   under the same key applied is answered by a receipt, or by the producer
//!   answer of a `403` (`Producer-Closed-Seq` of `epoch-closed`), never by a
//!   refusal of load or routing.
//! - [`WriteOutcome::Redirected`]: a `307` or `308`. This target applied
//!   nothing and names another. A node redirects a write only when it
//!   refused it before proposing it (extensions Section 5.7), and a gateway
//!   follows every redirect of a node, so a client of a gateway never gets
//!   one.
//! - [`WriteOutcome::Unknown`]: the write may have been proposed and its
//!   apply result was not observed, so it may or may not have applied, or
//!   may still apply. The client repeats the same keyed write (extensions
//!   Section 5.6). A request that got no answer at all, a transport failure,
//!   is [`WriteOutcome::NO_ANSWER`], which is `Unknown`.
//!
//! `NO_LEADER` and `OUTCOME_UNKNOWN` follow from these definitions. A node
//! answers `NO_LEADER` only for a write nothing of which applied: one it
//! refused before proposing it (before reading the body, a routing answer,
//! or after reading it when its own leadership check failed first), or one
//! an apply refused deterministically before the node lost the leadership.
//! A node that lost the leadership after it proposed the write and before it
//! observed the apply answers `OUTCOME_UNKNOWN`, the code of every answer
//! whose write may have applied. So `NO_LEADER` proves a refusal and
//! `OUTCOME_UNKNOWN` never does, wherever in the request each was sent.
//!
//! The rules, from the status first. One rule decides an answer without a
//! recognized Ursula error body: it is `Unknown` unless its status alone
//! proves an outcome. A `2xx` proves an apply, but for a producer write only
//! with its receipt. A `307` or `308` proves a redirect. A `4xx` other than
//! `408` proves a refusal, because extensions Section 11.1 requires every
//! layer in front of Ursula to send a `4xx` only before it forwards any of
//! the request. No other status proves anything: a `408`, a `5xx` or an
//! unexpected `1xx` or `3xx` may come from a load balancer or a proxy after
//! it forwarded some or all of the request.
//!
//! - `2xx` to a producer write ([`WriteKind::Producer`]): `Applied` for a
//!   `200` receipt, `Repeat` for a `204` receipt or `receipt-evicted`, and
//!   `Unknown` without a receipt.
//! - `2xx` to any other write: `Applied` with the typed answer Ursula always
//!   sends for that write, else `Unknown`. A keyless append or close carries
//!   `Stream-Next-Offset`, a create and a discard `Stream-Incarnation`, a
//!   snapshot publish or a parts commit the published snapshot, a part
//!   `Stream-Snapshot-Part`, and a retention advance
//!   `Stream-Retained-Offset` with `Stream-Incarnation`. Only a layer outside
//!   Ursula, such as a misrouting proxy, sends a success without it, and
//!   that says nothing of the write. A delete of a stream and a create of a
//!   bucket carry no typed answer, so their `2xx` holds by its status.
//! - `307`, `308`: `Redirected`. Every other `1xx` and `3xx`: `Unknown`.
//! - `4xx` but `408`: `Refused`. The code is the producer answer's when the
//!   answer carries one, else the error body's, else `INVALID_REQUEST` (the
//!   code of a `4xx` that no other code names, extensions Section 11). A
//!   `403` keeps a producer code only with the producer headers that the
//!   code needs: a `403` without `Producer-Epoch` is an access layer's, and
//!   a producer code in its body reads `INVALID_REQUEST`.
//! - `408` and every `5xx`: `Refused` only when the code proves that the
//!   write applied nothing ([`proves_refusal`]), else `Unknown`. A `5xx`
//!   without a body, or with a code this version does not know, is
//!   `Unknown`.
//!
//! A gateway's own refusals (extensions Section 11) are told from a node's
//! producer `403` by the producer headers: every producer `403` carries
//! `Producer-Epoch` and one `Producer-Outcome`, and a gateway's answer
//! carries neither, nor `Ursula-Protocol` or `Ursula-Cluster-Id`. Both are
//! `Refused`, each with its own code.
//!
//! # The inventory
//!
//! Every answer a write can get, one row per decision. A row stands for
//! every code its status shares a decision with: the `400` row covers every
//! typed `400`, for one. With the `test-support` feature, the `inventory`
//! module holds the rows as data. Tests drive each row through a real node router and a
//! real gateway and check that [`classify`] reads it as the row says.
//!
//! | Id | From | Status | Code or shape | Outcome |
//! | --- | --- | --- | --- | --- |
//! | `append-applied` | node | `200` | producer receipt | `Applied` |
//! | `append-repeat` | node | `204` | producer receipt | `Repeat` |
//! | `append-receipt-evicted` | node | `204` | `receipt-evicted` | `Repeat` |
//! | `append-keyless` | node | `204` | `Stream-Next-Offset` | `Applied` |
//! | `close-keyless` | node | `204` | `Stream-Next-Offset`, `Stream-Closed` | `Applied` |
//! | `create-new` | node | `201` | `Stream-Incarnation` | `Applied` |
//! | `create-matching` | node | `200` | `Stream-Incarnation` | `Applied` |
//! | `snapshot-published` | node | `204` | the snapshot (new or repeated) | `Applied` |
//! | `part-staged` | node | `201` | `Stream-Snapshot-Part` | `Applied` |
//! | `parts-discarded` | node | `204` | `Stream-Incarnation` | `Applied` |
//! | `retention-advanced` | node | `204` | `Stream-Retained-Offset`, `Stream-Incarnation` | `Applied` |
//! | `stream-deleted` | node | `204` | none: holds by its status | `Applied` |
//! | `bucket-created` | node | `201` | none: holds by its status | `Applied` |
//! | `parts-commit-published` | node | `204` | the snapshot (new or repeated) | `Applied` |
//! | `redirect` | node | `307` | the leader (or, from a node without a replica, the group) | `Redirected` |
//! | `producer-epoch-closed` | node | `403` | `PRODUCER_EPOCH_CLOSED`, `epoch-closed;by=claim` or `by=history` | `Refused` |
//! | `producer-stale-epoch` | node | `403` | `PRODUCER_EPOCH_STALE` | `Refused` |
//! | `producer-key-conflict` | node | `403` | `PRODUCER_KEY_CONFLICT` | `Refused` |
//! | `producer-expired` | node | `403` | `PRODUCER_EXPIRED` | `Refused` |
//! | `producer-seq-gap` | node | `409` | `PRODUCER_SEQ_CONFLICT` | `Refused` |
//! | `producer-epoch-not-claimed` | node | `400` | `INVALID_PRODUCER`, `Producer-Expected-Seq: 0` | `Refused` |
//! | `bad-request` | node | `400` | every typed `400`, such as `INVALID_HEADER`, `EMPTY_APPEND`, `PRODUCER_DIGEST_MISMATCH`, `UNKNOWN_PART`, `SNAPSHOT_DIGEST_MISMATCH` | `Refused` |
//! | `not-found` | node | `404` | `STREAM_NOT_FOUND`, `BUCKET_NOT_FOUND`, `SNAPSHOT_NOT_FOUND`, `NOT_FOUND` | `Refused` |
//! | `stream-closed` | node | `409` | `STREAM_CLOSED`, `Stream-Closed: true` | `Refused` |
//! | `stream-seq-conflict` | node | `409` | `STREAM_SEQ_CONFLICT`, `Stream-Last-Seq` | `Refused` |
//! | `snapshot-conflict` | node | `409` | `SNAPSHOT_CONFLICT`, the latest snapshot | `Refused` |
//! | `conflict` | node | `409` | `CONTENT_TYPE_MISMATCH`, `STREAM_ALREADY_EXISTS_CONFLICT` | `Refused` |
//! | `gone` | node | `410` | `STREAM_GONE` | `Refused` |
//! | `incarnation-mismatch` | node | `412` | `INCARNATION_MISMATCH` | `Refused` |
//! | `too-large` | node | `413` | `REQUEST_TOO_LARGE` or `SNAPSHOT_TOO_LARGE`, the limit (through a gateway, the gateway's own `413`, which fires first) | `Refused` |
//! | `producer-limit` | node | `429` | `PRODUCER_LIMIT` | `Refused` |
//! | `staged-parts-limit` | node | `429` | `STAGED_PARTS_LIMIT`, `Ursula-Max-Staged-Parts` or `Ursula-Max-Staged-Bytes` | `Refused` |
//! | `no-cold-store` | node | `501` | `NO_COLD_STORE` | `Refused` |
//! | `no-leader-before-body` | node | `503` | `NO_LEADER`, `x-ursula-raft-group` | `Refused` |
//! | `no-leader-before-proposal` | node | `503` | `NO_LEADER` after the body, before any proposal | `Refused` |
//! | `leadership-lost-after-proposal` | node | `503` | `OUTCOME_UNKNOWN` | `Unknown` |
//! | `outcome-unknown` | node | `503` | `OUTCOME_UNKNOWN`, a response lost after submission | `Unknown` |
//! | `wal-disk-pressure` | node | `503` | `WAL_DISK_PRESSURE` | `Refused` |
//! | `raft-log-pressure` | node | `503` | `RAFT_LOG_PRESSURE` | `Refused` |
//! | `ingress-body-budget` | node | `503` | `INGRESS_BODY_BYTES_LIMIT_REACHED` | `Refused` |
//! | `cold-backpressure` | node | `503` | `COLD_BACKPRESSURE` | `Refused` |
//! | `raft-uncommitted-backpressure` | node | `503` | `RAFT_UNCOMMITTED_BACKPRESSURE` | `Refused` |
//! | `history-pending` | node | `503` | `HISTORY_PENDING`: before the proposal, or a deterministic refusal at apply | `Refused` |
//! | `apply-stopped` | node | `503` | `APPLY_STOPPED` | `Refused` |
//! | `group-restarting` | node | `503` | `GROUP_RESTARTING` | `Refused` |
//! | `json-boundary-unverified` | node | `503` | `JSON_BOUNDARY_UNVERIFIED`: a deterministic refusal at apply | `Refused` |
//! | `snapshot-commits-busy` | node | `503` | `SNAPSHOT_COMMITS_BUSY` | `Refused` |
//! | `snapshot-commit-in-progress` | node | `503` | `SNAPSHOT_COMMIT_IN_PROGRESS`: an earlier attempt of the commit, another commit or a discard of the stream runs | `Unknown` |
//! | `internal` | node | `500` | `INTERNAL`, `CREATED_AT_OUT_OF_RANGE`, `HISTORY_MISMATCH` | `Unknown` |
//! | `cold-store-failed` | node | `502` | `COLD_STORE_FAILED` | `Unknown` |
//! | `gateway-invalid-credential` | gateway | `401` | `INVALID_CREDENTIAL`, `CREDENTIAL_EXPIRED` | `Refused` |
//! | `gateway-forbidden` | gateway | `403` | `FORBIDDEN`, no producer header | `Refused` |
//! | `gateway-concealed` | gateway | `404` | `BUCKET_NOT_FOUND` | `Refused` |
//! | `gateway-too-large` | gateway | `413` | `REQUEST_TOO_LARGE`, the gateway's limit | `Refused` |
//! | `gateway-rate-limited` | gateway | `429` | `RATE_LIMITED` | `Refused` |
//! | `gateway-uploads-exhausted` | gateway | `429` | `UPLOADS_EXHAUSTED`: the bucket's concurrent large bodies | `Refused` |
//! | `gateway-upload-bytes-exhausted` | gateway | `429` | `UPLOAD_BYTES_EXHAUSTED`: the bucket's large-body bytes | `Refused` |
//! | `gateway-upload-budget-exhausted` | gateway | `503` | `UPLOAD_BUDGET_EXHAUSTED`: the gateway's body budget | `Refused` |
//! | `gateway-body-too-slow` | gateway | `408` | `BODY_TOO_SLOW`: cut before any of it was forwarded | `Refused` |
//! | `gateway-authentication-unavailable` | gateway | `503` | `AUTHENTICATION_UNAVAILABLE` | `Refused` |
//! | `gateway-authorization-unavailable` | gateway | `503` | `AUTHORIZATION_UNAVAILABLE` | `Refused` |
//! | `gateway-no-upstream` | gateway | `503` | `NO_UPSTREAM` | `Refused` |
//! | `gateway-upstream-unreachable` | gateway | `503` | `UPSTREAM_UNREACHABLE`: no connection, nothing sent | `Refused` |
//! | `gateway-no-leader` | gateway | `503` | `NO_LEADER`: every upstream redirected | `Refused` |
//! | `gateway-upstream-failed` | gateway | `502` | `UPSTREAM_FAILED` | `Unknown` |
//! | `gateway-upstream-timeout` | gateway | `504` | `UPSTREAM_TIMEOUT` | `Unknown` |
//! | `embedder-payment-required` | embedder | `402` | `PAYMENT_REQUIRED` | `Refused` |
//! | `embedder-account-revoked` | embedder | `403` | `ACCOUNT_REVOKED` | `Refused` |
//! | `embedder-forbidden` | embedder | `403` | `FORBIDDEN` | `Refused` |
//! | `embedder-rate-limited` | embedder | `429` | `RATE_LIMITED` | `Refused` |
//! | `foreign-4xx` | another layer | `4xx` | no error body | `Refused` (`INVALID_REQUEST`) |
//! | `foreign-408` | another layer | `408` | no Ursula error code | `Unknown` |
//! | `foreign-5xx` | another layer | `5xx` | no error body | `Unknown` |
//! | `keyed-2xx-without-receipt` | another layer | `2xx` | a producer write's success without a receipt | `Unknown` |
//! | `keyless-2xx-untyped` | another layer | `2xx` | another write's success without its typed answer | `Unknown` |
//! | `transport-failure` | none | none | no answer, mid-upload included | `Unknown` (`WriteOutcome::NO_ANSWER`) |
//!
//! A node's answer reaches a client of a gateway unchanged, but for a
//! redirect, which the gateway follows and answers `gateway-no-leader` when
//! every upstream it tried redirected, and a body over the node's request
//! limit, which the gateway refuses first, since its own limit is at most
//! [`MAX_REQUEST_BYTES`](crate::limit::MAX_REQUEST_BYTES). The `foreign-*`
//! and `keyed-2xx-without-receipt` rows come from a layer outside Ursula,
//! such as a proxy, and `transport-failure` has no answer: unit tests cover
//! them.

use crate::answer::Delivery;
use crate::answer::Outcome;
use crate::answer::ProducerAnswer;
use crate::error_body::ErrorBody;
use crate::error_body::ErrorCode;
use crate::fields;
use crate::names;
use crate::snapshot::PartStaged;
use crate::snapshot::SnapshotPublishAnswer;

#[cfg(any(test, feature = "test-support"))]
pub mod inventory;

/// What one answer says about a write. See the module doc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOutcome {
    /// The write holds, and the answer does not say an earlier request
    /// applied it.
    Applied,
    /// A typed receipt for a write an earlier request with this key applied.
    /// A close-only producer write answers `204` even when this request
    /// applied it, so its first receipt reads `Repeat` too: both hold.
    Repeat,
    /// This request applied nothing of the write and never will.
    Refused(ErrorCode),
    /// A `307` or `308`: this target applied nothing and names another.
    Redirected,
    /// The write may have been proposed and its apply result was not
    /// observed: repeat it with its key.
    Unknown,
}

impl WriteOutcome {
    /// The outcome of a request that got no answer: a transport failure.
    pub const NO_ANSWER: Self = Self::Unknown;

    /// The write holds: `Applied` or `Repeat`.
    pub fn holds(&self) -> bool {
        matches!(self, Self::Applied | Self::Repeat)
    }

    /// The answer settles this request: anything but `Unknown`.
    pub fn is_definite(&self) -> bool {
        !matches!(self, Self::Unknown)
    }
}

/// What a write is, which decides the typed answer its `2xx` carries. A
/// later 0.8 release may add a kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WriteKind {
    /// An append or a close with `Producer-Id`, `Producer-Epoch` and
    /// `Producer-Seq`: its receipt.
    Producer,
    /// An append or a close without producer headers: `Stream-Next-Offset`.
    Append,
    /// A create: `Stream-Incarnation`.
    Create,
    /// A snapshot publish or a parts commit: the published snapshot.
    Publish,
    /// A part staged: `Stream-Snapshot-Part`.
    PartStage,
    /// A discard of the staged parts: `Stream-Incarnation`.
    PartsDiscard,
    /// A retention advance: `Stream-Retained-Offset` and
    /// `Stream-Incarnation`.
    Retention,
    /// A delete of a stream: no typed answer, so its `2xx` holds by its
    /// status.
    DeleteStream,
    /// A create of a bucket: no typed answer, so its `2xx` holds by its
    /// status.
    CreateBucket,
}

/// Reads the answer to a write of `kind` from its status, its
/// `(name, value)` header pairs and its body (an error body, or anything: a
/// body that is no error body names no code). The caller bounds the body it
/// reads. See the module doc for the rules.
#[must_use]
pub fn classify<'a, I>(kind: WriteKind, status: u16, headers: I, body: &[u8]) -> WriteOutcome
where I: IntoIterator<Item = (&'a str, &'a [u8])> {
    match status {
        200..=299 => success(kind, status, headers),
        307 | 308 => WriteOutcome::Redirected,
        408 => proven(status, body),
        400..=499 => {
            let producer = ProducerAnswer::parse(status, headers)
                .ok()
                .flatten()
                .and_then(|answer| answer.error_code());
            let code = producer.or_else(|| {
                body_code(body).filter(|code| status != 403 || !Outcome::is_refusal_code(code))
            });
            WriteOutcome::Refused(code.unwrap_or(ErrorCode::InvalidRequest))
        }
        _ => proven(status, body),
    }
}

/// A `2xx`: what its typed answer says, or `Unknown` without one, which only
/// a layer outside Ursula sends.
fn success<'a, I>(kind: WriteKind, status: u16, headers: I) -> WriteOutcome
where I: IntoIterator<Item = (&'a str, &'a [u8])> {
    let carried = match kind {
        WriteKind::Producer => {
            return match ProducerAnswer::parse(status, headers) {
                Ok(Some(ProducerAnswer::Receipt {
                    delivery: Delivery::Applied,
                    ..
                })) => WriteOutcome::Applied,
                Ok(Some(
                    ProducerAnswer::Receipt {
                        delivery: Delivery::Repeat,
                        ..
                    }
                    | ProducerAnswer::ReceiptEvicted { .. },
                )) => WriteOutcome::Repeat,
                // No receipt, or a malformed one: not Ursula's answer.
                _ => WriteOutcome::Unknown,
            };
        }
        WriteKind::Append => {
            let Ok([next]) = fields::collect(headers, [names::STREAM_NEXT_OFFSET]) else {
                return WriteOutcome::Unknown;
            };
            matches!(fields::offset(names::STREAM_NEXT_OFFSET, next), Ok(Some(_)))
        }
        WriteKind::Create | WriteKind::PartsDiscard => {
            let Ok([token]) = fields::collect(headers, [names::STREAM_INCARNATION]) else {
                return WriteOutcome::Unknown;
            };
            matches!(
                fields::incarnation(names::STREAM_INCARNATION, token),
                Ok(Some(_))
            )
        }
        WriteKind::Publish => matches!(
            SnapshotPublishAnswer::parse(status, headers),
            Ok(Some(SnapshotPublishAnswer::Published { .. }))
        ),
        WriteKind::PartStage => PartStaged::parse(headers).is_ok(),
        WriteKind::Retention => {
            let Ok([retained, token]) = fields::collect(headers, [
                names::STREAM_RETAINED_OFFSET,
                names::STREAM_INCARNATION,
            ]) else {
                return WriteOutcome::Unknown;
            };
            matches!(
                fields::offset(names::STREAM_RETAINED_OFFSET, retained),
                Ok(Some(_))
            ) && matches!(
                fields::incarnation(names::STREAM_INCARNATION, token),
                Ok(Some(_))
            )
        }
        WriteKind::DeleteStream | WriteKind::CreateBucket => true,
    };
    if carried {
        WriteOutcome::Applied
    } else {
        WriteOutcome::Unknown
    }
}

/// `Refused` when the body's code proves a refusal at `status`, else
/// `Unknown`.
fn proven(status: u16, body: &[u8]) -> WriteOutcome {
    match body_code(body) {
        Some(code) if proves_refusal(status, &code) => WriteOutcome::Refused(code),
        _ => WriteOutcome::Unknown,
    }
}

fn body_code(body: &[u8]) -> Option<ErrorCode> {
    ErrorBody::parse(body).ok().map(|body| body.code)
}

/// Whether `code`, at `status`, proves that the write applied nothing and
/// never will. Only a `408` or a `5xx` needs the proof: every other `4xx` is
/// a refusal by its status. The proof is the code, never the status: a
/// `503` is sent before a write is proposed and after it, so only its code
/// tells which.
///
/// The match names every code, so a new code does not compile until it is
/// decided here. A code proves a refusal when every sender of it sends it
/// only when nothing of the request applied: a check before the proposal
/// (admission and routing before the body, a leadership or pressure check
/// before the proposal, a gateway that never forwarded the request, a parts
/// commit that copied nothing), or a deterministic refusal at apply.
#[must_use]
pub fn proves_refusal(status: u16, code: &ErrorCode) -> bool {
    let proving_status = match code {
        // Routing before the body, or a leadership check before the
        // proposal; a gateway whose every upstream redirected.
        ErrorCode::NoLeader
        // Admission, before the handler.
        | ErrorCode::WalDiskPressure
        | ErrorCode::RaftLogPressure
        | ErrorCode::IngressBodyBytesLimitReached
        // The engine's checks before the proposal.
        | ErrorCode::ColdBackpressure
        | ErrorCode::RaftUncommittedBackpressure
        | ErrorCode::ApplyStopped
        // No engine of the group runs on the node.
        | ErrorCode::GroupRestarting
        // Before the proposal, or a deterministic refusal at apply.
        | ErrorCode::HistoryPending
        // A deterministic refusal at apply: never applied unverified
        // (extensions Section 2.2).
        | ErrorCode::JsonBoundaryUnverified
        // A parts commit refused before it copied.
        | ErrorCode::SnapshotCommitsBusy
        // A gateway that forwarded nothing.
        | ErrorCode::AuthenticationUnavailable
        | ErrorCode::AuthorizationUnavailable
        | ErrorCode::NoUpstream
        | ErrorCode::UpstreamUnreachable
        | ErrorCode::UploadBudgetExhausted => Some(503),
        ErrorCode::NoColdStore => Some(501),
        // A gateway that cut a slow body before forwarding any of it.
        ErrorCode::BodyTooSlow => Some(408),
        // A read's code: no write gets it.
        ErrorCode::LiveReadBackpressure
        // May follow a proposal, or an attempt that still runs.
        | ErrorCode::OutcomeUnknown
        | ErrorCode::SnapshotCommitInProgress
        | ErrorCode::ColdStoreFailed
        | ErrorCode::Internal
        | ErrorCode::CreatedAtOutOfRange
        | ErrorCode::HistoryMismatch
        | ErrorCode::UpstreamTimeout
        | ErrorCode::UpstreamFailed
        | ErrorCode::Unknown(_) => None,
        // `4xx` codes: a refusal by their status, never by a `5xx`.
        ErrorCode::InvalidBucketId
        | ErrorCode::InvalidStreamId
        | ErrorCode::BucketNotFound
        | ErrorCode::BucketErased
        | ErrorCode::StreamNotFound
        | ErrorCode::StreamGone
        | ErrorCode::StreamAlreadyExistsConflict
        | ErrorCode::MissingContentType
        | ErrorCode::ContentTypeMismatch
        | ErrorCode::EmptyAppend
        | ErrorCode::StreamClosed
        | ErrorCode::StreamSeqConflict
        | ErrorCode::InvalidProducer
        | ErrorCode::ProducerEpochClosed
        | ErrorCode::ProducerExpired
        | ErrorCode::ProducerEpochStale
        | ErrorCode::ProducerKeyConflict
        | ErrorCode::ProducerDigestMismatch
        | ErrorCode::ProducerSeqConflict
        | ErrorCode::InvalidRetention
        | ErrorCode::OffsetOutOfRange
        | ErrorCode::RangeNotSatisfiable
        | ErrorCode::InvalidColdFlush
        | ErrorCode::InvalidSnapshot
        | ErrorCode::SnapshotNotFound
        | ErrorCode::SnapshotConflict
        | ErrorCode::InvalidRecordBoundaries
        | ErrorCode::ImportConflict
        | ErrorCode::ImportInvalid
        | ErrorCode::ProducerLimit
        | ErrorCode::IncarnationMismatch
        | ErrorCode::InvalidRequest
        | ErrorCode::InvalidHeader
        | ErrorCode::InvalidQuery
        | ErrorCode::InvalidOffset
        | ErrorCode::QueryNotAccepted
        | ErrorCode::InvalidResource
        | ErrorCode::Unsupported
        | ErrorCode::BodyUnreadable
        | ErrorCode::NotJsonBoundary
        | ErrorCode::NotFound
        | ErrorCode::MethodNotAllowed
        | ErrorCode::RequestTooLarge
        | ErrorCode::SnapshotTooLarge
        | ErrorCode::InvalidPartsCommit
        | ErrorCode::UnknownPart
        | ErrorCode::SnapshotDigestMismatch
        | ErrorCode::InvalidCredential
        | ErrorCode::CredentialExpired
        | ErrorCode::Forbidden
        | ErrorCode::PaymentRequired
        | ErrorCode::AccountRevoked
        | ErrorCode::RateLimited
        | ErrorCode::LiveReadsExhausted
        | ErrorCode::UploadsExhausted
        | ErrorCode::UploadBytesExhausted
        | ErrorCode::StagedPartsLimit => None,
    };
    proving_status == Some(status)
}

#[cfg(test)]
mod tests {
    use super::WriteKind;
    use super::WriteOutcome;
    use super::classify;
    use super::inventory::INVENTORY;
    use super::proves_refusal;
    use crate::error_body::ErrorBody;
    use crate::error_body::ErrorCode;
    use crate::names;

    /// The table test: every row's sample reads as the row says.
    #[test]
    fn every_inventory_row_classifies_as_documented() {
        for row in INVENTORY {
            match row.sample().expect("a sample") {
                Some(sample) => {
                    assert_eq!(sample.classify(), row.outcome, "{}", row.id);
                    if let Some(code) = &row.code {
                        assert_eq!(
                            ErrorBody::parse(&sample.body).map(|body| body.code),
                            Ok(code.clone()),
                            "{}",
                            row.id
                        );
                    }
                }
                None => assert_eq!(WriteOutcome::NO_ANSWER, row.outcome, "{}", row.id),
            }
        }
    }

    /// The module doc's table and [`INVENTORY`] name the same rows, in the
    /// same order.
    #[test]
    fn the_doc_table_lists_every_row() {
        let source = include_str!("write_outcome.rs");
        let documented: Vec<&str> = source
            .lines()
            .filter_map(|line| line.strip_prefix("//! | `"))
            .filter_map(|rest| rest.split('`').next())
            .collect();
        let ids: Vec<&str> = INVENTORY.iter().map(|row| row.id).collect();
        assert_eq!(documented, ids);
    }

    #[test]
    fn ids_are_unique() {
        for (at, row) in INVENTORY.iter().enumerate() {
            assert!(
                INVENTORY
                    .iter()
                    .skip(at + 1)
                    .all(|other| other.id != row.id),
                "{}",
                row.id
            );
        }
    }

    /// A gateway's `403` and a node's producer `403` are both refusals,
    /// each with its own code, and a producer code without the producer
    /// headers is not believed.
    #[test]
    fn a_gateway_403_is_told_from_a_producer_403() {
        let gateway = ErrorBody::fixed(ErrorCode::Forbidden).render();
        assert_eq!(
            classify(WriteKind::Append, 403, [], gateway.as_bytes()),
            WriteOutcome::Refused(ErrorCode::Forbidden)
        );
        let forged = ErrorBody::fixed(ErrorCode::ProducerKeyConflict).render();
        assert_eq!(
            classify(WriteKind::Append, 403, [], forged.as_bytes()),
            WriteOutcome::Refused(ErrorCode::InvalidRequest)
        );
        let producer = [
            (names::PRODUCER_EPOCH, b"1".as_slice()),
            (names::PRODUCER_OUTCOME, b"key-conflict".as_slice()),
        ];
        assert_eq!(
            classify(WriteKind::Producer, 403, producer, gateway.as_bytes()),
            WriteOutcome::Refused(ErrorCode::ProducerKeyConflict)
        );
    }

    /// A producer write's success says something only with its receipt.
    #[test]
    fn a_producer_success_needs_its_receipt() {
        assert_eq!(
            classify(WriteKind::Producer, 200, [], b""),
            WriteOutcome::Unknown
        );
        assert_eq!(
            classify(WriteKind::Producer, 204, [], b""),
            WriteOutcome::Unknown
        );
        let malformed = [(crate::names::PRODUCER_EPOCH, b"1".as_slice())];
        assert_eq!(
            classify(WriteKind::Producer, 200, malformed, b""),
            WriteOutcome::Unknown
        );
    }

    /// A delete of a stream and a create of a bucket carry no typed answer:
    /// their success holds by its status.
    #[test]
    fn untyped_writes_hold_by_their_status() {
        for kind in [WriteKind::DeleteStream, WriteKind::CreateBucket] {
            assert_eq!(
                classify(kind, 204, [], b""),
                WriteOutcome::Applied,
                "{kind:?}"
            );
        }
    }

    /// Every other write's success says something only with the typed
    /// answer Ursula sends for it: a bare `2xx`, such as a misrouting
    /// proxy's, is unknown.
    #[test]
    fn every_success_needs_its_typed_answer() {
        let token = b"4503599627370497".as_slice();
        let next = b"00000000000000000003".as_slice();
        let digest = "07".repeat(32);
        let snapshot = [
            (crate::names::STREAM_SNAPSHOT_OFFSET, next),
            (crate::names::STREAM_SNAPSHOT_DIGEST, digest.as_bytes()),
            (crate::names::STREAM_INCARNATION, token),
        ];
        type Headers<'a> = Vec<(&'a str, &'a [u8])>;
        let retained = [
            (crate::names::STREAM_RETAINED_OFFSET, next),
            (crate::names::STREAM_INCARNATION, token),
        ];
        let typed: [(WriteKind, u16, Headers<'_>); 6] = [
            (WriteKind::Retention, 204, retained.to_vec()),
            (WriteKind::Append, 204, vec![(
                crate::names::STREAM_NEXT_OFFSET,
                next,
            )]),
            (WriteKind::Create, 201, vec![(
                crate::names::STREAM_INCARNATION,
                token,
            )]),
            (WriteKind::PartsDiscard, 204, vec![(
                crate::names::STREAM_INCARNATION,
                token,
            )]),
            (WriteKind::Publish, 204, snapshot.to_vec()),
            (WriteKind::PartStage, 201, vec![(
                crate::names::STREAM_SNAPSHOT_PART,
                b"part-1".as_slice(),
            )]),
        ];
        for (kind, status, headers) in typed {
            assert_eq!(
                classify(kind, status, headers, b""),
                WriteOutcome::Applied,
                "{kind:?}"
            );
            assert_eq!(
                classify(kind, status, [], b""),
                WriteOutcome::Unknown,
                "{kind:?}"
            );
            // A typed header that does not parse is no typed answer.
            let garbled = [
                (crate::names::STREAM_NEXT_OFFSET, b"x".as_slice()),
                (crate::names::STREAM_INCARNATION, b"x".as_slice()),
                (crate::names::STREAM_SNAPSHOT_PART, b"".as_slice()),
            ];
            assert_eq!(
                classify(kind, status, garbled, b""),
                WriteOutcome::Unknown,
                "{kind:?}"
            );
        }
    }

    /// A `503` code proves a refusal only at `503`, and a code this version
    /// does not know proves nothing.
    #[test]
    fn a_code_proves_a_refusal_at_its_status_only() {
        let no_leader = ErrorBody::fixed(ErrorCode::NoLeader).render();
        assert_eq!(
            classify(WriteKind::Append, 500, [], no_leader.as_bytes()),
            WriteOutcome::Unknown
        );
        assert_eq!(
            classify(WriteKind::Append, 503, [], no_leader.as_bytes()),
            WriteOutcome::Refused(ErrorCode::NoLeader)
        );
        let unknown = ErrorBody::new(ErrorCode::Unknown("NEW_CODE".to_owned()), "x").render();
        assert_eq!(
            classify(WriteKind::Append, 503, [], unknown.as_bytes()),
            WriteOutcome::Unknown
        );
        assert_eq!(
            classify(WriteKind::Append, 429, [], unknown.as_bytes()),
            WriteOutcome::Refused(ErrorCode::Unknown("NEW_CODE".to_owned()))
        );
        assert_eq!(
            classify(WriteKind::Append, 503, [], b"<html>busy</html>"),
            WriteOutcome::Unknown
        );
        assert_eq!(
            classify(WriteKind::Append, 100, [], b""),
            WriteOutcome::Unknown
        );
        assert_eq!(
            classify(WriteKind::Append, 303, [], b""),
            WriteOutcome::Unknown
        );
        assert_eq!(
            classify(WriteKind::Append, 308, [], b""),
            WriteOutcome::Redirected
        );
        assert!(!proves_refusal(503, &ErrorCode::OutcomeUnknown));
        assert!(proves_refusal(501, &ErrorCode::NoColdStore));
        assert!(!proves_refusal(503, &ErrorCode::NoColdStore));
    }

    /// Every known code proves a refusal at one status at most, and only at
    /// `408`, `501` or `503`.
    #[test]
    fn only_408_501_and_503_codes_prove_a_refusal() {
        for code in ErrorCode::ALL {
            let statuses: Vec<u16> = (100..600)
                .filter(|status| proves_refusal(*status, code))
                .collect();
            assert!(
                matches!(statuses.as_slice(), [] | [408] | [501] | [503]),
                "{code}: {statuses:?}"
            );
        }
    }
}
