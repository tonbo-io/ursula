use axum::body::Bytes;
use axum::http::StatusCode;
use axum::http::header::CACHE_CONTROL;
use axum::http::header::CONTENT_LENGTH;
use axum::http::header::CONTENT_TYPE;
use axum::http::header::ETAG;
use axum::http::header::HeaderMap;
use axum::http::header::HeaderValue;
use axum::http::header::IF_NONE_MATCH;
use axum::http::header::LOCATION;
use axum::response::IntoResponse;
use axum::response::Response;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
#[cfg(test)]
use serde_json::Value;
use ursula_headers::ACCEPT_RANGES_BYTES;
use ursula_headers::ChunkMark;
use ursula_headers::ChunkRead;
use ursula_headers::ClosedEpoch;
use ursula_headers::ContentRange;
use ursula_headers::CurrentEpoch;
use ursula_headers::Delivery;
use ursula_headers::EpochClosure;
use ursula_headers::ErrorBody;
use ursula_headers::ErrorCode;
use ursula_headers::HeaderError;
use ursula_headers::ImmutableSnapshotRead;
use ursula_headers::IncarnationMismatch;
use ursula_headers::Inherited;
use ursula_headers::ProducerAnswer;
use ursula_headers::Receipt;
use ursula_headers::SnapshotPublishAnswer;
use ursula_headers::SnapshotRead;
use ursula_headers::SnapshotRef;
use ursula_headers::StreamHead;
use ursula_headers::StreamSeq;
use ursula_headers::StreamSeqConflict;
use ursula_headers::names;
use ursula_headers::value::Epoch;
use ursula_headers::value::Incarnation;
use ursula_headers::value::Offset;
use ursula_headers::value::Seq;
use ursula_headers::value::SnapshotDigest;
use ursula_raft::RaftGroupMetricsSnapshot;
use ursula_raft::raft_grpc_metrics_snapshot;
use ursula_runtime::BootstrapStreamResponse;
use ursula_runtime::ClosedBy;
use ursula_runtime::ClosedEpoch as StreamClosedEpoch;
use ursula_runtime::ColdStoreInfo;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_runtime::GroupSnapshotError;
use ursula_runtime::HeadStreamResponse;
use ursula_runtime::ProducerAck;
use ursula_runtime::ProducerDelivery;
use ursula_runtime::ProducerRefusal;
use ursula_runtime::ReadEnd;
use ursula_runtime::ReadSnapshotResponse;
use ursula_runtime::ReadStreamResponse;
use ursula_runtime::RuntimeError;
use ursula_runtime::RuntimeMailboxSnapshot;
use ursula_runtime::RuntimeMetricsSnapshot;
use ursula_runtime::StreamErrorCode;
use ursula_runtime::StreamErrorContext;
use ursula_runtime::WriteReceipt;
use ursula_shard::BucketStreamId;

use crate::HEADER_CROSS_ORIGIN_RESOURCE_POLICY;
use crate::HEADER_STREAM_CURSOR;
use crate::HEADER_STREAM_EXPIRES_AT;
use crate::HEADER_STREAM_TTL;
use crate::HEADER_X_CONTENT_TYPE_OPTIONS;
use crate::HttpMetricsSnapshot;
use crate::SnapshotResource;
use crate::limits;

const JSON_READ_CONTENT_TYPE: &str = "application/x-ndjson";

/// The status and code of a runtime error's answer: the one mapping of a
/// runtime error to its answer.
pub(crate) fn runtime_error_answer(err: &RuntimeError) -> (StatusCode, ErrorCode) {
    match err {
        RuntimeError::InvalidRaftGroup { .. } | RuntimeError::SnapshotPlacementMismatch { .. } => {
            (StatusCode::BAD_REQUEST, ErrorCode::InvalidRequest)
        }
        RuntimeError::InvalidConfig(_)
        | RuntimeError::ColdStoreConfig { .. }
        | RuntimeError::StaticMembershipConfig { .. }
        | RuntimeError::MailboxClosed { .. }
        | RuntimeError::ResponseDropped { .. }
        | RuntimeError::SpawnCoreThread { .. }
        | RuntimeError::GroupAlreadyInstalled { .. }
        | RuntimeError::GroupRestartAborted { .. } => {
            (StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::Internal)
        }
        RuntimeError::ColdStoreIo { .. } => (StatusCode::BAD_GATEWAY, ErrorCode::ColdStoreFailed),
        RuntimeError::LiveReadBackpressure { .. } => (
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::LiveReadBackpressure,
        ),
        // Normal static-cluster path redirects GroupNotHosted to a voter. This is the
        // fallback when no routing target is available.
        RuntimeError::GroupNotHosted { .. } => {
            (StatusCode::SERVICE_UNAVAILABLE, ErrorCode::NoLeader)
        }
        // The group's engine opens on this node, so nothing was proposed.
        RuntimeError::GroupOpening { .. } => {
            (StatusCode::SERVICE_UNAVAILABLE, ErrorCode::GroupRestarting)
        }
        RuntimeError::GroupEngine { error, .. } => match error {
            GroupEngineError::Stream(refusal) => stream_error_answer(refusal.code),
            GroupEngineError::Infra(infra) => infra_error_answer(infra),
            // A leadership refusal the caller did not route answers `500`.
            GroupEngineError::NotLeader(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::Internal)
            }
        },
    }
}

fn infra_error_answer(err: &GroupInfraError) -> (StatusCode, ErrorCode) {
    let unavailable = |code| (StatusCode::SERVICE_UNAVAILABLE, code);
    match err {
        GroupInfraError::ColdBackpressure { .. } => unavailable(ErrorCode::ColdBackpressure),
        GroupInfraError::RaftUncommittedBackpressure { .. } => {
            unavailable(ErrorCode::RaftUncommittedBackpressure)
        }
        GroupInfraError::RecoveryPending { .. } => unavailable(ErrorCode::HistoryPending),
        GroupInfraError::ApplyStopped { .. } => unavailable(ErrorCode::ApplyStopped),
        // No engine of the group runs, so nothing was proposed: its
        // previous engine is still stopping, or the snapshot store did not
        // answer the restore at engine open.
        GroupInfraError::EngineWorkOutlivedShutdown { .. }
        | GroupInfraError::PreviousEngineWorkRunning { .. }
        | GroupInfraError::Snapshot(GroupSnapshotError::StoreUnavailable { .. }) => {
            unavailable(ErrorCode::GroupRestarting)
        }
        GroupInfraError::OutcomeUnknown => unavailable(ErrorCode::OutcomeUnknown),
        // Refused before proposal: nothing was written and a retry fails too.
        GroupInfraError::InvalidRaftCommand { .. } => {
            (StatusCode::BAD_REQUEST, ErrorCode::InvalidRequest)
        }
        // The same failure a snapshot read answers.
        GroupInfraError::ColdStore { .. } => (StatusCode::BAD_GATEWAY, ErrorCode::ColdStoreFailed),
        GroupInfraError::RecoveryNodeMismatch { .. }
        | GroupInfraError::RaftGroupNotRegistered { .. }
        | GroupInfraError::RecoveryAlreadyBound { .. }
        | GroupInfraError::RecoveryGroupMismatch { .. }
        | GroupInfraError::WalOpen { .. }
        | GroupInfraError::LogBelowSnapshot { .. }
        | GroupInfraError::RecoveryVoteFloor { .. }
        | GroupInfraError::RecoveryCutting { .. }
        | GroupInfraError::CommittedOverwrite { .. }
        | GroupInfraError::UnrelatedHistory { .. }
        | GroupInfraError::LogBehindCommit { .. }
        | GroupInfraError::RecoveryPlanFenced { .. }
        | GroupInfraError::OwnerStopped
        | GroupInfraError::UnexpectedResponse { .. }
        | GroupInfraError::MissingStreamId { .. }
        | GroupInfraError::Unsupported { .. }
        | GroupInfraError::BootstrapWindowNotHot { .. }
        | GroupInfraError::ColdCompactionStale { .. }
        | GroupInfraError::ForwardFailed { .. }
        | GroupInfraError::ForwardedAnswerInvalid { .. }
        | GroupInfraError::InvalidRaftConfig { .. }
        | GroupInfraError::RecoveryGateOpen { .. }
        | GroupInfraError::MembershipInit { .. }
        | GroupInfraError::LeaderWaitTimedOut { .. }
        | GroupInfraError::Snapshot(
            GroupSnapshotError::InvalidContent(_)
            | GroupSnapshotError::ForeignKey { .. }
            | GroupSnapshotError::ObjectMissing { .. }
            | GroupSnapshotError::ObjectCorrupt { .. }
            | GroupSnapshotError::PointerUnreadable { .. },
        ) => (StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::Internal),
    }
}

/// The status and code of a stream refusal; the code is the refusal's own
/// name.
pub(crate) fn stream_error_answer(code: StreamErrorCode) -> (StatusCode, ErrorCode) {
    use StatusCode as S;
    match code {
        StreamErrorCode::InvalidBucketId => (S::BAD_REQUEST, ErrorCode::InvalidBucketId),
        StreamErrorCode::InvalidStreamId => (S::BAD_REQUEST, ErrorCode::InvalidStreamId),
        StreamErrorCode::BucketNotFound => (S::NOT_FOUND, ErrorCode::BucketNotFound),
        StreamErrorCode::BucketErased => (S::NOT_FOUND, ErrorCode::BucketErased),
        StreamErrorCode::StreamNotFound => (S::NOT_FOUND, ErrorCode::StreamNotFound),
        StreamErrorCode::StreamGone => (S::GONE, ErrorCode::StreamGone),
        StreamErrorCode::StreamAlreadyExistsConflict => {
            (S::CONFLICT, ErrorCode::StreamAlreadyExistsConflict)
        }
        StreamErrorCode::MissingContentType => (S::BAD_REQUEST, ErrorCode::MissingContentType),
        StreamErrorCode::ContentTypeMismatch => (S::CONFLICT, ErrorCode::ContentTypeMismatch),
        StreamErrorCode::EmptyAppend => (S::BAD_REQUEST, ErrorCode::EmptyAppend),
        StreamErrorCode::StreamClosed => (S::CONFLICT, ErrorCode::StreamClosed),
        StreamErrorCode::StreamSeqConflict => (S::CONFLICT, ErrorCode::StreamSeqConflict),
        StreamErrorCode::InvalidProducer => (S::BAD_REQUEST, ErrorCode::InvalidProducer),
        StreamErrorCode::ProducerEpochClosed => (S::FORBIDDEN, ErrorCode::ProducerEpochClosed),
        StreamErrorCode::ProducerExpired => (S::FORBIDDEN, ErrorCode::ProducerExpired),
        StreamErrorCode::ProducerEpochStale => (S::FORBIDDEN, ErrorCode::ProducerEpochStale),
        StreamErrorCode::ProducerKeyConflict => (S::FORBIDDEN, ErrorCode::ProducerKeyConflict),
        StreamErrorCode::ProducerDigestMismatch => {
            (S::BAD_REQUEST, ErrorCode::ProducerDigestMismatch)
        }
        StreamErrorCode::ProducerSeqConflict => (S::CONFLICT, ErrorCode::ProducerSeqConflict),
        StreamErrorCode::InvalidRetention => (S::BAD_REQUEST, ErrorCode::InvalidRetention),
        StreamErrorCode::OffsetOutOfRange => {
            (S::RANGE_NOT_SATISFIABLE, ErrorCode::OffsetOutOfRange)
        }
        StreamErrorCode::InvalidColdFlush => (S::BAD_REQUEST, ErrorCode::InvalidColdFlush),
        StreamErrorCode::InvalidSnapshot => (S::BAD_REQUEST, ErrorCode::InvalidSnapshot),
        StreamErrorCode::SnapshotNotFound => (S::NOT_FOUND, ErrorCode::SnapshotNotFound),
        StreamErrorCode::SnapshotConflict => (S::CONFLICT, ErrorCode::SnapshotConflict),
        StreamErrorCode::InvalidRecordBoundaries => {
            (S::BAD_REQUEST, ErrorCode::InvalidRecordBoundaries)
        }
        // The HTTP layer verifies the boundary and proposes again; reaching
        // a client means the stream changed during the check (fail closed).
        StreamErrorCode::JsonBoundaryUnverified => {
            (S::SERVICE_UNAVAILABLE, ErrorCode::JsonBoundaryUnverified)
        }
        StreamErrorCode::ImportConflict => (S::CONFLICT, ErrorCode::ImportConflict),
        StreamErrorCode::ImportInvalid => (S::BAD_REQUEST, ErrorCode::ImportInvalid),
        // Only the history driver proposes the event: a server fault.
        StreamErrorCode::HistoryMismatch => (S::INTERNAL_SERVER_ERROR, ErrorCode::HistoryMismatch),
        // F3 producer cap.
        StreamErrorCode::ProducerLimit => (S::TOO_MANY_REQUESTS, ErrorCode::ProducerLimit),
        // The proposer's clock is about 140,000 years ahead: a server fault
        // a retry does not fix.
        StreamErrorCode::CreatedAtOutOfRange => {
            (S::INTERNAL_SERVER_ERROR, ErrorCode::CreatedAtOutOfRange)
        }
        // D12: 412, never 409, so it does not mix with `Stream-Seq` and
        // producer conflicts.
        StreamErrorCode::IncarnationMismatch => {
            (S::PRECONDITION_FAILED, ErrorCode::IncarnationMismatch)
        }
        // A write that raced its group's genesis applied nothing; a retry
        // soon succeeds.
        StreamErrorCode::HistoryPending => (S::SERVICE_UNAVAILABLE, ErrorCode::HistoryPending),
    }
}

/// The stream refusals whose own text a client reads: the allowlist. Each
/// of their messages names only what the request named, which
/// `error_body_tests` checks for every code here. Every other refusal, a
/// new one included, sends its code's fixed message.
pub(crate) const REFUSALS_WITH_TEXT: &[StreamErrorCode] = &[
    StreamErrorCode::InvalidBucketId,
    StreamErrorCode::InvalidStreamId,
    StreamErrorCode::ContentTypeMismatch,
    StreamErrorCode::StreamClosed,
    StreamErrorCode::StreamSeqConflict,
    StreamErrorCode::StreamAlreadyExistsConflict,
    StreamErrorCode::InvalidProducer,
    StreamErrorCode::ProducerSeqConflict,
    StreamErrorCode::ProducerEpochStale,
    StreamErrorCode::ProducerKeyConflict,
    StreamErrorCode::OffsetOutOfRange,
];

/// The message of a runtime error's answer: an allowlisted stream
/// refusal's own text ([`REFUSALS_WITH_TEXT`]), else its code's fixed
/// message. The full error is the server's log's.
pub(crate) fn runtime_error_message(err: &RuntimeError, code: &ErrorCode) -> String {
    let refusal = match err {
        RuntimeError::GroupEngine { error, .. }
            if error
                .code()
                .is_some_and(|code| REFUSALS_WITH_TEXT.contains(&code)) =>
        {
            error.refusal_text()
        }
        _ => None,
    };
    refusal.map_or_else(|| code.default_message().to_owned(), str::to_owned)
}

/// The body of a runtime error's answer, with its status: the one place a
/// runtime error becomes what a client reads, on an HTTP answer and an SSE
/// `error` event alike.
pub(crate) fn runtime_error_body(err: &RuntimeError) -> (StatusCode, ErrorBody) {
    let (status, code) = runtime_error_answer(err);
    let message = runtime_error_message(err, &code);
    // A cold-tier failure was logged with its cause where it was converted
    // (`ursula_runtime::cold_store_failure`).
    let logged_at_conversion = matches!(err, RuntimeError::GroupEngine {
        error: GroupEngineError::Infra(GroupInfraError::ColdStore { .. }),
        ..
    });
    if status.is_server_error() && !logged_at_conversion {
        tracing::warn!(%status, error = %err, "runtime request failed");
    } else {
        tracing::debug!(%status, error = %err, "runtime request refused");
    }
    (status, ErrorBody::new(code, message))
}

/// An SSE `error` event whose data is `body`, the JSON body of an HTTP
/// error answer, on one line.
pub(crate) fn sse_error_event(body: &ErrorBody) -> String {
    format!("event: error\ndata:{}\n\n", sse_safe_line(&body.render()))
}

/// The code of an error answer's body, for tests: panics unless the
/// answer is JSON and its body parses with a message.
#[cfg(test)]
pub(crate) async fn error_code_of(response: Response) -> ErrorCode {
    error_body_of(response).await.code
}

/// The error body of an answer, for tests: panics unless the answer is
/// JSON and its body parses with a message.
#[cfg(test)]
pub(crate) async fn error_body_of(response: Response) -> ErrorBody {
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some(ErrorBody::CONTENT_TYPE),
        "an error answer is JSON"
    );
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("an error body");
    match ErrorBody::parse(&body) {
        Ok(parsed) => {
            assert!(!parsed.message.is_empty(), "{}", parsed.code);
            parsed
        }
        Err(err) => panic!("{err}: {}", String::from_utf8_lossy(&body)),
    }
}

/// Marks an answer whose error body [`error_answer`] rendered, so the
/// client plane's net ([`render_unrendered_error`]) leaves it alone.
#[derive(Debug, Clone, Copy)]
struct RenderedErrorBody;

/// A response extension that keeps the compression layer off an answer:
/// a snapshot, whose byte ranges refer to its stored body, an immutable
/// chunk, which has one representation per `ETag` (extensions Section 4.3),
/// and every error answer.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Uncompressed;

/// `response`, which the compression layer leaves alone.
pub(crate) fn uncompressed(mut response: Response) -> Response {
    response.extensions_mut().insert(Uncompressed);
    response
}

/// [`error_answer`] with no headers of its own.
pub(crate) fn error_response(
    status: StatusCode,
    code: ErrorCode,
    message: impl Into<String>,
) -> Response {
    error_answer(status, code, message, HeaderMap::new())
}

/// [`error_answer`] with the code's fixed message.
pub(crate) fn fixed_error_response(status: StatusCode, code: ErrorCode) -> Response {
    fixed_error_answer(status, code, HeaderMap::new())
}

/// The one renderer of an error answer on the client plane: `status`, the
/// default headers, then `headers`, and the JSON body naming `code` with
/// `message`, a human-readable detail (extensions Section 11). A message
/// never echoes the request body or a credential, and names no address,
/// object key or other internal of the server. Error answers are never
/// compressed.
pub(crate) fn error_answer(
    status: StatusCode,
    code: ErrorCode,
    message: impl Into<String>,
    headers: HeaderMap,
) -> Response {
    let mut all = HeaderMap::new();
    insert_default_response_headers(&mut all);
    all.extend(headers);
    all.remove(CONTENT_LENGTH);
    all.insert(
        CONTENT_TYPE,
        HeaderValue::from_static(ErrorBody::CONTENT_TYPE),
    );
    let mut response = (status, all, ErrorBody::new(code, message).render()).into_response();
    response.extensions_mut().insert(RenderedErrorBody);
    uncompressed(response)
}

/// Renders the body of a client-plane error answer that no handler
/// rendered: axum's `404` of an unknown path, its `405`, its extractor
/// rejections and its body limit. The code comes from the status alone,
/// and the answer keeps its headers, extensions and version.
pub(crate) fn render_unrendered_error(response: &mut Response) {
    let status = response.status();
    if !(status.is_client_error() || status.is_server_error())
        || response.extensions().get::<RenderedErrorBody>().is_some()
    {
        return;
    }
    let code = match status {
        StatusCode::NOT_FOUND => ErrorCode::NotFound,
        StatusCode::METHOD_NOT_ALLOWED => ErrorCode::MethodNotAllowed,
        StatusCode::PAYLOAD_TOO_LARGE => ErrorCode::RequestTooLarge,
        status if status.is_server_error() => ErrorCode::Internal,
        _ => ErrorCode::InvalidRequest,
    };
    let (parts, _body) = std::mem::take(response).into_parts();
    let rendered = fixed_error_answer(status, code, parts.headers);
    let (mut rendered_parts, body) = rendered.into_parts();
    let mut extensions = parts.extensions;
    extensions.extend(rendered_parts.extensions);
    rendered_parts.extensions = extensions;
    rendered_parts.version = parts.version;
    *response = Response::from_parts(rendered_parts, body);
}

/// [`error_answer`] with the code's fixed message.
pub(crate) fn fixed_error_answer(
    status: StatusCode,
    code: ErrorCode,
    headers: HeaderMap,
) -> Response {
    let body = ErrorBody::fixed(code);
    error_answer(status, body.code, body.message, headers)
}

/// `Stream-Incarnation` (D12): the opaque token of the stream incarnation
/// that served a response. `0` means unknown and is omitted; a token is
/// never `0` (#581 section 4.2).
pub(crate) fn insert_incarnation(headers: &mut HeaderMap, incarnation: u64) {
    if incarnation != 0 {
        insert_u64_header(headers, names::STREAM_INCARNATION, incarnation);
    }
}

/// The stream metadata of a successful HEAD (extensions Section 10), with
/// the node's limits. `Err` only when a value is outside what the protocol
/// carries, which the state machine never produces: a stream's incarnation
/// is never `0`, its retained offset and snapshot lie at or below its tail,
/// and a snapshot digest is BLAKE3 hex.
pub(crate) fn stream_head(
    response: &HeadStreamResponse,
    max_snapshot_bytes: Option<u64>,
) -> Result<StreamHead, HeaderError> {
    let snapshot = match (response.snapshot_offset, &response.snapshot_digest) {
        (Some(offset), Some(digest)) => Some(snapshot_ref(offset, digest)?),
        _ => None,
    };
    let inherited = response
        .inherited
        .map(|inherited| -> Result<Inherited, HeaderError> {
            Ok(Inherited {
                previous_incarnation: Incarnation::try_from(inherited.previous_incarnation)?,
                offset: Offset::from_u64(inherited.offset),
            })
        })
        .transpose()?;
    let head = StreamHead::new(
        Incarnation::try_from(response.incarnation)?,
        Offset::from_u64(response.tail_offset),
        Offset::from_u64(response.retained_offset),
        limits::MAX_REQUEST_BYTES,
    )?
    .with_closed(response.closed)
    .with_snapshot(snapshot)?
    .with_max_snapshot_bytes(max_snapshot_bytes);
    // W5: what the stream's current history began from. Two histories give
    // one stream the same token with probability 2^-52; HEAD then reports
    // no predecessor rather than a previous token equal to the current one.
    match head.with_inherited(inherited) {
        Ok(head) => Ok(head),
        Err(HeaderError::PreviousIncarnationIsCurrent) => Ok(head),
        Err(err) => Err(err),
    }
}

/// Renders a HEAD's stream metadata with the shared header crate.
pub(crate) fn insert_stream_head(headers: &mut HeaderMap, head: &StreamHead) {
    head.render(|name, value| insert_header_str(headers, name, value));
}

pub(crate) fn insert_padded_offset(headers: &mut HeaderMap, name: &'static str, value: u64) {
    if let Ok(value) = HeaderValue::from_str(&format!("{value:020}")) {
        headers.insert(name, value);
    }
}

pub(crate) fn insert_offset(headers: &mut HeaderMap, next_offset: u64) {
    insert_padded_offset(headers, names::STREAM_NEXT_OFFSET, next_offset);
}

pub(crate) fn insert_snapshot_offset(headers: &mut HeaderMap, snapshot_offset: u64) {
    insert_padded_offset(headers, names::STREAM_SNAPSHOT_OFFSET, snapshot_offset);
}

pub(crate) fn insert_cursor(headers: &mut HeaderMap, cursor: u64) {
    insert_padded_offset(headers, HEADER_STREAM_CURSOR, cursor);
}

pub(crate) fn response_cursor(next_offset: u64, request_cursor: Option<&str>) -> u64 {
    let Some(request_cursor) = request_cursor else {
        return next_offset;
    };
    let Ok(request_cursor) = request_cursor.parse::<u64>() else {
        return next_offset;
    };
    if request_cursor >= next_offset {
        request_cursor.saturating_add(1)
    } else {
        next_offset
    }
}

pub(crate) fn insert_content_type(headers: &mut HeaderMap, content_type: &str) {
    if let Ok(value) = HeaderValue::from_str(content_type) {
        headers.insert(CONTENT_TYPE, value);
    }
}

pub(crate) fn insert_default_response_headers(headers: &mut HeaderMap) {
    insert_static(headers, HEADER_X_CONTENT_TYPE_OPTIONS, "nosniff");
    insert_static(headers, HEADER_CROSS_ORIGIN_RESOURCE_POLICY, "cross-origin");
    // A stream read carries an `ETag` and no cache directive would make it
    // *heuristically* cacheable, so a shared cache could store private stream
    // bytes and serve them on a URL match. Default to refusing storage and let
    // a path change it after this call: the SSE handler sends
    // `no-cache, no-store`, which the base protocol's `no-cache` asks for.
    //
    // This also forecloses caching a `public_read` stream at a CDN, which would
    // be a legitimate thing to want. Relaxing it belongs to the gateway: it is
    // the only layer that knows a bucket is public, whereas a node serves
    // whatever reaches it and cannot tell private bytes from public ones.
    insert_cache_control(headers, "no-store");
}

pub(crate) fn insert_cache_control(headers: &mut HeaderMap, value: &'static str) {
    headers.insert(CACHE_CONTROL, HeaderValue::from_static(value));
}

pub(crate) fn insert_lifetime_headers(
    headers: &mut HeaderMap,
    stream_ttl_seconds: Option<u64>,
    stream_expires_at_ms: Option<u64>,
) {
    if let Some(ttl) = stream_ttl_seconds {
        insert_u64_header(headers, HEADER_STREAM_TTL, ttl);
    }
    if let Some(expires_at_ms) = stream_expires_at_ms
        && let Some(expires_at) = DateTime::<Utc>::from_timestamp_millis(
            i64::try_from(expires_at_ms).expect("expires_at_ms fits i64"),
        )
        && let Ok(value) =
            HeaderValue::from_str(&expires_at.to_rfc3339_opts(SecondsFormat::Millis, true))
    {
        headers.insert(HEADER_STREAM_EXPIRES_AT, value);
    }
}

/// The typed answer (#581 section 2.4) to a producer write that applied, now
/// or before. `Err` only when a value is outside what the protocol carries,
/// which the state machine never produces (epochs and seqs are validated
/// before apply, a stream's incarnation is never `0`).
pub(crate) fn producer_ack_answer(
    ack: &ProducerAck,
    incarnation: u64,
) -> Result<ProducerAnswer, HeaderError> {
    let epoch = Epoch::try_from(ack.epoch)?;
    let current = match ack.closed_epoch {
        None => CurrentEpoch::new(epoch),
        Some(closed) => CurrentEpoch::closing(epoch, closed_epoch(closed)?)?,
    };
    let seq = Seq::try_from(ack.seq)?;
    let incarnation = Incarnation::try_from(incarnation)?;
    let receipt = |written: WriteReceipt| {
        Receipt::new(
            Offset::from_u64(written.start_offset),
            Offset::from_u64(written.next_offset),
            written.closed,
        )
    };
    Ok(match ack.delivery {
        ProducerDelivery::Applied(written) => ProducerAnswer::Receipt {
            delivery: Delivery::Applied,
            epoch: current,
            seq,
            receipt: receipt(written)?,
            incarnation,
        },
        ProducerDelivery::Repeat(written) => ProducerAnswer::Receipt {
            delivery: Delivery::Repeat,
            epoch: current,
            seq,
            receipt: receipt(written)?,
            incarnation,
        },
        ProducerDelivery::Evicted => ProducerAnswer::ReceiptEvicted {
            epoch: current,
            seq,
            incarnation,
        },
    })
}

fn closed_epoch(closed: StreamClosedEpoch) -> Result<ClosedEpoch, HeaderError> {
    Ok(ClosedEpoch {
        epoch: Epoch::try_from(closed.epoch)?,
        seq: Seq::try_from(closed.seq)?,
    })
}

/// The typed answer to a refused producer write, from the refusal's
/// context. `None` for every other error; `Err` as for
/// [`producer_ack_answer`].
pub(crate) fn producer_refusal_answer(
    context: &[StreamErrorContext],
) -> Option<Result<ProducerAnswer, HeaderError>> {
    let refusal = |refusal: ProducerRefusal| -> Result<ProducerAnswer, HeaderError> {
        Ok(match refusal {
            ProducerRefusal::EpochClosed {
                epoch,
                by: ClosedBy::Claim,
                closed,
            } => ProducerAnswer::EpochClosed(EpochClosure::by_claim(
                Epoch::try_from(epoch)?,
                closed_epoch(closed)?,
            )?),
            // Row 11 (G2): a history event closed the producer's current
            // epoch, which the refusal names as both epochs.
            ProducerRefusal::EpochClosed {
                epoch,
                by: ClosedBy::History,
                closed,
            } => {
                debug_assert_eq!(
                    epoch, closed.epoch,
                    "a history event closes the current epoch"
                );
                ProducerAnswer::EpochClosed(EpochClosure::by_history(closed_epoch(closed)?))
            }
            ProducerRefusal::Expired { epoch } => ProducerAnswer::ProducerExpired {
                epoch: Epoch::try_from(epoch)?,
            },
            ProducerRefusal::StaleEpoch { epoch } => ProducerAnswer::StaleEpoch {
                epoch: Epoch::try_from(epoch)?,
            },
            ProducerRefusal::KeyConflict { epoch } => ProducerAnswer::KeyConflict {
                epoch: Epoch::try_from(epoch)?,
            },
            ProducerRefusal::EpochNotClaimed => ProducerAnswer::EpochNotClaimed,
        })
    };
    context.iter().find_map(|context| match context {
        StreamErrorContext::Producer(producer) => Some(refusal(*producer)),
        StreamErrorContext::ProducerSeqConflict {
            expected_seq,
            received_seq,
        } => Some((|| {
            Ok(ProducerAnswer::SeqGap {
                expected: Seq::try_from(*expected_seq)?,
                received: Seq::try_from(*received_seq)?,
            })
        })()),
        StreamErrorContext::StreamClosed
        | StreamErrorContext::StaleColdFlushCandidate
        | StreamErrorContext::StreamIncarnation { .. }
        | StreamErrorContext::LatestSnapshot { .. }
        | StreamErrorContext::LastStreamSeq { .. } => None,
    })
}

/// `500` for a producer answer the server could not render: a value the
/// state machine produced is outside what the protocol carries.
pub(crate) fn producer_answer_failed(err: &HeaderError) -> Response {
    tracing::error!(error = %err, "a producer answer is outside the protocol's range");
    error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        ErrorCode::Internal,
        "producer answer outside the protocol's range",
    )
}

/// Renders a producer answer's headers with the shared header crate.
pub(crate) fn insert_producer_answer(headers: &mut HeaderMap, answer: &ProducerAnswer) {
    answer.render(|name, value| insert_header_str(headers, name, value));
}

/// Adds the producer answer of a refused producer write. `Err` when the
/// refusal holds a value outside the protocol's range: the caller answers
/// `500`, as [`producer_answer_failed`] does.
pub(crate) fn insert_producer_error_headers(
    headers: &mut HeaderMap,
    err: &RuntimeError,
) -> Result<(), HeaderError> {
    if let Some(answer) = producer_refusal_answer(err.stream_error_context()) {
        insert_producer_answer(headers, &answer?);
    }
    Ok(())
}

/// The typed answer of a refused `Stream-Seq` (extensions Section 5.8):
/// `None` for every other error. `Err` for a `STREAM_SEQ_CONFLICT` without
/// the last seq, or with one the header crate refuses, which the caller
/// answers `500`, so no `409` of the code goes out without
/// `Stream-Last-Seq`. Every stored seq passed the same check
/// ([`StreamSeq::new`]) at the request boundary, so neither happens.
pub(crate) fn stream_seq_conflict(
    err: &RuntimeError,
) -> Option<Result<StreamSeqConflict, HeaderError>> {
    if err.stream_error_code() != Some(StreamErrorCode::StreamSeqConflict) {
        return None;
    }
    let last = err
        .stream_error_context()
        .iter()
        .find_map(|context| match context {
            StreamErrorContext::LastStreamSeq { seq } => Some(seq.as_str()),
            _ => None,
        });
    Some(
        last.ok_or(HeaderError::Missing {
            name: names::STREAM_LAST_SEQ,
        })
        .and_then(StreamSeq::new)
        .map(|last| StreamSeqConflict { last }),
    )
}

pub(crate) fn insert_stream_error_headers(headers: &mut HeaderMap, err: &RuntimeError) {
    if err
        .stream_error_context()
        .iter()
        .any(|context| matches!(context, StreamErrorContext::StreamClosed))
    {
        insert_static(headers, names::STREAM_CLOSED, "true");
    }
    // A failed `Stream-Incarnation` precondition names the current
    // incarnation when the stream exists (D12).
    if err.stream_error_code() == Some(StreamErrorCode::IncarnationMismatch) {
        let current = err
            .stream_error_context()
            .iter()
            .find_map(|context| match context {
                StreamErrorContext::StreamIncarnation { incarnation } => {
                    Incarnation::try_from(*incarnation).ok()
                }
                _ => None,
            });
        insert_incarnation_mismatch(headers, IncarnationMismatch { current });
    }
    // A `410` or `416` of a read, and the `404` of a snapshot that is not
    // the latest, come before the `Stream-Incarnation` precondition, so they
    // name the token of the stream that decided them (extensions Section
    // 2.4). A missing stream has none.
    if matches!(
        err.stream_error_code(),
        Some(
            StreamErrorCode::StreamGone
                | StreamErrorCode::OffsetOutOfRange
                | StreamErrorCode::SnapshotNotFound
        )
    ) && let Some(token) = err
        .stream_error_context()
        .iter()
        .find_map(|context| match context {
            StreamErrorContext::StreamIncarnation { incarnation } => Some(*incarnation),
            _ => None,
        })
    {
        insert_incarnation(headers, token);
    }
    // A refused snapshot publish names the latest snapshot (#581 section 3).
    if let Some(latest) = err
        .stream_error_context()
        .iter()
        .find_map(|context| match context {
            StreamErrorContext::LatestSnapshot { offset, digest } => Some((*offset, digest)),
            _ => None,
        })
    {
        match snapshot_ref(latest.0, latest.1) {
            Ok(latest) => SnapshotPublishAnswer::Conflict { latest }
                .render(|name, value| insert_header_str(headers, name, value)),
            Err(err) => {
                tracing::error!(error = %err, "a latest snapshot outside the protocol's range")
            }
        }
    }
}

/// A published snapshot as the header crate types it. `Err` only for a
/// digest that is not BLAKE3 hex, which the state machine never stores.
pub(crate) fn snapshot_ref(offset: u64, digest: &str) -> Result<SnapshotRef, HeaderError> {
    Ok(SnapshotRef {
        offset: Offset::from_u64(offset),
        digest: SnapshotDigest::parse(names::STREAM_SNAPSHOT_DIGEST, digest)?,
    })
}

/// The `204` of a published snapshot, by this request or an identical
/// earlier one.
pub(crate) fn snapshot_published_response(offset: u64, digest: &str, incarnation: u64) -> Response {
    let answer = snapshot_ref(offset, digest).and_then(|snapshot| {
        Ok(SnapshotPublishAnswer::Published {
            snapshot,
            incarnation: Incarnation::try_from(incarnation)?,
        })
    });
    match answer {
        Ok(answer) => {
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            answer.render(|name, value| insert_header_str(&mut headers, name, value));
            (StatusCode::NO_CONTENT, headers).into_response()
        }
        Err(err) => {
            tracing::error!(error = %err, "a published snapshot outside the protocol's range");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::Internal,
                "snapshot answer outside the protocol's range",
            )
        }
    }
}

/// Renders the `412` of the `Stream-Incarnation` precondition with the
/// shared header crate: the stream's current token when it exists.
pub(crate) fn insert_incarnation_mismatch(headers: &mut HeaderMap, mismatch: IncarnationMismatch) {
    mismatch.render(|name, value| insert_header_str(headers, name, value));
}

pub(crate) fn insert_stream_error_offset(headers: &mut HeaderMap, err: &RuntimeError) {
    let Some(next_offset) = err.stream_next_offset() else {
        return;
    };
    insert_offset(headers, next_offset);
}

pub(crate) fn insert_u64_header(headers: &mut HeaderMap, name: &'static str, value: u64) {
    if let Ok(value) = HeaderValue::from_str(&value.to_string()) {
        headers.insert(name, value);
    }
}

pub(crate) fn insert_header_str(headers: &mut HeaderMap, name: &'static str, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}

pub(crate) fn insert_location(headers: &mut HeaderMap, stream_id: &BucketStreamId) {
    if let Ok(value) = HeaderValue::from_str(&format!("/{stream_id}")) {
        headers.insert(LOCATION, value);
    }
}

pub(crate) fn insert_static(headers: &mut HeaderMap, name: &'static str, value: &'static str) {
    headers.insert(name, HeaderValue::from_static(value));
}

#[derive(serde::Serialize)]
pub(crate) struct MetricsResponse {
    #[serde(flatten)]
    pub node: ursula_proto::admin::NodeMetrics,
    #[serde(flatten)]
    pub runtime: RuntimeDiagnostics,
    pub configured_raft_group_count: u32,
    pub group_state_gauges: GroupGaugeCollection,
    pub process_rss_bytes: u64,
    pub node_memory_abort_cap_bytes: u64,
    pub wal_recovery: Option<crate::WalRecoveryReport>,
    pub recovery_gates: Option<ursula_proto::admin::RecoveryGatesReport>,
    pub wal_available_bytes: u64,
    pub wal_min_available_bytes: u64,
    pub wal_resume_available_bytes: u64,
    pub wal_disk_pressure: bool,
    pub wal_disk_stat_errors: u64,
    /// Raft RPCs refused since start because the peer is in another
    /// cluster, inbound and outbound.
    pub raft_grpc_peer_cluster_refusals: u64,
    /// Inbound Raft RPCs refused since start because they named no cluster
    /// id (a node before format epoch 4, or a stray client).
    pub raft_grpc_unidentified_peer_refusals: u64,
    /// The configured peers refused for another cluster id within the last
    /// minute.
    pub peer_cluster_mismatches: Vec<ursula_proto::admin::PeerClusterMismatch>,
    /// Bytes of staged, unused snapshot parts in the streams this node
    /// leads and counts.
    pub staged_snapshot_part_bytes: u64,
    /// The groups whose replica on this node still holds removed streams in
    /// its current snapshot or its Raft log (#669): what they removed, how
    /// long ago, and the purge the log waits for.
    pub deleted_data: Vec<GroupDeletedData>,
}

/// Removed streams one group's replica still holds in its current snapshot
/// or its Raft log (#669).
#[derive(Debug, serde::Serialize)]
pub(crate) struct GroupDeletedData {
    pub raft_group_id: u32,
    #[serde(flatten)]
    pub removal: ursula_raft::snapshot_cadence::RemovalProgress,
}

/// [`GroupDeletedData`] of every group in `raft_groups` that still holds
/// removed streams.
pub(crate) fn deleted_data(raft_groups: &[RaftGroupMetricsSnapshot]) -> Vec<GroupDeletedData> {
    raft_groups
        .iter()
        .filter(|group| group.log.removal != ursula_raft::snapshot_cadence::RemovalProgress::Clear)
        .map(|group| GroupDeletedData {
            raft_group_id: group.raft_group_id,
            removal: group.log.removal,
        })
        .collect()
}

#[derive(serde::Serialize)]
pub(crate) struct RuntimeDiagnostics {
    #[serde(flatten)]
    runtime: RuntimeMetricsSnapshot,
    #[serde(flatten)]
    http: HttpMetricsSnapshot,
    #[serde(flatten)]
    raft_grpc: ursula_raft::RaftGrpcMetricsSnapshot,
    active_cores: usize,
    active_groups: usize,
    mailbox_depths: Vec<usize>,
    mailbox_capacities: Vec<usize>,
    cold_store: ColdStoreInfo,
    raft_group_count: usize,
}

#[derive(serde::Serialize)]
#[serde(untagged)]
pub(crate) enum GroupGaugeCollection {
    Groups(Vec<GroupGaugeMetrics>),
    Error { error: String },
}
pub(crate) enum GroupGaugeMetrics {
    Hosted {
        raft_group_id: u32,
        gauges: ursula_runtime::GroupStateGauges,
    },
    NotHosted {
        raft_group_id: u32,
    },
    Failed {
        raft_group_id: u32,
        error: String,
    },
}
impl serde::Serialize for GroupGaugeMetrics {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(serde::Serialize)]
        struct Hosted<'a> {
            raft_group_id: u32,
            hosted: bool,
            #[serde(flatten)]
            gauges: &'a ursula_runtime::GroupStateGauges,
        }
        #[derive(serde::Serialize)]
        struct Unavailable<'a> {
            raft_group_id: u32,
            hosted: bool,
            #[serde(skip_serializing_if = "Option::is_none")]
            error: Option<&'a str>,
        }
        match self {
            Self::Hosted {
                raft_group_id,
                gauges,
            } => Hosted {
                raft_group_id: *raft_group_id,
                hosted: true,
                gauges,
            }
            .serialize(serializer),
            Self::NotHosted { raft_group_id } => Unavailable {
                raft_group_id: *raft_group_id,
                hosted: false,
                error: None,
            }
            .serialize(serializer),
            Self::Failed {
                raft_group_id,
                error,
            } => Unavailable {
                raft_group_id: *raft_group_id,
                hosted: true,
                error: Some(error),
            }
            .serialize(serializer),
        }
    }
}

pub(crate) fn render_metrics(
    snapshot: RuntimeMetricsSnapshot,
    mailbox: RuntimeMailboxSnapshot,
    http: HttpMetricsSnapshot,
    raft_groups: &[RaftGroupMetricsSnapshot],
    cold_store: Option<&ColdStoreInfo>,
) -> RuntimeDiagnostics {
    RuntimeDiagnostics {
        active_cores: snapshot
            .per_core_appends
            .iter()
            .filter(|count| **count > 0)
            .count(),
        active_groups: snapshot
            .per_group_appends
            .iter()
            .filter(|count| **count > 0)
            .count(),
        runtime: snapshot,
        http,
        raft_grpc: raft_grpc_metrics_snapshot(),
        mailbox_depths: mailbox.depths,
        mailbox_capacities: mailbox.capacities,
        cold_store: cold_store.cloned().unwrap_or(ColdStoreInfo {
            backend: "none",
            root: None,
            bucket: None,
            region: None,
            endpoint: None,
            encryption: None,
        }),
        raft_group_count: raft_groups.len(),
    }
}

pub(crate) fn raft_group_metrics(
    value: &RaftGroupMetricsSnapshot,
) -> ursula_proto::admin::RaftGroupMetrics {
    ursula_proto::admin::RaftGroupMetrics {
        raft_group_id: u64::from(value.raft_group_id),
        node_id: value.node_id,
        current_term: value.current_term,
        current_leader: value.current_leader,
        committed_index: value.committed.map(|progress| progress.index),
        last_applied_index: value.last_applied.map(|progress| progress.index),
        voter_ids: value.voter_ids.clone(),
        voter_configs: value.voter_configurations.clone(),
        committed_voter_configs: value.committed_voter_configurations.clone(),
        learner_ids: value.learner_ids.clone(),
        maintenance: value.maintenance.clone(),
        last_log_index: value.last_log.map(|progress| progress.index),
        last_log_term: value.last_log.map(|progress| progress.term),
        committed_term: value.committed.map(|progress| progress.term),
        committed_node: value.committed.map(|progress| progress.node_id),
        last_applied_term: value.last_applied.map(|progress| progress.term),
        snapshot_term: value.snapshot.map(|progress| progress.term),
        snapshot_index: value.snapshot.map(|progress| progress.index),
        purged_term: value.purged.map(|progress| progress.term),
        purged_index: value.purged.map(|progress| progress.index),
        log_bytes_since_snapshot: value.log.log_bytes,
        log_entries_since_snapshot: value.log.log_entries,
        last_snapshot_bytes: value.log.last_snapshot_bytes,
        has_snapshot: value.log.has_snapshot,
        apply_failure: value.apply_failure.clone(),
        history: value.history,
        recovery: value.recovery,
        recovery_gate: value.recovery_gate,
        leads_live: value.leads_live,
    }
}

#[cfg(test)]
pub(crate) fn render_raft_group_metrics_array(values: &[RaftGroupMetricsSnapshot]) -> Value {
    serde_json::to_value(values.iter().map(raft_group_metrics).collect::<Vec<_>>())
        .unwrap_or(Value::Null)
}

pub(crate) fn should_base64_encode_sse_data(content_type: &str) -> bool {
    let content_type = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_ascii_lowercase();
    !(content_type.starts_with("text/") || content_type == "application/json")
}

/// The smallest body the compression layer compresses.
pub(crate) const COMPRESS_MIN_BYTES: u16 = 256;

/// Whether the compression layer may compress an answer of `content_type`
/// (JSON and NDJSON) that is not [`Uncompressed`] and holds at least
/// [`COMPRESS_MIN_BYTES`]. The layer and the read `ETag` share it, so a
/// strong tag never covers two representations.
pub(crate) fn compressible_media(content_type: &str) -> bool {
    let media_type = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim();
    media_type == "application/json" || media_type == "application/x-ndjson"
}

/// The `ETag` of a stream read or an immutable chunk: the first 128 bits,
/// in lowercase hex and quoted, of BLAKE3 over the token, the range, where
/// the read ended, the stored content type and the body.
///
/// - The body is hashed although under one token the range names the bytes
///   (G1): a deployment that breaks G1 without a history event, such as one
///   restored from volume snapshots, then gets a `200` with the bytes it now
///   serves at revalidation instead of a `304` for bytes it no longer has.
/// - The token is part of the tag (#581 section 4.7), so a `304` never
///   matches across a recreate or a history event.
/// - Closure counts only at the tail ([`ReadEnd`]), where a reader sees it,
///   so closing a stream keeps the tag of every chunk below the tail
///   (extensions Section 4.3).
/// - BLAKE3 is fixed, so every node and every release tags one answer
///   alike, and a cache revalidates against any of them.
///
/// The tag is strong. [`read_response`] weakens it where the compression
/// layer may send another representation of the same bytes.
pub(crate) fn read_etag(response: &ReadStreamResponse) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&response.incarnation.to_le_bytes());
    hasher.update(&response.offset.to_le_bytes());
    hasher.update(&response.next_offset.to_le_bytes());
    hasher.update(&[match response.end() {
        ReadEnd::BelowTail => 0,
        ReadEnd::OpenTail => 1,
        ReadEnd::ClosedTail => 2,
    }]);
    let content_type = response.content_type.as_bytes();
    hasher.update(
        &u64::try_from(content_type.len())
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    hasher.update(content_type);
    hasher.update(&response.payload);
    let hex = hasher.finalize().to_hex();
    format!("\"{}\"", hex.as_str().get(..32).unwrap_or(hex.as_str()))
}

/// The strong `ETag` of a snapshot answer, for both snapshot resources and
/// every range of one: its digest, quoted. The digest covers the content
/// type and the whole body, which never change at one offset of one token.
pub(crate) fn snapshot_etag(digest: &str) -> String {
    format!("\"{digest}\"")
}

/// Whether `If-None-Match` names `etag`: the request revalidates a stored
/// answer, and gets `304`. The comparison is weak (RFC 9110 Section
/// 8.8.3.2), as `If-None-Match` requires: `W/"x"` and `"x"` match, so a
/// layer that weakens a tag still revalidates. `*` is not supported.
pub(crate) fn if_none_match(request_headers: &HeaderMap, etag: &str) -> bool {
    let opaque = |tag: &str| {
        tag.trim()
            .strip_prefix("W/")
            .unwrap_or(tag.trim())
            .to_owned()
    };
    let wanted = opaque(etag);
    request_headers
        .get_all(IF_NONE_MATCH)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|tag| opaque(tag) == wanted)
}

pub(crate) fn read_response(
    response: ReadStreamResponse,
    request_headers: &HeaderMap,
    request_cursor: Option<&str>,
) -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(&mut headers, http_read_content_type(&response.content_type));
    insert_offset(&mut headers, response.next_offset);
    insert_incarnation(&mut headers, response.incarnation);
    // The compression layer may gzip this answer for one client and not for
    // another: one strong tag must not cover both, so the tag is weak.
    let strong = read_etag(&response);
    let etag = if compressible_media(http_read_content_type(&response.content_type))
        && response.payload.len() >= usize::from(COMPRESS_MIN_BYTES)
    {
        format!("W/{strong}")
    } else {
        strong
    };
    if let Ok(value) = HeaderValue::from_str(&etag) {
        headers.insert(ETAG, value);
    }
    if response.up_to_date {
        insert_static(&mut headers, names::STREAM_UP_TO_DATE, "true");
    }
    if response.end() == ReadEnd::ClosedTail {
        insert_static(&mut headers, names::STREAM_CLOSED, "true");
    } else if request_cursor.is_some() {
        insert_cursor(
            &mut headers,
            response_cursor(response.next_offset, request_cursor),
        );
    }
    // A `304` carries every header of the `200` it stands for.
    if if_none_match(request_headers, &etag) {
        return (StatusCode::NOT_MODIFIED, headers).into_response();
    }
    (StatusCode::OK, headers, response.payload).into_response()
}

/// The answer of an immutable chunk (extensions Section 4.1) that asked for
/// `requested_len` bytes, to the end of its chunk: `200`, or `304` with the
/// same headers when `If-None-Match` names its `ETag`. It is marked when the
/// read is immutable ([`ReadStreamResponse::is_immutable`]), and carries no
/// `Stream-Up-To-Date` and no `Stream-Cursor`. Never compressed.
pub(crate) fn chunk_response(
    response: ReadStreamResponse,
    requested_len: usize,
    request_headers: &HeaderMap,
) -> Response {
    let mark = match (response.is_immutable(requested_len), response.end()) {
        (false, _) => ChunkMark::Unmarked,
        (true, ReadEnd::ClosedTail) => ChunkMark::ClosedTail,
        (true, ReadEnd::BelowTail | ReadEnd::OpenTail) => ChunkMark::Immutable,
    };
    // A marked answer is cached for its lifetime: never send a body that
    // is not exactly the range its headers name.
    if u64::try_from(response.payload.len()).ok()
        != response.next_offset.checked_sub(response.offset)
    {
        tracing::error!(
            offset = response.offset,
            next_offset = response.next_offset,
            len = response.payload.len(),
            "a chunk's body does not hold its range"
        );
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::Internal,
            "chunk body does not hold its range",
        );
    }
    let chunk = Incarnation::try_from(response.incarnation).and_then(|incarnation| {
        ChunkRead::new(
            incarnation,
            Offset::from_u64(response.offset),
            Offset::from_u64(response.next_offset),
            mark,
        )
    });
    let chunk = match chunk {
        Ok(chunk) => chunk,
        Err(err) => {
            tracing::error!(error = %err, "a chunk answer outside the protocol's range");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::Internal,
                "chunk answer outside the protocol's range",
            );
        }
    };
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(&mut headers, http_read_content_type(&response.content_type));
    chunk.render(|name, value| insert_header_str(&mut headers, name, value));
    let etag = read_etag(&response);
    if let Ok(value) = HeaderValue::from_str(&etag) {
        headers.insert(ETAG, value);
    }
    if if_none_match(request_headers, &etag) {
        return uncompressed((StatusCode::NOT_MODIFIED, headers).into_response());
    }
    uncompressed((StatusCode::OK, headers, response.payload).into_response())
}

/// The status and headers of a snapshot read: `200`, or for a request with
/// a byte range `206` or `416` with its `Content-Range`. The snapshot
/// headers and the `ETag` describe the whole snapshot either way: its
/// digest covers the whole body, never the range (extensions Section 2.5).
/// The immutable resource marks every success (`200`, `206`, and the `304`
/// built from them) and sends no `Stream-Up-To-Date`. `Err` only for a
/// digest or token outside the protocol's range, which the state machine
/// never stores.
pub(crate) fn snapshot_head(
    response: &ReadSnapshotResponse,
    range: Option<ContentRange>,
    resource: SnapshotResource,
) -> Result<(StatusCode, HeaderMap), HeaderError> {
    let snapshot = snapshot_ref(response.snapshot_offset, &response.snapshot_digest)?;
    let incarnation = Incarnation::try_from(response.incarnation)?;
    let status = match range {
        None => StatusCode::OK,
        Some(ContentRange::Partial(_)) => StatusCode::PARTIAL_CONTENT,
        Some(ContentRange::Unsatisfied { .. }) => StatusCode::RANGE_NOT_SATISFIABLE,
    };
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(&mut headers, &response.content_type);
    let mut emit = |name, value: &str| insert_header_str(&mut headers, name, value);
    match resource {
        SnapshotResource::Stream => SnapshotRead {
            snapshot,
            incarnation,
            up_to_date: response.up_to_date,
        }
        .render(&mut emit),
        SnapshotResource::Immutable if status.is_success() => {
            ImmutableSnapshotRead {
                snapshot,
                incarnation,
            }
            .render(&mut emit);
        }
        // A `416` is no answer for the URL, so it carries no marker.
        SnapshotResource::Immutable => SnapshotRead {
            snapshot,
            incarnation,
            up_to_date: false,
        }
        .render(&mut emit),
    }
    if let Ok(value) = HeaderValue::from_str(&snapshot_etag(&response.snapshot_digest)) {
        headers.insert(ETAG, value);
    }
    insert_static(&mut headers, names::ACCEPT_RANGES, ACCEPT_RANGES_BYTES);
    if let Some(range) = range {
        range.render(|name, value| insert_header_str(&mut headers, name, value));
    }
    Ok((status, headers))
}

pub(crate) fn bootstrap_response(response: BootstrapStreamResponse) -> Response {
    let (boundary, headers) = bootstrap_head(&response);
    (
        StatusCode::OK,
        headers,
        render_bootstrap_multipart(&response, &boundary),
    )
        .into_response()
}

/// The multipart boundary and response headers of a `/bootstrap` answer.
pub(crate) fn bootstrap_head(response: &BootstrapStreamResponse) -> (String, HeaderMap) {
    let boundary = bootstrap_boundary(response);
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(
        &mut headers,
        &format!("multipart/mixed; boundary={boundary}"),
    );
    insert_incarnation(&mut headers, response.incarnation);
    match response.snapshot_offset {
        Some(snapshot_offset) => insert_snapshot_offset(&mut headers, snapshot_offset),
        None => insert_static(&mut headers, names::STREAM_SNAPSHOT_OFFSET, "-1"),
    }
    insert_offset(&mut headers, response.next_offset);
    if response.up_to_date {
        insert_static(&mut headers, names::STREAM_UP_TO_DATE, "true");
    }
    if response.closed {
        insert_static(&mut headers, names::STREAM_CLOSED, "true");
    }
    insert_cache_control(&mut headers, "no-store");
    (boundary, headers)
}

/// A fresh 128-bit random multipart boundary (D10). A boundary derived from
/// the response was predictable, so a writer could embed it in a part and
/// forge part breaks. No in-memory part (an inline snapshot body or an
/// update) contains the boundary; a cold snapshot body (F16, up to 1 GiB) is
/// streamed and not scanned, and 128 random bits make a collision there
/// negligible.
pub(crate) fn bootstrap_boundary(response: &BootstrapStreamResponse) -> String {
    loop {
        let boundary = format!("ursula-bootstrap-{:032x}", rand::random::<u128>());
        let finder = memchr::memmem::Finder::new(boundary.as_bytes());
        let mut in_memory_parts = std::iter::once(response.snapshot_payload.as_slice()).chain(
            response
                .updates
                .iter()
                .map(|update| update.payload.as_slice()),
        );
        if in_memory_parts.all(|part| finder.find(part).is_none()) {
            return boundary;
        }
    }
}

pub(crate) fn render_bootstrap_multipart(
    response: &BootstrapStreamResponse,
    boundary: &str,
) -> Vec<u8> {
    let (mut body, suffix) = bootstrap_multipart_around_snapshot(response, boundary);
    body.extend_from_slice(&response.snapshot_payload);
    body.extend_from_slice(&suffix);
    body
}

/// The multipart entity of a `/bootstrap` answer split around the snapshot
/// part's body: everything before it and everything after it. A cold
/// snapshot body (F16) is streamed between the two.
pub(crate) fn bootstrap_multipart_around_snapshot(
    response: &BootstrapStreamResponse,
    boundary: &str,
) -> (Vec<u8>, Vec<u8>) {
    let mut prefix = Vec::new();
    push_multipart_part_head(&mut prefix, boundary, &response.snapshot_content_type);
    let mut suffix = b"\r\n".to_vec();
    for update in &response.updates {
        push_multipart_part(&mut suffix, boundary, &update.content_type, &update.payload);
    }
    suffix.extend_from_slice(b"--");
    suffix.extend_from_slice(boundary.as_bytes());
    suffix.extend_from_slice(b"--\r\n");
    (prefix, suffix)
}

fn push_multipart_part_head(body: &mut Vec<u8>, boundary: &str, content_type: &str) {
    body.extend_from_slice(b"--");
    body.extend_from_slice(boundary.as_bytes());
    body.extend_from_slice(b"\r\nContent-Type: ");
    body.extend_from_slice(content_type.as_bytes());
    body.extend_from_slice(b"\r\n\r\n");
}

pub(crate) fn push_multipart_part(
    body: &mut Vec<u8>,
    boundary: &str,
    content_type: &str,
    payload: &[u8],
) {
    push_multipart_part_head(body, boundary, content_type);
    body.extend_from_slice(payload);
    body.extend_from_slice(b"\r\n");
}

pub(crate) fn offset_now_response(response: ReadStreamResponse) -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(&mut headers, http_read_content_type(&response.content_type));
    insert_offset(&mut headers, response.next_offset);
    insert_incarnation(&mut headers, response.incarnation);
    insert_static(&mut headers, names::STREAM_UP_TO_DATE, "true");
    insert_cache_control(&mut headers, "no-store");
    if response.closed {
        insert_static(&mut headers, names::STREAM_CLOSED, "true");
    }
    (StatusCode::OK, headers, Bytes::new()).into_response()
}

pub(crate) fn long_poll_no_content_response(
    response: &ReadStreamResponse,
    request_cursor: Option<&str>,
) -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_offset(&mut headers, response.next_offset);
    insert_incarnation(&mut headers, response.incarnation);
    insert_static(&mut headers, names::STREAM_UP_TO_DATE, "true");
    if response.closed {
        insert_static(&mut headers, names::STREAM_CLOSED, "true");
    } else {
        insert_cursor(
            &mut headers,
            response_cursor(response.next_offset, request_cursor),
        );
    }
    (StatusCode::NO_CONTENT, headers).into_response()
}

pub(crate) fn is_json_content_type(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .eq_ignore_ascii_case("application/json")
}

pub(crate) fn http_read_content_type(content_type: &str) -> &str {
    if is_json_content_type(content_type) {
        JSON_READ_CONTENT_TYPE
    } else {
        content_type
    }
}

pub(crate) fn normalize_http_write_payload(
    content_type: &str,
    body: Bytes,
    allow_empty_array: bool,
) -> Result<Bytes, String> {
    if !is_json_content_type(content_type) || body.is_empty() {
        return Ok(body);
    }
    crate::json_text::normalize_json_messages(&body, allow_empty_array)
        .map(Bytes::from)
        .map_err(|err| err.to_string())
}

pub(crate) fn clamp_sse_text_read(read: &mut ReadStreamResponse, encode_base64: bool) {
    if encode_base64 || read.payload.is_empty() {
        return;
    }

    let len = sse_text_payload_len(&read.content_type, &read.payload);
    if len == 0 || len == read.payload.len() {
        return;
    }

    // `offset + len` lies within the range the stream just served, so overflow
    // is unreachable. If it happened, send the read unclamped, never a wrong offset.
    let Some(next_offset) = u64::try_from(len)
        .ok()
        .and_then(|len| read.offset.checked_add(len))
    else {
        return;
    };
    read.payload.truncate(len);
    read.next_offset = next_offset;
    read.up_to_date = false;
}

fn sse_text_payload_len(content_type: &str, payload: &[u8]) -> usize {
    if is_json_content_type(content_type)
        && !payload.ends_with(b"\n")
        && let Some(newline) = payload.iter().rposition(|byte| *byte == b'\n')
    {
        // An index below `payload.len()` plus one never saturates.
        return newline.saturating_add(1);
    }

    match std::str::from_utf8(payload) {
        Ok(_) => payload.len(),
        Err(err) => err.valid_up_to(),
    }
}

pub(crate) fn render_sse_read(
    read: &ReadStreamResponse,
    encode_base64: bool,
    request_cursor: Option<&str>,
) -> String {
    let mut body = String::new();
    let closed_at_tail = read.end() == ReadEnd::ClosedTail;
    if !read.payload.is_empty() {
        body.push_str("event: data\n");
        let payload = if encode_base64 {
            BASE64_STANDARD.encode(&read.payload)
        } else {
            String::from_utf8_lossy(&read.payload).into_owned()
        };
        for line in payload.split('\n') {
            body.push_str("data:");
            body.push_str(&sse_safe_line(line));
            body.push('\n');
        }
        body.push('\n');
    }

    body.push_str("event: control\n");
    body.push_str("data:{\"streamNextOffset\":\"");
    body.push_str(&format!("{:020}", read.next_offset));
    body.push('"');
    if !closed_at_tail {
        body.push_str(",\"streamCursor\":\"");
        body.push_str(&format!(
            "{:020}",
            response_cursor(read.next_offset, request_cursor)
        ));
        body.push('"');
    }
    if read.up_to_date {
        body.push_str(",\"upToDate\":true");
    }
    if closed_at_tail {
        body.push_str(",\"streamClosed\":true");
    }
    body.push_str("}\n\n");
    body
}

pub(crate) fn sse_safe_line(line: &str) -> String {
    line.chars()
        .filter(|ch| *ch != '\r' && *ch != '\0')
        .collect()
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderMap;
    use axum::http::StatusCode;
    use axum::http::header::CACHE_CONTROL;
    use ursula_runtime::ClosedBy;
    use ursula_runtime::ClosedEpoch;
    use ursula_runtime::ProducerRefusal;
    use ursula_runtime::RuntimeError;
    use ursula_runtime::StreamErrorCode;
    use ursula_runtime::StreamErrorContext;

    use super::insert_cache_control;
    use super::insert_default_response_headers;
    use super::producer_refusal_answer;
    use super::runtime_error_answer;
    use super::stream_error_answer;

    /// Each stream refusal's code is its own name in the wire's case, so a
    /// swapped arm fails here.
    #[test]
    fn a_stream_refusals_code_is_its_name() {
        for code in [
            StreamErrorCode::InvalidBucketId,
            StreamErrorCode::InvalidStreamId,
            StreamErrorCode::BucketNotFound,
            StreamErrorCode::BucketErased,
            StreamErrorCode::StreamNotFound,
            StreamErrorCode::StreamGone,
            StreamErrorCode::StreamAlreadyExistsConflict,
            StreamErrorCode::MissingContentType,
            StreamErrorCode::ContentTypeMismatch,
            StreamErrorCode::EmptyAppend,
            StreamErrorCode::StreamClosed,
            StreamErrorCode::StreamSeqConflict,
            StreamErrorCode::InvalidProducer,
            StreamErrorCode::ProducerEpochClosed,
            StreamErrorCode::ProducerExpired,
            StreamErrorCode::ProducerEpochStale,
            StreamErrorCode::ProducerKeyConflict,
            StreamErrorCode::ProducerDigestMismatch,
            StreamErrorCode::ProducerSeqConflict,
            StreamErrorCode::InvalidRetention,
            StreamErrorCode::OffsetOutOfRange,
            StreamErrorCode::InvalidColdFlush,
            StreamErrorCode::InvalidSnapshot,
            StreamErrorCode::SnapshotNotFound,
            StreamErrorCode::SnapshotConflict,
            StreamErrorCode::InvalidRecordBoundaries,
            StreamErrorCode::JsonBoundaryUnverified,
            StreamErrorCode::ImportConflict,
            StreamErrorCode::ImportInvalid,
            StreamErrorCode::HistoryMismatch,
            StreamErrorCode::ProducerLimit,
            StreamErrorCode::CreatedAtOutOfRange,
            StreamErrorCode::IncarnationMismatch,
            StreamErrorCode::HistoryPending,
        ] {
            let name = format!("{code:?}");
            let mut wire = String::new();
            for (index, c) in name.char_indices() {
                if c.is_ascii_uppercase() && index > 0 {
                    wire.push('_');
                }
                wire.push(c.to_ascii_uppercase());
            }
            assert_eq!(stream_error_answer(code).1.as_str(), wire);
        }
    }

    /// The late path's status comes from the refusal's error code, the
    /// answer's from `ProducerAnswer::status`: they agree for every refusal.
    #[test]
    fn a_refusal_code_and_its_answer_name_one_status() {
        for refusal in [
            ProducerRefusal::EpochClosed {
                epoch: 3,
                by: ClosedBy::Claim,
                closed: ClosedEpoch { epoch: 2, seq: 1 },
            },
            ProducerRefusal::Expired { epoch: 3 },
            ProducerRefusal::StaleEpoch { epoch: 3 },
            ProducerRefusal::KeyConflict { epoch: 3 },
            ProducerRefusal::EpochNotClaimed,
        ] {
            let answer = producer_refusal_answer(&[StreamErrorContext::Producer(refusal)])
                .expect("a producer context")
                .expect("an answer");
            assert_eq!(
                stream_error_answer(refusal.code()).0.as_u16(),
                answer.status(),
                "{refusal:?}"
            );
        }
    }

    /// Every response path calls this helper, so pinning the set here is what
    /// makes a newly added path inherit the defaults rather than quietly ship
    /// without them.
    #[test]
    fn default_headers_refuse_storage_and_sniffing() {
        let mut headers = HeaderMap::new();

        insert_default_response_headers(&mut headers);

        assert_eq!(headers.get(CACHE_CONTROL).unwrap(), "no-store");
        assert_eq!(headers.get("x-content-type-options").unwrap(), "nosniff");
        assert_eq!(
            headers.get("cross-origin-resource-policy").unwrap(),
            "cross-origin"
        );
    }

    /// The SSE handler needs `no-cache` beside `no-store`, and gets it by
    /// calling the default helper first and overriding after. Reversing that
    /// order would silently drop `no-cache` on live tails, so the contract is
    /// asserted rather than left to call-site discipline.
    #[test]
    fn a_later_cache_directive_overrides_the_default() {
        let mut headers = HeaderMap::new();

        insert_default_response_headers(&mut headers);
        insert_cache_control(&mut headers, "no-cache, no-store");

        assert_eq!(headers.get(CACHE_CONTROL).unwrap(), "no-cache, no-store");
    }
    #[test]
    fn stopped_apply_and_uncertain_write_render_service_unavailable() {
        for error in [
            ursula_runtime::GroupInfraError::ApplyStopped {
                group: ursula_shard::RaftTarget::Data(ursula_shard::RaftGroupId(2)),
                term: 3,
                index: 7,
                kind: ursula_runtime::ApplyStopKind::Panic,
            },
            ursula_runtime::GroupInfraError::OutcomeUnknown,
        ] {
            let error = RuntimeError::GroupEngine {
                core_id: ursula_shard::CoreId(0),
                raft_group_id: ursula_shard::RaftGroupId(2),
                error: ursula_runtime::GroupEngineError::Infra(error),
            };
            assert_eq!(
                runtime_error_answer(&error).0,
                StatusCode::SERVICE_UNAVAILABLE
            );
            assert!(!error.to_string().contains("panic payload"));
        }
        let unproposable = RuntimeError::GroupEngine {
            core_id: ursula_shard::CoreId(0),
            raft_group_id: ursula_shard::RaftGroupId(2),
            error: ursula_runtime::GroupEngineError::Infra(
                ursula_runtime::GroupInfraError::InvalidRaftCommand {
                    command: ursula_runtime::UnproposableCommand::CreateBucket,
                },
            ),
        };
        assert_eq!(
            runtime_error_answer(&unproposable).0,
            StatusCode::BAD_REQUEST
        );
    }
}
