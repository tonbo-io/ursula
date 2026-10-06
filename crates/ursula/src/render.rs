#![expect(
    clippy::arithmetic_side_effects,
    reason = "pre-existing arithmetic debt; see Known debt in AGENTS.md"
)]
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use axum::body::Bytes;
use axum::http::StatusCode;
use axum::http::header::CACHE_CONTROL;
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
use serde_json::Value;
use serde_json::json;
use ursula_raft::RaftGroupMetricsSnapshot;
use ursula_raft::RaftGrpcMetricsSnapshot;
use ursula_raft::raft_grpc_metrics_snapshot;
use ursula_runtime::BootstrapStreamResponse;
use ursula_runtime::ColdStoreInfo;
use ursula_runtime::ProducerRequest;
use ursula_runtime::ReadSnapshotResponse;
use ursula_runtime::ReadStreamResponse;
use ursula_runtime::RuntimeError;
use ursula_runtime::RuntimeMailboxSnapshot;
use ursula_runtime::RuntimeMetricsSnapshot;
use ursula_runtime::StreamErrorCode;
use ursula_runtime::StreamErrorContext;
use ursula_shard::BucketStreamId;

use crate::HEADER_CROSS_ORIGIN_RESOURCE_POLICY;
use crate::HEADER_PRODUCER_EPOCH;
use crate::HEADER_PRODUCER_SEQ;
use crate::HEADER_STREAM_CLOSED;
use crate::HEADER_STREAM_CURSOR;
use crate::HEADER_STREAM_EXPIRES_AT;
use crate::HEADER_STREAM_INCARNATION;
use crate::HEADER_STREAM_NEXT_OFFSET;
use crate::HEADER_STREAM_SNAPSHOT_DIGEST;
use crate::HEADER_STREAM_SNAPSHOT_OFFSET;
use crate::HEADER_STREAM_TTL;
use crate::HEADER_STREAM_UP_TO_DATE;
use crate::HEADER_X_CONTENT_TYPE_OPTIONS;
use crate::HttpMetricsSnapshot;

const JSON_READ_CONTENT_TYPE: &str = "application/x-ndjson";

pub(crate) fn runtime_error_status(err: &RuntimeError) -> StatusCode {
    match err {
        RuntimeError::EmptyAppend
        | RuntimeError::InvalidRaftGroup { .. }
        | RuntimeError::SnapshotPlacementMismatch { .. } => StatusCode::BAD_REQUEST,
        RuntimeError::InvalidConfig(_)
        | RuntimeError::ColdStoreConfig { .. }
        | RuntimeError::StaticMembershipConfig { .. }
        | RuntimeError::ColdStoreIo { .. }
        | RuntimeError::MailboxClosed { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        RuntimeError::ResponseDropped { .. } | RuntimeError::SpawnCoreThread { .. } => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
        RuntimeError::LiveReadBackpressure { .. } => StatusCode::SERVICE_UNAVAILABLE,
        // Normal static-cluster path redirects GroupNotHosted to a voter. This is the
        // fallback when no routing target is available.
        RuntimeError::GroupNotHosted { .. } => StatusCode::SERVICE_UNAVAILABLE,
        RuntimeError::GroupEngine { error, .. } if error.is_backpressure() => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        RuntimeError::GroupEngine { error, .. } => match error.code() {
            Some(code) => stream_error_code_status(code),
            None => StatusCode::INTERNAL_SERVER_ERROR,
        },
    }
}

pub(crate) fn stream_error_code_status(code: StreamErrorCode) -> StatusCode {
    match code {
        StreamErrorCode::StreamGone => StatusCode::GONE,
        StreamErrorCode::BucketErased => StatusCode::NOT_FOUND,
        StreamErrorCode::BucketNotFound
        | StreamErrorCode::StreamNotFound
        | StreamErrorCode::SnapshotNotFound => StatusCode::NOT_FOUND,
        StreamErrorCode::ContentTypeMismatch
        | StreamErrorCode::StreamAlreadyExistsConflict
        | StreamErrorCode::StreamClosed
        | StreamErrorCode::StreamSeqConflict
        | StreamErrorCode::SnapshotConflict
        | StreamErrorCode::ProducerSeqConflict
        | StreamErrorCode::ImportConflict => StatusCode::CONFLICT,
        StreamErrorCode::ProducerEpochStale => StatusCode::FORBIDDEN,
        StreamErrorCode::OffsetOutOfRange => StatusCode::RANGE_NOT_SATISFIABLE,
        StreamErrorCode::InvalidBucketId
        | StreamErrorCode::InvalidStreamId
        | StreamErrorCode::MissingContentType
        | StreamErrorCode::EmptyAppend
        | StreamErrorCode::InvalidProducer
        | StreamErrorCode::InvalidRetention
        | StreamErrorCode::InvalidColdFlush
        | StreamErrorCode::InvalidSnapshot
        | StreamErrorCode::InvalidRecordBoundaries
        | StreamErrorCode::ImportInvalid => StatusCode::BAD_REQUEST,
        // F3 producer cap; the plain-text body starts with `producer_limit`.
        StreamErrorCode::ProducerLimit => StatusCode::TOO_MANY_REQUESTS,
        // The HTTP layer verifies the boundary and proposes again; reaching
        // a client means the stream changed during the check (fail closed).
        StreamErrorCode::JsonBoundaryUnverified => StatusCode::SERVICE_UNAVAILABLE,
        // D12: 412, never 409, so it does not mix with `Stream-Seq` and
        // producer conflicts.
        StreamErrorCode::IncarnationMismatch => StatusCode::PRECONDITION_FAILED,
    }
}

/// `Stream-Incarnation` (D12): the opaque token of the stream incarnation
/// that served a response. `0` means unknown and is omitted; every epoch-2
/// leader sets it, and a stream's `created_at_ms` is never `0`.
pub(crate) fn insert_incarnation(headers: &mut HeaderMap, incarnation: u64) {
    if incarnation != 0 {
        insert_u64_header(headers, HEADER_STREAM_INCARNATION, incarnation);
    }
}

pub(crate) fn insert_padded_offset(headers: &mut HeaderMap, name: &'static str, value: u64) {
    if let Ok(value) = HeaderValue::from_str(&format!("{value:020}")) {
        headers.insert(name, value);
    }
}

pub(crate) fn insert_offset(headers: &mut HeaderMap, next_offset: u64) {
    insert_padded_offset(headers, HEADER_STREAM_NEXT_OFFSET, next_offset);
}

pub(crate) fn insert_snapshot_offset(headers: &mut HeaderMap, snapshot_offset: u64) {
    insert_padded_offset(headers, HEADER_STREAM_SNAPSHOT_OFFSET, snapshot_offset);
}

pub(crate) fn insert_snapshot_digest(headers: &mut HeaderMap, digest: &str) {
    insert_header_str(headers, HEADER_STREAM_SNAPSHOT_DIGEST, digest);
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
    // a path opt into something weaker after this call — the SSE handler does
    // exactly that with `no-cache`.
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

pub(crate) fn insert_producer_ack(headers: &mut HeaderMap, producer: Option<&ProducerRequest>) {
    let Some(producer) = producer else {
        return;
    };
    insert_u64_header(headers, HEADER_PRODUCER_EPOCH, producer.producer_epoch);
    insert_u64_header(headers, HEADER_PRODUCER_SEQ, producer.producer_seq);
}

pub(crate) fn insert_producer_error_headers(headers: &mut HeaderMap, err: &RuntimeError) {
    for context in err.stream_error_context() {
        match context {
            StreamErrorContext::ProducerEpochStale { current_epoch } => {
                insert_u64_header(headers, HEADER_PRODUCER_EPOCH, *current_epoch);
            }
            StreamErrorContext::ProducerSeqConflict {
                expected_seq,
                received_seq,
            } => {
                insert_u64_header(headers, "producer-expected-seq", *expected_seq);
                insert_u64_header(headers, "producer-received-seq", *received_seq);
            }
            StreamErrorContext::StreamClosed
            | StreamErrorContext::StaleColdFlushCandidate
            | StreamErrorContext::StreamIncarnation { .. } => {}
        }
    }
}

pub(crate) fn insert_stream_error_headers(headers: &mut HeaderMap, err: &RuntimeError) {
    if err
        .stream_error_context()
        .iter()
        .any(|context| matches!(context, StreamErrorContext::StreamClosed))
    {
        insert_static(headers, HEADER_STREAM_CLOSED, "true");
    }
    // A failed `Stream-Incarnation` precondition names the current
    // incarnation when the stream exists (D12).
    if err.stream_error_code() == Some(StreamErrorCode::IncarnationMismatch) {
        for context in err.stream_error_context() {
            if let StreamErrorContext::StreamIncarnation { incarnation } = context {
                insert_incarnation(headers, *incarnation);
            }
        }
    }
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

pub(crate) fn render_metrics(
    snapshot: RuntimeMetricsSnapshot,
    mailbox: RuntimeMailboxSnapshot,
    http: HttpMetricsSnapshot,
    raft_groups: &[RaftGroupMetricsSnapshot],
    cold_store: Option<&ColdStoreInfo>,
) -> Value {
    // The bulk of the metrics object is the runtime + HTTP snapshots flattened
    // in verbatim (their field names are the wire keys, kept in sync by the
    // compiler). Only the derived/aggregate fields are spelled out here.
    #[derive(serde::Serialize)]
    struct MetricsView<'a> {
        #[serde(flatten)]
        runtime: &'a RuntimeMetricsSnapshot,
        active_cores: usize,
        active_groups: usize,
        #[serde(flatten)]
        http: &'a HttpMetricsSnapshot,
        #[serde(flatten)]
        raft_grpc: &'a RaftGrpcMetricsSnapshot,
        mailbox_depths: &'a [usize],
        mailbox_capacities: &'a [usize],
        cold_store: Value,
        raft_group_count: usize,
        raft_groups: Value,
    }

    let active_cores = snapshot
        .per_core_appends
        .iter()
        .filter(|appends| **appends > 0)
        .count();
    let active_groups = snapshot
        .per_group_appends
        .iter()
        .filter(|appends| **appends > 0)
        .count();

    let raft_grpc = raft_grpc_metrics_snapshot();
    let view = MetricsView {
        runtime: &snapshot,
        active_cores,
        active_groups,
        http: &http,
        raft_grpc: &raft_grpc,
        mailbox_depths: &mailbox.depths,
        mailbox_capacities: &mailbox.capacities,
        cold_store: render_cold_store_info(cold_store),
        raft_group_count: raft_groups.len(),
        raft_groups: render_raft_group_metrics_array(raft_groups),
    };
    serde_json::to_value(&view).unwrap_or(Value::Null)
}

pub(crate) fn render_cold_store_info(value: Option<&ColdStoreInfo>) -> Value {
    let Some(value) = value else {
        return json!({
            "backend": "none",
            "root": null,
            "bucket": null,
            "region": null,
            "endpoint": null,
            "encryption": null,
        });
    };
    json!({
        "backend": value.backend,
        "root": value.root,
        "bucket": value.bucket,
        "region": value.region,
        "endpoint": value.endpoint,
        "encryption": value.encryption,
    })
}

pub(crate) fn render_raft_group_metrics_array(values: &[RaftGroupMetricsSnapshot]) -> Value {
    Value::Array(
        values
            .iter()
            .map(|value| {
                json!({
                    "raft_group_id": value.raft_group_id,
                    "node_id": value.node_id,
                    "current_term": value.current_term,
                    "current_leader": value.current_leader,
                    "last_log_index": value.last_log_index,
                    "committed_term": value.committed.map(|progress| progress.term),
                    "committed_index": value.committed.map(|progress| progress.index),
                    "last_applied_term": value.last_applied.map(|progress| progress.term),
                    "last_applied_index": value.last_applied.map(|progress| progress.index),
                    "snapshot_term": value.snapshot.map(|progress| progress.term),
                    "snapshot_index": value.snapshot.map(|progress| progress.index),
                    "purged_term": value.purged.map(|progress| progress.term),
                    "purged_index": value.purged.map(|progress| progress.index),
                    "voter_ids": value.voter_ids,
                    "learner_ids": value.learner_ids,
                    "maintenance": value.maintenance,
                    // F12e cadence inputs (bounded-state §7.5 soak gauges).
                    "log_bytes_since_snapshot": value.log.log_bytes,
                    "log_entries_since_snapshot": value.log.log_entries,
                    "last_snapshot_bytes": value.log.last_snapshot_bytes,
                    "has_snapshot": value.log.has_snapshot,
                })
            })
            .collect(),
    )
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

pub(crate) fn read_etag(response: &ReadStreamResponse) -> String {
    let mut hasher = DefaultHasher::new();
    response.offset.hash(&mut hasher);
    response.next_offset.hash(&mut hasher);
    response.content_type.hash(&mut hasher);
    response.payload.hash(&mut hasher);
    response.up_to_date.hash(&mut hasher);
    response.closed.hash(&mut hasher);
    format!("\"{:016x}\"", hasher.finish())
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
    let etag = read_etag(&response);
    if let Ok(value) = HeaderValue::from_str(&etag) {
        headers.insert(ETAG, value);
    }
    if request_headers
        .get(IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.split(',').any(|part| part.trim() == etag))
    {
        return (StatusCode::NOT_MODIFIED, headers).into_response();
    }
    if response.up_to_date {
        insert_static(&mut headers, HEADER_STREAM_UP_TO_DATE, "true");
    }
    let closed_at_tail = response.closed && response.up_to_date;
    if closed_at_tail {
        insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
    } else if request_cursor.is_some() {
        insert_cursor(
            &mut headers,
            response_cursor(response.next_offset, request_cursor),
        );
    }
    (StatusCode::OK, headers, response.payload).into_response()
}

pub(crate) fn snapshot_response(response: ReadSnapshotResponse) -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(&mut headers, &response.content_type);
    insert_snapshot_offset(&mut headers, response.snapshot_offset);
    insert_snapshot_digest(&mut headers, &response.snapshot_digest);
    insert_offset(&mut headers, response.next_offset);
    insert_incarnation(&mut headers, response.incarnation);
    if response.up_to_date {
        insert_static(&mut headers, HEADER_STREAM_UP_TO_DATE, "true");
    }
    (StatusCode::OK, headers, response.payload).into_response()
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
        None => insert_static(&mut headers, HEADER_STREAM_SNAPSHOT_OFFSET, "-1"),
    }
    insert_offset(&mut headers, response.next_offset);
    if response.up_to_date {
        insert_static(&mut headers, HEADER_STREAM_UP_TO_DATE, "true");
    }
    if response.closed {
        insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
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
    insert_static(&mut headers, HEADER_STREAM_UP_TO_DATE, "true");
    insert_cache_control(&mut headers, "no-store");
    if response.closed {
        insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
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
    insert_static(&mut headers, HEADER_STREAM_UP_TO_DATE, "true");
    if response.closed {
        insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
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

    read.payload.truncate(len);
    read.next_offset = read.offset + u64::try_from(len).expect("payload len fits u64");
    read.up_to_date = false;
}

fn sse_text_payload_len(content_type: &str, payload: &[u8]) -> usize {
    if is_json_content_type(content_type)
        && !payload.ends_with(b"\n")
        && let Some(newline) = payload.iter().rposition(|byte| *byte == b'\n')
    {
        return newline + 1;
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
    let closed_at_tail = read.closed && read.up_to_date;
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
    use axum::http::header::CACHE_CONTROL;

    use super::insert_cache_control;
    use super::insert_default_response_headers;

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

    /// The SSE handler needs `no-cache`, not `no-store`, and gets it by calling
    /// the default helper first and overriding after. Reversing that order would
    /// silently restore `no-store` on live tails, so the contract is asserted
    /// rather than left to call-site discipline.
    #[test]
    fn a_later_cache_directive_overrides_the_default() {
        let mut headers = HeaderMap::new();

        insert_default_response_headers(&mut headers);
        insert_cache_control(&mut headers, "no-cache");

        assert_eq!(headers.get(CACHE_CONTROL).unwrap(), "no-cache");
    }
}
