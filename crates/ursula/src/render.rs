use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use axum::body::Bytes;
use axum::http::StatusCode;
use axum::http::header::CACHE_CONTROL;
use axum::http::header::CONTENT_TYPE;
use axum::http::header::ETAG;
use axum::http::header::HOST;
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
use ursula_runtime::AppendResponse;
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
use crate::HEADER_STREAM_NEXT_OFFSET;
use crate::HEADER_STREAM_SNAPSHOT_OFFSET;
use crate::HEADER_STREAM_TTL;
use crate::HEADER_STREAM_UP_TO_DATE;
use crate::HEADER_X_CONTENT_TYPE_OPTIONS;
use crate::HttpMetricsSnapshot;
use crate::insert_record_extension;
use crate::insert_record_head_headers;
use crate::insert_record_operation_headers;

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
        StreamErrorCode::BucketNotFound
        | StreamErrorCode::StreamNotFound
        | StreamErrorCode::SnapshotNotFound => StatusCode::NOT_FOUND,
        StreamErrorCode::ContentTypeMismatch
        | StreamErrorCode::StreamAlreadyExistsConflict
        | StreamErrorCode::StreamClosed
        | StreamErrorCode::StreamSeqConflict
        | StreamErrorCode::SnapshotConflict
        | StreamErrorCode::ProducerSeqConflict => StatusCode::CONFLICT,
        StreamErrorCode::RecordPreconditionFailed => StatusCode::PRECONDITION_FAILED,
        StreamErrorCode::ProducerEpochStale => StatusCode::FORBIDDEN,
        StreamErrorCode::OffsetOutOfRange => StatusCode::RANGE_NOT_SATISFIABLE,
        StreamErrorCode::InvalidBucketId
        | StreamErrorCode::InvalidStreamId
        | StreamErrorCode::BucketNotEmpty
        | StreamErrorCode::MissingContentType
        | StreamErrorCode::EmptyAppend
        | StreamErrorCode::InvalidProducer
        | StreamErrorCode::InvalidRetention
        | StreamErrorCode::InvalidColdFlush
        | StreamErrorCode::InvalidSnapshot
        | StreamErrorCode::InvalidStreamAttrs
        | StreamErrorCode::InvalidRecordBoundaries => StatusCode::BAD_REQUEST,
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
            StreamErrorContext::StreamClosed | StreamErrorContext::StaleColdFlushCandidate => {}
            StreamErrorContext::RecordTailMismatch { current_record } => {
                insert_record_extension(headers);
                insert_u64_header(headers, crate::HEADER_STREAM_RECORD_NEXT, *current_record);
            }
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
    for context in err.stream_error_context() {
        if let StreamErrorContext::RecordTailMismatch { current_record } = context {
            insert_record_extension(headers);
            insert_u64_header(headers, crate::HEADER_STREAM_RECORD_NEXT, *current_record);
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

pub(crate) fn insert_public_location(
    headers: &mut HeaderMap,
    request_headers: &HeaderMap,
    path: &str,
) {
    let location = request_headers
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .filter(|host| !host.trim().is_empty())
        .map(|host| format!("http://{host}{path}"))
        .unwrap_or_else(|| path.to_owned());
    if let Ok(value) = HeaderValue::from_str(&location) {
        headers.insert(LOCATION, value);
    }
}

pub(crate) fn insert_static(headers: &mut HeaderMap, name: &'static str, value: &'static str) {
    headers.insert(name, HeaderValue::from_static(value));
}

pub(crate) fn parse_append_batch(body: &Bytes) -> Result<Vec<Bytes>, String> {
    let mut payloads = Vec::new();
    let mut cursor = 0usize;
    while cursor < body.len() {
        let Some(header_end) = cursor.checked_add(4) else {
            return Err("append batch frame offset overflow".to_owned());
        };
        if header_end > body.len() {
            return Err("append batch frame is missing length header".to_owned());
        }
        let len = u32::from_be_bytes(
            body[cursor..header_end]
                .try_into()
                .expect("slice length is exactly 4"),
        ) as usize;
        cursor = header_end;
        let Some(payload_end) = cursor.checked_add(len) else {
            return Err("append batch payload length overflow".to_owned());
        };
        if payload_end > body.len() {
            return Err("append batch frame payload is truncated".to_owned());
        }
        payloads.push(body.slice(cursor..payload_end));
        cursor = payload_end;
    }
    if payloads.is_empty() {
        return Err("append batch must contain at least one frame".to_owned());
    }
    Ok(payloads)
}

pub(crate) fn render_batch_results(results: &[Result<AppendResponse, RuntimeError>]) -> String {
    let acks = results
        .iter()
        .map(|result| {
            let status = match result {
                Ok(_) => StatusCode::NO_CONTENT.as_u16(),
                Err(err) => runtime_error_status(err).as_u16(),
            };
            match result {
                Ok(response) => match response.record_range {
                    Some(range) => json!({
                        "status": status,
                        "stream_record_start": range.first_record,
                        "stream_record_next": range.next_record,
                    }),
                    None => json!({ "status": status }),
                },
                Err(_) => json!({ "status": status }),
            }
        })
        .collect::<Vec<Value>>();
    Value::Array(acks).to_string()
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

    let view = MetricsView {
        runtime: &snapshot,
        active_cores,
        active_groups,
        http: &http,
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
        });
    };
    json!({
        "backend": value.backend,
        "root": value.root,
        "bucket": value.bucket,
        "region": value.region,
        "endpoint": value.endpoint,
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
    read_response_with_etag(response, request_headers, request_cursor, None)
}

pub(crate) fn record_envelope_response(
    mut response: ReadStreamResponse,
    request_headers: &HeaderMap,
    request_cursor: Option<&str>,
) -> Response {
    let canonical_etag = read_etag(&response);
    if let Err(message) = apply_record_envelope(&mut response) {
        return (StatusCode::INTERNAL_SERVER_ERROR, message).into_response();
    }
    read_response_with_etag(
        response,
        request_headers,
        request_cursor,
        Some(canonical_etag),
    )
}

pub(crate) fn apply_record_envelope(response: &mut ReadStreamResponse) -> Result<(), String> {
    let Some(record_range) = response.record_range else {
        return Err("missing record range".to_owned());
    };
    let mut record = record_range.first_record;
    let mut payload = Vec::new();
    for line in response.payload.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let value = match serde_json::from_slice::<serde_json::Value>(line) {
            Ok(value) => value,
            Err(err) => return Err(format!("decode canonical JSON record: {err}")),
        };
        let envelope = serde_json::json!({ "record": record, "value": value });
        let encoded = match serde_json::to_vec(&envelope) {
            Ok(encoded) => encoded,
            Err(err) => return Err(format!("encode record envelope: {err}")),
        };
        payload.extend_from_slice(&encoded);
        payload.push(b'\n');
        record = record.saturating_add(1);
    }
    if record != record_range.next_record {
        return Err("record envelope count does not match coordinate range".to_owned());
    }
    response.payload = payload;
    response.content_type = "application/vnd.durable-stream-records+ndjson".to_owned();
    Ok(())
}

fn read_response_with_etag(
    response: ReadStreamResponse,
    request_headers: &HeaderMap,
    request_cursor: Option<&str>,
    canonical_etag: Option<String>,
) -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(&mut headers, http_read_content_type(&response.content_type));
    insert_offset(&mut headers, response.next_offset);
    if let Some(retained_record_range) = response.retained_record_range {
        insert_record_extension(&mut headers);
        if let Some(record_range) = response.record_range {
            insert_record_head_headers(&mut headers, retained_record_range);
            insert_record_operation_headers(&mut headers, record_range);
        }
    }
    let etag = canonical_etag.unwrap_or_else(|| read_etag(&response));
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
    insert_offset(&mut headers, response.next_offset);
    if response.up_to_date {
        insert_static(&mut headers, HEADER_STREAM_UP_TO_DATE, "true");
    }
    (StatusCode::OK, headers, response.payload).into_response()
}

pub(crate) fn bootstrap_response(response: BootstrapStreamResponse) -> Response {
    let boundary = bootstrap_boundary(&response);
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(
        &mut headers,
        &format!("multipart/mixed; boundary={boundary}"),
    );
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
    (
        StatusCode::OK,
        headers,
        render_bootstrap_multipart(&response, &boundary),
    )
        .into_response()
}

pub(crate) fn bootstrap_boundary(response: &BootstrapStreamResponse) -> String {
    let mut hasher = DefaultHasher::new();
    response.snapshot_offset.hash(&mut hasher);
    response.next_offset.hash(&mut hasher);
    response.updates.len().hash(&mut hasher);
    response.snapshot_payload.len().hash(&mut hasher);
    format!("ursula-bootstrap-{:016x}", hasher.finish())
}

pub(crate) fn render_bootstrap_multipart(
    response: &BootstrapStreamResponse,
    boundary: &str,
) -> Vec<u8> {
    let mut body = Vec::new();
    push_multipart_part(
        &mut body,
        boundary,
        &response.snapshot_content_type,
        &response.snapshot_payload,
    );
    for update in &response.updates {
        push_multipart_part(&mut body, boundary, &update.content_type, &update.payload);
    }
    body.extend_from_slice(b"--");
    body.extend_from_slice(boundary.as_bytes());
    body.extend_from_slice(b"--\r\n");
    body
}

pub(crate) fn push_multipart_part(
    body: &mut Vec<u8>,
    boundary: &str,
    content_type: &str,
    payload: &[u8],
) {
    body.extend_from_slice(b"--");
    body.extend_from_slice(boundary.as_bytes());
    body.extend_from_slice(b"\r\nContent-Type: ");
    body.extend_from_slice(content_type.as_bytes());
    body.extend_from_slice(b"\r\n\r\n");
    body.extend_from_slice(payload);
    body.extend_from_slice(b"\r\n");
}

pub(crate) fn offset_now_response(response: ReadStreamResponse) -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(&mut headers, http_read_content_type(&response.content_type));
    insert_offset(&mut headers, response.next_offset);
    insert_static(&mut headers, HEADER_STREAM_UP_TO_DATE, "true");
    if let Some(retained_record_range) = response.retained_record_range {
        insert_record_extension(&mut headers);
        if let Some(record_range) = response.record_range {
            insert_record_head_headers(&mut headers, retained_record_range);
            insert_record_operation_headers(&mut headers, record_range);
        }
    }
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

    let value: serde_json::Value =
        serde_json::from_slice(&body).map_err(|err| format!("invalid JSON payload: {err}"))?;
    let messages = match value {
        serde_json::Value::Array(items) => {
            if items.is_empty() && !allow_empty_array {
                return Err("JSON append array must not be empty".to_owned());
            }
            items
        }
        other => vec![other],
    };

    let mut out = Vec::new();
    for message in messages {
        serde_json::to_writer(&mut out, &message)
            .map_err(|err| format!("failed to encode JSON message: {err}"))?;
        out.push(b'\n');
    }
    Ok(Bytes::from(out))
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
    if let Some(retained_record_range) = read.retained_record_range {
        body.push_str(",\"streamFirstRecord\":");
        body.push_str(&retained_record_range.first_record.to_string());
    }
    if let Some(record_range) = read.record_range {
        body.push_str(",\"streamNextRecord\":");
        body.push_str(&record_range.next_record.to_string());
    }
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
