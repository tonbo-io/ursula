use axum::body::Bytes;
use axum::http::StatusCode;
use axum::http::header::{HeaderMap, HeaderValue};
use axum::response::Response;
use base64::Engine;
use ursula_raft::RaftGroupMetricsSnapshot;
use ursula_runtime::{
    AppendResponse, BootstrapStreamResponse, ProducerRequest, ReadSnapshotResponse,
    ReadStreamResponse, RuntimeError, RuntimeMailboxSnapshot, RuntimeMetricsSnapshot,
};
use ursula_shard::BucketStreamId;

use crate::*;

pub(crate) fn runtime_error_status(err: &RuntimeError) -> StatusCode {
    match err {
        RuntimeError::EmptyAppend
        | RuntimeError::InvalidRaftGroup { .. }
        | RuntimeError::SnapshotPlacementMismatch { .. } => StatusCode::BAD_REQUEST,
        RuntimeError::InvalidConfig(_)
        | RuntimeError::ColdStoreConfig { .. }
        | RuntimeError::ColdStoreIo { .. }
        | RuntimeError::MailboxClosed { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        RuntimeError::ResponseDropped { .. } | RuntimeError::SpawnCoreThread { .. } => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
        RuntimeError::LiveReadBackpressure { .. } => StatusCode::SERVICE_UNAVAILABLE,
        RuntimeError::GroupEngine { message, .. } => stream_error_status(message),
    }
}

pub(crate) fn stream_error_status(message: &str) -> StatusCode {
    if message.contains("ColdBackpressure") {
        StatusCode::SERVICE_UNAVAILABLE
    } else if message.contains("StreamGone") {
        StatusCode::GONE
    } else if message.contains("NotFound") {
        StatusCode::NOT_FOUND
    } else if message.contains("ContentTypeMismatch")
        || message.contains("StreamAlreadyExistsConflict")
        || message.contains("StreamClosed")
        || message.contains("StreamSeqConflict")
        || message.contains("SnapshotConflict")
        || message.contains("ProducerSeqConflict")
    {
        StatusCode::CONFLICT
    } else if message.contains("ProducerEpochStale") {
        StatusCode::FORBIDDEN
    } else if message.contains("Invalid") || message.contains("EmptyAppend") {
        StatusCode::BAD_REQUEST
    } else if message.contains("OffsetOutOfRange") {
        StatusCode::RANGE_NOT_SATISFIABLE
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

pub(crate) fn insert_offset(headers: &mut HeaderMap, next_offset: u64) {
    if let Ok(value) = HeaderValue::from_str(&format!("{next_offset:020}")) {
        headers.insert(HEADER_STREAM_NEXT_OFFSET, value);
    }
}

pub(crate) fn insert_snapshot_offset(headers: &mut HeaderMap, snapshot_offset: u64) {
    if let Ok(value) = HeaderValue::from_str(&format!("{snapshot_offset:020}")) {
        headers.insert(HEADER_STREAM_SNAPSHOT_OFFSET, value);
    }
}

pub(crate) fn insert_cursor(headers: &mut HeaderMap, cursor: u64) {
    if let Ok(value) = HeaderValue::from_str(&format!("{cursor:020}")) {
        headers.insert(HEADER_STREAM_CURSOR, value);
    }
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
    let RuntimeError::GroupEngine { message, .. } = err else {
        return;
    };
    if let Some(current_epoch) = parse_u64_after(message, "current epoch is ") {
        insert_u64_header(headers, HEADER_PRODUCER_EPOCH, current_epoch);
    }
    if let Some(expected_seq) = parse_u64_after(message, "expected sequence ") {
        insert_u64_header(headers, "producer-expected-seq", expected_seq);
    }
    if let Some(received_seq) = parse_u64_after(message, "received ") {
        insert_u64_header(headers, "producer-received-seq", received_seq);
    }
}

pub(crate) fn insert_stream_error_headers(headers: &mut HeaderMap, err: &RuntimeError) {
    let RuntimeError::GroupEngine { message, .. } = err else {
        return;
    };
    if message.contains("StreamClosed") || message.contains(" is closed") {
        insert_static(headers, HEADER_STREAM_CLOSED, "true");
    }
}

pub(crate) fn insert_stream_error_offset(headers: &mut HeaderMap, err: &RuntimeError) {
    let RuntimeError::GroupEngine {
        next_offset: Some(next_offset),
        ..
    } = err
    else {
        return;
    };
    insert_offset(headers, *next_offset);
}

pub(crate) fn parse_u64_after(message: &str, marker: &str) -> Option<u64> {
    let start = message.find(marker)? + marker.len();
    let digits = message[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    if digits.is_empty() {
        return None;
    }
    digits.parse().ok()
}

pub(crate) fn insert_u64_header(headers: &mut HeaderMap, name: &'static str, value: u64) {
    if let Ok(value) = HeaderValue::from_str(&value.to_string()) {
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
    const OK_ACK: &str = "{\"status\":204}";
    if results.iter().all(Result::is_ok) {
        let mut body = String::with_capacity(2 + results.len().saturating_mul(OK_ACK.len() + 1));
        body.push('[');
        for index in 0..results.len() {
            if index > 0 {
                body.push(',');
            }
            body.push_str(OK_ACK);
        }
        body.push(']');
        return body;
    }

    let mut body = String::with_capacity(2 + results.len().saturating_mul(OK_ACK.len() + 1));
    body.push('[');
    for (index, result) in results.iter().enumerate() {
        if index > 0 {
            body.push(',');
        }
        let status = match result {
            Ok(_) => StatusCode::NO_CONTENT.as_u16(),
            Err(err) => runtime_error_status(err).as_u16(),
        };
        body.push_str("{\"status\":");
        body.push_str(&status.to_string());
        body.push('}');
    }
    body.push(']');
    body
}

pub(crate) fn render_metrics(
    snapshot: RuntimeMetricsSnapshot,
    mailbox: RuntimeMailboxSnapshot,
    http: HttpMetricsSnapshot,
    raft_groups: &[RaftGroupMetricsSnapshot],
) -> String {
    let active_cores = active_count(&snapshot.per_core_appends);
    let active_groups = active_count(&snapshot.per_group_appends);
    let mut body = String::from("{");
    body.push_str("\"accepted_appends\":");
    body.push_str(&snapshot.accepted_appends.to_string());
    body.push_str(",\"active_cores\":");
    body.push_str(&active_cores.to_string());
    body.push_str(",\"active_groups\":");
    body.push_str(&active_groups.to_string());
    body.push_str(",\"per_core_appends\":");
    body.push_str(&render_u64_array(&snapshot.per_core_appends));
    body.push_str(",\"per_group_appends\":");
    body.push_str(&render_u64_array(&snapshot.per_group_appends));
    body.push_str(",\"applied_mutations\":");
    body.push_str(&snapshot.applied_mutations.to_string());
    body.push_str(",\"per_core_applied_mutations\":");
    body.push_str(&render_u64_array(&snapshot.per_core_applied_mutations));
    body.push_str(",\"per_group_applied_mutations\":");
    body.push_str(&render_u64_array(&snapshot.per_group_applied_mutations));
    body.push_str(",\"mutation_apply_ns\":");
    body.push_str(&snapshot.mutation_apply_ns.to_string());
    body.push_str(",\"per_core_mutation_apply_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_core_mutation_apply_ns));
    body.push_str(",\"per_group_mutation_apply_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_group_mutation_apply_ns));
    body.push_str(",\"group_lock_wait_ns\":");
    body.push_str(&snapshot.group_lock_wait_ns.to_string());
    body.push_str(",\"per_core_group_lock_wait_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_core_group_lock_wait_ns));
    body.push_str(",\"per_group_group_lock_wait_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_group_group_lock_wait_ns));
    body.push_str(",\"group_engine_exec_ns\":");
    body.push_str(&snapshot.group_engine_exec_ns.to_string());
    body.push_str(",\"per_core_group_engine_exec_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_core_group_engine_exec_ns));
    body.push_str(",\"per_group_group_engine_exec_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_group_group_engine_exec_ns));
    body.push_str(",\"group_mailbox_depth\":");
    body.push_str(&snapshot.group_mailbox_depth.to_string());
    body.push_str(",\"per_group_group_mailbox_depth\":");
    body.push_str(&render_u64_array(&snapshot.per_group_group_mailbox_depth));
    body.push_str(",\"group_mailbox_max_depth\":");
    body.push_str(&snapshot.group_mailbox_max_depth.to_string());
    body.push_str(",\"per_group_group_mailbox_max_depth\":");
    body.push_str(&render_u64_array(
        &snapshot.per_group_group_mailbox_max_depth,
    ));
    body.push_str(",\"group_mailbox_full_events\":");
    body.push_str(&snapshot.group_mailbox_full_events.to_string());
    body.push_str(",\"per_group_group_mailbox_full_events\":");
    body.push_str(&render_u64_array(
        &snapshot.per_group_group_mailbox_full_events,
    ));
    body.push_str(",\"raft_write_many_batches\":");
    body.push_str(&snapshot.raft_write_many_batches.to_string());
    body.push_str(",\"per_core_raft_write_many_batches\":");
    body.push_str(&render_u64_array(
        &snapshot.per_core_raft_write_many_batches,
    ));
    body.push_str(",\"per_group_raft_write_many_batches\":");
    body.push_str(&render_u64_array(
        &snapshot.per_group_raft_write_many_batches,
    ));
    body.push_str(",\"raft_write_many_commands\":");
    body.push_str(&snapshot.raft_write_many_commands.to_string());
    body.push_str(",\"per_core_raft_write_many_commands\":");
    body.push_str(&render_u64_array(
        &snapshot.per_core_raft_write_many_commands,
    ));
    body.push_str(",\"per_group_raft_write_many_commands\":");
    body.push_str(&render_u64_array(
        &snapshot.per_group_raft_write_many_commands,
    ));
    body.push_str(",\"raft_write_many_logical_commands\":");
    body.push_str(&snapshot.raft_write_many_logical_commands.to_string());
    body.push_str(",\"per_core_raft_write_many_logical_commands\":");
    body.push_str(&render_u64_array(
        &snapshot.per_core_raft_write_many_logical_commands,
    ));
    body.push_str(",\"per_group_raft_write_many_logical_commands\":");
    body.push_str(&render_u64_array(
        &snapshot.per_group_raft_write_many_logical_commands,
    ));
    body.push_str(",\"raft_write_many_responses\":");
    body.push_str(&snapshot.raft_write_many_responses.to_string());
    body.push_str(",\"per_core_raft_write_many_responses\":");
    body.push_str(&render_u64_array(
        &snapshot.per_core_raft_write_many_responses,
    ));
    body.push_str(",\"per_group_raft_write_many_responses\":");
    body.push_str(&render_u64_array(
        &snapshot.per_group_raft_write_many_responses,
    ));
    body.push_str(",\"raft_write_many_submit_ns\":");
    body.push_str(&snapshot.raft_write_many_submit_ns.to_string());
    body.push_str(",\"per_core_raft_write_many_submit_ns\":");
    body.push_str(&render_u64_array(
        &snapshot.per_core_raft_write_many_submit_ns,
    ));
    body.push_str(",\"per_group_raft_write_many_submit_ns\":");
    body.push_str(&render_u64_array(
        &snapshot.per_group_raft_write_many_submit_ns,
    ));
    body.push_str(",\"raft_write_many_response_ns\":");
    body.push_str(&snapshot.raft_write_many_response_ns.to_string());
    body.push_str(",\"per_core_raft_write_many_response_ns\":");
    body.push_str(&render_u64_array(
        &snapshot.per_core_raft_write_many_response_ns,
    ));
    body.push_str(",\"per_group_raft_write_many_response_ns\":");
    body.push_str(&render_u64_array(
        &snapshot.per_group_raft_write_many_response_ns,
    ));
    body.push_str(",\"raft_apply_entries\":");
    body.push_str(&snapshot.raft_apply_entries.to_string());
    body.push_str(",\"per_core_raft_apply_entries\":");
    body.push_str(&render_u64_array(&snapshot.per_core_raft_apply_entries));
    body.push_str(",\"per_group_raft_apply_entries\":");
    body.push_str(&render_u64_array(&snapshot.per_group_raft_apply_entries));
    body.push_str(",\"raft_apply_ns\":");
    body.push_str(&snapshot.raft_apply_ns.to_string());
    body.push_str(",\"per_core_raft_apply_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_core_raft_apply_ns));
    body.push_str(",\"per_group_raft_apply_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_group_raft_apply_ns));
    body.push_str(",\"live_read_waiters\":");
    body.push_str(&snapshot.live_read_waiters.to_string());
    body.push_str(",\"per_core_live_read_waiters\":");
    body.push_str(&render_u64_array(&snapshot.per_core_live_read_waiters));
    body.push_str(",\"live_read_backpressure_events\":");
    body.push_str(&snapshot.live_read_backpressure_events.to_string());
    body.push_str(",\"per_core_live_read_backpressure_events\":");
    body.push_str(&render_u64_array(
        &snapshot.per_core_live_read_backpressure_events,
    ));
    body.push_str(",\"sse_streams_opened\":");
    body.push_str(&http.sse_streams_opened.to_string());
    body.push_str(",\"sse_read_iterations\":");
    body.push_str(&http.sse_read_iterations.to_string());
    body.push_str(",\"sse_data_events\":");
    body.push_str(&http.sse_data_events.to_string());
    body.push_str(",\"sse_control_events\":");
    body.push_str(&http.sse_control_events.to_string());
    body.push_str(",\"sse_error_events\":");
    body.push_str(&http.sse_error_events.to_string());
    body.push_str(",\"routed_requests\":");
    body.push_str(&snapshot.routed_requests.to_string());
    body.push_str(",\"per_core_routed_requests\":");
    body.push_str(&render_u64_array(&snapshot.per_core_routed_requests));
    body.push_str(",\"mailbox_send_wait_ns\":");
    body.push_str(&snapshot.mailbox_send_wait_ns.to_string());
    body.push_str(",\"per_core_mailbox_send_wait_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_core_mailbox_send_wait_ns));
    body.push_str(",\"mailbox_full_events\":");
    body.push_str(&snapshot.mailbox_full_events.to_string());
    body.push_str(",\"per_core_mailbox_full_events\":");
    body.push_str(&render_u64_array(&snapshot.per_core_mailbox_full_events));
    body.push_str(",\"wal_batches\":");
    body.push_str(&snapshot.wal_batches.to_string());
    body.push_str(",\"per_core_wal_batches\":");
    body.push_str(&render_u64_array(&snapshot.per_core_wal_batches));
    body.push_str(",\"per_group_wal_batches\":");
    body.push_str(&render_u64_array(&snapshot.per_group_wal_batches));
    body.push_str(",\"wal_records\":");
    body.push_str(&snapshot.wal_records.to_string());
    body.push_str(",\"per_core_wal_records\":");
    body.push_str(&render_u64_array(&snapshot.per_core_wal_records));
    body.push_str(",\"per_group_wal_records\":");
    body.push_str(&render_u64_array(&snapshot.per_group_wal_records));
    body.push_str(",\"wal_write_ns\":");
    body.push_str(&snapshot.wal_write_ns.to_string());
    body.push_str(",\"per_core_wal_write_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_core_wal_write_ns));
    body.push_str(",\"per_group_wal_write_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_group_wal_write_ns));
    body.push_str(",\"wal_sync_ns\":");
    body.push_str(&snapshot.wal_sync_ns.to_string());
    body.push_str(",\"per_core_wal_sync_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_core_wal_sync_ns));
    body.push_str(",\"per_group_wal_sync_ns\":");
    body.push_str(&render_u64_array(&snapshot.per_group_wal_sync_ns));
    body.push_str(",\"cold_flush_uploads\":");
    body.push_str(&snapshot.cold_flush_uploads.to_string());
    body.push_str(",\"cold_flush_upload_bytes\":");
    body.push_str(&snapshot.cold_flush_upload_bytes.to_string());
    body.push_str(",\"cold_flush_upload_ns\":");
    body.push_str(&snapshot.cold_flush_upload_ns.to_string());
    body.push_str(",\"cold_flush_publishes\":");
    body.push_str(&snapshot.cold_flush_publishes.to_string());
    body.push_str(",\"cold_flush_publish_bytes\":");
    body.push_str(&snapshot.cold_flush_publish_bytes.to_string());
    body.push_str(",\"cold_flush_publish_ns\":");
    body.push_str(&snapshot.cold_flush_publish_ns.to_string());
    body.push_str(",\"cold_orphan_cleanup_attempts\":");
    body.push_str(&snapshot.cold_orphan_cleanup_attempts.to_string());
    body.push_str(",\"cold_orphan_cleanup_errors\":");
    body.push_str(&snapshot.cold_orphan_cleanup_errors.to_string());
    body.push_str(",\"cold_orphan_bytes\":");
    body.push_str(&snapshot.cold_orphan_bytes.to_string());
    body.push_str(",\"cold_hot_bytes\":");
    body.push_str(&snapshot.cold_hot_bytes.to_string());
    body.push_str(",\"per_group_cold_hot_bytes\":");
    body.push_str(&render_u64_array(&snapshot.per_group_cold_hot_bytes));
    body.push_str(",\"cold_hot_group_bytes_max\":");
    body.push_str(&snapshot.cold_hot_group_bytes_max.to_string());
    body.push_str(",\"per_group_cold_hot_bytes_max\":");
    body.push_str(&render_u64_array(&snapshot.per_group_cold_hot_bytes_max));
    body.push_str(",\"cold_hot_stream_bytes_max\":");
    body.push_str(&snapshot.cold_hot_stream_bytes_max.to_string());
    body.push_str(",\"cold_backpressure_events\":");
    body.push_str(&snapshot.cold_backpressure_events.to_string());
    body.push_str(",\"per_core_cold_backpressure_events\":");
    body.push_str(&render_u64_array(
        &snapshot.per_core_cold_backpressure_events,
    ));
    body.push_str(",\"per_group_cold_backpressure_events\":");
    body.push_str(&render_u64_array(
        &snapshot.per_group_cold_backpressure_events,
    ));
    body.push_str(",\"cold_backpressure_bytes\":");
    body.push_str(&snapshot.cold_backpressure_bytes.to_string());
    body.push_str(",\"mailbox_depths\":");
    body.push_str(&render_usize_array(&mailbox.depths));
    body.push_str(",\"mailbox_capacities\":");
    body.push_str(&render_usize_array(&mailbox.capacities));
    body.push_str(",\"raft_group_count\":");
    body.push_str(&raft_groups.len().to_string());
    body.push_str(",\"raft_groups\":");
    body.push_str(&render_raft_group_metrics_array(raft_groups));
    body.push('}');
    body
}

pub(crate) fn active_count(values: &[u64]) -> usize {
    values.iter().filter(|value| **value > 0).count()
}

pub(crate) fn render_u64_array(values: &[u64]) -> String {
    let mut body = String::from("[");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            body.push(',');
        }
        body.push_str(&value.to_string());
    }
    body.push(']');
    body
}

pub(crate) fn render_usize_array(values: &[usize]) -> String {
    let mut body = String::from("[");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            body.push(',');
        }
        body.push_str(&value.to_string());
    }
    body.push(']');
    body
}

pub(crate) fn render_raft_group_metrics_array(values: &[RaftGroupMetricsSnapshot]) -> String {
    let mut body = String::from("[");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            body.push(',');
        }
        body.push('{');
        body.push_str("\"raft_group_id\":");
        body.push_str(&value.raft_group_id.to_string());
        body.push_str(",\"node_id\":");
        body.push_str(&value.node_id.to_string());
        body.push_str(",\"current_term\":");
        body.push_str(&value.current_term.to_string());
        body.push_str(",\"current_leader\":");
        push_optional_u64(&mut body, value.current_leader);
        body.push_str(",\"last_log_index\":");
        push_optional_u64(&mut body, value.last_log_index);
        push_optional_log_progress(&mut body, "committed", value.committed);
        push_optional_log_progress(&mut body, "last_applied", value.last_applied);
        push_optional_log_progress(&mut body, "snapshot", value.snapshot);
        push_optional_log_progress(&mut body, "purged", value.purged);
        body.push_str(",\"voter_ids\":");
        body.push_str(&render_u64_array(&value.voter_ids));
        body.push_str(",\"learner_ids\":");
        body.push_str(&render_u64_array(&value.learner_ids));
        body.push('}');
    }
    body.push(']');
    body
}

pub(crate) fn push_optional_log_progress(
    body: &mut String,
    name: &str,
    progress: Option<RaftLogProgressSnapshot>,
) {
    body.push_str(",\"");
    body.push_str(name);
    body.push_str("_term\":");
    push_optional_u64(body, progress.map(|value| value.term));
    body.push_str(",\"");
    body.push_str(name);
    body.push_str("_index\":");
    push_optional_u64(body, progress.map(|value| value.index));
}

pub(crate) fn push_optional_u64(body: &mut String, value: Option<u64>) {
    match value {
        Some(value) => body.push_str(&value.to_string()),
        None => body.push_str("null"),
    }
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
    let payload = match project_http_read_payload(&response.content_type, &response.payload) {
        Ok(payload) => payload,
        Err(message) => return (StatusCode::INTERNAL_SERVER_ERROR, message).into_response(),
    };
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(&mut headers, &response.content_type);
    insert_offset(&mut headers, response.next_offset);
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
    (StatusCode::OK, headers, payload).into_response()
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
    insert_content_type(&mut headers, &response.content_type);
    insert_offset(&mut headers, response.next_offset);
    insert_static(&mut headers, HEADER_STREAM_UP_TO_DATE, "true");
    insert_cache_control(&mut headers, "no-store");
    if response.closed {
        insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
    }
    let body = if is_json_content_type(&response.content_type) {
        Bytes::from_static(b"[]")
    } else {
        Bytes::new()
    };
    (StatusCode::OK, headers, body).into_response()
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

pub(crate) fn project_http_read_payload(
    content_type: &str,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    if !is_json_content_type(content_type) {
        return Ok(payload.to_vec());
    }

    let mut out = Vec::with_capacity(payload.len().saturating_add(2));
    out.push(b'[');
    let mut first = true;
    let mut idx = 0usize;
    while idx < payload.len() {
        let Some(rel_end) = payload[idx..].iter().position(|byte| *byte == b'\n') else {
            return Err(format!("invalid JSON payload boundary at byte {idx}"));
        };
        let line_end = idx + rel_end;
        let line = &payload[idx..line_end];
        if !line.is_empty() {
            serde_json::from_slice::<serde_json::Value>(line)
                .map_err(|err| format!("invalid stored JSON message at byte {idx}: {err}"))?;
            if !first {
                out.push(b',');
            }
            out.extend_from_slice(line);
            first = false;
        }
        idx = line_end + 1;
    }
    out.push(b']');
    Ok(out)
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
        } else if is_json_content_type(&read.content_type) {
            match project_http_read_payload(&read.content_type, &read.payload) {
                Ok(payload) => String::from_utf8_lossy(&payload).into_owned(),
                Err(message) => message,
            }
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
