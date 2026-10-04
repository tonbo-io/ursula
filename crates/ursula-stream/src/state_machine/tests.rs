use std::collections::HashMap;

use proptest::collection::vec;
use proptest::prelude::*;

use super::*;

const OCTET: &str = "application/octet-stream";

fn stream(id: &str) -> BucketStreamId {
    BucketStreamId::new("benchcmp", id)
}

fn bucket_usage(machine: &StreamStateMachine, bucket_id: &str) -> crate::model::BucketUsage {
    machine
        .bucket_usage_report()
        .into_iter()
        .find(|entry| entry.bucket_id == bucket_id)
        .map(|entry| entry.usage)
        .unwrap_or_default()
}

#[test]
fn usage_tracks_appends_retention_and_stream_lifecycle() {
    let mut machine = machine();
    create_stream(&mut machine, "usage");
    let usage = bucket_usage(&machine, "benchcmp");
    assert_eq!(usage.stream_count, 1);
    assert_eq!(usage.committed_append_bytes, 0);
    assert_eq!(usage.committed_write_units, 1);

    assert!(matches!(
        machine.apply(append_cmd(stream("usage"), b"abc", Append::default())),
        StreamResponse::Appended { .. }
    ));
    assert!(matches!(
        machine.apply(append_cmd(stream("usage"), b"de", Append::default())),
        StreamResponse::Appended { .. }
    ));
    let usage = bucket_usage(&machine, "benchcmp");
    assert_eq!(usage.committed_append_bytes, 5);
    assert_eq!(usage.committed_records, 2);
    assert_eq!(usage.committed_write_units, 3);
    assert_eq!(usage.retained_bytes, 5);

    assert!(matches!(
        machine.apply(publish_snapshot_cmd(
            stream("usage"),
            3,
            "application/json",
            br#"{"state":"abc"}"#,
            0
        )),
        StreamResponse::SnapshotPublished { .. }
    ));
    assert!(matches!(
        machine.apply(advance_retention_cmd(stream("usage"), 3, 1)),
        StreamResponse::RetentionAdvanced { .. }
    ));
    let usage = bucket_usage(&machine, "benchcmp");
    assert_eq!(
        usage.committed_append_bytes, 5,
        "monotonic counter survives retention"
    );
    assert_eq!(usage.retained_bytes, 2);

    assert!(matches!(
        machine.apply(delete_cmd(stream("usage"))),
        StreamResponse::Deleted
    ));
    let usage = bucket_usage(&machine, "benchcmp");
    assert_eq!(usage.stream_count, 0);
    assert_eq!(usage.retained_bytes, 0);
    assert_eq!(usage.committed_append_bytes, 5);
}

#[test]
fn usage_decodes_the_pre_contract_counter_name() {
    let usage: crate::model::BucketUsage = serde_json::from_value(serde_json::json!({
        "committed_append_bytes": 100,
        "committed_records": 7,
        "committed_write_units_10kib": 3,
        "retained_bytes": 60,
        "stream_count": 2
    }))
    .expect("legacy usage snapshot");

    assert_eq!(usage.committed_write_units, 3);
}

#[test]
fn usage_does_not_count_deduplicated_appends() {
    let mut machine = machine();
    create_stream(&mut machine, "dedup");
    let request = append_cmd(stream("dedup"), b"abcd", Append {
        producer: Some(producer("writer", 0, 0)),
        ..Append::default()
    });
    assert!(matches!(
        machine.apply(request.clone()),
        StreamResponse::Appended {
            deduplicated: false,
            ..
        }
    ));
    assert!(matches!(machine.apply(request), StreamResponse::Appended {
        deduplicated: true,
        ..
    }));
    let usage = bucket_usage(&machine, "benchcmp");
    assert_eq!(
        usage.committed_append_bytes, 4,
        "retried append counts once"
    );
    assert_eq!(usage.committed_records, 1);
    assert_eq!(usage.committed_write_units, 2);
}

#[test]
fn committed_write_units_round_each_committed_operation() {
    let mut machine = machine();
    create_stream(&mut machine, "write-units");
    assert!(matches!(
        machine.apply(append_cmd(
            stream("write-units"),
            &vec![b'x'; 10 * 1024 + 1],
            Append::default()
        )),
        StreamResponse::Appended { .. }
    ));
    assert!(matches!(
        machine.apply(append_cmd(
            stream("write-units"),
            &vec![b'y'; 10 * 1024],
            Append::default()
        )),
        StreamResponse::Appended { .. }
    ));

    assert_eq!(
        bucket_usage(&machine, "benchcmp").committed_write_units,
        4,
        "create is one unit, the oversized append is two, and an exact-unit append is one"
    );
}

#[test]
fn usage_counts_json_records_and_survives_snapshot_restore() {
    let mut machine = machine();
    assert!(matches!(
        machine.apply(create_cmd(stream("json-usage"), Create {
            content_type: "application/json",
            payload: b"{\"a\":1}\n{\"b\":2}\n".to_vec(),
            ..Create::default()
        })),
        StreamResponse::Created { .. }
    ));
    let usage = bucket_usage(&machine, "benchcmp");
    assert_eq!(usage.committed_records, 2);
    assert_eq!(usage.committed_append_bytes, 16);

    let restored =
        StreamStateMachine::restore(machine.snapshot()).expect("snapshot restores cleanly");
    assert_eq!(
        bucket_usage(&restored, "benchcmp"),
        bucket_usage(&machine, "benchcmp"),
        "usage survives a snapshot round-trip"
    );

    // TTL expiry releases the gauges but never the monotonic counters.
    assert!(matches!(
        machine.apply(create_cmd(stream("expiring"), Create {
            payload: b"xyz".to_vec(),
            ttl_seconds: Some(1),
            now_ms: 0,
            ..Create::default()
        })),
        StreamResponse::Created { .. }
    ));
    assert!(matches!(
        machine.apply(touch_cmd(stream("expiring"), 10_000)),
        StreamResponse::Accessed { expired: true, .. }
    ));
    let usage = bucket_usage(&machine, "benchcmp");
    assert_eq!(usage.stream_count, 1, "expired stream left the gauge");
    assert_eq!(usage.committed_append_bytes, 19);
    assert_eq!(usage.retained_bytes, 16);
}

/// A fresh state machine with the shared `benchcmp` bucket created.
fn machine() -> StreamStateMachine {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    machine
}

fn create_bucket(machine: &mut StreamStateMachine) {
    assert_eq!(
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: "benchcmp".to_owned(),
        }),
        StreamResponse::BucketCreated {
            bucket_id: "benchcmp".to_owned(),
        }
    );
}

/// Overridable arguments for [`create_cmd`]; defaults mirror the most common
/// inline literal (octet-stream, empty payload, no TTL, `now_ms: 0`).
#[derive(Clone)]
struct Create {
    content_type: &'static str,
    payload: Vec<u8>,
    close_after: bool,
    stream_seq: Option<String>,
    ttl_seconds: Option<u64>,
    expires_at_ms: Option<u64>,
    now_ms: u64,
}

impl Default for Create {
    fn default() -> Self {
        Self {
            content_type: OCTET,
            payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            ttl_seconds: None,
            expires_at_ms: None,
            now_ms: 0,
        }
    }
}

fn create_cmd(stream_id: BucketStreamId, args: Create) -> StreamCommand {
    StreamCommand::CreateStream {
        stream_id,
        content_type: args.content_type.to_owned(),
        initial_payload: args.payload.into(),
        close_after: args.close_after,
        stream_seq: args.stream_seq,
        producer: None,
        stream_ttl_seconds: args.ttl_seconds,
        stream_expires_at_ms: args.expires_at_ms,
        now_ms: args.now_ms,
    }
}

/// Overridable arguments for [`append_cmd`]; defaults mirror the most common
/// inline literal (octet-stream, open append, no seq/producer, `now_ms: 0`).
#[derive(Clone)]
struct Append {
    content_type: Option<&'static str>,
    close_after: bool,
    stream_seq: Option<String>,
    producer: Option<ProducerRequest>,
    now_ms: u64,
}

impl Default for Append {
    fn default() -> Self {
        Self {
            content_type: Some(OCTET),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }
    }
}

fn append_cmd(stream_id: BucketStreamId, payload: &[u8], args: Append) -> StreamCommand {
    StreamCommand::Append {
        stream_id,
        content_type: args.content_type.map(str::to_owned),
        payload: bytes::Bytes::copy_from_slice(payload),
        close_after: args.close_after,
        stream_seq: args.stream_seq,
        producer: args.producer,
        now_ms: args.now_ms,
    }
}

fn close_cmd(stream_id: BucketStreamId) -> StreamCommand {
    StreamCommand::Close {
        stream_id,
        stream_seq: None,
        producer: None,
        now_ms: 0,
    }
}

fn delete_cmd(stream_id: BucketStreamId) -> StreamCommand {
    StreamCommand::DeleteStream { stream_id }
}

fn touch_cmd(stream_id: BucketStreamId, now_ms: u64) -> StreamCommand {
    StreamCommand::TouchStreamAccess {
        stream_id,
        now_ms,
        renew_ttl: true,
    }
}

fn publish_snapshot_cmd(
    stream_id: BucketStreamId,
    snapshot_offset: u64,
    content_type: &str,
    payload: &[u8],
    now_ms: u64,
) -> StreamCommand {
    StreamCommand::PublishSnapshot {
        stream_id,
        snapshot_offset,
        content_type: content_type.to_owned(),
        payload: bytes::Bytes::copy_from_slice(payload),
        now_ms,
        expected_incarnation: None,
    }
}

fn advance_retention_cmd(
    stream_id: BucketStreamId,
    retained_offset: u64,
    now_ms: u64,
) -> StreamCommand {
    StreamCommand::AdvanceRetention {
        stream_id,
        retained_offset,
        now_ms,
        expected_incarnation: None,
    }
}

/// A `FlushCold` planned from the live incarnation of `stream_id`.
fn flush_cold_cmd(
    machine: &StreamStateMachine,
    stream_id: BucketStreamId,
    start_offset: u64,
    end_offset: u64,
    s3_path: &str,
    object_size: u64,
) -> StreamCommand {
    let cold_generation = machine
        .cold_index_generation(&stream_id)
        .unwrap_or_default();
    flush_cold_cmd_at(
        stream_id,
        cold_generation,
        start_offset,
        end_offset,
        s3_path,
        object_size,
    )
}

fn flush_cold_cmd_at(
    stream_id: BucketStreamId,
    cold_generation: u64,
    start_offset: u64,
    end_offset: u64,
    s3_path: &str,
    object_size: u64,
) -> StreamCommand {
    StreamCommand::FlushCold {
        cold_generation,
        stream_id,
        chunk: ColdChunkRef {
            start_offset,
            end_offset,
            s3_path: s3_path.to_owned(),
            object_size,
            object_offset: 0,
            shared_object: false,
            payload_digest: String::new(),
        },
    }
}

fn flush_candidate_cmd(
    stream_id: BucketStreamId,
    candidate: &ColdFlushCandidate,
    s3_path: &str,
) -> StreamCommand {
    flush_cold_cmd_at(
        stream_id,
        candidate.cold_generation,
        candidate.start_offset,
        candidate.end_offset,
        s3_path,
        u64::try_from(candidate.payload.len()).expect("payload len fits u64"),
    )
}

/// The default successful `Created` response (open stream).
fn created(stream_id: BucketStreamId, next_offset: u64) -> StreamResponse {
    StreamResponse::Created {
        stream_id,
        next_offset,
        closed: false,
    }
}

/// The default successful `Appended` response (open, not deduplicated,
/// no producer).
fn appended(offset: u64, next_offset: u64) -> StreamResponse {
    StreamResponse::Appended {
        offset,
        next_offset,
        closed: false,
        deduplicated: false,
        producer: None,
        receipt_evicted: false,
    }
}

/// An `Appended` response acknowledged for `producer`.
fn appended_by(
    producer: ProducerRequest,
    offset: u64,
    next_offset: u64,
    closed: bool,
    deduplicated: bool,
) -> StreamResponse {
    StreamResponse::Appended {
        offset,
        next_offset,
        closed,
        deduplicated,
        producer: Some(producer),
        receipt_evicted: false,
    }
}

#[track_caller]
fn assert_error_code(response: StreamResponse, code: StreamErrorCode) {
    match &response {
        StreamResponse::Error { code: actual, .. } if *actual == code => {}
        other => panic!("expected {code:?} error, got {other:?}"),
    }
}

#[track_caller]
fn assert_error_at(response: StreamResponse, code: StreamErrorCode, next_offset: u64) {
    match &response {
        StreamResponse::Error {
            code: actual,
            next_offset: actual_next,
            ..
        } if *actual == code && *actual_next == Some(next_offset) => {}
        other => panic!("expected {code:?} error at next_offset {next_offset}, got {other:?}"),
    }
}

#[track_caller]
fn assert_err_code<T: std::fmt::Debug>(result: Result<T, StreamResponse>, code: StreamErrorCode) {
    match &result {
        Err(StreamResponse::Error { code: actual, .. }) if *actual == code => {}
        other => panic!("expected {code:?} error, got {other:?}"),
    }
}

#[track_caller]
fn assert_err_at<T: std::fmt::Debug>(
    result: Result<T, StreamResponse>,
    code: StreamErrorCode,
    next_offset: u64,
) {
    match &result {
        Err(StreamResponse::Error {
            code: actual,
            next_offset: actual_next,
            ..
        }) if *actual == code && *actual_next == Some(next_offset) => {}
        other => panic!("expected {code:?} error at next_offset {next_offset}, got {other:?}"),
    }
}

fn create_stream(machine: &mut StreamStateMachine, id: &str) {
    assert_eq!(
        machine.apply(create_cmd(stream(id), Create::default())),
        created(stream(id), 0)
    );
}

/// JSON snapshot and retention offsets must follow an LF byte (PR16). A hot
/// preceding byte is checked at apply; a cold one only by the proposer,
/// whose read apply trusts when it names the live incarnation.
#[test]
fn json_snapshot_and_retention_offsets_follow_lf_boundaries() {
    let mut machine = machine();
    let stream_id = stream("json-lf");
    assert_eq!(
        machine.apply(create_cmd(stream_id.clone(), Create {
            content_type: "application/json",
            payload: b"{\"a\":1}\n{\"b\":2}\n".to_vec(),
            ..Create::default()
        })),
        created(stream_id.clone(), 16)
    );
    assert_eq!(
        machine.apply(append_cmd(stream_id.clone(), b"{\"c\":3}\n", Append {
            content_type: Some("application/json"),
            now_ms: 1,
            ..Append::default()
        })),
        appended(16, 24)
    );
    let incarnation = machine.head(&stream_id).expect("head").created_at_ms;
    let publish = |offset: u64, expected_incarnation: Option<u64>| StreamCommand::PublishSnapshot {
        stream_id: stream_id.clone(),
        snapshot_offset: offset,
        content_type: "application/json".to_owned(),
        payload: bytes::Bytes::from_static(b"{}"),
        now_ms: 2,
        expected_incarnation,
    };
    let retain = |offset: u64, expected_incarnation: Option<u64>| StreamCommand::AdvanceRetention {
        stream_id: stream_id.clone(),
        retained_offset: offset,
        now_ms: 3,
        expected_incarnation,
    };

    // Hot: apply reads the preceding byte; an incarnation does not override it.
    let before = machine.snapshot();
    for offset in [3, 12, 20] {
        assert_error_code(
            machine.apply(publish(offset, Some(incarnation))),
            StreamErrorCode::InvalidSnapshot,
        );
    }
    assert_eq!(machine.snapshot(), before);
    assert!(matches!(
        machine.apply(publish(8, None)),
        StreamResponse::SnapshotPublished {
            snapshot_offset: 8,
            ..
        }
    ));

    // Cold: flush the first two messages. Apply cannot read the byte before
    // 16, so it names the incarnation and waits for a verified proposal.
    let candidate = machine
        .plan_cold_flush(&stream_id, 1, 16)
        .expect("plan cold flush")
        .expect("flush candidate");
    assert_eq!(candidate.end_offset, 16);
    assert!(matches!(
        machine.apply(flush_candidate_cmd(
            stream_id.clone(),
            &candidate,
            "s3://bucket/json-lf"
        )),
        StreamResponse::ColdFlushed { .. }
    ));
    let unverified = machine.apply(publish(16, None));
    match &unverified {
        StreamResponse::Error { code, context, .. } => {
            assert_eq!(*code, StreamErrorCode::JsonBoundaryUnverified);
            assert_eq!(context, &vec![StreamErrorContext::StreamIncarnation {
                incarnation
            }]);
        }
        other => panic!("expected an unverified boundary, got {other:?}"),
    }
    // A stale incarnation (a delete and recreate after the proposer's read)
    // is refused the same way.
    assert_error_code(
        machine.apply(publish(16, Some(incarnation + 1))),
        StreamErrorCode::JsonBoundaryUnverified,
    );
    assert!(matches!(
        machine.apply(publish(16, Some(incarnation))),
        StreamResponse::SnapshotPublished {
            snapshot_offset: 16,
            ..
        }
    ));

    // Retention is exact: no rounding to an earlier boundary.
    assert_error_code(
        machine.apply(retain(16, None)),
        StreamErrorCode::JsonBoundaryUnverified,
    );
    assert_eq!(
        machine.apply(retain(16, Some(incarnation))),
        StreamResponse::RetentionAdvanced {
            retained_offset: 16
        }
    );
    // The retained offset and the tail are boundaries by construction.
    assert!(matches!(
        machine.apply(publish(24, None)),
        StreamResponse::SnapshotPublished { .. }
    ));
    let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore");
    assert_eq!(restored.snapshot(), machine.snapshot());
}

#[test]
fn json_append_of_k_messages_adds_k_committed_records() {
    let mut machine = machine();
    let stream_id = stream("json-k");
    assert!(matches!(
        machine.apply(create_cmd(stream_id.clone(), Create {
            content_type: "application/json",
            ..Create::default()
        })),
        StreamResponse::Created { .. }
    ));
    let before = bucket_usage(&machine, "benchcmp").committed_records;
    assert_eq!(
        machine.apply(append_cmd(stream_id.clone(), b"1\n2\n3\n", Append {
            content_type: Some("application/json"),
            ..Append::default()
        })),
        appended(0, 6)
    );
    let response = machine.apply(StreamCommand::AppendExternal {
        stream_id,
        content_type: Some("application/json".to_owned()),
        payload: ExternalPayloadRef {
            s3_path: "json-k/external.json".to_owned(),
            payload_len: 4,
            object_size: 4,
        },
        record_ends: vec![2, 4],
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 1,
    });
    assert_eq!(response, appended(6, 10));
    assert_eq!(
        bucket_usage(&machine, "benchcmp").committed_records - before,
        5
    );
}

#[test]
fn retention_advance_without_checkpoint_does_not_mutate_stream_state() {
    let mut machine = machine();
    let stream_id = stream("snapshot-record-atomicity");
    assert!(matches!(
        machine.apply(create_cmd(stream_id.clone(), Create {
            content_type: "application/json",
            payload: b"{\"a\":1}\n{\"b\":2}\n".to_vec(),
            ..Create::default()
        })),
        StreamResponse::Created { .. }
    ));

    let before = machine.snapshot();

    assert_error_code(
        machine.apply(advance_retention_cmd(stream_id, 8, 1)),
        StreamErrorCode::SnapshotConflict,
    );
    assert_eq!(machine.snapshot(), before);
}

#[test]
fn inline_json_create_rejects_noncanonical_initial_payload() {
    let mut machine = machine();
    let stream_id = stream("invalid-inline-json");

    assert_error_code(
        machine.apply(create_cmd(stream_id.clone(), Create {
            content_type: "application/json",
            payload: br#"{"missing":"newline"}"#.to_vec(),
            ..Create::default()
        })),
        StreamErrorCode::InvalidRecordBoundaries,
    );
    assert!(machine.stream_metadata(&stream_id).is_none());
}

#[test]
fn legacy_external_json_append_without_boundaries_is_rejected_atomically() {
    let mut machine = machine();
    let stream_id = stream("legacy-external-json");
    assert!(matches!(
        machine.apply(create_cmd(stream_id.clone(), Create {
            content_type: "application/json",
            payload: b"{\"id\":1}\n".to_vec(),
            ..Create::default()
        })),
        StreamResponse::Created { .. }
    ));
    let before = machine.snapshot();

    assert_error_code(
        machine.apply(StreamCommand::AppendExternal {
            stream_id,
            content_type: Some("application/json".to_owned()),
            payload: ExternalPayloadRef {
                s3_path: "legacy/append.json".to_owned(),
                payload_len: 9,
                object_size: 9,
            },
            record_ends: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 1,
        }),
        StreamErrorCode::InvalidRecordBoundaries,
    );
    assert_eq!(machine.snapshot(), before);
}

fn producer(id: &str, epoch: u64, seq: u64) -> ProducerRequest {
    ProducerRequest {
        producer_id: id.to_owned(),
        producer_epoch: epoch,
        producer_seq: seq,
    }
}

#[test]
fn cold_flush_command_decodes_pre_pack_wal_records() {
    let command = flush_cold_cmd_at(stream("legacy-cold-wal"), 1, 0, 4, "legacy.bin", 4);
    let mut value = serde_json::to_value(&command).expect("encode cold flush command");
    let chunk = value
        .get_mut("FlushCold")
        .and_then(|variant| variant.get_mut("chunk"))
        .and_then(serde_json::Value::as_object_mut)
        .expect("cold flush chunk");
    assert!(chunk.remove("object_offset").is_some());
    assert!(chunk.remove("shared_object").is_some());
    assert!(chunk.remove("payload_digest").is_some());

    let decoded: StreamCommand = serde_json::from_value(value).expect("decode pre-pack WAL record");
    assert_eq!(decoded, command);
}

#[test]
fn stream_create_requires_existing_bucket_and_valid_ids() {
    let mut machine = StreamStateMachine::new();

    assert_error_code(
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: "Bad".to_owned(),
        }),
        StreamErrorCode::InvalidBucketId,
    );
    assert_error_code(
        machine.apply(create_cmd(stream("s-1"), Create::default())),
        StreamErrorCode::BucketNotFound,
    );

    create_bucket(&mut machine);
    assert_error_code(
        machine.apply(create_cmd(stream("streams"), Create::default())),
        StreamErrorCode::InvalidStreamId,
    );
}

#[test]
fn identical_stream_names_are_isolated_by_bucket_namespace() {
    let mut machine = StreamStateMachine::new();
    for bucket_id in ["owner-a", "owner-b"] {
        assert_eq!(
            machine.apply(StreamCommand::CreateBucket {
                bucket_id: bucket_id.to_owned(),
            }),
            StreamResponse::BucketCreated {
                bucket_id: bucket_id.to_owned(),
            }
        );
    }
    let first = BucketStreamId::new("owner-a", "same-stream");
    let second = BucketStreamId::new("owner-b", "same-stream");

    assert_eq!(
        machine.apply(create_cmd(first.clone(), Create {
            payload: b"first".to_vec(),
            ..Create::default()
        })),
        created(first.clone(), 5)
    );
    assert_eq!(
        machine.apply(create_cmd(second.clone(), Create {
            payload: b"second".to_vec(),
            ..Create::default()
        })),
        created(second.clone(), 6)
    );

    assert_eq!(
        machine
            .read(&first, 0, 16)
            .expect("read first bucket")
            .payload,
        b"first"
    );
    assert_eq!(
        machine
            .read(&second, 0, 16)
            .expect("read second bucket")
            .payload,
        b"second"
    );
}

#[test]
fn create_stream_is_idempotent_only_when_metadata_matches() {
    let mut machine = machine();
    create_stream(&mut machine, "s-1");

    assert_eq!(
        machine.apply(create_cmd(stream("s-1"), Create {
            payload: vec![0; 99],
            ..Create::default()
        })),
        StreamResponse::AlreadyExists {
            next_offset: 0,
            closed: false,
            content_type: OCTET.to_owned(),
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
        }
    );

    assert_error_code(
        machine.apply(create_cmd(stream("s-1"), Create {
            content_type: "text/plain",
            ..Create::default()
        })),
        StreamErrorCode::StreamAlreadyExistsConflict,
    );
}

#[test]
fn append_advances_offsets_and_checks_content_type() {
    let mut machine = machine();
    create_stream(&mut machine, "s-1");

    assert_eq!(
        machine.apply(append_cmd(stream("s-1"), b"abcdefg", Append::default())),
        appended(0, 7)
    );
    assert_error_at(
        machine.apply(append_cmd(stream("s-1"), b"x", Append {
            content_type: Some("text/plain"),
            ..Append::default()
        })),
        StreamErrorCode::ContentTypeMismatch,
        7,
    );
    assert_eq!(machine.head(&stream("s-1")).expect("stream").tail_offset, 7);
}

#[test]
fn catch_up_read_returns_payload_slice_and_bounds_errors() {
    let mut machine = machine();
    create_stream(&mut machine, "s-1");
    assert!(matches!(
        machine.apply(append_cmd(stream("s-1"), b"abcdefg", Append::default())),
        StreamResponse::Appended { .. }
    ));

    assert_eq!(
        machine.read(&stream("s-1"), 2, 3).expect("read"),
        StreamRead {
            offset: 2,
            next_offset: 5,
            content_type: OCTET.to_owned(),
            payload: b"cde".to_vec(),
            up_to_date: false,
            closed: false,
        }
    );
    assert_eq!(
        machine.read(&stream("s-1"), 7, 16).expect("tail read"),
        StreamRead {
            offset: 7,
            next_offset: 7,
            content_type: OCTET.to_owned(),
            payload: Vec::new(),
            up_to_date: true,
            closed: false,
        }
    );
    assert_err_at(
        machine.read(&stream("s-1"), 8, 1),
        StreamErrorCode::OffsetOutOfRange,
        7,
    );
}

#[test]
fn flush_cold_moves_hot_prefix_to_manifest_and_read_plan_splits() {
    let mut machine = machine();
    create_stream(&mut machine, "cold");
    assert!(matches!(
        machine.apply(append_cmd(stream("cold"), b"abcdef", Append::default())),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 6,
            ..
        }
    ));

    let candidate = machine
        .plan_cold_flush(&stream("cold"), 4, 4)
        .expect("plan cold flush")
        .expect("cold flush candidate");
    assert_eq!(candidate.start_offset, 0);
    assert_eq!(candidate.end_offset, 4);
    assert_eq!(candidate.payload, b"abcd");
    assert_eq!(
        machine.apply(flush_candidate_cmd(
            stream("cold"),
            &candidate,
            "s3://bucket/cold/000000"
        )),
        StreamResponse::ColdFlushed {
            hot_start_offset: 4,
        }
    );
    assert_eq!(machine.hot_start_offset(&stream("cold")), 4);
    assert!(machine.cold_chunks(&stream("cold")).is_empty());

    let plan = machine.read_plan(&stream("cold"), 2, 4).expect("read plan");
    assert_eq!(plan.next_offset, 6);
    assert_eq!(plan.segments.len(), 2);
    match &plan.segments[0] {
        StreamReadSegment::ColdIndex(segment) => {
            assert_eq!(segment.read_start_offset, 2);
            assert_eq!(segment.len, 2);
        }
        other => panic!("expected cold index segment, got {other:?}"),
    }
    match &plan.segments[1] {
        StreamReadSegment::Hot(payload) => assert_eq!(payload, b"ef"),
        other => panic!("expected hot segment, got {other:?}"),
    }
    assert_eq!(
        machine.read(&stream("cold"), 0, 6),
        Err(StreamResponse::Error {
            code: StreamErrorCode::InvalidColdFlush,
            message: "stream 'benchcmp/cold' read requires object payload store".to_owned(),
            next_offset: Some(6),
            context: Vec::new(),
        })
    );
    assert_eq!(
        machine.read(&stream("cold"), 4, 8).expect("hot read"),
        StreamRead {
            offset: 4,
            next_offset: 6,
            content_type: OCTET.to_owned(),
            payload: b"ef".to_vec(),
            up_to_date: true,
            closed: false,
        }
    );

    let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore snapshot");
    assert_eq!(restored.hot_start_offset(&stream("cold")), 4);
    assert!(restored.cold_chunks(&stream("cold")).is_empty());
    assert_eq!(
        restored.read(&stream("cold"), 4, 8).expect("hot read"),
        StreamRead {
            offset: 4,
            next_offset: 6,
            content_type: OCTET.to_owned(),
            payload: b"ef".to_vec(),
            up_to_date: true,
            closed: false,
        }
    );
}

#[test]
fn flush_cold_leaves_no_message_boundary_below_the_seal_point() {
    let mut machine = machine();
    create_stream(&mut machine, "cold-records");
    for payload in [b"ab".as_slice(), b"cd".as_slice(), b"ef".as_slice()] {
        assert!(matches!(
            machine.apply(append_cmd(
                stream("cold-records"),
                payload,
                Append::default()
            )),
            StreamResponse::Appended { .. }
        ));
    }

    assert_eq!(
        machine.apply(flush_cold_cmd(
            &machine,
            stream("cold-records"),
            0,
            4,
            "s3://bucket/cold-records/000000",
            4
        )),
        StreamResponse::ColdFlushed {
            hot_start_offset: 4,
        }
    );
    // Bootstrap must not return the cold prefix as one part.
    let plan = machine
        .bootstrap_plan(&stream("cold-records"))
        .expect("bootstrap");
    assert!(plan.updates.is_empty());
    assert_eq!(plan.next_offset, 0);
    assert!(!plan.up_to_date);

    assert_eq!(
        machine.apply(flush_cold_cmd(
            &machine,
            stream("cold-records"),
            4,
            6,
            "s3://bucket/cold-records/000001",
            2
        )),
        StreamResponse::ColdFlushed {
            hot_start_offset: 6,
        }
    );
    assert!(matches!(
        machine.apply(publish_snapshot_cmd(
            stream("cold-records"),
            3,
            OCTET,
            b"abc-state",
            0
        )),
        StreamResponse::SnapshotPublished {
            snapshot_offset: 3,
            ..
        }
    ));
}

fn append_all(machine: &mut StreamStateMachine, id: &str, payloads: &[&[u8]]) {
    for payload in payloads {
        assert!(matches!(
            machine.apply(append_cmd(stream(id), payload, Append::default())),
            StreamResponse::Appended { .. }
        ));
    }
}

fn records(ranges: &[(u64, u64)]) -> Vec<StreamMessageRecord> {
    ranges
        .iter()
        .map(|&(start_offset, end_offset)| StreamMessageRecord {
            start_offset,
            end_offset,
        })
        .collect()
}

/// Regression: a cold flush past the snapshot offset collapsed the
/// messages between the snapshot and the flush frontier into one record that
/// starts below the snapshot. Bootstrap used to drop that record and still
/// claim `up_to_date` at the tail, silently skipping messages.
#[test]
fn bootstrap_after_cold_flush_past_snapshot_does_not_skip_messages() {
    let mut machine = machine();
    create_stream(&mut machine, "boot-skip");
    append_all(&mut machine, "boot-skip", &[b"abc", b"de", b"fg"]);
    assert!(matches!(
        machine.apply(publish_snapshot_cmd(stream("boot-skip"), 3, OCTET, b"s", 0)),
        StreamResponse::SnapshotPublished { .. }
    ));
    assert_eq!(
        machine.apply(flush_cold_cmd(
            &machine,
            stream("boot-skip"),
            0,
            5,
            "s3://b/boot-skip/0",
            5
        )),
        StreamResponse::ColdFlushed {
            hot_start_offset: 5
        }
    );

    let plan = machine
        .bootstrap_plan(&stream("boot-skip"))
        .expect("bootstrap");
    assert_eq!(
        plan.snapshot.as_ref().map(|snapshot| snapshot.offset),
        Some(3)
    );
    assert!(plan.updates.is_empty());
    assert_eq!(plan.next_offset, 3);
    assert!(!plan.up_to_date);
    assert!(!plan.closed);
    // The client continues with ordinary reads from S and gets every
    // remaining message.
    let read = machine
        .read_plan(&stream("boot-skip"), 3, 4)
        .expect("read from S");
    assert_eq!(read.next_offset, 7);
}

/// Regression: when the snapshot offset equals the retained offset, the
/// collapsed cold prefix used to come back as one bootstrap part holding
/// several messages.
#[test]
fn bootstrap_never_returns_collapsed_cold_prefix_as_one_part() {
    let mut machine = machine();
    create_stream(&mut machine, "boot-merge");
    append_all(&mut machine, "boot-merge", &[b"ab", b"cd", b"ef"]);
    assert!(matches!(
        machine.apply(flush_cold_cmd(
            &machine,
            stream("boot-merge"),
            0,
            4,
            "s3://b/boot-merge/0",
            4
        )),
        StreamResponse::ColdFlushed { .. }
    ));

    // No snapshot: S is the retained offset 0, below the cold frontier.
    let plan = machine
        .bootstrap_plan(&stream("boot-merge"))
        .expect("bootstrap");
    assert!(plan.snapshot.is_none());
    assert!(plan.updates.is_empty());
    assert_eq!(plan.next_offset, 0);
    assert!(!plan.up_to_date);

    // Snapshot at the retained offset behaves the same.
    assert!(matches!(
        machine.apply(publish_snapshot_cmd(stream("boot-merge"), 0, OCTET, b"", 0)),
        StreamResponse::SnapshotPublished { .. }
    ));
    let plan = machine
        .bootstrap_plan(&stream("boot-merge"))
        .expect("bootstrap");
    assert_eq!(
        plan.snapshot.as_ref().map(|snapshot| snapshot.offset),
        Some(0)
    );
    assert!(plan.updates.is_empty());
    assert_eq!(plan.next_offset, 0);
    assert!(!plan.up_to_date);
}

/// A non-JSON stream has no message boundaries: from a
/// snapshot at or above the seal point, bootstrap answers `[S, tail)` as one
/// part, even when `S` is inside a message.
#[test]
fn binary_bootstrap_from_the_seal_point_is_one_part() {
    let mut machine = machine();
    create_stream(&mut machine, "boot-hot");
    append_all(&mut machine, "boot-hot", &[b"ab", b"cd", b"ef", b"gh"]);
    // The cold chunk ends inside message "cd", leaving the fragment [3, 4).
    assert!(matches!(
        machine.apply(flush_cold_cmd(
            &machine,
            stream("boot-hot"),
            0,
            3,
            "s3://b/boot-hot/0",
            3
        )),
        StreamResponse::ColdFlushed { .. }
    ));
    // A snapshot below the seal point is an honest partial.
    assert!(matches!(
        machine.apply(publish_snapshot_cmd(stream("boot-hot"), 2, OCTET, b"s", 0)),
        StreamResponse::SnapshotPublished { .. }
    ));
    let plan = machine
        .bootstrap_plan(&stream("boot-hot"))
        .expect("bootstrap");
    assert!(plan.updates.is_empty());
    assert_eq!(plan.next_offset, 2);
    assert!(!plan.up_to_date);

    // At the seal point, inside message "cd": the hot range is one part.
    assert!(matches!(
        machine.apply(publish_snapshot_cmd(stream("boot-hot"), 3, OCTET, b"s", 0)),
        StreamResponse::SnapshotPublished { .. }
    ));
    let plan = machine
        .bootstrap_plan(&stream("boot-hot"))
        .expect("bootstrap");
    assert_eq!(
        plan.snapshot.as_ref().map(|snapshot| snapshot.offset),
        Some(3)
    );
    assert_eq!(plan.updates, records(&[(3, 8)]));
    assert_eq!(plan.next_offset, 8);
    assert!(plan.up_to_date);

    assert!(matches!(
        machine.apply(publish_snapshot_cmd(stream("boot-hot"), 4, OCTET, b"s", 0)),
        StreamResponse::SnapshotPublished { .. }
    ));
    let plan = machine
        .bootstrap_plan(&stream("boot-hot"))
        .expect("bootstrap");
    assert_eq!(plan.updates, records(&[(4, 8)]));
    assert_eq!(plan.next_offset, 8);
    assert!(plan.up_to_date);
}

/// bounded-stream-state F11: JSON bootstrap updates stop at the response cap
/// on a record boundary, as an honest partial; a single record larger than
/// the cap is still returned whole.
#[test]
fn json_bootstrap_caps_updates_at_a_record_boundary() {
    let mut machine = machine();
    let stream_id = stream("boot-cap");
    assert!(matches!(
        machine.apply(create_cmd(stream_id.clone(), Create {
            content_type: "application/json",
            payload: b"{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n".to_vec(),
            ..Create::default()
        })),
        StreamResponse::Created { .. }
    ));
    assert!(matches!(
        machine.apply(close_cmd(stream_id.clone())),
        StreamResponse::Closed { .. }
    ));

    let plan = machine
        .bootstrap_plan_with_cap(&stream_id, 20)
        .expect("bootstrap");
    assert_eq!(plan.updates, records(&[(0, 8), (8, 16)]));
    assert_eq!(plan.next_offset, 16);
    assert!(!plan.up_to_date);
    assert!(!plan.closed);

    let plan = machine
        .bootstrap_plan_with_cap(&stream_id, 1)
        .expect("bootstrap");
    assert_eq!(plan.updates, records(&[(0, 8)]));
    assert_eq!(plan.next_offset, 8);
    assert!(!plan.up_to_date);

    let plan = machine
        .bootstrap_plan_with_cap(&stream_id, 24)
        .expect("bootstrap");
    assert_eq!(plan.updates, records(&[(0, 8), (8, 16), (16, 24)]));
    assert_eq!(plan.next_offset, 24);
    assert!(plan.up_to_date);
    assert!(plan.closed);
    assert_eq!(
        machine.bootstrap_plan(&stream_id).expect("default cap"),
        plan
    );
}

#[test]
fn bootstrap_reports_closed_only_when_complete() {
    let mut machine = machine();
    create_stream(&mut machine, "boot-closed");
    append_all(&mut machine, "boot-closed", &[b"ab", b"cd"]);
    assert!(matches!(
        machine.apply(close_cmd(stream("boot-closed"))),
        StreamResponse::Closed { .. }
    ));
    let plan = machine
        .bootstrap_plan(&stream("boot-closed"))
        .expect("bootstrap");
    assert!(plan.up_to_date);
    assert!(plan.closed);

    assert!(matches!(
        machine.apply(flush_cold_cmd(
            &machine,
            stream("boot-closed"),
            0,
            2,
            "s3://b/boot-closed/0",
            2
        )),
        StreamResponse::ColdFlushed { .. }
    ));
    let plan = machine
        .bootstrap_plan(&stream("boot-closed"))
        .expect("bootstrap");
    assert!(plan.updates.is_empty());
    assert_eq!(plan.next_offset, 0);
    assert!(!plan.up_to_date);
    assert!(!plan.closed);
}

#[test]
fn json_bootstrap_after_cold_flush_is_honest_partial() {
    let mut machine = machine();
    let stream_id = stream("boot-json");
    assert!(matches!(
        machine.apply(create_cmd(stream_id.clone(), Create {
            content_type: "application/json",
            payload: b"{\"a\":1}\n{\"b\":2}\n".to_vec(),
            ..Create::default()
        })),
        StreamResponse::Created { .. }
    ));
    assert!(matches!(
        machine.apply(append_cmd(stream_id.clone(), b"{\"c\":3}\n", Append {
            content_type: Some("application/json"),
            ..Append::default()
        })),
        StreamResponse::Appended { .. }
    ));
    let plan = machine.bootstrap_plan(&stream_id).expect("bootstrap");
    assert_eq!(plan.updates, records(&[(0, 8), (8, 16), (16, 24)]));
    assert!(plan.up_to_date);

    assert!(matches!(
        machine.apply(publish_snapshot_cmd(
            stream_id.clone(),
            8,
            "application/json",
            b"{}",
            0
        )),
        StreamResponse::SnapshotPublished { .. }
    ));
    assert!(matches!(
        machine.apply(flush_cold_cmd(
            &machine,
            stream_id.clone(),
            0,
            16,
            "s3://b/boot-json/0",
            16
        )),
        StreamResponse::ColdFlushed { .. }
    ));
    let plan = machine.bootstrap_plan(&stream_id).expect("bootstrap");
    assert!(plan.updates.is_empty());
    assert_eq!(plan.next_offset, 8);
    assert!(!plan.up_to_date);
}

fn flush_one_cold_chunk(machine: &mut StreamStateMachine, id: &str) {
    machine.apply(append_cmd(stream(id), b"abcd", Append::default()));
    let candidate = machine
        .plan_cold_flush(&stream(id), 4, 4)
        .expect("plan cold flush")
        .expect("cold flush candidate");
    machine.apply(flush_candidate_cmd(
        stream(id),
        &candidate,
        &format!("s3://bucket/{id}/000000"),
    ));
}

#[test]
fn delete_stream_enqueues_cold_gc_then_ack_drains_it() {
    let mut machine = machine();
    create_stream(&mut machine, "cold-a");
    create_stream(&mut machine, "cold-b");
    flush_one_cold_chunk(&mut machine, "cold-a");
    flush_one_cold_chunk(&mut machine, "cold-b");

    for id in ["cold-a", "cold-b"] {
        assert!(matches!(
            machine.apply(delete_cmd(stream(id))),
            StreamResponse::Deleted
        ));
    }

    let pending = machine.pending_cold_gc_batch(16);
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].target, ColdGcTarget::Stream(stream("cold-a")));
    assert_eq!(pending[1].target, ColdGcTarget::Stream(stream("cold-b")));
    // Seqs are monotonic and FIFO-ordered.
    assert!(pending[0].seq < pending[1].seq);

    // Snapshot round-trip must preserve the queue so a crash never loses the
    // reclamation work.
    let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore snapshot");
    assert_eq!(restored.pending_cold_gc_batch(16), pending);

    // Acking the first seq pops only that entry; the later one survives.
    assert_eq!(
        machine.apply(StreamCommand::AckColdGc {
            up_to_seq: pending[0].seq,
        }),
        StreamResponse::ColdGcAcked { removed: 1 }
    );
    assert_eq!(machine.pending_cold_gc_batch(16), vec![pending[1].clone()]);
    // Re-acking the same seq is idempotent.
    assert_eq!(
        machine.apply(StreamCommand::AckColdGc {
            up_to_seq: pending[0].seq,
        }),
        StreamResponse::ColdGcAcked { removed: 0 }
    );
    assert_eq!(
        machine.apply(StreamCommand::AckColdGc {
            up_to_seq: pending[1].seq,
        }),
        StreamResponse::ColdGcAcked { removed: 1 }
    );
    assert_eq!(machine.pending_cold_gc_len(), 0);
}

#[test]
fn shared_cold_object_is_reclaimed_after_last_stream_reference() {
    let mut machine = machine();
    let pack_path = "_packs/00000000/shared.bin";
    for (id, object_offset) in [("pack-a", 0), ("pack-b", 4)] {
        create_stream(&mut machine, id);
        machine.apply(append_cmd(stream(id), b"abcd", Append::default()));
        assert!(matches!(
            machine.apply(StreamCommand::FlushCold {
                cold_generation: machine
                    .cold_index_generation(&stream(id))
                    .unwrap_or_default(),
                stream_id: stream(id),
                chunk: ColdChunkRef {
                    start_offset: 0,
                    end_offset: 4,
                    s3_path: pack_path.to_owned(),
                    object_size: 8,
                    object_offset,
                    shared_object: true,
                    payload_digest: String::new(),
                },
            }),
            StreamResponse::ColdFlushed { .. }
        ));
    }

    let mut restored =
        StreamStateMachine::restore(machine.snapshot()).expect("restore shared pack refs");
    assert_eq!(
        restored.apply(delete_cmd(stream("pack-a"))),
        StreamResponse::Deleted
    );
    assert!(
        restored
            .pending_cold_gc_batch(16)
            .iter()
            .all(|entry| !matches!(
                &entry.target,
                ColdGcTarget::Paths(paths) if paths.iter().any(|path| path == pack_path)
            ))
    );

    assert_eq!(
        restored.apply(delete_cmd(stream("pack-b"))),
        StreamResponse::Deleted
    );
    let shared_reclaims = restored
        .pending_cold_gc_batch(16)
        .into_iter()
        .filter_map(|entry| match entry.target {
            ColdGcTarget::Paths(paths) if paths.iter().any(|path| path == pack_path) => Some(paths),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(shared_reclaims, vec![vec![pack_path.to_owned()]]);
}

#[test]
fn compact_cold_enqueues_inputs_with_gc_grace() {
    let mut machine = machine();
    create_stream(&mut machine, "compact");
    machine.apply(append_cmd(
        stream("compact"),
        b"abcdefgh",
        Append::default(),
    ));
    let first = ColdChunkRef {
        start_offset: 0,
        end_offset: 4,
        object_size: 4,
        s3_path: "old-0".to_owned(),
        object_offset: 0,
        shared_object: false,
        payload_digest: String::new(),
    };
    let second = ColdChunkRef {
        start_offset: 4,
        end_offset: 8,
        object_size: 4,
        s3_path: "old-1".to_owned(),
        object_offset: 0,
        shared_object: false,
        payload_digest: String::new(),
    };
    assert!(matches!(
        machine.apply(StreamCommand::FlushCold {
            cold_generation: machine
                .cold_index_generation(&stream("compact"))
                .unwrap_or_default(),
            stream_id: stream("compact"),
            chunk: first.clone(),
        }),
        StreamResponse::ColdFlushed { .. }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::FlushCold {
            cold_generation: machine
                .cold_index_generation(&stream("compact"))
                .unwrap_or_default(),
            stream_id: stream("compact"),
            chunk: second.clone(),
        }),
        StreamResponse::ColdFlushed { .. }
    ));

    assert_eq!(
        machine.apply(StreamCommand::CompactCold {
            stream_id: stream("compact"),
            old_chunks: vec![first, second],
            replacement: ColdChunkRef {
                start_offset: 0,
                end_offset: 8,
                object_size: 8,
                s3_path: "replacement".to_owned(),
                object_offset: 0,
                shared_object: false,
                payload_digest: String::new(),
            },
            gc_not_before_ms: 42,
        }),
        StreamResponse::ColdCompacted {
            compacted_chunks: 2,
            compacted_bytes: 8,
        }
    );
    let pending = machine.pending_cold_gc_batch(16);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].not_before_ms, 42);
    assert_eq!(
        pending[0].target,
        ColdGcTarget::Paths(vec!["old-0".to_owned(), "old-1".to_owned()])
    );
}

#[test]
fn expired_stream_with_cold_chunks_enqueues_cold_gc() {
    let mut machine = machine();
    machine.apply(create_cmd(stream("ttl"), Create {
        expires_at_ms: Some(1_000),
        ..Create::default()
    }));
    flush_one_cold_chunk(&mut machine, "ttl");

    // A lazy access past the expiry removes the stream and queues its cold prefix.
    assert!(machine.head_at(&stream("ttl"), 2_000).is_none());
    let pending = machine.pending_cold_gc_batch(16);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].target, ColdGcTarget::Stream(stream("ttl")));
}

#[test]
fn writes_sweep_expired_streams_in_bounded_deterministic_batches() {
    let mut machine = machine();
    create_stream(&mut machine, "active");

    let expired_count = TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE + 6;
    for index in 0..expired_count {
        let id = format!("old-{index:04}");
        assert!(matches!(
            machine.apply(create_cmd(stream(&id), Create {
                expires_at_ms: Some(1_000),
                ..Create::default()
            })),
            StreamResponse::Created { .. }
        ));
    }
    assert_eq!(machine.snapshot().streams.len(), expired_count + 1);

    assert!(matches!(
        machine.apply(append_cmd(stream("active"), b"x", Append {
            now_ms: 2_000,
            ..Append::default()
        })),
        StreamResponse::Appended { .. }
    ));

    let snapshot = machine.snapshot();
    let stream_ids = snapshot
        .streams
        .iter()
        .map(|entry| entry.metadata.stream_id.stream_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(stream_ids.len(), 7);
    assert!(stream_ids.contains(&"active"));
    assert!(!stream_ids.contains(&"old-0000"));
    let last_swept = format!("old-{:04}", TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE - 1);
    let first_retained = format!("old-{:04}", TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
    let last_retained = format!("old-{:04}", expired_count - 1);
    assert!(!stream_ids.contains(&last_swept.as_str()));
    assert!(stream_ids.contains(&first_retained.as_str()));
    assert!(stream_ids.contains(&last_retained.as_str()));
}

#[test]
fn stream_without_cold_chunks_enqueues_nothing_on_delete() {
    let mut machine = machine();
    create_stream(&mut machine, "hot-only");
    machine.apply(delete_cmd(stream("hot-only")));
    assert_eq!(machine.pending_cold_gc_len(), 0);
}

#[test]
fn flush_cold_can_coalesce_contiguous_hot_segments() {
    let mut machine = machine();
    create_stream(&mut machine, "cold-coalesced");
    assert!(matches!(
        machine.apply(append_cmd(
            stream("cold-coalesced"),
            b"abc",
            Append::default()
        )),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 3,
            ..
        }
    ));
    assert!(matches!(
        machine.apply(append_cmd(
            stream("cold-coalesced"),
            b"de",
            Append::default()
        )),
        StreamResponse::Appended {
            offset: 3,
            next_offset: 5,
            ..
        }
    ));

    assert_eq!(
        machine.apply(flush_cold_cmd(
            &machine,
            stream("cold-coalesced"),
            0,
            5,
            "s3://bucket/cold-coalesced/000000",
            5
        )),
        StreamResponse::ColdFlushed {
            hot_start_offset: 5,
        }
    );
    assert!(machine.hot_segments(&stream("cold-coalesced")).is_empty());
    assert_eq!(machine.hot_payload_len(&stream("cold-coalesced")), Ok(0));
    assert!(machine.cold_chunks(&stream("cold-coalesced")).is_empty());

    let plan = machine
        .read_plan(&stream("cold-coalesced"), 0, 5)
        .expect("read plan");
    assert_eq!(plan.next_offset, 5);
    assert_eq!(plan.segments.len(), 1);
    match &plan.segments[0] {
        StreamReadSegment::ColdIndex(segment) => {
            assert_eq!(segment.read_start_offset, 0);
            assert_eq!(segment.len, 5);
        }
        other => panic!("expected cold index segment, got {other:?}"),
    }
}

#[test]
fn plan_cold_flush_coalesces_contiguous_hot_segments() {
    let mut machine = machine();
    create_stream(&mut machine, "cold-planned-coalesced");
    for payload in [b"ab".as_slice(), b"cd".as_slice(), b"ef".as_slice()] {
        assert!(matches!(
            machine.apply(append_cmd(
                stream("cold-planned-coalesced"),
                payload,
                Append::default()
            )),
            StreamResponse::Appended { .. }
        ));
    }

    assert!(
        machine
            .plan_cold_flush(&stream("cold-planned-coalesced"), 4, 4)
            .expect("plan cold flush")
            .is_some(),
        "planner should consider contiguous small hot segments together"
    );
    let candidate = machine
        .plan_cold_flush(&stream("cold-planned-coalesced"), 5, 5)
        .expect("plan cold flush")
        .expect("candidate");
    assert_eq!(candidate.start_offset, 0);
    assert_eq!(candidate.end_offset, 5);
    assert_eq!(candidate.payload, b"abcde");
}

#[test]
fn plan_next_cold_flush_selects_deterministic_eligible_stream() {
    let mut machine = machine();
    create_stream(&mut machine, "z-cold");
    create_stream(&mut machine, "a-cold");
    assert!(matches!(
        machine.apply(append_cmd(stream("z-cold"), b"zzzz", Append::default())),
        StreamResponse::Appended { .. }
    ));
    assert!(matches!(
        machine.apply(append_cmd(stream("a-cold"), b"aaaa", Append::default())),
        StreamResponse::Appended { .. }
    ));

    let candidate = machine
        .plan_next_cold_flush_batch(4, 4, 4, 1)
        .expect("plan next cold flush")
        .into_iter()
        .next()
        .expect("candidate");
    assert_eq!(candidate.stream_id, stream("a-cold"));
    assert_eq!(candidate.payload, b"aaaa");
}

#[test]
fn plan_next_cold_flush_drains_distributed_group_hot_bytes() {
    let mut machine = machine();
    create_stream(&mut machine, "z-cold");
    create_stream(&mut machine, "a-cold");
    for stream_name in ["z-cold", "a-cold"] {
        assert!(matches!(
            machine.apply(append_cmd(stream(stream_name), b"aa", Append::default())),
            StreamResponse::Appended { .. }
        ));
    }

    // F6c: hot payload only, 2 x 2 B.
    let group_real = 4;
    assert_eq!(machine.total_hot_payload_bytes(), group_real as u64);
    assert_eq!(
        machine
            .plan_next_cold_flush_batch(group_real, 4, 4, 1)
            .expect("plan next cold flush")
            .len(),
        1
    );
    let candidates = machine
        .plan_next_cold_flush_batch(group_real + 1, 4, 4, 1)
        .expect("plan next cold flush");
    assert!(candidates.is_empty());
    let candidate = machine
        .plan_next_cold_flush_batch(4, 4, 4, 1)
        .expect("plan next cold flush")
        .into_iter()
        .next()
        .expect("candidate");
    assert_eq!(candidate.stream_id, stream("a-cold"));
    assert_eq!(candidate.payload, b"aa");
}

#[test]
fn plan_next_cold_flush_batch_drains_triggered_group_to_batch_target() {
    let mut machine = machine();
    create_stream(&mut machine, "pack-z");
    create_stream(&mut machine, "pack-a");
    for stream_name in ["pack-z", "pack-a"] {
        assert!(matches!(
            machine.apply(append_cmd(stream(stream_name), b"aa", Append::default())),
            StreamResponse::Appended { .. }
        ));
    }

    let candidates = machine
        .plan_next_cold_flush_batch(4, 4, 4, 4)
        .expect("plan group pack");
    assert_eq!(candidates.len(), 2);
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| candidate.stream_id.clone())
            .collect::<Vec<_>>(),
        vec![stream("pack-a"), stream("pack-z")]
    );
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| candidate.payload.len())
            .sum::<usize>(),
        4
    );
}

fn planner_request(
    min_hot_bytes: usize,
    max_flush_bytes: usize,
    max_batch_bytes: usize,
    max_candidates: usize,
) -> ColdFlushPassRequest {
    ColdFlushPassRequest {
        min_hot_bytes,
        max_flush_bytes,
        max_batch_bytes,
        max_candidates,
        pressure: None,
        max_hot_age: None,
    }
}

fn apply_flush_pass(machine: &mut StreamStateMachine, pass: &ColdFlushPass, round: usize) {
    for (index, candidate) in pass.candidates.iter().enumerate() {
        assert!(matches!(
            machine.apply(flush_candidate_cmd(
                candidate.stream_id.clone(),
                candidate,
                &format!("s3://bucket/planner/{round:04}-{index:04}"),
            )),
            StreamResponse::ColdFlushed { .. }
        ));
    }
}

/// bounded-stream-state F10: with flush_size = flush_max_size = batch, the
/// old drain pass stopped at the first stream that did not fit and always
/// walked streams in the same order, so a stream sorting after the small ones
/// starved and its hot bytes grew without bound.
#[test]
fn flush_planner_does_not_starve_a_stream_behind_small_streams() {
    const FLUSH_SIZE: usize = 64;
    let mut machine = machine();
    let small = (0..10)
        .map(|index| format!("a-{index:02}"))
        .collect::<Vec<_>>();
    for name in &small {
        create_stream(&mut machine, name);
    }
    create_stream(&mut machine, "zz-large");
    for round in 0..50 {
        for name in &small {
            append_all(&mut machine, name, &[b"abcd"]);
        }
        append_all(&mut machine, "zz-large", &[&[b'z'; 20]]);
        let pass = machine
            .plan_cold_flush_pass(planner_request(
                FLUSH_SIZE,
                FLUSH_SIZE,
                FLUSH_SIZE,
                usize::MAX,
            ))
            .expect("plan pass");
        apply_flush_pass(&mut machine, &pass, round);
        for name in small.iter().map(String::as_str).chain(["zz-large"]) {
            let hot = machine.hot_payload_len(&stream(name)).expect("hot len");
            assert!(
                hot <= 2 * FLUSH_SIZE as u64,
                "round {round}: stream {name} holds {hot} hot bytes"
            );
        }
    }
}

/// bounded-stream-state F10: planner work is bounded by the streams that hold
/// hot bytes, with one sort per pass, not by candidates times streams.
#[test]
fn flush_planner_cost_is_bounded_by_hot_streams() {
    let mut machine = machine();
    for index in 0..1_000 {
        create_stream(&mut machine, &format!("idle-{index:04}"));
    }
    for name in ["hot-a", "hot-b", "hot-c"] {
        create_stream(&mut machine, name);
        append_all(&mut machine, name, &[b"0123456789", b"0123456789"]);
    }
    let pass = machine
        .plan_cold_flush_pass(planner_request(8, 4, usize::MAX, usize::MAX))
        .expect("plan pass");
    assert_eq!(pass.candidates.len(), 15);
    assert_eq!(pass.stats.streams_visited, 3);
    assert_eq!(pass.stats.sorts, 1);
    assert_eq!(pass.stats.bytes_copied, 60);

    apply_flush_pass(&mut machine, &pass, 0);
    let pass = machine
        .plan_cold_flush_pass(planner_request(8, 4, usize::MAX, usize::MAX))
        .expect("plan empty pass");
    assert!(pass.candidates.is_empty());
    assert_eq!(pass.stats.streams_visited, 0);

    // The index survives restore without being part of the snapshot.
    append_all(&mut machine, "hot-b", &[b"0123456789"]);
    let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore");
    let (pass, _) = restored
        .plan_cold_flush_pass_from(planner_request(8, 16, usize::MAX, usize::MAX), None)
        .expect("plan restored pass");
    assert_eq!(pass.stats.streams_visited, 1);
    assert_eq!(pass.candidates.len(), 1);
    assert_eq!(pass.candidates[0].stream_id, stream("hot-b"));
}

/// bounded-stream-state F10: passes rotate through equally large streams
/// from a leader-local cursor, and candidates are a deterministic function of
/// the index and the cursor.
#[test]
fn flush_planner_rotates_equal_streams_from_the_cursor() {
    let mut machine = machine();
    for name in ["rot-a", "rot-b", "rot-c", "rot-small"] {
        create_stream(&mut machine, name);
    }
    for name in ["rot-a", "rot-b", "rot-c"] {
        append_all(&mut machine, name, &[b"abcd"]);
    }
    append_all(&mut machine, "rot-small", &[b"x"]);
    // Group hot is 13 bytes, at least min_hot_bytes = 4: the group drains,
    // one candidate per pass, and the three equal streams take turns.
    let mut request = planner_request(4, 4, 4, 1);
    let mut order = Vec::new();
    for _ in 0..4 {
        let pass = machine.plan_cold_flush_pass(request).expect("plan pass");
        order.push(pass.candidates[0].stream_id.stream_id.clone());
    }
    assert_eq!(order, ["rot-a", "rot-b", "rot-c", "rot-a"]);
    let (pass, cursor) = machine
        .plan_cold_flush_pass_from(request, Some(&stream("rot-b")))
        .expect("plan from cursor");
    assert_eq!(pass.candidates[0].stream_id, stream("rot-c"));
    assert_eq!(cursor, Some(stream("rot-c")));

    // A candidate gets at most the remaining batch budget, so a stream that
    // does not fit is cut instead of stopping the pass.
    request.max_candidates = usize::MAX;
    request.max_batch_bytes = 6;
    let (pass, _) = machine
        .plan_cold_flush_pass_from(request, None)
        .expect("plan budgeted pass");
    assert_eq!(
        pass.candidates
            .iter()
            .map(|candidate| (
                candidate.stream_id.stream_id.as_str(),
                candidate.payload.len()
            ))
            .collect::<Vec<_>>(),
        vec![("rot-a", 4), ("rot-b", 2)]
    );
}

/// bounded-stream-state F10: group drain flushes the largest streams until
/// the group is below half of flush_size, and node pressure drains the
/// group's proportional share of the node excess, largest first.
#[test]
fn flush_planner_drains_largest_streams_first() {
    let mut machine = machine();
    for (name, len) in [("d-10", 10usize), ("d-20", 20), ("d-30", 30), ("d-40", 40)] {
        create_stream(&mut machine, name);
        append_all(&mut machine, name, &[&vec![b'x'; len]]);
    }
    // F6c: hot payload only, 100 in all. Group hot
    // 100 >= 100: drain until below 50 (d-40 and d-30).
    let group_real = 100;
    assert_eq!(machine.total_hot_payload_bytes(), group_real as u64);
    let (pass, _) = machine
        .plan_cold_flush_pass_from(
            planner_request(group_real, 64, usize::MAX, usize::MAX),
            None,
        )
        .expect("plan drain");
    assert_eq!(
        pass.candidates
            .iter()
            .map(|candidate| candidate.stream_id.stream_id.as_str())
            .collect::<Vec<_>>(),
        vec!["d-40", "d-30"]
    );

    // Pressure: node 200 hot, target 150, so this group (100 of 200) drains
    // at least 25 bytes: the largest stream alone.
    let mut request = planner_request(1_000, 64, usize::MAX, usize::MAX);
    request.pressure = Some(ColdFlushPressure {
        node_hot_bytes: 200,
        node_target_bytes: 150,
    });
    let (pass, _) = machine
        .plan_cold_flush_pass_from(request, None)
        .expect("plan pressure");
    assert_eq!(pass.candidates.len(), 1);
    assert_eq!(pass.candidates[0].stream_id, stream("d-40"));
}

#[test]
fn plan_next_cold_flush_batch_advances() {
    let mut machine = machine();
    create_stream(&mut machine, "batched-cold");
    assert!(matches!(
        machine.apply(append_cmd(
            stream("batched-cold"),
            b"abcd",
            Append::default()
        )),
        StreamResponse::Appended { .. }
    ));

    let candidates = machine
        .plan_next_cold_flush_batch(1, 1, 4, 4)
        .expect("plan cold flush batch");
    assert_eq!(candidates.len(), 4);
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| (candidate.start_offset, candidate.end_offset))
            .collect::<Vec<_>>(),
        vec![(0, 1), (1, 2), (2, 3), (3, 4)]
    );
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| candidate.payload.as_slice())
            .collect::<Vec<_>>(),
        vec![
            b"a".as_slice(),
            b"b".as_slice(),
            b"c".as_slice(),
            b"d".as_slice()
        ]
    );
    assert_eq!(machine.hot_start_offset(&stream("batched-cold")), 0);
    assert_eq!(machine.hot_payload_len(&stream("batched-cold")), Ok(4));
    assert!(machine.cold_chunks(&stream("batched-cold")).is_empty());
}

#[test]
fn stale_cold_flush_candidate_after_delete_recreate_is_invalid_without_mutation() {
    let mut machine = machine();
    create_stream(&mut machine, "stale-cold");
    assert!(matches!(
        machine.apply(append_cmd(
            stream("stale-cold"),
            b"abcdefghijklmnopqr",
            Append::default()
        )),
        StreamResponse::Appended {
            next_offset: 18,
            ..
        }
    ));
    let candidate = machine
        .plan_cold_flush(&stream("stale-cold"), 18, 18)
        .expect("plan cold flush")
        .expect("candidate");

    assert!(matches!(
        machine.apply(delete_cmd(stream("stale-cold"))),
        StreamResponse::Deleted
    ));
    create_stream(&mut machine, "stale-cold");
    assert!(matches!(
        machine.apply(append_cmd(
            stream("stale-cold"),
            b"abcdefghijklmnopq",
            Append::default()
        )),
        StreamResponse::Appended {
            next_offset: 17,
            ..
        }
    ));

    match machine.apply(flush_candidate_cmd(
        stream("stale-cold"),
        &candidate,
        "s3://bucket/stale-cold/old-candidate",
    )) {
        StreamResponse::Error {
            code: StreamErrorCode::InvalidColdFlush,
            message,
            next_offset: Some(17),
            context,
            ..
        } => {
            assert!(message.contains("beyond stream"));
            assert_eq!(context, vec![StreamErrorContext::StaleColdFlushCandidate]);
        }
        other => panic!("expected stale invalid cold flush, got {other:?}"),
    }

    assert_eq!(
        machine.read(&stream("stale-cold"), 0, 32).expect("read"),
        StreamRead {
            offset: 0,
            next_offset: 17,
            content_type: OCTET.to_owned(),
            payload: b"abcdefghijklmnopq".to_vec(),
            up_to_date: true,
            closed: false,
        }
    );
}

#[test]
fn plan_next_cold_flush_skips_deleted_streams() {
    let mut machine = machine();
    create_stream(&mut machine, "a-gone");
    create_stream(&mut machine, "b-live");
    assert!(matches!(
        machine.apply(append_cmd(stream("a-gone"), b"gone", Append::default())),
        StreamResponse::Appended { .. }
    ));
    assert_eq!(
        machine.apply(delete_cmd(stream("a-gone"))),
        StreamResponse::Deleted
    );
    assert!(matches!(
        machine.apply(append_cmd(stream("b-live"), b"live", Append::default())),
        StreamResponse::Appended { .. }
    ));

    let candidate = machine
        .plan_next_cold_flush_batch(4, 4, 4, 1)
        .expect("plan next cold flush")
        .into_iter()
        .next()
        .expect("candidate");
    assert_eq!(candidate.stream_id, stream("b-live"));
    assert_eq!(candidate.payload, b"live");
}

#[test]
fn hot_payload_byte_metrics_follow_cold_flush() {
    let mut machine = machine();
    create_stream(&mut machine, "hot-a");
    create_stream(&mut machine, "hot-b");
    for (stream_name, payload) in [("hot-a", b"abcd".as_slice()), ("hot-b", b"xy".as_slice())] {
        assert!(matches!(
            machine.apply(append_cmd(stream(stream_name), payload, Append::default())),
            StreamResponse::Appended { .. }
        ));
    }

    assert_eq!(machine.hot_payload_len(&stream("hot-a")), Ok(4));
    assert_eq!(machine.hot_payload_len(&stream("hot-b")), Ok(2));
    assert_eq!(machine.total_hot_payload_bytes(), 6);

    assert_eq!(
        machine.apply(flush_cold_cmd(
            &machine,
            stream("hot-a"),
            0,
            3,
            "s3://bucket/hot-a/000000",
            3
        )),
        StreamResponse::ColdFlushed {
            hot_start_offset: 3,
        }
    );
    assert_eq!(machine.hot_payload_len(&stream("hot-a")), Ok(1));
    assert_eq!(machine.total_hot_payload_bytes(), 3);

    let mut restored =
        StreamStateMachine::restore(machine.snapshot()).expect("restore hot payload gauge");
    assert_eq!(restored.total_hot_payload_bytes(), 3);
    assert_eq!(
        restored.apply(delete_cmd(stream("hot-b"))),
        StreamResponse::Deleted
    );
    assert_eq!(restored.total_hot_payload_bytes(), 1);
}

#[test]
fn hot_start_offset_advances_to_tail_after_full_cold_flush() {
    let mut machine = machine();
    create_stream(&mut machine, "hot-start");
    assert!(matches!(
        machine.apply(append_cmd(stream("hot-start"), b"abcd", Append::default())),
        StreamResponse::Appended { .. }
    ));

    assert_eq!(
        machine.apply(flush_cold_cmd(
            &machine,
            stream("hot-start"),
            0,
            4,
            "s3://bucket/hot-start/000000",
            4
        )),
        StreamResponse::ColdFlushed {
            hot_start_offset: 4,
        }
    );
    assert_eq!(machine.hot_start_offset(&stream("hot-start")), 4);
    assert_eq!(machine.hot_payload_len(&stream("hot-start")), Ok(0));
}

#[test]
fn snapshot_restore_round_trips_payload_metadata_and_stream_seq() {
    let mut machine = machine();
    assert_eq!(
        machine.apply(create_cmd(stream("snap-open"), Create {
            payload: b"hi".to_vec(),
            stream_seq: Some("0001".to_owned()),
            ttl_seconds: Some(60),
            ..Create::default()
        })),
        created(stream("snap-open"), 2)
    );
    assert!(matches!(
        machine.apply(append_cmd(stream("snap-open"), b"abc", Append {
            stream_seq: Some("0002".to_owned()),
            ..Append::default()
        })),
        StreamResponse::Appended {
            offset: 2,
            next_offset: 5,
            ..
        }
    ));
    assert_eq!(
        machine.apply(create_cmd(stream("snap-closed"), Create {
            payload: b"x".to_vec(),
            close_after: true,
            ..Create::default()
        })),
        StreamResponse::Created {
            stream_id: stream("snap-closed"),
            next_offset: 1,
            closed: true,
        }
    );

    let encoded = serde_json::to_vec(&machine.snapshot()).expect("serialize snapshot");
    let decoded = serde_json::from_slice::<StreamSnapshot>(&encoded).expect("deserialize snapshot");
    let mut restored = StreamStateMachine::restore(decoded).expect("restore snapshot");

    assert_eq!(
        restored.read(&stream("snap-open"), 0, 16).expect("read"),
        StreamRead {
            offset: 0,
            next_offset: 5,
            content_type: OCTET.to_owned(),
            payload: b"hiabc".to_vec(),
            up_to_date: true,
            closed: false,
        }
    );
    let metadata = restored.head(&stream("snap-open")).expect("metadata");
    assert_eq!(metadata.last_stream_seq.as_deref(), Some("0002"));
    assert_eq!(metadata.stream_ttl_seconds, Some(60));
    assert_eq!(metadata.stream_expires_at_ms, None);

    assert_error_at(
        restored.apply(append_cmd(stream("snap-open"), b"bad", Append {
            stream_seq: Some("0002".to_owned()),
            ..Append::default()
        })),
        StreamErrorCode::StreamSeqConflict,
        5,
    );
    assert_eq!(
        restored.apply(append_cmd(stream("snap-open"), b"!", Append {
            stream_seq: Some("0003".to_owned()),
            ..Append::default()
        })),
        appended(5, 6)
    );
    assert_error_at(
        restored.apply(append_cmd(stream("snap-closed"), b"!", Append::default())),
        StreamErrorCode::StreamClosed,
        1,
    );
}

#[test]
fn snapshot_order_is_deterministic() {
    let mut machine = StreamStateMachine::new();
    for bucket_id in ["zzzz", "benchcmp", "aaaa"] {
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: bucket_id.to_owned(),
        });
    }
    for stream_id in [
        BucketStreamId::new("zzzz", "stream-b"),
        BucketStreamId::new("benchcmp", "stream-b"),
        BucketStreamId::new("benchcmp", "stream-a"),
        BucketStreamId::new("aaaa", "stream-z"),
    ] {
        assert!(matches!(
            machine.apply(create_cmd(stream_id, Create::default())),
            StreamResponse::Created { .. }
        ));
    }

    let snapshot = machine.snapshot();
    assert_eq!(snapshot.buckets, ["aaaa", "benchcmp", "zzzz"]);
    assert_eq!(
        snapshot
            .streams
            .iter()
            .map(|entry| entry.metadata.stream_id.to_string())
            .collect::<Vec<_>>(),
        [
            "aaaa/stream-z",
            "benchcmp/stream-a",
            "benchcmp/stream-b",
            "zzzz/stream-b",
        ]
    );
}

/// A snapshot entry with the given identity and payload; every other field is
/// the empty default.
fn snapshot_entry(
    stream_id: BucketStreamId,
    tail_offset: u64,
    payload: Vec<u8>,
    producer_states: Vec<ProducerSnapshot>,
) -> StreamSnapshotEntry {
    StreamSnapshotEntry {
        metadata: StreamMetadata {
            stream_id,
            content_type: OCTET.to_owned(),
            status: StreamStatus::Open,
            tail_offset,
            last_stream_seq: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            created_at_ms: 0,
            last_ttl_touch_at_ms: 0,
        },
        retained_offset: None,
        hot_start_offset: 0,
        payload,
        hot_segments: Vec::new(),
        cold_index_generation: 0,
        cold_chunks: Vec::new(),
        external_segments: Vec::new(),
        visible_snapshot: None,
        producer_states,
    }
}

fn producer_snapshot(epoch: u64) -> ProducerSnapshot {
    ProducerSnapshot {
        producer_id: "writer-1".to_owned(),
        producer_epoch: epoch,
        producer_seq: 0,
        last_start_offset: 0,
        last_next_offset: 0,
        last_closed: false,
        receipts: Vec::new(),
        last_seen_ms: 0,
    }
}

#[test]
fn snapshot_restore_rejects_invalid_entries() {
    assert_eq!(
        StreamStateMachine::restore(StreamSnapshot {
            pending_cold_gc: Vec::new(),
            next_cold_gc_seq: 0,
            shared_cold_object_owners: Vec::new(),
            buckets: vec!["benchcmp".to_owned(), "benchcmp".to_owned()],
            erased_buckets: Vec::new(),
            streams: Vec::new(),
            bucket_usage: Vec::new(),
            last_created_at_ms: 0,
            format_epoch: crate::FORMAT_EPOCH,
        })
        .expect_err("duplicate bucket"),
        StreamSnapshotError::DuplicateBucket("benchcmp".to_owned())
    );

    let restore_with = |entry: StreamSnapshotEntry| {
        StreamStateMachine::restore(StreamSnapshot {
            pending_cold_gc: Vec::new(),
            next_cold_gc_seq: 0,
            shared_cold_object_owners: Vec::new(),
            buckets: vec!["benchcmp".to_owned()],
            erased_buckets: Vec::new(),
            streams: vec![entry],
            bucket_usage: Vec::new(),
            last_created_at_ms: 0,
            format_epoch: crate::FORMAT_EPOCH,
        })
    };

    assert!(matches!(
        restore_with(snapshot_entry(
            BucketStreamId::new("missing", "stream"),
            0,
            Vec::new(),
            Vec::new(),
        )),
        Err(StreamSnapshotError::MissingBucket(_))
    ));

    assert!(matches!(
        restore_with(snapshot_entry(
            stream("bad-len"),
            2,
            b"x".to_vec(),
            Vec::new()
        )),
        Err(StreamSnapshotError::PayloadLengthMismatch { .. })
    ));

    assert!(matches!(
        restore_with(snapshot_entry(
            stream("duplicate-producer"),
            0,
            Vec::new(),
            vec![producer_snapshot(0), producer_snapshot(1),]
        )),
        Err(StreamSnapshotError::DuplicateProducer { .. })
    ));
}

#[test]
fn close_is_monotonic_and_close_only_is_idempotent() {
    let mut machine = machine();
    create_stream(&mut machine, "s-1");

    assert_eq!(
        machine.apply(append_cmd(stream("s-1"), b"abc", Append {
            close_after: true,
            ..Append::default()
        })),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 3,
            closed: true,
            deduplicated: false,
            producer: None,
            receipt_evicted: false,
        }
    );
    assert_eq!(
        machine.apply(close_cmd(stream("s-1"))),
        StreamResponse::Closed {
            next_offset: 3,
            deduplicated: false,
            producer: None,
        }
    );
    assert_error_at(
        machine.apply(append_cmd(stream("s-1"), b"x", Append::default())),
        StreamErrorCode::StreamClosed,
        3,
    );
}

#[test]
fn stream_seq_must_strictly_increase() {
    let mut machine = machine();
    create_stream(&mut machine, "s-1");

    assert!(matches!(
        machine.apply(append_cmd(stream("s-1"), b"a", Append {
            stream_seq: Some("0002".to_owned()),
            ..Append::default()
        })),
        StreamResponse::Appended { .. }
    ));
    assert_error_at(
        machine.apply(append_cmd(stream("s-1"), b"b", Append {
            stream_seq: Some("0002".to_owned()),
            ..Append::default()
        })),
        StreamErrorCode::StreamSeqConflict,
        1,
    );
    assert!(matches!(
        machine.apply(append_cmd(stream("s-1"), b"c", Append {
            stream_seq: Some("0003".to_owned()),
            ..Append::default()
        })),
        StreamResponse::Appended {
            offset: 1,
            next_offset: 2,
            ..
        }
    ));
}

#[test]
fn producer_headers_deduplicate_retries_and_fence_stale_epochs() {
    let mut machine = machine();
    create_stream(&mut machine, "producer-stream");

    assert_eq!(
        machine.apply(append_cmd(stream("producer-stream"), b"a", Append {
            producer: Some(producer("writer-1", 0, 0)),
            ..Append::default()
        })),
        appended_by(producer("writer-1", 0, 0), 0, 1, false, false)
    );
    assert_eq!(
        machine.apply(append_cmd(
            stream("producer-stream"),
            b"ignored-retry-body",
            Append {
                producer: Some(producer("writer-1", 0, 0)),
                ..Append::default()
            }
        )),
        appended_by(producer("writer-1", 0, 0), 0, 1, false, true)
    );
    assert_eq!(
        machine
            .read(&stream("producer-stream"), 0, 16)
            .expect("read")
            .payload,
        b"a"
    );

    assert_error_code(
        machine.apply(append_cmd(stream("producer-stream"), b"gap", Append {
            producer: Some(producer("writer-1", 0, 2)),
            ..Append::default()
        })),
        StreamErrorCode::ProducerSeqConflict,
    );

    assert_eq!(
        machine.apply(append_cmd(stream("producer-stream"), b"b", Append {
            producer: Some(producer("writer-1", 1, 0)),
            ..Append::default()
        })),
        appended_by(producer("writer-1", 1, 0), 1, 2, false, false)
    );
    assert_error_code(
        machine.apply(append_cmd(stream("producer-stream"), b"stale", Append {
            producer: Some(producer("writer-1", 0, 1)),
            ..Append::default()
        })),
        StreamErrorCode::ProducerEpochStale,
    );
}

#[test]
fn producer_delayed_retry_returns_its_original_receipt() {
    let mut machine = machine();
    create_stream(&mut machine, "producer-delayed-retry");

    assert_eq!(
        machine.apply(append_cmd(stream("producer-delayed-retry"), b"a", Append {
            producer: Some(producer("writer-1", 0, 0)),
            ..Append::default()
        })),
        appended_by(producer("writer-1", 0, 0), 0, 1, false, false)
    );
    assert_eq!(
        machine.apply(append_cmd(stream("producer-delayed-retry"), b"b", Append {
            producer: Some(producer("writer-1", 0, 1)),
            ..Append::default()
        })),
        appended_by(producer("writer-1", 0, 1), 1, 2, false, false)
    );
    let mut machine =
        StreamStateMachine::restore(machine.snapshot()).expect("restore producer receipts");
    assert_eq!(
        machine.apply(append_cmd(
            stream("producer-delayed-retry"),
            b"ignored",
            Append {
                producer: Some(producer("writer-1", 0, 0)),
                ..Append::default()
            }
        )),
        appended_by(producer("writer-1", 0, 0), 0, 1, false, true)
    );
    assert_eq!(
        machine
            .read(&stream("producer-delayed-retry"), 0, 16)
            .expect("read")
            .payload,
        b"ab"
    );
}

#[test]
fn producer_state_survives_snapshot_restore() {
    let mut machine = machine();
    create_stream(&mut machine, "producer-snapshot");
    assert!(matches!(
        machine.apply(append_cmd(stream("producer-snapshot"), b"a", Append {
            producer: Some(producer("writer-1", 0, 0)),
            ..Append::default()
        })),
        StreamResponse::Appended {
            deduplicated: false,
            ..
        }
    ));

    let snapshot = machine.snapshot();
    assert_eq!(snapshot.streams[0].producer_states.len(), 1);
    // F3: the response lives in the producer's bounded receipt window.
    let producer_state = &snapshot.streams[0].producer_states[0];
    assert_eq!(
        producer_state
            .receipts
            .iter()
            .map(|receipt| (
                receipt.producer_seq,
                receipt.start_offset,
                receipt.next_offset
            ))
            .collect::<Vec<_>>(),
        vec![(0, 0, 1)]
    );
    let mut restored = StreamStateMachine::restore(snapshot).expect("restore snapshot");

    assert!(matches!(
        restored.apply(append_cmd(stream("producer-snapshot"), b"retry", Append {
            producer: Some(producer("writer-1", 0, 0)),
            ..Append::default()
        })),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 1,
            deduplicated: true,
            ..
        }
    ));
    assert_eq!(
        restored.apply(append_cmd(stream("producer-snapshot"), b"b", Append {
            producer: Some(producer("writer-1", 0, 1)),
            ..Append::default()
        })),
        appended_by(producer("writer-1", 0, 1), 1, 2, false, false)
    );
}

#[test]
fn stream_ttl_uses_sliding_access_window() {
    let mut machine = machine();
    let stream_id = stream("ttl-window");

    assert_eq!(
        machine.apply(create_cmd(stream_id.clone(), Create {
            payload: b"hi".to_vec(),
            ttl_seconds: Some(1),
            now_ms: 1_000,
            ..Create::default()
        })),
        created(stream_id.clone(), 2)
    );

    assert_eq!(
        machine.access_requires_write(&stream_id, 1_500, false),
        Ok(false)
    );
    assert_eq!(
        machine
            .head_at(&stream_id, 1_500)
            .expect("head before ttl expiry")
            .last_ttl_touch_at_ms,
        1_000
    );
    assert_eq!(
        machine.access_requires_write(&stream_id, 1_249, true),
        Ok(false)
    );
    assert_eq!(
        machine.access_requires_write(&stream_id, 1_250, true),
        Ok(true)
    );
    assert_eq!(
        machine.apply(touch_cmd(stream_id.clone(), 1_250)),
        StreamResponse::Accessed {
            changed: true,
            expired: false,
        }
    );

    assert!(machine.read_plan_at(&stream_id, 2, 16, 2_149).is_ok());
    assert_eq!(
        machine.apply(append_cmd(stream_id.clone(), b"!", Append {
            now_ms: 2_149,
            ..Append::default()
        })),
        appended(2, 3)
    );
    assert!(machine.head_at(&stream_id, 3_148).is_some());
    assert!(machine.head_at(&stream_id, 3_149).is_none());
    assert_error_code(
        machine.apply(append_cmd(stream_id.clone(), b"late", Append {
            now_ms: 3_150,
            ..Append::default()
        })),
        StreamErrorCode::StreamNotFound,
    );
}

#[test]
fn ttl_renewal_with_earlier_clock_does_not_move_expiry_earlier() {
    let mut machine = machine();
    let stream_id = stream("ttl-skew");

    assert!(matches!(
        machine.apply(create_cmd(stream_id.clone(), Create {
            payload: b"hi".to_vec(),
            ttl_seconds: Some(1),
            now_ms: 1_000,
            ..Create::default()
        })),
        StreamResponse::Created { .. }
    ));
    assert_eq!(
        machine.apply(touch_cmd(stream_id.clone(), 1_600)),
        StreamResponse::Accessed {
            changed: true,
            expired: false,
        }
    );
    // A forwarding node whose clock lags proposes earlier timestamps.
    assert_eq!(
        machine.apply(touch_cmd(stream_id.clone(), 1_300)),
        StreamResponse::Accessed {
            changed: false,
            expired: false,
        }
    );
    assert_eq!(
        machine.apply(append_cmd(stream_id.clone(), b"!", Append {
            now_ms: 1_200,
            ..Append::default()
        })),
        appended(2, 3)
    );

    assert_eq!(
        machine
            .head_at(&stream_id, 2_599)
            .expect("expiry still follows the latest touch")
            .last_ttl_touch_at_ms,
        1_600
    );
    assert!(machine.head_at(&stream_id, 2_600).is_none());
}

#[test]
fn renewed_ttl_ignores_stale_expiry_index_entry() {
    let mut machine = machine();
    let stream_id = stream("ttl-renew-stale");

    assert!(matches!(
        machine.apply(create_cmd(stream_id.clone(), Create {
            payload: b"hi".to_vec(),
            ttl_seconds: Some(1),
            now_ms: 1_000,
            ..Create::default()
        })),
        StreamResponse::Created { .. }
    ));
    assert_eq!(
        machine.apply(touch_cmd(stream_id.clone(), 1_500)),
        StreamResponse::Accessed {
            changed: true,
            expired: false,
        }
    );
    assert!(matches!(
        machine.apply(create_cmd(stream("sweep-trigger"), Create {
            now_ms: 2_100,
            ..Create::default()
        })),
        StreamResponse::Created { .. }
    ));

    assert!(machine.head(&stream_id).is_some());
    assert!(machine.head_at(&stream_id, 2_500).is_none());
}

#[test]
fn restore_rebuilds_ttl_index_from_stream_metadata() {
    let mut machine = machine();
    let stream_id = stream("ttl-restored");

    assert!(matches!(
        machine.apply(create_cmd(stream_id.clone(), Create {
            expires_at_ms: Some(2_000),
            now_ms: 1_000,
            ..Create::default()
        })),
        StreamResponse::Created { .. }
    ));

    let mut restored = StreamStateMachine::restore(machine.snapshot()).expect("restore");
    assert!(matches!(
        restored.apply(create_cmd(stream("restore-sweep-trigger"), Create {
            now_ms: 2_100,
            ..Create::default()
        })),
        StreamResponse::Created { .. }
    ));
    assert!(restored.head(&stream_id).is_none());
}

#[test]
fn stream_expires_at_is_absolute_and_recreate_after_expiry() {
    let mut machine = machine();
    let stream_id = stream("absolute-expiry");

    assert!(matches!(
        machine.apply(create_cmd(stream_id.clone(), Create {
            expires_at_ms: Some(2_000),
            now_ms: 1_000,
            ..Create::default()
        })),
        StreamResponse::Created { .. }
    ));
    assert_eq!(
        machine.apply(touch_cmd(stream_id.clone(), 1_500)),
        StreamResponse::Accessed {
            changed: false,
            expired: false,
        }
    );
    assert!(matches!(
        machine.apply(append_cmd(stream_id.clone(), b"body", Append {
            now_ms: 1_600,
            ..Append::default()
        })),
        StreamResponse::Appended { .. }
    ));
    assert!(machine.read_plan_at(&stream_id, 0, 16, 1_999).is_ok());
    assert_err_code(
        machine.read_plan_at(&stream_id, 0, 16, 2_000),
        StreamErrorCode::StreamNotFound,
    );
    assert_eq!(
        machine.apply(create_cmd(stream_id.clone(), Create {
            content_type: "text/plain",
            now_ms: 2_001,
            ..Create::default()
        })),
        created(stream_id, 0)
    );
}

#[test]
fn producer_duplicate_final_append_remains_idempotent_after_close() {
    let mut machine = machine();
    create_stream(&mut machine, "producer-close");

    assert_eq!(
        machine.apply(append_cmd(stream("producer-close"), b"final", Append {
            close_after: true,
            producer: Some(producer("writer-1", 0, 0)),
            ..Append::default()
        })),
        appended_by(producer("writer-1", 0, 0), 0, 5, true, false)
    );
    assert_eq!(
        machine.apply(append_cmd(stream("producer-close"), b"final", Append {
            close_after: true,
            producer: Some(producer("writer-1", 0, 0)),
            ..Append::default()
        })),
        appended_by(producer("writer-1", 0, 0), 0, 5, true, true)
    );
    assert_error_at(
        machine.apply(append_cmd(stream("producer-close"), b"too-late", Append {
            producer: Some(producer("writer-1", 0, 1)),
            ..Append::default()
        })),
        StreamErrorCode::StreamClosed,
        5,
    );
}

#[test]
fn append_conflict_precedence_reports_closed_before_mismatch_or_seq() {
    let mut machine = machine();
    create_stream(&mut machine, "closed-precedence");

    assert_eq!(
        machine.apply(append_cmd(stream("closed-precedence"), b"final", Append {
            close_after: true,
            stream_seq: Some("0002".to_owned()),
            ..Append::default()
        })),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 5,
            closed: true,
            deduplicated: false,
            producer: None,
            receipt_evicted: false,
        }
    );

    assert_error_at(
        machine.apply(append_cmd(
            stream("closed-precedence"),
            b"too-late",
            Append {
                content_type: Some("text/plain"),
                stream_seq: Some("0001".to_owned()),
                ..Append::default()
            },
        )),
        StreamErrorCode::StreamClosed,
        5,
    );
}

#[test]
fn checkpoint_publish_and_retention_advance_are_independent() {
    let mut machine = machine();
    create_stream(&mut machine, "snap");
    assert!(matches!(
        machine.apply(append_cmd(stream("snap"), b"abc", Append::default())),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 3,
            ..
        }
    ));
    assert!(matches!(
        machine.apply(append_cmd(stream("snap"), b"de", Append::default())),
        StreamResponse::Appended {
            offset: 3,
            next_offset: 5,
            ..
        }
    ));

    assert!(matches!(
        machine.apply(publish_snapshot_cmd(
            stream("snap"),
            3,
            "application/json",
            br#"{"state":"abc"}"#,
            0
        )),
        StreamResponse::SnapshotPublished {
            snapshot_offset: 3,
            ..
        }
    ));
    assert_eq!(
        machine
            .read(&stream("snap"), 0, 5)
            .expect("unpruned read")
            .payload,
        b"abcde"
    );
    assert_eq!(machine.retained_offset(&stream("snap")), 0);
    assert_eq!(
        machine.apply(advance_retention_cmd(stream("snap"), 3, 1)),
        StreamResponse::RetentionAdvanced { retained_offset: 3 }
    );
    assert_err_at(
        machine.read_plan(&stream("snap"), 0, 1),
        StreamErrorCode::StreamGone,
        3,
    );
    let read = machine.read(&stream("snap"), 3, 2).expect("retained read");
    assert_eq!(read.payload, b"de");
    let snapshot = machine
        .read_snapshot(&stream("snap"), 3)
        .expect("visible snapshot");
    assert_eq!(snapshot.content_type, "application/json");
    assert_eq!(snapshot.payload, br#"{"state":"abc"}"#);
    let bootstrap = machine.bootstrap_plan(&stream("snap")).expect("bootstrap");
    assert_eq!(
        bootstrap.snapshot.as_ref().map(|snapshot| snapshot.offset),
        Some(3)
    );
    assert_eq!(bootstrap.updates, vec![StreamMessageRecord {
        start_offset: 3,
        end_offset: 5,
    }]);
    let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore snapshot");
    assert_eq!(restored.retained_offset(&stream("snap")), 3);
    assert_eq!(
        restored
            .head(&stream("snap"))
            .expect("restored head")
            .tail_offset,
        5
    );
}

/// F4b: a JSON snapshot must sit on a record boundary at or above the seal
/// point; a stream without a record index accepts any offset in range.
#[test]
fn publish_snapshot_rejects_an_intra_record_json_offset_only() {
    let mut machine = machine();
    assert!(matches!(
        machine.apply(create_cmd(stream("unaligned"), Create {
            content_type: "application/json",
            payload: b"{\"a\":1}\n".to_vec(),
            ..Create::default()
        })),
        StreamResponse::Created { .. }
    ));
    assert_error_at(
        machine.apply(publish_snapshot_cmd(
            stream("unaligned"),
            2,
            "application/json",
            b"{}",
            0,
        )),
        StreamErrorCode::InvalidSnapshot,
        8,
    );

    create_stream(&mut machine, "binary");
    assert!(matches!(
        machine.apply(append_cmd(stream("binary"), b"abc", Append::default())),
        StreamResponse::Appended { .. }
    ));
    assert!(matches!(
        machine.apply(publish_snapshot_cmd(stream("binary"), 2, OCTET, b"ab", 0)),
        StreamResponse::SnapshotPublished {
            snapshot_offset: 2,
            ..
        }
    ));
}

#[test]
fn snapshot_restore_preserves_visible_snapshot_and_message_boundaries() {
    let mut machine = machine();
    create_stream(&mut machine, "restore-snap");
    let _ = machine.apply(append_cmd(
        stream("restore-snap"),
        b"abc",
        Append::default(),
    ));
    let _ = machine.apply(append_cmd(stream("restore-snap"), b"de", Append::default()));
    let _ = machine.apply(publish_snapshot_cmd(
        stream("restore-snap"),
        3,
        OCTET,
        b"abc-state",
        0,
    ));

    let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore");
    assert_eq!(
        restored
            .read_snapshot(&stream("restore-snap"), 3)
            .expect("snapshot")
            .payload,
        b"abc-state"
    );
    assert_eq!(
        restored
            .bootstrap_plan(&stream("restore-snap"))
            .expect("bootstrap")
            .updates,
        vec![StreamMessageRecord {
            start_offset: 3,
            end_offset: 5,
        }]
    );
}

fn payload_strategy() -> impl Strategy<Value = Vec<u8>> {
    vec(any::<u8>(), 1..=16)
}

fn payloads_strategy() -> impl Strategy<Value = Vec<Vec<u8>>> {
    vec(payload_strategy(), 1..=24)
}

fn append_payload(
    machine: &mut StreamStateMachine,
    stream_name: &str,
    payload: Vec<u8>,
    close_after: bool,
    producer: Option<ProducerRequest>,
) -> StreamResponse {
    machine.apply(append_cmd(stream(stream_name), &payload, Append {
        close_after,
        producer,
        ..Append::default()
    }))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn prop_appends_are_offset_monotonic_and_snapshot_round_trips(
        initial in vec(any::<u8>(), 0..=16),
        payloads in payloads_strategy(),
    ) {
        let mut machine = machine();
        let stream_id = stream("prop-offsets");
        prop_assert_eq!(
            machine.apply(create_cmd(stream_id.clone(), Create {
                payload: initial.clone(),
                ..Create::default()
            })),
            created(stream_id.clone(), u64::try_from(initial.len()).unwrap())
        );

        let mut expected = initial;
        let mut expected_tail = u64::try_from(expected.len()).unwrap();
        for payload in payloads {
            let payload_len = u64::try_from(payload.len()).unwrap();
            prop_assert_eq!(
                append_payload(&mut machine, "prop-offsets", payload.clone(), false, None),
                appended(expected_tail, expected_tail + payload_len)
            );
            expected_tail += payload_len;
            expected.extend_from_slice(&payload);

            let head = machine.head(&stream_id).expect("stream head");
            prop_assert_eq!(head.tail_offset, expected_tail);
            let read = machine
                .read(&stream_id, 0, expected.len())
                .expect("read appended payload");
            prop_assert_eq!(read.next_offset, expected_tail);
            prop_assert_eq!(read.payload, expected.clone());
            prop_assert!(read.up_to_date);
        }

        let restored =
            StreamStateMachine::restore(machine.snapshot()).expect("restore snapshot");
        let restored_read = restored
            .read(&stream_id, 0, expected.len())
            .expect("restored read");
        prop_assert_eq!(restored_read.next_offset, expected_tail);
        prop_assert_eq!(restored_read.payload, expected);
        prop_assert_eq!(
            restored.head(&stream_id).expect("restored head").tail_offset,
            expected_tail
        );
    }

    #[test]
    fn prop_producer_retries_are_idempotent_and_stale_epochs_are_fenced(
        first_payload in payload_strategy(),
        retry_payload in payload_strategy(),
        next_payload in payload_strategy(),
    ) {
        let mut machine = machine();
        create_stream(&mut machine, "prop-producer");
        let stream_id = stream("prop-producer");

        let first_len = u64::try_from(first_payload.len()).unwrap();
        prop_assert_eq!(
            append_payload(
                &mut machine,
                "prop-producer",
                first_payload.clone(),
                false,
                Some(producer("writer-1", 0, 0)),
            ),
            appended_by(producer("writer-1", 0, 0), 0, first_len, false, false)
        );

        prop_assert_eq!(
            append_payload(
                &mut machine,
                "prop-producer",
                retry_payload,
                false,
                Some(producer("writer-1", 0, 0)),
            ),
            appended_by(producer("writer-1", 0, 0), 0, first_len, false, true)
        );
        prop_assert_eq!(
            machine
                .read(&stream_id, 0, usize::try_from(first_len).unwrap())
                .expect("read after duplicate")
                .payload,
            first_payload
        );

        let next_len = u64::try_from(next_payload.len()).unwrap();
        prop_assert_eq!(
            append_payload(
                &mut machine,
                "prop-producer",
                next_payload,
                false,
                Some(producer("writer-1", 1, 0)),
            ),
            appended_by(producer("writer-1", 1, 0), first_len, first_len + next_len, false, false)
        );
        let stale_response = append_payload(
            &mut machine,
            "prop-producer",
            b"stale".to_vec(),
            false,
            Some(producer("writer-1", 0, 1)),
        );
        prop_assert!(
            matches!(
                stale_response,
                StreamResponse::Error {
                    code: StreamErrorCode::ProducerEpochStale,
                    ..
                }
            ),
            "unexpected stale producer response: {:?}",
            stale_response
        );
        prop_assert_eq!(
            machine.head(&stream_id).expect("head").tail_offset,
            first_len + next_len
        );
    }

    #[test]
    fn prop_ttl_expiry_uses_generated_wall_clock(
        start_ms in 0_u64..10_000,
        ttl_seconds in 1_u64..60,
        touch_delta_ms in 0_u64..1_000,
        payload in payload_strategy(),
    ) {
        let mut machine = machine();
        let stream_id = stream("prop-ttl");
        let ttl_ms = ttl_seconds * 1_000;
        let touch_ms = start_ms + touch_delta_ms.min(ttl_ms - 1);
        let expire_ms = touch_ms + ttl_ms;

        let create_response = machine.apply(create_cmd(stream_id.clone(), Create {
            payload,
            ttl_seconds: Some(ttl_seconds),
            now_ms: start_ms,
            ..Create::default()
        }));
        prop_assert!(
            matches!(create_response, StreamResponse::Created { .. }),
            "unexpected create response: {:?}",
            create_response
        );
        prop_assert!(machine.read_plan_at(&stream_id, 0, 16, touch_ms).is_ok());
        prop_assert_eq!(
            machine.apply(touch_cmd(stream_id.clone(), touch_ms)),
            StreamResponse::Accessed {
                changed: touch_ms != start_ms,
                expired: false,
            }
        );
        prop_assert!(machine.read_plan_at(&stream_id, 0, 16, expire_ms - 1).is_ok());
        let expired_read = machine.read_plan_at(&stream_id, 0, 16, expire_ms);
        prop_assert!(
            matches!(
                expired_read,
                Err(StreamResponse::Error {
                    code: StreamErrorCode::StreamNotFound,
                    ..
                })
            ),
            "unexpected expired read response: {:?}",
            expired_read
        );
        prop_assert_eq!(
            machine.apply(touch_cmd(stream_id, expire_ms)),
            StreamResponse::Accessed {
                changed: true,
                expired: true,
            }
        );
    }

    #[test]
    fn prop_cold_flush_preserves_tail_and_retained_hot_suffix(
        payloads in payloads_strategy(),
        max_flush_bytes in 1_usize..=64,
    ) {
        let mut machine = machine();
        create_stream(&mut machine, "prop-cold");
        let stream_id = stream("prop-cold");
        let mut expected = Vec::new();
        for payload in payloads {
            expected.extend_from_slice(&payload);
            let append_response = append_payload(&mut machine, "prop-cold", payload, false, None);
            prop_assert!(
                matches!(append_response, StreamResponse::Appended { .. }),
                "unexpected append response: {:?}",
                append_response
            );
        }
        let before = machine
            .read(&stream_id, 0, expected.len())
            .expect("pre-flush read");
        prop_assert_eq!(before.payload, expected.clone());

        let candidate = machine
            .plan_cold_flush(&stream_id, 1, max_flush_bytes)
            .expect("plan cold flush")
            .expect("flush candidate");
        prop_assert_eq!(candidate.start_offset, 0);
        prop_assert!(!candidate.payload.is_empty());
        prop_assert!(candidate.payload.len() <= max_flush_bytes);
        prop_assert_eq!(
            candidate.payload.as_slice(),
            &expected[..candidate.payload.len()]
        );

        let flush_len = candidate.payload.len();
        prop_assert_eq!(
            machine.apply(flush_candidate_cmd(
                stream_id.clone(),
                &candidate,
                "s3://bucket/prop-cold/000000"
            )),
            StreamResponse::ColdFlushed {
                hot_start_offset: candidate.end_offset,
            }
        );

        prop_assert_eq!(machine.head(&stream_id).expect("head").tail_offset, u64::try_from(expected.len()).unwrap());
        prop_assert_eq!(machine.hot_start_offset(&stream_id), u64::try_from(flush_len).unwrap());
        prop_assert!(machine.cold_chunks(&stream_id).is_empty());
        let suffix = &expected[flush_len..];
        let hot_read = machine
            .read(&stream_id, u64::try_from(flush_len).unwrap(), suffix.len())
            .expect("hot suffix read");
        prop_assert_eq!(hot_read.payload, suffix);

        let plan = machine
            .read_plan(&stream_id, 0, expected.len())
            .expect("post-flush read plan");
        prop_assert_eq!(plan.next_offset, u64::try_from(expected.len()).unwrap());
        prop_assert!(matches!(plan.segments.first(), Some(StreamReadSegment::ColdIndex(_))));

        let restored =
            StreamStateMachine::restore(machine.snapshot()).expect("restore cold snapshot");
        let restored_suffix = restored
            .read(&stream_id, u64::try_from(flush_len).unwrap(), suffix.len())
            .expect("restored hot suffix read");
        prop_assert_eq!(restored_suffix.payload, suffix);
        prop_assert_eq!(restored.cold_chunks(&stream_id), machine.cold_chunks(&stream_id));
    }

    #[test]
    fn prop_visible_snapshot_and_cold_flush_survive_restore(
        payloads in vec(payload_strategy(), 2..=24),
        snapshot_index_seed in 0_usize..24,
        max_flush_bytes in 1_usize..=64,
        snapshot_payload in vec(any::<u8>(), 0..=32),
    ) {
        let mut machine = machine();
        create_stream(&mut machine, "prop-snapshot-cold");
        let stream_id = stream("prop-snapshot-cold");

        let mut expected = Vec::new();
        let mut boundaries = Vec::with_capacity(payloads.len());
        for payload in &payloads {
            let append_response = append_payload(
                &mut machine,
                "prop-snapshot-cold",
                payload.clone(),
                false,
                None,
            );
            prop_assert!(
                matches!(append_response, StreamResponse::Appended { .. }),
                "unexpected append response: {:?}",
                append_response
            );
            let start_offset = u64::try_from(expected.len()).expect("payload len fits u64");
            expected.extend_from_slice(payload);
            let end_offset = u64::try_from(expected.len()).expect("payload len fits u64");
            boundaries.push(StreamMessageRecord {
                start_offset,
                end_offset,
            });
        }

        let snapshot_message_count = 1 + (snapshot_index_seed % (payloads.len() - 1));
        let snapshot_offset = boundaries[snapshot_message_count - 1].end_offset;
        let publish_response = machine.apply(publish_snapshot_cmd(
            stream_id.clone(),
            snapshot_offset,
            OCTET,
            &snapshot_payload,
            0
        ));
        let StreamResponse::SnapshotPublished {
            snapshot_offset: published_offset,
            ..
        } = publish_response else {
            prop_assert!(false, "unexpected snapshot response: {:?}", publish_response);
            unreachable!();
        };
        prop_assert_eq!(published_offset, snapshot_offset);
        prop_assert_eq!(
            machine.apply(advance_retention_cmd(
                stream_id.clone(),
                snapshot_offset,
                1,
            )),
            StreamResponse::RetentionAdvanced {
                retained_offset: snapshot_offset,
            }
        );

        let prefix_read = machine.read_plan(&stream_id, 0, 1);
        prop_assert!(
            matches!(
                prefix_read,
                Err(StreamResponse::Error {
                    code: StreamErrorCode::StreamGone,
                    next_offset: Some(next_offset),
                    ..
                }) if next_offset == snapshot_offset
            ),
            "unexpected prefix read response after snapshot: {:?}",
            prefix_read
        );
        prop_assert_eq!(machine.hot_start_offset(&stream_id), snapshot_offset);

        let candidate = machine
            .plan_cold_flush(&stream_id, 1, max_flush_bytes)
            .expect("plan cold flush")
            .expect("flush candidate after visible snapshot");
        prop_assert_eq!(candidate.start_offset, snapshot_offset);
        prop_assert!(!candidate.payload.is_empty());
        prop_assert!(candidate.payload.len() <= max_flush_bytes);
        let candidate_start = usize::try_from(candidate.start_offset).expect("offset fits usize");
        let candidate_end = usize::try_from(candidate.end_offset).expect("offset fits usize");
        prop_assert_eq!(candidate.payload.as_slice(), &expected[candidate_start..candidate_end]);

        prop_assert_eq!(
            machine.apply(flush_candidate_cmd(
                stream_id.clone(),
                &candidate,
                "s3://bucket/prop-snapshot-cold/000000"
            )),
            StreamResponse::ColdFlushed {
                hot_start_offset: candidate.end_offset,
            }
        );

        let tail_offset = u64::try_from(expected.len()).expect("payload len fits u64");
        prop_assert_eq!(machine.head(&stream_id).expect("head").tail_offset, tail_offset);
        prop_assert_eq!(machine.hot_start_offset(&stream_id), candidate.end_offset);
        prop_assert!(machine.cold_chunks(&stream_id).is_empty());

        let retained_plan = machine
            .read_plan(
                &stream_id,
                snapshot_offset,
                usize::try_from(tail_offset - snapshot_offset).expect("read len fits usize"),
            )
            .expect("retained read plan");
        prop_assert_eq!(retained_plan.next_offset, tail_offset);
        prop_assert!(matches!(
            retained_plan.segments.first(),
            Some(StreamReadSegment::ColdIndex(_))
        ));

        let bootstrap = machine.bootstrap_plan(&stream_id).expect("bootstrap plan");
        let expected_snapshot = machine
            .latest_snapshot(&stream_id)
            .expect("latest snapshot");
        prop_assert_eq!(
            bootstrap.snapshot.as_ref(),
            expected_snapshot.as_ref()
        );
        // The flush starts at the snapshot offset, so the messages after the
        // snapshot are now (partly) cold: bootstrap is an honest partial.
        prop_assert!(bootstrap.updates.is_empty());
        prop_assert_eq!(bootstrap.next_offset, snapshot_offset);
        prop_assert!(!bootstrap.up_to_date);

        let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore snapshot");
        prop_assert_eq!(
            restored.latest_snapshot(&stream_id).expect("latest snapshot"),
            bootstrap.snapshot.clone()
        );
        prop_assert_eq!(restored.cold_chunks(&stream_id), machine.cold_chunks(&stream_id));
        prop_assert_eq!(restored.hot_start_offset(&stream_id), candidate.end_offset);
        prop_assert_eq!(restored.bootstrap_plan(&stream_id).expect("restored bootstrap"), bootstrap);
        prop_assert_eq!(
            restored.head(&stream_id).expect("restored head").tail_offset,
            tail_offset
        );
    }

    #[test]
    fn prop_stale_cold_flush_after_delete_recreate_does_not_mutate_new_stream(
        new_payload in vec(any::<u8>(), 1..=32),
        old_extra in vec(any::<u8>(), 1..=32),
    ) {
        let mut machine = machine();
        create_stream(&mut machine, "prop-stale-cold");
        let stream_id = stream("prop-stale-cold");

        let mut old_payload = new_payload.clone();
        old_payload.extend_from_slice(&old_extra);
        let old_tail = u64::try_from(old_payload.len()).expect("payload len fits u64");
        let new_tail = u64::try_from(new_payload.len()).expect("payload len fits u64");

        let append_response = append_payload(
            &mut machine,
            "prop-stale-cold",
            old_payload.clone(),
            false,
            None,
        );
        prop_assert!(
            matches!(append_response, StreamResponse::Appended { next_offset, .. } if next_offset == old_tail),
            "unexpected old append response: {:?}",
            append_response
        );
        let candidate = machine
            .plan_cold_flush(&stream_id, old_payload.len(), old_payload.len())
            .expect("plan old cold flush")
            .expect("old cold flush candidate");
        prop_assert_eq!(candidate.start_offset, 0);
        prop_assert_eq!(candidate.end_offset, old_tail);
        prop_assert_eq!(candidate.payload.as_slice(), old_payload.as_slice());

        let delete_response = machine.apply(delete_cmd(stream_id.clone()));
        prop_assert!(
            matches!(
                delete_response,
                StreamResponse::Deleted
            ),
            "unexpected delete response: {:?}",
            delete_response
        );
        create_stream(&mut machine, "prop-stale-cold");
        let append_response = append_payload(
            &mut machine,
            "prop-stale-cold",
            new_payload.clone(),
            false,
            None,
        );
        prop_assert!(
            matches!(append_response, StreamResponse::Appended { next_offset, .. } if next_offset == new_tail),
            "unexpected new append response: {:?}",
            append_response
        );

        let stale_flush = machine.apply(flush_candidate_cmd(
            stream_id.clone(),
            &candidate,
            "s3://bucket/prop-stale-cold/old-candidate",
        ));
        prop_assert!(
            matches!(
                stale_flush,
                StreamResponse::Error {
                    code: StreamErrorCode::InvalidColdFlush,
                    next_offset: Some(next_offset),
                    ..
                } if next_offset == new_tail
            ),
            "unexpected stale flush response: {:?}",
            stale_flush
        );
        prop_assert_eq!(machine.hot_start_offset(&stream_id), 0);
        prop_assert!(machine.cold_chunks(&stream_id).is_empty());
        prop_assert_eq!(
            machine
                .read(&stream_id, 0, new_payload.len())
                .expect("new stream read")
                .payload,
            new_payload.clone()
        );

        let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore snapshot");
        prop_assert_eq!(restored.hot_start_offset(&stream_id), 0);
        prop_assert!(restored.cold_chunks(&stream_id).is_empty());
        prop_assert_eq!(
            restored
                .read(&stream_id, 0, new_payload.len())
                .expect("restored new stream read")
                .payload,
            new_payload
        );
    }

    #[test]
    fn prop_cold_flush_batch_is_deterministic_and_preview_only(
        z_payloads in payloads_strategy(),
        a_payloads in payloads_strategy(),
        m_payloads in payloads_strategy(),
        min_hot_bytes in 1_usize..=32,
        max_flush_bytes in 1_usize..=32,
        max_candidates in 1_usize..=24,
    ) {
        let mut machine = machine();

        let streams = [
            ("z-prop-batch", z_payloads),
            ("a-prop-batch", a_payloads),
            ("m-prop-batch", m_payloads),
        ];
        let mut expected_payloads = HashMap::new();
        for (stream_name, payloads) in streams {
            create_stream(&mut machine, stream_name);
            let mut expected = Vec::new();
            for payload in payloads {
                expected.extend_from_slice(&payload);
                let append_response = append_payload(&mut machine, stream_name, payload, false, None);
                prop_assert!(
                    matches!(append_response, StreamResponse::Appended { .. }),
                    "unexpected append response: {:?}",
                    append_response
                );
            }
            expected_payloads.insert(stream(stream_name), expected);
        }

        let candidates = machine
            .plan_next_cold_flush_batch(
                min_hot_bytes,
                max_flush_bytes,
                usize::MAX,
                max_candidates,
            )
            .expect("plan cold flush batch");
        let repeated_candidates = machine
            .plan_next_cold_flush_batch(
                min_hot_bytes,
                max_flush_bytes,
                usize::MAX,
                max_candidates,
            )
            .expect("repeat plan cold flush batch");
        prop_assert_eq!(candidates.as_slice(), repeated_candidates.as_slice());

        let mut next_start_by_stream = Vec::<(BucketStreamId, u64)>::new();
        for candidate in &candidates {
            let expected_start = next_start_by_stream
                .iter()
                .find(|(stream_id, _)| stream_id == &candidate.stream_id)
                .map(|(_, next_start)| *next_start)
                .unwrap_or(0);
            prop_assert_eq!(candidate.start_offset, expected_start);
            prop_assert!(candidate.end_offset > candidate.start_offset);
            prop_assert!(candidate.payload.len() <= max_flush_bytes);

            let expected = expected_payloads
                .get(&candidate.stream_id)
                .expect("candidate stream should exist");
            let start = usize::try_from(candidate.start_offset).expect("offset fits usize");
            let end = usize::try_from(candidate.end_offset).expect("offset fits usize");
            prop_assert_eq!(candidate.payload.as_slice(), &expected[start..end]);
            if let Some((_, next_start)) = next_start_by_stream
                .iter_mut()
                .find(|(stream_id, _)| stream_id == &candidate.stream_id)
            {
                *next_start = candidate.end_offset;
            } else {
                next_start_by_stream.push((candidate.stream_id.clone(), candidate.end_offset));
            }
        }

        for stream_id in expected_payloads.keys() {
            prop_assert_eq!(machine.hot_start_offset(stream_id), 0);
            prop_assert!(machine.cold_chunks(stream_id).is_empty());
        }

        for (index, candidate) in candidates.iter().enumerate() {
            prop_assert_eq!(
                machine.apply(flush_candidate_cmd(
                    candidate.stream_id.clone(),
                    candidate,
                    &format!("s3://bucket/prop-batch/{index:06}")
                )),
                StreamResponse::ColdFlushed {
                    hot_start_offset: candidate.end_offset,
                }
            );
        }

        for (stream_id, expected_start) in next_start_by_stream {
            prop_assert_eq!(machine.hot_start_offset(&stream_id), expected_start);
        }

        let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore snapshot");
        for (stream_id, expected) in expected_payloads {
            prop_assert_eq!(
                restored.head(&stream_id).expect("restored head").tail_offset,
                u64::try_from(expected.len()).expect("payload len fits u64")
            );
            prop_assert_eq!(restored.cold_chunks(&stream_id), machine.cold_chunks(&stream_id));
            prop_assert_eq!(restored.hot_start_offset(&stream_id), machine.hot_start_offset(&stream_id));
        }
    }
}

#[test]
fn purge_bucket_removes_streams_but_preserves_accounting_idempotently() {
    let mut machine = machine();
    create_stream(&mut machine, "orders");
    create_stream(&mut machine, "journal");
    assert!(matches!(
        machine.apply(append_cmd(stream("orders"), b"abc", Append::default())),
        StreamResponse::Appended { .. }
    ));
    // A second tenant that must survive the purge untouched.
    assert!(matches!(
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: "other-tenant".to_owned(),
        }),
        StreamResponse::BucketCreated { .. }
    ));
    let other = BucketStreamId::new("other-tenant", "orders");
    assert!(matches!(
        machine.apply(create_cmd(other.clone(), Create::default())),
        StreamResponse::Created { .. }
    ));
    let response = machine.apply(StreamCommand::PurgeBucket {
        bucket_id: "benchcmp".to_owned(),
    });
    let StreamResponse::BucketPurged {
        bucket_id,
        removed_streams,
        pending_cold_gc_entries,
    } = response
    else {
        panic!("expected BucketPurged, got {response:?}");
    };
    assert_eq!(bucket_id, "benchcmp");
    assert_eq!(removed_streams, 2);
    assert_eq!(pending_cold_gc_entries, 0);

    // Content and the bucket are gone. The aggregate ledger remains with zero
    // gauges so an asynchronous meter cannot miss the committed writes.
    let purged_usage = bucket_usage(&machine, "benchcmp");
    assert_eq!(purged_usage.stream_count, 0);
    assert_eq!(purged_usage.retained_bytes, 0);
    assert!(purged_usage.committed_write_units > 0);
    let snapshot = machine.snapshot();
    assert_eq!(snapshot.erased_buckets, vec!["benchcmp".to_owned()]);
    let mut restored = StreamStateMachine::restore(snapshot).expect("restore snapshot");
    assert_eq!(bucket_usage(&restored, "benchcmp"), purged_usage);
    assert!(matches!(
        machine.apply(append_cmd(stream("orders"), b"x", Append::default())),
        StreamResponse::Error { .. }
    ));
    // Commands linearized after the purge can never recreate the namespace.
    assert!(matches!(
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: "benchcmp".to_owned(),
        }),
        StreamResponse::Error {
            code: StreamErrorCode::BucketErased,
            ..
        }
    ));
    assert!(matches!(
        restored.apply(StreamCommand::CreateBucket {
            bucket_id: "benchcmp".to_owned(),
        }),
        StreamResponse::Error {
            code: StreamErrorCode::BucketErased,
            ..
        }
    ));
    assert!(matches!(
        restored.apply(create_cmd(stream("after-restore"), Create::default())),
        StreamResponse::Error {
            code: StreamErrorCode::BucketErased,
            ..
        }
    ));
    // The other tenant is untouched.
    assert_eq!(bucket_usage(&machine, "other-tenant").stream_count, 1);

    // Idempotent re-run reports zero removals.
    let rerun = machine.apply(StreamCommand::PurgeBucket {
        bucket_id: "benchcmp".to_owned(),
    });
    assert!(matches!(rerun, StreamResponse::BucketPurged {
        removed_streams: 0,
        ..
    }));
}

#[test]
fn restore_refuses_another_format_epoch() {
    let mut snapshot = machine().snapshot();
    snapshot.format_epoch = crate::FORMAT_EPOCH - 1;
    assert_eq!(
        StreamStateMachine::restore(snapshot.clone()).expect_err("other epoch"),
        StreamSnapshotError::FormatEpoch {
            found: crate::FORMAT_EPOCH - 1,
        }
    );
    assert!(matches!(
        StreamStateMachine::new().apply(StreamCommand::ImportSnapshot {
            snapshot: Box::new(snapshot),
        }),
        StreamResponse::Error {
            code: StreamErrorCode::ImportInvalid,
            ..
        }
    ));
}

#[test]
fn import_snapshot_never_lowers_last_created_at_ms() {
    // SM3: an empty group whose C7 counter is ahead of a backup keeps its
    // counter when it imports that backup, so the next incarnation never
    // reuses a `created_at_ms` its objects may still be scoped to.
    let mut target = StreamStateMachine::restore(StreamSnapshot {
        last_created_at_ms: 5_000,
        ..StreamStateMachine::new().snapshot()
    })
    .expect("restore empty group");
    assert_eq!(target.last_created_at_ms(), 5_000);

    let mut backup = machine();
    create_stream(&mut backup, "a");
    assert_eq!(backup.last_created_at_ms(), 1);
    assert!(matches!(
        target.apply(StreamCommand::ImportSnapshot {
            snapshot: Box::new(backup.snapshot()),
        }),
        StreamResponse::SnapshotImported { .. }
    ));
    assert_eq!(target.last_created_at_ms(), 5_000);
    create_stream(&mut target, "b");
    assert_eq!(created_at_ms(&target, "b"), 5_001);
}

fn created_at_ms(machine: &StreamStateMachine, id: &str) -> u64 {
    machine
        .head(&stream(id))
        .expect("stream exists")
        .created_at_ms
}

fn flush_and_delete(machine: &mut StreamStateMachine, id: &str) {
    assert!(matches!(
        machine.apply(append_cmd(stream(id), b"cold", Append::default())),
        StreamResponse::Appended { .. }
    ));
    assert!(matches!(
        machine.apply(flush_cold_cmd(machine, stream(id), 0, 4, "chunk", 4)),
        StreamResponse::ColdFlushed { .. }
    ));
    assert_eq!(
        machine.apply(delete_cmd(stream(id))),
        StreamResponse::Deleted
    );
}

#[test]
fn c7_created_at_ms_is_unique_under_a_frozen_clock() {
    let mut machine = machine();
    let mut seen = Vec::new();
    for _ in 0..3 {
        create_stream(&mut machine, "frozen");
        seen.push(created_at_ms(&machine, "frozen"));
        assert_eq!(
            machine.apply(delete_cmd(stream("frozen"))),
            StreamResponse::Deleted
        );
    }
    create_stream(&mut machine, "other");
    seen.push(created_at_ms(&machine, "other"));
    assert_eq!(seen, vec![1, 2, 3, 4]);
    assert_eq!(machine.last_created_at_ms(), 4);

    // A later clock wins over the counter.
    assert_eq!(
        machine.apply(create_cmd(stream("later"), Create {
            now_ms: 1_000,
            ..Create::default()
        })),
        created(stream("later"), 0)
    );
    assert_eq!(created_at_ms(&machine, "later"), 1_000);
    assert_eq!(machine.last_created_at_ms(), 1_000);
}

#[test]
fn c7_last_created_at_ms_survives_snapshot_round_trip_and_legacy_decode() {
    let mut machine = machine();
    create_stream(&mut machine, "a");
    assert_eq!(
        machine.apply(delete_cmd(stream("a"))),
        StreamResponse::Deleted
    );
    let snapshot = machine.snapshot();
    assert_eq!(snapshot.last_created_at_ms, 1);
    let mut restored = StreamStateMachine::restore(snapshot).expect("restore snapshot");
    assert_eq!(restored.last_created_at_ms(), 1);
    create_stream(&mut restored, "a");
    assert_eq!(created_at_ms(&restored, "a"), 2);

    // A snapshot without the field restores normalized upwards, above every
    // live incarnation's creation time.
    let mut value = serde_json::to_value(restored.snapshot()).expect("encode snapshot");
    value
        .as_object_mut()
        .expect("snapshot object")
        .remove("last_created_at_ms");
    let legacy: StreamSnapshot = serde_json::from_value(value).expect("decode legacy snapshot");
    assert_eq!(legacy.last_created_at_ms, 0);
    let mut restored = StreamStateMachine::restore(legacy).expect("restore legacy snapshot");
    assert_eq!(restored.last_created_at_ms(), 2);
    create_stream(&mut restored, "b");
    assert_eq!(created_at_ms(&restored, "b"), 3);
}

#[test]
fn f14g_stream_gc_entries_name_the_incarnation() {
    let mut machine = machine();
    create_stream(&mut machine, "gc");
    let first = created_at_ms(&machine, "gc");
    assert_eq!(machine.cold_index_generation(&stream("gc")), Some(first));
    flush_and_delete(&mut machine, "gc");
    create_stream(&mut machine, "gc");
    let second = created_at_ms(&machine, "gc");
    assert_ne!(first, second);

    let planned = machine.plan_cold_gc_batch(8);
    assert_eq!(planned.len(), 1);
    assert_eq!(planned[0].entry.target, ColdGcTarget::Stream(stream("gc")));
    assert_eq!(planned[0].entry.cold_generation, Some(first));
    assert_eq!(planned[0].live_cold_generation, Some(second));

    // Generations and GC entries survive a snapshot round trip.
    let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore snapshot");
    assert_eq!(restored.cold_index_generation(&stream("gc")), Some(second));
    assert_eq!(restored.plan_cold_gc_batch(8), planned);
}

#[test]
fn f14g_external_create_keeps_its_payload_in_state_for_gc() {
    let mut machine = machine();
    let external = ExternalPayloadRef {
        s3_path: "benchcmp/ext/external/initial.bin".to_owned(),
        payload_len: 4,
        object_size: 4,
    };
    assert!(matches!(
        machine.apply(StreamCommand::CreateExternal {
            stream_id: stream("ext"),
            content_type: OCTET.to_owned(),
            initial_payload: external.clone(),
            record_ends: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            now_ms: 0,
        }),
        StreamResponse::Created { .. }
    ));
    let plan = machine
        .read_plan(&stream("ext"), 0, 4)
        .expect("read plan for external create");
    assert!(matches!(
        plan.segments.as_slice(),
        [StreamReadSegment::Object(segment)] if segment.object.s3_path == external.s3_path
    ));
    assert_eq!(
        machine.apply(delete_cmd(stream("ext"))),
        StreamResponse::Deleted
    );
    let entries = machine.pending_cold_gc_batch(8);
    assert_eq!(entries.len(), 2);
    assert_eq!(
        entries[1].target,
        ColdGcTarget::Paths(vec![external.s3_path.clone()])
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Bootstrap of a binary stream is either complete (`[S, tail)` as one
    /// hot part) or an honest partial that hands the client back to ordinary
    /// reads at the snapshot offset, exactly when `S` is below the seal point.
    #[test]
    fn prop_binary_bootstrap_is_one_hot_part_or_honest_partial(
        payloads in payloads_strategy(),
        flush_bytes in 0_usize..=96,
        snapshot_index_seed in 0_usize..=24,
        publish_snapshot in any::<bool>(),
    ) {
        let mut machine = machine();
        create_stream(&mut machine, "prop-boot");
        let stream_id = stream("prop-boot");
        let mut messages = Vec::with_capacity(payloads.len());
        let mut tail = 0_u64;
        for payload in &payloads {
            let response = append_payload(&mut machine, "prop-boot", payload.clone(), false, None);
            let appended = matches!(response, StreamResponse::Appended { .. });
            prop_assert!(appended);
            let end = tail + u64::try_from(payload.len()).expect("len fits u64");
            messages.push(StreamMessageRecord { start_offset: tail, end_offset: end });
            tail = end;
        }
        if flush_bytes > 0
            && let Some(candidate) = machine
                .plan_cold_flush(&stream_id, 1, flush_bytes)
                .expect("plan cold flush")
        {
            let flushed = matches!(
                machine.apply(flush_candidate_cmd(stream_id.clone(), &candidate, "s3://b/prop-boot/0")),
                StreamResponse::ColdFlushed { .. }
            );
            prop_assert!(flushed);
        }
        let mut snapshot_offset = 0;
        if publish_snapshot {
            let index = snapshot_index_seed % (messages.len() + 1);
            snapshot_offset = if index == 0 { 0 } else { messages[index - 1].end_offset };
            let published = matches!(
                machine.apply(publish_snapshot_cmd(stream_id.clone(), snapshot_offset, OCTET, b"s", 0)),
                StreamResponse::SnapshotPublished { .. }
            );
            prop_assert!(published);
        }

        let plan = machine.bootstrap_plan(&stream_id).expect("bootstrap");
        let hot_start = machine.hot_start_offset(&stream_id);
        // The seal point: the first hot byte, or the tail when nothing is hot.
        prop_assert_eq!(plan.up_to_date, snapshot_offset >= hot_start);
        if plan.up_to_date {
            prop_assert_eq!(plan.next_offset, tail);
            let expected = (snapshot_offset < tail)
                .then_some(StreamMessageRecord { start_offset: snapshot_offset, end_offset: tail })
                .into_iter()
                .collect::<Vec<_>>();
            prop_assert_eq!(&plan.updates, &expected);
        } else {
            prop_assert!(plan.updates.is_empty());
            prop_assert_eq!(plan.next_offset, snapshot_offset);
        }
    }
}

fn append_external_cmd(stream_id: BucketStreamId, s3_path: &str, len: u64) -> StreamCommand {
    StreamCommand::AppendExternal {
        stream_id,
        content_type: Some(OCTET.to_owned()),
        payload: ExternalPayloadRef {
            s3_path: s3_path.to_owned(),
            payload_len: len,
            object_size: len,
        },
        record_ends: Vec::new(),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 0,
    }
}

/// Bounded-state D1: hot bytes, then an external append above them, then a
/// flush of the hot prefix. The flush used to assign the scalar frontier
/// below the external, which left `[2, 5)` without a payload source.
fn d1_regressed_frontier_machine(id: &str) -> StreamStateMachine {
    let mut machine = machine();
    create_stream(&mut machine, id);
    assert!(matches!(
        machine.apply(append_cmd(stream(id), b"ab", Append::default())),
        StreamResponse::Appended { .. }
    ));
    let response = machine.apply(append_external_cmd(stream(id), "external/xyz.bin", 3));
    assert!(
        matches!(response, StreamResponse::Appended { .. }),
        "{response:?}"
    );
    assert!(matches!(
        machine.apply(flush_cold_cmd(
            &machine,
            stream(id),
            0,
            2,
            "chunks/ab.bin",
            2
        )),
        StreamResponse::ColdFlushed { .. }
    ));
    machine
}

#[test]
fn d1_read_plan_serves_external_bytes_above_a_flushed_hot_prefix() {
    let machine = d1_regressed_frontier_machine("d1-read");
    let plan = machine
        .read_plan(&stream("d1-read"), 0, 16)
        .expect("plan covers the external above the flushed prefix");
    assert_eq!(plan.next_offset, 5);
    // F5: the external append keeps its locator in state until the offload
    // pass, so the plan serves it as an object segment after the flushed
    // prefix.
    let covered = plan
        .segments
        .iter()
        .map(|segment| match segment {
            StreamReadSegment::ColdIndex(segment) => {
                ("cold", segment.read_start_offset, segment.len)
            }
            StreamReadSegment::Object(segment) => {
                ("object", segment.read_start_offset, segment.len)
            }
            other => panic!("expected no hot segment, got {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(covered, vec![("cold", 0, 2), ("object", 2, 3)]);
    let tail_plan = machine
        .read_plan(&stream("d1-read"), 2, 16)
        .expect("plan from inside the external");
    assert_eq!(tail_plan.next_offset, 5);
}

#[test]
fn d1_snapshot_with_regressed_frontier_restores() {
    let machine = d1_regressed_frontier_machine("d1-restore");
    let snapshot = machine.snapshot();
    let restored = StreamStateMachine::restore(snapshot).expect("restore regressed frontier");
    let plan = restored
        .read_plan(&stream("d1-restore"), 0, 16)
        .expect("restored plan");
    assert_eq!(plan.next_offset, 5);
    assert_eq!(restored.snapshot(), machine.snapshot());
}

#[test]
fn d1_read_plan_keeps_hot_bytes_between_cold_ranges() {
    let mut machine = d1_regressed_frontier_machine("d1-hot-gap");
    assert!(matches!(
        machine.apply(append_cmd(stream("d1-hot-gap"), b"cd", Append::default())),
        StreamResponse::Appended { .. }
    ));
    assert!(matches!(
        machine.apply(append_external_cmd(
            stream("d1-hot-gap"),
            "external/uvw.bin",
            3
        )),
        StreamResponse::Appended { .. }
    ));
    let plan = machine
        .read_plan(&stream("d1-hot-gap"), 0, 64)
        .expect("plan");
    assert_eq!(plan.next_offset, 10);
    let shape = plan
        .segments
        .iter()
        .map(|segment| match segment {
            StreamReadSegment::ColdIndex(segment) => ("cold", segment.read_start_offset),
            StreamReadSegment::Hot(_) => ("hot", 0),
            StreamReadSegment::Object(_) => ("object", 0),
        })
        .collect::<Vec<_>>();
    // F5: both externals are served from their in-state locators.
    assert_eq!(shape, vec![
        ("cold", 0),
        ("object", 0),
        ("hot", 0),
        ("object", 0)
    ]);
    StreamStateMachine::restore(machine.snapshot()).expect("restore with a hot gap");
}

/// Publishes one shared slice `[start, end)` of `pack` for `id`.
fn flush_shared_slice(
    machine: &mut StreamStateMachine,
    id: &str,
    start: u64,
    end: u64,
    pack: &str,
) -> ColdChunkRef {
    let chunk = ColdChunkRef {
        start_offset: start,
        end_offset: end,
        s3_path: pack.to_owned(),
        object_size: 1_024,
        object_offset: 0,
        shared_object: true,
        payload_digest: String::new(),
    };
    assert!(matches!(
        machine.apply(StreamCommand::FlushCold {
            stream_id: stream(id),
            chunk: chunk.clone(),
            cold_generation: machine
                .cold_index_generation(&stream(id))
                .unwrap_or_default(),
        }),
        StreamResponse::ColdFlushed { .. }
    ));
    chunk
}

/// F2 discovery: a stream qualifies at T shared refs, or with one shared
/// ref once its tail has stayed put for the idle period; candidates come
/// fewest-live-slices first, and compacting a stream's run releases the
/// packs it was the last reference of.
#[test]
fn shared_ref_candidates_follow_threshold_idle_and_pack_occupancy() {
    let mut machine = machine();
    for id in ["busy", "idle", "quiet"] {
        create_stream(&mut machine, id);
    }
    // `busy` holds 3 slices of three single-slice packs; `idle` and `quiet`
    // share one pack with each other.
    for index in 0..3_u64 {
        machine.apply(append_cmd(stream("busy"), b"abcd", Append::default()));
        flush_shared_slice(
            &mut machine,
            "busy",
            index * 4,
            index * 4 + 4,
            &format!("benchcmp/_packs/00000000/busy-{index}.bin"),
        );
    }
    for id in ["idle", "quiet"] {
        machine.apply(append_cmd(stream(id), b"abcd", Append::default()));
        flush_shared_slice(&mut machine, id, 0, 4, "benchcmp/_packs/00000000/two.bin");
    }

    let mut tracker = SharedRefIdleTracker::default();
    let mut request = SharedRefCompactionRequest {
        min_refs: 3,
        idle_ms: 1_000,
        now_ms: 10_000,
        max_run_bytes: 8,
        limit: 16,
        legacy_packs_only: false,
    };
    let candidates = machine.shared_ref_candidates(&request, &mut tracker);
    assert_eq!(
        candidates
            .iter()
            .map(|c| c.stream_id.stream_id.as_str())
            .collect::<Vec<_>>(),
        vec!["busy"],
        "only the stream at the threshold qualifies before anyone is idle"
    );
    let busy = &candidates[0];
    assert_eq!(busy.shared_refs, 3);
    assert_eq!(busy.run_bytes(), 8, "the run stops at max_run_bytes");
    assert_eq!(busy.min_pack_live_slices, 1);
    assert_eq!(tracker.len(), 3);

    // `quiet` keeps appending (hot bytes only), so only `idle` goes idle.
    machine.apply(append_cmd(stream("quiet"), b"e", Append::default()));
    request.now_ms = 11_500;
    let candidates = machine.shared_ref_candidates(&request, &mut tracker);
    assert_eq!(
        candidates
            .iter()
            .map(|c| (c.stream_id.stream_id.as_str(), c.min_pack_live_slices))
            .collect::<Vec<_>>(),
        vec![("busy", 1), ("idle", 2)],
        "fewest live slices first"
    );

    // Compacting `busy`'s whole run releases its packs with the grace.
    let run = plan_shared_ref_run(machine.cold_chunks(&stream("busy")), u64::MAX);
    assert_eq!(run.len(), 3);
    assert!(matches!(
        machine.apply(StreamCommand::CompactCold {
            stream_id: stream("busy"),
            old_chunks: run,
            replacement: ColdChunkRef {
                start_offset: 0,
                end_offset: 12,
                s3_path: "benchcmp/busy/chunks/0-12.bin".to_owned(),
                object_size: 12,
                object_offset: 0,
                shared_object: false,
                payload_digest: String::new(),
            },
            gc_not_before_ms: 99_000,
        }),
        StreamResponse::ColdCompacted { .. }
    ));
    let referenced = machine.group_referenced_cold_paths();
    for index in 0..3 {
        assert!(
            referenced.contains(&format!("benchcmp/_packs/00000000/busy-{index}.bin")),
            "released packs stay referenced by their GC entry until it is acked"
        );
    }
    assert!(referenced.contains("benchcmp/_packs/00000000/two.bin"));
    assert!(
        machine
            .shared_ref_candidates(&request, &mut tracker)
            .iter()
            .all(|c| c.stream_id != stream("busy"))
    );
    assert_eq!(
        tracker.len(),
        2,
        "streams without shared refs leave the tracker"
    );
    assert_eq!(machine.stream_referenced_cold_paths(&stream("idle")), vec![
        "benchcmp/_packs/00000000/two.bin".to_owned()
    ]);
    assert_eq!(machine.bucket_ids(), vec!["benchcmp".to_owned()]);

    let pending = machine.pending_cold_gc_batch(16);
    let last = pending.last().expect("pack releases queued").seq;
    machine.apply(StreamCommand::AckColdGc { up_to_seq: last });
    let referenced = machine.group_referenced_cold_paths();
    assert!(!referenced.contains("benchcmp/_packs/00000000/busy-0.bin"));
}

/// The legacy-pack filter (#278) on F2 discovery: only slices of packs
/// outside the stream's own `{bucket}/_packs/` count, any one makes the
/// stream a candidate, the run covers only them, and the idle tracker is not
/// touched.
#[test]
fn shared_ref_candidates_legacy_filter_selects_only_legacy_pack_slices() {
    let mut machine = machine();
    for id in ["legacy", "modern"] {
        create_stream(&mut machine, id);
    }
    for (index, pack) in [
        "_packs/old-0.bin",
        "_packs/old-1.bin",
        "benchcmp/_packs/0/new.bin",
    ]
    .iter()
    .enumerate()
    {
        let start = u64::try_from(index).unwrap() * 4;
        machine.apply(append_cmd(stream("legacy"), b"abcd", Append::default()));
        flush_shared_slice(&mut machine, "legacy", start, start + 4, pack);
    }
    machine.apply(append_cmd(stream("modern"), b"abcd", Append::default()));
    flush_shared_slice(&mut machine, "modern", 0, 4, "benchcmp/_packs/0/new.bin");

    let mut tracker = SharedRefIdleTracker::default();
    let candidates = machine.shared_ref_candidates(
        &SharedRefCompactionRequest::legacy_packs(u64::MAX, 16),
        &mut tracker,
    );
    assert_eq!(candidates.len(), 1);
    let legacy = &candidates[0];
    assert_eq!(legacy.stream_id, stream("legacy"));
    assert_eq!(legacy.shared_refs, 2, "only legacy slices count");
    assert_eq!(
        legacy
            .run
            .iter()
            .map(|chunk| chunk.s3_path.as_str())
            .collect::<Vec<_>>(),
        vec!["_packs/old-0.bin", "_packs/old-1.bin"]
    );
    assert!(
        tracker.is_empty(),
        "the legacy filter leaves the idle tracker alone"
    );
    assert!(crate::is_legacy_cross_bucket_pack(
        &stream("legacy"),
        &legacy.run[0]
    ));
}

/// bounded-stream-state F10 maximum hot age: in a group below its flush
/// threshold, a stream's small hot tail is flushed whole once it has been
/// hot for `flush_max_hot_age`; younger tails stay hot.
#[test]
fn flush_planner_flushes_tails_older_than_the_max_hot_age() {
    const AGE_MS: u64 = 300_000;
    let mut machine = machine();
    for name in ["old", "young"] {
        create_stream(&mut machine, name);
    }
    append_all(&mut machine, "old", &[b"abcd", b"efgh"]);
    let aged_request = |now_ms: u64| {
        let mut request = planner_request(1 << 20, 1 << 20, 1 << 20, usize::MAX);
        request.max_hot_age = Some(ColdFlushHotAge {
            now_ms,
            max_age_ms: AGE_MS,
        });
        request
    };
    // Without the age the group (8 B) is far below its 1 MiB threshold.
    let pass = machine
        .plan_cold_flush_pass(planner_request(1 << 20, 1 << 20, 1 << 20, usize::MAX))
        .expect("plan pass");
    assert!(pass.candidates.is_empty());
    let pass = machine
        .plan_cold_flush_pass(aged_request(1_000))
        .expect("plan pass");
    assert!(pass.candidates.is_empty(), "nothing is old yet");
    append_all(&mut machine, "young", &[b"ij"]);
    let pass = machine
        .plan_cold_flush_pass(aged_request(1_000 + AGE_MS / 2))
        .expect("plan pass");
    assert!(pass.candidates.is_empty());

    let pass = machine
        .plan_cold_flush_pass(aged_request(1_000 + AGE_MS))
        .expect("plan pass");
    assert_eq!(pass.candidates.len(), 1);
    let candidate = &pass.candidates[0];
    assert_eq!(candidate.stream_id, stream("old"));
    assert_eq!((candidate.start_offset, candidate.end_offset), (0, 8));
    apply_flush_pass(&mut machine, &pass, 0);
    assert_eq!(machine.hot_payload_len(&stream("old")).expect("hot"), 0);
    assert_eq!(machine.hot_payload_len(&stream("young")).expect("hot"), 2);

    // `young` was first seen hot at AGE/2 + 1s and ages out on its own clock.
    let pass = machine
        .plan_cold_flush_pass(aged_request(1_000 + AGE_MS / 2 + AGE_MS))
        .expect("plan pass");
    assert_eq!(pass.candidates.len(), 1);
    assert_eq!(pass.candidates[0].stream_id, stream("young"));
}
