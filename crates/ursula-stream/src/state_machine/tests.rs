use std::collections::HashMap;

use proptest::collection::vec;
use proptest::prelude::*;
use serde_json::json;

use super::persist::message_records_cover_retained_suffix;
use super::*;
use crate::RecordIndexError;
use crate::StreamRecordRange;
use crate::integrity::StreamIntegritySnapshot;

fn stream(id: &str) -> BucketStreamId {
    BucketStreamId::new("benchcmp", id)
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

fn create_stream(machine: &mut StreamStateMachine, id: &str) {
    assert_eq!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream(id),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            attrs: None,
            now_ms: 0,
        }),
        StreamResponse::Created {
            stream_id: stream(id),
            next_offset: 0,
            closed: false,
        }
    );
}

fn attrs(title: &str, purpose: &str) -> StreamAttrs {
    let metadata = json!({
        "agent": { "id": "agent-1", "version": 2 },
        "purpose": purpose
    })
    .as_object()
    .expect("metadata object")
    .clone();
    StreamAttrs {
        title: Some(title.to_owned()),
        metadata,
    }
}

#[test]
fn json_record_coordinates_survive_flush_restore_and_retention() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    let stream_id = stream("json-records");
    assert_eq!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: "application/json".to_owned(),
            initial_payload: b"{\"a\":1}\n{\"b\":2}\n".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            attrs: None,
            now_ms: 0,
        }),
        StreamResponse::Created {
            stream_id: stream_id.clone(),
            next_offset: 16,
            closed: false,
        }
    );
    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream_id.clone(),
            content_type: Some("application/json".to_owned()),
            payload: b"{\"c\":3}\n".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 1,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 16,
            next_offset: 24,
            closed: false,
            deduplicated: false,
            producer: None,
        }
    );
    assert_eq!(
        machine.record_range(&stream_id),
        Ok(Some(StreamRecordRange {
            first_record: 0,
            next_record: 3,
        }))
    );
    assert_eq!(machine.offset_for_record(&stream_id, 2), Ok(Some(16)));

    let candidate = machine
        .plan_cold_flush(&stream_id, 1, 1024)
        .expect("plan cold flush")
        .expect("flush candidate");
    assert_eq!(
        machine.apply(StreamCommand::FlushCold {
            stream_id: stream_id.clone(),
            chunk: ColdChunkRef {
                start_offset: candidate.start_offset,
                end_offset: candidate.end_offset,
                s3_path: "s3://bucket/json-records".to_owned(),
                object_size: u64::try_from(candidate.payload.len()).expect("payload len fits u64"),
            },
        }),
        StreamResponse::ColdFlushed {
            hot_start_offset: candidate.end_offset,
        }
    );
    let mut restored = StreamStateMachine::restore(machine.snapshot()).expect("restore snapshot");
    assert_eq!(restored.offset_for_record(&stream_id, 1), Ok(Some(8)));
    assert_eq!(
        restored.apply(StreamCommand::PublishSnapshot {
            stream_id: stream_id.clone(),
            snapshot_offset: 16,
            content_type: "application/json".to_owned(),
            payload: b"{\"state\":2}".to_vec(),
            now_ms: 2,
        }),
        StreamResponse::SnapshotPublished {
            snapshot_offset: 16,
            record_range: Some(StreamRecordRange {
                first_record: 2,
                next_record: 3,
            }),
        }
    );
    assert_eq!(
        restored.record_range(&stream_id),
        Ok(Some(StreamRecordRange {
            first_record: 2,
            next_record: 3,
        }))
    );
    assert_eq!(
        restored.offset_for_record(&stream_id, 1),
        Err(RecordIndexError::RecordGone {
            first_record: 2,
            next_record: 3,
        })
    );
    assert_eq!(restored.offset_for_record(&stream_id, 3), Ok(Some(24)));
}

#[test]
fn snapshot_record_trim_failure_does_not_mutate_stream_state() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    let stream_id = stream("snapshot-record-atomicity");
    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: "application/json".to_owned(),
            initial_payload: b"{\"a\":1}\n{\"b\":2}\n".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            attrs: None,
            now_ms: 0,
        }),
        StreamResponse::Created { .. }
    ));

    // Simulate an internally inconsistent index so record trimming fails even
    // though the requested snapshot offset is a retained message boundary.
    machine
        .stream_slot_mut(&stream_id)
        .expect("stream slot")
        .record_index
        .as_mut()
        .expect("record index")
        .retain_from_offset(8, 16)
        .expect("advance test index");
    let before = machine.snapshot();

    assert!(matches!(
        machine.apply(StreamCommand::PublishSnapshot {
            stream_id,
            snapshot_offset: 0,
            content_type: "application/json".to_owned(),
            payload: br#"{"state":0}"#.to_vec(),
            now_ms: 1,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::InvalidRecordBoundaries,
            ..
        }
    ));
    assert_eq!(machine.snapshot(), before);
}

fn producer(id: &str, epoch: u64, seq: u64) -> ProducerRequest {
    ProducerRequest {
        producer_id: id.to_owned(),
        producer_epoch: epoch,
        producer_seq: seq,
    }
}

fn empty_integrity() -> StreamIntegritySnapshot {
    let empty = setsum::Setsum::default().hexdigest();
    StreamIntegritySnapshot {
        live_setsum: empty.clone(),
        evicted_setsum: empty.clone(),
        total_setsum: empty,
        live_start_offset: 0,
        tail_offset: 0,
        live_records: 0,
        evicted_records: 0,
        total_records: 0,
    }
}

#[test]
fn create_stream_stores_stream_attrs_separately() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    let attrs = attrs("Support session", "customer-support");

    assert_eq!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream("attrs"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            attrs: Some(attrs.clone()),
            now_ms: 0,
        }),
        StreamResponse::Created {
            stream_id: stream("attrs"),
            next_offset: 0,
            closed: false,
        }
    );

    assert_eq!(machine.stream_attrs(&stream("attrs")), Some(&attrs));
    assert!(machine.head(&stream("attrs")).is_some());
}

#[test]
fn update_stream_attrs_replaces_existing_attrs() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "attrs");
    let first = attrs("Support session", "customer-support");
    let second = attrs("Escalated session", "incident-review");

    assert_eq!(
        machine.apply(StreamCommand::UpdateStreamAttrs {
            stream_id: stream("attrs"),
            attrs: Some(first.clone()),
            now_ms: 0,
        }),
        StreamResponse::AttrsUpdated { changed: true }
    );
    assert_eq!(machine.stream_attrs(&stream("attrs")), Some(&first));

    assert_eq!(
        machine.apply(StreamCommand::UpdateStreamAttrs {
            stream_id: stream("attrs"),
            attrs: Some(second.clone()),
            now_ms: 0,
        }),
        StreamResponse::AttrsUpdated { changed: true }
    );

    assert_eq!(machine.stream_attrs(&stream("attrs")), Some(&second));
}

#[test]
fn update_stream_attrs_is_allowed_after_stream_is_closed() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "attrs-closed");
    let attrs = attrs("Closed session", "post-close-metadata");

    assert_eq!(
        machine.apply(StreamCommand::Close {
            stream_id: stream("attrs-closed"),
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Closed {
            next_offset: 0,
            deduplicated: false,
            producer: None,
        }
    );
    assert_eq!(
        machine.apply(StreamCommand::UpdateStreamAttrs {
            stream_id: stream("attrs-closed"),
            attrs: Some(attrs.clone()),
            now_ms: 0,
        }),
        StreamResponse::AttrsUpdated { changed: true }
    );

    assert_eq!(machine.stream_attrs(&stream("attrs-closed")), Some(&attrs));
}

fn oversized_attrs() -> StreamAttrs {
    let mut attrs = attrs("Oversized", "size-cap");
    attrs.metadata.insert(
        "blob".to_owned(),
        serde_json::Value::String("x".repeat(MAX_STREAM_ATTRS_BYTES + 1)),
    );
    attrs
}

#[test]
fn create_stream_rejects_oversized_attrs() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);

    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream("attrs-too-big"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            attrs: Some(oversized_attrs()),
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::InvalidStreamAttrs,
            ..
        }
    ));
    assert!(machine.head(&stream("attrs-too-big")).is_none());
}

#[test]
fn update_stream_attrs_rejects_oversized_attrs() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "attrs-cap");

    assert!(matches!(
        machine.apply(StreamCommand::UpdateStreamAttrs {
            stream_id: stream("attrs-cap"),
            attrs: Some(oversized_attrs()),
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::InvalidStreamAttrs,
            ..
        }
    ));
    assert_eq!(machine.stream_attrs(&stream("attrs-cap")), None);
}

#[test]
fn stream_command_decodes_pre_attrs_wal_records() {
    let command = StreamCommand::CreateStream {
        stream_id: stream("legacy-wal"),
        content_type: "application/octet-stream".to_owned(),
        initial_payload: b"abc".to_vec(),
        close_after: false,
        stream_seq: None,
        producer: None,
        stream_ttl_seconds: None,
        stream_expires_at_ms: None,
        attrs: None,
        now_ms: 7,
    };
    let mut value = serde_json::to_value(&command).expect("encode command");
    let fields = value
        .get_mut("CreateStream")
        .expect("create stream variant")
        .as_object_mut()
        .expect("variant object");
    assert!(fields.remove("attrs").is_some());

    let decoded: StreamCommand =
        serde_json::from_value(value).expect("decode pre-attrs WAL record");
    assert_eq!(decoded, command);
}

#[test]
fn stream_create_requires_existing_bucket_and_valid_ids() {
    let mut machine = StreamStateMachine::new();

    assert!(matches!(
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: "Bad".to_owned(),
        }),
        StreamResponse::Error {
            code: StreamErrorCode::InvalidBucketId,
            ..
        }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream("s-1"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            attrs: None,
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::BucketNotFound,
            ..
        }
    ));

    create_bucket(&mut machine);
    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream("streams"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            attrs: None,
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::InvalidStreamId,
            ..
        }
    ));
}

#[test]
fn create_stream_is_idempotent_only_when_metadata_matches() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "s-1");

    assert_eq!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream("s-1"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: vec![0; 99],
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            attrs: None,
            now_ms: 0,
        }),
        StreamResponse::AlreadyExists {
            next_offset: 0,
            closed: false,
            content_type: "application/octet-stream".to_owned(),
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
        }
    );

    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream("s-1"),
            content_type: "text/plain".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            attrs: None,
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamAlreadyExistsConflict,
            ..
        }
    ));
}

#[test]
fn create_stream_is_idempotent_only_when_attrs_match() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    let first = attrs("Support session", "customer-support");
    let second = attrs("Escalated session", "incident-review");

    assert_eq!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream("attrs-idempotent"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            attrs: Some(first.clone()),
            now_ms: 0,
        }),
        StreamResponse::Created {
            stream_id: stream("attrs-idempotent"),
            next_offset: 0,
            closed: false,
        }
    );

    assert_eq!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream("attrs-idempotent"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            attrs: Some(first),
            now_ms: 0,
        }),
        StreamResponse::AlreadyExists {
            next_offset: 0,
            closed: false,
            content_type: "application/octet-stream".to_owned(),
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
        }
    );

    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream("attrs-idempotent"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            attrs: Some(second),
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamAlreadyExistsConflict,
            ..
        }
    ));
}

#[test]
fn append_advances_offsets_and_checks_content_type() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "s-1");

    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("s-1"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abcdefg".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 7,
            closed: false,
            deduplicated: false,
            producer: None,
        }
    );
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("s-1"),
            content_type: Some("text/plain".to_owned()),
            payload: b"x".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::ContentTypeMismatch,
            next_offset: Some(7),
            ..
        }
    ));
    assert_eq!(machine.head(&stream("s-1")).expect("stream").tail_offset, 7);
    let integrity = machine
        .integrity_snapshot(&stream("s-1"))
        .expect("integrity");
    assert_eq!(integrity.live_start_offset, 0);
    assert_eq!(integrity.tail_offset, 7);
    assert_eq!(integrity.live_records, 1);
    assert_eq!(integrity.evicted_records, 0);
    assert_eq!(integrity.total_records, 1);
    assert_eq!(integrity.live_setsum, integrity.total_setsum);
}

#[test]
fn catch_up_read_returns_payload_slice_and_bounds_errors() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "s-1");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("s-1"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abcdefg".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended { .. }
    ));

    assert_eq!(
        machine.read(&stream("s-1"), 2, 3).expect("read"),
        StreamRead {
            offset: 2,
            next_offset: 5,
            content_type: "application/octet-stream".to_owned(),
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
            content_type: "application/octet-stream".to_owned(),
            payload: Vec::new(),
            up_to_date: true,
            closed: false,
        }
    );
    assert!(matches!(
        machine.read(&stream("s-1"), 8, 1),
        Err(StreamResponse::Error {
            code: StreamErrorCode::OffsetOutOfRange,
            next_offset: Some(7),
            ..
        })
    ));
}

#[test]
fn flush_cold_moves_hot_prefix_to_manifest_and_read_plan_splits() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "cold");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("cold"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abcdef".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
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
        machine.apply(StreamCommand::FlushCold {
            stream_id: stream("cold"),
            chunk: ColdChunkRef {
                start_offset: candidate.start_offset,
                end_offset: candidate.end_offset,
                s3_path: "s3://bucket/cold/000000".to_owned(),
                object_size: u64::try_from(candidate.payload.len()).unwrap(),
            },
        }),
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
            content_type: "application/octet-stream".to_owned(),
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
            content_type: "application/octet-stream".to_owned(),
            payload: b"ef".to_vec(),
            up_to_date: true,
            closed: false,
        }
    );
}

#[test]
fn flush_cold_compacts_message_records_to_cold_prefix() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "cold-records");
    for payload in [b"ab".as_slice(), b"cd".as_slice(), b"ef".as_slice()] {
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("cold-records"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: payload.to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
                record_match: None,
            }),
            StreamResponse::Appended { .. }
        ));
    }

    assert_eq!(
        machine.apply(StreamCommand::FlushCold {
            stream_id: stream("cold-records"),
            chunk: ColdChunkRef {
                start_offset: 0,
                end_offset: 4,
                s3_path: "s3://bucket/cold-records/000000".to_owned(),
                object_size: 4,
            },
        }),
        StreamResponse::ColdFlushed {
            hot_start_offset: 4,
        }
    );
    assert_eq!(
        machine
            .bootstrap_plan(&stream("cold-records"))
            .expect("bootstrap")
            .updates,
        vec![
            StreamMessageRecord {
                start_offset: 0,
                end_offset: 4,
            },
            StreamMessageRecord {
                start_offset: 4,
                end_offset: 6,
            },
        ]
    );

    assert_eq!(
        machine.apply(StreamCommand::FlushCold {
            stream_id: stream("cold-records"),
            chunk: ColdChunkRef {
                start_offset: 4,
                end_offset: 6,
                s3_path: "s3://bucket/cold-records/000001".to_owned(),
                object_size: 2,
            },
        }),
        StreamResponse::ColdFlushed {
            hot_start_offset: 6,
        }
    );
    assert_eq!(
        machine
            .bootstrap_plan(&stream("cold-records"))
            .expect("bootstrap")
            .updates,
        vec![StreamMessageRecord {
            start_offset: 0,
            end_offset: 6,
        }]
    );
    let snapshot = machine.snapshot();
    let entry = snapshot
        .streams
        .iter()
        .find(|entry| entry.metadata.stream_id == stream("cold-records"))
        .expect("stream snapshot");
    assert_eq!(entry.message_records, vec![StreamMessageRecord {
        start_offset: 0,
        end_offset: 6,
    }]);
    assert_eq!(
        machine.apply(StreamCommand::PublishSnapshot {
            stream_id: stream("cold-records"),
            snapshot_offset: 3,
            content_type: "application/octet-stream".to_owned(),
            payload: b"abc-state".to_vec(),
            now_ms: 0,
        }),
        StreamResponse::SnapshotPublished {
            snapshot_offset: 3,
            record_range: None,
        }
    );
}

fn flush_one_cold_chunk(machine: &mut StreamStateMachine, id: &str) {
    machine.apply(StreamCommand::Append {
        stream_id: stream(id),
        content_type: Some("application/octet-stream".to_owned()),
        payload: b"abcd".to_vec(),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 0,
        record_match: None,
    });
    let candidate = machine
        .plan_cold_flush(&stream(id), 4, 4)
        .expect("plan cold flush")
        .expect("cold flush candidate");
    machine.apply(StreamCommand::FlushCold {
        stream_id: stream(id),
        chunk: ColdChunkRef {
            start_offset: candidate.start_offset,
            end_offset: candidate.end_offset,
            s3_path: format!("s3://bucket/{id}/000000"),
            object_size: u64::try_from(candidate.payload.len()).unwrap(),
        },
    });
}

#[test]
fn delete_stream_enqueues_cold_gc_then_ack_drains_it() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "cold-a");
    create_stream(&mut machine, "cold-b");
    flush_one_cold_chunk(&mut machine, "cold-a");
    flush_one_cold_chunk(&mut machine, "cold-b");

    for id in ["cold-a", "cold-b"] {
        assert!(matches!(
            machine.apply(StreamCommand::DeleteStream {
                stream_id: stream(id)
            }),
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
fn expired_stream_with_cold_chunks_enqueues_cold_gc() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    machine.apply(StreamCommand::CreateStream {
        stream_id: stream("ttl"),
        content_type: "application/octet-stream".to_owned(),
        initial_payload: Vec::new(),
        close_after: false,
        stream_seq: None,
        producer: None,
        stream_ttl_seconds: None,
        stream_expires_at_ms: Some(1_000),
        attrs: None,
        now_ms: 0,
    });
    flush_one_cold_chunk(&mut machine, "ttl");

    // A lazy access past the expiry removes the stream and queues its cold prefix.
    assert!(machine.head_at(&stream("ttl"), 2_000).is_none());
    let pending = machine.pending_cold_gc_batch(16);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].target, ColdGcTarget::Stream(stream("ttl")));
}

#[test]
fn writes_sweep_expired_streams_in_bounded_deterministic_batches() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "active");

    let expired_count = TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE + 6;
    for index in 0..expired_count {
        let id = format!("old-{index:04}");
        assert!(matches!(
            machine.apply(StreamCommand::CreateStream {
                stream_id: stream(&id),
                content_type: "application/octet-stream".to_owned(),
                initial_payload: Vec::new(),
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: Some(1_000),
                attrs: None,
                now_ms: 0,
            }),
            StreamResponse::Created { .. }
        ));
    }
    assert_eq!(machine.snapshot().streams.len(), expired_count + 1);

    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("active"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"x".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 2_000,
            record_match: None,
        }),
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
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "hot-only");
    machine.apply(StreamCommand::DeleteStream {
        stream_id: stream("hot-only"),
    });
    assert_eq!(machine.pending_cold_gc_len(), 0);
}

#[test]
fn flush_cold_can_coalesce_contiguous_hot_segments() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "cold-coalesced");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("cold-coalesced"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abc".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 3,
            ..
        }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("cold-coalesced"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"de".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 3,
            next_offset: 5,
            ..
        }
    ));

    assert_eq!(
        machine.apply(StreamCommand::FlushCold {
            stream_id: stream("cold-coalesced"),
            chunk: ColdChunkRef {
                start_offset: 0,
                end_offset: 5,
                s3_path: "s3://bucket/cold-coalesced/000000".to_owned(),
                object_size: 5,
            },
        }),
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
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "cold-planned-coalesced");
    for payload in [b"ab".as_slice(), b"cd".as_slice(), b"ef".as_slice()] {
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("cold-planned-coalesced"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: payload.to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
                record_match: None,
            }),
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
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "z-cold");
    create_stream(&mut machine, "a-cold");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("z-cold"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"zzzz".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended { .. }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("a-cold"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"aaaa".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended { .. }
    ));

    let candidate = machine
        .plan_next_cold_flush_batch(4, 4, 1)
        .expect("plan next cold flush")
        .into_iter()
        .next()
        .expect("candidate");
    assert_eq!(candidate.stream_id, stream("a-cold"));
    assert_eq!(candidate.payload, b"aaaa");
}

#[test]
fn plan_next_cold_flush_drains_distributed_group_hot_bytes() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "z-cold");
    create_stream(&mut machine, "a-cold");
    for stream_name in ["z-cold", "a-cold"] {
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream(stream_name),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"aa".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
                record_match: None,
            }),
            StreamResponse::Appended { .. }
        ));
    }

    assert_eq!(
        machine
            .plan_next_cold_flush_batch(4, 4, 1)
            .expect("plan next cold flush")
            .len(),
        1
    );
    let candidates = machine
        .plan_next_cold_flush_batch(5, 4, 1)
        .expect("plan next cold flush");
    assert!(candidates.is_empty());
    let candidate = machine
        .plan_next_cold_flush_batch(4, 4, 1)
        .expect("plan next cold flush")
        .into_iter()
        .next()
        .expect("candidate");
    assert_eq!(candidate.stream_id, stream("a-cold"));
    assert_eq!(candidate.payload, b"aa");
}

#[test]
fn plan_next_cold_flush_batch_advances() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "batched-cold");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("batched-cold"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abcd".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended { .. }
    ));

    let candidates = machine
        .plan_next_cold_flush_batch(1, 1, 4)
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
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "stale-cold");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("stale-cold"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abcdefghijklmnopqr".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
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
        machine.apply(StreamCommand::DeleteStream {
            stream_id: stream("stale-cold")
        }),
        StreamResponse::Deleted
    ));
    create_stream(&mut machine, "stale-cold");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("stale-cold"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abcdefghijklmnopq".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            next_offset: 17,
            ..
        }
    ));

    match machine.apply(StreamCommand::FlushCold {
        stream_id: stream("stale-cold"),
        chunk: ColdChunkRef {
            start_offset: candidate.start_offset,
            end_offset: candidate.end_offset,
            s3_path: "s3://bucket/stale-cold/old-candidate".to_owned(),
            object_size: u64::try_from(candidate.payload.len()).unwrap(),
        },
    }) {
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
            content_type: "application/octet-stream".to_owned(),
            payload: b"abcdefghijklmnopq".to_vec(),
            up_to_date: true,
            closed: false,
        }
    );
}

#[test]
fn plan_next_cold_flush_skips_deleted_streams() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "a-gone");
    create_stream(&mut machine, "b-live");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("a-gone"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"gone".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended { .. }
    ));
    assert_eq!(
        machine.apply(StreamCommand::DeleteStream {
            stream_id: stream("a-gone"),
        }),
        StreamResponse::Deleted
    );
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("b-live"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"live".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended { .. }
    ));

    let candidate = machine
        .plan_next_cold_flush_batch(4, 4, 1)
        .expect("plan next cold flush")
        .into_iter()
        .next()
        .expect("candidate");
    assert_eq!(candidate.stream_id, stream("b-live"));
    assert_eq!(candidate.payload, b"live");
}

#[test]
fn hot_payload_byte_metrics_follow_cold_flush() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "hot-a");
    create_stream(&mut machine, "hot-b");
    for (stream_name, payload) in [("hot-a", b"abcd".as_slice()), ("hot-b", b"xy".as_slice())] {
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream(stream_name),
                content_type: Some("application/octet-stream".to_owned()),
                payload: payload.to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
                record_match: None,
            }),
            StreamResponse::Appended { .. }
        ));
    }

    assert_eq!(machine.hot_payload_len(&stream("hot-a")), Ok(4));
    assert_eq!(machine.hot_payload_len(&stream("hot-b")), Ok(2));
    assert_eq!(machine.total_hot_payload_bytes(), 6);

    assert_eq!(
        machine.apply(StreamCommand::FlushCold {
            stream_id: stream("hot-a"),
            chunk: ColdChunkRef {
                start_offset: 0,
                end_offset: 3,
                s3_path: "s3://bucket/hot-a/000000".to_owned(),
                object_size: 3,
            },
        }),
        StreamResponse::ColdFlushed {
            hot_start_offset: 3,
        }
    );
    assert_eq!(machine.hot_payload_len(&stream("hot-a")), Ok(1));
    assert_eq!(machine.total_hot_payload_bytes(), 3);
}

#[test]
fn hot_start_offset_advances_to_tail_after_full_cold_flush() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "hot-start");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("hot-start"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abcd".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended { .. }
    ));

    assert_eq!(
        machine.apply(StreamCommand::FlushCold {
            stream_id: stream("hot-start"),
            chunk: ColdChunkRef {
                start_offset: 0,
                end_offset: 4,
                s3_path: "s3://bucket/hot-start/000000".to_owned(),
                object_size: 4,
            },
        }),
        StreamResponse::ColdFlushed {
            hot_start_offset: 4,
        }
    );
    assert_eq!(machine.hot_start_offset(&stream("hot-start")), 4);
    assert_eq!(machine.hot_payload_len(&stream("hot-start")), Ok(0));
}

#[test]
fn snapshot_restore_round_trips_payload_metadata_and_stream_seq() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    let attrs = attrs("Snapshot session", "snapshot-restore");
    assert_eq!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream("snap-open"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: b"hi".to_vec(),
            close_after: false,
            stream_seq: Some("0001".to_owned()),
            producer: None,
            stream_ttl_seconds: Some(60),
            stream_expires_at_ms: None,
            attrs: Some(attrs.clone()),
            now_ms: 0,
        }),
        StreamResponse::Created {
            stream_id: stream("snap-open"),
            next_offset: 2,
            closed: false,
        }
    );
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("snap-open"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abc".to_vec(),
            close_after: false,
            stream_seq: Some("0002".to_owned()),
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 2,
            next_offset: 5,
            ..
        }
    ));
    assert_eq!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream("snap-closed"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: b"x".to_vec(),
            close_after: true,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            attrs: None,
            now_ms: 0,
        }),
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
            content_type: "application/octet-stream".to_owned(),
            payload: b"hiabc".to_vec(),
            up_to_date: true,
            closed: false,
        }
    );
    let metadata = restored.head(&stream("snap-open")).expect("metadata");
    assert_eq!(metadata.last_stream_seq.as_deref(), Some("0002"));
    assert_eq!(metadata.stream_ttl_seconds, Some(60));
    assert_eq!(metadata.stream_expires_at_ms, None);
    assert_eq!(restored.stream_attrs(&stream("snap-open")), Some(&attrs));

    assert!(matches!(
        restored.apply(StreamCommand::Append {
            stream_id: stream("snap-open"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"bad".to_vec(),
            close_after: false,
            stream_seq: Some("0002".to_owned()),
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamSeqConflict,
            next_offset: Some(5),
            ..
        }
    ));
    assert_eq!(
        restored.apply(StreamCommand::Append {
            stream_id: stream("snap-open"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"!".to_vec(),
            close_after: false,
            stream_seq: Some("0003".to_owned()),
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 5,
            next_offset: 6,
            closed: false,
            deduplicated: false,
            producer: None,
        }
    );
    assert!(matches!(
        restored.apply(StreamCommand::Append {
            stream_id: stream("snap-closed"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"!".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamClosed,
            next_offset: Some(1),
            ..
        }
    ));
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
            machine.apply(StreamCommand::CreateStream {
                stream_id,
                content_type: "application/octet-stream".to_owned(),
                initial_payload: Vec::new(),
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
                attrs: None,
                now_ms: 0,
            }),
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

#[test]
fn snapshot_restore_rejects_invalid_entries() {
    assert_eq!(
        StreamStateMachine::restore(StreamSnapshot {
            pending_cold_gc: Vec::new(),
            next_cold_gc_seq: 0,
            buckets: vec!["benchcmp".to_owned(), "benchcmp".to_owned()],
            streams: Vec::new(),
        })
        .expect_err("duplicate bucket"),
        StreamSnapshotError::DuplicateBucket("benchcmp".to_owned())
    );

    assert!(matches!(
        StreamStateMachine::restore(StreamSnapshot {
            pending_cold_gc: Vec::new(),
            next_cold_gc_seq: 0,
            buckets: vec!["benchcmp".to_owned()],
            streams: vec![StreamSnapshotEntry {
                metadata: StreamMetadata {
                    stream_id: BucketStreamId::new("missing", "stream"),
                    content_type: "application/octet-stream".to_owned(),
                    status: StreamStatus::Open,
                    tail_offset: 0,
                    last_stream_seq: None,
                    stream_ttl_seconds: None,
                    stream_expires_at_ms: None,
                    created_at_ms: 0,
                    last_ttl_touch_at_ms: 0,
                },
                attrs: None,
                hot_start_offset: 0,
                payload: Vec::new(),
                hot_segments: Vec::new(),
                cold_frontier_offset: 0,
                cold_index_generation: 0,
                cold_chunks: Vec::new(),
                external_segments: Vec::new(),
                message_records: Vec::new(),
                record_index: None,
                integrity: empty_integrity(),
                visible_snapshot: None,
                producer_states: Vec::new(),
            }],
        }),
        Err(StreamSnapshotError::MissingBucket(_))
    ));

    assert!(matches!(
        StreamStateMachine::restore(StreamSnapshot {
            pending_cold_gc: Vec::new(),
            next_cold_gc_seq: 0,
            buckets: vec!["benchcmp".to_owned()],
            streams: vec![StreamSnapshotEntry {
                metadata: StreamMetadata {
                    stream_id: stream("bad-len"),
                    content_type: "application/octet-stream".to_owned(),
                    status: StreamStatus::Open,
                    tail_offset: 2,
                    last_stream_seq: None,
                    stream_ttl_seconds: None,
                    stream_expires_at_ms: None,
                    created_at_ms: 0,
                    last_ttl_touch_at_ms: 0,
                },
                attrs: None,
                hot_start_offset: 0,
                payload: b"x".to_vec(),
                hot_segments: Vec::new(),
                cold_frontier_offset: 0,
                cold_index_generation: 0,
                cold_chunks: Vec::new(),
                external_segments: Vec::new(),
                message_records: Vec::new(),
                record_index: None,
                integrity: empty_integrity(),
                visible_snapshot: None,
                producer_states: Vec::new(),
            }],
        }),
        Err(StreamSnapshotError::PayloadLengthMismatch { .. })
    ));

    assert!(matches!(
        StreamStateMachine::restore(StreamSnapshot {
            pending_cold_gc: Vec::new(),
            next_cold_gc_seq: 0,
            buckets: vec!["benchcmp".to_owned()],
            streams: vec![StreamSnapshotEntry {
                metadata: StreamMetadata {
                    stream_id: stream("duplicate-producer"),
                    content_type: "application/octet-stream".to_owned(),
                    status: StreamStatus::Open,
                    tail_offset: 0,
                    last_stream_seq: None,
                    stream_ttl_seconds: None,
                    stream_expires_at_ms: None,
                    created_at_ms: 0,
                    last_ttl_touch_at_ms: 0,
                },
                attrs: None,
                hot_start_offset: 0,
                payload: Vec::new(),
                hot_segments: Vec::new(),
                cold_frontier_offset: 0,
                cold_index_generation: 0,
                cold_chunks: Vec::new(),
                external_segments: Vec::new(),
                message_records: Vec::new(),
                record_index: None,
                integrity: empty_integrity(),
                visible_snapshot: None,
                producer_states: vec![
                    ProducerSnapshot {
                        producer_id: "writer-1".to_owned(),
                        producer_epoch: 0,
                        producer_seq: 0,
                        last_start_offset: 0,
                        last_next_offset: 0,
                        last_closed: false,
                        last_items: Vec::new(),
                    },
                    ProducerSnapshot {
                        producer_id: "writer-1".to_owned(),
                        producer_epoch: 1,
                        producer_seq: 0,
                        last_start_offset: 0,
                        last_next_offset: 0,
                        last_closed: false,
                        last_items: Vec::new(),
                    },
                ],
            }],
        }),
        Err(StreamSnapshotError::DuplicateProducer { .. })
    ));
}

#[test]
fn close_is_monotonic_and_close_only_is_idempotent() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "s-1");

    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("s-1"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abc".to_vec(),
            close_after: true,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 3,
            closed: true,
            deduplicated: false,
            producer: None,
        }
    );
    assert_eq!(
        machine.apply(StreamCommand::Close {
            stream_id: stream("s-1"),
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Closed {
            next_offset: 3,
            deduplicated: false,
            producer: None,
        }
    );
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("s-1"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"x".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamClosed,
            next_offset: Some(3),
            ..
        }
    ));
}

#[test]
fn stream_seq_must_strictly_increase() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "s-1");

    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("s-1"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"a".to_vec(),
            close_after: false,
            stream_seq: Some("0002".to_owned()),
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended { .. }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("s-1"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"b".to_vec(),
            close_after: false,
            stream_seq: Some("0002".to_owned()),
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamSeqConflict,
            next_offset: Some(1),
            ..
        }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("s-1"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"c".to_vec(),
            close_after: false,
            stream_seq: Some("0003".to_owned()),
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 1,
            next_offset: 2,
            ..
        }
    ));
}

#[test]
fn producer_headers_deduplicate_retries_and_fence_stale_epochs() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "producer-stream");

    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("producer-stream"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"a".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 0)),
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 1,
            closed: false,
            deduplicated: false,
            producer: Some(producer("writer-1", 0, 0)),
        }
    );
    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("producer-stream"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"ignored-retry-body".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 0)),
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 1,
            closed: false,
            deduplicated: true,
            producer: Some(producer("writer-1", 0, 0)),
        }
    );
    assert_eq!(
        machine
            .read(&stream("producer-stream"), 0, 16)
            .expect("read")
            .payload,
        b"a"
    );

    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("producer-stream"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"gap".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 2)),
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::ProducerSeqConflict,
            ..
        }
    ));

    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("producer-stream"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"b".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: Some(producer("writer-1", 1, 0)),
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 1,
            next_offset: 2,
            closed: false,
            deduplicated: false,
            producer: Some(producer("writer-1", 1, 0)),
        }
    );
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("producer-stream"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"stale".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 1)),
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::ProducerEpochStale,
            ..
        }
    ));
}

#[test]
fn producer_append_batch_deduplicates_retries_without_partial_mutation() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "producer-batch");

    let first_payloads = [b"ab".as_slice(), b"c".as_slice()];
    let first = machine
        .append_batch_borrowed(
            stream("producer-batch"),
            Some("application/octet-stream"),
            &first_payloads,
            Some(producer("writer-1", 0, 0)),
            0,
        )
        .expect("first batch");
    assert_eq!(first.items, vec![
        StreamBatchAppendItem {
            offset: 0,
            next_offset: 2,
            closed: false,
            deduplicated: false,
        },
        StreamBatchAppendItem {
            offset: 2,
            next_offset: 3,
            closed: false,
            deduplicated: false,
        },
    ]);
    assert!(!first.deduplicated);

    let duplicate = machine
        .append_batch_borrowed(
            stream("producer-batch"),
            Some("application/octet-stream"),
            &first_payloads,
            Some(producer("writer-1", 0, 0)),
            0,
        )
        .expect("duplicate batch");
    assert!(duplicate.deduplicated);
    assert!(duplicate.items.iter().all(|item| item.deduplicated));
    assert_eq!(duplicate.items[0].offset, 0);
    assert_eq!(duplicate.items[1].next_offset, 3);
    assert_eq!(
        machine
            .read(&stream("producer-batch"), 0, 16)
            .expect("read")
            .payload,
        b"abc"
    );

    let invalid_payloads = [b"".as_slice()];
    assert!(matches!(
        machine.append_batch_borrowed(
            stream("producer-batch"),
            Some("application/octet-stream"),
            &invalid_payloads,
            Some(producer("writer-1", 0, 1)),
            0,
        ),
        Err(StreamResponse::Error {
            code: StreamErrorCode::EmptyAppend,
            ..
        })
    ));

    let next_payloads = [b"d".as_slice()];
    let next = machine
        .append_batch_borrowed(
            stream("producer-batch"),
            Some("application/octet-stream"),
            &next_payloads,
            Some(producer("writer-1", 0, 1)),
            0,
        )
        .expect("next batch");
    assert_eq!(next.items[0].offset, 3);
    assert_eq!(
        machine
            .read(&stream("producer-batch"), 0, 16)
            .expect("read")
            .payload,
        b"abcd"
    );
}

#[test]
fn producer_state_survives_snapshot_restore() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "producer-snapshot");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("producer-snapshot"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"a".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 0)),
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            deduplicated: false,
            ..
        }
    ));

    let snapshot = machine.snapshot();
    assert_eq!(snapshot.streams[0].producer_states.len(), 1);
    assert_eq!(snapshot.streams[0].producer_states[0].last_items, vec![
        ProducerAppendRecord {
            start_offset: 0,
            next_offset: 1,
            closed: false,
            record_start: None,
            record_next: None,
        }
    ]);
    let mut restored = StreamStateMachine::restore(snapshot).expect("restore snapshot");

    assert!(matches!(
        restored.apply(StreamCommand::Append {
            stream_id: stream("producer-snapshot"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"retry".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 0)),
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 1,
            deduplicated: true,
            ..
        }
    ));
    assert_eq!(
        restored.apply(StreamCommand::Append {
            stream_id: stream("producer-snapshot"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"b".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 1)),
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 1,
            next_offset: 2,
            closed: false,
            deduplicated: false,
            producer: Some(producer("writer-1", 0, 1)),
        }
    );
}

#[test]
fn stream_ttl_uses_sliding_access_window() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    let stream_id = stream("ttl-window");

    assert_eq!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: b"hi".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: Some(1),
            stream_expires_at_ms: None,
            attrs: None,
            now_ms: 1_000,
        }),
        StreamResponse::Created {
            stream_id: stream_id.clone(),
            next_offset: 2,
            closed: false,
        }
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
        machine.access_requires_write(&stream_id, 1_500, true),
        Ok(true)
    );
    assert_eq!(
        machine.apply(StreamCommand::TouchStreamAccess {
            stream_id: stream_id.clone(),
            now_ms: 1_500,
            renew_ttl: true,
        }),
        StreamResponse::Accessed {
            changed: true,
            expired: false,
        }
    );

    assert!(machine.read_plan_at(&stream_id, 2, 16, 2_400).is_ok());
    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream_id.clone(),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"!".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 2_400,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 2,
            next_offset: 3,
            closed: false,
            deduplicated: false,
            producer: None,
        }
    );
    assert!(machine.head_at(&stream_id, 3_399).is_some());
    assert!(machine.head_at(&stream_id, 3_400).is_none());
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream_id.clone(),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"late".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 3_401,
            record_match: None,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamNotFound,
            ..
        }
    ));
}

#[test]
fn renewed_ttl_ignores_stale_expiry_index_entry() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    let stream_id = stream("ttl-renew-stale");

    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: b"hi".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: Some(1),
            stream_expires_at_ms: None,
            attrs: None,
            now_ms: 1_000,
        }),
        StreamResponse::Created { .. }
    ));
    assert_eq!(
        machine.apply(StreamCommand::TouchStreamAccess {
            stream_id: stream_id.clone(),
            now_ms: 1_500,
            renew_ttl: true,
        }),
        StreamResponse::Accessed {
            changed: true,
            expired: false,
        }
    );
    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream("sweep-trigger"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            attrs: None,
            now_ms: 2_100,
        }),
        StreamResponse::Created { .. }
    ));

    assert!(machine.head(&stream_id).is_some());
    assert!(machine.head_at(&stream_id, 2_500).is_none());
}

#[test]
fn restore_rebuilds_ttl_index_from_stream_metadata() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    let stream_id = stream("ttl-restored");

    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: Some(2_000),
            attrs: None,
            now_ms: 1_000,
        }),
        StreamResponse::Created { .. }
    ));

    let mut restored = StreamStateMachine::restore(machine.snapshot()).expect("restore");
    assert!(matches!(
        restored.apply(StreamCommand::CreateStream {
            stream_id: stream("restore-sweep-trigger"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            attrs: None,
            now_ms: 2_100,
        }),
        StreamResponse::Created { .. }
    ));
    assert!(restored.head(&stream_id).is_none());
}

#[test]
fn stream_expires_at_is_absolute_and_recreate_after_expiry() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    let stream_id = stream("absolute-expiry");

    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: Some(2_000),
            attrs: None,
            now_ms: 1_000,
        }),
        StreamResponse::Created { .. }
    ));
    assert_eq!(
        machine.apply(StreamCommand::TouchStreamAccess {
            stream_id: stream_id.clone(),
            now_ms: 1_500,
            renew_ttl: true,
        }),
        StreamResponse::Accessed {
            changed: false,
            expired: false,
        }
    );
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream_id.clone(),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"body".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 1_600,
            record_match: None,
        }),
        StreamResponse::Appended { .. }
    ));
    assert!(machine.read_plan_at(&stream_id, 0, 16, 1_999).is_ok());
    assert!(matches!(
        machine.read_plan_at(&stream_id, 0, 16, 2_000),
        Err(StreamResponse::Error {
            code: StreamErrorCode::StreamNotFound,
            ..
        })
    ));
    assert_eq!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: "text/plain".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            attrs: None,
            now_ms: 2_001,
        }),
        StreamResponse::Created {
            stream_id,
            next_offset: 0,
            closed: false,
        }
    );
}

#[test]
fn producer_duplicate_final_append_remains_idempotent_after_close() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "producer-close");

    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("producer-close"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"final".to_vec(),
            close_after: true,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 0)),
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 5,
            closed: true,
            deduplicated: false,
            producer: Some(producer("writer-1", 0, 0)),
        }
    );
    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("producer-close"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"final".to_vec(),
            close_after: true,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 0)),
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 5,
            closed: true,
            deduplicated: true,
            producer: Some(producer("writer-1", 0, 0)),
        }
    );
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("producer-close"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"too-late".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 1)),
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamClosed,
            next_offset: Some(5),
            ..
        }
    ));
}

#[test]
fn append_conflict_precedence_reports_closed_before_mismatch_or_seq() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "closed-precedence");

    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("closed-precedence"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"final".to_vec(),
            close_after: true,
            stream_seq: Some("0002".to_owned()),
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 5,
            closed: true,
            deduplicated: false,
            producer: None,
        }
    );

    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("closed-precedence"),
            content_type: Some("text/plain".to_owned()),
            payload: b"too-late".to_vec(),
            close_after: false,
            stream_seq: Some("0001".to_owned()),
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamClosed,
            next_offset: Some(5),
            ..
        }
    ));
}

#[test]
fn bucket_delete_requires_empty_bucket() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "s-1");

    assert!(matches!(
        machine.apply(StreamCommand::DeleteBucket {
            bucket_id: "benchcmp".to_owned(),
        }),
        StreamResponse::Error {
            code: StreamErrorCode::BucketNotEmpty,
            ..
        }
    ));
    assert_eq!(
        machine.apply(StreamCommand::DeleteStream {
            stream_id: stream("s-1"),
        }),
        StreamResponse::Deleted
    );
    assert_eq!(
        machine.apply(StreamCommand::DeleteBucket {
            bucket_id: "benchcmp".to_owned(),
        }),
        StreamResponse::BucketDeleted {
            bucket_id: "benchcmp".to_owned(),
        }
    );
}

#[test]
fn publish_snapshot_advances_retention_on_message_boundary() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "snap");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("snap"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abc".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 3,
            ..
        }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("snap"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"de".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended {
            offset: 3,
            next_offset: 5,
            ..
        }
    ));

    assert_eq!(
        machine.apply(StreamCommand::PublishSnapshot {
            stream_id: stream("snap"),
            snapshot_offset: 3,
            content_type: "application/json".to_owned(),
            payload: br#"{"state":"abc"}"#.to_vec(),
            now_ms: 0,
        }),
        StreamResponse::SnapshotPublished {
            snapshot_offset: 3,
            record_range: None,
        }
    );
    assert!(matches!(
        machine.read_plan(&stream("snap"), 0, 1),
        Err(StreamResponse::Error {
            code: StreamErrorCode::StreamGone,
            next_offset: Some(3),
            ..
        })
    ));
    let read = machine.read(&stream("snap"), 3, 2).expect("retained read");
    assert_eq!(read.payload, b"de");
    let integrity = machine
        .integrity_snapshot(&stream("snap"))
        .expect("integrity");
    assert_eq!(integrity.live_start_offset, 3);
    assert_eq!(integrity.tail_offset, 5);
    assert_eq!(integrity.live_records, 2);
    assert_eq!(integrity.evicted_records, 0);
    assert_eq!(integrity.total_records, 2);
    assert_eq!(integrity.live_setsum, integrity.total_setsum);
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
    assert_eq!(
        restored
            .integrity_snapshot(&stream("snap"))
            .expect("restored integrity"),
        integrity
    );
}

#[test]
fn publish_snapshot_rejects_unaligned_offset() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "unaligned");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("unaligned"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abc".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        }),
        StreamResponse::Appended { .. }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::PublishSnapshot {
            stream_id: stream("unaligned"),
            snapshot_offset: 2,
            content_type: "application/octet-stream".to_owned(),
            payload: b"ab".to_vec(),
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::InvalidSnapshot,
            next_offset: Some(3),
            ..
        }
    ));
}

#[test]
fn snapshot_restore_preserves_visible_snapshot_and_message_records() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "restore-snap");
    let _ = machine.apply(StreamCommand::Append {
        stream_id: stream("restore-snap"),
        content_type: Some("application/octet-stream".to_owned()),
        payload: b"abc".to_vec(),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 0,
        record_match: None,
    });
    let _ = machine.apply(StreamCommand::Append {
        stream_id: stream("restore-snap"),
        content_type: Some("application/octet-stream".to_owned()),
        payload: b"de".to_vec(),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 0,
        record_match: None,
    });
    let _ = machine.apply(StreamCommand::PublishSnapshot {
        stream_id: stream("restore-snap"),
        snapshot_offset: 3,
        content_type: "application/octet-stream".to_owned(),
        payload: b"abc-state".to_vec(),
        now_ms: 0,
    });

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
    machine.apply(StreamCommand::Append {
        stream_id: stream(stream_name),
        content_type: Some("application/octet-stream".to_owned()),
        payload,
        close_after,
        stream_seq: None,
        producer,
        now_ms: 0,
        record_match: None,
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn prop_appends_are_offset_monotonic_and_snapshot_round_trips(
        initial in vec(any::<u8>(), 0..=16),
        payloads in payloads_strategy(),
    ) {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        let stream_id = stream("prop-offsets");
        prop_assert_eq!(
            machine.apply(StreamCommand::CreateStream {
                stream_id: stream_id.clone(),
                content_type: "application/octet-stream".to_owned(),
                initial_payload: initial.clone(),
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
                attrs: None,
                now_ms: 0,
            }),
            StreamResponse::Created {
                stream_id: stream_id.clone(),
                next_offset: u64::try_from(initial.len()).unwrap(),
                closed: false,
            }
        );

        let mut expected = initial;
        let mut expected_tail = u64::try_from(expected.len()).unwrap();
        for payload in payloads {
            let payload_len = u64::try_from(payload.len()).unwrap();
            prop_assert_eq!(
                append_payload(&mut machine, "prop-offsets", payload.clone(), false, None),
                StreamResponse::Appended {
                    offset: expected_tail,
                    next_offset: expected_tail + payload_len,
                    closed: false,
                    deduplicated: false,
                    producer: None,
                }
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
            restored
                .integrity_snapshot(&stream_id)
                .expect("restored integrity")
                .tail_offset,
            expected_tail
        );
    }

    #[test]
    fn prop_producer_retries_are_idempotent_and_stale_epochs_are_fenced(
        first_payload in payload_strategy(),
        retry_payload in payload_strategy(),
        next_payload in payload_strategy(),
    ) {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
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
            StreamResponse::Appended {
                offset: 0,
                next_offset: first_len,
                closed: false,
                deduplicated: false,
                producer: Some(producer("writer-1", 0, 0)),
            }
        );

        prop_assert_eq!(
            append_payload(
                &mut machine,
                "prop-producer",
                retry_payload,
                false,
                Some(producer("writer-1", 0, 0)),
            ),
            StreamResponse::Appended {
                offset: 0,
                next_offset: first_len,
                closed: false,
                deduplicated: true,
                producer: Some(producer("writer-1", 0, 0)),
            }
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
            StreamResponse::Appended {
                offset: first_len,
                next_offset: first_len + next_len,
                closed: false,
                deduplicated: false,
                producer: Some(producer("writer-1", 1, 0)),
            }
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
    fn prop_producer_batch_state_survives_snapshot_restore(
        first_payloads in vec(payload_strategy(), 1..=8),
        retry_payloads in vec(payload_strategy(), 1..=8),
        next_payloads in vec(payload_strategy(), 1..=8),
        duplicate_epoch_payloads in vec(payload_strategy(), 1..=8),
    ) {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "prop-producer-batch");
        let stream_id = stream("prop-producer-batch");

        let first_refs = first_payloads
            .iter()
            .map(Vec::as_slice)
            .collect::<Vec<_>>();
        let first = machine
            .append_batch_borrowed(
                stream_id.clone(),
                Some("application/octet-stream"),
                &first_refs,
                Some(producer("writer-1", 0, 0)),
                0,
            )
            .expect("first producer batch");
        prop_assert!(!first.deduplicated);
        prop_assert_eq!(first.items.len(), first_payloads.len());

        let mut expected = Vec::new();
        let mut expected_items = Vec::with_capacity(first_payloads.len());
        for (item, payload) in first.items.iter().zip(first_payloads.iter()) {
            let start_offset = u64::try_from(expected.len()).expect("payload len fits u64");
            expected.extend_from_slice(payload);
            let next_offset = u64::try_from(expected.len()).expect("payload len fits u64");
            prop_assert_eq!(item.offset, start_offset);
            prop_assert_eq!(item.next_offset, next_offset);
            prop_assert!(!item.closed);
            prop_assert!(!item.deduplicated);
            expected_items.push(StreamBatchAppendItem {
                offset: start_offset,
                next_offset,
                closed: false,
                deduplicated: true,
            });
        }

        let snapshot = machine.snapshot();
        let producer_state = snapshot.streams[0]
            .producer_states
            .iter()
            .find(|state| state.producer_id == "writer-1")
            .expect("producer state in snapshot");
        prop_assert_eq!(producer_state.last_items.len(), first_payloads.len());
        let mut restored = StreamStateMachine::restore(snapshot).expect("restore snapshot");

        let retry_refs = retry_payloads
            .iter()
            .map(Vec::as_slice)
            .collect::<Vec<_>>();
        let duplicate = restored
            .append_batch_borrowed(
                stream_id.clone(),
                Some("application/octet-stream"),
                &retry_refs,
                Some(producer("writer-1", 0, 0)),
                0,
            )
            .expect("duplicate producer batch after restore");
        prop_assert!(duplicate.deduplicated);
        prop_assert_eq!(duplicate.items, expected_items);
        prop_assert_eq!(
            restored
                .read(&stream_id, 0, expected.len())
                .expect("read after duplicate producer batch")
                .payload,
            expected.clone()
        );

        let next_refs = next_payloads
            .iter()
            .map(Vec::as_slice)
            .collect::<Vec<_>>();
        let next = restored
            .append_batch_borrowed(
                stream_id.clone(),
                Some("application/octet-stream"),
                &next_refs,
                Some(producer("writer-1", 1, 0)),
                0,
        )
        .expect("new producer epoch batch after restore");
        prop_assert!(!next.deduplicated);
        let next_start = u64::try_from(expected.len()).expect("payload len fits u64");
        for (item, payload) in next.items.iter().zip(next_payloads.iter()) {
            let start_offset = u64::try_from(expected.len()).expect("payload len fits u64");
            expected.extend_from_slice(payload);
            let next_offset = u64::try_from(expected.len()).expect("payload len fits u64");
            prop_assert_eq!(item.offset, start_offset);
            prop_assert_eq!(item.next_offset, next_offset);
            prop_assert!(!item.deduplicated);
        }
        prop_assert_eq!(next.items[0].offset, next_start);

        let mut restored =
            StreamStateMachine::restore(restored.snapshot()).expect("restore after epoch batch");
        let duplicate_epoch_refs = duplicate_epoch_payloads
            .iter()
            .map(Vec::as_slice)
            .collect::<Vec<_>>();
        let duplicate_epoch = restored
            .append_batch_borrowed(
                stream_id.clone(),
                Some("application/octet-stream"),
                &duplicate_epoch_refs,
                Some(producer("writer-1", 1, 0)),
                0,
            )
            .expect("duplicate producer epoch batch");
        prop_assert!(duplicate_epoch.deduplicated);
        prop_assert_eq!(duplicate_epoch.items.len(), next.items.len());
        for (duplicate_item, original_item) in duplicate_epoch.items.iter().zip(next.items.iter()) {
            prop_assert_eq!(duplicate_item.offset, original_item.offset);
            prop_assert_eq!(duplicate_item.next_offset, original_item.next_offset);
            prop_assert_eq!(duplicate_item.closed, original_item.closed);
            prop_assert!(duplicate_item.deduplicated);
        }

        let stale_response = restored.append_batch_borrowed(
            stream_id.clone(),
            Some("application/octet-stream"),
            &duplicate_epoch_refs,
            Some(producer("writer-1", 0, 1)),
            0,
        );
        prop_assert!(
            matches!(
                stale_response,
                Err(StreamResponse::Error {
                    code: StreamErrorCode::ProducerEpochStale,
                    ..
                })
            ),
            "unexpected stale producer batch response: {:?}",
            stale_response
        );
        prop_assert_eq!(
            restored.head(&stream_id).expect("head").tail_offset,
            u64::try_from(expected.len()).expect("payload len fits u64")
        );
        prop_assert_eq!(
            restored
                .read(&stream_id, 0, expected.len())
                .expect("final producer batch read")
                .payload,
            expected
        );
    }

    #[test]
    fn prop_ttl_expiry_uses_generated_wall_clock(
        start_ms in 0_u64..10_000,
        ttl_seconds in 1_u64..60,
        touch_delta_ms in 0_u64..1_000,
        payload in payload_strategy(),
    ) {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        let stream_id = stream("prop-ttl");
        let ttl_ms = ttl_seconds * 1_000;
        let touch_ms = start_ms + touch_delta_ms.min(ttl_ms - 1);
        let expire_ms = touch_ms + ttl_ms;

        let create_response = machine.apply(StreamCommand::CreateStream {
                stream_id: stream_id.clone(),
                content_type: "application/octet-stream".to_owned(),
                initial_payload: payload,
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: Some(ttl_seconds),
                stream_expires_at_ms: None,
                attrs: None,
                now_ms: start_ms,
            });
        prop_assert!(
            matches!(create_response, StreamResponse::Created { .. }),
            "unexpected create response: {:?}",
            create_response
        );
        prop_assert!(machine.read_plan_at(&stream_id, 0, 16, touch_ms).is_ok());
        prop_assert_eq!(
            machine.apply(StreamCommand::TouchStreamAccess {
                stream_id: stream_id.clone(),
                now_ms: touch_ms,
                renew_ttl: true,
            }),
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
            machine.apply(StreamCommand::TouchStreamAccess {
                stream_id,
                now_ms: expire_ms,
                renew_ttl: true,
            }),
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
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
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
            machine.apply(StreamCommand::FlushCold {
                stream_id: stream_id.clone(),
                chunk: ColdChunkRef {
                    start_offset: candidate.start_offset,
                    end_offset: candidate.end_offset,
                    s3_path: "s3://bucket/prop-cold/000000".to_owned(),
                    object_size: u64::try_from(candidate.payload.len()).unwrap(),
                },
            }),
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
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
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
        prop_assert_eq!(
            machine.apply(StreamCommand::PublishSnapshot {
                stream_id: stream_id.clone(),
                snapshot_offset,
                content_type: "application/octet-stream".to_owned(),
                payload: snapshot_payload.clone(),
                now_ms: 0,
            }),
            StreamResponse::SnapshotPublished {
                snapshot_offset,
                record_range: None,
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
            machine.apply(StreamCommand::FlushCold {
                stream_id: stream_id.clone(),
                chunk: ColdChunkRef {
                    start_offset: candidate.start_offset,
                    end_offset: candidate.end_offset,
                    s3_path: "s3://bucket/prop-snapshot-cold/000000".to_owned(),
                    object_size: u64::try_from(candidate.payload.len()).expect("payload len fits u64"),
                },
            }),
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
        let expected_snapshot = Some(StreamVisibleSnapshot {
            offset: snapshot_offset,
            content_type: "application/octet-stream".to_owned(),
            payload: snapshot_payload.clone(),
        });
        prop_assert_eq!(
            bootstrap.snapshot.as_ref(),
            expected_snapshot.as_ref()
        );
        prop_assert!(message_records_cover_retained_suffix(
            &bootstrap.updates,
            snapshot_offset,
            tail_offset
        ));
        prop_assert_eq!(bootstrap.next_offset, tail_offset);

        let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore snapshot");
        prop_assert_eq!(
            restored.latest_snapshot(&stream_id).expect("latest snapshot"),
            bootstrap.snapshot.clone()
        );
        prop_assert_eq!(restored.cold_chunks(&stream_id), machine.cold_chunks(&stream_id));
        prop_assert_eq!(restored.hot_start_offset(&stream_id), candidate.end_offset);
        prop_assert_eq!(restored.bootstrap_plan(&stream_id).expect("restored bootstrap"), bootstrap);
        prop_assert_eq!(
            restored
                .integrity_snapshot(&stream_id)
                .expect("restored integrity")
                .tail_offset,
            tail_offset
        );
    }

    #[test]
    fn prop_stale_cold_flush_after_delete_recreate_does_not_mutate_new_stream(
        new_payload in vec(any::<u8>(), 1..=32),
        old_extra in vec(any::<u8>(), 1..=32),
    ) {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
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

        let delete_response = machine.apply(StreamCommand::DeleteStream {
            stream_id: stream_id.clone(),
        });
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

        let stale_flush = machine.apply(StreamCommand::FlushCold {
            stream_id: stream_id.clone(),
            chunk: ColdChunkRef {
                start_offset: candidate.start_offset,
                end_offset: candidate.end_offset,
                s3_path: "s3://bucket/prop-stale-cold/old-candidate".to_owned(),
                object_size: u64::try_from(candidate.payload.len()).expect("payload len fits u64"),
            },
        });
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
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);

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
            .plan_next_cold_flush_batch(min_hot_bytes, max_flush_bytes, max_candidates)
            .expect("plan cold flush batch");
        let repeated_candidates = machine
            .plan_next_cold_flush_batch(min_hot_bytes, max_flush_bytes, max_candidates)
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
                machine.apply(StreamCommand::FlushCold {
                    stream_id: candidate.stream_id.clone(),
                    chunk: ColdChunkRef {
                        start_offset: candidate.start_offset,
                        end_offset: candidate.end_offset,
                        s3_path: format!("s3://bucket/prop-batch/{index:06}"),
                        object_size: u64::try_from(candidate.payload.len())
                            .expect("payload len fits u64"),
                    },
                }),
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
