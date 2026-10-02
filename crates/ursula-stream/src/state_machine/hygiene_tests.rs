//! Capacity hygiene (F7) measured through the state machine: after a
//! flush, retention or collapse, retained containers hold at most
//! `2 * len + 64` elements of capacity.

use super::*;

const JSON: &str = "application/json";
const RECORDS: usize = 100_000;

fn bounded(len: usize, capacity: usize) -> bool {
    capacity <= 2 * len + 64
}

fn machine_with_json_stream(name: &str) -> (StreamStateMachine, BucketStreamId) {
    let mut machine = StreamStateMachine::new();
    assert!(matches!(
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: "hygiene".to_owned(),
        }),
        StreamResponse::BucketCreated { .. }
    ));
    let stream_id = BucketStreamId::new("hygiene", name);
    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: JSON.to_owned(),
            initial_payload: b"1\n".repeat(RECORDS).into(),
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
    (machine, stream_id)
}

fn slot<'a>(machine: &'a StreamStateMachine, stream_id: &BucketStreamId) -> &'a StreamSlot {
    machine.stream_slot(stream_id).unwrap()
}

#[test]
fn retention_shrinks_record_index_and_message_records() {
    // Measured before F7: trimming 990k of 1M records kept 8.04 MB of index.
    let (mut machine, stream_id) = machine_with_json_stream("retention");
    let keep = 10u64;
    let retained = 2 * (u64::try_from(RECORDS).unwrap() - keep);
    assert!(matches!(
        machine.apply(StreamCommand::PublishSnapshot {
            stream_id: stream_id.clone(),
            snapshot_offset: retained,
            content_type: JSON.to_owned(),
            payload: bytes::Bytes::from_static(b"{}"),
            expected_digest: None,
            now_ms: 1,
        }),
        StreamResponse::SnapshotPublished { .. }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::AdvanceRetention {
            stream_id: stream_id.clone(),
            retained_offset: retained,
            now_ms: 2,
        }),
        StreamResponse::RetentionAdvanced { .. }
    ));
    let slot = slot(&machine, &stream_id);
    let index = slot.record_index.as_ref().unwrap();
    assert_eq!(index.record_offsets().len(), 10);
    assert!(
        bounded(10, index.record_offsets_capacity()),
        "record index capacity {}",
        index.record_offsets_capacity()
    );
    assert!(
        bounded(slot.message_records.len(), slot.message_records.capacity()),
        "message records: len {} capacity {}",
        slot.message_records.len(),
        slot.message_records.capacity()
    );
}

#[test]
fn rejected_retention_leaves_record_index_untouched() {
    let (mut machine, stream_id) = machine_with_json_stream("retention-reject");
    let before = slot(&machine, &stream_id).record_index.clone();
    assert!(matches!(
        machine.apply(StreamCommand::PublishSnapshot {
            stream_id: stream_id.clone(),
            snapshot_offset: 20,
            content_type: JSON.to_owned(),
            payload: bytes::Bytes::from_static(b"{}"),
            expected_digest: None,
            now_ms: 1,
        }),
        StreamResponse::SnapshotPublished { .. }
    ));
    // Offset 13 is inside a record.
    assert!(matches!(
        machine.apply(StreamCommand::AdvanceRetention {
            stream_id: stream_id.clone(),
            retained_offset: 13,
            now_ms: 2,
        }),
        StreamResponse::Error { .. }
    ));
    assert_eq!(slot(&machine, &stream_id).record_index, before);
}

#[test]
fn flush_collapse_allocates_post_collapse_message_records() {
    // Measured before F7: collapse allocated with the old length, holding
    // 16 MB for a single message record.
    let (mut machine, stream_id) = machine_with_json_stream("collapse");
    assert_eq!(slot(&machine, &stream_id).message_records.len(), RECORDS);
    let end = 2 * u64::try_from(RECORDS).unwrap() - 20;
    assert!(matches!(
        machine.apply(StreamCommand::FlushCold {
            cold_generation: None,
            stream_id: stream_id.clone(),
            chunk: ColdChunkRef {
                start_offset: 0,
                end_offset: end,
                s3_path: "s3://hygiene/collapse".to_owned(),
                object_size: end,
                object_offset: 0,
                shared_object: false,
                payload_digest: String::new(),
            },
        }),
        StreamResponse::ColdFlushed { .. }
    ));
    let slot = slot(&machine, &stream_id);
    assert_eq!(slot.message_records.len(), 11);
    assert!(
        bounded(slot.message_records.len(), slot.message_records.capacity()),
        "message records capacity {}",
        slot.message_records.capacity()
    );
    assert_eq!(slot.hot_buffer.len(), 20);
}
