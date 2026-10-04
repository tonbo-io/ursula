//! Capacity hygiene (F7) measured through the state machine: after a
//! flush or retention, retained containers hold at most
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
fn retention_shrinks_record_index() {
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
    assert_eq!(index.dense_len(), 10);
    assert!(
        bounded(10, index.record_offsets_capacity()),
        "record index capacity {}",
        index.record_offsets_capacity()
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
fn compaction_shrinks_cold_ref_vectors() {
    // F7: CompactCold and retention remove whole runs of state refs; the
    // vectors return the freed capacity.
    const SLICES: u64 = 512;
    let mut machine = StreamStateMachine::new();
    assert!(matches!(
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: "hygiene".to_owned(),
        }),
        StreamResponse::BucketCreated { .. }
    ));
    let stream_id = BucketStreamId::new("hygiene", "cold-refs");
    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: vec![b'x'; usize::try_from(SLICES * 2).unwrap()].into(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            now_ms: 0,
        }),
        StreamResponse::Created { .. }
    ));
    let slice = |index: u64| ColdChunkRef {
        start_offset: index * 2,
        end_offset: index * 2 + 2,
        s3_path: format!("hygiene/_packs/0/pack-{index}.bin"),
        object_size: 1_024,
        object_offset: 0,
        shared_object: true,
        payload_digest: String::new(),
    };
    for index in 0..SLICES {
        assert!(matches!(
            machine.apply(StreamCommand::FlushCold {
                cold_generation: machine
                    .cold_index_generation(&stream_id)
                    .unwrap_or_default(),
                stream_id: stream_id.clone(),
                chunk: slice(index),
            }),
            StreamResponse::ColdFlushed { .. }
        ));
    }
    let (chunks_capacity, _) = slot(&machine, &stream_id).cold.ref_capacities();
    assert!(chunks_capacity >= usize::try_from(SLICES).unwrap());

    // Compact all but the last 4 slices into one exclusive chunk.
    let compacted = SLICES - 4;
    assert!(matches!(
        machine.apply(StreamCommand::CompactCold {
            stream_id: stream_id.clone(),
            old_chunks: (0..compacted).map(slice).collect(),
            replacement: ColdChunkRef {
                start_offset: 0,
                end_offset: compacted * 2,
                s3_path: "hygiene/cold-refs/chunks/compacted.bin".to_owned(),
                object_size: compacted * 2,
                object_offset: 0,
                shared_object: false,
                payload_digest: String::new(),
            },
            gc_not_before_ms: 0,
        }),
        StreamResponse::ColdCompacted { .. }
    ));
    let slot_after = slot(&machine, &stream_id);
    let len = slot_after.cold.cold_chunks().len();
    assert_eq!(len, 4);
    let (chunks_capacity, _) = slot_after.cold.ref_capacities();
    assert!(
        bounded(len, chunks_capacity),
        "cold chunks: len {len} capacity {chunks_capacity}"
    );
}
