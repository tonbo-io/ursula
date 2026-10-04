//! Capacity hygiene (F7) measured through the state machine: after a
//! flush or retention, retained containers hold at most
//! `2 * len + 64` elements of capacity.

use super::*;

fn bounded(len: usize, capacity: usize) -> bool {
    capacity <= 2 * len + 64
}

fn slot<'a>(machine: &'a StreamStateMachine, stream_id: &BucketStreamId) -> &'a StreamSlot {
    machine.stream_slot(stream_id).unwrap()
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
