//! Bounded-state level Lb1 cold hygiene (feature level 1): F18 step 2
//! derived cold coverage, F14b `DeferColdGc`, F14i retention grace, and the
//! incarnation check on `FlushCold`. Each test pins the level-0 behavior next
//! to the level-1 one, because level 0 must stay byte-for-byte what older
//! binaries apply.

use super::*;

const BUCKET: &str = "lb1cold";
const OCTET: &str = "application/octet-stream";

fn stream(id: &str) -> BucketStreamId {
    BucketStreamId::new(BUCKET, id)
}

fn machine_at(level: u32) -> StreamStateMachine {
    let mut machine = StreamStateMachine::new();
    assert!(matches!(
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: BUCKET.to_owned(),
        }),
        StreamResponse::BucketCreated { .. }
    ));
    if level > 0 {
        assert!(matches!(
            machine.apply(StreamCommand::SetFeatureLevel { level }),
            StreamResponse::FeatureLevelSet { .. }
        ));
    }
    machine
}

fn create(machine: &mut StreamStateMachine, id: &str, now_ms: u64) {
    let response = machine.apply(StreamCommand::CreateStream {
        stream_id: stream(id),
        content_type: OCTET.to_owned(),
        initial_payload: bytes::Bytes::new(),
        close_after: false,
        stream_seq: None,
        producer: None,
        stream_ttl_seconds: None,
        stream_expires_at_ms: None,
        attrs: None,
        now_ms,
    });
    assert!(
        matches!(response, StreamResponse::Created { .. }),
        "{response:?}"
    );
}

fn append(machine: &mut StreamStateMachine, id: &str, payload: &[u8]) {
    let response = machine.apply(StreamCommand::Append {
        stream_id: stream(id),
        content_type: Some(OCTET.to_owned()),
        payload: bytes::Bytes::copy_from_slice(payload),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 0,
        record_match: None,
    });
    assert!(
        matches!(response, StreamResponse::Appended { .. }),
        "{response:?}"
    );
}

fn append_external(machine: &mut StreamStateMachine, id: &str, path: &str, len: u64) {
    let response = machine.apply(StreamCommand::AppendExternal {
        stream_id: stream(id),
        content_type: Some(OCTET.to_owned()),
        payload: ExternalPayloadRef {
            s3_path: path.to_owned(),
            payload_len: len,
            object_size: len,
        },
        record_ends: Vec::new(),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 0,
        record_match: None,
    });
    assert!(
        matches!(response, StreamResponse::Appended { .. }),
        "{response:?}"
    );
}

fn flush(
    machine: &mut StreamStateMachine,
    id: &str,
    start_offset: u64,
    end_offset: u64,
    path: &str,
) -> StreamResponse {
    machine.apply(StreamCommand::FlushCold {
        stream_id: stream(id),
        chunk: ColdChunkRef {
            start_offset,
            end_offset,
            s3_path: path.to_owned(),
            object_size: end_offset - start_offset,
            object_offset: 0,
            shared_object: false,
            payload_digest: String::new(),
        },
        cold_generation: None,
    })
}

fn publish_snapshot(machine: &mut StreamStateMachine, id: &str, offset: u64) -> StreamResponse {
    machine.apply(StreamCommand::PublishSnapshot {
        stream_id: stream(id),
        snapshot_offset: offset,
        content_type: OCTET.to_owned(),
        payload: bytes::Bytes::from_static(b"state"),
        expected_digest: None,
        now_ms: 0,
    })
}

fn retain(machine: &mut StreamStateMachine, id: &str, offset: u64, now_ms: u64) -> StreamResponse {
    machine.apply(StreamCommand::AdvanceRetention {
        stream_id: stream(id),
        retained_offset: offset,
        now_ms,
    })
}

fn entry<'a>(snapshot: &'a StreamSnapshot, id: &str) -> &'a StreamSnapshotEntry {
    snapshot
        .streams
        .iter()
        .find(|entry| entry.metadata.stream_id == stream(id))
        .expect("snapshot entry")
}

#[track_caller]
fn assert_code(response: &StreamResponse, code: StreamErrorCode) {
    match response {
        StreamResponse::Error { code: actual, .. } if *actual == code => {}
        other => panic!("expected {code:?}, got {other:?}"),
    }
}

/// Hot message `[0, 4)` with an external append `[4, 7)` above it: the
/// level-0 scalar frontier is raised to 7 by the external, which made the
/// frontier clause accept the intra-message offset 2.
fn hot_message_below_external(level: u32, id: &str) -> StreamStateMachine {
    let mut machine = machine_at(level);
    create(&mut machine, id, 1);
    append(&mut machine, id, b"abcd");
    append_external(&mut machine, id, "lb1/external/x.bin", 3);
    machine
}

#[test]
fn f18_snapshot_at_intra_message_hot_offset_below_external_is_rejected_at_lb1() {
    // Level 0 keeps the old (wrong) acceptance: older binaries apply it.
    let mut legacy = hot_message_below_external(0, "align");
    assert!(matches!(
        publish_snapshot(&mut legacy, "align", 2),
        StreamResponse::SnapshotPublished { .. }
    ));

    let mut machine = hot_message_below_external(1, "align");
    assert_code(
        &publish_snapshot(&mut machine, "align", 2),
        StreamErrorCode::InvalidSnapshot,
    );
    // Message ends and the retained offset stay valid.
    assert!(matches!(
        publish_snapshot(&mut machine, "align", 4),
        StreamResponse::SnapshotPublished { .. }
    ));
    assert!(matches!(
        publish_snapshot(&mut machine, "align", 7),
        StreamResponse::SnapshotPublished { .. }
    ));
}

#[test]
fn f18_offsets_at_or_below_the_seal_point_are_aligned_at_lb1() {
    let mut machine = hot_message_below_external(1, "seal");
    // Flush the hot message: nothing is hot, so p(s) is the tail.
    assert!(matches!(
        flush(&mut machine, "seal", 0, 4, "lb1/chunks/a.bin"),
        StreamResponse::ColdFlushed { .. }
    ));
    assert_eq!(machine.seal_point(&stream("seal")), 7);
    // Intra-record cold offsets below p(s) stay accepted at Lb1 (F1 rejects
    // them for JSON leader-side, later).
    assert!(matches!(
        publish_snapshot(&mut machine, "seal", 5),
        StreamResponse::SnapshotPublished { .. }
    ));
}

#[test]
fn f18_retention_collapse_stops_at_the_seal_point_at_lb1() {
    for (level, expected_updates) in [(0, None), (1, Some(2))] {
        let mut machine = machine_at(level);
        create(&mut machine, "collapse", 1);
        append(&mut machine, "collapse", b"ab");
        append(&mut machine, "collapse", b"cd");
        append_external(&mut machine, "collapse", "lb1/external/y.bin", 3);
        assert!(matches!(
            publish_snapshot(&mut machine, "collapse", 2),
            StreamResponse::SnapshotPublished { .. }
        ));
        assert!(matches!(
            retain(&mut machine, "collapse", 2, 0),
            StreamResponse::RetentionAdvanced { .. }
        ));
        let plan = machine
            .bootstrap_plan(&stream("collapse"))
            .expect("bootstrap plan");
        match expected_updates {
            // Level 0 collapses `[2, 7)` (a hot message and the external)
            // into one record, so bootstrap can only answer a partial.
            None => {
                assert!(plan.updates.is_empty());
                assert!(!plan.up_to_date);
                assert_eq!(plan.next_offset, 2);
            }
            // Lb1 keeps both messages: `[2, 4)` is hot, above p(s) = 2.
            Some(count) => {
                assert_eq!(plan.updates.len(), count, "{plan:?}");
                assert!(plan.up_to_date);
                assert_eq!(plan.next_offset, 7);
            }
        }
    }
}

#[test]
fn f18_bootstrap_after_a_d1_flush_uses_the_seal_point() {
    // D1 at Lb1: hot `ab`, external `[2, 5)`, flush of the hot prefix.
    let mut machine = machine_at(1);
    create(&mut machine, "boot", 1);
    append(&mut machine, "boot", b"ab");
    append_external(&mut machine, "boot", "lb1/external/z.bin", 3);
    assert!(matches!(
        flush(&mut machine, "boot", 0, 2, "lb1/chunks/ab.bin"),
        StreamResponse::ColdFlushed { .. }
    ));
    // Everything below p(s) = 5 is cold, so a bootstrap from 0 is partial.
    let plan = machine.bootstrap_plan(&stream("boot")).expect("plan");
    assert!(plan.updates.is_empty());
    assert_eq!(plan.next_offset, 0);
    // A checkpoint at the tail bootstraps up to date.
    assert!(matches!(
        publish_snapshot(&mut machine, "boot", 5),
        StreamResponse::SnapshotPublished { .. }
    ));
    let plan = machine.bootstrap_plan(&stream("boot")).expect("plan");
    assert!(plan.up_to_date);
    assert_eq!(plan.next_offset, 5);
}

#[test]
fn f18_snapshot_field_six_is_the_seal_point_and_ignored_at_restore() {
    let mut machine = machine_at(1);
    create(&mut machine, "field", 1);
    append(&mut machine, "field", b"ab");
    append_external(&mut machine, "field", "lb1/external/w.bin", 3);
    assert!(matches!(
        flush(&mut machine, "field", 0, 2, "lb1/chunks/ab.bin"),
        StreamResponse::ColdFlushed { .. }
    ));
    append(&mut machine, "field", b"cd");
    let snapshot = machine.snapshot();
    // p(s) is the first hot byte, 5.
    assert_eq!(entry(&snapshot, "field").cold_frontier_offset, 5);

    // Restore ignores the field: any value restores to the same state.
    let mut tampered = snapshot.clone();
    for stream_entry in &mut tampered.streams {
        stream_entry.cold_frontier_offset = 1;
    }
    let restored = StreamStateMachine::restore(tampered).expect("restore");
    assert_eq!(restored.snapshot(), snapshot);
    let restored_plan = restored
        .read_plan(&stream("field"), 0, 64)
        .expect("restored read plan");
    assert_eq!(restored_plan.next_offset, 7);

    // Level 0 still writes the legacy scalar (2, regressed below the
    // external: bounded-state D1).
    let mut legacy = machine_at(0);
    create(&mut legacy, "field", 1);
    append(&mut legacy, "field", b"ab");
    append_external(&mut legacy, "field", "lb1/external/w.bin", 3);
    assert!(matches!(
        flush(&mut legacy, "field", 0, 2, "lb1/chunks/ab.bin"),
        StreamResponse::ColdFlushed { .. }
    ));
    assert_eq!(entry(&legacy.snapshot(), "field").cold_frontier_offset, 2);
}

#[test]
fn f18_delete_enqueues_stream_gc_only_when_bytes_left_the_hot_buffer() {
    let mut machine = machine_at(1);
    create(&mut machine, "hot-only", 1);
    append(&mut machine, "hot-only", b"abcd");
    assert!(matches!(
        machine.apply(StreamCommand::DeleteStream {
            stream_id: stream("hot-only"),
        }),
        StreamResponse::Deleted
    ));
    assert_eq!(machine.pending_cold_gc_len(), 0);

    create(&mut machine, "flushed", 2);
    append(&mut machine, "flushed", b"abcd");
    assert!(matches!(
        flush(&mut machine, "flushed", 0, 2, "lb1/chunks/ab.bin"),
        StreamResponse::ColdFlushed { .. }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::DeleteStream {
            stream_id: stream("flushed"),
        }),
        StreamResponse::Deleted
    ));
    assert_eq!(machine.pending_cold_gc_len(), 1);
}

/// Appends `abcd` and publishes `[0, 4)` as a slice of a shared pack.
fn shared_pack_stream(level: u32, id: &str) -> StreamStateMachine {
    let mut machine = machine_at(level);
    create(&mut machine, id, 1);
    append(&mut machine, id, b"abcd");
    let response = machine.apply(StreamCommand::FlushCold {
        stream_id: stream(id),
        chunk: ColdChunkRef {
            start_offset: 0,
            end_offset: 4,
            s3_path: "_packs/0/pack.bin".to_owned(),
            object_size: 64,
            object_offset: 16,
            shared_object: true,
            payload_digest: String::new(),
        },
        cold_generation: None,
    });
    assert!(
        matches!(response, StreamResponse::ColdFlushed { .. }),
        "{response:?}"
    );
    append(&mut machine, id, b"ef");
    assert!(matches!(
        publish_snapshot(&mut machine, id, 4),
        StreamResponse::SnapshotPublished { .. }
    ));
    machine
}

#[test]
fn f14i_retention_keeps_dropped_pack_slices_for_the_grace_at_lb1() {
    const NOW_MS: u64 = 1_000_000;
    for (level, expected_not_before) in [
        (0, 0),
        (1, NOW_MS + super::cold::RETENTION_COLD_GC_GRACE_MS),
    ] {
        let mut machine = shared_pack_stream(level, "pack");
        assert!(matches!(
            retain(&mut machine, "pack", 4, NOW_MS),
            StreamResponse::RetentionAdvanced { .. }
        ));
        let pending = machine.pending_cold_gc_batch(8);
        assert_eq!(pending.len(), 1, "level {level}: {pending:?}");
        assert_eq!(
            pending[0].target,
            ColdGcTarget::Paths(vec!["_packs/0/pack.bin".to_owned()])
        );
        assert_eq!(
            pending[0].not_before_ms, expected_not_before,
            "level {level}"
        );
        // The grace is replicated state: a restored replica keeps it.
        let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore");
        assert_eq!(restored.pending_cold_gc_batch(8), pending);
    }
}

fn gc_queue_with_two_entries(level: u32) -> StreamStateMachine {
    let mut machine = machine_at(level);
    for (index, id) in ["gc-a", "gc-b"].into_iter().enumerate() {
        create(&mut machine, id, 10 + index as u64);
        append(&mut machine, id, b"abcd");
        assert!(matches!(
            flush(&mut machine, id, 0, 4, &format!("lb1/chunks/{id}.bin")),
            StreamResponse::ColdFlushed { .. }
        ));
        assert!(matches!(
            machine.apply(StreamCommand::DeleteStream {
                stream_id: stream(id),
            }),
            StreamResponse::Deleted
        ));
    }
    assert_eq!(machine.pending_cold_gc_len(), 2);
    machine
}

#[test]
fn f14b_defer_cold_gc_requires_level_one() {
    let mut machine = gc_queue_with_two_entries(0);
    let head = machine.pending_cold_gc_batch(1)[0].seq;
    assert_code(
        &machine.apply(StreamCommand::DeferColdGc {
            seq: head,
            not_before_ms: 5,
        }),
        StreamErrorCode::FeatureNotEnabled,
    );
    assert_eq!(machine.pending_cold_gc_batch(1)[0].seq, head);
}

#[test]
fn f14b_defer_cold_gc_moves_the_failing_head_behind_the_queue() {
    let mut machine = gc_queue_with_two_entries(1);
    let before = machine.pending_cold_gc_batch(8);
    let (head, second) = (before[0].clone(), before[1].clone());
    let response = machine.apply(StreamCommand::DeferColdGc {
        seq: head.seq,
        not_before_ms: 60_000,
    });
    let StreamResponse::ColdGcDeferred {
        new_seq: Some(new_seq),
    } = response
    else {
        panic!("expected a deferral, got {response:?}");
    };
    assert!(new_seq > second.seq);
    let after = machine.pending_cold_gc_batch(8);
    assert_eq!(after[0], second);
    assert_eq!(after[1].seq, new_seq);
    assert_eq!(after[1].target, head.target);
    assert_eq!(after[1].cold_generation, head.cold_generation);
    assert_eq!(after[1].not_before_ms, 60_000);

    // Acking the entry now at the head leaves the deferred one pending.
    assert_eq!(
        machine.apply(StreamCommand::AckColdGc {
            up_to_seq: second.seq,
        }),
        StreamResponse::ColdGcAcked { removed: 1 }
    );
    assert_eq!(machine.pending_cold_gc_len(), 1);
    // Replays are no-ops, and the order survives a snapshot.
    assert_eq!(
        machine.apply(StreamCommand::DeferColdGc {
            seq: head.seq,
            not_before_ms: 60_000,
        }),
        StreamResponse::ColdGcDeferred { new_seq: None }
    );
    let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore");
    assert_eq!(
        restored.pending_cold_gc_batch(8),
        machine.pending_cold_gc_batch(8)
    );
}

/// Wave-1 follow-up: a flush planned from one incarnation must not publish
/// into the incarnation that replaced it, even when the new hot prefix holds
/// the same bytes (its chunk lives under the old generation, which stream GC
/// of the old incarnation deletes).
#[test]
fn flush_cold_from_a_removed_incarnation_is_stale_at_lb1() {
    let mut machine = machine_at(1);
    create(&mut machine, "reborn", 100);
    append(&mut machine, "reborn", b"abcd");
    let candidate = machine
        .plan_cold_flush(&stream("reborn"), 1, 64)
        .expect("plan")
        .expect("candidate");
    assert!(matches!(
        machine.apply(StreamCommand::DeleteStream {
            stream_id: stream("reborn"),
        }),
        StreamResponse::Deleted
    ));
    create(&mut machine, "reborn", 100);
    append(&mut machine, "reborn", b"abcd");
    let live = machine
        .cold_index_generation(&stream("reborn"))
        .expect("live generation");
    assert_ne!(live, candidate.cold_generation);

    let stale_chunk = ColdChunkRef {
        start_offset: candidate.start_offset,
        end_offset: candidate.end_offset,
        s3_path: "lb1/chunks/old-incarnation.bin".to_owned(),
        object_size: 4,
        object_offset: 0,
        shared_object: false,
        payload_digest: candidate.payload_digest.clone(),
    };
    assert!(
        machine
            .check_cold_flush(&stream("reborn"), &stale_chunk)
            .is_ok(),
        "the byte-level check alone cannot tell the incarnations apart"
    );
    let response = machine.apply(StreamCommand::FlushCold {
        stream_id: stream("reborn"),
        chunk: stale_chunk.clone(),
        cold_generation: Some(candidate.cold_generation),
    });
    match &response {
        StreamResponse::Error { code, context, .. } => {
            assert_eq!(*code, StreamErrorCode::InvalidColdFlush);
            assert!(context.contains(&StreamErrorContext::StaleColdFlushCandidate));
        }
        other => panic!("expected a stale flush, got {other:?}"),
    }
    assert_eq!(machine.hot_start_offset(&stream("reborn")), 0);

    // The live incarnation's own flush, and a proposer without the field,
    // still apply.
    assert!(matches!(
        machine.apply(StreamCommand::FlushCold {
            stream_id: stream("reborn"),
            chunk: ColdChunkRef {
                end_offset: 2,
                object_size: 2,
                payload_digest: String::new(),
                ..stale_chunk.clone()
            },
            cold_generation: Some(live),
        }),
        StreamResponse::ColdFlushed { .. }
    ));
    assert!(matches!(
        flush(&mut machine, "reborn", 2, 4, "lb1/chunks/cd.bin"),
        StreamResponse::ColdFlushed { .. }
    ));
}

#[test]
fn flush_cold_generation_is_not_checked_at_level_zero() {
    let mut machine = machine_at(0);
    create(&mut machine, "legacy", 1);
    append(&mut machine, "legacy", b"abcd");
    // Old binaries ignore the field, so level 0 must too.
    assert!(matches!(
        machine.apply(StreamCommand::FlushCold {
            stream_id: stream("legacy"),
            chunk: ColdChunkRef {
                start_offset: 0,
                end_offset: 4,
                s3_path: "lb1/chunks/legacy.bin".to_owned(),
                object_size: 4,
                object_offset: 0,
                shared_object: false,
                payload_digest: String::new(),
            },
            cold_generation: Some(99),
        }),
        StreamResponse::ColdFlushed { .. }
    ));
}
