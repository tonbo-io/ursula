//! Bounded-state cold hygiene: F18 step 2 derived cold coverage, F14b
//! `DeferColdGc`, F14i retention grace, and the incarnation check on
//! `FlushCold`.

use super::*;

const BUCKET: &str = "derivedcold";
const OCTET: &str = "application/octet-stream";

fn stream(id: &str) -> BucketStreamId {
    BucketStreamId::new(BUCKET, id)
}

fn fresh_machine() -> StreamStateMachine {
    let mut machine = StreamStateMachine::new();
    assert!(matches!(
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: BUCKET.to_owned(),
        }),
        StreamResponse::BucketCreated { .. }
    ));
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
        cold_generation: machine
            .cold_index_generation(&stream(id))
            .unwrap_or_default(),
    })
}

fn publish_snapshot(machine: &mut StreamStateMachine, id: &str, offset: u64) -> StreamResponse {
    machine.apply(StreamCommand::PublishSnapshot {
        stream_id: stream(id),
        snapshot_offset: offset,
        content_type: OCTET.to_owned(),
        payload: bytes::Bytes::from_static(b"state"),
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

#[track_caller]
fn assert_code(response: &StreamResponse, code: StreamErrorCode) {
    match response {
        StreamResponse::Error { code: actual, .. } if *actual == code => {}
        other => panic!("expected {code:?}, got {other:?}"),
    }
}

/// Hot message `[0, 4)` with an external append `[4, 7)` above it.
fn hot_message_below_external(id: &str) -> StreamStateMachine {
    let mut machine = fresh_machine();
    create(&mut machine, id, 1);
    append(&mut machine, id, b"abcd");
    append_external(&mut machine, id, "derived/external/x.bin", 3);
    machine
}

#[test]
fn f18_snapshot_at_intra_message_hot_offset_below_external_is_rejected() {
    let mut machine = hot_message_below_external("align");
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
fn f18_offsets_at_or_below_the_seal_point_are_aligned() {
    let mut machine = hot_message_below_external("seal");
    // Flush the hot message: nothing is hot, so p(s) is the tail.
    assert!(matches!(
        flush(&mut machine, "seal", 0, 4, "derived/chunks/a.bin"),
        StreamResponse::ColdFlushed { .. }
    ));
    assert_eq!(machine.seal_point(&stream("seal")), 7);
    // Intra-record cold offsets below p(s) stay accepted (F1 rejects them
    // for JSON leader-side, later).
    assert!(matches!(
        publish_snapshot(&mut machine, "seal", 5),
        StreamResponse::SnapshotPublished { .. }
    ));
}

#[test]
fn f18_retention_keeps_hot_messages_above_the_seal_point() {
    let mut machine = fresh_machine();
    create(&mut machine, "collapse", 1);
    append(&mut machine, "collapse", b"ab");
    append(&mut machine, "collapse", b"cd");
    append_external(&mut machine, "collapse", "derived/external/y.bin", 3);
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
    // Both messages are kept: `[2, 4)` is hot, above p(s) = 2.
    assert_eq!(plan.updates.len(), 2, "{plan:?}");
    assert!(plan.up_to_date);
    assert_eq!(plan.next_offset, 7);
}

#[test]
fn f18_bootstrap_after_a_d1_flush_uses_the_seal_point() {
    // D1: hot `ab`, external `[2, 5)`, flush of the hot prefix.
    let mut machine = fresh_machine();
    create(&mut machine, "boot", 1);
    append(&mut machine, "boot", b"ab");
    append_external(&mut machine, "boot", "derived/external/z.bin", 3);
    assert!(matches!(
        flush(&mut machine, "boot", 0, 2, "derived/chunks/ab.bin"),
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
fn f18_delete_enqueues_stream_gc_only_when_bytes_left_the_hot_buffer() {
    let mut machine = fresh_machine();
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
        flush(&mut machine, "flushed", 0, 2, "derived/chunks/ab.bin"),
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
fn shared_pack_stream(id: &str) -> StreamStateMachine {
    let mut machine = fresh_machine();
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
        cold_generation: machine
            .cold_index_generation(&stream(id))
            .unwrap_or_default(),
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
fn f14i_retention_keeps_dropped_pack_slices_for_the_grace() {
    const NOW_MS: u64 = 1_000_000;
    let mut machine = shared_pack_stream("pack");
    assert!(matches!(
        retain(&mut machine, "pack", 4, NOW_MS),
        StreamResponse::RetentionAdvanced { .. }
    ));
    let pending = machine.pending_cold_gc_batch(8);
    assert_eq!(pending.len(), 1, "{pending:?}");
    assert_eq!(
        pending[0].target,
        ColdGcTarget::Paths(vec!["_packs/0/pack.bin".to_owned()])
    );
    assert_eq!(
        pending[0].not_before_ms,
        NOW_MS + super::cold::RETENTION_COLD_GC_GRACE_MS
    );
    // The grace is replicated state: a restored replica keeps it.
    let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore");
    assert_eq!(restored.pending_cold_gc_batch(8), pending);
}

fn gc_queue_with_two_entries() -> StreamStateMachine {
    let mut machine = fresh_machine();
    for (index, id) in ["gc-a", "gc-b"].into_iter().enumerate() {
        create(&mut machine, id, 10 + index as u64);
        append(&mut machine, id, b"abcd");
        assert!(matches!(
            flush(&mut machine, id, 0, 4, &format!("derived/chunks/{id}.bin")),
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
fn f14b_defer_cold_gc_moves_the_failing_head_behind_the_queue() {
    let mut machine = gc_queue_with_two_entries();
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
fn flush_cold_from_a_removed_incarnation_is_stale() {
    let mut machine = fresh_machine();
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
        s3_path: "derived/chunks/old-incarnation.bin".to_owned(),
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
        cold_generation: candidate.cold_generation,
    });
    match &response {
        StreamResponse::Error { code, context, .. } => {
            assert_eq!(*code, StreamErrorCode::InvalidColdFlush);
            assert!(context.contains(&StreamErrorContext::StaleColdFlushCandidate));
        }
        other => panic!("expected a stale flush, got {other:?}"),
    }
    assert_eq!(machine.hot_start_offset(&stream("reborn")), 0);

    // The live incarnation's own flushes still apply.
    assert!(matches!(
        machine.apply(StreamCommand::FlushCold {
            stream_id: stream("reborn"),
            chunk: ColdChunkRef {
                end_offset: 2,
                object_size: 2,
                payload_digest: String::new(),
                ..stale_chunk.clone()
            },
            cold_generation: live,
        }),
        StreamResponse::ColdFlushed { .. }
    ));
    assert!(matches!(
        flush(&mut machine, "reborn", 2, 4, "derived/chunks/cd.bin"),
        StreamResponse::ColdFlushed { .. }
    ));
}

/// A whole hot message at the seal point stays a complete bootstrap.
#[test]
fn bootstrap_keeps_whole_hot_messages_at_the_seal_point() {
    let mut fresh = fresh_machine();
    create(&mut fresh, "fresh", 1);
    append(&mut fresh, "fresh", b"ab");
    append(&mut fresh, "fresh", b"cd");
    let plan = fresh.bootstrap_plan(&stream("fresh")).expect("plan");
    assert!(plan.up_to_date);
    assert_eq!(plan.updates.len(), 2);
}

/// Bootstrap never merges or skips messages, whatever history built the
/// stream (B6 follow-up). This test sweeps random histories of hot appends,
/// external appends, cold flushes (also at intra-message offsets), snapshots
/// and retention, and checks that every bootstrap update is exactly one
/// appended message and that a complete bootstrap covers `[snapshot, tail)`
/// with no gap.
#[test]
fn bootstrap_never_merges_or_skips_messages_for_any_history() {
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, bound: u64) -> u64 {
            self.next() % bound.max(1)
        }
    }

    fn check(machine: &StreamStateMachine, messages: &[(u64, u64)], seed: u64, phase: &str) {
        let plan = machine
            .bootstrap_plan(&stream("sweep"))
            .expect("bootstrap plan");
        let tail = messages.last().map_or(0, |message| message.1);
        let start = plan
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.offset)
            .unwrap_or_else(|| {
                machine
                    .stream_slot(&stream("sweep"))
                    .expect("slot")
                    .retained_offset
            });
        let mut expected_start = start;
        for update in &plan.updates {
            assert!(
                messages.contains(&(update.start_offset, update.end_offset)),
                "seed {seed} {phase}: update {update:?} is not exactly one message; \
                 messages {messages:?}, plan {plan:?}"
            );
            assert_eq!(
                update.start_offset, expected_start,
                "seed {seed} {phase}: bootstrap skipped bytes; plan {plan:?}"
            );
            expected_start = update.end_offset;
        }
        if plan.up_to_date {
            assert_eq!(expected_start, tail, "seed {seed} {phase}: {plan:?}");
            assert_eq!(plan.next_offset, tail);
        } else if plan.updates.is_empty() {
            assert_eq!(plan.next_offset, start, "seed {seed} {phase}: {plan:?}");
        }
    }

    for seed in 1..=3_000_u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let mut machine = fresh_machine();
        create(&mut machine, "sweep", 1);
        let mut messages: Vec<(u64, u64)> = Vec::new();
        let mut tail = 0_u64;
        let mut external = 0_u64;
        let steps = 4 + rng.below(12);
        for step in 0..steps {
            match rng.below(10) {
                0..=3 => {
                    let len = 1 + rng.below(3);
                    let payload = vec![b'h'; usize::try_from(len).unwrap()];
                    append(&mut machine, "sweep", &payload);
                    messages.push((tail, tail + len));
                    tail += len;
                }
                4..=5 => {
                    let len = 1 + rng.below(3);
                    external += 1;
                    append_external(
                        &mut machine,
                        "sweep",
                        &format!("derived/external/sweep-{seed}-{external}.bin"),
                        len,
                    );
                    messages.push((tail, tail + len));
                    tail += len;
                }
                6..=7 => {
                    let hot_start = machine.hot_start_offset(&stream("sweep"));
                    if hot_start < tail {
                        let end = hot_start + 1 + rng.below(tail - hot_start);
                        let _ = flush(
                            &mut machine,
                            "sweep",
                            hot_start,
                            end,
                            &format!("derived/chunks/sweep-{seed}-{step}.bin"),
                        );
                    }
                }
                _ => {
                    // Snapshots at message boundaries, then retention to them.
                    if let Some(&(_, end)) =
                        messages.get(usize::try_from(rng.below(len_u64(messages.len()))).unwrap())
                        && matches!(
                            publish_snapshot(&mut machine, "sweep", end),
                            StreamResponse::SnapshotPublished { .. }
                        )
                        && rng.below(2) == 0
                    {
                        let _ = retain(&mut machine, "sweep", end, 0);
                    }
                }
            }
            check(&machine, &messages, seed, "step");
        }
        check(&machine, &messages, seed, "final");
    }
}

fn len_u64(len: usize) -> u64 {
    u64::try_from(len).unwrap()
}
