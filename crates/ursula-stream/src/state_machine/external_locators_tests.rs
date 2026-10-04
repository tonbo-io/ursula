//! Bounded-state F5: external payload locators committed in state at apply
//! and removed by `OffloadColdRefs`.

use super::*;

const BUCKET: &str = "f5locators";
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
    let response = machine.apply(StreamCommand::CreateStream {
        stream_id: stream("s"),
        content_type: OCTET.to_owned(),
        initial_payload: bytes::Bytes::new(),
        close_after: false,
        stream_seq: None,
        producer: None,
        stream_ttl_seconds: None,
        stream_expires_at_ms: None,
        now_ms: 1,
    });
    assert!(
        matches!(response, StreamResponse::Created { .. }),
        "{response:?}"
    );
    machine
}

fn append_external_with_match(
    machine: &mut StreamStateMachine,
    path: &str,
    len: u64,
    record_match: Option<u64>,
) -> StreamResponse {
    machine.apply(StreamCommand::AppendExternal {
        stream_id: stream("s"),
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
        now_ms: 2,
        record_match,
    })
}

fn append_external(machine: &mut StreamStateMachine, path: &str, len: u64) {
    let response = append_external_with_match(machine, path, len, None);
    assert!(
        matches!(response, StreamResponse::Appended { .. }),
        "{response:?}"
    );
}

fn append_inline(machine: &mut StreamStateMachine, payload: &[u8]) {
    let response = machine.apply(StreamCommand::Append {
        stream_id: stream("s"),
        content_type: Some(OCTET.to_owned()),
        payload: bytes::Bytes::copy_from_slice(payload),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 2,
        record_match: None,
    });
    assert!(
        matches!(response, StreamResponse::Appended { .. }),
        "{response:?}"
    );
}

fn object(start: u64, end: u64, path: &str) -> ObjectPayloadRef {
    ObjectPayloadRef {
        start_offset: start,
        end_offset: end,
        s3_path: path.to_owned(),
        object_size: end - start,
        object_offset: 0,
    }
}

fn offload(machine: &mut StreamStateMachine, refs: Vec<ObjectPayloadRef>) -> StreamResponse {
    machine.apply(StreamCommand::OffloadColdRefs {
        stream_id: stream("s"),
        refs,
    })
}

#[test]
fn external_append_keeps_its_locator_in_state() {
    let mut machine = fresh_machine();
    append_external(&mut machine, "s/external/a.bin", 10);
    assert_eq!(machine.external_segments(&stream("s")), &[object(
        0,
        10,
        "s/external/a.bin"
    )]);
    // Reads serve the committed locator directly, without a page lookup.
    let plan = machine.read_plan(&stream("s"), 0, 64).expect("read plan");
    assert!(
        matches!(plan.segments.as_slice(), [StreamReadSegment::Object(segment)]
            if segment.object.s3_path == "s/external/a.bin"),
        "{plan:?}"
    );
}

#[test]
fn rejected_external_append_leaves_no_locator() {
    let mut machine = fresh_machine();
    // A record match that does not hold rejects the append on every replica.
    let response = append_external_with_match(&mut machine, "s/external/lost.bin", 10, Some(7));
    assert!(
        matches!(response, StreamResponse::Error { .. }),
        "{response:?}"
    );
    assert!(machine.external_segments(&stream("s")).is_empty());
    assert_eq!(machine.head(&stream("s")).map(|h| h.tail_offset), Some(0));
}

#[test]
fn offload_removes_exactly_the_listed_refs_and_is_idempotent() {
    let mut machine = fresh_machine();
    append_external(&mut machine, "s/external/a.bin", 10);
    append_inline(&mut machine, b"hot");
    append_external(&mut machine, "s/external/b.bin", 5);
    let a = object(0, 10, "s/external/a.bin");
    let b = object(13, 18, "s/external/b.bin");
    assert_eq!(machine.external_segments(&stream("s")), &[
        a.clone(),
        b.clone()
    ]);
    let gc_before = machine.pending_cold_gc_len();

    // A ref that differs in any field is not removed.
    let mut wrong = a.clone();
    wrong.object_size += 1;
    assert_eq!(
        offload(&mut machine, vec![wrong]),
        StreamResponse::ColdRefsOffloaded {
            removed: 0,
            remaining: 2,
        }
    );
    assert_eq!(
        offload(&mut machine, vec![a.clone()]),
        StreamResponse::ColdRefsOffloaded {
            removed: 1,
            remaining: 1,
        }
    );
    // Replay of the same command removes nothing more.
    assert_eq!(
        offload(&mut machine, vec![a.clone()]),
        StreamResponse::ColdRefsOffloaded {
            removed: 0,
            remaining: 1,
        }
    );
    assert_eq!(machine.external_segments(&stream("s")), &[b]);
    assert_eq!(
        machine.pending_cold_gc_len(),
        gc_before,
        "pages reference an offloaded object; nothing is queued for GC"
    );
    // The offloaded range is never hot, so it is planned from the cold index.
    let plan = machine.read_plan(&stream("s"), 0, 10).expect("read plan");
    assert!(
        matches!(plan.segments.as_slice(), [StreamReadSegment::ColdIndex(_)]),
        "{plan:?}"
    );
    let plan = machine
        .read_plan(&stream("s"), 10, 3)
        .expect("hot read plan");
    assert!(
        matches!(plan.segments.as_slice(), [StreamReadSegment::Hot(bytes)] if bytes == b"hot"),
        "{plan:?}"
    );
}

#[test]
fn offload_is_refused_for_missing_streams() {
    let mut machine = fresh_machine();
    let response = machine.apply(StreamCommand::OffloadColdRefs {
        stream_id: stream("missing"),
        refs: Vec::new(),
    });
    assert!(
        matches!(response, StreamResponse::Error {
            code: StreamErrorCode::StreamNotFound,
            ..
        }),
        "{response:?}"
    );
}

#[test]
fn candidates_follow_the_count_bound_and_the_due_predicate() {
    let never_due = |_: &ObjectPayloadRef| false;
    let mut machine = fresh_machine();
    for index in 0..MAX_STAGED_EXTERNAL_REFS {
        append_external(&mut machine, &format!("s/external/{index:02}.bin"), 4);
    }
    assert!(
        machine
            .staged_external_ref_candidates(MAX_STAGED_EXTERNAL_REFS, &never_due, 8)
            .is_empty(),
        "at most T_ext fresh refs stay staged"
    );
    let due = machine.staged_external_ref_candidates(
        MAX_STAGED_EXTERNAL_REFS,
        &|object| object.s3_path.ends_with("03.bin"),
        8,
    );
    assert_eq!(due.len(), 1, "one due ref offloads the stream's refs");
    assert_eq!(due[0].refs.len(), MAX_STAGED_EXTERNAL_REFS);

    append_external(&mut machine, "s/external/16.bin", 4);
    let over = machine.staged_external_ref_candidates(MAX_STAGED_EXTERNAL_REFS, &never_due, 8);
    assert_eq!(over.len(), 1, "more than T_ext refs are offloaded at once");
    let refs = over[0].refs.clone();
    assert!(
        refs.windows(2)
            .all(|pair| pair[0].start_offset < pair[1].start_offset)
    );
    assert_eq!(
        over[0].cold_generation,
        machine.cold_index_generation(&stream("s")).unwrap_or(0)
    );
    assert_eq!(
        offload(&mut machine, refs),
        StreamResponse::ColdRefsOffloaded {
            removed: 17,
            remaining: 0,
        }
    );
    assert!(
        machine
            .staged_external_ref_candidates(0, &|_| true, 8)
            .is_empty()
    );
}

#[test]
fn delete_queues_state_held_external_refs_for_gc() {
    let mut machine = fresh_machine();
    append_external(&mut machine, "s/external/a.bin", 10);
    let response = machine.apply(StreamCommand::DeleteStream {
        stream_id: stream("s"),
    });
    assert!(matches!(response, StreamResponse::Deleted), "{response:?}");
    assert!(
        machine
            .pending_cold_gc_batch(16)
            .iter()
            .any(|entry| matches!(
                &entry.target,
                ColdGcTarget::Paths(paths) if paths.iter().any(|path| path == "s/external/a.bin")
            )),
        "a staged ref that was never offloaded is reclaimed with its stream"
    );
}

/// A snapshot holding staged locators restores to the same state, and the
/// restored machine still offloads them.
#[test]
fn snapshot_with_staged_locators_round_trips() {
    let mut machine = fresh_machine();
    append_inline(&mut machine, b"abcd");
    append_external(&mut machine, "s/external/a.bin", 10);
    append_external(&mut machine, "s/external/b.bin", 6);
    let snapshot = machine.snapshot();
    let mut restored = StreamStateMachine::restore(snapshot.clone()).expect("restore");
    assert_eq!(restored.snapshot(), snapshot);
    assert_eq!(restored.external_segments(&stream("s")), &[
        object(4, 14, "s/external/a.bin"),
        object(14, 20, "s/external/b.bin"),
    ]);
    let response = offload(&mut restored, vec![object(4, 14, "s/external/a.bin")]);
    assert!(
        matches!(response, StreamResponse::ColdRefsOffloaded {
            removed: 1,
            remaining: 1
        }),
        "{response:?}"
    );
}
