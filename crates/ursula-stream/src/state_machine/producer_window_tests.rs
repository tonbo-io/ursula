//! F3 producer bounds and `TidyStream` (`docs/architecture/bounded-stream-state.md` §5.1, §5.4,
//! §5.5).

use bytes::Bytes;

use super::producers::PRODUCER_IDLE_EXPIRY_MS;
use super::producers::RECEIPT_WINDOW_ITEMS;
use super::*;

const OCTET: &str = "application/octet-stream";
const BUCKET: &str = "window";
const R: u64 = RECEIPT_WINDOW_ITEMS;

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

fn sid(name: &str) -> BucketStreamId {
    BucketStreamId::new(BUCKET, name)
}

fn create(machine: &mut StreamStateMachine, name: &str, content_type: &str) -> BucketStreamId {
    let stream_id = sid(name);
    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: content_type.to_owned(),
            initial_payload: Bytes::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            now_ms: 0,
        }),
        StreamResponse::Created { .. }
    ));
    stream_id
}

fn producer(id: &str, seq: u64) -> ProducerRequest {
    ProducerRequest {
        producer_id: id.to_owned(),
        producer_epoch: 1,
        producer_seq: seq,
    }
}

fn append(
    machine: &mut StreamStateMachine,
    stream_id: &BucketStreamId,
    producer_id: &str,
    seq: u64,
    now_ms: u64,
) -> StreamResponse {
    machine.apply(StreamCommand::Append {
        stream_id: stream_id.clone(),
        content_type: Some(OCTET.to_owned()),
        payload: Bytes::from_static(b"abcd"),
        close_after: false,
        stream_seq: None,
        producer: Some(producer(producer_id, seq)),
        now_ms,
        record_match: None,
    })
}

fn appended(response: StreamResponse) -> (u64, u64, bool, bool) {
    match response {
        StreamResponse::Appended {
            offset,
            next_offset,
            deduplicated,
            receipt_evicted,
            ..
        } => (offset, next_offset, deduplicated, receipt_evicted),
        other => panic!("expected Appended, got {other:?}"),
    }
}

fn receipts(
    machine: &StreamStateMachine,
    stream_id: &BucketStreamId,
    producer_id: &str,
) -> Vec<u64> {
    machine
        .stream_slot(stream_id)
        .unwrap()
        .producers
        .get(producer_id)
        .map(|state| state.receipts.iter().map(|r| r.producer_seq).collect())
        .unwrap_or_default()
}

fn window_items(machine: &StreamStateMachine, stream_id: &BucketStreamId) -> u64 {
    let slot = machine.stream_slot(stream_id).unwrap();
    let derived = slot.receipt_window.items();
    let counted = slot
        .producers
        .values()
        .flat_map(|state| state.receipts.iter())
        .map(super::producers::receipt_items)
        .sum::<u64>();
    assert_eq!(derived, counted, "derived window item count drifted");
    derived
}

fn producer_snapshot(machine: &StreamStateMachine) -> Vec<Vec<ProducerSnapshot>> {
    machine
        .snapshot()
        .streams
        .into_iter()
        .map(|entry| entry.producer_states)
        .collect()
}

#[test]
fn window_edges_at_r_minus_one_r_and_r_plus_one() {
    for (appends, expected_front) in [(R - 1, 0), (R, 0), (R + 1, 1)] {
        let mut machine = fresh_machine();
        let stream_id = create(&mut machine, "edges", OCTET);
        for seq in 0..appends {
            appended(append(&mut machine, &stream_id, "p", seq, 1));
        }
        let held = receipts(&machine, &stream_id, "p");
        assert_eq!(held.len() as u64, appends.min(R), "appends {appends}");
        assert_eq!(
            held.first().copied(),
            Some(expected_front),
            "appends {appends}"
        );
        assert!(window_items(&machine, &stream_id) <= R);
    }
}

#[test]
fn interleaved_producers_evict_in_commit_order_and_keep_each_newest() {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "interleaved", OCTET);
    // One early write from "quiet", then R + 100 writes from "busy".
    let (quiet_offset, quiet_next, _, _) =
        appended(append(&mut machine, &stream_id, "quiet", 0, 1));
    for seq in 0..(R + 100) {
        appended(append(&mut machine, &stream_id, "busy", seq, 2));
    }
    // Quiet's only (newest) receipt is never evicted.
    assert_eq!(receipts(&machine, &stream_id, "quiet"), vec![0]);
    assert!(window_items(&machine, &stream_id) <= R + 1);
    let busy = receipts(&machine, &stream_id, "busy");
    assert_eq!(busy.len() as u64, R - 1);
    assert_eq!(busy.first().copied(), Some(101));

    // Quiet retries its newest sequence after others filled the window and
    // gets its exact ranges.
    let tail = machine.head(&stream_id).unwrap().tail_offset;
    let (offset, next, deduplicated, evicted) =
        appended(append(&mut machine, &stream_id, "quiet", 0, 3));
    assert_eq!((offset, next), (quiet_offset, quiet_next));
    assert!(deduplicated);
    assert!(!evicted);
    assert_eq!(machine.head(&stream_id).unwrap().tail_offset, tail);

    // Interleaving: alternate two producers; eviction follows commit order,
    // so both lose their oldest receipts at the same pace.
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "alternate", OCTET);
    for seq in 0..R {
        appended(append(&mut machine, &stream_id, "a", seq, 1));
        appended(append(&mut machine, &stream_id, "b", seq, 1));
    }
    let a = receipts(&machine, &stream_id, "a");
    let b = receipts(&machine, &stream_id, "b");
    assert_eq!(a.len() + b.len(), R as usize);
    assert_eq!(a.first().copied(), Some(R / 2));
    assert_eq!(b.first().copied(), Some(R / 2));
}

#[test]
fn duplicate_beyond_window_is_deduplicated_without_ranges_and_never_appends() {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "beyond", OCTET);
    for seq in 0..(R + 5) {
        appended(append(&mut machine, &stream_id, "p", seq, 1));
    }
    let tail = machine.head(&stream_id).unwrap().tail_offset;
    let (offset, next, deduplicated, evicted) =
        appended(append(&mut machine, &stream_id, "p", 0, 2));
    assert!(deduplicated);
    assert!(evicted);
    assert_eq!((offset, next), (tail, tail));
    assert_eq!(machine.head(&stream_id).unwrap().tail_offset, tail);
    assert!(machine.append_would_deduplicate(&stream_id, Some(&producer("p", 0)), 2));

    // Retrying the newest sequence still answers with its exact ranges.
    let (offset, next, deduplicated, evicted) =
        appended(append(&mut machine, &stream_id, "p", R + 4, 2));
    assert!(deduplicated);
    assert!(!evicted);
    assert_eq!((offset, next), (tail - 4, tail));
}

#[test]
fn idle_producer_expires_at_its_own_next_write() {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "idle", OCTET);
    let t0 = 1_000;
    for seq in 0..3 {
        appended(append(&mut machine, &stream_id, "p", seq, t0));
    }
    // Just before the idle period: still a duplicate.
    let just_before = t0 + PRODUCER_IDLE_EXPIRY_MS - 1;
    assert!(appended(append(&mut machine, &stream_id, "p", 0, just_before)).2);
    // At the idle period a later sequence is rejected (expected 0) and
    // cannot double-write.
    let idle_at = t0 + PRODUCER_IDLE_EXPIRY_MS;
    let tail = machine.head(&stream_id).unwrap().tail_offset;
    assert!(matches!(
        append(&mut machine, &stream_id, "p", 3, idle_at),
        StreamResponse::Error {
            code: StreamErrorCode::ProducerSeqConflict,
            ..
        }
    ));
    assert_eq!(machine.head(&stream_id).unwrap().tail_offset, tail);
    assert!(
        machine
            .stream_slot(&stream_id)
            .unwrap()
            .producers
            .is_empty()
    );
    // Sequence 0 is accepted as a new producer.
    let (_, _, deduplicated, _) = appended(append(&mut machine, &stream_id, "p", 0, idle_at));
    assert!(!deduplicated);
    assert_eq!(receipts(&machine, &stream_id, "p"), vec![0]);
    assert_eq!(window_items(&machine, &stream_id), 1);
    let state = machine
        .stream_slot(&stream_id)
        .unwrap()
        .producers
        .get("p")
        .unwrap()
        .clone();
    assert_eq!(state.last_seen_ms, idle_at);
}

#[test]
fn snapshot_round_trip_and_restore_versus_live_differential() {
    let mut live = fresh_machine();
    let stream_id = create(&mut live, "diff", OCTET);
    // A producer that is fully evicted down to its newest receipt, another
    // with a full window, and a batch producer.
    appended(append(&mut live, &stream_id, "old", 0, 10));
    appended(append(&mut live, &stream_id, "old", 1, 10));
    for seq in 0..(R + 50) {
        appended(append(&mut live, &stream_id, "busy", seq, 20));
    }
    assert_eq!(receipts(&live, &stream_id, "old"), vec![1]);

    let snapshot = live.snapshot();
    let encoded = serde_json::to_vec(&snapshot).unwrap();
    let decoded: StreamSnapshot = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, snapshot);
    let mut restored = StreamStateMachine::restore(decoded).unwrap();
    assert_eq!(restored.snapshot(), snapshot);
    assert_eq!(
        restored.stream_slot(&stream_id).unwrap().receipt_window,
        live.stream_slot(&stream_id).unwrap().receipt_window
    );

    // Identical commands keep both replicas identical.
    let mut ops = Vec::new();
    for seq in (R + 50)..(R + 400) {
        ops.push(("busy", seq, 30));
        if seq % 7 == 0 {
            ops.push(("other", (seq - R - 50) / 7, 30));
        }
    }
    ops.push(("old", 2, 40 + PRODUCER_IDLE_EXPIRY_MS));
    ops.push(("old", 0, 40 + PRODUCER_IDLE_EXPIRY_MS));
    for (id, seq, now) in ops {
        let left = append(&mut live, &stream_id, id, seq, now);
        let right = append(&mut restored, &stream_id, id, seq, now);
        assert_eq!(left, right, "{id} {seq}");
    }
    assert_eq!(live.snapshot(), restored.snapshot());
    for machine in [&live, &restored] {
        assert!(window_items(machine, &stream_id) <= R + 3);
    }
}

#[test]
fn tidy_stream_is_idempotent() {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "tidy", OCTET);
    assert_eq!(
        machine.apply(StreamCommand::TidyStream {
            stream_id: stream_id.clone(),
            now_ms: 1,
        }),
        StreamResponse::StreamTidied {
            debt_remaining: false
        }
    );
    let before = machine.snapshot();
    assert_eq!(
        machine.apply(StreamCommand::TidyStream {
            stream_id: stream_id.clone(),
            now_ms: 1,
        }),
        StreamResponse::StreamTidied {
            debt_remaining: false
        }
    );
    assert_eq!(machine.snapshot(), before);
    assert!(matches!(
        machine.apply(StreamCommand::TidyStream {
            stream_id: sid("missing"),
            now_ms: 1,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamNotFound,
            ..
        }
    ));
}

#[test]
fn duplicate_lookup_is_direct_at_a_million_receipts() {
    let receipts = (0..1_000_000u64)
        .map(|seq| ProducerReceipt {
            producer_seq: seq + 7,
            start_offset: seq,
            next_offset: seq + 1,
            closed: false,
            items: Vec::new(),
        })
        .collect::<std::collections::VecDeque<_>>();
    let started = std::time::Instant::now();
    for seq in [7, 500_007, 1_000_006] {
        assert_eq!(
            super::append::find_receipt(&receipts, seq).map(|r| r.producer_seq),
            Some(seq)
        );
    }
    assert!(super::append::find_receipt(&receipts, 6).is_none());
    assert!(super::append::find_receipt(&receipts, 1_000_007).is_none());
    assert!(started.elapsed() < std::time::Duration::from_millis(50));
}

#[test]
fn bootstrap_stays_honest_after_external_appends() {
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "boot", OCTET);
    let ext = |machine: &mut StreamStateMachine, path: &str| {
        let response = machine.apply(StreamCommand::AppendExternal {
            stream_id: stream_id.clone(),
            content_type: Some(OCTET.to_owned()),
            payload: ExternalPayloadRef {
                s3_path: path.to_owned(),
                payload_len: 10,
                object_size: 10,
            },
            record_ends: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 1,
            record_match: None,
        });
        assert!(matches!(response, StreamResponse::Appended { .. }));
    };
    ext(&mut machine, "a");
    ext(&mut machine, "b");
    ext(&mut machine, "c");
    // A snapshot inside the cold region is still accepted (F18),
    // and bootstrap from it is an honest partial: no update parts.
    assert!(matches!(
        machine.apply(StreamCommand::PublishSnapshot {
            stream_id: stream_id.clone(),
            snapshot_offset: 10,
            content_type: OCTET.to_owned(),
            payload: Bytes::from_static(b"s"),
            now_ms: 1,
        }),
        StreamResponse::SnapshotPublished { .. }
    ));
    let plan = machine.bootstrap_plan(&stream_id).unwrap();
    assert!(plan.updates.is_empty());
    assert_eq!(plan.next_offset, 10);
    assert!(!plan.up_to_date);
    // A snapshot at the tail is up to date with no parts.
    assert!(matches!(
        machine.apply(StreamCommand::PublishSnapshot {
            stream_id: stream_id.clone(),
            snapshot_offset: 30,
            content_type: OCTET.to_owned(),
            payload: Bytes::from_static(b"s"),
            now_ms: 1,
        }),
        StreamResponse::SnapshotPublished { .. }
    ));
    let plan = machine.bootstrap_plan(&stream_id).unwrap();
    assert!(plan.updates.is_empty());
    assert_eq!(plan.next_offset, 30);
    assert!(plan.up_to_date);
}

#[test]
fn bootstrap_is_partial_when_a_hot_prefix_flush_leaves_an_external_below_the_seal_point() {
    // D1 shape: small hot bytes, an external append, then a flush of only
    // the hot prefix. Every byte below the seal point is cold, so bootstrap
    // from inside it must be partial.
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "d1", OCTET);
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream_id.clone(),
            content_type: Some(OCTET.to_owned()),
            payload: Bytes::from_static(b"0123456789"),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 1,
            record_match: None,
        }),
        StreamResponse::Appended { .. }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::AppendExternal {
            stream_id: stream_id.clone(),
            content_type: Some(OCTET.to_owned()),
            payload: ExternalPayloadRef {
                s3_path: "big".to_owned(),
                payload_len: 100,
                object_size: 100,
            },
            record_ends: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 1,
            record_match: None,
        }),
        StreamResponse::Appended { .. }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::FlushCold {
            stream_id: stream_id.clone(),
            cold_generation: machine
                .cold_index_generation(&stream_id)
                .unwrap_or_default(),
            chunk: ColdChunkRef {
                start_offset: 0,
                end_offset: 10,
                s3_path: "chunk".to_owned(),
                object_size: 10,
                ..ColdChunkRef::default()
            },
        }),
        StreamResponse::ColdFlushed { .. }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::PublishSnapshot {
            stream_id: stream_id.clone(),
            snapshot_offset: 10,
            content_type: OCTET.to_owned(),
            payload: Bytes::from_static(b"s"),
            now_ms: 1,
        }),
        StreamResponse::SnapshotPublished { .. }
    ));
    let plan = machine.bootstrap_plan(&stream_id).unwrap();
    assert!(plan.updates.is_empty(), "{plan:?}");
    assert_eq!(plan.next_offset, 10);
    assert!(!plan.up_to_date);
}

#[test]
fn tidy_stream_command_round_trips_through_serde() {
    let command = StreamCommand::TidyStream {
        stream_id: sid("serde"),
        now_ms: 9,
    };
    let encoded = serde_json::to_vec(&command).unwrap();
    assert_eq!(
        serde_json::from_slice::<StreamCommand>(&encoded).unwrap(),
        command
    );
    assert_eq!(command.to_string(), "tidy_stream:window/serde");
}

#[test]
fn producer_cap_evicts_hour_idle_producers_and_rejects_otherwise() {
    use super::producers::MAX_PRODUCERS_PER_STREAM;
    use super::producers::PRODUCER_CAP_EVICT_IDLE_MS;
    let mut machine = fresh_machine();
    let stream_id = create(&mut machine, "cap", OCTET);
    // Producer `p0000` writes first, so it is the least recently seen.
    for index in 0..MAX_PRODUCERS_PER_STREAM {
        let now_ms = u64::try_from(index).unwrap();
        appended(append(
            &mut machine,
            &stream_id,
            &format!("p{index:04}"),
            0,
            now_ms,
        ));
    }
    let count =
        |machine: &StreamStateMachine| machine.stream_slot(&stream_id).unwrap().producers.len();
    assert_eq!(count(&machine), MAX_PRODUCERS_PER_STREAM);

    // Nobody has been idle for an hour: a new producer gets 429 producer_limit
    // and nothing is written.
    let tail_before = machine.stream_metadata(&stream_id).unwrap().tail_offset;
    match append(&mut machine, &stream_id, "new-a", 0, 10_000) {
        StreamResponse::Error { code, message, .. } => {
            assert_eq!(code, StreamErrorCode::ProducerLimit);
            assert!(message.starts_with("producer_limit"), "{message}");
        }
        other => panic!("expected ProducerLimit, got {other:?}"),
    }
    assert_eq!(
        machine.stream_metadata(&stream_id).unwrap().tail_offset,
        tail_before
    );
    assert_eq!(count(&machine), MAX_PRODUCERS_PER_STREAM);
    // Existing producers keep writing at the cap.
    appended(append(&mut machine, &stream_id, "p0001", 1, 10_000));

    // An hour after `p0000` and `p0002` last wrote they are evictable; the
    // least recently seen goes first and the cap holds.
    let later = PRODUCER_CAP_EVICT_IDLE_MS + 2;
    appended(append(&mut machine, &stream_id, "new-a", 0, later));
    assert_eq!(count(&machine), MAX_PRODUCERS_PER_STREAM);
    let slot = machine.stream_slot(&stream_id).unwrap();
    assert!(!slot.producers.contains_key("p0000"));
    assert!(
        slot.producers.contains_key("p0001"),
        "recently seen is kept"
    );
    assert!(slot.producers.contains_key("p0002"));
    assert!(slot.producers.contains_key("new-a"));
    appended(append(&mut machine, &stream_id, "new-b", 0, later));
    let slot = machine.stream_slot(&stream_id).unwrap();
    assert_eq!(slot.producers.len(), MAX_PRODUCERS_PER_STREAM);
    assert!(!slot.producers.contains_key("p0002"));
    window_items(&machine, &stream_id);

    // A replica that restores the snapshot holds the same producers.
    let restored = StreamStateMachine::restore(machine.snapshot()).unwrap();
    assert_eq!(producer_snapshot(&restored), producer_snapshot(&machine));
}
