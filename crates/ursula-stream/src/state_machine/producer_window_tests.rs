//! F3 producer bounds, F4a message-record collapse and F0 `TidyStream` at
//! feature level 1 (`docs/architecture/bounded-stream-state.md` §5.1, §5.4,
//! §5.5).

use super::producers::PRODUCER_IDLE_EXPIRY_MS;
use super::producers::RECEIPT_TRIM_BUDGET;
use super::producers::RECEIPT_WINDOW_ITEMS;
use super::*;

const OCTET: &str = "application/octet-stream";
const JSON: &str = "application/json";
const BUCKET: &str = "window";
const R: u64 = RECEIPT_WINDOW_ITEMS;

fn machine_at(level: u32) -> StreamStateMachine {
    let mut machine = StreamStateMachine::new();
    machine.apply(StreamCommand::SetFeatureLevel { level });
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
            attrs: None,
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
        let mut machine = machine_at(1);
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
fn level_zero_keeps_every_receipt_and_answers_409_shape_unchanged() {
    let mut machine = machine_at(0);
    let stream_id = create(&mut machine, "legacy", OCTET);
    for seq in 0..(R + 10) {
        appended(append(&mut machine, &stream_id, "p", seq, 1));
    }
    assert_eq!(receipts(&machine, &stream_id, "p").len() as u64, R + 10);
    let state = machine
        .stream_slot(&stream_id)
        .unwrap()
        .producers
        .get("p")
        .unwrap();
    assert_eq!(state.last_items.len(), 1, "level 0 keeps last_items");
    assert_eq!(state.last_seen_ms, None, "level 0 stamps nothing new");
}

#[test]
fn interleaved_producers_evict_in_commit_order_and_keep_each_newest() {
    let mut machine = machine_at(1);
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
    let mut machine = machine_at(1);
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
    let mut machine = machine_at(1);
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

    // A batch duplicate beyond the window carries no per-frame ranges.
    let batch = machine
        .append_batch_borrowed(
            stream_id.clone(),
            Some(OCTET),
            &[b"x"],
            Some(producer("p", 1)),
            2,
        )
        .unwrap();
    assert!(batch.deduplicated);
    assert!(batch.receipt_evicted);
    assert!(batch.items.is_empty());
    assert_eq!(machine.head(&stream_id).unwrap().tail_offset, tail);

    // Retrying the newest sequence still answers with its exact ranges.
    let (offset, next, deduplicated, evicted) =
        appended(append(&mut machine, &stream_id, "p", R + 4, 2));
    assert!(deduplicated);
    assert!(!evicted);
    assert_eq!((offset, next), (tail - 4, tail));
}

#[test]
fn batch_receipts_count_one_item_per_frame() {
    let mut machine = machine_at(1);
    let stream_id = create(&mut machine, "batch", OCTET);
    let frames: Vec<&[u8]> = vec![b"a"; 100];
    for seq in 0..20 {
        machine
            .append_batch_borrowed(
                stream_id.clone(),
                Some(OCTET),
                &frames,
                Some(producer("p", seq)),
                1,
            )
            .unwrap();
    }
    // 20 batches of 100 items: the window keeps the newest receipt plus
    // whole receipts while the total stays within R.
    let held = receipts(&machine, &stream_id, "p");
    assert_eq!(held.len(), 10);
    assert_eq!(window_items(&machine, &stream_id), 1_000);
    // A duplicate of a retained batch gets per-frame ranges.
    let batch = machine
        .append_batch_borrowed(
            stream_id.clone(),
            Some(OCTET),
            &frames,
            Some(producer("p", 15)),
            1,
        )
        .unwrap();
    assert!(batch.deduplicated && !batch.receipt_evicted);
    assert_eq!(batch.items.len(), 100);
}

#[test]
fn failed_transaction_evicts_nothing_from_any_producer() {
    let mut machine = machine_at(1);
    let a = BucketStreamId::with_affinity(BUCKET, "h", "a");
    let b = BucketStreamId::with_affinity(BUCKET, "h", "b");
    for stream_id in [&a, &b] {
        assert!(matches!(
            machine.apply(StreamCommand::CreateStream {
                stream_id: stream_id.clone(),
                content_type: OCTET.to_owned(),
                initial_payload: Bytes::new(),
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
    for seq in 0..R {
        appended(append(&mut machine, &a, "p", seq, 1));
    }
    let before = producer_snapshot(&machine);
    let ok = StreamCommand::Append {
        stream_id: a.clone(),
        content_type: Some(OCTET.to_owned()),
        payload: Bytes::from_static(b"x"),
        close_after: false,
        stream_seq: None,
        producer: Some(producer("p", R)),
        now_ms: 2,
        record_match: None,
    };
    let bad = StreamCommand::Append {
        stream_id: b.clone(),
        content_type: Some("text/plain".to_owned()),
        payload: Bytes::from_static(b"x"),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 2,
        record_match: None,
    };
    assert!(machine.append_transaction(vec![ok.clone(), bad]).is_err());
    assert_eq!(producer_snapshot(&machine), before);
    assert_eq!(window_items(&machine, &a), R);

    // The same append committed alone evicts exactly one receipt.
    assert!(machine.append_transaction(vec![ok]).is_ok());
    assert_eq!(receipts(&machine, &a, "p").first().copied(), Some(1));
    assert_eq!(window_items(&machine, &a), R);
}

#[test]
fn idle_producer_expires_at_its_own_next_write() {
    let mut machine = machine_at(1);
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
    assert_eq!(state.last_seen_ms, Some(idle_at));
    assert!(state.last_items.is_empty());
}

#[test]
fn snapshot_round_trip_and_restore_versus_live_differential() {
    let mut live = machine_at(1);
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
fn legacy_snapshot_without_last_seen_restores_unstamped() {
    let mut machine = machine_at(1);
    let stream_id = create(&mut machine, "legacy-field", OCTET);
    appended(append(&mut machine, &stream_id, "p", 0, 5));
    let mut value = serde_json::to_value(machine.snapshot()).unwrap();
    value["streams"][0]["producer_states"][0]
        .as_object_mut()
        .unwrap()
        .remove("last_seen_ms");
    let legacy: StreamSnapshot = serde_json::from_value(value).unwrap();
    assert_eq!(legacy.streams[0].producer_states[0].last_seen_ms, None);
    let restored = StreamStateMachine::restore(legacy).unwrap();
    assert_eq!(
        restored
            .stream_slot(&stream_id)
            .unwrap()
            .producers
            .get("p")
            .unwrap()
            .last_seen_ms,
        None
    );
    // Its idle period starts at the first tidy.
    assert!(restored.stream_has_tidy_debt(&stream_id, 6));
}

#[test]
fn level_one_restore_does_not_synthesize_receipts() {
    let mut machine = machine_at(1);
    let stream_id = create(&mut machine, "synth", OCTET);
    appended(append(&mut machine, &stream_id, "p", 0, 5));
    let mut snapshot = machine.snapshot();
    snapshot.streams[0].producer_states[0].receipts.clear();
    let restored = StreamStateMachine::restore(snapshot.clone()).unwrap();
    assert!(receipts(&restored, &stream_id, "p").is_empty());
    snapshot.feature_level = 0;
    let restored = StreamStateMachine::restore(snapshot).unwrap();
    assert_eq!(receipts(&restored, &stream_id, "p"), vec![0]);
}

#[test]
fn tidy_stream_requires_level_one_and_is_idempotent() {
    let mut machine = machine_at(0);
    let stream_id = create(&mut machine, "tidy-gate", OCTET);
    assert!(matches!(
        machine.apply(StreamCommand::TidyStream {
            stream_id: stream_id.clone(),
            now_ms: 1,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::FeatureNotEnabled,
            ..
        }
    ));
    assert!(machine.tidy_candidates(1, 64).is_empty());

    let mut machine = machine_at(1);
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
fn tidy_stream_stamps_and_expires_pre_raise_producers() {
    let mut machine = machine_at(0);
    let stream_id = create(&mut machine, "raise", OCTET);
    appended(append(&mut machine, &stream_id, "p", 0, 1));
    machine.apply(StreamCommand::SetFeatureLevel { level: 1 });
    assert_eq!(machine.tidy_candidates(2, 64), vec![stream_id.clone()]);
    assert_eq!(
        machine.apply(StreamCommand::TidyStream {
            stream_id: stream_id.clone(),
            now_ms: 2,
        }),
        StreamResponse::StreamTidied {
            debt_remaining: false
        }
    );
    let state = machine
        .stream_slot(&stream_id)
        .unwrap()
        .producers
        .get("p")
        .unwrap()
        .clone();
    assert_eq!(state.last_seen_ms, Some(2));
    assert!(state.last_items.is_empty());
    assert!(machine.tidy_candidates(3, 64).is_empty());
    // Idle from the stamp on.
    let idle_at = 2 + PRODUCER_IDLE_EXPIRY_MS;
    assert_eq!(machine.tidy_candidates(idle_at, 64), vec![
        stream_id.clone()
    ]);
    machine.apply(StreamCommand::TidyStream {
        stream_id: stream_id.clone(),
        now_ms: idle_at,
    });
    assert!(
        machine
            .stream_slot(&stream_id)
            .unwrap()
            .producers
            .is_empty()
    );
    assert_eq!(window_items(&machine, &stream_id), 0);
}

/// A legacy producer with 1M receipts converges through bounded
/// `TidyStream` commands (§8 B3 exit: under 10 ms of apply each).
#[test]
fn legacy_producer_with_a_million_receipts_converges_in_bounded_commands() {
    const LEGACY: u64 = 1_000_000;
    let mut machine = machine_at(0);
    let stream_id = create(&mut machine, "million", OCTET);
    appended(append(&mut machine, &stream_id, "p", 0, 1));
    let mut snapshot = machine.snapshot();
    let state = &mut snapshot.streams[0].producer_states[0];
    let template = state.receipts[0].clone();
    state.receipts = (0..LEGACY)
        .map(|seq| ProducerReceipt {
            producer_seq: seq,
            ..template.clone()
        })
        .collect();
    state.producer_seq = LEGACY - 1;
    let mut machine = StreamStateMachine::restore(snapshot).unwrap();
    assert_eq!(window_items(&machine, &stream_id), LEGACY);
    machine.apply(StreamCommand::SetFeatureLevel { level: 1 });

    let mut commands = 0;
    let mut slowest = std::time::Duration::ZERO;
    loop {
        let before = receipts(&machine, &stream_id, "p").len();
        let started = std::time::Instant::now();
        let response = machine.apply(StreamCommand::TidyStream {
            stream_id: stream_id.clone(),
            now_ms: 2,
        });
        slowest = slowest.max(started.elapsed());
        commands += 1;
        let after = receipts(&machine, &stream_id, "p").len();
        assert!(before - after <= RECEIPT_TRIM_BUDGET);
        if response
            == (StreamResponse::StreamTidied {
                debt_remaining: false,
            })
        {
            break;
        }
        assert!(commands < 32, "tidy did not converge");
    }
    assert_eq!(
        commands,
        (LEGACY - R).div_ceil(RECEIPT_TRIM_BUDGET as u64) as usize
    );
    assert_eq!(window_items(&machine, &stream_id), R);
    assert_eq!(
        receipts(&machine, &stream_id, "p").first().copied(),
        Some(LEGACY - R)
    );
    // The newest sequence is still answered exactly; an evicted one without
    // ranges, and the lookup does not scan.
    assert!(!appended(append(&mut machine, &stream_id, "p", LEGACY - 1, 3)).3);
    assert!(appended(append(&mut machine, &stream_id, "p", 5, 3)).3);
    if !cfg!(debug_assertions) {
        assert!(
            slowest < std::time::Duration::from_millis(10),
            "slowest tidy apply took {slowest:?}"
        );
    }
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

fn external(
    machine: &mut StreamStateMachine,
    stream_id: &BucketStreamId,
    path: &str,
    len: u64,
    record_ends: Vec<u64>,
) {
    let response = machine.apply(StreamCommand::AppendExternal {
        stream_id: stream_id.clone(),
        content_type: Some(JSON.to_owned()),
        payload: ExternalPayloadRef {
            s3_path: path.to_owned(),
            payload_len: len,
            object_size: len,
        },
        record_ends,
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 1,
        record_match: None,
    });
    assert!(
        matches!(response, StreamResponse::Appended { .. }),
        "{response:?}"
    );
}

#[test]
fn external_appends_keep_at_most_two_message_records_at_level_one() {
    for (level, expected) in [(0usize, 50usize), (1, 1)] {
        let mut machine = machine_at(level as u32);
        let stream_id = create(&mut machine, "w3", JSON);
        for index in 0..10 {
            external(&mut machine, &stream_id, &format!("ext/{index}"), 10, vec![
                2, 4, 6, 8, 10,
            ]);
        }
        let slot = machine.stream_slot(&stream_id).unwrap();
        assert_eq!(slot.message_records.len(), expected, "level {level}");
    }
    // Interleaved hot bytes: records above the first hot byte stay exact.
    let mut machine = machine_at(1);
    let stream_id = create(&mut machine, "w3-inline", JSON);
    external(&mut machine, &stream_id, "ext/a", 10, vec![5, 10]);
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream_id.clone(),
            content_type: Some(JSON.to_owned()),
            payload: Bytes::from_static(b"1\n2\n"),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 1,
            record_match: None,
        }),
        StreamResponse::Appended { .. }
    ));
    external(&mut machine, &stream_id, "ext/b", 10, vec![5, 10]);
    let records = &machine.stream_slot(&stream_id).unwrap().message_records;
    assert_eq!(
        records
            .iter()
            .map(|record| (record.start_offset, record.end_offset))
            .collect::<Vec<_>>(),
        vec![(0, 10), (10, 12), (12, 14), (14, 19), (19, 24)]
    );
    let plan = machine.bootstrap_plan(&stream_id).unwrap();
    assert!(
        !plan.up_to_date,
        "bootstrap from 0 below the seal point is partial"
    );
    assert!(plan.updates.is_empty());
    assert_eq!(plan.next_offset, 0);
}

#[test]
fn bootstrap_stays_honest_after_external_collapse() {
    let mut machine = machine_at(1);
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
    // A snapshot inside the collapsed cold region is still accepted (F18),
    // and bootstrap from it is an honest partial: no update parts.
    assert!(matches!(
        machine.apply(StreamCommand::PublishSnapshot {
            stream_id: stream_id.clone(),
            snapshot_offset: 10,
            content_type: OCTET.to_owned(),
            payload: Bytes::from_static(b"s"),
            expected_digest: None,
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
            expected_digest: None,
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
fn bootstrap_is_partial_when_the_cold_frontier_regressed_below_the_seal_point() {
    // D1 shape: small hot bytes, an external append, then a flush of only
    // the hot prefix moves the scalar frontier back. Every byte below the
    // seal point is still cold, so bootstrap from inside it must be partial.
    let mut machine = machine_at(1);
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
            cold_generation: None,
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
            expected_digest: None,
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
fn tidy_stream_collapses_legacy_external_message_records() {
    let mut machine = machine_at(0);
    let stream_id = create(&mut machine, "tidy-w3", JSON);
    for index in 0..5 {
        external(&mut machine, &stream_id, &format!("ext/{index}"), 10, vec![
            5, 10,
        ]);
    }
    assert_eq!(
        machine
            .stream_slot(&stream_id)
            .unwrap()
            .message_records
            .len(),
        10
    );
    machine.apply(StreamCommand::SetFeatureLevel { level: 1 });
    assert_eq!(machine.tidy_candidates(1, 64), vec![stream_id.clone()]);
    assert_eq!(
        machine.apply(StreamCommand::TidyStream {
            stream_id: stream_id.clone(),
            now_ms: 1,
        }),
        StreamResponse::StreamTidied {
            debt_remaining: false
        }
    );
    let records = &machine.stream_slot(&stream_id).unwrap().message_records;
    assert_eq!(records.len(), 1);
    assert_eq!((records[0].start_offset, records[0].end_offset), (0, 50));
    assert_eq!(records.capacity(), 1);
    // Restore accepts the collapsed coverage.
    StreamStateMachine::restore(machine.snapshot()).unwrap();
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
    let mut machine = machine_at(1);
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

#[test]
fn producer_cap_does_not_apply_below_level_one() {
    use super::producers::MAX_PRODUCERS_PER_STREAM;
    let mut machine = machine_at(0);
    let stream_id = create(&mut machine, "cap-l0", OCTET);
    for index in 0..=MAX_PRODUCERS_PER_STREAM {
        appended(append(
            &mut machine,
            &stream_id,
            &format!("p{index:04}"),
            0,
            0,
        ));
    }
    assert_eq!(
        machine.stream_slot(&stream_id).unwrap().producers.len(),
        MAX_PRODUCERS_PER_STREAM + 1
    );
}
