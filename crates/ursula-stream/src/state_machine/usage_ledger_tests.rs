//! The per-stream usage ledger (U-A to U-C, bounded-state F20).

use proptest::collection::vec;
use proptest::prelude::*;

use super::*;
use crate::usage::StreamEnd;
use crate::usage::StreamUsageChange;
use crate::usage::UsageAck;
use crate::usage::UsageLedgerProgress;
use crate::usage::UsageLedgerSnapshot;

const OCTET: &str = "application/octet-stream";
const BUCKETS: [&str; 3] = ["bkt0", "bkt1", "bkt2"];

fn machine() -> StreamStateMachine {
    let mut machine = StreamStateMachine::new();
    for bucket_id in BUCKETS {
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: bucket_id.to_owned(),
        });
    }
    machine
}

fn id(bucket: &str, stream: &str) -> BucketStreamId {
    BucketStreamId::new(bucket, stream)
}

fn create(
    stream_id: &BucketStreamId,
    body: &[u8],
    ttl_seconds: Option<u64>,
    now_ms: u64,
) -> StreamCommand {
    StreamCommand::CreateStream {
        stream_id: stream_id.clone(),
        content_type: OCTET.to_owned(),
        initial_payload: bytes::Bytes::copy_from_slice(body),
        close_after: false,
        stream_seq: None,
        stream_ttl_seconds: ttl_seconds,
        stream_expires_at_ms: None,
        now_ms,
    }
}

fn append(stream_id: &BucketStreamId, body: &[u8], now_ms: u64) -> StreamCommand {
    StreamCommand::Append {
        stream_id: stream_id.clone(),
        content_type: Some(OCTET.to_owned()),
        payload: bytes::Bytes::copy_from_slice(body),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms,
    }
}

fn delete(stream_id: &BucketStreamId, now_ms: u64) -> StreamCommand {
    StreamCommand::DeleteStream {
        stream_id: stream_id.clone(),
        now_ms,
    }
}

fn ack(machine: &mut StreamStateMachine, through_seq: u64) -> UsageAck {
    let history = machine.history_id();
    match machine.apply(StreamCommand::AckStreamUsage {
        history,
        through_seq,
    }) {
        StreamResponse::StreamUsageAcked { ack } => ack,
        other => panic!("not an acknowledgement: {other:?}"),
    }
}

fn changes(machine: &StreamStateMachine, after_seq: u64) -> Vec<StreamUsageChange> {
    machine.stream_usage_changes(after_seq, usize::MAX).changes
}

fn usage_seq(machine: &StreamStateMachine) -> u64 {
    match machine.usage_ledger_progress() {
        UsageLedgerProgress::NotStarted => 0,
        UsageLedgerProgress::Started { usage_seq, .. } => usage_seq,
    }
}

/// A group snapshot's round trip: the stream snapshot, then its ledger.
fn round_trip(machine: &StreamStateMachine) -> StreamStateMachine {
    let mut restored = StreamStateMachine::restore(machine.snapshot()).expect("restore");
    restored.restore_usage_ledger(machine.usage_ledger_snapshot());
    restored
}

#[test]
fn before_the_first_acknowledgement_nothing_is_kept_per_stream() {
    let mut machine = machine();
    let notes = id("bkt0", "notes");
    machine.apply(create(&notes, b"abc", None, 10));
    machine.apply(append(&notes, b"defg", 20));
    machine.apply(delete(&notes, 30));

    assert_eq!(
        machine.usage_ledger_progress(),
        UsageLedgerProgress::NotStarted
    );
    assert_eq!(
        machine.usage_ledger_snapshot(),
        UsageLedgerSnapshot::NotStarted
    );
    let export = machine.stream_usage_changes(0, 10);
    assert_eq!(export.through, 0);
    assert!(export.changes.is_empty());
    assert_eq!(machine.state_gauges().ended_stream_usage, 0);
    // The bucket keeps both writes, as Ursula 0.10.1 counts them.
    let report = machine.bucket_usage_report();
    assert_eq!(report.len(), 1);
    assert_eq!(report[0].usage.committed_append_bytes, 7);
    assert_eq!(report[0].usage.committed_write_units, 2);
    assert_eq!(report[0].usage.stream_count, 0);
}

#[test]
fn the_first_acknowledgement_starts_the_ledger_with_every_live_stream_in_order() {
    let mut machine = machine();
    for (bucket, stream) in [("bkt1", "z"), ("bkt0", "y"), ("bkt0", "x")] {
        machine.apply(create(&id(bucket, stream), b"before", None, 1));
    }
    let before = machine.bucket_usage_report();

    assert_eq!(ack(&mut machine, 0), UsageAck::Started { usage_seq: 3 });
    let listed = changes(&machine, 0)
        .into_iter()
        .map(|change| (change.stream_id, change.usage))
        .collect::<Vec<_>>();
    let zero = |usage_seq| crate::usage::StreamUsage {
        usage_seq,
        ..crate::usage::StreamUsage::default()
    };
    assert_eq!(listed, vec![
        (id("bkt0", "x"), zero(1)),
        (id("bkt0", "y"), zero(2)),
        (id("bkt1", "z"), zero(3)),
    ]);
    // Starting moves no usage: what was written before stays retired.
    assert_eq!(machine.bucket_usage_report(), before);
    // A second start is an acknowledgement.
    assert_eq!(ack(&mut machine, 0), UsageAck::Acknowledged {
        acked_through: 0,
        folded: 0
    });
}

#[test]
fn a_stream_created_and_deleted_between_two_polls_is_exported_as_ended() {
    let mut machine = machine();
    ack(&mut machine, 0);
    let cursor = changes(&machine, 0)
        .last()
        .map_or(0, |change| change.usage.usage_seq);

    let brief = id("bkt0", "brief");
    machine.apply(create(&brief, &[1; 10_241], None, 100));
    machine.apply(append(&brief, b"x", 200));
    assert_eq!(
        machine.apply(delete(&brief, 300)),
        StreamResponse::Deleted { expired: false }
    );

    let export = machine.stream_usage_changes(cursor, 100);
    assert_eq!(export.through, usage_seq(&machine));
    assert_eq!(export.changes.len(), 1);
    let ended = &export.changes[0];
    assert_eq!(ended.stream_id, brief);
    assert_eq!(ended.end, Some(StreamEnd::Deleted));
    assert_eq!(ended.usage.committed_append_bytes, 10_242);
    assert_eq!(ended.usage.committed_write_units, 3);
    assert_eq!(ended.usage.committed_records, 2);
    assert_eq!(ended.usage.last_write_at_ms, 200);
    assert_eq!(ended.retained_bytes, 0);
    assert_eq!(ended.content_type, OCTET);
    assert_eq!(
        ended.incarnation,
        crate::history::incarnation_token(ended.created_at_ms, machine.history_id())
    );
    assert_eq!(machine.state_gauges().ended_stream_usage, 1);
}

#[test]
fn nothing_is_folded_without_an_acknowledgement() {
    let mut machine = machine();
    ack(&mut machine, 0);
    let (first, second) = (id("bkt0", "first"), id("bkt0", "second"));
    machine.apply(create(&first, b"one", None, 1));
    machine.apply(create(&second, b"two", None, 1));
    machine.apply(delete(&first, 2));
    machine.apply(delete(&second, 3));
    let ended = changes(&machine, 0);
    let seqs = ended
        .iter()
        .map(|change| change.usage.usage_seq)
        .collect::<Vec<_>>();
    assert_eq!(seqs.len(), 2);
    let report = machine.bucket_usage_report();

    // Through the first end: only it folds, and the totals do not move.
    assert_eq!(ack(&mut machine, seqs[0]), UsageAck::Acknowledged {
        acked_through: seqs[0],
        folded: 1
    });
    assert_eq!(machine.bucket_usage_report(), report);
    assert_eq!(changes(&machine, 0).len(), 1);
    // A replay, or an older acknowledgement, folds nothing.
    assert_eq!(ack(&mut machine, seqs[0]), UsageAck::Acknowledged {
        acked_through: seqs[0],
        folded: 0
    });
    assert_eq!(ack(&mut machine, 0), UsageAck::Acknowledged {
        acked_through: seqs[0],
        folded: 0
    });
    // Past the ledger's sequence number it acknowledges up to it.
    assert_eq!(ack(&mut machine, u64::MAX), UsageAck::Acknowledged {
        acked_through: usage_seq(&machine),
        folded: 1
    });
    assert!(changes(&machine, 0).is_empty());
    assert_eq!(machine.bucket_usage_report(), report);
    assert_eq!(machine.state_gauges().usage_ack_lag, 0);
}

#[test]
fn an_acknowledgement_of_another_history_changes_nothing() {
    let mut machine = machine();
    machine.apply(create(&id("bkt0", "s"), b"x", None, 1));
    let response = machine.apply(StreamCommand::AckStreamUsage {
        history: 7,
        through_seq: 0,
    });
    assert_eq!(response, StreamResponse::StreamUsageAcked {
        ack: UsageAck::OtherHistory { history: 0 },
    });
    assert_eq!(
        machine.usage_ledger_progress(),
        UsageLedgerProgress::NotStarted
    );
}

#[test]
fn a_purge_and_an_expiry_end_each_stream_once() {
    let mut machine = machine();
    ack(&mut machine, 0);
    for stream in ["p1", "p2"] {
        machine.apply(create(&id("bkt1", stream), b"purged", None, 1));
    }
    machine.apply(create(&id("bkt0", "ttl"), b"expiring", Some(1), 1));
    machine.apply(StreamCommand::PurgeBucket {
        bucket_id: "bkt1".to_owned(),
    });
    machine.apply(StreamCommand::ExpireStreams {
        now_ms: 5_000,
        limit: std::num::NonZeroU32::new(10).expect("positive"),
    });
    let ends = changes(&machine, 0)
        .into_iter()
        .map(|change| (change.stream_id, change.end))
        .collect::<Vec<_>>();
    assert_eq!(ends, vec![
        (id("bkt1", "p1"), Some(StreamEnd::Purged)),
        (id("bkt1", "p2"), Some(StreamEnd::Purged)),
        (id("bkt0", "ttl"), Some(StreamEnd::Expired)),
    ]);
    // A purge that finds nothing ends nothing.
    machine.apply(StreamCommand::PurgeBucket {
        bucket_id: "bkt1".to_owned(),
    });
    assert_eq!(changes(&machine, 0).len(), 3);
}

#[test]
fn retention_and_snapshots_change_a_streams_sizes_and_sequence() {
    let mut machine = machine();
    let stream_id = id("bkt0", "sized");
    machine.apply(create(&stream_id, b"0123456789", None, 1));
    ack(&mut machine, 0);
    let seq = |machine: &StreamStateMachine| changes(machine, 0)[0].usage.usage_seq;
    let created = seq(&machine);

    machine.apply(StreamCommand::PublishSnapshot {
        stream_id: stream_id.clone(),
        snapshot_offset: 6,
        content_type: OCTET.to_owned(),
        payload: bytes::Bytes::from_static(b"snap"),
        now_ms: 2,
        expected_incarnation: None,
    });
    let published = seq(&machine);
    assert!(published > created);
    assert_eq!(changes(&machine, 0)[0].snapshot_bytes, 4);

    machine.apply(StreamCommand::AdvanceRetention {
        stream_id: stream_id.clone(),
        retained_offset: 6,
        now_ms: 3,
        expected_incarnation: None,
    });
    let retained = changes(&machine, 0);
    assert!(retained[0].usage.usage_seq > published);
    assert_eq!(retained[0].retained_bytes, 4);
    // Neither counts as a write.
    assert_eq!(retained[0].usage.committed_write_units, 0);
}

#[test]
fn an_export_pages_by_sequence_number() {
    let mut machine = machine();
    ack(&mut machine, 0);
    for index in 0..5 {
        machine.apply(create(&id("bkt0", &format!("s{index}")), b"x", None, 1));
    }
    machine.apply(delete(&id("bkt0", "s1"), 2));
    machine.apply(append(&id("bkt0", "s0"), b"y", 3));

    let full = changes(&machine, 0);
    let mut paged = Vec::new();
    let mut cursor = 0;
    loop {
        let page = machine.stream_usage_changes(cursor, 2);
        assert!(page.changes.len() <= 2);
        paged.extend(page.changes);
        cursor = page.through;
        if cursor == usage_seq(&machine) {
            break;
        }
    }
    assert_eq!(paged, full);
    // Each stream version once, in sequence order.
    let seqs = full
        .iter()
        .map(|change| change.usage.usage_seq)
        .collect::<Vec<_>>();
    assert!(seqs.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(full.len(), 5);
}

#[test]
fn an_import_restarts_the_ledger_with_its_totals_retired() {
    let mut source = machine();
    source.apply(create(&id("bkt0", "s"), b"source", None, 1));
    let backup = source.snapshot();

    let mut target = StreamStateMachine::new();
    ack(&mut target, 0);
    let imported = target.apply(StreamCommand::ImportSnapshot {
        snapshot: Box::new(backup),
        history: std::num::NonZeroU64::new(9).expect("non-zero"),
        backup_digest: [3; 32],
        now_ms: 10,
    });
    assert!(
        matches!(imported, StreamResponse::SnapshotImported { .. }),
        "{imported:?}"
    );
    assert_eq!(
        target.usage_ledger_progress(),
        UsageLedgerProgress::NotStarted
    );
    assert_eq!(target.bucket_usage_report(), source.bucket_usage_report());
}

#[derive(Debug, Clone)]
enum Op {
    Create {
        bucket: usize,
        stream: u8,
        len: usize,
        ttl: bool,
    },
    Append {
        bucket: usize,
        stream: u8,
        len: usize,
    },
    Delete {
        bucket: usize,
        stream: u8,
    },
    Publish {
        bucket: usize,
        stream: u8,
    },
    Retain {
        bucket: usize,
        stream: u8,
    },
    Purge {
        bucket: usize,
    },
    Expire,
    Tick,
    Ack {
        fraction: u8,
    },
    OtherHistoryAck,
    RoundTrip,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    let bucket = 0..BUCKETS.len();
    let stream = 0u8..4;
    prop_oneof![
        6 => (bucket.clone(), stream.clone(), 0usize..12_000, any::<bool>())
            .prop_map(|(bucket, stream, len, ttl)| Op::Create { bucket, stream, len, ttl }),
        6 => (bucket.clone(), stream.clone(), 0usize..25_000)
            .prop_map(|(bucket, stream, len)| Op::Append { bucket, stream, len }),
        2 => (bucket.clone(), stream.clone()).prop_map(|(bucket, stream)| Op::Delete { bucket, stream }),
        1 => (bucket.clone(), stream.clone()).prop_map(|(bucket, stream)| Op::Publish { bucket, stream }),
        1 => (bucket.clone(), stream).prop_map(|(bucket, stream)| Op::Retain { bucket, stream }),
        1 => bucket.prop_map(|bucket| Op::Purge { bucket }),
        1 => Just(Op::Expire),
        1 => Just(Op::Tick),
        3 => (0u8..=4).prop_map(|fraction| Op::Ack { fraction }),
        1 => Just(Op::OtherHistoryAck),
        1 => Just(Op::RoundTrip),
    ]
}

/// The ledger invariants at one point of a group's log.
fn check_ledger(machine: &StreamStateMachine) -> Result<(), TestCaseError> {
    let report = machine.bucket_usage_report();
    let export = machine.stream_usage_changes(0, usize::MAX);
    match machine.usage_ledger_snapshot() {
        UsageLedgerSnapshot::NotStarted => {
            prop_assert!(export.changes.is_empty());
        }
        UsageLedgerSnapshot::Started(ledger) => {
            // Bucket totals are retired + Σ live + Σ ended, per bucket.
            prop_assert_eq!(ledger.check(machine.registry.stream_ids(), &report), Ok(()));
            // An export from 0 lists every live stream and every ended
            // stream not yet acknowledged, once each, in sequence order.
            prop_assert_eq!(export.through, ledger.usage_seq);
            let live = export
                .changes
                .iter()
                .filter(|change| change.end.is_none())
                .count();
            let ended = export
                .changes
                .iter()
                .filter(|change| change.end.is_some())
                .count();
            prop_assert_eq!(live, ledger.streams.len());
            prop_assert_eq!(ended, ledger.ended.len());
            prop_assert!(
                export
                    .changes
                    .windows(2)
                    .all(|pair| pair[0].usage.usage_seq < pair[1].usage.usage_seq)
            );
            // The derived index lists exactly the slots' sequence numbers.
            let mut slot_seqs = ledger
                .streams
                .iter()
                .map(|entry| entry.usage.usage_seq)
                .collect::<Vec<_>>();
            slot_seqs.sort_unstable();
            let listed = export
                .changes
                .iter()
                .filter(|change| change.end.is_none())
                .map(|change| change.usage.usage_seq)
                .collect::<Vec<_>>();
            prop_assert_eq!(listed, slot_seqs);
            prop_assert_eq!(
                Some(machine.state_gauges().ended_stream_usage),
                u64::try_from(ledger.ended.len()).ok()
            );
        }
    }
    // A group snapshot restores the same ledger, totals and export.
    let restored = round_trip(machine);
    prop_assert_eq!(
        restored.usage_ledger_snapshot(),
        machine.usage_ledger_snapshot()
    );
    prop_assert_eq!(restored.bucket_usage_report(), report);
    prop_assert_eq!(restored.stream_usage_changes(0, usize::MAX), export);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// U-A, U-B: in every group and after every entry, each bucket's
    /// totals equal its retired, live and ended usage, the totals equal
    /// those of a group whose ledger never started (Ursula 0.10.1's
    /// `/__ursula/usage`), and a group snapshot restores the same ledger. A
    /// twin replica that applies the same entries without ever restoring a
    /// snapshot, whose registry hashes differently, holds the same ledger:
    /// no sequence number depends on a replica's history or hash order.
    #[test]
    fn prop_bucket_totals_equal_retired_live_and_ended(ops in vec(op_strategy(), 1..60)) {
        let mut ledger = machine();
        let mut twin = machine();
        let mut reference = machine();
        let mut now_ms = 1u64;
        let mut written = false;
        for op in ops {
            let stream_of = |bucket: usize, stream: u8| id(BUCKETS[bucket], &format!("s{stream}"));
            let commands = match op {
                Op::Create { bucket, stream, len, ttl } => vec![create(
                    &stream_of(bucket, stream),
                    &vec![b'c'; len],
                    ttl.then_some(2),
                    now_ms,
                )],
                Op::Append { bucket, stream, len } => {
                    vec![append(&stream_of(bucket, stream), &vec![b'a'; len], now_ms)]
                }
                Op::Delete { bucket, stream } => vec![delete(&stream_of(bucket, stream), now_ms)],
                Op::Publish { bucket, stream } | Op::Retain { bucket, stream } => {
                    let stream_id = stream_of(bucket, stream);
                    let Some(tail) = ledger.stream_metadata(&stream_id).map(|meta| meta.tail_offset) else {
                        continue;
                    };
                    let mut commands = vec![StreamCommand::PublishSnapshot {
                        stream_id: stream_id.clone(),
                        snapshot_offset: tail,
                        content_type: OCTET.to_owned(),
                        payload: bytes::Bytes::from_static(b"image"),
                        now_ms,
                        expected_incarnation: None,
                    }];
                    if matches!(op, Op::Retain { .. }) {
                        commands.push(StreamCommand::AdvanceRetention {
                            stream_id,
                            retained_offset: tail,
                            now_ms,
                            expected_incarnation: None,
                        });
                    }
                    commands
                }
                Op::Purge { bucket } => vec![StreamCommand::PurgeBucket {
                    bucket_id: BUCKETS[bucket].to_owned(),
                }],
                Op::Expire => vec![StreamCommand::ExpireStreams {
                    now_ms,
                    limit: std::num::NonZeroU32::new(2).expect("positive"),
                }],
                Op::Tick => {
                    now_ms = now_ms.saturating_add(1_500);
                    continue;
                }
                Op::Ack { fraction } => {
                    let through = usage_seq(&ledger).saturating_mul(u64::from(fraction)) / 4;
                    prop_assert_eq!(ack(&mut ledger, through), ack(&mut twin, through));
                    check_ledger(&ledger)?;
                    prop_assert_eq!(twin.usage_ledger_snapshot(), ledger.usage_ledger_snapshot());
                    continue;
                }
                Op::OtherHistoryAck => {
                    let before = ledger.usage_ledger_snapshot();
                    let other = StreamCommand::AckStreamUsage {
                        history: ledger.history_id().wrapping_add(1),
                        through_seq: u64::MAX,
                    };
                    ledger.apply(other.clone());
                    twin.apply(other);
                    prop_assert_eq!(ledger.usage_ledger_snapshot(), before);
                    continue;
                }
                Op::RoundTrip => {
                    ledger = round_trip(&ledger);
                    reference = StreamStateMachine::restore(reference.snapshot()).expect("restore");
                    continue;
                }
            };
            for command in commands {
                let answer = ledger.apply(command.clone());
                prop_assert_eq!(&twin.apply(command.clone()), &answer);
                written |= matches!(answer, StreamResponse::Created { .. });
                let expected = reference.apply(command);
                prop_assert_eq!(answer, expected);
            }
            prop_assert_eq!(ledger.bucket_usage_report(), reference.bucket_usage_report());
            check_ledger(&ledger)?;
            prop_assert_eq!(twin.usage_ledger_snapshot(), ledger.usage_ledger_snapshot());
        }
        // The generator's streams are valid: a run that creates nothing
        // tests nothing.
        prop_assume!(written);
    }
}
