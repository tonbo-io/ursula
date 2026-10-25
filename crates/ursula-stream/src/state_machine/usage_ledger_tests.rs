//! Per-version usage (F20, F9) through the production entry points: the
//! counting rule at the bucket counter's call sites, the seqs, the history
//! baseline, end records and their cap, and the bucket totals derived from
//! retired usage, live versions and end records.

use std::num::NonZeroU32;
use std::num::NonZeroU64;

use bytes::Bytes;

use super::BucketStreamId;
use super::MAX_END_RECORDS;
use super::ProducerRequest;
use super::StreamCommand;
use super::StreamResponse;
use super::StreamSnapshotError;
use super::StreamStateMachine;
use crate::model::BucketUsage;
use crate::model::BucketUsageSnapshot;
use crate::model::ExternalPayloadRef;
use crate::usage::CommittedUsage;
use crate::usage::EndRecord;
use crate::usage::LastWrite;
use crate::usage::StreamEnd;
use crate::usage::VersionCounters;
use crate::usage::VersionUsage;

const OCTET: &str = "application/octet-stream";
const BUCKET: &str = "usage";

fn id(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("non-zero history id")
}

fn sid(name: &str) -> BucketStreamId {
    BucketStreamId::new(BUCKET, name)
}

fn new_history(machine: &mut StreamStateMachine, index: u64, history: u64) {
    let previous = machine.history_id();
    assert!(matches!(
        machine.apply_entry(index, StreamCommand::NewHistory {
            history: id(history),
            now_ms: 0,
            previous,
            kept: index.saturating_sub(1),
            leader: crate::HistoryLeader {
                term: 1,
                node_id: 1,
            },
        }),
        StreamResponse::HistoryStarted { .. }
    ));
}

/// A group whose genesis, history `genesis`, applied, with one bucket.
fn group(genesis: u64) -> StreamStateMachine {
    let mut machine = StreamStateMachine::new();
    new_history(&mut machine, 1, genesis);
    assert!(matches!(
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: BUCKET.to_owned(),
        }),
        StreamResponse::BucketCreated { .. }
    ));
    machine
}

fn create(machine: &mut StreamStateMachine, name: &str, body: &[u8], now_ms: u64) {
    let response = machine.apply(StreamCommand::CreateStream {
        stream_id: sid(name),
        content_type: OCTET.to_owned(),
        initial_payload: Bytes::copy_from_slice(body),
        close_after: false,
        stream_seq: None,
        stream_ttl_seconds: None,
        stream_expires_at_ms: None,
        now_ms,
    });
    assert!(
        matches!(response, StreamResponse::Created { .. }),
        "{response:?}"
    );
}

fn append_as(
    machine: &mut StreamStateMachine,
    name: &str,
    content_type: &str,
    body: &[u8],
    producer_seq: Option<u64>,
    now_ms: u64,
) -> StreamResponse {
    machine.apply(StreamCommand::Append {
        stream_id: sid(name),
        content_type: Some(content_type.to_owned()),
        payload: Bytes::copy_from_slice(body),
        close_after: false,
        stream_seq: None,
        producer: producer_seq.map(|seq| ProducerRequest {
            producer_id: "writer".to_owned(),
            producer_epoch: 0,
            producer_seq: seq,
            ..Default::default()
        }),
        now_ms,
    })
}

fn append(machine: &mut StreamStateMachine, name: &str, body: &[u8], now_ms: u64) {
    let response = append_as(machine, name, OCTET, body, None, now_ms);
    assert!(
        matches!(response, StreamResponse::Appended { .. }),
        "{response:?}"
    );
}

fn usage(machine: &StreamStateMachine, name: &str) -> VersionUsage {
    machine.version_usage(&sid(name)).expect("a live version")
}

fn bucket_row(machine: &StreamStateMachine) -> BucketUsage {
    let report = machine.bucket_usage_report();
    assert_eq!(report.len(), 1, "{report:?}");
    report[0].usage
}

fn committed_of(row: BucketUsage) -> CommittedUsage {
    CommittedUsage::of_bucket(&row)
}

fn sum(writes: &[(u64, u64)]) -> CommittedUsage {
    let mut total = CommittedUsage::default();
    for (bytes, records) in writes {
        total.add(CommittedUsage::of_write(*bytes, *records));
    }
    total
}

/// A create counts one write, its initial payload; every applied append
/// counts one more and one append, takes a new seq and records its time.
/// The bucket's totals are the version's counters.
#[test]
fn a_create_and_its_appends_count_by_the_bucket_counters_rule() {
    let mut machine = group(7);
    let initial = vec![b'x'; 10_241];
    create(&mut machine, "s", &initial, 5);
    assert_eq!(usage(&machine, "s"), VersionUsage {
        seq: 1,
        counters: VersionCounters {
            committed: CommittedUsage {
                committed_append_bytes: 10_241,
                committed_records: 1,
                committed_write_units: 2,
            },
            appends: 0,
        },
        history_baseline: VersionCounters::default(),
        last_write: LastWrite::At(5),
        counted_since: id(7),
    });

    append(&mut machine, "s", b"abc", 6);
    let after = usage(&machine, "s");
    assert_eq!(after.seq, 2);
    assert_eq!(after.counters, VersionCounters {
        committed: sum(&[(10_241, 1), (3, 1)]),
        appends: 1,
    });
    assert_eq!(after.last_write, LastWrite::At(6));
    assert_eq!(after.history_baseline, VersionCounters::default());
    assert_eq!(machine.usage_seq(), 2);

    let row = bucket_row(&machine);
    assert_eq!(committed_of(row), after.counters.committed);
    assert_eq!(row.retained_bytes, 10_244);
    assert_eq!(row.stream_count, 1);
}

/// A producer repeat, a refused write and a close-only write count
/// nothing and take no seq.
#[test]
fn a_repeat_a_refusal_and_a_close_count_nothing() {
    let mut machine = group(7);
    create(&mut machine, "s", b"", 1);
    let first = append_as(&mut machine, "s", OCTET, b"ab", Some(0), 2);
    assert!(
        matches!(first, StreamResponse::Appended { .. }),
        "{first:?}"
    );
    let counted = usage(&machine, "s");
    assert_eq!(counted.counters.appends, 1);

    let repeat = append_as(&mut machine, "s", OCTET, b"ab", Some(0), 3);
    assert!(
        matches!(
            &repeat,
            StreamResponse::Appended {
                producer: Some(ack),
                ..
            } if ack.is_repeat()
        ),
        "{repeat:?}"
    );
    assert_eq!(usage(&machine, "s"), counted);

    let refused = append_as(&mut machine, "s", "text/plain", b"cd", None, 4);
    assert!(
        matches!(refused, StreamResponse::Error { .. }),
        "{refused:?}"
    );
    assert_eq!(usage(&machine, "s"), counted);

    let closed = machine.apply(StreamCommand::Close {
        stream_id: sid("s"),
        stream_seq: None,
        producer: None,
        now_ms: 5,
    });
    assert!(
        matches!(closed, StreamResponse::Closed { .. }),
        "{closed:?}"
    );
    assert_eq!(usage(&machine, "s"), counted);
    assert_eq!(committed_of(bucket_row(&machine)), sum(&[(0, 0), (2, 1)]));
}

/// An external append counts the bytes it staged, as the bucket counter
/// did.
#[test]
fn an_external_append_counts_its_staged_bytes() {
    let mut machine = group(7);
    create(&mut machine, "s", b"", 1);
    let response = machine.apply(StreamCommand::AppendExternal {
        stream_id: sid("s"),
        content_type: Some(OCTET.to_owned()),
        payload: ExternalPayloadRef {
            s3_path: "usage/external/a.bin".to_owned(),
            payload_len: 20_000,
            object_size: 20_000,
        },
        message_count: 0,
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 2,
    });
    assert!(
        matches!(response, StreamResponse::Appended { .. }),
        "{response:?}"
    );
    let usage = usage(&machine, "s");
    assert_eq!(usage.counters, VersionCounters {
        committed: sum(&[(0, 0), (20_000, 1)]),
        appends: 1,
    });
    assert_eq!(usage.last_write, LastWrite::At(2));
    assert_eq!(bucket_row(&machine).retained_bytes, 20_000);
}

/// A publish that replaces the visible snapshot and a retention that moves
/// the retained offset change what the version holds, not what it
/// committed: each takes a new seq and counts nothing. Their repeats change
/// nothing.
#[test]
fn a_publish_and_a_retention_take_a_seq_and_count_nothing() {
    let mut machine = group(7);
    create(&mut machine, "s", b"abcdef", 1);
    let created = usage(&machine, "s");
    let publish = || StreamCommand::PublishSnapshot {
        stream_id: sid("s"),
        snapshot_offset: 4,
        content_type: OCTET.to_owned(),
        payload: Bytes::from_static(b"snap"),
        now_ms: 2,
        expected_incarnation: None,
    };
    let published = machine.apply(publish());
    assert!(
        matches!(published, StreamResponse::SnapshotPublished { .. }),
        "{published:?}"
    );
    let after_publish = usage(&machine, "s");
    assert_eq!(after_publish.seq, created.seq + 1);
    assert_eq!(after_publish.counters, created.counters);
    assert_eq!(after_publish.last_write, created.last_write);

    let repeated = machine.apply(publish());
    assert!(
        matches!(repeated, StreamResponse::SnapshotPublished { .. }),
        "{repeated:?}"
    );
    assert_eq!(usage(&machine, "s"), after_publish);

    let retention = || StreamCommand::AdvanceRetention {
        stream_id: sid("s"),
        retained_offset: 4,
        now_ms: 3,
        expected_incarnation: None,
    };
    assert_eq!(
        machine.apply(retention()),
        StreamResponse::RetentionAdvanced { retained_offset: 4 }
    );
    let after_retention = usage(&machine, "s");
    assert_eq!(after_retention.seq, after_publish.seq + 1);
    assert_eq!(after_retention.counters, created.counters);
    assert_eq!(
        machine.apply(retention()),
        StreamResponse::RetentionAdvanced { retained_offset: 4 }
    );
    assert_eq!(usage(&machine, "s"), after_retention);

    let row = bucket_row(&machine);
    assert_eq!(committed_of(row), created.counters.committed);
    assert_eq!(row.retained_bytes, 2);
}

/// A removal leaves an end record with the version's final usage, how and
/// when it ended and the history it ended in, under a new seq: the totals
/// keep every committed counter, and the gauges leave with the stream. A
/// purge ends its streams in stream-id order at its own clock, and the
/// purged bucket keeps its row at zero gauges (F15).
#[test]
fn a_removal_leaves_an_end_record_and_keeps_the_totals() {
    let mut machine = group(7);
    create(&mut machine, "a", b"abc", 1);
    create(&mut machine, "c", b"defg", 1);
    create(&mut machine, "b", b"", 1);
    append(&mut machine, "a", b"hi", 2);
    let before = committed_of(bucket_row(&machine));
    assert_eq!(before, sum(&[(3, 1), (4, 1), (0, 0), (2, 1)]));
    let a = usage(&machine, "a");

    assert_eq!(
        machine.apply(StreamCommand::DeleteStream {
            stream_id: sid("a"),
            now_ms: 3,
        }),
        StreamResponse::Deleted { expired: false }
    );
    assert_eq!(machine.version_usage(&sid("a")), None);
    assert_eq!(machine.usage_seq(), 5);
    assert_eq!(machine.end_records().collect::<Vec<_>>(), [&EndRecord {
        seq: 5,
        stream_id: sid("a"),
        created_at_ms: 1,
        counted_since: id(7),
        ended_in: id(7),
        end: StreamEnd::Deleted,
        ended_at_ms: 3,
        counters: a.counters,
        history_baseline: VersionCounters::default(),
        last_write: LastWrite::At(2),
    }]);
    let row = bucket_row(&machine);
    assert_eq!(committed_of(row), before);
    assert_eq!(row.stream_count, 2);
    assert_eq!(row.retained_bytes, 4);

    let purged = machine.apply(StreamCommand::PurgeBucket {
        bucket_id: BUCKET.to_owned(),
        now_ms: 40,
    });
    assert!(
        matches!(purged, StreamResponse::BucketPurged {
            removed_streams: 2,
            ..
        }),
        "{purged:?}"
    );
    let ends = machine
        .end_records()
        .map(|record| {
            (
                record.seq,
                record.stream_id.stream_id.as_str(),
                record.end,
                record.ended_at_ms,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(ends, [
        (5, "a", StreamEnd::Deleted, 3),
        (6, "b", StreamEnd::Purged, 40),
        (7, "c", StreamEnd::Purged, 40),
    ]);
    let row = bucket_row(&machine);
    assert_eq!(committed_of(row), before);
    assert_eq!(row.stream_count, 0);
    assert_eq!(row.retained_bytes, 0);
    assert_eq!(machine.state_gauges().bucket_usage_rows, 1);
    assert_eq!(machine.usage_retired_through(), 0);
    assert_eq!(machine.removed_state().end_records, 0);
}

fn create_expiring(machine: &mut StreamStateMachine, name: &str, expires_at_ms: u64, now_ms: u64) {
    let response = machine.apply(StreamCommand::CreateStream {
        stream_id: sid(name),
        content_type: OCTET.to_owned(),
        initial_payload: Bytes::from_static(b"xy"),
        close_after: false,
        stream_seq: None,
        stream_ttl_seconds: None,
        stream_expires_at_ms: Some(expires_at_ms),
        now_ms,
    });
    assert!(
        matches!(response, StreamResponse::Created { .. }),
        "{response:?}"
    );
}

/// Every removal of an expired stream ends it `Expired` at the removing
/// entry's clock: the sweep, a delete, a touch, a write and a create over
/// it. A create over an expired stream stamps the end before the new
/// version.
#[test]
fn every_removal_of_an_expired_stream_ends_it_expired() {
    let mut machine = group(7);
    create_expiring(&mut machine, "swept", 9, 1);
    for name in ["deleted", "touched", "written", "recreated"] {
        create_expiring(&mut machine, name, 10, 1);
    }
    assert_eq!(
        machine.apply(StreamCommand::Sweep {
            now_ms: 11,
            limit: NonZeroU32::MIN,
            retire_through: 0,
        }),
        StreamResponse::Swept {
            expired: 1,
            retired: 0,
        }
    );
    assert_eq!(
        machine.apply(StreamCommand::DeleteStream {
            stream_id: sid("deleted"),
            now_ms: 12,
        }),
        StreamResponse::Deleted { expired: true }
    );
    let touched = machine.apply(StreamCommand::TouchStreamAccess {
        stream_id: sid("touched"),
        now_ms: 13,
        renew_ttl: false,
    });
    assert_eq!(touched, StreamResponse::Accessed {
        changed: true,
        expired: true,
    });
    let written = append_as(&mut machine, "written", OCTET, b"z", None, 14);
    assert!(
        matches!(written, StreamResponse::Error { .. }),
        "{written:?}"
    );
    create(&mut machine, "recreated", b"new", 15);

    let ends = machine
        .end_records()
        .map(|record| {
            (
                record.seq,
                record.stream_id.stream_id.as_str(),
                record.end,
                record.ended_at_ms,
                record.counters.committed,
            )
        })
        .collect::<Vec<_>>();
    let initial = CommittedUsage::of_write(2, 1);
    assert_eq!(ends, [
        (6, "swept", StreamEnd::Expired, 11, initial),
        (7, "deleted", StreamEnd::Expired, 12, initial),
        (8, "touched", StreamEnd::Expired, 13, initial),
        (9, "written", StreamEnd::Expired, 14, initial),
        (10, "recreated", StreamEnd::Expired, 15, initial),
    ]);
    assert_eq!(usage(&machine, "recreated").seq, 11);
    let mut expected = sum(&[(2, 1); 5]);
    expected.add(CommittedUsage::of_write(3, 1));
    assert_eq!(committed_of(bucket_row(&machine)), expected);
}

/// An end record carries the version's counters at the start of the
/// group's history when it ended: its recorded baseline when it was
/// written in that history, else its final counters, since nothing counted
/// after the event.
#[test]
fn an_end_record_carries_the_baseline_of_the_history_it_ended_in() {
    let mut machine = group(7);
    create(&mut machine, "quiet", b"abc", 1);
    create(&mut machine, "written", b"de", 1);
    let quiet = usage(&machine, "quiet").counters;
    let at_event = usage(&machine, "written").counters;
    new_history(&mut machine, 10, 9);
    append(&mut machine, "written", b"fgh", 2);
    for (name, now_ms) in [("quiet", 3), ("written", 4)] {
        assert_eq!(
            machine.apply(StreamCommand::DeleteStream {
                stream_id: sid(name),
                now_ms,
            }),
            StreamResponse::Deleted { expired: false }
        );
    }
    let ends = machine.end_records().cloned().collect::<Vec<_>>();
    assert_eq!(ends.len(), 2);
    assert_eq!(ends[0].stream_id, sid("quiet"));
    assert_eq!(ends[0].counted_since, id(7));
    assert_eq!(ends[0].ended_in, id(9));
    assert_eq!(ends[0].counters, quiet);
    assert_eq!(ends[0].history_baseline, quiet);
    assert_eq!(ends[1].stream_id, sid("written"));
    assert_eq!(ends[1].ended_in, id(9));
    assert_eq!(ends[1].history_baseline, at_event);
    assert_eq!(ends[1].counters, VersionCounters {
        committed: sum(&[(2, 1), (3, 1)]),
        appends: 1,
    });
}

/// A removal that finds the group holding `MAX_END_RECORDS` end records
/// first folds the lowest-seq one into its bucket's retired usage, counted
/// as an early fold: the group never holds more, and no total changes.
#[test]
fn a_removal_past_the_cap_folds_the_oldest_end_record_first() {
    let mut machine = group(7);
    let name = |index: usize| format!("s{index:06}");
    for index in 0..=MAX_END_RECORDS {
        create(&mut machine, &name(index), b"ab", 1);
    }
    let before = committed_of(bucket_row(&machine));
    let purged = machine.apply(StreamCommand::PurgeBucket {
        bucket_id: BUCKET.to_owned(),
        now_ms: 2,
    });
    assert!(
        matches!(purged, StreamResponse::BucketPurged { .. }),
        "{purged:?}"
    );
    let created = u64::try_from(MAX_END_RECORDS + 1).expect("count fits u64");
    assert_eq!(machine.end_records().len(), MAX_END_RECORDS);
    // The first purged stream, `s000000`, ended at seq `created + 1` and
    // was folded when the last one ended.
    let first = machine.end_records().next().expect("an end record");
    assert_eq!(first.stream_id, sid(&name(1)));
    assert_eq!(first.seq, created + 2);
    assert_eq!(machine.usage_retired_through(), created + 1);
    assert_eq!(machine.usage_seq(), 2 * created);
    let removed = machine.removed_state();
    assert_eq!(removed.streams, created);
    assert_eq!(removed.end_records, 1);
    assert_eq!(removed.end_records_early, 1);
    assert_eq!(committed_of(bucket_row(&machine)), before);
}

fn delete(machine: &mut StreamStateMachine, name: &str, now_ms: u64) {
    assert_eq!(
        machine.apply(StreamCommand::DeleteStream {
            stream_id: sid(name),
            now_ms,
        }),
        StreamResponse::Deleted { expired: false }
    );
}

fn sweep(machine: &mut StreamStateMachine, limit: u32, retire_through: u64) -> StreamResponse {
    machine.apply(StreamCommand::Sweep {
        now_ms: 100,
        limit: NonZeroU32::new(limit).expect("a non-zero limit"),
        retire_through,
    })
}

/// The sweep folds the end records at or below its `retire_through` into
/// their bucket's retired usage, lowest seq first and at most its limit,
/// and `retired_through` stays the seq of the newest one folded. Each fold
/// counts as a removal, not an early one, and no total changes.
#[test]
fn the_sweep_folds_end_records_through_its_watermark() {
    let mut machine = group(7);
    for name in ["a", "b", "c", "d"] {
        create(&mut machine, name, b"abc", 1);
    }
    for (name, now_ms) in [("a", 2), ("b", 3), ("c", 4), ("d", 5)] {
        delete(&mut machine, name, now_ms);
    }
    let seqs = machine
        .end_records()
        .map(|record| record.seq)
        .collect::<Vec<_>>();
    assert_eq!(seqs, [5, 6, 7, 8]);
    let before = bucket_row(&machine);
    assert!(!machine.sweep_due(100, 4));
    assert!(machine.sweep_due(100, 5));

    // A watermark between two end records folds the ones below it, at most
    // the limit, lowest seq first.
    assert_eq!(sweep(&mut machine, 1, 7), StreamResponse::Swept {
        expired: 0,
        retired: 1,
    });
    assert_eq!(machine.usage_retired_through(), 5);
    assert_eq!(sweep(&mut machine, 16, 7), StreamResponse::Swept {
        expired: 0,
        retired: 2,
    });
    assert_eq!(machine.usage_retired_through(), 7);
    let left = machine
        .end_records()
        .map(|record| record.stream_id.stream_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(left, ["d"]);
    assert!(!machine.sweep_due(100, 7));
    assert_eq!(sweep(&mut machine, 16, 7), StreamResponse::Swept {
        expired: 0,
        retired: 0,
    });
    // Seqs and the totals stay: a fold stamps nothing and only moves usage.
    assert_eq!(machine.usage_seq(), 8);
    assert_eq!(bucket_row(&machine), before);
    let removed = machine.removed_state();
    assert_eq!(removed.streams, 4);
    assert_eq!(removed.end_records, 3);
    assert_eq!(removed.end_records_early, 0);

    // A snapshot round trip keeps the totals (the backup restore retires
    // every end record).
    let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore");
    assert_eq!(committed_of(bucket_row(&restored)), committed_of(before));
}

/// One `Sweep` first removes the expired streams, then folds only end
/// records the group held before the entry: a watermark above the group's
/// usage seq never folds the entry's own removals, whatever the proposer
/// sampled.
#[test]
fn a_sweep_never_folds_its_own_removals() {
    let mut machine = group(7);
    create(&mut machine, "deleted", b"abc", 1);
    delete(&mut machine, "deleted", 2);
    create_expiring(&mut machine, "expired", 50, 3);
    assert_eq!(machine.usage_seq(), 3);
    assert_eq!(sweep(&mut machine, 16, u64::MAX), StreamResponse::Swept {
        expired: 1,
        retired: 1,
    });
    let ends = machine
        .end_records()
        .map(|record| (record.seq, record.stream_id.stream_id.as_str(), record.end))
        .collect::<Vec<_>>();
    assert_eq!(ends, [(4, "expired", StreamEnd::Expired)]);
    assert_eq!(machine.usage_retired_through(), 2);
    // The next entry folds it.
    assert_eq!(sweep(&mut machine, 16, u64::MAX), StreamResponse::Swept {
        expired: 0,
        retired: 1,
    });
    assert_eq!(machine.end_records().len(), 0);
    assert_eq!(machine.usage_retired_through(), 4);
    assert_eq!(machine.state_gauges().bucket_usage_rows, 1);
}

/// A restarted replica's retirement clock seeds from the end records'
/// removal times: the highest seq through which every end record ended at
/// or before a time, the folded watermark when the lowest did not.
#[test]
fn end_records_ended_by_a_time_stop_at_the_first_later_one() {
    let mut machine = group(7);
    for name in ["a", "b", "c", "d"] {
        create(&mut machine, name, b"abc", 1);
    }
    // Removed out of clock order: `c` by a node whose clock was ahead.
    for (name, now_ms) in [("a", 10), ("b", 20), ("c", 90), ("d", 30)] {
        delete(&mut machine, name, now_ms);
    }
    assert_eq!(machine.end_records_ended_by(9), 0);
    assert_eq!(machine.end_records_ended_by(10), 5);
    assert_eq!(machine.end_records_ended_by(30), 6);
    assert_eq!(machine.end_records_ended_by(89), 6);
    assert_eq!(machine.end_records_ended_by(90), 8);
    assert_eq!(sweep(&mut machine, 16, 6), StreamResponse::Swept {
        expired: 0,
        retired: 2,
    });
    assert_eq!(machine.end_records_ended_by(9), 6);
    assert_eq!(machine.end_records_ended_by(90), 8);
}

/// The genesis records the group's genesis id, and a later event keeps
/// it. A version's first write after an event records its counters as its
/// history baseline before the write counts (W5). A version created after
/// the event counts since it, from a zero baseline.
#[test]
fn the_first_write_after_a_history_event_records_the_baseline() {
    let mut machine = group(7);
    assert_eq!(machine.usage_genesis(), 7);
    create(&mut machine, "s", b"abc", 1);
    append(&mut machine, "s", b"de", 2);
    let at_event = usage(&machine, "s").counters;

    new_history(&mut machine, 10, 9);
    assert_eq!(machine.usage_genesis(), 7);
    let seq_at_event = machine.usage_seq();
    assert_eq!(
        usage(&machine, "s").history_baseline,
        VersionCounters::default()
    );

    append(&mut machine, "s", b"fgh", 3);
    let written = usage(&machine, "s");
    assert_eq!(written.history_baseline, at_event);
    assert_eq!(written.counters, VersionCounters {
        committed: sum(&[(3, 1), (2, 1), (3, 1)]),
        appends: 2,
    });
    assert_eq!(written.counted_since, id(7));
    assert_eq!(written.seq, seq_at_event + 1);

    append(&mut machine, "s", b"i", 4);
    assert_eq!(usage(&machine, "s").history_baseline, at_event);

    create(&mut machine, "t", b"j", 5);
    let created = usage(&machine, "t");
    assert_eq!(created.counted_since, id(9));
    assert_eq!(created.history_baseline, VersionCounters::default());
}

/// An import retires the backup's bucket totals and restores every
/// version at zero, counted since the import's history so that each takes
/// a new key, stamped in stream-id order. The group keeps its genesis.
#[test]
fn an_import_retires_the_totals_and_gives_every_version_a_new_key() {
    let mut source = group(7);
    create(&mut source, "b", b"bb", 1);
    create(&mut source, "a", b"aaa", 1);
    create(&mut source, "c", b"c", 1);
    append(&mut source, "a", b"more", 2);
    assert_eq!(
        source.apply(StreamCommand::DeleteStream {
            stream_id: sid("c"),
            now_ms: 3,
        }),
        StreamResponse::Deleted { expired: false }
    );

    let mut target = StreamStateMachine::new();
    new_history(&mut target, 1, 11);
    let imported = target.apply_entry(2, StreamCommand::ImportSnapshot {
        snapshot: Box::new(source.snapshot()),
        history: id(13),
        backup_digest: [3; 32],
        now_ms: 4,
    });
    assert!(
        matches!(imported, StreamResponse::SnapshotImported { .. }),
        "{imported:?}"
    );
    assert_eq!(target.bucket_usage_report(), source.bucket_usage_report());
    assert_eq!(target.usage_genesis(), 11);
    assert_eq!(target.usage_seq(), 2);
    for (name, seq) in [("a", 1), ("b", 2)] {
        assert_eq!(usage(&target, name), VersionUsage {
            seq,
            counters: VersionCounters::default(),
            history_baseline: VersionCounters::default(),
            last_write: LastWrite::Imported,
            counted_since: id(13),
        });
    }

    append(&mut target, "a", b"x", 5);
    let written = usage(&target, "a");
    assert_eq!(written.counters, VersionCounters {
        committed: CommittedUsage::of_write(1, 1),
        appends: 1,
    });
    assert_eq!(written.history_baseline, VersionCounters::default());
    assert_eq!(written.last_write, LastWrite::At(5));
    let mut expected = committed_of(bucket_row(&source));
    expected.add(CommittedUsage::of_write(1, 1));
    assert_eq!(committed_of(bucket_row(&target)), expected);
}

/// A backup that names one bucket's usage twice is refused, where 0.10
/// kept the last row.
#[test]
fn a_restore_refuses_two_usage_rows_of_one_bucket() {
    let mut machine = group(7);
    create(&mut machine, "s", b"abc", 1);
    let mut snapshot = machine.snapshot();
    let row = snapshot.bucket_usage[0].clone();
    snapshot.bucket_usage.push(BucketUsageSnapshot {
        bucket_id: row.bucket_id,
        usage: BucketUsage::default(),
    });
    assert_eq!(
        StreamStateMachine::restore(snapshot).err(),
        Some(StreamSnapshotError::DuplicateBucketUsage(BUCKET.to_owned()))
    );
}
