//! A group snapshot's usage (F20) through `usage_snapshot` and
//! `restore_group`: a round trip keeps every version's usage, every end
//! record, the seqs and the genesis, and a usage that does not fit its
//! stream snapshot is refused by the check it fails.

use std::num::NonZeroU32;
use std::num::NonZeroU64;

use bytes::Bytes;

use super::BucketStreamId;
use super::MAX_END_RECORDS;
use super::StreamCommand;
use super::StreamResponse;
use super::StreamSnapshot;
use super::StreamSnapshotError;
use super::StreamStateMachine;
use crate::model::BucketUsageSnapshot;
use crate::usage::UsageCounter;
use crate::usage::UsageSnapshot;
use crate::usage::UsageSnapshotError;

const OCTET: &str = "application/octet-stream";

fn sid(bucket: &str, name: &str) -> BucketStreamId {
    BucketStreamId::new(bucket, name)
}

fn apply_ok(machine: &mut StreamStateMachine, command: StreamCommand) {
    let response = machine.apply(command);
    assert!(
        !matches!(response, StreamResponse::Error { .. }),
        "{response:?}"
    );
}

fn create(machine: &mut StreamStateMachine, stream_id: BucketStreamId, body: &'static [u8]) {
    apply_ok(machine, StreamCommand::CreateStream {
        stream_id,
        content_type: OCTET.to_owned(),
        initial_payload: Bytes::from_static(body),
        close_after: false,
        stream_seq: None,
        stream_ttl_seconds: None,
        stream_expires_at_ms: None,
        now_ms: 1,
    });
}

/// A group of history 7 with two buckets, live versions written after a
/// later history event, three end records and one of them folded.
fn ledger_group() -> StreamStateMachine {
    let mut machine = StreamStateMachine::new();
    let history = |id| NonZeroU64::new(id).expect("non-zero");
    machine.apply_entry(1, StreamCommand::NewHistory {
        history: history(7),
        now_ms: 0,
        previous: 0,
        kept: 0,
        leader: crate::HistoryLeader {
            term: 1,
            node_id: 1,
        },
    });
    for bucket in ["bucket-one", "bucket-two"] {
        apply_ok(&mut machine, StreamCommand::CreateBucket {
            bucket_id: bucket.to_owned(),
        });
    }
    for (bucket, name, body) in [
        ("bucket-one", "a", &b"aaa"[..]),
        ("bucket-one", "b", b"bb"),
        ("bucket-one", "c", b""),
        ("bucket-two", "d", b"dddd"),
        ("bucket-two", "e", b"e"),
    ] {
        create(&mut machine, sid(bucket, name), body);
    }
    machine.apply_entry(9, StreamCommand::NewHistory {
        history: history(9),
        now_ms: 0,
        previous: 7,
        kept: 8,
        leader: crate::HistoryLeader {
            term: 2,
            node_id: 1,
        },
    });
    apply_ok(&mut machine, StreamCommand::Append {
        stream_id: sid("bucket-one", "b"),
        content_type: Some(OCTET.to_owned()),
        payload: Bytes::from_static(b"more"),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 2,
    });
    for (bucket, name, now_ms) in [
        ("bucket-one", "a", 3),
        ("bucket-two", "e", 4),
        ("bucket-one", "c", 5),
    ] {
        apply_ok(&mut machine, StreamCommand::DeleteStream {
            stream_id: sid(bucket, name),
            now_ms,
        });
    }
    let first_end = machine.end_records().next().expect("an end record").seq;
    assert_eq!(
        machine.apply(StreamCommand::Sweep {
            now_ms: 6,
            limit: NonZeroU32::MIN,
            retire_through: first_end,
        }),
        StreamResponse::Swept {
            expired: 0,
            retired: 1,
        }
    );
    machine
}

/// A group snapshot's state restores exactly: every version's usage, the
/// end records, the seq, the watermark, the retired rows and the genesis,
/// whatever the order of its streams.
#[test]
fn a_group_snapshot_round_trip_keeps_the_whole_ledger() {
    let machine = ledger_group();
    let usage = machine.usage_snapshot();
    assert_eq!(usage.genesis, 7);
    assert_eq!(usage.streams.len(), 2);
    assert_eq!(usage.ends.len(), 2);
    assert_eq!(usage.retired.len(), 2);
    assert!(usage.retired_through > 0);
    assert!(
        usage
            .streams
            .iter()
            .any(|version| version.history_baseline != version.counters
                && version.history_baseline.committed.committed_write_units > 0),
        "{usage:?}"
    );

    let restored =
        StreamStateMachine::restore_group(machine.snapshot(), usage.clone()).expect("restore");
    assert_eq!(restored.usage_snapshot(), usage);
    assert_eq!(restored.snapshot(), machine.snapshot());
    assert_eq!(
        restored.bucket_usage_report(),
        machine.bucket_usage_report()
    );
    assert_eq!(restored.state_gauges(), machine.state_gauges());
    assert_eq!(restored.usage_genesis(), 7);

    // A twin whose snapshot lists its streams the other way round holds the
    // same ledger.
    let mut snapshot = machine.snapshot();
    let mut reversed = usage.clone();
    snapshot.streams.reverse();
    reversed.streams.reverse();
    let twin = StreamStateMachine::restore_group(snapshot, reversed).expect("restore");
    assert_eq!(twin.usage_snapshot(), usage);

    // The backup restore of the same state keeps only the totals.
    let backup = StreamStateMachine::restore(machine.snapshot()).expect("restore");
    assert_eq!(backup.bucket_usage_report(), machine.bucket_usage_report());
    assert_eq!(backup.end_records().len(), 0);
    assert_eq!(backup.usage_genesis(), 0);
}

fn refused(change: impl FnOnce(&mut StreamSnapshot, &mut UsageSnapshot)) -> StreamSnapshotError {
    let machine = ledger_group();
    let mut snapshot = machine.snapshot();
    let mut usage = machine.usage_snapshot();
    change(&mut snapshot, &mut usage);
    StreamStateMachine::restore_group(snapshot, usage).expect_err("a usage that does not fit")
}

fn usage_error(error: StreamSnapshotError) -> UsageSnapshotError {
    match error {
        StreamSnapshotError::Usage(error) => error,
        other => panic!("not a usage refusal: {other:?}"),
    }
}

/// Every structural check of `restore_group`: the stream count, the retired
/// rows, the end records against the cap and the watermark, and the seqs.
#[test]
fn a_restore_refuses_a_usage_of_the_wrong_shape() {
    assert_eq!(
        usage_error(refused(|_, usage| {
            usage.streams.pop();
        })),
        UsageSnapshotError::StreamCount {
            streams: 2,
            usages: 1,
        }
    );
    assert_eq!(
        usage_error(refused(|_, usage| {
            let row = usage.retired[0].clone();
            usage.retired.push(row);
        })),
        UsageSnapshotError::DuplicateRetired {
            bucket_id: "bucket-one".to_owned(),
        }
    );
    let error = usage_error(refused(|_, usage| {
        usage.ends[0].seq = usage.retired_through;
    }));
    assert!(
        matches!(error, UsageSnapshotError::RetiredEndRecord { .. }),
        "{error:?}"
    );
    let error = usage_error(refused(|_, usage| {
        usage.retired_through = usage.usage_seq + 1;
    }));
    assert!(
        matches!(error, UsageSnapshotError::RetiredThroughAhead { .. }),
        "{error:?}"
    );
    let error = usage_error(refused(|_, usage| {
        let record = usage.ends[0].clone();
        let first = usage.usage_seq + 1;
        usage.ends = (0..=MAX_END_RECORDS)
            .map(|offset| {
                let mut record = record.clone();
                record.seq = first + u64::try_from(offset).expect("fits");
                record
            })
            .collect();
        usage.usage_seq += u64::try_from(MAX_END_RECORDS).expect("fits") + 1;
    }));
    assert_eq!(error, UsageSnapshotError::TooManyEndRecords {
        count: 65_537,
        max: 65_536,
    });
}

/// Every seq is non-zero, at most the group's, and unique among live
/// versions and end records, and end records come by ascending seq.
#[test]
fn a_restore_refuses_a_zero_repeated_or_late_seq() {
    let seq_refusal = |change: fn(&mut UsageSnapshot)| {
        let error = usage_error(refused(|_, usage| change(usage)));
        assert!(
            matches!(error, UsageSnapshotError::InvalidSeq { .. }),
            "{error:?}"
        );
    };
    seq_refusal(|usage| usage.streams[0].seq = 0);
    seq_refusal(|usage| usage.streams[0].seq = usage.usage_seq + 1);
    seq_refusal(|usage| usage.streams[0].seq = usage.streams[1].seq);
    seq_refusal(|usage| usage.streams[0].seq = usage.ends[0].seq);
    seq_refusal(|usage| usage.ends.swap(0, 1));
    seq_refusal(|usage| usage.ends[1].seq = usage.usage_seq + 1);
}

/// Each bucket's total must equal its retired usage plus its live versions
/// plus its end records exactly, counter by counter: an understated or a
/// dropped part fails as surely as an overstated one.
#[test]
fn a_restore_refuses_totals_that_are_not_the_sum_of_their_parts() {
    let disagree = |error| match usage_error(error) {
        UsageSnapshotError::TotalsDisagree { bucket_id, counter } => (bucket_id, counter),
        other => panic!("{other:?}"),
    };
    let row = |snapshot: &mut StreamSnapshot, bucket: &str| -> usize {
        snapshot
            .bucket_usage
            .iter()
            .position(|row| row.bucket_id == bucket)
            .expect("a total row")
    };
    assert_eq!(
        disagree(refused(|snapshot, _| {
            let at = row(snapshot, "bucket-one");
            snapshot.bucket_usage[at].usage.committed_append_bytes += 1;
        })),
        ("bucket-one".to_owned(), UsageCounter::AppendBytes)
    );
    assert_eq!(
        disagree(refused(|snapshot, _| {
            let at = row(snapshot, "bucket-two");
            snapshot.bucket_usage[at].usage.committed_records -= 1;
        })),
        ("bucket-two".to_owned(), UsageCounter::Records)
    );
    assert_eq!(
        disagree(refused(|snapshot, _| {
            let at = row(snapshot, "bucket-two");
            snapshot.bucket_usage[at].usage.committed_write_units += 1;
        })),
        ("bucket-two".to_owned(), UsageCounter::WriteUnits)
    );
    // A dropped end record, a dropped retired row and a version whose
    // counters were lost each leave a total above its parts.
    assert_eq!(
        disagree(refused(|_, usage| {
            usage.ends.remove(0);
        }))
        .1,
        UsageCounter::AppendBytes
    );
    assert_eq!(
        disagree(refused(|_, usage| {
            usage.retired.retain(|row| row.bucket_id != "bucket-one");
        })),
        ("bucket-one".to_owned(), UsageCounter::AppendBytes)
    );
    assert_eq!(
        disagree(refused(|_, usage| {
            usage.streams[0].counters = crate::usage::VersionCounters::default();
        }))
        .1,
        UsageCounter::AppendBytes
    );
    // A total missing for a bucket with usage reads as zero.
    assert_eq!(
        disagree(refused(|snapshot, _| {
            snapshot
                .bucket_usage
                .retain(|row| row.bucket_id != "bucket-two");
        })),
        ("bucket-two".to_owned(), UsageCounter::AppendBytes)
    );
}

/// The shared restore refuses a second total row of one bucket, for a
/// group snapshot as for a backup.
#[test]
fn a_group_restore_refuses_two_total_rows_of_one_bucket() {
    let error = refused(|snapshot, _| {
        let row = snapshot.bucket_usage[0].clone();
        snapshot.bucket_usage.push(BucketUsageSnapshot {
            bucket_id: row.bucket_id,
            usage: row.usage,
        });
    });
    assert_eq!(
        error,
        StreamSnapshotError::DuplicateBucketUsage("bucket-one".to_owned())
    );
}
