//! The export's read (F20): each rule of the cursor decision, in its
//! order, and the paging of a group's rows and entries, through the
//! production entry points. `usage_oracle_tests` checks the listings
//! against an oracle over generated runs.

use std::num::NonZeroU32;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;

use bytes::Bytes;

use super::BucketStreamId;
use super::StreamCommand;
use super::StreamResponse;
use super::StreamStateMachine;
use super::usage_ledger::test_cap;
use crate::history::incarnation_token;
use crate::usage::CommittedUsage;
use crate::usage::GroupUsageView;
use crate::usage::Lifecycle;
use crate::usage::RelistReason;
use crate::usage::StreamEnd;
use crate::usage::StreamUsageRead;
use crate::usage::UsageCursor;
use crate::usage::UsageEntry;
use crate::usage::UsageListing;
use crate::usage::UsageLogId;
use crate::usage::UsagePage;
use crate::usage::UsagePosition;

const OCTET: &str = "application/octet-stream";
const BUCKETS: [&str; 2] = ["a", "b"];
const GENESIS: u64 = 0x11;

fn id(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("non-zero history id")
}

fn sid(bucket: &str, name: &str) -> BucketStreamId {
    BucketStreamId::new(bucket, name)
}

fn log_id(term: u64, index: u64) -> UsageLogId {
    UsageLogId { term, index }
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

/// A group whose genesis applied, with the buckets `a` and `b`. A bucket
/// has a row once a stream was created in it (F15).
fn group() -> StreamStateMachine {
    let mut machine = StreamStateMachine::new();
    new_history(&mut machine, 1, GENESIS);
    for bucket in BUCKETS {
        assert!(matches!(
            machine.apply(StreamCommand::CreateBucket {
                bucket_id: bucket.to_owned(),
            }),
            StreamResponse::BucketCreated { .. }
        ));
    }
    machine
}

fn create(machine: &mut StreamStateMachine, stream: &BucketStreamId, body: &[u8]) {
    let response = machine.apply(StreamCommand::CreateStream {
        stream_id: stream.clone(),
        content_type: OCTET.to_owned(),
        initial_payload: Bytes::copy_from_slice(body),
        close_after: false,
        stream_seq: None,
        stream_ttl_seconds: None,
        stream_expires_at_ms: None,
        now_ms: 1,
    });
    assert!(
        matches!(response, StreamResponse::Created { .. }),
        "{response:?}"
    );
}

fn append(machine: &mut StreamStateMachine, stream: &BucketStreamId, body: &[u8]) {
    let response = machine.apply(StreamCommand::Append {
        stream_id: stream.clone(),
        content_type: Some(OCTET.to_owned()),
        payload: Bytes::copy_from_slice(body),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 2,
    });
    assert!(
        matches!(response, StreamResponse::Appended { .. }),
        "{response:?}"
    );
}

fn delete(machine: &mut StreamStateMachine, stream: &BucketStreamId) {
    assert_eq!(
        machine.apply(StreamCommand::DeleteStream {
            stream_id: stream.clone(),
            now_ms: 3,
        }),
        StreamResponse::Deleted { expired: false }
    );
}

/// Folds every end record at or below `retire_through`.
fn fold_through(machine: &mut StreamStateMachine, retire_through: u64) {
    let response = machine.apply(StreamCommand::Sweep {
        now_ms: 4,
        limit: NonZeroU32::new(256).expect("non-zero"),
        retire_through,
    });
    assert!(
        matches!(response, StreamResponse::Swept { .. }),
        "{response:?}"
    );
}

fn view(
    machine: &StreamStateMachine,
    cursor: UsageCursor,
    limit: usize,
    applied: UsageLogId,
) -> GroupUsageView {
    let read = StreamUsageRead {
        cursor,
        limit: NonZeroUsize::new(limit).expect("a positive limit"),
    };
    machine.stream_usage(&read, applied)
}

fn listed(view: GroupUsageView) -> UsageListing {
    match view {
        GroupUsageView::Listed(listing) => listing,
        other => panic!("not listed: {other:?}"),
    }
}

fn page(
    machine: &StreamStateMachine,
    cursor: UsageCursor,
    limit: usize,
    applied: UsageLogId,
) -> UsageListing {
    listed(view(machine, cursor, limit, applied))
}

/// The cursor that continues `listing` at `position`.
fn after(listing: &UsageListing, position: UsagePosition) -> UsageCursor {
    UsageCursor::After {
        genesis: listing.genesis,
        history: listing.history.id,
        applied: listing.applied,
        retired_through: listing.retired_through,
        position,
    }
}

/// The cursor that continues the complete `listing` with its next changes.
fn next_changes(listing: &UsageListing) -> UsageCursor {
    assert_eq!(listing.page, UsagePage::Complete);
    after(listing, UsagePosition::Entries {
        after_seq: listing.usage_seq,
    })
}

fn seqs(entries: &[UsageEntry]) -> Vec<u64> {
    entries.iter().map(|entry| entry.seq).collect()
}

fn names(entries: &[UsageEntry]) -> Vec<&str> {
    entries
        .iter()
        .map(|entry| entry.stream_id.stream_id.as_str())
        .collect()
}

/// Rule 1: before the genesis applied, every cursor answers `NoHistory`.
#[test]
fn a_group_before_its_genesis_answers_no_history() {
    let machine = StreamStateMachine::new();
    assert_eq!(
        view(&machine, UsageCursor::Start, 10, log_id(0, 0)),
        GroupUsageView::NoHistory
    );
    let cursor = UsageCursor::After {
        genesis: id(GENESIS),
        history: id(GENESIS),
        applied: log_id(0, 0),
        retired_through: 0,
        position: UsagePosition::Entries { after_seq: 0 },
    };
    assert_eq!(
        view(&machine, cursor, 10, log_id(0, 0)),
        GroupUsageView::NoHistory
    );
}

/// Rule 2: `start` relists: every bucket's row by bucket id with its
/// retired usage, totals and gauges at the read, then every live version
/// and end record in seq order, and the group's counts.
#[test]
fn a_start_relists_every_row_and_then_every_entry() {
    let mut machine = group();
    create(&mut machine, &sid("b", "y"), b"yy");
    create(&mut machine, &sid("a", "x"), b"xxx");
    create(&mut machine, &sid("a", "gone"), b"g");
    delete(&mut machine, &sid("a", "gone"));
    let applied = log_id(2, 9);
    let listing = page(&machine, UsageCursor::Start, 100, applied);

    assert_eq!(listing.relist, Some(RelistReason::Start));
    assert_eq!(listing.genesis, id(GENESIS));
    assert_eq!(listing.history, machine.history().expect("a history"));
    assert_eq!(listing.applied, applied);
    assert_eq!(listing.usage_seq, 4);
    assert_eq!(listing.retired_through, 0);
    assert_eq!(listing.live_versions, 2);
    assert_eq!(listing.end_records, 1);
    assert_eq!(listing.page, UsagePage::Complete);

    let report = machine.bucket_usage_report();
    assert_eq!(listing.rows.len(), 2);
    for (row, total) in listing.rows.iter().zip(&report) {
        assert_eq!(row.bucket_id, total.bucket_id);
        assert_eq!(row.totals, total.usage);
        assert_eq!(row.retired, CommittedUsage::default());
        assert_eq!(row.snapshot_bytes, 0);
    }
    assert_eq!(listing.rows[0].bucket_id, "a");
    assert_eq!(listing.rows[0].totals.stream_count, 1);
    assert_eq!(listing.rows[0].totals.retained_bytes, 3);

    assert_eq!(names(&listing.entries), ["y", "x", "gone"]);
    assert_eq!(seqs(&listing.entries), [1, 2, 4]);
    let gone = &listing.entries[2];
    assert_eq!(gone.lifecycle, Lifecycle::Ended {
        end: StreamEnd::Deleted,
        ended_at_ms: 3,
    });
    let x = &listing.entries[1];
    assert_eq!(x.lifecycle, Lifecycle::Live {
        retained_bytes: 3,
        snapshot_bytes: 0,
    });
    assert_eq!(
        Some(x.incarnation),
        machine.stream_incarnation(&sid("a", "x"))
    );
}

/// Rules 3 to 5, in their order: another genesis relists before log order
/// is compared, a replica below the cursor is behind before its history is
/// compared, and another history relists.
#[test]
fn genesis_decides_before_log_order_and_log_order_before_history() {
    let mut machine = group();
    create(&mut machine, &sid("a", "x"), b"x");
    let applied = log_id(3, 20);
    let kept = page(&machine, UsageCursor::Start, 100, applied);
    let ahead = log_id(3, 21);
    let cursor = |genesis: u64, history: u64, applied: UsageLogId| UsageCursor::After {
        genesis: id(genesis),
        history: id(history),
        applied,
        retired_through: kept.retired_through,
        position: UsagePosition::Entries {
            after_seq: kept.usage_seq,
        },
    };

    // Another cluster's group, whose log is ahead: relisted.
    let other_cluster = page(&machine, cursor(0x99, 0x99, ahead), 100, applied);
    assert_eq!(other_cluster.relist, Some(RelistReason::History));
    assert_eq!(names(&other_cluster.entries), ["x"]);

    // Behind, whatever history the cursor names. A higher term with a
    // lower index is ahead too.
    for cursor_applied in [ahead, log_id(4, 1)] {
        for history in [GENESIS, 0x77] {
            assert_eq!(
                view(
                    &machine,
                    cursor(GENESIS, history, cursor_applied),
                    100,
                    applied
                ),
                GroupUsageView::Behind {
                    genesis: id(GENESIS),
                    history: id(GENESIS),
                    applied,
                }
            );
        }
    }

    // At or below the replica's log id, another history relists.
    for cursor_applied in [applied, log_id(2, 900)] {
        let other_history = page(
            &machine,
            cursor(GENESIS, 0x77, cursor_applied),
            100,
            applied,
        );
        assert_eq!(other_history.relist, Some(RelistReason::History));
        assert_eq!(other_history.rows.len(), 1);
        assert_eq!(names(&other_history.entries), ["x"]);
    }

    // A history event: the cursor of the earlier history relists, after
    // the replica applied it, and is behind before.
    new_history(&mut machine, 21, 0x22);
    let later = log_id(3, 21);
    let relisted = page(&machine, next_changes(&kept), 100, later);
    assert_eq!(relisted.relist, Some(RelistReason::History));
    assert_eq!(relisted.history.id, id(0x22));
    assert_eq!(relisted.history.previous, GENESIS);
    assert_eq!(relisted.genesis, id(GENESIS));
    let continued = page(&machine, next_changes(&relisted), 100, later);
    assert_eq!(continued.relist, None);
    assert!(continued.entries.is_empty());
    assert_eq!(
        view(&machine, next_changes(&relisted), 100, applied),
        GroupUsageView::Behind {
            genesis: id(GENESIS),
            history: id(0x22),
            applied,
        }
    );
}

/// Rule 6: a fold of an end record above both the cursor's position and
/// its own watermark relists as `retired`. One the caller listed does not,
/// and during the rows of a relisting, whose position is seq 0, any fold
/// since its first page does.
#[test]
fn a_fold_the_caller_had_not_listed_relists_as_retired() {
    let mut machine = group();
    let applied = log_id(1, 50);
    create(&mut machine, &sid("b", "z"), b"z");
    create(&mut machine, &sid("a", "x"), b"x");
    create(&mut machine, &sid("a", "y"), b"y");
    let before = page(&machine, UsageCursor::Start, 100, applied);
    delete(&mut machine, &sid("a", "x"));
    let listed_end = page(&machine, next_changes(&before), 100, applied);
    assert_eq!(listed_end.relist, None);
    assert_eq!(names(&listed_end.entries), ["x"]);
    let end_seq = listed_end.entries[0].seq;

    fold_through(&mut machine, end_seq);
    assert_eq!(machine.usage_retired_through(), end_seq);
    // The caller listed the end record: its listing continues.
    let continued = page(&machine, next_changes(&listed_end), 100, applied);
    assert_eq!(continued.relist, None);
    assert!(continued.entries.is_empty());
    assert_eq!(continued.retired_through, end_seq);
    // A caller that had not: relisted, and its rows carry the folded usage.
    let missed = page(&machine, next_changes(&before), 100, applied);
    assert_eq!(missed.relist, Some(RelistReason::Retired));
    assert_eq!(missed.rows[0].retired, CommittedUsage::of_write(1, 1));
    assert_eq!(names(&missed.entries), ["z", "y"]);
    // The relisting's own watermark covers the fold: its later pages and
    // its continuation are not retired.
    let after_relisting = page(&machine, next_changes(&missed), 100, applied);
    assert_eq!(after_relisting.relist, None);

    // A relisting cut in its rows restarts on a fold since its first page.
    let first = page(&machine, UsageCursor::Start, 1, applied);
    let UsagePage::More { next } = first.page.clone() else {
        panic!("a cut relisting: {first:?}");
    };
    assert_eq!(next, UsagePosition::Rows {
        after_bucket: Some("a".to_owned()),
    });
    let unchanged = page(&machine, after(&first, next.clone()), 1, applied);
    assert_eq!(unchanged.relist, None);
    delete(&mut machine, &sid("a", "y"));
    let through = machine.usage_seq();
    fold_through(&mut machine, through);
    let restarted = page(&machine, after(&first, next), 1, applied);
    assert_eq!(restarted.relist, Some(RelistReason::Retired));
    assert_eq!(restarted.rows[0].bucket_id, "a");
}

/// Rule 6 at the cap: a removal that folds the oldest end record early
/// relists a caller whose listing did not reach it.
#[test]
fn a_fold_at_the_cap_relists_a_listing_that_missed_the_end_record() {
    test_cap::with(1, || {
        let mut machine = group();
        let applied = log_id(1, 50);
        create(&mut machine, &sid("a", "x"), b"x");
        create(&mut machine, &sid("a", "y"), b"y");
        let before = page(&machine, UsageCursor::Start, 100, applied);
        delete(&mut machine, &sid("a", "x"));
        delete(&mut machine, &sid("a", "y"));
        assert_eq!(machine.end_records().len(), 1);
        assert_eq!(machine.usage_retired_through(), 3);
        let missed = page(&machine, next_changes(&before), 100, applied);
        assert_eq!(missed.relist, Some(RelistReason::Retired));
        assert_eq!(missed.end_records, 1);
        assert_eq!(names(&missed.entries), ["y"]);
        assert_eq!(missed.rows[0].retired, CommittedUsage::of_write(1, 1));
    });
}

/// Rules 7 and 8: rows without a bucket ask for a relisting, and rows after
/// a bucket continue one with the next rows and then entries from seq 0.
#[test]
fn rows_relist_on_request_and_continue_after_their_bucket() {
    let mut machine = group();
    let applied = log_id(1, 50);
    create(&mut machine, &sid("a", "x"), b"x");
    create(&mut machine, &sid("b", "y"), b"y");
    let kept = page(&machine, UsageCursor::Start, 100, applied);

    let requested = page(
        &machine,
        after(&kept, UsagePosition::Rows { after_bucket: None }),
        100,
        applied,
    );
    assert_eq!(requested.relist, Some(RelistReason::Requested));
    assert_eq!(requested.rows, kept.rows);
    assert_eq!(requested.entries, kept.entries);

    let continued = page(
        &machine,
        after(&kept, UsagePosition::Rows {
            after_bucket: Some("a".to_owned()),
        }),
        100,
        applied,
    );
    assert_eq!(continued.relist, None);
    assert_eq!(continued.rows.len(), 1);
    assert_eq!(continued.rows[0].bucket_id, "b");
    assert_eq!(names(&continued.entries), ["x", "y"]);
    assert_eq!(continued.page, UsagePage::Complete);

    // Past the last bucket: entries from seq 0 only.
    let past = page(
        &machine,
        after(&kept, UsagePosition::Rows {
            after_bucket: Some("z".to_owned()),
        }),
        100,
        applied,
    );
    assert!(past.rows.is_empty());
    assert_eq!(names(&past.entries), ["x", "y"]);
}

/// Rule 9: entries continue above the position, each changed version once
/// at its newest seq, and an unchanged one not at all.
#[test]
fn entries_continue_above_their_position_each_version_once() {
    let mut machine = group();
    let applied = log_id(1, 50);
    create(&mut machine, &sid("a", "x"), b"x");
    create(&mut machine, &sid("a", "y"), b"y");
    let kept = page(&machine, UsageCursor::Start, 100, applied);
    append(&mut machine, &sid("a", "x"), b"more");
    append(&mut machine, &sid("a", "x"), b"again");
    let changes = page(&machine, next_changes(&kept), 100, applied);
    assert_eq!(changes.relist, None);
    assert!(changes.rows.is_empty());
    assert_eq!(names(&changes.entries), ["x"]);
    assert_eq!(seqs(&changes.entries), [machine.usage_seq()]);
    assert_eq!(changes.entries[0].counters.appends, 2);
    assert_eq!(changes.live_versions, 2);

    // A position above the group's seq lists nothing, and is complete.
    let beyond = page(
        &machine,
        after(&changes, UsagePosition::Entries {
            after_seq: u64::MAX,
        }),
        100,
        applied,
    );
    assert!(beyond.entries.is_empty());
    assert_eq!(beyond.page, UsagePage::Complete);
}

/// A page holds at most its limit of rows and entries together. A limit
/// that cuts the rows continues after the last row, one that ends with the
/// rows continues with entries from seq 0, and one that cuts the entries
/// continues after the last entry. A page with nothing after it is complete.
#[test]
fn a_page_holds_at_most_its_limit_and_names_what_follows() {
    let applied = log_id(1, 50);

    // One row and no entry: a limit of one completes.
    let mut machine = group();
    create(&mut machine, &sid("a", "gone"), b"g");
    delete(&mut machine, &sid("a", "gone"));
    let through = machine.usage_seq();
    fold_through(&mut machine, through);
    let rows_only = page(&machine, UsageCursor::Start, 1, applied);
    assert_eq!(rows_only.rows.len(), 1);
    assert!(rows_only.entries.is_empty());
    assert_eq!(rows_only.page, UsagePage::Complete);

    let mut machine = group();
    create(&mut machine, &sid("a", "x"), b"x");
    create(&mut machine, &sid("a", "y"), b"y");
    create(&mut machine, &sid("a", "z"), b"z");
    create(&mut machine, &sid("b", "w"), b"w");
    let cut_rows = page(&machine, UsageCursor::Start, 1, applied);
    assert_eq!(cut_rows.page, UsagePage::More {
        next: UsagePosition::Rows {
            after_bucket: Some("a".to_owned()),
        },
    });
    let end_of_rows = page(&machine, UsageCursor::Start, 2, applied);
    assert_eq!(end_of_rows.rows.len(), 2);
    assert!(end_of_rows.entries.is_empty());
    assert_eq!(end_of_rows.page, UsagePage::More {
        next: UsagePosition::Entries { after_seq: 0 },
    });
    let into_entries = page(&machine, UsageCursor::Start, 4, applied);
    assert_eq!(names(&into_entries.entries), ["x", "y"]);
    assert_eq!(into_entries.page, UsagePage::More {
        next: UsagePosition::Entries { after_seq: 2 },
    });
    let rest = page(
        &machine,
        after(&into_entries, UsagePosition::Entries { after_seq: 2 }),
        4,
        applied,
    );
    assert_eq!(rest.relist, None);
    assert_eq!(names(&rest.entries), ["z", "w"]);
    assert_eq!(rest.page, UsagePage::Complete);
}

/// An entry's derived fields follow the group's current history H: a live
/// version's token is its token in H, and its baseline is its counters
/// until its first write in H records them. An end record's token is the
/// one of the history it ended in, and its baseline is its recorded one
/// when it ended in H, else its final counters.
#[test]
fn an_entrys_token_and_baseline_follow_the_current_history() {
    let mut machine = group();
    let applied = log_id(1, 50);
    let x = sid("a", "x");
    let early = sid("a", "early");
    let late = sid("a", "late");
    create(&mut machine, &x, b"x");
    append(&mut machine, &x, b"xx");
    create(&mut machine, &early, b"e");
    append(&mut machine, &early, b"ee");
    create(&mut machine, &late, b"l");
    delete(&mut machine, &early);
    let entry = |machine: &StreamStateMachine, name: &str| {
        page(machine, UsageCursor::Start, 100, applied)
            .entries
            .into_iter()
            .find(|entry| entry.stream_id.stream_id == name)
            .expect("listed")
    };
    let early_created = entry(&machine, "early").created_at_ms;

    new_history(&mut machine, 30, 0x33);
    let unwritten = entry(&machine, "x");
    assert_eq!(unwritten.history_baseline, unwritten.counters);
    assert_eq!(Some(unwritten.incarnation), machine.stream_incarnation(&x));
    let ended_before = entry(&machine, "early");
    assert_eq!(ended_before.history_baseline, ended_before.counters);
    assert_eq!(
        ended_before.incarnation,
        incarnation_token(early_created, GENESIS)
    );

    append(&mut machine, &x, b"xxx");
    let written = entry(&machine, "x");
    assert_eq!(written.history_baseline, unwritten.counters);
    assert_eq!(written.counters.appends, 2);
    append(&mut machine, &late, b"ll");
    let before_end = entry(&machine, "late");
    delete(&mut machine, &late);
    let ended_in_h = entry(&machine, "late");
    assert_eq!(ended_in_h.history_baseline, before_end.history_baseline);
    assert_ne!(ended_in_h.history_baseline, ended_in_h.counters);
    assert_eq!(
        ended_in_h.incarnation,
        incarnation_token(ended_in_h.created_at_ms, 0x33)
    );
}

/// A live entry's gauges are read with the page: its retained bytes and
/// the body length of its visible snapshot.
#[test]
fn a_live_entry_reads_its_gauges_with_the_page() {
    let mut machine = group();
    let applied = log_id(1, 50);
    let x = sid("b", "x");
    create(&mut machine, &x, b"abcdef");
    let published = machine.apply(StreamCommand::PublishSnapshot {
        stream_id: x.clone(),
        snapshot_offset: 4,
        content_type: OCTET.to_owned(),
        payload: Bytes::from_static(b"snap!"),
        now_ms: 2,
        expected_incarnation: None,
    });
    assert!(
        matches!(published, StreamResponse::SnapshotPublished { .. }),
        "{published:?}"
    );
    assert_eq!(
        machine.apply(StreamCommand::AdvanceRetention {
            stream_id: x.clone(),
            retained_offset: 4,
            now_ms: 3,
            expected_incarnation: None,
        }),
        StreamResponse::RetentionAdvanced { retained_offset: 4 }
    );
    let listing = page(&machine, UsageCursor::Start, 100, applied);
    assert_eq!(listing.entries[0].lifecycle, Lifecycle::Live {
        retained_bytes: 2,
        snapshot_bytes: 5,
    });
    assert_eq!(listing.entries[0].seq, 3);
    assert_eq!(listing.rows.len(), 1);
    assert_eq!(listing.rows[0].bucket_id, "b");
    assert_eq!(listing.rows[0].snapshot_bytes, 5);
    assert_eq!(listing.rows[0].totals.retained_bytes, 2);
}
