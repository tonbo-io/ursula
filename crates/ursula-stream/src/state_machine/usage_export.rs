//! The per-stream usage export's read (F20): one page of a group's usage
//! from one applied state, and the rule that decides from the caller's
//! cursor whether the page continues its listing, relists the group or is
//! behind. Pure: the caller passes the log id of the entry the replica
//! applied last, read with the state.
//!
//! A page lists live versions from the node-local index of live versions
//! by seq and end records from the ledger, merged in seq order, so it costs
//! the entries it lists. Only a relisting carries bucket rows, whose totals
//! are derived and cost one pass over the group's versions and end records.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use super::StreamStateMachine;
use super::usage_ledger::retained_bytes;
use super::usage_ledger::visible_snapshot_bytes;
use crate::history::incarnation_token;
use crate::usage::BucketUsageRow;
use crate::usage::EndRecord;
use crate::usage::GroupUsageView;
use crate::usage::Lifecycle;
use crate::usage::RelistReason;
use crate::usage::StreamUsageRead;
use crate::usage::UsageCursor;
use crate::usage::UsageEntry;
use crate::usage::UsageListing;
use crate::usage::UsageLogId;
use crate::usage::UsagePage;
use crate::usage::UsagePosition;

/// Where a page starts: the decision of the cursor rule.
enum PageStart<'a> {
    /// A relisting: every bucket row, then every entry from seq 0.
    Relist(RelistReason),
    /// The caller's relisting continues with the rows after this bucket.
    RowsAfter(&'a str),
    /// The caller's listing continues with the entries above this seq.
    EntriesAfter(u64),
}

impl StreamStateMachine {
    /// One page of the group's per-stream usage, read at `applied`, the
    /// log id of the entry this state applied last. Let G be the group's
    /// genesis id, H its history id, L `applied` and R its fold watermark.
    /// For a cursor `after { g, h, l, r, p }`, `after_seq(p)` is the
    /// position's seq for entries and 0 for rows. In order:
    ///
    /// 1. G is 0 (the genesis has not applied): [`GroupUsageView::NoHistory`].
    /// 2. `start`: relist, [`RelistReason::Start`].
    /// 3. g is not G: relist, [`RelistReason::History`] (another cluster's
    ///    instance of the group).
    /// 4. l is above L: [`GroupUsageView::Behind`].
    /// 5. h is not H: relist, [`RelistReason::History`].
    /// 6. R is above `max(after_seq(p), r)`: relist,
    ///    [`RelistReason::Retired`] (an end record the caller had not
    ///    listed was folded).
    /// 7. p is rows without a bucket: relist, [`RelistReason::Requested`].
    /// 8. p is rows after bucket b: rows after b, then entries from seq 0.
    /// 9. p is entries after a: entries above a.
    ///
    /// A page holds at most the read's limit of items, bucket rows and
    /// entries together.
    pub fn stream_usage(&self, read: &StreamUsageRead, applied: UsageLogId) -> GroupUsageView {
        let (Some(genesis), Some(history)) = (NonZeroU64::new(self.usage_genesis()), self.history)
        else {
            return GroupUsageView::NoHistory;
        };
        let retired_through = self.usage_retired_through();
        let start = match &read.cursor {
            UsageCursor::Start => PageStart::Relist(RelistReason::Start),
            UsageCursor::After {
                genesis: cursor_genesis,
                history: cursor_history,
                applied: cursor_applied,
                retired_through: cursor_retired_through,
                position,
            } => {
                let after_seq = match position {
                    UsagePosition::Rows { .. } => 0,
                    UsagePosition::Entries { after_seq } => *after_seq,
                };
                if *cursor_genesis != genesis {
                    PageStart::Relist(RelistReason::History)
                } else if *cursor_applied > applied
                    && !crate::sim_controls::off(crate::sim_controls::Defence::UsageReadOrder)
                {
                    return GroupUsageView::Behind {
                        genesis,
                        history: history.id,
                        applied,
                    };
                } else if *cursor_history != history.id {
                    PageStart::Relist(RelistReason::History)
                } else if retired_through > after_seq.max(*cursor_retired_through) {
                    PageStart::Relist(RelistReason::Retired)
                } else {
                    match position {
                        UsagePosition::Rows { after_bucket: None } => {
                            PageStart::Relist(RelistReason::Requested)
                        }
                        UsagePosition::Rows {
                            after_bucket: Some(bucket_id),
                        } => PageStart::RowsAfter(bucket_id),
                        UsagePosition::Entries { after_seq } => PageStart::EntriesAfter(*after_seq),
                    }
                }
            }
        };
        let (relist, rows_after, entries_after) = match start {
            PageStart::Relist(reason) => (Some(reason), Some(None), 0),
            PageStart::RowsAfter(bucket_id) => (None, Some(Some(bucket_id)), 0),
            PageStart::EntriesAfter(after_seq) => (None, None, after_seq),
        };
        let limit = read.limit.get();
        let mut rows = Vec::new();
        let mut entries = Vec::new();
        let page = match rows_after
            .and_then(|after_bucket| self.usage_rows(after_bucket, limit, &mut rows))
        {
            Some(next) => UsagePage::More { next },
            None => self.usage_entries(
                entries_after,
                limit.saturating_sub(rows.len()),
                &mut entries,
            ),
        };
        GroupUsageView::Listed(UsageListing {
            genesis,
            history,
            applied,
            usage_seq: self.usage_seq(),
            retired_through,
            live_versions: u64::try_from(self.registry.len()).unwrap_or(u64::MAX),
            end_records: u64::try_from(self.end_records().len()).unwrap_or(u64::MAX),
            relist,
            rows,
            entries,
            page,
        })
    }

    /// Pushes onto `rows` the bucket rows after `after_bucket`, from the
    /// first without one, at most `limit`, each with the bucket's totals
    /// and gauges at this read. The position after the last one when the
    /// limit cut them, else `None`: the rows ended.
    fn usage_rows(
        &self,
        after_bucket: Option<&str>,
        limit: usize,
        rows: &mut Vec<BucketUsageRow>,
    ) -> Option<UsagePosition> {
        let mut retired = self.usage.retired_after(after_bucket).peekable();
        retired.peek()?;
        let mut totals = self
            .bucket_usage_report()
            .into_iter()
            .map(|row| (row.bucket_id, row.usage))
            .collect::<BTreeMap<_, _>>();
        let snapshot_bytes = self.snapshot_bytes_by_bucket();
        for (bucket_id, retired) in retired {
            if rows.len() >= limit {
                return rows.last().map(|row| UsagePosition::Rows {
                    after_bucket: Some(row.bucket_id.clone()),
                });
            }
            // Every bucket with a retired row has a total (L4).
            let total = totals.remove(bucket_id).unwrap_or_default();
            rows.push(BucketUsageRow {
                bucket_id: bucket_id.clone(),
                retired: *retired,
                totals: total,
                snapshot_bytes: snapshot_bytes.get(bucket_id).copied().unwrap_or(0),
            });
        }
        None
    }

    /// Pushes onto `entries` the live versions and end records above
    /// `after_seq` in seq order, at most `limit`. Each version is listed
    /// once, at its newest seq: the index holds a live version at its seq
    /// only, and an end record is a version that is no longer live.
    fn usage_entries(
        &self,
        after_seq: u64,
        limit: usize,
        entries: &mut Vec<UsageEntry>,
    ) -> UsagePage {
        let mut live = self.usage.live_after(after_seq).peekable();
        let mut ends = self.usage.ends_after(after_seq).peekable();
        let mut listed = 0_usize;
        let mut last = after_seq;
        loop {
            let take_live = match (live.peek(), ends.peek()) {
                (None, None) => return UsagePage::Complete,
                (Some((live_seq, _)), Some(record)) => *live_seq < record.seq,
                (Some(_), None) => true,
                (None, Some(_)) => false,
            };
            if listed >= limit {
                return UsagePage::More {
                    next: UsagePosition::Entries { after_seq: last },
                };
            }
            let entry = if take_live {
                let Some((seq, key)) = live.next() else {
                    return UsagePage::Complete;
                };
                let slot = self.registry.slot_by_key(key);
                debug_assert!(slot.is_some(), "the usage index names a removed stream");
                let Some(slot) = slot else {
                    continue;
                };
                debug_assert_eq!(slot.usage.seq, seq, "the usage index is stale");
                UsageEntry {
                    seq,
                    stream_id: slot.metadata.stream_id.clone(),
                    created_at_ms: slot.metadata.created_at_ms,
                    counted_since: slot.usage.counted_since,
                    incarnation: self.token_of(&slot.metadata),
                    counters: slot.usage.counters,
                    history_baseline: slot.history_baseline_in(self.history_id()),
                    last_write: slot.usage.last_write,
                    lifecycle: Lifecycle::Live {
                        retained_bytes: retained_bytes(slot),
                        snapshot_bytes: visible_snapshot_bytes(slot).unwrap_or(0),
                    },
                }
            } else {
                let Some(record) = ends.next() else {
                    return UsagePage::Complete;
                };
                self.ended_entry(record)
            };
            last = entry.seq;
            entries.push(entry);
            listed = listed.saturating_add(1);
        }
    }

    /// The entry of `record`: its token in the history it ended in, and its
    /// counters at the start of the group's current history, which are its
    /// recorded baseline when it ended in that history and its final
    /// counters when the history began after it ended.
    fn ended_entry(&self, record: &EndRecord) -> UsageEntry {
        let history_baseline = if record.ended_in.get() == self.history_id() {
            record.history_baseline
        } else {
            record.counters
        };
        UsageEntry {
            seq: record.seq,
            stream_id: record.stream_id.clone(),
            created_at_ms: record.created_at_ms,
            counted_since: record.counted_since,
            incarnation: incarnation_token(record.created_at_ms, record.ended_in.get()),
            counters: record.counters,
            history_baseline,
            last_write: record.last_write,
            lifecycle: Lifecycle::Ended {
                end: record.end,
                ended_at_ms: record.ended_at_ms,
            },
        }
    }
}
