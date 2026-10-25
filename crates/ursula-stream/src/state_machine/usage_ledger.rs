//! The per-stream usage ledger (U-A, U-B, bounded-state F20); its public
//! types live in [`crate::usage`].
//!
//! Every change of committed usage goes through one of four hooks: a write
//! ([`StreamStateMachine::usage_on_write`]), a change of a stream's sizes
//! ([`StreamStateMachine::usage_on_size_change`]), a stream's end
//! ([`StreamStateMachine::usage_on_end`]) and an acknowledgement
//! ([`StreamStateMachine::ack_stream_usage`]). Each moves usage between
//! `retired`, a live stream and an ended stream without creating or losing
//! any, so the bucket totals [`StreamStateMachine::bucket_usage_report`]
//! derives equal the per-stream sums at every entry.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;

use ursula_shard::BucketStreamId;

use super::StreamKey;
use super::StreamResponse;
use super::StreamSlot;
use super::StreamStateMachine;
use super::compare_stream_ids;
use crate::history::incarnation_token;
use crate::model::BucketUsage;
use crate::model::BucketUsageSnapshot;
use crate::usage::BucketCommittedUsage;
use crate::usage::CommittedUsage;
use crate::usage::EndedStreamUsage;
use crate::usage::StartedUsageLedger;
use crate::usage::StreamEnd;
use crate::usage::StreamUsageChange;
use crate::usage::StreamUsageChanges;
use crate::usage::StreamUsageEntry;
use crate::usage::UsageAck;
use crate::usage::UsageLedgerProgress;
use crate::usage::UsageLedgerSnapshot;

/// A group's ledger: the retired usage per bucket, and whether a consumer
/// started the per-stream part.
#[derive(Debug, Clone, Default)]
pub(super) struct UsageLedger {
    /// Usage folded out of the per-stream ledger, and every write applied
    /// before it started. One row per bucket ever written (F15).
    retired: HashMap<String, CommittedUsage>,
    progress: Progress,
}

#[derive(Debug, Clone, Default)]
enum Progress {
    /// Writes count into `retired`; streams keep nothing.
    #[default]
    NotStarted,
    Started(Started),
}

#[derive(Debug, Clone, Default)]
struct Started {
    usage_seq: u64,
    acked_through: u64,
    /// Ended streams not yet acknowledged, by the sequence number of their
    /// end (F20).
    ended: BTreeMap<u64, EndedStreamUsage>,
    /// Node-local and derived, never replicated: each live stream at its
    /// latest sequence number, so an export costs the changes it lists,
    /// not the group's streams. Restore rebuilds it from the slots.
    live_by_seq: BTreeMap<u64, StreamKey>,
}

impl Started {
    /// Gives the stream whose usage is `usage` the next sequence number.
    fn stamp(&mut self, key: StreamKey, usage: &mut crate::usage::StreamUsage) {
        if usage.usage_seq != 0 {
            self.live_by_seq.remove(&usage.usage_seq);
        }
        self.usage_seq = self.usage_seq.saturating_add(1);
        usage.usage_seq = self.usage_seq;
        self.live_by_seq.insert(self.usage_seq, key);
    }
}

impl UsageLedger {
    /// The ledger of a restored stream snapshot: its bucket totals retired,
    /// the per-stream part not started (Ursula 0.10.1's state).
    pub(super) fn retiring(bucket_usage: &[BucketUsageSnapshot]) -> Self {
        Self {
            retired: bucket_usage
                .iter()
                .map(|row| (row.bucket_id.clone(), crate::usage::committed_of(row)))
                .collect(),
            progress: Progress::NotStarted,
        }
    }

    fn started(&self) -> Option<&Started> {
        match &self.progress {
            Progress::NotStarted => None,
            Progress::Started(started) => Some(started),
        }
    }
}

/// Body length of a slot's latest visible snapshot, as uploaded: the
/// inline payload's length, or `payload_len` of the cold object that holds
/// the body. `None` without a published snapshot.
pub(super) fn visible_snapshot_bytes(slot: &StreamSlot) -> Option<u64> {
    slot.visible_snapshot.as_ref().map(|snapshot| {
        snapshot.object.as_ref().map_or_else(
            || u64::try_from(snapshot.payload.len()).unwrap_or(u64::MAX),
            |object| object.payload_len,
        )
    })
}

fn add_committed(usage: &mut BucketUsage, committed: CommittedUsage) {
    usage.committed_append_bytes = usage
        .committed_append_bytes
        .saturating_add(committed.committed_append_bytes);
    usage.committed_records = usage
        .committed_records
        .saturating_add(committed.committed_records);
    usage.committed_write_units = usage
        .committed_write_units
        .saturating_add(committed.committed_write_units);
}

impl StreamStateMachine {
    /// One accepted (non-deduplicated) write of `bytes` payload bytes and
    /// `records` records to the live stream `stream_id`, its create
    /// included.
    pub(super) fn usage_on_write(
        &mut self,
        stream_id: &BucketStreamId,
        bytes: u64,
        records: u64,
        now_ms: u64,
    ) {
        let write = CommittedUsage::of_write(bytes, records);
        let Self {
            usage, registry, ..
        } = self;
        let key = registry.key(stream_id);
        if let (Progress::Started(started), Some(key)) = (&mut usage.progress, key)
            && let Some(slot) = registry.slot_by_key_mut(key)
        {
            slot.usage.add_write(write, now_ms);
            started.stamp(key, &mut slot.usage);
            return;
        }
        debug_assert!(
            usage.started().is_none(),
            "a write to stream '{stream_id}', which is not live"
        );
        usage
            .retired
            .entry(stream_id.bucket_id.clone())
            .or_default()
            .add(write);
    }

    /// The live stream `stream_id`'s retained or snapshot bytes changed.
    pub(super) fn usage_on_size_change(&mut self, stream_id: &BucketStreamId) {
        let Self {
            usage, registry, ..
        } = self;
        if let Progress::Started(started) = &mut usage.progress
            && let Some(key) = registry.key(stream_id)
            && let Some(slot) = registry.slot_by_key_mut(key)
        {
            started.stamp(key, &mut slot.usage);
        }
    }

    /// `slot`, which the registry just removed, ended: its final usage
    /// waits in `ended` for an acknowledgement (U-B). Before the ledger
    /// starts its usage is all retired already.
    pub(super) fn usage_on_end(&mut self, slot: &StreamSlot, end: StreamEnd) {
        let history = self.history_id();
        let Progress::Started(started) = &mut self.usage.progress else {
            return;
        };
        if slot.usage.usage_seq != 0 {
            started.live_by_seq.remove(&slot.usage.usage_seq);
        }
        started.usage_seq = started.usage_seq.saturating_add(1);
        let seq = started.usage_seq;
        started.ended.insert(seq, EndedStreamUsage {
            stream_id: slot.metadata.stream_id.clone(),
            created_at_ms: slot.metadata.created_at_ms,
            incarnation: incarnation_token(slot.metadata.created_at_ms, history),
            content_type: slot.metadata.content_type.clone(),
            usage: crate::usage::StreamUsage {
                usage_seq: seq,
                ..slot.usage
            },
            end,
        });
    }

    /// Applies [`crate::StreamCommand::AckStreamUsage`]. Named for another
    /// history, it changes nothing. The first one starts the ledger and
    /// gives every live stream a sequence number, in stream order, so
    /// that an export from 0 lists every live stream. A later one folds
    /// every ended stream through `through_seq` into its bucket's retired
    /// usage; an acknowledgement above the ledger's sequence number
    /// acknowledges up to it, one at or below the acknowledged is a no-op.
    pub(super) fn ack_stream_usage(&mut self, history: u64, through_seq: u64) -> StreamResponse {
        let current = self.history_id();
        if history != current {
            return StreamResponse::StreamUsageAcked {
                ack: UsageAck::OtherHistory { history: current },
            };
        }
        let Self {
            usage, registry, ..
        } = self;
        let ack = match &mut usage.progress {
            Progress::NotStarted => {
                let mut keys = registry
                    .stream_ids()
                    .filter_map(|stream_id| Some((stream_id, registry.key(stream_id)?)))
                    .collect::<Vec<_>>();
                keys.sort_by(|left, right| compare_stream_ids(left.0, right.0));
                let keys = keys.into_iter().map(|(_, key)| key).collect::<Vec<_>>();
                let mut started = Started::default();
                for key in keys {
                    if let Some(slot) = registry.slot_by_key_mut(key) {
                        started.stamp(key, &mut slot.usage);
                    }
                }
                let usage_seq = started.usage_seq;
                usage.progress = Progress::Started(started);
                UsageAck::Started { usage_seq }
            }
            Progress::Started(started) => {
                let through = through_seq.min(started.usage_seq);
                let mut folded = 0u64;
                if through > started.acked_through {
                    let unacknowledged = started.ended.split_off(&through.saturating_add(1));
                    let acknowledged = std::mem::replace(&mut started.ended, unacknowledged);
                    for ended in acknowledged.into_values() {
                        usage
                            .retired
                            .entry(ended.stream_id.bucket_id)
                            .or_default()
                            .add(ended.usage.committed());
                        folded = folded.saturating_add(1);
                    }
                    started.acked_through = through;
                }
                UsageAck::Acknowledged {
                    acked_through: started.acked_through,
                    folded,
                }
            }
        };
        StreamResponse::StreamUsageAcked { ack }
    }

    /// Per-bucket usage of this group, sorted by bucket: the committed
    /// counters are `retired + Σ live + Σ ended`, the gauges are summed
    /// over the live streams. Derived on every call (O(streams)), like
    /// [`Self::snapshot_bytes_by_bucket`].
    pub fn bucket_usage_report(&self) -> Vec<BucketUsageSnapshot> {
        let mut rows = BTreeMap::<&str, BucketUsage>::new();
        for (bucket_id, retired) in &self.usage.retired {
            add_committed(rows.entry(bucket_id.as_str()).or_default(), *retired);
        }
        for slot in self.registry.slots() {
            let row = rows
                .entry(slot.metadata.stream_id.bucket_id.as_str())
                .or_default();
            add_committed(row, slot.usage.committed());
            row.stream_count = row.stream_count.saturating_add(1);
            row.retained_bytes = row.retained_bytes.saturating_add(
                slot.metadata
                    .tail_offset
                    .saturating_sub(slot.retained_offset),
            );
        }
        for ended in self
            .usage
            .started()
            .into_iter()
            .flat_map(|s| s.ended.values())
        {
            add_committed(
                rows.entry(ended.stream_id.bucket_id.as_str()).or_default(),
                ended.usage.committed(),
            );
        }
        rows.into_iter()
            .map(|(bucket_id, usage)| BucketUsageSnapshot {
                bucket_id: bucket_id.to_owned(),
                usage,
            })
            .collect()
    }

    /// How many buckets [`Self::bucket_usage_report`] names (F15).
    pub(super) fn bucket_usage_rows(&self) -> usize {
        let mut buckets = self
            .usage
            .retired
            .keys()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        buckets.extend(
            self.registry
                .slots()
                .map(|slot| slot.metadata.stream_id.bucket_id.as_str()),
        );
        if let Some(started) = self.usage.started() {
            buckets.extend(
                started
                    .ended
                    .values()
                    .map(|ended| ended.stream_id.bucket_id.as_str()),
            );
        }
        buckets.len()
    }

    /// Where the ledger stands.
    pub fn usage_ledger_progress(&self) -> UsageLedgerProgress {
        match self.usage.started() {
            None => UsageLedgerProgress::NotStarted,
            Some(started) => UsageLedgerProgress::Started {
                usage_seq: started.usage_seq,
                acked_through: started.acked_through,
            },
        }
    }

    /// Ended streams awaiting an acknowledgement (F20 gauge).
    pub(super) fn ended_stream_usage_len(&self) -> usize {
        self.usage
            .started()
            .map_or(0, |started| started.ended.len())
    }

    /// The changes after `after_seq`, at most `limit` of them, each stream
    /// version once at its latest sequence number (U-C). Costs the changes
    /// it lists, not the group's streams.
    pub fn stream_usage_changes(&self, after_seq: u64, limit: usize) -> StreamUsageChanges {
        let history = self.history_id();
        let Some(started) = self.usage.started() else {
            return StreamUsageChanges {
                history,
                progress: UsageLedgerProgress::NotStarted,
                through: 0,
                changes: Vec::new(),
            };
        };
        let from = after_seq.saturating_add(1);
        let mut live = started.live_by_seq.range(from..).peekable();
        let mut ended = started.ended.range(from..).peekable();
        let mut changes = Vec::new();
        let mut through = started.usage_seq;
        loop {
            let take_live = match (live.peek(), ended.peek()) {
                (None, None) => break,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some((live_seq, _)), Some((ended_seq, _))) => live_seq < ended_seq,
            };
            if changes.len() >= limit {
                through = changes
                    .last()
                    .map_or(after_seq, |change: &StreamUsageChange| {
                        change.usage.usage_seq
                    });
                break;
            }
            let change = if take_live {
                let Some((_, key)) = live.next() else { break };
                let Some(slot) = self.registry.slot_by_key(*key) else {
                    debug_assert!(false, "the usage index names a removed stream");
                    continue;
                };
                StreamUsageChange {
                    stream_id: slot.metadata.stream_id.clone(),
                    created_at_ms: slot.metadata.created_at_ms,
                    incarnation: incarnation_token(slot.metadata.created_at_ms, history),
                    content_type: slot.metadata.content_type.clone(),
                    usage: slot.usage,
                    retained_bytes: slot
                        .metadata
                        .tail_offset
                        .saturating_sub(slot.retained_offset),
                    snapshot_bytes: visible_snapshot_bytes(slot).unwrap_or(0),
                    end: None,
                }
            } else {
                let Some((_, ended)) = ended.next() else {
                    break;
                };
                StreamUsageChange {
                    stream_id: ended.stream_id.clone(),
                    created_at_ms: ended.created_at_ms,
                    incarnation: ended.incarnation,
                    content_type: ended.content_type.clone(),
                    usage: ended.usage,
                    retained_bytes: 0,
                    snapshot_bytes: 0,
                    end: Some(ended.end),
                }
            };
            changes.push(change);
        }
        StreamUsageChanges {
            history,
            progress: UsageLedgerProgress::Started {
                usage_seq: started.usage_seq,
                acked_through: started.acked_through,
            },
            through,
            changes,
        }
    }

    /// The ledger a group snapshot carries beside [`Self::snapshot`].
    pub fn usage_ledger_snapshot(&self) -> UsageLedgerSnapshot {
        let Some(started) = self.usage.started() else {
            return UsageLedgerSnapshot::NotStarted;
        };
        let mut retired = self
            .usage
            .retired
            .iter()
            .map(|(bucket_id, usage)| BucketCommittedUsage {
                bucket_id: bucket_id.clone(),
                usage: *usage,
            })
            .collect::<Vec<_>>();
        retired.sort_by(|left, right| left.bucket_id.cmp(&right.bucket_id));
        let mut streams = self
            .registry
            .slots()
            .map(|slot| StreamUsageEntry {
                stream_id: slot.metadata.stream_id.clone(),
                usage: slot.usage,
            })
            .collect::<Vec<_>>();
        streams.sort_by(|left, right| compare_stream_ids(&left.stream_id, &right.stream_id));
        UsageLedgerSnapshot::Started(StartedUsageLedger {
            usage_seq: started.usage_seq,
            acked_through: started.acked_through,
            retired,
            streams,
            ended: started.ended.values().cloned().collect(),
        })
    }

    /// Installs the ledger of a group snapshot over the state
    /// [`Self::restore`] built from its stream snapshot. The snapshot codec
    /// checks a started ledger against its stream snapshot
    /// ([`StartedUsageLedger::check`]) before anything installs it.
    pub fn restore_usage_ledger(&mut self, snapshot: UsageLedgerSnapshot) {
        let UsageLedgerSnapshot::Started(ledger) = snapshot else {
            return;
        };
        debug_assert_eq!(
            ledger.check(self.registry.stream_ids(), &self.bucket_usage_report()),
            Ok(()),
            "an unchecked usage ledger"
        );
        let StartedUsageLedger {
            usage_seq,
            acked_through,
            retired,
            streams,
            ended,
        } = ledger;
        let mut started = Started {
            usage_seq,
            acked_through,
            ended: ended
                .into_iter()
                .map(|ended| (ended.usage.usage_seq, ended))
                .collect(),
            live_by_seq: BTreeMap::new(),
        };
        for entry in streams {
            if let Some(key) = self.registry.key(&entry.stream_id)
                && let Some(slot) = self.registry.slot_by_key_mut(key)
            {
                slot.usage = entry.usage;
                started.live_by_seq.insert(entry.usage.usage_seq, key);
            }
        }
        self.usage = UsageLedger {
            retired: retired
                .into_iter()
                .map(|row| (row.bucket_id, row.usage))
                .collect(),
            progress: Progress::Started(started),
        };
    }
}
