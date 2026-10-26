//! The per-stream usage ledger (bounded-state F20); its public types live
//! in [`crate::usage`].
//!
//! Each live version holds its usage in its slot ([`VersionUsage`]). Every
//! change of committed usage happens at the bucket counter's call sites and
//! goes through one of these hooks: a create
//! ([`StreamStateMachine::usage_on_create`]), a counted append
//! ([`StreamStateMachine::usage_on_append`]), a change of a version's
//! retained or snapshot bytes ([`StreamStateMachine::usage_on_size_change`])
//! and a removal ([`StreamStateMachine::usage_on_end`]), which leaves an
//! end record with the version's final usage. A fold moves an end record's
//! usage into its bucket's retired usage. None creates or loses usage, so
//! the bucket totals that [`StreamStateMachine::bucket_usage_report`]
//! derives on every read equal the per-version sums at every entry.
//!
//! Beside the replicated ledger each replica keeps a node-local index of
//! live versions by seq, which every slot insert, restamp and removal keeps
//! exact (L1) and a restore rebuilds, so an export page
//! ([`StreamStateMachine::stream_usage`]) costs the entries it lists.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::ops::Bound;

use ursula_shard::BucketStreamId;

use super::MAX_END_RECORDS;
use super::StreamKey;
use super::StreamSlot;
use super::StreamStateMachine;
use super::compare_stream_ids;
use crate::model::BucketUsage;
use crate::model::BucketUsageSnapshot;
use crate::snapshot::StreamSnapshotError;
use crate::usage::BucketRetired;
use crate::usage::CommittedUsage;
use crate::usage::EndRecord;
use crate::usage::LastWrite;
use crate::usage::StreamEnd;
use crate::usage::UsageSnapshot;
use crate::usage::UsageSnapshotError;
use crate::usage::VersionCounters;
use crate::usage::VersionUsage;

/// The most end records a group holds: [`MAX_END_RECORDS`], or in a
/// unit test the cap [`test_cap::with`] set, so a generated run reaches it
/// with a few removals.
fn end_record_cap() -> usize {
    #[cfg(test)]
    {
        test_cap::get()
    }
    #[cfg(not(test))]
    {
        MAX_END_RECORDS
    }
}

/// A smaller end-record cap for one unit test's thread.
#[cfg(test)]
pub(super) mod test_cap {
    use std::cell::Cell;

    use super::MAX_END_RECORDS;

    thread_local! {
        static CAP: Cell<usize> = const { Cell::new(MAX_END_RECORDS) };
    }

    pub(super) fn get() -> usize {
        CAP.with(Cell::get)
    }

    /// Runs `run` with the group cap at `cap` on this thread.
    pub(in crate::state_machine) fn with<R>(cap: usize, run: impl FnOnce() -> R) -> R {
        /// Restores the cap however `run` ends.
        struct Restore(usize);
        impl Drop for Restore {
            fn drop(&mut self) {
                CAP.with(|current| current.set(self.0));
            }
        }
        let _restore = Restore(CAP.with(|current| current.replace(cap)));
        run()
    }
}

/// What an end record adds to a group snapshot beside its stream id: its
/// seq, times, histories, end and counters, rounded up.
const END_RECORD_FIXED_BYTES: u64 = 128;

/// A group's ledger beside its slots.
#[derive(Debug, Clone, Default)]
pub(super) struct UsageLedger {
    /// The id of the group's genesis history: set by the genesis
    /// `NewHistory` (`previous` 0), kept by every later event and by an
    /// import. 0 before the genesis.
    genesis: u64,
    /// Per bucket, the usage of every version whose end record was folded
    /// and of every version a backup import restored. A bucket's first
    /// create inserts its row at zero and nothing removes it (F15).
    retired: BTreeMap<String, CommittedUsage>,
    /// The latest seq the group assigned.
    usage_seq: u64,
    /// Every end record at or below it is folded, every one above it is
    /// held.
    retired_through: u64,
    /// Unretired end records by seq (F20), at most [`MAX_END_RECORDS`].
    ends: BTreeMap<u64, EndRecord>,
    /// Each live version's slot at its seq (L1), so an export page costs
    /// the entries it lists. Node-local and derived: never replicated or
    /// persisted. Every slot insert indexes its version, every restamp
    /// moves it and every removal drops it, so a restore, which inserts
    /// every slot, rebuilds it.
    live_by_seq: BTreeMap<u64, StreamKey>,
}

impl UsageLedger {
    /// Gives the next seq.
    fn stamp(&mut self) -> u64 {
        self.usage_seq = self.usage_seq.saturating_add(1);
        self.usage_seq
    }

    /// The ledger of a backup's state: every bucket total retired.
    pub(super) fn retiring(totals: BTreeMap<String, CommittedUsage>) -> Self {
        Self {
            genesis: 0,
            retired: totals,
            usage_seq: 0,
            retired_through: 0,
            ends: BTreeMap::new(),
            live_by_seq: BTreeMap::new(),
        }
    }

    /// The ledger of a group snapshot's usage, but its live versions, which
    /// the restore attaches to their slots: its genesis, seq and watermark,
    /// its retired rows, refusing a second row of one bucket, and its end
    /// records, refusing more than [`MAX_END_RECORDS`] and any not above
    /// the watermark, not ascending or above the seq. Every end record's
    /// bucket has its retired row (F15).
    pub(super) fn of_group(
        genesis: u64,
        usage_seq: u64,
        retired_through: u64,
        retired: Vec<BucketRetired>,
        ends: Vec<EndRecord>,
    ) -> Result<Self, UsageSnapshotError> {
        if retired_through > usage_seq {
            return Err(UsageSnapshotError::RetiredThroughAhead {
                retired_through,
                usage_seq,
            });
        }
        let mut ledger = Self {
            genesis,
            retired: BTreeMap::new(),
            usage_seq,
            retired_through,
            ends: BTreeMap::new(),
            live_by_seq: BTreeMap::new(),
        };
        for row in retired {
            if ledger
                .retired
                .insert(row.bucket_id.clone(), row.retired)
                .is_some()
            {
                return Err(UsageSnapshotError::DuplicateRetired {
                    bucket_id: row.bucket_id,
                });
            }
        }
        if ends.len() > MAX_END_RECORDS {
            return Err(UsageSnapshotError::TooManyEndRecords {
                count: u64::try_from(ends.len()).unwrap_or(u64::MAX),
                max: u64::try_from(MAX_END_RECORDS).unwrap_or(u64::MAX),
            });
        }
        let mut previous = retired_through;
        for record in ends {
            if record.seq <= retired_through {
                return Err(UsageSnapshotError::RetiredEndRecord {
                    seq: record.seq,
                    retired_through,
                });
            }
            if record.seq <= previous || record.seq > usage_seq {
                return Err(UsageSnapshotError::InvalidSeq {
                    seq: record.seq,
                    usage_seq,
                });
            }
            previous = record.seq;
            ledger.insert_bucket(&record.stream_id.bucket_id);
            ledger.ends.insert(record.seq, record);
        }
        Ok(ledger)
    }

    /// The usage of a version a backup restored: zero counters and
    /// baseline, no write since the restore, counted since `counted_since`,
    /// stamped with the next seq. Its bucket gets its retired row (F15).
    pub(super) fn restored_version(
        &mut self,
        bucket_id: &str,
        counted_since: NonZeroU64,
    ) -> VersionUsage {
        self.insert_bucket(bucket_id);
        VersionUsage {
            seq: self.stamp(),
            counters: VersionCounters::default(),
            history_baseline: VersionCounters::default(),
            last_write: LastWrite::Imported,
            counted_since,
        }
    }

    /// Indexes the live version `key` at its seq `seq`: its slot was just
    /// inserted.
    pub(super) fn index_live(&mut self, seq: u64, key: StreamKey) {
        let earlier = self.live_by_seq.insert(seq, key);
        debug_assert!(earlier.is_none(), "two live versions at seq {seq}");
    }

    /// Drops the live version at `seq` from the index: its slot was just
    /// removed.
    fn unindex_live(&mut self, seq: u64) {
        let removed = self.live_by_seq.remove(&seq);
        debug_assert!(removed.is_some(), "no live version at seq {seq}");
    }

    /// Gives the live version `version`, whose slot is `key`, the next seq,
    /// and moves it there in the index.
    fn restamp(&mut self, version: &mut VersionUsage, key: StreamKey) {
        self.unindex_live(version.seq);
        version.seq = self.stamp();
        self.index_live(version.seq, key);
    }

    /// Inserts the bucket's retired row at zero if it has none (F15).
    pub(super) fn insert_bucket(&mut self, bucket_id: &str) {
        if !self.retired.contains_key(bucket_id) {
            self.retired
                .insert(bucket_id.to_owned(), CommittedUsage::default());
        }
    }

    /// Folds the lowest-seq end record into its bucket's retired usage and
    /// raises `retired_through` to its seq. `None` when the group holds no
    /// end record.
    fn fold_first_end_record(&mut self) -> Option<EndRecord> {
        let (seq, record) = self.ends.pop_first()?;
        self.retired
            .entry(record.stream_id.bucket_id.clone())
            .or_default()
            .add(record.counters.committed);
        self.retired_through = seq;
        Some(record)
    }

    /// The retired rows after `after_bucket`, from the first without one,
    /// by bucket id.
    pub(super) fn retired_after(
        &self,
        after_bucket: Option<&str>,
    ) -> impl Iterator<Item = (&String, &CommittedUsage)> {
        let start = after_bucket.map_or(Bound::Unbounded, Bound::Excluded);
        self.retired.range::<str, _>((start, Bound::Unbounded))
    }

    /// The live versions above `after_seq`, by seq.
    pub(super) fn live_after(&self, after_seq: u64) -> impl Iterator<Item = (u64, StreamKey)> {
        self.live_by_seq
            .range((Bound::Excluded(after_seq), Bound::Unbounded))
            .map(|(seq, key)| (*seq, *key))
    }

    /// The end records above `after_seq`, by seq.
    pub(super) fn ends_after(&self, after_seq: u64) -> impl Iterator<Item = &EndRecord> {
        self.ends
            .range((Bound::Excluded(after_seq), Bound::Unbounded))
            .map(|(_, record)| record)
    }

    /// The live versions the index holds, each at its seq (L1).
    #[cfg(test)]
    pub(super) fn live_index(&self) -> &BTreeMap<u64, StreamKey> {
        &self.live_by_seq
    }

    /// Records the genesis history event's id. Every later event keeps it.
    pub(super) fn record_genesis(&mut self, history: NonZeroU64) {
        self.genesis = history.get();
    }

    /// Keeps the genesis of `earlier`, the ledger an import replaces: an
    /// import is a history event of the same group.
    pub(super) fn keep_genesis(&mut self, earlier: &Self) {
        self.genesis = earlier.genesis;
    }
}

impl StreamStateMachine {
    /// The usage of a version a create makes in the group's history
    /// `counted_since`, with `bytes` initial payload bytes and `records`
    /// records: one counted write at `now_ms`, stamped with the next seq.
    /// Its bucket gets its retired row (F15).
    pub(super) fn usage_on_create(
        &mut self,
        counted_since: NonZeroU64,
        bucket_id: &str,
        bytes: u64,
        records: u64,
        now_ms: u64,
    ) -> VersionUsage {
        self.usage.insert_bucket(bucket_id);
        VersionUsage {
            seq: self.usage.stamp(),
            counters: VersionCounters {
                committed: CommittedUsage::of_write(bytes, records),
                appends: 0,
            },
            history_baseline: VersionCounters::default(),
            last_write: LastWrite::At(now_ms),
            counted_since,
        }
    }

    /// One applied (non-deduplicated) append of `bytes` payload bytes and
    /// `records` records to the live stream `stream_id` at `now_ms`.
    pub(super) fn usage_on_append(
        &mut self,
        stream_id: &BucketStreamId,
        bytes: u64,
        records: u64,
        now_ms: u64,
    ) {
        let Self {
            usage, registry, ..
        } = self;
        // The caller applied the append to this live stream.
        let Some(key) = registry.key(stream_id) else {
            return;
        };
        let Some(slot) = registry.slot_by_key_mut(key) else {
            return;
        };
        let counters = &mut slot.usage.counters;
        counters
            .committed
            .add(CommittedUsage::of_write(bytes, records));
        counters.appends = counters.appends.saturating_add(1);
        slot.usage.last_write = LastWrite::At(now_ms);
        usage.restamp(&mut slot.usage, key);
    }

    /// The live stream `stream_id`'s retained or visible snapshot bytes
    /// changed: its usage takes a new seq.
    pub(super) fn usage_on_size_change(&mut self, stream_id: &BucketStreamId) {
        let Self {
            usage, registry, ..
        } = self;
        if let Some(key) = registry.key(stream_id)
            && let Some(slot) = registry.slot_by_key_mut(key)
        {
            usage.restamp(&mut slot.usage, key);
        }
    }

    /// `slot`, which the registry just removed, ended as `end` at
    /// `now_ms`: it leaves an end record with its final usage, stamped with
    /// the next seq. A group that holds [`MAX_END_RECORDS`] first folds its
    /// lowest-seq end record, early.
    pub(super) fn usage_on_end(&mut self, slot: &StreamSlot, end: StreamEnd, now_ms: u64) {
        self.usage.unindex_live(slot.usage.seq);
        if self.usage.ends.len() >= end_record_cap()
            && let Some(folded) = self.usage.fold_first_end_record()
        {
            self.record_folded(&folded);
            self.removed.end_records_early = self.removed.end_records_early.saturating_add(1);
        }
        // A version lives only after its group's genesis: a create before it
        // is refused, and a restore refuses a stream without a history. The
        // version's own history stands in for one the group cannot lack.
        let ended_in = self
            .history
            .map_or(slot.usage.counted_since, |history| history.id);
        let history_baseline = slot.history_baseline_in(self.history_id());
        let seq = self.usage.stamp();
        self.usage.ends.insert(seq, EndRecord {
            seq,
            stream_id: slot.metadata.stream_id.clone(),
            created_at_ms: slot.metadata.created_at_ms,
            counted_since: slot.usage.counted_since,
            ended_in,
            end,
            ended_at_ms: now_ms,
            counters: slot.usage.counters,
            history_baseline,
            last_write: slot.usage.last_write,
        });
    }

    /// The sweep's fold (F20): folds the lowest-seq end records at or
    /// below `retire_through` into their buckets' retired usage, at most
    /// `limit` of them, and returns how many it folded. `retired_through`
    /// ends at the seq of the newest one folded, so it stays a watermark.
    pub(super) fn retire_end_records(&mut self, retire_through: u64, limit: u64) -> u64 {
        let mut retired = 0u64;
        while retired < limit && self.end_record_due(retire_through) {
            let Some(folded) = self.usage.fold_first_end_record() else {
                break;
            };
            self.record_folded(&folded);
            retired = retired.saturating_add(1);
        }
        retired
    }

    /// Whether the group holds an end record at or below `retire_through`.
    /// O(1): the lowest seq.
    pub(super) fn end_record_due(&self, retire_through: u64) -> bool {
        self.usage
            .ends
            .first_key_value()
            .is_some_and(|(seq, _)| *seq <= retire_through)
    }

    /// The highest seq at or below `through` through which every end
    /// record of the group ended at or before `at_ms`, by its
    /// `ended_at_ms`: the seq of the last one of the run from the lowest
    /// seq that did, or `retired_through` when the lowest did not. A
    /// replica's retirement clock dates by it the end records it held at
    /// its start, installed, or held at a history change, so an end record
    /// keeps its age across them when the nodes' clocks agree. A scan from
    /// the lowest seq that stops at `through` or at the first end record
    /// dated after `at_ms`.
    pub fn end_records_ended_by(&self, at_ms: u64, through: u64) -> u64 {
        self.usage
            .ends
            .range(..=through)
            .map(|(_, record)| record)
            .take_while(|record| record.ended_at_ms <= at_ms)
            .last()
            .map_or(self.usage.retired_through, |record| record.seq)
    }

    /// Counts a folded end record and what it held in a group snapshot
    /// (#669).
    fn record_folded(&mut self, record: &EndRecord) {
        self.removed.end_records = self.removed.end_records.saturating_add(1);
        self.removed.bytes = self.removed.bytes.saturating_add(end_record_bytes(record));
    }

    /// The usage of the live stream `stream_id`'s version, `None` when there
    /// is no live stream.
    pub fn version_usage(&self, stream_id: &BucketStreamId) -> Option<VersionUsage> {
        self.stream_slot(stream_id).map(|slot| slot.usage)
    }

    /// The latest seq the group assigned.
    pub fn usage_seq(&self) -> u64 {
        self.usage.usage_seq
    }

    /// The id of the group's genesis history, 0 before its genesis.
    pub fn usage_genesis(&self) -> u64 {
        self.usage.genesis
    }

    /// The group's unretired end records, by ascending seq (F20).
    pub fn end_records(&self) -> impl ExactSizeIterator<Item = &EndRecord> {
        self.usage.ends.values()
    }

    /// The seq through which every end record is folded.
    pub fn usage_retired_through(&self) -> u64 {
        self.usage.retired_through
    }

    /// Per-bucket usage of this group, sorted by bucket: the committed
    /// counters are each bucket's retired usage plus its live versions' plus
    /// its end records', the gauges are summed over its live streams. Derived on every call
    /// (O(streams)), like [`Self::snapshot_bytes_by_bucket`]. Every bucket
    /// the group ever wrote has a row (F15).
    pub fn bucket_usage_report(&self) -> Vec<BucketUsageSnapshot> {
        let mut rows = self
            .usage
            .retired
            .iter()
            .map(|(bucket_id, retired)| {
                let mut usage = BucketUsage::default();
                retired.add_to(&mut usage);
                (bucket_id.as_str(), usage)
            })
            .collect::<BTreeMap<_, _>>();
        for slot in self.registry.slots() {
            let row = rows
                .entry(slot.metadata.stream_id.bucket_id.as_str())
                .or_default();
            slot.usage.counters.committed.add_to(row);
            row.stream_count = row.stream_count.saturating_add(1);
            row.retained_bytes = row.retained_bytes.saturating_add(retained_bytes(slot));
        }
        for record in self.usage.ends.values() {
            record
                .counters
                .committed
                .add_to(rows.entry(record.stream_id.bucket_id.as_str()).or_default());
        }
        rows.into_iter()
            .map(|(bucket_id, usage)| BucketUsageSnapshot {
                bucket_id: bucket_id.to_owned(),
                usage,
            })
            .collect()
    }

    /// How many rows [`Self::bucket_usage_report`] holds (F15): one per
    /// bucket the group ever wrote, each of which has a retired row.
    pub(super) fn bucket_usage_rows(&self) -> usize {
        self.usage.retired.len()
    }

    /// The usage half of a group snapshot, from the same state as
    /// [`Self::snapshot`]: one version usage per stream, in the stream
    /// snapshot's order.
    pub fn usage_snapshot(&self) -> UsageSnapshot {
        let mut slots = self.registry.slots().collect::<Vec<_>>();
        slots.sort_by(|left, right| {
            compare_stream_ids(&left.metadata.stream_id, &right.metadata.stream_id)
        });
        UsageSnapshot {
            genesis: self.usage.genesis,
            usage_seq: self.usage.usage_seq,
            retired_through: self.usage.retired_through,
            retired: self
                .usage
                .retired
                .iter()
                .map(|(bucket_id, retired)| BucketRetired {
                    bucket_id: bucket_id.clone(),
                    retired: *retired,
                })
                .collect(),
            streams: slots.into_iter().map(|slot| slot.usage).collect(),
            ends: self.usage.ends.values().cloned().collect(),
        }
    }

    /// The checks of a restored group snapshot that need its whole state
    /// (F20): every live version's seq is in `[1, usage_seq]`, and no two
    /// live versions or end records share one. Then every bucket of
    /// `totals`, of the retired rows, of a live version or of an end record
    /// has a total, a missing one read as zero, equal to its retired usage
    /// plus its live versions plus its end records, counter by counter.
    pub(super) fn check_restored_usage(
        &self,
        totals: &BTreeMap<String, CommittedUsage>,
    ) -> Result<(), UsageSnapshotError> {
        let usage_seq = self.usage.usage_seq;
        let mut seqs = self.usage.ends.keys().copied().collect::<BTreeSet<_>>();
        for slot in self.registry.slots() {
            let seq = slot.usage.seq;
            if seq == 0 || seq > usage_seq || !seqs.insert(seq) {
                return Err(UsageSnapshotError::InvalidSeq { seq, usage_seq });
            }
        }
        let parts = self
            .bucket_usage_report()
            .into_iter()
            .map(|row| (row.bucket_id, CommittedUsage::of_bucket(&row.usage)))
            .collect::<BTreeMap<_, _>>();
        for bucket_id in totals.keys().chain(parts.keys()) {
            let total = totals.get(bucket_id).copied().unwrap_or_default();
            let sum = parts.get(bucket_id).copied().unwrap_or_default();
            if let Some(counter) = total.first_difference(sum) {
                return Err(UsageSnapshotError::TotalsDisagree {
                    bucket_id: bucket_id.clone(),
                    counter,
                });
            }
        }
        Ok(())
    }
}

/// Each bucket's committed counters in `rows`, a group's or a backup's
/// bucket totals, refusing a second row of one bucket.
pub(super) fn committed_by_bucket(
    rows: &[BucketUsageSnapshot],
) -> Result<BTreeMap<String, CommittedUsage>, StreamSnapshotError> {
    let mut totals = BTreeMap::new();
    for row in rows {
        if totals
            .insert(row.bucket_id.clone(), CommittedUsage::of_bucket(&row.usage))
            .is_some()
        {
            return Err(StreamSnapshotError::DuplicateBucketUsage(
                row.bucket_id.clone(),
            ));
        }
    }
    Ok(totals)
}

/// The bytes `slot` retains: `tail_offset - retained_offset`, the one rule
/// of the `retained_bytes` gauge, per bucket and per version.
pub(super) fn retained_bytes(slot: &StreamSlot) -> u64 {
    slot.metadata
        .tail_offset
        .saturating_sub(slot.retained_offset)
}

/// Body length of `slot`'s latest visible snapshot, as uploaded: the inline
/// payload's length, or `payload_len` of the cold object that holds the
/// body. `None` without a published snapshot. The one rule of the
/// `snapshot_bytes` gauge, per bucket and per version.
pub(super) fn visible_snapshot_bytes(slot: &StreamSlot) -> Option<u64> {
    slot.visible_snapshot.as_ref().map(|snapshot| {
        snapshot.object.as_ref().map_or_else(
            || u64::try_from(snapshot.payload.len()).unwrap_or(u64::MAX),
            |object| object.payload_len,
        )
    })
}

/// What `record` adds to a group snapshot: its ids and
/// [`END_RECORD_FIXED_BYTES`].
pub(super) fn end_record_bytes(record: &EndRecord) -> u64 {
    let ids = record
        .stream_id
        .bucket_id
        .len()
        .saturating_add(record.stream_id.stream_id.len());
    u64::try_from(ids)
        .unwrap_or(u64::MAX)
        .saturating_add(END_RECORD_FIXED_BYTES)
}
