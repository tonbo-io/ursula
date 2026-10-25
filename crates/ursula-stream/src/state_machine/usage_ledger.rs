//! The per-stream usage ledger (bounded-state F20); its public types live
//! in [`crate::usage`].
//!
//! Each live version holds its usage in its slot ([`VersionUsage`]). Every
//! change of committed usage happens at the bucket counter's call sites and
//! goes through one of these hooks: a create
//! ([`StreamStateMachine::usage_on_create`]), a counted append
//! ([`StreamStateMachine::usage_on_append`]), a change of a version's
//! retained or snapshot bytes ([`StreamStateMachine::usage_on_size_change`])
//! and a removal ([`StreamStateMachine::usage_on_removed`]). Each moves usage
//! between a bucket's retired usage and its live versions without creating
//! or losing any, so the bucket totals that
//! [`StreamStateMachine::bucket_usage_report`] derives on every read equal
//! the per-version sums at every entry.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use ursula_shard::BucketStreamId;

use super::StreamSlot;
use super::StreamStateMachine;
use crate::model::BucketUsage;
use crate::model::BucketUsageSnapshot;
use crate::snapshot::StreamSnapshotError;
use crate::usage::CommittedUsage;
use crate::usage::LastWrite;
use crate::usage::VersionCounters;
use crate::usage::VersionUsage;

/// The history a version counts in when it is created or restored before
/// its group's genesis. Only a state machine driven without an engine
/// (tests, benchmarks, the state probe) applies a create then: both engines
/// apply nothing but the genesis before it.
const BEFORE_GENESIS: NonZeroU64 = NonZeroU64::MIN;

/// A group's ledger beside its slots.
#[derive(Debug, Clone, Default)]
pub(super) struct UsageLedger {
    /// The id of the group's genesis history: set by the genesis
    /// `NewHistory` (`previous` 0), kept by every later event and by an
    /// import. 0 before the genesis.
    genesis: u64,
    /// Per bucket, the usage of every removed version and of every version
    /// a backup import restored. A bucket's first create inserts its row at
    /// zero and nothing removes it (F15).
    retired: BTreeMap<String, CommittedUsage>,
    /// The latest seq the group assigned.
    usage_seq: u64,
}

impl UsageLedger {
    /// Gives the next seq.
    fn stamp(&mut self) -> u64 {
        self.usage_seq = self.usage_seq.saturating_add(1);
        self.usage_seq
    }

    /// The ledger of a backup's state: every bucket total of `bucket_usage`
    /// retired, refusing a second row of one bucket.
    pub(super) fn retiring(
        bucket_usage: Vec<BucketUsageSnapshot>,
    ) -> Result<Self, StreamSnapshotError> {
        let mut retired = BTreeMap::new();
        for row in bucket_usage {
            let usage = CommittedUsage::of_bucket(&row.usage);
            if retired.insert(row.bucket_id.clone(), usage).is_some() {
                return Err(StreamSnapshotError::DuplicateBucketUsage(row.bucket_id));
            }
        }
        Ok(Self {
            genesis: 0,
            retired,
            usage_seq: 0,
        })
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

    /// Inserts the bucket's retired row at zero if it has none (F15).
    fn insert_bucket(&mut self, bucket_id: &str) {
        if !self.retired.contains_key(bucket_id) {
            self.retired
                .insert(bucket_id.to_owned(), CommittedUsage::default());
        }
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
    /// The history new counters begin in: the group's current one.
    pub(super) fn counting_history(&self) -> NonZeroU64 {
        NonZeroU64::new(self.history_id()).unwrap_or(BEFORE_GENESIS)
    }

    /// The usage of a version a create makes, with `bytes` initial payload
    /// bytes and `records` records: one counted write at `now_ms`, counted
    /// since the group's history, stamped with the next seq. Its bucket gets
    /// its retired row (F15).
    pub(super) fn usage_on_create(
        &mut self,
        bucket_id: &str,
        bytes: u64,
        records: u64,
        now_ms: u64,
    ) -> VersionUsage {
        let counted_since = self.counting_history();
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
        let Some(slot) = registry.slot_mut(stream_id) else {
            return;
        };
        let counters = &mut slot.usage.counters;
        counters
            .committed
            .add(CommittedUsage::of_write(bytes, records));
        counters.appends = counters.appends.saturating_add(1);
        slot.usage.last_write = LastWrite::At(now_ms);
        slot.usage.seq = usage.stamp();
    }

    /// The live stream `stream_id`'s retained or visible snapshot bytes
    /// changed: its usage takes a new seq.
    pub(super) fn usage_on_size_change(&mut self, stream_id: &BucketStreamId) {
        let Self {
            usage, registry, ..
        } = self;
        if let Some(slot) = registry.slot_mut(stream_id) {
            slot.usage.seq = usage.stamp();
        }
    }

    /// `slot`, which the registry just removed, ended: its usage retires
    /// into its bucket's.
    pub(super) fn usage_on_removed(&mut self, slot: &StreamSlot) {
        self.usage
            .retired
            .entry(slot.metadata.stream_id.bucket_id.clone())
            .or_default()
            .add(slot.usage.counters.committed);
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

    /// Per-bucket usage of this group, sorted by bucket: the committed
    /// counters are each bucket's retired usage plus its live versions',
    /// the gauges are summed over its live streams. Derived on every call
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
            row.retained_bytes = row.retained_bytes.saturating_add(
                slot.metadata
                    .tail_offset
                    .saturating_sub(slot.retained_offset),
            );
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
}
