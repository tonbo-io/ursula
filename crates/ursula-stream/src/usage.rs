//! Per-stream usage (bounded-state F20): what each stream version committed
//! and its applied appends (F9), counted at the bucket counter's call sites
//! by its rule.
//!
//! A removal leaves an [`EndRecord`] with the version's final usage. A
//! group's bucket totals are not a counter of their own: they derive from
//! each bucket's retired usage, its live versions and its end records on
//! every read ([`crate::StreamStateMachine::bucket_usage_report`]), so they
//! equal the per-version sums by construction.

use std::num::NonZeroU64;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;

use crate::model::BucketUsage;
use crate::state_machine::COMMITTED_WRITE_UNIT_BYTES;

/// The committed counters of the usage contract, in the order of
/// [`BucketUsage`] and of `/__ursula/usage`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommittedUsage {
    pub committed_append_bytes: u64,
    pub committed_records: u64,
    pub committed_write_units: u64,
}

impl CommittedUsage {
    /// One counted write of `bytes` payload bytes and `records` records:
    /// `max(1, ceil(bytes / COMMITTED_WRITE_UNIT_BYTES))` units.
    pub fn of_write(bytes: u64, records: u64) -> Self {
        Self {
            committed_append_bytes: bytes,
            committed_records: records,
            committed_write_units: bytes.div_ceil(COMMITTED_WRITE_UNIT_BYTES).max(1),
        }
    }

    /// The committed counters of a bucket's usage row.
    pub fn of_bucket(usage: &BucketUsage) -> Self {
        Self {
            committed_append_bytes: usage.committed_append_bytes,
            committed_records: usage.committed_records,
            committed_write_units: usage.committed_write_units,
        }
    }

    /// Adds `other`, saturating.
    pub fn add(&mut self, other: Self) {
        self.committed_append_bytes = self
            .committed_append_bytes
            .saturating_add(other.committed_append_bytes);
        self.committed_records = self
            .committed_records
            .saturating_add(other.committed_records);
        self.committed_write_units = self
            .committed_write_units
            .saturating_add(other.committed_write_units);
    }

    /// Adds these counters to a bucket's usage row, saturating. The row's
    /// gauges are left as they are.
    pub fn add_to(self, usage: &mut BucketUsage) {
        usage.committed_append_bytes = usage
            .committed_append_bytes
            .saturating_add(self.committed_append_bytes);
        usage.committed_records = usage
            .committed_records
            .saturating_add(self.committed_records);
        usage.committed_write_units = usage
            .committed_write_units
            .saturating_add(self.committed_write_units);
    }
}

/// What a version committed: its share of the bucket counters, and its
/// applied appends (F9).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VersionCounters {
    pub committed: CommittedUsage,
    /// Applied, non-deduplicated appends, inline and external. A create
    /// with an initial payload counts in `committed`, not here.
    pub appends: u64,
}

/// When a version last counted a write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LastWrite {
    /// `now_ms` of its newest counted write, its create included.
    At(u64),
    /// Restored from a backup, and no counted write since the restore.
    Imported,
}

/// A live version's usage (F20).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionUsage {
    /// Its seq, unique in the group: every change of its counters, its
    /// retained bytes or its visible snapshot stamps a new one.
    pub seq: u64,
    /// What it committed since `counted_since` began counting it.
    pub counters: VersionCounters,
    /// Its counters when the history its slot names (W5) began for it:
    /// zero for a version created or restored in that history, else
    /// recorded by its first write after the history event, before that
    /// write counts.
    pub history_baseline: VersionCounters,
    pub last_write: LastWrite,
    /// The history its counters began in: its create's, or the one of the
    /// import that restored it. Part of the version's key, so a restore
    /// gives every version a new key.
    pub counted_since: NonZeroU64,
}

/// How a version ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamEnd {
    /// A `DeleteStream` of a stream that had not expired at its `now_ms`.
    Deleted,
    /// Expired at the removing entry's `now_ms`: the expiry sweep, a
    /// command that named it, a create over it, or a delete after its
    /// expiry.
    Expired,
    /// Its bucket was purged.
    Purged,
}

/// The final usage of a version that ended, held in its group until it is
/// folded into its bucket's retired usage (F20).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndRecord {
    /// The seq of its end, unique in the group.
    pub seq: u64,
    pub stream_id: BucketStreamId,
    pub created_at_ms: u64,
    pub counted_since: NonZeroU64,
    /// The group's history when it ended. Its `Stream-Incarnation` token is
    /// `incarnation_token(created_at_ms, ended_in)`.
    pub ended_in: NonZeroU64,
    pub end: StreamEnd,
    /// `now_ms` of the entry that removed it, the removing proposer's
    /// clock.
    pub ended_at_ms: u64,
    pub counters: VersionCounters,
    /// Its counters when `ended_in` began for it.
    pub history_baseline: VersionCounters,
    pub last_write: LastWrite,
}

/// A bucket's retired usage: of every version whose end record was folded,
/// and of every version a backup import restored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketRetired {
    pub bucket_id: String,
    pub retired: CommittedUsage,
}

/// What a group snapshot holds of the ledger beside its
/// [`StreamSnapshot`](crate::StreamSnapshot). A backup holds none of it
/// (backup format 5), so a restore from a backup starts per-version usage
/// at zero.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageSnapshot {
    /// The group's genesis history id, 0 before its genesis.
    pub genesis: u64,
    /// The latest seq the group assigned.
    pub usage_seq: u64,
    /// Every end record at or below it is folded.
    pub retired_through: u64,
    /// One row per bucket the group ever wrote, by bucket id (F15).
    pub retired: Vec<BucketRetired>,
    /// One per stream of the stream snapshot, in its order.
    pub streams: Vec<VersionUsage>,
    /// Every unretired end record, by ascending seq.
    pub ends: Vec<EndRecord>,
}

/// Why a group snapshot's usage does not fit its stream snapshot. An
/// install refuses it as corruption.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum UsageSnapshotError {
    #[error("{usages} stream usages for {streams} streams")]
    StreamCount { streams: u64, usages: u64 },
    #[error("usage seq {seq} is zero, repeated, out of order, or above the group's {usage_seq}")]
    InvalidSeq { seq: u64, usage_seq: u64 },
    #[error("end record {seq} is at or below retired_through {retired_through}")]
    RetiredEndRecord { seq: u64, retired_through: u64 },
    #[error("retired_through {retired_through} is above usage seq {usage_seq}")]
    RetiredThroughAhead {
        retired_through: u64,
        usage_seq: u64,
    },
    #[error("{count} end records, above the most a group holds, {max}")]
    TooManyEndRecords { count: u64, max: u64 },
    #[error("bucket '{bucket_id}' has two retired rows")]
    DuplicateRetired { bucket_id: String },
    #[error(
        "bucket '{bucket_id}': its total {counter:?} is not its retired usage plus its versions and end records"
    )]
    TotalsDisagree {
        bucket_id: String,
        counter: UsageCounter,
    },
}

/// One counter of [`CommittedUsage`], as a refusal names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UsageCounter {
    AppendBytes,
    Records,
    WriteUnits,
}

impl CommittedUsage {
    /// The first counter in which `self` and `other` differ, `None` when
    /// they are equal.
    pub fn first_difference(self, other: Self) -> Option<UsageCounter> {
        if self.committed_append_bytes != other.committed_append_bytes {
            Some(UsageCounter::AppendBytes)
        } else if self.committed_records != other.committed_records {
            Some(UsageCounter::Records)
        } else if self.committed_write_units != other.committed_write_units {
            Some(UsageCounter::WriteUnits)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::COMMITTED_WRITE_UNIT_BYTES;
    use super::CommittedUsage;

    #[test]
    fn a_write_counts_its_units_rounded_up_and_at_least_one() {
        let units = |bytes| CommittedUsage::of_write(bytes, 1).committed_write_units;
        assert_eq!(COMMITTED_WRITE_UNIT_BYTES, 10_240);
        assert_eq!(units(0), 1);
        assert_eq!(units(1), 1);
        assert_eq!(units(10_240), 1);
        assert_eq!(units(10_241), 2);
        assert_eq!(units(u64::MAX), u64::MAX.div_ceil(10_240));
        assert_eq!(CommittedUsage::of_write(7, 3), CommittedUsage {
            committed_append_bytes: 7,
            committed_records: 3,
            committed_write_units: 1,
        });
    }

    #[test]
    fn counters_add_saturating() {
        let mut total = CommittedUsage {
            committed_append_bytes: u64::MAX,
            committed_records: 1,
            committed_write_units: 2,
        };
        total.add(CommittedUsage::of_write(10_241, 4));
        assert_eq!(total, CommittedUsage {
            committed_append_bytes: u64::MAX,
            committed_records: 5,
            committed_write_units: 4,
        });
    }
}
