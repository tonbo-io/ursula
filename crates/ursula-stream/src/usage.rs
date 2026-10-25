//! Per-stream usage (bounded-state F20): what each stream version committed
//! and its applied appends (F9), counted at the bucket counter's call sites
//! by its rule.
//!
//! A group's bucket totals are not a counter of their own: they derive from
//! each bucket's retired usage and its live versions on every read
//! ([`crate::StreamStateMachine::bucket_usage_report`]), so they equal the
//! per-version sums by construction.

use std::num::NonZeroU64;

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
