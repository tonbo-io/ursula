//! Byte-based snapshot cadence (bounded-stream-state F12e).
//!
//! A group snapshots once the Raft log it applied since its last snapshot
//! reaches `max(F, 2 × last snapshot size)`, so snapshot bytes written per
//! appended log byte stay near one half and never grow with history. The
//! floor `F` is the node log-byte budget divided by twice the group count,
//! capped at 16 MiB (4 MiB at the default 1 GiB budget and 128 groups). An
//! entry count remains only as a far backstop.
//!
//! A node pressure pass keeps unpurged log within the node budget: once the
//! log bytes held across the node's groups reach three quarters of the
//! budget, it snapshots the groups that free the most log per snapshot byte
//! first, until the node would fall to half of the budget.
//!
//! Groups the Raft WAL reports lagging (their live log keeps old journal
//! segments on disk) go first, ahead of the cadence and the pressure pass,
//! as long as they applied log since their last snapshot.
//!
//! The policy is pure: the snapshot driver feeds it each group's
//! [`GroupLogProgress`], read from the [`GroupLogGauge`] its state machine
//! maintains, and the state probe feeds it simulated groups.

use std::cmp::Ordering;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering as AtomicOrdering;

/// Default node log-byte budget (F12e).
pub const DEFAULT_NODE_LOG_BUDGET_BYTES: u64 = 1 << 30;
/// Largest per-group floor `F`.
pub const MAX_SNAPSHOT_FLOOR_BYTES: u64 = 16 << 20;
/// Smallest per-group floor, so a tiny budget or a huge group count cannot
/// make every few entries a snapshot.
pub const MIN_SNAPSHOT_FLOOR_BYTES: u64 = 64 << 10;
/// Default far backstop in applied entries.
pub const DEFAULT_SNAPSHOT_BACKSTOP_ENTRIES: u64 = 100_000;

/// Log a group applied since its last snapshot, and that snapshot's size.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct GroupLogProgress {
    /// Raft log bytes applied since the last snapshot (estimated per entry).
    pub log_bytes: u64,
    /// Raft log entries applied since the last snapshot.
    pub log_entries: u64,
    /// Raw size of the last snapshot; 0 before the first one.
    pub last_snapshot_bytes: u64,
    /// Whether the group holds any snapshot yet.
    pub has_snapshot: bool,
}

/// The F12e policy for one node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotCadence {
    /// Per-group floor `F` in log bytes.
    pub floor_bytes: u64,
    /// Node log-byte budget.
    pub node_budget_bytes: u64,
    /// Far backstop in applied entries.
    pub backstop_entries: u64,
}

impl SnapshotCadence {
    pub fn new(node_budget_bytes: u64, group_count: usize, backstop_entries: u64) -> Self {
        let groups = u64::try_from(group_count.max(1)).unwrap_or(u64::MAX);
        // `groups` is at least 1, so the divisor is at least 2 and the quotient always exists.
        let floor_bytes = node_budget_bytes
            .checked_div(groups.saturating_mul(2))
            .unwrap_or(0)
            .clamp(MIN_SNAPSHOT_FLOOR_BYTES, MAX_SNAPSHOT_FLOOR_BYTES);
        Self {
            floor_bytes,
            node_budget_bytes: node_budget_bytes.max(1),
            backstop_entries: backstop_entries.max(1),
        }
    }

    /// Log bytes after which a group whose last snapshot held
    /// `last_snapshot_bytes` snapshots again: `max(F, 2 × S)`.
    pub fn threshold_bytes(&self, last_snapshot_bytes: u64) -> u64 {
        self.floor_bytes.max(last_snapshot_bytes.saturating_mul(2))
    }

    /// Whether a group is due on its own cadence. A group with applied state
    /// and no snapshot yet is due at once, so it gets a recovery source.
    pub fn is_due(&self, progress: &GroupLogProgress) -> bool {
        if progress.log_entries == 0 {
            return false;
        }
        !progress.has_snapshot
            || progress.log_bytes >= self.threshold_bytes(progress.last_snapshot_bytes)
            || progress.log_entries >= self.backstop_entries
    }

    /// Node log bytes at which the pressure pass starts.
    pub fn pressure_watermark_bytes(&self) -> u64 {
        // `n / 4 * 3` is below `u64::MAX`, so this never saturates.
        (self.node_budget_bytes / 4).saturating_mul(3)
    }

    /// Node log bytes the pressure pass aims to fall to.
    pub fn pressure_target_bytes(&self) -> u64 {
        self.node_budget_bytes / 2
    }

    /// Groups to snapshot now, at most `max_groups` of them, by index into
    /// `groups`. Below the pressure watermark these are the due groups; at or
    /// above it, the groups that free the most log per snapshot byte, until
    /// the node would fall to the pressure target. Either way the order is
    /// most log freed per snapshot byte first, then the lowest index.
    pub fn plan(&self, groups: &[GroupLogProgress], max_groups: usize) -> SnapshotPlan {
        self.plan_with_lagging(groups, &[], max_groups)
    }

    /// [`SnapshotCadence::plan`], with the groups whose `lagging` flag is set
    /// first, most log first, whenever they hold log since their last
    /// snapshot. A missing flag counts as unset.
    pub fn plan_with_lagging(
        &self,
        groups: &[GroupLogProgress],
        lagging: &[bool],
        max_groups: usize,
    ) -> SnapshotPlan {
        let is_lagging = |index: usize| {
            lagging.get(index).copied().unwrap_or(false)
                && groups
                    .get(index)
                    .is_some_and(|progress| progress.log_entries > 0)
        };
        let mut forced = (0..groups.len())
            .filter(|index| is_lagging(*index))
            .collect::<Vec<_>>();
        forced.sort_by(|left, right| {
            let bytes = |index: &usize| groups.get(*index).map_or(0, |progress| progress.log_bytes);
            bytes(right).cmp(&bytes(left)).then_with(|| left.cmp(right))
        });
        forced.truncate(max_groups);
        let mut plan = self.plan_cadence(groups, &forced, max_groups.saturating_sub(forced.len()));
        let mut selected = forced;
        selected.append(&mut plan.groups);
        plan.groups = selected;
        plan
    }

    /// The cadence and pressure plan over the groups not in `skip`.
    fn plan_cadence(
        &self,
        groups: &[GroupLogProgress],
        skip: &[usize],
        max_groups: usize,
    ) -> SnapshotPlan {
        let node_log_bytes = groups
            .iter()
            .map(|progress| progress.log_bytes)
            .fold(0u64, u64::saturating_add);
        let pressure = node_log_bytes >= self.pressure_watermark_bytes();
        let mut order = (0..groups.len())
            .filter(|index| !skip.contains(index))
            .filter(|index| {
                groups.get(*index).is_some_and(|progress| {
                    if pressure {
                        progress.log_entries > 0
                    } else {
                        self.is_due(progress)
                    }
                })
            })
            .collect::<Vec<_>>();
        order.sort_by(|left, right| {
            match (groups.get(*left), groups.get(*right)) {
                (Some(left), Some(right)) => compare_yield(right, left),
                _ => Ordering::Equal,
            }
            .then_with(|| left.cmp(right))
        });
        let mut selected = Vec::new();
        let mut remaining = node_log_bytes;
        for index in order {
            if selected.len() >= max_groups {
                break;
            }
            let Some(progress) = groups.get(index) else {
                continue;
            };
            if pressure && remaining <= self.pressure_target_bytes() && !self.is_due(progress) {
                continue;
            }
            remaining = remaining.saturating_sub(progress.log_bytes);
            selected.push(index);
        }
        SnapshotPlan {
            groups: selected,
            pressure,
            node_log_bytes,
        }
    }
}

/// Orders by log bytes freed per snapshot byte, `a.log / a.snap` against
/// `b.log / b.snap`, without division. A group without a snapshot counts as
/// a one-byte snapshot.
fn compare_yield(left: &GroupLogProgress, right: &GroupLogProgress) -> Ordering {
    let left_cost = u128::from(left.last_snapshot_bytes.max(1));
    let right_cost = u128::from(right.last_snapshot_bytes.max(1));
    // Each factor is below 2^64, so the product fits `u128` and never saturates.
    u128::from(left.log_bytes)
        .saturating_mul(right_cost)
        .cmp(&u128::from(right.log_bytes).saturating_mul(left_cost))
}

/// One planning decision of [`SnapshotCadence::plan`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotPlan {
    /// Indexes of the groups to snapshot, in order.
    pub groups: Vec<usize>,
    /// Whether node log bytes reached the pressure watermark.
    pub pressure: bool,
    /// Log bytes held across the node's groups.
    pub node_log_bytes: u64,
}

/// Lock-free per-group log counters, written by the group's state machine
/// and read by the snapshot driver. Applied counters are cumulative; a
/// snapshot records the counter values it covers, so log applied while a
/// snapshot builds still counts toward the next one.
#[derive(Debug, Default)]
pub struct GroupLogGauge {
    applied_bytes: AtomicU64,
    applied_entries: AtomicU64,
    snapshot_bytes_mark: AtomicU64,
    snapshot_entries_mark: AtomicU64,
    last_snapshot_bytes: AtomicU64,
    has_snapshot: AtomicBool,
}

/// Counter values a snapshot covers; see [`GroupLogGauge::mark`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GroupLogMark {
    bytes: u64,
    entries: u64,
}

impl GroupLogMark {
    /// Log bytes applied up to this mark.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl GroupLogGauge {
    /// Counts one applied entry of `bytes` estimated log bytes.
    pub fn record_applied(&self, bytes: u64) {
        self.applied_bytes.fetch_add(bytes, AtomicOrdering::Relaxed);
        self.applied_entries.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// What a snapshot of the state applied so far covers.
    pub fn mark(&self) -> GroupLogMark {
        GroupLogMark {
            bytes: self.applied_bytes.load(AtomicOrdering::Relaxed),
            entries: self.applied_entries.load(AtomicOrdering::Relaxed),
        }
    }

    /// Records a snapshot of `snapshot_bytes` raw bytes covering `mark`.
    pub fn record_snapshot(&self, mark: GroupLogMark, snapshot_bytes: u64) {
        self.snapshot_bytes_mark
            .fetch_max(mark.bytes, AtomicOrdering::Relaxed);
        self.snapshot_entries_mark
            .fetch_max(mark.entries, AtomicOrdering::Relaxed);
        self.last_snapshot_bytes
            .store(snapshot_bytes, AtomicOrdering::Relaxed);
        self.has_snapshot.store(true, AtomicOrdering::Relaxed);
    }

    pub fn progress(&self) -> GroupLogProgress {
        let applied_bytes = self.applied_bytes.load(AtomicOrdering::Relaxed);
        let applied_entries = self.applied_entries.load(AtomicOrdering::Relaxed);
        GroupLogProgress {
            log_bytes: applied_bytes
                .saturating_sub(self.snapshot_bytes_mark.load(AtomicOrdering::Relaxed)),
            log_entries: applied_entries
                .saturating_sub(self.snapshot_entries_mark.load(AtomicOrdering::Relaxed)),
            last_snapshot_bytes: self.last_snapshot_bytes.load(AtomicOrdering::Relaxed),
            has_snapshot: self.has_snapshot.load(AtomicOrdering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;

    fn progress(log_bytes: u64, last_snapshot_bytes: u64) -> GroupLogProgress {
        GroupLogProgress {
            log_bytes,
            log_entries: (log_bytes / 256).saturating_add(1),
            last_snapshot_bytes,
            has_snapshot: true,
        }
    }

    #[test]
    fn floor_is_budget_over_twice_the_groups_capped() {
        let cadence = SnapshotCadence::new(1 << 30, 128, 100_000);
        assert_eq!(cadence.floor_bytes, 4 * MIB);
        assert_eq!(
            SnapshotCadence::new(1 << 30, 1, 100_000).floor_bytes,
            MAX_SNAPSHOT_FLOOR_BYTES
        );
        assert_eq!(
            SnapshotCadence::new(1 << 20, 1_000, 100_000).floor_bytes,
            MIN_SNAPSHOT_FLOOR_BYTES
        );
    }

    #[test]
    fn a_group_is_due_after_max_of_floor_and_twice_its_snapshot() {
        let cadence = SnapshotCadence::new(1 << 30, 128, 100_000);
        assert!(!cadence.is_due(&progress(4 * MIB - 1, MIB)));
        assert!(cadence.is_due(&progress(4 * MIB, MIB)));
        assert!(!cadence.is_due(&progress(19 * MIB, 10 * MIB)));
        assert!(cadence.is_due(&progress(20 * MIB, 10 * MIB)));
        // The entry backstop.
        assert!(cadence.is_due(&GroupLogProgress {
            log_bytes: 1,
            log_entries: 100_000,
            last_snapshot_bytes: 10 * MIB,
            has_snapshot: true,
        }));
        // First snapshot as soon as there is applied state; nothing to do
        // without any.
        assert!(cadence.is_due(&GroupLogProgress {
            log_bytes: 10,
            log_entries: 1,
            ..GroupLogProgress::default()
        }));
        assert!(!cadence.is_due(&GroupLogProgress::default()));
    }

    #[test]
    fn plan_orders_due_groups_by_log_freed_per_snapshot_byte() {
        let cadence = SnapshotCadence::new(1 << 30, 128, 100_000);
        let groups = [
            progress(5 * MIB, MIB),
            progress(MIB, 0),
            progress(8 * MIB, 2 * MIB),
            progress(9 * MIB, 512 << 10),
        ];
        let plan = cadence.plan(&groups, 16);
        assert!(!plan.pressure);
        assert_eq!(plan.groups, vec![3, 0, 2]);
        assert_eq!(cadence.plan(&groups, 1).groups, vec![3]);
    }

    #[test]
    fn pressure_pass_frees_the_most_log_per_snapshot_byte_down_to_half_the_budget() {
        let cadence = SnapshotCadence::new(64 * MIB, 4, 100_000);
        assert_eq!(cadence.floor_bytes, 8 * MIB);
        // Large snapshots hold every group below its own threshold while the
        // node passes three quarters of its budget.
        let groups = [
            progress(14 * MIB, 8 * MIB),
            progress(12 * MIB, 16 * MIB),
            progress(14 * MIB, 10 * MIB),
            progress(10 * MIB, 6 * MIB),
        ];
        assert!(groups.iter().all(|group| !cadence.is_due(group)));
        let plan = cadence.plan(&groups, 16);
        assert!(plan.pressure);
        assert_eq!(plan.node_log_bytes, 50 * MIB);
        // 14/8 > 10/6 > 14/10 > 12/16; stop once at or below 32 MiB.
        assert_eq!(plan.groups, vec![0, 3]);
    }

    /// Lagging groups go first, most log first, ahead of due groups and
    /// whatever their own cadence says, unless they applied nothing since
    /// their last snapshot.
    #[test]
    fn lagging_groups_are_snapshotted_first() {
        let cadence = SnapshotCadence::new(1 << 30, 128, 100_000);
        let groups = [
            progress(5 * MIB, MIB),
            progress(MIB, 0),
            progress(2 * MIB, 4 * MIB),
            progress(3 * MIB, 4 * MIB),
            GroupLogProgress {
                has_snapshot: true,
                ..GroupLogProgress::default()
            },
        ];
        assert!(!cadence.is_due(&groups[2]) && !cadence.is_due(&groups[3]));
        let plan = cadence.plan_with_lagging(&groups, &[false, false, true, true, true], 16);
        assert_eq!(plan.groups, vec![3, 2, 0]);
        assert_eq!(
            cadence
                .plan_with_lagging(&groups, &[false, false, true, true], 1)
                .groups,
            vec![3]
        );
        assert_eq!(
            cadence.plan_with_lagging(&groups, &[], 16),
            cadence.plan(&groups, 16)
        );
    }

    #[test]
    fn gauge_counts_log_since_the_snapshot_mark() {
        let gauge = GroupLogGauge::default();
        gauge.record_applied(100);
        gauge.record_applied(50);
        let mark = gauge.mark();
        // Applied while the snapshot builds: counts toward the next one.
        gauge.record_applied(25);
        gauge.record_snapshot(mark, 1_000);
        assert_eq!(gauge.progress(), GroupLogProgress {
            log_bytes: 25,
            log_entries: 1,
            last_snapshot_bytes: 1_000,
            has_snapshot: true,
        });
        // An older mark never moves the counters back.
        gauge.record_snapshot(GroupLogMark::default(), 900);
        assert_eq!(gauge.progress().log_bytes, 25);
    }
}
