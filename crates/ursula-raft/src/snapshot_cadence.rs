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
//! budget, it snapshots the groups that hold the most reclaimable log
//! first, until the node would fall to half of the budget. Most of a
//! build's time is a fixed chain of snapshot-store round trips, so under
//! pressure the builds, not the snapshot bytes, bound how fast the log
//! shrinks. Ordered by log per snapshot byte instead, a group that applied
//! a few entries over a near-empty snapshot came before every group being
//! written, and in #519 such groups took about 7% of the builds.
//!
//! At or past the budget the pass escalates ([`LogPressure::Escalated`],
//! #519) and keeps more builds in flight: one at the budget and more in
//! even steps toward the hard limit (twice the budget, where admission
//! refuses client writes), rounded up so that every build the node allows
//! runs before the log gets there. It starts as many more groups, so it
//! lasts about as long as a pass below the budget. Before each build a pass
//! weighs the log again ([`SnapshotCadence::pass_step`]): it keeps only as
//! many builds in flight as the pressure now calls for, one once its builds
//! brought the log below the budget, and it ends once the log climbed to a
//! higher pressure, so the next pass plans for that.
//!
//! Groups the Raft WAL reports lagging (their live log keeps old journal
//! segments on disk) go first, ahead of the cadence and the pressure pass,
//! as long as they applied log since their last snapshot. A group whose
//! build is in flight is not planned again: OpenRaft drops a trigger while
//! a build runs, and the log that build covers counts as freed.
//!
//! Removals (#669) count too. The bytes a deletion, a bucket purge or a TTL
//! expiry removed since a group's last snapshot count toward its threshold,
//! so a large removal leaves the snapshot within minutes. A removal no
//! snapshot covers also makes its group due once it is
//! [`SnapshotCadence::removal_floor`] old, a quarter of
//! `raft.deleted_data_max_age`, and such overdue groups go first in a pass,
//! even ahead of lagging ones. Removed bytes never count toward the node's
//! log: they neither raise the pressure nor close admission, so a mass
//! deletion never refuses writes.
//!
//! The policy is pure: the snapshot driver feeds it each group's
//! [`GroupLogProgress`], read from the [`GroupLogGauge`] its state machine
//! maintains, with the builds in flight, and the state probe feeds it
//! simulated groups.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::Duration;

use crate::rt::time::Instant;

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
    /// Removed streams the replica's latest snapshot or its log still holds
    /// (#669).
    pub removal: RemovalProgress,
    /// The log index the replica purges its log through once its snapshot
    /// reaches it (#669): the latest snapshot that covered a removal, or
    /// the one it restored at startup. 0 for none.
    pub scrub_purge_through: u64,
}

/// Removed streams a replica's latest snapshot or its log still holds
/// (#669), as [`GroupLogGauge::progress`] samples them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RemovalProgress {
    /// The latest snapshot covers every removal the replica applied.
    #[default]
    Clear,
    /// Removals no snapshot covers yet.
    Owed {
        /// What they held in a group snapshot.
        bytes: u64,
        /// How long ago the oldest of them applied.
        age_ms: u64,
    },
    /// Removals the replica applied again while it replayed its log at
    /// startup. When they first applied is unknown, so they are due at once.
    Replayed {
        /// What they held in a group snapshot.
        bytes: u64,
        /// How long ago the replay ended.
        age_ms: u64,
    },
    /// A snapshot covers every removal, but the log still holds entries up
    /// to `through`, the snapshot's last index, until it is purged through
    /// it.
    Purging {
        /// How long ago the oldest removal the log still holds applied.
        age_ms: u64,
        /// The log index the log is purged through.
        through: u64,
    },
}

impl RemovalProgress {
    /// What the removals no snapshot covers held in a group snapshot.
    pub fn bytes(&self) -> u64 {
        match self {
            Self::Clear | Self::Purging { .. } => 0,
            Self::Owed { bytes, .. } | Self::Replayed { bytes, .. } => *bytes,
        }
    }

    /// How long the oldest removal the replica's snapshot or log still
    /// holds has been held, `None` when there is none.
    pub fn age(&self) -> Option<Duration> {
        match self {
            Self::Clear => None,
            Self::Owed { age_ms, .. }
            | Self::Replayed { age_ms, .. }
            | Self::Purging { age_ms, .. } => Some(Duration::from_millis(*age_ms)),
        }
    }
}

/// How hard the log held across the node's groups presses on the node
/// budget, which sets how a snapshot pass works against it. Ordered from
/// calm to the most builds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogPressure {
    /// Below three quarters of the budget: the groups due on their own
    /// cadence.
    #[default]
    Calm,
    /// From three quarters of the budget up to it: any group that holds
    /// log, the most reclaimable log first, one build at a time.
    Watermark,
    /// At or past the budget: the most reclaimable log first, `builds` at a
    /// time, and `builds` times as many groups per pass.
    Escalated {
        /// One at the budget and more in even steps, up to the node's build
        /// concurrency before the hard limit.
        builds: usize,
    },
}

impl LogPressure {
    /// Whether the node's log reached the pressure watermark.
    pub fn is_pressure(self) -> bool {
        !matches!(self, Self::Calm)
    }

    /// Snapshot builds a pass keeps in flight.
    pub fn builds(self) -> usize {
        match self {
            Self::Calm | Self::Watermark => 1,
            Self::Escalated { builds } => builds,
        }
    }

    /// The level a metric reports: 0 calm, 1 watermark, 2 escalated.
    pub fn level(self) -> u64 {
        match self {
            Self::Calm => 0,
            Self::Watermark => 1,
            Self::Escalated { .. } => 2,
        }
    }
}

/// What a pass of the snapshot driver does before it starts its next build
/// (#519), from [`SnapshotCadence::pass_step`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassStep {
    /// The log climbed to a higher pressure than the pass was planned for.
    /// The pass ends, so the next one plans for the pressure now, with its
    /// group count, order and builds.
    Outgrown,
    /// The next build starts once fewer than `builds` run: as many as the
    /// pressure now calls for, never more than the pass was planned for.
    Start {
        /// Builds the pass keeps in flight now.
        builds: usize,
    },
}

/// What the snapshot driver knows of one group when it plans a pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GroupDriveState {
    /// Log applied since the group's last snapshot. It counts toward the
    /// node's log until a snapshot that covers it completes.
    pub progress: GroupLogProgress,
    /// Whether the Raft WAL reports the group keeping old journal segments.
    pub lagging: bool,
    /// Log bytes the group's build in flight will still free, `None` while
    /// no build runs. The snapshot driver also sets `Some(0)` for a group it
    /// holds out of its passes after a trigger of it went unclaimed: never
    /// planned, and none of its log counted as freed. It sets `Some(0)` as
    /// well for a group whose build released its permit with a snapshot
    /// OpenRaft has not published yet, for at most
    /// [`crate::SNAPSHOT_HANDOFF_WAIT`]
    /// ([`crate::SnapshotBuildCoordinator::add_publishing`]), so the pass
    /// log's `building` count includes such groups.
    pub building: Option<u64>,
}

impl GroupDriveState {
    /// Log a build started now would free. None while a build is in
    /// flight: OpenRaft drops the trigger, and that build frees the log.
    pub fn reclaimable_bytes(&self) -> u64 {
        match self.building {
            Some(_) => 0,
            None => self.progress.log_bytes,
        }
    }

    /// Whether a build can start now: the group applied log since its last
    /// snapshot and runs no build.
    fn startable(&self) -> bool {
        self.building.is_none() && self.progress.log_entries > 0
    }
}

/// How much one pass of the snapshot driver may start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriveLimits {
    /// Groups a pass starts below the budget
    /// (`raft.snapshot_pressure_max_groups_per_tick`). An escalated pass
    /// starts this many per build it keeps in flight.
    pub max_groups: usize,
    /// Builds the node runs at once (`raft.snapshot_build_max_concurrency`).
    pub max_builds: usize,
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
    /// How old a removal no snapshot covers may get before its group is due
    /// (#669): a quarter of `raft.deleted_data_max_age`.
    pub removal_floor: Duration,
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
            removal_floor: removal_floor_of(
                ursula_config::RaftConfig::DEFAULT_DELETED_DATA_MAX_AGE,
            ),
        }
    }

    /// This policy with `raft.deleted_data_max_age` set to `max_age`: a
    /// removal no snapshot covers makes its group due a quarter of it later.
    #[must_use]
    pub fn with_deleted_data_max_age(mut self, max_age: Duration) -> Self {
        self.removal_floor = removal_floor_of(max_age);
        self
    }

    /// Whether `progress` holds a removal past [`Self::removal_floor`], or
    /// one replayed at startup, which no snapshot covers (#669). Its group
    /// is due, ahead of every other group of a pass.
    pub fn removal_overdue(&self, progress: &GroupLogProgress) -> bool {
        if ursula_stream::sim_controls::off(ursula_stream::sim_controls::Defence::RemovalFloor) {
            return false;
        }
        match progress.removal {
            RemovalProgress::Clear | RemovalProgress::Purging { .. } => false,
            RemovalProgress::Owed { age_ms, .. } => {
                let overdue = Duration::from_millis(age_ms) >= self.removal_floor;
                if overdue {
                    ursula_stream::sim_controls::note_fired(
                        ursula_stream::sim_controls::Defence::RemovalFloor,
                    );
                }
                overdue
            }
            RemovalProgress::Replayed { .. } => true,
        }
    }

    /// Log bytes after which a group whose last snapshot held
    /// `last_snapshot_bytes` snapshots again: `max(F, 2 × S)`.
    pub fn threshold_bytes(&self, last_snapshot_bytes: u64) -> u64 {
        self.floor_bytes.max(last_snapshot_bytes.saturating_mul(2))
    }

    /// Whether a group is due on its own cadence. A group with applied state
    /// and no snapshot yet is due at once, so it gets a recovery source. The
    /// bytes it removed since its last snapshot count toward its threshold,
    /// and an overdue removal makes it due whatever its log (#669).
    pub fn is_due(&self, progress: &GroupLogProgress) -> bool {
        if progress.log_entries == 0 {
            return false;
        }
        !progress.has_snapshot
            || progress.log_bytes.saturating_add(progress.removal.bytes())
                >= self.threshold_bytes(progress.last_snapshot_bytes)
            || progress.log_entries >= self.backstop_entries
            || self.removal_overdue(progress)
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

    /// Node log bytes over which admission refuses client writes: twice the
    /// budget. The driver normally holds the log near the budget, and this
    /// is the bound for when it cannot keep up.
    pub fn hard_limit_bytes(&self) -> u64 {
        self.node_budget_bytes.saturating_mul(2)
    }

    /// Node log bytes under which refused client writes are admitted again:
    /// one and a half budgets.
    pub fn resume_bytes(&self) -> u64 {
        self.node_budget_bytes.saturating_mul(3) / 2
    }

    /// The pressure of `node_log_bytes` held across the node's groups, on a
    /// node that runs at most `max_builds` builds at once. At or past the
    /// budget a pass keeps `1 + ceil((max_builds - 1) × over / budget)`
    /// builds in flight, at most `max_builds`, `over` being the log past the
    /// budget. With four: two past the budget, three past a third of the way
    /// to the hard limit and four past two thirds.
    pub fn pressure(&self, node_log_bytes: u64, max_builds: usize) -> LogPressure {
        if node_log_bytes < self.pressure_watermark_bytes() {
            return LogPressure::Calm;
        }
        if node_log_bytes < self.node_budget_bytes {
            return LogPressure::Watermark;
        }
        let steps = max_builds.saturating_sub(1);
        // One budget separates the budget from the hard limit.
        let span = self
            .hard_limit_bytes()
            .saturating_sub(self.node_budget_bytes)
            .max(1);
        let over = node_log_bytes
            .saturating_sub(self.node_budget_bytes)
            .min(span);
        // ceil(over × steps / span), at most `steps`. Each factor is below
        // 2^64, so the product fits `u128`.
        let taken = u128::from(over)
            .saturating_mul(u128::try_from(steps).unwrap_or(u128::MAX))
            .saturating_add(u128::from(span).saturating_sub(1))
            .checked_div(u128::from(span))
            .map_or(steps, |taken| usize::try_from(taken).unwrap_or(steps))
            .min(steps);
        LogPressure::Escalated {
            builds: taken.saturating_add(1),
        }
    }

    /// What a pass planned under `planned` does before its next build, the
    /// node's log now holding `node_log_bytes`, on a node that runs at most
    /// `max_builds` builds at once. A pass whose log climbed past `planned`
    /// ends. Otherwise its builds follow the pressure down as they bring the
    /// log down: a pass planned past the budget keeps one build in flight
    /// once the log is below it, as a pass planned there does.
    pub fn pass_step(
        &self,
        planned: LogPressure,
        node_log_bytes: u64,
        max_builds: usize,
    ) -> PassStep {
        let now = self.pressure(node_log_bytes, max_builds);
        if now > planned {
            PassStep::Outgrown
        } else {
            PassStep::Start {
                builds: now.builds(),
            }
        }
    }

    /// Groups to snapshot now, at most `max_groups` of them, by index into
    /// `groups`: [`SnapshotCadence::plan_pass`] for groups that neither lag
    /// nor build, on a node that builds one group at a time.
    pub fn plan(&self, groups: &[GroupLogProgress], max_groups: usize) -> SnapshotPlan {
        let groups = groups
            .iter()
            .map(|progress| GroupDriveState {
                progress: *progress,
                ..GroupDriveState::default()
            })
            .collect::<Vec<_>>();
        self.plan_pass(&groups, DriveLimits {
            max_groups,
            max_builds: 1,
        })
    }

    /// One pass of the snapshot driver: the groups to snapshot, by index
    /// into `groups`, in the order to start them.
    ///
    /// Below the pressure watermark, groups with an overdue removal
    /// ([`Self::removal_overdue`], #669) go first, replayed ones and then
    /// the oldest removal first. Under pressure they plan as due groups.
    /// Lagging groups that hold log come next, most log first. Below the
    /// pressure watermark the rest are the groups due on their own cadence,
    /// the most log freed per snapshot byte first. At or above it they are
    /// any group that holds log, the most reclaimable log first, until the
    /// node would fall to the pressure target once the builds in flight and
    /// the planned ones complete, and due groups after that. Below the
    /// budget a pass starts at most `max_groups`, and at or past it
    /// `max_groups` per build it keeps in flight. Ties go to the lowest
    /// index. A group whose build is in flight is never planned.
    pub fn plan_pass(&self, groups: &[GroupDriveState], limits: DriveLimits) -> SnapshotPlan {
        let node_log_bytes = groups
            .iter()
            .map(|group| group.progress.log_bytes)
            .fold(0u64, u64::saturating_add);
        let pressure = self.pressure(node_log_bytes, limits.max_builds);
        let max_groups = limits.max_groups.max(1).saturating_mul(pressure.builds());
        let at = |index: &usize| groups.get(*index);
        // Overdue removals lead a calm pass, replayed ones first, then the
        // oldest. Under pressure the log comes first: overdue groups are due
        // groups like any other, after the groups that free the most log, so
        // removal debt never holds back the relief the 503 gate waits for.
        let overdue_rank = |index: &usize| {
            at(index)
                .filter(|group| {
                    !pressure.is_pressure()
                        && group.startable()
                        && self.removal_overdue(&group.progress)
                })
                .map(|group| {
                    (
                        matches!(group.progress.removal, RemovalProgress::Replayed { .. }),
                        group.progress.removal.age(),
                    )
                })
        };
        let mut selected = (0..groups.len())
            .filter(|index| overdue_rank(index).is_some())
            .collect::<Vec<_>>();
        selected.sort_by(|left, right| {
            overdue_rank(right)
                .cmp(&overdue_rank(left))
                .then_with(|| left.cmp(right))
        });
        let overdue = selected.len().min(max_groups);
        let mut chosen = selected.iter().copied().collect::<BTreeSet<_>>();
        let mut lagging = (0..groups.len())
            .filter(|index| !chosen.contains(index))
            .filter(|index| at(index).is_some_and(|group| group.lagging && group.startable()))
            .collect::<Vec<_>>();
        lagging.sort_by(|left, right| {
            let bytes = |index: &usize| at(index).map_or(0, GroupDriveState::reclaimable_bytes);
            bytes(right).cmp(&bytes(left)).then_with(|| left.cmp(right))
        });
        selected.extend(lagging);
        selected.truncate(max_groups);
        chosen = selected.iter().copied().collect();
        // The log left once the builds in flight and the planned ones
        // complete.
        let in_flight = groups
            .iter()
            .filter_map(|group| group.building)
            .fold(0u64, u64::saturating_add);
        let planned = selected
            .iter()
            .filter_map(at)
            .map(|group| group.progress.log_bytes)
            .fold(0u64, u64::saturating_add);
        let mut remaining = node_log_bytes
            .saturating_sub(in_flight)
            .saturating_sub(planned);
        let mut order = (0..groups.len())
            .filter(|index| !chosen.contains(index))
            .filter(|index| {
                at(index).is_some_and(|group| {
                    group.startable() && (pressure.is_pressure() || self.is_due(&group.progress))
                })
            })
            .collect::<Vec<_>>();
        order.sort_by(|left, right| {
            match (at(left), at(right)) {
                (Some(left), Some(right)) if pressure.is_pressure() => {
                    right.reclaimable_bytes().cmp(&left.reclaimable_bytes())
                }
                (Some(left), Some(right)) => compare_yield(&right.progress, &left.progress),
                _ => Ordering::Equal,
            }
            .then_with(|| left.cmp(right))
        });
        for index in order {
            if selected.len() >= max_groups {
                break;
            }
            let Some(group) = at(&index) else {
                continue;
            };
            if pressure.is_pressure()
                && remaining <= self.pressure_target_bytes()
                && !self.is_due(&group.progress)
            {
                continue;
            }
            remaining = remaining.saturating_sub(group.progress.log_bytes);
            selected.push(index);
        }
        SnapshotPlan {
            groups: selected,
            overdue,
            pressure,
            node_log_bytes,
        }
    }
}

/// A quarter of `max_age`, the age at which a removal no snapshot covers
/// makes its group due (#669).
fn removal_floor_of(max_age: Duration) -> Duration {
    max_age.checked_div(4).unwrap_or_default()
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

/// One planning decision of [`SnapshotCadence::plan_pass`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotPlan {
    /// Indexes of the groups to snapshot, in order.
    pub groups: Vec<usize>,
    /// How many of the first `groups` hold an overdue removal (#669).
    pub overdue: usize,
    /// How hard the node's log presses on its budget.
    pub pressure: LogPressure,
    /// Log bytes held across the node's groups, the log of builds in
    /// flight included.
    pub node_log_bytes: u64,
}

/// Per-group log counters, written by the group's state machine and read by
/// the snapshot driver. The log counters are atomics; the removal ledger
/// (#669) sits behind one mutex. Applied counters are cumulative; a
/// snapshot records the counter values it covers, so log applied while a
/// snapshot builds still counts toward the next one.
///
/// The gauge also keeps the replica's removal ledger (#669): what its state
/// machine removed, what its latest snapshot covers of it, and since when it
/// owes a snapshot that drops the rest, or a purge of its log through the
/// snapshot that did. The ledger is local to the replica and kept in memory
/// only: each replica scrubs its own copies, by its own clock.
#[derive(Debug, Default)]
pub struct GroupLogGauge {
    /// The membership the state machine applied last (#613): what a replica
    /// keeps after a recovery plan's cut when its log holds no membership
    /// entry at or below its committed index.
    applied_membership:
        std::sync::Mutex<openraft::alias::StoredMembershipOf<crate::types::UrsulaRaftTypeConfig>>,
    apply_stopped: AtomicBool,
    applied_bytes: AtomicU64,
    applied_entries: AtomicU64,
    snapshot_bytes_mark: AtomicU64,
    snapshot_entries_mark: AtomicU64,
    last_snapshot_bytes: AtomicU64,
    has_snapshot: AtomicBool,
    /// One lock for every removal counter, so a removal that applies while
    /// a build records its snapshot is never lost between them.
    removals: std::sync::Mutex<RemovalLedger>,
}

/// Whether a replica owes a snapshot that drops removed streams (#669).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RemovalDebt {
    /// Its latest snapshot covers every removal it applied.
    #[default]
    Clear,
    /// It applied a removal no snapshot covers, the oldest at `since`.
    Owed {
        /// When the oldest such removal applied.
        since: Instant,
    },
    /// It applied such a removal again while it replayed its log at
    /// startup, which ended at `since`. When the removal first applied is
    /// unknown, so it is due at once.
    Replayed {
        /// When the replay ended.
        since: Instant,
    },
    /// A snapshot covers every removal it applied, and its log still holds
    /// entries up to `through`, that snapshot's last index, until a purge
    /// through it lands ([`GroupLogGauge::observe_purged`]).
    Purging {
        /// When the oldest removal its log still holds applied.
        since: Instant,
        /// The log index the log is purged through.
        through: u64,
    },
}

impl RemovalDebt {
    /// When the oldest removal the debt is for applied, `None` when clear.
    fn since(self) -> Option<Instant> {
        match self {
            Self::Clear => None,
            Self::Owed { since } | Self::Replayed { since } | Self::Purging { since, .. } => {
                Some(since)
            }
        }
    }
}

/// The removal counters of a [`GroupLogGauge`] (#669).
#[derive(Debug, Default)]
struct RemovalLedger {
    /// Removals applied, cumulative.
    removals: u64,
    /// What they held in a group snapshot, cumulative.
    removed_bytes: u64,
    /// `removals` the latest snapshot covers.
    covered_removals: u64,
    /// `removed_bytes` the latest snapshot covers.
    covered_bytes: u64,
    debt: RemovalDebt,
    /// See [`GroupLogProgress::scrub_purge_through`].
    purge_through: u64,
    /// Entries the group's OpenRaft keeps below a snapshot it built
    /// (`max_in_snapshot_log_to_keep`), once its engine started.
    log_keep: Option<u64>,
    /// `removals` when the latest mark was taken, and when the first
    /// removal after it applied: what a snapshot of that mark leaves owed.
    marked_removals: u64,
    first_after_mark: Option<Instant>,
}

/// Counter values a snapshot covers; see [`GroupLogGauge::mark`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GroupLogMark {
    bytes: u64,
    entries: u64,
    removals: u64,
    removed_bytes: u64,
}

impl GroupLogMark {
    /// Log bytes applied up to this mark.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl GroupLogGauge {
    /// Records the membership the state machine applied last.
    pub(crate) fn record_membership(
        &self,
        membership: &openraft::alias::StoredMembershipOf<crate::types::UrsulaRaftTypeConfig>,
    ) {
        *self
            .applied_membership
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = membership.clone();
    }

    /// The membership the state machine applied last.
    pub(crate) fn applied_membership(
        &self,
    ) -> openraft::alias::StoredMembershipOf<crate::types::UrsulaRaftTypeConfig> {
        self.applied_membership
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn stop_apply(&self) {
        self.apply_stopped.store(true, AtomicOrdering::Release);
    }
    pub(crate) fn apply_stopped(&self) -> bool {
        self.apply_stopped.load(AtomicOrdering::Acquire)
    }

    /// Counts one applied entry of `bytes` estimated log bytes.
    pub fn record_applied(&self, bytes: u64) {
        self.applied_bytes.fetch_add(bytes, AtomicOrdering::Relaxed);
        self.applied_entries.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Counts an applied entry that removed streams holding `bytes` in a
    /// group snapshot (#669). A replica that owed no snapshot owes one from
    /// now, and one whose log still holds earlier removals owes it from
    /// when the oldest of them applied.
    pub fn record_removal(&self, bytes: u64) {
        let mut ledger = self.ledger();
        if ledger.removals == ledger.marked_removals && ledger.first_after_mark.is_none() {
            ledger.first_after_mark = Some(Instant::now());
        }
        ledger.removals = ledger.removals.saturating_add(1);
        ledger.removed_bytes = ledger.removed_bytes.saturating_add(bytes);
        ledger.debt = match ledger.debt {
            RemovalDebt::Clear => RemovalDebt::Owed {
                since: Instant::now(),
            },
            RemovalDebt::Purging { since, .. } => RemovalDebt::Owed { since },
            owed @ (RemovalDebt::Owed { .. } | RemovalDebt::Replayed { .. }) => owed,
        };
    }

    /// Records that the group's log is purged through `purged`: a purge
    /// through the snapshot that covers every removal ends the debt.
    pub fn observe_purged(&self, purged: Option<u64>) {
        let mut ledger = self.ledger();
        if let RemovalDebt::Purging { through, .. } = ledger.debt
            && purged >= Some(through)
        {
            ledger.debt = RemovalDebt::Clear;
        }
    }

    /// Records the entries the group's OpenRaft keeps below a snapshot it
    /// built (`max_in_snapshot_log_to_keep`).
    pub(crate) fn set_log_keep(&self, log_keep: u64) {
        self.ledger().log_keep = Some(log_keep);
    }

    /// The entries the group's OpenRaft keeps below a snapshot it built,
    /// `None` before its engine started.
    pub(crate) fn log_keep(&self) -> Option<u64> {
        self.ledger().log_keep
    }

    /// The startup replay ended (#669): a removal it applied again first
    /// applied at a time nobody knows, so it is due at once. Called once
    /// `Raft::new` returned, when every replayed entry applied.
    pub fn replay_ended(&self) {
        let mut ledger = self.ledger();
        if let RemovalDebt::Owed { .. } = ledger.debt {
            ledger.debt = RemovalDebt::Replayed {
                since: Instant::now(),
            };
        }
    }

    /// What a snapshot of the state applied so far covers.
    ///
    /// Taking a mark changes the gauge: it becomes the latest mark, and the
    /// first removal applied after it dates what a snapshot of it leaves
    /// owed ([`Self::record_snapshot`]). A snapshot of an earlier mark,
    /// recorded after a newer mark was taken, leaves its removals owed from
    /// when it is recorded instead.
    pub fn mark(&self) -> GroupLogMark {
        let mut ledger = self.ledger();
        if ledger.marked_removals != ledger.removals {
            ledger.marked_removals = ledger.removals;
            ledger.first_after_mark = None;
        }
        GroupLogMark {
            bytes: self.applied_bytes.load(AtomicOrdering::Relaxed),
            entries: self.applied_entries.load(AtomicOrdering::Relaxed),
            removals: ledger.removals,
            removed_bytes: ledger.removed_bytes,
        }
    }

    /// Records a snapshot of `snapshot_bytes` raw bytes covering `mark`,
    /// whose last log index is `index`: one this replica built or
    /// installed.
    ///
    /// A snapshot that covers a removal no earlier one covered raises the
    /// scrub purge watermark to `index`. Once it covers every removal
    /// applied so far, the replica owes a purge of its log through the
    /// watermark ([`RemovalDebt::Purging`]). A removal that applied while
    /// the snapshot was built makes it owe the next snapshot from when that
    /// removal applied, so a group whose streams keep expiring snapshots
    /// once per [`SnapshotCadence::removal_floor`], not back to back.
    pub fn record_snapshot(&self, mark: GroupLogMark, snapshot_bytes: u64, index: u64) {
        self.snapshot_bytes_mark
            .fetch_max(mark.bytes, AtomicOrdering::Relaxed);
        self.snapshot_entries_mark
            .fetch_max(mark.entries, AtomicOrdering::Relaxed);
        self.last_snapshot_bytes
            .store(snapshot_bytes, AtomicOrdering::Relaxed);
        self.has_snapshot.store(true, AtomicOrdering::Relaxed);
        let mut ledger = self.ledger();
        let covers_new = mark.removals > ledger.covered_removals;
        ledger.covered_removals = ledger.covered_removals.max(mark.removals);
        ledger.covered_bytes = ledger.covered_bytes.max(mark.removed_bytes);
        if covers_new {
            ledger.purge_through = ledger.purge_through.max(index);
        }
        if ledger.removals <= ledger.covered_removals {
            if let Some(since) = ledger.debt.since() {
                ledger.debt = RemovalDebt::Purging {
                    since,
                    through: ledger.purge_through,
                };
            }
        } else if covers_new {
            // The removals the snapshot left owed applied after its mark:
            // the oldest of them when this was the latest mark, else no
            // earlier than the snapshot's record.
            let since = (ledger.marked_removals == mark.removals)
                .then_some(ledger.first_after_mark)
                .flatten()
                .unwrap_or_else(Instant::now);
            ledger.debt = RemovalDebt::Owed { since };
        }
    }

    /// Records the snapshot this replica restored at startup, whose last
    /// log index is `index` ([`Self::record_snapshot`]). The scrub purge
    /// watermark rises to `index` whatever the snapshot covers: the log
    /// kept below it may hold the records of a stream removed before the
    /// restart, whose purge through its snapshot the restart cut short.
    pub fn record_restored_snapshot(&self, mark: GroupLogMark, snapshot_bytes: u64, index: u64) {
        self.record_snapshot(mark, snapshot_bytes, index);
        if ursula_stream::sim_controls::off(ursula_stream::sim_controls::Defence::RestorePurge) {
            return;
        }
        let mut ledger = self.ledger();
        if index > ledger.purge_through {
            ursula_stream::sim_controls::note_fired(
                ursula_stream::sim_controls::Defence::RestorePurge,
            );
            ledger.purge_through = index;
        }
    }

    /// Applied log bytes the latest snapshot covers.
    pub(crate) fn snapshot_mark_bytes(&self) -> u64 {
        self.snapshot_bytes_mark.load(AtomicOrdering::Relaxed)
    }

    /// Whether this replica owes a snapshot that drops removed streams.
    pub fn removal_debt(&self) -> RemovalDebt {
        self.ledger().debt
    }

    pub fn progress(&self) -> GroupLogProgress {
        let applied_bytes = self.applied_bytes.load(AtomicOrdering::Relaxed);
        let applied_entries = self.applied_entries.load(AtomicOrdering::Relaxed);
        let ledger = self.ledger();
        let owed_bytes = ledger.removed_bytes.saturating_sub(ledger.covered_bytes);
        let age_ms =
            |since: Instant| u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX);
        let removal = match ledger.debt {
            RemovalDebt::Clear => RemovalProgress::Clear,
            RemovalDebt::Owed { since } => RemovalProgress::Owed {
                bytes: owed_bytes,
                age_ms: age_ms(since),
            },
            RemovalDebt::Replayed { since } => RemovalProgress::Replayed {
                bytes: owed_bytes,
                age_ms: age_ms(since),
            },
            RemovalDebt::Purging { since, through } => RemovalProgress::Purging {
                age_ms: age_ms(since),
                through,
            },
        };
        GroupLogProgress {
            log_bytes: applied_bytes
                .saturating_sub(self.snapshot_bytes_mark.load(AtomicOrdering::Relaxed)),
            log_entries: applied_entries
                .saturating_sub(self.snapshot_entries_mark.load(AtomicOrdering::Relaxed)),
            last_snapshot_bytes: self.last_snapshot_bytes.load(AtomicOrdering::Relaxed),
            has_snapshot: self.has_snapshot.load(AtomicOrdering::Relaxed),
            removal,
            scrub_purge_through: ledger.purge_through,
        }
    }

    fn ledger(&self) -> std::sync::MutexGuard<'_, RemovalLedger> {
        self.removals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::DriveLimits;
    use super::GroupDriveState;
    use super::GroupLogGauge;
    use super::GroupLogMark;
    use super::GroupLogProgress;
    use super::LogPressure;
    use super::MAX_SNAPSHOT_FLOOR_BYTES;
    use super::MIN_SNAPSHOT_FLOOR_BYTES;
    use super::PassStep;
    use super::RemovalProgress;
    use super::SnapshotCadence;

    const MIB: u64 = 1 << 20;
    const GIB: u64 = 1 << 30;

    fn progress(log_bytes: u64, last_snapshot_bytes: u64) -> GroupLogProgress {
        GroupLogProgress {
            log_bytes,
            log_entries: (log_bytes / 256).saturating_add(1),
            last_snapshot_bytes,
            has_snapshot: true,
            ..GroupLogProgress::default()
        }
    }

    /// The groups as the driver sees them when none lags or builds.
    fn drive(groups: &[GroupLogProgress]) -> Vec<GroupDriveState> {
        groups
            .iter()
            .map(|progress| GroupDriveState {
                progress: *progress,
                ..GroupDriveState::default()
            })
            .collect()
    }

    fn limits(max_groups: usize, max_builds: usize) -> DriveLimits {
        DriveLimits {
            max_groups,
            max_builds,
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
            ..GroupLogProgress::default()
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
        assert_eq!(plan.pressure, LogPressure::Calm);
        assert_eq!(plan.groups, vec![3, 0, 2]);
        assert_eq!(cadence.plan(&groups, 1).groups, vec![3]);
    }

    #[test]
    fn pressure_pass_frees_the_most_log_first_down_to_half_the_budget() {
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
        assert_eq!(plan.pressure, LogPressure::Watermark);
        assert_eq!(plan.node_log_bytes, 50 * MIB);
        // 14 and 14 MiB, the lower index first, then 22 MiB is at or below
        // 32 MiB.
        assert_eq!(plan.groups, vec![0, 2]);
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
        let lagging = |flags: &[bool]| {
            groups
                .iter()
                .enumerate()
                .map(|(index, progress)| GroupDriveState {
                    progress: *progress,
                    lagging: flags.get(index).copied().unwrap_or(false),
                    building: None,
                })
                .collect::<Vec<_>>()
        };
        let plan = cadence.plan_pass(&lagging(&[false, false, true, true, true]), limits(16, 1));
        assert_eq!(plan.groups, vec![3, 2, 0]);
        assert_eq!(
            cadence
                .plan_pass(&lagging(&[false, false, true, true]), limits(1, 1))
                .groups,
            vec![3]
        );
        assert_eq!(
            cadence.plan_pass(&lagging(&[]), limits(16, 1)),
            cadence.plan(&groups, 16)
        );
    }

    /// #519: past the budget a pass keeps more builds in flight, from one
    /// at the budget to the node's build concurrency before the hard limit.
    #[test]
    fn pressure_escalates_from_the_budget_to_the_hard_limit() {
        let cadence = SnapshotCadence::new(GIB, 128, 100_000);
        assert_eq!(cadence.hard_limit_bytes(), 2 * GIB);
        assert_eq!(cadence.resume_bytes(), 3 * GIB / 2);
        let at = |bytes: u64| cadence.pressure(bytes, 4);
        assert_eq!(at(0), LogPressure::Calm);
        assert_eq!(at(3 * GIB / 4 - 1), LogPressure::Calm);
        assert_eq!(at(3 * GIB / 4), LogPressure::Watermark);
        assert_eq!(at(GIB - 1), LogPressure::Watermark);
        assert_eq!(at(GIB), LogPressure::Escalated { builds: 1 });
        assert_eq!(at(GIB + 1), LogPressure::Escalated { builds: 2 });
        // Thirds of the way up: 357_913_941 × 3 < 2^30 < 357_913_942 × 3.
        assert_eq!(at(GIB + 357_913_941), LogPressure::Escalated { builds: 2 });
        assert_eq!(at(GIB + 357_913_942), LogPressure::Escalated { builds: 3 });
        assert_eq!(at(GIB + 715_827_882), LogPressure::Escalated { builds: 3 });
        assert_eq!(at(GIB + 715_827_883), LogPressure::Escalated { builds: 4 });
        assert_eq!(at(2 * GIB), LogPressure::Escalated { builds: 4 });
        assert_eq!(at(u64::MAX), LogPressure::Escalated { builds: 4 });
        // A node that builds one group at a time only changes the order.
        assert_eq!(cadence.pressure(2 * GIB, 1), LogPressure::Escalated {
            builds: 1
        });
        assert_eq!(cadence.pressure(2 * GIB, 0), LogPressure::Escalated {
            builds: 1
        });
        assert_eq!(
            (at(0).level(), at(GIB - 1).level(), at(GIB).level()),
            (0, 1, 2)
        );
        assert!(!at(0).is_pressure() && at(3 * GIB / 4).is_pressure());
        assert_eq!(at(GIB - 1).builds(), 1);
    }

    /// #519: before each build a pass weighs the log again. Its builds
    /// follow the pressure down, so a pass planned near the hard limit keeps
    /// one build in flight once its builds brought the log below the budget,
    /// and a log that climbs past the pressure the pass was planned for ends
    /// it, so the next pass plans for more.
    #[test]
    fn a_pass_follows_the_log_down_and_ends_when_it_climbs() {
        let cadence = SnapshotCadence::new(GIB, 128, 100_000);
        let step = |planned: LogPressure, bytes: u64| cadence.pass_step(planned, bytes, 4);
        let four = LogPressure::Escalated { builds: 4 };
        let two = LogPressure::Escalated { builds: 2 };
        assert_eq!(step(four, 2 * GIB), PassStep::Start { builds: 4 });
        assert_eq!(step(four, GIB + GIB / 2), PassStep::Start { builds: 3 });
        assert_eq!(step(four, GIB + 1), PassStep::Start { builds: 2 });
        assert_eq!(step(four, GIB), PassStep::Start { builds: 1 });
        assert_eq!(step(four, GIB - 1), PassStep::Start { builds: 1 });
        assert_eq!(step(four, 0), PassStep::Start { builds: 1 });
        assert_eq!(step(two, GIB + 1), PassStep::Start { builds: 2 });
        // The log climbed past the pressure the pass was planned for.
        assert_eq!(step(two, GIB + GIB / 2), PassStep::Outgrown);
        assert_eq!(step(LogPressure::Watermark, GIB), PassStep::Outgrown);
        assert_eq!(step(LogPressure::Calm, 3 * GIB / 4), PassStep::Outgrown);
        assert_eq!(step(LogPressure::Watermark, 3 * GIB / 4), PassStep::Start {
            builds: 1
        });
        assert_eq!(step(LogPressure::Calm, 0), PassStep::Start { builds: 1 });
    }

    /// #519: under pressure the builds are what runs short, so a pass starts
    /// with the groups that free the most log per build. Ordered by log per
    /// snapshot byte, a group that applied a few entries over a near-empty
    /// snapshot came before every group being written.
    #[test]
    fn a_pressure_pass_starts_with_the_most_reclaimable_log() {
        // A group of #519 that took no writes: 14 entries over a 22-byte
        // snapshot, 81 log bytes per snapshot byte.
        let idle = GroupLogProgress {
            log_bytes: 1792,
            log_entries: 14,
            last_snapshot_bytes: 22,
            has_snapshot: true,
            ..GroupLogProgress::default()
        };
        let groups = drive(&[
            idle,
            progress(30 * MIB, 30 * MIB),
            progress(20 * MIB, MIB),
            progress(16 * MIB, 2 * MIB),
        ]);
        // An entry backstop no group reaches: only log bytes make one due.
        let backstop = 1 << 20;
        // 66 MiB against an 80 MiB budget is at the watermark: floor 10 MiB,
        // target 40 MiB. 66 MiB left, then only due groups: 20 and 16 MiB
        // are past the floor, the idle group's 1792 bytes are not.
        let watermark = SnapshotCadence::new(80 * MIB, 4, backstop);
        let plan = watermark.plan_pass(&groups, limits(16, 1));
        assert_eq!(plan.pressure, LogPressure::Watermark);
        assert_eq!(plan.groups, vec![1, 2, 3]);
        // Against a 64 MiB budget it is past the budget, target 32 MiB: 66
        // and 36 MiB left, then the due 16 MiB group.
        let escalated = SnapshotCadence::new(64 * MIB, 4, backstop);
        let plan = escalated.plan_pass(&groups, limits(16, 1));
        assert_eq!(plan.pressure, LogPressure::Escalated { builds: 1 });
        assert_eq!(plan.groups, vec![1, 2, 3]);
        // In calm only due groups run, the most log freed per snapshot byte
        // first: 20 MiB over 1 MiB before 16 MiB over 2 MiB.
        let calm = SnapshotCadence::new(GIB, 4, backstop);
        let plan = calm.plan_pass(&groups, limits(16, 1));
        assert_eq!(plan.pressure, LogPressure::Calm);
        assert_eq!(plan.groups, vec![2, 3]);
    }

    /// #519: an escalated pass starts `max_groups` groups per build it
    /// keeps in flight, so it lasts about as long as a pass below the
    /// budget.
    #[test]
    fn an_escalated_pass_starts_more_groups_the_more_builds_it_runs() {
        // 8 MiB of log over 8 MiB snapshots: no group is due on its own.
        let cadence = SnapshotCadence::new(64 * MIB, 16, 100_000);
        let groups = drive(&[progress(8 * MIB, 8 * MIB); 16]);
        assert!(!cadence.is_due(&groups[0].progress));
        // 128 MiB is the hard limit: four builds, eight groups.
        let plan = cadence.plan_pass(&groups, limits(2, 4));
        assert_eq!(plan.pressure, LogPressure::Escalated { builds: 4 });
        assert_eq!(plan.groups, (0..8).collect::<Vec<_>>());
        // At the budget: one build, two groups.
        let plan = cadence.plan_pass(&groups[..8], limits(2, 4));
        assert_eq!(plan.pressure, LogPressure::Escalated { builds: 1 });
        assert_eq!(plan.groups, vec![0, 1]);
    }

    /// A group whose build is in flight is never planned again (OpenRaft
    /// drops the trigger), and the log that build will free counts as freed.
    #[test]
    fn a_pass_skips_builds_in_flight_and_counts_their_log_as_freed() {
        // Four 16 MiB groups over 16 MiB snapshots, none due, fill a 64 MiB
        // budget, whose target is 32 MiB.
        let cadence = SnapshotCadence::new(64 * MIB, 4, 100_000);
        let mut groups = drive(&[progress(16 * MIB, 16 * MIB); 4]);
        assert_eq!(cadence.plan_pass(&groups, limits(16, 1)).groups, vec![0, 1]);
        groups[0].building = Some(16 * MIB);
        assert_eq!(groups[0].reclaimable_bytes(), 0);
        let plan = cadence.plan_pass(&groups, limits(16, 1));
        // The node still holds the log of the build in flight.
        assert_eq!(plan.node_log_bytes, 64 * MIB);
        assert_eq!(plan.groups, vec![1]);
        // A lagging group waits for its build in flight too.
        groups[0].lagging = true;
        assert_eq!(cadence.plan_pass(&groups, limits(16, 1)).groups, vec![1]);
    }

    #[test]
    fn gauge_counts_log_since_the_snapshot_mark() {
        let gauge = GroupLogGauge::default();
        gauge.record_applied(100);
        gauge.record_applied(50);
        let mark = gauge.mark();
        // Applied while the snapshot builds: counts toward the next one.
        gauge.record_applied(25);
        assert_eq!(gauge.snapshot_mark_bytes(), 0);
        gauge.record_snapshot(mark, 1_000, 2);
        assert_eq!(gauge.snapshot_mark_bytes(), 150);
        assert_eq!(gauge.progress(), GroupLogProgress {
            log_bytes: 25,
            log_entries: 1,
            last_snapshot_bytes: 1_000,
            has_snapshot: true,
            ..GroupLogProgress::default()
        });
        // An older mark never moves the counters back.
        gauge.record_snapshot(GroupLogMark::default(), 900, 1);
        assert_eq!(gauge.progress().log_bytes, 25);
    }

    fn owed(bytes: u64, age_ms: u64) -> RemovalProgress {
        RemovalProgress::Owed { bytes, age_ms }
    }

    /// #669: the default floor is a quarter of the default
    /// `raft.deleted_data_max_age`, and it follows a configured one.
    #[test]
    fn the_removal_floor_is_a_quarter_of_the_max_age() {
        let cadence = SnapshotCadence::new(GIB, 128, 100_000);
        assert_eq!(cadence.removal_floor, Duration::from_secs(15 * 60));
        let cadence = cadence.with_deleted_data_max_age(Duration::from_secs(60));
        assert_eq!(cadence.removal_floor, Duration::from_secs(15));
    }

    /// #669: the bytes a group removed since its last snapshot count toward
    /// its threshold, and an overdue removal makes it due whatever its log.
    #[test]
    fn removed_bytes_and_an_overdue_removal_make_a_group_due() {
        let cadence = SnapshotCadence::new(GIB, 128, 100_000)
            .with_deleted_data_max_age(Duration::from_secs(60));
        let quiet = progress(MIB, MIB);
        assert!(!cadence.is_due(&quiet));
        // 1 MiB of log and 3 MiB removed reach the 4 MiB floor.
        let removed = |removal| GroupLogProgress { removal, ..quiet };
        assert!(!cadence.is_due(&removed(owed(3 * MIB - 1, 0))));
        assert!(cadence.is_due(&removed(owed(3 * MIB, 0))));
        // A small removal is due once a quarter of the max age old.
        assert!(!cadence.removal_overdue(&removed(owed(1, 14_999))));
        assert!(!cadence.is_due(&removed(owed(1, 14_999))));
        assert!(cadence.removal_overdue(&removed(owed(1, 15_000))));
        assert!(cadence.is_due(&removed(owed(1, 15_000))));
        // A replayed removal is due at once.
        let replayed = removed(RemovalProgress::Replayed {
            bytes: 1,
            age_ms: 0,
        });
        assert!(cadence.removal_overdue(&replayed) && cadence.is_due(&replayed));
        // A removal the snapshot dropped waits for the purge, not a build.
        let purging = removed(RemovalProgress::Purging {
            age_ms: 60_000,
            through: 9,
        });
        assert!(!cadence.removal_overdue(&purging) && !cadence.is_due(&purging));
        // Nothing applied since the last snapshot: nothing to build.
        let built = GroupLogProgress {
            log_entries: 0,
            log_bytes: 0,
            ..removed(owed(1, 60_000))
        };
        assert!(!cadence.is_due(&built));
    }

    /// #669: groups with an overdue removal lead a pass, the oldest removal
    /// first and ahead of lagging groups, under the pass's cap. Removed
    /// bytes never count toward the node's log, so a mass removal never
    /// raises the pressure.
    #[test]
    fn overdue_groups_lead_a_pass_and_removed_bytes_never_press() {
        let cadence = SnapshotCadence::new(GIB, 128, 100_000)
            .with_deleted_data_max_age(Duration::from_secs(60));
        let mut groups = drive(&[
            // Lagging, with the most log.
            progress(3 * MIB, 4 * MIB),
            // Due on its own cadence.
            progress(5 * MIB, MIB),
            GroupLogProgress {
                removal: owed(1, 20_000),
                ..progress(1_024, MIB)
            },
            GroupLogProgress {
                removal: owed(1, 30_000),
                ..progress(1_024, MIB)
            },
            GroupLogProgress {
                removal: RemovalProgress::Replayed {
                    bytes: 1,
                    age_ms: 5,
                },
                ..progress(1_024, MIB)
            },
            // Removed a lot, recently: due on its bytes, but not overdue.
            GroupLogProgress {
                removal: owed(64 * GIB, 0),
                ..progress(1_024, MIB)
            },
        ]);
        groups[0].lagging = true;
        let plan = cadence.plan_pass(&groups, limits(16, 1));
        assert_eq!(plan.pressure, LogPressure::Calm);
        assert_eq!(plan.overdue, 3);
        // The replayed removal first, then the oldest.
        assert_eq!(&plan.groups[..4], &[4, 3, 2, 0]);
        assert_eq!(plan.groups.len(), 6);
        assert_eq!(
            plan.node_log_bytes,
            8 * MIB + 4 * 1_024,
            "removed bytes are not log"
        );
        let capped = cadence.plan_pass(&groups, limits(2, 1));
        assert_eq!((capped.groups, capped.overdue), (vec![4, 3], 2));
        // A group whose build is in flight is not planned, overdue or not.
        groups[3].building = Some(0);
        let plan = cadence.plan_pass(&groups, limits(16, 1));
        assert_eq!(plan.overdue, 2);
        assert_eq!(&plan.groups[..3], &[4, 2, 0]);
    }

    /// #669 review: under pressure removal debt never runs ahead of log
    /// relief. An overdue group with little log plans after the groups
    /// that free the most.
    #[test]
    fn under_pressure_overdue_groups_wait_for_the_log() {
        let cadence = SnapshotCadence::new(64 * MIB, 4, 100_000)
            .with_deleted_data_max_age(Duration::from_secs(60));
        let groups = drive(&[
            GroupLogProgress {
                removal: owed(1, 60_000),
                ..progress(1_024, MIB)
            },
            progress(30 * MIB, 30 * MIB),
            progress(20 * MIB, 20 * MIB),
        ]);
        let plan = cadence.plan_pass(&groups, limits(16, 1));
        assert!(plan.pressure.is_pressure());
        assert_eq!(plan.overdue, 0);
        // The largest group brings the log under the target, then the due
        // group with the overdue removal. The 20 MiB group is not due.
        assert_eq!(plan.groups, vec![1, 0]);
        assert_eq!(cadence.plan_pass(&groups, limits(1, 1)).groups, vec![1]);
    }
}

/// The removal ledger of [`GroupLogGauge`] (#669). Not under madsim: its
/// clock needs a simulation runtime there.
#[cfg(all(test, not(madsim)))]
mod removal_ledger_tests {
    use super::GroupLogGauge;
    use super::Instant;
    use super::RemovalDebt;
    use super::RemovalProgress;

    #[test]
    fn a_covering_snapshot_owes_a_purge_until_the_log_is_purged_through_it() {
        let gauge = GroupLogGauge::default();
        gauge.record_applied(10);
        let before = gauge.mark();
        gauge.record_applied(10);
        gauge.record_removal(300);
        let RemovalDebt::Owed { since } = gauge.removal_debt() else {
            panic!("a removal is owed");
        };
        assert!(matches!(gauge.progress().removal, RemovalProgress::Owed {
            bytes: 300,
            ..
        }));
        // A snapshot from before the removal covers none of it.
        gauge.record_snapshot(before, 1_000, 4);
        assert_eq!(gauge.removal_debt(), RemovalDebt::Owed { since });
        assert_eq!(gauge.progress().removal.bytes(), 300);
        assert_eq!(gauge.progress().scrub_purge_through, 0);
        // One that covers it raises the watermark, and the debt lasts until
        // the log is purged through it, from when the removal applied.
        gauge.record_snapshot(gauge.mark(), 1_000, 7);
        assert_eq!(gauge.removal_debt(), RemovalDebt::Purging {
            since,
            through: 7
        });
        assert!(matches!(
            gauge.progress().removal,
            RemovalProgress::Purging { through: 7, .. }
        ));
        assert_eq!(gauge.progress().removal.bytes(), 0);
        assert!(gauge.progress().removal.age().is_some());
        assert_eq!(gauge.progress().scrub_purge_through, 7);
        gauge.observe_purged(None);
        gauge.observe_purged(Some(6));
        assert!(matches!(gauge.removal_debt(), RemovalDebt::Purging { .. }));
        gauge.observe_purged(Some(7));
        assert_eq!(gauge.removal_debt(), RemovalDebt::Clear);
        assert_eq!(gauge.progress().removal, RemovalProgress::Clear);
        // A later snapshot that covers no new removal leaves the watermark,
        // and a purge observed with nothing owed changes nothing.
        gauge.record_applied(10);
        gauge.record_snapshot(gauge.mark(), 1_000, 9);
        gauge.observe_purged(Some(9));
        assert_eq!(gauge.progress().scrub_purge_through, 7);
        assert_eq!(gauge.removal_debt(), RemovalDebt::Clear);
    }

    #[test]
    fn a_removal_during_a_build_is_owed_by_the_next_snapshot() {
        let gauge = GroupLogGauge::default();
        gauge.record_removal(100);
        let RemovalDebt::Owed { since: first } = gauge.removal_debt() else {
            panic!("a removal is owed");
        };
        let mark = gauge.mark();
        // Applied while the build runs.
        let applying = Instant::now();
        gauge.record_removal(50);
        let applied = Instant::now();
        assert_eq!(gauge.removal_debt(), RemovalDebt::Owed { since: first });
        gauge.record_snapshot(mark, 1_000, 5);
        // The build covered the first removal: the watermark moves, and the
        // second one is owed from when it applied, not from the snapshot.
        assert_eq!(gauge.progress().scrub_purge_through, 5);
        assert!(matches!(
            gauge.removal_debt(),
            RemovalDebt::Owed { since } if since >= applying && since <= applied
        ));
        assert_eq!(gauge.progress().removal.bytes(), 50);
        gauge.record_snapshot(gauge.mark(), 1_000, 6);
        assert!(matches!(gauge.removal_debt(), RemovalDebt::Purging {
            through: 6,
            ..
        }));
        assert_eq!(gauge.progress().scrub_purge_through, 6);
    }

    #[test]
    fn a_removal_while_a_purge_is_pending_is_owed_from_the_oldest() {
        let gauge = GroupLogGauge::default();
        gauge.record_removal(10);
        let RemovalDebt::Owed { since } = gauge.removal_debt() else {
            panic!("a removal is owed");
        };
        gauge.record_snapshot(gauge.mark(), 1_000, 3);
        gauge.record_removal(10);
        assert_eq!(gauge.removal_debt(), RemovalDebt::Owed { since });
    }

    #[test]
    fn a_replayed_removal_is_due_at_once_and_a_restore_purges_through_itself() {
        let gauge = GroupLogGauge::default();
        // The restore covers what applied before it, whatever it removed.
        gauge.record_restored_snapshot(gauge.mark(), 1_000, 12);
        assert_eq!(gauge.removal_debt(), RemovalDebt::Clear);
        assert_eq!(gauge.progress().scrub_purge_through, 12);
        // Nothing replayed: the debt stays clear.
        gauge.replay_ended();
        assert_eq!(gauge.removal_debt(), RemovalDebt::Clear);
        // A removal the replay applied again is due at once.
        gauge.record_removal(70);
        gauge.replay_ended();
        assert!(matches!(gauge.removal_debt(), RemovalDebt::Replayed { .. }));
        assert!(matches!(
            gauge.progress().removal,
            RemovalProgress::Replayed { bytes: 70, .. }
        ));
        // Another removal keeps it replayed, and a covering snapshot owes a
        // purge through itself.
        gauge.record_removal(5);
        assert!(matches!(gauge.removal_debt(), RemovalDebt::Replayed { .. }));
        gauge.record_snapshot(gauge.mark(), 1_000, 20);
        assert!(matches!(gauge.removal_debt(), RemovalDebt::Purging {
            through: 20,
            ..
        }));
        assert_eq!(gauge.progress().scrub_purge_through, 20);
    }

    #[test]
    fn the_log_keep_is_unknown_until_the_engine_sets_it() {
        let gauge = GroupLogGauge::default();
        assert_eq!(gauge.log_keep(), None);
        gauge.set_log_keep(8);
        assert_eq!(gauge.log_keep(), Some(8));
    }
}

/// Properties of the pass policy over random nodes (#519). Not under
/// madsim: the runner draws its seeds from the host.
#[cfg(all(test, not(madsim)))]
mod proptests {
    use std::cmp::Reverse;
    use std::collections::BTreeSet;
    use std::time::Duration;

    use proptest::collection::vec;
    use proptest::prelude::*;

    use super::DriveLimits;
    use super::GroupDriveState;
    use super::GroupLogProgress;
    use super::LogPressure;
    use super::PassStep;
    use super::RemovalProgress;
    use super::SnapshotCadence;

    const MIB: u64 = 1 << 20;
    /// The removal floor of the generated nodes (#669).
    const FLOOR_MS: u64 = 1_000;

    fn sum(values: impl Iterator<Item = u64>) -> u64 {
        values.fold(0, u64::saturating_add)
    }

    /// Removed streams a group's snapshot still holds, up to `max_bytes`,
    /// on either side of the removal floor.
    fn removal(max_bytes: u64) -> impl Strategy<Value = RemovalProgress> {
        prop_oneof![
            3 => Just(RemovalProgress::Clear),
            2 => (0..=max_bytes, 0..=FLOOR_MS.saturating_mul(2))
                .prop_map(|(bytes, age_ms)| RemovalProgress::Owed { bytes, age_ms }),
            1 => (0..=max_bytes, 0..=FLOOR_MS)
                .prop_map(|(bytes, age_ms)| RemovalProgress::Replayed { bytes, age_ms }),
            1 => (0..=FLOOR_MS.saturating_mul(2), 0..=max_bytes)
                .prop_map(|(age_ms, through)| RemovalProgress::Purging { age_ms, through }),
        ]
    }

    /// One group holding up to `max_log` bytes of log.
    fn group(max_log: u64) -> impl Strategy<Value = GroupDriveState> {
        (
            0..=max_log,
            0..=max_log,
            proptest::bool::weighted(0.2),
            proptest::option::weighted(0.25, 0..=max_log),
            any::<bool>(),
            removal(max_log.saturating_mul(4)),
        )
            .prop_map(
                |(log_bytes, last_snapshot_bytes, lagging, building, applied, removal)| {
                    GroupDriveState {
                        progress: GroupLogProgress {
                            log_bytes,
                            // Every entry carries log bytes, but a group may
                            // hold entries the estimate counts as nothing.
                            log_entries: if log_bytes > 0 || applied {
                                (log_bytes / 4096).saturating_add(1)
                            } else {
                                0
                            },
                            last_snapshot_bytes,
                            has_snapshot: last_snapshot_bytes > 0,
                            removal,
                            ..GroupLogProgress::default()
                        },
                        lagging,
                        // A build in flight frees at most the log its group holds.
                        building: building.map(|pending| pending.min(log_bytes)),
                    }
                },
            )
    }

    /// A node from calm to past its hard limit: up to an eighth of the
    /// budget per group.
    fn node() -> impl Strategy<Value = (SnapshotCadence, Vec<GroupDriveState>, DriveLimits)> {
        (1u64..=256, 0usize..=40, 1usize..=6, 1usize..=4).prop_flat_map(
            |(budget_mib, count, max_groups, max_builds)| {
                let budget = budget_mib.saturating_mul(MIB);
                (
                    Just(
                        SnapshotCadence::new(budget, count, 100_000).with_deleted_data_max_age(
                            Duration::from_millis(FLOOR_MS.saturating_mul(4)),
                        ),
                    ),
                    vec(group((budget / 8).max(1)), count..=count),
                    Just(DriveLimits {
                        max_groups,
                        max_builds,
                    }),
                )
            },
        )
    }

    /// A budget and two log sizes up to three budgets.
    fn logs() -> impl Strategy<Value = (u64, u64, u64)> {
        (1u64..=(8 << 30)).prop_flat_map(|budget| {
            let top = budget.saturating_mul(3);
            (Just(budget), 0..=top, 0..=top)
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// The pressure only rises with the log, never asks for more builds
        /// than the node runs, and asks for all of them at the hard limit.
        #[test]
        fn prop_pressure_rises_with_the_log(
            (budget, first, second) in logs(),
            max_builds in 0usize..=8,
        ) {
            let cadence = SnapshotCadence::new(budget, 128, 100_000);
            let (low, high) = (first.min(second), first.max(second));
            let pressure = cadence.pressure(high, max_builds);
            prop_assert!(cadence.pressure(low, max_builds) <= pressure);
            prop_assert!((1..=max_builds.max(1)).contains(&pressure.builds()));
            prop_assert_eq!(pressure.is_pressure(), high >= cadence.pressure_watermark_bytes());
            if high < cadence.node_budget_bytes {
                prop_assert_eq!(pressure.builds(), 1);
            }
            if high >= cadence.hard_limit_bytes() {
                prop_assert_eq!(pressure, LogPressure::Escalated { builds: max_builds.max(1) });
            }
        }

        /// Before each build a pass keeps as many builds in flight as the
        /// pressure now calls for, never more than it was planned for, and
        /// ends exactly when the log climbed past the pressure it was
        /// planned for.
        #[test]
        fn prop_a_pass_never_runs_more_builds_than_planned(
            (budget, planned_log, now_log) in logs(),
            max_builds in 0usize..=8,
        ) {
            let cadence = SnapshotCadence::new(budget, 128, 100_000);
            let planned = cadence.pressure(planned_log, max_builds);
            let now = cadence.pressure(now_log, max_builds);
            match cadence.pass_step(planned, now_log, max_builds) {
                PassStep::Outgrown => prop_assert!(now > planned),
                PassStep::Start { builds } => {
                    prop_assert!(now <= planned);
                    prop_assert_eq!(builds, now.builds());
                    prop_assert!(builds <= planned.builds());
                    prop_assert!((1..=max_builds.max(1)).contains(&builds));
                }
            }
            if now_log <= planned_log {
                prop_assert_ne!(cadence.pass_step(planned, now_log, max_builds), PassStep::Outgrown);
            }
        }

        /// #669: removed bytes neither count toward the node's log nor
        /// raise its pressure: the same node without them plans with the
        /// same log and pressure, and every group due without them is due
        /// with them.
        #[test]
        fn prop_removed_bytes_never_press_on_the_node((cadence, groups, limits) in node()) {
            let cleared = groups
                .iter()
                .map(|group| GroupDriveState {
                    progress: GroupLogProgress {
                        removal: RemovalProgress::Clear,
                        ..group.progress
                    },
                    ..*group
                })
                .collect::<Vec<_>>();
            let plan = cadence.plan_pass(&groups, limits);
            let without = cadence.plan_pass(&cleared, limits);
            prop_assert_eq!(plan.node_log_bytes, without.node_log_bytes);
            prop_assert_eq!(plan.pressure, without.pressure);
            for (group, clear) in groups.iter().zip(&cleared) {
                if cadence.is_due(&clear.progress) {
                    prop_assert!(cadence.is_due(&group.progress));
                }
            }
        }

        /// #669 review: under pressure removal debt never starves log relief.
        /// Until the log would fall to the target, a pass starts the same
        /// groups, in the same order, as it would with no removal at all.
        #[test]
        fn prop_removal_debt_never_starves_log_relief((cadence, groups, limits) in node()) {
            let cleared = groups
                .iter()
                .map(|group| GroupDriveState {
                    progress: GroupLogProgress {
                        removal: RemovalProgress::Clear,
                        ..group.progress
                    },
                    ..*group
                })
                .collect::<Vec<_>>();
            let plan = cadence.plan_pass(&groups, limits);
            prop_assume!(plan.pressure.is_pressure());
            let without = cadence.plan_pass(&cleared, limits);
            let mut left = plan.node_log_bytes.saturating_sub(sum(groups.iter().filter_map(|group| group.building)));
            let mut relief = Vec::new();
            for index in &without.groups {
                if left <= cadence.pressure_target_bytes() && !groups[*index].lagging {
                    break;
                }
                relief.push(*index);
                left = left.saturating_sub(groups[*index].progress.log_bytes);
            }
            prop_assert!(
                plan.groups.starts_with(&relief),
                "relief {:?} not leading {:?}",
                relief,
                plan.groups
            );
        }

        /// A pass never plans a group that cannot build, a group twice, or
        /// more groups than its cap. Groups with an overdue removal lead,
        /// the oldest first, then lagging groups (#669). A pass under
        /// pressure orders the rest by reclaimable log, and one that stops
        /// short of its cap either plans the node down to the target or
        /// plans every group it could.
        #[test]
        fn prop_a_pass_keeps_its_limits((cadence, groups, limits) in node()) {
            let plan = cadence.plan_pass(&groups, limits);
            let node_log = sum(groups.iter().map(|group| group.progress.log_bytes));
            prop_assert_eq!(plan.node_log_bytes, node_log);
            prop_assert_eq!(plan.pressure, cadence.pressure(node_log, limits.max_builds));
            let cap = limits.max_groups.saturating_mul(plan.pressure.builds());
            prop_assert!(plan.groups.len() <= cap);
            let startable = |group: &GroupDriveState| {
                group.building.is_none() && group.progress.log_entries > 0
            };
            let mut seen = BTreeSet::new();
            for index in &plan.groups {
                prop_assert!(seen.insert(*index), "group {} planned twice", index);
                let group = groups[*index];
                prop_assert!(startable(&group), "group {} cannot build", index);
                if !plan.pressure.is_pressure() && !group.lagging {
                    prop_assert!(cadence.is_due(&group.progress));
                }
            }
            // Overdue groups lead only a calm pass.
            let calm = !plan.pressure.is_pressure();
            let overdue =
                |group: &GroupDriveState| calm && cadence.removal_overdue(&group.progress);
            let owing = groups.iter().filter(|group| startable(group) && overdue(group)).count();
            prop_assert_eq!(plan.overdue, owing.min(cap));
            let (first, rest) = plan.groups.split_at(plan.overdue);
            prop_assert!(first.iter().all(|index| overdue(&groups[*index])));
            prop_assert!(rest.iter().all(|index| !overdue(&groups[*index])));
            let rank = |index: usize| {
                let removal = groups[index].progress.removal;
                (
                    Reverse((
                        matches!(removal, RemovalProgress::Replayed { .. }),
                        removal.age(),
                    )),
                    index,
                )
            };
            prop_assert!(first.windows(2).all(|pair| rank(pair[0]) < rank(pair[1])));
            let led = rest
                .iter()
                .take_while(|index| groups[**index].lagging)
                .count();
            let lagging = groups
                .iter()
                .filter(|group| group.lagging && startable(group) && !overdue(group))
                .count();
            prop_assert_eq!(led, lagging.min(cap.saturating_sub(plan.overdue)));
            prop_assert!(rest[led..].iter().all(|index| !groups[*index].lagging));
            let key = |index: usize| (Reverse(groups[index].reclaimable_bytes()), index);
            prop_assert!(rest[..led].windows(2).all(|pair| key(pair[0]) < key(pair[1])));
            if plan.pressure.is_pressure() {
                prop_assert!(rest[led..].windows(2).all(|pair| key(pair[0]) < key(pair[1])));
            }
            if plan.pressure.is_pressure() && plan.groups.len() < cap {
                let left = node_log
                    .saturating_sub(sum(groups.iter().filter_map(|group| group.building)))
                    .saturating_sub(sum(plan.groups.iter().map(|index| groups[*index].progress.log_bytes)));
                let all = groups.iter().filter(|group| startable(group)).count();
                prop_assert!(
                    left <= cadence.pressure_target_bytes() || plan.groups.len() == all,
                    "stopped at {} of {} groups with {} bytes left",
                    plan.groups.len(),
                    all,
                    left
                );
            }
        }
    }
}
