use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use ursula_raft::BuildsChanged;
use ursula_raft::Handoff;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftGroupMetricsSnapshot;
use ursula_raft::SNAPSHOT_HANDOFF_WAIT;
use ursula_raft::SnapshotBuildCoordinator;
use ursula_raft::WalMaintenance;
use ursula_raft::snapshot_cadence::DriveLimits;
use ursula_raft::snapshot_cadence::GroupDriveState;
use ursula_raft::snapshot_cadence::GroupLogProgress;
use ursula_raft::snapshot_cadence::LogPressure;
use ursula_raft::snapshot_cadence::PassStep;
use ursula_raft::snapshot_cadence::SnapshotCadence;
use ursula_raft::snapshot_cadence::SnapshotPlan;
use ursula_runtime::RaftDeletedDataSample;
use ursula_runtime::RaftLogPressureSample;
use ursula_runtime::RuntimeMetrics;
use ursula_runtime::ShardRuntime;
use ursula_runtime::SharedSnapshotStore;
use ursula_shard::RaftGroupId;
use ursula_stream::sim_controls::Defence;

use self::clock::Instant;
use crate::bootstrap::participation::PeerParticipation;
use crate::bootstrap::store_health::StoreGateConfig;
use crate::bootstrap::store_health::spawn_store_health_gate;

/// The driver's clock: madsim's under `cfg(madsim)`, so that a simulation
/// runs the production driver ([`SnapshotDriver`]) in virtual time.
mod clock {
    #[cfg(madsim)]
    pub(super) use madsim::time::Instant;
    #[cfg(madsim)]
    pub(super) use madsim::time::sleep;
    #[cfg(madsim)]
    pub(super) use madsim::time::timeout;
    #[cfg(not(madsim))]
    pub(super) use tokio::time::Instant;
    #[cfg(not(madsim))]
    pub(super) use tokio::time::sleep;
    #[cfg(not(madsim))]
    pub(super) use tokio::time::timeout;

    /// A task the driver spawns and keeps.
    #[cfg(madsim)]
    pub(super) type Task = madsim::task::JoinHandle<()>;
    #[cfg(not(madsim))]
    pub(super) type Task = tokio::task::JoinHandle<()>;

    #[cfg(madsim)]
    pub(super) fn spawn(task: impl std::future::Future<Output = ()> + Send + 'static) -> Task {
        madsim::task::spawn(task)
    }
    #[cfg(not(madsim))]
    pub(super) fn spawn(task: impl std::future::Future<Output = ()> + Send + 'static) -> Task {
        tokio::spawn(task)
    }
}

/// The longest a tick spends starting snapshot GC passes (#669), so that
/// they never hold the next build pass back for long.
const SNAPSHOT_GC_TICK_BUDGET: Duration = Duration::from_secs(2);
/// Longest the driver waits for a build slot (a build in flight to finish)
/// before it ends the tick.
const SNAPSHOT_PERMIT_WAIT: Duration = Duration::from_secs(60);
/// Pause between ticks while the node is over its log watermark, so a
/// pressure pass is not capped at its group count per interval.
const SNAPSHOT_PRESSURE_RETICK: Duration = Duration::from_millis(250);
/// How often the node's unsnapshotted log is checked against its hard limit,
/// and weighed again by a pass that waits for a build slot.
const LOG_PRESSURE_POLL: Duration = Duration::from_millis(100);
/// How long a group is left out of the driver's passes after a trigger of it
/// went unclaimed ([`HeldGroups`]), doubled for each such trigger in a row.
const FIRST_HOLD: Duration = Duration::from_millis(500);
/// The longest a group is left out of the driver's passes after triggers of
/// it went unclaimed ([`HeldGroups`]).
const MAX_HOLD: Duration = Duration::from_secs(16);

/// What became of one snapshot trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Trigger {
    /// The group claimed the handed-off permit, so its build runs.
    Started,
    /// The group is gone or the trigger failed.
    NotStarted,
    /// A permit handed to the group earlier is still pending (an operator's
    /// snapshot of it starts), or a build of the group started since the
    /// pass was planned: nothing was triggered, and the group is not held.
    Busy,
    /// The group did not claim the handed-off permit in time: OpenRaft
    /// dropped the trigger, or the build waits behind the group's applies.
    /// The pass goes on, and the group is left out of the passes for a while
    /// ([`HeldGroups`]).
    Unclaimed,
    /// No build slot came free in time: the pass ends.
    NoSlot,
    /// The log climbed past the pressure the pass was planned for: the pass
    /// ends, so the next one plans for the pressure now.
    Outgrown,
}

/// Trigger one group's snapshot and see the build through to its start.
///
/// Builds share the node-wide build permits, and a triggered build that
/// finds none free is refused rather than queued. Triggering a whole plan at
/// once therefore built about one group per tick while the rest were
/// refused, and under sustained writes the Raft log outgrew the node. The
/// driver takes a permit first and hands it to the group it triggers. It
/// takes one once fewer builds run than `pace` asks for, and asks again
/// whenever a build starts or ends and every [`LOG_PRESSURE_POLL`], so the
/// pass follows the node's log while it waits (#519). `pace` returns `None`
/// once the log outgrew the pass.
///
/// A group that is busy, building or with a permit handed to it, is skipped
/// before the wait for a slot as well as after it: with one slot, the wait
/// ends only once the build holding it released it, so a build of the group
/// that held the slot would have ended unseen and the trigger would go to a
/// group just built.
///
/// The wait for a slot watches the builds through `watching`, which the
/// caller keeps: after `NoSlot` it ends a later wait only on a build that
/// starts or ends after the slot wait's last check.
pub(crate) async fn trigger_snapshot_build(
    registry: &RaftGroupHandleRegistry,
    coordinator: &SnapshotBuildCoordinator,
    raft_group_id: u32,
    watching: &mut BuildsChanged,
    pace: impl FnMut() -> Option<usize>,
) -> Trigger {
    let Some(raft) = registry.get(RaftGroupId(raft_group_id)) else {
        return Trigger::NotStarted;
    };
    if coordinator.busy(raft_group_id) {
        return Trigger::Busy;
    }
    let slot = coordinator.acquire_below_watching(watching, LOG_PRESSURE_POLL, pace);
    let permit = match clock::timeout(SNAPSHOT_PERMIT_WAIT, slot).await {
        Ok(Ok(Some(permit))) => permit,
        Ok(Ok(None)) => return Trigger::Outgrown,
        Ok(Err(err)) => {
            tracing::error!(
                raft_group_id,
                %err,
                "snapshot driver: no build permit; ending this tick"
            );
            return Trigger::NoSlot;
        }
        Err(_elapsed) => {
            tracing::warn!(
                raft_group_id,
                "snapshot driver: no build slot within {SNAPSHOT_PERMIT_WAIT:?}; ending this tick"
            );
            return Trigger::NoSlot;
        }
    };
    match coordinator
        .hand_off_and_trigger(
            raft_group_id,
            permit,
            SNAPSHOT_HANDOFF_WAIT,
            raft.trigger().snapshot(),
        )
        .await
    {
        Handoff::Claimed => Trigger::Started,
        Handoff::Unclaimed => {
            // Each such trigger holds the pass up for the whole wait.
            tracing::warn!(
                raft_group_id,
                "snapshot driver: the group did not claim its build permit within \
                 {SNAPSHOT_HANDOFF_WAIT:?}; taking it back"
            );
            Trigger::Unclaimed
        }
        Handoff::TriggerFailed(err) => {
            tracing::error!("snapshot driver trigger group {raft_group_id} error: {err}");
            Trigger::NotStarted
        }
        Handoff::Busy => Trigger::Busy,
    }
}

/// The groups the snapshot driver leaves out of its passes for a while
/// because a trigger of theirs went unclaimed.
///
/// OpenRaft drops a trigger while it counts a build of the group as running,
/// which it does until RaftCore handles the build's end, and that can come
/// after the build released its permit. A pass planned in between can draw
/// such a trigger, and each holds the pass up for [`SNAPSHOT_HANDOFF_WAIT`]
/// with its build slot idle. So a group whose trigger went unclaimed is left
/// out of the passes for [`FIRST_HOLD`], and for twice as long after each
/// such trigger in a row, up to [`MAX_HOLD`], so that a group whose triggers
/// keep going unclaimed does not head every pass. A claimed trigger clears
/// it. A hold is a deadline rather than a count of passes: what it waits
/// out, RaftCore handling a build's end or a build waiting behind its
/// group's applies, takes time, while a pass lasts from milliseconds to
/// minutes (an escalated pass waits for a build slot before each trigger),
/// so a hold counted in passes lasted anywhere from half a second to
/// minutes. Holds are weighed only when a pass is planned, so a hold lasts
/// at least until the end of the pass planned during it, which can be many
/// seconds under escalated pressure.
#[derive(Debug, Default)]
struct HeldGroups {
    groups: BTreeMap<u32, Hold>,
}

#[derive(Debug, Clone, Copy)]
struct Hold {
    /// When the group's hold ends.
    until: Instant,
    /// How long its next unclaimed trigger holds it.
    next: Duration,
}

impl HeldGroups {
    /// The groups held out of a pass at `now`.
    fn held(&self, now: Instant) -> BTreeSet<u32> {
        self.groups
            .iter()
            .filter(|(_, hold)| hold.until > now)
            .map(|(group, _)| *group)
            .collect()
    }

    /// Records what became of a trigger of `raft_group_id` at `now`.
    fn triggered(&mut self, raft_group_id: u32, trigger: Trigger, now: Instant) {
        match trigger {
            Trigger::Started => {
                self.groups.remove(&raft_group_id);
            }
            Trigger::Unclaimed => {
                let hold = self.groups.entry(raft_group_id).or_insert(Hold {
                    until: now,
                    next: FIRST_HOLD,
                });
                hold.until = now.checked_add(hold.next).unwrap_or(now);
                hold.next = hold.next.saturating_mul(2).min(MAX_HOLD);
            }
            Trigger::NotStarted | Trigger::Busy | Trigger::NoSlot | Trigger::Outgrown => {}
        }
    }
}

/// Whether this tick triggers snapshot builds.
///
/// While the store health gate's latest tick was bad (the probe failed, or a
/// cold-flush write failed within the gate's flush window and none succeeded
/// since), routine snapshots are held back so the driver does not push
/// uploads into a failing store. Not once the log is over the pressure
/// watermark, nor for a group with an overdue removal (#669): a build whose
/// object write or its check fails falls back to an inline snapshot, which still truncates the log and drops the removed
/// streams, so nothing a snapshot needs depends on the store. Holding builds
/// back there would grow the log into the node-wide 503 write gate while the
/// store is merely throttling, or keep deleted data past its bound.
fn should_drive_snapshots(store_trouble: bool, urgent: bool) -> bool {
    !store_trouble || urgent
}

/// The groups of a pass the driver starts ([`SnapshotPlan::groups`]): all of
/// them, or, while the store is in trouble and the node's log is not under
/// pressure, only those with an overdue removal, which lead the plan.
pub(crate) fn groups_to_drive(plan: &SnapshotPlan, store_trouble: bool) -> usize {
    if store_trouble && !plan.pressure.is_pressure() {
        plan.overdue
    } else {
        plan.groups.len()
    }
}

/// What one pass of the snapshot driver did, which decides the pause after
/// it ([`next_pause`]).
#[derive(Debug, Clone, Copy)]
struct PassOutcome {
    /// The pressure the pass was planned for.
    pressure: LogPressure,
    /// Builds the pass started.
    triggered: u64,
    /// Builds in flight when the pass was planned or once it ended,
    /// whichever were more, counting groups whose build ended but whose
    /// snapshot OpenRaft has not published yet.
    in_flight: usize,
    /// Whether the pass ended because the log climbed to a higher pressure
    /// than it was planned for.
    outgrown: bool,
    /// Whether groups with an overdue removal were left for a later pass
    /// (#669): past the pass's cap, or with no build slot in time.
    overdue_left: bool,
}

/// The pause before the snapshot driver's next pass, at `now`, once the
/// pass is over. While the node's log is over its pressure watermark, the
/// next pass comes after [`SNAPSHOT_PRESSURE_RETICK`] when this one started
/// a build, builds were in flight or groups are held ([`HeldGroups`]), the
/// holds this pass made among them, so neither a pass's group count, a group
/// that still builds nor a held group holds the node to one pass per
/// interval. So does a pass that ended because the log climbed to a higher
/// pressure than it was planned for, and one that left groups with an
/// overdue removal for later (#669).
fn next_pause(
    interval: Duration,
    pass: PassOutcome,
    held_groups: &HeldGroups,
    now: Instant,
) -> Duration {
    let busy = pass.triggered > 0 || pass.in_flight > 0 || !held_groups.held(now).is_empty();
    if pass.outgrown || pass.overdue_left || (pass.pressure.is_pressure() && busy) {
        interval.min(SNAPSHOT_PRESSURE_RETICK)
    } else {
        interval
    }
}

/// What the node's groups still hold of removed streams (#669): the age of
/// the oldest removal no snapshot covers, and how many groups hold one past
/// [`SnapshotCadence::removal_floor`] or replayed at startup.
pub(crate) fn deleted_data_sample<'a>(
    cadence: &SnapshotCadence,
    groups: impl IntoIterator<Item = &'a GroupLogProgress>,
) -> RaftDeletedDataSample {
    groups
        .into_iter()
        .fold(RaftDeletedDataSample::default(), |sample, progress| {
            let age_ms = progress
                .removal
                .age()
                .map_or(0, |age| u64::try_from(age.as_millis()).unwrap_or(u64::MAX));
            RaftDeletedDataSample {
                oldest_age_ms: sample.oldest_age_ms.max(age_ms),
                overdue_groups: sample
                    .overdue_groups
                    .saturating_add(u64::from(cadence.removal_overdue(progress))),
            }
        })
}

/// Whether a node refuses client writes with a body while its
/// unsnapshotted Raft log is past [`SnapshotCadence::hard_limit_bytes`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LogWriteGate {
    /// From the hard limit until the log is back at
    /// [`SnapshotCadence::resume_bytes`], HTTP admission answers such writes
    /// `503 RAFT_LOG_PRESSURE`: a cluster node.
    RefuseAtHardLimit,
    /// Never: a single node, which 0.8.0 never refused for its log, and
    /// which a patch release does not start refusing.
    Open,
}

/// The snapshot policy of a node, a single node's as a cluster node's
/// (F12e, #519, #669).
pub(crate) fn snapshot_cadence(raft: &ursula_config::RaftConfig, groups: usize) -> SnapshotCadence {
    SnapshotCadence::new(
        raft.snapshot_log_budget.as_bytes(),
        groups,
        raft.snapshot_backstop_logs,
    )
    .with_deleted_data_max_age(raft.effective_deleted_data_max_age())
}

/// Sets the coordinator's log-pressure flag, which HTTP admission reads,
/// from the node's unsnapshotted Raft log bytes under `gate`.
fn gate_writes(
    coordinator: &SnapshotBuildCoordinator,
    gate: LogWriteGate,
    log_bytes: u64,
    limit: u64,
    resume: u64,
) {
    if gate == LogWriteGate::Open {
        return;
    }
    match coordinator.observe_log_bytes(log_bytes, limit, resume) {
        Some(true) => tracing::warn!(
            log_bytes,
            limit,
            "raft log over its hard limit: refusing client writes until snapshots catch up"
        ),
        Some(false) => tracing::info!(log_bytes, resume, "raft log pressure cleared"),
        None => {}
    }
}

/// Keeps the coordinator's log-pressure flag (read by HTTP admission) in step
/// with the node's unsnapshotted Raft log bytes under `gate`, and reports the
/// pressure the snapshot driver works under and the deleted data its groups
/// still hold (#669).
fn spawn_log_pressure_monitor(
    coordinator: SnapshotBuildCoordinator,
    gate: LogWriteGate,
    cadence: SnapshotCadence,
    max_builds: usize,
    metrics: RuntimeMetrics,
) {
    let limit = cadence.hard_limit_bytes();
    let resume = cadence.resume_bytes();
    tokio::spawn(async move {
        loop {
            let log_bytes = coordinator.node_log_bytes();
            let pressure = cadence.pressure(log_bytes, max_builds);
            metrics.record_raft_log_pressure(RaftLogPressureSample {
                log_bytes,
                level: pressure.level(),
                builds: u64::try_from(pressure.builds()).unwrap_or(u64::MAX),
                builds_in_flight: u64::try_from(coordinator.running_builds()).unwrap_or(u64::MAX),
            });
            metrics.record_raft_deleted_data(deleted_data_sample(
                &cadence,
                &coordinator.running_log_progress(),
            ));
            gate_writes(&coordinator, gate, log_bytes, limit, resume);
            tokio::time::sleep(LOG_PRESSURE_POLL).await;
        }
    });
}

/// The driver interval: the configured one, which config validation keeps
/// non-zero, else 5 s with an external snapshot store, whose health gate
/// ticks on the same interval, and 1 s inline.
pub(crate) fn resolve_snapshot_drive_interval(
    configured: Option<Duration>,
    snapshot_store_configured: bool,
) -> Duration {
    configured.unwrap_or(if snapshot_store_configured {
        Duration::from_secs(5)
    } else {
        Duration::from_secs(1)
    })
}

/// One group's log progress as the driver sees it (F12e): the state
/// machine's log gauge, plus OpenRaft's view of whether a snapshot exists.
/// A group with no applied state reports no log, so it is never due: an
/// empty in-memory node must not publish a `group-X-empty` snapshot.
pub(crate) fn group_log_progress(snapshot: &RaftGroupMetricsSnapshot) -> GroupLogProgress {
    if snapshot.last_applied.is_none() {
        return GroupLogProgress::default();
    }
    let mut progress = snapshot.log;
    progress.has_snapshot |= snapshot.snapshot.is_some();
    progress
}

/// The groups to snapshot this pass under the byte-based cadence (F12e) and
/// the node's log pressure (#519): the groups the Raft WAL reports
/// `lagging` first, never a group with a build in `building` (the log each
/// such build will still free) or one the driver `held` out of the pass
/// ([`HeldGroups`], counted as building with nothing to free), within
/// `limits`. `building` also holds, with nothing to free, each group whose
/// build ended with a snapshot OpenRaft has not published yet
/// ([`SnapshotBuildCoordinator::add_publishing`]).
pub(crate) fn plan_snapshot_drive<'a>(
    snapshots: &'a [RaftGroupMetricsSnapshot],
    cadence: &SnapshotCadence,
    limits: DriveLimits,
    lagging: &BTreeSet<RaftGroupId>,
    building: &BTreeMap<u32, u64>,
    held: &BTreeSet<u32>,
) -> (SnapshotPlan, Vec<&'a RaftGroupMetricsSnapshot>) {
    let groups = snapshots
        .iter()
        .map(|snapshot| GroupDriveState {
            progress: if snapshot.apply_failure.is_some() {
                GroupLogProgress::default()
            } else {
                group_log_progress(snapshot)
            },
            lagging: lagging.contains(&RaftGroupId(snapshot.raft_group_id)),
            building: building
                .get(&snapshot.raft_group_id)
                .copied()
                .or_else(|| held.contains(&snapshot.raft_group_id).then_some(0)),
        })
        .collect::<Vec<_>>();
    let plan = cadence.plan_pass(&groups, limits);
    let selected = plan
        .groups
        .iter()
        .filter_map(|index| snapshots.get(*index))
        .collect();
    (plan, selected)
}

/// What the snapshot driver hands its snapshot-store health gate.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StoreGateInputs<'a> {
    /// `[storage.cold.s3]`, or `None` for its defaults.
    pub s3: Option<&'a ursula_config::S3Config>,
    /// `storage.cold.flush_interval`, the flush worker's pause between
    /// passes, which the gate's flush window spans.
    pub cold_flush_interval: Duration,
    /// The node's peers, which take over the leaderships the gate sheds.
    pub participation: &'a PeerParticipation,
}

/// A purge of one group's log through the snapshot that covers its removals
/// (#669), from [`plan_scrub_purges`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScrubPurge {
    pub(crate) raft_group_id: u32,
    /// The log index the purge goes through.
    pub(crate) through: u64,
    /// The last log index of the snapshot OpenRaft's metrics show.
    pub(crate) published: u64,
}

/// The purges through a covering snapshot this node owes (#669).
///
/// OpenRaft purges a group's log only up to `raft.max_in_snapshot_log_to_keep`
/// entries below a snapshot it built, so on a quiet group the entries just
/// below the snapshot that dropped a removed stream, its records among them,
/// would stay until a later snapshot moved past them. A group whose latest
/// snapshot covering a removal, or the snapshot it restored at startup, is
/// past its purged log is purged through it once OpenRaft's metrics show that
/// snapshot. A group whose Raft is not running is left alone.
pub(crate) fn plan_scrub_purges(snapshots: &[RaftGroupMetricsSnapshot]) -> Vec<ScrubPurge> {
    if ursula_stream::sim_controls::off(Defence::ScrubPurge) {
        return Vec::new();
    }
    let purges = snapshots
        .iter()
        .filter(|snapshot| snapshot.apply_failure.is_none() && snapshot.maintenance.running)
        .filter_map(|snapshot| {
            let published = snapshot.snapshot?.index;
            let purged = snapshot.purged.map(|purged| purged.index);
            let through = snapshot.log.scrub_purge_through;
            (through > 0 && Some(through) > purged && through <= published).then_some(ScrubPurge {
                raft_group_id: snapshot.raft_group_id,
                through,
                published,
            })
        })
        .collect::<Vec<_>>();
    if !purges.is_empty() {
        ursula_stream::sim_controls::note_fired(Defence::ScrubPurge);
    }
    purges
}

/// The purges through its snapshot of each group the Raft WAL reports
/// lagging that applied nothing since that snapshot (#669).
///
/// The WAL frees a journal segment that held dead frames too long by
/// copying out the few records groups still hold there, and reports a group
/// holding more as lagging, for the driver to snapshot first. A group that
/// applied nothing since its snapshot has nothing to build, and the entries
/// OpenRaft keeps below that snapshot (`raft.max_in_snapshot_log_to_keep`)
/// would pin the segment for good. Such a group's log is purged through its
/// snapshot instead, once OpenRaft's metrics show it. A group whose Raft is
/// not running, or whose apply stopped, is left alone.
pub(crate) fn plan_lagging_purges(
    snapshots: &[RaftGroupMetricsSnapshot],
    lagging: &BTreeSet<RaftGroupId>,
) -> Vec<ScrubPurge> {
    snapshots
        .iter()
        .filter(|snapshot| snapshot.apply_failure.is_none() && snapshot.maintenance.running)
        .filter(|snapshot| lagging.contains(&RaftGroupId(snapshot.raft_group_id)))
        .filter(|snapshot| snapshot.log.log_entries == 0)
        .filter_map(|snapshot| {
            let published = snapshot.snapshot?.index;
            let purged = snapshot.purged.map(|purged| purged.index);
            (published > 0 && Some(published) > purged).then_some(ScrubPurge {
                raft_group_id: snapshot.raft_group_id,
                through: published,
                published,
            })
        })
        .collect()
}

/// The purges a tick starts: [`plan_scrub_purges`] and
/// [`plan_lagging_purges`], at most one per group, through the higher index.
pub(crate) fn plan_purges(
    snapshots: &[RaftGroupMetricsSnapshot],
    lagging: &BTreeSet<RaftGroupId>,
) -> Vec<ScrubPurge> {
    let mut purges = BTreeMap::<u32, ScrubPurge>::new();
    for purge in plan_scrub_purges(snapshots)
        .into_iter()
        .chain(plan_lagging_purges(snapshots, lagging))
    {
        let entry = purges.entry(purge.raft_group_id).or_insert(purge);
        if purge.through > entry.through {
            *entry = purge;
        }
    }
    purges.into_values().collect()
}

/// What the snapshot driver works with besides its registry.
#[derive(Debug, Clone, Copy)]
pub struct SnapshotDriveSettings {
    /// The pause between ticks.
    pub interval: Duration,
    /// The node's snapshot policy (F12e, #519, #669).
    pub cadence: SnapshotCadence,
    /// `raft.snapshot_pressure_max_groups_per_tick`.
    pub max_groups_per_tick: usize,
}

/// The node's snapshot driver: one per node, over every group its registry
/// holds, leader, follower or learner.
///
/// Each tick ([`SnapshotDriver::tick`]) plans one pass
/// ([`plan_snapshot_drive`]) and starts its builds in order, at most as many
/// at once as the node's log pressure calls for now (#519,
/// [`SnapshotCadence::pass_step`]): fewer as the pass brings the log down,
/// one below the budget. A pass ends early once the log has climbed to a
/// higher pressure than when the pass was planned, so the next pass plans
/// for that. Before its pass a tick purges each group's log through the
/// snapshot that covers its removals ([`plan_scrub_purges`], #669), and
/// holds the group's builds until that purge lands
/// ([`SnapshotBuildCoordinator::hold_for_purge`]). It purges a lagging group
/// that applied nothing since its snapshot through that snapshot the same
/// way ([`plan_lagging_purges`]). It then starts a maintenance pass of every
/// core journal of the node's Raft WAL ([`WalMaintenance::maintain`]) in a
/// task of its own, at most once per interval, which frees journal segments
/// that held dead frames for a quarter of `raft.deleted_data_max_age`. After
/// its pass a tick runs the snapshot GC passes that are due, for at most
/// `max_groups_per_tick` groups
/// ([`RaftGroupHandleRegistry::collect_snapshot_garbage`], #669): each lists
/// the node's own snapshot objects of its group and deletes those the group
/// no longer holds, such as the object it replaced once no send of it is in
/// flight (#687).
pub struct SnapshotDriver {
    registry: RaftGroupHandleRegistry,
    coordinator: SnapshotBuildCoordinator,
    cadence: SnapshotCadence,
    limits: DriveLimits,
    interval: Duration,
    wal: Option<WalMaintenance>,
    /// When the last maintenance pass of the Raft WAL started, and the task
    /// that runs it. A pass runs at most once per `interval`, however often
    /// the driver ticks, so the journals' copies keep their rate, and the
    /// next one waits for the last one to end.
    maintenance: Maintenance,
    /// The lagging groups the last tick logged.
    last_lagging: BTreeSet<RaftGroupId>,
    metrics: RuntimeMetrics,
    held_groups: HeldGroups,
    /// The groups held out of the last pass, so that passes that only carry
    /// the same holds are neither logged nor counted.
    last_held: BTreeSet<u32>,
    /// Set when the last pass ended without a build slot for an overdue
    /// group (#669): the pause then ends once a build starts or ends. The
    /// subscription was taken before that group's wait for a slot, so a
    /// build that ends before the pause still ends it.
    wait_for_slot: Option<BuildsChanged>,
}

/// The driver's maintenance passes of the Raft WAL.
enum Maintenance {
    /// None started yet.
    Idle,
    /// The last one started `at`, in `task`.
    Started { at: Instant, task: clock::Task },
}

impl Maintenance {
    /// Whether a pass starts at `now`: once `interval` passed since the
    /// last one started and that one ended.
    fn due(&self, now: Instant, interval: Duration) -> bool {
        match self {
            Self::Idle => true,
            Self::Started { at, task } => {
                now.saturating_duration_since(*at) >= interval && task.is_finished()
            }
        }
    }
}

impl Drop for SnapshotDriver {
    /// The pass in flight goes with the driver, so that nothing of it keeps
    /// a journal writer.
    fn drop(&mut self) {
        if let Maintenance::Started { task, .. } = &self.maintenance {
            task.abort();
        }
    }
}

impl SnapshotDriver {
    pub fn new(
        registry: &RaftGroupHandleRegistry,
        metrics: RuntimeMetrics,
        wal: Option<WalMaintenance>,
        settings: SnapshotDriveSettings,
    ) -> Self {
        let coordinator = registry.snapshot_build_coordinator();
        let limits = DriveLimits {
            max_groups: settings.max_groups_per_tick,
            max_builds: coordinator.max_concurrency(),
        };
        Self {
            registry: registry.clone(),
            coordinator,
            cadence: settings.cadence,
            limits,
            interval: settings.interval,
            wal,
            maintenance: Maintenance::Idle,
            last_lagging: BTreeSet::new(),
            metrics,
            held_groups: HeldGroups::default(),
            last_held: BTreeSet::new(),
            wait_for_slot: None,
        }
    }

    /// Ticks until the task is dropped, without a store health gate: a
    /// simulation's driver.
    #[cfg(madsim)]
    pub async fn run(mut self) {
        loop {
            let pause = self.tick(false).await;
            self.pause(pause).await;
        }
    }

    /// Waits `pause`, or until the WAL's lagging groups change, so that they
    /// are snapshotted and purged sooner.
    /// A pass that left an overdue group without a build slot ends its
    /// pause once a build starts or ends instead (#669).
    pub async fn pause(&mut self, pause: Duration) {
        let slot = self.wait_for_slot.take();
        let builds = async move {
            match slot {
                Some(builds) => builds.changed().await,
                None => std::future::pending::<()>().await,
            }
        };
        let lagging = async {
            match &self.wal {
                Some(wal) => {
                    let lagging = wal.lagging_groups();
                    lagging.changed().await;
                }
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            biased;
            () = clock::sleep(pause) => {}
            () = lagging => {}
            () = builds => {}
        }
    }

    /// One tick: the purges through covering snapshots and those of idle
    /// lagging groups, a maintenance pass of the Raft WAL, then a pass of
    /// snapshot builds. `store_trouble` says that the store health gate's
    /// latest tick was bad ([`should_drive_snapshots`]). Returns the pause
    /// before the next tick.
    pub async fn tick(&mut self, store_trouble: bool) -> Duration {
        let coordinator = self.coordinator.clone();
        let cadence = self.cadence;
        let limits = self.limits;
        let lagging = self
            .wal
            .as_ref()
            .map(|wal| wal.lagging_groups().groups())
            .unwrap_or_default();
        // The builds first: a group whose build ends after this read still
        // counts as in flight, and one whose build ended before it shows, in
        // the metrics read next, the log after the build freed it, never the
        // log before. OpenRaft keeps counting a build until RaftCore handles
        // its end, which can come after the build released its permit, and
        // drops a trigger in between, so a group whose build made a snapshot
        // the metrics do not show yet counts as building too, with nothing
        // left to free. Read after the metrics, so a snapshot published in
        // between still counts. A trigger that goes unclaimed all the same
        // holds its group out of the passes for a while (`HeldGroups`). A
        // group a purge through a snapshot holds (#669) counts as building
        // with nothing left to free as well.
        let mut building = coordinator.builds_in_flight();
        let held = self.held_groups.held(Instant::now());
        let snaps = self.registry.metrics_snapshot();
        // The purges through covering snapshots go before the pass (#669):
        // they are cheap, and a pass may wait long for build slots and
        // hand-offs. A group they hold counts as building below. The WAL's
        // maintenance pass starts before the pass too, in a task of its own,
        // and copies at most a pass's budget per core.
        self.purge_through_snapshots(&snaps, &lagging).await;
        self.maintain_wal();
        coordinator.add_publishing(
            &mut building,
            snaps.iter().map(|snapshot| {
                (
                    snapshot.raft_group_id,
                    snapshot.snapshot.as_ref().map(|published| published.index),
                )
            }),
        );
        coordinator.add_purging(&mut building);
        let mut pause = self.interval;
        if !lagging.is_empty() && lagging != self.last_lagging {
            tracing::info!(
                groups = ?lagging,
                "snapshot driver: the Raft WAL reports groups keeping old segments"
            );
        }
        self.last_lagging.clone_from(&lagging);
        let (plan, mut selected) =
            plan_snapshot_drive(&snaps, &cadence, limits, &lagging, &building, &held);
        // Overdue groups the pass could not plan for its cap. Those that
        // build or are held wait for that.
        let overdue_total = snaps
            .iter()
            .filter(|snapshot| snapshot.apply_failure.is_none() && snapshot.maintenance.running)
            .filter(|snapshot| {
                !building.contains_key(&snapshot.raft_group_id)
                    && !held.contains(&snapshot.raft_group_id)
            })
            .filter(|snapshot| {
                let progress = group_log_progress(snapshot);
                progress.log_entries > 0 && cadence.removal_overdue(&progress)
            })
            .count();
        // Under pressure overdue groups plan as due groups, paced by the
        // pressure pass.
        let mut overdue_left = !plan.pressure.is_pressure() && overdue_total > plan.overdue;
        // Measured the way the pass measures it below, so only a rise
        // during the pass ends it.
        let planned_pressure = cadence.pressure(coordinator.node_log_bytes(), limits.max_builds);
        if should_drive_snapshots(
            store_trouble,
            plan.pressure.is_pressure() || plan.overdue > 0,
        ) {
            selected.truncate(groups_to_drive(&plan, store_trouble));
            let started_at = Instant::now();
            let builds = plan.pressure.builds();
            let planned = selected.len();
            let mut triggered = 0u64;
            let mut not_started = 0u64;
            let mut busy = 0u64;
            let mut unclaimed = 0u64;
            let mut outgrown = false;
            // The builds the pass keeps in flight as the log moves, or none
            // once the log outgrew the pass.
            let pace = || match cadence.pass_step(
                planned_pressure,
                coordinator.node_log_bytes(),
                limits.max_builds,
            ) {
                PassStep::Outgrown => None,
                PassStep::Start { builds: now } => Some(now),
            };
            for (position, snapshot) in selected.into_iter().enumerate() {
                // The slot wait's own subscription, so that no build end
                // between a `NoSlot` and the pause goes unseen, and none
                // during the wait ends the pause at once.
                let mut builds = coordinator.subscribe_builds();
                let trigger = trigger_snapshot_build(
                    &self.registry,
                    &coordinator,
                    snapshot.raft_group_id,
                    &mut builds,
                    pace,
                )
                .await;
                self.held_groups
                    .triggered(snapshot.raft_group_id, trigger, Instant::now());
                match trigger {
                    Trigger::Started => triggered = triggered.saturating_add(1),
                    Trigger::NotStarted => not_started = not_started.saturating_add(1),
                    Trigger::Busy => busy = busy.saturating_add(1),
                    Trigger::Unclaimed => unclaimed = unclaimed.saturating_add(1),
                    Trigger::NoSlot => {
                        // An overdue group left without a slot waits for one
                        // on the builds, not on a timer.
                        self.wait_for_slot = (position < plan.overdue).then_some(builds);
                        break;
                    }
                    Trigger::Outgrown => {
                        overdue_left |= position < plan.overdue;
                        outgrown = true;
                        break;
                    }
                }
            }
            pause = next_pause(
                self.interval,
                PassOutcome {
                    pressure: plan.pressure,
                    triggered,
                    in_flight: building.len().max(coordinator.running_builds()),
                    outgrown,
                    overdue_left,
                },
                &self.held_groups,
                Instant::now(),
            );
            // Counts and logs the passes that planned a build or held other
            // groups out than the last pass: one that found nothing to start
            // while builds run or the same groups are held only waits for
            // them.
            let held_changed = held != self.last_held;
            if plan.pressure.is_pressure() && (planned > 0 || outgrown || held_changed) {
                let elapsed = started_at.elapsed();
                self.metrics.record_raft_snapshot_pressure(
                    triggered,
                    u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
                );
                tracing::info!(
                    node_log_bytes = plan.node_log_bytes,
                    node_log_budget = cadence.node_budget_bytes,
                    level = plan.pressure.level(),
                    builds,
                    building = building.len(),
                    held = held.len(),
                    planned,
                    triggered,
                    not_started,
                    busy,
                    unclaimed,
                    outgrown,
                    pass_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
                    "raft snapshot pressure pass completed"
                );
            }
            if plan.overdue > 0 {
                tracing::info!(
                    overdue = plan.overdue,
                    triggered,
                    "snapshot driver: snapshotting groups that still hold deleted streams"
                );
            }
        }
        self.last_held = held;
        // Snapshot GC after the pass, within a time budget, so that store
        // calls never hold back builds for long. None while the store is in
        // trouble or the log is under pressure: the passes stay due, and a
        // failed one runs again later.
        if !store_trouble && !plan.pressure.is_pressure() {
            let gc = self
                .registry
                .collect_snapshot_garbage(self.limits.max_groups, SNAPSHOT_GC_TICK_BUDGET)
                .await;
            self.metrics
                .record_snapshot_gc(ursula_runtime::SnapshotGcSample {
                    deleted: gc.deleted,
                    failed: u64::try_from(gc.failed).unwrap_or(u64::MAX),
                });
            if gc.left > 0 {
                pause = pause.min(SNAPSHOT_PRESSURE_RETICK);
            }
        }
        pause
    }

    /// Starts a maintenance pass of every core journal as of now, freeing
    /// segments that held dead frames for the removal floor, a quarter of
    /// `raft.deleted_data_max_age` (#669). It runs in a task of its own, so
    /// a core writer that is slow to answer holds up no snapshot build, and
    /// at most once per drive interval, so ticks that come sooner under
    /// pressure or after the lagging groups changed copy no more.
    fn maintain_wal(&mut self) {
        let Some(wal) = &self.wal else {
            return;
        };
        let now = Instant::now();
        if !self.maintenance.due(now, self.interval) {
            return;
        }
        let wal = wal.clone();
        let max_age = self.cadence.removal_floor;
        let task = clock::spawn(async move {
            if let Err(err) = wal.maintain(now, max_age).await {
                tracing::warn!(
                    %err,
                    "snapshot driver: a Raft WAL maintenance pass did not run; a later tick retries"
                );
            }
        });
        self.maintenance = Maintenance::Started { at: now, task };
    }

    /// Purges each group's log through the snapshot that covers its
    /// removals ([`plan_scrub_purges`]), and each idle lagging group's
    /// through its snapshot ([`plan_lagging_purges`]), holding the group's
    /// builds until the purge lands. A group whose snapshot work is not idle
    /// ([`ursula_raft::GroupSnapshotWork`]) is purged on a later tick.
    async fn purge_through_snapshots(
        &self,
        snaps: &[RaftGroupMetricsSnapshot],
        lagging: &BTreeSet<RaftGroupId>,
    ) {
        for purge in plan_purges(snaps, lagging) {
            let Some(raft) = self.registry.get(RaftGroupId(purge.raft_group_id)) else {
                continue;
            };
            let guard = match self.coordinator.hold_for_purge(
                purge.raft_group_id,
                Some(purge.published),
                purge.through,
                raft.metrics(),
            ) {
                Ok(guard) => guard,
                Err(busy) => {
                    tracing::debug!(
                        through = purge.through,
                        %busy,
                        "snapshot driver: purging through the covering snapshot later"
                    );
                    continue;
                }
            };
            if let Err(err) = raft.trigger().purge_log(purge.through).await {
                // Dropping the guard lifts the hold.
                drop(guard);
                tracing::error!(
                    raft_group_id = purge.raft_group_id,
                    through = purge.through,
                    %err,
                    "snapshot driver: purge through the covering snapshot failed"
                );
            } else {
                guard.keep();
                tracing::info!(
                    raft_group_id = purge.raft_group_id,
                    through = purge.through,
                    "snapshot driver: purging the log through its snapshot"
                );
            }
        }
    }
}

/// Config-driven snapshot driver. Reads parameters from typed config.
///
/// With an external snapshot store it also spawns the store health gate
/// ([`spawn_store_health_gate`]) on the same interval, and owns it: the gate
/// stops when the driver does. Only an external store is probed: inline
/// snapshots depend on nothing outside the node, so the inline driver never
/// sheds leadership. `write_gate` says whether the node refuses client
/// writes while its log is past the hard limit. The driver alone takes every
/// snapshot: OpenRaft's own snapshot policy is off on every group.
pub fn spawn_snapshot_driver(
    runtime: &ShardRuntime,
    registry: &RaftGroupHandleRegistry,
    store: Option<(SharedSnapshotStore, StoreGateInputs<'_>)>,
    write_gate: LogWriteGate,
    interval: Duration,
    cadence: SnapshotCadence,
    wal: Option<WalMaintenance>,
    max_groups_per_tick: usize,
) {
    // Config validation refuses a zero `storage.snapshot.drive_interval`:
    // the driver would tick without pause.
    debug_assert!(
        !interval.is_zero(),
        "the snapshot drive interval is non-zero"
    );
    let mut gate = store.map(|(store, store_gate)| {
        let default_s3 = ursula_config::S3Config::default();
        spawn_store_health_gate(
            runtime,
            registry,
            store,
            store_gate.participation.clone(),
            StoreGateConfig::new(
                store_gate.s3.unwrap_or(&default_s3),
                store_gate.cold_flush_interval,
                interval,
            ),
        )
    });
    let mut driver = SnapshotDriver::new(registry, runtime.metrics(), wal, SnapshotDriveSettings {
        interval,
        cadence,
        max_groups_per_tick,
    });
    spawn_log_pressure_monitor(
        driver.coordinator.clone(),
        write_gate,
        cadence,
        driver.limits.max_builds,
        runtime.metrics(),
    );
    tokio::spawn(async move {
        loop {
            let store_trouble = match gate.as_mut().map(|gate| gate.latest_bad()) {
                None => false,
                Some(Ok(latest_bad)) => latest_bad,
                Some(Err(err)) => {
                    tracing::error!(
                        %err,
                        "snapshot driver: the store is no longer judged; driving routine \
                         snapshots as if it were healthy"
                    );
                    gate = None;
                    false
                }
            };
            let pause = driver.tick(store_trouble).await;
            driver.pause(pause).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use ursula_raft::SnapshotBuildCoordinator;
    use ursula_raft::snapshot_cadence::LogPressure;

    use super::FIRST_HOLD;
    use super::HeldGroups;
    use super::LogWriteGate;
    use super::PassOutcome;
    use super::SNAPSHOT_PRESSURE_RETICK;
    use super::Trigger;
    use super::clock::Instant;
    use super::gate_writes;
    use super::next_pause;
    use super::should_drive_snapshots;

    fn pass(
        pressure: LogPressure,
        triggered: u64,
        in_flight: usize,
        outgrown: bool,
    ) -> PassOutcome {
        PassOutcome {
            pressure,
            triggered,
            in_flight,
            outgrown,
            overdue_left: false,
        }
    }

    /// #519: under pressure the driver plans again soon while its passes
    /// start builds, builds run or groups are held, so a group whose build
    /// is in flight, and skipped for it, is triggered again soon after the
    /// build ends, and a held group soon after its hold ends.
    #[test]
    fn a_pressure_pass_plans_again_soon_while_builds_start_or_run() {
        let interval = Duration::from_secs(5);
        let escalated = LogPressure::Escalated { builds: 2 };
        let now = Instant::now();
        let none = HeldGroups::default();
        let mut one = HeldGroups::default();
        one.triggered(7, Trigger::Unclaimed, now);
        assert_eq!(
            next_pause(interval, pass(LogPressure::Calm, 3, 2, false), &one, now),
            interval
        );
        assert_eq!(
            next_pause(
                interval,
                pass(LogPressure::Watermark, 1, 0, false),
                &none,
                now
            ),
            SNAPSHOT_PRESSURE_RETICK
        );
        assert_eq!(
            next_pause(interval, pass(escalated, 0, 1, false), &none, now),
            SNAPSHOT_PRESSURE_RETICK
        );
        // Nothing started and nothing runs: the log is at its target.
        assert_eq!(
            next_pause(interval, pass(escalated, 0, 0, false), &none, now),
            interval
        );
        // A held group comes back once its hold ends, so passes come soon.
        assert_eq!(
            next_pause(interval, pass(escalated, 0, 0, false), &one, now),
            SNAPSHOT_PRESSURE_RETICK
        );
        // A pass the log outgrew goes again soon.
        assert_eq!(
            next_pause(interval, pass(LogPressure::Calm, 0, 0, true), &none, now),
            SNAPSHOT_PRESSURE_RETICK
        );
        // A shorter interval stays.
        let short = Duration::from_millis(100);
        assert_eq!(
            next_pause(short, pass(escalated, 1, 0, false), &none, now),
            short
        );
    }

    /// #561: a pressure pass whose only trigger went unclaimed, with no
    /// build started or in flight, plans again soon for the hold it made,
    /// which the groups it held out when it began do not include. Once the
    /// hold ended the node is back to one pass per interval.
    #[test]
    fn a_pressure_pass_plans_again_soon_for_the_hold_it_made() {
        let interval = Duration::from_secs(5);
        let now = Instant::now();
        let mut held = HeldGroups::default();
        assert!(held.held(now).is_empty());
        held.triggered(7, Trigger::Unclaimed, now);
        let outcome = pass(LogPressure::Escalated { builds: 2 }, 0, 0, false);
        assert_eq!(
            next_pause(interval, outcome, &held, now),
            SNAPSHOT_PRESSURE_RETICK
        );
        assert_eq!(
            next_pause(interval, outcome, &held, now + FIRST_HOLD),
            interval
        );
    }

    /// A group whose trigger went unclaimed sits out the passes for half a
    /// second, twice as long after each such trigger in a row, at most 16 s,
    /// until a trigger of it is claimed.
    #[test]
    fn a_group_whose_trigger_went_unclaimed_sits_out_the_passes_for_a_while() {
        let mut held = HeldGroups::default();
        let now = Instant::now();
        let held_for = |held: &HeldGroups| {
            held.groups
                .get(&7)
                .map(|hold| hold.until.saturating_duration_since(now))
        };
        held.triggered(7, Trigger::Unclaimed, now);
        assert!(held.held(now + Duration::from_millis(499)).contains(&7));
        assert!(held.held(now + FIRST_HOLD).is_empty());
        for secs in [1, 2, 4, 8, 16, 16] {
            held.triggered(7, Trigger::Unclaimed, now);
            assert_eq!(held_for(&held), Some(Duration::from_secs(secs)));
        }
        // A claimed trigger clears it.
        held.triggered(7, Trigger::Started, now);
        assert!(held.held(now).is_empty());
        held.triggered(7, Trigger::Unclaimed, now);
        assert_eq!(held_for(&held), Some(FIRST_HOLD));
        // Other outcomes leave it as it was.
        for trigger in [
            Trigger::NotStarted,
            Trigger::Busy,
            Trigger::NoSlot,
            Trigger::Outgrown,
        ] {
            held.triggered(7, trigger, now);
        }
        assert_eq!(held_for(&held), Some(FIRST_HOLD));
        held.triggered(7, Trigger::Unclaimed, now);
        assert_eq!(held_for(&held), Some(Duration::from_secs(1)));
        // Each group is held on its own.
        held.triggered(8, Trigger::Unclaimed, now);
        assert_eq!(
            held.held(now + Duration::from_millis(250)),
            BTreeSet::from([7, 8])
        );
        assert_eq!(
            held.held(now + Duration::from_millis(750)),
            BTreeSet::from([7])
        );
    }

    /// #669: a pass that left groups with an overdue removal for later
    /// plans again soon, under pressure or not.
    #[test]
    fn a_pass_that_left_overdue_groups_plans_again_soon() {
        let interval = Duration::from_secs(5);
        let now = Instant::now();
        let none = HeldGroups::default();
        let mut left = pass(LogPressure::Calm, 1, 0, false);
        assert_eq!(next_pause(interval, left, &none, now), interval);
        left.overdue_left = true;
        assert_eq!(
            next_pause(interval, left, &none, now),
            SNAPSHOT_PRESSURE_RETICK
        );
    }

    /// #669: a single node, which 0.8.0 never refused for its log, never
    /// closes admission for it. A cluster node does past the hard limit,
    /// until its log is back at the resume level.
    #[test]
    fn a_single_node_never_refuses_writes_for_its_log() {
        let coordinator = SnapshotBuildCoordinator::default();
        gate_writes(&coordinator, LogWriteGate::Open, 10, 5, 3);
        assert!(!coordinator.log_pressured());
        gate_writes(&coordinator, LogWriteGate::RefuseAtHardLimit, 10, 5, 3);
        assert!(coordinator.log_pressured());
        gate_writes(&coordinator, LogWriteGate::RefuseAtHardLimit, 4, 5, 3);
        assert!(coordinator.log_pressured());
        gate_writes(&coordinator, LogWriteGate::RefuseAtHardLimit, 2, 5, 3);
        assert!(!coordinator.log_pressured());
    }

    #[test]
    fn store_trouble_holds_back_routine_snapshots_but_not_a_pressure_pass() {
        assert!(should_drive_snapshots(false, false));
        assert!(!should_drive_snapshots(true, false));
        assert!(should_drive_snapshots(true, true));
        assert!(should_drive_snapshots(false, true));
    }
}
