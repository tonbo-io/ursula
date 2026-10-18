use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::Stream;
use futures_util::TryStreamExt;
use openraft::EntryPayload;
use openraft::RaftMetrics;
use openraft::alias::LogIdOf;
use openraft::alias::SnapshotDataOf;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::SnapshotOf;
use openraft::alias::StoredMembershipOf;
use openraft::rt::WatchReceiver;
use openraft::storage::EntryResponder;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use openraft::type_config::alias::WatchReceiverOf;
use serde::Deserialize;
use serde::Serialize;
use ursula_runtime::AppendRequest;
use ursula_runtime::BootstrapStreamRequest;
use ursula_runtime::BootstrapStreamResponse;
use ursula_runtime::ColdFlushCandidate;
use ursula_runtime::ColdGcPlanEntry;
use ursula_runtime::ColdHotBacklog;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::ErrorChain;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::GroupInfraError;
use ursula_runtime::GroupSnapshot;
use ursula_runtime::GroupSnapshotError;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::HeadStreamResponse;
use ursula_runtime::InMemoryGroupEngine;
use ursula_runtime::PlanColdFlushRequest;
use ursula_runtime::PlanGroupColdFlushRequest;
use ursula_runtime::ReadSnapshotRequest;
use ursula_runtime::ReadSnapshotResponse;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::ReadStreamResponse;
use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::SnapshotKey;
use ursula_runtime::SnapshotLocation;
use ursula_runtime::SnapshotObjectCheck;
use ursula_runtime::SnapshotPointer;
use ursula_runtime::SnapshotStoreError;
use ursula_runtime::SnapshotStoreOperation;
use ursula_runtime::decode_snapshot_envelope;
use ursula_runtime::default_snapshot_store;
use ursula_runtime::encode_binary_envelope;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;

use crate::engine::group_engine_io_error;
use crate::engine::invalid_data;
use crate::log_store::disk::Disk;
use crate::log_store::disk::JournalDisk;
use crate::log_store::disk::JournalFile;
use crate::log_store::disk::create_dir_all_durable;
use crate::log_store::elapsed_ns;
use crate::log_store::replace_file;
use crate::registry::SnapshotInstallError;
use crate::rt::sync::OwnedSemaphorePermit;
use crate::rt::sync::Semaphore;
use crate::rt::sync::watch;
use crate::rt::time::Instant;
use crate::snapshot_cadence::GroupLogGauge;
use crate::snapshot_cadence::GroupLogMark;
use crate::snapshot_cadence::GroupLogProgress;
use crate::snapshot_codec::decode_group_snapshot;
use crate::snapshot_codec::group_snapshot_frames;
use crate::snapshot_references::SnapshotReferenceLease;
use crate::snapshot_references::SnapshotReferences;
use crate::types::RaftGroupResponse;
use crate::types::SnapshotData;
use crate::types::UrsulaRaftTypeConfig;

#[derive(Debug, Clone)]
pub struct SnapshotBuildCoordinator {
    inner: Arc<SnapshotBuildCoordinatorInner>,
}

#[derive(Debug)]
struct SnapshotBuildCoordinatorInner {
    semaphore: Arc<Semaphore>,
    /// The semaphore's permits: the builds the node runs at once at most.
    max_concurrency: usize,
    /// Per-group log gauges (F12e), shared node-wide like the permit, so
    /// the snapshot driver reads what every group's state machine counts.
    log_gauges: Arc<Mutex<BTreeMap<u32, Arc<GroupLogGauge>>>>,
    /// Build permits handed to a group about to be triggered, one per group
    /// at a time; that group's next build uses it instead of competing for
    /// one.
    handoffs: Mutex<BTreeMap<u32, HandedOff>>,
    /// Numbers the hand-offs in `handoffs`.
    next_handoff: AtomicU64,
    /// The builds that hold a permit, by build number. The snapshot driver
    /// reads them to skip groups that already build and to wait for a free
    /// slot (#519). A hand-off taken back marks it changed too, for the
    /// waits on hand-offs.
    builds: watch::Sender<BTreeMap<u64, BuildInFlight>>,
    /// Numbers the builds in `builds`.
    next_build: AtomicU64,
    /// Set while the node's unsnapshotted Raft log is over its hard limit;
    /// client writes are refused with 503 until snapshots bring it back.
    log_pressure: Arc<AtomicBool>,
    /// The snapshot each group's latest build made, recorded once it is
    /// durable and before the build releases its permit. OpenRaft publishes
    /// that snapshot only after the permit is back, when RaftCore handled
    /// the build's end.
    built: Mutex<BTreeMap<u32, BuiltSnapshot>>,
    /// Purges through a covering snapshot the snapshot driver started
    /// (#669), each holding its group's builds until it lands. Shared like
    /// the log gauges. Locked after `handoffs` when both are.
    purges: Arc<Mutex<BTreeMap<u32, PurgeHold>>>,
    /// Numbers the holds in `purges`.
    next_purge: AtomicU64,
}

/// A group's Raft metrics, which show when a purge landed.
pub type RaftMetricsWatch =
    WatchReceiverOf<UrsulaRaftTypeConfig, RaftMetrics<UrsulaRaftTypeConfig>>;

/// What a group's snapshot work does now (#669): the one state every path
/// that starts a snapshot build or a purge through a snapshot checks, the
/// snapshot driver's builds and purges, an operator's snapshot and a build
/// OpenRaft starts on its own ([`SnapshotBuildCoordinator::work`]).
///
/// OpenRaft asserts that the log index it schedules a purge to never goes
/// back. A build schedules a purge to `max_in_snapshot_log_to_keep` entries
/// below its snapshot, judged against the last purged entry only, so a build
/// that ends while an explicit purge to a higher index is still pending (a
/// leader defers one while replication reads the range) would schedule a
/// lower one and stop the group's Raft core. So a purge starts only from
/// [`GroupSnapshotWork::Idle`], and no build starts while one is
/// [`GroupSnapshotWork::Purging`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupSnapshotWork {
    /// Nothing runs: a build or a purge may start.
    Idle,
    /// A build permit handed to the group waits for its build.
    HandedOff,
    /// A build of the group holds a permit.
    Building,
    /// The group's latest build made a snapshot OpenRaft has not published
    /// yet. OpenRaft schedules the purge that build calls for when it does.
    Unpublished,
    /// A purge through `through` has not landed: the log is not purged
    /// through it yet, and the group has not applied as many entries past
    /// it as its OpenRaft keeps below a snapshot
    /// (`max_in_snapshot_log_to_keep`), after which any build's own purge
    /// reaches it.
    Purging {
        /// The log index the purge goes through.
        through: u64,
    },
}

/// Why [`SnapshotBuildCoordinator::hold_for_purge`] did not hold a group
/// for a purge: its snapshot work is not idle. The purge can start once it
/// is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("raft group {raft_group_id}'s snapshot work is {work:?}; a purge waits until it is idle")]
pub struct PurgeBusy {
    pub raft_group_id: u32,
    /// The work that stands in the way.
    pub work: GroupSnapshotWork,
}

/// A hold of a group's builds for a purge
/// ([`SnapshotBuildCoordinator::hold_for_purge`]). Dropped before
/// [`PurgeHoldGuard::keep`], it lifts the hold, so a caller that stops before
/// it triggered the purge never leaves the group held.
#[derive(Debug)]
#[must_use = "dropping the guard lifts the hold"]
pub struct PurgeHoldGuard {
    coordinator: SnapshotBuildCoordinator,
    raft_group_id: u32,
    /// The hold, `None` once kept or when no hold was taken.
    id: Option<u64>,
}

impl PurgeHoldGuard {
    /// The purge was triggered: the hold stays until it lands.
    pub fn keep(mut self) {
        self.id = None;
    }
}

impl Drop for PurgeHoldGuard {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.coordinator.release_hold(self.raft_group_id, id);
        }
    }
}

/// A purge through a covering snapshot that holds its group's builds until
/// it lands ([`GroupSnapshotWork::Purging`]).
struct PurgeHold {
    /// Numbers the holds, so that a wait or a release names its own.
    id: u64,
    through: u64,
    /// Entries the group's OpenRaft keeps below a snapshot it built
    /// (`max_in_snapshot_log_to_keep`), `None` when its engine has not
    /// said: the hold then lasts until the log is purged.
    keep: Option<u64>,
    metrics: RaftMetricsWatch,
}

impl std::fmt::Debug for PurgeHold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PurgeHold")
            .field("id", &self.id)
            .field("through", &self.through)
            .field("keep", &self.keep)
            .finish_non_exhaustive()
    }
}

impl PurgeHold {
    /// Whether the purge landed: the log is purged through it, the group
    /// applied `keep` entries past it, so that the purge any later build
    /// schedules reaches it, or its Raft stopped.
    fn landed(&self) -> bool {
        let metrics = self.metrics.borrow_watched();
        metrics
            .purged
            .is_some_and(|purged| purged.index >= self.through)
            || self.keep.is_some_and(|keep| {
                metrics
                    .last_applied
                    .is_some_and(|applied| applied.index >= self.through.saturating_add(keep))
            })
            || metrics.running_state.is_err()
            || metrics.state == openraft::ServerState::Shutdown
    }
}

/// Why a build OpenRaft triggered defers
/// ([`SnapshotBuildCoordinator::lease_for_build`]). OpenRaft retries on a
/// later trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuildDeferred {
    /// A purge through a snapshot holds the group until it lands (#669).
    PurgePending { through: u64 },
    /// Other groups hold every build permit.
    NoPermit,
}

/// The snapshot a group's latest build made
/// ([`SnapshotBuildCoordinatorInner::built`]).
#[derive(Debug, Clone, Copy)]
struct BuiltSnapshot {
    /// The log the snapshot covers.
    log_id: LogIdOf<UrsulaRaftTypeConfig>,
    /// When the build that recorded it released its permit, or `None` while
    /// it holds it.
    released: Option<crate::rt::time::Instant>,
}

/// Longest a handed-off permit waits for the triggered group's build to claim
/// it ([`SnapshotBuildCoordinator::hand_off_and_trigger`]).
pub const SNAPSHOT_HANDOFF_WAIT: Duration = Duration::from_secs(5);

/// Why the node's snapshot builds gave no permit
/// ([`SnapshotBuildCoordinator::acquire`],
/// [`SnapshotBuildCoordinator::acquire_below`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotPermitError {
    /// The node's build permits are closed.
    #[error("the snapshot build permits are closed")]
    PermitsClosed,
    /// The registry of the builds in flight is closed, so a wait for a
    /// build slot would never end.
    #[error("the snapshot build registry is closed")]
    RegistryClosed,
}

/// Why a snapshot build of a group did not start
/// ([`SnapshotBuildCoordinator::start_snapshot`]).
#[derive(Debug, thiserror::Error)]
pub enum SnapshotStartError<E: std::error::Error + 'static> {
    /// No build of the group started within the wait: other builds held
    /// every permit, a build of the group ran, or the group's build did not
    /// claim its permit in time. Builds end, so a later request may start
    /// one.
    #[error("no snapshot build of group {raft_group_id} started within {wait:?}")]
    NotStarted { raft_group_id: u32, wait: Duration },
    /// The node's build permits are closed.
    #[error("take a snapshot build permit")]
    Permit(#[source] SnapshotPermitError),
    /// The group's snapshot trigger failed.
    #[error("trigger the snapshot of group {raft_group_id}")]
    Trigger {
        raft_group_id: u32,
        #[source]
        source: E,
    },
}

/// What became of a build permit handed to a group with its snapshot trigger
/// ([`SnapshotBuildCoordinator::hand_off_and_trigger`]).
#[derive(Debug)]
pub enum Handoff<E> {
    /// The group's build claimed the permit, so it runs.
    Claimed,
    /// The group's build did not claim the permit in time, and it went back
    /// to the node's permits. OpenRaft drops a trigger while it counts a
    /// build of the group as running, which it does until RaftCore handles
    /// the build's end, and that can come after the build released its
    /// permit. A build that waits behind its group's applies claims too
    /// late as well.
    Unclaimed,
    /// The trigger failed, and the permit went back to the node's permits.
    TriggerFailed(E),
    /// A permit handed to the group earlier is still pending, or a build of
    /// the group holds a permit, so this one went back to the node's permits
    /// and nothing was triggered: the group's next build claims the earlier
    /// permit, and OpenRaft would drop a trigger while the group builds.
    Busy,
}

/// A build permit handed to a group, with its hand-off's number.
#[derive(Debug)]
struct HandedOff {
    token: u64,
    permit: OwnedSemaphorePermit,
}

/// A hand-off its caller still waits on. Dropped before a build claimed the
/// permit, it takes the permit back, so a caller that stops waiting (its
/// future dropped, or its trigger panicking) never leaves the permit parked
/// under the group.
struct PendingHandoff<'a> {
    coordinator: &'a SnapshotBuildCoordinator,
    raft_group_id: u32,
    token: u64,
}

impl PendingHandoff<'_> {
    /// Takes the permit back unless a build claimed it; returns whether it
    /// did.
    fn reclaim(&self) -> bool {
        self.coordinator
            .reclaim_handoff(self.raft_group_id, self.token)
    }
}

impl Drop for PendingHandoff<'_> {
    fn drop(&mut self) {
        self.reclaim();
    }
}

/// A subscription to the node's snapshot builds
/// ([`SnapshotBuildCoordinator::subscribe_builds`]).
#[derive(Debug)]
pub struct BuildsChanged(watch::Receiver<BTreeMap<u64, BuildInFlight>>);

impl BuildsChanged {
    /// Waits until a build started or ended, or a hand-off was taken back,
    /// since the subscription was taken.
    pub async fn changed(mut self) {
        // A closed registry never changes again.
        if self.0.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// One snapshot build that holds a permit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BuildInFlight {
    raft_group_id: u32,
    /// Applied log bytes the build covers ([`GroupLogMark::bytes`]).
    log_mark: u64,
}

/// One snapshot build's hold on a node-wide build permit. While it lives
/// the build counts in [`SnapshotBuildCoordinator::builds_in_flight`].
#[derive(Debug)]
pub(crate) struct SnapshotBuildLease {
    _permit: OwnedSemaphorePermit,
    build: u64,
    raft_group_id: u32,
    coordinator: SnapshotBuildCoordinator,
    /// Whether the build recorded its snapshot
    /// ([`SnapshotBuildLease::record_built`]).
    recorded: bool,
}

impl SnapshotBuildLease {
    /// Records the log the build made its durable snapshot to
    /// ([`SnapshotBuildCoordinator::built_snapshot`]). A later record of an
    /// older snapshot does not move it back.
    fn record_built(&mut self, log_id: Option<LogIdOf<UrsulaRaftTypeConfig>>) {
        let Some(log_id) = log_id else {
            return;
        };
        let mut built = self.coordinator.built();
        let latest = built.entry(self.raft_group_id).or_insert(BuiltSnapshot {
            log_id,
            released: None,
        });
        if latest.log_id <= log_id {
            *latest = BuiltSnapshot {
                log_id,
                released: None,
            };
            self.recorded = true;
        }
    }
}

impl Drop for SnapshotBuildLease {
    fn drop(&mut self) {
        if self.recorded
            && let Some(built) = self.coordinator.built().get_mut(&self.raft_group_id)
            && built.released.is_none()
        {
            built.released = Some(crate::rt::time::Instant::now());
        }
        let build = self.build;
        self.coordinator
            .inner
            .builds
            .send_if_modified(|builds| builds.remove(&build).is_some());
    }
}

impl Default for SnapshotBuildCoordinator {
    fn default() -> Self {
        Self::new(1)
    }
}

impl SnapshotBuildCoordinator {
    pub fn new(max_concurrency: usize) -> Self {
        Self::with_shared(
            max_concurrency,
            Arc::default(),
            Arc::default(),
            Arc::default(),
        )
    }

    /// A coordinator with a new build concurrency that keeps this one's log
    /// gauges.
    pub fn with_max_concurrency(&self, max_concurrency: usize) -> Self {
        Self::with_shared(
            max_concurrency,
            Arc::clone(&self.inner.log_gauges),
            Arc::clone(&self.inner.log_pressure),
            Arc::clone(&self.inner.purges),
        )
    }

    fn with_shared(
        max_concurrency: usize,
        log_gauges: Arc<Mutex<BTreeMap<u32, Arc<GroupLogGauge>>>>,
        log_pressure: Arc<AtomicBool>,
        purges: Arc<Mutex<BTreeMap<u32, PurgeHold>>>,
    ) -> Self {
        let max_concurrency = max_concurrency.max(1);
        Self {
            inner: Arc::new(SnapshotBuildCoordinatorInner {
                semaphore: Arc::new(Semaphore::new(max_concurrency)),
                max_concurrency,
                log_gauges,
                handoffs: Mutex::default(),
                next_handoff: AtomicU64::new(0),
                builds: watch::Sender::new(BTreeMap::new()),
                next_build: AtomicU64::new(0),
                log_pressure,
                built: Mutex::default(),
                purges,
                next_purge: AtomicU64::new(0),
            }),
        }
    }

    /// The builds the node runs at once at most.
    pub fn max_concurrency(&self) -> usize {
        self.inner.max_concurrency
    }

    /// The log gauge of `raft_group_id`, created on first use (F12e).
    pub fn log_gauge(&self, raft_group_id: u32) -> Arc<GroupLogGauge> {
        let mut gauges = self.log_gauges();
        Arc::clone(gauges.entry(raft_group_id).or_default())
    }

    /// Actual log progress, including retained stopped-group history.
    pub fn log_progress(&self) -> BTreeMap<u32, GroupLogProgress> {
        self.log_gauges()
            .iter()
            .map(|(group, gauge)| (*group, gauge.progress()))
            .collect()
    }

    /// The log progress of the groups that can still snapshot: what the
    /// snapshot driver's monitor weighs for the deleted data the node still
    /// holds (#669). A stopped group keeps its whole log for replay.
    pub fn running_log_progress(&self) -> Vec<GroupLogProgress> {
        self.log_gauges()
            .values()
            .filter(|gauge| !gauge.apply_stopped())
            .map(|gauge| gauge.progress())
            .collect()
    }

    /// The unsnapshotted log the groups that can still snapshot hold: what
    /// the hard limit and the snapshot driver's pressure weigh. Stopped
    /// groups cannot snapshot, so their log must not hold the write gate
    /// closed for healthy groups for good.
    pub fn node_log_bytes(&self) -> u64 {
        self.log_gauges()
            .values()
            .filter(|gauge| !gauge.apply_stopped())
            .map(|gauge| gauge.progress().log_bytes)
            .fold(0, u64::saturating_add)
    }

    /// The groups whose snapshot build holds a permit, each with the log
    /// bytes its build will still free: none once the build recorded its
    /// snapshot, while it still publishes the snapshot's reference.
    pub fn builds_in_flight(&self) -> BTreeMap<u32, u64> {
        let gauges = self.log_gauges();
        let mut groups = BTreeMap::<u32, u64>::new();
        for build in self.inner.builds.borrow().values() {
            let freed = gauges
                .get(&build.raft_group_id)
                .map_or(0, |gauge| gauge.snapshot_mark_bytes());
            let pending = groups.entry(build.raft_group_id).or_default();
            *pending = pending.saturating_add(build.log_mark.saturating_sub(freed));
        }
        groups
    }

    /// Snapshot builds that hold a permit now.
    pub fn running_builds(&self) -> usize {
        self.inner.builds.borrow().len()
    }

    pub async fn acquire(&self) -> Result<OwnedSemaphorePermit, SnapshotPermitError> {
        self.inner
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_closed| SnapshotPermitError::PermitsClosed)
    }

    /// Waits until fewer builds hold a permit than `limit` asks for, then
    /// takes one. `limit` is asked again whenever a build starts or ends and
    /// at least every `poll`, since what the snapshot driver weighs (its log
    /// pressure, #519) moves with the builds and between them. `None` ends
    /// the wait without a permit. A build that starts in between can take
    /// the node past the limit for a while: the driver paces its own builds
    /// with it, and the permits bound every build.
    pub async fn acquire_below(
        &self,
        poll: Duration,
        limit: impl FnMut() -> Option<usize>,
    ) -> Result<Option<OwnedSemaphorePermit>, SnapshotPermitError> {
        self.acquire_below_watching(&mut self.subscribe_builds(), poll, limit)
            .await
    }

    /// [`Self::acquire_below`] on the caller's subscription to the builds.
    /// When the caller stops waiting, the subscription is marked seen at
    /// the wait's last check, so it ends a later wait only on a build that
    /// starts or ends after that check.
    pub async fn acquire_below_watching(
        &self,
        watching: &mut BuildsChanged,
        poll: Duration,
        mut limit: impl FnMut() -> Option<usize>,
    ) -> Result<Option<OwnedSemaphorePermit>, SnapshotPermitError> {
        let builds = &mut watching.0;
        loop {
            let Some(slots) = limit() else {
                return Ok(None);
            };
            // Marked seen before the comparison, so a build that starts or
            // ends after it wakes the wait below.
            let running = builds.borrow_and_update().len();
            if running < slots.max(1) {
                return self.acquire().await.map(Some);
            }
            // Biased, so the branch polled first is fixed rather than drawn
            // from tokio's thread RNG, which a process-wide counter seeds
            // (#638).
            crate::rt::select! {
                biased;
                changed = builds.changed() => {
                    changed.map_err(|_closed| SnapshotPermitError::RegistryClosed)?;
                }
                () = crate::rt::time::sleep(poll) => {}
            }
        }
    }

    /// Hands `permit` to `raft_group_id`, runs `trigger` (the group's
    /// snapshot trigger) and waits up to `wait` for the group's next build
    /// to claim the permit. The permit goes back to the node's permits
    /// unless the build claimed it, also when the caller stops waiting or
    /// the trigger panics. While a permit handed to the group earlier is
    /// pending or a build of the group runs, this one goes back at once and
    /// nothing is triggered ([`Handoff::Busy`]).
    ///
    /// A triggered build otherwise only *tries* for a permit and is refused
    /// while other groups hold every one, so a driver firing many groups at
    /// once built about one per tick and the Raft log outgrew memory under
    /// sustained writes (#519). A caller that needs the build to run takes a
    /// permit first (waiting until fewer builds run than it allows, as
    /// [`SnapshotBuildCoordinator::acquire_below`] does) and hands it over.
    pub async fn hand_off_and_trigger<E>(
        &self,
        raft_group_id: u32,
        permit: OwnedSemaphorePermit,
        wait: Duration,
        trigger: impl Future<Output = Result<(), E>>,
    ) -> Handoff<E> {
        let Ok(token) = self.hand_off(raft_group_id, permit) else {
            return Handoff::Busy;
        };
        let pending = PendingHandoff {
            coordinator: self,
            raft_group_id,
            token,
        };
        if let Err(err) = trigger.await {
            pending.reclaim();
            return Handoff::TriggerFailed(err);
        }
        let _claimed_or_late =
            crate::rt::time::timeout(wait, self.handoff_claimed(raft_group_id, token)).await;
        if pending.reclaim() {
            Handoff::Unclaimed
        } else {
            Handoff::Claimed
        }
    }

    /// Waits until hand-off `token` to `raft_group_id` is no longer
    /// pending: a build claimed it, or it was taken back. A claim registers
    /// its build, which wakes the wait.
    async fn handoff_claimed(&self, raft_group_id: u32, token: u64) {
        // Subscribed before the check, so a claim right after it still
        // wakes the wait.
        let mut builds = self.inner.builds.subscribe();
        while self.handoff_pending(raft_group_id, token) {
            if builds.changed().await.is_err() {
                return;
            }
        }
    }

    /// Starts a snapshot build of `raft_group_id` that covers `wanted`, the
    /// log the caller asks a snapshot for, within `wait`: an operator's
    /// snapshot (#529). With `wanted` `None` (nothing applied) a build is
    /// always triggered.
    ///
    /// The request is covered once a snapshot OpenRaft published covers it,
    /// which `published` says and `covering` ends on, or once the group's
    /// latest build does. OpenRaft publishes the snapshot a build made only
    /// after the build released its permit, once RaftCore handled the
    /// build's end, so the start also weighs the snapshot the build recorded
    /// before it released its permit: a start that waited for the group's
    /// build then triggers no other build when that one covers the request
    /// (#561). The start also ends as soon as `covering` does, whatever
    /// round it is in, and a permit it handed over that no build claimed yet
    /// comes back.
    ///
    /// Each round first waits until no hand-off to the group is pending, no
    /// build of it runs and no purge through a snapshot holds it
    /// ([`GroupSnapshotWork`]), since OpenRaft drops a trigger while the
    /// group builds, a permit handed to it would sit idle, and a build that
    /// ends under a pending purge stops the group (#669). The round then ends
    /// the start when the request is covered (a build that began after the
    /// request covers it), or takes a permit. The wait for the permit can be
    /// the long one, so the round asks again whether the request is covered,
    /// and hands the permit over and triggers the group with `trigger`
    /// unless a build of the group started or another caller handed it a
    /// permit meanwhile ([`Handoff::Busy`]). A trigger that went unclaimed
    /// or a busy group starts the next round. A start whose wait ran out
    /// still ends when the request is covered by then.
    pub async fn start_snapshot<F, E>(
        &self,
        raft_group_id: u32,
        wanted: Option<LogIdOf<UrsulaRaftTypeConfig>>,
        wait: Duration,
        mut published: impl FnMut() -> bool,
        covering: impl Future<Output = ()>,
        mut trigger: impl FnMut() -> F,
    ) -> Result<(), SnapshotStartError<E>>
    where
        F: Future<Output = Result<(), E>>,
        E: std::error::Error + 'static,
    {
        let mut covered = || published() || self.build_covers(raft_group_id, wanted.as_ref());
        let rounds = async {
            loop {
                self.handoff_settled(raft_group_id).await;
                self.build_ended(raft_group_id).await;
                self.purge_landed(raft_group_id).await;
                if covered() {
                    return Ok(());
                }
                let permit = self.acquire().await.map_err(SnapshotStartError::Permit)?;
                if covered() {
                    return Ok(());
                }
                match self
                    .hand_off_and_trigger(raft_group_id, permit, SNAPSHOT_HANDOFF_WAIT, trigger())
                    .await
                {
                    Handoff::Claimed => return Ok(()),
                    Handoff::Unclaimed | Handoff::Busy => {}
                    Handoff::TriggerFailed(source) => {
                        return Err(SnapshotStartError::Trigger {
                            raft_group_id,
                            source,
                        });
                    }
                }
            }
        };
        let started = async {
            crate::rt::select! {
                biased;
                () = covering => Ok(()),
                started = rounds => started,
            }
        };
        let started = crate::rt::time::timeout(wait, started).await;
        match started {
            Ok(started) => started,
            Err(_elapsed) if covered() => Ok(()),
            Err(_elapsed) => Err(SnapshotStartError::NotStarted {
                raft_group_id,
                wait,
            }),
        }
    }

    /// Whether a build of `raft_group_id` holds a permit now.
    fn building(&self, raft_group_id: u32) -> bool {
        self.inner
            .builds
            .borrow()
            .values()
            .any(|build| build.raft_group_id == raft_group_id)
    }

    /// Whether a permit handed to `raft_group_id` is pending, a build of it
    /// holds a permit or a purge through a snapshot holds it, so a hand-off
    /// to it now would answer [`Handoff::Busy`]. A caller that would wait
    /// for a permit before it hands one over asks this first, since a build
    /// of the group that holds the permit it waits for ends before the wait
    /// does.
    pub fn busy(&self, raft_group_id: u32) -> bool {
        self.busy_with(&self.handoffs(), raft_group_id)
    }

    /// [`SnapshotBuildCoordinator::busy`] under the hand-off lock.
    fn busy_with(&self, handoffs: &BTreeMap<u32, HandedOff>, raft_group_id: u32) -> bool {
        !matches!(
            self.work_with(handoffs, raft_group_id, None),
            GroupSnapshotWork::Idle | GroupSnapshotWork::Unpublished
        )
    }

    /// What `raft_group_id`'s snapshot work does now ([`GroupSnapshotWork`]),
    /// `published_index` being the last log index of the snapshot OpenRaft's
    /// metrics show for it.
    pub fn work(&self, raft_group_id: u32, published_index: Option<u64>) -> GroupSnapshotWork {
        self.work_with(&self.handoffs(), raft_group_id, published_index)
    }

    /// [`SnapshotBuildCoordinator::work`] under the hand-off lock, which a
    /// build holds while it claims a hand-off or takes a free permit and
    /// registers itself, and which a purge holds while it registers its
    /// hold, so neither slips in between.
    fn work_with(
        &self,
        handoffs: &BTreeMap<u32, HandedOff>,
        raft_group_id: u32,
        published_index: Option<u64>,
    ) -> GroupSnapshotWork {
        if handoffs.contains_key(&raft_group_id) {
            GroupSnapshotWork::HandedOff
        } else if self.building(raft_group_id) {
            GroupSnapshotWork::Building
        } else if let Some(through) = self.purging(raft_group_id) {
            GroupSnapshotWork::Purging { through }
        } else if self.unpublished(raft_group_id, published_index) {
            GroupSnapshotWork::Unpublished
        } else {
            GroupSnapshotWork::Idle
        }
    }

    /// Whether the latest build of `raft_group_id` made a snapshot past
    /// `published_index`, the last log index of the one OpenRaft's metrics
    /// show. Unlike [`Self::publishing`] it is not bounded in time: a purge
    /// must never start before OpenRaft handled that build's end.
    fn unpublished(&self, raft_group_id: u32, published_index: Option<u64>) -> bool {
        self.built()
            .get(&raft_group_id)
            .is_some_and(|built| Some(built.log_id.index) > published_index)
    }

    /// The index a purge that has not landed goes through, forgetting a
    /// hold whose purge landed.
    fn purging(&self, raft_group_id: u32) -> Option<u64> {
        self.current_hold(raft_group_id)
            .map(|(through, _id)| through)
    }

    /// The index the hold of `raft_group_id` that has not landed goes
    /// through, and its number, forgetting a hold whose purge landed.
    fn current_hold(&self, raft_group_id: u32) -> Option<(u64, u64)> {
        let mut purges = self.purges();
        let hold = purges.get(&raft_group_id)?;
        if hold.landed() {
            purges.remove(&raft_group_id);
            return None;
        }
        Some((hold.through, hold.id))
    }

    /// Forgets the hold `id` of `raft_group_id`, leaving any other.
    fn release_hold(&self, raft_group_id: u32, id: u64) {
        let mut purges = self.purges();
        if purges.get(&raft_group_id).is_some_and(|hold| hold.id == id) {
            purges.remove(&raft_group_id);
        }
    }

    /// Holds `raft_group_id`'s builds for a purge of its log through
    /// `through` (#669), the caller then triggering the purge: only when
    /// its work is [`GroupSnapshotWork::Idle`], judged with
    /// `published_index`, the last log index of the snapshot OpenRaft's
    /// metrics show. The hold lasts until the group's `metrics` show the
    /// purge landed: the log purged through `through`, or as many entries
    /// applied past it as the group's OpenRaft keeps below a snapshot.
    pub fn hold_for_purge(
        &self,
        raft_group_id: u32,
        published_index: Option<u64>,
        through: u64,
        metrics: RaftMetricsWatch,
    ) -> Result<PurgeHoldGuard, PurgeBusy> {
        if ursula_stream::sim_controls::off(ursula_stream::sim_controls::Defence::ScrubPurgeHold) {
            return Ok(PurgeHoldGuard {
                coordinator: self.clone(),
                raft_group_id,
                id: None,
            });
        }
        let keep = self.log_gauge(raft_group_id).log_keep();
        let handoffs = self.handoffs();
        match self.work_with(&handoffs, raft_group_id, published_index) {
            GroupSnapshotWork::Idle => {}
            work => {
                ursula_stream::sim_controls::note_fired(
                    ursula_stream::sim_controls::Defence::ScrubPurgeHold,
                );
                return Err(PurgeBusy {
                    raft_group_id,
                    work,
                });
            }
        }
        let id = self.inner.next_purge.fetch_add(1, Ordering::Relaxed);
        self.purges().insert(raft_group_id, PurgeHold {
            id,
            through,
            keep,
            metrics,
        });
        drop(handoffs);
        Ok(PurgeHoldGuard {
            coordinator: self.clone(),
            raft_group_id,
            id: Some(id),
        })
    }

    /// Lifts the hold of `raft_group_id`'s builds for a purge: its trigger
    /// failed, or a new incarnation of the group's Raft starts, whose
    /// OpenRaft has no purge pending.
    pub fn release_purge(&self, raft_group_id: u32) {
        self.purges().remove(&raft_group_id);
    }

    /// Adds to `building` every group a purge through a snapshot holds
    /// ([`GroupSnapshotWork::Purging`]), as building with nothing left to
    /// free, so the snapshot driver leaves it out of its pass.
    pub fn add_purging(&self, building: &mut BTreeMap<u32, u64>) {
        let groups = self.purges().keys().copied().collect::<Vec<_>>();
        for raft_group_id in groups {
            if self.purging(raft_group_id).is_some() {
                building.entry(raft_group_id).or_insert(0);
            }
        }
    }

    /// Waits until no purge through a snapshot holds `raft_group_id`'s
    /// builds: each hold in turn, on the metrics of the Raft it is for.
    pub async fn purge_landed(&self, raft_group_id: u32) {
        loop {
            let Some((id, mut metrics)) = self
                .purges()
                .get(&raft_group_id)
                .map(|hold| (hold.id, hold.metrics.clone()))
            else {
                return;
            };
            while self
                .current_hold(raft_group_id)
                .is_some_and(|(_through, current)| current == id)
            {
                if metrics.changed().await.is_err() {
                    // That Raft stopped: no purge of it lands.
                    self.release_hold(raft_group_id, id);
                }
            }
        }
    }

    /// A subscription that ends once a build starts or ends, or a hand-off
    /// is taken back, after this call. Take it when the wait is decided, so
    /// that a change before the wait begins still ends it.
    pub fn subscribe_builds(&self) -> BuildsChanged {
        BuildsChanged(self.inner.builds.subscribe())
    }

    /// Waits until no build of `raft_group_id` holds a permit.
    pub async fn build_ended(&self, raft_group_id: u32) {
        // Subscribed before the check, so a build that ends right after it
        // still wakes the wait.
        let mut builds = self.inner.builds.subscribe();
        while builds
            .borrow_and_update()
            .values()
            .any(|build| build.raft_group_id == raft_group_id)
        {
            if builds.changed().await.is_err() {
                return;
            }
        }
    }

    /// Waits until no permit handed to `raft_group_id` is pending: a build
    /// claimed it, or its caller took it back ([`Handoff::Busy`]).
    pub(crate) async fn handoff_settled(&self, raft_group_id: u32) {
        // Subscribed before the check, so a claim or a take-back right after
        // it still wakes the wait.
        let mut builds = self.inner.builds.subscribe();
        while self.handoffs().contains_key(&raft_group_id) {
            if builds.changed().await.is_err() {
                return;
            }
        }
    }

    /// Reserves `permit` for the next snapshot build of `raft_group_id`
    /// ([`SnapshotBuildCoordinator::hand_off_and_trigger`]) and returns the
    /// hand-off's number, unless a permit handed to the group is still
    /// pending or a build of the group holds a permit: `permit` then comes
    /// back. Private, so that only a [`PendingHandoff`] parks a permit.
    fn hand_off(
        &self,
        raft_group_id: u32,
        permit: OwnedSemaphorePermit,
    ) -> Result<u64, OwnedSemaphorePermit> {
        let mut handoffs = self.handoffs();
        // Checked under the hand-off lock, which a build holds while it
        // claims a hand-off or takes a free permit and registers itself
        // ([`SnapshotBuildCoordinator::lease_for_build`]), so no build can
        // slip in between.
        if self.busy_with(&handoffs, raft_group_id) {
            return Err(permit);
        }
        let token = self.inner.next_handoff.fetch_add(1, Ordering::Relaxed);
        handoffs.insert(raft_group_id, HandedOff { token, permit });
        Ok(token)
    }

    /// Whether hand-off `token` to `raft_group_id` is still unclaimed.
    fn handoff_pending(&self, raft_group_id: u32, token: u64) -> bool {
        self.handoffs()
            .get(&raft_group_id)
            .is_some_and(|handed| handed.token == token)
    }

    /// Takes back hand-off `token` to `raft_group_id` unless a build claimed
    /// it; returns whether it did. Another hand-off to the group stays.
    fn reclaim_handoff(&self, raft_group_id: u32, token: u64) -> bool {
        let mut handoffs = self.handoffs();
        let ours = handoffs
            .get(&raft_group_id)
            .is_some_and(|handed| handed.token == token);
        let taken = if ours {
            handoffs.remove(&raft_group_id)
        } else {
            None
        };
        drop(handoffs);
        let reclaimed = taken.is_some();
        // The permit goes back to the node's permits, outside the lock and
        // before the waits on hand-offs wake.
        drop(taken);
        if reclaimed {
            self.inner.builds.send_modify(|_builds| {});
        }
        reclaimed
    }

    /// Claims the permit handed off to `raft_group_id` for a build covering
    /// `log_mark`. The build counts as in flight before the handoff is gone,
    /// so the driver never sees neither.
    fn claim_handoff(&self, raft_group_id: u32, log_mark: u64) -> Option<SnapshotBuildLease> {
        let mut handoffs = self.handoffs();
        let handed = handoffs.remove(&raft_group_id)?;
        let lease = self.lease(handed.permit, raft_group_id, log_mark);
        drop(handoffs);
        Some(lease)
    }

    /// The permit for a triggered build of `raft_group_id` covering
    /// `log_mark`: the one handed to the group, or else one free right now,
    /// so that one group's apply worker never waits behind another group's
    /// snapshot build (bounded-stream-state F12d). Both under one hand-off
    /// lock, so no hand-off is parked on the group between the two
    /// ([`SnapshotBuildCoordinator::hand_off`]). No build starts while a
    /// purge through a snapshot holds the group (#669).
    fn lease_for_build(
        &self,
        raft_group_id: u32,
        log_mark: u64,
    ) -> Result<SnapshotBuildLease, BuildDeferred> {
        let mut handoffs = self.handoffs();
        if let Some(through) = self.purging(raft_group_id) {
            ursula_stream::sim_controls::note_fired(
                ursula_stream::sim_controls::Defence::ScrubPurgeHold,
            );
            return Err(BuildDeferred::PurgePending { through });
        }
        let permit = match handoffs.remove(&raft_group_id) {
            Some(handed) => handed.permit,
            None => self
                .inner
                .semaphore
                .clone()
                .try_acquire_owned()
                .map_err(|_none_free| BuildDeferred::NoPermit)?,
        };
        let lease = self.lease(permit, raft_group_id, log_mark);
        drop(handoffs);
        Ok(lease)
    }

    /// Takes a build permit only if one is free right now: a build that
    /// holds one in a test.
    #[cfg(test)]
    fn try_lease(&self, raft_group_id: u32, log_mark: u64) -> Option<SnapshotBuildLease> {
        let permit = self.inner.semaphore.clone().try_acquire_owned().ok()?;
        Some(self.lease(permit, raft_group_id, log_mark))
    }

    /// Waits for a build permit.
    async fn wait_lease(
        &self,
        raft_group_id: u32,
        log_mark: u64,
    ) -> Result<SnapshotBuildLease, SnapshotPermitError> {
        let permit = self.acquire().await?;
        Ok(self.lease(permit, raft_group_id, log_mark))
    }

    fn lease(
        &self,
        permit: OwnedSemaphorePermit,
        raft_group_id: u32,
        log_mark: u64,
    ) -> SnapshotBuildLease {
        let build = self.inner.next_build.fetch_add(1, Ordering::Relaxed);
        self.inner.builds.send_modify(|builds| {
            builds.insert(build, BuildInFlight {
                raft_group_id,
                log_mark,
            });
        });
        SnapshotBuildLease {
            _permit: permit,
            build,
            raft_group_id,
            coordinator: self.clone(),
            recorded: false,
        }
    }

    /// The log the latest snapshot build of `raft_group_id` made its
    /// durable snapshot to. It is recorded before the build releases its
    /// permit, so once [`SnapshotBuildCoordinator::running_builds`] no
    /// longer counts the build this shows its snapshot, which OpenRaft's
    /// metrics show only once RaftCore handled the build's end.
    fn built_snapshot(&self, raft_group_id: u32) -> Option<LogIdOf<UrsulaRaftTypeConfig>> {
        self.built().get(&raft_group_id).map(|built| built.log_id)
    }

    /// Whether the latest build of `raft_group_id` covers `wanted`, the log
    /// a snapshot is asked for. `None` (nothing applied) is never covered.
    fn build_covers(
        &self,
        raft_group_id: u32,
        wanted: Option<&LogIdOf<UrsulaRaftTypeConfig>>,
    ) -> bool {
        wanted.is_some_and(|wanted| {
            self.built_snapshot(raft_group_id)
                .is_some_and(|built| &built >= wanted)
        })
    }

    /// Whether a build of `raft_group_id` released its permit within
    /// [`SNAPSHOT_HANDOFF_WAIT`] with a snapshot past `published_index`, the
    /// last log index of the snapshot OpenRaft's metrics show. OpenRaft
    /// counts that build as running until RaftCore handled its end, and
    /// drops a trigger of the group meanwhile, so a trigger then goes
    /// unclaimed. Bounded in time, so a group whose Raft never handles the
    /// end, or one that registered again, is not left out for good.
    pub fn publishing(&self, raft_group_id: u32, published_index: Option<u64>) -> bool {
        self.built().get(&raft_group_id).is_some_and(|built| {
            Some(built.log_id.index) > published_index
                && built
                    .released
                    .is_some_and(|released| released.elapsed() < SNAPSHOT_HANDOFF_WAIT)
        })
    }

    /// Adds to `building`, the groups [`Self::builds_in_flight`] read, each
    /// group in `published` (a group and the last log index of the snapshot
    /// OpenRaft's metrics show for it) that is [`Self::publishing`], as
    /// building with nothing left to free. A group already in `building`
    /// keeps the log its build will still free. The snapshot driver leaves
    /// such a group out of its pass, since OpenRaft would drop a trigger of
    /// it, for at most [`SNAPSHOT_HANDOFF_WAIT`] after its build ended.
    pub fn add_publishing(
        &self,
        building: &mut BTreeMap<u32, u64>,
        published: impl IntoIterator<Item = (u32, Option<u64>)>,
    ) {
        for (raft_group_id, published_index) in published {
            if self.publishing(raft_group_id, published_index) {
                building.entry(raft_group_id).or_insert(0);
            }
        }
    }

    fn built(&self) -> std::sync::MutexGuard<'_, BTreeMap<u32, BuiltSnapshot>> {
        self.inner
            .built
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn purges(&self) -> std::sync::MutexGuard<'_, BTreeMap<u32, PurgeHold>> {
        self.inner
            .purges
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn handoffs(&self) -> std::sync::MutexGuard<'_, BTreeMap<u32, HandedOff>> {
        self.inner
            .handoffs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn log_gauges(&self) -> std::sync::MutexGuard<'_, BTreeMap<u32, Arc<GroupLogGauge>>> {
        self.inner
            .log_gauges
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Whether the node's unsnapshotted Raft log is over its hard limit.
    pub fn log_pressured(&self) -> bool {
        self.inner.log_pressure.load(Ordering::Acquire)
    }

    /// Updates the log-pressure flag from the node's unsnapshotted log
    /// bytes, with hysteresis: it sets above `limit_bytes` and clears below
    /// `resume_bytes`. Returns the new state when it changed.
    pub fn observe_log_bytes(
        &self,
        log_bytes: u64,
        limit_bytes: u64,
        resume_bytes: u64,
    ) -> Option<bool> {
        let pressured = self.log_pressured();
        let next = if pressured {
            log_bytes >= resume_bytes
        } else {
            log_bytes > limit_bytes
        };
        (next != pressured).then(|| {
            self.inner.log_pressure.store(next, Ordering::Release);
            next
        })
    }

    #[cfg(all(test, not(madsim)))]
    fn available_permits(&self) -> usize {
        self.inner.semaphore.available_permits()
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotInstallCoordinator {
    inner: Arc<SnapshotInstallCoordinatorInner>,
}

#[derive(Debug, Clone, Copy, Default)]
struct InstallActivity {
    closed: bool,
    active: usize,
}

/// Owned by one engine incarnation, including canceled RPCs' admitted work.
#[derive(Debug)]
pub(crate) struct SnapshotInstallLifecycle {
    activity: crate::rt::sync::watch::Sender<InstallActivity>,
}

impl Default for SnapshotInstallLifecycle {
    fn default() -> Self {
        Self {
            activity: crate::rt::sync::watch::channel(InstallActivity::default()).0,
        }
    }
}

impl SnapshotInstallLifecycle {
    pub(crate) fn admit(self: &Arc<Self>) -> Option<SnapshotInstallLease> {
        let mut admitted = false;
        self.activity.send_if_modified(|state| {
            if state.closed {
                return false;
            }
            state.active = state.active.saturating_add(1);
            admitted = true;
            true
        });
        admitted.then(|| SnapshotInstallLease(self.clone()))
    }

    pub(crate) fn close(&self) {
        self.activity.send_modify(|state| state.closed = true);
    }

    pub(crate) async fn drain(&self) {
        let mut activity = self.activity.subscribe();
        while activity.borrow_and_update().active != 0 {
            if activity.changed().await.is_err() {
                return;
            }
        }
    }
}

pub(crate) struct SnapshotInstallLease(Arc<SnapshotInstallLifecycle>);

impl Drop for SnapshotInstallLease {
    fn drop(&mut self) {
        self.0
            .activity
            .send_modify(|state| state.active = state.active.saturating_sub(1));
    }
}

#[derive(Debug)]
struct SnapshotInstallCoordinatorInner {
    semaphore: Arc<Semaphore>,
    next_install: AtomicU64,
    /// Snapshots downloaded and decoded before OpenRaft's install, keyed by
    /// pointer. Install consumes the decoded group, so it decodes once.
    prefetched: Mutex<BTreeMap<String, PrefetchedGroupSnapshot>>,
    references: Mutex<BTreeMap<u32, Arc<SnapshotReferences>>>,
    installs: Mutex<BTreeMap<u32, Arc<crate::rt::sync::Mutex<()>>>>,
    /// G: how long each group's replaced snapshot object stays pinned
    /// before a GC pass deletes it (#669).
    gc_grace: Duration,
}

/// G when the server does not set it: a quarter of the default
/// `raft.deleted_data_max_age`.
pub const DEFAULT_SNAPSHOT_GC_GRACE: Duration = Duration::from_secs(15 * 60);

#[derive(Debug)]
pub(crate) struct PrefetchedGroupSnapshot {
    pub(crate) snapshot: GroupSnapshot,
    reference: Option<SnapshotReferenceLease>,
    original_snapshot_id: String,
}

impl Default for SnapshotInstallCoordinator {
    fn default() -> Self {
        Self::new(1)
    }
}

impl SnapshotInstallCoordinator {
    pub fn new(max_concurrency: usize) -> Self {
        Self::with_gc_grace(max_concurrency, DEFAULT_SNAPSHOT_GC_GRACE)
    }

    /// A coordinator whose groups keep the snapshot object they replaced
    /// pinned for `gc_grace`, G (#669).
    pub fn with_gc_grace(max_concurrency: usize, gc_grace: Duration) -> Self {
        Self {
            inner: Arc::new(SnapshotInstallCoordinatorInner {
                semaphore: Arc::new(Semaphore::new(max_concurrency.max(1))),
                next_install: AtomicU64::new(0),
                prefetched: Mutex::new(BTreeMap::new()),
                references: Mutex::new(BTreeMap::new()),
                installs: Mutex::new(BTreeMap::new()),
                gc_grace,
            }),
        }
    }

    /// The snapshot GC grace G.
    pub fn gc_grace(&self) -> Duration {
        self.inner.gc_grace
    }

    /// The groups whose snapshot GC is due by `now` (#669), in group order.
    pub(crate) fn gc_due(&self, now: Instant) -> Vec<(u32, Arc<SnapshotReferences>)> {
        self.inner
            .references
            .lock()
            .expect("snapshot references mutex")
            .iter()
            .filter(|(_, references)| references.gc_due(now))
            .map(|(group, references)| (*group, references.clone()))
            .collect()
    }

    pub async fn acquire(
        &self,
        group: RaftGroupId,
    ) -> Result<OwnedSemaphorePermit, SnapshotInstallError> {
        self.inner
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_closed| SnapshotInstallError::GateClosed { group })
    }

    pub(crate) fn install_lock(&self, group: u32) -> Arc<crate::rt::sync::Mutex<()>> {
        self.inner
            .installs
            .lock()
            .expect("snapshot install mutex")
            .entry(group)
            .or_default()
            .clone()
    }

    pub fn cache_key(snapshot_id: &str, location: &SnapshotLocation) -> String {
        match location {
            SnapshotLocation::Inline { bytes } => {
                format!("{snapshot_id}:inline:{}", bytes.len())
            }
            SnapshotLocation::Local { path, size_bytes } => {
                format!("{snapshot_id}:local:{}:{size_bytes}", path.display())
            }
            SnapshotLocation::S3 {
                key,
                size_bytes,
                stored_size_bytes,
                compression,
                ..
            } => {
                format!("{snapshot_id}:s3:{key}:{size_bytes}:{stored_size_bytes}:{compression:?}")
            }
        }
    }

    pub(crate) fn references(&self, group: u32) -> Arc<SnapshotReferences> {
        self.inner
            .references
            .lock()
            .expect("snapshot references mutex")
            .entry(group)
            .or_insert_with(|| Arc::new(SnapshotReferences::new(self.inner.gc_grace)))
            .clone()
    }

    pub(crate) fn cache_prefetched(
        &self,
        pointer: &mut SnapshotPointer,
        snapshot: GroupSnapshot,
        reference: Option<SnapshotReferenceLease>,
    ) -> String {
        let install = self.inner.next_install.fetch_add(1, Ordering::Relaxed);
        let original_snapshot_id = pointer.snapshot_id.clone();
        // This token exists only on the in-process handoff. Installation
        // restores the original pointer before persisting or advertising it.
        pointer.snapshot_id = format!("install-{install}:{}", pointer.snapshot_id);
        let key = Self::cache_key(&pointer.snapshot_id, &pointer.location);
        self.inner
            .prefetched
            .lock()
            .expect("snapshot install prefetch cache mutex")
            .insert(key.clone(), PrefetchedGroupSnapshot {
                snapshot,
                reference,
                original_snapshot_id,
            });
        key
    }

    pub(crate) fn take_prefetched(
        &self,
        pointer: &SnapshotPointer,
    ) -> Option<PrefetchedGroupSnapshot> {
        let key = Self::cache_key(&pointer.snapshot_id, &pointer.location);
        self.clear_prefetched_key(&key)
    }

    pub(crate) fn clear_prefetched_key(&self, key: &str) -> Option<PrefetchedGroupSnapshot> {
        self.inner
            .prefetched
            .lock()
            .expect("snapshot install prefetch cache mutex")
            .remove(key)
    }

    #[cfg(test)]
    fn available_permits(&self) -> usize {
        self.inner.semaphore.available_permits()
    }
}

/// Why this node's persisted snapshot did not restore when its group's
/// engine opened. It keeps every source, and converts once, into the
/// group's error, at the engine boundary.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotRestoreError {
    /// The snapshot metadata file did not read or become durable.
    #[error("raft group {raft_group_id:?}: read the persisted snapshot metadata")]
    Metadata {
        raft_group_id: RaftGroupId,
        #[source]
        source: io::Error,
    },
    /// The snapshot metadata file holds no snapshot pointer.
    #[error("raft group {raft_group_id:?}: decode the persisted snapshot pointer")]
    Pointer {
        raft_group_id: RaftGroupId,
        #[source]
        source: SnapshotStoreError,
    },
    /// Protecting or reading the object the pointer names failed. `key` is
    /// `None` for an inline pointer, which names no object.
    #[error("raft group {raft_group_id:?}: restore the persisted snapshot's object {key:?}")]
    Object {
        raft_group_id: RaftGroupId,
        key: Option<String>,
        /// The directory this node owns for the group, if its store holds
        /// objects.
        directory: Option<String>,
        #[source]
        source: SnapshotStoreError,
    },
    /// The snapshot's body does not decode as a group snapshot. `key` is
    /// `None` for an inline pointer, which carries the body itself.
    #[error("raft group {raft_group_id:?}: decode the persisted snapshot's body {key:?}")]
    Content {
        raft_group_id: RaftGroupId,
        key: Option<String>,
        #[source]
        source: SnapshotStoreError,
    },
    /// The engine refused the restored snapshot.
    #[error(transparent)]
    Engine(GroupEngineError),
}

impl From<SnapshotRestoreError> for GroupEngineError {
    fn from(error: SnapshotRestoreError) -> Self {
        let refusal = match error {
            SnapshotRestoreError::Engine(error) => return error,
            SnapshotRestoreError::Metadata {
                raft_group_id,
                source,
            } => {
                tracing::error!(?raft_group_id, error = %ErrorChain(&source), "the persisted snapshot metadata does not read");
                GroupSnapshotError::PointerUnreadable { raft_group_id }
            }
            SnapshotRestoreError::Pointer {
                raft_group_id,
                source,
            } => {
                tracing::error!(?raft_group_id, error = %ErrorChain(&source), "the persisted snapshot pointer does not decode");
                GroupSnapshotError::PointerUnreadable { raft_group_id }
            }
            SnapshotRestoreError::Content {
                raft_group_id,
                key,
                source,
            } => {
                tracing::error!(?raft_group_id, ?key, error = %ErrorChain(&source), "the persisted snapshot does not decode");
                match key {
                    Some(key) => GroupSnapshotError::ObjectCorrupt {
                        raft_group_id,
                        key,
                        check: SnapshotObjectCheck::Decode,
                    },
                    // An inline pointer carries the body, so the pointer is
                    // what does not read.
                    None => GroupSnapshotError::PointerUnreadable { raft_group_id },
                }
            }
            SnapshotRestoreError::Object {
                raft_group_id,
                key,
                directory,
                source,
            } => {
                tracing::error!(?raft_group_id, ?key, error = %ErrorChain(&source), "the persisted snapshot object does not restore");
                match key {
                    Some(key) => object_refusal(raft_group_id, key, directory, source),
                    None => GroupSnapshotError::PointerUnreadable { raft_group_id },
                }
            }
        };
        GroupEngineError::Infra(GroupInfraError::Snapshot(refusal))
    }
}

/// The restore refusal for a store failure on the object `key` names.
fn object_refusal(
    raft_group_id: RaftGroupId,
    key: String,
    directory: Option<String>,
    error: SnapshotStoreError,
) -> GroupSnapshotError {
    let corrupt = |key, check| GroupSnapshotError::ObjectCorrupt {
        raft_group_id,
        key,
        check,
    };
    match error {
        SnapshotStoreError::NotFound { key } => {
            GroupSnapshotError::ObjectMissing { raft_group_id, key }
        }
        SnapshotStoreError::SizeMismatch {
            key,
            expected,
            actual,
        } => corrupt(key, SnapshotObjectCheck::StoredSize { expected, actual }),
        SnapshotStoreError::LogicalSizeMismatch {
            key,
            expected,
            actual,
        } => corrupt(key, SnapshotObjectCheck::LogicalSize { expected, actual }),
        SnapshotStoreError::Decompress { key, .. }
        | SnapshotStoreError::LogicalSizeUnaddressable { key, .. } => {
            corrupt(key, SnapshotObjectCheck::Decompress)
        }
        SnapshotStoreError::Backend { operation, key, .. } => {
            GroupSnapshotError::StoreUnavailable {
                raft_group_id,
                key,
                operation,
            }
        }
        // An I/O failure inside the store, as of the task that runs its
        // codec, may pass on a retry.
        SnapshotStoreError::Io(_) => GroupSnapshotError::StoreUnavailable {
            raft_group_id,
            key,
            operation: SnapshotStoreOperation::Get,
        },
        // The pointer names an object outside this node's namespace, or one
        // its store does not hold, as after a change of `s3_prefix` or of
        // the snapshot backend.
        SnapshotStoreError::OutsideGroup { key, .. } => GroupSnapshotError::ForeignKey {
            raft_group_id,
            key,
            directory,
        },
        SnapshotStoreError::UnsupportedLocation { .. } => GroupSnapshotError::ForeignKey {
            raft_group_id,
            key,
            directory,
        },
        // The records that protect the object do not read, or the store is
        // misconfigured: no retry reads it.
        SnapshotStoreError::NameCollision { .. }
        | SnapshotStoreError::NotOwnedHere { .. }
        | SnapshotStoreError::Encoder(_)
        | SnapshotStoreError::Compress(_)
        | SnapshotStoreError::BodyTooLarge
        | SnapshotStoreError::ConditionalCreateUnsupported { .. }
        | SnapshotStoreError::Config { .. }
        | SnapshotStoreError::Open(_)
        | SnapshotStoreError::Encryption(_)
        | SnapshotStoreError::InvalidPrefix(_)
        | SnapshotStoreError::InvalidReference { .. }
        | SnapshotStoreError::ReferenceEncode(_)
        | SnapshotStoreError::ReferenceDecode { .. }
        | SnapshotStoreError::Serialize(_)
        | SnapshotStoreError::Deserialize(_)
        | SnapshotStoreError::MissingField { .. }
        | SnapshotStoreError::ProducerFieldMissing { .. }
        | SnapshotStoreError::ProducerDigestLength { .. }
        | SnapshotStoreError::HistoryIdZero
        | SnapshotStoreError::BackupDigestLength { .. }
        | SnapshotStoreError::UnsupportedVersion { .. } => {
            GroupSnapshotError::PointerUnreadable { raft_group_id }
        }
    }
}

/// The key or path of the object a snapshot location names: `None` for an
/// inline location, which names none.
pub(crate) fn object_key(location: &SnapshotLocation) -> Option<String> {
    match location {
        SnapshotLocation::S3 { key, .. } => Some(key.clone()),
        SnapshotLocation::Local { path, .. } => Some(path.display().to_string()),
        SnapshotLocation::Inline { .. } => None,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CurrentSnapshot {
    pub(crate) meta: SnapshotMetaOf<UrsulaRaftTypeConfig>,
    /// Bytes that ride through openraft's `SnapshotData`. With the default
    /// [`ursula_runtime::InlineSnapshotStore`] this is the full snapshot; with
    /// out-of-line backends (Local/S3) this is a tiny [`SnapshotPointer`].
    pointer_bytes: Vec<u8>,
}

pub struct RaftGroupStateMachine {
    pub(crate) apply_stop_signal: Option<Arc<AtomicBool>>,
    pub(crate) apply_health: crate::apply_failure::ApplyHealth,
    #[cfg(any(test, madsim, feature = "fault-injection"))]
    pub(crate) apply_fault: Option<crate::apply_failure::ApplyFault>,
    pub(crate) placement: ShardPlacement,
    pub(crate) engine: InMemoryGroupEngine,
    pub(crate) metrics: Option<GroupEngineMetrics>,
    pub(crate) last_applied_log_id: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    pub(crate) last_membership: StoredMembershipOf<UrsulaRaftTypeConfig>,
    pub(crate) current_snapshot: Arc<Mutex<Option<CurrentSnapshot>>>,
    pub(crate) metadata_serial: Arc<crate::rt::sync::Mutex<MetadataPublicationState>>,
    pub(crate) snapshot_store: SharedSnapshotStore,
    pub(crate) snapshot_build: SnapshotBuildCoordinator,
    pub(crate) snapshot_install: SnapshotInstallCoordinator,
    snapshot_metadata_path: Option<PathBuf>,
    /// Log applied since the last snapshot (F12e), read by the driver.
    log_gauge: Arc<GroupLogGauge>,
}

/// Node-local record of the current snapshot (`group-N.snapshot.json`),
/// in the MessagePack envelope (bounded-state F12a).
#[derive(Debug, Serialize, Deserialize)]
struct PersistedSnapshot {
    meta: SnapshotMetaOf<UrsulaRaftTypeConfig>,
    #[serde(with = "serde_bytes")]
    pointer_bytes: Vec<u8>,
}

impl RaftGroupStateMachine {
    fn stop_apply(
        &self,
        log_id: LogIdOf<UrsulaRaftTypeConfig>,
        error: crate::apply_failure::ApplyError,
    ) -> io::Error {
        let failure = crate::apply_failure::ApplyFailure {
            term: log_id.committed_leader_id().term,
            index: log_id.index(),
            kind: error.kind(),
            message: error.to_string(),
        };
        tracing::error!(raft_group_id = self.placement.raft_group_id.0, log_index = failure.index, error = %error, "stopping failed group; preserve WAL for corrected-code replay");
        self.apply_health.stop(failure, self.last_applied_log_id);
        self.log_gauge.stop_apply();
        if let Some(signal) = &self.apply_stop_signal {
            signal.store(true, Ordering::Release);
        }
        // OpenRaft's state-machine interface requires io::Error at this boundary.
        io::Error::other(error)
    }

    pub fn new(placement: ShardPlacement) -> Self {
        Self::new_with_metrics(placement, None)
    }

    pub(crate) fn new_with_metrics(
        placement: ShardPlacement,
        metrics: Option<GroupEngineMetrics>,
    ) -> Self {
        Self::new_with_metrics_and_cold_store(placement, metrics, None)
    }

    pub(crate) fn new_with_metrics_and_cold_store(
        placement: ShardPlacement,
        metrics: Option<GroupEngineMetrics>,
        cold_store: Option<ColdStoreHandle>,
    ) -> Self {
        Self::new_with_stores(placement, metrics, cold_store, default_snapshot_store())
    }

    pub(crate) fn new_with_stores(
        placement: ShardPlacement,
        metrics: Option<GroupEngineMetrics>,
        cold_store: Option<ColdStoreHandle>,
        snapshot_store: SharedSnapshotStore,
    ) -> Self {
        Self::new_with_stores_and_snapshot_install(
            placement,
            metrics,
            cold_store,
            snapshot_store,
            SnapshotBuildCoordinator::default(),
            SnapshotInstallCoordinator::default(),
            None,
        )
    }

    pub(crate) fn new_with_stores_and_snapshot_install(
        placement: ShardPlacement,
        metrics: Option<GroupEngineMetrics>,
        cold_store: Option<ColdStoreHandle>,
        snapshot_store: SharedSnapshotStore,
        snapshot_build: SnapshotBuildCoordinator,
        snapshot_install: SnapshotInstallCoordinator,
        snapshot_metadata_path: Option<PathBuf>,
    ) -> Self {
        let log_gauge = snapshot_build.log_gauge(placement.raft_group_id.0);
        Self {
            apply_health: Default::default(),
            apply_stop_signal: None,
            #[cfg(any(test, madsim, feature = "fault-injection"))]
            apply_fault: None,
            placement,
            engine: match cold_store {
                Some(cold_store) => InMemoryGroupEngine::with_cold_store(cold_store),
                None => InMemoryGroupEngine::default(),
            },
            metrics,
            last_applied_log_id: None,
            last_membership: StoredMembershipOf::<UrsulaRaftTypeConfig>::default(),
            current_snapshot: Arc::new(Mutex::new(None)),
            metadata_serial: Arc::default(),
            snapshot_store,
            snapshot_build,
            snapshot_install,
            snapshot_metadata_path,
            log_gauge,
        }
    }

    pub(crate) async fn restore_persisted_snapshot(&mut self) -> Result<(), SnapshotRestoreError> {
        let raft_group_id = self.placement.raft_group_id;
        let Some(path) = &self.snapshot_metadata_path else {
            return Ok(());
        };
        if !Disk::exists(path) {
            return Ok(());
        }

        let metadata = |source| SnapshotRestoreError::Metadata {
            raft_group_id,
            source,
        };
        let mut file = Disk::open_read(path).map_err(metadata)?;
        let length = usize::try_from(file.file_len().map_err(metadata)?)
            .map_err(|source| metadata(io::Error::new(io::ErrorKind::InvalidData, source)))?;
        let mut bytes = vec![0; length];
        file.read_exact(&mut bytes).map_err(metadata)?;
        let pointer_unreadable = |source| SnapshotRestoreError::Pointer {
            raft_group_id,
            source,
        };
        let persisted =
            decode_snapshot_envelope::<PersistedSnapshot>(&bytes).map_err(pointer_unreadable)?;
        let pointer =
            SnapshotPointer::decode(&persisted.pointer_bytes).map_err(pointer_unreadable)?;
        // A preceding process may have exited after rename but before its
        // directory fsync. Establish this visible pointer's durability before
        // reconciling away pins for the previously durable pointer.
        if let Some(parent) = path.parent() {
            Disk::sync_dir(parent).map_err(metadata)?;
        }
        let key = object_key(&pointer.location);
        let directory = self.snapshot_store.namespace(raft_group_id).directory();
        let object = |source| SnapshotRestoreError::Object {
            raft_group_id,
            key: key.clone(),
            directory: directory.clone(),
            source,
        };
        let references = self.snapshot_install.references(raft_group_id.0);
        let reference = references
            .prepare(&self.snapshot_store, raft_group_id.0, &pointer.location)
            .await
            .map_err(object)?;
        let snapshot_bytes = match &pointer.location {
            SnapshotLocation::Inline { bytes } => bytes.clone(),
            location => self
                .snapshot_store
                .download(location)
                .await
                .map_err(object)?,
        };
        let group_snapshot = decode_group_snapshot(&snapshot_bytes).map_err(|source| {
            SnapshotRestoreError::Content {
                raft_group_id,
                key: key.clone(),
                source,
            }
        })?;
        self.engine
            .install_snapshot(group_snapshot)
            .await
            .map_err(SnapshotRestoreError::Engine)?;
        self.last_applied_log_id = persisted.meta.last_log_id;
        self.last_membership = persisted.meta.last_membership.clone();
        self.log_gauge.record_membership(&self.last_membership);
        self.log_gauge.record_restored_snapshot(
            self.log_gauge.mark(),
            pointer.location.size_hint(),
            persisted
                .meta
                .last_log_id
                .map_or(0, |log_id| log_id.index()),
        );
        *self.current_snapshot.lock().expect("snapshot mutex") = Some(CurrentSnapshot {
            meta: persisted.meta,
            pointer_bytes: persisted.pointer_bytes,
        });
        references.commit_current(&pointer.location);
        drop(reference);
        if let Err(err) = references
            .publish_current(&self.snapshot_store, self.placement.raft_group_id.0)
            .await
        {
            tracing::warn!(error = %ErrorChain(&err), "restored snapshot remains pinned while current-reference publication is unavailable");
        }
        // The reconcile above dropped the pins of the objects that were
        // retiring before the restart: the restart ended this process's
        // sends of them, so they need no pin. This pass makes sure they are
        // deleted (#669).
        references.schedule_pass_after_restore(self.placement.raft_group_id.0);
        Ok(())
    }

    /// Log applied since this group's last snapshot (F12e).
    pub fn log_progress(&self) -> GroupLogProgress {
        self.log_gauge.progress()
    }

    pub async fn group_snapshot(&mut self) -> Result<GroupSnapshot, io::Error> {
        self.engine
            .snapshot(self.placement)
            .await
            .map_err(group_engine_io_error)
    }

    pub async fn head_stream(
        &mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> Result<HeadStreamResponse, GroupEngineError> {
        self.engine.head_stream(request, placement).await
    }

    pub async fn read_stream(
        &mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> Result<ReadStreamResponse, GroupEngineError> {
        self.engine.read_stream(request, placement).await
    }

    pub async fn read_snapshot(
        &mut self,
        request: ReadSnapshotRequest,
        placement: ShardPlacement,
    ) -> Result<ReadSnapshotResponse, GroupEngineError> {
        self.engine.read_snapshot(request, placement).await
    }

    pub async fn bootstrap_stream(
        &mut self,
        request: BootstrapStreamRequest,
        placement: ShardPlacement,
    ) -> Result<BootstrapStreamResponse, GroupEngineError> {
        self.engine.bootstrap_stream(request, placement).await
    }

    pub async fn access_requires_write(
        &mut self,
        stream_id: &BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
    ) -> Result<bool, GroupEngineError> {
        self.engine
            .access_requires_write(stream_id, now_ms, renew_ttl)
    }

    pub async fn plan_cold_flush(
        &mut self,
        request: PlanColdFlushRequest,
        placement: ShardPlacement,
    ) -> Result<Option<ColdFlushCandidate>, GroupEngineError> {
        self.engine.plan_cold_flush(request, placement).await
    }

    pub async fn plan_next_cold_flush_batch(
        &mut self,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
        max_candidates: usize,
    ) -> Result<Vec<ColdFlushCandidate>, GroupEngineError> {
        self.engine
            .plan_next_cold_flush_batch(request, placement, max_candidates)
            .await
    }

    pub async fn cold_hot_backlog(
        &mut self,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> Result<ColdHotBacklog, GroupEngineError> {
        self.engine.cold_hot_backlog(stream_id, placement).await
    }

    pub async fn plan_cold_gc(
        &mut self,
        max: usize,
        placement: ShardPlacement,
    ) -> Result<Vec<ColdGcPlanEntry>, GroupEngineError> {
        self.engine.plan_cold_gc(max, placement).await
    }

    pub async fn check_create_stream_cold_admission(
        &mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<(), GroupEngineError> {
        let _ = placement;
        self.engine.check_cold_write_admission(
            &request.stream_id,
            admission,
            u64::try_from(request.initial_payload.len()).expect("payload len fits u64"),
        )?;
        Ok(())
    }

    pub async fn check_append_cold_admission(
        &mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<(), GroupEngineError> {
        let _ = placement;
        self.engine.check_cold_write_admission(
            &request.stream_id,
            admission,
            u64::try_from(request.payload.len()).expect("payload len fits u64"),
        )?;
        Ok(())
    }

    pub async fn install_group_snapshot(
        &mut self,
        snapshot: GroupSnapshot,
    ) -> Result<(), GroupEngineError> {
        self.engine.install_snapshot(snapshot).await
    }

    async fn snapshot_builder_with_lease(
        &mut self,
        build: SnapshotBuildLease,
    ) -> RaftGroupSnapshotBuilder {
        let snapshot = self.engine.snapshot(self.placement).await.map(Arc::new);
        RaftGroupSnapshotBuilder {
            apply_health: self.apply_health.clone(),
            placement: self.placement,
            snapshot,
            meta: self.snapshot_meta(),
            current_snapshot: self.current_snapshot.clone(),
            metadata_serial: self.metadata_serial.clone(),
            snapshot_store: self.snapshot_store.clone(),
            metrics: self.metrics.clone(),
            build,
            snapshot_metadata_path: self.snapshot_metadata_path.clone(),
            log_gauge: Arc::clone(&self.log_gauge),
            log_mark: self.log_gauge.mark(),
            references: self
                .snapshot_install
                .references(self.placement.raft_group_id.0),
        }
    }

    pub(crate) fn snapshot_meta(&self) -> SnapshotMetaOf<UrsulaRaftTypeConfig> {
        SnapshotMetaOf::<UrsulaRaftTypeConfig> {
            last_log_id: self.last_applied_log_id,
            last_membership: self.last_membership.clone(),
            snapshot_id: self
                .last_applied_log_id
                .map(|log_id| {
                    format!(
                        "group-{}-{}-{}",
                        self.placement.raft_group_id.0,
                        log_id.committed_leader_id(),
                        log_id.index()
                    )
                })
                .unwrap_or_else(|| format!("group-{}-empty", self.placement.raft_group_id.0)),
        }
    }
}

impl RaftStateMachine<UrsulaRaftTypeConfig> for RaftGroupStateMachine {
    type SnapshotBuilder = RaftGroupSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogIdOf<UrsulaRaftTypeConfig>>,
            StoredMembershipOf<UrsulaRaftTypeConfig>,
        ),
        io::Error,
    > {
        Ok((self.last_applied_log_id, self.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where Strm: Stream<Item = Result<EntryResponder<UrsulaRaftTypeConfig>, io::Error>>
            + Unpin
            + openraft::OptionalSend {
        let mut applied_entries = 0usize;
        let mut apply_ns = 0u64;
        while let Some((entry, responder)) = entries.try_next().await? {
            let entry_bytes = crate::types::entry_log_bytes(&entry);
            let response = match entry.payload {
                EntryPayload::Blank => RaftGroupResponse::Blank,
                EntryPayload::Normal(command) => {
                    let apply_started_at = Instant::now();
                    applied_entries = applied_entries.saturating_add(1);
                    // What the entry removes shows in the tally around it
                    // (#669).
                    let removed_before = self.engine.removed_state();
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let result = self.engine.apply_committed_entry(
                            command,
                            entry.log_id.index(),
                            self.placement,
                        );
                        // Inject AFTER mutation to prove partial state cannot escape.
                        #[cfg(any(test, madsim, feature = "fault-injection"))]
                        match self.apply_fault {
                            Some(crate::apply_failure::ApplyFault::InvariantAfterMutation {
                                index,
                            }) if index == entry.log_id.index() => {
                                return Err(GroupEngineError::Infra(
                                    ursula_runtime::GroupInfraError::ProtoDecode {
                                        field: "injected-after-mutation".to_owned(),
                                    },
                                ));
                            }
                            Some(crate::apply_failure::ApplyFault::PanicAfterMutation {
                                index,
                            }) => assert_ne!(
                                index,
                                entry.log_id.index(),
                                "injected deterministic apply failure"
                            ),
                            _ => {}
                        }
                        result
                    }));
                    let response = match outcome {
                        Ok(Err(error @ GroupEngineError::Infra(_))) => {
                            return Err(self.stop_apply(
                                entry.log_id,
                                crate::apply_failure::ApplyError::InvariantViolation(error),
                            ));
                        }
                        Ok(result) => {
                            let removed = self.engine.removed_state().since(removed_before);
                            if removed.streams > 0 {
                                self.log_gauge.record_removal(removed.bytes);
                            }
                            RaftGroupResponse::Write(result)
                        }
                        Err(payload) => {
                            let message = payload
                                .downcast_ref::<&str>()
                                .map(|value| (*value).to_owned())
                                .or_else(|| payload.downcast_ref::<String>().cloned())
                                .unwrap_or_else(|| "non-string panic payload".to_owned());
                            return Err(self.stop_apply(
                                entry.log_id,
                                crate::apply_failure::ApplyError::Panic { message },
                            ));
                        }
                    };
                    apply_ns = apply_ns.saturating_add(elapsed_ns(apply_started_at));
                    response
                }
                EntryPayload::Membership(membership) => {
                    self.last_membership = StoredMembershipOf::<UrsulaRaftTypeConfig>::new(
                        Some(entry.log_id),
                        membership,
                    );
                    self.log_gauge.record_membership(&self.last_membership);
                    RaftGroupResponse::Membership
                }
            };

            self.last_applied_log_id = Some(entry.log_id);
            self.log_gauge.record_applied(entry_bytes);
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        if applied_entries > 0
            && let Some(metrics) = &self.metrics
        {
            metrics.record_raft_apply_batch(self.placement, applied_entries, apply_ns);
        }
        Ok(())
    }

    async fn try_create_snapshot_builder(&mut self, force: bool) -> Option<Self::SnapshotBuilder> {
        if force {
            return Some(self.get_snapshot_builder().await);
        }
        let raft_group_id = self.placement.raft_group_id.0;
        // Nothing applies on this worker until the builder captured the
        // group, so the log applied now is what the snapshot covers.
        let log_mark = self.log_gauge.mark().bytes();
        // The snapshot driver hands the permit over before it triggers. A
        // policy-triggered build otherwise defers instead of waiting for the
        // node-wide permit on this group's state-machine worker; OpenRaft
        // retries on a later trigger.
        let build = match self.snapshot_build.lease_for_build(raft_group_id, log_mark) {
            Ok(build) => build,
            Err(BuildDeferred::PurgePending { through }) => {
                tracing::debug!(
                    raft_group_id,
                    through,
                    "deferring OpenRaft snapshot build until the purge through a snapshot lands"
                );
                return None;
            }
            Err(BuildDeferred::NoPermit) => {
                tracing::debug!(
                    raft_group_id,
                    "deferring OpenRaft snapshot build while other groups hold every build permit"
                );
                return None;
            }
        };
        Some(self.snapshot_builder_with_lease(build).await)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let raft_group_id = self.placement.raft_group_id.0;
        // A build here waits for a purge through a snapshot to land, like
        // every other build of the group (#669).
        self.snapshot_build.purge_landed(raft_group_id).await;
        let log_mark = self.log_gauge.mark().bytes();
        // A permit already handed to this group would otherwise be held
        // while this build waits for another one.
        let build = match self.snapshot_build.claim_handoff(raft_group_id, log_mark) {
            Some(build) => build,
            None => self
                .snapshot_build
                .wait_lease(raft_group_id, log_mark)
                .await
                .expect("snapshot build coordinator should not close"),
        };
        self.snapshot_builder_with_lease(build).await
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<SnapshotDataOf<UrsulaRaftTypeConfig>, io::Error> {
        Ok(SnapshotData::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<UrsulaRaftTypeConfig>,
        snapshot: SnapshotDataOf<UrsulaRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        // A received snapshot holds nothing on this node, so the lease is
        // `None`, and it is kept to the end of the install all the same.
        let (mut pointer_bytes, _lease) = snapshot.into_parts();
        // A publication retry still passes through Raft's vote checks, but the
        // already durable snapshot needs neither download nor installation.
        if self
            .current_snapshot
            .lock()
            .expect("snapshot mutex")
            .as_ref()
            .is_some_and(|current| current.meta == *meta && current.pointer_bytes == pointer_bytes)
        {
            return Ok(());
        }
        let mut pointer = SnapshotPointer::decode(&pointer_bytes)
            .map_err(|err| invalid_data(io::Error::other(err.to_string())))?;
        // Decode exactly once (bounded-stream-state F12c): inline bytes are
        // decoded in place, and a prefetched external snapshot arrives
        // already decoded.
        let mut reference = None;
        let group_snapshot = match &pointer.location {
            SnapshotLocation::Inline { bytes } => {
                decode_group_snapshot(bytes).map_err(|err| err.into_io())?
            }
            location => match self.snapshot_install.take_prefetched(&pointer) {
                Some(prefetched) => {
                    reference = prefetched.reference;
                    pointer.snapshot_id = prefetched.original_snapshot_id;
                    pointer_bytes = pointer.encode_binary().map_err(|err| err.into_io())?;
                    prefetched.snapshot
                }
                None => {
                    if matches!(location, SnapshotLocation::S3 { .. }) {
                        return Err(io::Error::other(
                            "external S3 snapshots must be pinned and prefetched before Raft installation",
                        ));
                    }
                    let bytes = self
                        .snapshot_store
                        .download(location)
                        .await
                        .map_err(|err| err.into_io())?;
                    decode_group_snapshot(&bytes).map_err(|err| err.into_io())?
                }
            },
        };
        self.engine
            .install_snapshot(group_snapshot)
            .await
            .map_err(group_engine_io_error)?;
        self.last_applied_log_id = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        self.log_gauge.record_membership(&self.last_membership);
        self.log_gauge.record_snapshot(
            self.log_gauge.mark(),
            pointer.location.size_hint(),
            meta.last_log_id.map_or(0, |log_id| log_id.index()),
        );
        let current_snapshot = self.current_snapshot.clone();
        let metadata_path = self.snapshot_metadata_path.clone();
        let references = self
            .snapshot_install
            .references(self.placement.raft_group_id.0);
        let meta = meta.clone();
        snapshot_metadata_work(self.metadata_serial.clone(), move || {
            let _reference = reference;
            persist_snapshot_candidate(
                metadata_path.as_deref(),
                &meta,
                &pointer_bytes,
                &pointer.location,
                &references,
            )?;
            *current_snapshot.lock().expect("snapshot mutex") = Some(CurrentSnapshot {
                meta,
                pointer_bytes,
            });
            references.commit_current(&pointer.location);
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<UrsulaRaftTypeConfig>>, io::Error> {
        Ok(self
            .current_snapshot
            .lock()
            .expect("snapshot mutex")
            .as_ref()
            .map(|snapshot| SnapshotOf::<UrsulaRaftTypeConfig> {
                meta: snapshot.meta.clone(),
                snapshot: SnapshotData::new(snapshot.pointer_bytes.clone()),
            }))
    }
}

pub struct RaftGroupSnapshotBuilder {
    apply_health: crate::apply_failure::ApplyHealth,
    placement: ShardPlacement,
    /// Shared with the frame iterators, so an upload and any inline fallback
    /// encode the same group without deep-cloning it (F12c).
    snapshot: Result<Arc<GroupSnapshot>, GroupEngineError>,
    pub(crate) meta: SnapshotMetaOf<UrsulaRaftTypeConfig>,
    current_snapshot: Arc<Mutex<Option<CurrentSnapshot>>>,
    metadata_serial: Arc<crate::rt::sync::Mutex<MetadataPublicationState>>,
    snapshot_store: SharedSnapshotStore,
    metrics: Option<GroupEngineMetrics>,
    /// The build's permit, counted in flight until the builder is dropped.
    build: SnapshotBuildLease,
    snapshot_metadata_path: Option<PathBuf>,
    log_gauge: Arc<GroupLogGauge>,
    /// The applied log this snapshot covers (F12e).
    log_mark: GroupLogMark,
    references: Arc<SnapshotReferences>,
}

impl RaftSnapshotBuilder<UrsulaRaftTypeConfig> for RaftGroupSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<UrsulaRaftTypeConfig>, io::Error> {
        self.apply_health
            .check(self.placement.raft_group_id)
            .map_err(group_engine_io_error)?;
        // A build of an engine that already shut down would be refused at its
        // publication anyway. Refusing it here skips its upload and its pin,
        // and hands the node-wide build permit back sooner. The check under
        // the publication's lock stays the one that counts.
        if snapshot_metadata_closed(&self.metadata_serial) {
            return Err(SnapshotMetadataStopped::EngineShutDown.into_io());
        }
        let snapshot = self
            .snapshot
            .as_ref()
            .map_err(|error| group_engine_io_error(error.clone()))?;
        let started_at = Instant::now();
        let stream_count = snapshot.stream_snapshot.streams.len();
        let snapshot_id = self.meta.snapshot_id.clone();
        let key = SnapshotKey {
            raft_group_id: self.placement.raft_group_id.0,
            snapshot_id: snapshot_id.clone(),
        };
        let location = match self
            .snapshot_store
            .upload_iter(key, group_snapshot_frames(Arc::clone(snapshot)))
            .await
        {
            Ok(location) => {
                // An upload that never becomes current, because a newer
                // install overtakes this build or its pin fails, has no
                // retirement to delete it: a GC pass G later does (#669).
                self.references.schedule_pass_after_upload();
                // Re-stat immediately so a silent partial-success (multipart
                // Complete failing after the parts uploaded, opendal retry
                // caching, etc.) is caught HERE rather than 10 minutes later
                // as an install_snapshot NotFound on a follower. Cheap
                // relative to the upload itself.
                match self.snapshot_store.verify_uploaded(&location).await {
                    Ok(()) => location,
                    Err(err) => {
                        tracing::warn!(
                            snapshot_id,
                            error = %ErrorChain(&err),
                            "falling back to inline OpenRaft snapshot after external snapshot verification failed"
                        );
                        SnapshotLocation::Inline {
                            bytes: group_snapshot_frames(Arc::clone(snapshot))
                                .collect::<Result<Vec<_>, _>>()
                                .map_err(|err| err.into_io())?
                                .into_iter()
                                .flat_map(|chunk| chunk.to_vec())
                                .collect(),
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(
                    snapshot_id,
                    error = %ErrorChain(&err),
                    "falling back to inline OpenRaft snapshot after external snapshot upload failed"
                );
                SnapshotLocation::Inline {
                    bytes: group_snapshot_frames(Arc::clone(snapshot))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|err| err.into_io())?
                        .into_iter()
                        .flat_map(|chunk| chunk.to_vec())
                        .collect(),
                }
            }
        };
        let mut pointer = SnapshotPointer {
            snapshot_id: snapshot_id.clone(),
            location,
        };
        let reference = match self
            .references
            .prepare(
                &self.snapshot_store,
                self.placement.raft_group_id.0,
                &pointer.location,
            )
            .await
        {
            Ok(reference) => reference,
            Err(err) => {
                tracing::warn!(snapshot_id, error = %ErrorChain(&err), "falling back to inline snapshot after external pin failure");
                pointer.location = SnapshotLocation::Inline {
                    bytes: group_snapshot_frames(Arc::clone(snapshot))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|err| err.into_io())?
                        .into_iter()
                        .flat_map(|chunk| chunk.to_vec())
                        .collect(),
                };
                None
            }
        };
        let pointer_bytes = pointer.encode_binary().map_err(|err| err.into_io())?;
        let external_upload = !matches!(pointer.location, SnapshotLocation::Inline { .. });
        let snapshot_bytes = usize::try_from(pointer.location.size_hint()).unwrap_or(usize::MAX);
        let pointer_len = pointer_bytes.len();
        let current_snapshot = self.current_snapshot.clone();
        let metadata_path = self.snapshot_metadata_path.clone();
        let meta = self.meta.clone();
        let references = self.references.clone();
        let log_gauge = self.log_gauge.clone();
        let log_mark = self.log_mark;
        let snapshot_index = meta.last_log_id.map_or(0, |log_id| log_id.index());
        let chosen = snapshot_metadata_work(self.metadata_serial.clone(), move || {
            let _reference = reference;
            let previous = current_snapshot.lock().expect("snapshot mutex").clone();
            // A build captured before a newer install must not overwrite its
            // durable metadata, reference or pointer when the upload finishes.
            if let Some(current) = previous.as_ref()
                && current.meta.last_log_id > meta.last_log_id
            {
                Ok(current.clone())
            } else {
                persist_snapshot_candidate(
                    metadata_path.as_deref(),
                    &meta,
                    &pointer_bytes,
                    &pointer.location,
                    &references,
                )?;
                let current = CurrentSnapshot {
                    meta,
                    pointer_bytes,
                };
                current_snapshot
                    .lock()
                    .expect("snapshot mutex")
                    .replace(current.clone());
                references.commit_current(&pointer.location);
                log_gauge.record_snapshot(log_mark, pointer.location.size_hint(), snapshot_index);
                Ok(current)
            }
        })
        .await?;
        // Only a build whose publication ran counts as built.
        if let Some(metrics) = &self.metrics {
            metrics.record_raft_snapshot_build(
                self.placement,
                stream_count,
                snapshot_bytes,
                pointer_len,
                elapsed_ns(started_at),
                external_upload,
                !external_upload,
            );
        }
        // The snapshot is durable now: its body uploaded and verified (or
        // inline), its reference pinned and its metadata persisted, and it is
        // the group's current snapshot. Nothing below fails the build: the
        // reference publication and the pruning can take seconds and only
        // warn. So the snapshot is recorded here, before them and before the
        // permit goes back with the builder, and a snapshot start that waited
        // for this build sees what it covers (#561).
        self.build.record_built(chosen.meta.last_log_id);
        if let Err(err) = self
            .references
            .publish_current(&self.snapshot_store, self.placement.raft_group_id.0)
            .await
        {
            tracing::warn!(snapshot_id, error = %ErrorChain(&err), "snapshot remains pinned while current-reference publication is unavailable");
        }
        // The object this build replaced stays pinned for the GC grace, so
        // this pass deletes only what was retiring before (#669).
        match self
            .references
            .prune(&self.snapshot_store, self.placement.raft_group_id.0)
            .await
        {
            Ok(prune) => {
                if let Some(metrics) = &self.metrics {
                    metrics.record_snapshot_gc(ursula_runtime::SnapshotGcSample {
                        deleted: prune.deleted(),
                        skipped_unreferenced_voter: u64::from(prune.skipped_unreferenced_voter()),
                        failed: 0,
                    });
                }
            }
            Err(err) => {
                if let Some(metrics) = &self.metrics {
                    metrics.record_snapshot_gc(ursula_runtime::SnapshotGcSample {
                        failed: 1,
                        ..ursula_runtime::SnapshotGcSample::default()
                    });
                }
                tracing::warn!(
                    snapshot_id = pointer.snapshot_id,
                    error = %ErrorChain(&err),
                    "failed to prune retired OpenRaft snapshots"
                );
            }
        }
        Ok(SnapshotOf::<UrsulaRaftTypeConfig> {
            meta: chosen.meta,
            snapshot: SnapshotData::new(chosen.pointer_bytes),
        })
    }
}

/// Where a group's snapshot metadata publication stands. Local snapshot
/// metadata errors stop OpenRaft, and queued pointer operations must observe
/// the same terminal boundary even before RaftCore sees the error. A group
/// engine that shut down publishes nothing more: OpenRaft runs each snapshot
/// build in a task that its shutdown neither stops nor waits for, so a build
/// can reach its publication after the engine stopped, while the WAL closes
/// or a restarted replica of the group owns the same metadata file (#489).
#[derive(Debug, Default)]
pub(crate) enum MetadataPublicationState {
    #[default]
    Ready,
    Failed,
    ShutDown,
}

/// Why a snapshot metadata publication was refused without running.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotMetadataStopped {
    #[error(
        "snapshot metadata publication stopped after an earlier failure; restart to resolve the durable pointer"
    )]
    AfterFailure,
    #[error("snapshot metadata publication stopped because the group engine shut down")]
    EngineShutDown,
}

impl SnapshotMetadataStopped {
    /// The refusal as the io::Error OpenRaft's snapshot storage API requires.
    fn into_io(self) -> io::Error {
        io::Error::new(io::ErrorKind::BrokenPipe, self)
    }
}

/// Whether the group engine closed its snapshot metadata publication. A
/// publication running now holds the lock and counts as open: the check
/// under the lock in [`snapshot_metadata_work`] decides.
fn snapshot_metadata_closed(serial: &crate::rt::sync::Mutex<MetadataPublicationState>) -> bool {
    serial
        .try_lock()
        .is_ok_and(|state| matches!(*state, MetadataPublicationState::ShutDown))
}

/// Ends a group's snapshot metadata publication for good, once a publication
/// that is running finished: a blocking fsync cannot be cancelled. Every
/// later publication is refused with [`SnapshotMetadataStopped::EngineShutDown`].
pub(crate) async fn close_snapshot_metadata(
    serial: &crate::rt::sync::Mutex<MetadataPublicationState>,
) {
    *serial.lock().await = MetadataPublicationState::ShutDown;
}

/// Keep metadata fsync and the serialized pointer transition off core workers.
pub(crate) async fn snapshot_metadata_work<T: Send + 'static>(
    serial: Arc<crate::rt::sync::Mutex<MetadataPublicationState>>,
    work: impl FnOnce() -> Result<T, io::Error> + Send + 'static,
) -> Result<T, io::Error> {
    let mut serial = serial.lock_owned().await;
    let work = move || {
        let refused = match *serial {
            MetadataPublicationState::Ready => None,
            MetadataPublicationState::Failed => Some(SnapshotMetadataStopped::AfterFailure),
            MetadataPublicationState::ShutDown => Some(SnapshotMetadataStopped::EngineShutDown),
        };
        if let Some(refused) = refused {
            return Err(refused.into_io());
        }
        let result = work();
        if result.is_err() {
            *serial = MetadataPublicationState::Failed;
        }
        result
    };
    #[cfg(not(madsim))]
    {
        tokio::task::spawn_blocking(work)
            .await
            .map_err(io::Error::other)?
    }
    #[cfg(madsim)]
    {
        work()
    }
}

fn persist_snapshot_candidate(
    path: Option<&Path>,
    meta: &SnapshotMetaOf<UrsulaRaftTypeConfig>,
    pointer_bytes: &[u8],
    location: &SnapshotLocation,
    references: &SnapshotReferences,
) -> Result<(), io::Error> {
    persist_snapshot_metadata(path, meta, pointer_bytes).inspect_err(|_error| {
        // Rename may already have happened. Keep the candidate even if this
        // particular failure occurred earlier; the terminal latch bounds it.
        references.retain_failed_candidate(location);
    })
}

fn persist_snapshot_metadata(
    path: Option<&Path>,
    meta: &SnapshotMetaOf<UrsulaRaftTypeConfig>,
    pointer_bytes: &[u8],
) -> Result<(), io::Error> {
    let Some(path) = path else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        create_dir_all_durable(parent)?;
    }
    let encoded = encode_binary_envelope(&PersistedSnapshot {
        meta: meta.clone(),
        pointer_bytes: pointer_bytes.to_vec(),
    })
    .map_err(|err| invalid_data(io::Error::other(err.to_string())))?;
    // The WAL state files' publication protocol, with this record's own encoding.
    replace_file(path, &path.with_extension("json.tmp"), &encoded)
        .map_err(|err| io::Error::new(err.kind(), err))?;
    Ok(())
}

#[cfg(all(test, madsim))]
#[path = "snapshot_metadata_tests.rs"]
mod snapshot_metadata_tests;

#[cfg(test)]
mod tests {
    #[cfg(all(unix, not(madsim)))]
    #[test]
    fn snapshot_metadata_rejects_an_unsyncable_parent_directory() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("snapshot.json");
        let meta = SnapshotMetaOf::<UrsulaRaftTypeConfig> {
            last_log_id: None,
            last_membership: Default::default(),
            snapshot_id: "directory-sync-failure".to_owned(),
        };
        // Write/search permits file creation and rename; lack of read permission
        // prevents opening the directory for fsync after the rename succeeds.
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o300)).unwrap();
        let can_open = std::fs::File::open(root.path());
        let result = persist_snapshot_metadata(Some(&path), &meta, b"pointer");
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let error = can_open.expect_err(
            "directory-permission fault must be exercised: run this test as an unprivileged user on a filesystem that enforces directory read permissions",
        );
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(
            path.exists(),
            "the metadata rename must finish before the injected failure"
        );
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    }
    #[test]
    fn stopped_log_does_not_hold_healthy_group_write_pressure_closed() {
        let coordinator = SnapshotBuildCoordinator::default();
        let stopped = coordinator.log_gauge(0);
        let healthy = coordinator.log_gauge(1);
        stopped.record_applied(10_000);
        healthy.record_applied(1);
        assert_eq!(coordinator.node_log_bytes(), 10_001);
        stopped.stop_apply();
        assert_eq!(coordinator.node_log_bytes(), 1);
        assert_eq!(
            stopped.progress().log_bytes,
            10_000,
            "diagnostics retain the actual byte count"
        );
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn canceled_metadata_waiter_retains_serial_until_publication() {
        let serial = Arc::new(crate::rt::sync::Mutex::new(MetadataPublicationState::Ready));
        let (release, wait) = std::sync::mpsc::channel();
        let entered = Arc::new(crate::rt::sync::Notify::new());
        let worker_entered = entered.clone();
        let published = Arc::new(AtomicBool::new(false));
        let worker_published = published.clone();
        let work = crate::rt::spawn(snapshot_metadata_work(serial.clone(), move || {
            worker_entered.notify_one();
            wait.recv_timeout(std::time::Duration::from_secs(2))
                .map_err(io::Error::other)?;
            worker_published.store(true, Ordering::Release);
            Ok(())
        }));
        entered.notified().await;
        work.abort();
        assert!(work.await.unwrap_err().is_cancelled());
        assert!(
            serial.try_lock().is_err(),
            "cancellation must retain the publication guard"
        );
        release.send(()).unwrap();
        let _drained = tokio::time::timeout(std::time::Duration::from_secs(2), serial.lock())
            .await
            .unwrap();
        assert!(published.load(Ordering::Acquire));
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn failed_metadata_publication_rejects_an_already_queued_writer() {
        let serial = Arc::new(crate::rt::sync::Mutex::new(MetadataPublicationState::Ready));
        let (release, wait) = std::sync::mpsc::channel();
        let (entered, entered_rx) = tokio::sync::oneshot::channel();
        let first = tokio::spawn(snapshot_metadata_work(serial.clone(), move || {
            entered.send(()).unwrap();
            wait.recv_timeout(std::time::Duration::from_secs(2))
                .unwrap();
            Err::<(), _>(io::Error::from(io::ErrorKind::Other))
        }));
        entered_rx.await.unwrap();
        let executed = Arc::new(AtomicBool::new(false));
        let second_executed = executed.clone();
        let second = snapshot_metadata_work(serial, move || {
            second_executed.store(true, Ordering::Release);
            Ok(())
        });
        tokio::pin!(second);
        assert!(futures_util::poll!(&mut second).is_pending());
        release.send(()).unwrap();
        assert_eq!(
            first.await.unwrap().unwrap_err().kind(),
            io::ErrorKind::Other
        );
        let error = tokio::time::timeout(std::time::Duration::from_secs(2), second)
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(
            error
                .get_ref()
                .unwrap()
                .downcast_ref::<SnapshotMetadataStopped>(),
            Some(SnapshotMetadataStopped::AfterFailure)
        ));
        assert!(!executed.load(Ordering::Acquire));
    }

    #[cfg(not(madsim))]
    #[tokio::test(flavor = "current_thread")]
    async fn metadata_work_does_not_block_the_core_executor() {
        let (release, wait) = std::sync::mpsc::channel();
        let entered = Arc::new(crate::rt::sync::Notify::new());
        let worker_entered = entered.clone();
        let metadata = crate::rt::spawn(snapshot_metadata_work(Arc::default(), move || {
            worker_entered.notify_one();
            wait.recv_timeout(std::time::Duration::from_secs(2))
                .map_err(io::Error::other)
        }));
        entered.notified().await;
        // This runs on the same single-thread executor as snapshot installation.
        // Running blocking metadata work inline would time out before reaching it.
        release.send(()).unwrap();
        metadata.await.unwrap().unwrap();
    }
    #[cfg(not(madsim))]
    use bytes::Bytes;

    use super::*;

    #[cfg(not(madsim))]
    fn test_log_id(index: u64) -> LogIdOf<UrsulaRaftTypeConfig> {
        use openraft::LogId;
        use openraft::vote::RaftLeaderId;

        type LeaderId = <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;
        LogId {
            leader_id: LeaderId::new(1, 1),
            index,
        }
    }

    #[cfg(not(madsim))]
    fn test_snapshot_meta(index: u64) -> SnapshotMetaOf<UrsulaRaftTypeConfig> {
        let log_id = test_log_id(index);
        SnapshotMetaOf::<UrsulaRaftTypeConfig> {
            last_log_id: Some(log_id),
            last_membership: StoredMembershipOf::<UrsulaRaftTypeConfig>::default(),
            snapshot_id: format!(
                "group-7-{}-{}",
                log_id.committed_leader_id(),
                log_id.index()
            ),
        }
    }

    #[cfg(not(madsim))]
    fn test_group_snapshot(placement: ShardPlacement, commit_index: u64) -> GroupSnapshot {
        GroupSnapshot {
            placement,
            group_commit_index: commit_index,
            stream_snapshot: Default::default(),
            stream_append_counts: Vec::new(),
        }
    }

    #[cfg(not(madsim))]
    async fn test_build_lease() -> SnapshotBuildLease {
        SnapshotBuildCoordinator::default()
            .wait_lease(7, 0)
            .await
            .expect("test snapshot build permit")
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn persisted_snapshot_restores_state_machine_before_log_replay() {
        use ursula_shard::CoreId;
        use ursula_shard::RaftGroupId;
        use ursula_shard::ShardId;

        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(7),
        };
        let directory =
            std::env::temp_dir().join(format!("ursula-persisted-snapshot-{}", std::process::id()));
        crate::tests::remove_test_path(&directory);
        std::fs::create_dir_all(&directory).expect("snapshot metadata directory");
        let metadata_path = directory.join("group-7.snapshot.json");
        let current_snapshot = Arc::new(Mutex::new(None));
        let mut builder = RaftGroupSnapshotBuilder {
            apply_health: Default::default(),
            placement,
            snapshot: Ok(Arc::new(test_group_snapshot(placement, 42))),
            meta: test_snapshot_meta(42),
            current_snapshot,
            metadata_serial: Arc::default(),
            snapshot_store: default_snapshot_store(),
            metrics: None,
            build: test_build_lease().await,
            log_gauge: Arc::default(),
            log_mark: GroupLogMark::default(),
            references: Arc::default(),
            snapshot_metadata_path: Some(metadata_path.clone()),
        };
        builder.build_snapshot().await.expect("persist snapshot");
        // #561: the build records what it covers while it holds its permit.
        assert_eq!(
            builder.build.coordinator.built_snapshot(7),
            Some(test_log_id(42))
        );

        let mut restored = RaftGroupStateMachine::new_with_stores_and_snapshot_install(
            placement,
            None,
            None,
            default_snapshot_store(),
            SnapshotBuildCoordinator::default(),
            SnapshotInstallCoordinator::default(),
            Some(metadata_path),
        );
        restored
            .restore_persisted_snapshot()
            .await
            .expect("restore persisted snapshot");

        assert_eq!(restored.last_applied_log_id, Some(test_log_id(42)));
        assert_eq!(
            restored
                .group_snapshot()
                .await
                .expect("snapshot restored state")
                .group_commit_index,
            42
        );
        crate::tests::remove_test_path(directory);
    }

    /// F12a: the persisted snapshot record and the pointer inside it use the
    /// MessagePack envelope (format epoch 2 writes nothing else), and a node
    /// restores from it.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn persisted_snapshot_restores_from_the_binary_envelope() {
        use ursula_shard::CoreId;
        use ursula_shard::RaftGroupId;
        use ursula_shard::ShardId;

        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(7),
        };
        let directory = std::env::temp_dir().join(format!(
            "ursula-persisted-binary-snapshot-{}",
            std::process::id()
        ));
        crate::tests::remove_test_path(&directory);
        std::fs::create_dir_all(&directory).expect("snapshot metadata directory");
        let metadata_path = directory.join("group-7.snapshot.json");
        let mut builder = RaftGroupSnapshotBuilder {
            apply_health: Default::default(),
            placement,
            snapshot: Ok(Arc::new(test_group_snapshot(placement, 42))),
            meta: test_snapshot_meta(42),
            current_snapshot: Arc::new(Mutex::new(None)),
            metadata_serial: Arc::default(),
            snapshot_store: default_snapshot_store(),
            metrics: None,
            build: test_build_lease().await,
            log_gauge: Arc::default(),
            log_mark: GroupLogMark::default(),
            references: Arc::default(),
            snapshot_metadata_path: Some(metadata_path.clone()),
        };
        builder.build_snapshot().await.expect("persist snapshot");

        let binary = std::fs::read(&metadata_path).expect("persisted record");
        let persisted =
            decode_snapshot_envelope::<PersistedSnapshot>(&binary).expect("decode binary record");
        SnapshotPointer::decode(&persisted.pointer_bytes).expect("binary pointer");

        let mut restored = RaftGroupStateMachine::new_with_stores_and_snapshot_install(
            placement,
            None,
            None,
            default_snapshot_store(),
            SnapshotBuildCoordinator::default(),
            SnapshotInstallCoordinator::default(),
            Some(metadata_path),
        );
        restored
            .restore_persisted_snapshot()
            .await
            .expect("restore the binary record");
        assert_eq!(restored.last_applied_log_id, Some(test_log_id(42)));
        assert_eq!(
            restored
                .group_snapshot()
                .await
                .expect("snapshot restored state")
                .group_commit_index,
            42
        );
        crate::tests::remove_test_path(directory);
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn older_snapshot_build_cannot_overwrite_a_newer_installed_pointer_or_metadata() {
        let placement = ShardPlacement {
            core_id: ursula_shard::CoreId(0),
            shard_id: ursula_shard::ShardId(0),
            raft_group_id: ursula_shard::RaftGroupId(7),
        };
        let directory = tempfile::tempdir().unwrap();
        let metadata = directory.path().join("group-7.snapshot.json");
        let store: SharedSnapshotStore =
            Arc::new(ursula_runtime::S3SnapshotStore::memory_for_tests("stale-build", 1).unwrap());
        let coordinator = SnapshotInstallCoordinator::default();
        let mut state = RaftGroupStateMachine::new_with_stores_and_snapshot_install(
            placement,
            None,
            None,
            store.clone(),
            SnapshotBuildCoordinator::default(),
            coordinator.clone(),
            Some(metadata.clone()),
        );
        state
            .engine
            .install_snapshot(test_group_snapshot(placement, 1))
            .await
            .unwrap();
        state.last_applied_log_id = Some(test_log_id(1));
        let mut old_builder = state.get_snapshot_builder().await;
        let bytes = group_snapshot_frames(Arc::new(test_group_snapshot(placement, 2)))
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .into_iter()
            .flat_map(|chunk| chunk.to_vec())
            .collect();
        let pointer = SnapshotPointer {
            snapshot_id: "newer-install".to_owned(),
            location: SnapshotLocation::Inline { bytes },
        };
        state
            .install_snapshot(
                &test_snapshot_meta(2),
                SnapshotData::new(pointer.encode_binary().unwrap()),
            )
            .await
            .unwrap();
        // Complete the upload captured at index 1 only after installing index 2.
        let returned = old_builder.build_snapshot().await.unwrap();
        assert_eq!(returned.meta.last_log_id, Some(test_log_id(2)));
        let pointer = SnapshotPointer::decode(returned.snapshot.pointer_bytes()).unwrap();
        assert_eq!(pointer.snapshot_id, "newer-install");
        // The overtaken build's upload never became current: a GC pass G
        // later deletes it (#669).
        let references = coordinator.references(7);
        assert!(!references.gc_due(Instant::now()));
        assert!(
            references.gc_due(
                Instant::now()
                    .checked_add(DEFAULT_SNAPSHOT_GC_GRACE)
                    .unwrap()
            )
        );
        let mut restored = RaftGroupStateMachine::new_with_stores_and_snapshot_install(
            placement,
            None,
            None,
            store,
            SnapshotBuildCoordinator::default(),
            SnapshotInstallCoordinator::default(),
            Some(metadata),
        );
        restored.restore_persisted_snapshot().await.unwrap();
        assert_eq!(restored.last_applied_log_id, Some(test_log_id(2)));
        assert_eq!(
            restored.group_snapshot().await.unwrap().group_commit_index,
            2
        );
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn snapshot_builder_keeps_external_snapshots_referenced_by_published_pointers() {
        use std::sync::Arc;

        use ursula_runtime::S3SnapshotStore;
        use ursula_runtime::SnapshotStore;
        use ursula_shard::CoreId;
        use ursula_shard::RaftGroupId;
        use ursula_shard::ShardId;

        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(7),
        };
        let raw_store = Arc::new(
            S3SnapshotStore::memory_for_tests(
                &format!("state-machine-snapshot-retention-{}", std::process::id()),
                1,
            )
            .expect("memory S3 snapshot store"),
        );
        let snapshot_store: SharedSnapshotStore = raw_store.clone();
        let current_snapshot = Arc::new(Mutex::new(None));

        let mut first = RaftGroupSnapshotBuilder {
            apply_health: Default::default(),
            placement,
            snapshot: Ok(Arc::new(test_group_snapshot(placement, 1))),
            meta: test_snapshot_meta(1),
            current_snapshot: current_snapshot.clone(),
            metadata_serial: Arc::default(),
            snapshot_store: snapshot_store.clone(),
            metrics: None,
            build: test_build_lease().await,
            log_gauge: Arc::default(),
            log_mark: GroupLogMark::default(),
            references: Arc::default(),
            snapshot_metadata_path: None,
        };
        let first_snapshot = first.build_snapshot().await.expect("first snapshot");
        let first_pointer = SnapshotPointer::decode(first_snapshot.snapshot.pointer_bytes())
            .expect("first pointer");

        let mut second = RaftGroupSnapshotBuilder {
            apply_health: Default::default(),
            placement,
            snapshot: Ok(Arc::new(test_group_snapshot(placement, 2))),
            meta: test_snapshot_meta(2),
            current_snapshot: current_snapshot.clone(),
            metadata_serial: Arc::default(),
            snapshot_store: snapshot_store.clone(),
            metrics: None,
            build: test_build_lease().await,
            log_gauge: Arc::default(),
            log_mark: GroupLogMark::default(),
            references: Arc::default(),
            snapshot_metadata_path: None,
        };
        let second_snapshot = second.build_snapshot().await.expect("second snapshot");
        let second_pointer = SnapshotPointer::decode(second_snapshot.snapshot.pointer_bytes())
            .expect("second pointer");

        let first_bytes = raw_store
            .download(&first_pointer.location)
            .await
            .expect("previous snapshot remains readable");
        let second_bytes = raw_store
            .download(&second_pointer.location)
            .await
            .expect("current snapshot remains readable");
        let first_group = decode_group_snapshot(&first_bytes).expect("decode first group snapshot");
        let second_group =
            decode_group_snapshot(&second_bytes).expect("decode second group snapshot");
        assert_eq!(first_group.group_commit_index, 1);
        assert_eq!(second_group.group_commit_index, 2);

        // Simulate a process restart: the published snapshot pointer survives
        // in external storage, but builder-local retired state is gone.
        let current_snapshot = Arc::new(Mutex::new(None));
        let mut third = RaftGroupSnapshotBuilder {
            apply_health: Default::default(),
            placement,
            snapshot: Ok(Arc::new(test_group_snapshot(placement, 3))),
            meta: test_snapshot_meta(3),
            current_snapshot,
            metadata_serial: Arc::default(),
            snapshot_store,
            metrics: None,
            build: test_build_lease().await,
            log_gauge: Arc::default(),
            log_mark: GroupLogMark::default(),
            references: Arc::default(),
            snapshot_metadata_path: None,
        };
        let third_snapshot = third.build_snapshot().await.expect("third snapshot");
        let third_pointer = SnapshotPointer::decode(third_snapshot.snapshot.pointer_bytes())
            .expect("third pointer");

        raw_store
            .download(&first_pointer.location)
            .await
            .expect("old published snapshot pointer remains readable");
        raw_store
            .download(&second_pointer.location)
            .await
            .expect("previous retired snapshot remains readable");
        raw_store
            .download(&third_pointer.location)
            .await
            .expect("current snapshot remains readable");
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn snapshot_builder_falls_back_to_inline_when_external_upload_fails() {
        use ursula_runtime::SnapshotStore;
        use ursula_runtime::SnapshotStoreFuture;
        use ursula_shard::CoreId;
        use ursula_shard::RaftGroupId;
        use ursula_shard::ShardId;

        #[derive(Debug)]
        struct FailingSnapshotStore;

        impl SnapshotStore for FailingSnapshotStore {
            fn upload<'a>(
                &'a self,
                _key: SnapshotKey,
                _bytes: Bytes,
            ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
                Box::pin(async move {
                    Err(ursula_runtime::SnapshotStoreError::backend(
                        ursula_runtime::SnapshotStoreOperation::Put,
                        "test",
                        opendal::Error::new(
                            opendal::ErrorKind::Unexpected,
                            "seeded upload failure",
                        ),
                    ))
                })
            }

            fn download<'a>(
                &'a self,
                _location: &'a SnapshotLocation,
            ) -> SnapshotStoreFuture<'a, Vec<u8>> {
                Box::pin(async move {
                    Err(ursula_runtime::SnapshotStoreError::backend(
                        ursula_runtime::SnapshotStoreOperation::Get,
                        "test",
                        opendal::Error::new(
                            opendal::ErrorKind::Unexpected,
                            "download should not be used",
                        ),
                    ))
                })
            }

            fn delete<'a>(
                &'a self,
                _location: &'a SnapshotLocation,
            ) -> SnapshotStoreFuture<'a, ()> {
                Box::pin(async move { Ok(()) })
            }

            ursula_runtime::inline_owned_snapshot_methods!();
        }

        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(7),
        };
        let current_snapshot = Arc::new(Mutex::new(None));
        let mut builder = RaftGroupSnapshotBuilder {
            apply_health: Default::default(),
            placement,
            snapshot: Ok(Arc::new(test_group_snapshot(placement, 3))),
            meta: test_snapshot_meta(3),
            current_snapshot: current_snapshot.clone(),
            metadata_serial: Arc::default(),
            snapshot_store: Arc::new(FailingSnapshotStore),
            metrics: None,
            build: test_build_lease().await,
            log_gauge: Arc::default(),
            log_mark: GroupLogMark::default(),
            references: Arc::default(),
            snapshot_metadata_path: None,
        };

        let snapshot = builder.build_snapshot().await.expect("inline fallback");
        let pointer =
            SnapshotPointer::decode(snapshot.snapshot.pointer_bytes()).expect("snapshot pointer");
        let SnapshotLocation::Inline { bytes } = pointer.location else {
            panic!("expected inline fallback");
        };
        let group = decode_group_snapshot(&bytes).expect("decode inline snapshot");
        assert_eq!(group.group_commit_index, 3);
        assert!(current_snapshot.lock().expect("snapshot mutex").is_some());
        // F12c: the upload attempt and the inline fallback both encoded from
        // the shared group without keeping or deep-cloning it.
        assert_eq!(Arc::strong_count(builder.snapshot.as_ref().unwrap()), 1);
    }

    #[tokio::test]
    async fn snapshot_install_coordinator_defaults_to_single_permit() {
        let coordinator = SnapshotInstallCoordinator::default();

        let permit = coordinator
            .acquire(ursula_shard::RaftGroupId(7))
            .await
            .expect("acquire install permit");
        assert_eq!(coordinator.available_permits(), 0);

        drop(permit);
        assert_eq!(coordinator.available_permits(), 1);
    }

    /// The restore refusal of a sample of each kind of store failure on the
    /// object the persisted pointer names, and of an unreadable pointer,
    /// body or metadata file. `object_refusal`'s own match is the
    /// exhaustive one.
    #[test]
    fn every_restore_failure_answers_its_refusal() {
        use ursula_runtime::SnapshotStoreConfigProblem;

        let group = ursula_shard::RaftGroupId(7);
        let key = || "snapshots/k.snap".to_owned();
        let directory = || Some("snapshots/node-1/group-7/".to_owned());
        let object = |source| SnapshotRestoreError::Object {
            raft_group_id: group,
            key: Some(key()),
            directory: directory(),
            source,
        };
        let corrupt = |check| GroupSnapshotError::ObjectCorrupt {
            raft_group_id: group,
            key: key(),
            check,
        };
        let unreadable = GroupSnapshotError::PointerUnreadable {
            raft_group_id: group,
        };
        let rows = vec![
            (
                object(SnapshotStoreError::NotFound { key: key() }),
                GroupSnapshotError::ObjectMissing {
                    raft_group_id: group,
                    key: key(),
                },
            ),
            (
                object(SnapshotStoreError::SizeMismatch {
                    key: key(),
                    expected: 2,
                    actual: 1,
                }),
                corrupt(SnapshotObjectCheck::StoredSize {
                    expected: 2,
                    actual: 1,
                }),
            ),
            (
                object(SnapshotStoreError::LogicalSizeMismatch {
                    key: key(),
                    expected: 4,
                    actual: 3,
                }),
                corrupt(SnapshotObjectCheck::LogicalSize {
                    expected: 4,
                    actual: 3,
                }),
            ),
            (
                object(SnapshotStoreError::Decompress {
                    key: key(),
                    source: io::Error::other("bad frame"),
                }),
                corrupt(SnapshotObjectCheck::Decompress),
            ),
            (
                object(SnapshotStoreError::LogicalSizeUnaddressable {
                    key: key(),
                    size_bytes: u64::MAX,
                }),
                corrupt(SnapshotObjectCheck::Decompress),
            ),
            (
                object(SnapshotStoreError::backend(
                    SnapshotStoreOperation::Head,
                    key(),
                    opendal::Error::new(opendal::ErrorKind::Unexpected, "down"),
                )),
                GroupSnapshotError::StoreUnavailable {
                    raft_group_id: group,
                    key: key(),
                    operation: SnapshotStoreOperation::Head,
                },
            ),
            (
                object(SnapshotStoreError::Io(io::Error::other("encoder"))),
                GroupSnapshotError::StoreUnavailable {
                    raft_group_id: group,
                    key: key(),
                    operation: SnapshotStoreOperation::Get,
                },
            ),
            (
                object(SnapshotStoreError::OutsideGroup {
                    raft_group_id: group,
                    key: key(),
                }),
                GroupSnapshotError::ForeignKey {
                    raft_group_id: group,
                    key: key(),
                    directory: directory(),
                },
            ),
            (
                SnapshotRestoreError::Object {
                    raft_group_id: group,
                    key: Some(key()),
                    directory: None,
                    source: SnapshotStoreError::UnsupportedLocation {
                        store: ursula_runtime::SnapshotBackend::Inline,
                        location: ursula_runtime::SnapshotLocationKind::S3,
                    },
                },
                GroupSnapshotError::ForeignKey {
                    raft_group_id: group,
                    key: key(),
                    directory: None,
                },
            ),
            (
                object(SnapshotStoreError::InvalidReference { record: key() }),
                unreadable.clone(),
            ),
            (
                object(SnapshotStoreError::Config {
                    problem: SnapshotStoreConfigProblem::NoBucket,
                }),
                unreadable.clone(),
            ),
            (
                SnapshotRestoreError::Object {
                    raft_group_id: group,
                    key: None,
                    directory: directory(),
                    source: SnapshotStoreError::NotFound { key: key() },
                },
                unreadable.clone(),
            ),
            (
                SnapshotRestoreError::Content {
                    raft_group_id: group,
                    key: Some(key()),
                    source: SnapshotStoreError::UnsupportedVersion { found: Some(4) },
                },
                corrupt(SnapshotObjectCheck::Decode),
            ),
            (
                SnapshotRestoreError::Content {
                    raft_group_id: group,
                    key: None,
                    source: SnapshotStoreError::HistoryIdZero,
                },
                unreadable.clone(),
            ),
            (
                SnapshotRestoreError::Pointer {
                    raft_group_id: group,
                    source: SnapshotStoreError::Deserialize("garbage".to_owned()),
                },
                unreadable.clone(),
            ),
            (
                SnapshotRestoreError::Metadata {
                    raft_group_id: group,
                    source: io::Error::other("fsync"),
                },
                unreadable,
            ),
        ];
        for (failure, refusal) in rows {
            let described = format!("{failure:?}");
            assert_eq!(
                GroupEngineError::from(failure),
                GroupEngineError::Infra(GroupInfraError::Snapshot(refusal)),
                "{described}"
            );
        }
        let engine = GroupEngineError::Infra(GroupInfraError::OwnerStopped);
        assert_eq!(
            GroupEngineError::from(SnapshotRestoreError::Engine(engine.clone())),
            engine,
            "an engine refusal passes through"
        );
    }

    #[tokio::test]
    async fn snapshot_install_coordinator_clamps_zero_to_one_permit() {
        let coordinator = SnapshotInstallCoordinator::new(0);

        let permit = coordinator
            .acquire(ursula_shard::RaftGroupId(7))
            .await
            .expect("acquire install permit");
        assert_eq!(coordinator.available_permits(), 0);

        drop(permit);
        assert_eq!(coordinator.available_permits(), 1);
    }

    #[cfg(not(madsim))]
    fn test_state_machine(
        build: SnapshotBuildCoordinator,
        install: SnapshotInstallCoordinator,
    ) -> RaftGroupStateMachine {
        use ursula_shard::CoreId;
        use ursula_shard::RaftGroupId;
        use ursula_shard::ShardId;

        RaftGroupStateMachine::new_with_stores_and_snapshot_install(
            ShardPlacement {
                core_id: CoreId(0),
                shard_id: ShardId(0),
                raft_group_id: RaftGroupId(7),
            },
            None,
            None,
            default_snapshot_store(),
            build,
            install,
            None,
        )
    }

    /// F12d: a policy-triggered snapshot never waits on the node-wide build
    /// permit; it defers while another group builds, and a forced one (needed
    /// for replication) still waits for the permit.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn policy_snapshot_build_defers_while_another_group_holds_the_permit() {
        let build = SnapshotBuildCoordinator::new(1);
        let mut state_machine =
            test_state_machine(build.clone(), SnapshotInstallCoordinator::default());
        let held = build.acquire().await.expect("other group's build permit");

        let deferred = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            state_machine.try_create_snapshot_builder(false),
        )
        .await
        .expect("policy build must not wait for the permit");
        assert!(deferred.is_none());

        drop(held);
        let builder = state_machine
            .try_create_snapshot_builder(false)
            .await
            .expect("free permit builds");
        assert_eq!(build.available_permits(), 0);
        drop(builder);
        assert_eq!(build.available_permits(), 1);
        assert!(
            state_machine
                .try_create_snapshot_builder(true)
                .await
                .is_some()
        );
    }

    /// The snapshot driver's permit handoff: a group handed a permit builds
    /// on it even while the node's only other permit is gone, and a group
    /// without one still defers.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn handed_off_permit_builds_instead_of_deferring() {
        let build = SnapshotBuildCoordinator::new(1);
        let mut state_machine =
            test_state_machine(build.clone(), SnapshotInstallCoordinator::default());
        let group = state_machine.placement.raft_group_id.0;
        let permit = build.acquire().await.expect("driver's build permit");
        let token = build.hand_off(group, permit).expect("no hand-off pending");
        assert!(build.handoff_pending(group, token));

        let builder = state_machine
            .try_create_snapshot_builder(false)
            .await
            .expect("a handed-off permit builds");
        assert!(!build.handoff_pending(group, token));
        assert!(!build.reclaim_handoff(group, token));
        assert_eq!(build.available_permits(), 0);
        // The next trigger without a handoff defers while this build runs.
        assert!(
            state_machine
                .try_create_snapshot_builder(false)
                .await
                .is_none()
        );
        drop(builder);
        assert_eq!(build.available_permits(), 1);

        // An unclaimed handoff is taken back.
        let token = build
            .hand_off(group, build.acquire().await.expect("permit"))
            .expect("no hand-off pending");
        assert!(build.reclaim_handoff(group, token));
        assert_eq!(build.available_permits(), 1);
    }

    /// #519: a permit handed to a group with its trigger goes back to the
    /// node's permits when the trigger fails or no build claims it in time,
    /// and stays with the build that claims it until the build ends.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_handed_off_permit_comes_back_unless_a_build_claims_it() {
        let build = SnapshotBuildCoordinator::new(2);
        let mut state_machine =
            test_state_machine(build.clone(), SnapshotInstallCoordinator::default());
        let group = state_machine.placement.raft_group_id.0;
        let never = Duration::from_secs(3_600);

        let permit = build.acquire().await.expect("driver's build permit");
        let failed = build
            .hand_off_and_trigger(group, permit, never, async {
                Err::<(), _>("the group stopped")
            })
            .await;
        assert!(matches!(
            failed,
            Handoff::TriggerFailed("the group stopped")
        ));
        assert_eq!(build.available_permits(), 2);

        // No build claims it, as when OpenRaft dropped the trigger.
        let permit = build.acquire().await.expect("driver's build permit");
        let unclaimed = build
            .hand_off_and_trigger(group, permit, Duration::from_millis(20), async {
                Ok::<(), &str>(())
            })
            .await;
        assert!(matches!(unclaimed, Handoff::Unclaimed));
        assert_eq!(build.available_permits(), 2);

        // The trigger starts the group's build, which claims the permit.
        let permit = build.acquire().await.expect("driver's build permit");
        let mut builder = None;
        let claimed = build
            .hand_off_and_trigger(group, permit, never, async {
                builder = state_machine.try_create_snapshot_builder(false).await;
                Ok::<(), &str>(())
            })
            .await;
        assert!(matches!(claimed, Handoff::Claimed));
        assert!(builder.is_some());
        assert_eq!(build.available_permits(), 1);
        drop(builder);
        assert_eq!(build.available_permits(), 2);
    }

    /// A hand-off whose caller stops waiting, during its trigger or its wait
    /// for the claim, or whose trigger panics, still gives its permit back:
    /// an operator's request that times out or whose client went away must
    /// not park a permit (#558).
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_dropped_or_panicking_handoff_gives_its_permit_back() {
        use futures_util::FutureExt as _;

        let build = SnapshotBuildCoordinator::new(2);
        let group = 7;
        let never = Duration::from_secs(3_600);
        let soon = Duration::from_millis(20);

        let permit = build.acquire().await.expect("a permit");
        let triggering = build.hand_off_and_trigger(
            group,
            permit,
            never,
            std::future::pending::<Result<(), &str>>(),
        );
        let _elapsed = tokio::time::timeout(soon, triggering)
            .await
            .expect_err("the caller stops waiting during the trigger");
        assert_eq!(build.available_permits(), 2);

        let permit = build.acquire().await.expect("a permit");
        let waiting =
            build.hand_off_and_trigger(group, permit, never, async { Ok::<(), &str>(()) });
        let _elapsed = tokio::time::timeout(soon, waiting)
            .await
            .expect_err("the caller stops waiting for the claim");
        assert_eq!(build.available_permits(), 2);

        let permit = build.acquire().await.expect("a permit");
        let panics = std::future::poll_fn(|_| -> std::task::Poll<Result<(), &str>> {
            panic!("the trigger panicked")
        });
        let _panic =
            std::panic::AssertUnwindSafe(build.hand_off_and_trigger(group, permit, never, panics))
                .catch_unwind()
                .await
                .expect_err("the trigger panics");
        assert_eq!(build.available_permits(), 2);
    }

    /// One hand-off per group at a time: a second one while the first is
    /// pending gives its permit back at once, and each caller takes back
    /// only its own permit.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_group_takes_one_handoff_at_a_time() {
        let build = SnapshotBuildCoordinator::new(2);
        let mut state_machine =
            test_state_machine(build.clone(), SnapshotInstallCoordinator::default());
        let group = state_machine.placement.raft_group_id.0;
        let never = Duration::from_secs(3_600);

        let first = build.hand_off_and_trigger(
            group,
            build.acquire().await.expect("a permit"),
            never,
            async { Ok::<(), &str>(()) },
        );
        tokio::pin!(first);
        assert!(futures_util::poll!(&mut first).is_pending());
        let second = tokio::time::timeout(
            Duration::from_secs(5),
            build.hand_off_and_trigger(
                group,
                build.acquire().await.expect("a permit"),
                never,
                async { Ok::<(), &str>(()) },
            ),
        )
        .await
        .expect("a second hand-off returns at once");
        assert!(matches!(second, Handoff::Busy));
        assert_eq!(build.available_permits(), 1);
        let builder = state_machine
            .try_create_snapshot_builder(false)
            .await
            .expect("the first hand-off's permit builds");
        assert!(matches!(first.await, Handoff::Claimed));
        assert_eq!(build.available_permits(), 1);
        drop(builder);

        // A caller takes back only its own hand-off, never one made to the
        // group since its own was claimed.
        let token = build
            .hand_off(group, build.acquire().await.expect("a permit"))
            .expect("no hand-off pending");
        let claimed_earlier = token.wrapping_sub(1);
        assert!(!build.reclaim_handoff(group, claimed_earlier));
        assert!(build.handoff_pending(group, token));
        assert!(build.reclaim_handoff(group, token));
        assert_eq!(build.available_permits(), 2);
    }

    /// #529: a wait for a group's pending hand-off ends once a build
    /// claims it or its caller takes it back, both of which wake it.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_pending_handoff_settles_when_claimed_or_taken_back() {
        let build = SnapshotBuildCoordinator::new(2);
        let mut state_machine =
            test_state_machine(build.clone(), SnapshotInstallCoordinator::default());
        let group = state_machine.placement.raft_group_id.0;

        let token = build
            .hand_off(group, build.acquire().await.expect("a permit"))
            .expect("no hand-off pending");
        let settled = build.handoff_settled(group);
        tokio::pin!(settled);
        assert!(futures_util::poll!(&mut settled).is_pending());
        assert!(build.reclaim_handoff(group, token));
        assert!(futures_util::poll!(&mut settled).is_ready());

        build
            .hand_off(group, build.acquire().await.expect("a permit"))
            .expect("no hand-off pending");
        let settled = build.handoff_settled(group);
        tokio::pin!(settled);
        assert!(futures_util::poll!(&mut settled).is_pending());
        let builder = state_machine
            .try_create_snapshot_builder(false)
            .await
            .expect("the hand-off's permit builds");
        assert!(futures_util::poll!(&mut settled).is_ready());
        // The build of the group holds its permit until it ends.
        let ended = build.build_ended(group);
        tokio::pin!(ended);
        assert!(futures_util::poll!(&mut ended).is_pending());
        drop(builder);
        assert!(futures_util::poll!(&mut ended).is_ready());
        assert_eq!(build.available_permits(), 2);
    }

    /// #529: an operator's snapshot start waits, holding no permit, while a
    /// build of its group runs, and then hands a permit over and triggers
    /// once.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_snapshot_start_waits_for_the_groups_build_without_a_permit() {
        let build = SnapshotBuildCoordinator::new(2);
        let mut state_machine =
            test_state_machine(build.clone(), SnapshotInstallCoordinator::default());
        let group = state_machine.placement.raft_group_id.0;
        let running = build.try_lease(group, 0).expect("a free permit");
        let triggers = AtomicU64::new(0);
        let start = build.start_snapshot(
            group,
            None,
            Duration::from_secs(3_600),
            || false,
            std::future::pending(),
            || async {
                triggers.fetch_add(1, Ordering::Relaxed);
                Ok::<(), std::fmt::Error>(())
            },
        );
        tokio::pin!(start);
        assert!(futures_util::poll!(&mut start).is_pending());
        assert_eq!(triggers.load(Ordering::Relaxed), 0);
        assert_eq!(build.available_permits(), 1);
        drop(running);
        assert!(futures_util::poll!(&mut start).is_pending());
        assert_eq!(triggers.load(Ordering::Relaxed), 1);
        let builder = state_machine
            .try_create_snapshot_builder(false)
            .await
            .expect("the handed-off permit builds");
        assert!(matches!(
            futures_util::poll!(&mut start),
            std::task::Poll::Ready(Ok(()))
        ));
        drop(builder);
        assert_eq!(build.available_permits(), 2);
    }

    /// #529: a start waits, holding no permit, while another caller's
    /// hand-off to its group is pending, and goes on once that hand-off is
    /// taken back.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_snapshot_start_waits_for_a_pending_handoff_without_a_permit() {
        let build = SnapshotBuildCoordinator::new(2);
        let mut state_machine =
            test_state_machine(build.clone(), SnapshotInstallCoordinator::default());
        let group = state_machine.placement.raft_group_id.0;
        let other = build
            .hand_off(group, build.acquire().await.expect("a permit"))
            .expect("no hand-off pending");
        let triggers = AtomicU64::new(0);
        let start = build.start_snapshot(
            group,
            None,
            Duration::from_secs(3_600),
            || false,
            std::future::pending(),
            || async {
                triggers.fetch_add(1, Ordering::Relaxed);
                Ok::<(), std::fmt::Error>(())
            },
        );
        tokio::pin!(start);
        assert!(futures_util::poll!(&mut start).is_pending());
        assert_eq!(triggers.load(Ordering::Relaxed), 0);
        assert_eq!(build.available_permits(), 1);
        assert!(build.reclaim_handoff(group, other));
        assert!(futures_util::poll!(&mut start).is_pending());
        assert_eq!(triggers.load(Ordering::Relaxed), 1);
        let builder = state_machine
            .try_create_snapshot_builder(false)
            .await
            .expect("the handed-off permit builds");
        assert!(matches!(
            futures_util::poll!(&mut start),
            std::task::Poll::Ready(Ok(()))
        ));
        drop(builder);
    }

    /// #561: a start that waited for its group's build needs no build of
    /// its own when that build covers the request, although OpenRaft
    /// publishes the build's snapshot only after the build released its
    /// permit: the build records what it covers before.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_snapshot_start_ends_when_the_build_it_waited_for_covers_the_request() {
        let build = SnapshotBuildCoordinator::new(2);
        let group = 7;
        let wanted = test_log_id(5);
        let mut running = build.try_lease(group, 0).expect("a free permit");
        let triggers = AtomicU64::new(0);
        let start = build.start_snapshot(
            group,
            Some(wanted),
            Duration::from_secs(3_600),
            // OpenRaft has not published the build's snapshot yet.
            || false,
            std::future::pending(),
            || async {
                triggers.fetch_add(1, Ordering::Relaxed);
                Ok::<(), std::fmt::Error>(())
            },
        );
        tokio::pin!(start);
        assert!(futures_util::poll!(&mut start).is_pending());
        running.record_built(Some(test_log_id(6)));
        drop(running);
        assert!(matches!(
            futures_util::poll!(&mut start),
            std::task::Poll::Ready(Ok(()))
        ));
        assert_eq!(triggers.load(Ordering::Relaxed), 0);
        assert_eq!(build.available_permits(), 2);
        // A later build that made an older snapshot does not move it back.
        build
            .try_lease(group, 0)
            .expect("a free permit")
            .record_built(Some(test_log_id(4)));
        assert_eq!(build.built_snapshot(group), Some(test_log_id(6)));
    }

    /// #561: a build covers a start only when its snapshot reaches what the
    /// start asks for, and a start that asks for nothing applied always
    /// triggers: `Some(_) >= None` must not count as covered.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_build_covers_a_snapshot_start_only_up_to_its_snapshot() {
        let build = SnapshotBuildCoordinator::new(2);
        let group = 7;
        build
            .try_lease(group, 0)
            .expect("a free permit")
            .record_built(Some(test_log_id(4)));
        for wanted in [Some(test_log_id(5)), None] {
            let triggers = AtomicU64::new(0);
            let start = build.start_snapshot(
                group,
                wanted,
                Duration::from_secs(3_600),
                || false,
                std::future::pending(),
                || async {
                    triggers.fetch_add(1, Ordering::Relaxed);
                    Ok::<(), std::fmt::Error>(())
                },
            );
            tokio::pin!(start);
            assert!(futures_util::poll!(&mut start).is_pending());
            assert_eq!(triggers.load(Ordering::Relaxed), 1, "{wanted:?}");
        }
        assert_eq!(build.available_permits(), 2);
    }

    /// #561: a build records its snapshot once it is durable, before it
    /// publishes the snapshot's reference and prunes retired snapshots,
    /// which can take seconds: a snapshot start that times out meanwhile
    /// still sees the snapshot that covers it. Each step stalls in turn.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_build_records_its_snapshot_before_its_housekeeping() {
        use ursula_runtime::SnapshotStore;
        use ursula_runtime::SnapshotStoreFuture;
        use ursula_shard::CoreId;
        use ursula_shard::RaftGroupId;
        use ursula_shard::ShardId;

        /// A build's housekeeping step after its snapshot is durable.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum Housekeeping {
            /// Publishing the current reference, with its retry backoff of
            /// up to 5 s.
            Publish,
            /// Pruning retired snapshots.
            Prune,
        }

        /// Inline snapshots whose `stuck` housekeeping step never ends.
        #[derive(Debug)]
        struct StuckHousekeepingStore {
            inline: SharedSnapshotStore,
            stuck: Housekeeping,
            entered: Arc<crate::rt::sync::Notify>,
        }

        impl StuckHousekeepingStore {
            fn step(&self, step: Housekeeping) -> SnapshotStoreFuture<'_, ()> {
                if step == self.stuck {
                    self.entered.notify_one();
                    Box::pin(std::future::pending())
                } else {
                    Box::pin(async { Ok(()) })
                }
            }
        }

        impl SnapshotStore for StuckHousekeepingStore {
            fn upload<'a>(
                &'a self,
                key: SnapshotKey,
                bytes: Bytes,
            ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
                self.inline.upload(key, bytes)
            }

            fn download<'a>(
                &'a self,
                location: &'a SnapshotLocation,
            ) -> SnapshotStoreFuture<'a, Vec<u8>> {
                self.inline.download(location)
            }

            fn delete<'a>(&'a self, location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()> {
                self.inline.delete(location)
            }

            fn publish_reference<'a>(
                &'a self,
                _raft_group_id: u32,
                _location: &'a SnapshotLocation,
            ) -> SnapshotStoreFuture<'a, ()> {
                self.step(Housekeeping::Publish)
            }

            fn prune_retired<'a>(
                &'a self,
                _raft_group_id: u32,
                _current: &'a SnapshotLocation,
            ) -> SnapshotStoreFuture<'a, ursula_runtime::SnapshotPrune> {
                let step = self.step(Housekeeping::Prune);
                Box::pin(async move {
                    step.await?;
                    Ok(ursula_runtime::SnapshotPrune::Unmanaged)
                })
            }

            ursula_runtime::inline_owned_snapshot_methods!();
        }

        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(7),
        };
        for stuck in [Housekeeping::Publish, Housekeeping::Prune] {
            let entered = Arc::new(crate::rt::sync::Notify::new());
            let mut builder = RaftGroupSnapshotBuilder {
                apply_health: Default::default(),
                placement,
                snapshot: Ok(Arc::new(test_group_snapshot(placement, 9))),
                meta: test_snapshot_meta(9),
                current_snapshot: Arc::new(Mutex::new(None)),
                metadata_serial: Arc::default(),
                snapshot_store: Arc::new(StuckHousekeepingStore {
                    inline: default_snapshot_store(),
                    stuck,
                    entered: Arc::clone(&entered),
                }),
                metrics: None,
                build: test_build_lease().await,
                log_gauge: Arc::default(),
                log_mark: GroupLogMark::default(),
                references: Arc::default(),
                snapshot_metadata_path: None,
            };
            let coordinator = builder.build.coordinator.clone();
            {
                let built = builder.build_snapshot();
                tokio::pin!(built);
                tokio::select! {
                    biased;
                    _built = &mut built => panic!("{stuck:?} never ends"),
                    () = entered.notified() => {}
                }
                assert_eq!(
                    coordinator.built_snapshot(7),
                    Some(test_log_id(9)),
                    "{stuck:?}"
                );
                // Still building: the group is not left out as publishing.
                assert!(!coordinator.publishing(7, None), "{stuck:?}");
            }
            drop(builder);
            // Released with a snapshot OpenRaft has not published yet.
            assert!(coordinator.publishing(7, None), "{stuck:?}");
            assert!(coordinator.publishing(7, Some(8)), "{stuck:?}");
            assert!(!coordinator.publishing(7, Some(9)), "{stuck:?}");
        }
    }

    /// #561: a group whose build released its permit with a snapshot that
    /// OpenRaft has not published yet counts as publishing, for at most
    /// [`SNAPSHOT_HANDOFF_WAIT`], and a group with no build never does.
    #[cfg(not(madsim))]
    #[tokio::test(start_paused = true)]
    async fn a_group_publishes_its_build_for_a_bounded_while() {
        let build = SnapshotBuildCoordinator::new(1);
        let mut running = build.try_lease(7, 0).expect("a free permit");
        running.record_built(Some(test_log_id(5)));
        assert!(!build.publishing(7, Some(4)));
        drop(running);
        assert!(build.publishing(7, Some(4)));
        assert!(!build.publishing(7, Some(5)));
        assert!(!build.publishing(8, None));
        tokio::time::advance(
            SNAPSHOT_HANDOFF_WAIT
                .checked_sub(Duration::from_millis(1))
                .expect("longer than a millisecond"),
        )
        .await;
        assert!(build.publishing(7, Some(4)));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(!build.publishing(7, Some(4)));
        // A build that recorded nothing leaves the record as it was.
        drop(build.try_lease(7, 0).expect("a free permit"));
        assert!(!build.publishing(7, Some(4)));
    }

    /// #561: the snapshot driver counts a group whose build released its
    /// permit, but whose snapshot OpenRaft has not published yet, as
    /// building with nothing to free, for at most [`SNAPSHOT_HANDOFF_WAIT`],
    /// and leaves the log a running build will still free as it was.
    #[cfg(not(madsim))]
    #[tokio::test(start_paused = true)]
    async fn the_driver_counts_a_publishing_group_as_building_for_a_bounded_while() {
        let build = SnapshotBuildCoordinator::new(2);
        build
            .try_lease(7, 0)
            .expect("a free permit")
            .record_built(Some(test_log_id(5)));
        // Group 8 published an earlier build and builds again.
        build
            .try_lease(8, 0)
            .expect("a free permit")
            .record_built(Some(test_log_id(3)));
        let _running = build.try_lease(8, 300).expect("a free permit");
        let building = |published: [(u32, Option<u64>); 3]| {
            let mut building = build.builds_in_flight();
            build.add_publishing(&mut building, published);
            building
        };
        assert_eq!(
            building([(7, Some(4)), (8, None), (9, None)]),
            BTreeMap::from([(7, 0), (8, 300)])
        );
        // Published: the group is planned again.
        assert_eq!(
            building([(7, Some(5)), (8, None), (9, None)]),
            BTreeMap::from([(8, 300)])
        );
        tokio::time::advance(
            SNAPSHOT_HANDOFF_WAIT
                .checked_sub(Duration::from_millis(1))
                .expect("longer than a millisecond"),
        )
        .await;
        assert_eq!(
            building([(7, Some(4)), (8, None), (9, None)]),
            BTreeMap::from([(7, 0), (8, 300)])
        );
        // Unpublished all the same once the wait passed: OpenRaft no longer
        // counts the build, so the group is planned again.
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(
            building([(7, Some(4)), (8, None), (9, None)]),
            BTreeMap::from([(8, 300)])
        );
    }

    /// #561: a group is busy while a permit handed to it is pending or a
    /// build of it holds a permit, which a hand-off to it then answers.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_group_is_busy_while_a_hand_off_to_it_is_pending_or_it_builds() {
        let build = SnapshotBuildCoordinator::new(2);
        assert!(!build.busy(7));
        let token = build
            .hand_off(7, build.acquire().await.expect("a permit"))
            .expect("not busy");
        assert!(build.busy(7));
        assert!(!build.busy(8));
        assert!(build.reclaim_handoff(7, token));
        assert!(!build.busy(7));
        let running = build.try_lease(7, 0).expect("a free permit");
        assert!(build.busy(7));
        let busy = build.hand_off(7, build.acquire().await.expect("a permit"));
        assert!(matches!(busy, Err(ref _permit)), "{busy:?}");
        drop((busy, running));
        assert!(!build.busy(7));
        assert_eq!(build.available_permits(), 2);
    }

    /// Raft metrics of a replica that applied up to `applied` and purged up
    /// to `purged`.
    #[cfg(not(madsim))]
    fn purge_metrics(applied: u64, purged: Option<u64>) -> RaftMetrics<UrsulaRaftTypeConfig> {
        let mut metrics = RaftMetrics::new_initial(1);
        metrics.last_applied = Some(test_log_id(applied));
        metrics.purged = purged.map(test_log_id);
        metrics
    }

    /// #669: a purge through a snapshot starts only while its group's
    /// snapshot work is idle, and holds every build of the group, OpenRaft's
    /// own, the snapshot driver's and an operator's hand-off, until the log
    /// is purged through it or the group applied `keep` entries past it.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_purge_holds_its_groups_builds_until_it_lands() {
        use openraft::rt::WatchSender;
        use openraft::type_config::TypeConfigExt;

        let build = SnapshotBuildCoordinator::new(2);
        build.log_gauge(7).set_log_keep(4);
        let (sender, metrics) = UrsulaRaftTypeConfig::watch_channel(purge_metrics(10, None));
        assert_eq!(build.work(7, Some(10)), GroupSnapshotWork::Idle);
        // Busy groups take no purge.
        let running = build.try_lease(8, 0).expect("a free permit");
        assert!(matches!(
            build.hold_for_purge(8, Some(10), 10, metrics.clone()),
            Err(PurgeBusy {
                raft_group_id: 8,
                work: GroupSnapshotWork::Building
            })
        ));
        drop(running);
        let token = build
            .hand_off(8, build.acquire().await.expect("a permit"))
            .expect("not busy");
        assert!(matches!(
            build.hold_for_purge(8, Some(10), 10, metrics.clone()),
            Err(PurgeBusy {
                raft_group_id: 8,
                work: GroupSnapshotWork::HandedOff
            })
        ));
        assert!(build.reclaim_handoff(8, token));

        build
            .hold_for_purge(7, Some(10), 10, metrics.clone())
            .expect("an idle group")
            .keep();
        assert_eq!(build.work(7, Some(10)), GroupSnapshotWork::Purging {
            through: 10
        });
        assert!(matches!(
            build.hold_for_purge(7, Some(10), 12, metrics.clone()),
            Err(PurgeBusy {
                raft_group_id: 7,
                work: GroupSnapshotWork::Purging { through: 10 }
            })
        ));
        // No build of the group starts meanwhile: OpenRaft's defers, and a
        // hand-off answers busy.
        assert!(build.busy(7));
        assert!(matches!(
            build.lease_for_build(7, 0),
            Err(BuildDeferred::PurgePending { through: 10 })
        ));
        let busy = build.hand_off(7, build.acquire().await.expect("a permit"));
        assert!(matches!(busy, Err(ref _permit)), "{busy:?}");
        drop(busy);
        let mut building = BTreeMap::new();
        build.add_purging(&mut building);
        assert_eq!(building, BTreeMap::from([(7, 0)]));
        // Other groups build.
        drop(build.lease_for_build(9, 0).expect("another group builds"));
        // A hold dropped before its purge was triggered lifts.
        let dropped = build
            .hold_for_purge(9, Some(5), 5, metrics.clone())
            .expect("an idle group");
        assert_eq!(build.work(9, Some(5)), GroupSnapshotWork::Purging {
            through: 5
        });
        drop(dropped);
        assert_eq!(build.work(9, Some(5)), GroupSnapshotWork::Idle);

        // Three entries past the purge are not enough to cover it...
        sender.send(purge_metrics(13, None)).expect("a receiver");
        assert_eq!(build.work(7, Some(10)), GroupSnapshotWork::Purging {
            through: 10
        });
        // ...four are: any later build's own purge reaches it.
        sender.send(purge_metrics(14, None)).expect("a receiver");
        assert_eq!(build.work(7, Some(10)), GroupSnapshotWork::Idle);
        let mut building = BTreeMap::new();
        build.add_purging(&mut building);
        assert!(building.is_empty());

        // A purged log lands it too.
        build
            .hold_for_purge(7, Some(20), 20, metrics.clone())
            .expect("an idle group")
            .keep();
        assert!(build.busy(7));
        sender
            .send(purge_metrics(20, Some(20)))
            .expect("a receiver");
        assert!(!build.busy(7));
        drop(build.lease_for_build(7, 0).expect("the group builds again"));
        assert_eq!(build.available_permits(), 2);
    }

    /// #669: a purge starts neither while the group's latest build made a
    /// snapshot OpenRaft has not published, however long ago the build
    /// ended, nor while a build runs.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_purge_waits_for_openraft_to_publish_the_groups_build() {
        use openraft::type_config::TypeConfigExt;

        let build = SnapshotBuildCoordinator::new(2);
        let (_sender, metrics) = UrsulaRaftTypeConfig::watch_channel(purge_metrics(12, None));
        let mut lease = build.try_lease(7, 0).expect("a free permit");
        lease.record_built(Some(test_log_id(12)));
        drop(lease);
        assert_eq!(build.work(7, Some(10)), GroupSnapshotWork::Unpublished);
        assert!(matches!(
            build.hold_for_purge(7, Some(10), 10, metrics.clone()),
            Err(PurgeBusy {
                raft_group_id: 7,
                work: GroupSnapshotWork::Unpublished
            })
        ));
        // A hand-off is not refused for it: OpenRaft drops the trigger.
        assert!(!build.busy(7));
        assert_eq!(build.work(7, Some(12)), GroupSnapshotWork::Idle);
        build
            .hold_for_purge(7, Some(12), 12, metrics)
            .expect("published")
            .keep();
    }

    /// #669: an operator's snapshot start waits while a purge through a
    /// snapshot holds its group, holding no permit, and triggers once the
    /// purge landed.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_snapshot_start_waits_for_a_purge_to_land() {
        use openraft::rt::WatchSender;
        use openraft::type_config::TypeConfigExt;

        let build = SnapshotBuildCoordinator::new(2);
        let (sender, metrics) = UrsulaRaftTypeConfig::watch_channel(purge_metrics(10, None));
        build
            .hold_for_purge(7, Some(10), 10, metrics)
            .expect("an idle group")
            .keep();
        let triggers = AtomicU64::new(0);
        let start = build.start_snapshot(
            7,
            None,
            Duration::from_secs(3_600),
            || false,
            std::future::pending(),
            || async {
                triggers.fetch_add(1, Ordering::Relaxed);
                Ok::<(), std::fmt::Error>(())
            },
        );
        tokio::pin!(start);
        assert!(futures_util::poll!(&mut start).is_pending());
        assert_eq!(triggers.load(Ordering::Relaxed), 0);
        assert_eq!(build.available_permits(), 2);
        sender
            .send(purge_metrics(10, Some(10)))
            .expect("a receiver");
        assert!(futures_util::poll!(&mut start).is_pending());
        assert_eq!(triggers.load(Ordering::Relaxed), 1);
        let claimed = build.lease_for_build(7, 0).expect("the handed-off permit");
        assert!(matches!(
            futures_util::poll!(&mut start),
            std::task::Poll::Ready(Ok(()))
        ));
        drop(claimed);
        assert_eq!(build.available_permits(), 2);
    }

    /// #669: a purge a stopped Raft held its group for releases the hold
    /// of the group's next incarnation.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_stopped_raft_releases_its_purge_hold() {
        use openraft::type_config::TypeConfigExt;

        let build = SnapshotBuildCoordinator::new(1);
        let (sender, metrics) = UrsulaRaftTypeConfig::watch_channel(purge_metrics(10, None));
        build
            .hold_for_purge(7, Some(10), 10, metrics)
            .expect("an idle group")
            .keep();
        let landed = build.purge_landed(7);
        tokio::pin!(landed);
        assert!(futures_util::poll!(&mut landed).is_pending());
        drop(sender);
        assert!(futures_util::poll!(&mut landed).is_ready());
        assert_eq!(build.work(7, Some(10)), GroupSnapshotWork::Idle);
        build.release_purge(7);
    }

    /// #669 review: a wait on a hold whose Raft stopped lifts only that
    /// hold, never the hold the group's next incarnation took, and goes on
    /// to wait for that one.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_stale_purge_wait_leaves_the_next_hold() {
        use openraft::rt::WatchSender;
        use openraft::type_config::TypeConfigExt;

        let build = SnapshotBuildCoordinator::new(1);
        let (old_sender, old) = UrsulaRaftTypeConfig::watch_channel(purge_metrics(10, None));
        build
            .hold_for_purge(7, Some(10), 10, old)
            .expect("an idle group")
            .keep();
        let landed = build.purge_landed(7);
        tokio::pin!(landed);
        assert!(futures_util::poll!(&mut landed).is_pending());
        // The group's Raft restarts and its new incarnation holds again.
        build.release_purge(7);
        let (sender, metrics) = UrsulaRaftTypeConfig::watch_channel(purge_metrics(10, None));
        build
            .hold_for_purge(7, Some(10), 10, metrics)
            .expect("an idle group")
            .keep();
        drop(old_sender);
        assert!(futures_util::poll!(&mut landed).is_pending());
        assert_eq!(build.work(7, Some(10)), GroupSnapshotWork::Purging {
            through: 10
        });
        sender
            .send(purge_metrics(10, Some(10)))
            .expect("a receiver");
        assert!(futures_util::poll!(&mut landed).is_ready());
        assert_eq!(build.work(7, Some(10)), GroupSnapshotWork::Idle);
    }

    /// #561: a triggered build takes the permit handed to its group before a
    /// free one, and a free one when none was handed over.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_triggered_build_takes_its_handed_off_permit_first() {
        let build = SnapshotBuildCoordinator::new(2);
        let token = build
            .hand_off(7, build.acquire().await.expect("a permit"))
            .expect("not busy");
        let claimed = build.lease_for_build(7, 0).expect("the handed-off permit");
        assert!(!build.handoff_pending(7, token));
        assert_eq!(build.available_permits(), 1);
        let free = build.lease_for_build(8, 0).expect("a free permit");
        assert_eq!(build.available_permits(), 0);
        assert!(matches!(
            build.lease_for_build(9, 0),
            Err(BuildDeferred::NoPermit)
        ));
        drop((claimed, free));
        assert_eq!(build.available_permits(), 2);
    }

    /// #561: a start ends as soon as a snapshot covering the request is
    /// published, in whatever round it is: here while it waits for the claim
    /// of a trigger that OpenRaft dropped, still counting the build that
    /// made that snapshot. Its permit comes back at once.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_snapshot_start_ends_once_a_covering_snapshot_is_published() {
        let build = SnapshotBuildCoordinator::new(2);
        let (publish, published) = crate::rt::sync::watch::channel(false);
        let triggers = AtomicU64::new(0);
        let start = build.start_snapshot(
            7,
            None,
            Duration::from_secs(3_600),
            || *published.borrow(),
            async {
                let mut published = published.clone();
                let _published = published.wait_for(|published| *published).await;
            },
            || async {
                triggers.fetch_add(1, Ordering::Relaxed);
                Ok::<(), std::fmt::Error>(())
            },
        );
        tokio::pin!(start);
        assert!(futures_util::poll!(&mut start).is_pending());
        assert_eq!(triggers.load(Ordering::Relaxed), 1);
        assert_eq!(build.available_permits(), 1);
        publish.send_replace(true);
        assert!(matches!(
            futures_util::poll!(&mut start),
            std::task::Poll::Ready(Ok(()))
        ));
        assert_eq!(build.available_permits(), 2);
    }

    /// #561: a start rounds again, holding no permit, when a build of its
    /// group started while it waited for its permit, as when the snapshot
    /// driver took a permit first and started the group. A hand-off then
    /// would sit idle, since OpenRaft drops a trigger while the group builds
    /// ([`Handoff::Busy`]).
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_snapshot_start_rounds_again_when_its_group_builds_once_it_has_a_permit() {
        let build = SnapshotBuildCoordinator::new(3);
        let mut state_machine =
            test_state_machine(build.clone(), SnapshotInstallCoordinator::default());
        let group = state_machine.placement.raft_group_id.0;
        let first = build.try_lease(1, 0).expect("a free permit");
        let second = build.try_lease(2, 0).expect("a free permit");
        let third = build.try_lease(3, 0).expect("a free permit");
        let triggers = AtomicU64::new(0);
        let start = build.start_snapshot(
            group,
            None,
            Duration::from_secs(3_600),
            || false,
            std::future::pending(),
            || async {
                triggers.fetch_add(1, Ordering::Relaxed);
                Ok::<(), std::fmt::Error>(())
            },
        );
        tokio::pin!(start);
        assert!(futures_util::poll!(&mut start).is_pending());
        // The start waits for a permit. The first that comes free is its,
        // and the driver takes the next and starts the group.
        drop(first);
        drop(second);
        let driver = build.acquire().await.expect("a permit");
        let token = build.hand_off(group, driver).expect("no hand-off pending");
        let running = state_machine
            .try_create_snapshot_builder(false)
            .await
            .expect("the driver's permit builds");
        assert!(!build.handoff_pending(group, token));
        assert!(futures_util::poll!(&mut start).is_pending());
        assert_eq!(triggers.load(Ordering::Relaxed), 0);
        assert_eq!(build.available_permits(), 1);
        // Once that build ended, the next round triggers.
        drop(running);
        assert!(futures_util::poll!(&mut start).is_pending());
        assert_eq!(triggers.load(Ordering::Relaxed), 1);
        let builder = state_machine
            .try_create_snapshot_builder(false)
            .await
            .expect("the start's permit builds");
        assert!(matches!(
            futures_util::poll!(&mut start),
            std::task::Poll::Ready(Ok(()))
        ));
        drop((builder, third));
        assert_eq!(build.available_permits(), 3);
    }

    /// #561: a start that waited for its permit triggers nothing when a
    /// snapshot covering the request came meanwhile, and its permit goes
    /// back.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_snapshot_start_asks_again_whether_it_is_covered_once_it_has_a_permit() {
        let build = SnapshotBuildCoordinator::new(1);
        let other = build.try_lease(1, 0).expect("a free permit");
        let covered = AtomicBool::new(false);
        let triggers = AtomicU64::new(0);
        let start = build.start_snapshot(
            7,
            None,
            Duration::from_secs(3_600),
            || covered.load(Ordering::Relaxed),
            std::future::pending(),
            || async {
                triggers.fetch_add(1, Ordering::Relaxed);
                Ok::<(), std::fmt::Error>(())
            },
        );
        tokio::pin!(start);
        assert!(futures_util::poll!(&mut start).is_pending());
        covered.store(true, Ordering::Relaxed);
        drop(other);
        assert!(matches!(
            futures_util::poll!(&mut start),
            std::task::Poll::Ready(Ok(()))
        ));
        assert_eq!(triggers.load(Ordering::Relaxed), 0);
        assert_eq!(build.available_permits(), 1);
    }

    /// #529: a trigger that went unclaimed starts the next round, which
    /// triggers again, and the round after it finds a snapshot covering the
    /// request. The permit comes back.
    #[cfg(not(madsim))]
    #[tokio::test(start_paused = true)]
    async fn a_snapshot_start_tries_again_after_an_unclaimed_trigger() {
        let build = SnapshotBuildCoordinator::new(2);
        let triggers = AtomicU64::new(0);
        let started = build
            .start_snapshot(
                7,
                None,
                Duration::from_secs(60),
                || triggers.load(Ordering::Relaxed) >= 2,
                std::future::pending(),
                || async {
                    triggers.fetch_add(1, Ordering::Relaxed);
                    Ok::<(), std::fmt::Error>(())
                },
            )
            .await;
        assert!(matches!(started, Ok(())));
        assert_eq!(triggers.load(Ordering::Relaxed), 2);
        assert_eq!(build.available_permits(), 2);
    }

    /// #529: a start that no build could begin within its wait fails as not
    /// started, unless a snapshot covers the request by then, and one whose
    /// trigger failed fails with that failure, its permit back either way.
    #[cfg(not(madsim))]
    #[tokio::test(start_paused = true)]
    async fn a_snapshot_start_fails_as_not_started_or_as_its_trigger() {
        let build = SnapshotBuildCoordinator::new(2);
        let running = build.try_lease(7, 0).expect("a free permit");
        let late = build
            .start_snapshot(
                7,
                None,
                Duration::from_secs(10),
                || false,
                std::future::pending(),
                || async { Ok::<(), std::fmt::Error>(()) },
            )
            .await;
        assert!(matches!(
            late,
            Err(SnapshotStartError::NotStarted {
                raft_group_id: 7,
                ..
            })
        ));
        assert_eq!(build.available_permits(), 1);
        // Covered by the time the wait ran out, with nothing to wake it.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let covered_late = build
            .start_snapshot(
                7,
                None,
                Duration::from_secs(10),
                || tokio::time::Instant::now() >= deadline,
                std::future::pending(),
                || async { Ok::<(), std::fmt::Error>(()) },
            )
            .await;
        assert!(matches!(covered_late, Ok(())));
        drop(running);
        let failed = build
            .start_snapshot(
                7,
                None,
                Duration::from_secs(10),
                || false,
                std::future::pending(),
                || async { Err(std::fmt::Error) },
            )
            .await;
        assert!(matches!(
            failed,
            Err(SnapshotStartError::Trigger {
                raft_group_id: 7,
                ..
            })
        ));
        assert_eq!(build.available_permits(), 2);
    }

    /// #529: a start dropped while its own hand-off is pending, because its
    /// request timed out or its client went away, gives its permit back.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_dropped_snapshot_start_gives_its_permit_back() {
        let build = SnapshotBuildCoordinator::new(2);
        let triggers = AtomicU64::new(0);
        let mut start = Box::pin(build.start_snapshot(
            7,
            None,
            Duration::from_secs(3_600),
            || false,
            std::future::pending(),
            || async {
                triggers.fetch_add(1, Ordering::Relaxed);
                Ok::<(), std::fmt::Error>(())
            },
        ));
        assert!(futures_util::poll!(&mut start).is_pending());
        assert_eq!(triggers.load(Ordering::Relaxed), 1);
        assert_eq!(build.available_permits(), 1);
        drop(start);
        assert_eq!(build.available_permits(), 2);
    }

    /// #519: a build counts in flight, with the log it will still free,
    /// from the moment it claims its permit until its builder is dropped,
    /// and the log it covers stops counting once it recorded its snapshot.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_build_counts_in_flight_while_it_holds_its_permit() {
        let build = SnapshotBuildCoordinator::new(2);
        let mut state_machine =
            test_state_machine(build.clone(), SnapshotInstallCoordinator::default());
        let group = state_machine.placement.raft_group_id.0;
        let gauge = build.log_gauge(group);
        gauge.record_applied(300);
        assert!(build.builds_in_flight().is_empty());
        assert_eq!(build.node_log_bytes(), 300);

        let token = build
            .hand_off(group, build.acquire().await.expect("driver's build permit"))
            .expect("no hand-off pending");
        let builder = state_machine
            .try_create_snapshot_builder(false)
            .await
            .expect("a handed-off permit builds");
        assert!(!build.handoff_pending(group, token));
        assert_eq!(build.builds_in_flight(), BTreeMap::from([(group, 300)]));
        // Log applied after the capture is not this build's to free.
        gauge.record_applied(50);
        assert_eq!(build.builds_in_flight(), BTreeMap::from([(group, 300)]));
        assert_eq!(build.node_log_bytes(), 350);
        // Once the build recorded its snapshot it frees nothing more, while
        // it still holds its permit to publish the snapshot's reference.
        gauge.record_snapshot(builder.log_mark, 10, 0);
        assert_eq!(build.builds_in_flight(), BTreeMap::from([(group, 0)]));
        assert_eq!(build.node_log_bytes(), 50);
        drop(builder);
        assert!(build.builds_in_flight().is_empty());
        assert_eq!(build.available_permits(), 2);
    }

    /// #519: the snapshot driver waits on the build registry, not a timer,
    /// for the group it triggered to claim its permit.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_claim_wakes_the_driver_waiting_for_it() {
        let build = SnapshotBuildCoordinator::new(2);
        let mut state_machine =
            test_state_machine(build.clone(), SnapshotInstallCoordinator::default());
        let group = state_machine.placement.raft_group_id.0;
        build.log_gauge(group).record_applied(300);
        let token = build
            .hand_off(group, build.acquire().await.expect("driver's build permit"))
            .expect("no hand-off pending");
        let claimed = build.handoff_claimed(group, token);
        tokio::pin!(claimed);
        assert!(futures_util::poll!(&mut claimed).is_pending());
        let builder = state_machine
            .try_create_snapshot_builder(false)
            .await
            .expect("a handed-off permit builds");
        // No timer wakes the wait: the claim does, so the next poll ends it.
        assert!(futures_util::poll!(&mut claimed).is_ready());
        assert!(!build.handoff_pending(group, token));
        drop(builder);
        // Nothing pending: the wait returns at once.
        tokio::time::timeout(Duration::from_secs(5), build.handoff_claimed(group, token))
            .await
            .expect("no handoff to wait for");
    }

    /// #519: the snapshot driver paces its builds below a limit under the
    /// node's permits. It waits until fewer builds run than the limit asks
    /// for, asks again whenever a build ends and on every poll, and stops
    /// waiting when the limit says so.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn acquire_below_waits_until_fewer_builds_run_than_the_limit_asks() {
        let build = SnapshotBuildCoordinator::new(4);
        assert_eq!(build.max_concurrency(), 4);
        let first = build.try_lease(1, 0).expect("a free permit");
        let second = build.try_lease(2, 0).expect("a free permit");
        assert_eq!(build.running_builds(), 2);
        let wait = Duration::from_secs(5);
        let never = Duration::from_secs(3_600);

        // Two builds run: a third may start under a limit of three.
        let third = tokio::time::timeout(wait, build.acquire_below(never, || Some(3)))
            .await
            .expect("a free slot")
            .expect("the registry stays open")
            .expect("a permit");
        // The driver's own permit is not a build until a group claims it.
        assert_eq!(build.running_builds(), 2);
        drop(third);

        // Under a limit of two it waits, and a build that ends lets it go
        // without waiting for a poll.
        let paced = build.acquire_below(never, || Some(2));
        tokio::pin!(paced);
        assert!(futures_util::poll!(&mut paced).is_pending());
        drop(first);
        let permit = tokio::time::timeout(wait, paced)
            .await
            .expect("a slot frees once a build ends")
            .expect("the registry stays open")
            .expect("a permit");
        assert_eq!(build.available_permits(), 2);
        drop(permit);

        // The limit is asked again on every poll, so a limit that rises
        // with no build ending lets the wait go too.
        let limit = AtomicU64::new(1);
        let rising = build.acquire_below(Duration::from_millis(10), || {
            Some(usize::try_from(limit.load(Ordering::Relaxed)).unwrap())
        });
        tokio::pin!(rising);
        assert!(futures_util::poll!(&mut rising).is_pending());
        limit.store(2, Ordering::Relaxed);
        let permit = tokio::time::timeout(wait, rising)
            .await
            .expect("a poll sees the higher limit")
            .expect("the registry stays open")
            .expect("a permit");
        drop(permit);
        // A limit that says stop ends the wait without a permit.
        let stopped = tokio::time::timeout(wait, build.acquire_below(never, || None))
            .await
            .expect("no wait")
            .expect("the registry stays open");
        assert!(stopped.is_none());

        drop(second);
        assert_eq!(build.available_permits(), 4);
        assert_eq!(build.running_builds(), 0);
        // The node's permits still bound every build.
        assert_eq!(SnapshotBuildCoordinator::new(0).max_concurrency(), 1);
    }

    #[test]
    fn log_pressure_uses_hysteresis() {
        let build = SnapshotBuildCoordinator::new(1);
        assert_eq!(build.observe_log_bytes(150, 200, 100), None);
        assert_eq!(build.observe_log_bytes(201, 200, 100), Some(true));
        assert!(build.log_pressured());
        assert_eq!(build.observe_log_bytes(150, 200, 100), None);
        assert!(build.log_pressured());
        assert_eq!(build.observe_log_bytes(99, 200, 100), Some(false));
        assert!(!build.log_pressured());
        // A coordinator rebuilt with another concurrency shares the flag.
        let resized = build.with_max_concurrency(2);
        assert_eq!(resized.observe_log_bytes(500, 200, 100), Some(true));
        assert!(build.log_pressured());
    }

    /// F12c: installing an inline snapshot decodes it exactly once.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn inline_snapshot_install_decodes_once() {
        use crate::snapshot_codec::decode_calls_on_this_thread;

        let mut state_machine = test_state_machine(
            SnapshotBuildCoordinator::default(),
            SnapshotInstallCoordinator::default(),
        );
        let placement = state_machine.placement;
        let bytes = group_snapshot_frames(Arc::new(test_group_snapshot(placement, 9)))
            .collect::<Result<Vec<_>, _>>()
            .expect("encode group snapshot")
            .concat();
        let meta = test_snapshot_meta(9);
        let pointer = SnapshotPointer {
            snapshot_id: meta.snapshot_id.clone(),
            location: SnapshotLocation::Inline { bytes },
        };
        let before = decode_calls_on_this_thread();
        state_machine
            .install_snapshot(
                &meta,
                SnapshotData::new(pointer.encode_binary().expect("pointer")),
            )
            .await
            .expect("install inline snapshot");
        assert_eq!(decode_calls_on_this_thread() - before, 1);
        assert_eq!(
            state_machine
                .group_snapshot()
                .await
                .expect("installed group")
                .group_commit_index,
            9
        );
    }
}
