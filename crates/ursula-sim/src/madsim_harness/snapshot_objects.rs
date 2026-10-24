//! Snapshot objects stay owned (#687), on journal-backed replicas that run
//! the production snapshot driver and write their snapshot objects to the
//! faulty in-memory store ([`JournalCluster`] with [`set_snapshot_store`]).
//!
//! Scenario `snapshot-objects-stay-owned`, invariants I1 to I5 of
//! [`super::snapshot_checks`]. I1 to I3 are checked after every action and
//! every [`STEP`] of a wait, I5 at three quiet points and I4 once over the
//! bound. A run:
//!
//! - A: a seeded node leads both groups, a stream whose records carry a
//!   marker goes into group 0, and every node snapshots both groups.
//! - B: a build storm. Rounds of appends and operator snapshots on every
//!   node, while the drivers' GC passes run, with late and lost PUTs at the
//!   seed's rates, stale LISTs on even seeds and collided retries on every
//!   third seed.
//! - C: a lagging follower. The leader of group 0 is cut off from a
//!   follower, appends, snapshots and purges its log, and heals. The
//!   follower's GET of the sent object is held at its store's gate while the
//!   leader builds again, so the pass after that build finds the sent object
//!   replaced. When the send ends, the pass the leader then owes races a
//!   build in one forced order: the build's PUT acts, the pass lists and
//!   reads the holds, the build commits, then the pass deletes. Then the
//!   follower lags on
//!   group 1 too, and for a while the leaders append and build every 50 ms.
//! - Q1: a quiet point (I5).
//! - D: a wiped follower. It loses its disk, the leaders purge their logs,
//!   and it installs both groups while the leaders keep building.
//! - E: every node restarts in a seeded order, one of them after a power
//!   loss. Its restores read the objects its records name.
//! - Q2: a quiet point, after the restores.
//! - F: the marked stream is deleted and every node snapshots group 0: within
//!   the bound no object that no node names holds the marker (I4).
//! - Q3: a final quiet point, and every acknowledged write reads back.
//!
//! The run ends with [`SimEvent::SnapshotObjectsOwned`], which counts the
//! store's requests and digests its log, so a replay compares the requests.
//!
//! The negative controls ([`Controls`]) each turn off one defence of #687.
//! Every control breaks on every seed. A lease after the PUT and a read of
//! the holds before the LIST break through the forced race of phase C.

use std::time::Duration;

use ursula_config::WalFsync;
use ursula_raft::JournalTuning;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::DeleteStreamRequest;
use ursula_runtime::GroupEngine;
use ursula_runtime::snapshot_harness::Gate;
use ursula_runtime::snapshot_harness::GateEvent;
use ursula_runtime::snapshot_harness::GatePass;
use ursula_runtime::snapshot_harness::PerMille;
use ursula_runtime::snapshot_harness::SnapshotStoreEvent;
use ursula_runtime::snapshot_harness::SnapshotStoreFaults;
use ursula_runtime::snapshot_harness::SnapshotStoreGates;
use ursula_runtime::snapshot_harness::StoreOp;
use ursula_runtime::snapshot_harness::contains_marker;
use ursula_runtime::snapshot_harness::is_owned_by;
use ursula_shard::BucketStreamId;
use ursula_stream::sim_controls::Defence;
use ursula_stream::sim_controls::set_off;

use super::SimEvent;
use super::SimTrace;
use super::SplitMix64;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::journal_cluster::DriverPlan;
use super::journal_cluster::JOURNAL_GROUPS;
use super::journal_cluster::JournalCluster;
use super::journal_cluster::NODES;
use super::journal_cluster::SnapshotStorePlan;
use super::journal_cluster::group_placement;
use super::journal_cluster::lead_every_group;
use super::journal_cluster::metrics;
use super::journal_cluster::set_snapshot_drivers;
use super::journal_cluster::set_snapshot_store;
use super::journal_cluster::wait_leader;
use super::snapshot_checks::STEP;
use super::snapshot_checks::SnapshotChecks;

/// The drivers every node runs: a pass every 100 ms, four entries kept
/// below a snapshot, and no removal due on its age within a run.
const DRIVERS: DriverPlan = DriverPlan {
    max_age: Duration::from_secs(12),
    keep: 4,
    interval: Duration::from_millis(100),
    permits: 1,
};
/// `raft.install_snapshot_timeout` of every node: the wait of a group's
/// second GC pass after a write that failed or a restore. A late PUT lands
/// within four [`MAX_DELAY`]s of acting, well inside it.
const INSTALL_TIMEOUT: Duration = Duration::from_secs(1);
/// The longest delay of a store request before and after it acts.
const MAX_DELAY: Duration = Duration::from_millis(20);
/// How young a change a stale LIST may leave out or repeat.
const STALE_FOR: Duration = Duration::from_millis(80);
/// I4's bound: an install timeout, two driver intervals, the stale LISTs'
/// reach and six request delays: the second pass of a write, the driver
/// tick that runs it, and its LIST and DELETE.
const BOUND: Duration = Duration::from_millis(1_400);
/// The group of the marked stream.
const GROUP: u32 = 0;
/// The bytes every record of the marked stream carries.
const MARKER: &[u8] = b"snapshot-objects-marker-687;";
/// How long the leaders keep building while a follower installs.
const BUILDING: Duration = Duration::from_millis(1_500);
/// The pause between those builds.
const BUILD_EVERY: Duration = Duration::from_millis(50);
/// How long a run waits for one of its steps before it gives up.
const PATIENCE: Duration = Duration::from_secs(5);
/// Mixed into the seed of a run's draws.
const SALT: u64 = 0x736e_6170_6f62_6a73;

fn marked_stream() -> BucketStreamId {
    BucketStreamId::new("owned", "marked")
}

/// The faults outside the quiet points: request delays of up to
/// [`MAX_DELAY`] and the seed's rates of late and lost PUTs.
fn base_faults(seed: u64) -> SnapshotStoreFaults {
    SnapshotStoreFaults::from_seed(seed, MAX_DELAY)
}

/// The build storm's: stale LISTs on even seeds, and PUTs that land and
/// answer a name collision on every third seed.
fn storm_faults(seed: u64) -> SnapshotStoreFaults {
    let mut faults = base_faults(seed);
    if seed % 2 == 0 {
        faults = faults.with_stale_lists(PerMille::new(300), 8, STALE_FOR);
    }
    if seed % 3 == 0 {
        faults = faults.with_collided_retries(PerMille::new(50));
    }
    faults
}

/// Request delays only: every PUT lands as it answers, so a phase that
/// needs objects written gets them.
fn delays_only(seed: u64) -> SnapshotStoreFaults {
    SnapshotStoreFaults {
        late_put: PerMille::NEVER,
        lost_put: PerMille::NEVER,
        ..base_faults(seed)
    }
}

/// Which defence of #687 the run turns off. Production runs use the
/// default; only the negative controls build the others.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum Controls {
    /// Every defence on.
    #[default]
    Production,
    /// A write leases its object once the PUT answered
    /// (`Defence::LeaseBeforePut` off).
    LeaseAfterPut,
    /// A pass reads the holds before its LIST (`Defence::HoldsAfterList`
    /// off).
    HoldsBeforeList,
    /// An install commits the sender's pointer, uncopied
    /// (`Defence::InstallCopy` off).
    NoInstallCopy,
    /// A send holds nothing (`Defence::SendHold` off).
    NoSendHold,
    /// A pass deletes nothing (`Defence::SnapshotGc` off).
    SnapshotGcOff,
    /// A restore runs a pass before it reads its pointer
    /// (`Defence::NoPassBeforeRestore` off).
    PassBeforeRestore,
}

/// Turns off the defence `controls` names, and turns every other one of
/// #687 back on.
fn set_defences(controls: Controls) {
    set_off(Defence::LeaseBeforePut, controls == Controls::LeaseAfterPut);
    set_off(
        Defence::HoldsAfterList,
        controls == Controls::HoldsBeforeList,
    );
    set_off(Defence::InstallCopy, controls == Controls::NoInstallCopy);
    set_off(Defence::SendHold, controls == Controls::NoSendHold);
    set_off(Defence::SnapshotGc, controls == Controls::SnapshotGcOff);
    set_off(
        Defence::NoPassBeforeRestore,
        controls == Controls::PassBeforeRestore,
    );
}

/// The run's defences, drivers and snapshot store, reset when it ends.
struct Switched;

impl Switched {
    fn apply(controls: Controls, seed: u64) -> Self {
        set_defences(controls);
        ursula_stream::sim_controls::reset_fired();
        set_snapshot_drivers(Some(DRIVERS));
        set_snapshot_store(Some(SnapshotStorePlan {
            seed,
            faults: delays_only(seed),
            install_timeout: INSTALL_TIMEOUT,
        }));
        Self
    }
}

impl Drop for Switched {
    fn drop(&mut self) {
        set_defences(Controls::Production);
        set_snapshot_drivers(None);
        set_snapshot_store(None);
    }
}

/// Runs `operation` on the leader of `group` until it answers, while the
/// leadership moves.
macro_rules! on_leader {
    ($run:expr, $group:expr, |$engine:ident| $operation:expr) => {{
        let mut answered = false;
        for _ in 0..100 {
            let leader = $run.cluster.leader($group).await;
            if let Some($engine) = $run.cluster.engines.get_mut(&($group, leader))
                && $operation.await.is_ok()
            {
                answered = true;
                break;
            }
            madsim::time::sleep(STEP).await;
        }
        assert!(
            answered,
            "{}: the leader of group {} never answered",
            $run.context, $group
        );
    }};
}

/// One run: its cluster, its trace, its checks and its draws.
struct Run {
    cluster: JournalCluster,
    trace: SimTrace,
    checks: SnapshotChecks,
    rng: SplitMix64,
    seed: u64,
    context: String,
}

impl Run {
    fn step(&mut self, after_event: &str) {
        self.checks
            .step(&self.cluster, &mut self.trace, after_event);
    }

    async fn wait(&mut self, after_event: &str, duration: Duration) {
        self.checks
            .wait(&self.cluster, &mut self.trace, after_event, duration)
            .await;
    }

    async fn caught_up(&mut self, after_event: &str) {
        self.checks
            .wait_caught_up(&self.cluster, &mut self.trace, after_event)
            .await;
    }

    async fn quiesce(&mut self, after_event: &str) {
        self.checks
            .quiesce(&self.cluster, &mut self.trace, after_event)
            .await;
    }

    fn set_faults(&self, faults: SnapshotStoreFaults) {
        self.cluster.snapshot_bucket().set_faults(faults);
    }

    /// An operator's snapshot of `group` on `node_id`, then the checks. A
    /// build the node's coordinator does not start, or that does not
    /// publish in time, is no failure of the run: the storm runs on.
    async fn snapshot(&mut self, group: u32, node_id: u64, after_event: &str) {
        if self.cluster.engines.contains_key(&(group, node_id)) {
            let _built: Result<u64, &str> = self
                .cluster
                .operator_snapshot(group, node_id, PATIENCE)
                .await;
        }
        self.step(after_event);
    }

    /// A seeded node of `nodes`.
    fn pick(&mut self, nodes: &[u64]) -> u64 {
        let count = u64::try_from(nodes.len()).expect("a node count");
        let index = usize::try_from(self.rng.next_bounded(count)).expect("an index");
        nodes[index]
    }

    /// A seeded node that does not lead `group`.
    async fn follower_of(&mut self, group: u32) -> u64 {
        let leader = wait_leader(&self.cluster, group, &self.context).await;
        let followers: Vec<u64> = NODES.into_iter().filter(|node| *node != leader).collect();
        self.pick(&followers)
    }

    /// A: a seeded node leads both groups, the marked stream goes into
    /// group 0, and every node snapshots both groups.
    async fn start(&mut self) {
        let leader = self.pick(&NODES);
        lead_every_group(&self.cluster, leader, &self.context).await;
        self.trace
            .push(SimEvent::LeaderElected { leader_id: leader });
        let placement = group_placement(GROUP);
        let create = CreateStreamRequest::new(marked_stream(), "application/octet-stream");
        on_leader!(self, GROUP, |engine| engine.create_stream(
            create.clone(),
            placement,
            ColdWriteAdmission::default(),
        ));
        for record in 0..3 {
            let mut payload = MARKER.to_vec();
            payload.extend_from_slice(format!("{record};").as_bytes());
            on_leader!(self, GROUP, |engine| engine.append(
                AppendRequest::from_bytes(marked_stream(), payload.clone()),
                placement,
                ColdWriteAdmission::default(),
            ));
        }
        self.cluster.append(1, 2).await;
        self.caught_up("marked_stream_written").await;
        for node_id in NODES {
            for group in JOURNAL_GROUPS {
                self.snapshot(group, node_id, "first_snapshots").await;
            }
        }
        let marked = self
            .cluster
            .snapshot_bucket()
            .view()
            .objects
            .values()
            .any(|object| contains_marker(&object.bytes, MARKER));
        assert!(
            marked,
            "{}: some snapshot object holds the marked stream",
            self.context
        );
    }

    /// B: rounds of appends and operator snapshots, every node at least
    /// twice, under the storm's faults.
    async fn storm(&mut self) {
        self.set_faults(storm_faults(self.seed));
        let rounds = 8_u64.saturating_add(self.rng.next_bounded(5));
        let offset = self.rng.next_bounded(3);
        for round in 0..rounds {
            let group = u32::try_from(self.rng.next_bounded(2)).expect("a group");
            let entries = usize::try_from(self.rng.next_bounded(3))
                .expect("an entry count")
                .saturating_add(1);
            self.cluster.append(group, entries).await;
            let node_id = round.saturating_add(offset) % 3 + 1;
            self.snapshot(group, node_id, "build_storm").await;
        }
    }

    /// Cuts `follower` off from `group`'s leader, appends, snapshots and
    /// purges the leader's log through it, so the follower can catch up
    /// only from a snapshot. Answers the leader.
    async fn lag(&mut self, group: u32, follower: u64) -> u64 {
        let leader = wait_leader(&self.cluster, group, &self.context).await;
        self.cluster
            .policy
            .partition_bidirectional(leader, follower);
        self.cluster.append(group, 3).await;
        let purged = self
            .cluster
            .try_snapshot_and_purge_node(group, leader, 0, PATIENCE)
            .await;
        if let Err(stage) = purged {
            panic!("{}: group {group}: node {leader}: {stage}", self.context);
        }
        self.step("leader_purged");
        leader
    }

    /// C: the lagging follower, whose GET of the sent object waits at its
    /// store's gate while the leader builds again.
    async fn lagging_follower(&mut self) {
        self.set_faults(delays_only(self.seed));
        let follower = self.follower_of(GROUP).await;
        let sent_before = self.cluster.full_snapshots_to(GROUP, follower);
        let leader = self.lag(GROUP, follower).await;
        let gates = self.cluster.snapshot_gates(follower);
        let get = Gate::before(follower, StoreOp::Get);
        let arrived = gates.arrived(get);
        gates.hold(get);
        self.cluster.policy.heal_bidirectional(leader, follower);
        let deadline = madsim::time::Instant::now() + PATIENCE;
        while gates.arrived(get) <= arrived {
            assert!(
                madsim::time::Instant::now() < deadline,
                "{}: node {follower} never read the snapshot node {leader} sent",
                self.context
            );
            madsim::time::sleep(STEP).await;
            self.step("snapshot_sent");
        }
        // The sent object is no longer current once this build commits, so
        // the pass after it deletes it unless the send still holds it.
        self.cluster.append(GROUP, 1).await;
        self.snapshot(GROUP, leader, "sent_object_replaced").await;
        // Once the send ends, its hold on the replaced object drops and the
        // leader owes a pass, which its driver runs. That pass races a build
        // in one forced order.
        let leader_gates = self.cluster.snapshot_gates(leader);
        let list = Gate::before(leader, StoreOp::ListOwn);
        let lists_before = leader_gates.arrived(list);
        leader_gates.hold(list);
        gates.open(get);
        if self
            .waiting_group_lists(&leader_gates, list, lists_before)
            .await
        {
            self.pass_races_a_build(leader, &leader_gates, list).await;
        } else {
            // A run whose send held nothing, or whose passes delete nothing,
            // owes no pass here: the race is not run, visibly.
            leader_gates.open(list);
            self.trace.push(SimEvent::FaultApplied {
                phase: "no_pass_to_race_a_build".to_owned(),
            });
        }
        self.caught_up("snapshot_installed").await;
        assert!(
            self.cluster.full_snapshots_to(GROUP, follower) > sent_before,
            "{}: node {leader} sent node {follower} a snapshot",
            self.context
        );

        // The follower lags on the other group too, and the leaders keep
        // building while it installs, under the storm's faults.
        self.set_faults(storm_faults(self.seed));
        let other = 1;
        let other_leader = self.lag(other, follower).await;
        self.cluster
            .policy
            .heal_bidirectional(other_leader, follower);
        self.build_for(BUILDING, "follower_installing").await;
        self.caught_up("follower_caught_up").await;
    }

    /// A pass of group 0 waiting at the held `list` gate of `leader` races a
    /// build, in this order: the build's PUT acts and its answer waits, the
    /// pass lists, which shows the PUT, and reads the holds, the PUT answers
    /// and the build leases (when the lease comes after the PUT), checks and
    /// commits its object, and only then the pass deletes. With the lease
    /// before the PUT and the holds read after the LIST, the pass finds the
    /// object held. With either defence off it deletes the object the build
    /// committed (I1).
    async fn pass_races_a_build(&mut self, leader: u64, gates: &SnapshotStoreGates, list: Gate) {
        let answer = Gate::after(leader, StoreOp::WriteStaged);
        let listed = Gate::after(leader, StoreOp::ListOwn);
        let delete = Gate::before(leader, StoreOp::DeleteOwn);
        let answers_before = gates.arrived(answer);
        gates.hold(answer);
        gates.hold(delete);
        self.cluster.append(GROUP, 1).await;
        self.cluster.engines[&(GROUP, leader)]
            .raft_handle()
            .trigger()
            .snapshot()
            .await
            .expect("queue a snapshot build on the leader");
        let put = self
            .wait_for("put_answer_held", |_| {
                gates.arrived(answer) > answers_before
            })
            .await;
        assert!(
            put,
            "{}: node {leader}'s build never reached its PUT",
            self.context
        );
        let key = gates
            .log()
            .into_iter()
            .rev()
            .find(|event| event.gate == answer && event.pass == GatePass::Arrived)
            .map(|event| event.subject)
            .expect("the held PUT answer");
        let listed_before = gates.passed(listed);
        gates.open(list);
        let passed = self
            .wait_for("pass_listed", |_| gates.passed(listed) > listed_before)
            .await;
        assert!(
            passed,
            "{}: node {leader}'s held pass never listed",
            self.context
        );
        // The pass reads the holds in the step its LIST answers in.
        madsim::time::sleep(STEP).await;
        self.step("pass_listed");
        gates.open(answer);
        let committed = self
            .wait_for("put_answered", |cluster| {
                cluster
                    .snapshot_names()
                    .iter()
                    .any(|node| node.named.contains(&key))
            })
            .await;
        assert!(
            committed,
            "{}: node {leader} never committed {key}",
            self.context
        );
        gates.open(delete);
        self.trace.push(SimEvent::FaultApplied {
            phase: "pass_raced_a_build".to_owned(),
        });
        self.step("pass_deleted");
    }

    /// Waits until `done` holds, stepping, and answers whether it did within
    /// [`PATIENCE`].
    async fn wait_for(
        &mut self,
        after_event: &str,
        mut done: impl FnMut(&JournalCluster) -> bool,
    ) -> bool {
        let deadline = madsim::time::Instant::now() + PATIENCE;
        loop {
            if done(&self.cluster) {
                return true;
            }
            if madsim::time::Instant::now() >= deadline {
                return false;
            }
            madsim::time::sleep(STEP).await;
            self.step(after_event);
        }
    }

    /// Waits until a pass of group 0 that arrived at the held `list` gate
    /// since its `since`th arrival waits there, letting each pass of the
    /// other group through, and answers whether one did within
    /// [`PATIENCE`]. A node's driver runs its passes one after the other, so
    /// a held pass of the other group would hold back group 0's.
    async fn waiting_group_lists(
        &mut self,
        gates: &SnapshotStoreGates,
        list: Gate,
        since: usize,
    ) -> bool {
        let count = 1;
        let subject = format!("node-{}/group-{GROUP}", list.node_id);
        let deadline = madsim::time::Instant::now() + PATIENCE;
        loop {
            let log = gates.log();
            let resolved: Vec<usize> = log
                .iter()
                .filter(|event| event.gate == list && event.pass != GatePass::Arrived)
                .map(|event| event.ticket)
                .collect();
            let waiting: Vec<&GateEvent> = log
                .iter()
                .filter(|event| {
                    event.gate == list
                        && event.pass == GatePass::Arrived
                        && event.ticket >= since
                        && !resolved.contains(&event.ticket)
                })
                .collect();
            if waiting
                .iter()
                .filter(|event| event.subject == subject)
                .count()
                >= count
            {
                return true;
            }
            if madsim::time::Instant::now() >= deadline {
                return false;
            }
            let others_first = waiting
                .iter()
                .min_by_key(|event| event.ticket)
                .is_some_and(|event| event.subject != subject);
            if others_first && gates.release(list, 1).is_err() {
                return false;
            }
            madsim::time::sleep(STEP).await;
            self.step("pass_held_at_list");
        }
    }

    /// Appends and builds on the leaders every [`BUILD_EVERY`] for
    /// `duration`.
    async fn build_for(&mut self, duration: Duration, after_event: &str) {
        let deadline = madsim::time::Instant::now() + duration;
        while madsim::time::Instant::now() < deadline {
            let group = u32::try_from(self.rng.next_bounded(2)).expect("a group");
            self.cluster.append(group, 1).await;
            let leader = wait_leader(&self.cluster, group, &self.context).await;
            self.snapshot(group, leader, after_event).await;
            madsim::time::sleep(BUILD_EVERY).await;
            self.step(after_event);
        }
    }

    /// D: a follower loses its disk and installs both groups while the
    /// leaders keep building.
    async fn wiped_follower(&mut self) {
        self.set_faults(delays_only(self.seed));
        let wiped = self.follower_of(GROUP).await;
        let name = format!("snapshot-objects-{}-wiped-{wiped}", self.seed);
        self.cluster
            .wipe_node(wiped, &name, JournalTuning::new(WalFsync::Always))
            .await;
        self.trace.push(SimEvent::NodeStopped { node_id: wiped });
        self.step("node_wiped");
        for group in JOURNAL_GROUPS {
            let leader = wait_leader(&self.cluster, group, &self.context).await;
            self.cluster.append(group, 2).await;
            let purged = self
                .cluster
                .try_snapshot_and_purge_node(group, leader, 0, PATIENCE)
                .await;
            if let Err(stage) = purged {
                panic!("{}: group {group}: node {leader}: {stage}", self.context);
            }
            self.step("leader_purged");
        }
        self.cluster.start_node(wiped).await;
        self.trace.push(SimEvent::NodeRestarted { node_id: wiped });
        self.step("wiped_node_started");
        self.build_for(BUILDING / 3, "wiped_node_installing").await;
        self.cluster.wait_gates_open(PATIENCE).await;
        self.caught_up("wiped_node_caught_up").await;
    }

    /// E: every node restarts in a seeded order, one after a power loss.
    async fn restarts(&mut self) {
        self.set_faults(base_faults(self.seed));
        let mut order = NODES.to_vec();
        for index in (1..order.len()).rev() {
            let bound = u64::try_from(index).expect("an index").saturating_add(1);
            let swap = usize::try_from(self.rng.next_bounded(bound)).expect("an index");
            order.swap(index, swap);
        }
        let power_lost = self.pick(&NODES);
        for node_id in order {
            self.cluster.stop_node(node_id).await;
            self.trace.push(SimEvent::NodeStopped { node_id });
            if node_id == power_lost {
                self.cluster.wals[&node_id]
                    .power_loss_losing_unsynced()
                    .await;
                self.trace.push(SimEvent::FaultApplied {
                    phase: "power_lost".to_owned(),
                });
            }
            self.step("node_stopped");
            let started = self.cluster.try_start_node(node_id).await;
            // A restore that refused to open is checked first: the object
            // it found missing is a named one.
            self.step("node_restarted");
            if let Err(refused) = started {
                panic!(
                    "{}: node {} refused to start its replica of group {}: {:?}",
                    self.context, refused.node_id, refused.group, refused.error
                );
            }
            self.trace.push(SimEvent::NodeRestarted { node_id });
            self.cluster.wait_gates_open(PATIENCE).await;
            self.caught_up("node_caught_up").await;
        }
    }

    /// F: the marked stream is deleted, every node snapshots its group, and
    /// within the bound no object that no node names holds the marker.
    async fn marker_removed(&mut self) {
        self.set_faults(delays_only(self.seed));
        // The named objects that hold the marker, which the snapshots below
        // replace: I4 then has objects to check.
        let marked = self.named_marked_objects();
        assert!(
            !marked.is_empty(),
            "{}: some node names an object that holds the marked stream",
            self.context
        );
        let placement = group_placement(GROUP);
        on_leader!(self, GROUP, |engine| engine.delete_stream(
            DeleteStreamRequest {
                stream_id: marked_stream(),
                now_ms: 0,
                if_incarnation: None,
            },
            placement,
        ));
        self.caught_up("marked_stream_deleted").await;
        for node_id in NODES {
            self.snapshot(GROUP, node_id, "snapshot_without_marker")
                .await;
        }
        self.wait("snapshot_without_marker", BOUND.saturating_add(STEP))
            .await;
        let still_named: Vec<String> = self
            .named_marked_objects()
            .into_iter()
            .filter(|key| marked.contains(key))
            .collect();
        assert!(
            still_named.len() < marked.len(),
            "{}: the snapshots without the marker replaced none of {marked:?}",
            self.context
        );
        self.checks.bound(
            &self.cluster,
            &mut self.trace,
            "marker_bound",
            MARKER,
            BOUND,
        );
    }

    /// The keys some node names whose objects hold the marker.
    fn named_marked_objects(&self) -> Vec<String> {
        let view = self.cluster.snapshot_bucket().view();
        let named: std::collections::BTreeSet<String> = self
            .cluster
            .snapshot_names()
            .into_iter()
            .flat_map(|node| node.named)
            .collect();
        named
            .into_iter()
            .filter(|key| {
                view.objects
                    .get(key)
                    .is_some_and(|object| contains_marker(&object.bytes, MARKER))
            })
            .collect()
    }

    /// What the store served in the run, with a digest of its log.
    fn summary(&self) -> SimEvent {
        let bucket = self.cluster.snapshot_bucket();
        let events = bucket.events();
        let prefix = bucket.prefix().clone();
        let count = |matches: &dyn Fn(&SnapshotStoreEvent) -> bool| {
            u64::try_from(events.iter().filter(|event| matches(event)).count()).unwrap_or(u64::MAX)
        };
        let digest = blake3::hash(format!("{events:?}").as_bytes())
            .to_hex()
            .to_string();
        SimEvent::SnapshotObjectsOwned {
            puts: count(&|event| matches!(event, SnapshotStoreEvent::Put { .. })),
            install_reads: count(&|event| {
                matches!(event, SnapshotStoreEvent::Get { node_id, path, found: true }
                    if !is_owned_by(&prefix, *node_id, path))
            }),
            restore_reads: count(&|event| {
                matches!(event, SnapshotStoreEvent::Get { node_id, path, .. }
                    if is_owned_by(&prefix, *node_id, path))
            }),
            deletes: count(&|event| matches!(event, SnapshotStoreEvent::Delete { .. })),
            late_puts_landed: count(&|event| {
                matches!(event, SnapshotStoreEvent::LatePutLanded { .. })
            }),
            store_log_digest: digest,
        }
    }
}

pub(super) async fn run_snapshot_objects_inner(
    config: ThreeNodeRaftSimConfig,
    controls: Controls,
) -> ThreeNodeRaftSimOutcome {
    let seed = config.seed;
    let _switched = Switched::apply(controls, seed);
    let mut trace = SimTrace::default();
    trace.push(SimEvent::ClusterBuilt { seed });
    let cluster =
        JournalCluster::start_with_fsync(&format!("snapshot-objects-{seed}"), WalFsync::Always)
            .await;
    let mut run = Run {
        cluster,
        trace,
        checks: SnapshotChecks::default(),
        rng: SplitMix64::new(seed ^ SALT),
        seed,
        context: format!("seed {seed}"),
    };
    run.step("cluster_started");
    run.start().await;
    run.storm().await;
    run.lagging_follower().await;
    run.quiesce("lagging_follower").await;
    run.wiped_follower().await;
    run.restarts().await;
    run.quiesce("restarts").await;
    run.marker_removed().await;
    run.quiesce("marker_removed").await;
    run.cluster.verify_reads().await;
    let summary = run.summary();
    run.trace.push(summary);
    let leader = wait_leader(&run.cluster, GROUP, &run.context).await;
    let applied = metrics(&run.cluster, GROUP, leader)
        .last_applied
        .map_or(0, |log_id| log_id.index);
    ThreeNodeRaftSimOutcome {
        seed,
        leader_id: leader,
        target_node_id: None,
        appended_log_index: applied,
        trace: run.trace,
    }
}

#[cfg(test)]
mod tests {
    use madsim::runtime::Handle;
    use madsim::runtime::Runtime;
    use ursula_raft::MadsimOpenRaftRuntime;

    use super::Controls;
    use super::run_snapshot_objects_inner;
    use crate::madsim_harness::SimEvent;
    use crate::madsim_harness::SimReport;
    use crate::madsim_harness::SimScenario;
    use crate::madsim_harness::SimTrace;
    use crate::madsim_harness::ThreeNodeRaftSim;
    use crate::madsim_harness::ThreeNodeRaftSimConfig;
    use crate::madsim_harness::snapshot_checks::NAMED_KEYS_EXIST;
    use crate::madsim_harness::snapshot_checks::QUIET_PREFIXES;
    use crate::madsim_harness::snapshot_checks::READS_FOUND;
    use crate::madsim_harness::tests::seeds_from_env;
    use crate::madsim_harness::tests::sim_test_guard;

    const SCENARIO: SimScenario = SimScenario::SnapshotObjectsStayOwned;
    /// The seeds whose production runs the replays cover.
    const REPLAYED: std::ops::RangeInclusive<u64> = 546..=548;

    fn config(seed: u64) -> ThreeNodeRaftSimConfig {
        ThreeNodeRaftSimConfig::new(seed, format!("ursula-sim-{seed}-{}", SCENARIO.slug()))
    }

    fn run(seed: u64, controls: Controls) -> SimReport {
        ThreeNodeRaftSim::run_snapshot_objects_with_controls_report(config(seed), controls)
    }

    /// The summary a passing run ends with.
    fn summary(report: &SimReport) -> Option<&SimEvent> {
        report
            .outcome
            .trace
            .events
            .iter()
            .find(|event| matches!(event, SimEvent::SnapshotObjectsOwned { .. }))
    }

    /// Every snapshot object stays owned, the same way twice per seed: the
    /// same report, the same store requests.
    #[test]
    fn snapshot_objects_stay_owned_replays() {
        let _guard = sim_test_guard();
        for seed in REPLAYED {
            let first = run(seed, Controls::Production);
            let second = run(seed, Controls::Production);
            assert_eq!(first, second, "seed {seed}");
            assert_eq!(first.scenario, SCENARIO);
            let Some(SimEvent::SnapshotObjectsOwned {
                puts,
                install_reads,
                restore_reads,
                deletes,
                ..
            }) = summary(&first)
            else {
                panic!("seed {seed}: the run ends with its store summary");
            };
            assert!(
                first.outcome.trace.events.iter().any(|event| matches!(
                    event,
                    SimEvent::FaultApplied { phase } if phase == "pass_raced_a_build"
                )),
                "seed {seed}: a pass raced a build in phase C"
            );
            assert!(
                *puts > 0 && *install_reads > 0 && *restore_reads > 0 && *deletes > 0,
                "seed {seed}: the run wrote, installed, restored and deleted objects: {:?}",
                summary(&first)
            );
        }
    }

    /// Runs `seed` under `controls`, which must fail `invariant`, and
    /// answers the failure's message. The production run of the seed must
    /// pass.
    fn assert_fails(seed: u64, controls: Controls, invariant: &str) -> String {
        assert_fails_after(seed, controls, invariant).1
    }

    /// [`assert_fails`], answering the event the failure came after too.
    fn assert_fails_after(seed: u64, controls: Controls, invariant: &str) -> (String, String) {
        if !REPLAYED.contains(&seed) {
            summary(&run(seed, Controls::Production))
                .unwrap_or_else(|| panic!("seed {seed}: the defended run passes"));
        }
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(|| run(seed, controls));
        std::panic::set_hook(previous_hook);
        let Err(_panic) = result else {
            panic!("{controls:?} left seed {seed} passing");
        };
        let trace = SimTrace::last_recorded();
        trace
            .events
            .iter()
            .find_map(|event| match event {
                SimEvent::InvariantFailed {
                    invariant: failed,
                    after_event,
                    message,
                } => {
                    assert_eq!(
                        failed, invariant,
                        "{controls:?} on seed {seed} failed another invariant: {message}"
                    );
                    Some((after_event.clone(), message.clone()))
                }
                _ => None,
            })
            .unwrap_or_else(|| {
                panic!(
                    "{controls:?} on seed {seed} failed no invariant: {:?}",
                    trace.events.last()
                )
            })
    }

    /// A lease taken once the PUT answered leaves a race: a pass whose LIST
    /// shows the PUT reads the holds before the lease, the build commits the
    /// object, and the pass deletes it.
    #[test]
    fn negative_control_a_lease_after_the_put_loses_a_named_object() {
        let _guard = sim_test_guard();
        let message = assert_fails(546, Controls::LeaseAfterPut, NAMED_KEYS_EXIST);
        assert!(
            message.contains("which the store does not hold"),
            "{message}"
        );
    }

    /// A pass that reads the holds before its LIST deletes an object leased
    /// and written while the LIST ran, which its build then commits.
    #[test]
    fn negative_control_holds_read_before_the_list_lose_a_named_object() {
        let _guard = sim_test_guard();
        let message = assert_fails(546, Controls::HoldsBeforeList, NAMED_KEYS_EXIST);
        assert!(
            message.contains("which the store does not hold"),
            "{message}"
        );
    }

    /// An install that commits the sender's pointer names the sender's
    /// object, which the sender deletes once it built again.
    #[test]
    fn negative_control_an_install_without_a_copy_names_the_senders_deleted_object() {
        let _guard = sim_test_guard();
        let message = assert_fails(546, Controls::NoInstallCopy, NAMED_KEYS_EXIST);
        let (node, owner) = named_by_and_owner(&message);
        assert_ne!(node, owner, "a node names another's object: {message}");
    }

    /// A send that holds nothing lets the sender's next build and its pass
    /// delete the object while the receiver still reads it.
    #[test]
    fn negative_control_a_send_without_its_hold_loses_the_object_mid_send() {
        let _guard = sim_test_guard();
        let message = assert_fails(546, Controls::NoSendHold, READS_FOUND);
        assert!(
            message.contains("read of an object a send holds"),
            "{message}"
        );
    }

    /// With GC off, every replaced object stays in its directory, and the
    /// first quiet point finds one its node does not name.
    #[test]
    fn negative_control_gc_off_leaves_replaced_objects_in_every_prefix() {
        let _guard = sim_test_guard();
        let (after_event, message) =
            assert_fails_after(546, Controls::SnapshotGcOff, QUIET_PREFIXES);
        assert_eq!(after_event, "lagging_follower", "the first quiet point");
        assert!(
            message.contains("which its metadata does not name"),
            "{message}"
        );
    }

    /// A pass before the restore read its pointer finds the restored object
    /// unheld and deletes it: the node's record names a missing object.
    #[test]
    fn negative_control_a_pass_before_the_restore_deletes_the_restored_object() {
        let _guard = sim_test_guard();
        let message = assert_fails(546, Controls::PassBeforeRestore, NAMED_KEYS_EXIST);
        let (node, owner) = named_by_and_owner(&message);
        assert_eq!(node, owner, "the node's own object is gone: {message}");
    }

    /// The node an I1 message names and the owner of the object it names:
    /// `I1: node {node} names snapshots/node-{owner}/...`.
    fn named_by_and_owner(message: &str) -> (u64, u64) {
        let parse = |text: &str, before: &str, after: &str| -> u64 {
            text.split_once(before)
                .and_then(|(_, rest)| rest.split_once(after))
                .and_then(|(number, _)| number.parse().ok())
                .unwrap_or_else(|| panic!("an I1 message: {text}"))
        };
        (
            parse(message, "I1: node ", " names"),
            parse(message, "names snapshots/node-", "/"),
        )
    }

    /// Two seeds' runs schedule the same in both runs, at every draw of
    /// madsim's RNG.
    #[test]
    fn strict_replay_snapshot_objects_stay_owned() {
        let _guard = sim_test_guard();
        for seed in [546, 547] {
            let outcome = Runtime::check_determinism(seed, madsim::Config::default(), || async {
                let seed = Handle::current().seed();
                MadsimOpenRaftRuntime::scope(
                    seed,
                    run_snapshot_objects_inner(config(seed), Controls::Production),
                )
                .await
            });
            assert_eq!(outcome.seed, seed);
        }
    }

    /// For each control and seed of `SNAPSHOT_OBJECTS_CONTROL_SEEDS`
    /// (546..=599 by default) and the production twin, prints the first
    /// invariant the run failed or `passed`. The controls default to the two
    /// that need a race, all of them with `SNAPSHOT_OBJECTS_SCAN_ALL=1`.
    /// Seeds run on `SNAPSHOT_OBJECTS_SCAN_THREADS` threads (4).
    #[test]
    #[ignore = "diagnostic: scans seeds for the negative controls that need a race"]
    fn control_seed_scan() {
        let _guard = sim_test_guard();
        let seeds = seeds_from_env(
            "SNAPSHOT_OBJECTS_CONTROL_SEEDS",
            &(546..=599).collect::<Vec<_>>(),
        );
        let controls: Vec<Controls> = if std::env::var("SNAPSHOT_OBJECTS_SCAN_ALL").is_ok() {
            vec![
                Controls::Production,
                Controls::LeaseAfterPut,
                Controls::HoldsBeforeList,
                Controls::NoInstallCopy,
                Controls::NoSendHold,
                Controls::SnapshotGcOff,
                Controls::PassBeforeRestore,
            ]
        } else {
            vec![
                Controls::Production,
                Controls::LeaseAfterPut,
                Controls::HoldsBeforeList,
            ]
        };
        let threads: usize = std::env::var("SNAPSHOT_OBJECTS_SCAN_THREADS")
            .ok()
            .and_then(|threads| threads.parse().ok())
            .unwrap_or(4);
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let work: Vec<(u64, Controls)> = seeds
            .iter()
            .flat_map(|seed| controls.iter().map(move |controls| (*seed, *controls)))
            .collect();
        let next = std::sync::atomic::AtomicUsize::new(0);
        let lines = std::sync::Mutex::new(Vec::new());
        std::thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(|| {
                    loop {
                        let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let Some((seed, controls)) = work.get(index).copied() else {
                            break;
                        };
                        let result = std::panic::catch_unwind(|| run(seed, controls));
                        let outcome = match result {
                            Ok(_report) => "passed".to_owned(),
                            Err(_panic) => SimTrace::last_recorded()
                                .events
                                .iter()
                                .find_map(|event| match event {
                                    SimEvent::InvariantFailed {
                                        invariant,
                                        after_event,
                                        message,
                                    } => {
                                        Some(format!("{invariant} after {after_event}: {message}"))
                                    }
                                    _ => None,
                                })
                                .unwrap_or_else(|| "panicked outside an invariant".to_owned()),
                        };
                        lines
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .push((seed, format!("{controls:?}"), outcome));
                    }
                });
            }
        });
        std::panic::set_hook(previous_hook);
        let mut lines = lines
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        lines.sort();
        for (seed, controls, outcome) in lines {
            println!("p4-scan seed {seed} {controls}: {outcome}");
        }
    }
}
