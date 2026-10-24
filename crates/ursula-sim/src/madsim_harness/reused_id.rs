//! Decision 15 of #768 in the `placement-move` family: a removed node id
//! runs again on a new disk once its removal completed.
//!
//! - The decommission shape uses node 4's id again on half its seeds
//!   ([`Run::reuse`]): its process stops, the meta leader completes its
//!   removal (the latest view shows it `vacant`), a new disk starts under
//!   the id, binds a new root and takes group 0 by a move, and the run then
//!   settles and reads back every acknowledged append (I3, I1). I13 judges
//!   the removed life per root throughout ([`super::placed_removals`]).
//! - Two negative controls run their own scripts on four nodes behind the
//!   identity fence ([`MetaCluster::start_fenced`]), with meta voters 1 to
//!   3 and node 4 decommissioned, each turning its defence off only at the
//!   step it judges:
//!   - [`reused_id_vote`] (`Defence::EmptyReplicaVote`): group G votes on
//!     node 4, its leader K and a voter S that is cut off from G while node
//!     4 leaves, so G converges without S, which keeps the membership that
//!     names node 4. Node 4's new disk starts at once, waits for the
//!     removal (`removal_pending`), binds once the id is vacant, and a move
//!     puts it in place of S while K holds its learner addition and no G
//!     message passes between K and node 4, so node 4's empty replica gets
//!     no barrier and no floor. Then S and node 4 reach each other in G
//!     alone. S campaigns on its old membership: defended, node 4's empty
//!     replica refuses it, and with the screen off it grants, so S leads a
//!     membership whose committed history it lacks ([`REUSED_VOTE`]).
//!   - [`returning_old_disk`] (`Defence::DeleteAfterRegistration`), with
//!     the production snapshot driver on every node over the in-memory
//!     snapshot store, each process's store behind its node fence: node 4
//!     dies and is decommissioned, its id turns vacant, a new disk takes a
//!     group node 4 never held and snapshots it, which names an object
//!     under `node-4/`. Then the new disk stops and the old disk starts
//!     where it reaches no peer, so the meta group never answers its run.
//!     Its start pass finds the new life's object in the directory of a
//!     group it does not hold: defended, every DELETE of the run is
//!     refused, and with the delete standing off the pass deletes the new
//!     life's named object once its predecessor wait ended
//!     (`snapshot_named_keys_exist`).

use std::collections::BTreeSet;
use std::time::Duration;

use openraft::ServerState;
use ursula_config::WalFsync;
use ursula_control::LifecycleChange;
use ursula_control::RegistrationStatus;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::RootId;
use ursula_proto::placement::RootSlot;
use ursula_proto::placement::StepKind;
use ursula_raft::predecessor_wait::PREDECESSOR_WAIT;
use ursula_runtime::snapshot_harness::NodeNames;
use ursula_runtime::snapshot_harness::SnapshotStoreFaults;
use ursula_runtime::snapshot_harness::check_named_keys_exist;
use ursula_stream::sim_controls::Defence;

use super::SimEvent;
use super::ThreeNodeRaftSimOutcome;
use super::journal_cluster::DriverPlan;
use super::journal_cluster::JOURNAL_GROUPS;
use super::journal_cluster::SnapshotStorePlan;
use super::journal_cluster::set_snapshot_drivers;
use super::journal_cluster::set_snapshot_store;
use super::meta_checks::BOUND;
use super::meta_checks::STEP;
use super::meta_checks::fail;
use super::meta_cluster::MetaCluster;
use super::placed_hosting::I3;
use super::placed_removals::I13;
use super::placement_decommission::REMOVAL_BOUND;
use super::placement_move::Controls;
use super::placement_move::MOVE_BOUND;
use super::placement_move::MoveDraw;
use super::placement_move::Run;
use super::placement_move::claims;
use super::placement_move::defence_off_from_here;
use super::snapshot_checks::NAMED_KEYS_EXIST;
use super::snapshot_checks::SnapshotChecks;

/// A stale membership's candidate never leads with a reused id's vote.
pub(super) const REUSED_VOTE: &str = "a_reused_id_elects_no_stale_leader";

/// The nodes of a control's script, and the one it decommissions.
const NODES: u64 = 4;
const LEAVING: u64 = 4;
/// The disk lease of a fenced script's nodes.
const LEASE: Duration = Duration::from_secs(5);
/// How long a removal may take to complete once the node's process
/// stopped: two passes of the meta leader and the meta learner's removal.
const COMPLETION_BOUND: Duration = Duration::from_secs(100);
/// How long a new disk under a vacant id may take to bind, and to wait out
/// its predecessor.
const BINDING_BOUND: Duration = Duration::from_secs(60);
/// How long the stale candidate is watched once it reaches node 4.
const CAMPAIGN_WATCH: Duration = Duration::from_secs(10);
/// How long the returning old disk is watched past its predecessor wait,
/// for its start pass and the retries of its refused DELETEs.
const RETURN_WATCH: Duration = Duration::from_secs(20);
/// Mixed into the seed of the snapshot bucket's draws.
const SALT: u64 = 0x7265_7573_6564_6964;
/// The snapshot drivers of [`returning_old_disk`]: a pass every 100 ms and
/// four entries kept below a snapshot.
const DRIVERS: DriverPlan = DriverPlan {
    max_age: Duration::from_secs(600),
    keep: 4,
    interval: Duration::from_millis(100),
    permits: 1,
};
/// `raft.install_snapshot_timeout` of every node.
const INSTALL_TIMEOUT: Duration = Duration::from_secs(1);

/// `node`'s record in the latest view, as its lifecycle and slot.
fn record_of(cluster: &MetaCluster, node: u64) -> Option<(NodeLifecycle, RootSlot)> {
    cluster
        .latest_view()
        .cluster()?
        .node(NodeId::new(node))
        .map(|record| (record.lifecycle, record.root))
}

impl Run {
    /// Uses the removed `node`'s id again (see the module docs): its
    /// process stops, its removal completes, a new disk named after `name`
    /// binds a new root under the id, and group 0 moves onto it.
    pub(super) async fn reuse(&mut self, node: u64, name: &str) {
        const PHASE: &str = "reuse";
        let old = self.removed_root(PHASE, node);
        if self.cluster.runtimes.contains_key(&node) {
            let status = self.cluster.data.wals[&node]
                .raft_wal()
                .fence()
                .status(ursula_runtime::node_fence::fence_now());
            if status == ursula_runtime::node_fence::FenceStatus::Superseded {
                self.cluster.kill_node(node).await;
            } else {
                self.cluster.stop_node(node).await;
            }
        }
        self.step("removed_node_stopped");
        self.vacant(PHASE, node, old).await;
        let tuning = self.cluster.data.wals[&node].tuning();
        self.cluster.data.new_disk(node, name, tuning);
        self.cluster.start_node(node).await;
        self.bound_anew(PHASE, node, old).await;
        self.move_group(0, MoveDraw::ONE).await;
        if !self.cluster.placed_voters(0).contains(&node) {
            fail(
                &mut self.trace,
                I3,
                PHASE,
                format!("group 0 did not move onto node {node}'s new life"),
            );
        }
        self.step("reused_node_placed");
    }

    /// The root of `node`'s removed life, as the latest view shows it.
    fn removed_root(&mut self, phase: &str, node: u64) -> RootId {
        match record_of(&self.cluster, node) {
            Some((NodeLifecycle::Removed, slot)) => slot.root(),
            other => fail(
                &mut self.trace,
                I13,
                phase,
                format!("node {node} is not removed before its id is used again: {other:?}"),
            ),
        }
    }

    /// Waits until the latest view shows `node` vacant on `old` released:
    /// the meta leader completed its removal.
    async fn vacant(&mut self, phase: &str, node: u64, old: RootId) {
        self.until(phase, I13, COMPLETION_BOUND, move |cluster| {
            let record = record_of(cluster, node);
            (record != Some((NodeLifecycle::Vacant, RootSlot::Released(old)))).then(|| {
                format!(
                    "node {node}'s removal did not complete once its process stopped: {record:?}"
                )
            })
        })
        .await;
        self.step("node_vacant");
    }

    /// Waits until the latest view shows `node` active on a root other than
    /// `old`, and its new run took part again: its fence holds nothing.
    async fn bound_anew(&mut self, phase: &str, node: u64, old: RootId) {
        self.until(phase, I13, BINDING_BOUND, move |cluster| {
            let record = record_of(cluster, node);
            match record {
                Some((NodeLifecycle::Active, RootSlot::Bound(root))) if root != old => None,
                _ => Some(format!(
                    "a new disk under node {node}'s vacant id did not bind: {record:?}"
                )),
            }
        })
        .await;
        self.step("reused_node_bound");
    }

    /// Decommissions `node`, which may be down, and waits until the latest
    /// view shows it removed.
    async fn decommissioned(&mut self, phase: &str, node: u64) {
        self.set_lifecycle(phase, node, LifecycleChange::Decommission)
            .await;
        self.until(phase, I3, REMOVAL_BOUND, move |cluster| {
            let lifecycle = cluster.lifecycle(node);
            (lifecycle != Some(NodeLifecycle::Removed))
                .then(|| format!("node {node} is {lifecycle:?} after its decommission"))
        })
        .await;
        self.step("node_removed");
    }

    /// Forms the placed cluster and its groups.
    async fn formed_and_live(&mut self) {
        self.formed("formed").await;
        self.learned("formed").await;
        self.bootstrapped("formed").await;
        self.append_data("formed").await;
        self.cluster
            .groups_live(&mut self.trace, "groups_live")
            .await;
        self.step("groups_live");
    }

    /// Cuts `node`'s replica of `group` off from every other node's, both
    /// ways, by the processes that run now. Its other groups and its meta
    /// plane stay.
    fn cut_off_in(&self, group: u32, node: u64) {
        let process = self.cluster.process(node).expect("a fenced node runs");
        for other in self.cluster.running() {
            if other == node {
                continue;
            }
            let theirs = self.cluster.process(other).expect("a fenced node runs");
            self.cluster.cut_group_route(group, process, other);
            self.cluster.cut_group_route(group, theirs, node);
        }
    }
}

/// The run of a fenced control's script: four nodes at RF 3 behind the
/// identity fence, formed and live.
async fn fenced_run(seed: u64) -> Run {
    let mut run = Run::start_fenced(seed, NODES, WalFsync::Always, claims(NODES), LEASE).await;
    run.formed_and_live().await;
    run
}

/// `negative_control_empty_replica_vote` (see the module docs).
pub(super) async fn reused_id_vote(seed: u64, controls: Controls) -> ThreeNodeRaftSimOutcome {
    const PHASE: &str = "stale_candidate";
    let mut run = fenced_run(seed).await;
    let Some(group) = JOURNAL_GROUPS
        .iter()
        .copied()
        .find(|group| run.cluster.placed_voters(*group).contains(&LEAVING))
    else {
        panic!("node {LEAVING} votes in no group");
    };
    let voters = run.cluster.placed_voters(group);
    let mut keeper = run.leader(PHASE, group).await;
    if keeper == LEAVING {
        let to = voters
            .iter()
            .copied()
            .find(|voter| *voter != LEAVING)
            .expect("another voter");
        run.transfer(PHASE, group, keeper, to).await;
        keeper = to;
    }
    let stale = voters
        .iter()
        .copied()
        .find(|voter| *voter != keeper && *voter != LEAVING)
        .expect("a third voter");
    run.trace.push(SimEvent::FaultApplied {
        phase: format!(
            "{PHASE}: group {group} on {voters:?}, led by {keeper}, its voter {stale} cut off"
        ),
    });
    // S misses every entry of G from here on: the change that drops node 4.
    run.cut_off_in(group, stale);
    run.decommissioned(PHASE, LEAVING).await;
    let target = run.cluster.placed_voters(group);
    run.settled(PHASE, group, &target).await;
    let old = run.removed_root(PHASE, LEAVING);
    // The old process stops, so its removal completes, and the new disk
    // starts at once: it waits for the removal before it binds.
    run.cluster.stop_node(LEAVING).await;
    run.cluster
        .new_fenced_disk(LEAVING, &format!("placement-move-fenced-{seed}-reused"));
    defence_off_from_here(
        Defence::EmptyReplicaVote,
        controls == Controls::ReusedIdVote { defended: false },
    );
    run.cluster.start_node(LEAVING).await;
    run.until(PHASE, I13, BOUND, |cluster| {
        let waits = cluster.runtimes.get(&LEAVING).is_some_and(|runtime| {
            runtime.status_reader().placement_status().registration
                == RegistrationStatus::RemovalPending
        });
        let bound = record_of(cluster, LEAVING).is_some_and(|(lifecycle, _)| lifecycle.has_life());
        (!waits && !bound)
            .then(|| format!("node {LEAVING}'s new disk neither waited for its removal nor bound"))
    })
    .await;
    run.step("the_new_disk_waits_or_bound");
    run.vacant(PHASE, LEAVING, old).await;
    run.bound_anew(PHASE, LEAVING, old).await;
    // No message of G passes between K and node 4, nor yet between S and
    // node 4, and K holds its learner addition, so node 4's replica gets no
    // barrier and no floor.
    let new_process = run.cluster.process(LEAVING).expect("node 4 runs");
    let keeper_process = run.cluster.process(keeper).expect("the keeper runs");
    let stale_process = run.cluster.process(stale).expect("S runs");
    run.cluster.cut_group_route(group, new_process, keeper);
    run.cluster.cut_group_route(group, keeper_process, LEAVING);
    run.cluster.cut_group_route(group, new_process, stale);
    run.cluster.cut_group_route(group, stale_process, LEAVING);
    run.cluster.hold(keeper, group, StepKind::AddLearners);
    let mut to = run.cluster.placed_voters(group);
    to.remove(&stale);
    to.insert(LEAVING);
    run.accepted(PHASE, group, &to, None).await;
    run.until(PHASE, I3, MOVE_BOUND, move |cluster| {
        cluster
            .hosted_raft(LEAVING, group)
            .is_none()
            .then(|| format!("node {LEAVING}'s new life created no replica of group {group}"))
    })
    .await;
    // S and node 4 reach each other in G, and nothing else of S does.
    run.cluster.pin_group_route(group, stale_process, LEAVING);
    run.cluster.pin_group_route(group, new_process, stale);
    run.step("stale_reaches_the_new_life");
    let deadline = madsim::time::Instant::now() + CAMPAIGN_WATCH;
    while madsim::time::Instant::now() < deadline {
        madsim::time::sleep(STEP).await;
        run.observe(PHASE);
        let leads = run.cluster.hosted_raft(stale, group).is_some_and(|raft| {
            openraft::rt::WatchReceiver::borrow_watched(&raft.metrics()).state
                == ServerState::Leader
        });
        if leads {
            fail(
                &mut run.trace,
                REUSED_VOTE,
                PHASE,
                format!(
                    "stale_candidate_won: node {stale} leads group {group} on its membership \
                     that names node {LEAVING}, with node {LEAVING}'s new life's vote"
                ),
            );
        }
    }
    run.cluster.release(keeper, group, StepKind::AddLearners);
    run.cluster.clear_routes();
    run.step("stale_healed");
    run.settled(PHASE, group, &to).await;
    run.settle_and_read().await;
    run.finish()
}

/// `negative_control_delete_after_registration` (see the module docs).
pub(super) async fn returning_old_disk(seed: u64, controls: Controls) -> ThreeNodeRaftSimOutcome {
    const PHASE: &str = "old_disk_returned";
    set_snapshot_drivers(Some(DRIVERS));
    set_snapshot_store(Some(SnapshotStorePlan {
        seed: seed ^ SALT,
        faults: SnapshotStoreFaults::none(),
        install_timeout: INSTALL_TIMEOUT,
    }));
    let mut run = Run::start_fenced(seed, NODES, WalFsync::Always, claims(NODES), LEASE).await;
    // The cluster read them when it was provisioned.
    set_snapshot_drivers(None);
    set_snapshot_store(None);
    run.snapshots = Some(SnapshotChecks::default());
    run.formed_and_live().await;
    let Some(group) = JOURNAL_GROUPS
        .iter()
        .copied()
        .find(|group| !run.cluster.placed_voters(*group).contains(&LEAVING))
    else {
        panic!("node {LEAVING} votes in every group");
    };
    // Node 4 dies and is removed, and its removal completes.
    run.cluster.stop_node(LEAVING).await;
    run.decommissioned(PHASE, LEAVING).await;
    let old = run.removed_root(PHASE, LEAVING);
    run.vacant(PHASE, LEAVING, old).await;
    let old_disk = run.cluster.data.wals[&LEAVING].clone();
    run.cluster
        .new_fenced_disk(LEAVING, &format!("placement-move-fenced-{seed}-reused"));
    run.cluster.start_node(LEAVING).await;
    run.bound_anew(PHASE, LEAVING, old).await;
    let mut to = run.cluster.placed_voters(group);
    let out = to.iter().copied().max().expect("a voter");
    to.remove(&out);
    to.insert(LEAVING);
    run.accepted(PHASE, group, &to, None).await;
    run.settled(PHASE, group, &to).await;
    // The new life names an object of its own under `node-4/`.
    let deadline = madsim::time::Instant::now() + BOUND;
    let named = loop {
        let named: BTreeSet<String> = run
            .cluster
            .data
            .snapshot_names()
            .into_iter()
            .find(|names| names.node_id == LEAVING)
            .map(|names| names.named)
            .unwrap_or_default();
        if !named.is_empty() {
            break named;
        }
        if madsim::time::Instant::now() >= deadline {
            fail(
                &mut run.trace,
                I3,
                PHASE,
                format!("node {LEAVING}'s new life named no snapshot object of group {group}"),
            );
        }
        run.cluster.try_append(group, 1).await;
        if let Err(error) = run.cluster.placed_snapshot(group, LEAVING, STEP * 20).await {
            tracing::info!(error, group, "the new life built no snapshot yet");
        }
        run.observe(PHASE);
    };
    run.trace.push(SimEvent::SnapshotKeysRestored {
        node_id: LEAVING,
        keys: named.iter().cloned().collect(),
    });
    // The new life stops, and the old disk starts where it reaches no
    // peer: the meta group never answers its run.
    run.cluster.stop_node(LEAVING).await;
    let new_disk = run.cluster.data.wals[&LEAVING].clone();
    run.cluster.data.wals.insert(LEAVING, old_disk);
    run.excluded.insert(LEAVING);
    run.cluster.cut_data(LEAVING);
    defence_off_from_here(
        Defence::DeleteAfterRegistration,
        controls == Controls::ReturningOldDisk { defended: false },
    );
    run.cluster.start_node(LEAVING).await;
    run.step(PHASE);
    let names = [NodeNames::new(LEAVING, named.iter().cloned())];
    let deadline = madsim::time::Instant::now() + PREDECESSOR_WAIT + RETURN_WATCH;
    while madsim::time::Instant::now() < deadline {
        madsim::time::sleep(STEP).await;
        run.observe(PHASE);
        let view = run.cluster.data.snapshot_bucket().key_view();
        if let Err(violation) = check_named_keys_exist(&view, &names) {
            fail(
                &mut run.trace,
                NAMED_KEYS_EXIST,
                PHASE,
                violation.to_string(),
            );
        }
    }
    defence_off_from_here(Defence::DeleteAfterRegistration, false);
    run.cluster.stop_node(LEAVING).await;
    run.cluster.data.wals.insert(LEAVING, new_disk);
    run.cluster.heal_data(LEAVING);
    run.excluded.remove(&LEAVING);
    run.cluster.start_node(LEAVING).await;
    run.step("new_life_back");
    run.settle_and_read().await;
    run.finish()
}
