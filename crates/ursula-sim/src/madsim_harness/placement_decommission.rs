//! The decommission shape of the `placement-move` family (design 8 and
//! 15.2): four placed nodes at RF 3 with meta voters `{1,2,3}`, so node 4,
//! the one node that is no meta voter, is the node a run decommissions.
//! Every node runs the production snapshot driver over the in-memory
//! snapshot store, each process's store behind its node fence, and node 4
//! snapshots each group it is a voter of before it leaves, so its directory
//! holds objects for the removed-node sweep. A run, with its draws from the
//! seed:
//!
//! - on half the seeds first moves group 0 onto node 4, so node 4 is a
//!   voter of both groups and its decommission moves both;
//! - kills node 4 for good before its decommission (a dead decommission)
//!   on half the seeds, and keeps it running on the others (a live one);
//! - on half the seeds cordons node 4 first, as `ursulactl node
//!   decommission` cordons every node it names, then decommissions it
//!   through the meta group. The meta leader's production decommission
//!   driver moves node 4's groups away one at a time, and the kernel
//!   removes the node at its last group's converge. On a third of the seeds
//!   the meta leader is killed as soon as the decommission applied, and on
//!   a third once a group of node 4 moves;
//! - once node 4 is removed and every group settled, a live node 4 closes
//!   every replica (I13);
//! - kills the meta leader once node 4 is removed, so a new meta leadership
//!   begins, and a leadership sweeps at its start and then every 10
//!   minutes. On three quarters of the seeds the new leader's sweep holds
//!   at a drawn store call, the LIST of a group's objects or before or
//!   after a DELETE, and that leader is killed there too. The leader after
//!   it sweeps node 4's directory empty (I5);
//! - restarts node 4 with its disk before or after the sweep. While its
//!   objects remain it opens what it holds, its left marks gone with its
//!   process, and closes each once its group answered (I13, which fails a
//!   restart that opens nothing). Once they are swept it keeps closed for
//!   5 s every replica whose snapshot object the sweep took, since its
//!   restore finds the object gone (I13);
//! - and every group settles live on its placement and every voter reads
//!   every acknowledged append (I3, I1).
//!
//! Besides the family's checks, the removal checks
//! ([`super::placed_removals`]) and the snapshot checks
//! ([`super::snapshot_checks`]) run at every observation. The snapshot
//! checks leave out the removed node: its records still name the objects
//! the sweep took, and its restart reads one. A sweep of another node's
//! named object still fails `snapshot_named_keys_exist` and I8's sweep
//! half. At the run's end the removal checks fail what judged nothing of
//! the removed node.

use std::time::Duration;

use ursula_config::WalFsync;
use ursula_control::ControlApplied;
use ursula_control::ControlError;
use ursula_control::LifecycleChange;
use ursula_proto::placement::NodeLifecycle;
use ursula_raft::meta::handle::MetaWriteError;
use ursula_runtime::snapshot_harness::Gate;
use ursula_runtime::snapshot_harness::SnapshotStoreFaults;
use ursula_runtime::snapshot_harness::StoreOp;

use super::SimEvent;
use super::SplitMix64;
use super::ThreeNodeRaftSimOutcome;
use super::journal_cluster::DriverPlan;
use super::journal_cluster::JOURNAL_GROUPS;
use super::journal_cluster::SnapshotStorePlan;
use super::journal_cluster::set_snapshot_drivers;
use super::journal_cluster::set_snapshot_store;
use super::meta_checks::BOUND;
use super::meta_checks::STEP;
use super::meta_checks::fail;
use super::meta_cluster::Hosting;
use super::placed_hosting::I3;
use super::placed_hosting::I5;
use super::placed_removals::I13;
use super::placement_move::META_VOTERS;
use super::placement_move::MOVE_BOUND;
use super::placement_move::MoveDraw;
use super::placement_move::Run;
use super::placement_move::TEARDOWN_BOUND;
use super::placement_move::claims;
use super::snapshot_checks::SnapshotChecks;

/// The nodes of a run, and the one it decommissions.
const NODES: u64 = 4;
const LEAVING: u64 = 4;
/// Mixed into the seed of the snapshot bucket's draws.
const SALT: u64 = 0x6465_636f_6d6d_6973;
/// The snapshot drivers: a pass every 100 ms, four entries kept below a
/// snapshot, so a new voter installs one, and no removal due on its age.
const DRIVERS: DriverPlan = DriverPlan {
    max_age: Duration::from_secs(600),
    keep: 4,
    interval: Duration::from_millis(100),
    permits: 1,
};
/// `raft.install_snapshot_timeout` of every node.
const INSTALL_TIMEOUT: Duration = Duration::from_secs(1);
/// How long node 4's removal may take: a move of each of its groups, one
/// at a time, and a meta election.
const REMOVAL_BOUND: Duration = Duration::from_secs(100);
/// How long a removed node restarted after the sweep is watched for a
/// replica it opens.
const KEPT_CLOSED: Duration = Duration::from_secs(5);

/// When the meta leader is killed during the decommission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetaKill {
    /// As soon as the decommission applied.
    Decommissioned,
    /// Once a group of the node moves.
    Moving,
}

/// The store call a new meta leader's sweep holds at before it is killed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SweepHold {
    /// Before the LIST of the removed node's objects of a group.
    List,
    /// Before a DELETE.
    BeforeDelete,
    /// After a DELETE, before the sweep sees its answer.
    AfterDelete,
}

impl SweepHold {
    /// The gate of the hold on `node`'s store.
    fn gate(self, node: u64) -> Gate {
        match self {
            Self::List => Gate::before(node, StoreOp::ListNodeGroup),
            Self::BeforeDelete => Gate::before(node, StoreOp::DeleteNodeObject),
            Self::AfterDelete => Gate::after(node, StoreOp::DeleteNodeObject),
        }
    }
}

/// When the removed node restarts with its disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Restart {
    BeforeSweep,
    AfterSweep,
}

/// What a decommission run draws.
#[derive(Debug, Clone, Copy)]
struct Draw {
    /// Node 4 dies before its decommission.
    dead: bool,
    /// Group 0 first moves onto node 4.
    both: bool,
    /// Node 4 is cordoned before its decommission.
    cordon: bool,
    meta_kill: Option<MetaKill>,
    sweep_hold: Option<SweepHold>,
    restart: Restart,
}

impl Draw {
    fn drawn(draws: &mut SplitMix64) -> Self {
        let dead = draws.next_bounded(2) == 0;
        let both = draws.next_bounded(2) == 0;
        let cordon = draws.next_bounded(2) == 0;
        let meta_kill = match draws.next_bounded(3) {
            0 => None,
            1 => Some(MetaKill::Decommissioned),
            _ => Some(MetaKill::Moving),
        };
        let sweep_hold = match draws.next_bounded(4) {
            0 => None,
            1 => Some(SweepHold::List),
            2 => Some(SweepHold::BeforeDelete),
            _ => Some(SweepHold::AfterDelete),
        };
        let restart = if draws.next_bounded(2) == 0 {
            Restart::BeforeSweep
        } else {
            Restart::AfterSweep
        };
        Self {
            dead,
            both,
            cordon,
            meta_kill,
            sweep_hold,
            restart,
        }
    }
}

/// The decommission shape's run (see the module docs), under the data
/// policy `fsync`, with its draws from `draws`.
pub(super) async fn decommission_family(
    seed: u64,
    draws: &mut SplitMix64,
    fsync: WalFsync,
) -> ThreeNodeRaftSimOutcome {
    let draw = Draw::drawn(draws);
    set_snapshot_drivers(Some(DRIVERS));
    set_snapshot_store(Some(SnapshotStorePlan {
        seed: seed ^ SALT,
        faults: SnapshotStoreFaults::none(),
        install_timeout: INSTALL_TIMEOUT,
    }));
    let mut run = Run::start(seed, NODES, fsync, claims(NODES), Hosting::Placed).await;
    // The cluster read them when it was provisioned.
    set_snapshot_drivers(None);
    set_snapshot_store(None);
    run.snapshots = Some(SnapshotChecks::default());
    run.trace.push(SimEvent::FaultApplied {
        phase: format!("decommission node {LEAVING}: {draw:?}"),
    });
    run.formed("formed").await;
    run.learned("formed").await;
    run.bootstrapped("formed").await;
    run.append_data("formed").await;
    run.cluster.groups_live(&mut run.trace, "groups_live").await;
    run.step("groups_live");
    if draw.both {
        run.move_group(0, MoveDraw::ONE).await;
    }
    run.snapshot_groups_of(LEAVING).await;
    run.decommission(LEAVING, draw).await;
    if !draw.dead {
        run.closes(LEAVING).await;
    }
    if draw.restart == Restart::BeforeSweep {
        run.restart_removed(LEAVING).await;
    }
    run.sweep(LEAVING, draw.sweep_hold).await;
    if draw.restart == Restart::AfterSweep {
        run.restart_removed(LEAVING).await;
    }
    run.settle_and_read().await;
    run.removals.judged_the_removals(&mut run.trace, "finished");
    run.finish()
}

impl Run {
    /// Builds a snapshot of each group `node` is a voter of on its replica,
    /// as an operator's `POST /snapshot` does, after an append, until the
    /// bucket holds an object of `node` of the group.
    async fn snapshot_groups_of(&mut self, node: u64) {
        const PHASE: &str = "snapshot";
        let groups: Vec<u32> = JOURNAL_GROUPS
            .iter()
            .copied()
            .filter(|group| self.cluster.placed_voters(*group).contains(&node))
            .collect();
        for group in groups {
            let deadline = madsim::time::Instant::now() + BOUND;
            while !self.cluster.objects_of(node).contains_key(&group) {
                if madsim::time::Instant::now() >= deadline {
                    fail(
                        &mut self.trace,
                        I5,
                        PHASE,
                        format!("node {node} built no snapshot object of group {group}"),
                    );
                }
                self.cluster.try_append(group, 1).await;
                if let Err(error) = self.cluster.placed_snapshot(group, node, STEP * 20).await {
                    tracing::info!(error, group, node, "no snapshot built yet");
                }
                self.observe(PHASE);
            }
        }
        self.step(PHASE);
    }

    /// Submits `change` of `node`'s lifecycle through the other running
    /// nodes in turn, as an operator retries a write that did not apply,
    /// until one applies.
    async fn set_lifecycle(&mut self, phase: &str, node: u64, change: LifecycleChange) {
        let deadline = madsim::time::Instant::now() + BOUND;
        let via: Vec<u64> = self
            .cluster
            .running()
            .into_iter()
            .filter(|running| *running != node)
            .collect();
        for running in via.into_iter().cycle() {
            match self.cluster.submit_lifecycle(running, node, change).await {
                Ok(ControlApplied::LifecycleChanged { .. }) => return,
                Ok(answer) => {
                    panic!("node {running}: {change:?} of node {node} answered {answer:?}")
                }
                // A decommission that applied unanswered may have removed
                // the node since.
                Err(MetaWriteError::Refused(ControlError::NodeRemoved { .. }))
                    if change == LifecycleChange::Decommission => {}
                Err(MetaWriteError::Refused(error)) => {
                    panic!("{change:?} of node {node} was refused: {error}")
                }
                Err(_unanswered) if madsim::time::Instant::now() < deadline => {
                    madsim::time::sleep(STEP).await;
                    self.observe(phase);
                    continue;
                }
                Err(error) => panic!("{change:?} of node {node} did not apply: {error}"),
            }
            return;
        }
    }

    /// The node whose meta replica leads, once one does.
    async fn meta_leader(&mut self, phase: &str) -> u64 {
        self.until(phase, I3, BOUND, |cluster| {
            cluster
                .meta_leader()
                .is_none()
                .then(|| "meta has no running leader".to_owned())
        })
        .await;
        self.cluster.meta_leader().unwrap_or(0)
    }

    /// Kills the meta leader `leader`'s process and restarts it once
    /// another meta voter leads, so a new meta leadership begins. Answers
    /// the new leader.
    async fn kill_meta_leader(&mut self, phase: &str, leader: u64) -> u64 {
        self.trace.push(SimEvent::FaultApplied {
            phase: format!("{phase}: the meta leader {leader} is killed"),
        });
        self.cluster.stop_node(leader).await;
        self.until(phase, I3, BOUND, move |cluster| {
            cluster
                .meta_leader()
                .is_none_or(|next| next == leader)
                .then(|| format!("no meta voter took over from the killed meta leader {leader}"))
        })
        .await;
        let next = self.cluster.meta_leader().unwrap_or(0);
        self.cluster.start_node(leader).await;
        self.step(phase);
        next
    }

    /// Decommissions `node` as `draw` has it, and waits until the latest
    /// view shows it removed.
    async fn decommission(&mut self, node: u64, draw: Draw) {
        const PHASE: &str = "decommission";
        if draw.dead {
            self.cluster.stop_node(node).await;
            self.step("leaving_node_died");
        }
        if draw.cordon {
            self.set_lifecycle(PHASE, node, LifecycleChange::Cordon)
                .await;
        }
        self.set_lifecycle(PHASE, node, LifecycleChange::Decommission)
            .await;
        self.step("decommissioned");
        match draw.meta_kill {
            None => {}
            Some(MetaKill::Decommissioned) => {
                let leader = self.meta_leader(PHASE).await;
                self.kill_meta_leader(PHASE, leader).await;
            }
            Some(MetaKill::Moving) => {
                let moving = self
                    .within(PHASE, BOUND, |cluster| {
                        JOURNAL_GROUPS
                            .iter()
                            .any(|group| cluster.moving(*group).is_some())
                    })
                    .await;
                if moving {
                    let leader = self.meta_leader(PHASE).await;
                    self.kill_meta_leader(PHASE, leader).await;
                }
            }
        }
        self.until(PHASE, I3, REMOVAL_BOUND, move |cluster| {
            let lifecycle = cluster.lifecycle(node);
            (lifecycle != Some(NodeLifecycle::Removed)).then(|| {
                format!("node {node} is {lifecycle:?}, not removed, after its decommission")
            })
        })
        .await;
        self.step("node_removed");
        for group in JOURNAL_GROUPS {
            let voters = self.cluster.placed_voters(group);
            self.settled(PHASE, group, &voters).await;
        }
        self.append_data("node_removed").await;
    }

    /// I13's close half: within the bound, the removed `node` holds no
    /// replica open, each group having answered that it is no member.
    async fn closes(&mut self, node: u64) {
        self.until("removed_closes", I13, TEARDOWN_BOUND, move |cluster| {
            cluster.removed_open(node)
        })
        .await;
        self.removals.closed(node);
    }

    /// Restarts the removed `node` with its disk, its process killed first
    /// if it runs. The step names what the sweep had done when the restart
    /// began: `removed_node_restarted` with no DELETE of the sweep judged,
    /// `removed_node_restarted_mid_sweep` with some and objects left, and
    /// `removed_node_restarted_swept` with its directory empty.
    ///
    /// While its objects remain, I13 waits until it opens a replica, since
    /// its new process holds no left mark, so a closed log counts as a
    /// voter until its group answers (S10), and then until it holds
    /// nothing open. Once they are gone, it keeps closed ([`Self::kept_closed`]).
    async fn restart_removed(&mut self, node: u64) {
        const PHASE: &str = "removed_restart";
        self.observe(PHASE);
        let swept = self.removals.swept(node);
        let holding = !self.cluster.objects_of(node).is_empty();
        let starts = self.cluster.replica_starts(node);
        if self.cluster.runtimes.contains_key(&node) {
            self.cluster.stop_node(node).await;
        }
        self.cluster.start_node(node).await;
        self.step(match (swept, holding) {
            (0, _) => "removed_node_restarted",
            (_, true) => "removed_node_restarted_mid_sweep",
            (_, false) => "removed_node_restarted_swept",
        });
        if holding {
            self.until(PHASE, I13, BOUND, move |cluster| {
                (cluster.replica_starts(node) == starts).then(|| {
                    format!(
                        "removed node {node}, restarted with its disk and its snapshot objects, \
                         opened none of the replicas it holds"
                    )
                })
            })
            .await;
            self.closes(node).await;
        } else {
            self.kept_closed(node).await;
            self.removals.closed(node);
        }
    }

    /// I13 for a removed `node` restarted after the sweep: for a while it
    /// opens no replica, since each one's snapshot object is gone.
    async fn kept_closed(&mut self, node: u64) {
        let deadline = madsim::time::Instant::now() + KEPT_CLOSED;
        while madsim::time::Instant::now() < deadline {
            if let Some(open) = self.cluster.removed_open(node) {
                fail(&mut self.trace, I13, "kept_closed", open);
            }
            madsim::time::sleep(STEP).await;
            self.observe("kept_closed");
        }
    }

    /// The sweep of the removed `node`: the meta leader is killed, so a new
    /// leadership sweeps at its start. With `hold`, that leader's sweep
    /// holds at the store call and it is killed there too. Then I5's
    /// removal half: within the bound, `node`'s directory is empty.
    async fn sweep(&mut self, node: u64, hold: Option<SweepHold>) {
        const PHASE: &str = "sweep";
        let leader = self.meta_leader(PHASE).await;
        // The next leader's store holds, whichever meta voter it is.
        let held: Vec<u64> = match hold {
            Some(_) => META_VOTERS
                .into_iter()
                .filter(|voter| *voter != leader && self.cluster.runtimes.contains_key(voter))
                .collect(),
            None => Vec::new(),
        };
        if let Some(hold) = hold {
            for voter in &held {
                self.cluster
                    .data
                    .snapshot_gates(*voter)
                    .hold(hold.gate(*voter));
            }
        }
        let next = self.kill_meta_leader(PHASE, leader).await;
        if let Some(hold) = hold {
            for voter in held.iter().filter(|voter| **voter != next) {
                self.cluster
                    .data
                    .snapshot_gates(*voter)
                    .open(hold.gate(*voter));
            }
            let gate = hold.gate(next);
            let arrived = self
                .within(PHASE, BOUND, move |cluster| {
                    cluster.runtimes.contains_key(&next)
                        && cluster.data.snapshot_gates(next).arrived(gate) > 0
                })
                .await;
            if arrived {
                self.trace.push(SimEvent::FaultApplied {
                    phase: format!("{PHASE}: node {next}'s sweep holds at {hold:?}"),
                });
                self.kill_meta_leader(PHASE, next).await;
            } else if self.cluster.runtimes.contains_key(&next) {
                self.cluster.data.snapshot_gates(next).open(gate);
            }
        }
        self.until(PHASE, I5, MOVE_BOUND, move |cluster| cluster.unswept(node))
            .await;
        self.removals.emptied(node);
        self.step("swept");
    }
}

#[cfg(test)]
mod tests {
    use crate::madsim_harness::SimEvent;
    use crate::madsim_harness::SimScenario;
    use crate::madsim_harness::ThreeNodeRaftSim;
    use crate::madsim_harness::ThreeNodeRaftSimConfig;
    use crate::madsim_harness::placement_move::Controls;
    use crate::madsim_harness::tests::sim_test_guard;

    const SCENARIO: SimScenario = SimScenario::PlacementMove;

    /// A window of a decommission run, as its trace shows it.
    #[derive(Debug, Clone, Copy)]
    enum Window {
        /// The meta leader killed while a group of node 4 moves.
        MovingKill,
        /// The new meta leader's sweep held at this store call, and that
        /// leader killed there.
        SweepHold(&'static str),
        /// Node 4 restarted at this step of the sweep.
        Restarted(&'static str),
    }

    impl Window {
        fn reached(self, phase: &str) -> bool {
            match self {
                Self::MovingKill => {
                    phase.starts_with("decommission: the meta leader ")
                        && phase.ends_with(" is killed")
                }
                Self::SweepHold(hold) => {
                    phase.starts_with("sweep: node ")
                        && phase.ends_with(&format!("'s sweep holds at {hold}"))
                }
                Self::Restarted(step) => phase == step,
            }
        }
    }

    /// Family seeds whose shape is the decommission, with the windows each
    /// must reach: 1120 a live one of both groups, the meta leader killed
    /// while a group moves and the sweep held before a DELETE, the node
    /// restarted after the sweep. 1138 a dead one of group 1, the sweep
    /// held before a DELETE, the node restarted before any DELETE of the
    /// sweep: no meta leadership begins between the removal and the
    /// sweep's kill, so none sweeps first. 1152 a live one of both groups,
    /// the sweep held before a LIST, the node restarted before the sweep.
    const DECOMMISSIONS: [(u64, &[Window]); 3] = [
        (1120, &[
            Window::MovingKill,
            Window::SweepHold("BeforeDelete"),
            Window::Restarted("removed_node_restarted_swept"),
        ]),
        (1138, &[
            Window::SweepHold("BeforeDelete"),
            Window::Restarted("removed_node_restarted"),
        ]),
        (1152, &[
            Window::SweepHold("List"),
            Window::Restarted("removed_node_restarted"),
        ]),
    ];

    /// The family's decommission seeds remove node 4, reach their windows
    /// and hold, the same way twice per seed.
    #[test]
    fn placement_move_decommission_seeds_replays() {
        let _guard = sim_test_guard();
        for (seed, windows) in DECOMMISSIONS {
            let run = || {
                ThreeNodeRaftSim::run_placement_move_with_controls_report(
                    ThreeNodeRaftSimConfig::new(
                        seed,
                        format!("ursula-sim-{seed}-{}", SCENARIO.slug()),
                    ),
                    Controls::Family,
                )
            };
            let first = run();
            let second = run();
            assert_eq!(first, second, "seed {seed}");
            let removed =
                first.outcome.trace.events.iter().any(
                    |event| matches!(event, SimEvent::FaultApplied { phase } if phase == "swept"),
                );
            let held = first
                .outcome
                .trace
                .events
                .iter()
                .any(|event| matches!(event, SimEvent::PlacementMoveMetaHeld { .. }));
            assert!(removed, "seed {seed}: node 4 is removed and swept");
            assert!(held, "seed {seed}: the run ends held");
            for window in windows {
                let reached = first.outcome.trace.events.iter().any(|event| {
                    matches!(event, SimEvent::FaultApplied { phase } if window.reached(phase))
                });
                assert!(reached, "seed {seed}: the run reaches {window:?}");
            }
        }
    }
}
