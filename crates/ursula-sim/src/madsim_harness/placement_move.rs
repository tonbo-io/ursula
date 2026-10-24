//! Scenario `placement-move` (design 15.2): the meta group on two to five
//! journal-backed nodes ([`MetaCluster`]), meta voters `{1,2,3}` (`{1,2}` on
//! two nodes), the other nodes meta learners, and two data groups that meta
//! seeds and every node hosts by placement: each node's hosting driver opens
//! what its journals hold and creates what a confirmed placement gives it
//! ([`super::placed_hosting`]), and each group's leader reconciles its
//! membership toward the placement and reports each convergence.
//!
//! A family run draws from the seed the data journals' policy (`always` or
//! `never`) and a shape: the meta faults on three or five nodes at RF 3,
//! followed on five nodes by moves, or moves alone on three nodes at RF 2,
//! on four nodes at RF 2 (a full replacement) or on two nodes at RF 1 with
//! node 1 drained, whose shed exemption as its group's only voter goes and
//! comes back as the move and its cancel change the group's placed voters,
//! or on four nodes at RF 3 a live or dead decommission of node 4 and the
//! removed-node sweep ([`super::placement_decommission`]). A meta run draws whether one node first claims other genesis values,
//! how a group's initializer restarts before its group formed, and the
//! order of the meta faults. Each meta run:
//!
//! - forms meta, on some seeds while the last node claims other genesis
//!   values until it restarts with the fixed claim (design 3.5): meta forms
//!   only once the claims agree (I12), and no node hosts a data replica
//!   before meta bootstrapped (the hosting check);
//! - on most seeds kills the initializer of a drawn group as soon as it
//!   learned the bootstrap, and restarts it after a process crash, with its
//!   run state lost (a regressed meta view) or with its meta journal
//!   deleted: every seeded group still forms on its seed voters (I15);
//! - registers every node, the others than `{1,2,3}` as meta learners
//!   (I11), waits for the bootstrapped view, and writes control commands
//!   and data appends throughout (I10);
//! - loses meta's quorum, two meta voters' processes killed, while every
//!   data group with a quorum keeps committing (I6), and restarts a node
//!   that serves such a group while meta has no quorum: its hosting driver
//!   opens what its journals hold and the node serves the group (I7);
//! - cuts the power of every meta voter under `always`, or of one under
//!   `never`, and finds every acknowledged control write on every meta
//!   replica (I10);
//! - discards the meta journal of one meta voter, which opens `Recovering`
//!   over its data history (design 3.2) and rejoins through the meta leader
//!   (I11), then loses the run state of the meta leader and one more meta
//!   voter, whose meta gates open `Recovering` and stall, and recovers meta
//!   by a plan through `ursulactl recover-group --target meta --accept-loss`'s
//!   loop, planner and sender (the meta plan check), after which every node
//!   learns the view again (I11), and then every data group those two
//!   stalled by a data plan through the same tool;
//! - waits until every group's leader reported its seed voters, so every
//!   running node's view shows every group `Live` (I3's live half).
//!
//! The moves ([`super::placed_moves`]), each to nodes that hold no role in
//! the group and no retirement of it, with keyed appends between steps:
//! on five nodes, on some seeds a move of each group at the move limit 1,
//! the second refused until the first converged, then of each group one
//! move of one
//! voter (two at RF 2 on four nodes), its outgoing voter the leader on some
//! seeds, and on some seeds the leader cut off on the data plane while it
//! moves, or the move cancelled or retargeted at a drawn time or while the
//! leader's shell holds a drawn step at its pause point. On some seeds the
//! node a converging move drops crashes while its teardown holds a drawn
//! step at its pause point: its process crashes, its power is cut, or (at
//! RF 3) its process crashes and its core journal's `journal.meta` is lost,
//! and the restarted node tears the replica down again or reports the log
//! it retired. After each move, on some seeds the group moves back onto the
//! voters it left (A to B to A), each returning node reopening its retired
//! log behind its gate, on half of those with the power of a returning node
//! cut once it runs the group again. Every group then settles live on its
//! placement, every replica without a role is torn down and every
//! retirement reported (I3, I5), a settled five-node cluster keeps its
//! leaderships (I14, narrowed: the balancer runs in crate `ursula`), and
//! every voter reads every acknowledged append back (I1). A run that moved
//! a group fails a check that judged nothing of it.
//!
//! A killed or discarded node ends its WAL run, so its next start is a new
//! run that decides again how its journals open. I12, I2, I3 and the
//! hosting checks (I2 to I4, I8, I9) are checked after each phase and at
//! every poll of the waits. The negative controls ([`Controls`]) each run a
//! script of their own with one defence off: the three meta ones over
//! static data groups, N3, the reconciler's N1, N5 and N9 and the
//! teardown's N4 and N6 over placed ones. N4, N5 and N9 turn theirs off
//! inside the script, at the step they judge, so their setup runs
//! defended.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use ursula_config::WalFsync;
use ursula_control::ControlApplied;
use ursula_control::ControlError;
use ursula_control::TeardownStep;
use ursula_proto::placement::GroupCount;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::StepKind;
use ursula_raft::JournalTuning;
use ursula_raft::meta::handle::MetaWriteError;
use ursula_stream::sim_controls::Defence;
use ursula_stream::sim_controls::set_off;

use super::SimEvent;
use super::SimTrace;
use super::SplitMix64;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::journal_cluster::JOURNAL_GROUPS;
use super::meta_checks;
use super::meta_checks::MetaChecks;
use super::meta_checks::STEP;
use super::meta_cluster::Hosting;
use super::meta_cluster::MetaCluster;
use super::meta_cluster::claim;
use super::meta_cluster::claim_with_factor;
use super::placed_hosting;
use super::placed_hosting::I3;
use super::placed_hosting::I5;
use super::placed_removals::RemovalChecks;
use super::placement_decommission::decommission_family;
use super::snapshot_checks::SnapshotChecks;

/// The most nodes a run starts, and the meta voters.
const NODES: u64 = 5;
pub(super) const META_VOTERS: [u64; 3] = [1, 2, 3];
/// How long a script watches for a second genesis.
const WATCH: Duration = Duration::from_secs(8);
/// How often the meta recovery observes the replicas again.
const PLAN_POLL: Duration = Duration::from_millis(250);
/// Mixed into the seed of a run's draws.
const SALT: u64 = 0x6d65_7461_6d6f_7665;
/// How long a move may take to settle: the new replica's creation from a
/// confirmed view, its catch-up and gate, and the steps, through a
/// partition's heal.
pub(super) const MOVE_BOUND: Duration = Duration::from_secs(40);
/// How long a family run waits for a leader to hold a drawn step. A leader
/// that confirmed its view earlier in its leadership never plans
/// `ConfirmView`, and the move then ends without a hold.
const HELD_BOUND: Duration = Duration::from_secs(10);
/// How long a partitioned leader stays cut off.
const CUT: Duration = Duration::from_secs(2);
/// How long a settled cluster is watched for a leadership change (I14).
const SETTLED: Duration = Duration::from_secs(3);
/// How long the teardowns of a settled group may take: each of its two
/// asks may wait one ask interval for the next answer.
pub(super) const TEARDOWN_BOUND: Duration = Duration::from_secs(100);
/// How long N4's script watches for the teardown it released to retire
/// the log of a voter.
const RETIRE_WATCH: Duration = Duration::from_secs(20);

/// Which run a scenario makes, and which meta defence it turns off.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum Controls {
    /// The family run, every defence on.
    #[default]
    Family,
    /// Meta voters lose everything while the learners keep meta history,
    /// with the genesis probe of every peer on (`true`) or off.
    WipedMetaVoters { defended: bool },
    /// Every meta voter's meta journal is discarded while data holds
    /// history, with the `Recovering` opening of design 3.2 on (`true`) or
    /// off.
    MetaDiscardedOverData { defended: bool },
    /// Every node loses its power under data `never`, with the meta
    /// journal's own `always` on (`true`) or off.
    PowerLossUnderNever { defended: bool },
    /// Placed groups form on five nodes, with the seed role read from the
    /// placement view (N3) on (`true`) or off.
    SeededFormation { defended: bool },
    /// A leader's voter change held while the move is cancelled and the
    /// leadership goes to another voter and back, with the step's CAS (N1)
    /// on (`true`) or off.
    StaleStep { defended: bool },
    /// A leader whose meta replica lags at a move's first target, after a
    /// retarget converged without it, with voter changes waiting for a
    /// confirmed view (N5) on (`true`) or off.
    LaggingLeader { defended: bool },
    /// A leader whose report is held while it is deposed and cut off on the
    /// data plane, and its successor, whose meta replica lags, changes the
    /// membership, with the leadership proof (N9) on (`true`) or off.
    DeposedReport { defended: bool },
    /// A dropped voter whose teardown holds before it closes the replica,
    /// while a meta loss is accepted and the group makes the node its voter
    /// again, with retirements waiting for the group's answers (N4) on
    /// (`true`) or off.
    ReplacedMidTeardown { defended: bool },
    /// A group moved off a node and back onto it (A to B to A), the node
    /// cut off on the data plane and its power cut before its reopened log
    /// took an entry, with each retirement in a new log generation (N6) on
    /// (`true`) or off.
    ReopenedRetiredLog { defended: bool },
    /// A removed node id used again while a voter cut off from the group
    /// keeps the membership that named the old life, with the empty
    /// replica's screen on (`true`) or off ([`super::reused_id`]).
    ReusedIdVote { defended: bool },
    /// A removed node's old disk started again, cut off from every peer,
    /// after a new life of its id stored an object, with snapshot DELETEs
    /// waiting for the registration on (`true`) or off
    /// ([`super::reused_id`]).
    ReturningOldDisk { defended: bool },
}

/// The run's defences, reset when the run ends.
struct Switched;

impl Switched {
    fn apply(controls: Controls) -> Self {
        set_off(
            Defence::GenesisEveryPeer,
            controls == Controls::WipedMetaVoters { defended: false },
        );
        set_off(
            Defence::MetaOverDataHistory,
            controls == Controls::MetaDiscardedOverData { defended: false },
        );
        set_off(
            Defence::MetaAlwaysSync,
            controls == Controls::PowerLossUnderNever { defended: false },
        );
        set_off(
            Defence::InitializeOnlySeeded,
            controls == Controls::SeededFormation { defended: false },
        );
        set_off(
            Defence::MembershipCas,
            controls == Controls::StaleStep { defended: false },
        );
        set_off(
            Defence::LogGeneration,
            controls == Controls::ReopenedRetiredLog { defended: false },
        );
        // These go off inside their scripts, at the step they judge
        // ([`defence_off_from_here`]), so the setup runs defended.
        set_off(Defence::PlacementBarrier, false);
        set_off(Defence::ConvergeLeadershipProof, false);
        set_off(Defence::RetireAnswer, false);
        set_off(Defence::EmptyReplicaVote, false);
        set_off(Defence::DeleteAfterRegistration, false);
        Self
    }
}

/// Turns `defence` off from here to the run's end when `off`, so a
/// control's undefended run differs from its defended one only inside its
/// script.
pub(super) fn defence_off_from_here(defence: Defence, off: bool) {
    set_off(defence, off);
}

impl Drop for Switched {
    fn drop(&mut self) {
        set_off(Defence::GenesisEveryPeer, false);
        set_off(Defence::MetaOverDataHistory, false);
        set_off(Defence::MetaAlwaysSync, false);
        set_off(Defence::InitializeOnlySeeded, false);
        set_off(Defence::MembershipCas, false);
        set_off(Defence::PlacementBarrier, false);
        set_off(Defence::ConvergeLeadershipProof, false);
        set_off(Defence::RetireAnswer, false);
        set_off(Defence::LogGeneration, false);
        set_off(Defence::EmptyReplicaVote, false);
        set_off(Defence::DeleteAfterRegistration, false);
    }
}

/// How a family run restarts a group's initializer once it learned the
/// bootstrap, before its group formed (I15).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InitializerRestart {
    /// Its process crashes, and its meta replica replays what it held.
    Crash,
    /// It loses its run state: every journal opens `Recovering`, and its
    /// meta view stays regressed until a meta leader's barrier.
    RunStateLost,
    /// Its meta journal, `raft-log/meta/`, is deleted.
    MetaDeleted,
}

/// The meta faults of a family run, in the order the seed draws.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetaFault {
    QuorumLoss,
    PowerLoss,
    Plan,
}

/// The shape of a family run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// `nodes` nodes at RF 3: the meta faults, then on five nodes the moves.
    Meta { nodes: u64 },
    /// Three nodes at RF 2: each group moves one voter to the third node.
    Rf2,
    /// Four nodes at RF 2: one group moves both voters, the other one.
    FullReplacement,
    /// Two nodes at RF 1 with node 1 drained: each group moves its voter to
    /// the other node, the first with a cancel while the step that replaces
    /// the voter is held.
    Rf1,
    /// Four nodes at RF 3: node 4 is decommissioned, removed and swept
    /// ([`super::placement_decommission`]).
    Decommission,
}

/// When a move is cancelled or retargeted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum At {
    /// This long after the move was accepted.
    After(Duration),
    /// While the leader's shell holds a step of this kind at its pause
    /// point, which it runs as planned once released.
    Held(StepKind),
}

/// How a move ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
    Converges,
    /// Cancelled, back to the move's source.
    Cancel(At),
    /// Retargeted to another fresh node in place of the first.
    Retarget(At),
}

/// How a family run crashes a node whose teardown holds a step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TeardownCrash {
    /// Its process crashes.
    Process,
    /// Its power is cut, under the run's data policy.
    Power,
    /// Its process crashes and its core journal's metadata file
    /// (`journal.meta`) is lost.
    MetadataLost,
}

/// A crash of the node a converging move drops, while its teardown of the
/// group holds `step`: its restart starts the teardown again from its
/// first ask, or reports a log it retired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TeardownFault {
    step: TeardownStep,
    crash: TeardownCrash,
}

impl TeardownFault {
    /// On a third of the draws, a fault at a drawn step, with a lost
    /// `journal.meta` among the crashes only when `metadata_loss`: a node
    /// that loses it opens the group's other replicas `Recovering`, which
    /// an RF 2 group cannot spare.
    fn drawn(draws: &mut SplitMix64, metadata_loss: bool) -> Option<Self> {
        if draws.next_bounded(3) != 0 {
            return None;
        }
        let steps = [
            TeardownStep::Unregister,
            TeardownStep::AwaitIdle,
            TeardownStep::AskAgain,
            TeardownStep::Retire,
            TeardownStep::ForgetMetrics,
        ];
        let index = usize::try_from(draws.next_bounded(5)).unwrap_or(0);
        let crash = match draws.next_bounded(3) {
            0 => TeardownCrash::Process,
            1 => TeardownCrash::Power,
            _ if metadata_loss => TeardownCrash::MetadataLost,
            _ => TeardownCrash::Process,
        };
        Some(Self {
            step: steps
                .get(index)
                .copied()
                .unwrap_or(TeardownStep::Unregister),
            crash,
        })
    }
}

/// One move of a group's voters to fresh nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MoveDraw {
    /// Every voter moves, not one.
    full: bool,
    /// The leader is the voter that moves out.
    outgoing_leader: bool,
    /// The leader is cut off on the data plane while it moves.
    partition: bool,
    ending: Ending,
    /// A crash of the node a converging move drops, mid-teardown.
    teardown: Option<TeardownFault>,
}

impl MoveDraw {
    /// A plain move of one voter, not the leader.
    pub(super) const ONE: Self = Self {
        full: false,
        outgoing_leader: false,
        partition: false,
        ending: Ending::Converges,
        teardown: None,
    };

    /// A move with every option drawn, for a group with `fresh` fresh
    /// nodes, a lost `journal.meta` among its teardown crashes only when
    /// `metadata_loss`.
    fn drawn(draws: &mut SplitMix64, fresh: usize, metadata_loss: bool) -> Self {
        let partition = draws.next_bounded(4) == 0;
        let outgoing_leader = draws.next_bounded(2) == 0;
        let at = if partition || draws.next_bounded(2) == 0 {
            let millis = [0, 200, 500, 1_000, 2_000];
            let index = usize::try_from(draws.next_bounded(5)).unwrap_or(0);
            At::After(Duration::from_millis(
                millis.get(index).copied().unwrap_or(0),
            ))
        } else {
            // An outgoing leader hands off before any voter change, so it
            // holds only the steps it takes first.
            let kinds: &[StepKind] = if outgoing_leader {
                &[StepKind::AddLearners, StepKind::ConfirmView]
            } else {
                &[
                    StepKind::AddLearners,
                    StepKind::ConfirmView,
                    StepKind::ChangeVoters,
                    StepKind::ReportConverged,
                ]
            };
            let bound = u64::try_from(kinds.len()).unwrap_or(1);
            let index = usize::try_from(draws.next_bounded(bound)).unwrap_or(0);
            At::Held(kinds.get(index).copied().unwrap_or(StepKind::AddLearners))
        };
        let ending = match draws.next_bounded(5) {
            0 | 1 => Ending::Converges,
            2 => Ending::Cancel(at),
            _ if fresh >= 2 => Ending::Retarget(at),
            _ => Ending::Cancel(at),
        };
        Self {
            full: false,
            outgoing_leader,
            partition,
            ending,
            teardown: TeardownFault::drawn(draws, metadata_loss),
        }
    }
}

pub(super) struct Run {
    pub(super) cluster: MetaCluster,
    pub(super) trace: SimTrace,
    checks: MetaChecks,
    /// The removed nodes' checks, at every observation.
    pub(super) removals: RemovalChecks,
    /// The snapshot object checks, when the nodes run the snapshot store.
    pub(super) snapshots: Option<SnapshotChecks>,
    /// The nodes the snapshot checks also leave out for now: a node whose
    /// current disk is a removed life's ([`super::reused_id`]).
    pub(super) excluded: BTreeSet<u64>,
    rng: SplitMix64,
    pub(super) seed: u64,
}

impl Run {
    pub(super) async fn start(
        seed: u64,
        nodes: u64,
        fsync: WalFsync,
        claims: BTreeMap<u64, ursula_proto::placement::GenesisClaim>,
        hosting: Hosting,
    ) -> Self {
        let mut trace = SimTrace::default();
        trace.push(SimEvent::ClusterBuilt { seed });
        let cluster = MetaCluster::start(
            &format!("placement-move-{seed}"),
            JournalTuning::new(fsync),
            nodes,
            claims,
            hosting,
        )
        .await;
        Self::over(seed, trace, cluster)
    }

    /// [`Self::start`] for nodes hosting by placement behind the identity
    /// fence, as servers run ([`MetaCluster::start_fenced`]), each behind a
    /// disk lease of `lease`.
    pub(super) async fn start_fenced(
        seed: u64,
        nodes: u64,
        fsync: WalFsync,
        claims: BTreeMap<u64, ursula_proto::placement::GenesisClaim>,
        lease: Duration,
    ) -> Self {
        let mut trace = SimTrace::default();
        trace.push(SimEvent::ClusterBuilt { seed });
        let cluster = MetaCluster::start_fenced(
            &format!("placement-move-fenced-{seed}"),
            JournalTuning::new(fsync),
            nodes,
            claims,
            lease,
        )
        .await;
        Self::over(seed, trace, cluster)
    }

    fn over(seed: u64, trace: SimTrace, cluster: MetaCluster) -> Self {
        let mut run = Self {
            cluster,
            trace,
            checks: MetaChecks::default(),
            removals: RemovalChecks::default(),
            snapshots: None,
            excluded: BTreeSet::new(),
            rng: SplitMix64::new(seed ^ SALT),
            seed,
        };
        run.step("cluster_started");
        run
    }

    /// Records `phase` and checks.
    pub(super) fn step(&mut self, phase: &str) {
        self.trace.push(SimEvent::FaultApplied {
            phase: phase.to_owned(),
        });
        self.observe(phase);
    }

    /// The checks of every observation: the meta and hosting checks, the
    /// removed nodes' and, with a snapshot store, the snapshot objects',
    /// which leave out the removed nodes.
    pub(super) fn observe(&mut self, phase: &str) {
        self.checks.observe(&self.cluster, &mut self.trace, phase);
        self.removals.observe(&self.cluster, &mut self.trace, phase);
        if let Some(snapshots) = self.snapshots.as_mut() {
            let mut removed = self.cluster.removed_nodes();
            removed.extend(self.excluded.iter().copied());
            snapshots.step_except(&self.cluster.data, &mut self.trace, phase, &removed);
        }
    }

    /// Waits `wait`, checking every step.
    pub(super) async fn watch(&mut self, phase: &str, wait: Duration) {
        let deadline = madsim::time::Instant::now() + wait;
        while madsim::time::Instant::now() < deadline {
            madsim::time::sleep(STEP).await;
            self.observe(phase);
        }
    }

    fn nodes(&self) -> Vec<u64> {
        self.cluster.data.nodes().collect()
    }

    /// Every node registered and learns the view (I11).
    pub(super) async fn learned(&mut self, phase: &str) {
        let nodes = self.nodes();
        self.checks
            .registered_nodes_learn(&self.cluster, &mut self.trace, phase, &nodes)
            .await;
        self.step(phase);
    }

    /// Every node holds the bootstrapped view, before the first control
    /// write.
    pub(super) async fn bootstrapped(&mut self, phase: &str) {
        let nodes = self.nodes();
        self.checks
            .bootstrapped(&self.cluster, &mut self.trace, phase, &nodes)
            .await;
    }

    async fn write(&mut self, count: u32) {
        let via = self.cluster.running();
        self.checks.write_limits(&self.cluster, &via, count).await;
    }

    /// Every placed group forms on its seed voters within the bound (I15).
    pub(super) async fn formed(&mut self, phase: &str) {
        self.cluster.groups_form(&mut self.trace, phase).await;
        self.step(phase);
    }

    /// Two appends to every data group, each taken within the bound.
    pub(super) async fn append_data(&mut self, phase: &str) {
        for group in JOURNAL_GROUPS {
            if !self.cluster.try_append(group, 2).await {
                meta_checks::fail(
                    &mut self.trace,
                    placed_hosting::I1,
                    phase,
                    format!("data group {group} took no append within the bound"),
                );
            }
        }
        self.step(phase);
    }

    /// Kills the initializer of `group` as soon as its view shows the
    /// bootstrap, so its hosting driver may have created the group but the
    /// group has seldom formed, and restarts it `how`.
    async fn initializer_restart(&mut self, group: u32, how: InitializerRestart) {
        const PHASE: &str = "initializer_restart";
        let initializer = placed_hosting::initializer(&self.cluster.claims[&1], group);
        let deadline = madsim::time::Instant::now() + meta_checks::BOUND;
        while self.cluster.view(initializer).cluster().is_none() {
            if madsim::time::Instant::now() >= deadline {
                meta_checks::fail(
                    &mut self.trace,
                    meta_checks::I11,
                    PHASE,
                    format!("node {initializer} never learned the bootstrap"),
                );
            }
            madsim::time::sleep(STEP).await;
            self.observe(PHASE);
        }
        match how {
            InitializerRestart::Crash => self.cluster.stop_node(initializer).await,
            InitializerRestart::RunStateLost => self.cluster.lose_run_state(initializer).await,
            InitializerRestart::MetaDeleted => self.cluster.discard_meta(initializer).await,
        }
        self.cluster.start_node(initializer).await;
        self.step("initializer_restarted");
    }

    /// Two distinct meta voters, drawn.
    fn two_voters(&mut self) -> [u64; 2] {
        let spared = usize::try_from(self.rng.next_bounded(3)).unwrap_or(0);
        let mut victims = META_VOTERS
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != spared)
            .map(|(_, node_id)| *node_id);
        [
            victims.next().expect("a first victim"),
            victims.next().expect("a second victim"),
        ]
    }

    /// The meta leader and one more meta voter, drawn. A meta leader that
    /// kept its meta journal would keep its gated followers replicating,
    /// and no plan would be needed.
    fn leader_and_one_voter(&mut self) -> [u64; 2] {
        let leader = META_VOTERS.into_iter().find_map(|node_id| {
            let runtime = self.cluster.runtimes.get(&node_id)?;
            match runtime.status_reader().placement_status().meta {
                ursula_control::MetaReplicaStatus::Running {
                    leader: Some(leader),
                    ..
                } if META_VOTERS.contains(&leader.get()) => Some(leader.get()),
                _ => None,
            }
        });
        let Some(leader) = leader else {
            return self.two_voters();
        };
        let others: Vec<u64> = META_VOTERS
            .into_iter()
            .filter(|node_id| *node_id != leader)
            .collect();
        let other = usize::try_from(self.rng.next_bounded(2)).unwrap_or(0);
        [leader, others.get(other).copied().unwrap_or(leader)]
    }

    /// The last node claims another group count, then restarts with the
    /// fixed claim (design 3.5). Meanwhile no genesis forms meta, and no node
    /// hosts a data replica, which only a bootstrapped placement creates
    /// (the hosting check, at every step of the watch).
    async fn claim_mismatch(&mut self) {
        let last = self.nodes().last().copied().unwrap_or(NODES);
        let peers = u64::try_from(self.nodes().len()).unwrap_or(NODES);
        self.watch("claim_mismatch", Duration::from_secs(3)).await;
        self.checks.no_genesis(&mut self.trace, "claim_mismatch");
        let mismatch = self.cluster.runtimes[&1]
            .status()
            .genesis
            .borrow()
            .mismatch
            .clone();
        if !mismatch.contains_key(&NodeId::new(last)) {
            meta_checks::fail(
                &mut self.trace,
                meta_checks::I12,
                "claim_mismatch",
                format!("the genesis node names no mismatch of node {last}: {mismatch:?}"),
            );
        }
        self.cluster.stop_node(last).await;
        self.cluster.claims.insert(last, claim(peers, &META_VOTERS));
        self.cluster.start_node(last).await;
        self.step("claim_fixed");
    }

    /// Two meta voters killed: every data group with a quorum keeps
    /// committing (I6), and a node that serves such a group restarts and
    /// serves it while meta has no quorum (I7).
    async fn quorum_loss(&mut self) {
        let victims = self.two_voters();
        for victim in victims {
            self.cluster.stop_node(victim).await;
        }
        self.step("meta_quorum_lost");
        meta_checks::data_commits_without_meta(
            &mut self.cluster,
            &mut self.trace,
            "meta_quorum_lost",
        )
        .await;
        // The highest running node that is a seed voter of a group whose
        // seed voters keep a quorum.
        let restarted = self.cluster.running().into_iter().rev().find(|node_id| {
            JOURNAL_GROUPS.iter().any(|group| {
                self.cluster.seeds(*group).contains(node_id) && self.cluster.has_quorum(*group)
            })
        });
        if let Some(restarted) = restarted {
            self.cluster.stop_node(restarted).await;
            self.cluster.start_node(restarted).await;
            self.step("restarted_without_meta");
            self.cluster
                .serves_without_meta(&mut self.trace, "restarted_without_meta", restarted)
                .await;
            self.step("restarted_without_meta");
        }
        for victim in victims {
            self.cluster.start_node(victim).await;
        }
        self.step("meta_quorum_back");
        self.learned("meta_quorum_back").await;
        self.formed("meta_quorum_back").await;
        self.write(2).await;
        self.append_data("meta_quorum_back").await;
    }

    /// Every meta voter loses power under `always`, one under `never`, and
    /// every acknowledged control write survives (I10).
    async fn power_loss(&mut self, fsync: WalFsync) {
        let victims: Vec<u64> = match fsync {
            WalFsync::Always => META_VOTERS.to_vec(),
            WalFsync::Never => vec![self.two_voters()[0]],
        };
        self.power_loss_of(&victims).await;
        let nodes = self.nodes();
        self.checks
            .acknowledged_writes_survive(&self.cluster, &mut self.trace, "power_loss", &nodes)
            .await;
        self.learned("power_loss").await;
        self.formed("power_loss").await;
        self.write(2).await;
        self.append_data("power_loss").await;
    }

    async fn power_loss_of(&mut self, victims: &[u64]) {
        for victim in victims {
            self.cluster.stop_node(*victim).await;
        }
        for victim in victims {
            self.cluster.data.wals[victim]
                .power_loss_losing_unsynced()
                .await;
        }
        for victim in victims {
            self.cluster.start_node(*victim).await;
        }
        self.step("power_loss");
    }

    /// One meta voter's meta journal discarded: it opens `Recovering` over
    /// its data history (design 3.2) and rejoins through the meta leader (I11).
    /// Then the meta leader and one more meta voter lose their run state, so
    /// a majority of meta's gates open `Recovering`, where a replica neither
    /// votes nor campaigns, and stall. A meta plan recovers meta. Their data
    /// journals open `Recovering` too, so a data group whose seed voters
    /// they are most of stalls, and a data plan after the meta plan
    /// recovers it, as the operations order has it.
    async fn plan(&mut self) {
        let [discarded, _] = self.two_voters();
        self.cluster.discard_meta(discarded).await;
        self.cluster.start_node(discarded).await;
        self.step("meta_discarded");
        self.learned("meta_discarded").await;
        let victims = self.leader_and_one_voter();
        let survivor = META_VOTERS
            .into_iter()
            .find(|node_id| !victims.contains(node_id))
            .expect("a surviving meta voter");
        let before = meta_checks::history_of(&self.cluster, survivor)
            .expect("the surviving meta voter applied a history");
        self.accept_meta_loss(victims, &JOURNAL_GROUPS).await;
        let nodes = self.nodes();
        self.checks
            .plan_applied(&self.cluster, &mut self.trace, "meta_plan", &nodes, before)
            .await;
        // I11 after the plan. Whether the plan restarted a replica, and
        // whether its drivers and shell then still act (D11), is not
        // checked here.
        self.learned("meta_plan_applied").await;
        self.formed("meta_plan_applied").await;
        self.write(2).await;
        self.append_data("meta_plan_applied").await;
    }

    /// The meta leader and one more meta voter, `victims`, lose their run
    /// state, so their meta gates open `Recovering` and stall, and meta
    /// recovers by a plan through `ursulactl recover-group --target meta
    /// --accept-loss`: a meta loss accepted, whose history event drops every
    /// retirement. Then each data group of `stalled` that the loss stalled
    /// recovers by a data plan through the same tool.
    async fn accept_meta_loss(&mut self, victims: [u64; 2], stalled: &[u32]) {
        for victim in victims {
            self.cluster.lose_run_state(victim).await;
        }
        for victim in victims {
            self.cluster.start_node(victim).await;
        }
        self.step("run_state_lost");
        let nodes = self.nodes();
        self.recover_meta(&nodes).await;
        self.step("meta_plan");
        self.cluster
            .recover_data(&mut self.trace, "data_plans", stalled)
            .await;
        self.step("data_plans");
    }

    /// `ursulactl recover-group --target meta --accept-loss` over `nodes`:
    /// the tool's rounds, every `PLAN_POLL`, until its plan is satisfied.
    /// While a gate may still open the tool waits and observes again, as
    /// the planner asks: a gated meta replica reports its group stalled only
    /// after `RECOVERY_STALL_AFTER` without a leader barrier, so the wait is
    /// that and the bound.
    async fn recover_meta(&mut self, nodes: &[u64]) {
        let mut state = ursula_ctl::recovery::LoopState::default();
        let within = ursula_raft::RECOVERY_STALL_AFTER + meta_checks::BOUND;
        let deadline = madsim::time::Instant::now() + within;
        loop {
            match self.cluster.recover_meta_round(&mut state, nodes).await {
                Ok(Some(_satisfied)) => return,
                Ok(None) if madsim::time::Instant::now() < deadline => {
                    madsim::time::sleep(PLAN_POLL).await;
                    self.observe("meta_plan");
                }
                Ok(None) => {
                    let members = self.describe_meta(nodes);
                    meta_checks::fail(
                        &mut self.trace,
                        meta_checks::PLAN,
                        "meta_plan",
                        format!(
                            "no meta plan was satisfied within {within:?}: {state:?}; {members}"
                        ),
                    );
                }
                Err(error) => {
                    let members = self.describe_meta(nodes);
                    meta_checks::fail(
                        &mut self.trace,
                        meta_checks::PLAN,
                        "meta_plan",
                        format!("the meta recovery stopped: {error}; {state:?}; {members}"),
                    );
                }
            }
        }
    }

    /// What the recovery tool and each node's status show of meta on
    /// `nodes`, for a failure's message.
    fn describe_meta(&self, nodes: &[u64]) -> String {
        let members = self.cluster.plan_members(nodes);
        nodes
            .iter()
            .map(|node_id| {
                let status = self
                    .cluster
                    .runtimes
                    .get(node_id)
                    .map(|runtime| runtime.status_reader().placement_status().meta);
                let member = members
                    .iter()
                    .find(|member| member.node_id == *node_id)
                    .map(|member| {
                        format!(
                            "running {} gate {:?} leader {:?} last {:?} committed {:?} voters {:?} \
                             plan {:?}",
                            member.running,
                            member.gate,
                            member.current_leader,
                            member.log.last_log_index,
                            member.committed,
                            member.voter_configs,
                            member.plan,
                        )
                    });
                format!("node {node_id}: status {status:?}, member {member:?}")
            })
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// Waits until `ready` answers `None`, checking at every step, and
    /// fails `invariant` with its last answer after `bound`.
    pub(super) async fn until(
        &mut self,
        phase: &str,
        invariant: &str,
        bound: Duration,
        ready: impl Fn(&MetaCluster) -> Option<String>,
    ) {
        let deadline = madsim::time::Instant::now() + bound;
        loop {
            self.observe(phase);
            let Some(why) = ready(&self.cluster) else {
                return;
            };
            if madsim::time::Instant::now() >= deadline {
                meta_checks::fail(&mut self.trace, invariant, phase, why);
            }
            madsim::time::sleep(STEP).await;
        }
    }

    /// Whether `ready` holds within `bound`, checking at every step.
    pub(super) async fn within(
        &mut self,
        phase: &str,
        bound: Duration,
        ready: impl Fn(&MetaCluster) -> bool,
    ) -> bool {
        let deadline = madsim::time::Instant::now() + bound;
        loop {
            self.observe(phase);
            if ready(&self.cluster) {
                return true;
            }
            if madsim::time::Instant::now() >= deadline {
                return false;
            }
            madsim::time::sleep(STEP).await;
        }
    }

    /// Asks the meta group to move `group` to `to`, through the running
    /// nodes in turn but `avoid`, as an operator retries a write that did
    /// not apply, until the latest view targets `to`. The refusal of a
    /// command the meta group applied answers.
    async fn request(
        &mut self,
        phase: &str,
        group: u32,
        to: &BTreeSet<u64>,
        avoid: Option<u64>,
    ) -> Result<(), ControlError> {
        let deadline = madsim::time::Instant::now() + meta_checks::BOUND;
        let via: Vec<u64> = self
            .cluster
            .running()
            .into_iter()
            .filter(|node_id| Some(*node_id) != avoid)
            .collect();
        let mut turn = 0usize;
        loop {
            self.observe(phase);
            if self.cluster.target_voters(group).as_ref() == Some(to) {
                return Ok(());
            }
            if madsim::time::Instant::now() >= deadline {
                meta_checks::fail(
                    &mut self.trace,
                    I3,
                    phase,
                    format!("no move of group {group} to {to:?} applied within the bound"),
                );
            }
            let node_id = via.get(turn % via.len().max(1)).copied().unwrap_or(1);
            turn = turn.wrapping_add(1);
            match self.cluster.submit_move(node_id, group, to).await {
                Ok(ControlApplied::MoveAccepted { .. }) => {
                    let to = to.clone();
                    self.until(phase, I3, meta_checks::BOUND, move |cluster| {
                        (cluster.target_voters(group).as_ref() != Some(&to))
                            .then(|| format!("no view shows group {group}'s move to {to:?}"))
                    })
                    .await;
                    return Ok(());
                }
                Ok(answer) => panic!("node {node_id}: a move of group {group} answered {answer:?}"),
                // The view the command named its placement from was behind.
                Err(MetaWriteError::Refused(ControlError::StalePlacement { .. })) => {}
                Err(MetaWriteError::Refused(error)) => return Err(error),
                // Not applied, or it may have: the view shows which.
                Err(_unanswered) => {}
            }
            madsim::time::sleep(STEP).await;
        }
    }

    /// [`Self::request`] of a move the meta group must accept.
    pub(super) async fn accepted(
        &mut self,
        phase: &str,
        group: u32,
        to: &BTreeSet<u64>,
        avoid: Option<u64>,
    ) {
        if let Err(error) = self.request(phase, group, to, avoid).await {
            panic!("the move of group {group} to {to:?} was refused: {error}");
        }
    }

    /// Sets the move limit, as an operator retries a write that did not
    /// apply.
    async fn set_limit(&mut self, limit: u32) {
        let deadline = madsim::time::Instant::now() + meta_checks::BOUND;
        for node_id in self.cluster.running().into_iter().cycle() {
            match self.cluster.submit_limit(node_id, limit).await {
                Ok(ControlApplied::MoveLimitSet { .. }) => return,
                Ok(answer) => panic!("node {node_id}: SetMoveLimit answered {answer:?}"),
                Err(_not_now) if madsim::time::Instant::now() < deadline => {
                    madsim::time::sleep(STEP).await;
                }
                Err(error) => panic!("the move limit {limit} was not set: {error}"),
            }
        }
    }

    /// I3 for one group: it settles live on `voters` within the move bound,
    /// and its replicas without a role are torn down.
    pub(super) async fn settled(&mut self, phase: &str, group: u32, voters: &BTreeSet<u64>) {
        let voters = voters.clone();
        self.until(phase, I3, MOVE_BOUND, move |cluster| {
            cluster.not_settled(group).or_else(|| {
                let placed = cluster.placed_voters(group);
                (placed != voters)
                    .then(|| format!("group {group} settled on {placed:?}, not on {voters:?}"))
            })
        })
        .await;
        self.torn_down(phase, group).await;
    }

    /// I3's teardown half and I5's record half for one group: no running
    /// node holds a log of it without a role, and every retirement of it
    /// is reported.
    pub(super) async fn torn_down(&mut self, phase: &str, group: u32) {
        self.until(phase, I3, TEARDOWN_BOUND, move |cluster| {
            cluster.unplaced_replica(group)
        })
        .await;
        self.until(phase, I5, TEARDOWN_BOUND, move |cluster| {
            cluster.unreported(group)
        })
        .await;
    }

    /// Waits until `node_id`'s teardown of `group` holds its step `step`,
    /// then crashes the node as `crash` has it. A teardown that does not
    /// reach the step is released, and the settle that follows judges it.
    async fn crash_mid_teardown(&mut self, group: u32, node_id: u64, fault: TeardownFault) {
        const PHASE: &str = "teardown_crash";
        let TeardownFault { step, crash } = fault;
        let held = self
            .within(PHASE, TEARDOWN_BOUND, move |cluster| {
                cluster.teardown_held(node_id, group, step)
            })
            .await;
        if !held {
            self.cluster.release_teardown(node_id, group, step);
            return;
        }
        self.trace.push(SimEvent::FaultApplied {
            phase: format!(
                "node {node_id} crashes ({crash:?}) while its teardown of group {group} holds \
                 {step:?}"
            ),
        });
        match crash {
            TeardownCrash::Process => {
                self.cluster.stop_node(node_id).await;
                self.cluster.start_node(node_id).await;
            }
            TeardownCrash::Power => self.power_loss_of(&[node_id]).await,
            TeardownCrash::MetadataLost => {
                self.cluster.lose_core_metadata(node_id).await;
                self.cluster.start_node(node_id).await;
            }
        }
        self.step("teardown_crashed");
    }

    /// Moves `group` back onto `from`, the voters it moved off (A to B to
    /// A), once its last move settled: each node it returns to reopens its
    /// retired log behind its gate. With `power`, the first returning node
    /// loses its power once it runs the group again.
    async fn move_back(&mut self, group: u32, from: &BTreeSet<u64>, power: bool) {
        const PHASE: &str = "move_back";
        let to = self.cluster.placed_voters(group);
        let Some(returning) = from.difference(&to).copied().min() else {
            return;
        };
        self.trace.push(SimEvent::FaultApplied {
            phase: format!("move group {group} from {to:?} back to {from:?}"),
        });
        self.accepted(PHASE, group, from, None).await;
        if power
            && self
                .within(PHASE, MOVE_BOUND, move |cluster| {
                    cluster.hosted_raft(returning, group).is_some()
                })
                .await
        {
            self.power_loss_of(&[returning]).await;
        }
        self.settled(PHASE, group, from).await;
        self.append_data(PHASE).await;
    }

    /// The running leader of `group`, once it has one.
    pub(super) async fn leader(&mut self, phase: &str, group: u32) -> u64 {
        self.until(phase, I3, meta_checks::BOUND, move |cluster| {
            cluster
                .leader_of(group)
                .is_none()
                .then(|| format!("group {group} has no running leader"))
        })
        .await;
        self.cluster.leader_of(group).unwrap_or(0)
    }

    /// Hands `group`'s leadership from `from` to `to` through OpenRaft, and
    /// waits until `to` leads.
    pub(super) async fn transfer(&mut self, phase: &str, group: u32, from: u64, to: u64) {
        let deadline = madsim::time::Instant::now() + meta_checks::BOUND;
        while self.cluster.leader_of(group) != Some(to) {
            if madsim::time::Instant::now() >= deadline {
                panic!("group {group}'s leadership did not move from {from} to {to}");
            }
            self.cluster.trigger_transfer(group, from, to).await;
            let moved = self
                .within(phase, Duration::from_secs(2), move |cluster| {
                    cluster.leader_of(group) == Some(to)
                })
                .await;
            if moved {
                return;
            }
        }
    }

    /// The nodes of the run that hold neither a role in `group` nor a
    /// retirement of it, in id order.
    fn fresh(&self, group: u32) -> Vec<u64> {
        let view = self.cluster.latest_view();
        let Some(bootstrapped) = view.cluster() else {
            return Vec::new();
        };
        self.nodes()
            .into_iter()
            .filter(|node_id| {
                let node = NodeId::new(*node_id);
                let group = ursula_shard::RaftGroupId(group);
                bootstrapped.role(group, node).is_none()
                    && bootstrapped.retirement(group, node).is_none()
            })
            .collect()
    }

    /// One move of `group` as `draw` has it, to fresh nodes, with keyed
    /// appends before and after, that settles live on its last target.
    pub(super) async fn move_group(&mut self, group: u32, draw: MoveDraw) {
        const PHASE: &str = "move";
        let from = self.cluster.placed_voters(group);
        let fresh = self.fresh(group);
        let leader = self.leader(PHASE, group).await;
        let mut to: BTreeSet<u64> = if draw.full {
            fresh.iter().copied().take(from.len()).collect()
        } else {
            let out = if draw.outgoing_leader {
                Some(leader)
            } else {
                from.iter()
                    .copied()
                    .filter(|node_id| *node_id != leader)
                    .max()
            };
            let mut to = from.clone();
            if let (Some(out), Some(fresh)) = (out, fresh.first()) {
                to.remove(&out);
                to.insert(*fresh);
            }
            to
        };
        if to.len() != from.len() || to == from {
            return;
        }
        // The node a converging move drops, crashed while its teardown
        // holds the drawn step.
        let crashed = match (draw.ending, draw.teardown) {
            (Ending::Converges, Some(fault)) => from
                .difference(&to)
                .copied()
                .min()
                .map(|node_id| (node_id, fault)),
            _ => None,
        };
        if let Some((node_id, fault)) = crashed {
            self.cluster.hold_teardown(node_id, group, fault.step);
        }
        let held = match draw.ending {
            Ending::Cancel(At::Held(step)) | Ending::Retarget(At::Held(step)) => Some(step),
            Ending::Converges | Ending::Cancel(At::After(_)) | Ending::Retarget(At::After(_)) => {
                None
            }
        };
        if let Some(step) = held {
            self.cluster.hold(leader, group, step);
        }
        self.trace.push(SimEvent::FaultApplied {
            phase: format!("move group {group} from {from:?} to {to:?}: {draw:?}"),
        });
        self.accepted(PHASE, group, &to, None).await;
        self.append_data(PHASE).await;
        if draw.partition {
            self.cluster.cut_data(leader);
            self.watch(PHASE, CUT).await;
            self.cluster.heal_data(leader);
            self.step("leader_healed");
        }
        let at = match draw.ending {
            Ending::Converges => None,
            Ending::Cancel(at) | Ending::Retarget(at) => Some(at),
        };
        if let Some(at) = at {
            match at {
                At::After(wait) => self.watch(PHASE, wait).await,
                At::Held(step) => {
                    // A step the leader never plans is never held: the move
                    // ends when the bound has passed.
                    self.within(PHASE, HELD_BOUND, move |cluster| {
                        cluster.stepping(leader, group, step)
                    })
                    .await;
                }
            }
            let next = match draw.ending {
                Ending::Retarget(_) => {
                    let first = fresh.first().copied();
                    match fresh.get(1).copied() {
                        Some(other) => {
                            let mut next = to.clone();
                            if let Some(first) = first {
                                next.remove(&first);
                            }
                            next.insert(other);
                            next
                        }
                        None => from.clone(),
                    }
                }
                Ending::Converges | Ending::Cancel(_) => from.clone(),
            };
            match self.request(PHASE, group, &next, None).await {
                Ok(()) => to = next,
                // The move converged before its end came, and a move back
                // to a retired voter is refused.
                Err(_refused) if self.cluster.moving(group).is_none() => {}
                Err(error) => panic!("the end of group {group}'s move was refused: {error}"),
            }
        }
        if let Some(step) = held {
            self.cluster.release(leader, group, step);
        }
        if let Some((node_id, fault)) = crashed {
            self.crash_mid_teardown(group, node_id, fault).await;
        }
        self.settled(PHASE, group, &to).await;
        self.append_data(PHASE).await;
    }

    /// Both groups move at the move limit 1: the second move is refused
    /// until the first converged.
    async fn limit_one(&mut self) {
        const PHASE: &str = "limit_one";
        self.set_limit(1).await;
        let mut moves = Vec::new();
        for group in JOURNAL_GROUPS {
            let from = self.cluster.placed_voters(group);
            let leader = self.leader(PHASE, group).await;
            let (Some(out), Some(fresh)) = (
                from.iter()
                    .copied()
                    .filter(|node_id| *node_id != leader)
                    .max(),
                self.fresh(group).first().copied(),
            ) else {
                continue;
            };
            let mut to = from;
            to.remove(&out);
            to.insert(fresh);
            moves.push((group, to));
        }
        if let [(first, first_to), (second, second_to)] = moves.as_slice() {
            self.accepted(PHASE, *first, first_to, None).await;
            match self.request(PHASE, *second, second_to, None).await {
                Err(ControlError::MoveLimit { .. }) => {}
                answer => panic!("a second move at the move limit 1 answered {answer:?}"),
            }
            self.settled(PHASE, *first, first_to).await;
            self.append_data(PHASE).await;
            self.accepted(PHASE, *second, second_to, None).await;
            self.settled(PHASE, *second, second_to).await;
            self.append_data(PHASE).await;
        }
        self.set_limit(2).await;
    }

    /// [`Self::move_group`], then on a third of the draws the move back
    /// onto the voters it left ([`Self::move_back`]), with a power loss of a
    /// returning node on half of those.
    async fn move_and_back(&mut self, group: u32, draw: MoveDraw, draws: &mut SplitMix64) {
        let from = self.cluster.placed_voters(group);
        self.move_group(group, draw).await;
        if draws.next_bounded(3) == 0 {
            let power = draws.next_bounded(2) == 0;
            self.move_back(group, &from, power).await;
        }
    }

    /// The moves of `shape`, drawn.
    async fn moves(&mut self, shape: Shape, draws: &mut SplitMix64) {
        match shape {
            Shape::Meta { nodes } if nodes < NODES => return,
            Shape::Meta { .. } => {
                if draws.next_bounded(2) == 0 {
                    self.limit_one().await;
                }
                for group in JOURNAL_GROUPS {
                    let fresh = self.fresh(group).len();
                    if fresh > 0 {
                        let draw = MoveDraw::drawn(draws, fresh, true);
                        self.move_and_back(group, draw, draws).await;
                    }
                }
            }
            Shape::Rf2 => {
                for group in JOURNAL_GROUPS {
                    let mut draw = MoveDraw::drawn(draws, 1, false);
                    if draws.next_bounded(3) != 0 {
                        draw.partition = false;
                    }
                    self.move_and_back(group, draw, draws).await;
                }
            }
            Shape::FullReplacement => {
                let full = MoveDraw {
                    full: true,
                    ..MoveDraw::ONE
                };
                self.move_and_back(0, full, draws).await;
                let draw = MoveDraw::drawn(draws, 1, false);
                self.move_and_back(1, draw, draws).await;
            }
            Shape::Rf1 => {
                self.cluster.drain(1);
                self.step("node_drained");
                let cancelled = MoveDraw {
                    full: true,
                    ending: Ending::Cancel(At::Held(StepKind::ChangeVoters)),
                    ..MoveDraw::ONE
                };
                self.move_group(0, cancelled).await;
                let moved = MoveDraw {
                    full: true,
                    ..MoveDraw::ONE
                };
                self.move_group(1, moved).await;
            }
            // Its groups move through the decommission driver.
            Shape::Decommission => {}
        }
    }

    /// I14, narrowed: a settled cluster, every group live on its placement
    /// with no fault, keeps every leadership where it is.
    async fn leaderships_stay(&mut self, phase: &str) {
        let leaders = |cluster: &MetaCluster| -> Vec<(u32, Option<(u64, u64)>)> {
            JOURNAL_GROUPS
                .iter()
                .map(|group| {
                    let leader = cluster.leader_of(*group).and_then(|leader| {
                        let raft = cluster.hosted_raft(leader, *group)?;
                        let term = openraft::rt::WatchReceiver::borrow_watched(&raft.metrics())
                            .current_term;
                        Some((leader, term))
                    });
                    (*group, leader)
                })
                .collect()
        };
        let before = leaders(&self.cluster);
        self.watch(phase, SETTLED).await;
        let after = leaders(&self.cluster);
        // A group without a running leader keeps none to move, so I14
        // would pass without judging it.
        if let Some((group, _)) = before
            .iter()
            .chain(&after)
            .find(|(_, leader)| leader.is_none())
        {
            meta_checks::fail(
                &mut self.trace,
                placed_hosting::I14,
                phase,
                format!("group {group} had no running leader, so I14 judged nothing of it"),
            );
        }
        if before != after {
            meta_checks::fail(
                &mut self.trace,
                placed_hosting::I14,
                phase,
                format!("a settled cluster's leaderships moved from {before:?} to {after:?}"),
            );
        }
    }

    /// The end of every placed run: every group settles live on its
    /// placement and its replicas without a role are torn down (I3, I5),
    /// and every voter reads every acknowledged append (I1).
    pub(super) async fn settle_and_read(&mut self) {
        self.cluster
            .groups_live(&mut self.trace, "groups_live")
            .await;
        self.step("groups_live");
        for group in JOURNAL_GROUPS {
            self.torn_down("groups_live", group).await;
        }
        for group in JOURNAL_GROUPS {
            self.cluster
                .placed_reads(&mut self.trace, "final_reads", group)
                .await;
        }
        self.step("final_reads");
    }

    /// Ends the run: a moved group fails a check that judged nothing of it
    /// ([`MetaChecks::judged_the_moves`]).
    pub(super) fn finish(mut self) -> ThreeNodeRaftSimOutcome {
        self.checks
            .judged_the_moves(&self.cluster, &mut self.trace, "finished");
        self.trace.push(SimEvent::PlacementMoveMetaHeld {
            genesis: self.checks.genesis().unwrap_or(0),
            nodes: self.cluster.running(),
        });
        ThreeNodeRaftSimOutcome {
            seed: self.seed,
            leader_id: 0,
            target_node_id: None,
            appended_log_index: 0,
            trace: self.trace,
        }
    }
}

pub(super) async fn run_placement_move_inner(
    config: ThreeNodeRaftSimConfig,
    controls: Controls,
) -> ThreeNodeRaftSimOutcome {
    let seed = config.seed;
    let _switched = Switched::apply(controls);
    match controls {
        Controls::Family => family(seed).await,
        Controls::WipedMetaVoters { .. } => wiped_meta_voters(seed).await,
        Controls::MetaDiscardedOverData { .. } => meta_discarded_over_data(seed).await,
        Controls::PowerLossUnderNever { .. } => power_loss_under_never(seed).await,
        Controls::SeededFormation { .. } => seeded_formation(seed).await,
        Controls::StaleStep { .. } => stale_step(seed).await,
        Controls::LaggingLeader { .. } => lagging_leader(seed, controls).await,
        Controls::DeposedReport { .. } => deposed_report(seed, controls).await,
        Controls::ReplacedMidTeardown { .. } => replaced_mid_teardown(seed, controls).await,
        Controls::ReopenedRetiredLog { .. } => reopened_retired_log(seed).await,
        Controls::ReusedIdVote { .. } => super::reused_id::reused_id_vote(seed, controls).await,
        Controls::ReturningOldDisk { .. } => {
            super::reused_id::returning_old_disk(seed, controls).await
        }
    }
}

pub(super) fn claims(nodes: u64) -> BTreeMap<u64, ursula_proto::placement::GenesisClaim> {
    claims_with_factor(nodes, 3)
}

/// The claims of `nodes` nodes at replication factor `factor`, the meta
/// voters the smallest `min(3, nodes)` ids.
fn claims_with_factor(
    nodes: u64,
    factor: u8,
) -> BTreeMap<u64, ursula_proto::placement::GenesisClaim> {
    let voters: Vec<u64> = META_VOTERS
        .into_iter()
        .filter(|node_id| *node_id <= nodes)
        .collect();
    (1..=nodes)
        .map(|node_id| (node_id, claim_with_factor(nodes, &voters, factor)))
        .collect()
}

async fn family(seed: u64) -> ThreeNodeRaftSimOutcome {
    let mut draws = SplitMix64::new(seed ^ SALT ^ 1);
    let fsync = if draws.next_bounded(2) == 0 {
        WalFsync::Always
    } else {
        WalFsync::Never
    };
    let shape = match draws.next_bounded(8) {
        0 => Shape::Meta { nodes: 3 },
        1 | 2 => Shape::Meta { nodes: NODES },
        3 => Shape::Rf2,
        4 => Shape::FullReplacement,
        5 => Shape::Rf1,
        _ => Shape::Decommission,
    };
    match shape {
        Shape::Decommission => decommission_family(seed, &mut draws, fsync).await,
        Shape::Meta { nodes } => meta_family(seed, &mut draws, fsync, nodes).await,
        Shape::Rf2 => moves_family(seed, &mut draws, fsync, shape, 3, 2).await,
        Shape::FullReplacement => moves_family(seed, &mut draws, fsync, shape, 4, 2).await,
        Shape::Rf1 => moves_family(seed, &mut draws, fsync, shape, 2, 1).await,
    }
}

/// The meta faults on `nodes` nodes at RF 3, then on five nodes the moves.
async fn meta_family(
    seed: u64,
    draws: &mut SplitMix64,
    fsync: WalFsync,
    nodes: u64,
) -> ThreeNodeRaftSimOutcome {
    let mismatch = draws.next_bounded(3) == 0;
    let restart = match draws.next_bounded(4) {
        0 => None,
        1 => Some(InitializerRestart::Crash),
        2 => Some(InitializerRestart::RunStateLost),
        _ => Some(InitializerRestart::MetaDeleted),
    };
    let restarted_group = JOURNAL_GROUPS
        .get(usize::try_from(draws.next_bounded(2)).unwrap_or(0))
        .copied()
        .unwrap_or(0);
    let mut claims = claims(nodes);
    if mismatch {
        let mut other = claim(nodes, &META_VOTERS);
        other.group_count = GroupCount::new(std::num::NonZeroU32::new(3).expect("three groups"));
        claims.insert(nodes, other);
    }
    let mut run = Run::start(seed, nodes, fsync, claims, Hosting::Placed).await;
    if mismatch {
        run.claim_mismatch().await;
    }
    if let Some(how) = restart {
        run.initializer_restart(restarted_group, how).await;
    }
    run.formed("formed").await;
    run.learned("formed").await;
    run.bootstrapped("formed").await;
    run.write(4).await;
    run.append_data("formed").await;
    let mut faults = vec![MetaFault::QuorumLoss, MetaFault::PowerLoss, MetaFault::Plan];
    // A seeded order of the three faults.
    for index in (1..faults.len()).rev() {
        let upper = u64::try_from(index).unwrap_or(0).saturating_add(1);
        let other = usize::try_from(draws.next_bounded(upper)).unwrap_or(0);
        faults.swap(index, other);
    }
    for fault in faults {
        match fault {
            MetaFault::QuorumLoss => run.quorum_loss().await,
            MetaFault::PowerLoss => run.power_loss(fsync).await,
            MetaFault::Plan => run.plan().await,
        }
    }
    run.cluster.groups_live(&mut run.trace, "groups_live").await;
    run.step("groups_live");
    let shape = Shape::Meta { nodes };
    run.moves(shape, draws).await;
    run.settle_and_read().await;
    if nodes == NODES {
        run.leaderships_stay("settled").await;
    }
    run.finish()
}

/// The moves of `shape` alone, on `nodes` nodes at RF `factor`.
async fn moves_family(
    seed: u64,
    draws: &mut SplitMix64,
    fsync: WalFsync,
    shape: Shape,
    nodes: u64,
    factor: u8,
) -> ThreeNodeRaftSimOutcome {
    let claims = claims_with_factor(nodes, factor);
    let mut run = Run::start(seed, nodes, fsync, claims, Hosting::Placed).await;
    run.formed("formed").await;
    run.learned("formed").await;
    run.bootstrapped("formed").await;
    run.append_data("formed").await;
    run.cluster.groups_live(&mut run.trace, "groups_live").await;
    run.step("groups_live");
    run.moves(shape, draws).await;
    run.settle_and_read().await;
    run.finish()
}

/// `negative_control_initialize_only_seeded`: five nodes whose data groups
/// form by placement. Only each group's seeded initializer initializes it.
/// With the defence off every seed voter shows itself the initializer, and
/// the voters that find the others empty each initialize the group (I2).
async fn seeded_formation(seed: u64) -> ThreeNodeRaftSimOutcome {
    let mut run = Run::start(
        seed,
        NODES,
        WalFsync::Always,
        claims(NODES),
        Hosting::Placed,
    )
    .await;
    run.formed("seeded_formation").await;
    run.append_data("seeded_formation").await;
    run.finish()
}

/// Five placed nodes at RF 3 under `always`, live on their seeds, and
/// group 0's roles: its leader, its other two voters and the two nodes
/// outside it, each pair in id order.
async fn placed_five(seed: u64) -> (Run, Roles) {
    let mut run = Run::start(
        seed,
        NODES,
        WalFsync::Always,
        claims(NODES),
        Hosting::Placed,
    )
    .await;
    run.formed("formed").await;
    run.learned("formed").await;
    run.bootstrapped("formed").await;
    run.append_data("formed").await;
    run.cluster.groups_live(&mut run.trace, "groups_live").await;
    run.step("groups_live");
    let leader = run.leader("groups_live", 0).await;
    let voters = run.cluster.placed_voters(0);
    let others: Vec<u64> = voters
        .iter()
        .copied()
        .filter(|node_id| *node_id != leader)
        .collect();
    let spares = run.fresh(0);
    let roles = Roles {
        leader,
        voters,
        others: [others[0], others[1]],
        spares: [spares[0], spares[1]],
    };
    (run, roles)
}

/// Group 0's roles in a control's script.
struct Roles {
    leader: u64,
    voters: BTreeSet<u64>,
    /// The voters other than the leader.
    others: [u64; 2],
    /// The nodes outside the group.
    spares: [u64; 2],
}

impl Roles {
    /// The voters with `out` replaced by `into`.
    fn with(&self, out: u64, into: u64) -> BTreeSet<u64> {
        let mut voters = self.voters.clone();
        voters.remove(&out);
        voters.insert(into);
        voters
    }
}

/// `negative_control_membership_cas` (N1): group 0's leader A holds its
/// voter change toward a move's target, and the move is cancelled. The
/// leadership goes to B, which holds its removal of the cancelled learner,
/// and back to A in a later term. A's held change then runs: its CAS on
/// the leadership it was planned under refuses it. With the defence off it
/// makes the abandoned learner a voter (I8).
async fn stale_step(seed: u64) -> ThreeNodeRaftSimOutcome {
    const PHASE: &str = "stale_step";
    const GROUP: u32 = 0;
    let (mut run, roles) = placed_five(seed).await;
    let [b, c] = roles.others;
    let [z, _] = roles.spares;
    let a = roles.leader;
    run.cluster.hold(a, GROUP, StepKind::ChangeVoters);
    run.accepted(PHASE, GROUP, &roles.with(c, z), None).await;
    assert!(
        run.within(PHASE, MOVE_BOUND, move |cluster| cluster.stepping(
            a,
            GROUP,
            StepKind::ChangeVoters
        ))
        .await,
        "node {a} never held its voter change"
    );
    run.accepted(PHASE, GROUP, &roles.voters, None).await;
    let voters = roles.voters.clone();
    run.until(PHASE, I3, meta_checks::BOUND, move |cluster| {
        (!cluster.every_view_targets(GROUP, &voters)).then(|| "a view misses the cancel".to_owned())
    })
    .await;
    run.cluster.hold(b, GROUP, StepKind::RemoveLearners);
    run.transfer(PHASE, GROUP, a, b).await;
    assert!(
        run.within(PHASE, MOVE_BOUND, move |cluster| cluster.stepping(
            b,
            GROUP,
            StepKind::RemoveLearners
        ))
        .await,
        "node {b} never held its learner removal"
    );
    run.transfer(PHASE, GROUP, b, a).await;
    run.cluster.release(a, GROUP, StepKind::ChangeVoters);
    run.watch(PHASE, CUT).await;
    run.cluster.release(b, GROUP, StepKind::RemoveLearners);
    run.settled(PHASE, GROUP, &roles.voters).await;
    run.settle_and_read().await;
    run.finish()
}

/// `negative_control_placement_barrier` (N5): group 0 moves toward a first
/// target, its leader A holding its first learner addition, and its voter
/// B's meta replica is cut off once B's view shows the target. The move is
/// retargeted, and A converges without the first target's node z, which
/// the report retires. The leadership goes to B, whose view still shows
/// the first target: B adds z back as a learner, and its voter change
/// waits for a confirmation it cannot get. With the defence off from the
/// handoff on, B promotes z (I8).
async fn lagging_leader(seed: u64, controls: Controls) -> ThreeNodeRaftSimOutcome {
    const PHASE: &str = "lagging_leader";
    const GROUP: u32 = 0;
    let (mut run, roles) = placed_five(seed).await;
    let [b, c] = roles.others;
    let [z, w] = roles.spares;
    let a = roles.leader;
    let first = roles.with(c, z);
    let second = roles.with(c, w);
    // Every leader's first step toward a new voter is its learner
    // addition, whatever its view's freshness, so the hold is reached in
    // the defended run and the undefended one alike.
    run.cluster.hold(a, GROUP, StepKind::AddLearners);
    run.accepted(PHASE, GROUP, &first, None).await;
    let targets = first.clone();
    assert!(
        run.within(PHASE, MOVE_BOUND, move |cluster| {
            cluster.stepping(a, GROUP, StepKind::AddLearners)
                && cluster.every_view_targets(GROUP, &targets)
        })
        .await,
        "node {a} never held its learner addition"
    );
    run.cluster.cut_meta(b);
    run.accepted(PHASE, GROUP, &second, Some(b)).await;
    run.cluster.release(a, GROUP, StepKind::AddLearners);
    let retired = second.clone();
    run.until(PHASE, I3, MOVE_BOUND, move |cluster| {
        let view = cluster.latest_view();
        let converged = view.cluster().is_some_and(|bootstrapped| {
            bootstrapped
                .retirement(ursula_shard::RaftGroupId(GROUP), NodeId::new(z))
                .is_some()
        });
        (!converged || cluster.placed_voters(GROUP) != retired)
            .then(|| format!("group {GROUP} has not converged on {retired:?}"))
    })
    .await;
    defence_off_from_here(
        Defence::PlacementBarrier,
        controls == Controls::LaggingLeader { defended: false },
    );
    run.transfer(PHASE, GROUP, a, b).await;
    run.watch(PHASE, Duration::from_secs(5)).await;
    run.cluster.heal_meta(b);
    run.settled(PHASE, GROUP, &second).await;
    run.settle_and_read().await;
    run.finish()
}

/// `negative_control_converge_leadership_proof` (N9): group 0 moves toward a
/// first target, its leader A holding its first learner addition, and its
/// voter B's meta replica is cut off once B's view shows the target. The
/// move is retargeted, and A reaches the new target and holds its report.
/// The leadership goes to B and A is cut off on the data plane. B, whose
/// view still shows the first target, adds the first target's node as a
/// learner. A's report then runs: its leadership proof fails. With the
/// defence off from there on, A reports the membership B changed (I9).
async fn deposed_report(seed: u64, controls: Controls) -> ThreeNodeRaftSimOutcome {
    const PHASE: &str = "deposed_report";
    const GROUP: u32 = 0;
    let (mut run, roles) = placed_five(seed).await;
    let [b, c] = roles.others;
    let [x, y] = roles.spares;
    let a = roles.leader;
    let first = roles.with(c, y);
    let second = roles.with(c, x);
    // Every leader's first step toward a new voter is its learner
    // addition, whatever its view's freshness, so the hold is reached in
    // the defended run and the undefended one alike.
    run.cluster.hold(a, GROUP, StepKind::AddLearners);
    run.accepted(PHASE, GROUP, &first, None).await;
    let targets = first.clone();
    assert!(
        run.within(PHASE, MOVE_BOUND, move |cluster| {
            cluster.stepping(a, GROUP, StepKind::AddLearners)
                && cluster.every_view_targets(GROUP, &targets)
        })
        .await,
        "node {a} never held its learner addition"
    );
    run.cluster.cut_meta(b);
    run.cluster.hold(a, GROUP, StepKind::ReportConverged);
    run.accepted(PHASE, GROUP, &second, Some(b)).await;
    run.cluster.release(a, GROUP, StepKind::AddLearners);
    assert!(
        run.within(PHASE, MOVE_BOUND, move |cluster| {
            cluster.stepping(a, GROUP, StepKind::ReportConverged)
        })
        .await,
        "node {a} never held its report"
    );
    run.transfer(PHASE, GROUP, a, b).await;
    run.cluster.cut_data(a);
    run.step(PHASE);
    assert!(
        run.within(PHASE, MOVE_BOUND, move |cluster| {
            cluster.hosted_raft(b, GROUP).is_some_and(|raft| {
                openraft::rt::WatchReceiver::borrow_watched(&raft.metrics())
                    .committed_membership_config
                    .membership()
                    .learner_ids()
                    .any(|learner| learner == y)
            })
        })
        .await,
        "node {b} never added node {y} back as a learner"
    );
    defence_off_from_here(
        Defence::ConvergeLeadershipProof,
        controls == Controls::DeposedReport { defended: false },
    );
    run.cluster.release(a, GROUP, StepKind::ReportConverged);
    run.watch(PHASE, Duration::from_secs(3)).await;
    run.cluster.heal_meta(b);
    run.cluster.heal_data(a);
    run.settled(PHASE, GROUP, &second).await;
    run.settle_and_read().await;
    run.finish()
}

/// `negative_control_retire_answer` (N4): group 0's voter z, a node outside
/// the meta voters, is moved off the group (by way of a first move that
/// makes a spare its voter, when no voter but the leader is outside them),
/// and its teardown holds before it closes the replica. Group 0's
/// leadership goes to a voter the meta loss spares, and the meta leader and
/// one more meta voter lose their run state: meta recovers by a plan, whose
/// history event drops z's retirement. The group moves back onto z, whose
/// replica still runs, and its leader makes z a voter again. z's teardown
/// then asks again and finds itself a voter, so its log stays. With the
/// defence off from the release on, z retires the log of a voter (I8). z's
/// teardown also holds before it forgets the retired replica, for the rest
/// of the run, which only the undefended run reaches: otherwise the next
/// driver pass would reopen the retired log as the voter's
/// (`Admission::Create`), and the retired log would show for one pass only.
async fn replaced_mid_teardown(seed: u64, controls: Controls) -> ThreeNodeRaftSimOutcome {
    const PHASE: &str = "replaced_mid_teardown";
    const GROUP: u32 = 0;
    const HELD: TeardownStep = TeardownStep::Unregister;
    const RETIRED: TeardownStep = TeardownStep::ForgetMetrics;
    let (mut run, roles) = placed_five(seed).await;
    let outside_meta = |nodes: &[u64]| {
        nodes
            .iter()
            .copied()
            .find(|node_id| !META_VOTERS.contains(node_id))
    };
    let (z, voters) = match outside_meta(&roles.others) {
        Some(z) => (z, roles.voters.clone()),
        None => {
            let z = outside_meta(&roles.spares).expect("a node outside the meta voters");
            let voters = roles.with(roles.others[1], z);
            run.accepted(PHASE, GROUP, &voters, None).await;
            run.settled(PHASE, GROUP, &voters).await;
            (z, voters)
        }
    };
    let x = run
        .fresh(GROUP)
        .first()
        .copied()
        .expect("a node outside the group");
    let mut dropped = voters.clone();
    dropped.remove(&z);
    dropped.insert(x);
    run.cluster.hold_teardown(z, GROUP, HELD);
    run.accepted(PHASE, GROUP, &dropped, None).await;
    assert!(
        run.within(PHASE, TEARDOWN_BOUND, move |cluster| {
            cluster.teardown_held(z, GROUP, HELD)
        })
        .await,
        "node {z} never held its teardown of group {GROUP}"
    );
    let victims = run.leader_and_one_voter();
    let keeper = dropped
        .iter()
        .copied()
        .find(|node_id| !victims.contains(node_id))
        .expect("a voter the meta loss spares");
    let leader = run.leader(PHASE, GROUP).await;
    if leader != keeper {
        run.transfer(PHASE, GROUP, leader, keeper).await;
    }
    run.accept_meta_loss(victims, &[1]).await;
    run.learned(PHASE).await;
    run.accepted(PHASE, GROUP, &voters, None).await;
    let back = voters.clone();
    run.until(PHASE, I3, MOVE_BOUND, move |cluster| {
        (cluster.committed_voters(GROUP).as_ref() != Some(&back)).then(|| {
            format!("group {GROUP}'s committed membership is not {back:?} with node {z} a voter")
        })
    })
    .await;
    defence_off_from_here(
        Defence::RetireAnswer,
        controls == Controls::ReplacedMidTeardown { defended: false },
    );
    run.cluster.hold_teardown(z, GROUP, RETIRED);
    run.cluster.release_teardown(z, GROUP, HELD);
    // Defended, z's second ask hears that it is a voter, so its teardown
    // ends before it retires and the watch runs out. Undefended, the poll
    // that sees z hold before it forgets has already failed I8.
    let retired = run
        .within(PHASE, RETIRE_WATCH, move |cluster| {
            cluster.teardown_held(z, GROUP, RETIRED)
        })
        .await;
    assert!(
        !retired,
        "node {z} retired its log of group {GROUP}, a voter's, and I8 passed"
    );
    run.settled(PHASE, GROUP, &voters).await;
    run.settle_and_read().await;
    run.finish()
}

/// `negative_control_log_generation` (N6): group 0 moves voter c off and
/// back onto it (A to B to A), c cut off on the data plane before the move
/// back, so its reopened log takes nothing. c's power is cut, and its log
/// opens again empty, its retired life's frames of an older generation.
/// With the defence off the retirement keeps the generation, and the
/// reopened log replays the retired life (I5).
async fn reopened_retired_log(seed: u64) -> ThreeNodeRaftSimOutcome {
    const PHASE: &str = "reopened_retired_log";
    const GROUP: u32 = 0;
    let (mut run, roles) = placed_five(seed).await;
    let [_, c] = roles.others;
    let [x, _] = roles.spares;
    let away = roles.with(c, x);
    run.accepted(PHASE, GROUP, &away, None).await;
    run.append_data(PHASE).await;
    run.settled(PHASE, GROUP, &away).await;
    run.cluster.cut_data(c);
    run.accepted(PHASE, GROUP, &roles.voters, None).await;
    run.until(PHASE, I3, MOVE_BOUND, move |cluster| {
        cluster
            .hosted_raft(c, GROUP)
            .is_none()
            .then(|| format!("node {c} did not reopen its retired log of group {GROUP}"))
    })
    .await;
    run.power_loss_of(&[c]).await;
    run.until(PHASE, I5, MOVE_BOUND, move |cluster| {
        cluster.hosted_raft(c, GROUP).is_none().then(|| {
            format!("node {c}'s reopened log of group {GROUP} does not open after its power loss")
        })
    })
    .await;
    if let Some(replayed) = run.cluster.replayed_life(c, GROUP).await {
        meta_checks::fail(&mut run.trace, I5, PHASE, replayed);
    }
    run.cluster.heal_data(c);
    run.step(PHASE);
    run.settled(PHASE, GROUP, &roles.voters).await;
    run.settle_and_read().await;
    run.finish()
}

/// `negative_control_genesis_probes_every_peer`: the meta voters lose their
/// disks while nodes 4 and 5 keep meta history. Their genesis node asks
/// every peer, and the learners' answers hold the genesis back. With the
/// defence off it asks the voters only and forms a second meta group (I12).
async fn wiped_meta_voters(seed: u64) -> ThreeNodeRaftSimOutcome {
    let mut run = Run::start(
        seed,
        NODES,
        WalFsync::Always,
        claims(NODES),
        Hosting::Static,
    )
    .await;
    run.learned("formed").await;
    for victim in META_VOTERS {
        run.cluster
            .wipe(victim, &format!("placement-move-{seed}-wiped-{victim}"))
            .await;
    }
    for victim in META_VOTERS {
        run.cluster.start_node(victim).await;
    }
    run.step("meta_voters_wiped");
    run.watch("meta_voters_wiped", WATCH).await;
    run.finish()
}

/// `negative_control_meta_recovers_over_data_history`: three nodes, every
/// meta voter's meta journal discarded while data holds history. Each opens
/// `Recovering` and no genesis follows. With the defence off they open
/// empty and form a second meta group (I12).
async fn meta_discarded_over_data(seed: u64) -> ThreeNodeRaftSimOutcome {
    let mut run = Run::start(seed, 3, WalFsync::Always, claims(3), Hosting::Static).await;
    run.learned("formed").await;
    for group in JOURNAL_GROUPS {
        run.cluster.data.append(group, 2).await;
    }
    for victim in META_VOTERS {
        run.cluster.discard_meta(victim).await;
    }
    for victim in META_VOTERS {
        run.cluster.start_node(victim).await;
    }
    run.step("meta_discarded");
    run.watch("meta_discarded", WATCH).await;
    run.finish()
}

/// `negative_control_meta_journal_syncs_always`: three nodes under data
/// `never`. What meta wrote while forming reaches the disk, the control
/// writes after it are acknowledged, and every node loses its power: each
/// acknowledged write is on every meta replica (I10). With the defence off
/// the meta journal runs `never` and loses them.
async fn power_loss_under_never(seed: u64) -> ThreeNodeRaftSimOutcome {
    let mut run = Run::start(seed, 3, WalFsync::Never, claims(3), Hosting::Static).await;
    run.learned("formed").await;
    run.bootstrapped("formed").await;
    for node_id in META_VOTERS {
        run.cluster.data.wals[&node_id].write_back();
    }
    run.write(4).await;
    run.power_loss_of(&META_VOTERS).await;
    let nodes = run.nodes();
    run.checks
        .acknowledged_writes_survive(&run.cluster, &mut run.trace, "power_loss", &nodes)
        .await;
    run.finish()
}

#[cfg(test)]
mod tests {
    use super::Controls;
    use crate::madsim_harness::SimEvent;
    use crate::madsim_harness::SimReport;
    use crate::madsim_harness::SimScenario;
    use crate::madsim_harness::SimTrace;
    use crate::madsim_harness::ThreeNodeRaftSim;
    use crate::madsim_harness::ThreeNodeRaftSimConfig;
    use crate::madsim_harness::meta_checks::I10;
    use crate::madsim_harness::meta_checks::I12;
    use crate::madsim_harness::placed_hosting::I2;
    use crate::madsim_harness::placed_hosting::I5;
    use crate::madsim_harness::placed_hosting::I8;
    use crate::madsim_harness::placed_hosting::I9;
    use crate::madsim_harness::tests::sim_test_guard;

    const SCENARIO: SimScenario = SimScenario::PlacementMove;
    /// The seeds whose family runs the replay covers, both of the shape
    /// `Meta { nodes: 5 }` under `family`'s draw (the decommission seeds
    /// replay in `placement_decommission`).
    const REPLAYED: [u64; 2] = [1102, 1104];
    /// The seed every scripted run uses.
    const SCRIPTED: u64 = 1100;

    fn run(seed: u64, controls: Controls) -> SimReport {
        ThreeNodeRaftSim::run_placement_move_with_controls_report(
            ThreeNodeRaftSimConfig::new(seed, format!("ursula-sim-{seed}-{}", SCENARIO.slug())),
            controls,
        )
    }

    fn held(report: &SimReport) -> bool {
        report
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::PlacementMoveMetaHeld { .. }))
    }

    /// The family's meta seeds hold, the same way twice per seed.
    #[test]
    fn placement_move_meta_seeds_replays() {
        let _guard = sim_test_guard();
        for seed in REPLAYED {
            let first = run(seed, Controls::Family);
            let second = run(seed, Controls::Family);
            assert_eq!(first, second, "seed {seed}");
            assert_eq!(first.scenario, SCENARIO);
            assert!(held(&first), "seed {seed}: the run ends held");
        }
    }

    /// The defended script passes, and the one with `off` fails `invariant`
    /// in the script's own `phase`, not while the cluster forms.
    fn assert_caught(defended: Controls, off: Controls, invariant: &str, phase: &str) -> String {
        assert!(held(&run(SCRIPTED, defended)), "{defended:?} passes");
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(|| run(SCRIPTED, off));
        std::panic::set_hook(previous_hook);
        assert!(result.is_err(), "{off:?} left the run passing");
        SimTrace::last_recorded()
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
                        "{off:?} failed another invariant: {message}"
                    );
                    assert_eq!(
                        after_event, phase,
                        "{off:?} failed outside its script: {message}"
                    );
                    Some(message.clone())
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("{off:?} failed no invariant"))
    }

    /// Wiped meta voters beside learners that hold meta history form a
    /// second meta group when the genesis node asks the voters only.
    #[test]
    fn negative_control_genesis_probes_every_peer() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::WipedMetaVoters { defended: true },
            Controls::WipedMetaVoters { defended: false },
            I12,
            "meta_voters_wiped",
        );
        assert!(message.contains("more than one meta genesis"), "{message}");
    }

    /// Meta voters whose meta journals are gone over data history form a
    /// second meta group when they open them empty.
    #[test]
    fn negative_control_meta_recovers_over_data_history() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::MetaDiscardedOverData { defended: true },
            Controls::MetaDiscardedOverData { defended: false },
            I12,
            "meta_discarded",
        );
        assert!(message.contains("more than one meta genesis"), "{message}");
    }

    /// Seed voters that each show themselves the initializer of their
    /// placed group initialize it more than once.
    #[test]
    fn negative_control_initialize_only_seeded() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::SeededFormation { defended: true },
            Controls::SeededFormation { defended: false },
            I2,
            "seeded_formation",
        );
        assert!(message.contains("initialized more than once"), "{message}");
    }

    /// A voter change planned in an earlier leadership runs after the move
    /// was cancelled when its step goes without its CAS.
    #[test]
    fn negative_control_membership_cas() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::StaleStep { defended: true },
            Controls::StaleStep { defended: false },
            I8,
            "stale_step",
        );
        assert!(
            message.contains("after the placement took its role away"),
            "{message}"
        );
    }

    /// A leader whose meta replica lags promotes a node the placement
    /// retired when a voter change needs no confirmed view.
    #[test]
    fn negative_control_placement_barrier() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::LaggingLeader { defended: true },
            Controls::LaggingLeader { defended: false },
            I8,
            "lagging_leader",
        );
        assert!(
            message.contains("after the placement took its role away"),
            "{message}"
        );
    }

    /// A deposed leader reports a membership its successor changed when a
    /// report goes without the leadership proof.
    #[test]
    fn negative_control_converge_leadership_proof() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::DeposedReport { defended: true },
            Controls::DeposedReport { defended: false },
            I9,
            "deposed_report",
        );
        assert!(message.contains("were committed before"), "{message}");
    }

    /// A replica its group made a voter again while its teardown ran retires
    /// the voter's log when the teardown goes on without the group's
    /// answers.
    #[test]
    fn negative_control_retire_answer() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::ReplacedMidTeardown { defended: true },
            Controls::ReplacedMidTeardown { defended: false },
            I8,
            "replaced_mid_teardown",
        );
        assert!(message.contains("retired its log"), "{message}");
    }

    /// A log retired in the generation it lived in replays its retired life
    /// once it reopened and opens again.
    #[test]
    fn negative_control_log_generation() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::ReopenedRetiredLog { defended: true },
            Controls::ReopenedRetiredLog { defended: false },
            I5,
            "reopened_retired_log",
        );
        assert!(message.contains("reopened log"), "{message}");
    }

    /// A meta journal under the data policy `never` loses acknowledged
    /// control writes to a power loss.
    #[test]
    fn negative_control_meta_journal_syncs_always() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::PowerLossUnderNever { defended: true },
            Controls::PowerLossUnderNever { defended: false },
            I10,
            "power_loss",
        );
        assert!(message.contains("acknowledged move limit"), "{message}");
    }

    /// A reused id's empty replica, unscreened, grants the vote of a
    /// candidate whose membership still names the id's old life, so that
    /// candidate leads without the group's committed history.
    #[test]
    fn negative_control_empty_replica_vote() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::ReusedIdVote { defended: true },
            Controls::ReusedIdVote { defended: false },
            crate::madsim_harness::reused_id::REUSED_VOTE,
            "stale_candidate",
        );
        assert!(message.starts_with("stale_candidate_won"), "{message}");
    }

    /// A removed node's old disk, never answered by the meta group, deletes
    /// a new life's named object under the shared node directory once its
    /// predecessor wait ended.
    #[test]
    fn negative_control_delete_after_registration() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::ReturningOldDisk { defended: true },
            Controls::ReturningOldDisk { defended: false },
            crate::madsim_harness::snapshot_checks::NAMED_KEYS_EXIST,
            "old_disk_returned",
        );
        let named = SimTrace::last_recorded()
            .events
            .iter()
            .find_map(|event| match event {
                SimEvent::SnapshotKeysRestored { keys, .. } => Some(keys.clone()),
                _ => None,
            })
            .expect("the new life named its objects");
        assert!(
            named.iter().any(|key| message.contains(key.as_str())),
            "{message} names none of the new life's keys {named:?}"
        );
    }
}
