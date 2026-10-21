//! Scenario `placement-move` (design 15.2), S2's hosting: the meta group on
//! three or five journal-backed nodes ([`MetaCluster`]), meta voters
//! `{1,2,3}`, the other nodes meta learners, and two data groups that meta
//! seeds and every node hosts by placement: each node's hosting driver opens
//! what its journals hold and creates what a confirmed placement gives it
//! ([`super::placed_hosting`]). S3 widens the family to moves.
//!
//! A family run draws from the seed the node count, the data journals'
//! policy (`always` or `never`), whether one node first claims other
//! genesis values, how a group's initializer restarts before its group
//! formed, and the order of the meta faults. Each run:
//!
//! - forms meta, on some seeds while the last node claims other genesis
//!   values until it restarts with the fixed claim (D9, r1 H1): meta forms
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
//!   over its data history (D9) and rejoins through the meta leader (I11),
//!   then the meta journals of the meta leader and one more meta voter,
//!   whose meta gates open `Recovering` and stall, and recovers meta by a
//!   plan through `ursulactl recover-group --target meta --accept-loss`'s
//!   loop, planner and sender (the meta plan check), after which every node
//!   learns the view again (I11);
//! - reads every acknowledged append back from every seed voter (I1).
//!
//! A killed or discarded node ends its WAL run, so its next start is a new
//! run that decides again how its journals open. I12, I2, I3 and the
//! hosting check are checked after each phase and at every poll of the
//! I11, bootstrap, plan and watch waits. The negative controls
//! ([`Controls`]) each run a script of their own with one defence off: the
//! three meta ones over static data groups, N3 over placed ones.

use std::collections::BTreeMap;
use std::time::Duration;

use ursula_config::WalFsync;
use ursula_proto::placement::GroupCount;
use ursula_proto::placement::NodeId;
use ursula_raft::JournalTuning;
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
use super::placed_hosting;

/// The most nodes a run starts, and the meta voters.
const NODES: u64 = 5;
const META_VOTERS: [u64; 3] = [1, 2, 3];
/// How long a script watches for a second genesis.
const WATCH: Duration = Duration::from_secs(8);
/// How often the meta recovery observes the replicas again.
const PLAN_POLL: Duration = Duration::from_millis(250);
/// Mixed into the seed of a run's draws.
const SALT: u64 = 0x6d65_7461_6d6f_7665;

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
    /// history, with D9's `Recovering` opening on (`true`) or off.
    MetaDiscardedOverData { defended: bool },
    /// Every node loses its power under data `never`, with the meta
    /// journal's own `always` on (`true`) or off.
    PowerLossUnderNever { defended: bool },
    /// Placed groups form on five nodes, with the seed role read from the
    /// placement view (N3) on (`true`) or off.
    SeededFormation { defended: bool },
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
        Self
    }
}

impl Drop for Switched {
    fn drop(&mut self) {
        set_off(Defence::GenesisEveryPeer, false);
        set_off(Defence::MetaOverDataHistory, false);
        set_off(Defence::MetaAlwaysSync, false);
        set_off(Defence::InitializeOnlySeeded, false);
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

struct Run {
    cluster: MetaCluster,
    trace: SimTrace,
    checks: MetaChecks,
    rng: SplitMix64,
    seed: u64,
}

impl Run {
    async fn start(
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
        let mut run = Self {
            cluster,
            trace,
            checks: MetaChecks::default(),
            rng: SplitMix64::new(seed ^ SALT),
            seed,
        };
        run.step("cluster_started");
        run
    }

    /// Records `phase` and checks I12.
    fn step(&mut self, phase: &str) {
        self.trace.push(SimEvent::FaultApplied {
            phase: phase.to_owned(),
        });
        self.checks.observe(&self.cluster, &mut self.trace, phase);
    }

    /// Waits `wait`, checking I12 every step.
    async fn watch(&mut self, phase: &str, wait: Duration) {
        let deadline = madsim::time::Instant::now() + wait;
        while madsim::time::Instant::now() < deadline {
            madsim::time::sleep(STEP).await;
            self.checks.observe(&self.cluster, &mut self.trace, phase);
        }
    }

    fn nodes(&self) -> Vec<u64> {
        self.cluster.data.nodes().collect()
    }

    /// Every node registered and learns the view (I11).
    async fn learned(&mut self, phase: &str) {
        let nodes = self.nodes();
        self.checks
            .registered_nodes_learn(&self.cluster, &mut self.trace, phase, &nodes)
            .await;
        self.step(phase);
    }

    /// Every node holds the bootstrapped view, before the first control
    /// write.
    async fn bootstrapped(&mut self, phase: &str) {
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
    async fn formed(&mut self, phase: &str) {
        self.cluster.groups_form(&mut self.trace, phase).await;
        self.step(phase);
    }

    /// Two appends to every data group, each taken within the bound.
    async fn append_data(&mut self, phase: &str) {
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
            self.checks.observe(&self.cluster, &mut self.trace, PHASE);
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
    /// fixed claim (D9, r1 H1). Meanwhile no genesis forms meta, and no node
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
    /// its data history (D9) and rejoins through the meta leader (I11).
    /// Then the meta leader and one more meta voter lose their meta
    /// journals, so a majority of meta's gates open `Recovering`, where a
    /// replica neither votes nor campaigns, and stall. A meta plan recovers
    /// meta. Their data journals stay: a lost run state would also gate the
    /// data groups whose seed voters they are most of, which only a data
    /// plan recovers.
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
        for victim in victims {
            self.cluster.discard_meta(victim).await;
        }
        for victim in victims {
            self.cluster.start_node(victim).await;
        }
        self.step("meta_journals_lost");
        let nodes = self.nodes();
        self.recover_meta(&nodes).await;
        self.step("meta_plan");
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
                    self.checks
                        .observe(&self.cluster, &mut self.trace, "meta_plan");
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

    fn finish(mut self) -> ThreeNodeRaftSimOutcome {
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
    }
}

fn claims(nodes: u64) -> BTreeMap<u64, ursula_proto::placement::GenesisClaim> {
    (1..=nodes)
        .map(|node_id| (node_id, claim(nodes, &META_VOTERS)))
        .collect()
}

async fn family(seed: u64) -> ThreeNodeRaftSimOutcome {
    let mut draws = SplitMix64::new(seed ^ SALT ^ 1);
    let fsync = if draws.next_bounded(2) == 0 {
        WalFsync::Always
    } else {
        WalFsync::Never
    };
    let nodes = if draws.next_bounded(3) == 0 { 3 } else { NODES };
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
    for group in JOURNAL_GROUPS {
        run.cluster
            .placed_reads(&mut run.trace, "final_reads", group)
            .await;
    }
    run.step("final_reads");
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
    use crate::madsim_harness::tests::sim_test_guard;

    const SCENARIO: SimScenario = SimScenario::PlacementMove;
    /// The seeds whose family runs the replay covers.
    const REPLAYED: std::ops::RangeInclusive<u64> = 1100..=1101;
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
}
