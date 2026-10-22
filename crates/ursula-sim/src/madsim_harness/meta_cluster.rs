//! The meta group on the journal-backed nodes of a [`JournalCluster`]
//! ([`MetaCluster`]): every node runs the production meta runtime
//! ([`MetaRuntime`]) over its node's meta journal, with the in-process
//! meta network and recovery transport, beside the node's data replicas.
//!
//! A node opens its meta journal before its data replicas, as the server's
//! spawn path does, so a lost meta journal over data history is told apart
//! from a new one. The data and meta planes share the cluster's one network
//! policy, so a partition cuts both. The data replicas are static, every
//! node a voter of every data group, or placed, opened by each node's
//! hosting driver ([`Hosting`]). The meta voters are the claim's.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroU8;
use std::num::NonZeroU32;
use std::path::Path;
use std::time::Duration;

use ursula_control::ControlCommand;
use ursula_control::ControlResponse;
use ursula_control::MoveLimit;
use ursula_control::PlacementView;
use ursula_ctl::recovery::GroupPlan;
use ursula_ctl::recovery::LoopState;
use ursula_ctl::recovery::PlanMember;
use ursula_ctl::recovery::PlanTransport;
use ursula_ctl::recovery::RecoverError;
use ursula_ctl::recovery::RecoverOutcome;
use ursula_ctl::recovery::SendError;
use ursula_ctl::recovery::Step;
use ursula_ctl::recovery::electorate;
use ursula_ctl::recovery::next_step;
use ursula_ctl::recovery::plan_sent;
use ursula_ctl::recovery::send_follows;
use ursula_ctl::recovery::send_plan;
use ursula_proto::admin::PlanFloor;
use ursula_proto::admin::RecoveryPlanRefused;
use ursula_proto::admin::RecoveryPlanReport;
use ursula_proto::admin::RecoveryPlanRequest;
use ursula_proto::placement::GenesisClaim;
use ursula_proto::placement::GroupCount;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeSet;
use ursula_proto::placement::ReplicationFactor;
use ursula_proto::placement::VoterSet;
use ursula_raft::BarrierAnswer;
use ursula_raft::InProcessMetaNetworkFactory;
use ursula_raft::InProcessMetaRegistry;
use ursula_raft::InProcessRaftNetworkPolicy;
use ursula_raft::JournalTuning;
use ursula_raft::PeerDirectory;
use ursula_raft::PeerEndpoints;
use ursula_raft::PeerGroupLog;
use ursula_raft::PeerIdentity;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftTarget;
use ursula_raft::meta::MetaForwardError;
use ursula_raft::meta::formation::FormationParams;
use ursula_raft::meta::genesis::GenesisProbe;
use ursula_raft::meta::handle::MetaHandle;
use ursula_raft::meta::replica::MetaSnapshotTrigger;
use ursula_raft::meta::runtime::MetaJournal;
use ursula_raft::meta::runtime::MetaRuntime;
use ursula_raft::meta::runtime::MetaRuntimeConfig;
use ursula_raft::wal::diagnostics::JournalDisk;
use ursula_raft::wal::diagnostics::RUN_STATE_FILE;
use ursula_raft::wal::diagnostics::SimDisk;
use ursula_runtime::RuntimeMetrics;

use super::journal_cluster::JOURNAL_GROUPS;
use super::journal_cluster::JournalCluster;
use super::placed_hosting::DetachedHost;
use super::placed_hosting::Placed;
use super::recovery_wiring::RecoveryProbeError;
use super::sim_wal::FencedRuns;
use super::sim_wal::SimNodeWal;

/// A node's meta runtime in the simulation.
pub(super) type SimMetaRuntime = MetaRuntime<InProcessMetaNetworkFactory>;

/// How many plans one recovery may send, as `ursulactl recover-group
/// --max-plans` bounds it.
const MAX_META_PLANS: usize = 3;

/// Why a recovery round ended without a satisfied plan.
#[derive(Debug, thiserror::Error)]
pub(super) enum MetaRecoverFailed {
    /// The recovery tool stopped.
    #[error(transparent)]
    Recover(#[from] RecoverError),
    /// The tool found nothing to plan.
    #[error("the group needed no plan: {outcome:?}")]
    NoPlan { outcome: RecoverOutcome },
}

/// The meta snapshot trigger every node runs: often enough that a run's
/// meta journals roll and snapshot.
const SNAPSHOTS: MetaSnapshotTrigger = MetaSnapshotTrigger {
    interval: Duration::from_millis(200),
    max_dirty_age: Duration::from_secs(1),
};

/// The claim of a cluster of `nodes` nodes, `1..=nodes`, with `meta_voters`,
/// over the cluster's data groups at replication factor 3.
pub(super) fn claim(nodes: u64, meta_voters: &[u64]) -> GenesisClaim {
    let ids = |ids: &mut dyn Iterator<Item = u64>| ids.map(NodeId::new).collect::<BTreeSet<_>>();
    GenesisClaim {
        meta_voters: VoterSet::new(ids(&mut meta_voters.iter().copied())).expect("meta voters"),
        peers: NodeSet::new(ids(&mut (1..=nodes))).expect("peers"),
        group_count: GroupCount::new(
            NonZeroU32::new(u32::try_from(JOURNAL_GROUPS.len()).expect("group count"))
                .expect("groups"),
        ),
        replication_factor: ReplicationFactor::new(NonZeroU8::new(3).expect("factor")),
    }
}

/// The meta recovery transport of one node: the genesis probe, meta
/// barriers and forwarded control commands over the meta plane's
/// in-process registry, cut off by the network policy.
#[derive(Clone)]
pub(super) struct MetaTransport {
    registry: InProcessMetaRegistry,
    policy: InProcessRaftNetworkPolicy,
    node_id: u64,
}

impl ursula_raft::RecoveryTransport for MetaTransport {
    type Error = RecoveryProbeError;

    async fn probe(&self, peer: u64) -> Option<PeerGroupLog> {
        if self.policy.partitioned(self.node_id, peer) {
            return None;
        }
        self.registry.probe(peer).await
    }

    async fn genesis_probe(&self, peer: u64, probe: &GenesisProbe) -> Option<PeerGroupLog> {
        if self.policy.partitioned(self.node_id, peer) {
            return None;
        }
        self.registry.genesis_probe(peer, probe.clone()).await
    }

    async fn barrier(&self, leader: u64, target: RaftTarget) -> Result<BarrierAnswer, Self::Error> {
        assert_eq!(target, RaftTarget::Meta, "the meta transport asks meta");
        if self.policy.partitioned(self.node_id, leader) {
            return Err(RecoveryProbeError::Partitioned);
        }
        self.registry
            .barrier(leader)
            .await
            .map_err(RecoveryProbeError::Proof)
    }

    async fn forward_control(
        &self,
        leader: u64,
        command: ControlCommand,
    ) -> Result<ControlResponse, MetaForwardError> {
        if self.policy.partitioned(self.node_id, leader) {
            return Err(MetaForwardError::leader_unreachable());
        }
        self.registry
            .write_meta(leader, command, NodeId::new(self.node_id))
            .await
    }
}

/// How a [`MetaCluster`]'s nodes host their data replicas.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Hosting {
    /// Every node is a voter of every data group, which node 1 initializes
    /// ([`super::recovery_wiring::fixed_placement`]), and the groups form
    /// before the cluster starts.
    Static,
    /// Each node's hosting driver opens what its journals hold and what its
    /// placement gives it ([`super::placed_hosting`]). The groups form once
    /// meta bootstrapped.
    Placed,
}

/// The data cluster with a meta runtime on every running node.
pub(super) struct MetaCluster {
    pub(super) data: JournalCluster,
    meta: InProcessMetaRegistry,
    /// The claim each node starts its meta runtime with.
    pub(super) claims: BTreeMap<u64, GenesisClaim>,
    pub(super) runtimes: BTreeMap<u64, SimMetaRuntime>,
    /// The placed data plane, when the nodes host by placement.
    pub(super) placed: Option<Placed>,
    /// The processes of a cluster whose nodes run behind the identity
    /// fence ([`Self::start_fenced`]).
    fenced: Option<Processes>,
}

/// The processes of a fenced cluster: each node's runs start behind a disk
/// lease as their node of one cluster, and each process stamps its RPCs
/// with its fence and incarnation and follows its own routes.
struct Processes {
    /// The tag of each node's current process.
    current: BTreeMap<u64, u64>,
    next: u64,
}

/// A node process that no longer answers for its node: its volume moved
/// away ([`MetaCluster::move_volume`]), and it runs on, as a process the
/// operator cannot reach does, until [`Self::kill`].
pub(super) struct Zombie {
    /// The simulation's tag of the process.
    pub(super) process: u64,
    runtime: SimMetaRuntime,
    host: DetachedHost,
    wal: SimNodeWal,
}

impl Zombie {
    /// Kills the process: its meta runtime, driver and replicas stop, and
    /// its run ends without a record, as a killed process's does.
    pub(super) async fn kill(self) {
        if let Err(error) = self.runtime.shutdown().await {
            tracing::info!(%error, "a detached process's meta runtime stopped with an error");
        }
        self.host.kill().await;
        self.wal.end_run();
    }
}

impl MetaCluster {
    /// `nodes` nodes whose data WALs run with `tuning`, each started with
    /// its claim in `claims` (`claim` by default): every node's meta
    /// runtime, then its data replicas by `hosting`. Static data groups
    /// then form.
    pub(super) async fn start(
        name: &str,
        tuning: JournalTuning,
        nodes: u64,
        claims: BTreeMap<u64, GenesisClaim>,
        hosting: Hosting,
    ) -> Self {
        let mut cluster = Self {
            data: JournalCluster::provisioned(name, tuning, nodes),
            meta: InProcessMetaRegistry::default(),
            claims,
            runtimes: BTreeMap::new(),
            placed: (hosting == Hosting::Placed).then(Placed::default),
            fenced: None,
        };
        cluster.start_nodes(nodes, hosting).await;
        cluster
    }

    /// [`Self::start`] for nodes hosting by placement behind the identity
    /// fence, as servers run: each node's runs start as their node of the
    /// cluster `cluster_id` behind a disk lease of `lease`, and each process
    /// binds its incarnation and fence, so the Raft RPCs of both planes
    /// carry them. The recovery transport's barriers, genesis probe and
    /// `write_meta` carry no incarnation and meet no fence.
    pub(super) async fn start_fenced(
        name: &str,
        tuning: JournalTuning,
        nodes: u64,
        claims: BTreeMap<u64, GenesisClaim>,
        lease: Duration,
    ) -> Self {
        let cluster_id = ursula_headers::ClusterId::new("sim-fence").expect("a valid cluster id");
        let mut data = JournalCluster::provisioned(name, tuning, nodes);
        for (node_id, wal) in &mut data.wals {
            *wal = wal.clone().with_fenced_runs(FencedRuns {
                node: *node_id,
                cluster_id: cluster_id.clone(),
                lease,
            });
        }
        let mut cluster = Self {
            data,
            meta: InProcessMetaRegistry::default(),
            claims,
            runtimes: BTreeMap::new(),
            placed: Some(Placed::default()),
            fenced: Some(Processes {
                current: BTreeMap::new(),
                next: 1,
            }),
        };
        cluster.start_nodes(nodes, Hosting::Placed).await;
        cluster
    }

    async fn start_nodes(&mut self, nodes: u64, hosting: Hosting) {
        for node_id in 1..=nodes {
            self.start_node(node_id).await;
        }
        if hosting == Hosting::Static {
            self.data.form_groups().await;
        }
    }

    /// The tag the next process a fenced cluster starts gets.
    pub(super) fn next_process(&self) -> Option<u64> {
        self.fenced.as_ref().map(|fenced| fenced.next)
    }

    /// The tag of `node_id`'s current process in a fenced cluster.
    pub(super) fn process(&self, node_id: u64) -> Option<u64> {
        self.fenced.as_ref()?.current.get(&node_id).copied()
    }

    /// Moves `node_id`'s volume away from its running process to a new
    /// root named after `name` ([`SimDisk::move_volume`]). The process runs
    /// on as the returned [`Zombie`], which the RPCs of the processes in
    /// `reaching` keep reaching ([`Self::pin_route`]). The node answers no
    /// other RPC until its next start, which runs on the moved volume as a
    /// new process. A fenced cluster only.
    pub(super) fn move_volume(&mut self, node_id: u64, name: &str, reaching: &[u64]) -> Zombie {
        for sender in reaching {
            self.pin_route(*sender, node_id);
        }
        let process = self.process(node_id).expect("a fenced cluster's node runs");
        let runtime = self
            .runtimes
            .remove(&node_id)
            .expect("the node's meta runtime runs");
        let host = self.take_host(node_id).expect("the node's host runs");
        let wal = self.data.wals[&node_id].clone();
        let root = SimDisk::move_volume(wal.root(), name).expect("move the node's volume");
        self.data.wals.insert(node_id, wal.moved_to(root));
        self.data.new_process(node_id);
        // The node answers nothing until its next process registers, but
        // a route pinned to the old process still reaches it.
        self.meta.unregister(node_id);
        for registry in self.data.group_registries() {
            registry.unregister(node_id);
        }
        Zombie {
            process,
            runtime,
            host,
            wal,
        }
    }

    /// Pins the RPCs that `process` sends to `target`, on both planes, to
    /// the process `target` runs now ([`InProcessRaftRegistry::pin_route`]).
    pub(super) fn pin_route(&self, process: u64, target: u64) {
        assert!(
            self.meta.pin_route(process, target),
            "node {target}'s meta plane is registered"
        );
        for registry in self.data.group_registries() {
            registry.pin_route(process, target);
        }
    }

    /// Drops the RPCs that `process` sends to `target`, on both planes.
    pub(super) fn cut_route(&self, process: u64, target: u64) {
        self.meta.cut_route(process, target);
        for registry in self.data.group_registries() {
            registry.cut_route(process, target);
        }
    }

    /// Forgets every route, on both planes.
    pub(super) fn clear_routes(&self) {
        self.meta.clear_routes();
        for registry in self.data.group_registries() {
            registry.clear_routes();
        }
    }

    /// Starts `node_id`'s meta runtime on its meta journal, then its data
    /// replicas, or its hosting driver when the nodes host by placement.
    pub(super) async fn start_node(&mut self, node_id: u64) {
        self.start_meta(node_id).await;
        if self.placed.is_some() {
            self.start_host(node_id);
        } else {
            self.data.start_node(node_id).await;
        }
    }

    async fn start_meta(&mut self, node_id: u64) {
        let process = self.fenced.as_mut().map(|fenced| {
            let process = fenced.next;
            fenced.next = fenced.next.saturating_add(1);
            fenced.current.insert(node_id, process);
            process
        });
        let wal = self.data.wals[&node_id].clone();
        let raft_wal = wal.raft_wal();
        let metrics = RuntimeMetrics::new(1, 1).group_engine_metrics();
        let journal = MetaJournal {
            opened: wal.open_meta(metrics.clone()),
            metrics,
        };
        let registry = self.registry(node_id);
        let claim = self.claims[&node_id].clone();
        let identity = match process {
            // As a server binds its node: its process, behind its fence.
            Some(_) => PeerIdentity::of_process(
                node_id,
                raft_wal.cluster_id().clone(),
                raft_wal.fence(),
                raft_wal.incarnation(),
            ),
            None => PeerIdentity::new(node_id, raft_wal.cluster_id().clone()),
        };
        registry.peer_cluster().bind(
            identity,
            PeerDirectory::new(claim.peers.iter().map(|peer| {
                let url = format!("http://sim-node-{}", peer.get());
                (peer.get(), PeerEndpoints::new(url.clone(), url))
            })),
        );
        if process.is_some() {
            // Before any group opens, so none campaigns while a wait is owed.
            ursula_raft::predecessor_wait::sync_fence_shed(&registry, &raft_wal.fence());
        }
        self.meta.register(node_id, &registry);
        let mut network = InProcessMetaNetworkFactory::new(self.meta.clone())
            .with_source(node_id)
            .with_policy(self.data.policy.clone());
        if let Some(process) = process {
            network = network.with_process(process, registry.clone());
        }
        let runtime = MetaRuntime::start(
            MetaRuntimeConfig {
                node: node_id,
                claim,
                formation: FormationParams {
                    max_concurrent_moves: MoveLimit::new(2),
                },
                engine: ursula_raft::RaftEngineConfig::default(),
                snapshots: Some(SNAPSHOTS),
            },
            journal,
            raft_wal,
            registry,
            network,
            self.transport(node_id),
        )
        .await;
        self.runtimes.insert(node_id, runtime);
    }

    /// Kills `node_id`'s process: stops its meta runtime and its data
    /// replicas, then ends its WAL run as a crashed process does (the page
    /// cache survives). Its next start is a new run, which reads the run
    /// state this one left and decides again how the journals open.
    pub(super) async fn stop_node(&mut self, node_id: u64) {
        self.stop_replicas(node_id).await;
        self.data.wals[&node_id].process_crash().await;
    }

    /// Stops `node_id`'s meta runtime, then its data replicas. The WAL run
    /// stays: the caller ends it.
    async fn stop_replicas(&mut self, node_id: u64) {
        if let Some(runtime) = self.runtimes.remove(&node_id) {
            runtime.shutdown().await.expect("stop the meta runtime");
        }
        if self.placed.is_some() {
            self.stop_host(node_id).await;
            self.data.new_process(node_id);
        } else {
            self.data.stop_node(node_id).await;
        }
    }

    /// Shuts `node_id` down cleanly and removes every file of its meta
    /// journal (`raft-log/meta/`, the meta snapshot record included), as an
    /// operator who discards it does. Its data journals stay, so its next
    /// start opens the lost meta journal over data history (design 3.2).
    pub(super) async fn discard_meta(&mut self, node_id: u64) {
        let wal = self.data.wals[&node_id].clone();
        wal.clean_shutdown(self.stop_replicas(node_id)).await;
        remove_tree(&wal.root().join("meta"));
    }

    /// Kills `node_id`'s process and removes its run state, as a host that
    /// lost it does: how the last run ended is unknown, so its next start
    /// opens its journals `Recovering`, the meta journal included, with the
    /// history they hold.
    pub(super) async fn lose_run_state(&mut self, node_id: u64) {
        self.stop_node(node_id).await;
        let root = self.data.wals[&node_id].root().to_owned();
        SimDisk::remove_file(&root.join(RUN_STATE_FILE)).expect("remove the run state");
        SimDisk::sync_dir(&root).expect("sync the node's root");
    }

    /// Stops `node_id` and gives it an empty disk, data and meta journals
    /// alike.
    pub(super) async fn wipe(&mut self, node_id: u64, name: &str) {
        self.stop_replicas(node_id).await;
        let tuning = JournalTuning::new(ursula_config::WalFsync::Always);
        self.data.new_disk(node_id, name, tuning);
    }

    /// Appends `count` payloads to `group` through its leader, and answers
    /// `false` at the first one it does not take.
    pub(super) async fn try_append(&mut self, group: u32, count: usize) -> bool {
        if self.placed.is_some() {
            self.placed_append(group, count).await
        } else {
            self.data.try_append(group, count).await
        }
    }

    /// `node_id`'s registry: its process's, which its meta runtime publishes
    /// its meta replica and placement views in.
    pub(super) fn registry(&self, node_id: u64) -> RaftGroupHandleRegistry {
        self.data.node_registries[&node_id].clone()
    }

    fn transport(&self, node_id: u64) -> MetaTransport {
        MetaTransport {
            registry: self.meta.clone(),
            policy: self.data.policy.clone(),
            node_id,
        }
    }

    /// How `node_id` writes to the meta group.
    pub(super) fn handle(&self, node_id: u64) -> MetaHandle<MetaTransport> {
        MetaHandle::new(
            NodeId::new(node_id),
            self.registry(node_id),
            self.transport(node_id),
            &self.claims[&node_id],
        )
    }

    /// The placement view `node_id` holds now.
    pub(super) fn view(&self, node_id: u64) -> std::sync::Arc<PlacementView> {
        self.registry(node_id).placement_watch().current()
    }

    /// The nodes whose meta runtime runs.
    pub(super) fn running(&self) -> Vec<u64> {
        self.runtimes.keys().copied().collect()
    }

    /// The meta replica of each node in `nodes` as the recovery tool reads
    /// it from the node's metrics, skipping nodes that publish none.
    pub(super) fn plan_members(&self, nodes: &[u64]) -> Vec<PlanMember> {
        nodes
            .iter()
            .filter_map(|node_id| self.registry(*node_id).meta_metrics_snapshot())
            .map(|snapshot| PlanMember::from_metrics(&snapshot.to_admin()))
            .collect()
    }

    /// One round of `ursulactl recover-group --target meta --accept-loss`
    /// over the meta replicas of `nodes`: the shipped planning step
    /// (`next_step`), planner and sender, through each node's production
    /// plan entry point, as the tool's loop drives them. Answers the plan
    /// once it is satisfied, and `None` while the tool would observe again.
    pub(super) async fn recover_meta_round(
        &mut self,
        state: &mut LoopState,
        nodes: &[u64],
    ) -> Result<Option<GroupPlan>, MetaRecoverFailed> {
        let members = self.plan_members(nodes);
        let mut transport = MetaPlanTransport { cluster: self };
        recover_round(state, &members, &mut transport, PlanFloor::Meta).await
    }
}

/// One round of `ursulactl recover-group --accept-loss` over `members`:
/// the shipped planning step (`next_step`), planner and sender over
/// `transport`, as the tool's loop drives them. Answers the plan once it is
/// satisfied, and `None` while the tool would observe again.
pub(super) async fn recover_round<T: PlanTransport>(
    state: &mut LoopState,
    members: &[PlanMember],
    transport: &mut T,
    floor: PlanFloor,
) -> Result<Option<GroupPlan>, MetaRecoverFailed> {
    match next_step(state, members, MAX_META_PLANS)? {
        Step::Done(RecoverOutcome::Satisfied { plan, .. }) => Ok(Some(plan)),
        Step::Done(outcome) => Err(MetaRecoverFailed::NoPlan { outcome }),
        Step::Wait => Ok(None),
        Step::Plan(plan) => {
            state.plans_sent = state.plans_sent.saturating_add(1);
            let sent = send_plan(transport, plan, floor, members).await;
            plan_sent(state, plan, sent, &electorate(&plan, members))?;
            Ok(None)
        }
        Step::Resume { plan, leader_term } => {
            let sent = send_follows(transport, plan, floor, leader_term, members).await;
            plan_sent(state, plan, Ok(sent), &electorate(&plan, members))?;
            Ok(None)
        }
    }
}

/// [`PlanTransport`] over each node's production meta plan entry point
/// (`MetaPlans::accept_recovery_plan`), its refusal typed as the node's
/// route types it.
struct MetaPlanTransport<'a> {
    cluster: &'a MetaCluster,
}

impl MetaPlanTransport<'_> {
    async fn accept(
        &self,
        node_id: u64,
        request: &RecoveryPlanRequest,
    ) -> Result<RecoveryPlanReport, SendError> {
        let plans = self
            .cluster
            .runtimes
            .get(&node_id)
            .ok_or(SendError::NotInManifest { node_id })?
            .plans();
        plans
            .accept_recovery_plan(request)
            .await
            .map_err(|error| match error.refusal() {
                Some(refusal) => SendError::Refused {
                    node_id,
                    refused: RecoveryPlanRefused {
                        raft_group_id: 0,
                        node_id,
                        refusal,
                        message: error.to_string(),
                    },
                },
                None => SendError::Failed {
                    node_id,
                    source: Box::new(error),
                },
            })
    }
}

impl PlanTransport for MetaPlanTransport<'_> {
    fn send(
        &mut self,
        node_id: u64,
        request: &RecoveryPlanRequest,
    ) -> impl std::future::Future<Output = Result<RecoveryPlanReport, SendError>> {
        self.accept(node_id, request)
    }
}

/// Removes every file under `dir`, and makes the removals durable.
fn remove_tree(dir: &Path) {
    let Ok(entries) = SimDisk::read_dir(dir) else {
        return;
    };
    for path in entries {
        if SimDisk::remove_file(&path).is_err() {
            remove_tree(&path);
        }
    }
    SimDisk::sync_dir(dir).expect("sync the emptied directory");
}
