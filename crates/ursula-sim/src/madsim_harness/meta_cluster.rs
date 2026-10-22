//! The meta group on the journal-backed nodes of a [`JournalCluster`]
//! ([`MetaCluster`]): every node runs the production meta runtime
//! ([`MetaRuntime`]) over its node's meta journal, with the in-process
//! meta network and recovery transport, beside the node's data replicas.
//!
//! A node opens its meta journal before its data replicas, as the server's
//! spawn path does, so a lost meta journal over data history is told apart
//! from a new one. A fenced cluster's data and meta planes share one
//! network policy, so a partition cuts both. Every other cluster's meta
//! plane has a policy of its own ([`MetaCluster::meta_policy`]), so a
//! partition of one plane leaves the other. The data replicas are static, every
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
use ursula_ctl::recovery::Floor;
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
use ursula_proto::admin::RecoveryPlanRefused;
use ursula_proto::admin::RecoveryPlanReport;
use ursula_proto::admin::RecoveryPlanRequest;
use ursula_proto::placement::GenesisClaim;
use ursula_proto::placement::GroupCount;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeSet;
use ursula_proto::placement::ProcessIncarnation;
use ursula_proto::placement::ReplicationFactor;
use ursula_proto::placement::RootSlot;
use ursula_proto::placement::VoterSet;
use ursula_raft::BarrierAnswer;
use ursula_raft::InProcessCaller;
use ursula_raft::InProcessMetaNetworkFactory;
use ursula_raft::InProcessMetaRegistry;
use ursula_raft::InProcessRaftNetworkPolicy;
use ursula_raft::InProcessRaftRegistry;
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
use ursula_raft::meta::pause::MetaPausePoint;
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
use super::recovery_wiring::in_process_barrier;
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
    claim_with_factor(nodes, meta_voters, 3)
}

/// [`claim`] at replication factor `factor`.
pub(super) fn claim_with_factor(nodes: u64, meta_voters: &[u64], factor: u8) -> GenesisClaim {
    let ids = |ids: &mut dyn Iterator<Item = u64>| ids.map(NodeId::new).collect::<BTreeSet<_>>();
    GenesisClaim {
        meta_voters: VoterSet::new(ids(&mut meta_voters.iter().copied())).expect("meta voters"),
        peers: NodeSet::new(ids(&mut (1..=nodes))).expect("peers"),
        group_count: GroupCount::new(
            NonZeroU32::new(u32::try_from(JOURNAL_GROUPS.len()).expect("group count"))
                .expect("groups"),
        ),
        replication_factor: ReplicationFactor::new(NonZeroU8::new(factor).expect("factor")),
    }
}

/// The meta recovery transport of one node process: the genesis probe,
/// meta barriers and forwarded control commands over the meta plane's
/// in-process registry, and the data barriers its hosting driver's
/// membership asker sends over each data group's, each cut off by its own
/// plane's network policy. A fenced cluster's process makes every call as
/// itself ([`InProcessCaller`]).
#[derive(Clone)]
pub(super) struct MetaTransport {
    registry: InProcessMetaRegistry,
    /// The in-process network of each data group.
    data: BTreeMap<u32, InProcessRaftRegistry>,
    /// The data plane's policy, which judges the data barriers.
    data_policy: InProcessRaftNetworkPolicy,
    /// The meta plane's policy, which judges every other call.
    meta_policy: InProcessRaftNetworkPolicy,
    caller: InProcessCaller,
}

impl MetaTransport {
    fn node_id(&self) -> u64 {
        self.caller.node_id()
    }
}

impl ursula_raft::RecoveryTransport for MetaTransport {
    type Error = RecoveryProbeError;

    async fn probe(&self, peer: u64) -> Option<PeerGroupLog> {
        if self.meta_policy.partitioned(self.node_id(), peer) {
            return None;
        }
        self.registry.probe(&self.caller, peer).await
    }

    async fn genesis_probe(&self, peer: u64, probe: &GenesisProbe) -> Option<PeerGroupLog> {
        if self.meta_policy.partitioned(self.node_id(), peer) {
            return None;
        }
        self.registry
            .genesis_probe(&self.caller, peer, probe.clone())
            .await
    }

    async fn barrier(&self, leader: u64, target: RaftTarget) -> Result<BarrierAnswer, Self::Error> {
        if let RaftTarget::Data(group) = target {
            // The simulation's groups are fixed: no network reaches another.
            let registry = self
                .data
                .get(&group.0)
                .ok_or(RecoveryProbeError::Partitioned)?;
            return in_process_barrier(registry, &self.data_policy, &self.caller, leader, target)
                .await;
        }
        if self.meta_policy.partitioned(self.node_id(), leader) {
            return Err(RecoveryProbeError::Partitioned);
        }
        self.registry
            .barrier(&self.caller, leader)
            .await
            .map_err(RecoveryProbeError::Proof)
    }

    async fn forward_control(
        &self,
        leader: u64,
        command: ControlCommand,
    ) -> Result<ControlResponse, MetaForwardError> {
        if self.meta_policy.partitioned(self.node_id(), leader) {
            return Err(MetaForwardError::leader_unreachable());
        }
        self.registry
            .write_meta(&self.caller, leader, command)
            .await
    }
}

/// Whether a process's node answers RPCs through it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Listed {
    /// Its peers reach it, as the node's current process.
    Yes,
    /// No peer reaches it ([`MetaCluster::start_stranger`]).
    No,
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
    /// The meta pause point the next meta runtime started closes at once
    /// ([`Self::pause_next_start`]).
    pause_next: Option<MetaPausePoint>,
    /// The meta plane's network policy: the data plane's for a fenced
    /// cluster, else one of its own.
    pub(super) meta_policy: InProcessRaftNetworkPolicy,
}

/// The processes of a fenced cluster: each node's runs start behind a disk
/// lease as their node of one cluster, and each process stamps its RPCs
/// with its fence and incarnation and follows its own routes.
struct Processes {
    /// The tag of each node's current process.
    current: BTreeMap<u64, u64>,
    next: u64,
    /// The node and incarnation of every process started, by tag.
    started: BTreeMap<u64, (u64, ProcessIncarnation)>,
}

/// A node process that no longer answers for its node, which runs on, as a
/// process the operator cannot reach does, until [`Self::kill`]: its volume
/// moved away ([`MetaCluster::move_volume`]), it keeps its disk while its
/// node runs a new one ([`MetaCluster::replace_disk`]), or no peer reaches
/// it ([`MetaCluster::start_stranger`]).
pub(super) struct Zombie {
    /// The simulation's tag of the process.
    pub(super) process: u64,
    runtime: SimMetaRuntime,
    host: Option<DetachedHost>,
    wal: SimNodeWal,
    registry: RaftGroupHandleRegistry,
}

impl Zombie {
    /// Kills the process: its meta runtime, driver and replicas stop, and
    /// its run ends without a record, as a killed process's does. Answers
    /// its journals, which a later start may open.
    pub(super) async fn kill(self) -> SimNodeWal {
        if let Err(error) = self.runtime.shutdown().await {
            tracing::info!(%error, "a detached process's meta runtime stopped with an error");
        }
        if let Some(host) = self.host {
            host.kill().await;
        }
        self.wal.end_run();
        self.wal
    }

    /// The process's registration with the meta group, as its status shows
    /// it.
    pub(super) fn registration(&self) -> ursula_control::RegistrationStatus {
        self.runtime.status_reader().placement_status().registration
    }

    /// The root the process runs on.
    pub(super) fn fence_root(&self) -> ursula_proto::placement::RootId {
        self.wal.raft_wal().incarnation().root
    }

    /// The process's registry, which its meta replica publishes in.
    pub(super) fn registry(&self) -> &RaftGroupHandleRegistry {
        &self.registry
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
            pause_next: None,
            meta_policy: InProcessRaftNetworkPolicy::default(),
        };
        cluster.start_nodes(nodes, hosting).await;
        cluster
    }

    /// [`Self::start`] for nodes hosting by placement behind the identity
    /// fence, as servers run: each node's runs start as their node of the
    /// cluster `cluster_id` behind a disk lease of `lease`, and each process
    /// binds its incarnation and fence, so the Raft RPCs and recovery calls
    /// of both planes carry them, follow the process's routes and meet the
    /// fences at both ends.
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
        let meta_policy = data.policy.clone();
        let mut cluster = Self {
            data,
            meta: InProcessMetaRegistry::default(),
            claims,
            runtimes: BTreeMap::new(),
            placed: Some(Placed::default()),
            fenced: Some(Processes {
                current: BTreeMap::new(),
                next: 1,
                started: BTreeMap::new(),
            }),
            pause_next: None,
            meta_policy,
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

    /// The tag and incarnation of every process a fenced cluster started,
    /// by tag, with its node.
    pub(super) fn started_processes(&self) -> BTreeMap<u64, (u64, ProcessIncarnation)> {
        self.fenced
            .as_ref()
            .map(|fenced| fenced.started.clone())
            .unwrap_or_default()
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
        let zombie = self.detach(node_id);
        let root = SimDisk::move_volume(zombie.wal.root(), name).expect("move the node's volume");
        self.data.wals.insert(node_id, zombie.wal.moved_to(root));
        zombie
    }

    /// Takes `node_id`'s running process out of the cluster with its disk,
    /// and gives the node an empty disk named after `name`, whose next
    /// start binds a new root once an operator released the old one: the
    /// release rule broken, as when a node whose old process still runs on
    /// its disk gets a replacement disk. The returned [`Zombie`] keeps its
    /// disk and lease, and answers only RPCs whose routes reach it. A
    /// fenced cluster only.
    pub(super) fn replace_disk(&mut self, node_id: u64, name: &str) -> Zombie {
        let zombie = self.detach(node_id);
        self.new_fenced_disk(node_id, name);
        zombie
    }

    /// Gives the stopped `node_id` an empty disk named after `name`, whose
    /// runs start as the node's runs do.
    pub(super) fn new_fenced_disk(&mut self, node_id: u64, name: &str) {
        let runs = self.data.wals[&node_id].fenced_runs();
        let tuning = self.data.wals[&node_id].tuning();
        self.data.new_disk(node_id, name, tuning);
        if let Some(runs) = runs {
            let wal = self.data.wals[&node_id].clone().with_fenced_runs(runs);
            self.data.wals.insert(node_id, wal);
        }
    }

    /// Takes `node_id`'s running process out of the cluster as a
    /// [`Zombie`]: its node answers nothing until its next process
    /// registers, but a route pinned to the old process still reaches it.
    fn detach(&mut self, node_id: u64) -> Zombie {
        let zombie = self.take_process(node_id);
        self.data.new_process(node_id);
        self.meta.unregister(node_id);
        for registry in self.data.group_registries() {
            registry.unregister(node_id);
        }
        zombie
    }

    /// Takes `node_id`'s running process out of the cluster's maps, as it
    /// is: what it registered stays registered.
    fn take_process(&mut self, node_id: u64) -> Zombie {
        let process = self.process(node_id).expect("a fenced cluster's node runs");
        let runtime = self
            .runtimes
            .remove(&node_id)
            .expect("the node's meta runtime runs");
        let host = self.take_host(node_id);
        Zombie {
            process,
            runtime,
            host,
            wal: self.data.wals[&node_id].clone(),
            registry: self.registry(node_id),
        }
    }

    /// Puts `zombie`, a process taken out of `node_id` by
    /// [`Self::take_process`] whose registrations stand, back as the node's
    /// current process.
    fn put_process(&mut self, node_id: u64, zombie: Zombie) {
        let Zombie {
            process,
            runtime,
            host,
            wal,
            registry,
        } = zombie;
        if let Some(fenced) = self.fenced.as_mut() {
            fenced.current.insert(node_id, process);
        }
        self.runtimes.insert(node_id, runtime);
        if let Some(host) = host {
            self.put_host(node_id, host);
        }
        self.data.wals.insert(node_id, wal);
        self.data.node_registries.insert(node_id, registry);
    }

    /// Takes `node_id`'s running process out of the cluster, as
    /// [`Self::move_volume`] does but with its disk, for a process the
    /// caller kills: one that fenced itself for good, whose stop may fail.
    pub(super) fn retire(&mut self, node_id: u64) -> Zombie {
        self.detach(node_id)
    }

    /// Makes `stranger`, a process of `node_id` that no peer reached
    /// ([`Self::start_stranger`]), the node's process, which its peers now
    /// reach: as when the operator points the node's address at its host,
    /// once the old process is gone. Its hosting driver starts.
    pub(super) fn promote(&mut self, node_id: u64, stranger: Zombie) {
        let registry = stranger.registry.clone();
        self.put_process(node_id, stranger);
        self.meta.register(node_id, &registry);
        self.start_host(node_id);
    }

    /// Starts a process of `node_id` on an empty disk named after `name`
    /// that no peer reaches: a second host configured with the node's id,
    /// at an address no peer dials, beside the node's running process. It
    /// sends its registration and its Raft RPCs, and runs no hosting
    /// driver: an unbound root opens no data replica. The node's running
    /// process stays the node's. A fenced cluster only.
    pub(super) async fn start_stranger(&mut self, node_id: u64, name: &str) -> Zombie {
        let node = self.take_process(node_id);
        self.new_fenced_disk(node_id, name);
        let disk = self.data.wals[&node_id].clone();
        self.put_process(node_id, node);
        self.start_stranger_on(node_id, disk).await
    }

    /// [`Self::start_stranger`] on `disk`, a disk of `node_id` no running
    /// process holds.
    pub(super) async fn start_stranger_on(&mut self, node_id: u64, disk: SimNodeWal) -> Zombie {
        let node = self.take_process(node_id);
        self.data.new_process(node_id);
        self.data.wals.insert(node_id, disk);
        self.start_meta(node_id, Listed::No).await;
        let stranger = self.take_process(node_id);
        self.put_process(node_id, node);
        stranger
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

    /// Pins the RPCs that `process` sends to `target` in data group
    /// `group` to the process `target` runs now.
    pub(super) fn pin_group_route(&self, group: u32, process: u64, target: u64) {
        assert!(
            self.data.group_registry(group).pin_route(process, target),
            "node {target}'s replica of group {group} is registered"
        );
    }

    /// Drops the RPCs that `process` sends to `target` in data group
    /// `group`.
    pub(super) fn cut_group_route(&self, group: u32, process: u64, target: u64) {
        self.data.group_registry(group).cut_route(process, target);
    }

    /// Drops the RPCs and recovery calls that `process` sends to `target`
    /// on the meta plane.
    pub(super) fn cut_meta_route(&self, process: u64, target: u64) {
        self.meta.cut_route(process, target);
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
        self.start_meta(node_id, Listed::Yes).await;
        if self.placed.is_some() {
            self.start_host(node_id);
        } else {
            self.data.start_node(node_id).await;
        }
    }

    async fn start_meta(&mut self, node_id: u64, listed: Listed) {
        let process = self.fenced.as_mut().map(|fenced| {
            let process = fenced.next;
            fenced.next = fenced.next.saturating_add(1);
            fenced.current.insert(node_id, process);
            process
        });
        let wal = self.data.wals[&node_id].clone();
        let raft_wal = wal.raft_wal();
        if let (Some(process), Some(fenced)) = (process, self.fenced.as_mut()) {
            fenced
                .started
                .insert(process, (node_id, raft_wal.incarnation()));
        }
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
        if listed == Listed::Yes {
            self.meta.register(node_id, &registry);
        }
        let mut network = InProcessMetaNetworkFactory::new(self.meta.clone())
            .with_source(node_id)
            .with_policy(self.meta_policy.clone());
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
        // Before anything awaits, so the runtime's tasks have not run yet.
        if let Some(point) = self.pause_next.take() {
            drop(runtime.pauses().pause(point));
        }
        self.runtimes.insert(node_id, runtime);
    }

    /// Makes the next meta runtime this cluster starts wait at `point` for
    /// the rest of its run.
    pub(super) fn pause_next_start(&mut self, point: MetaPausePoint) {
        self.pause_next = Some(point);
    }

    /// The node whose meta replica leads, as the running nodes' status
    /// reports name it, if that node runs.
    pub(super) fn meta_leader(&self) -> Option<u64> {
        self.running().into_iter().find_map(|node_id| {
            match self.runtimes[&node_id]
                .status_reader()
                .placement_status()
                .meta
            {
                ursula_control::MetaReplicaStatus::Running {
                    leader: Some(leader),
                    ..
                } if self.runtimes.contains_key(&leader.get()) => Some(leader.get()),
                _ => None,
            }
        })
    }

    /// The root slot `node_id`'s view gives `of`, once that view is
    /// bootstrapped.
    pub(super) fn slot_in_view(&self, node_id: u64, of: u64) -> Option<RootSlot> {
        self.view(node_id)
            .cluster()?
            .nodes()
            .get(&NodeId::new(of))
            .map(|record| record.root)
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

    /// The meta recovery transport of `node_id`'s current process.
    fn transport(&self, node_id: u64) -> MetaTransport {
        let caller = match self.process(node_id) {
            Some(process) => InProcessCaller::process(
                node_id,
                process,
                self.registry(node_id).peer_cluster().clone(),
            )
            .with_policy(self.meta_policy.clone()),
            None => InProcessCaller::node(node_id),
        };
        MetaTransport {
            registry: self.meta.clone(),
            data: self.data.group_registry_map(),
            data_policy: self.data.policy.clone(),
            meta_policy: self.meta_policy.clone(),
            caller,
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
        recover_round(state, &members, &mut transport, &Floor::Meta).await
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
    floor: &Floor,
) -> Result<Option<GroupPlan>, MetaRecoverFailed> {
    match next_step(state, members, floor, MAX_META_PLANS)? {
        Step::Done(RecoverOutcome::Satisfied { plan, .. }) => Ok(Some(plan)),
        Step::Done(outcome) => Err(MetaRecoverFailed::NoPlan { outcome }),
        Step::Wait => Ok(None),
        Step::Plan(plan) => {
            state.plans_sent = state.plans_sent.saturating_add(1);
            let sent = send_plan(transport, plan, floor.plan_floor(), members).await;
            plan_sent(state, plan, sent, &electorate(&plan, members, floor))?;
            Ok(None)
        }
        Step::Resume { plan, leader_term } => {
            let sent =
                send_follows(transport, plan, floor.plan_floor(), leader_term, members).await;
            plan_sent(state, plan, Ok(sent), &electorate(&plan, members, floor))?;
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
