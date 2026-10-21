//! The meta group on the journal-backed nodes of a [`JournalCluster`]
//! ([`MetaCluster`]): every node runs the production meta runtime
//! ([`MetaRuntime`]) over its node's meta journal, with the in-process
//! meta network and recovery transport, beside the node's data replicas.
//!
//! A node opens its meta journal before its data replicas, as the server's
//! spawn path does, so a lost meta journal over data history is told apart
//! from a new one. The data and meta planes share the cluster's one network
//! policy, so a partition cuts both. Every node is a voter of every data
//! group, as static data groups place them in S1, and the meta voters are
//! the claim's.

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
use super::recovery_wiring::RecoveryProbeError;

/// A node's meta runtime in the simulation.
pub(super) type SimMetaRuntime = MetaRuntime<InProcessMetaNetworkFactory>;

/// How many plans one meta recovery may send, as `ursulactl
/// recover-group --max-plans` bounds it.
const MAX_META_PLANS: usize = 3;

/// Why a meta recovery round ended without a satisfied plan.
#[derive(Debug, thiserror::Error)]
pub(super) enum MetaRecoverFailed {
    /// The recovery tool stopped.
    #[error(transparent)]
    Recover(#[from] RecoverError),
    /// The tool found nothing to plan.
    #[error("the meta group needed no plan: {outcome:?}")]
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
            .confirm_recovery_barrier(leader)
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

/// The data cluster with a meta runtime on every running node.
pub(super) struct MetaCluster {
    pub(super) data: JournalCluster,
    meta: InProcessMetaRegistry,
    /// The claim each node starts its meta runtime with.
    pub(super) claims: BTreeMap<u64, GenesisClaim>,
    pub(super) runtimes: BTreeMap<u64, SimMetaRuntime>,
}

impl MetaCluster {
    /// `nodes` nodes whose data WALs run with `tuning`, each started with
    /// its claim in `claims` (`claim` by default): every node's meta
    /// runtime, then its data replicas, then the data groups form.
    pub(super) async fn start(
        name: &str,
        tuning: JournalTuning,
        nodes: u64,
        claims: BTreeMap<u64, GenesisClaim>,
    ) -> Self {
        let mut cluster = Self {
            data: JournalCluster::provisioned(name, tuning, nodes),
            meta: InProcessMetaRegistry::default(),
            claims,
            runtimes: BTreeMap::new(),
        };
        for node_id in 1..=nodes {
            cluster.start_node(node_id).await;
        }
        cluster.data.form_groups().await;
        cluster
    }

    /// Starts `node_id`'s meta runtime on its meta journal, then its data
    /// replicas.
    pub(super) async fn start_node(&mut self, node_id: u64) {
        self.start_meta(node_id).await;
        self.data.start_node(node_id).await;
    }

    async fn start_meta(&mut self, node_id: u64) {
        let wal = self.data.wals[&node_id].clone();
        let raft_wal = wal.raft_wal();
        let metrics = RuntimeMetrics::new(1, 1).group_engine_metrics();
        let journal = MetaJournal {
            opened: wal.open_meta(metrics.clone()),
            metrics,
        };
        let registry = self.registry(node_id);
        let claim = self.claims[&node_id].clone();
        registry.peer_cluster().bind(
            PeerIdentity::new(node_id, raft_wal.cluster_id().clone()),
            PeerDirectory::new(claim.peers.iter().map(|peer| {
                let url = format!("http://sim-node-{}", peer.get());
                (peer.get(), PeerEndpoints::new(url.clone(), url))
            })),
        );
        self.meta.register(node_id, &registry);
        let policy = self.data.policy.clone();
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
            InProcessMetaNetworkFactory::new(self.meta.clone())
                .with_source(node_id)
                .with_policy(policy.clone()),
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
        self.data.stop_node(node_id).await;
    }

    /// Shuts `node_id` down cleanly and removes every file of its meta
    /// journal (`raft-log/meta/`, the meta snapshot record included), as an
    /// operator who discards it does. Its data journals stay, so its next
    /// start opens the lost meta journal over data history (D9).
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
        if let Some(runtime) = self.runtimes.remove(&node_id) {
            runtime.shutdown().await.expect("stop the meta runtime");
        }
        let tuning = JournalTuning::new(ursula_config::WalFsync::Always);
        self.data.wipe_node(node_id, name, tuning).await;
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
        match next_step(state, &members, MAX_META_PLANS)? {
            Step::Done(RecoverOutcome::Satisfied { plan, .. }) => Ok(Some(plan)),
            Step::Done(outcome) => Err(MetaRecoverFailed::NoPlan { outcome }),
            Step::Wait => Ok(None),
            Step::Plan(plan) => {
                state.plans_sent = state.plans_sent.saturating_add(1);
                let sent = send_plan(
                    &mut MetaPlanTransport { cluster: self },
                    plan,
                    PlanFloor::Meta,
                    &members,
                )
                .await;
                plan_sent(state, plan, sent, &electorate(&plan, &members))?;
                Ok(None)
            }
            Step::Resume { plan, leader_term } => {
                let sent = send_follows(
                    &mut MetaPlanTransport { cluster: self },
                    plan,
                    PlanFloor::Meta,
                    leader_term,
                    &members,
                )
                .await;
                plan_sent(state, plan, Ok(sent), &electorate(&plan, &members))?;
                Ok(None)
            }
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
