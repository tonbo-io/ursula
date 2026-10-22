//! Placement-driven hosting in the `placement-move` family (design 4.1 to
//! 4.3 and 15.2). Every node of a [`MetaCluster`] started placed runs the
//! production hosting driver ([`HostingDriver`]) over its journals and its
//! meta runtime's views. The driver opens and closes the node's data
//! replicas through a [`SimHost`], the simulation's [`GroupHost`], and every
//! open takes [`MetaPlacement`] as its placement inputs, as the server's
//! engine factory does. Nothing else starts a placed replica, so a node runs
//! what its journals hold and what a confirmed placement gives it.
//!
//! The hosting checks ([`HostingChecks`]) run at every observation of the
//! meta checks:
//!
//! - I2 ([`I2`]): no group is initialized twice, and no two replicas lead
//!   one group in one term.
//! - I3 ([`I3`]), in its S2 form: every membership a running replica holds
//!   is its group's seed voters, with no learner.
//! - The hosting check ([`HOSTED`]): a node runs only the groups it is a
//!   seed voter of, and no node runs one before the view of some running
//!   node bootstrapped. It does not see whether the placement that created
//!   a replica was confirmed.
//!
//! I1 ([`I1`]), I7 ([`I7`]) and I15 ([`I15`]) are waits the family runs
//! after its faults: every seed voter reads the acknowledged appends, a
//! node restarted without meta serves its groups, and every seeded group
//! forms.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;

use futures_util::future::BoxFuture;
use futures_util::lock::Mutex as AsyncMutex;
use openraft::Raft;
use openraft::ServerState;
use ursula_ctl::recovery::LoopState;
use ursula_ctl::recovery::PlanError;
use ursula_ctl::recovery::PlanMember;
use ursula_ctl::recovery::PlanTransport;
use ursula_ctl::recovery::RecoverError;
use ursula_ctl::recovery::RecoverOutcome;
use ursula_ctl::recovery::SendError;
use ursula_proto::admin::PlanFloor;
use ursula_proto::admin::RecoveryPlanRefused;
use ursula_proto::admin::RecoveryPlanReport;
use ursula_proto::admin::RecoveryPlanRequest;
use ursula_proto::placement::GenesisClaim;
use ursula_raft::GroupHost;
use ursula_raft::GroupRejoin;
use ursula_raft::HostingDriver;
use ursula_raft::HostingSet;
use ursula_raft::HostingTask;
use ursula_raft::InProcessRaftNetworkFactory;
use ursula_raft::MadsimOpenRaftRuntime;
use ursula_raft::MetaPlacement;
use ursula_raft::PlacementInputs;
use ursula_raft::RaftGroupEngine;
use ursula_raft::RaftGroupEngineOptions;
use ursula_raft::RaftGroupStateMachine;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::FollowerRead;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineError;
use ursula_runtime::ReadStreamRequest;
use ursula_shard::RaftGroupId;

use super::SimTrace;
use super::journal_cluster::JOURNAL_GROUPS;
use super::journal_cluster::ReplicaParts;
use super::journal_cluster::group_placement;
use super::journal_cluster::group_stream;
use super::meta_checks::BOUND;
use super::meta_checks::STEP;
use super::meta_checks::fail;
use super::meta_cluster::MetaCluster;
use super::meta_cluster::MetaRecoverFailed;
use super::meta_cluster::recover_round;
use super::recovery_wiring;

pub(super) const I1: &str = "acknowledged_writes_read_back";
pub(super) const I2: &str = "no_group_initialized_twice";
pub(super) const I3: &str = "memberships_are_the_seed_voters";
pub(super) const I7: &str = "a_node_restarted_without_meta_serves";
pub(super) const I15: &str = "every_seeded_group_forms";
pub(super) const HOSTED: &str = "a_node_runs_only_its_placed_groups";
pub(super) const DATA_PLAN: &str = "data_plans_recover_after_meta";

/// Mixed into the OpenRaft seed of each placed replica's start.
const HOST_SEED: u64 = 0x686f_7374_6564_0000;

/// Every gate a placed replica ever opened with, by group and node, so I2
/// counts the initializations of replicas that stopped too.
#[derive(Clone, Default)]
pub(super) struct StartedGates(Arc<Mutex<Vec<(u32, u64, Arc<GroupRejoin>)>>>);

impl StartedGates {
    fn push(&self, group: u32, node_id: u64, rejoin: Arc<GroupRejoin>) {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((group, node_id, rejoin));
    }

    /// How many replicas started so far.
    fn len(&self) -> u64 {
        let started = self
            .0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .len();
        u64::try_from(started).unwrap_or(u64::MAX)
    }

    /// The nodes whose replicas of each group ran `Initialize`.
    fn initializers(&self) -> BTreeMap<u32, Vec<u64>> {
        let mut initializers: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
        for (group, node_id, rejoin) in self
            .0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
        {
            if rejoin.initialized_here() {
                initializers.entry(*group).or_default().push(*node_id);
            }
        }
        initializers
    }
}

/// One placed replica.
struct Hosted {
    engine: Arc<AsyncMutex<RaftGroupEngine>>,
    raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
    rejoin: Arc<GroupRejoin>,
}

/// Why a [`SimHost`] did not open or close a replica.
#[derive(Debug, thiserror::Error)]
pub(super) enum SimHostError {
    #[error("group {group} is not in the hosting set")]
    NotAdmitted { group: u32 },
    #[error("the log store of group {group} did not open")]
    Store {
        group: u32,
        #[source]
        source: ursula_raft::RaftWalError,
    },
    #[error("the recovery gate of group {group} did not open")]
    Gate {
        group: u32,
        #[source]
        source: ursula_raft::GateOpenError,
    },
    #[error("the engine of group {group} did not start or stop")]
    Engine {
        group: u32,
        #[source]
        source: GroupEngineError,
    },
}

/// One node process's [`GroupHost`]: it opens a group's replica as the
/// engine factory does, over the node's journals, the in-process network and
/// the node's [`MetaPlacement`], and keeps it until a close.
pub(super) struct SimHost {
    parts: ReplicaParts,
    inputs: MetaPlacement,
    replicas: Mutex<BTreeMap<u32, Hosted>>,
    started: StartedGates,
}

impl SimHost {
    async fn start(&self, group: u32) -> Result<(), SimHostError> {
        let node_id = self.parts.node_id;
        let placement = group_placement(group);
        let admission = self
            .inputs
            .admission(placement.raft_group_id)
            .ok_or(SimHostError::NotAdmitted { group })?;
        let wal = &self.parts.wal;
        let store = wal
            .open_admitted(
                placement,
                admission,
                self.parts.metrics.group_engine_metrics(),
            )
            .map_err(|source| SimHostError::Store { group, source })?;
        let rejoin = Arc::new(
            GroupRejoin::durable(
                node_id,
                ursula_shard::RaftTarget::Data(placement.raft_group_id),
                &store,
            )
            .await
            .map_err(|source| SimHostError::Gate { group, source })?,
        );
        let registry = &self.parts.registry;
        let network = self.parts.registries[&group].clone();
        let mut config = (*self.parts.config).clone();
        config.enable_elect = registry.election_policy().may_campaign(Some(&rejoin));
        let mut options = RaftGroupEngineOptions::gated(Arc::clone(&rejoin));
        options.metrics = Some(self.parts.metrics.group_engine_metrics());
        options.peer_cluster = Some(registry.peer_cluster().clone());
        options.fence = Some(wal.raft_wal().fence());
        // The driver's tasks run outside the simulation's OpenRaft seed
        // scope, so each start gets a seed of its own, as the runtime
        // scenarios' engine factory gives each create.
        let seed = HOST_SEED
            ^ node_id.rotate_left(32)
            ^ u64::from(group).rotate_left(16)
            ^ self.started.len();
        let engine = MadsimOpenRaftRuntime::scope(
            seed,
            RaftGroupEngine::new_node(
                placement,
                node_id,
                Arc::new(config),
                InProcessRaftNetworkFactory::new(network.clone())
                    .with_source(node_id)
                    .with_policy(self.parts.policy.clone()),
                store,
                wal.recording_work(options),
            ),
        )
        .await
        .map_err(|source| SimHostError::Engine { group, source })?;
        // Nothing below awaits, so a stopped driver never leaves a replica
        // half published.
        recovery_wiring::wire_recovery(
            node_id,
            placement,
            &engine,
            &rejoin,
            &network,
            &self.parts.policy,
            &self.inputs,
            registry,
        );
        network.register(node_id, &engine);
        self.started.push(group, node_id, Arc::clone(&rejoin));
        let hosted = Hosted {
            raft: engine.raft_handle(),
            engine: Arc::new(AsyncMutex::new(engine)),
            rejoin,
        };
        self.lock().insert(group, hosted);
        Ok(())
    }

    /// Steps 2 and 3 of a close (design 7.3): the handle goes, then the
    /// engine stops and releases its store.
    async fn stop(&self, group: u32) -> Result<(), SimHostError> {
        let hosted = self.lock().remove(&group);
        self.parts.registry.unregister_group(RaftGroupId(group));
        self.parts.registries[&group].unregister(self.parts.node_id);
        let Some(hosted) = hosted else {
            return Ok(());
        };
        let engine = hosted.engine.lock().await;
        engine
            .shutdown()
            .await
            .map_err(|source| SimHostError::Engine { group, source })
    }

    /// Restarts the replica of `group` for a recovery plan's cut, as the
    /// node's runtime restarts one group's engine: it stops, and opens
    /// again from its store. The hosting driver may open it first, as it
    /// opens every admitted group the node does not run, and that replica
    /// is the restarted one.
    async fn restart(&self, group: u32) -> Option<Arc<GroupRejoin>> {
        if let Err(error) = self.stop(group).await {
            panic!(
                "node {}: stop group {group} for its plan: {error}",
                self.parts.node_id
            );
        }
        let deadline = madsim::time::Instant::now() + BOUND;
        loop {
            if let Some(rejoin) = self
                .lock()
                .get(&group)
                .map(|hosted| Arc::clone(&hosted.rejoin))
            {
                return Some(rejoin);
            }
            match self.start(group).await {
                Ok(()) => {}
                Err(error) if madsim::time::Instant::now() >= deadline => panic!(
                    "node {}: restart group {group} for its plan: {error}",
                    self.parts.node_id
                ),
                Err(_opening) => madsim::time::sleep(STEP).await,
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<u32, Hosted>> {
        self.replicas
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn groups(&self) -> Vec<u32> {
        self.lock().keys().copied().collect()
    }

    fn raft(&self, group: u32) -> Option<Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>> {
        self.lock().get(&group).map(|hosted| hosted.raft.clone())
    }

    fn engine(&self, group: u32) -> Option<Arc<AsyncMutex<RaftGroupEngine>>> {
        self.lock()
            .get(&group)
            .map(|hosted| Arc::clone(&hosted.engine))
    }

    fn gates_open(&self) -> bool {
        self.lock()
            .values()
            .all(|hosted| hosted.rejoin.vote_gate_open())
    }
}

impl GroupHost for SimHost {
    type Error = SimHostError;

    fn open(&self, group: RaftGroupId) -> BoxFuture<'_, Result<(), SimHostError>> {
        Box::pin(self.start(group.0))
    }

    fn close(&self, group: RaftGroupId) -> BoxFuture<'_, Result<(), SimHostError>> {
        Box::pin(self.stop(group.0))
    }

    /// A simulated node keeps no gauges or open back-off of its own.
    fn forget(&self, _group: RaftGroupId) {}
}

/// A running node's host and hosting driver.
struct NodeHost {
    host: Arc<SimHost>,
    driver: HostingTask,
}

/// The placed data plane of a [`MetaCluster`]: each running node's host.
#[derive(Default)]
pub(super) struct Placed {
    hosts: BTreeMap<u64, NodeHost>,
    started: StartedGates,
    /// The groups whose stream was created.
    created: BTreeSet<u32>,
}

/// `group`'s seed voters by the kernel's seed rule over `claim`.
pub(super) fn seed_voters(claim: &GenesisClaim, group: u32) -> BTreeSet<u64> {
    let factor =
        ursula_control::effective_replication_factor(claim.replication_factor, &claim.peers);
    ursula_control::seed_voters(RaftGroupId(group), factor, &claim.peers)
        .iter()
        .map(ursula_proto::placement::NodeId::get)
        .collect()
}

/// `group`'s initializer by the kernel's seed rule over `claim`.
pub(super) fn initializer(claim: &GenesisClaim, group: u32) -> u64 {
    ursula_control::initializer(RaftGroupId(group), &claim.peers).get()
}

impl MetaCluster {
    /// Opens `node_id`'s held cores and starts its hosting driver, after
    /// its meta runtime started, as the server's spawn path does (4.3).
    pub(super) fn start_host(&mut self, node_id: u64) {
        let wal = self.data.wals[&node_id].raft_wal();
        if let Err(error) = wal.open_held_cores(self.data.metrics[&node_id].group_engine_metrics())
        {
            panic!("node {node_id}: the held cores do not open: {error}");
        }
        let reader = self.runtimes[&node_id].status_reader().clone();
        let hosting = HostingSet::default();
        let placed = self.placed_mut();
        let started = placed.started.clone();
        let parts = self.data.replica_parts(node_id);
        let host = Arc::new(SimHost {
            parts,
            inputs: MetaPlacement::new(reader.clone(), hosting.clone()),
            replicas: Mutex::new(BTreeMap::new()),
            started,
        });
        let driver = HostingDriver::start(Arc::clone(&host), wal, reader, hosting);
        self.placed_mut()
            .hosts
            .insert(node_id, NodeHost { host, driver });
    }

    /// Stops `node_id`'s hosting driver, then every replica it runs.
    pub(super) async fn stop_host(&mut self, node_id: u64) {
        let Some(node) = self.placed_mut().hosts.remove(&node_id) else {
            return;
        };
        node.driver.shutdown().await;
        for group in node.host.groups() {
            if let Err(error) = node.host.stop(group).await {
                panic!("node {node_id}: stop the replica of group {group}: {error}");
            }
        }
    }

    fn placed(&self) -> &Placed {
        self.placed
            .as_ref()
            .expect("the cluster's data replicas are placed")
    }

    fn placed_mut(&mut self) -> &mut Placed {
        self.placed
            .as_mut()
            .expect("the cluster's data replicas are placed")
    }

    /// The seed voters of `group` by the claim of node 1.
    pub(super) fn seeds(&self, group: u32) -> BTreeSet<u64> {
        seed_voters(&self.claims[&1], group)
    }

    /// The running nodes that host a replica of `group`.
    fn hosting(&self, group: u32) -> Vec<(u64, Arc<SimHost>)> {
        self.placed()
            .hosts
            .iter()
            .filter(|(_, node)| node.host.raft(group).is_some())
            .map(|(node_id, node)| (*node_id, Arc::clone(&node.host)))
            .collect()
    }

    /// The leader some running replica of `group` names, if that leader's
    /// replica runs.
    fn placed_leader(&self, group: u32) -> Option<u64> {
        let hosting = self.hosting(group);
        hosting.iter().find_map(|(_, host)| {
            let leader = openraft::rt::WatchReceiver::borrow_watched(&host.raft(group)?.metrics())
                .current_leader?;
            hosting
                .iter()
                .any(|(node_id, _)| *node_id == leader)
                .then_some(leader)
        })
    }

    /// Whether the voters of `group` that run hold a majority of them: its
    /// seed voters when placed, else every node.
    pub(super) fn has_quorum(&self, group: u32) -> bool {
        let voters: BTreeSet<u64> = if self.placed.is_some() {
            self.seeds(group)
        } else {
            self.data.nodes().collect()
        };
        let running = voters
            .iter()
            .filter(|node_id| self.runtimes.contains_key(node_id))
            .count();
        running.saturating_mul(2) > voters.len()
    }

    /// Appends `count` payloads to `group` through its leader, retrying
    /// while leadership moves, and answers `false` at the first one the
    /// group does not take within the bound.
    pub(super) async fn placed_append(&mut self, group: u32, count: usize) -> bool {
        for _ in 0..count {
            let acknowledged = self.data.acknowledged.entry(group).or_default();
            let payload = format!("g{group}-{};", acknowledged.len()).into_bytes();
            let deadline = madsim::time::Instant::now() + BOUND;
            loop {
                let engine = self
                    .placed_leader(group)
                    .and_then(|leader| self.placed().hosts.get(&leader)?.host.engine(group));
                if let Some(engine) = engine
                    && engine
                        .lock()
                        .await
                        .append(
                            AppendRequest::from_bytes(group_stream(group), payload.clone()),
                            group_placement(group),
                            ColdWriteAdmission::default(),
                        )
                        .await
                        .is_ok()
                {
                    break;
                }
                if madsim::time::Instant::now() >= deadline {
                    return false;
                }
                madsim::time::sleep(STEP).await;
            }
            self.data
                .acknowledged
                .entry(group)
                .or_default()
                .extend_from_slice(&payload);
        }
        true
    }

    /// Why `node_id`'s replica of `group` does not read every acknowledged
    /// payload of the group, if it does not.
    async fn read_lag(&self, group: u32, node_id: u64) -> Option<String> {
        let expected = self
            .data
            .acknowledged
            .get(&group)
            .cloned()
            .unwrap_or_default();
        let Some(engine) = self
            .placed()
            .hosts
            .get(&node_id)
            .and_then(|node| node.host.engine(group))
        else {
            return Some(format!("node {node_id} runs no replica of group {group}"));
        };
        let read = engine
            .lock()
            .await
            .sim_read_local_stream(
                ReadStreamRequest {
                    stream_id: group_stream(group),
                    offset: 0,
                    max_len: expected.len().saturating_add(64),
                    now_ms: 0,
                    leader_only: false,
                    read_index: None,
                    follower: FollowerRead::Prefix,
                },
                group_placement(group),
            )
            .await
            .map(|read| read.payload);
        match read {
            Ok(payload) if payload == expected => None,
            Ok(payload) => Some(format!(
                "node {node_id} reads {} of group {group}'s {} acknowledged bytes",
                payload.len(),
                expected.len()
            )),
            Err(error) => Some(format!(
                "node {node_id} does not read group {group}: {error}"
            )),
        }
    }

    /// I1: every running seed voter of `group` reads the group's
    /// acknowledged payloads within the bound.
    pub(super) async fn placed_reads(&self, trace: &mut SimTrace, after_event: &str, group: u32) {
        let deadline = madsim::time::Instant::now() + BOUND;
        for node_id in self.seeds(group) {
            if !self.runtimes.contains_key(&node_id) {
                continue;
            }
            while let Some(lag) = self.read_lag(group, node_id).await {
                if madsim::time::Instant::now() >= deadline {
                    fail(trace, I1, after_event, lag);
                }
                madsim::time::sleep(STEP).await;
            }
        }
    }

    /// I15: every group forms within the bound: it elects a leader among its
    /// seed voters, its stream exists, and every running seed voter's gate
    /// opens.
    pub(super) async fn groups_form(&mut self, trace: &mut SimTrace, after_event: &str) {
        let deadline = madsim::time::Instant::now() + BOUND;
        for group in JOURNAL_GROUPS {
            loop {
                let leader = self.placed_leader(group);
                let formed = leader.is_some()
                    && self.seeds(group).iter().all(|node_id| {
                        !self.runtimes.contains_key(node_id)
                            || self
                                .placed()
                                .hosts
                                .get(node_id)
                                .is_some_and(|node| node.host.raft(group).is_some())
                    });
                if formed && self.placed().created.contains(&group) {
                    break;
                }
                if formed && let Some(leader) = leader {
                    let engine = self.placed().hosts[&leader].host.engine(group);
                    let created = match engine {
                        Some(engine) => engine
                            .lock()
                            .await
                            .create_stream(
                                CreateStreamRequest::new(
                                    group_stream(group),
                                    "application/octet-stream",
                                ),
                                group_placement(group),
                                ColdWriteAdmission::default(),
                            )
                            .await
                            .is_ok(),
                        None => false,
                    };
                    if created {
                        self.placed_mut().created.insert(group);
                        continue;
                    }
                }
                if madsim::time::Instant::now() >= deadline {
                    fail(
                        trace,
                        I15,
                        after_event,
                        format!(
                            "group {group} did not form on its seed voters {:?}: leader {leader:?}, \
                             hosted on {:?}",
                            self.seeds(group),
                            self.hosting(group)
                                .iter()
                                .map(|(node_id, _)| *node_id)
                                .collect::<Vec<_>>()
                        ),
                    );
                }
                madsim::time::sleep(STEP).await;
            }
        }
        while !self
            .placed()
            .hosts
            .values()
            .all(|node| node.host.gates_open())
        {
            if madsim::time::Instant::now() >= deadline {
                fail(
                    trace,
                    I15,
                    after_event,
                    "a placed replica's recovery gate stayed closed".to_owned(),
                );
            }
            madsim::time::sleep(STEP).await;
        }
    }

    /// I7: `node_id`, restarted while meta has no quorum, serves every
    /// group it is a seed voter of whose seed voters keep a quorum: its
    /// hosting driver opened the replica from the journals, the group takes
    /// appends, and the replica reads them back.
    pub(super) async fn serves_without_meta(
        &mut self,
        trace: &mut SimTrace,
        after_event: &str,
        node_id: u64,
    ) {
        for group in JOURNAL_GROUPS {
            if !self.seeds(group).contains(&node_id) || !self.has_quorum(group) {
                continue;
            }
            if !self.placed_append(group, 2).await {
                fail(
                    trace,
                    I7,
                    after_event,
                    format!("group {group} took no append after node {node_id} restarted"),
                );
            }
            let deadline = madsim::time::Instant::now() + BOUND;
            while let Some(lag) = self.read_lag(group, node_id).await {
                if madsim::time::Instant::now() >= deadline {
                    fail(trace, I7, after_event, lag);
                }
                madsim::time::sleep(STEP).await;
            }
        }
    }
}

/// Restarts one placed replica for a recovery plan's cut.
struct HostRestart {
    host: Arc<SimHost>,
}

impl ursula_raft::ReplicaRestart for HostRestart {
    fn restart_replica(
        &mut self,
        raft_group_id: RaftGroupId,
    ) -> BoxFuture<'_, Result<Option<Arc<GroupRejoin>>, ursula_runtime::RuntimeError>> {
        Box::pin(async move { Ok(self.host.restart(raft_group_id.0).await) })
    }
}

/// [`PlanTransport`] over each node's production data plan entry point
/// ([`ursula_raft::RaftGroupHandleRegistry::accept_recovery_plan`]), its
/// refusal typed as the node's route types it.
struct DataPlanTransport<'a> {
    cluster: &'a MetaCluster,
    group: u32,
}

impl DataPlanTransport<'_> {
    async fn accept(
        &self,
        node_id: u64,
        request: &RecoveryPlanRequest,
    ) -> Result<RecoveryPlanReport, SendError> {
        let host = self
            .cluster
            .placed()
            .hosts
            .get(&node_id)
            .map(|node| Arc::clone(&node.host))
            .ok_or(SendError::NotInManifest { node_id })?;
        let registry = host.parts.registry.clone();
        registry
            .accept_recovery_plan(RaftGroupId(self.group), request, &mut HostRestart { host })
            .await
            .map_err(|error| match error.refusal() {
                Some(refusal) => SendError::Refused {
                    node_id,
                    refused: RecoveryPlanRefused {
                        raft_group_id: u64::from(self.group),
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

impl PlanTransport for DataPlanTransport<'_> {
    fn send(
        &mut self,
        node_id: u64,
        request: &RecoveryPlanRequest,
    ) -> impl std::future::Future<Output = Result<RecoveryPlanReport, SendError>> {
        self.accept(node_id, request)
    }
}

impl MetaCluster {
    /// The replicas of `group` on `nodes` as the recovery tool reads them
    /// from each node's metrics, skipping nodes that publish none.
    fn data_members(&self, group: u32, nodes: &[u64]) -> Vec<PlanMember> {
        nodes
            .iter()
            .filter_map(|node_id| {
                self.registry(*node_id)
                    .metrics_snapshot()
                    .into_iter()
                    .find(|snapshot| snapshot.raft_group_id == group)
            })
            .map(|snapshot| PlanMember::from_metrics(&snapshot.to_admin()))
            .collect()
    }

    /// `ursulactl recover-group --accept-loss` for each data group after a
    /// meta plan, as the operations order has it (meta first): over the
    /// group's running seed voters, the tool's rounds until its plan is
    /// satisfied, or until it finds the group healthy.
    pub(super) async fn recover_data(&mut self, trace: &mut SimTrace, after_event: &str) {
        for group in JOURNAL_GROUPS {
            let nodes: Vec<u64> = self
                .seeds(group)
                .into_iter()
                .filter(|node_id| self.runtimes.contains_key(node_id))
                .collect();
            let mut state = LoopState::default();
            let within = super::recovery_wiring::RECOVERY_STALL_AFTER + BOUND;
            let deadline = madsim::time::Instant::now() + within;
            loop {
                let members = self.data_members(group, &nodes);
                let mut transport = DataPlanTransport {
                    cluster: self,
                    group,
                };
                match recover_round(&mut state, &members, &mut transport, PlanFloor::Seeded).await {
                    Ok(Some(_))
                    | Err(MetaRecoverFailed::NoPlan {
                        outcome: RecoverOutcome::Healthy,
                    }) => break,
                    // A group whose leader outlived the loss needs no plan:
                    // its gated replicas open through its barrier.
                    Err(MetaRecoverFailed::Recover(RecoverError::Plan(PlanError::HasLeader {
                        ..
                    }))) if state.sent.is_none() => break,
                    Ok(None) if madsim::time::Instant::now() < deadline => {
                        madsim::time::sleep(STEP).await;
                    }
                    Ok(None) => fail(
                        trace,
                        DATA_PLAN,
                        after_event,
                        format!(
                            "no plan of group {group} was satisfied within {within:?}: {state:?}"
                        ),
                    ),
                    Err(error) => fail(
                        trace,
                        DATA_PLAN,
                        after_event,
                        format!("the recovery of group {group} stopped: {error}; {state:?}"),
                    ),
                }
            }
        }
    }
}

/// What the hosting checks keep across a run.
#[derive(Debug, Default)]
pub(super) struct HostingChecks {
    /// The leader of each group in each term some replica led it in.
    leaders: BTreeMap<(u32, u64), u64>,
}

impl HostingChecks {
    /// I2, I3 and the hosting check, over every running placed replica.
    pub(super) fn observe(
        &mut self,
        cluster: &MetaCluster,
        trace: &mut SimTrace,
        after_event: &str,
    ) {
        let Some(placed) = cluster.placed.as_ref() else {
            return;
        };
        for (group, nodes) in placed.started.initializers() {
            if nodes.len() > 1 {
                fail(
                    trace,
                    I2,
                    after_event,
                    format!("group {group} was initialized more than once, by nodes {nodes:?}"),
                );
            }
        }
        let bootstrapped = cluster
            .running()
            .iter()
            .any(|node_id| cluster.view(*node_id).cluster().is_some());
        for (node_id, node) in &placed.hosts {
            let groups = node.host.groups();
            let unplaced: Vec<u32> = groups
                .iter()
                .copied()
                .filter(|group| !bootstrapped || !cluster.seeds(*group).contains(node_id))
                .collect();
            if !unplaced.is_empty() {
                fail(
                    trace,
                    HOSTED,
                    after_event,
                    format!(
                        "node {node_id} runs groups {unplaced:?} it is no seed voter of \
                         (bootstrapped: {bootstrapped})"
                    ),
                );
            }
            for group in groups {
                let Some(raft) = node.host.raft(group) else {
                    continue;
                };
                let metrics = openraft::rt::WatchReceiver::borrow_watched(&raft.metrics()).clone();
                if metrics.state == ServerState::Leader {
                    let leader = *self
                        .leaders
                        .entry((group, metrics.current_term))
                        .or_insert(*node_id);
                    if leader != *node_id {
                        fail(
                            trace,
                            I2,
                            after_event,
                            format!(
                                "nodes {leader} and {node_id} both led group {group} in term {}",
                                metrics.current_term
                            ),
                        );
                    }
                }
                let membership = metrics.membership_config.membership();
                let configs = membership.get_joint_config();
                if configs.is_empty() || configs.iter().all(BTreeSet::is_empty) {
                    continue;
                }
                let voters: BTreeSet<u64> = membership.voter_ids().collect();
                let learners: Vec<u64> = membership.learner_ids().collect();
                let seeds = cluster.seeds(group);
                if configs.len() != 1 || voters != seeds || !learners.is_empty() {
                    fail(
                        trace,
                        I3,
                        after_event,
                        format!(
                            "node {node_id} holds group {group}'s membership {configs:?} with \
                             learners {learners:?}, not its seed voters {seeds:?}"
                        ),
                    );
                }
            }
        }
    }
}
