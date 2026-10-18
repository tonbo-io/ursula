//! The runtime's three-node OpenRaft group: a [`GroupEngineFactory`] that
//! builds one replica per node over the in-process network, hands the
//! leader's engine to the runtime and keeps the followers, so a scenario can
//! kill, depose and restart replicas and swap the runtime's engine.
//!
//! The front door's registry ([`MadsimRuntimeRaftNetworkFactory::front_door`])
//! holds the replica the runtime hosts, as a node's registry holds its own:
//! the first leader once the group is built, each successor the runtime
//! takes, and nothing while a killed leader's engine is gone. An `HttpState`
//! built over it routes writes through the production leadership view.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use openraft::BasicNode;
use openraft::Config;
use openraft::SnapshotPolicy;
use openraft::storage::RaftLogStorage;
use ursula_raft::InProcessRaftNetworkFactory;
use ursula_raft::InProcessRaftNetworkPolicy;
use ursula_raft::InProcessRaftRegistry;
use ursula_raft::MadsimOpenRaftRuntime;
use ursula_raft::RaftGroupEngine;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftGroupStateMachine;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineCreateFuture;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupEngineFactory;
use ursula_runtime::GroupEngineMetrics;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;

use super::MadsimScopedGroupEngine;
use super::SimNodeWal;

#[derive(Clone)]
pub(super) struct MadsimRuntimeRaftNetworkFactory {
    seed: u64,
    policy: InProcessRaftNetworkPolicy,
    cold_store: Option<ColdStoreHandle>,
    aggressive_snapshot_purge: bool,
    pub(super) followers: Arc<Mutex<Vec<(u64, RaftGroupEngine)>>>,
    leaders: Arc<Mutex<BTreeMap<u32, u64>>>,
    groups: Arc<Mutex<BTreeMap<u32, RuntimeRaftNetworkGroupControl>>>,
    node_wals: Arc<Mutex<BTreeMap<u64, SimNodeWal>>>,
    /// The registry of the node whose replica the runtime hosts.
    front_door: RaftGroupHandleRegistry,
}

#[derive(Clone)]
struct RuntimeRaftNetworkGroupControl {
    placement: ShardPlacement,
    config: Arc<Config>,
    registry: InProcessRaftRegistry,
    metrics: GroupEngineMetrics,
    wals: BTreeMap<u64, SimNodeWal>,
}

impl MadsimRuntimeRaftNetworkFactory {
    pub(super) fn new(seed: u64, policy: InProcessRaftNetworkPolicy) -> Self {
        Self::with_cold_store(seed, policy, None)
    }

    pub(super) fn with_cold_store(
        seed: u64,
        policy: InProcessRaftNetworkPolicy,
        cold_store: Option<ColdStoreHandle>,
    ) -> Self {
        Self {
            seed,
            policy,
            cold_store,
            aggressive_snapshot_purge: false,
            followers: Arc::new(Mutex::new(Vec::new())),
            leaders: Arc::new(Mutex::new(BTreeMap::new())),
            groups: Arc::new(Mutex::new(BTreeMap::new())),
            node_wals: Arc::new(Mutex::new(BTreeMap::new())),
            front_door: RaftGroupHandleRegistry::default(),
        }
    }

    #[cfg(test)]
    pub(super) fn with_aggressive_snapshot_purge(mut self) -> Self {
        self.aggressive_snapshot_purge = true;
        self
    }

    /// The registry of the node whose replica the runtime hosts, for the
    /// front door's `HttpState`.
    pub(super) fn front_door(&self) -> RaftGroupHandleRegistry {
        self.front_door.clone()
    }

    /// The history every retained follower applied last.
    pub(super) fn follower_histories(&self) -> Vec<(u64, Option<ursula_runtime::HistoryEvent>)> {
        self.followers
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .map(|(node_id, engine)| (*node_id, engine.history().applied()))
            .collect()
    }

    pub(super) fn leader_id(&self, raft_group_id: RaftGroupId) -> Option<u64> {
        self.leaders
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(&raft_group_id.0)
            .copied()
    }

    pub(super) fn unregister_current_leader(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<u64, GroupEngineError> {
        let leader_id = self.leader_id(raft_group_id).ok_or_else(|| {
            GroupEngineError::new(format!(
                "runtime raft leader for group {} not found",
                raft_group_id.0
            ))
        })?;
        let groups = self
            .groups
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let control = groups.get(&raft_group_id.0).ok_or_else(|| {
            GroupEngineError::new(format!(
                "runtime raft group control for group {} not found",
                raft_group_id.0
            ))
        })?;
        control.registry.unregister(leader_id);
        self.front_door
            .unregister_group_for_simulation(raft_group_id);
        Ok(leader_id)
    }

    pub(super) fn follower_raft_handle(
        &self,
        node_id: u64,
    ) -> Option<openraft::Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>> {
        self.followers
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .find_map(|(candidate, engine)| (*candidate == node_id).then(|| engine.raft_handle()))
    }

    pub(super) fn raft_handle(
        &self,
        node_id: u64,
    ) -> Option<openraft::Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>> {
        self.groups
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .values()
            .find_map(|control| control.registry.get(node_id))
    }

    pub(super) async fn log_store_last_log_index(&self, node_id: u64) -> Option<u64> {
        let mut log_store = self
            .groups
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .values()
            .find_map(|control| {
                control
                    .wals
                    .get(&node_id)?
                    .store(control.placement.raft_group_id)
            })?;
        log_store
            .get_log_state()
            .await
            .ok()
            .and_then(|state| state.last_log_id.map(|log_id| log_id.index))
    }

    pub(super) fn retained_follower_id_prefer(&self, preferred_node_id: u64) -> Option<u64> {
        let followers = self
            .followers
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        followers
            .iter()
            .find_map(|(candidate, _)| (*candidate == preferred_node_id).then_some(*candidate))
            .or_else(|| followers.iter().map(|(candidate, _)| *candidate).min())
    }

    /// Waits until a retained follower names a leader other than the one
    /// the runtime hosts, and returns it. Takes nothing: the runtime keeps
    /// its engine until [`Self::take_current_leader_engine`].
    pub(super) async fn wait_successor(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<u64, GroupEngineError> {
        let old_leader_id = self.leader_id(raft_group_id).ok_or_else(|| {
            GroupEngineError::new(format!(
                "runtime raft leader for group {} not found",
                raft_group_id.0
            ))
        })?;
        let handles = self
            .followers
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .map(|(node_id, engine)| (*node_id, engine.raft_handle()))
            .collect::<Vec<_>>();
        let wait_handle = handles
            .iter()
            .find(|(node_id, _)| *node_id != old_leader_id)
            .map(|(_, raft)| raft.clone())
            .ok_or_else(|| GroupEngineError::new("runtime raft replacement follower not found"))?;
        let metrics = wait_handle
            .wait(Some(Duration::from_secs(5)))
            .metrics(
                |metrics| {
                    metrics
                        .current_leader
                        .is_some_and(|leader_id| leader_id != old_leader_id)
                },
                "runtime raft new leader elected after old leader shutdown",
            )
            .await
            .map_err(|err| {
                GroupEngineError::new(format!("wait for runtime raft replacement leader: {err}"))
            })?;
        metrics
            .current_leader
            .ok_or_else(|| GroupEngineError::new("runtime raft replacement leader missing"))
    }

    pub(super) async fn take_current_leader_engine(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<(u64, Box<dyn GroupEngine>), GroupEngineError> {
        let new_leader_id = self.wait_successor(raft_group_id).await?;
        let engine = {
            let mut followers = self
                .followers
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let Some(index) = followers
                .iter()
                .position(|(candidate, _)| *candidate == new_leader_id)
            else {
                return Err(GroupEngineError::new(format!(
                    "runtime raft replacement leader {new_leader_id} not retained as follower"
                )));
            };
            followers.swap_remove(index).1
        };
        self.front_door.register_engine_for_simulation(&engine);
        self.leaders
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(raft_group_id.0, new_leader_id);
        let seed = self.seed ^ 0x7274_6c66_6169_6c6f ^ u64::from(raft_group_id.0);
        let engine: Box<dyn GroupEngine> = Box::new(MadsimScopedGroupEngine {
            seed,
            inner: Box::new(engine),
        });
        Ok((new_leader_id, engine))
    }

    pub(super) async fn stop_follower(&self, node_id: u64) -> Result<(), GroupEngineError> {
        let engine = {
            let mut followers = self
                .followers
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let Some(index) = followers
                .iter()
                .position(|(candidate, _)| *candidate == node_id)
            else {
                return Err(GroupEngineError::new(format!(
                    "runtime raft follower {node_id} not found"
                )));
            };
            followers.swap_remove(index).1
        };
        {
            let groups = self
                .groups
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            for control in groups.values() {
                control.registry.unregister(node_id);
            }
        }
        engine.shutdown().await
    }

    pub(super) async fn restart_follower(&self, node_id: u64) -> Result<(), GroupEngineError> {
        let control = {
            let groups = self
                .groups
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            groups
                .values()
                .next()
                .cloned()
                .ok_or_else(|| GroupEngineError::new("runtime raft group control not found"))?
        };
        let wal = control.wals.get(&node_id).cloned().ok_or_else(|| {
            GroupEngineError::new(format!(
                "runtime raft log store for node {node_id} not found"
            ))
        })?;
        let log_store = wal.open(control.placement, control.metrics.clone()).await;
        let engine = RaftGroupEngine::new_node_with_log_store_network_and_options(
            control.placement,
            node_id,
            control.config,
            InProcessRaftNetworkFactory::new(control.registry.clone())
                .with_source(node_id)
                .with_policy(self.policy.clone()),
            log_store,
            wal.recording_work(super::sim_wal::engine_options(
                Some(control.metrics),
                self.cold_store.clone(),
            )),
        )
        .await?;
        control.registry.register(node_id, &engine);
        self.followers
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((node_id, engine));
        Ok(())
    }
}

impl GroupEngineFactory for MadsimRuntimeRaftNetworkFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        let seed = self.seed ^ 0x7274_6e65_7477_6f72 ^ u64::from(placement.raft_group_id.0);
        let policy = self.policy.clone();
        let cold_store = self.cold_store.clone();
        let aggressive_snapshot_purge = self.aggressive_snapshot_purge;
        let followers = self.followers.clone();
        let leaders = self.leaders.clone();
        let groups = self.groups.clone();
        let node_wals = self.node_wals.clone();
        let front_door = self.front_door.clone();
        Box::pin(MadsimOpenRaftRuntime::scope(seed, async move {
            let registry = InProcessRaftRegistry::default();
            let mut config = Config {
                cluster_name: format!("ursula-sim-runtime-group-{}", placement.raft_group_id.0),
                heartbeat_interval: 10,
                election_timeout_min: 50,
                election_timeout_max: 100,
                ..Default::default()
            };
            if aggressive_snapshot_purge {
                config.max_in_snapshot_log_to_keep = 0;
                config.purge_batch_size = 1;
                config.replication_lag_threshold = 0;
                config.snapshot_policy = SnapshotPolicy::LogsSinceLast(4);
            }
            let config =
                Arc::new(config.validate().map_err(|err| {
                    GroupEngineError::new(format!("invalid OpenRaft config: {err}"))
                })?);
            let mut nodes = BTreeMap::new();
            for node_id in 1..=3 {
                nodes.insert(
                    node_id,
                    BasicNode::new(format!("runtime-raft-node-{node_id}")),
                );
            }

            let mut engines = Vec::new();
            let mut wals = BTreeMap::new();
            for node_id in 1..=3 {
                let wal = node_wals
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .entry(node_id)
                    .or_insert_with(|| {
                        SimNodeWal::provision(&format!("runtime-raft-node-{node_id}"))
                    })
                    .clone();
                let log_store = wal.open(placement, metrics.clone()).await;
                wals.insert(node_id, wal);
                let engine = RaftGroupEngine::new_node_with_log_store_network_and_options(
                    placement,
                    node_id,
                    config.clone(),
                    InProcessRaftNetworkFactory::new(registry.clone())
                        .with_source(node_id)
                        .with_policy(policy.clone()),
                    log_store,
                    wals[&node_id].recording_work(super::sim_wal::engine_options(
                        Some(metrics.clone()),
                        cold_store.clone(),
                    )),
                )
                .await?;
                registry.register(node_id, &engine);
                engines.push((node_id, engine));
            }

            engines[0].1.initialize_membership(nodes).await?;
            let leader_metrics = engines[0]
                .1
                .raft_handle()
                .wait(Some(Duration::from_secs(5)))
                .metrics(|metrics| metrics.current_leader.is_some(), "leader elected")
                .await
                .map_err(|err| GroupEngineError::new(format!("wait for leader: {err}")))?;
            let leader_id = leader_metrics.current_leader.expect("leader id");
            leaders
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .insert(placement.raft_group_id.0, leader_id);
            groups
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .insert(placement.raft_group_id.0, RuntimeRaftNetworkGroupControl {
                    placement,
                    config: config.clone(),
                    registry: registry.clone(),
                    metrics: metrics.clone(),
                    wals,
                });

            let leader_index = engines
                .iter()
                .position(|(node_id, _)| *node_id == leader_id)
                .expect("leader engine exists");
            let (_leader_id, leader) = engines.swap_remove(leader_index);
            front_door.register_engine_for_simulation(&leader);
            followers
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .extend(engines);

            let engine: Box<dyn GroupEngine> = Box::new(MadsimScopedGroupEngine {
                seed,
                inner: Box::new(leader),
            });
            Ok(engine)
        }))
    }
}
