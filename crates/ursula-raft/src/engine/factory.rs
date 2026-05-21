use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use openraft::BasicNode;
use openraft::Config;
use ursula_runtime::{
    ColdStoreHandle, GroupEngine, GroupEngineCreateFuture, GroupEngineError, GroupEngineFactory,
    GroupEngineMetrics,
};
use ursula_shard::{CoreId, RaftGroupId, ShardPlacement};

use crate::grpc::GrpcRaftNetworkFactory;
use crate::log_store::{CoreFileLogWriter, RaftGroupFileLogStore, RaftGroupLogStore};
use crate::registry::RaftGroupHandleRegistry;

use super::RaftGroupEngine;

#[derive(Debug, Clone, Copy, Default)]
pub struct RaftGroupEngineFactory;

impl GroupEngineFactory for RaftGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let engine: Box<dyn GroupEngine> = Box::new(
                RaftGroupEngine::new_single_node_with_optional_metrics(placement, Some(metrics))
                    .await?,
            );
            Ok(engine)
        })
    }
}

#[derive(Debug, Clone)]
pub struct RegisteredRaftGroupEngineFactory {
    registry: RaftGroupHandleRegistry,
}

impl RegisteredRaftGroupEngineFactory {
    pub fn new(registry: RaftGroupHandleRegistry) -> Self {
        Self { registry }
    }

    pub fn registry(&self) -> &RaftGroupHandleRegistry {
        &self.registry
    }
}

impl GroupEngineFactory for RegisteredRaftGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let engine =
                RaftGroupEngine::new_single_node_with_optional_metrics(placement, Some(metrics))
                    .await?;
            self.registry.register(placement, engine.raft.clone());
            let engine: Box<dyn GroupEngine> = Box::new(engine);
            Ok(engine)
        })
    }
}

#[derive(Debug, Clone)]
pub struct ColdRaftGroupEngineFactory {
    cold_store: ColdStoreHandle,
}

impl ColdRaftGroupEngineFactory {
    pub fn new(cold_store: ColdStoreHandle) -> Self {
        Self { cold_store }
    }
}

impl GroupEngineFactory for ColdRaftGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let config = Arc::new(
                Config {
                    cluster_name: format!("ursula-group-{}", placement.raft_group_id.0),
                    heartbeat_interval: 10,
                    election_timeout_min: 30,
                    election_timeout_max: 60,
                    ..Default::default()
                }
                .validate()
                .map_err(|err| GroupEngineError::new(format!("invalid OpenRaft config: {err}")))?,
            );
            let engine: Box<dyn GroupEngine> = Box::new(
                RaftGroupEngine::new_single_node_with_log_store_and_metrics(
                    placement,
                    1,
                    BasicNode::new("local"),
                    config,
                    RaftGroupLogStore::shared(),
                    Some(metrics),
                    Some(self.cold_store.clone()),
                )
                .await?,
            );
            Ok(engine)
        })
    }
}

#[derive(Debug, Clone)]
pub struct DurableRaftLogStoreFactory {
    root: PathBuf,
    core_writers: Arc<Mutex<BTreeMap<u16, Arc<CoreFileLogWriter>>>>,
}

impl DurableRaftLogStoreFactory {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            core_writers: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(crate) fn log_path(&self, placement: ShardPlacement) -> PathBuf {
        self.root
            .join(format!("core-{}", placement.core_id.0))
            .join(format!("group-{}.json", placement.raft_group_id.0))
    }

    pub(crate) fn core_journal_path(&self, core_id: CoreId) -> PathBuf {
        self.root
            .join(format!("core-{}", core_id.0))
            .join("journal.bin")
    }

    pub(crate) fn core_writer(
        &self,
        core_id: CoreId,
    ) -> Result<Arc<CoreFileLogWriter>, GroupEngineError> {
        let mut writers = self
            .core_writers
            .lock()
            .map_err(|_| GroupEngineError::new("core file log writer mutex poisoned"))?;
        if let Some(writer) = writers.get(&core_id.0) {
            return Ok(writer.clone());
        }

        let writer = CoreFileLogWriter::shared(self.core_journal_path(core_id))
            .map_err(|err| GroupEngineError::new(format!("open OpenRaft core journal: {err}")))?;
        writers.insert(core_id.0, writer.clone());
        Ok(writer)
    }

    pub fn open(
        &self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> Result<Arc<RaftGroupFileLogStore>, GroupEngineError> {
        RaftGroupFileLogStore::shared_with_core_writer(
            self.log_path(placement),
            placement,
            metrics,
            self.core_writer(placement.core_id)?,
        )
        .map_err(|err| GroupEngineError::new(format!("open OpenRaft file log: {err}")))
    }
}

#[derive(Debug, Clone)]
pub struct DurableRaftGroupEngineFactory {
    log_stores: DurableRaftLogStoreFactory,
    cold_store: Option<ColdStoreHandle>,
}

impl DurableRaftGroupEngineFactory {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            log_stores: DurableRaftLogStoreFactory::new(root),
            cold_store: None,
        }
    }

    pub fn with_cold_store(root: impl Into<PathBuf>, cold_store: Option<ColdStoreHandle>) -> Self {
        Self {
            log_stores: DurableRaftLogStoreFactory::new(root),
            cold_store,
        }
    }
}

impl GroupEngineFactory for DurableRaftGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let config = Arc::new(
                Config {
                    cluster_name: format!("ursula-group-{}", placement.raft_group_id.0),
                    heartbeat_interval: 10,
                    election_timeout_min: 30,
                    election_timeout_max: 60,
                    ..Default::default()
                }
                .validate()
                .map_err(|err| GroupEngineError::new(format!("invalid OpenRaft config: {err}")))?,
            );
            let log_store = self.log_stores.open(placement, metrics.clone())?;
            let engine: Box<dyn GroupEngine> = Box::new(
                RaftGroupEngine::new_single_node_with_log_store_and_metrics(
                    placement,
                    1,
                    BasicNode::new("local"),
                    config,
                    log_store,
                    Some(metrics),
                    self.cold_store.clone(),
                )
                .await?,
            );
            Ok(engine)
        })
    }
}

#[derive(Debug, Clone)]
pub struct StaticGrpcRaftGroupEngineFactory {
    node_id: u64,
    peers: BTreeMap<u64, String>,
    initialize_membership: bool,
    initialize_membership_per_group: bool,
    registry: RaftGroupHandleRegistry,
    cold_store: Option<ColdStoreHandle>,
    log_stores: Option<DurableRaftLogStoreFactory>,
}

impl StaticGrpcRaftGroupEngineFactory {
    pub fn new(
        node_id: u64,
        peers: impl IntoIterator<Item = (u64, String)>,
        initialize_membership: bool,
        registry: RaftGroupHandleRegistry,
    ) -> Self {
        Self {
            node_id,
            peers: peers.into_iter().collect(),
            initialize_membership,
            initialize_membership_per_group: false,
            registry,
            cold_store: None,
            log_stores: None,
        }
    }

    pub fn registry(&self) -> &RaftGroupHandleRegistry {
        &self.registry
    }

    pub fn with_cold_store(mut self, cold_store: Option<ColdStoreHandle>) -> Self {
        self.cold_store = cold_store;
        self
    }

    pub fn with_raft_log_dir(mut self, root: impl Into<PathBuf>) -> Self {
        self.log_stores = Some(DurableRaftLogStoreFactory::new(root));
        self
    }

    pub fn with_per_group_membership_initializers(mut self, enabled: bool) -> Self {
        self.initialize_membership_per_group = enabled;
        self
    }

    fn peer_nodes(&self) -> BTreeMap<u64, BasicNode> {
        self.peers
            .iter()
            .map(|(node_id, address)| (*node_id, BasicNode::new(address.clone())))
            .collect()
    }

    fn should_initialize_membership(&self, raft_group_id: RaftGroupId) -> bool {
        if !self.initialize_membership {
            return false;
        }
        if !self.initialize_membership_per_group {
            return true;
        }
        let peer_count = self.peers.len();
        if peer_count == 0 {
            return false;
        }
        let initializer_index =
            usize::try_from(raft_group_id.0).expect("raft group id fits usize") % peer_count;
        self.peers
            .keys()
            .nth(initializer_index)
            .is_some_and(|node_id| *node_id == self.node_id)
    }
}

impl GroupEngineFactory for StaticGrpcRaftGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ursula_shard::ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            if !self.peers.contains_key(&self.node_id) {
                return Err(GroupEngineError::new(format!(
                    "raft node {} is not present in static peer config",
                    self.node_id
                )));
            }
            let config = Arc::new(
                Config {
                    cluster_name: format!("ursula-group-{}", placement.raft_group_id.0),
                    heartbeat_interval: 100,
                    election_timeout_min: 300,
                    election_timeout_max: 600,
                    ..Default::default()
                }
                .validate()
                .map_err(|err| GroupEngineError::new(format!("invalid OpenRaft config: {err}")))?,
            );
            let engine = if let Some(log_stores) = &self.log_stores {
                RaftGroupEngine::new_node_with_log_store_and_network(
                    placement,
                    self.node_id,
                    config,
                    GrpcRaftNetworkFactory::new(placement.raft_group_id),
                    log_stores.open(placement, metrics.clone())?,
                    Some(metrics),
                    self.cold_store.clone(),
                )
                .await?
            } else {
                RaftGroupEngine::new_node_with_log_store_and_network(
                    placement,
                    self.node_id,
                    config,
                    GrpcRaftNetworkFactory::new(placement.raft_group_id),
                    RaftGroupLogStore::shared(),
                    Some(metrics),
                    self.cold_store.clone(),
                )
                .await?
            };
            self.registry.register(placement, engine.raft_handle());
            if self.should_initialize_membership(placement.raft_group_id) {
                engine.initialize_membership(self.peer_nodes()).await?;
            }
            let engine: Box<dyn GroupEngine> = Box::new(engine);
            Ok(engine)
        })
    }
}
