use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use openraft::BasicNode;
use openraft::Config;
use openraft::SnapshotPolicy;
use ursula_runtime::{
    ColdStoreHandle, GroupEngine, GroupEngineCreateFuture, GroupEngineError, GroupEngineFactory,
    GroupEngineMetrics, SharedSnapshotStore,
};
use ursula_shard::{CoreId, RaftGroupId, ShardPlacement};

use crate::grpc::GrpcRaftNetworkFactory;
use crate::log_store::{CoreFileLogWriter, RaftGroupFileLogStore, RaftGroupLogStore};
use crate::registry::RaftGroupHandleRegistry;

use super::RaftGroupEngine;

const RAFT_INSTALL_SNAPSHOT_TIMEOUT_DEFAULT_MS: u64 = 120_000;
/// How long a restarting bootstrap node waits to observe an already-established
/// (or freshly re-elected) leader before deciding the group is truly new and
/// bootstrapping it. Must exceed the election window so peers that lost this
/// node as their leader have time to elect a replacement. Tunable via
/// `URSULA_RAFT_REJOIN_PROBE_MS`.
fn rejoin_leader_probe_timeout() -> Duration {
    let ms = std::env::var("URSULA_RAFT_REJOIN_PROBE_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(6000);
    Duration::from_millis(ms)
}

/// OpenRaft's `install_snapshot_timeout` covers the whole FullSnapshot RPC.
/// Ursula sends only a snapshot pointer in that RPC, but the receiver downloads
/// and installs the referenced object before replying, so this timeout must be
/// comfortably above the S3 per-attempt timeout plus retries. Tunable via
/// `URSULA_RAFT_INSTALL_SNAPSHOT_TIMEOUT_MS`.
fn raft_install_snapshot_timeout_ms() -> u64 {
    parse_positive_millis_env(
        "URSULA_RAFT_INSTALL_SNAPSHOT_TIMEOUT_MS",
        RAFT_INSTALL_SNAPSHOT_TIMEOUT_DEFAULT_MS,
    )
}

fn parse_positive_millis_env(name: &str, default_ms: u64) -> u64 {
    let raw = std::env::var(name).ok();
    parse_positive_millis(raw.as_deref(), default_ms)
}

fn parse_positive_millis(raw: Option<&str>, default_ms: u64) -> u64 {
    raw.and_then(|raw| raw.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(default_ms)
}

/// Whether the manual snapshot driver is configured. When it is, snapshots are
/// fully driver-driven and gated on S3 health, so openraft must not run its own
/// size-based snapshot policy: an auto-triggered `build_snapshot` during a local
/// S3 outage returns a `StorageError`, which openraft treats as fatal and kills
/// the raft group. Without the driver, openraft's default policy still applies.
fn snapshot_driver_configured() -> bool {
    std::env::var("URSULA_SNAPSHOT_DRIVE_INTERVAL_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .is_some_and(|ms| ms > 0)
}

fn raft_memory_bootstrap_marker_dir() -> Option<PathBuf> {
    std::env::var("URSULA_RAFT_MEMORY_BOOTSTRAP_MARKER_DIR")
        .ok()
        .filter(|raw| !raw.trim().is_empty())
        .map(PathBuf::from)
}

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
    per_group_voters: BTreeMap<RaftGroupId, BTreeSet<u64>>,
    initialize_membership: bool,
    initialize_membership_per_group: bool,
    registry: RaftGroupHandleRegistry,
    cold_store: Option<ColdStoreHandle>,
    log_stores: Option<DurableRaftLogStoreFactory>,
    snapshot_store: Option<SharedSnapshotStore>,
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
            per_group_voters: BTreeMap::new(),
            initialize_membership,
            initialize_membership_per_group: false,
            registry,
            cold_store: None,
            log_stores: None,
            snapshot_store: None,
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

    pub fn with_snapshot_store(mut self, snapshot_store: Option<SharedSnapshotStore>) -> Self {
        self.registry.set_snapshot_store(snapshot_store.clone());
        self.snapshot_store = snapshot_store;
        self
    }

    pub fn with_per_group_membership_initializers(mut self, enabled: bool) -> Self {
        self.initialize_membership_per_group = enabled;
        self
    }

    pub fn with_per_group_voters(mut self, voters: BTreeMap<RaftGroupId, BTreeSet<u64>>) -> Self {
        self.per_group_voters = voters;
        self
    }

    fn uses_memory_log_store(&self) -> bool {
        self.log_stores.is_none()
    }

    fn raft_memory_bootstrap_marker_path(&self, raft_group_id: RaftGroupId) -> Option<PathBuf> {
        Some(raft_memory_bootstrap_marker_dir()?.join(format!(
            "node-{}-group-{}.bootstrapped",
            self.node_id, raft_group_id.0
        )))
    }

    fn raft_memory_bootstrap_seen(&self, raft_group_id: RaftGroupId) -> bool {
        if !self.uses_memory_log_store() {
            return false;
        }
        self.raft_memory_bootstrap_marker_path(raft_group_id)
            .is_some_and(|path| path.exists())
    }

    fn mark_raft_memory_bootstrap_seen(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<(), GroupEngineError> {
        if !self.uses_memory_log_store() {
            return Ok(());
        }
        let Some(path) = self.raft_memory_bootstrap_marker_path(raft_group_id) else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                GroupEngineError::new(format!("create raft-memory bootstrap marker dir: {err}"))
            })?;
        }
        std::fs::write(&path, b"initialized\n").map_err(|err| {
            GroupEngineError::new(format!(
                "write raft-memory bootstrap marker {}: {err}",
                path.display()
            ))
        })
    }

    fn peer_nodes(&self) -> BTreeMap<u64, BasicNode> {
        self.peers
            .iter()
            .map(|(node_id, address)| (*node_id, BasicNode::new(address.clone())))
            .collect()
    }

    fn peer_nodes_for_group(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<BTreeMap<u64, BasicNode>, GroupEngineError> {
        let Some(voters) = self.per_group_voters.get(&raft_group_id) else {
            return Ok(self.peer_nodes());
        };
        if voters.is_empty() {
            return Err(GroupEngineError::new(format!(
                "raft group {} has an empty static voter set",
                raft_group_id.0
            )));
        }

        let mut nodes = BTreeMap::new();
        for node_id in voters {
            let address = self.peers.get(node_id).ok_or_else(|| {
                GroupEngineError::new(format!(
                    "raft group {} voter {} is not present in static peer config",
                    raft_group_id.0, node_id
                ))
            })?;
            nodes.insert(*node_id, BasicNode::new(address.clone()));
        }
        Ok(nodes)
    }

    fn membership_initializer_ids(&self, raft_group_id: RaftGroupId) -> Vec<u64> {
        self.per_group_voters
            .get(&raft_group_id)
            .map(|voters| voters.iter().copied().collect())
            .unwrap_or_else(|| self.peers.keys().copied().collect())
    }

    fn should_initialize_membership(&self, raft_group_id: RaftGroupId) -> bool {
        if !self.initialize_membership {
            return false;
        }
        if let Some(voters) = self.per_group_voters.get(&raft_group_id)
            && !voters.contains(&self.node_id)
        {
            return false;
        }
        if !self.initialize_membership_per_group {
            return true;
        }
        let initializer_ids = self.membership_initializer_ids(raft_group_id);
        let peer_count = initializer_ids.len();
        if peer_count == 0 {
            return false;
        }
        let initializer_index =
            usize::try_from(raft_group_id.0).expect("raft group id fits usize") % peer_count;
        initializer_ids
            .get(initializer_index)
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
            let mut raft_config = Config {
                cluster_name: format!("ursula-group-{}", placement.raft_group_id.0),
                // Timeouts tuned for a multi-AZ EC2 cluster carrying chaos faults.
                // The chaos test injects netem_delay 250ms±75ms; under that load,
                // the previous 100/300/600 produced 100s+ of spurious elections
                // (term 200-600 in 30 min). Heartbeat must stay well below
                // election_timeout_min, and election_timeout_min must stay above
                // worst-case fault-induced inter-heartbeat arrival.
                heartbeat_interval: 250,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                install_snapshot_timeout: raft_install_snapshot_timeout_ms(),
                ..Default::default()
            };
            // With the manual snapshot driver, snapshots are driver-driven and
            // gated on S3 health; openraft must not auto-trigger its own snapshot
            // because a build_snapshot failure during an S3 outage is fatal to
            // the group (it kills the raft core, so leadership can no longer be
            // yielded and only a process restart recovers it).
            if snapshot_driver_configured() {
                raft_config.snapshot_policy = SnapshotPolicy::Never;
            }
            let config =
                Arc::new(raft_config.validate().map_err(|err| {
                    GroupEngineError::new(format!("invalid OpenRaft config: {err}"))
                })?);
            let engine = if let Some(log_stores) = &self.log_stores {
                RaftGroupEngine::new_node_full(
                    placement,
                    self.node_id,
                    config,
                    GrpcRaftNetworkFactory::new(placement.raft_group_id),
                    log_stores.open(placement, metrics.clone())?,
                    Some(metrics),
                    self.cold_store.clone(),
                    self.snapshot_store.clone(),
                    Some(self.registry.snapshot_install_coordinator()),
                )
                .await?
            } else {
                RaftGroupEngine::new_node_full(
                    placement,
                    self.node_id,
                    config,
                    GrpcRaftNetworkFactory::new(placement.raft_group_id),
                    RaftGroupLogStore::shared(),
                    Some(metrics),
                    self.cold_store.clone(),
                    self.snapshot_store.clone(),
                    Some(self.registry.snapshot_install_coordinator()),
                )
                .await?
            };
            self.registry.register(placement, engine.raft_handle());
            if self.should_initialize_membership(placement.raft_group_id) {
                // An in-memory Raft node is not crash-recoverable. After this
                // node has once bootstrapped a group, a later empty startup is
                // a lost-node/rejoin case, not proof that the group is new.
                // Leave the raft uninitialized so quorum can remove/re-add it
                // as a learner instead of minting a second empty history.
                let bootstrapped = self.raft_memory_bootstrap_seen(placement.raft_group_id);
                let recovery_possible = self.snapshot_store.is_some() && !bootstrapped;
                let rejoin_existing_cluster = recovery_possible
                    && engine
                        .observe_any_leader(rejoin_leader_probe_timeout())
                        .await;
                if rejoin_existing_cluster {
                    self.mark_raft_memory_bootstrap_seen(placement.raft_group_id)?;
                } else if bootstrapped {
                    tracing::warn!(
                        "raft-memory: skip bootstrap for node {} group {} because this node already initialized it once; waiting for membership repair",
                        self.node_id,
                        placement.raft_group_id.0
                    );
                } else {
                    engine
                        .initialize_membership(self.peer_nodes_for_group(placement.raft_group_id)?)
                        .await?;
                    self.mark_raft_memory_bootstrap_seen(placement.raft_group_id)?;
                }
            }
            let engine: Box<dyn GroupEngine> = Box::new(engine);
            Ok(engine)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn peer_ids(nodes: BTreeMap<u64, BasicNode>) -> Vec<u64> {
        nodes.keys().copied().collect()
    }

    fn per_group_voters(groups: &[(u32, &[u64])]) -> BTreeMap<RaftGroupId, BTreeSet<u64>> {
        groups
            .iter()
            .map(|(group_id, voters)| (RaftGroupId(*group_id), voters.iter().copied().collect()))
            .collect()
    }

    fn factory_for_node(node_id: u64) -> StaticGrpcRaftGroupEngineFactory {
        StaticGrpcRaftGroupEngineFactory::new(
            node_id,
            [
                (1, "http://node-1".to_owned()),
                (2, "http://node-2".to_owned()),
                (3, "http://node-3".to_owned()),
                (4, "http://node-4".to_owned()),
            ],
            true,
            RaftGroupHandleRegistry::default(),
        )
        .with_per_group_voters(per_group_voters(&[(0, &[1, 2, 3]), (1, &[2, 3, 4])]))
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        static TEST_DIR_COUNTER: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let ordinal = TEST_DIR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "ursula-raft-{name}-{}-{}",
            std::process::id(),
            ordinal,
        ))
    }

    #[test]
    fn per_group_static_voters_override_default_peer_set() {
        let factory = factory_for_node(1);

        assert_eq!(
            peer_ids(factory.peer_nodes_for_group(RaftGroupId(0)).unwrap()),
            vec![1, 2, 3]
        );
        assert_eq!(
            peer_ids(factory.peer_nodes_for_group(RaftGroupId(1)).unwrap()),
            vec![2, 3, 4]
        );
        assert_eq!(
            peer_ids(factory.peer_nodes_for_group(RaftGroupId(2)).unwrap()),
            vec![1, 2, 3, 4]
        );
    }

    #[test]
    fn per_group_initializers_are_chosen_from_group_voters() {
        let node_1 = factory_for_node(1).with_per_group_membership_initializers(true);
        let node_3 = factory_for_node(3).with_per_group_membership_initializers(true);
        let node_4 = factory_for_node(4).with_per_group_membership_initializers(true);

        assert!(node_1.should_initialize_membership(RaftGroupId(0)));
        assert!(!node_4.should_initialize_membership(RaftGroupId(0)));

        assert!(node_3.should_initialize_membership(RaftGroupId(1)));
        assert!(!node_1.should_initialize_membership(RaftGroupId(1)));
    }

    #[test]
    fn raft_memory_bootstrap_marker_blocks_reinitialize() {
        let dir = unique_test_dir("memory-bootstrap-marker");
        let previous = std::env::var_os("URSULA_RAFT_MEMORY_BOOTSTRAP_MARKER_DIR");
        unsafe {
            std::env::set_var("URSULA_RAFT_MEMORY_BOOTSTRAP_MARKER_DIR", &dir);
        }

        let memory_factory = factory_for_node(1).with_per_group_membership_initializers(true);
        assert!(memory_factory.uses_memory_log_store());
        assert!(!memory_factory.raft_memory_bootstrap_seen(RaftGroupId(0)));
        memory_factory
            .mark_raft_memory_bootstrap_seen(RaftGroupId(0))
            .expect("write marker");
        assert!(memory_factory.raft_memory_bootstrap_seen(RaftGroupId(0)));

        let durable_factory = factory_for_node(1).with_raft_log_dir(dir.join("raft-log"));
        assert!(!durable_factory.uses_memory_log_store());
        assert!(!durable_factory.raft_memory_bootstrap_seen(RaftGroupId(0)));

        if let Some(previous) = previous {
            unsafe {
                std::env::set_var("URSULA_RAFT_MEMORY_BOOTSTRAP_MARKER_DIR", previous);
            }
        } else {
            unsafe {
                std::env::remove_var("URSULA_RAFT_MEMORY_BOOTSTRAP_MARKER_DIR");
            }
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn raft_snapshot_timeout_parser_uses_positive_millis_only() {
        assert_eq!(parse_positive_millis(Some("45000"), 120_000), 45_000);
        assert_eq!(parse_positive_millis(Some("0"), 120_000), 120_000);
        assert_eq!(parse_positive_millis(Some("-1"), 120_000), 120_000);
        assert_eq!(
            parse_positive_millis(Some("not-a-number"), 120_000),
            120_000
        );
        assert_eq!(parse_positive_millis(None, 120_000), 120_000);
    }
}
