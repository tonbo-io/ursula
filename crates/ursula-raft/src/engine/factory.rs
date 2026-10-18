use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use openraft::BasicNode;
use openraft::Config;
use openraft::SnapshotPolicy;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineCreateFuture;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupEngineFactory;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::SharedSnapshotStore;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;

use super::RaftGroupEngine;
use super::RaftGroupEngineOptions;
use crate::grpc::GrpcRaftNetworkFactory;
use crate::log_store::RaftWal;
use crate::peer_cluster::PeerIdentity;
use crate::registry::RaftGroupHandleRegistry;
use crate::rejoin::GroupRejoin;
use crate::rejoin::RECOVERY_STALL_AFTER;
use crate::rejoin::REJOIN_HEAL_INTERVAL;

/// Minimum election timeout of every data-group Raft, in milliseconds.
const GROUP_ELECTION_TIMEOUT_MIN_MS: u64 = 1500;

#[cfg(test)]
fn parse_positive_millis(raw: Option<&str>, default_ms: u64) -> u64 {
    raw.and_then(|raw| raw.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(default_ms)
}

#[derive(Debug, Clone)]
pub struct RaftEngineConfig {
    pub bootstrap_peer_probe: Duration,
    pub bootstrap_peer_probe_interval: Duration,
    pub bootstrap_peer_connect: Duration,
    pub install_snapshot_timeout_ms: u64,
    pub snapshot_drive_interval_ms: u64,
    pub grpc_reconnect_after_failures: u32,
    /// Snapshot builds the node runs at once. The server takes it from
    /// `raft.snapshot_build_max_concurrency` (2 by default). The default
    /// here is 1, the node-wide coordinator's own default
    /// ([`crate::RaftGroupHandleRegistry`] starts with one permit and a
    /// factory changes it only through
    /// [`StaticGrpcRaftGroupEngineFactory::with_engine_config`]), so an
    /// engine built without a config, as in the tests and the simulation,
    /// keeps one build at a time and the two cannot disagree.
    pub snapshot_build_max_concurrency: usize,
    pub snapshot_logs_since_last: u64,
    pub max_in_snapshot_log_to_keep: u64,
}

impl Default for RaftEngineConfig {
    fn default() -> Self {
        Self {
            bootstrap_peer_probe: Duration::from_millis(60_000),
            bootstrap_peer_probe_interval: Duration::from_millis(250),
            bootstrap_peer_connect: Duration::from_millis(500),
            install_snapshot_timeout_ms: 120_000,
            snapshot_drive_interval_ms: 0,
            grpc_reconnect_after_failures: 8,
            snapshot_build_max_concurrency: 1,
            snapshot_logs_since_last: 5_000,
            max_in_snapshot_log_to_keep: 64,
        }
    }
}

impl From<&ursula_config::RaftConfig> for RaftEngineConfig {
    fn from(cfg: &ursula_config::RaftConfig) -> Self {
        Self {
            bootstrap_peer_probe: cfg.bootstrap_peer_probe.as_duration(),
            bootstrap_peer_probe_interval: cfg.bootstrap_peer_probe_interval.as_duration(),
            bootstrap_peer_connect: cfg.bootstrap_peer_connect.as_duration(),
            install_snapshot_timeout_ms: cfg.install_snapshot_timeout.as_duration().as_millis()
                as u64,
            snapshot_drive_interval_ms: 0, // set by caller from RaftSnapshotConfig
            grpc_reconnect_after_failures: u32::try_from(cfg.grpc_reconnect_after_failures)
                .expect("config validation ensures grpc_reconnect_after_failures fits u32"),
            snapshot_build_max_concurrency: cfg.snapshot_build_max_concurrency,
            snapshot_logs_since_last: cfg.snapshot_logs_since_last,
            max_in_snapshot_log_to_keep: cfg.max_in_snapshot_log_to_keep,
        }
    }
}

fn jittered_snapshot_logs_since_last(base: u64, placement: ShardPlacement, node_id: u64) -> u64 {
    let base = base.max(1);
    // Groups receiving a uniform workload otherwise cross the same snapshot
    // threshold together on all replicas. Spread each group/replica over one
    // additional base interval so snapshot CPU and memory-bandwidth work does
    // not arrive as a node-wide burst. The mixer is deterministic because this
    // value is operational scheduling only; it is not replicated state.
    let seed = u64::from(placement.raft_group_id.0).wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ node_id.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    // `base` is at least 1, so the remainder always exists.
    base.saturating_add(seed.checked_rem(base).unwrap_or(0))
}

/// The Raft node id of every single-node group.
const SINGLE_NODE_ID: u64 = 1;

/// Single-node groups over the per-core journals of `log_stores`.
#[derive(Debug, Clone)]
pub struct DurableRaftGroupEngineFactory {
    #[cfg(test)]
    pub(crate) fail_apply_at: Option<(ursula_shard::RaftGroupId, u64)>,
    log_stores: RaftWal,
    cold_store: Option<ColdStoreHandle>,
    registry: Option<RaftGroupHandleRegistry>,
    /// The node's snapshot settings, which a server sets
    /// ([`Self::with_engine_config`]). Without them each group keeps
    /// OpenRaft's own snapshot policy and a coordinator of its own, as the
    /// tests, benchmarks and probes that build this factory expect.
    engine_config: Option<RaftEngineConfig>,
    /// How long an engine's shutdown waits for OpenRaft's work, for tests.
    #[cfg(test)]
    pub(crate) work_stop_timeout: Option<Duration>,
}

impl DurableRaftGroupEngineFactory {
    pub fn new(log_stores: RaftWal) -> Self {
        Self::with_cold_store(log_stores, None)
    }

    pub fn with_cold_store(log_stores: RaftWal, cold_store: Option<ColdStoreHandle>) -> Self {
        Self {
            #[cfg(test)]
            fail_apply_at: None,
            log_stores,
            cold_store,
            registry: None,
            engine_config: None,
            #[cfg(test)]
            work_stop_timeout: None,
        }
    }

    /// Publishes each live group's barrier, Raft handle and cold cache, or its
    /// terminal replay-failure diagnostics when committed application stops.
    pub fn with_registry(mut self, registry: RaftGroupHandleRegistry) -> Self {
        // A single node has no peers, but its Raft gRPC plane still refuses a
        // node of another cluster by the id of its WAL root.
        registry.peer_cluster().bind(
            PeerIdentity::new(SINGLE_NODE_ID, self.log_stores.cluster_id().clone()),
            BTreeMap::new(),
        );
        if let Some(config) = &self.engine_config {
            registry.set_snapshot_build_max_concurrency(config.snapshot_build_max_concurrency);
        }
        self.registry = Some(registry);
        self
    }

    pub fn registry(&self) -> Option<&RaftGroupHandleRegistry> {
        self.registry.as_ref()
    }

    /// Runs every group with the node's snapshot settings, as a static
    /// cluster's groups run (#669): `config.max_in_snapshot_log_to_keep`
    /// entries below a snapshot, and the snapshot driver's policy
    /// (`SnapshotPolicy::Never`) while `config.snapshot_drive_interval_ms`
    /// is not 0, else OpenRaft's jittered entry count. With a registry
    /// ([`Self::with_registry`], before or after this) every group shares
    /// its build and install coordinators, whose gauges the driver, the
    /// metrics and admission read, and the build coordinator gets
    /// `config.snapshot_build_max_concurrency` permits.
    pub fn with_engine_config(mut self, config: RaftEngineConfig) -> Self {
        if let Some(registry) = &self.registry {
            registry.set_snapshot_build_max_concurrency(config.snapshot_build_max_concurrency);
        }
        self.engine_config = Some(config);
        self
    }

    /// The OpenRaft config of one group: the single node's timeouts, with
    /// the node's snapshot settings when it has them.
    fn raft_config(&self, placement: ShardPlacement) -> Config {
        let mut config = Config {
            cluster_name: format!("ursula-group-{}", placement.raft_group_id.0),
            heartbeat_interval: 10,
            election_timeout_min: 30,
            election_timeout_max: 60,
            ..Default::default()
        };
        if let Some(engine) = &self.engine_config {
            config.max_in_snapshot_log_to_keep = engine.max_in_snapshot_log_to_keep;
            config.snapshot_policy = snapshot_policy(engine, placement, SINGLE_NODE_ID);
        }
        config
    }
}

/// OpenRaft's snapshot policy for a group under `engine`: none of its own
/// while the snapshot driver runs, so that the driver alone decides (F12e,
/// #669), else the jittered entry count.
fn snapshot_policy(
    engine: &RaftEngineConfig,
    placement: ShardPlacement,
    node_id: u64,
) -> SnapshotPolicy {
    if engine.snapshot_drive_interval_ms > 0 {
        SnapshotPolicy::Never
    } else {
        SnapshotPolicy::LogsSinceLast(jittered_snapshot_logs_since_last(
            engine.snapshot_logs_since_last,
            placement,
            node_id,
        ))
    }
}

impl GroupEngineFactory for DurableRaftGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let config =
                Arc::new(self.raft_config(placement).validate().map_err(|err| {
                    GroupEngineError::new(format!("invalid OpenRaft config: {err}"))
                })?);
            let log_store = self
                .log_stores
                .open(placement, metrics.clone())
                .map_err(|source| super::wal_error::group_open_error(placement, source))?;
            let apply_stop_signal = log_store.apply_stop_signal();
            let snapshot_record = log_store.snapshot_record();
            let health = crate::apply_failure::ApplyHealth::default();
            let engine =
                RaftGroupEngine::new_single_node_observed(
                    placement,
                    SINGLE_NODE_ID,
                    BasicNode::new("local"),
                    config,
                    log_store,
                    RaftGroupEngineOptions {
                        apply_stop_signal: Some(apply_stop_signal),
                        #[cfg(test)]
                        apply_fault: self
                            .fail_apply_at
                            .filter(|(group, _)| *group == placement.raft_group_id)
                            .map(|(_, index)| {
                                crate::apply_failure::ApplyFault::PanicAfterMutation { index }
                            }),
                        metrics: Some(metrics),
                        cold_store: self.cold_store.clone(),
                        snapshot_record: Some(snapshot_record),
                        snapshot_build: self
                            .registry
                            .as_ref()
                            .filter(|_| self.engine_config.is_some())
                            .map(RaftGroupHandleRegistry::snapshot_build_coordinator),
                        snapshot_install: self
                            .registry
                            .as_ref()
                            .filter(|_| self.engine_config.is_some())
                            .map(RaftGroupHandleRegistry::snapshot_install_coordinator),
                        #[cfg(test)]
                        work_stop_timeout: self.work_stop_timeout,
                        engine_work: Some(self.log_stores.engine_work_ledger()),
                        ..Default::default()
                    },
                    health.clone(),
                )
                .await
                .inspect_err(|_error| {
                    if let Some(stopped) = health.stopped()
                        && let Some(registry) = &self.registry
                    {
                        registry.register_apply_stopped(
                            placement.raft_group_id,
                            SINGLE_NODE_ID,
                            stopped,
                        );
                    }
                })?;
            if let Some(registry) = &self.registry {
                registry.register_engine(&engine);
            }
            let engine: Box<dyn GroupEngine> = Box::new(engine);
            Ok(engine)
        })
    }
}

#[derive(Debug, Clone)]
pub struct StaticGrpcRaftGroupEngineFactory {
    transports: Arc<Mutex<BTreeMap<u16, Arc<crate::grpc::CoreRaftTransport>>>>,
    node_id: u64,
    /// This node and the cluster id of its WAL root, which its peers check.
    identity: PeerIdentity,
    peers: BTreeMap<u64, String>,
    per_group_voters: BTreeMap<RaftGroupId, BTreeSet<u64>>,
    initialize_membership: bool,
    initialize_membership_per_group: bool,
    registry: RaftGroupHandleRegistry,
    cold_store: Option<ColdStoreHandle>,
    log_stores: RaftWal,
    snapshot_store: Option<SharedSnapshotStore>,
    engine_config: RaftEngineConfig,
    /// Faulty code that every replica of one group runs, for tests.
    #[cfg(any(test, madsim, feature = "fault-injection"))]
    apply_fault: Option<(RaftGroupId, crate::apply_failure::ApplyFault)>,
}

impl StaticGrpcRaftGroupEngineFactory {
    /// Groups of a static gRPC cluster whose Raft logs live in `log_stores`'
    /// journals; how they opened is published in `registry`, and the cluster
    /// id their root holds is the one this node's peers must share.
    pub fn new(
        node_id: u64,
        peers: impl IntoIterator<Item = (u64, String)>,
        initialize_membership: bool,
        registry: RaftGroupHandleRegistry,
        log_stores: RaftWal,
    ) -> Self {
        registry.set_wal_opening(log_stores.opening());
        let peers = peers.into_iter().collect::<BTreeMap<_, _>>();
        let identity = PeerIdentity::new(node_id, log_stores.cluster_id().clone());
        registry.peer_cluster().bind(
            identity.clone(),
            peers
                .iter()
                .filter(|(peer, _)| **peer != node_id)
                .map(|(peer, url)| (*peer, url.clone()))
                .collect(),
        );
        Self {
            transports: Arc::default(),
            node_id,
            identity,
            peers,
            per_group_voters: BTreeMap::new(),
            initialize_membership,
            initialize_membership_per_group: false,
            registry,
            cold_store: None,
            log_stores,
            snapshot_store: None,
            engine_config: RaftEngineConfig::default(),
            #[cfg(any(test, madsim, feature = "fault-injection"))]
            apply_fault: None,
        }
    }

    /// Run faulty code for `group`, as a deployment of a buggy binary does.
    #[cfg(any(test, madsim, feature = "fault-injection"))]
    pub fn with_apply_fault(
        mut self,
        group: RaftGroupId,
        fault: crate::apply_failure::ApplyFault,
    ) -> Self {
        self.apply_fault = Some((group, fault));
        self
    }

    pub fn registry(&self) -> &RaftGroupHandleRegistry {
        &self.registry
    }

    pub fn with_cold_store(mut self, cold_store: Option<ColdStoreHandle>) -> Self {
        self.cold_store = cold_store;
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

    pub fn with_engine_config(mut self, config: RaftEngineConfig) -> Self {
        self.registry
            .set_snapshot_build_max_concurrency(config.snapshot_build_max_concurrency);
        self.engine_config = config;
        self
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
        let voters = if self.per_group_voters.is_empty() {
            return Ok(self.peer_nodes());
        } else {
            self.per_group_voters.get(&raft_group_id).ok_or_else(|| {
                GroupEngineError::new(format!(
                    "raft group {} is missing from static per-group voter config",
                    raft_group_id.0
                ))
            })?
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

    fn membership_initializer_ids(&self, raft_group_id: RaftGroupId) -> Option<Vec<u64>> {
        if self.per_group_voters.is_empty() {
            return Some(self.peers.keys().copied().collect());
        }
        self.per_group_voters
            .get(&raft_group_id)
            .map(|voters| voters.iter().copied().collect())
    }

    fn should_initialize_membership(&self, raft_group_id: RaftGroupId) -> bool {
        if !self.initialize_membership {
            return false;
        }
        if !self.per_group_voters.is_empty()
            && !self
                .per_group_voters
                .get(&raft_group_id)
                .is_some_and(|voters| voters.contains(&self.node_id))
        {
            return false;
        }
        if !self.initialize_membership_per_group {
            return true;
        }
        let Some(initializer_ids) = self
            .membership_initializer_ids(raft_group_id)
            .filter(|ids| !ids.is_empty())
        else {
            return false;
        };
        usize::try_from(raft_group_id.0)
            .expect("raft group id fits usize")
            .checked_rem(initializer_ids.len())
            .and_then(|initializer_index| initializer_ids.get(initializer_index))
            .is_some_and(|node_id| *node_id == self.node_id)
    }
}

impl GroupEngineFactory for StaticGrpcRaftGroupEngineFactory {
    fn hosts_group(&self, placement: ShardPlacement) -> bool {
        if self.per_group_voters.is_empty() {
            return true;
        }
        if self
            .per_group_voters
            .get(&placement.raft_group_id)
            .is_some_and(|voters| voters.contains(&self.node_id))
        {
            return true;
        }
        self.registry
            .dynamic_group_hosting_allowed(placement.raft_group_id)
    }

    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            if !self.peers.contains_key(&self.node_id) {
                return Err(GroupEngineError::new(format!(
                    "raft node {} is not present in static peer config",
                    self.node_id
                )));
            }
            let nodes = self.peer_nodes_for_group(placement.raft_group_id)?;
            // The log store and the recovery gate go first: a gated replica's
            // Raft core starts with elections disabled.
            let store = self
                .log_stores
                .open(placement, metrics.clone())
                .map_err(|source| super::wal_error::group_open_error(placement, source))?;
            let rejoin = Arc::new(
                GroupRejoin::durable(self.node_id, placement.raft_group_id, &store)
                    .await
                    .map_err(|err| {
                        GroupEngineError::new(format!("open the recovery gate: {err}"))
                    })?,
            );
            let mut raft_config = Config {
                cluster_name: format!("ursula-group-{}", placement.raft_group_id.0),
                // Timeouts tuned for a multi-AZ EC2 cluster carrying chaos faults.
                // The chaos test injects netem_delay 250ms±75ms; under that load,
                // the previous 100/300/600 produced 100s+ of spurious elections
                // (term 200-600 in 30 min). Heartbeat must stay well below
                // election_timeout_min, and election_timeout_min must stay above
                // worst-case fault-induced inter-heartbeat arrival.
                heartbeat_interval: 250,
                election_timeout_min: GROUP_ELECTION_TIMEOUT_MIN_MS,
                election_timeout_max: 3000,
                install_snapshot_timeout: self.engine_config.install_snapshot_timeout_ms,
                max_in_snapshot_log_to_keep: self.engine_config.max_in_snapshot_log_to_keep,
                enable_elect: self.registry.election_policy().may_campaign(Some(&rejoin)),
                ..Default::default()
            };
            // With the manual snapshot driver, snapshots are driver-driven and
            // gated on S3 health; openraft must not auto-trigger its own snapshot
            // because a build_snapshot failure during an S3 outage is fatal to
            // the group (it kills the raft core, so leadership can no longer be
            // yielded and only a process restart recovers it).
            raft_config.snapshot_policy =
                snapshot_policy(&self.engine_config, placement, self.node_id);
            let config =
                Arc::new(raft_config.validate().map_err(|err| {
                    GroupEngineError::new(format!("invalid OpenRaft config: {err}"))
                })?);
            let transport = self
                .transports
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(placement.core_id.0)
                .or_insert_with(|| {
                    Arc::new(crate::grpc::CoreRaftTransport::with_budget(
                        self.registry.append_send_budget.clone(),
                        self.identity.clone(),
                        self.registry.peer_cluster().clone(),
                    ))
                })
                .clone();
            let network = GrpcRaftNetworkFactory::new(transport.clone(), placement.raft_group_id)
                .with_reconnect_threshold(self.engine_config.grpc_reconnect_after_failures)
                .with_rejoin(Some(rejoin.clone()));
            let apply_stop_signal = store.apply_stop_signal();
            let snapshot_record = store.snapshot_record();
            let health = crate::apply_failure::ApplyHealth::default();
            let engine = RaftGroupEngine::new_node_observed(
                placement,
                self.node_id,
                config,
                network,
                store,
                RaftGroupEngineOptions {
                    apply_stop_signal: Some(apply_stop_signal),
                    metrics: Some(metrics),
                    cold_store: self.cold_store.clone(),
                    snapshot_store: self.snapshot_store.clone(),
                    snapshot_build: Some(self.registry.snapshot_build_coordinator()),
                    snapshot_install: Some(self.registry.snapshot_install_coordinator()),
                    #[cfg(any(test, madsim, feature = "fault-injection"))]
                    apply_fault: self
                        .apply_fault
                        .filter(|(group, _)| *group == placement.raft_group_id)
                        .map(|(_, fault)| fault),
                    snapshot_record: Some(snapshot_record),
                    recovery: Some(rejoin.clone()),
                    peer_cluster: Some(self.registry.peer_cluster().clone()),
                    #[cfg(test)]
                    work_stop_timeout: None,
                    engine_work: Some(self.log_stores.engine_work_ledger()),
                    #[cfg(test)]
                    startup_fault: None,
                },
                health.clone(),
            )
            .await
            .inspect_err(|_error| {
                if let Some(stopped) = health.stopped() {
                    self.registry.register_apply_stopped(
                        placement.raft_group_id,
                        self.node_id,
                        stopped,
                    );
                }
            })?;
            let attached = engine.recovery_tasks.attach(
                &engine,
                rejoin,
                &self.registry,
                nodes,
                crate::recovery_transport::GrpcRecoveryTransport {
                    transport,
                    placement,
                    timeout: self.engine_config.bootstrap_peer_connect,
                },
                crate::rejoin::RecoveryConfig {
                    initialize: self.should_initialize_membership(placement.raft_group_id),
                    interval: REJOIN_HEAL_INTERVAL,
                    barrier_timeout: crate::rejoin::RECOVERY_BARRIER_TIMEOUT,
                    stall_after: RECOVERY_STALL_AFTER,
                    bootstrap_interval: self.engine_config.bootstrap_peer_probe_interval,
                    bootstrap_warn_after: self.engine_config.bootstrap_peer_probe,
                },
            );
            if let Err(err) = attached {
                // The engine never serves. Dropping it stops it as any engine
                // stops, so its snapshot publication closes before the group
                // can reopen (#489), in a task the WAL's ledger keeps: the
                // core loop does not wait, and the group stays closed until
                // the engine's work ended (#709).
                drop(engine);
                return Err(err);
            }
            let engine: Box<dyn GroupEngine> = Box::new(engine);
            Ok(engine)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    #[cfg(not(madsim))]
    use ursula_config::WalFsync;
    use ursula_shard::CoreId;
    use ursula_shard::ShardId;

    use super::*;

    #[cfg(not(madsim))]
    fn peer_ids(nodes: BTreeMap<u64, BasicNode>) -> Vec<u64> {
        nodes.keys().copied().collect()
    }

    #[cfg(not(madsim))]
    fn per_group_voters(groups: &[(u32, &[u64])]) -> BTreeMap<RaftGroupId, BTreeSet<u64>> {
        groups
            .iter()
            .map(|(group_id, voters)| (RaftGroupId(*group_id), voters.iter().copied().collect()))
            .collect()
    }

    #[cfg(not(madsim))]
    fn factory_for_node(
        node_id: u64,
        wal_root: &tempfile::TempDir,
    ) -> StaticGrpcRaftGroupEngineFactory {
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
            RaftWal::start(
                wal_root.path().join(format!("node-{node_id}")),
                WalFsync::Never,
                &ursula_shard::StaticShardMap::new(1, 2).expect("valid topology"),
            )
            .expect("start the WAL"),
        )
        .with_per_group_voters(per_group_voters(&[(0, &[1, 2, 3]), (1, &[2, 3, 4])]))
    }

    #[cfg(not(madsim))]
    #[test]
    fn per_group_static_voters_override_default_peer_set() {
        let wal_root = tempfile::tempdir().expect("WAL root");
        let factory = factory_for_node(1, &wal_root);

        assert_eq!(
            peer_ids(factory.peer_nodes_for_group(RaftGroupId(0)).unwrap()),
            vec![1, 2, 3]
        );
        assert_eq!(
            peer_ids(factory.peer_nodes_for_group(RaftGroupId(1)).unwrap()),
            vec![2, 3, 4]
        );
        let err = factory
            .peer_nodes_for_group(RaftGroupId(2))
            .expect_err("partial per-group voter config must not fall back to all peers");
        assert!(
            err.message()
                .contains("missing from static per-group voter config")
        );
    }

    #[cfg(not(madsim))]
    #[test]
    fn per_group_initializers_are_chosen_from_group_voters() {
        let wal_root = tempfile::tempdir().expect("WAL root");
        let node_1 = factory_for_node(1, &wal_root).with_per_group_membership_initializers(true);
        let node_3 = factory_for_node(3, &wal_root).with_per_group_membership_initializers(true);
        let node_4 = factory_for_node(4, &wal_root).with_per_group_membership_initializers(true);

        assert!(node_1.should_initialize_membership(RaftGroupId(0)));
        assert!(!node_4.should_initialize_membership(RaftGroupId(0)));

        assert!(node_3.should_initialize_membership(RaftGroupId(1)));
        assert!(!node_1.should_initialize_membership(RaftGroupId(1)));
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

    #[test]
    fn engine_config_uses_bounded_snapshot_log_retention() {
        let config = ursula_config::RaftConfig {
            snapshot_logs_since_last: 20_000,
            max_in_snapshot_log_to_keep: 128,
            ..Default::default()
        };

        let engine_config = RaftEngineConfig::from(&config);

        assert_eq!(engine_config.snapshot_logs_since_last, 20_000);
        assert_eq!(engine_config.max_in_snapshot_log_to_keep, 128);
    }

    /// The config's build concurrency reaches the engine: 2 by default
    /// (#519). An engine built without a config keeps one build at a time.
    #[test]
    fn engine_config_carries_the_configs_build_concurrency() {
        let engine_config = RaftEngineConfig::from(&ursula_config::RaftConfig::default());
        assert_eq!(engine_config.snapshot_build_max_concurrency, 2);
        let four = ursula_config::RaftConfig {
            snapshot_build_max_concurrency: 4,
            ..Default::default()
        };
        assert_eq!(
            RaftEngineConfig::from(&four).snapshot_build_max_concurrency,
            4
        );
        assert_eq!(
            RaftEngineConfig::default().snapshot_build_max_concurrency,
            1
        );
    }

    #[test]
    fn automatic_snapshot_thresholds_are_bounded_and_spread() {
        let placement = |group_id| ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(group_id),
            raft_group_id: RaftGroupId(group_id),
        };
        let base = 5_000;
        let thresholds = (0..32)
            .flat_map(|group_id| {
                (1..=3).map(move |node_id| {
                    jittered_snapshot_logs_since_last(base, placement(group_id), node_id)
                })
            })
            .collect::<BTreeSet<_>>();

        assert!(thresholds.iter().all(|value| *value >= base));
        assert!(thresholds.iter().all(|value| *value < base * 2));
        assert!(
            thresholds.len() >= 80,
            "group/replica thresholds should be well distributed"
        );
    }

    #[test]
    fn automatic_snapshot_threshold_handles_zero_base() {
        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(0),
        };

        assert_eq!(jittered_snapshot_logs_since_last(0, placement, 1), 1);
    }

    /// #669: a single node's groups take the node's snapshot settings once
    /// a server sets them: the driver's policy, the configured entries kept
    /// below a snapshot, and the registry's build coordinator with the
    /// configured permits. Without them they keep OpenRaft's own policy, as
    /// the tests, benchmarks and probes that build this factory expect.
    #[cfg(not(madsim))]
    #[test]
    fn a_single_node_factory_takes_the_node_snapshot_settings() {
        let wal_root = tempfile::tempdir().expect("WAL root");
        let wal = RaftWal::start(
            wal_root.path(),
            WalFsync::Never,
            &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
        )
        .expect("start the WAL");
        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(0),
        };
        let openraft = Config::default();
        let plain = DurableRaftGroupEngineFactory::new(wal.clone()).raft_config(placement);
        assert_eq!(
            plain.max_in_snapshot_log_to_keep,
            openraft.max_in_snapshot_log_to_keep
        );
        assert_eq!(plain.snapshot_policy, openraft.snapshot_policy);

        let registry = RaftGroupHandleRegistry::default();
        let driven = DurableRaftGroupEngineFactory::new(wal.clone())
            .with_registry(registry.clone())
            .with_engine_config(RaftEngineConfig {
                snapshot_drive_interval_ms: 1_000,
                snapshot_build_max_concurrency: 3,
                ..RaftEngineConfig::default()
            });
        let config = driven.raft_config(placement);
        assert_eq!(config.max_in_snapshot_log_to_keep, 64);
        assert_eq!(config.snapshot_policy, SnapshotPolicy::Never);
        assert_eq!(registry.snapshot_build_coordinator().max_concurrency(), 3);
        // The order of the two calls does not matter.
        let registry = RaftGroupHandleRegistry::default();
        let _config_first = DurableRaftGroupEngineFactory::new(wal.clone())
            .with_engine_config(RaftEngineConfig {
                snapshot_drive_interval_ms: 1_000,
                snapshot_build_max_concurrency: 4,
                ..RaftEngineConfig::default()
            })
            .with_registry(registry.clone());
        assert_eq!(registry.snapshot_build_coordinator().max_concurrency(), 4);

        // With the driver off, OpenRaft's jittered entry count.
        let undriven = DurableRaftGroupEngineFactory::new(wal)
            .with_registry(RaftGroupHandleRegistry::default())
            .with_engine_config(RaftEngineConfig::default())
            .raft_config(placement);
        assert_eq!(
            undriven.snapshot_policy,
            SnapshotPolicy::LogsSinceLast(jittered_snapshot_logs_since_last(
                5_000,
                placement,
                SINGLE_NODE_ID
            ))
        );
    }
}
