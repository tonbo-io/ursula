use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use openraft::Config;
use openraft::SnapshotPolicy;
use ursula_control::Admission;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineCreateFuture;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupEngineFactory;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::GroupInfraError;
use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::WalOpenFailureKind;
#[cfg(any(test, madsim, feature = "fault-injection"))]
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;

use super::RaftGroupEngine;
use super::RaftGroupEngineOptions;
use crate::PlacementInputs;
use crate::grpc::GrpcRaftNetworkFactory;
use crate::log_store::RaftWal;
use crate::registry::RaftGroupHandleRegistry;
use crate::rejoin::GroupRejoin;
use crate::rejoin::RECOVERY_STALL_AFTER;
use crate::rejoin::REJOIN_HEAL_INTERVAL;

/// Minimum election timeout of every data-group Raft, in milliseconds.
const GROUP_ELECTION_TIMEOUT_MIN_MS: u64 = 1500;

/// The OpenRaft config of a Raft group of a cluster, of either plane: the
/// static data groups and the meta group run with the same timeouts,
/// snapshot install timeout and log retention, and OpenRaft never takes a
/// snapshot by itself.
pub(crate) fn cluster_raft_config(
    cluster_name: String,
    engine: &RaftEngineConfig,
    enable_elect: bool,
) -> Config {
    Config {
        cluster_name,
        // Timeouts tuned for a multi-AZ EC2 cluster carrying chaos faults.
        // The chaos test injects netem_delay 250ms±75ms; under that load,
        // the previous 100/300/600 produced 100s+ of spurious elections
        // (term 200-600 in 30 min). Heartbeat must stay well below
        // election_timeout_min, and election_timeout_min must stay above
        // worst-case fault-induced inter-heartbeat arrival.
        heartbeat_interval: 250,
        election_timeout_min: GROUP_ELECTION_TIMEOUT_MIN_MS,
        election_timeout_max: 3000,
        install_snapshot_timeout: engine.install_snapshot_timeout_ms,
        max_in_snapshot_log_to_keep: engine.max_in_snapshot_log_to_keep,
        enable_elect,
        // The snapshot drivers alone take snapshots (F12e), a data group's
        // gated on S3 health. OpenRaft must not trigger its own: a
        // build_snapshot failure during an S3 outage is fatal to the group
        // (it kills the raft core, so leadership can no longer be yielded and
        // only a process restart recovers it).
        snapshot_policy: SnapshotPolicy::Never,
        ..Default::default()
    }
}

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
    pub grpc_reconnect_after_failures: u32,
    /// Snapshot builds the node runs at once. The server takes it from
    /// `raft.snapshot_build_max_concurrency` (2 by default). The default
    /// here is 1, the node-wide coordinator's own default
    /// ([`crate::RaftGroupHandleRegistry`] starts with one permit and a
    /// factory changes it only through
    /// [`PlacedRaftGroupEngineFactory::with_engine_config`]), so an
    /// engine built without a config, as in the tests and the simulation,
    /// keeps one build at a time and the two cannot disagree.
    pub snapshot_build_max_concurrency: usize,
    pub max_in_snapshot_log_to_keep: u64,
}

impl Default for RaftEngineConfig {
    fn default() -> Self {
        Self {
            bootstrap_peer_probe: Duration::from_millis(60_000),
            bootstrap_peer_probe_interval: Duration::from_millis(250),
            bootstrap_peer_connect: Duration::from_millis(500),
            install_snapshot_timeout_ms: 120_000,
            grpc_reconnect_after_failures: 8,
            snapshot_build_max_concurrency: 1,
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
            grpc_reconnect_after_failures: u32::try_from(cfg.grpc_reconnect_after_failures)
                .expect("config validation ensures grpc_reconnect_after_failures fits u32"),
            snapshot_build_max_concurrency: cfg.snapshot_build_max_concurrency,
            max_in_snapshot_log_to_keep: cfg.max_in_snapshot_log_to_keep,
        }
    }
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
    /// OpenRaft's default config, as the tests, benchmarks and probes that
    /// build this factory without a snapshot driver expect.
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
    ///
    /// It binds no peer identity: the server binds the node's
    /// ([`crate::PeerCluster::bind`]) before the runtime starts, and a bind
    /// here would replace it.
    pub fn with_registry(mut self, registry: RaftGroupHandleRegistry) -> Self {
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
    /// entries below a snapshot, and no snapshot policy of OpenRaft's own
    /// (`SnapshotPolicy::Never`), since the snapshot driver alone takes
    /// every snapshot (F12e). With a registry
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
            config.snapshot_policy = SnapshotPolicy::Never;
        }
        config
    }
}

impl GroupEngineFactory for DurableRaftGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let config = Arc::new(crate::openraft_error::validated_config(
                self.raft_config(placement),
            )?);
            let log_store = self
                .log_stores
                .open_waiting(placement, Admission::Create, metrics.clone())
                .await
                .map_err(|source| super::wal_error::group_open_error(placement, source))?;
            let apply_stop_signal = log_store.apply_stop_signal();
            let snapshot_record = log_store.snapshot_record();
            let health = crate::apply_failure::ApplyHealth::default();
            let engine =
                RaftGroupEngine::new_single_node_observed(
                    placement,
                    SINGLE_NODE_ID,
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
                        fence: Some(self.log_stores.fence()),
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

/// The data groups of a cluster node: their Raft logs live in the per-core
/// journals of a [`RaftWal`], they talk gRPC, every replica opens behind its
/// recovery gate, and every input placement decides comes from one
/// [`PlacementInputs`].
#[derive(Clone)]
pub struct PlacedRaftGroupEngineFactory {
    transports: Arc<Mutex<BTreeMap<u16, Arc<crate::grpc::CoreRaftTransport>>>>,
    inputs: Arc<dyn PlacementInputs>,
    registry: RaftGroupHandleRegistry,
    cold_store: Option<ColdStoreHandle>,
    log_stores: RaftWal,
    engine_config: RaftEngineConfig,
    /// Faulty code that every replica of one group runs, for tests.
    #[cfg(any(test, madsim, feature = "fault-injection"))]
    apply_fault: Option<(RaftGroupId, crate::apply_failure::ApplyFault)>,
}

impl std::fmt::Debug for PlacedRaftGroupEngineFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlacedRaftGroupEngineFactory")
            .field("engine_config", &self.engine_config)
            .finish_non_exhaustive()
    }
}

impl PlacedRaftGroupEngineFactory {
    /// Groups whose Raft logs live in `log_stores`' journals; how they
    /// opened is published in `registry`. They run as the identity and with
    /// the peers bound in `registry`'s peer cluster
    /// ([`crate::PeerCluster::bind`]), which the caller binds before any
    /// group opens: a group of a node with none bound, or not among its
    /// bound peers, is refused as an invalid configuration. `inputs` says
    /// which groups the node hosts, how each opens, its seed role and the
    /// node sets its recovery drivers read.
    pub fn new(
        log_stores: RaftWal,
        registry: RaftGroupHandleRegistry,
        inputs: Arc<dyn PlacementInputs>,
    ) -> Self {
        registry.set_wal_opening(log_stores.opening());
        Self {
            transports: Arc::default(),
            inputs,
            registry,
            cold_store: None,
            log_stores,
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

    /// The store every engine of the node writes its snapshot objects to:
    /// the registry's ([`RaftGroupHandleRegistry::set_snapshot_store`]),
    /// behind the node fence of the WAL, so no object PUT or DELETE leaves
    /// a fenced node or one that owes its predecessor wait.
    pub fn with_snapshot_store(self, snapshot_store: Option<SharedSnapshotStore>) -> Self {
        let fence = self.log_stores.fence();
        self.registry.set_snapshot_store(
            snapshot_store.map(|store| ursula_runtime::FencedSnapshotStore::wrap(store, fence)),
        );
        self
    }

    pub fn with_engine_config(mut self, config: RaftEngineConfig) -> Self {
        self.registry
            .set_snapshot_build_max_concurrency(config.snapshot_build_max_concurrency);
        self.engine_config = config;
        self
    }
}

/// The refusal of a group whose placement this node cannot run: it reaches
/// a group's open only as an invalid configuration.
fn invalid_configuration(placement: ShardPlacement) -> GroupEngineError {
    GroupEngineError::Infra(GroupInfraError::WalOpen {
        core_id: placement.core_id,
        group: ursula_shard::RaftTarget::Data(placement.raft_group_id),
        failure: WalOpenFailureKind::InvalidConfiguration,
    })
}

impl GroupEngineFactory for PlacedRaftGroupEngineFactory {
    fn hosts_group(&self, placement: ShardPlacement) -> bool {
        self.inputs.admission(placement.raft_group_id).is_some()
    }

    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let group = placement.raft_group_id;
            let identity = self
                .registry
                .peer_cluster()
                .identity()
                .ok_or_else(|| invalid_configuration(placement))?;
            let node_id = identity.node_id();
            let peers = self.registry.peer_cluster().directory();
            // A group the node does not host, or whose voters it cannot
            // dial, never opens.
            let admission = self
                .inputs
                .admission(group)
                .ok_or_else(|| invalid_configuration(placement))?;
            let voters = self.inputs.nodes(group).voters();
            if !peers.contains(node_id)
                || voters.is_empty()
                || !voters.iter().all(|voter| peers.contains(*voter))
            {
                return Err(invalid_configuration(placement));
            }
            // The log store and the recovery gate go first: a gated replica's
            // Raft core starts with elections disabled.
            let store = self
                .log_stores
                .open_waiting(placement, admission, metrics.clone())
                .await
                .map_err(|source| super::wal_error::group_open_error(placement, source))?;
            let rejoin = Arc::new(
                GroupRejoin::durable(
                    node_id,
                    ursula_shard::RaftTarget::Data(placement.raft_group_id),
                    &store,
                )
                .await
                .map_err(|source| super::wal_error::gate_open_error(placement, source))?,
            );
            let raft_config = cluster_raft_config(
                format!("ursula-group-{}", placement.raft_group_id.0),
                &self.engine_config,
                self.registry.election_policy().may_campaign(Some(&rejoin)),
            );
            let config = Arc::new(crate::openraft_error::validated_config(raft_config)?);
            let transport = self
                .transports
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(placement.core_id.0)
                .or_insert_with(|| {
                    Arc::new(crate::grpc::CoreRaftTransport::with_budget(
                        self.registry.append_send_budget.clone(),
                        identity,
                        self.registry.peer_cluster().clone(),
                    ))
                })
                .clone();
            let network = GrpcRaftNetworkFactory::new(
                transport.clone(),
                crate::RaftTarget::Data(placement.raft_group_id),
            )
            .with_reconnect_threshold(self.engine_config.grpc_reconnect_after_failures);
            let apply_stop_signal = store.apply_stop_signal();
            let snapshot_record = store.snapshot_record();
            let health = crate::apply_failure::ApplyHealth::default();
            let engine = RaftGroupEngine::new_node_observed(
                placement,
                node_id,
                config,
                network,
                store,
                RaftGroupEngineOptions {
                    apply_stop_signal: Some(apply_stop_signal),
                    metrics: Some(metrics),
                    cold_store: self.cold_store.clone(),
                    snapshot_store: Some(self.registry.snapshot_store()),
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
                    fence: Some(self.log_stores.fence()),
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
                    self.registry
                        .register_apply_stopped(placement.raft_group_id, node_id, stopped);
                }
            })?;
            let attached = engine.recovery_tasks.attach(
                &engine,
                rejoin,
                &self.registry,
                &*self.inputs,
                crate::recovery_transport::GrpcRecoveryTransport {
                    transport,
                    target: crate::RaftTarget::Data(placement.raft_group_id),
                    timeout: self.engine_config.bootstrap_peer_connect,
                },
                crate::rejoin::RecoveryConfig {
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
    #[cfg(not(madsim))]
    use ursula_config::WalFsync;
    #[cfg(not(madsim))]
    use ursula_shard::CoreId;
    #[cfg(not(madsim))]
    use ursula_shard::ShardId;

    use super::*;

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
            max_in_snapshot_log_to_keep: 128,
            ..Default::default()
        };

        let engine_config = RaftEngineConfig::from(&config);

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

    /// #669: a single node's groups take the node's snapshot settings once
    /// a server sets them: no snapshot policy of OpenRaft's own, the
    /// configured entries kept below a snapshot, and the registry's build
    /// coordinator with the configured permits. Without them they keep
    /// OpenRaft's default config, as the tests, benchmarks and probes that
    /// build this factory expect.
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
                snapshot_build_max_concurrency: 3,
                ..RaftEngineConfig::default()
            });
        let config = driven.raft_config(placement);
        assert_eq!(config.max_in_snapshot_log_to_keep, 64);
        assert_eq!(config.snapshot_policy, SnapshotPolicy::Never);
        assert_eq!(registry.snapshot_build_coordinator().max_concurrency(), 3);
        // The order of the two calls does not matter.
        let registry = RaftGroupHandleRegistry::default();
        let _config_first = DurableRaftGroupEngineFactory::new(wal)
            .with_engine_config(RaftEngineConfig {
                snapshot_build_max_concurrency: 4,
                ..RaftEngineConfig::default()
            })
            .with_registry(registry.clone());
        assert_eq!(registry.snapshot_build_coordinator().max_concurrency(), 4);
    }
}
