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
use crate::rejoin::RECOVERY_INTERVAL;
use crate::rejoin::RECOVERY_STALL_AFTER;

/// Minimum election timeout of every cluster Raft group, in milliseconds.
pub(crate) const GROUP_ELECTION_TIMEOUT_MIN_MS: u64 = 1500;

/// Heartbeat interval of every cluster Raft group, in milliseconds: two
/// fifths of the minimum election timeout.
///
/// OpenRaft waits at most one heartbeat interval for a heartbeat's answer.
/// That makes it the longest round trip over which an idle group keeps
/// working: its leader takes writes only while a quorum answered within the
/// leader lease (`election_timeout_max`), and only heartbeats answer while
/// nothing replicates. ReadIndex reads, `leads_live` and the meta leader's
/// view of which nodes are live count those answers too. So the interval is
/// as long as the followers allow. A follower hears a heartbeat at least
/// every interval plus one tick (13/64 of the interval), 721 ms, under half
/// the minimum election timeout, so a heartbeat may arrive one more such
/// wait late before the follower campaigns.
const GROUP_HEARTBEAT_INTERVAL_MS: u64 = GROUP_ELECTION_TIMEOUT_MIN_MS * 2 / 5;

/// The OpenRaft config of a Raft group of a cluster, of either plane: the
/// static data groups and the meta group run with the same timeouts,
/// snapshot install timeout and log retention, and OpenRaft never takes a
/// snapshot by itself.
///
/// The election timeouts bound the slowest link a group keeps a leader
/// over. Replication is not held to the heartbeat interval: the gRPC
/// transport gives each Append call `election_timeout_min` to answer
/// (`grpc::RAFT_GRPC_APPEND_TIMEOUT`), so a slow link slows commits instead
/// of stopping them.
pub(crate) fn cluster_raft_config(
    cluster_name: String,
    engine: &RaftEngineConfig,
    enable_elect: bool,
) -> Config {
    Config {
        cluster_name,
        heartbeat_interval: GROUP_HEARTBEAT_INTERVAL_MS,
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

/// The data groups of a node, a single node included: their Raft logs live
/// in the per-core journals of a [`RaftWal`], they talk gRPC, every replica
/// opens behind its recovery gate, and every input placement decides comes
/// from one [`PlacementInputs`].
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
    /// How long an engine's shutdown waits for OpenRaft's work, for tests.
    #[cfg(test)]
    pub(crate) work_stop_timeout: Option<Duration>,
    /// A failure injected into every start once its Raft runs, for tests.
    #[cfg(test)]
    pub(crate) startup_fault: Option<super::StartupFault>,
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
            #[cfg(test)]
            work_stop_timeout: None,
            #[cfg(test)]
            startup_fault: None,
        }
    }

    /// The groups of a single node `node` over `log_stores`, each with `node`
    /// as its only voter and initializer ([`FixedPlacement::single`]). It
    /// binds `registry`'s peer cluster first, as a server binds its node
    /// before its runtime starts: `node` of the WAL's cluster, behind the
    /// WAL's fence, with an address book of itself.
    ///
    /// [`FixedPlacement::single`]: crate::FixedPlacement::single
    #[cfg(any(test, madsim, feature = "test-support"))]
    pub fn single_node(log_stores: RaftWal, registry: RaftGroupHandleRegistry, node: u64) -> Self {
        registry.peer_cluster().bind(
            crate::PeerIdentity::fenced_by(
                node,
                log_stores.cluster_id().clone(),
                log_stores.fence(),
            ),
            crate::PeerDirectory::new([(
                node,
                crate::PeerEndpoints::shared(format!("http://node-{node}")),
            )]),
        );
        Self::new(
            log_stores,
            registry,
            Arc::new(crate::FixedPlacement::single(node)),
        )
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
            let engine: Box<dyn GroupEngine> = Box::new(self.open(placement, metrics).await?);
            Ok(engine)
        })
    }
}

impl PlacedRaftGroupEngineFactory {
    /// Opens this node's replica of `placement`'s group: its log store with
    /// the placement's admission, then the replica on it ([`Self::start`]).
    pub async fn open(
        &self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> Result<RaftGroupEngine, GroupEngineError> {
        let (identity, admission) = self.placed(placement)?;
        let store = self
            .log_stores
            .open_waiting(placement, admission, metrics.clone())
            .await
            .map_err(|source| super::wal_error::group_open_error(placement, source))?;
        self.start(placement, identity, store, metrics).await
    }

    /// Starts this node's replica of `placement`'s group on `store`, a log
    /// store the caller opened from this factory's WAL, as [`Self::open`]
    /// starts one on the store it opens. For a simulation that keeps the
    /// stores it hands out.
    #[cfg(any(test, madsim, feature = "test-support"))]
    pub async fn start_on(
        &self,
        placement: ShardPlacement,
        store: Arc<crate::RaftGroupFileLogStore>,
        metrics: GroupEngineMetrics,
    ) -> Result<RaftGroupEngine, GroupEngineError> {
        let (identity, _admission) = self.placed(placement)?;
        self.start(placement, identity, store, metrics).await
    }

    /// The identity this node runs `placement`'s group as and how its store
    /// may open. A group the node does not host, or whose voters it cannot
    /// dial, never opens.
    fn placed(
        &self,
        placement: ShardPlacement,
    ) -> Result<(crate::PeerIdentity, Admission), GroupEngineError> {
        let group = placement.raft_group_id;
        let identity = self
            .registry
            .peer_cluster()
            .identity()
            .ok_or_else(|| invalid_configuration(placement))?;
        let node_id = identity.node_id();
        let peers = self.registry.peer_cluster().directory();
        // A group the node does not host, or whose voters it cannot dial,
        // never opens, and a replica is created only with voters. A held
        // replica opens without placed voters: it then asks only the members
        // of its own membership.
        let admission = self
            .inputs
            .admission(group)
            .ok_or_else(|| invalid_configuration(placement))?;
        let voters = self.inputs.nodes(group).voters();
        if !peers.contains(node_id)
            || (admission == Admission::Create && voters.is_empty())
            || !voters.iter().all(|voter| peers.contains(*voter))
        {
            return Err(invalid_configuration(placement));
        }
        Ok((identity, admission))
    }

    /// Starts the replica on `store`: its recovery gate, its Raft and its
    /// recovery drivers, and, for a group's only voter, its first
    /// leadership.
    async fn start(
        &self,
        placement: ShardPlacement,
        identity: crate::PeerIdentity,
        store: Arc<crate::RaftGroupFileLogStore>,
        metrics: GroupEngineMetrics,
    ) -> Result<RaftGroupEngine, GroupEngineError> {
        let group = placement.raft_group_id;
        let node_id = identity.node_id();
        // The recovery gate goes first: a gated replica's Raft core starts
        // with elections disabled.
        let rejoin = Arc::new(
            GroupRejoin::durable(node_id, ursula_shard::RaftTarget::Data(group), &store)
                .await
                .map_err(|source| super::wal_error::gate_open_error(placement, source))?,
        );
        let raft_config = cluster_raft_config(
            format!("ursula-group-{}", group.0),
            &self.engine_config,
            self.registry.election_policy().may_campaign(Some(&rejoin)),
        );
        let config = Arc::new(crate::openraft_error::validated_config(raft_config)?);
        let sole_voter_wait = Duration::from_millis(config.election_timeout_min);
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
        let network =
            GrpcRaftNetworkFactory::new(transport.clone(), crate::RaftTarget::Data(group))
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
                    .filter(|(faulty, _)| *faulty == group)
                    .map(|(_, fault)| fault),
                snapshot_record: Some(snapshot_record),
                recovery: rejoin.clone(),
                peer_cluster: Some(self.registry.peer_cluster().clone()),
                fence: Some(self.log_stores.fence()),
                #[cfg(test)]
                work_stop_timeout: self.work_stop_timeout,
                engine_work: Some(self.log_stores.engine_work_ledger()),
            },
            health.clone(),
        )
        .await
        .inspect_err(|_error| {
            if let Some(stopped) = health.stopped() {
                self.registry
                    .register_apply_stopped(group, node_id, stopped);
            }
        })?;
        #[cfg(test)]
        if let Some(fault) = &self.startup_fault {
            // The engine never serves. Dropping it stops it as any engine
            // stops (#709), as below.
            let error = engine.fail_start(fault).await;
            drop(engine);
            return Err(error);
        }
        let attached = engine.recovery_tasks.attach(
            &engine,
            rejoin,
            &self.registry,
            &*self.inputs,
            crate::recovery_transport::GrpcRecoveryTransport {
                transport,
                target: crate::RaftTarget::Data(group),
                timeout: self.engine_config.bootstrap_peer_connect,
            },
            crate::rejoin::RecoveryConfig {
                interval: RECOVERY_INTERVAL,
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
        // A replica its placement makes the group's only voter needs no
        // other node to lead: it leads as soon as its gate lets it campaign,
        // a fresh group once its bootstrap driver initialized it. Its open
        // waits for that, so a group opened by a request does not refuse
        // that request for want of a leader. A gate that waits for a
        // recovery plan keeps it from leading, and the open returns at the
        // bound with the replica waiting.
        if self.inputs.nodes(group).is_only_voter(node_id)
            && engine
                .wait_for_current_leader(node_id, sole_voter_wait)
                .await
                .is_err()
        {
            tracing::info!(
                raft_group_id = group.0,
                "the group's only voter opened without leading it; its recovery gate decides when it leads"
            );
        }
        Ok(engine)
    }
}

#[cfg(test)]
mod tests {
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
}
