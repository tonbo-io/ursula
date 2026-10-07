use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;

use futures_util::future::join_all;
use openraft::BasicNode;
use openraft::Config;
use openraft::RaftNetworkV2;
use openraft::SnapshotPolicy;
use openraft::network::RPCOption;
use openraft::rt::WatchReceiver;
use ursula_config::WalFsync;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineCreateFuture;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupEngineFactory;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::SharedSnapshotStore;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;

use super::RaftGroupEngine;
use crate::grpc::GrpcRaftNetwork;
use crate::grpc::GrpcRaftNetworkFactory;
use crate::grpc::probe_rejoin_vote_barrier;
use crate::log_store::CORE_JOURNAL_FILE;
use crate::log_store::CoreFileLogWriter;
use crate::log_store::CoreJournalOptions;
use crate::log_store::NodeWal;
use crate::log_store::RaftGroupFileLogStore;
use crate::log_store::RaftGroupLogStore;
use crate::log_store::RaftWalError;
use crate::log_store::RecoveryState;
use crate::log_store::WalOpening;
use crate::registry::RaftGroupHandle;
use crate::registry::RaftGroupHandleRegistry;
use crate::rejoin::GroupBootstrap;
use crate::rejoin::GroupRejoin;
use crate::rejoin::PeerGroupLog;
use crate::rejoin::RECOVERY_STALL_AFTER;
use crate::rejoin::REJOIN_HEAL_INTERVAL;
use crate::rejoin::bootstrap_probe_vote;
use crate::rejoin::run_group_bootstrap;
use crate::rejoin::run_rejoin_heal;

/// Minimum election timeout of every data-group Raft, in milliseconds.
/// `ursulactl`'s restart fence waits this long for an in-flight leadership
/// transfer to settle, so it reads the same constant.
pub const GROUP_ELECTION_TIMEOUT_MIN_MS: u64 = 1500;

const REJOIN_VOTE_BARRIER_TIMEOUT: Duration = Duration::from_secs(3);

fn spawn_rejoin_vote_barrier(
    placement: ShardPlacement,
    raft: RaftGroupHandle,
    rejoin: Arc<GroupRejoin>,
    registry: RaftGroupHandleRegistry,
    nodes: BTreeMap<u64, BasicNode>,
) {
    let node_id = raft.metrics().borrow_watched().id;
    tokio::spawn(crate::rejoin::run_rejoin_vote_barrier(
        raft,
        rejoin,
        registry,
        nodes,
        move |leader_id, address| async move {
            probe_rejoin_vote_barrier(
                placement,
                node_id,
                leader_id,
                &address,
                REJOIN_VOTE_BARRIER_TIMEOUT,
            )
            .await
        },
        REJOIN_VOTE_BARRIER_TIMEOUT,
        REJOIN_HEAL_INTERVAL,
        RECOVERY_STALL_AFTER,
    ));
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
    pub snapshot_drive_interval_ms: u64,
    pub memory_bootstrap_marker_dir: Option<PathBuf>,
    pub grpc_reconnect_after_failures: u32,
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
            memory_bootstrap_marker_dir: None,
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
            memory_bootstrap_marker_dir: cfg.memory_bootstrap_marker_dir.clone(),
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

fn write_bootstrap_marker(
    node_id: u64,
    raft_group_id: RaftGroupId,
    marker_path: Option<&std::path::Path>,
) {
    let Some(path) = marker_path else {
        return;
    };
    if let Some(parent) = path.parent()
        && let Err(err) = std::fs::create_dir_all(parent)
    {
        tracing::error!(
            "raft bootstrap: node {node_id} group {} failed to create marker dir: {err}",
            raft_group_id.0
        );
        return;
    }
    if let Err(err) = std::fs::write(path, b"initialized\n") {
        tracing::error!(
            "raft bootstrap: node {node_id} group {} failed to write marker {}: {err}",
            raft_group_id.0,
            path.display()
        );
    }
}

/// Ask one voter what it holds for the group with the bootstrap probe vote
/// (see [`bootstrap_probe_vote`]). `None`: no answer (unreachable, or the group is not
/// registered there yet).
async fn probe_peer_group_log(
    raft_group_id: RaftGroupId,
    node_id: u64,
    peer_id: u64,
    address: &str,
    timeout: Duration,
) -> Option<PeerGroupLog> {
    let mut network = GrpcRaftNetwork::new(raft_group_id, peer_id, address);
    network
        .vote(bootstrap_probe_vote(node_id), RPCOption::new(timeout))
        .await
        .ok()
        .map(|response| PeerGroupLog::from_vote_response(&response))
}

/// Membership bootstrap of a group's initializer over gRPC; see
/// [`run_group_bootstrap`]. One voter holding the group means the replica
/// lost what it held; it then waits to be replicated to, and the leader's
/// heal driver rebuilds it. A memory-WAL node with a bootstrap marker
/// directory records the bootstrap at `marker_path`.
fn spawn_group_bootstrap(
    node_id: u64,
    raft_group_id: RaftGroupId,
    raft: RaftGroupHandle,
    rejoin: Arc<GroupRejoin>,
    nodes: BTreeMap<u64, BasicNode>,
    marker_path: Option<PathBuf>,
    engine_config: RaftEngineConfig,
) {
    tokio::spawn(async move {
        let connect = engine_config.bootstrap_peer_connect;
        let outcome = run_group_bootstrap(
            node_id,
            raft,
            rejoin,
            nodes,
            move |peer_id, address| async move {
                probe_peer_group_log(raft_group_id, node_id, peer_id, &address, connect).await
            },
            engine_config.bootstrap_peer_probe_interval,
            engine_config.bootstrap_peer_probe,
        )
        .await;
        if matches!(
            outcome,
            GroupBootstrap::Initialized | GroupBootstrap::Rejoined
        ) {
            write_bootstrap_marker(node_id, raft_group_id, marker_path.as_deref());
        }
    });
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
            // The barrier goes in before the raft handle, so a forwarded read
            // that finds the group always finds its barrier.
            self.registry
                .register_read_barrier(placement.raft_group_id, engine.read_barrier.clone());
            self.registry.register(placement, engine.raft.clone());
            self.registry.register_cold_index_cache(
                placement.raft_group_id,
                engine.cold_index_cache.clone(),
            );
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

/// A group's Raft log store on this node.
enum GroupLogStore {
    /// The core journal, and where the group's snapshot metadata lives.
    Durable {
        store: Arc<RaftGroupFileLogStore>,
        snapshot_metadata: PathBuf,
    },
    /// The volatile memory log.
    Memory(Arc<RaftGroupLogStore>),
}

/// The open writer of one core's journal, if any.
type CoreWriterSlot = Arc<Mutex<Weak<CoreFileLogWriter>>>;

/// The core writers of a running WAL, by core.
#[derive(Debug)]
enum CoreWriterSlots {
    Running(BTreeMap<u16, CoreWriterSlot>),
    /// [`DurableRaftLogStoreFactory::shutdown`] closed every writer; no
    /// journal opens again in this run.
    ShutDown,
}

/// Opens each group's durable log store over its core's shared journal, for
/// one run of the node's Raft WAL.
///
/// [`DurableRaftLogStoreFactory::start`] begins the run: it reads the run
/// state the previous run left, decides how the journals open
/// ([`WalOpening`]) and records this run before any journal write.
/// [`DurableRaftLogStoreFactory::shutdown`] ends it cleanly.
#[derive(Debug, Clone)]
pub struct DurableRaftLogStoreFactory {
    node: Arc<NodeWal>,
    /// One slot per core. Opening a journal holds only its core's slot, so
    /// cores recover their journals in parallel.
    core_writers: Arc<Mutex<CoreWriterSlots>>,
}

impl DurableRaftLogStoreFactory {
    /// Starts a run of the Raft WAL under `root` with the `fsync` policy.
    pub fn start(root: impl Into<PathBuf>, fsync: WalFsync) -> Result<Self, RaftWalError> {
        Ok(Self {
            node: Arc::new(NodeWal::start(root.into(), fsync)?),
            core_writers: Arc::new(Mutex::new(CoreWriterSlots::Running(BTreeMap::new()))),
        })
    }

    pub fn root(&self) -> &Path {
        self.node.root()
    }

    pub fn fsync(&self) -> WalFsync {
        self.node.fsync()
    }

    /// How this run opens the journals the previous run left.
    pub fn opening(&self) -> WalOpening {
        self.node.opening()
    }

    /// Whether this node's logs may be missing entries it acknowledged.
    pub fn recovery_state(&self) -> RecoveryState {
        self.node.opening().recovery
    }

    pub(crate) fn core_journal_path(&self, core_id: CoreId) -> PathBuf {
        self.root()
            .join(format!("core-{}", core_id.0))
            .join(CORE_JOURNAL_FILE)
    }

    pub(crate) fn snapshot_metadata_path(&self, placement: ShardPlacement) -> PathBuf {
        self.root()
            .join(format!("core-{}", placement.core_id.0))
            .join(format!("group-{}.snapshot.json", placement.raft_group_id.0))
    }

    pub(crate) fn core_writer(
        &self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> Result<Arc<CoreFileLogWriter>, GroupEngineError> {
        let poisoned = || GroupEngineError::new("core file log writer mutex poisoned");
        let slot = match &mut *self.core_writers.lock().map_err(|_poisoned| poisoned())? {
            CoreWriterSlots::Running(slots) => {
                slots.entry(placement.core_id.0).or_default().clone()
            }
            CoreWriterSlots::ShutDown => {
                return Err(GroupEngineError::new(format!(
                    "open OpenRaft core journal: {}",
                    RaftWalError::ShutDown {
                        root: self.root().to_owned(),
                    }
                )));
            }
        };
        let mut slot = slot.lock().map_err(|_poisoned| poisoned())?;
        if let Some(writer) = slot.upgrade() {
            return Ok(writer);
        }

        let opening = self.node.opening();
        let writer = CoreFileLogWriter::open(
            self.core_journal_path(placement.core_id),
            CoreJournalOptions {
                fsync: self.fsync(),
                recovery_epoch: opening.recovery_epoch,
                run_state: self.node.run_state().clone(),
                node_recovery: opening.recovery,
            },
            Some((placement, metrics)),
        )
        .map_err(|err| GroupEngineError::new(format!("open OpenRaft core journal: {err}")))?;
        *slot = Arc::downgrade(&writer);
        Ok(writer)
    }

    pub fn open(
        &self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> Result<Arc<RaftGroupFileLogStore>, GroupEngineError> {
        let core_writer = self.core_writer(placement, metrics.clone())?;
        RaftGroupFileLogStore::open(placement, metrics, core_writer)
            .map_err(|err| GroupEngineError::new(format!("open OpenRaft file log: {err}")))
    }

    /// Ends this run cleanly: closes every core writer, each of which
    /// `fsync`s its journal, and only then records a clean shutdown. Stop the
    /// Raft groups first; a write after this fails and no journal opens
    /// again. When a writer cannot close, the run is not recorded as clean.
    pub async fn shutdown(&self) -> Result<(), RaftWalError> {
        let slots = {
            let mut core_writers = self
                .core_writers
                .lock()
                .map_err(|_poisoned| RaftWalError::LockPoisoned)?;
            match std::mem::replace(&mut *core_writers, CoreWriterSlots::ShutDown) {
                CoreWriterSlots::Running(slots) => slots,
                CoreWriterSlots::ShutDown => {
                    return Err(RaftWalError::ShutDown {
                        root: self.root().to_owned(),
                    });
                }
            }
        };
        let mut writers = Vec::with_capacity(slots.len());
        for (core, slot) in slots {
            let writer = slot
                .lock()
                .map_err(|_poisoned| RaftWalError::LockPoisoned)?
                .upgrade();
            writers.extend(writer.map(|writer| (core, writer)));
        }
        let closed = join_all(writers.iter().map(|(core, writer)| async move {
            writer
                .close()
                .await
                .map_err(|source| RaftWalError::CloseJournal {
                    core: *core,
                    source,
                })
        }))
        .await;
        closed.into_iter().collect::<Result<Vec<()>, _>>()?;
        self.node.record_clean()?;
        tracing::info!(
            root = %self.root().display(),
            cores = writers.len(),
            "shut down the Raft WAL cleanly"
        );
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DurableRaftGroupEngineFactory {
    log_stores: DurableRaftLogStoreFactory,
    cold_store: Option<ColdStoreHandle>,
}

impl DurableRaftGroupEngineFactory {
    pub fn new(log_stores: DurableRaftLogStoreFactory) -> Self {
        Self::with_cold_store(log_stores, None)
    }

    pub fn with_cold_store(
        log_stores: DurableRaftLogStoreFactory,
        cold_store: Option<ColdStoreHandle>,
    ) -> Self {
        Self {
            log_stores,
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
                RaftGroupEngine::new_single_node_with_log_store_metrics_and_snapshot_metadata(
                    placement,
                    1,
                    BasicNode::new("local"),
                    config,
                    log_store,
                    Some(metrics),
                    self.cold_store.clone(),
                    Some(self.log_stores.snapshot_metadata_path(placement)),
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
    engine_config: RaftEngineConfig,
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
            engine_config: RaftEngineConfig::default(),
        }
    }

    pub fn registry(&self) -> &RaftGroupHandleRegistry {
        &self.registry
    }

    pub fn with_cold_store(mut self, cold_store: Option<ColdStoreHandle>) -> Self {
        self.cold_store = cold_store;
        self
    }

    /// Keeps the groups' Raft logs in `log_stores`' journals, and publishes
    /// how they opened in the registry.
    pub fn with_raft_log_stores(mut self, log_stores: DurableRaftLogStoreFactory) -> Self {
        self.registry.set_wal_opening(log_stores.opening());
        self.log_stores = Some(log_stores);
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

    fn uses_memory_log_store(&self) -> bool {
        self.log_stores.is_none()
    }

    fn raft_memory_bootstrap_marker_path(&self, raft_group_id: RaftGroupId) -> Option<PathBuf> {
        Some(
            self.engine_config
                .memory_bootstrap_marker_dir
                .clone()?
                .join(format!(
                    "node-{}-group-{}.bootstrapped",
                    self.node_id, raft_group_id.0
                )),
        )
    }

    fn raft_memory_bootstrap_seen(&self, raft_group_id: RaftGroupId) -> bool {
        if !self.uses_memory_log_store() {
            return false;
        }
        self.raft_memory_bootstrap_marker_path(raft_group_id)
            .is_some_and(|path| path.exists())
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
            if self.should_initialize_membership(placement.raft_group_id)
                && self.raft_memory_bootstrap_seen(placement.raft_group_id)
            {
                return Err(GroupEngineError::new(format!(
                    "raft-memory node {} group {} has already bootstrapped once; refusing automatic restart because the volatile Raft log was lost. Use an explicit operator reset or persistent Raft storage.",
                    self.node_id, placement.raft_group_id.0
                )));
            }
            // The log store and the recovery gate go first: a gated replica's
            // Raft core starts with elections disabled.
            let (log_store, rejoin) = match &self.log_stores {
                Some(log_stores) => {
                    let store = log_stores.open(placement, metrics.clone())?;
                    let rejoin = Arc::new(GroupRejoin::durable(
                        self.node_id,
                        placement.raft_group_id,
                        &store,
                    ));
                    (
                        GroupLogStore::Durable {
                            store,
                            snapshot_metadata: log_stores.snapshot_metadata_path(placement),
                        },
                        rejoin,
                    )
                }
                None => (
                    GroupLogStore::Memory(RaftGroupLogStore::shared()),
                    Arc::new(GroupRejoin::volatile(self.node_id, placement.raft_group_id)),
                ),
            };
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
                enable_elect: rejoin.may_campaign(),
                ..Default::default()
            };
            // With the manual snapshot driver, snapshots are driver-driven and
            // gated on S3 health; openraft must not auto-trigger its own snapshot
            // because a build_snapshot failure during an S3 outage is fatal to
            // the group (it kills the raft core, so leadership can no longer be
            // yielded and only a process restart recovers it).
            if self.engine_config.snapshot_drive_interval_ms > 0 {
                raft_config.snapshot_policy = SnapshotPolicy::Never;
            } else {
                raft_config.snapshot_policy =
                    SnapshotPolicy::LogsSinceLast(jittered_snapshot_logs_since_last(
                        self.engine_config.snapshot_logs_since_last,
                        placement,
                        self.node_id,
                    ));
            }
            let config =
                Arc::new(raft_config.validate().map_err(|err| {
                    GroupEngineError::new(format!("invalid OpenRaft config: {err}"))
                })?);
            let network = GrpcRaftNetworkFactory::new(placement.raft_group_id)
                .with_reconnect_threshold(self.engine_config.grpc_reconnect_after_failures)
                .with_rejoin(Some(rejoin.clone()));
            let engine = match log_store {
                GroupLogStore::Durable {
                    store,
                    snapshot_metadata,
                } => {
                    RaftGroupEngine::new_node_full(
                        placement,
                        self.node_id,
                        config,
                        network,
                        store,
                        Some(metrics),
                        self.cold_store.clone(),
                        self.snapshot_store.clone(),
                        Some(self.registry.snapshot_build_coordinator()),
                        Some(self.registry.snapshot_install_coordinator()),
                        Some(snapshot_metadata),
                    )
                    .await?
                }
                GroupLogStore::Memory(store) => {
                    RaftGroupEngine::new_node_full(
                        placement,
                        self.node_id,
                        config,
                        network,
                        store,
                        Some(metrics),
                        self.cold_store.clone(),
                        self.snapshot_store.clone(),
                        Some(self.registry.snapshot_build_coordinator()),
                        Some(self.registry.snapshot_install_coordinator()),
                        None,
                    )
                    .await?
                }
            };
            // The barrier and the recovery gate go in before the raft handle,
            // so a forwarded read always finds its barrier and no vote reaches
            // the group unscreened.
            self.registry
                .register_read_barrier(placement.raft_group_id, engine.read_barrier.clone());
            rejoin.bind(&engine.raft_handle());
            self.registry
                .register_rejoin(placement.raft_group_id, rejoin.clone());
            self.registry.register(placement, engine.raft_handle());
            self.registry.register_cold_index_cache(
                placement.raft_group_id,
                engine.cold_index_cache.clone(),
            );
            if let Ok(configured) = self.peer_nodes_for_group(placement.raft_group_id) {
                tokio::spawn(run_rejoin_heal(
                    engine.raft_handle(),
                    rejoin.clone(),
                    configured.clone(),
                    REJOIN_HEAL_INTERVAL,
                ));
                spawn_rejoin_vote_barrier(
                    placement,
                    engine.raft_handle(),
                    rejoin.clone(),
                    self.registry.clone(),
                    configured,
                );
            }
            // A replica that ever held the group never initializes it again.
            if self.should_initialize_membership(placement.raft_group_id)
                && !rejoin.holds_group_history()
            {
                spawn_group_bootstrap(
                    self.node_id,
                    placement.raft_group_id,
                    engine.raft_handle(),
                    rejoin,
                    self.peer_nodes_for_group(placement.raft_group_id)?,
                    self.uses_memory_log_store()
                        .then(|| self.raft_memory_bootstrap_marker_path(placement.raft_group_id))
                        .flatten(),
                    self.engine_config.clone(),
                );
            }
            let engine: Box<dyn GroupEngine> = Box::new(engine);
            Ok(engine)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use ursula_shard::ShardId;

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

    /// A core recovering its journal does not hold up another core's.
    #[test]
    fn cores_open_their_journals_independently() {
        let root = unique_test_dir("parallel-core-open");
        let factory = DurableRaftLogStoreFactory::start(&root, WalFsync::Always).expect("start");
        let metrics = ursula_runtime::RuntimeMetrics::new(2, 2).group_engine_metrics();
        let placement = |core: u16, group: u32| ShardPlacement {
            core_id: CoreId(core),
            shard_id: ShardId(group),
            raft_group_id: RaftGroupId(group),
        };
        // Stand in for a long recovery of core 0 by holding its slot.
        let slot = {
            let mut core_writers = factory.core_writers.lock().expect("slots");
            let CoreWriterSlots::Running(slots) = &mut *core_writers else {
                panic!("a started WAL is running");
            };
            slots.entry(0).or_default().clone()
        };
        let held = slot.lock().expect("hold core 0");

        // Core 0's slot stays held on this thread, so this open would never
        // return if it waited for core 0.
        let store = factory
            .open(placement(1, 1), metrics)
            .expect("core 1 opens while core 0 recovers");
        drop(store);
        drop(held);
        crate::tests::remove_test_path(&root);
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
        let err = factory
            .peer_nodes_for_group(RaftGroupId(2))
            .expect_err("partial per-group voter config must not fall back to all peers");
        assert!(
            err.message()
                .contains("missing from static per-group voter config")
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

        let engine_config = RaftEngineConfig {
            memory_bootstrap_marker_dir: Some(dir.clone()),
            ..Default::default()
        };
        let memory_factory = factory_for_node(1)
            .with_per_group_membership_initializers(true)
            .with_engine_config(engine_config.clone());
        assert!(memory_factory.uses_memory_log_store());
        assert!(!memory_factory.raft_memory_bootstrap_seen(RaftGroupId(0)));
        write_bootstrap_marker(
            1,
            RaftGroupId(0),
            memory_factory
                .raft_memory_bootstrap_marker_path(RaftGroupId(0))
                .as_deref(),
        );
        assert!(memory_factory.raft_memory_bootstrap_seen(RaftGroupId(0)));

        let durable_factory = factory_for_node(1)
            .with_raft_log_stores(
                DurableRaftLogStoreFactory::start(dir.join("raft-log"), WalFsync::Always)
                    .expect("start the WAL"),
            )
            .with_engine_config(engine_config.clone());
        assert!(!durable_factory.uses_memory_log_store());
        assert!(!durable_factory.raft_memory_bootstrap_seen(RaftGroupId(0)));

        crate::tests::remove_test_path(dir);
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
}
