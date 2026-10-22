use std::sync::Arc;

use ursula_config::config::ColdBackend;
use ursula_raft::HostingSet;
use ursula_raft::JournalTuning;
use ursula_raft::MetaPlacement;
use ursula_raft::PeerDirectory;
use ursula_raft::PlacementInputs;
use ursula_raft::RaftEngineConfig;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftWal;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::InMemoryGroupEngineFactory;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::RuntimeError;
use ursula_runtime::ShardRuntime;
use ursula_runtime::snapshot_store_from_config;
use ursula_runtime::spawn_cold_compaction_worker_if_configured;
use ursula_runtime::spawn_cold_flush_worker_if_configured;
use ursula_runtime::spawn_cold_gc_worker_if_configured;
use ursula_runtime::spawn_cold_index_repair_worker;
use ursula_runtime::spawn_cold_orphan_sweep_worker;
use ursula_runtime::spawn_cold_ref_offload_worker;

use crate::bootstrap::cold_health;
use crate::bootstrap::commit_stall;
use crate::bootstrap::egress;
use crate::bootstrap::hosting::SpawnedHosting;
use crate::bootstrap::leadership;
use crate::bootstrap::meta;
use crate::bootstrap::meta::SpawnedMeta;
use crate::bootstrap::participation::PeerParticipation;
use crate::bootstrap::snapshot;
use crate::bootstrap::snapshot::StoreGateInputs;
use crate::bootstrap::topology::ClusterWal;
use crate::bootstrap::topology::Topology;

/// Result of spawning a runtime.
#[derive(Debug)]
pub struct SpawnedRuntime {
    pub runtime: ShardRuntime,
    pub raft_registry: Option<RaftGroupHandleRegistry>,
    /// The node's Raft WAL when it runs Raft. Shut it down once the
    /// runtime's groups have stopped, so the next start finds a clean run.
    pub raft_wal: Option<RaftWal>,
    /// The meta journal, opened before the runtime started, with what the
    /// meta runtime starts with, when the node runs Raft. The server
    /// starts it ([`SpawnedMeta::start`]). A caller that does not start it
    /// runs no meta, and the WAL's shutdown closes the journal. Only meta
    /// binds a root, so the data replicas of a root its identity file
    /// records unbound then stay held out
    /// ([`ursula_runtime::NodeFence::awaits_binding`]) for the whole run.
    pub meta: Option<SpawnedMeta>,
    /// The hosting set and placement the hosting driver takes, when the
    /// node's groups are placed by meta. The server starts the driver
    /// ([`SpawnedHosting::start`]). Until then the node hosts nothing.
    pub hosting: Option<SpawnedHosting>,
}

#[cfg(test)]
impl SpawnedRuntime {
    /// For a test that runs no meta: settles the binding of an unbound root
    /// on the fence as a forming cluster's answer to its registration does,
    /// so its data replicas take part. It records nothing, so the root stays
    /// unbound on disk and every run of such a test settles it again. A
    /// bound root is not held and is left as it is.
    pub(crate) fn bind_without_meta(&self) {
        if let Some(wal) = &self.raft_wal {
            wal.fence().bind(false);
        }
    }
}

/// Failure to spawn a runtime.
#[derive(Debug, thiserror::Error)]
pub enum SpawnRuntimeError {
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    RaftWal(#[from] ursula_raft::RaftWalError),
    /// The client that reads the peers' leadership-shed state, which every
    /// handoff planner needs, could not be built.
    #[error("build the client that reads the peers' leadership-shed state")]
    PeerStatusClient {
        #[source]
        source: reqwest::Error,
    },
}

/// Spawns a runtime from a typed `ursula_config::UrsulaConfig`. A Raft
/// node's groups are placed by meta ([`MetaPlacement`]): it hosts nothing
/// until the server starts its hosting driver ([`SpawnedRuntime::hosting`]).
pub fn spawn_runtime(
    config: &ursula_config::UrsulaConfig,
    topology: Topology,
) -> Result<SpawnedRuntime, SpawnRuntimeError> {
    spawn_with_inputs(config, topology, |node_id, registry| {
        let hosting = HostingSet::default();
        let placement = MetaPlacement::new(
            ursula_proto::placement::NodeId::new(node_id),
            registry.clone(),
            hosting.clone(),
        );
        (
            Arc::new(placement.clone()),
            Some(SpawnedHosting { placement, hosting }),
        )
    })
}

/// [`spawn_runtime`] with every Raft group placed by `placement`, for a
/// test that runs no meta: production's placement inputs read meta's view.
#[cfg(test)]
pub(crate) fn spawn_runtime_with_placement(
    config: &ursula_config::UrsulaConfig,
    topology: Topology,
    placement: ursula_raft::FixedPlacement,
) -> Result<SpawnedRuntime, SpawnRuntimeError> {
    spawn_with_inputs(config, topology, move |_node_id, _registry| {
        (Arc::new(placement), None)
    })
}

/// The placement inputs of a node's groups, by its node id and registry,
/// and what its hosting driver takes, if one decides what it hosts.
type NodeInputs = (Arc<dyn PlacementInputs>, Option<SpawnedHosting>);

/// The one spawn path. A Raft node's groups open behind their recovery
/// gates with the placement inputs `inputs` gives for the node and its
/// registry.
fn spawn_with_inputs(
    config: &ursula_config::UrsulaConfig,
    topology: Topology,
    inputs: impl FnOnce(u64, &RaftGroupHandleRegistry) -> NodeInputs,
) -> Result<SpawnedRuntime, SpawnRuntimeError> {
    let mut runtime_config =
        RuntimeConfig::from_ursula_config(&config.runtime, topology.group_count());
    runtime_config.raft_max_uncommitted_bytes_per_group =
        config.raft.max_uncommitted_size_per_group.and_then(|s| {
            let bytes = s.as_bytes();
            if bytes == 0 { None } else { Some(bytes) }
        });
    runtime_config.cold_max_hot_bytes_per_group =
        config.storage.cold.max_hot_size_per_group.and_then(|s| {
            let bytes = s.as_bytes();
            if bytes == 0 { None } else { Some(bytes) }
        });
    let spawned = match topology {
        Topology::Development { .. } => {
            let cold_store = cold_store(config, ursula_runtime::NodeFence::unleased())?;
            let factory = InMemoryGroupEngineFactory::with_cold_store(cold_store.clone());
            SpawnedRuntime {
                runtime: ShardRuntime::spawn_with_engine_factory_and_cold_store(
                    runtime_config,
                    factory,
                    cold_store,
                )?,
                raft_registry: None,
                raft_wal: None,
                meta: None,
                hosting: None,
            }
        }
        Topology::Cluster {
            node_id,
            peers,
            group_count: _,
            wal,
        } => spawn_raft_node(config, runtime_config, node_id, peers, &wal, inputs)?,
    };
    ursula_runtime::tidy_worker::spawn_tidy_worker(
        &spawned.runtime,
        config.raft.effective_deleted_data_max_age(),
    );
    if spawned.runtime.has_cold_store() {
        spawn_cold_flush_worker_if_configured(&spawned.runtime, &config.storage.cold);
        spawn_cold_compaction_worker_if_configured(&spawned.runtime, &config.storage.cold);
        spawn_cold_gc_worker_if_configured(&spawned.runtime, &config.storage.cold);
        spawn_cold_index_repair_worker(&spawned.runtime);
        spawn_cold_orphan_sweep_worker(&spawned.runtime);
        spawn_cold_ref_offload_worker(&spawned.runtime);
    }
    Ok(spawned)
}

/// The cold store of `storage.cold`, behind `fence`, if one is configured.
fn cold_store(
    config: &ursula_config::UrsulaConfig,
    fence: ursula_runtime::NodeFence,
) -> Result<Option<ColdStoreHandle>, RuntimeError> {
    if config.storage.cold.backend == ColdBackend::None {
        return Ok(None);
    }
    Ok(Some(Arc::new(
        ColdStore::try_new(&config.storage.cold)
            .map_err(|err| RuntimeError::ColdStoreConfig {
                message: err.to_string(),
            })?
            .with_fence(fence),
    )))
}

/// A Raft node, a single node as a node of a cluster: its WAL, its meta
/// journal, its groups over the placement inputs `inputs` gives, and every
/// gate and driver of a Raft node.
fn spawn_raft_node(
    config: &ursula_config::UrsulaConfig,
    runtime_config: RuntimeConfig,
    node_id: u64,
    peers: PeerDirectory,
    wal: &ClusterWal,
    inputs: impl FnOnce(u64, &RaftGroupHandleRegistry) -> NodeInputs,
) -> Result<SpawnedRuntime, SpawnRuntimeError> {
    let group_count = runtime_config.raft_group_count;
    let snapshot_store =
        snapshot_store_from_config(&config.storage.snapshot, &config.storage.cold, node_id)
            .map_err(|err| RuntimeError::ColdStoreConfig {
                message: ursula_runtime::ErrorChain(&err).to_string(),
            })?;
    let engine_config = RaftEngineConfig::from(&config.raft);
    let snapshot_drive_interval = snapshot::resolve_snapshot_drive_interval(
        config
            .storage
            .snapshot
            .drive_interval
            .map(|interval| interval.as_duration()),
        snapshot_store.is_some(),
    );
    let cadence = snapshot::snapshot_cadence(&config.raft, group_count);
    let registry = RaftGroupHandleRegistry::default()
        .with_append_transport_budget_bytes(config.raft.append_transport_budget_bytes)
        .with_snapshot_install(
            config.raft.snapshot_install_max_concurrency,
            config.raft.install_snapshot_timeout.as_duration(),
        );

    // The node fence: a disk lease over the WAL root, which the WAL keeps,
    // the transports and leader predicate read, and the object stores
    // check before each PUT and DELETE.
    let fence = ursula_runtime::NodeFence::leased(config.raft.wal.lease.as_duration());
    let cold_store = cold_store(config, fence.clone())?;

    // Built before the runtime starts, so a failure leaves nothing running.
    let participation = PeerParticipation::new(
        node_id,
        reqwest::Client::builder()
            .timeout(
                config
                    .governance
                    .leadership_balance
                    .peer_timeout
                    .as_duration(),
            )
            .build()
            .map_err(|source| SpawnRuntimeError::PeerStatusClient { source })?,
    );

    let wal_config = &config.raft.wal;
    let shard_map = ursula_shard::StaticShardMap::new(runtime_config.core_count, group_count)
        .map_err(RuntimeError::from)?;
    // Reads the previous run's state and counts and records this run
    // before any group opens a journal.
    let raft_wal = RaftWal::start_in_cluster(
        wal.log_dir(),
        JournalTuning {
            // The resolved policy, which the WAL records in its run state.
            fsync: config.raft.wal_fsync(),
            segment_bytes: wal_config.segment_size.as_bytes(),
            group_cache_bytes: wal_config.group_cache_bytes(group_count),
        },
        &shard_map,
        ursula_proto::placement::NodeId::new(node_id),
        config.server.cluster_id.as_ref(),
        fence,
    )?;
    // Before the runtime starts, so `raft-log/meta/` exists before any data
    // group of the run opens and the peer identity is bound before any
    // group dials or answers a peer.
    let meta = meta::open_meta(
        config,
        node_id,
        peers,
        &raft_wal,
        &registry,
        &shard_map,
        engine_config.clone(),
        ursula_raft::meta::replica::MetaSnapshotTrigger {
            interval: snapshot_drive_interval,
            max_dirty_age: cadence.removal_floor,
        },
        participation.clone(),
    )
    .map_err(RuntimeError::from)?;
    let (placement, hosting) = inputs(node_id, &registry);
    let factory = ursula_raft::PlacedRaftGroupEngineFactory::new(
        raft_wal.clone(),
        registry.clone(),
        Arc::clone(&placement),
    )
    .with_cold_store(cold_store.clone())
    .with_snapshot_store(snapshot_store.clone())
    .with_engine_config(engine_config);
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        runtime_config,
        factory,
        cold_store,
    )?;

    snapshot::spawn_snapshot_driver(
        &runtime,
        &registry,
        snapshot_store.map(|store| {
            (store, StoreGateInputs {
                s3: config.storage.cold.s3.as_ref(),
                cold_flush_interval: config.storage.cold.flush_interval.as_duration(),
                participation: &participation,
            })
        }),
        snapshot_drive_interval,
        cadence,
        Some(raft_wal.maintenance()),
        config.raft.snapshot_pressure_max_groups_per_tick,
    );
    leadership::spawn_leadership_balancer(
        &registry,
        &participation,
        &config.governance.leadership_balance,
    );
    egress::spawn_egress_gate(&registry, &participation, &config.governance.cluster_probe);
    commit_stall::spawn_commit_stall_watchdog(&registry, &config.governance.commit_stall);
    // Hot bytes are a cold-tier backlog signal only when a cold store can
    // drain them. Memory-only nodes intentionally retain all payloads hot,
    // so applying the cold-health watermark there would eventually make
    // every voter shed leadership.
    if runtime.has_cold_store() {
        cold_health::spawn_cold_health_gate(
            &runtime,
            &registry,
            &participation,
            &config.governance.cold_health,
        );
    }
    Ok(SpawnedRuntime {
        runtime,
        raft_registry: Some(registry),
        raft_wal: Some(raft_wal),
        meta: Some(meta),
        hosting,
    })
}
