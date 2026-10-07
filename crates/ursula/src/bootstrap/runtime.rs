use std::sync::Arc;

use ursula_config::config::ColdBackend;
use ursula_raft::DurableRaftLogStoreFactory;
use ursula_raft::JournalTuning;
use ursula_raft::RaftEngineConfig;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::StaticGrpcRaftMembershipConfig;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::InMemoryGroupEngineFactory;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::RuntimeError;
use ursula_runtime::ShardRuntime;
use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::SnapshotReferenceConfig;
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
use crate::bootstrap::leadership;
use crate::bootstrap::snapshot;
use crate::bootstrap::topology::Persistence;
use crate::bootstrap::topology::Topology;

/// Result of spawning a runtime.
#[derive(Debug)]
pub struct SpawnedRuntime {
    pub runtime: ShardRuntime,
    pub raft_registry: Option<RaftGroupHandleRegistry>,
    /// The node's Raft WAL when it runs Raft. Shut it down once the
    /// runtime's groups have stopped, so the next start finds a clean run.
    pub raft_wal: Option<DurableRaftLogStoreFactory>,
}

/// Failure to spawn a runtime.
#[derive(Debug, thiserror::Error)]
pub enum SpawnRuntimeError {
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    RaftWal(#[from] ursula_raft::RaftWalError),
}

/// Where the runtime's groups keep their Raft logs, once the WAL has
/// started.
enum GroupStorage {
    /// No Raft: the in-memory engine.
    InMemory,
    /// Raft over the per-core journals of this run of the WAL.
    Raft(DurableRaftLogStoreFactory),
}

impl GroupStorage {
    /// Starts the Raft WAL when `persistence` runs Raft: reads the previous
    /// run's state and records this run before any group opens a journal.
    fn start(
        persistence: Persistence,
        tuning: JournalTuning,
        topology: &ursula_shard::StaticShardMap,
    ) -> Result<Self, SpawnRuntimeError> {
        Ok(match persistence {
            Persistence::InMemory => Self::InMemory,
            Persistence::Raft { log_dir } => Self::Raft(DurableRaftLogStoreFactory::start_with(
                log_dir, tuning, topology,
            )?),
        })
    }

    fn raft_wal(&self) -> Option<DurableRaftLogStoreFactory> {
        match self {
            Self::Raft(log_stores) => Some(log_stores.clone()),
            Self::InMemory => None,
        }
    }
}

/// Spawn a runtime from a typed `ursula_config::UrsulaConfig`.
pub fn spawn_runtime(
    config: &ursula_config::UrsulaConfig,
    persistence: Persistence,
    topology: Topology,
) -> Result<SpawnedRuntime, SpawnRuntimeError> {
    spawn_runtime_with_maintenance_drain(config, persistence, topology, false)
}

/// Spawn a runtime and install the operator drain fence before any Raft core
/// or background leadership worker can observe the process.
pub(crate) fn spawn_runtime_with_maintenance_drain(
    config: &ursula_config::UrsulaConfig,
    persistence: Persistence,
    topology: Topology,
    start_maintenance_drained: bool,
) -> Result<SpawnedRuntime, SpawnRuntimeError> {
    let mut runtime_config =
        RuntimeConfig::from_ursula_config(&config.runtime, topology.raft_group_count());
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
    let default_voters = if config.raft.peers.is_empty() {
        std::collections::BTreeSet::from([config.raft.node_id])
    } else {
        config.raft.peers.iter().map(|peer| peer.node_id).collect()
    };
    let snapshot_references = SnapshotReferenceConfig {
        node_id: config.raft.node_id,
        default_voters,
        per_group_voters: config
            .raft
            .groups
            .iter()
            .map(|group| (group.raft_group_id, group.voters.iter().copied().collect()))
            .collect(),
    };
    let snapshot_store = snapshot_store_from_config(
        &config.storage.snapshot,
        &config.storage.cold,
        snapshot_references,
    )
    .map_err(|err| RuntimeError::ColdStoreConfig {
        message: err.to_string(),
    })?;
    let mut engine_config = RaftEngineConfig::from(&config.raft);
    let configured_snapshot_drive_interval_ms = config
        .storage
        .snapshot
        .drive_interval
        .as_ref()
        .map(|interval| interval.as_duration().as_millis() as usize);
    let snapshot_drive_interval_ms = snapshot::resolve_snapshot_drive_interval_ms(
        configured_snapshot_drive_interval_ms,
        snapshot_store.is_some(),
    );
    engine_config.snapshot_drive_interval_ms = snapshot_drive_interval_ms as u64;
    let registry = RaftGroupHandleRegistry::default()
        .with_snapshot_install_max_concurrency(config.raft.snapshot_install_max_concurrency);
    if start_maintenance_drained {
        registry.mark_leadership_shed(ursula_raft::LeadershipShedReason::MaintenanceDrain);
    }

    let cold_store = if config.storage.cold.backend != ColdBackend::None {
        Some(Arc::new(ColdStore::try_new(&config.storage.cold).map_err(
            |err| RuntimeError::ColdStoreConfig {
                message: err.to_string(),
            },
        )?))
    } else {
        None
    };

    let wal = &config.raft.wal;
    let shard_map = ursula_shard::StaticShardMap::new(
        runtime_config.core_count,
        runtime_config.raft_group_count,
    )
    .map_err(RuntimeError::from)?;
    let storage = GroupStorage::start(
        persistence,
        JournalTuning {
            fsync: wal.fsync,
            segment_bytes: wal.segment_size.as_bytes(),
            group_cache_bytes: wal.group_cache_bytes(topology.raft_group_count()),
        },
        &shard_map,
    )?;
    let spawned = spawn_runtime_core(
        runtime_config,
        cold_store,
        storage,
        &topology,
        snapshot_store.clone(),
        Some(engine_config),
        Some(registry),
    )?;

    ursula_runtime::tidy_worker::spawn_tidy_worker(&spawned.runtime);
    if spawned.runtime.has_cold_store() {
        spawn_cold_flush_worker_if_configured(&spawned.runtime, &config.storage.cold);
        spawn_cold_compaction_worker_if_configured(&spawned.runtime, &config.storage.cold);
        spawn_cold_gc_worker_if_configured(&spawned.runtime, &config.storage.cold);
        spawn_cold_index_repair_worker(&spawned.runtime);
        spawn_cold_orphan_sweep_worker(&spawned.runtime);
        spawn_cold_ref_offload_worker(&spawned.runtime);
    }

    if let Topology::StaticCluster { node_id, peers, .. } = &topology {
        let registry = spawned
            .raft_registry
            .clone()
            .expect("static cluster has registry");
        snapshot::spawn_snapshot_driver(
            &spawned.runtime,
            &registry,
            snapshot_store,
            config.storage.cold.s3.as_ref(),
            snapshot_drive_interval_ms,
            ursula_raft::snapshot_cadence::SnapshotCadence::new(
                config.raft.snapshot_log_budget.as_bytes(),
                topology.raft_group_count(),
                config.raft.snapshot_backstop_logs,
            ),
            config.raft.snapshot_pressure_max_groups_per_tick,
        );
        leadership::spawn_leadership_balancer(
            &registry,
            *node_id,
            peers,
            &config.governance.leadership_balance,
        );
        let per_group_voters: std::collections::BTreeMap<
            ursula_shard::RaftGroupId,
            std::collections::BTreeSet<u64>,
        > = config
            .raft
            .groups
            .iter()
            .map(|g| {
                (
                    ursula_shard::RaftGroupId(g.raft_group_id),
                    g.voters.iter().cloned().collect(),
                )
            })
            .collect();
        egress::spawn_egress_gate(
            &registry,
            *node_id,
            peers,
            per_group_voters,
            &config.governance.cluster_probe,
        );
        commit_stall::spawn_commit_stall_watchdog(&registry, &config.governance.commit_stall);
        // Hot bytes are a cold-tier backlog signal only when a cold store can
        // drain them. Memory-only clusters intentionally retain all payloads
        // hot, so applying the cold-health watermark there would eventually
        // make every voter shed leadership.
        if spawned.runtime.has_cold_store() {
            cold_health::spawn_cold_health_gate(
                &spawned.runtime,
                &registry,
                *node_id,
                &config.governance.cold_health,
            );
        }
    }

    Ok(spawned)
}

/// Core runtime construction — maps persistence + topology to the correct
/// engine factory and spawns the runtime.  No background workers started here.
fn spawn_runtime_core(
    runtime_config: RuntimeConfig,
    cold_store: Option<ColdStoreHandle>,
    storage: GroupStorage,
    topology: &Topology,
    snapshot_store: Option<SharedSnapshotStore>,
    raft_engine_config: Option<RaftEngineConfig>,
    registry: Option<RaftGroupHandleRegistry>,
) -> Result<SpawnedRuntime, RuntimeError> {
    match topology {
        Topology::SingleNode { .. } => spawn_singleton(runtime_config, cold_store, storage),
        Topology::StaticCluster {
            node_id,
            peers,
            raft_group_count,
            initialize_membership,
            membership_config,
        } => spawn_static_cluster(
            runtime_config,
            cold_store,
            storage,
            *node_id,
            peers.clone(),
            *raft_group_count,
            *initialize_membership,
            membership_config.clone(),
            snapshot_store,
            raft_engine_config,
            registry.unwrap_or_default(),
        ),
    }
}

fn spawn_singleton(
    runtime_config: RuntimeConfig,
    cold_store: Option<ColdStoreHandle>,
    storage: GroupStorage,
) -> Result<SpawnedRuntime, RuntimeError> {
    let raft_wal = storage.raft_wal();
    let runtime = match storage {
        GroupStorage::InMemory => {
            let factory = InMemoryGroupEngineFactory::with_cold_store(cold_store.clone());
            ShardRuntime::spawn_with_engine_factory_and_cold_store(
                runtime_config,
                factory,
                cold_store,
            )?
        }
        GroupStorage::Raft(log_stores) => {
            let factory = ursula_raft::DurableRaftGroupEngineFactory::with_cold_store(
                log_stores,
                cold_store.clone(),
            );
            ShardRuntime::spawn_with_engine_factory_and_cold_store(
                runtime_config,
                factory,
                cold_store,
            )?
        }
    };
    Ok(SpawnedRuntime {
        runtime,
        raft_registry: None,
        raft_wal,
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "static-cluster bootstrap wires every runtime dependency in one place"
)]
fn spawn_static_cluster(
    runtime_config: RuntimeConfig,
    cold_store: Option<ColdStoreHandle>,
    storage: GroupStorage,
    node_id: u64,
    peers: Vec<(u64, String)>,
    _raft_group_count: usize,
    initialize_membership: bool,
    membership_config: StaticGrpcRaftMembershipConfig,
    snapshot_store: Option<SharedSnapshotStore>,
    raft_engine_config: Option<RaftEngineConfig>,
    registry: RaftGroupHandleRegistry,
) -> Result<SpawnedRuntime, RuntimeError> {
    let GroupStorage::Raft(log_stores) = storage else {
        return Err(RuntimeError::StaticMembershipConfig {
            message: "static cluster topology requires Raft persistence".to_owned(),
        });
    };
    let raft_wal = Some(log_stores.clone());
    let mut factory = ursula_raft::StaticGrpcRaftGroupEngineFactory::new(
        node_id,
        peers.clone(),
        initialize_membership,
        registry.clone(),
        log_stores,
    )
    .with_per_group_membership_initializers(membership_config.initialize_membership_per_group)
    .with_per_group_voters(membership_config.per_group_voters)
    .with_cold_store(cold_store.clone())
    .with_snapshot_store(snapshot_store);
    if let Some(engine_config) = raft_engine_config {
        factory = factory.with_engine_config(engine_config);
    }
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        runtime_config,
        factory,
        cold_store,
    )?;
    Ok(SpawnedRuntime {
        runtime,
        raft_registry: Some(registry),
        raft_wal,
    })
}
