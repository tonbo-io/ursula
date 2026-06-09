use std::sync::Arc;

use ursula_config::config::ColdBackend;
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
use ursula_runtime::WalGroupEngineFactory;
use ursula_runtime::snapshot_store_from_config;
use ursula_runtime::spawn_cold_flush_worker_if_configured;
use ursula_runtime::spawn_cold_gc_worker_if_configured;

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
}

/// Spawn a runtime from a typed `ursula_config::UrsulaConfig`.
pub fn spawn_runtime(
    config: &ursula_config::UrsulaConfig,
    persistence: Persistence,
    topology: Topology,
) -> Result<SpawnedRuntime, RuntimeError> {
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
    let snapshot_store = snapshot_store_from_config(&config.storage.snapshot, &config.storage.cold)
        .map_err(|err| RuntimeError::ColdStoreConfig {
            message: err.to_string(),
        })?;
    let mut engine_config = RaftEngineConfig::from(&config.raft);
    let snapshot_drive_interval_ms = snapshot::resolve_snapshot_drive_interval_ms(
        Some(
            config
                .storage
                .snapshot
                .drive_interval
                .as_duration()
                .as_millis() as usize,
        ),
        snapshot_store.is_some(),
    );
    engine_config.snapshot_drive_interval_ms = snapshot_drive_interval_ms as u64;
    let registry = RaftGroupHandleRegistry::default()
        .with_snapshot_install_max_concurrency(config.raft.snapshot_install_max_concurrency);

    let cold_store = if config.storage.cold.backend != ColdBackend::None {
        Some(Arc::new(ColdStore::try_new(&config.storage.cold).map_err(
            |err| RuntimeError::ColdStoreConfig {
                message: err.to_string(),
            },
        )?))
    } else {
        None
    };

    let spawned = spawn_runtime_core(
        runtime_config,
        cold_store,
        persistence,
        &topology,
        snapshot_store.clone(),
        Some(engine_config),
        Some(registry),
    )?;

    if spawned.runtime.has_cold_store() {
        spawn_cold_flush_worker_if_configured(&spawned.runtime, &config.storage.cold);
        spawn_cold_gc_worker_if_configured(&spawned.runtime, &config.storage.cold);
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
            &config.storage.snapshot,
            config.storage.cold.s3.as_ref(),
            snapshot_drive_interval_ms,
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
        cold_health::spawn_cold_health_gate(
            &spawned.runtime,
            &registry,
            *node_id,
            &config.governance.cold_health,
        );
    }

    Ok(spawned)
}

/// Core runtime construction — maps persistence + topology to the correct
/// engine factory and spawns the runtime.  No background workers started here.
fn spawn_runtime_core(
    runtime_config: RuntimeConfig,
    cold_store: Option<ColdStoreHandle>,
    persistence: Persistence,
    topology: &Topology,
    snapshot_store: Option<SharedSnapshotStore>,
    raft_engine_config: Option<RaftEngineConfig>,
    registry: Option<RaftGroupHandleRegistry>,
) -> Result<SpawnedRuntime, RuntimeError> {
    match topology {
        Topology::SingleNode { .. } => spawn_singleton(runtime_config, cold_store, persistence),
        Topology::StaticCluster {
            node_id,
            peers,
            raft_group_count,
            initialize_membership,
            membership_config,
        } => spawn_static_cluster(
            runtime_config,
            cold_store,
            persistence,
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
    persistence: Persistence,
) -> Result<SpawnedRuntime, RuntimeError> {
    let runtime = match persistence {
        Persistence::InMemory => {
            let factory = InMemoryGroupEngineFactory::with_cold_store(cold_store.clone());
            ShardRuntime::spawn_with_engine_factory_and_cold_store(
                runtime_config,
                factory,
                cold_store,
            )?
        }
        Persistence::Wal { wal_dir } => {
            let factory = WalGroupEngineFactory::with_cold_store(wal_dir, cold_store.clone());
            ShardRuntime::spawn_with_engine_factory_and_cold_store(
                runtime_config,
                factory,
                cold_store,
            )?
        }
        Persistence::Raft { log_dir: None } => match cold_store {
            Some(ref cs) => ShardRuntime::spawn_with_engine_factory_and_cold_store(
                runtime_config,
                ursula_raft::ColdRaftGroupEngineFactory::new(cs.clone()),
                cold_store,
            )?,
            None => ShardRuntime::spawn_with_engine_factory_and_cold_store(
                runtime_config,
                ursula_raft::RaftGroupEngineFactory,
                cold_store,
            )?,
        },
        Persistence::Raft { log_dir: Some(dir) } => {
            let factory = ursula_raft::DurableRaftGroupEngineFactory::with_cold_store(
                dir,
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
    })
}

#[allow(clippy::too_many_arguments)]
fn spawn_static_cluster(
    runtime_config: RuntimeConfig,
    cold_store: Option<ColdStoreHandle>,
    persistence: Persistence,
    node_id: u64,
    peers: Vec<(u64, String)>,
    _raft_group_count: usize,
    initialize_membership: bool,
    membership_config: StaticGrpcRaftMembershipConfig,
    snapshot_store: Option<SharedSnapshotStore>,
    raft_engine_config: Option<RaftEngineConfig>,
    registry: RaftGroupHandleRegistry,
) -> Result<SpawnedRuntime, RuntimeError> {
    if !matches!(persistence, Persistence::Raft { .. }) {
        return Err(RuntimeError::StaticMembershipConfig {
            message: "static cluster topology requires Raft persistence".to_owned(),
        });
    }
    let mut factory = ursula_raft::StaticGrpcRaftGroupEngineFactory::new(
        node_id,
        peers.clone(),
        initialize_membership,
        registry.clone(),
    )
    .with_per_group_membership_initializers(membership_config.initialize_membership_per_group)
    .with_per_group_voters(membership_config.per_group_voters)
    .with_cold_store(cold_store.clone())
    .with_snapshot_store(snapshot_store);
    if let Some(engine_config) = raft_engine_config {
        factory = factory.with_engine_config(engine_config);
    }
    if let Persistence::Raft { log_dir: Some(dir) } = persistence {
        factory = factory.with_raft_log_dir(dir);
    }
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        runtime_config,
        factory,
        cold_store,
    )?;
    Ok(SpawnedRuntime {
        runtime,
        raft_registry: Some(registry),
    })
}
