use std::sync::Arc;

use ursula_config::config::ColdBackend;
use ursula_raft::ConfigSeeds;
use ursula_raft::JournalTuning;
use ursula_raft::RaftEngineConfig;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftWal;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::InMemoryGroupEngineFactory;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::RuntimeError;
use ursula_runtime::ShardRuntime;
use ursula_runtime::SharedSnapshotStore;
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
use crate::bootstrap::meta;
use crate::bootstrap::meta::SpawnedMeta;
use crate::bootstrap::participation::PeerParticipation;
use crate::bootstrap::snapshot;
use crate::bootstrap::snapshot::StoreGateInputs;
use crate::bootstrap::topology::Persistence;
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
    /// binds a root, so the data replicas of a root that was empty at its
    /// open then stay held out ([`ursula_runtime::NodeFence::awaits_binding`]).
    pub meta: Option<SpawnedMeta>,
}

#[cfg(test)]
impl SpawnedRuntime {
    /// For a test that runs no meta: settles the binding of a root that was
    /// empty at its open as a forming cluster's answer to its registration
    /// does, so its data replicas take part. A root that held state is not
    /// held and is left as it is.
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

/// Where the runtime's groups keep their Raft logs, once the WAL has
/// started.
enum GroupStorage {
    /// No Raft: the in-memory engine.
    InMemory,
    /// Raft over the per-core journals of this run of the WAL.
    Raft(RaftWal),
}

impl GroupStorage {
    /// Starts node `node`'s Raft WAL when `persistence` runs Raft: reads the
    /// previous run's state and counts and records this run before any group
    /// opens a journal.
    fn start(
        persistence: Persistence,
        tuning: JournalTuning,
        topology: &ursula_shard::StaticShardMap,
        node: u64,
        cluster_id: Option<&ursula_headers::ClusterId>,
        fence: &ursula_runtime::NodeFence,
    ) -> Result<Self, SpawnRuntimeError> {
        Ok(match persistence {
            Persistence::InMemory => Self::InMemory,
            Persistence::Raft { log_dir } => Self::Raft(RaftWal::start_in_cluster(
                log_dir,
                tuning,
                topology,
                ursula_proto::placement::NodeId::new(node),
                cluster_id,
                fence.clone(),
            )?),
        })
    }

    fn raft_wal(&self) -> Option<RaftWal> {
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
    let snapshot_store = snapshot_store_from_config(
        &config.storage.snapshot,
        &config.storage.cold,
        config.raft.node_id,
    )
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
        topology.uses_snapshot_store(snapshot_store.is_some()),
    );
    let registry = RaftGroupHandleRegistry::default()
        .with_append_transport_budget_bytes(config.raft.append_transport_budget_bytes)
        .with_snapshot_install(
            config.raft.snapshot_install_max_concurrency,
            config.raft.install_snapshot_timeout.as_duration(),
        );

    // The node fence: a disk lease over the WAL root for a node that runs
    // Raft, which its WAL keeps, its transports and leader predicate read,
    // and its object stores check before each PUT and DELETE.
    let fence = match &persistence {
        Persistence::Raft { .. } => {
            ursula_runtime::NodeFence::leased(config.raft.wal.lease.as_duration())
        }
        Persistence::InMemory => ursula_runtime::NodeFence::unleased(),
    };
    let cold_store = if config.storage.cold.backend != ColdBackend::None {
        Some(Arc::new(
            ColdStore::try_new(&config.storage.cold)
                .map_err(|err| RuntimeError::ColdStoreConfig {
                    message: err.to_string(),
                })?
                .with_fence(fence.clone()),
        ))
    } else {
        None
    };

    // Built before the runtime starts, so a failure leaves nothing running.
    let participation = match &topology {
        Topology::StaticCluster { node_id, peers, .. } => Some(PeerParticipation::new(
            *node_id,
            peers,
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
        )),
        Topology::SingleNode { .. } => None,
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
        config.raft.node_id,
        config.server.cluster_id.as_ref(),
        &fence,
    )?;
    // Before the runtime starts, so `raft-log/meta/` exists before any data
    // group of the run opens and the peer identity is bound before any
    // group dials or answers a peer.
    let meta = match &storage {
        GroupStorage::Raft(wal) => Some(
            meta::open_meta(
                config,
                &topology,
                wal,
                &registry,
                &shard_map,
                engine_config.clone(),
                ursula_raft::meta::replica::MetaSnapshotTrigger {
                    interval: snapshot_drive_interval,
                    max_dirty_age: snapshot::snapshot_cadence(
                        &config.raft,
                        topology.raft_group_count(),
                    )
                    .removal_floor,
                },
                participation.clone(),
            )
            .map_err(RuntimeError::from)?,
        ),
        GroupStorage::InMemory => None,
    };
    let spawned = spawn_runtime_core(
        runtime_config,
        cold_store,
        storage,
        &topology,
        snapshot_store.clone(),
        engine_config,
        Some(registry),
    )?;

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

    // A single node that runs Raft drives its snapshots as a cluster node
    // does (#669), with inline snapshots and no write gate.
    if let (Topology::SingleNode { .. }, Some(registry), Some(wal)) =
        (&topology, &spawned.raft_registry, &spawned.raft_wal)
    {
        snapshot::spawn_snapshot_driver(
            &spawned.runtime,
            registry,
            None,
            snapshot::LogWriteGate::Open,
            snapshot_drive_interval,
            snapshot::snapshot_cadence(&config.raft, topology.raft_group_count()),
            Some(wal.maintenance()),
            config.raft.snapshot_pressure_max_groups_per_tick,
        );
    }

    if let (Topology::StaticCluster { peers, seeds, .. }, Some(participation)) =
        (&topology, &participation)
    {
        let registry = spawned
            .raft_registry
            .clone()
            .expect("static cluster has registry");
        snapshot::spawn_snapshot_driver(
            &spawned.runtime,
            &registry,
            snapshot_store.map(|store| {
                (store, StoreGateInputs {
                    s3: config.storage.cold.s3.as_ref(),
                    cold_flush_interval: config.storage.cold.flush_interval.as_duration(),
                    participation,
                })
            }),
            snapshot::LogWriteGate::RefuseAtHardLimit,
            snapshot_drive_interval,
            snapshot::snapshot_cadence(&config.raft, topology.raft_group_count()),
            spawned
                .raft_wal
                .as_ref()
                .map(ursula_raft::RaftWal::maintenance),
            config.raft.snapshot_pressure_max_groups_per_tick,
        );
        leadership::spawn_leadership_balancer(
            &registry,
            participation,
            &config.governance.leadership_balance,
        );
        egress::spawn_egress_gate(
            &registry,
            participation,
            peers,
            seeds.voter_table(),
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
                participation,
                &config.governance.cold_health,
            );
        }
    }

    Ok(SpawnedRuntime { meta, ..spawned })
}

/// Core runtime construction — maps persistence + topology to the correct
/// engine factory and spawns the runtime.  No background workers started here.
fn spawn_runtime_core(
    runtime_config: RuntimeConfig,
    cold_store: Option<ColdStoreHandle>,
    storage: GroupStorage,
    topology: &Topology,
    snapshot_store: Option<SharedSnapshotStore>,
    raft_engine_config: RaftEngineConfig,
    registry: Option<RaftGroupHandleRegistry>,
) -> Result<SpawnedRuntime, RuntimeError> {
    match topology {
        Topology::SingleNode { .. } => spawn_singleton(
            runtime_config,
            cold_store,
            storage,
            raft_engine_config,
            registry.unwrap_or_default(),
        ),
        // The node's peers reach the factory bound with its identity
        // (`open_meta`), before the runtime starts.
        Topology::StaticCluster { node_id, seeds, .. } => spawn_static_cluster(
            runtime_config,
            cold_store,
            storage,
            *node_id,
            seeds.clone(),
            snapshot_store,
            raft_engine_config,
            registry.unwrap_or_default(),
        ),
    }
}

/// A single node: the in-memory engine, or single-node Raft groups that
/// publish themselves in `registry` and run with `raft_engine_config`'s
/// snapshot settings (#669).
fn spawn_singleton(
    runtime_config: RuntimeConfig,
    cold_store: Option<ColdStoreHandle>,
    storage: GroupStorage,
    raft_engine_config: RaftEngineConfig,
    registry: RaftGroupHandleRegistry,
) -> Result<SpawnedRuntime, RuntimeError> {
    let raft_wal = storage.raft_wal();
    let mut raft_registry = None;
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
            raft_registry = Some(registry.clone());
            let factory = ursula_raft::DurableRaftGroupEngineFactory::with_cold_store(
                log_stores,
                cold_store.clone(),
            )
            .with_registry(registry)
            .with_engine_config(raft_engine_config);
            ShardRuntime::spawn_with_engine_factory_and_cold_store(
                runtime_config,
                factory,
                cold_store,
            )?
        }
    };
    Ok(SpawnedRuntime {
        runtime,
        raft_registry,
        raft_wal,
        meta: None,
    })
}

fn spawn_static_cluster(
    runtime_config: RuntimeConfig,
    cold_store: Option<ColdStoreHandle>,
    storage: GroupStorage,
    node_id: u64,
    seeds: ConfigSeeds,
    snapshot_store: Option<SharedSnapshotStore>,
    raft_engine_config: RaftEngineConfig,
    registry: RaftGroupHandleRegistry,
) -> Result<SpawnedRuntime, RuntimeError> {
    let GroupStorage::Raft(log_stores) = storage else {
        return Err(RuntimeError::StaticMembershipConfig {
            message: "static cluster topology requires Raft persistence".to_owned(),
        });
    };
    let raft_wal = Some(log_stores.clone());
    let factory = ursula_raft::PlacedRaftGroupEngineFactory::new(
        log_stores,
        registry.clone(),
        Arc::new(seeds.for_node(node_id)),
    )
    .with_cold_store(cold_store.clone())
    .with_snapshot_store(snapshot_store)
    .with_engine_config(raft_engine_config);
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        runtime_config,
        factory,
        cold_store,
    )?;
    Ok(SpawnedRuntime {
        runtime,
        raft_registry: Some(registry),
        raft_wal,
        meta: None,
    })
}
