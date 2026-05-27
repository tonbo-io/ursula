//! Process-level orchestration: env-driven `ShardRuntime` constructors and
//! the cold-flush background worker.

use std::path::PathBuf;
use std::time::Duration;

use ursula_raft::{
    ColdRaftGroupEngineFactory, DurableRaftGroupEngineFactory, RaftGroupEngineFactory,
    RaftGroupHandleRegistry, StaticGrpcRaftGroupEngineFactory,
};
use ursula_runtime::{
    ColdStore, InMemoryGroupEngineFactory, PlanGroupColdFlushRequest, RuntimeConfig, RuntimeError,
    ShardRuntime, SharedSnapshotStore, WalGroupEngineFactory, snapshot_store_from_env,
};
use ursula_shard::RaftGroupId;

pub fn spawn_default_runtime(
    core_count: usize,
    raft_group_count: usize,
) -> Result<ShardRuntime, RuntimeError> {
    let cold_store = cold_store_from_env()?;
    let config = runtime_config_from_env(core_count, raft_group_count, cold_store.is_some());
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        config,
        InMemoryGroupEngineFactory::with_cold_store(cold_store.clone()),
        cold_store,
    )?;
    spawn_cold_flush_worker_if_configured(&runtime);
    Ok(runtime)
}

pub fn spawn_wal_runtime(
    core_count: usize,
    raft_group_count: usize,
    wal_dir: impl Into<PathBuf>,
) -> Result<ShardRuntime, RuntimeError> {
    let cold_store = cold_store_from_env()?;
    let config = runtime_config_from_env(core_count, raft_group_count, cold_store.is_some());
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        config,
        WalGroupEngineFactory::with_cold_store(wal_dir, cold_store.clone()),
        cold_store,
    )?;
    spawn_cold_flush_worker_if_configured(&runtime);
    Ok(runtime)
}

pub fn spawn_raft_memory_runtime(
    core_count: usize,
    raft_group_count: usize,
) -> Result<ShardRuntime, RuntimeError> {
    let cold_store = cold_store_from_env()?;
    let config = runtime_config_from_env(core_count, raft_group_count, cold_store.is_some());
    let runtime = match cold_store {
        Some(cold_store) => ShardRuntime::spawn_with_engine_factory_and_cold_store(
            config,
            ColdRaftGroupEngineFactory::new(cold_store.clone()),
            Some(cold_store),
        ),
        None => ShardRuntime::spawn_with_engine_factory(config, RaftGroupEngineFactory),
    }?;
    spawn_cold_flush_worker_if_configured(&runtime);
    Ok(runtime)
}

pub fn spawn_static_grpc_raft_memory_runtime(
    core_count: usize,
    raft_group_count: usize,
    node_id: u64,
    peers: impl IntoIterator<Item = (u64, String)>,
    initialize_membership: bool,
) -> Result<(ShardRuntime, RaftGroupHandleRegistry), RuntimeError> {
    let cold_store = cold_store_from_env()?;
    let snapshot_store = snapshot_store_from_env_or_error()?;
    let config = runtime_config_from_env(core_count, raft_group_count, cold_store.is_some());
    let registry = RaftGroupHandleRegistry::default();
    let factory = StaticGrpcRaftGroupEngineFactory::new(
        node_id,
        peers,
        initialize_membership,
        registry.clone(),
    )
    .with_cold_store(cold_store.clone())
    .with_snapshot_store(snapshot_store);
    let runtime =
        ShardRuntime::spawn_with_engine_factory_and_cold_store(config, factory, cold_store)?;
    spawn_cold_flush_worker_if_configured(&runtime);
    spawn_snapshot_driver_if_configured(&runtime, &registry);
    Ok((runtime, registry))
}

pub fn spawn_static_grpc_raft_memory_runtime_with_per_group_initializers(
    core_count: usize,
    raft_group_count: usize,
    node_id: u64,
    peers: impl IntoIterator<Item = (u64, String)>,
    initialize_membership: bool,
) -> Result<(ShardRuntime, RaftGroupHandleRegistry), RuntimeError> {
    let cold_store = cold_store_from_env()?;
    let snapshot_store = snapshot_store_from_env_or_error()?;
    let config = runtime_config_from_env(core_count, raft_group_count, cold_store.is_some());
    let registry = RaftGroupHandleRegistry::default();
    let factory = StaticGrpcRaftGroupEngineFactory::new(
        node_id,
        peers,
        initialize_membership,
        registry.clone(),
    )
    .with_per_group_membership_initializers(true)
    .with_cold_store(cold_store.clone())
    .with_snapshot_store(snapshot_store);
    let runtime =
        ShardRuntime::spawn_with_engine_factory_and_cold_store(config, factory, cold_store)?;
    spawn_cold_flush_worker_if_configured(&runtime);
    spawn_snapshot_driver_if_configured(&runtime, &registry);
    Ok((runtime, registry))
}

pub fn spawn_static_grpc_raft_runtime(
    core_count: usize,
    raft_group_count: usize,
    node_id: u64,
    peers: impl IntoIterator<Item = (u64, String)>,
    initialize_membership: bool,
    raft_log_dir: impl Into<PathBuf>,
) -> Result<(ShardRuntime, RaftGroupHandleRegistry), RuntimeError> {
    let cold_store = cold_store_from_env()?;
    let snapshot_store = snapshot_store_from_env_or_error()?;
    let config = runtime_config_from_env(core_count, raft_group_count, cold_store.is_some());
    let registry = RaftGroupHandleRegistry::default();
    let factory = StaticGrpcRaftGroupEngineFactory::new(
        node_id,
        peers,
        initialize_membership,
        registry.clone(),
    )
    .with_cold_store(cold_store.clone())
    .with_raft_log_dir(raft_log_dir)
    .with_snapshot_store(snapshot_store);
    let runtime =
        ShardRuntime::spawn_with_engine_factory_and_cold_store(config, factory, cold_store)?;
    spawn_cold_flush_worker_if_configured(&runtime);
    spawn_snapshot_driver_if_configured(&runtime, &registry);
    Ok((runtime, registry))
}

pub fn spawn_static_grpc_raft_runtime_with_per_group_initializers(
    core_count: usize,
    raft_group_count: usize,
    node_id: u64,
    peers: impl IntoIterator<Item = (u64, String)>,
    initialize_membership: bool,
    raft_log_dir: impl Into<PathBuf>,
) -> Result<(ShardRuntime, RaftGroupHandleRegistry), RuntimeError> {
    let cold_store = cold_store_from_env()?;
    let snapshot_store = snapshot_store_from_env_or_error()?;
    let config = runtime_config_from_env(core_count, raft_group_count, cold_store.is_some());
    let registry = RaftGroupHandleRegistry::default();
    let factory = StaticGrpcRaftGroupEngineFactory::new(
        node_id,
        peers,
        initialize_membership,
        registry.clone(),
    )
    .with_per_group_membership_initializers(true)
    .with_cold_store(cold_store.clone())
    .with_raft_log_dir(raft_log_dir)
    .with_snapshot_store(snapshot_store);
    let runtime =
        ShardRuntime::spawn_with_engine_factory_and_cold_store(config, factory, cold_store)?;
    spawn_cold_flush_worker_if_configured(&runtime);
    spawn_snapshot_driver_if_configured(&runtime, &registry);
    Ok((runtime, registry))
}

pub fn spawn_raft_runtime(
    core_count: usize,
    raft_group_count: usize,
    raft_log_dir: impl Into<PathBuf>,
) -> Result<ShardRuntime, RuntimeError> {
    let cold_store = cold_store_from_env()?;
    let config = runtime_config_from_env(core_count, raft_group_count, cold_store.is_some());
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        config,
        DurableRaftGroupEngineFactory::with_cold_store(raft_log_dir, cold_store.clone()),
        cold_store,
    )?;
    spawn_cold_flush_worker_if_configured(&runtime);
    Ok(runtime)
}

fn snapshot_store_from_env_or_error() -> Result<Option<SharedSnapshotStore>, RuntimeError> {
    snapshot_store_from_env().map_err(|err| RuntimeError::ColdStoreConfig {
        message: err.to_string(),
    })
}

fn cold_store_from_env() -> Result<Option<ursula_runtime::ColdStoreHandle>, RuntimeError> {
    ColdStore::from_env().map_err(|err| RuntimeError::ColdStoreConfig {
        message: err.to_string(),
    })
}

fn runtime_config_from_env(
    core_count: usize,
    raft_group_count: usize,
    cold_store_configured: bool,
) -> RuntimeConfig {
    let mut config = RuntimeConfig::new(core_count, raft_group_count);
    let live_read_max_waiters = env_usize("URSULA_LIVE_READ_MAX_WAITERS_PER_CORE", 65_536);
    config = config.with_live_read_max_waiters_per_core(if live_read_max_waiters == 0 {
        None
    } else {
        Some(u64::try_from(live_read_max_waiters).unwrap_or(u64::MAX))
    });
    if cold_store_configured {
        let max_hot_bytes = env_usize("URSULA_COLD_MAX_HOT_BYTES_PER_GROUP", 64 * 1024 * 1024);
        if max_hot_bytes > 0 {
            config = config.with_cold_max_hot_bytes_per_group(Some(
                u64::try_from(max_hot_bytes).unwrap_or(u64::MAX),
            ));
        }
    }
    if let Some(raft_max_uncommitted) =
        env_optional_usize("URSULA_RAFT_MAX_UNCOMMITTED_BYTES_PER_GROUP")
    {
        config = config.with_raft_max_uncommitted_bytes_per_group(if raft_max_uncommitted == 0 {
            None
        } else {
            Some(u64::try_from(raft_max_uncommitted).unwrap_or(u64::MAX))
        });
    }
    config
}

fn env_optional_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
}

pub fn spawn_cold_flush_worker_if_configured(runtime: &ShardRuntime) {
    if !runtime.has_cold_store() {
        return;
    }
    let interval_ms = env_usize("URSULA_COLD_FLUSH_INTERVAL_MS", 1_000);
    if interval_ms == 0 {
        return;
    }
    let min_hot_bytes = env_usize("URSULA_COLD_FLUSH_MIN_HOT_BYTES", 8 * 1024 * 1024);
    let max_flush_bytes = env_usize("URSULA_COLD_FLUSH_MAX_BYTES", 8 * 1024 * 1024);
    let max_concurrency = env_usize("URSULA_COLD_FLUSH_MAX_CONCURRENCY", 4).max(1);
    let runtime = runtime.clone();
    tokio::spawn(async move {
        let interval = Duration::from_millis(u64::try_from(interval_ms).unwrap_or(u64::MAX));
        loop {
            if let Err(err) = runtime
                .flush_cold_all_groups_once_bounded(
                    PlanGroupColdFlushRequest {
                        min_hot_bytes,
                        max_flush_bytes,
                    },
                    max_concurrency,
                )
                .await
            {
                eprintln!("cold flush worker error: {err}");
            }
            tokio::time::sleep(interval).await;
        }
    });
}

/// Drives raft snapshots manually after first draining each group's hot tail to
/// cold. The drain makes the resulting snapshot's `payload` field empty (no
/// uncommitted hot bytes), shrinking the manifest install_snapshot has to ship.
///
/// When `URSULA_SNAPSHOT_DRIVE_INTERVAL_MS` is unset or zero this is a no-op
/// and openraft's automatic [`SnapshotPolicy::LogsSinceLast`] still drives
/// snapshot timing.
pub fn spawn_snapshot_driver_if_configured(
    runtime: &ShardRuntime,
    registry: &RaftGroupHandleRegistry,
) {
    let interval_ms = env_usize("URSULA_SNAPSHOT_DRIVE_INTERVAL_MS", 0);
    if interval_ms == 0 {
        return;
    }
    let max_concurrency = env_usize("URSULA_SNAPSHOT_DRIVE_FLUSH_CONCURRENCY", 4).max(1);
    let runtime = runtime.clone();
    let registry = registry.clone();
    tokio::spawn(async move {
        let interval = Duration::from_millis(u64::try_from(interval_ms).unwrap_or(u64::MAX));
        loop {
            // Drain every group's hot tail to cold via the existing raft-
            // replicated flush path. `min_hot_bytes=1` makes "any hot bytes"
            // eligible; `max_flush_bytes` is left wide so a single tick can
            // catch up if the background worker is lagging.
            if runtime.has_cold_store()
                && let Err(err) = runtime
                    .flush_cold_all_groups_once_bounded(
                        PlanGroupColdFlushRequest {
                            min_hot_bytes: 1,
                            max_flush_bytes: 64 * 1024 * 1024,
                        },
                        max_concurrency,
                    )
                    .await
            {
                eprintln!("snapshot driver pre-flush error: {err}");
            }
            // Trigger snapshot per registered group; openraft will dedup if a
            // snapshot is already in flight and the call is otherwise cheap.
            for snapshot in registry.metrics_snapshot() {
                let group_id = RaftGroupId(snapshot.raft_group_id);
                let Some(raft) = registry.get(group_id) else {
                    continue;
                };
                if let Err(err) = raft.trigger().snapshot().await {
                    eprintln!(
                        "snapshot driver trigger group {} error: {err}",
                        snapshot.raft_group_id,
                    );
                }
            }
            tokio::time::sleep(interval).await;
        }
    });
}

pub(crate) fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(default)
}
