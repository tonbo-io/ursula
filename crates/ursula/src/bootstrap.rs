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
    ShardRuntime, SharedSnapshotStore, WalGroupEngineFactory, default_snapshot_store,
    snapshot_store_from_env,
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
    .with_snapshot_store(snapshot_store.clone());
    let runtime =
        ShardRuntime::spawn_with_engine_factory_and_cold_store(config, factory, cold_store)?;
    spawn_cold_flush_worker_if_configured(&runtime);
    spawn_snapshot_driver_if_configured(&runtime, &registry, snapshot_store);
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
    .with_snapshot_store(snapshot_store.clone());
    let runtime =
        ShardRuntime::spawn_with_engine_factory_and_cold_store(config, factory, cold_store)?;
    spawn_cold_flush_worker_if_configured(&runtime);
    spawn_snapshot_driver_if_configured(&runtime, &registry, snapshot_store);
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
    .with_snapshot_store(snapshot_store.clone());
    let runtime =
        ShardRuntime::spawn_with_engine_factory_and_cold_store(config, factory, cold_store)?;
    spawn_cold_flush_worker_if_configured(&runtime);
    spawn_snapshot_driver_if_configured(&runtime, &registry, snapshot_store);
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
    .with_snapshot_store(snapshot_store.clone());
    let runtime =
        ShardRuntime::spawn_with_engine_factory_and_cold_store(config, factory, cold_store)?;
    spawn_cold_flush_worker_if_configured(&runtime);
    spawn_snapshot_driver_if_configured(&runtime, &registry, snapshot_store);
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
    snapshot_store: Option<SharedSnapshotStore>,
) {
    let interval_ms = env_usize("URSULA_SNAPSHOT_DRIVE_INTERVAL_MS", 0);
    if interval_ms == 0 {
        return;
    }
    // Falls back to the inline (always-healthy) store when no external snapshot
    // backend is configured, so the S3-health probe is simply a no-op there.
    let snapshot_store = snapshot_store.unwrap_or_else(default_snapshot_store);
    let max_concurrency = env_usize("URSULA_SNAPSHOT_DRIVE_FLUSH_CONCURRENCY", 4).max(1);
    // The S3-health probe is a single cheap `stat`, bounded by this deadline so
    // a stalled S3 surfaces as "unhealthy" within one tick instead of dragging
    // on for the full TimeoutLayer+RetryLayer budget.
    let probe_timeout = Duration::from_millis(
        u64::try_from(env_usize("URSULA_S3_PROBE_TIMEOUT_MS", 2_000)).unwrap_or(2_000),
    );
    // Consecutive ticks where the S3-health probe fails before this node
    // declares its own S3 unhealthy and yields leadership.
    let unhealthy_ticks = env_usize("URSULA_S3_UNHEALTHY_TICKS", 1).max(1);
    let runtime = runtime.clone();
    let registry = registry.clone();
    tokio::spawn(async move {
        let interval = Duration::from_millis(u64::try_from(interval_ms).unwrap_or(u64::MAX));
        // S3-health-aware leadership yield: a node whose own S3 is unavailable
        // cannot flush cold or persist snapshots, so it must not keep leading
        // groups (it would reject every append on them while healthy peers sit
        // idle). On sustained local S3 failure it transfers leadership away and
        // disables its own elections; once its S3 recovers it re-enables
        // elections and rejoins/catches up normally (self-heal).
        let mut consecutive_bad = 0usize;
        let mut yielded = false;
        loop {
            // Cheap S3-health probe: a single `stat` round-trip. Crucially this
            // does NOT build a snapshot — a failed `build_snapshot` returns a
            // StorageError that openraft treats as fatal, killing the raft core
            // (after which leadership can no longer be yielded). Probing before
            // the snapshot triggers and the (possibly slow) cold flush keeps
            // leadership-yield detection fast.
            let snaps = registry.metrics_snapshot();
            let healthy = matches!(
                tokio::time::timeout(probe_timeout, snapshot_store.health_check()).await,
                Ok(Ok(()))
            );
            let bad_tick = !healthy;
            if bad_tick {
                consecutive_bad += 1;
            } else {
                consecutive_bad = 0;
            }

            if !yielded && consecutive_bad >= unhealthy_ticks {
                // Local S3 is unhealthy: stop campaigning everywhere and hand
                // off any group we currently lead to a healthy peer so the
                // cluster keeps serving appends on those groups.
                yielded = true;
                for snapshot in &snaps {
                    let Some(raft) = registry.get(RaftGroupId(snapshot.raft_group_id)) else {
                        continue;
                    };
                    raft.runtime_config().elect(false);
                    if snapshot.current_leader == Some(snapshot.node_id)
                        && let Some(target) = snapshot
                            .voter_ids
                            .iter()
                            .copied()
                            .find(|voter| *voter != snapshot.node_id)
                    {
                        match raft.trigger().transfer_leader(target).await {
                            Ok(()) => eprintln!(
                                "s3-unhealthy: node {} yielded leadership of group {} to node {}",
                                snapshot.node_id, snapshot.raft_group_id, target,
                            ),
                            Err(err) => eprintln!(
                                "s3-unhealthy: transfer_leader group {} -> {} failed: {err}",
                                snapshot.raft_group_id, target,
                            ),
                        }
                    }
                }
            } else if yielded && !bad_tick {
                // Local S3 recovered: re-enable elections so this node rejoins
                // normal raft participation and catches up (self-heal).
                yielded = false;
                for snapshot in &snaps {
                    if let Some(raft) = registry.get(RaftGroupId(snapshot.raft_group_id)) {
                        raft.runtime_config().elect(true);
                    }
                }
                eprintln!("s3-healthy: node re-enabled elections after S3 recovery");
            }

            // Drive snapshots (log compaction) only while S3 is healthy. With
            // SnapshotPolicy::Never these driver triggers are the only snapshots,
            // so skipping them during an outage keeps the raft core alive: the
            // in-memory log grows until S3 returns (bounded by the outage), then
            // the next healthy tick compacts it.
            if healthy {
                let mut triggers = tokio::task::JoinSet::new();
                for snapshot in &snaps {
                    let Some(raft) = registry.get(RaftGroupId(snapshot.raft_group_id)) else {
                        continue;
                    };
                    let gid = snapshot.raft_group_id;
                    triggers.spawn(async move {
                        if let Err(err) = raft.trigger().snapshot().await {
                            eprintln!("snapshot driver trigger group {gid} error: {err}");
                        }
                    });
                }
                while triggers.join_next().await.is_some() {}
            }

            // Drain every group's hot tail to cold AFTER the health decision so
            // a slow/stalled flush never delays the leadership yield above.
            // `min_hot_bytes=1` makes any hot bytes eligible; `max_flush_bytes`
            // is left wide so a single tick can catch up a lagging worker.
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
                eprintln!("snapshot driver flush error: {err}");
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
