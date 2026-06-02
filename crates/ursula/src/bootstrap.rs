//! Process-level orchestration: env-driven `ShardRuntime` constructors and
//! the cold-flush background worker.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;
// `tokio::time::Instant` (not `std::time::Instant`) so the M3 commit-stall
// timer behaves deterministically under madsim, which shims tokio's clock but
// can't shim std's. The non-test code path is unchanged: outside madsim it's
// just std's monotonic clock under tokio's facade.
use tokio::time::Instant;

use ursula_raft::{
    ColdRaftGroupEngineFactory, DurableRaftGroupEngineFactory, LeadershipShedReason,
    RaftGroupEngineFactory, RaftGroupHandleRegistry, RaftGroupMetricsSnapshot,
    StaticGrpcRaftGroupEngineFactory,
};
use ursula_runtime::{
    ColdStore, InMemoryGroupEngineFactory, PlanGroupColdFlushRequest, RuntimeConfig, RuntimeError,
    ShardRuntime, SharedSnapshotStore, WalGroupEngineFactory, default_snapshot_store,
    snapshot_store_from_env,
};
use ursula_shard::RaftGroupId;

#[derive(Debug, Clone, Default)]
pub struct StaticGrpcRaftMembershipConfig {
    pub initialize_membership_per_group: bool,
    pub per_group_voters: BTreeMap<RaftGroupId, BTreeSet<u64>>,
}

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
    spawn_cold_gc_worker_if_configured(&runtime);
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
    spawn_cold_gc_worker_if_configured(&runtime);
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
    spawn_cold_gc_worker_if_configured(&runtime);
    Ok(runtime)
}

pub fn spawn_static_grpc_raft_memory_runtime(
    core_count: usize,
    raft_group_count: usize,
    node_id: u64,
    peers: impl IntoIterator<Item = (u64, String)>,
    initialize_membership: bool,
) -> Result<(ShardRuntime, RaftGroupHandleRegistry), RuntimeError> {
    spawn_static_grpc_raft_memory_runtime_with_membership_config(
        core_count,
        raft_group_count,
        node_id,
        peers,
        initialize_membership,
        StaticGrpcRaftMembershipConfig::default(),
    )
}

pub fn spawn_static_grpc_raft_memory_runtime_with_membership_config(
    core_count: usize,
    raft_group_count: usize,
    node_id: u64,
    peers: impl IntoIterator<Item = (u64, String)>,
    initialize_membership: bool,
    membership_config: StaticGrpcRaftMembershipConfig,
) -> Result<(ShardRuntime, RaftGroupHandleRegistry), RuntimeError> {
    let cold_store = cold_store_from_env()?;
    let snapshot_store = snapshot_store_from_env_or_error()?;
    let config = runtime_config_from_env(core_count, raft_group_count, cold_store.is_some());
    let peers: Vec<(u64, String)> = peers.into_iter().collect();
    let registry = RaftGroupHandleRegistry::default();
    let per_group_voters = membership_config.per_group_voters.clone();
    let factory = StaticGrpcRaftGroupEngineFactory::new(
        node_id,
        peers.clone(),
        initialize_membership,
        registry.clone(),
    )
    .with_per_group_membership_initializers(membership_config.initialize_membership_per_group)
    .with_per_group_voters(membership_config.per_group_voters)
    .with_cold_store(cold_store.clone())
    .with_snapshot_store(snapshot_store.clone());
    let runtime =
        ShardRuntime::spawn_with_engine_factory_and_cold_store(config, factory, cold_store)?;
    spawn_cold_flush_worker_if_configured(&runtime);
    spawn_cold_gc_worker_if_configured(&runtime);
    spawn_snapshot_driver_if_configured(&runtime, &registry, snapshot_store);
    spawn_leadership_balancer_if_configured(&registry, node_id, &peers);
    spawn_cluster_egress_gate_if_configured(&registry, node_id, &peers, per_group_voters);
    spawn_commit_stall_watchdog_if_configured(&registry);
    spawn_cold_health_gate_if_configured(&runtime, &registry, node_id);
    Ok((runtime, registry))
}

pub fn spawn_static_grpc_raft_memory_runtime_with_per_group_initializers(
    core_count: usize,
    raft_group_count: usize,
    node_id: u64,
    peers: impl IntoIterator<Item = (u64, String)>,
    initialize_membership: bool,
) -> Result<(ShardRuntime, RaftGroupHandleRegistry), RuntimeError> {
    spawn_static_grpc_raft_memory_runtime_with_membership_config(
        core_count,
        raft_group_count,
        node_id,
        peers,
        initialize_membership,
        StaticGrpcRaftMembershipConfig {
            initialize_membership_per_group: true,
            per_group_voters: BTreeMap::new(),
        },
    )
}

pub fn spawn_static_grpc_raft_runtime(
    core_count: usize,
    raft_group_count: usize,
    node_id: u64,
    peers: impl IntoIterator<Item = (u64, String)>,
    initialize_membership: bool,
    raft_log_dir: impl Into<PathBuf>,
) -> Result<(ShardRuntime, RaftGroupHandleRegistry), RuntimeError> {
    spawn_static_grpc_raft_runtime_with_membership_config(
        core_count,
        raft_group_count,
        node_id,
        peers,
        initialize_membership,
        StaticGrpcRaftMembershipConfig::default(),
        raft_log_dir,
    )
}

pub fn spawn_static_grpc_raft_runtime_with_membership_config(
    core_count: usize,
    raft_group_count: usize,
    node_id: u64,
    peers: impl IntoIterator<Item = (u64, String)>,
    initialize_membership: bool,
    membership_config: StaticGrpcRaftMembershipConfig,
    raft_log_dir: impl Into<PathBuf>,
) -> Result<(ShardRuntime, RaftGroupHandleRegistry), RuntimeError> {
    let cold_store = cold_store_from_env()?;
    let snapshot_store = snapshot_store_from_env_or_error()?;
    let config = runtime_config_from_env(core_count, raft_group_count, cold_store.is_some());
    let peers: Vec<(u64, String)> = peers.into_iter().collect();
    let registry = RaftGroupHandleRegistry::default();
    let per_group_voters = membership_config.per_group_voters.clone();
    let factory = StaticGrpcRaftGroupEngineFactory::new(
        node_id,
        peers.clone(),
        initialize_membership,
        registry.clone(),
    )
    .with_per_group_membership_initializers(membership_config.initialize_membership_per_group)
    .with_per_group_voters(membership_config.per_group_voters)
    .with_cold_store(cold_store.clone())
    .with_raft_log_dir(raft_log_dir)
    .with_snapshot_store(snapshot_store.clone());
    let runtime =
        ShardRuntime::spawn_with_engine_factory_and_cold_store(config, factory, cold_store)?;
    spawn_cold_flush_worker_if_configured(&runtime);
    spawn_cold_gc_worker_if_configured(&runtime);
    spawn_snapshot_driver_if_configured(&runtime, &registry, snapshot_store);
    spawn_leadership_balancer_if_configured(&registry, node_id, &peers);
    spawn_cluster_egress_gate_if_configured(&registry, node_id, &peers, per_group_voters);
    spawn_commit_stall_watchdog_if_configured(&registry);
    spawn_cold_health_gate_if_configured(&runtime, &registry, node_id);
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
    spawn_static_grpc_raft_runtime_with_membership_config(
        core_count,
        raft_group_count,
        node_id,
        peers,
        initialize_membership,
        StaticGrpcRaftMembershipConfig {
            initialize_membership_per_group: true,
            per_group_voters: BTreeMap::new(),
        },
        raft_log_dir,
    )
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
    spawn_cold_gc_worker_if_configured(&runtime);
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
                tracing::error!("cold flush worker error: {err}");
            }
            tokio::time::sleep(interval).await;
        }
    });
}

/// Drains the cold-GC queue on each tick: the leader of every group physically
/// reclaims the cold objects of deleted/expired streams and replicates an ack
/// that pops them. A no-op when no cold store is configured or the interval is
/// zero.
pub fn spawn_cold_gc_worker_if_configured(runtime: &ShardRuntime) {
    if !runtime.has_cold_store() {
        return;
    }
    let interval_ms = env_usize("URSULA_COLD_GC_INTERVAL_MS", 5_000);
    if interval_ms == 0 {
        return;
    }
    let max_entries = env_usize("URSULA_COLD_GC_MAX_ENTRIES_PER_GROUP", 256).max(1);
    let runtime = runtime.clone();
    tokio::spawn(async move {
        let interval = Duration::from_millis(u64::try_from(interval_ms).unwrap_or(u64::MAX));
        loop {
            if let Err(err) = runtime.run_cold_gc_all_groups_once(max_entries).await {
                tracing::error!("cold gc worker error: {err}");
            }
            tokio::time::sleep(interval).await;
        }
    });
}

fn set_registered_group_elections(registry: &RaftGroupHandleRegistry, enabled: bool) {
    for snapshot in registry.metrics_snapshot() {
        if let Some(raft) = registry.get(RaftGroupId(snapshot.raft_group_id)) {
            raft.runtime_config().elect(enabled);
        }
    }
}

fn reenable_elections_if_campaign_allowed(registry: &RaftGroupHandleRegistry, context: &str) {
    let shed_state = registry.leadership_shed_state();
    if shed_state.should_campaign() {
        set_registered_group_elections(registry, true);
        tracing::warn!("{context}; re-enabling elections");
    } else {
        tracing::warn!(
            "{context}; elections remain disabled while leadership-shed state={shed_state}"
        );
    }
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
    // Consecutive healthy ticks required before this reason clears. Elections
    // are only re-enabled if no other shed reason remains set. Hysteresis
    // matters because the cold-flush failure signal disappears once a node
    // yields (a follower does not flush), so recovery is judged by the probe
    // alone — a short hold-down avoids re-grabbing leadership only to fail the
    // next flush and yield again (flapping).
    let heal_ticks = env_usize("URSULA_S3_HEAL_TICKS", 2).max(1);
    let runtime = runtime.clone();
    let registry = registry.clone();
    tokio::spawn(async move {
        let interval = Duration::from_millis(u64::try_from(interval_ms).unwrap_or(u64::MAX));
        // S3-health-aware leadership yield: a node whose own S3 is unavailable
        // cannot flush cold or persist snapshots, so it must not keep leading
        // groups (it would reject every append on them while healthy peers sit
        // idle). On sustained local S3 failure it transfers leadership away and
        // disables its own elections; once its S3 recovers it clears this shed
        // reason and rejoins only when no other reason still blocks campaigns.
        let mut consecutive_bad = 0usize;
        let mut consecutive_good = 0usize;
        let mut yielded = false;
        // Baseline the cold-flush error counter so the first tick measures a
        // delta, not the run's cumulative total.
        let mut last_flush_errors = runtime.metrics().snapshot().cold_flush_write_errors;
        loop {
            // Cheap S3-health probe: a single `stat` round-trip. Crucially this
            // does NOT build a snapshot — a failed `build_snapshot` returns a
            // StorageError that openraft treats as fatal, killing the raft core
            // (after which leadership can no longer be yielded). Probing before
            // the snapshot triggers and the (possibly slow) cold flush keeps
            // leadership-yield detection fast.
            let snaps = registry.metrics_snapshot();
            let probe_healthy = matches!(
                tokio::time::timeout(probe_timeout, snapshot_store.health_check()).await,
                Ok(Ok(()))
            );
            // Primary signal: did a real cold-flush S3 write fail since the last
            // tick? This detects "this node can't write to S3" directly, which a
            // keep-alive `stat` probe can mask (an idle pooled connection answers
            // the probe while concurrent flushes that open new connections fail).
            let flush_errors_now = runtime.metrics().snapshot().cold_flush_write_errors;
            let flush_failing = flush_errors_now > last_flush_errors;
            last_flush_errors = flush_errors_now;
            let bad_tick = !probe_healthy || flush_failing;
            if bad_tick {
                consecutive_bad += 1;
                consecutive_good = 0;
            } else {
                consecutive_bad = 0;
                consecutive_good += 1;
            }

            if !yielded && consecutive_bad >= unhealthy_ticks {
                // Local S3 is unhealthy: stop campaigning everywhere and hand
                // off any group we currently lead to a healthy peer so the
                // cluster keeps serving appends on those groups.
                yielded = true;
                registry.mark_leadership_shed(LeadershipShedReason::SnapshotDriverS3);
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
                            Ok(()) => tracing::warn!(
                                "s3-unhealthy: node {} yielded leadership of group {} to node {}",
                                snapshot.node_id,
                                snapshot.raft_group_id,
                                target,
                            ),
                            Err(err) => tracing::error!(
                                "s3-unhealthy: transfer_leader group {} -> {} failed: {err}",
                                snapshot.raft_group_id,
                                target,
                            ),
                        }
                    }
                }
            } else if yielded && consecutive_good >= heal_ticks {
                // Local S3 recovered (sustained healthy ticks): clear this
                // reason and re-enable elections only if no other shed reason
                // still requires this node to stay out of campaigns.
                yielded = false;
                registry.clear_leadership_shed(LeadershipShedReason::SnapshotDriverS3);
                reenable_elections_if_campaign_allowed(&registry, "s3-healthy: node S3 recovered");
            }

            // Drive snapshots (log compaction) only while fully healthy (probe
            // ok AND cold flush succeeding). With SnapshotPolicy::Never these
            // driver triggers are the only snapshots, so skipping them during an
            // outage keeps the raft core alive: the in-memory log grows until S3
            // returns (bounded by the outage), then the next healthy tick
            // compacts it. Gating on `!bad_tick` (not just the probe) avoids
            // triggering a snapshot build against an S3 the probe only thinks is
            // reachable.
            if !bad_tick {
                let mut triggers = tokio::task::JoinSet::new();
                for snapshot in &snaps {
                    if !should_drive_snapshot_for_group(snapshot) {
                        continue;
                    }
                    let Some(raft) = registry.get(RaftGroupId(snapshot.raft_group_id)) else {
                        continue;
                    };
                    let gid = snapshot.raft_group_id;
                    triggers.spawn(async move {
                        if let Err(err) = raft.trigger().snapshot().await {
                            tracing::error!("snapshot driver trigger group {gid} error: {err}");
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
                tracing::error!("snapshot driver flush error: {err}");
            }

            tokio::time::sleep(interval).await;
        }
    });
}

pub(crate) fn should_drive_snapshot_for_group(snapshot: &RaftGroupMetricsSnapshot) -> bool {
    // An empty in-memory raft node has no applied state yet; triggering a
    // manual snapshot there publishes a `group-X-empty` object that cannot be a
    // valid recovery source for an existing group.
    snapshot.last_applied.is_some()
}

/// M1 — leadership balancing. Spreads raft-group leadership evenly across
/// nodes so a single node's failure or network impairment can stall at most
/// its `ceil(groups / nodes)` share rather than the whole cluster (leadership
/// concentration is what turns one impaired node into a cluster-wide ops drop).
///
/// Each node runs this independently and only moves groups it currently leads;
/// `current_leader` is globally agreed, so every node computes the same
/// distribution and converges. One transfer per tick keeps convergence gentle
/// and avoids thundering-herd handoffs. Before planning a handoff, M1 asks
/// peers for their leadership-shed policy state and only targets nodes whose
/// policy says they may campaign. That prevents balancing into a hard-yielded
/// peer that cannot safely lead. Cold-health remains campaign-eligible because
/// it is a cluster-wide pressure signal under backlog: excluding every hot
/// peer can deadlock leadership movement. `transfer_leader` still brings
/// lagging eligible targets up to date before handing off.
pub fn spawn_leadership_balancer_if_configured(
    registry: &RaftGroupHandleRegistry,
    node_id: u64,
    peers: &[(u64, String)],
) {
    // 5s default is fast enough to converge a 6-group cluster after a sudden
    // leader-concentration event (e.g. one node dies, all its leaderships
    // land on whoever wins the elections first) within a couple of ticks.
    // 15s left a 30s+ window of single-node overload that masked itself as
    // ColdBackpressure storms in the chaos test.
    let interval_ms = env_usize("URSULA_LEADERSHIP_BALANCE_MS", 5_000);
    if interval_ms == 0 {
        return;
    }
    // Cap transfers per tick to bound thundering-herd risk while still
    // letting a deeply skewed cluster heal in one or two ticks. None=unbounded.
    let max_per_tick = env_usize("URSULA_LEADERSHIP_BALANCE_MAX_PER_TICK", 4);
    let peer_timeout_ms = env_usize("URSULA_LEADERSHIP_BALANCE_PEER_TIMEOUT_MS", 500);
    let registry = registry.clone();
    let peers: Vec<(u64, String)> = peers.to_vec();
    tokio::spawn(async move {
        let interval = Duration::from_millis(u64::try_from(interval_ms).unwrap_or(5_000));
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_millis(
                u64::try_from(peer_timeout_ms).unwrap_or(500),
            ))
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                tracing::error!("leadership-balance: failed to build peer-status client: {err}");
                return;
            }
        };
        loop {
            tokio::time::sleep(interval).await;
            let snaps = registry.metrics_snapshot();
            if snaps.is_empty() {
                continue;
            }
            let my_id = snaps[0].node_id;
            let eligible_nodes =
                leadership_balance_eligible_nodes(&registry, node_id, &peers, &client).await;
            let actions = plan_leadership_balance_with_eligible_nodes(
                &snaps,
                my_id,
                max_per_tick,
                &eligible_nodes,
            );
            for action in actions {
                let Some(raft) = registry.get(RaftGroupId(action.group_id)) else {
                    continue;
                };
                match raft.trigger().transfer_leader(action.target).await {
                    Ok(()) => tracing::warn!(
                        "leadership-balance: node {my_id} handing group {} -> node {} (fair={})",
                        action.group_id,
                        action.target,
                        action.fair
                    ),
                    Err(err) => tracing::error!(
                        "leadership-balance: transfer_leader group {} -> {} failed: {err}",
                        action.group_id,
                        action.target
                    ),
                }
            }
        }
    });
}

/// One planned leader handoff for the M1 balancer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LeadershipBalanceAction {
    pub group_id: u32,
    pub target: u64,
    pub fair: usize,
}

#[derive(Debug, serde::Deserialize)]
struct LeadershipShedPeerStatus {
    should_campaign: bool,
}

async fn leadership_balance_eligible_nodes(
    registry: &RaftGroupHandleRegistry,
    node_id: u64,
    peers: &[(u64, String)],
    client: &reqwest::Client,
) -> HashSet<u64> {
    let mut eligible = HashSet::new();
    if registry.leadership_shed_state().should_campaign() {
        eligible.insert(node_id);
    }

    for (peer_id, peer_url) in peers {
        if *peer_id == node_id {
            continue;
        }
        let url = format!("{peer_url}{}", crate::LEADERSHIP_SHED_PATH);
        let Ok(response) = client.get(url).send().await else {
            continue;
        };
        if !response.status().is_success() {
            continue;
        }
        let Ok(body) = response.text().await else {
            continue;
        };
        let Ok(status) = serde_json::from_str::<LeadershipShedPeerStatus>(&body) else {
            continue;
        };
        if status.should_campaign {
            eligible.insert(*peer_id);
        }
    }

    eligible
}

/// Plan the leader handoffs this tick should attempt. Pure function — split
/// out so it tests against synthetic snapshots without spawning a task.
///
/// Each node runs this independently using the cluster-wide leader map
/// (`current_leader` is globally agreed), so every node computes the same
/// distribution and only schedules transfers it itself can perform (i.e. for
/// groups where it is currently leader). The result respects `max_per_tick`
/// and produces a deterministic order (smallest group id first, smallest
/// target id on ties) so retries on the same snapshot are stable.
#[cfg(test)]
pub(crate) fn plan_leadership_balance(
    snaps: &[ursula_raft::RaftGroupMetricsSnapshot],
    my_id: u64,
    max_per_tick: usize,
) -> Vec<LeadershipBalanceAction> {
    let eligible_nodes: HashSet<u64> = snaps
        .iter()
        .flat_map(|snap| snap.voter_ids.iter().copied())
        .collect();
    plan_leadership_balance_with_eligible_nodes(snaps, my_id, max_per_tick, &eligible_nodes)
}

pub(crate) fn plan_leadership_balance_with_eligible_nodes(
    snaps: &[ursula_raft::RaftGroupMetricsSnapshot],
    my_id: u64,
    max_per_tick: usize,
    eligible_nodes: &HashSet<u64>,
) -> Vec<LeadershipBalanceAction> {
    if snaps.is_empty() {
        return Vec::new();
    }
    let node_ids: HashSet<u64> = snaps
        .iter()
        .flat_map(|snap| snap.voter_ids.iter().copied())
        .collect();
    let eligible_voters: HashSet<u64> = node_ids
        .iter()
        .copied()
        .filter(|node_id| eligible_nodes.contains(node_id))
        .collect();
    if eligible_voters.is_empty() {
        return Vec::new();
    }
    let node_count = eligible_voters.len();
    let group_count = snaps.len();
    let fair = group_count.div_ceil(node_count);

    let leader_count = leader_counts(snaps);
    let my_load = leader_count.get(&my_id).copied().unwrap_or(0);
    if my_load <= fair {
        return Vec::new();
    }
    let mut excess = my_load - fair;
    // Groups we currently lead, smallest id first — deterministic shedding
    // order so consecutive ticks against the same snapshot stay stable.
    let mut groups_we_lead: Vec<&ursula_raft::RaftGroupMetricsSnapshot> = snaps
        .iter()
        .filter(|s| s.current_leader == Some(my_id))
        .collect();
    groups_we_lead.sort_by_key(|s| s.raft_group_id);

    let mut planned_load: HashMap<u64, usize> = leader_count.clone();
    let mut actions = Vec::new();
    for snap in groups_we_lead {
        if excess == 0 {
            break;
        }
        if max_per_tick > 0 && actions.len() >= max_per_tick {
            break;
        }
        // Pick the least-loaded peer voter that still has headroom under
        // `fair`; ties broken by id ascending for determinism.
        let mut peers: Vec<u64> = snap
            .voter_ids
            .iter()
            .copied()
            .filter(|v| *v != my_id)
            .filter(|v| eligible_voters.contains(v))
            .collect();
        peers.sort_by_key(|v| (planned_load.get(v).copied().unwrap_or(0), *v));
        let Some(&target) = peers
            .iter()
            .find(|v| planned_load.get(*v).copied().unwrap_or(0) < fair)
        else {
            // No peer of *this* group can take it (e.g. the group has a
            // shrunken voter set or all eligible peers are already at fair).
            // Move on to the next group — a different group may still have a
            // viable peer. Without `continue` a structurally-restricted group
            // would block all subsequent rebalancing.
            continue;
        };
        actions.push(LeadershipBalanceAction {
            group_id: snap.raft_group_id,
            target,
            fair,
        });
        *planned_load.entry(target).or_insert(0) += 1;
        // We model the handoff as already done for planning purposes so the
        // next iteration sees the updated load distribution.
        if let Some(slot) = planned_load.get_mut(&my_id) {
            *slot = slot.saturating_sub(1);
        }
        excess -= 1;
    }
    actions
}

pub(crate) fn leader_counts(
    snaps: &[ursula_raft::RaftGroupMetricsSnapshot],
) -> HashMap<u64, usize> {
    let mut leader_count = HashMap::new();
    for snap in snaps {
        if let Some(leader) = snap.current_leader {
            *leader_count.entry(leader).or_insert(0) += 1;
        }
    }
    leader_count
}

pub(crate) fn prioritized_transfer_targets(
    snap: &ursula_raft::RaftGroupMetricsSnapshot,
    my_id: u64,
    leader_count: &HashMap<u64, usize>,
) -> Vec<u64> {
    let mut targets: Vec<u64> = snap
        .voter_ids
        .iter()
        .copied()
        .filter(|voter| *voter != my_id)
        .collect();
    targets.sort_by_key(|target| (leader_count.get(target).copied().unwrap_or(0), *target));
    targets
}

const DEFAULT_CLUSTER_PROBE_INTERVAL_MS: usize = 500;
const DEFAULT_CLUSTER_PROBE_TIMEOUT_MS: usize = 200;
const DEFAULT_CLUSTER_PROBE_UNHEALTHY_TICKS: usize = 2;
const DEFAULT_CLUSTER_PROBE_HEAL_TICKS: usize = 6;

/// M2 — egress-health leadership gate. Each node actively measures its own
/// egress to peers over the cluster plane (a payload POST with a tight
/// deadline, sized so 15% loss / added delay fails it while a healthy link
/// passes trivially — a heartbeat-sized probe would be masked by loss). A node
/// that cannot push to a quorum of peers cannot replicate, so it sheds
/// leadership and disables elections; it clears this reason once its egress is
/// clean and re-enables elections only if no other shed reason remains.
///
/// Unlike inferring health from commit progress (load-dependent, and gone once
/// the client gives up), this probe is role-independent: a yielded follower
/// keeps measuring its own egress, so recovery is self-evident even though
/// egress-only loss leaves its inbound replication looking healthy. That is
/// what prevents the flap that a commit-stall step-down suffers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClusterEgressProbeScope {
    Global,
    Group(RaftGroupId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClusterEgressProbeGroup {
    pub scope: ClusterEgressProbeScope,
    pub peer_urls: Vec<String>,
    pub needed_peers: usize,
}

fn remote_peers_needed_for_quorum(total_voters: usize) -> usize {
    (total_voters / 2 + 1).saturating_sub(1)
}

pub(crate) fn cluster_egress_probe_groups(
    node_id: u64,
    peers: &[(u64, String)],
    per_group_voters: &BTreeMap<RaftGroupId, BTreeSet<u64>>,
    snapshots: &[RaftGroupMetricsSnapshot],
) -> Vec<ClusterEgressProbeGroup> {
    let peer_urls: BTreeMap<u64, String> = peers
        .iter()
        .map(|(node_id, url)| (*node_id, url.clone()))
        .collect();

    if per_group_voters.is_empty() {
        let peer_urls: Vec<String> = peers
            .iter()
            .filter(|(id, _)| *id != node_id)
            .map(|(_, url)| url.clone())
            .collect();
        if peer_urls.is_empty() {
            return Vec::new();
        }
        return vec![ClusterEgressProbeGroup {
            scope: ClusterEgressProbeScope::Global,
            peer_urls,
            needed_peers: remote_peers_needed_for_quorum(peers.len()),
        }];
    }

    let registered_groups: BTreeSet<RaftGroupId> = snapshots
        .iter()
        .map(|snapshot| RaftGroupId(snapshot.raft_group_id))
        .collect();

    per_group_voters
        .iter()
        .filter(|(raft_group_id, voters)| {
            voters.contains(&node_id)
                && (registered_groups.is_empty() || registered_groups.contains(raft_group_id))
        })
        .filter_map(|(raft_group_id, voters)| {
            let peer_urls: Vec<String> = voters
                .iter()
                .filter(|id| **id != node_id)
                .filter_map(|id| peer_urls.get(id).cloned())
                .collect();
            if peer_urls.is_empty() {
                return None;
            }
            Some(ClusterEgressProbeGroup {
                scope: ClusterEgressProbeScope::Group(*raft_group_id),
                peer_urls,
                needed_peers: remote_peers_needed_for_quorum(voters.len()),
            })
        })
        .collect()
}

pub fn spawn_cluster_egress_gate_if_configured(
    registry: &RaftGroupHandleRegistry,
    node_id: u64,
    peers: &[(u64, String)],
    per_group_voters: BTreeMap<RaftGroupId, BTreeSet<u64>>,
) {
    let interval_ms = env_usize("URSULA_CLUSTER_PROBE_MS", DEFAULT_CLUSTER_PROBE_INTERVAL_MS);
    if interval_ms == 0 {
        return;
    }
    let initial_probe_groups = cluster_egress_probe_groups(node_id, peers, &per_group_voters, &[]);
    if initial_probe_groups.is_empty() {
        return; // single node: no quorum to lose
    }
    let probe_bytes = env_usize("URSULA_CLUSTER_PROBE_BYTES", 64 * 1024);
    // Detect impaired leadership before the default 3s client append deadline:
    // if a node cannot push a 64KiB probe to a peer within 200ms for two
    // 500ms ticks, it should stop leading and let healthy peers take over.
    let probe_timeout_ms = env_usize(
        "URSULA_CLUSTER_PROBE_TIMEOUT_MS",
        DEFAULT_CLUSTER_PROBE_TIMEOUT_MS,
    );
    let unhealthy_ticks = env_usize(
        "URSULA_CLUSTER_PROBE_UNHEALTHY_TICKS",
        DEFAULT_CLUSTER_PROBE_UNHEALTHY_TICKS,
    )
    .max(1);
    let heal_ticks = env_usize(
        "URSULA_CLUSTER_PROBE_HEAL_TICKS",
        DEFAULT_CLUSTER_PROBE_HEAL_TICKS,
    )
    .max(1);
    let registry = registry.clone();
    let peers = peers.to_vec();
    tokio::spawn(async move {
        let interval = Duration::from_millis(
            u64::try_from(interval_ms)
                .unwrap_or(u64::try_from(DEFAULT_CLUSTER_PROBE_INTERVAL_MS).unwrap_or(500)),
        );
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_millis(
                u64::try_from(probe_timeout_ms)
                    .unwrap_or(u64::try_from(DEFAULT_CLUSTER_PROBE_TIMEOUT_MS).unwrap_or(200)),
            ))
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                tracing::error!("cluster-egress: failed to build probe client: {err}");
                return;
            }
        };
        let payload = vec![0u8; probe_bytes];
        let mut consecutive_bad = 0usize;
        let mut consecutive_good = 0usize;
        let mut yielded = false;
        loop {
            tokio::time::sleep(interval).await;
            let snaps = registry.metrics_snapshot();
            let probe_groups =
                cluster_egress_probe_groups(node_id, &peers, &per_group_voters, &snaps);
            if probe_groups.is_empty() {
                continue;
            }
            let mut degraded_probe = None;
            for group in &probe_groups {
                let mut healthy_peers = 0usize;
                for url in &group.peer_urls {
                    let probe_url = format!("{url}{}", crate::CLUSTER_PROBE_PATH);
                    if let Ok(resp) = client.post(&probe_url).body(payload.clone()).send().await
                        && resp.status().is_success()
                    {
                        healthy_peers += 1;
                    }
                }
                if healthy_peers < group.needed_peers {
                    degraded_probe = Some((
                        group.scope.clone(),
                        healthy_peers,
                        group.peer_urls.len(),
                        group.needed_peers,
                    ));
                    break;
                }
            }
            let can_reach_quorum = degraded_probe.is_none();
            if can_reach_quorum {
                consecutive_good += 1;
                consecutive_bad = 0;
            } else {
                consecutive_bad += 1;
                consecutive_good = 0;
            }

            if !yielded && consecutive_bad >= unhealthy_ticks {
                yielded = true;
                // Publish "do not accept leadership" BEFORE asking openraft to
                // hand off, so any peer's M1 transfer that races our shed sees
                // the flag and gets rejected at our gRPC handler.
                registry.mark_leadership_shed(LeadershipShedReason::ClusterEgress);
                for snap in &snaps {
                    let Some(raft) = registry.get(RaftGroupId(snap.raft_group_id)) else {
                        continue;
                    };
                    raft.runtime_config().elect(false);
                    if snap.current_leader == Some(node_id)
                        && let Some(target) = snap.voter_ids.iter().copied().find(|v| *v != node_id)
                    {
                        let _ = raft.trigger().transfer_leader(target).await;
                    }
                }
                if let Some((scope, healthy_peers, peer_count, needed_peers)) = degraded_probe {
                    tracing::warn!(
                        "cluster-egress: node {node_id} {scope:?} egress degraded (reached {healthy_peers}/{peer_count} peers, need {needed_peers}); yielding leadership",
                    );
                }
            } else if yielded && consecutive_good >= heal_ticks {
                yielded = false;
                registry.clear_leadership_shed(LeadershipShedReason::ClusterEgress);
                reenable_elections_if_campaign_allowed(
                    &registry,
                    &format!("cluster-egress: node {node_id} egress recovered"),
                );
            }
        }
    });
}

/// M3 — per-group commit-stall watchdog. A group's leader can wedge with
/// `last_log_index > committed_index` indefinitely (replication queue choked,
/// follower transport stuck, qdisc backlog never drains, etc.). The HTTP
/// layer faithfully forwards every write to that leader and every write hangs
/// at the 3s client deadline — agent throughput collapses on the fraction of
/// streams hashed into the wedged group while the rest of the cluster looks
/// fine.
///
/// M1's leadership balancer can't help: it only moves load when the *count*
/// of leaderships is uneven, not when a single leadership is stuck.
/// M2's egress gate can't help either: that leader's egress probes succeed,
/// because the wedge is internal (raft submit / mailbox / log-store), not
/// network. We need a stall detector keyed on actual log progress.
///
/// Mechanism: per tick, snapshot each group's `last_log_index` and
/// `committed.index` on the local node. For groups where this node is leader
/// AND `last_log > committed`, remember (last_log, committed, first_seen).
/// If on a later tick both indices are unchanged for longer than
/// `URSULA_COMMIT_STALL_THRESHOLD_MS`, call `transfer_leader` to a peer voter.
/// A new leader gets a fresh runtime path and re-replicates from quorum.
/// After attempting a transfer we clear the entry; if the stall persists
/// (transfer no-op or new leader is stuck too), the next tick re-baselines
/// and waits the full threshold again — a natural per-group backoff that
/// avoids hammering. Any forward progress in (last_log, committed) resets
/// the baseline immediately, so transient in-flight writes never trip the
/// detector (the threshold is many seconds; one commit RTT is sub-second).
pub fn spawn_commit_stall_watchdog_if_configured(registry: &RaftGroupHandleRegistry) {
    let interval_ms = env_usize("URSULA_COMMIT_STALL_MS", 2_000);
    if interval_ms == 0 {
        return;
    }
    let threshold_ms = env_usize("URSULA_COMMIT_STALL_THRESHOLD_MS", 15_000);
    let registry = registry.clone();
    tokio::spawn(async move {
        let interval = Duration::from_millis(u64::try_from(interval_ms).unwrap_or(2_000));
        let threshold = Duration::from_millis(u64::try_from(threshold_ms).unwrap_or(15_000));
        let mut tracker = CommitStallTracker::default();
        loop {
            tokio::time::sleep(interval).await;
            let snaps = registry.metrics_snapshot();
            if snaps.is_empty() {
                continue;
            }
            let my_id = snaps[0].node_id;
            let actions = tracker.evaluate(&snaps, my_id, Instant::now(), threshold);
            for action in actions {
                let Some(raft) = registry.get(RaftGroupId(action.group_id)) else {
                    continue;
                };
                tracing::warn!(
                    "commit-stall: node {my_id} group {} stalled {:.1}s (last_log={:?} committed={:?}); trying targets {:?}",
                    action.group_id,
                    action.stalled_for.as_secs_f64(),
                    action.last_log,
                    action.committed,
                    action.targets,
                );
                // Walk the prioritized target list (least-loaded first). The
                // first target whose transfer_leader call succeeds wins; if
                // ALL fail, the next tick re-baselines and waits the full
                // threshold again before retrying — naturally backs off.
                let mut handed_off = false;
                for target in &action.targets {
                    match raft.trigger().transfer_leader(*target).await {
                        Ok(()) => {
                            tracing::warn!(
                                "commit-stall: group {} handed off -> {}",
                                action.group_id,
                                target
                            );
                            handed_off = true;
                            break;
                        }
                        Err(err) => {
                            tracing::error!(
                                "commit-stall: transfer_leader group {} -> {} failed: {err}; trying next target",
                                action.group_id,
                                target
                            );
                        }
                    }
                }
                if !handed_off {
                    tracing::error!(
                        "commit-stall: group {} no target accepted transfer (all {} candidates failed); will retry after threshold",
                        action.group_id,
                        action.targets.len(),
                    );
                }
            }
        }
    });
}

/// Per-tick decision (transfer leader of this group because it has been
/// commit-stalled this long). `targets` is a priority-ordered list of peer
/// voters to try; least-loaded first so a wedge doesn't pile load onto the
/// already-busy node. The driver walks the list and stops at the first
/// `transfer_leader` that succeeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitStallAction {
    pub group_id: u32,
    pub targets: Vec<u64>,
    pub stalled_for: Duration,
    pub last_log: Option<u64>,
    pub committed: Option<u64>,
}

/// Pure decision core for the M3 watchdog — split from the spawn so it can be
/// driven from unit tests with synthetic snapshots and a fake clock.
#[derive(Default)]
pub(crate) struct CommitStallTracker {
    baseline: HashMap<u32, (Option<u64>, Option<u64>, Instant)>,
}

impl CommitStallTracker {
    pub fn evaluate(
        &mut self,
        snaps: &[ursula_raft::RaftGroupMetricsSnapshot],
        my_id: u64,
        now: Instant,
        threshold: Duration,
    ) -> Vec<CommitStallAction> {
        // Drop tracking for groups we no longer lead.
        self.baseline.retain(|gid, _| {
            snaps
                .iter()
                .any(|s| s.raft_group_id == *gid && s.current_leader == Some(my_id))
        });
        // Count leaderships per node so we can prefer least-loaded targets.
        // Without this, a single stalled leader handing off would always pick
        // the same first-non-self voter, potentially the already-loaded one.
        let mut leader_count: HashMap<u64, usize> = HashMap::new();
        for snap in snaps {
            if let Some(leader) = snap.current_leader {
                *leader_count.entry(leader).or_insert(0) += 1;
            }
        }
        let mut actions = Vec::new();
        for snap in snaps {
            if snap.current_leader != Some(my_id) {
                continue;
            }
            let last_log = snap.last_log_index;
            let committed = snap.committed.map(|c| c.index);
            let has_gap = match (last_log, committed) {
                (Some(ll), Some(c)) => ll > c,
                (Some(_), None) => true,
                _ => false,
            };
            if !has_gap {
                self.baseline.remove(&snap.raft_group_id);
                continue;
            }
            let entry = self
                .baseline
                .entry(snap.raft_group_id)
                .or_insert((last_log, committed, now));
            if entry.0 != last_log || entry.1 != committed {
                // Progress: re-baseline and wait again. A wedge that releases
                // even a single index resets the clock — which is what we want;
                // forward motion proves the leader isn't actually stuck.
                *entry = (last_log, committed, now);
                continue;
            }
            let stalled_for = now.duration_since(entry.2);
            if stalled_for < threshold {
                continue;
            }
            // Build prioritized target list: every peer voter, sorted by
            // current leader load (ascending) so we shed to the lightest
            // node first. Ties broken by voter id for determinism.
            let mut targets: Vec<u64> = snap
                .voter_ids
                .iter()
                .copied()
                .filter(|v| *v != my_id)
                .collect();
            targets.sort_by_key(|v| (leader_count.get(v).copied().unwrap_or(0), *v));
            if targets.is_empty() {
                continue;
            }
            actions.push(CommitStallAction {
                group_id: snap.raft_group_id,
                targets,
                stalled_for,
                last_log,
                committed,
            });
            // Clear so the next tick re-baselines: if the transfer succeeded
            // we'll see a new leader (or no gap); if it didn't, we wait the
            // full threshold again before retrying — a natural per-group
            // backoff that prevents hammering.
            self.baseline.remove(&snap.raft_group_id);
        }
        actions
    }
}

/// M4 — cold-health leadership gate. Two signals declare this node
/// "cold-impaired":
///
/// 1. `cold_flush_write_errors` climbed since last tick — S3 PUT or its
///    network path is failing under load.
/// 2. `cold_hot_group_bytes_max` stayed ≥ HOT_HIGH bytes — the per-group
///    hot tier isn't draining back, so we're queued up against the cap
///    and writes will reactively backpressure at the cliff.
///
/// Either condition sustained for `unhealthy_ticks` ticks triggers a soft
/// shed: the node attempts to `transfer_leader` away for every group it
/// currently leads, but it stays campaign-eligible. Cold-health is often
/// cluster-wide during backlog; disabling elections on every hot peer can
/// freeze leadership movement. The node heals when error growth stops AND
/// hot_max drops below HOT_LOW for `heal_ticks` ticks.
///
/// Why this matters: ColdBackpressure 503 is a per-write reactive gate —
/// once hot pegs at cap, the leader keeps accepting writes that immediately
/// 503, and the cold queue can only drain at the per-publish raft RTT.
/// Shedding leadership routes new client writes to a peer instead, giving
/// this node's cold queue an actual catch-up window. That's the gap that
/// dominated the long-running chaos test — many `s3_unavailable` injections
/// on the same node drove cumulative deficit because writes kept arriving.
pub fn spawn_cold_health_gate_if_configured(
    runtime: &ShardRuntime,
    registry: &RaftGroupHandleRegistry,
    node_id: u64,
) {
    let interval_ms = env_usize("URSULA_COLD_HEALTH_MS", 2_000);
    if interval_ms == 0 {
        return;
    }
    let unhealthy_ticks = env_usize("URSULA_COLD_HEALTH_UNHEALTHY_TICKS", 3).max(1);
    let heal_ticks = env_usize("URSULA_COLD_HEALTH_HEAL_TICKS", 5).max(1);
    // 7 MiB high water vs the default 8 MiB cap: shed BEFORE 503 cliff, not
    // after. 4 MiB low water gives a meaningful catch-up window before
    // clearing the cold-health signal; otherwise we'd flap on every fault
    // recovery.
    let hot_high_bytes = u64::try_from(env_usize(
        "URSULA_COLD_HEALTH_HOT_BYTES_HIGH",
        7 * 1024 * 1024,
    ))
    .unwrap_or(7 * 1024 * 1024);
    let hot_low_bytes = u64::try_from(env_usize(
        "URSULA_COLD_HEALTH_HOT_BYTES_LOW",
        4 * 1024 * 1024,
    ))
    .unwrap_or(4 * 1024 * 1024);
    let errors_per_tick_high =
        u64::try_from(env_usize("URSULA_COLD_HEALTH_ERRORS_PER_TICK_HIGH", 1)).unwrap_or(1);
    let metrics = runtime.metrics();
    let registry = registry.clone();
    tokio::spawn(async move {
        let interval = Duration::from_millis(u64::try_from(interval_ms).unwrap_or(2_000));
        let mut tracker = ColdHealthTracker::new(
            unhealthy_ticks,
            heal_ticks,
            hot_high_bytes,
            hot_low_bytes,
            errors_per_tick_high,
        );
        loop {
            tokio::time::sleep(interval).await;
            let snap = metrics.snapshot();
            let sample = ColdHealthSample {
                cold_flush_write_errors: snap.cold_flush_write_errors,
                cold_hot_group_bytes_max: snap.cold_hot_group_bytes_max,
            };
            match tracker.evaluate(sample) {
                ColdHealthDecision::Shed { reason } => {
                    tracing::warn!(
                        "cold-health: node {node_id} cold-impaired ({reason}); yielding leadership"
                    );
                    registry.mark_leadership_shed(LeadershipShedReason::ColdHealth);
                    let snaps = registry.metrics_snapshot();
                    let leader_count = leader_counts(&snaps);
                    for snap in snaps {
                        if snap.current_leader != Some(node_id) {
                            continue;
                        }
                        let Some(raft) = registry.get(RaftGroupId(snap.raft_group_id)) else {
                            continue;
                        };
                        let targets = prioritized_transfer_targets(&snap, node_id, &leader_count);
                        if targets.is_empty() {
                            tracing::warn!(
                                "cold-health: group {} has no peer voter target",
                                snap.raft_group_id
                            );
                            continue;
                        }
                        for target in targets {
                            match raft.trigger().transfer_leader(target).await {
                                Ok(()) => {
                                    tracing::warn!(
                                        "cold-health: node {node_id} yielded leadership of group {} to node {target}",
                                        snap.raft_group_id
                                    );
                                    break;
                                }
                                Err(err) => tracing::error!(
                                    "cold-health: transfer_leader group {} -> {target} failed: {err}",
                                    snap.raft_group_id
                                ),
                            }
                        }
                    }
                }
                ColdHealthDecision::Heal => {
                    registry.clear_leadership_shed(LeadershipShedReason::ColdHealth);
                    reenable_elections_if_campaign_allowed(
                        &registry,
                        &format!("cold-health: node {node_id} cold recovered"),
                    );
                }
                ColdHealthDecision::NoChange => {}
            }
        }
    });
}

/// Per-tick input for the M4 tracker. Both fields are cumulative counters
/// straight from `RuntimeMetricsSnapshot`; the tracker handles the delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ColdHealthSample {
    pub cold_flush_write_errors: u64,
    pub cold_hot_group_bytes_max: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ColdHealthDecision {
    NoChange,
    Shed { reason: String },
    Heal,
}

/// Pure decision core for the M4 cold-health gate, split from the spawn so
/// tests drive it with synthetic samples instead of a live runtime.
pub(crate) struct ColdHealthTracker {
    unhealthy_ticks: usize,
    heal_ticks: usize,
    hot_high_bytes: u64,
    hot_low_bytes: u64,
    errors_per_tick_high: u64,
    last_errors: Option<u64>,
    consecutive_bad: usize,
    consecutive_good: usize,
    yielded: bool,
}

impl ColdHealthTracker {
    pub fn new(
        unhealthy_ticks: usize,
        heal_ticks: usize,
        hot_high_bytes: u64,
        hot_low_bytes: u64,
        errors_per_tick_high: u64,
    ) -> Self {
        Self {
            unhealthy_ticks: unhealthy_ticks.max(1),
            heal_ticks: heal_ticks.max(1),
            hot_high_bytes,
            hot_low_bytes,
            errors_per_tick_high,
            last_errors: None,
            consecutive_bad: 0,
            consecutive_good: 0,
            yielded: false,
        }
    }

    pub fn evaluate(&mut self, sample: ColdHealthSample) -> ColdHealthDecision {
        // First tick has no baseline; treat delta as zero so a startup with
        // accumulated errors from a prior run doesn't immediately shed.
        let prev_errors = self.last_errors.unwrap_or(sample.cold_flush_write_errors);
        let delta_errors = sample.cold_flush_write_errors.saturating_sub(prev_errors);
        self.last_errors = Some(sample.cold_flush_write_errors);

        let errors_unhealthy = delta_errors > self.errors_per_tick_high;
        let hot_unhealthy = sample.cold_hot_group_bytes_max >= self.hot_high_bytes;
        let unhealthy = errors_unhealthy || hot_unhealthy;
        let healthy = delta_errors == 0 && sample.cold_hot_group_bytes_max <= self.hot_low_bytes;

        if unhealthy {
            self.consecutive_bad = self.consecutive_bad.saturating_add(1);
            self.consecutive_good = 0;
        } else if healthy {
            self.consecutive_good = self.consecutive_good.saturating_add(1);
            self.consecutive_bad = 0;
        } else {
            // Middle band (hot between LOW and HIGH, or sporadic errors).
            // Reset the good streak so we don't re-elect in a marginal state,
            // but don't accumulate toward shedding either — only HIGH or
            // sustained errors trip the shed.
            self.consecutive_good = 0;
        }

        if !self.yielded && self.consecutive_bad >= self.unhealthy_ticks {
            self.yielded = true;
            let reason = if errors_unhealthy {
                format!(
                    "cold_flush_write_errors +{delta_errors}/tick > {}",
                    self.errors_per_tick_high
                )
            } else {
                format!(
                    "cold_hot_max {} ≥ HIGH {}",
                    sample.cold_hot_group_bytes_max, self.hot_high_bytes
                )
            };
            return ColdHealthDecision::Shed { reason };
        }

        if self.yielded && self.consecutive_good >= self.heal_ticks {
            self.yielded = false;
            return ColdHealthDecision::Heal;
        }

        ColdHealthDecision::NoChange
    }

    #[cfg(test)]
    pub fn yielded(&self) -> bool {
        self.yielded
    }
}

pub(crate) fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(default)
}
