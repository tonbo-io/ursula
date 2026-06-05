//! Prcess-level orchestration: env-driven `ShardRuntime` constructors and
//! raft-related background workers.
//!
//! This module is responsible for:
//! - reading process environment variables,
//! - validating configuration,
//! - constructing the [`ShardRuntime`]
//! - spawning raft-related background workers.
//!
//! Runtime internals (engine factories, cold-store handles, etc.) do not leak
//! here; they are assembled by the caller and passed to the builder.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

// `tokio::time::Instant` (not `std::time::Instant`) so the M3 commit-stall
// timer behaves deterministically under madsim, which shims tokio's clock but
// can't shim std's. The non-test code path is unchanged: outside madsim it's
// just std's monotonic clock under tokio's facade.
use tokio::time::Instant;
use ursula_raft::ColdRaftGroupEngineFactory;
use ursula_raft::DurableRaftGroupEngineFactory;
use ursula_raft::LeadershipShedReason;
use ursula_raft::RaftGroupEngineFactory;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftGroupMetricsSnapshot;
use ursula_raft::StaticGrpcRaftGroupEngineFactory;
use ursula_raft::StaticGrpcRaftMembershipConfig;
use ursula_runtime::ColdConfig;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::InMemoryGroupEngineFactory;
use ursula_runtime::PlanGroupColdFlushRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::RuntimeError;
use ursula_runtime::ShardRuntime;
use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::WalGroupEngineFactory;
use ursula_runtime::default_snapshot_store;
use ursula_runtime::snapshot_store_from_env;
use ursula_runtime::spawn_cold_flush_worker_if_configured;
use ursula_runtime::spawn_cold_gc_worker_if_configured;
use ursula_shard::RaftGroupId;

/// Persistence strategy for the runtime.
#[derive(Debug, Clone)]
pub enum Persistence {
    InMemory,
    Wal { wal_dir: PathBuf },
    Raft { log_dir: Option<PathBuf> },
}

/// Deployment topology.
#[derive(Debug, Clone)]
pub enum Topology {
    SingleNode {
        raft_group_count: usize,
    },
    StaticCluster {
        node_id: u64,
        peers: Vec<(u64, String)>,
        raft_group_count: usize,
        initialize_membership: bool,
        membership_config: StaticGrpcRaftMembershipConfig,
    },
}

impl Topology {
    pub fn raft_group_count(&self) -> usize {
        match self {
            Topology::SingleNode { raft_group_count } => *raft_group_count,
            Topology::StaticCluster {
                raft_group_count, ..
            } => *raft_group_count,
        }
    }

    /// Construct a [`Topology::StaticCluster`] with validated membership.
    pub fn static_cluster(
        node_id: u64,
        peers: Vec<(u64, String)>,
        raft_group_count: usize,
        initialize_membership: bool,
        membership_config: StaticGrpcRaftMembershipConfig,
    ) -> Result<Self, RuntimeError> {
        Self::validate_static_cluster(raft_group_count, &peers, &membership_config)?;
        Ok(Self::StaticCluster {
            node_id,
            peers,
            raft_group_count,
            initialize_membership,
            membership_config,
        })
    }

    fn validate_static_cluster(
        raft_group_count: usize,
        peers: &[(u64, String)],
        membership_config: &StaticGrpcRaftMembershipConfig,
    ) -> Result<(), RuntimeError> {
        let per_group_voters = &membership_config.per_group_voters;
        if per_group_voters.is_empty() {
            return Ok(());
        }

        let peer_ids: BTreeSet<u64> = peers.iter().map(|(node_id, _)| *node_id).collect();
        let raft_group_count_u32 =
            u32::try_from(raft_group_count).map_err(|_| RuntimeError::StaticMembershipConfig {
                message: format!("raft_group_count {raft_group_count} exceeds u32::MAX"),
            })?;

        for (raft_group_id, voters) in per_group_voters {
            if raft_group_id.0 >= raft_group_count_u32 {
                return Err(RuntimeError::InvalidRaftGroup {
                    raft_group_id: *raft_group_id,
                    raft_group_count: raft_group_count_u32,
                });
            }
            if voters.is_empty() {
                return Err(RuntimeError::StaticMembershipConfig {
                    message: format!("raft group {} has no voters", raft_group_id.0),
                });
            }
            for voter in voters {
                if !peer_ids.contains(voter) {
                    return Err(RuntimeError::StaticMembershipConfig {
                        message: format!(
                            "raft group {} voter {} is not present in static peer config",
                            raft_group_id.0, voter
                        ),
                    });
                }
            }
        }

        for raw_group_id in 0..raft_group_count_u32 {
            let raft_group_id = RaftGroupId(raw_group_id);
            if !per_group_voters.contains_key(&raft_group_id) {
                return Err(RuntimeError::StaticMembershipConfig {
                    message: format!(
                        "partial raft_group_voters config is not supported; missing raft group {} of {}",
                        raw_group_id, raft_group_count
                    ),
                });
            }
        }

        Ok(())
    }
}

/// Result of spawning a runtime.
#[derive(Debug)]
pub struct SpawnedRuntime {
    pub runtime: ShardRuntime,
    pub raft_registry: Option<RaftGroupHandleRegistry>,
}

/// Spawn a runtime with configuration read from the process environment.
///
/// Cold-store, worker, and runtime configuration are all parsed from the
/// process environment inside this function so the caller does not need to
/// deal with env vars.
pub fn spawn_runtime(
    core_count: usize,
    persistence: Persistence,
    topology: Topology,
) -> Result<SpawnedRuntime, RuntimeError> {
    let cold_config = ColdConfig::from_env();
    let mut runtime_config = RuntimeConfig::from_env(core_count, topology.raft_group_count());
    if cold_config.is_enabled() && runtime_config.cold_max_hot_bytes_per_group.is_none() {
        runtime_config.cold_max_hot_bytes_per_group = Some(64 * 1024 * 1024);
    }
    spawn_runtime_with_config(runtime_config, cold_config, persistence, topology)
}

/// Core runtime construction from fully-resolved configuration.
///
/// This does **not** read environment variables; use [`spawn_runtime`] for
/// env-driven construction.
fn spawn_runtime_with_config(
    runtime_config: RuntimeConfig,
    cold_config: ColdConfig,
    persistence: Persistence,
    topology: Topology,
) -> Result<SpawnedRuntime, RuntimeError> {
    let cold_store =
        ColdStore::from_config(&cold_config).map_err(|err| RuntimeError::ColdStoreConfig {
            message: err.to_string(),
        })?;
    let snapshot_store = snapshot_store_from_env_or_error(&cold_config.storage)?;

    let spawned = spawn_runtime_core(
        runtime_config,
        cold_store.clone(),
        persistence,
        &topology,
        snapshot_store.clone(),
    )?;

    if spawned.runtime.has_cold_store() {
        spawn_cold_flush_worker_if_configured(&spawned.runtime, &cold_config.worker);
        spawn_cold_gc_worker_if_configured(&spawned.runtime, &cold_config.worker);
    }
    spawn_governance_tasks(&spawned, &topology, snapshot_store);
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
                ColdRaftGroupEngineFactory::new(cs.clone()),
                cold_store,
            )?,
            None => ShardRuntime::spawn_with_engine_factory_and_cold_store(
                runtime_config,
                RaftGroupEngineFactory,
                cold_store,
            )?,
        },
        Persistence::Raft { log_dir: Some(dir) } => {
            let factory = DurableRaftGroupEngineFactory::with_cold_store(dir, cold_store.clone());
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
) -> Result<SpawnedRuntime, RuntimeError> {
    if !matches!(persistence, Persistence::Raft { .. }) {
        return Err(RuntimeError::StaticMembershipConfig {
            message: "static cluster topology requires Raft persistence".to_owned(),
        });
    }
    let registry = RaftGroupHandleRegistry::default();
    let mut factory = StaticGrpcRaftGroupEngineFactory::new(
        node_id,
        peers.clone(),
        initialize_membership,
        registry.clone(),
    )
    .with_per_group_membership_initializers(membership_config.initialize_membership_per_group)
    .with_per_group_voters(membership_config.per_group_voters)
    .with_cold_store(cold_store.clone())
    .with_snapshot_store(snapshot_store);
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

/// Start all governance tasks (M0–M4) after the runtime is constructed.
fn spawn_governance_tasks(
    spawned: &SpawnedRuntime,
    topology: &Topology,
    snapshot_store: Option<SharedSnapshotStore>,
) {
    let Topology::StaticCluster {
        node_id,
        peers,
        raft_group_count: _,
        initialize_membership: _,
        membership_config,
    } = topology
    else {
        return;
    };
    let Some(registry) = spawned.raft_registry.clone() else {
        return;
    };

    let per_group_voters = membership_config.per_group_voters.clone();

    spawn_snapshot_driver_if_configured(&spawned.runtime, &registry, snapshot_store);
    spawn_leadership_balancer_if_configured(&registry, *node_id, peers);
    spawn_cluster_egress_gate_if_configured(&registry, *node_id, peers, per_group_voters);
    spawn_commit_stall_watchdog_if_configured(&registry);
    spawn_cold_health_gate_if_configured(&spawned.runtime, &registry, *node_id);
}

// ── M0: Snapshot driver ──────────────────────────────────────────────────────

/// Drives raft snapshots manually after first draining each group's hot tail to
/// cold. The drain makes the resulting snapshot's `payload` field empty (no
/// uncommitted hot bytes), shrinking the manifest install_snapshot has to ship.
///
/// When an external snapshot store is configured, the driver runs by default so
/// snapshots are built after first draining hot tails to cold. Set
/// `URSULA_SNAPSHOT_DRIVE_INTERVAL_MS=0` to disable it. Without an external
/// store, this is a no-op unless the interval is set explicitly, and openraft's
/// automatic [`SnapshotPolicy::LogsSinceLast`] still drives snapshot timing.
pub fn spawn_snapshot_driver_if_configured(
    runtime: &ShardRuntime,
    registry: &RaftGroupHandleRegistry,
    snapshot_store: Option<SharedSnapshotStore>,
) {
    let interval_ms = snapshot_drive_interval_ms(snapshot_store.is_some());
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
        let mut next_snapshot_drive_pos = 0usize;
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

            // Drive snapshots (log compaction) only while fully healthy (probe
            // ok AND cold flush succeeding). With SnapshotPolicy::Never these
            // driver triggers are the only snapshots, so skipping them during an
            // outage keeps the raft core alive: the in-memory log grows until S3
            // returns (bounded by the outage), then the next healthy tick
            // compacts it. Trigger at most one group per tick: OpenRaft returns
            // from trigger().snapshot() after enqueueing work, while the actual
            // full-state clone/upload runs in a background task.
            if !bad_tick
                && let Some((pos, snapshot)) =
                    next_snapshot_to_drive(&snaps, next_snapshot_drive_pos)
            {
                next_snapshot_drive_pos = pos.wrapping_add(1);
                if let Some(raft) = registry.get(RaftGroupId(snapshot.raft_group_id)) {
                    let gid = snapshot.raft_group_id;
                    if let Err(err) = raft.trigger().snapshot().await {
                        tracing::error!("snapshot driver trigger group {gid} error: {err}");
                    }
                }
            }

            tokio::time::sleep(interval).await;
        }
    });
}

pub(crate) fn snapshot_drive_interval_ms(snapshot_store_configured: bool) -> usize {
    resolve_snapshot_drive_interval_ms(
        env_optional_usize("URSULA_SNAPSHOT_DRIVE_INTERVAL_MS"),
        snapshot_store_configured,
    )
}

pub(crate) fn resolve_snapshot_drive_interval_ms(
    configured: Option<usize>,
    snapshot_store_configured: bool,
) -> usize {
    configured.unwrap_or(if snapshot_store_configured { 60_000 } else { 0 })
}

pub(crate) fn next_snapshot_to_drive(
    snapshots: &[RaftGroupMetricsSnapshot],
    next_pos: usize,
) -> Option<(usize, &RaftGroupMetricsSnapshot)> {
    if snapshots.is_empty() {
        return None;
    }
    let start = next_pos % snapshots.len();
    snapshots
        .iter()
        .enumerate()
        .cycle()
        .skip(start)
        .take(snapshots.len())
        .find(|(_, snapshot)| should_drive_snapshot_for_group(snapshot))
}

pub(crate) fn should_drive_snapshot_for_group(snapshot: &RaftGroupMetricsSnapshot) -> bool {
    // An empty in-memory raft node has no applied state yet; triggering a
    // manual snapshot there publishes a `group-X-empty` object that cannot be a
    // valid recovery source for an existing group.
    let Some(last_applied) = snapshot.last_applied else {
        return false;
    };
    snapshot
        .snapshot
        .is_none_or(|current| current.index < last_applied.index)
}

// ── M1: Leadership balancer ──────────────────────────────────────────────────

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

// ── M2: Cluster egress gate ──────────────────────────────────────────────────

const DEFAULT_CLUSTER_PROBE_INTERVAL_MS: usize = 500;
const DEFAULT_CLUSTER_PROBE_TIMEOUT_MS: usize = 200;
const DEFAULT_CLUSTER_PROBE_UNHEALTHY_TICKS: usize = 2;
const DEFAULT_CLUSTER_PROBE_HEAL_TICKS: usize = 6;

/// M2 — egress-health leadership gate. Each node actively measures its own
/// egress to peers over the cluster plane (a payload POST with a tight
/// deadline, sized so 15% loss / added delay fails it while a healthy link
/// passes trivially — a heartbeat-sized probe would mask loss). A node
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
                let handoffs = plan_cluster_egress_shed(&snaps, node_id);
                for snap in &snaps {
                    let Some(raft) = registry.get(RaftGroupId(snap.raft_group_id)) else {
                        continue;
                    };
                    raft.runtime_config().elect(false);
                }
                for handoff in handoffs {
                    let Some(raft) = registry.get(RaftGroupId(handoff.group_id)) else {
                        continue;
                    };
                    if let Err(err) = raft.trigger().transfer_leader(handoff.target).await {
                        tracing::error!(
                            "cluster-egress: transfer_leader group {} -> {} failed while yielding: {err}",
                            handoff.group_id,
                            handoff.target,
                        );
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClusterEgressShedAction {
    pub group_id: u32,
    pub target: u64,
}

pub(crate) fn plan_cluster_egress_shed(
    snaps: &[RaftGroupMetricsSnapshot],
    node_id: u64,
) -> Vec<ClusterEgressShedAction> {
    let mut planned_load = leader_counts(snaps);
    let mut groups_we_lead: Vec<&RaftGroupMetricsSnapshot> = snaps
        .iter()
        .filter(|snap| snap.current_leader == Some(node_id))
        .collect();
    groups_we_lead.sort_by_key(|snap| snap.raft_group_id);

    let mut actions = Vec::new();
    for snap in groups_we_lead {
        let mut targets: Vec<u64> = snap
            .voter_ids
            .iter()
            .copied()
            .filter(|voter| *voter != node_id)
            .collect();
        targets.sort_by_key(|target| (planned_load.get(target).copied().unwrap_or(0), *target));
        let Some(target) = targets.into_iter().next() else {
            continue;
        };
        actions.push(ClusterEgressShedAction {
            group_id: snap.raft_group_id,
            target,
        });
        *planned_load.entry(target).or_insert(0) += 1;
        if let Some(load) = planned_load.get_mut(&node_id) {
            *load = load.saturating_sub(1);
        }
    }
    actions
}

// ── M3: Commit stall watchdog ────────────────────────────────────────────────

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

// ── M4: Cold health gate ─────────────────────────────────────────────────────

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

// ── Shared leadership helpers ────────────────────────────────────────────────

fn set_registered_group_elections(registry: &RaftGroupHandleRegistry, enabled: bool) {
    for snapshot in registry.metrics_snapshot() {
        if let Some(raft) = registry.get(RaftGroupId(snapshot.raft_group_id)) {
            raft.runtime_config().elect(enabled);
        }
    }
}

pub(crate) fn reenable_elections_if_campaign_allowed(
    registry: &RaftGroupHandleRegistry,
    context: &str,
) {
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

// ── Env utilities ────────────────────────────────────────────────────────────

fn env_optional_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
}

/// Read an environment variable as `usize`, falling back to `default`.
pub fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(default)
}

fn snapshot_store_from_env_or_error(
    cold_storage: &ursula_runtime::ColdStorageConfig,
) -> Result<Option<SharedSnapshotStore>, RuntimeError> {
    snapshot_store_from_env(cold_storage).map_err(|err| RuntimeError::ColdStoreConfig {
        message: err.to_string(),
    })
}
