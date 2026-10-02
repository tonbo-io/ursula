use std::time::Duration;

use ursula_raft::LeadershipShedReason;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftGroupMetricsSnapshot;
use ursula_raft::snapshot_cadence::GroupLogProgress;
use ursula_raft::snapshot_cadence::SnapshotCadence;
use ursula_raft::snapshot_cadence::SnapshotPlan;
use ursula_runtime::ShardRuntime;
use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::default_snapshot_store;
use ursula_shard::RaftGroupId;

/// Default driver interval: 5 s with an external snapshot store, whose
/// health the driver probes every tick, and 1 s inline. `Some(0)` disables
/// the driver, and OpenRaft's entry-count policy runs instead.
pub(crate) fn resolve_snapshot_drive_interval_ms(
    configured: Option<usize>,
    snapshot_store_configured: bool,
) -> usize {
    configured.unwrap_or(if snapshot_store_configured {
        5_000
    } else {
        1_000
    })
}

/// One group's log progress as the driver sees it (F12e): the state
/// machine's log gauge, plus OpenRaft's view of whether a snapshot exists.
/// A group with no applied state reports no log, so it is never due: an
/// empty in-memory node must not publish a `group-X-empty` snapshot.
pub(crate) fn group_log_progress(snapshot: &RaftGroupMetricsSnapshot) -> GroupLogProgress {
    if snapshot.last_applied.is_none() {
        return GroupLogProgress::default();
    }
    let mut progress = snapshot.log;
    progress.has_snapshot |= snapshot.snapshot.is_some();
    progress
}

/// Groups to snapshot this tick under the byte-based cadence (F12e).
pub(crate) fn plan_snapshot_drive<'a>(
    snapshots: &'a [RaftGroupMetricsSnapshot],
    cadence: &SnapshotCadence,
    max_groups: usize,
) -> (SnapshotPlan, Vec<&'a RaftGroupMetricsSnapshot>) {
    let progress = snapshots.iter().map(group_log_progress).collect::<Vec<_>>();
    let plan = cadence.plan(&progress, max_groups.max(1));
    let selected = plan
        .groups
        .iter()
        .filter_map(|index| snapshots.get(*index))
        .collect();
    (plan, selected)
}

/// Config-driven snapshot driver. Reads parameters from typed config.
pub fn spawn_snapshot_driver(
    runtime: &ShardRuntime,
    registry: &RaftGroupHandleRegistry,
    snapshot_store: Option<SharedSnapshotStore>,
    s3_cfg: Option<&ursula_config::S3Config>,
    interval_ms: usize,
    cadence: SnapshotCadence,
    max_groups_per_tick: usize,
) {
    if interval_ms == 0 {
        return;
    }
    // Only an external store is probed; inline snapshots depend on nothing
    // outside the node, so the inline driver never sheds leadership.
    let external_store = snapshot_store.is_some();
    let snapshot_store = snapshot_store.unwrap_or_else(default_snapshot_store);
    let probe_timeout = Duration::from_millis(
        s3_cfg
            .map(|c| c.probe_timeout.as_duration().as_millis() as u64)
            .unwrap_or(2_000),
    );
    let unhealthy_ticks = s3_cfg.map(|c| c.unhealthy_ticks).unwrap_or(1).max(1);
    let heal_ticks = s3_cfg.map(|c| c.heal_ticks).unwrap_or(2).max(1);
    let runtime = runtime.clone();
    let registry = registry.clone();
    tokio::spawn(async move {
        let interval = Duration::from_millis(u64::try_from(interval_ms).unwrap_or(u64::MAX));
        let mut consecutive_bad = 0usize;
        let mut consecutive_good = 0usize;
        let mut yielded = false;
        let mut last_flush_errors = runtime.metrics().snapshot().cold_flush_write_errors;
        loop {
            let snaps = registry.metrics_snapshot();
            let bad_tick = if external_store {
                let probe_healthy = matches!(
                    tokio::time::timeout(probe_timeout, snapshot_store.health_check()).await,
                    Ok(Ok(()))
                );
                let flush_errors_now = runtime.metrics().snapshot().cold_flush_write_errors;
                let flush_failing = flush_errors_now > last_flush_errors;
                last_flush_errors = flush_errors_now;
                !probe_healthy || flush_failing
            } else {
                false
            };
            if bad_tick {
                consecutive_bad += 1;
                consecutive_good = 0;
            } else {
                consecutive_bad = 0;
                consecutive_good += 1;
            }

            if !yielded && consecutive_bad >= unhealthy_ticks {
                yielded = true;
                registry.mark_leadership_shed(LeadershipShedReason::SnapshotDriverS3);
                for snapshot in &snaps {
                    let Some(raft) = registry.get(RaftGroupId(snapshot.raft_group_id)) else {
                        continue;
                    };
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
                yielded = false;
                registry.clear_leadership_shed(LeadershipShedReason::SnapshotDriverS3);
            }

            if !bad_tick {
                let (plan, selected) = plan_snapshot_drive(&snaps, &cadence, max_groups_per_tick);
                let mut triggered = 0u64;
                for snapshot in selected {
                    let gid = snapshot.raft_group_id;
                    let Some(raft) = registry.get(RaftGroupId(gid)) else {
                        continue;
                    };
                    match raft.trigger().snapshot().await {
                        Ok(()) => triggered = triggered.saturating_add(1),
                        Err(err) => {
                            tracing::error!("snapshot driver trigger group {gid} error: {err}")
                        }
                    }
                }
                if plan.pressure {
                    runtime.metrics().record_raft_snapshot_pressure(triggered);
                    tracing::info!(
                        node_log_bytes = plan.node_log_bytes,
                        node_log_budget = cadence.node_budget_bytes,
                        triggered,
                        "raft snapshot pressure pass completed"
                    );
                }
            }

            tokio::time::sleep(interval).await;
        }
    });
}
