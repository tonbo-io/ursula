use std::time::Duration;

use ursula_raft::LeadershipShedReason;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftGroupMetricsSnapshot;
use ursula_runtime::PlanGroupColdFlushRequest;
use ursula_runtime::ShardRuntime;
use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::default_snapshot_store;
use ursula_shard::RaftGroupId;

use crate::bootstrap::util::reenable_elections_if_campaign_allowed;

pub(crate) fn resolve_snapshot_drive_interval_ms(
    configured: Option<usize>,
    snapshot_store_configured: bool,
) -> usize {
    configured.unwrap_or(if snapshot_store_configured { 5_000 } else { 0 })
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

/// Config-driven snapshot driver. Reads parameters from typed config.
pub fn spawn_snapshot_driver(
    runtime: &ShardRuntime,
    registry: &RaftGroupHandleRegistry,
    snapshot_store: Option<SharedSnapshotStore>,
    snapshot_cfg: &ursula_config::RaftSnapshotConfig,
    s3_cfg: Option<&ursula_config::S3Config>,
    interval_ms: usize,
) {
    if interval_ms == 0 {
        return;
    }
    let snapshot_store = snapshot_store.unwrap_or_else(default_snapshot_store);
    let max_concurrency = snapshot_cfg.drive_flush_concurrency.max(1);
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
        let mut next_snapshot_drive_pos = 0usize;
        loop {
            let snaps = registry.metrics_snapshot();
            let probe_healthy = matches!(
                tokio::time::timeout(probe_timeout, snapshot_store.health_check()).await,
                Ok(Ok(()))
            );
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
                yielded = false;
                registry.clear_leadership_shed(LeadershipShedReason::SnapshotDriverS3);
                reenable_elections_if_campaign_allowed(&registry, "s3-healthy: node S3 recovered");
            }

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
