use std::time::Duration;

use ursula_raft::LeadershipShedReason;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftGroupMetricsSnapshot;
use ursula_raft::SnapshotBuildCoordinator;
use ursula_raft::snapshot_cadence::GroupLogProgress;
use ursula_raft::snapshot_cadence::SnapshotCadence;
use ursula_raft::snapshot_cadence::SnapshotPlan;
use ursula_runtime::ShardRuntime;
use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::default_snapshot_store;
use ursula_shard::RaftGroupId;

/// Longest the driver waits for a build permit (the previous build to finish)
/// before it ends the tick.
const SNAPSHOT_PERMIT_WAIT: Duration = Duration::from_secs(60);
/// Longest a handed-off permit waits for the triggered group to claim it.
const SNAPSHOT_HANDOFF_WAIT: Duration = Duration::from_secs(5);
/// Pause between ticks while the node is over its log watermark, so a
/// pressure pass is not capped at `max_groups_per_tick` per interval.
const SNAPSHOT_PRESSURE_RETICK: Duration = Duration::from_millis(250);
/// How often the node's unsnapshotted log is checked against its hard limit.
const LOG_PRESSURE_POLL: Duration = Duration::from_millis(100);

/// Node log bytes above which client writes are refused (503) until snapshots
/// bring the log back under [`log_pressure_resume_bytes`]: twice the
/// snapshot driver's log budget. The driver normally holds the log near the
/// budget; this is the bound for when it cannot keep up.
fn log_pressure_limit_bytes(cadence: &SnapshotCadence) -> u64 {
    cadence.node_budget_bytes.saturating_mul(2)
}

fn log_pressure_resume_bytes(cadence: &SnapshotCadence) -> u64 {
    cadence.node_budget_bytes.saturating_mul(3) / 2
}

/// Trigger one group's snapshot and see the build through to its start.
///
/// Builds share the node-wide build permit, and a triggered build that finds
/// it taken is refused rather than queued. Triggering a whole plan at once
/// therefore built about one group per tick while the rest were refused, and
/// under sustained writes the Raft log (held in memory by both WAL backends)
/// outgrew the node. The driver takes the permit first — which waits for the
/// previous build to finish — and hands it to the group it triggers.
async fn trigger_snapshot_build(
    registry: &RaftGroupHandleRegistry,
    coordinator: &SnapshotBuildCoordinator,
    raft_group_id: u32,
) -> Result<bool, ()> {
    let Some(raft) = registry.get(RaftGroupId(raft_group_id)) else {
        return Ok(false);
    };
    let Ok(Ok(permit)) = tokio::time::timeout(SNAPSHOT_PERMIT_WAIT, coordinator.acquire()).await
    else {
        tracing::warn!(
            raft_group_id,
            "snapshot driver: no build permit within {SNAPSHOT_PERMIT_WAIT:?}; ending this tick"
        );
        return Err(());
    };
    coordinator.hand_off(raft_group_id, permit);
    if let Err(err) = raft.trigger().snapshot().await {
        coordinator.reclaim_handoff(raft_group_id);
        tracing::error!("snapshot driver trigger group {raft_group_id} error: {err}");
        return Ok(false);
    }
    let deadline = tokio::time::Instant::now() + SNAPSHOT_HANDOFF_WAIT;
    while coordinator.handoff_pending(raft_group_id) && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Unclaimed: OpenRaft dropped the trigger (e.g. a build was already running).
    Ok(!coordinator.reclaim_handoff(raft_group_id))
}

/// Whether this tick triggers snapshot builds.
///
/// A bad tick (store probe failed, or new cold-flush write errors) holds
/// routine snapshots back so the driver does not push uploads into a failing
/// store. Not once the log is over the pressure watermark: a build whose
/// upload, verification or reference publication fails falls back to an
/// inline snapshot, which still truncates the log, so nothing a snapshot needs
/// depends on the store. Holding builds back there would grow the log into
/// the node-wide 503 write gate while the store is merely throttling.
fn should_drive_snapshots(bad_tick: bool, log_pressure: bool) -> bool {
    !bad_tick || log_pressure
}

/// Keeps the coordinator's log-pressure flag (read by HTTP admission) in step
/// with the node's unsnapshotted Raft log bytes.
fn spawn_log_pressure_monitor(coordinator: SnapshotBuildCoordinator, cadence: &SnapshotCadence) {
    let limit = log_pressure_limit_bytes(cadence);
    let resume = log_pressure_resume_bytes(cadence);
    tokio::spawn(async move {
        loop {
            let log_bytes = coordinator
                .log_progress()
                .values()
                .fold(0_u64, |total, progress| {
                    total.saturating_add(progress.log_bytes)
                });
            match coordinator.observe_log_bytes(log_bytes, limit, resume) {
                Some(true) => tracing::warn!(
                    log_bytes,
                    limit,
                    "raft log over its hard limit: refusing client writes until snapshots catch up"
                ),
                Some(false) => tracing::info!(log_bytes, resume, "raft log pressure cleared"),
                None => {}
            }
            tokio::time::sleep(LOG_PRESSURE_POLL).await;
        }
    });
}

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
    let coordinator = registry.snapshot_build_coordinator();
    spawn_log_pressure_monitor(coordinator.clone(), &cadence);
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

            let mut pause = interval;
            let (plan, selected) = plan_snapshot_drive(&snaps, &cadence, max_groups_per_tick);
            if should_drive_snapshots(bad_tick, plan.pressure) {
                let mut triggered = 0u64;
                for snapshot in selected {
                    match trigger_snapshot_build(&registry, &coordinator, snapshot.raft_group_id)
                        .await
                    {
                        Ok(true) => triggered = triggered.saturating_add(1),
                        Ok(false) => {}
                        Err(()) => break,
                    }
                }
                if plan.pressure {
                    if triggered > 0 {
                        pause = pause.min(SNAPSHOT_PRESSURE_RETICK);
                    }
                    runtime.metrics().record_raft_snapshot_pressure(triggered);
                    tracing::info!(
                        node_log_bytes = plan.node_log_bytes,
                        node_log_budget = cadence.node_budget_bytes,
                        triggered,
                        "raft snapshot pressure pass completed"
                    );
                }
            }

            tokio::time::sleep(pause).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_trouble_holds_back_routine_snapshots_but_not_a_pressure_pass() {
        assert!(should_drive_snapshots(false, false));
        assert!(!should_drive_snapshots(true, false));
        assert!(should_drive_snapshots(true, true));
        assert!(should_drive_snapshots(false, true));
    }
}
