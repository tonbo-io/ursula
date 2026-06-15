use std::time::Duration;

use ursula_raft::LeadershipShedReason;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_runtime::ShardRuntime;
use ursula_shard::RaftGroupId;

use crate::bootstrap::util::leader_counts;
use crate::bootstrap::util::prioritized_transfer_targets;
use crate::bootstrap::util::reenable_elections_if_campaign_allowed;

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

/// Config-driven cold health gate.
pub fn spawn_cold_health_gate(
    runtime: &ShardRuntime,
    registry: &RaftGroupHandleRegistry,
    node_id: u64,
    ch_cfg: &ursula_config::ColdHealthConfig,
) {
    let interval_ms = ch_cfg.interval.as_duration().as_millis() as usize;
    if interval_ms == 0 {
        return;
    }
    let unhealthy_ticks = ch_cfg.unhealthy_ticks.max(1);
    let heal_ticks = ch_cfg.heal_ticks.max(1);
    let hot_high_bytes = ch_cfg.hot_size_high.as_bytes();
    let hot_low_bytes = ch_cfg.hot_size_low.as_bytes();
    let errors_per_tick_high = u64::try_from(ch_cfg.errors_per_tick_high).unwrap_or(1);
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
