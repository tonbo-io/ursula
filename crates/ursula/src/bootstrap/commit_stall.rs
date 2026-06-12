use std::collections::HashMap;
use std::time::Duration;

// `tokio::time::Instant` (not `std::time::Instant`) so the M3 commit-stall
// timer behaves deterministically under madsim, which shims tokio's clock but
// can't shim std's. The non-test code path is unchanged: outside madsim it's
// just std's monotonic clock under tokio's facade.
use tokio::time::Instant;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_shard::RaftGroupId;

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
/// `commit_stall.threshold`, call `transfer_leader` to a peer voter.
/// A new leader gets a fresh runtime path and re-replicates from quorum.
/// After attempting a transfer we clear the entry; if the stall persists
/// (transfer no-op or new leader is stuck too), the next tick re-baselines
/// and waits the full threshold again — a natural per-group backoff that
/// avoids hammering. Any forward progress in (last_log, committed) resets
/// the baseline immediately, so transient in-flight writes never trip the
/// detector (the threshold is many seconds; one commit RTT is sub-second).
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
                *entry = (last_log, committed, now);
                continue;
            }
            let stalled_for = now.duration_since(entry.2);
            if stalled_for < threshold {
                continue;
            }
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
            self.baseline.remove(&snap.raft_group_id);
        }
        actions
    }
}

/// Config-driven commit stall watchdog.
pub fn spawn_commit_stall_watchdog(
    registry: &RaftGroupHandleRegistry,
    cs_cfg: &ursula_config::CommitStallConfig,
) {
    let interval_ms = cs_cfg.interval.as_duration().as_millis() as usize;
    if interval_ms == 0 {
        return;
    }
    let threshold_ms = cs_cfg.threshold.as_duration().as_millis() as usize;
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
