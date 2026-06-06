//! In-process validation of the `ursulactl restart` orchestration against a
//! real OpenRaft 3-node cluster running under madsim.
//!
//! Reuses [`ursula_ctl::plan_drain`] and [`ursula_ctl::check_readiness`] as
//! pure functions over a [`ClusterSnapshot`] synthesised from openraft
//! metrics. This catches the failure modes the python rolling restart silently
//! ignored:
//!   * a target node is restarted while it still leads at least one group;
//!   * a target node is declared "ready" before its `last_applied_index` has
//!     caught up to peers' `committed_index`;
//!   * a drain step that can never succeed (because the chosen successor is
//!     unreachable) still proceeds to restart.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use openraft::rt::WatchReceiver;
use ursula_ctl::ClusterSnapshot;
use ursula_ctl::NodeInfo;
use ursula_ctl::RaftGroupView;
use ursula_ctl::metrics::NodeMetricsView;
use ursula_ctl::plan::check_readiness;
use ursula_ctl::plan::plan_drain;
use ursula_raft::InProcessRaftRegistry;

use super::placement;

/// What the validator does next when a per-node step cannot complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RestartStepOutcome {
    Restarted,
    AbortedDrain { reason: String },
    AbortedReady { reason: String },
}

/// Tunables for the in-process rollout. Mirrors `RestartOptions` but in
/// simulation-time units.
#[derive(Debug, Clone)]
pub(super) struct ValidatorOptions {
    pub drain_timeout: Duration,
    pub ready_timeout: Duration,
    pub poll_interval: Duration,
    pub lag_tolerance: u64,
}

impl Default for ValidatorOptions {
    fn default() -> Self {
        Self {
            drain_timeout: Duration::from_secs(2),
            ready_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_millis(50),
            lag_tolerance: 4,
        }
    }
}

/// Restart action injected per-target. Real code calls
/// `MadsimRuntimeRaftNetworkFactory::restart_follower`; failure-mode tests
/// pass closures that never succeed.
pub(super) type RestartFn =
    Box<dyn Fn(u64) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync>;

pub(super) struct RollingRestartValidator {
    registry: InProcessRaftRegistry,
    node_ids: Vec<u64>,
    options: ValidatorOptions,
}

impl RollingRestartValidator {
    pub(super) fn new(
        registry: InProcessRaftRegistry,
        node_ids: Vec<u64>,
        options: ValidatorOptions,
    ) -> Self {
        Self {
            registry,
            node_ids,
            options,
        }
    }

    /// Validate a per-node rollout step. Returns the outcome the equivalent
    /// `ursulactl restart` loop would have returned for this target.
    pub(super) async fn restart_one(
        &self,
        target_node_id: u64,
        restart: &RestartFn,
    ) -> RestartStepOutcome {
        // 1. Pre-flight snapshot from openraft metrics → ClusterSnapshot.
        let snapshot = self.snapshot();
        let plan = plan_drain(&snapshot, target_node_id);

        // 2. Drain — call transfer_leader on the target's own handle (which is
        // the current leader for each group in the plan).
        let target_handle = self.registry.get(target_node_id);
        for transfer in &plan.transfers {
            let Some(raft) = target_handle.as_ref() else {
                return RestartStepOutcome::AbortedDrain {
                    reason: format!("target node {target_node_id} is not registered"),
                };
            };
            if let Err(err) = raft
                .trigger()
                .transfer_leader(transfer.preferred_successor)
                .await
            {
                return RestartStepOutcome::AbortedDrain {
                    reason: format!(
                        "trigger transfer_leader to {}: {err}",
                        transfer.preferred_successor
                    ),
                };
            }
        }

        // 3. Poll until target leads zero groups.
        let drain_deadline = madsim::time::Instant::now() + self.options.drain_timeout;
        loop {
            let snap = self.snapshot();
            if snap.groups_led_by(target_node_id).is_empty() {
                break;
            }
            if madsim::time::Instant::now() >= drain_deadline {
                return RestartStepOutcome::AbortedDrain {
                    reason: format!(
                        "drain timeout: still leading {} group(s)",
                        snap.groups_led_by(target_node_id).len()
                    ),
                };
            }
            madsim::time::sleep(self.options.poll_interval).await;
        }

        // 4. External restart.
        if let Err(err) = restart(target_node_id).await {
            return RestartStepOutcome::AbortedReady {
                reason: format!("restart action failed: {err}"),
            };
        }

        // 5. Wait for readiness.
        let ready_deadline = madsim::time::Instant::now() + self.options.ready_timeout;
        loop {
            let snap = self.snapshot();
            let report = check_readiness(&snap, target_node_id, self.options.lag_tolerance);
            if report.all_ready {
                return RestartStepOutcome::Restarted;
            }
            if madsim::time::Instant::now() >= ready_deadline {
                let unready: Vec<String> = report
                    .per_group
                    .values()
                    .filter(|g| !g.ready)
                    .map(|g| {
                        format!(
                            "group {}: voter={} applied={:?} peer_committed={:?}",
                            g.raft_group_id,
                            g.voter_member,
                            g.target_applied_index,
                            g.peer_max_committed_index,
                        )
                    })
                    .collect();
                return RestartStepOutcome::AbortedReady {
                    reason: format!("readiness timeout: {}", unready.join("; ")),
                };
            }
            madsim::time::sleep(self.options.poll_interval).await;
        }
    }

    fn snapshot(&self) -> ClusterSnapshot {
        let placement = placement();
        let raft_group_id = u64::from(placement.raft_group_id.0);
        let mut per_node = Vec::with_capacity(self.node_ids.len());
        for &node_id in &self.node_ids {
            let Some(raft) = self.registry.get(node_id) else {
                continue;
            };
            let metrics = raft.metrics().borrow_watched().clone();
            let membership = metrics.membership_config.membership();
            let voter_ids: Vec<u64> = membership.voter_ids().collect();
            let learner_ids: Vec<u64> = membership.learner_ids().collect();
            let view = NodeMetricsView {
                node: synthetic_node_info(node_id),
                groups: vec![RaftGroupView {
                    raft_group_id,
                    node_id,
                    current_leader: metrics.current_leader,
                    committed_index: metrics.committed.map(|log_id| log_id.index),
                    last_applied_index: metrics.last_applied.map(|log_id| log_id.index),
                    voter_ids,
                    learner_ids,
                }],
            };
            per_node.push(view);
        }
        ClusterSnapshot { per_node }
    }
}

fn synthetic_node_info(node_id: u64) -> NodeInfo {
    NodeInfo {
        id: node_id,
        // The planner never reaches over the network in this validator; we
        // synthesise URLs so NodeInfo's invariants are satisfied.
        http_url: format!("http://node-{node_id}.sim/")
            .parse()
            .expect("synthetic url"),
        host: format!("node-{node_id}"),
        name: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::MutexGuard;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use super::super::build_restartable_three_node_cluster;
    use super::super::run_with_madsim;
    use super::super::sim_network_policy;
    use super::*;

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn guard() -> MutexGuard<'static, ()> {
        TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn validator(registry: InProcessRaftRegistry) -> RollingRestartValidator {
        RollingRestartValidator::new(registry, vec![1, 2, 3], ValidatorOptions::default())
    }

    #[test]
    #[ignore = "diagnostic: madsim process-global state; run individually"]
    fn rolling_restart_happy_path_transfers_leadership_and_observes_readiness() {
        let _g = guard();
        let outcome = run_with_madsim(0xc7c7_0001, async {
            let policy = sim_network_policy();
            let (registry, mut engines, _log_stores, _config, leader_id) =
                build_restartable_three_node_cluster(policy.clone()).await;

            let validator = validator(registry.clone());

            // No-op restart closure: in a healthy cluster, the post-drain
            // wait-ready check should succeed even without a real restart
            // because applied_index for every node is already >= peer
            // committed_index.
            let restart: RestartFn = Box::new(move |_node_id| Box::pin(async { Ok(()) }));

            let result = validator.restart_one(leader_id, &restart).await;

            // Cleanly shut down to release madsim globals.
            for engine in engines.drain(..) {
                let _ = engine.shutdown().await;
            }
            result
        });

        assert!(
            matches!(outcome, RestartStepOutcome::Restarted),
            "expected Restarted, got {outcome:?}"
        );
    }

    #[test]
    #[ignore = "diagnostic: madsim process-global state; run individually"]
    fn rolling_restart_aborts_drain_when_successors_are_partitioned() {
        let _g = guard();
        let outcome = run_with_madsim(0xc7c7_0002, async {
            let policy = sim_network_policy();
            let (registry, mut engines, _log_stores, _config, leader_id) =
                build_restartable_three_node_cluster(policy.clone()).await;

            // Partition the leader bidirectionally from BOTH potential
            // successors. transfer_leader requests still send, but the
            // candidate cannot receive logs / acks needed to complete the
            // handover, and the leader keeps leading. Drain must time out.
            let successors: Vec<u64> = [1u64, 2, 3]
                .into_iter()
                .filter(|id| *id != leader_id)
                .collect();
            for peer in &successors {
                policy.partition_bidirectional(leader_id, *peer);
            }

            let validator = validator(registry.clone());
            let restart_called = Arc::new(AtomicBool::new(false));
            let flag = restart_called.clone();
            let restart: RestartFn = Box::new(move |_node_id| {
                let flag = flag.clone();
                Box::pin(async move {
                    flag.store(true, Ordering::SeqCst);
                    Ok(())
                })
            });

            let result = validator.restart_one(leader_id, &restart).await;

            // Heal so engines can shut down cleanly.
            for peer in &successors {
                policy.heal_bidirectional(leader_id, *peer);
            }
            for engine in engines.drain(..) {
                let _ = engine.shutdown().await;
            }

            (result, restart_called.load(Ordering::SeqCst))
        });

        assert!(
            matches!(outcome.0, RestartStepOutcome::AbortedDrain { .. }),
            "expected AbortedDrain, got {:?}",
            outcome.0
        );
        assert!(
            !outcome.1,
            "restart action MUST NOT be invoked after a drain abort"
        );
    }

    #[test]
    #[ignore = "diagnostic: madsim process-global state; run individually"]
    fn rolling_restart_aborts_when_restart_action_signals_failure() {
        let _g = guard();
        let outcome = run_with_madsim(0xc7c7_0003, async {
            let policy = sim_network_policy();
            let (registry, mut engines, _log_stores, _config, leader_id) =
                build_restartable_three_node_cluster(policy.clone()).await;

            let validator = validator(registry.clone());
            // Simulates a crash-loop: the systemd unit (or moral equivalent)
            // can't bring the node back up. ursulactl must surface this as
            // an abort rather than silently moving to the next node.
            let restart: RestartFn = Box::new(move |node_id| {
                Box::pin(async move { Err(format!("simulated crash-loop on node {node_id}")) })
            });

            let result = validator.restart_one(leader_id, &restart).await;
            for engine in engines.drain(..) {
                let _ = engine.shutdown().await;
            }
            result
        });

        assert!(
            matches!(outcome, RestartStepOutcome::AbortedReady { .. }),
            "expected AbortedReady, got {outcome:?}"
        );
    }
}
