//! Leader-side repair planning and driver.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use openraft::RaftMetrics;
use openraft::ServerState;
use openraft::rt::WatchReceiver;
use openraft::rt::WatchSender;

use super::GroupRejoin;
use super::REJOIN_HEAL_STEP_TIMEOUT;
use super::attach::wait_recovery_change;
use super::log_index;
use crate::registry::RaftGroupHandle;
use crate::types::UrsulaRaftTypeConfig;

/// The leader's view of one group, as the heal driver reads it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct HealView {
    pub is_leader: bool,
    pub reverted: BTreeSet<u64>,
    pub awaiting_rewind: BTreeSet<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HealStep {
    RewindVoters { targets: BTreeSet<u64> },
    ReplicateRewound,
}

pub(crate) fn plan_heal_step(view: &HealView) -> Option<HealStep> {
    if !view.is_leader {
        return None;
    }
    if !view.reverted.is_empty() {
        return Some(HealStep::RewindVoters {
            targets: view.reverted.clone(),
        });
    }
    (!view.awaiting_rewind.is_empty()).then_some(HealStep::ReplicateRewound)
}

fn heal_view(
    metrics: &RaftMetrics<UrsulaRaftTypeConfig>,
    rejoin: &GroupRejoin,
    _configured: &BTreeSet<u64>,
) -> HealView {
    let membership = metrics.membership_config.membership();
    let matched = metrics
        .replication
        .as_ref()
        .map(|replication| {
            replication
                .iter()
                .map(|(node_id, matched)| (*node_id, log_index(matched.as_ref())))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let voters = membership.voter_ids().collect::<BTreeSet<_>>();
    let awaiting_rewind = rejoin
        .allowed_rewinds(&metrics.vote)
        .into_iter()
        .filter(|(target, previous)| {
            voters.contains(target) && matched.get(target).copied().flatten() == Some(*previous)
        })
        .map(|(target, _)| target)
        .collect();
    HealView {
        is_leader: metrics.state == ServerState::Leader,
        reverted: rejoin.reverted_followers(&metrics.vote),
        awaiting_rewind,
    }
}

/// Repairs replication without changing voter or learner membership.
/// Membership transitions belong exclusively to committed meta operations.
pub async fn run_rejoin_heal(
    raft: RaftGroupHandle,
    rejoin: Arc<GroupRejoin>,
    configured: BTreeMap<u64, BasicNode>,
    interval: Duration,
) {
    let configured_ids = configured.keys().copied().collect::<BTreeSet<_>>();
    let mut last_attempt = None;
    let mut observed = raft.server_metrics();
    let mut changes = rejoin.changes.subscribe();
    loop {
        if !wait_recovery_change(&mut observed, &mut changes, interval).await {
            return;
        }
        let step = {
            let metrics = raft.metrics().borrow_watched().clone();
            if metrics.running_state.is_err() {
                return;
            }
            let view = heal_view(&metrics, &rejoin, &configured_ids);
            plan_heal_step(&view)
        };
        let Some(step) = step else {
            continue;
        };
        if last_attempt.as_ref().is_some_and(
            |(previous, at): &(HealStep, crate::rt::time::Instant)| {
                previous == &step && at.elapsed() < interval
            },
        ) {
            continue;
        }
        // Replication attempts publish metrics themselves. Retry an unchanged
        // step on its deadline while allowing a different step immediately.
        last_attempt = Some((step.clone(), crate::rt::time::Instant::now()));
        let group = rejoin.raft_group_id.0;
        let node_id = rejoin.node_id;
        tracing::warn!(
            node_id,
            raft_group_id = group,
            step = ?step,
            "recovery: healing a replica that lost Raft log entries"
        );
        let result = match &step {
            HealStep::RewindVoters { targets } => {
                crate::rt::time::timeout(
                    REJOIN_HEAL_STEP_TIMEOUT,
                    rejoin.rewind_followers(&raft, targets),
                )
                .await
            }
            HealStep::ReplicateRewound => crate::rt::time::timeout(
                REJOIN_HEAL_STEP_TIMEOUT,
                raft.client_write(ursula_runtime::GroupWriteCommand::Stream(
                    ursula_stream::StreamCommand::ReplicationBarrier,
                )),
            )
            .await
            .map(|result| {
                result.map(|_| ()).map_err(|error| {
                    ursula_runtime::GroupEngineError::backend(
                        ursula_runtime::BackendOperation::Write,
                        error,
                    )
                })
            }),
        };
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => tracing::warn!(
                node_id,
                raft_group_id = group,
                step = ?step,
                "recovery: heal step failed, retrying: {err}"
            ),
            Err(_elapsed) => tracing::warn!(
                node_id,
                raft_group_id = group,
                step = ?step,
                "recovery: heal step timed out, retrying"
            ),
        }
    }
}
