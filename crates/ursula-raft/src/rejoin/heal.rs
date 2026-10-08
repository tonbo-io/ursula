//! Leader-side repair planning and driver.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use openraft::ChangeMembers;
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
    /// The effective membership is a single (non-joint) config.
    pub uniform: bool,
    /// The joint config was already the effective one on the previous
    /// tick: nothing is flattening it.
    pub stale_joint: bool,
    pub voters: BTreeSet<u64>,
    pub learners: BTreeSet<u64>,
    pub reverted: BTreeSet<u64>,
    /// Voters whose rewind this leader allowed but OpenRaft has not done yet:
    /// their progress still shows the index they had matched.
    pub awaiting_rewind: BTreeSet<u64>,
    pub matched: BTreeMap<u64, Option<u64>>,
    pub committed: Option<u64>,
    /// The group's configured (static) voters.
    pub configured: BTreeSet<u64>,
}

/// One step of the heal driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HealStep {
    /// Remove a voter that lost its log; `voters` is the new voter set.
    RemoveVoter { target: u64, voters: BTreeSet<u64> },
    /// The voters that lost entries are a majority, so no removal can
    /// commit: rewind their replication and send them this leader's log.
    RewindVoters { targets: BTreeSet<u64> },
    /// OpenRaft rewinds a follower only on the conflict of a request that
    /// carries entries, and an idle group sends none: propose an unchanged
    /// membership so replication carries one.
    ReplicateRewound,
    /// Remove a learner that lost its log, so it is added back fresh.
    RemoveLearner { target: u64 },
    /// Add a configured voter that is missing as a learner.
    AddLearner { target: u64 },
    /// Promote a caught-up learner: `voters` is the current voters plus it.
    Promote { target: u64, voters: BTreeSet<u64> },
    /// Flatten a joint config whose second step was never proposed (the
    /// proposing future timed out or its leader stepped down).
    FinishJoint,
}

fn quorum(voter_count: usize) -> usize {
    (voter_count / 2).saturating_add(1)
}

/// The next heal step for a group this node leads, if any.
pub(crate) fn plan_heal_step(view: &HealView) -> Option<HealStep> {
    if !view.is_leader {
        return None;
    }
    let reverted_voters = view
        .voters
        .intersection(&view.reverted)
        .copied()
        .collect::<BTreeSet<_>>();
    if !view.uniform {
        // A second lost voter can be discovered after RemoveVoter already
        // proposed a joint configuration. Restore replication first: another
        // membership proposal cannot commit the pending joint entry.
        if !reverted_voters.is_empty() {
            return Some(HealStep::RewindVoters {
                targets: reverted_voters,
            });
        }
        return view.stale_joint.then_some(HealStep::FinishJoint);
    }
    if let Some(target) = reverted_voters.first() {
        // The removal commits only with a quorum of the current voters that
        // still hold their log. Without one, this leader rewinds them: it
        // holds every committed entry and they cannot vote meanwhile.
        let healthy_voters = view.voters.difference(&view.reverted).count();
        if healthy_voters < quorum(view.voters.len()) {
            return Some(HealStep::RewindVoters {
                targets: reverted_voters,
            });
        }
        let mut voters = view.voters.clone();
        voters.remove(target);
        return Some(HealStep::RemoveVoter {
            target: *target,
            voters,
        });
    }
    if !view.awaiting_rewind.is_empty() {
        return Some(HealStep::ReplicateRewound);
    }
    if let Some(target) = view.learners.intersection(&view.reverted).next() {
        return Some(HealStep::RemoveLearner { target: *target });
    }
    if !view.voters.is_subset(&view.configured) {
        return None;
    }
    // Several configured voters can be missing at once (two overlapping
    // restarts in a group of five): promote whichever caught up first, and
    // add the others back as learners one step at a time.
    let missing = view.configured.difference(&view.voters);
    let caught_up = missing.clone().find(|target| {
        view.learners.contains(target)
            && view.matched.get(target).copied().flatten() >= view.committed
    });
    if let Some(target) = caught_up {
        let mut voters = view.voters.clone();
        voters.insert(*target);
        return Some(HealStep::Promote {
            target: *target,
            voters,
        });
    }
    missing
        .clone()
        .find(|target| !view.learners.contains(target))
        .map(|target| HealStep::AddLearner { target: *target })
}

fn heal_view(
    metrics: &RaftMetrics<UrsulaRaftTypeConfig>,
    rejoin: &GroupRejoin,
    configured: &BTreeSet<u64>,
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
        uniform: membership.get_joint_config().len() == 1,
        stale_joint: false,
        voters,
        learners: membership.learner_ids().collect(),
        reverted: rejoin.reverted_followers(&metrics.vote),
        awaiting_rewind,
        matched,
        committed: log_index(metrics.local_committed.as_ref()),
        configured: configured.clone(),
    }
}

/// Leader-side heal driver for one group: rebuilds a voter that lost
/// entries through remove / learner / catch-up / promote, rewinds voters
/// that lost entries when they are a majority, and finishes a rebuild
/// another leader started. Returns once the Raft stops.
pub async fn run_rejoin_heal(
    raft: RaftGroupHandle,
    rejoin: Arc<GroupRejoin>,
    configured: BTreeMap<u64, BasicNode>,
    interval: Duration,
) {
    let configured_ids = configured.keys().copied().collect::<BTreeSet<_>>();
    // The log id of the joint config seen on the previous tick, if any.
    let mut last_joint = None;
    let mut last_attempt = None;
    let mut joint_since = crate::rt::time::Instant::now();
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
            let mut view = heal_view(&metrics, &rejoin, &configured_ids);
            let joint = (!view.uniform).then(|| *metrics.membership_config.log_id());
            if joint != last_joint {
                joint_since = crate::rt::time::Instant::now();
            }
            view.stale_joint = joint.is_some() && joint_since.elapsed() >= interval;
            last_joint = joint;
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
        // Membership proposals publish metrics themselves. Retry an unchanged
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
            HealStep::RemoveVoter { voters, .. } => crate::rt::time::timeout(
                REJOIN_HEAL_STEP_TIMEOUT,
                raft.change_membership(voters.clone(), false),
            )
            .await
            .map(|result| result.map(|_| ()).map_err(|err| err.to_string())),
            HealStep::RewindVoters { targets } => {
                crate::rt::time::timeout(
                    REJOIN_HEAL_STEP_TIMEOUT,
                    rejoin.rewind_followers(&raft, targets),
                )
                .await
            }
            HealStep::RemoveLearner { target } => crate::rt::time::timeout(
                REJOIN_HEAL_STEP_TIMEOUT,
                raft.change_membership(
                    ChangeMembers::RemoveNodes(BTreeSet::from([*target])),
                    false,
                ),
            )
            .await
            .map(|result| result.map(|_| ()).map_err(|err| err.to_string())),
            HealStep::AddLearner { target } => {
                let Some(node) = configured.get(target).cloned() else {
                    continue;
                };
                crate::rt::time::timeout(
                    REJOIN_HEAL_STEP_TIMEOUT,
                    raft.add_learner(*target, node, false),
                )
                .await
                .map(|result| result.map(|_| ()).map_err(|err| err.to_string()))
            }
            HealStep::Promote { voters, .. } => crate::rt::time::timeout(
                REJOIN_HEAL_STEP_TIMEOUT,
                raft.change_membership(voters.clone(), false),
            )
            .await
            .map(|result| result.map(|_| ()).map_err(|err| err.to_string())),
            // A no-op change on a joint config is OpenRaft's own second step:
            // it commits the new config alone; on a uniform config it only
            // proposes the same config again.
            HealStep::ReplicateRewound | HealStep::FinishJoint => crate::rt::time::timeout(
                REJOIN_HEAL_STEP_TIMEOUT,
                raft.change_membership(ChangeMembers::AddVoterIds(BTreeSet::new()), false),
            )
            .await
            .map(|result| result.map(|_| ()).map_err(|err| err.to_string())),
        };
        match result {
            Ok(Ok(())) => match &step {
                HealStep::RemoveVoter { target, .. } | HealStep::RemoveLearner { target } => {
                    rejoin.clear_reverted(*target);
                }
                HealStep::RewindVoters { targets } => tracing::warn!(
                    node_id,
                    raft_group_id = group,
                    followers = ?targets,
                    "recovery: a majority of voters lost entries; replicating this leader's log \
                     to them again"
                ),
                HealStep::AddLearner { .. }
                | HealStep::ReplicateRewound
                | HealStep::FinishJoint => {}
                HealStep::Promote { target, .. } => tracing::info!(
                    node_id,
                    raft_group_id = group,
                    target,
                    "recovery: the replica is a caught-up voter again"
                ),
            },
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
