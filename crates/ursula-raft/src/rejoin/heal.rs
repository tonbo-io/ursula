//! Leader-side membership repair and the follower log reversion observer.

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
use crate::types::UrsulaVote;

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
    pub matched: BTreeMap<u64, Option<u64>>,
    pub committed: Option<u64>,
    /// The group's configured (static) voters.
    pub configured: BTreeSet<u64>,
}

/// One step of the heal driver. A follower that lost entries stays a
/// member: OpenRaft rewinds it in place (log reversion is allowed in every
/// gated group), so the driver only brings the membership back to the
/// configured voters: after an operator removed one, or a recovery plan
/// kept a membership that lacks one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HealStep {
    /// Add a configured voter that is missing as a learner.
    AddLearner { target: u64 },
    /// Promote a caught-up learner: `voters` is the current voters plus it.
    Promote { target: u64, voters: BTreeSet<u64> },
    /// Flatten a joint config whose second step was never proposed (the
    /// proposing future timed out or its leader stepped down).
    FinishJoint,
}

/// The next heal step for a group this node leads, if any.
pub(crate) fn plan_heal_step(view: &HealView) -> Option<HealStep> {
    if !view.is_leader {
        return None;
    }
    if !view.uniform {
        return view.stale_joint.then_some(HealStep::FinishJoint);
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

fn matched_indexes(metrics: &RaftMetrics<UrsulaRaftTypeConfig>) -> BTreeMap<u64, Option<u64>> {
    metrics
        .replication
        .as_ref()
        .map(|replication| {
            replication
                .iter()
                .map(|(node_id, matched)| (*node_id, log_index(matched.as_ref())))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default()
}

fn heal_view(metrics: &RaftMetrics<UrsulaRaftTypeConfig>, configured: &BTreeSet<u64>) -> HealView {
    let membership = metrics.membership_config.membership();
    HealView {
        is_leader: metrics.state == ServerState::Leader,
        uniform: membership.get_joint_config().len() == 1,
        stale_joint: false,
        voters: membership.voter_ids().collect(),
        learners: membership.learner_ids().collect(),
        matched: matched_indexes(metrics),
        committed: log_index(metrics.local_committed.as_ref()),
        configured: configured.clone(),
    }
}

/// A follower whose matched index went down within one leadership: OpenRaft
/// rewound it because it answered a conflict below what it had matched, so
/// it lost entries it had acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LogReversion {
    pub target: u64,
    pub matched: u64,
    pub now: Option<u64>,
}

/// What this leader saw each follower match in its current leadership: the
/// passive observer of follower log reversions. It reads the replication
/// metrics only, and changes nothing OpenRaft does.
#[derive(Debug, Default)]
pub(crate) struct ReversionWatch {
    leader: Option<UrsulaVote>,
    matched: BTreeMap<u64, u64>,
}

impl ReversionWatch {
    /// The followers whose matched index went down since the previous
    /// observation under the same leadership `vote`. A new leadership starts
    /// afresh, since a new leader learns what each follower holds.
    pub(crate) fn observe(
        &mut self,
        vote: &UrsulaVote,
        is_leader: bool,
        matched: &BTreeMap<u64, Option<u64>>,
    ) -> Vec<LogReversion> {
        if !is_leader || self.leader.as_ref() != Some(vote) {
            self.leader = is_leader.then_some(*vote);
            self.matched.clear();
        }
        if !is_leader {
            return Vec::new();
        }
        let mut reversions = Vec::new();
        // A follower leaving the membership leaves the replication metrics:
        // that is no reversion.
        self.matched
            .retain(|target, _| matched.contains_key(target));
        for (target, now) in matched {
            let previous = self.matched.get(target).copied();
            if let Some(previous) = previous
                && now.is_none_or(|now| now < previous)
            {
                reversions.push(LogReversion {
                    target: *target,
                    matched: previous,
                    now: *now,
                });
            }
            match now {
                Some(now) => {
                    self.matched.insert(*target, *now);
                }
                None => {
                    self.matched.remove(target);
                }
            }
        }
        reversions
    }
}

/// Leader-side heal driver for one group. It observes follower log
/// reversions, which OpenRaft repairs in place, and counts and reports each
/// one. It also brings the membership back to the configured voters (add a
/// missing one as a learner, promote it once caught up, flatten a stale
/// joint config). Returns once the Raft stops.
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
    let mut reversions = ReversionWatch::default();
    // Every metrics change wakes the driver, replication progress included,
    // so the observer sees a rewound follower before it matches again.
    let mut observed = raft.metrics();
    let mut changes = rejoin.changes.subscribe();
    loop {
        if !wait_recovery_change(&mut observed, &mut changes, interval).await {
            return;
        }
        let (step, reverted) = {
            let metrics = observed.borrow_watched();
            if metrics.running_state.is_err() {
                return;
            }
            let mut view = heal_view(&metrics, &configured_ids);
            let reverted = reversions.observe(&metrics.vote, view.is_leader, &view.matched);
            let joint = (!view.uniform).then(|| *metrics.membership_config.log_id());
            if joint != last_joint {
                joint_since = crate::rt::time::Instant::now();
            }
            view.stale_joint = joint.is_some() && joint_since.elapsed() >= interval;
            last_joint = joint;
            (plan_heal_step(&view), reverted)
        };
        for reversion in reverted {
            rejoin.record_log_reversion(reversion);
        }
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
            "recovery: restoring the group's configured voters"
        );
        let result = match &step {
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
            // it commits the new config alone.
            HealStep::FinishJoint => crate::rt::time::timeout(
                REJOIN_HEAL_STEP_TIMEOUT,
                raft.change_membership(ChangeMembers::AddVoterIds(BTreeSet::new()), false),
            )
            .await
            .map(|result| result.map(|_| ()).map_err(|err| err.to_string())),
        };
        match result {
            Ok(Ok(())) => match &step {
                HealStep::AddLearner { .. } | HealStep::FinishJoint => {}
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
