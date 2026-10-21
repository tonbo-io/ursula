//! Leader-side membership repair toward a group's configured voters.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use openraft::ChangeMembers;
use openraft::EmptyNode;
use openraft::Raft;
use openraft::RaftMetrics;
use openraft::ServerState;
use openraft::rt::WatchSender;
use openraft::storage::RaftStateMachine;

use super::GroupRejoin;
use super::REJOIN_HEAL_STEP_TIMEOUT;
use super::leader_tick::FollowerLogReversions;
use super::leader_tick::LeaderTick;
use super::leader_tick::StepRetry;
use super::leader_tick::matched_indexes;
use super::log_index;
use crate::GroupNodes;
use crate::target::WireTarget;
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

/// Leader-side heal driver for one group. While this replica leads, its
/// [`LeaderTick`] observes follower log reversions, which OpenRaft repairs
/// in place, and counts each one in `reversions` and logs it. The driver
/// also brings the membership back to the configured voters (add a missing
/// one as a learner, promote it once caught up, flatten a stale joint
/// config). It reads the configured voters from `nodes` on every tick.
/// Returns once the Raft stops.
pub(crate) async fn run_rejoin_heal<SM: RaftStateMachine<UrsulaRaftTypeConfig>>(
    raft: Raft<UrsulaRaftTypeConfig, SM>,
    rejoin: Arc<GroupRejoin>,
    nodes: Arc<dyn GroupNodes>,
    interval: Duration,
    reversions: FollowerLogReversions,
) {
    // The log id of the joint config seen on the previous tick, if any.
    let mut last_joint = None;
    let mut joint_since = crate::rt::time::Instant::now();
    let mut retry = StepRetry::default();
    let mut tick = LeaderTick::new(
        rejoin.group,
        rejoin.node_id,
        raft.server_metrics(),
        raft.metrics(),
        rejoin.changes.subscribe(),
        interval,
        reversions,
    );
    loop {
        let Some(planned) = tick
            .next(|current| {
                let mut view = heal_view(current, &nodes.voters());
                let joint = (!view.uniform).then_some(*current.membership_config.log_id());
                if joint != last_joint {
                    joint_since = crate::rt::time::Instant::now();
                }
                view.stale_joint = joint.is_some() && joint_since.elapsed() >= interval;
                last_joint = joint;
                plan_heal_step(&view)
            })
            .await
        else {
            return;
        };
        let Some(step) = planned.flatten() else {
            continue;
        };
        if !retry.admit(&step, interval) {
            continue;
        }
        let plane = rejoin.group.plane().as_str_name();
        let group = rejoin.group.wire_group_id();
        let node_id = rejoin.node_id;
        tracing::warn!(
            node_id,
            plane,
            raft_group_id = group,
            step = ?step,
            "recovery: restoring the group's configured voters"
        );
        let result = match &step {
            HealStep::AddLearner { target } => crate::rt::time::timeout(
                REJOIN_HEAL_STEP_TIMEOUT,
                raft.add_learner(*target, EmptyNode {}, false),
            )
            .await
            .map(|result| result.map(|_| ()).map_err(|err| err.to_string())),
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
                    plane,
                    raft_group_id = group,
                    target,
                    "recovery: the replica is a caught-up voter again"
                ),
            },
            Ok(Err(err)) => tracing::warn!(
                node_id,
                plane,
                raft_group_id = group,
                step = ?step,
                "recovery: heal step failed, retrying: {err}"
            ),
            Err(_elapsed) => tracing::warn!(
                node_id,
                plane,
                raft_group_id = group,
                step = ?step,
                "recovery: heal step timed out, retrying"
            ),
        }
    }
}
