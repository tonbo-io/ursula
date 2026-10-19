//! Leader-side membership repair and the follower log reversion observer.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use openraft::ChangeMembers;
use openraft::EmptyNode;
use openraft::RaftMetrics;
use openraft::ServerState;
use openraft::alias::LogIdOf;
use openraft::rt::WatchReceiver;
use openraft::rt::WatchSender;

use super::GroupRejoin;
use super::REJOIN_HEAL_STEP_TIMEOUT;
use super::attach::Wake;
use super::attach::wait_change;
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
    pub follower: u64,
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
    /// observation under the leadership `vote`, read from OpenRaft's
    /// replication metrics. A new leadership starts afresh, since a new
    /// leader learns what each follower holds. Allocates only to report a
    /// reversion or to track a follower it has not seen match yet.
    pub(crate) fn observe(
        &mut self,
        vote: &UrsulaVote,
        replication: &BTreeMap<u64, Option<LogIdOf<UrsulaRaftTypeConfig>>>,
    ) -> Vec<LogReversion> {
        if self.leader.as_ref() != Some(vote) {
            self.leader = Some(*vote);
            self.matched.clear();
        }
        // A follower leaving the membership leaves the replication metrics:
        // that is no reversion.
        self.matched
            .retain(|follower, _| replication.contains_key(follower));
        let mut reversions = Vec::new();
        for (follower, now) in replication {
            let now = log_index(now.as_ref());
            if let Some(previous) = self.matched.get(follower).copied()
                && now.is_none_or(|now| now < previous)
            {
                reversions.push(LogReversion {
                    follower: *follower,
                    matched: previous,
                    now,
                });
            }
            // Replacing the value of a follower already tracked allocates
            // nothing.
            match now {
                Some(now) => {
                    self.matched.insert(*follower, now);
                }
                None => {
                    self.matched.remove(follower);
                }
            }
        }
        reversions
    }

    /// This replica stopped leading: the next leadership starts afresh.
    pub(crate) fn forget(&mut self) {
        self.leader = None;
        self.matched.clear();
    }
}

/// How many times each follower lost entries it had acknowledged to a
/// leader on this node, by follower node id. One per node, in its registry,
/// so it only grows: a group's restart or a stopped group keeps its counts.
#[derive(Debug, Clone, Default)]
pub(crate) struct FollowerLogReversions(Arc<Mutex<BTreeMap<u64, u64>>>);

impl FollowerLogReversions {
    fn record(&self, follower: u64) {
        let mut counts = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let count = counts.entry(follower).or_insert(0);
        *count = count.saturating_add(1);
    }

    pub(crate) fn snapshot(&self) -> BTreeMap<u64, u64> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// What a heal plan depends on besides replication progress: a change of it
/// plans at once, anything else waits for the interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PlanInputs {
    state: ServerState,
    membership: Option<LogIdOf<UrsulaRaftTypeConfig>>,
}

/// Leader-side heal driver for one group. While this replica leads, it
/// observes follower log reversions, which OpenRaft repairs in place, and
/// counts each one in `reversions` and logs it. It also brings the
/// membership back to the configured voters (add a missing one as a
/// learner, promote it once caught up, flatten a stale joint config).
/// Returns once the Raft stops.
pub(crate) async fn run_rejoin_heal(
    raft: RaftGroupHandle,
    rejoin: Arc<GroupRejoin>,
    configured: BTreeSet<u64>,
    interval: Duration,
    reversions: FollowerLogReversions,
) {
    // The log id of the joint config seen on the previous tick, if any.
    let mut last_joint = None;
    let mut last_attempt = None;
    let mut joint_since = crate::rt::time::Instant::now();
    let mut watch = ReversionWatch::default();
    let mut planned: Option<(PlanInputs, crate::rt::time::Instant)> = None;
    // A follower wakes on server-state changes only. A leader wakes on every
    // metrics change, replication progress included, so the observer sees a
    // rewound follower's matched index drop before it matches again.
    let mut server = raft.server_metrics();
    let mut metrics = raft.metrics();
    let mut changes = rejoin.changes.subscribe();
    let mut leading = false;
    loop {
        let wake = if leading {
            wait_change(&mut metrics, &mut changes, interval).await
        } else {
            wait_change(&mut server, &mut changes, interval).await
        };
        let Some(wake) = wake else {
            return;
        };
        let (step, reverted) = {
            let current = metrics.borrow_watched();
            if current.running_state.is_err() {
                return;
            }
            leading = current.state == ServerState::Leader;
            let reverted = match current.replication.as_ref() {
                Some(replication) if leading => watch.observe(&current.vote, replication),
                _ => {
                    watch.forget();
                    Vec::new()
                }
            };
            let inputs = PlanInputs {
                state: current.state,
                membership: *current.membership_config.log_id(),
            };
            let due = wake != Wake::Metrics
                || planned
                    .is_none_or(|(previous, at)| previous != inputs || at.elapsed() >= interval);
            let step = if due {
                planned = Some((inputs, crate::rt::time::Instant::now()));
                let mut view = heal_view(&current, &configured);
                let joint = (!view.uniform).then_some(inputs.membership);
                if joint != last_joint {
                    joint_since = crate::rt::time::Instant::now();
                }
                view.stale_joint = joint.is_some() && joint_since.elapsed() >= interval;
                last_joint = joint;
                plan_heal_step(&view)
            } else {
                None
            };
            (step, reverted)
        };
        for reversion in reverted {
            reversions.record(reversion.follower);
            tracing::warn!(
                node_id = rejoin.node_id,
                raft_group_id = rejoin.raft_group_id.0,
                follower = reversion.follower,
                matched_index = reversion.matched,
                now_matched_index = ?reversion.now,
                "recovery: follower lost Raft log entries it had acknowledged; replicating \
                 this leader's log to it again"
            );
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
