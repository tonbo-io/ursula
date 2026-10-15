//! Recovery plans for a stalled group after an accepted loss (#581 section
//! 4.5), over `POST /__ursula/raft/{group}/recovery/accept-unsynced-loss`.
//!
//! The tool reads every reachable replica of the group, computes the plan
//! ([`plan_group`]), and sends it to the plan's leader first. The leader
//! answers its plan term, and every other member takes the plan with it: a
//! member then votes for the leader only in a campaign the leader started
//! after it took the plan. The tool waits until the plan's history event
//! applied on every replica it reached. A plan whose leader lost entries it
//! had replicated wedges (its members' logs are longer than its own), and
//! the tool plans again ([`plan_wedged`]).

use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use reqwest::Method;
use tokio::time::Instant;
use ursula_proto::admin::PlanStep;
use ursula_proto::admin::RaftGroupMetrics;
use ursula_proto::admin::RecoveryGateStatus;
use ursula_proto::admin::RecoveryPlanReport;
use ursula_proto::admin::RecoveryPlanRequest;
use ursula_proto::admin::ReplicaLogView;

use crate::metrics::MetricsClient;
use crate::provider::NodeInfo;

/// One replica of the group as the tool read it from its metrics.
#[derive(Debug, Clone)]
pub struct PlanMember {
    pub node_id: u64,
    pub log: ReplicaLogView,
    pub committed: Option<u64>,
    pub gate: RecoveryGateStatus,
    /// The plan leader this replica holds a plan for, if any.
    pub plan_leader: Option<u64>,
    /// The log index of the history event it applied last.
    pub history_event_index: Option<u64>,
    pub current_leader: Option<u64>,
    pub voter_ids: Vec<u64>,
}

impl PlanMember {
    /// Reads one replica from its group metrics.
    pub fn from_metrics(group: &RaftGroupMetrics) -> Self {
        Self {
            node_id: group.node_id,
            log: ReplicaLogView {
                last_log_index: group.last_log_index,
                last_log_term: group.last_log_term,
                current_term: group.current_term,
            },
            committed: group.committed_index,
            gate: group.recovery_gate,
            plan_leader: group.recovery.as_ref().map(|recovery| recovery.leader),
            history_event_index: group.history.as_ref().map(|history| history.event_index),
            current_leader: group.current_leader,
            voter_ids: group.voter_ids.clone(),
        }
    }

    /// The gate restricts this replica's vote: it kept its vote while it may
    /// have lost entries, so its vote term bounds the terms of entries the
    /// group may have lost (`floor`).
    fn restricted(&self) -> bool {
        self.gate != RecoveryGateStatus::Open || self.plan_leader.is_some()
    }

    /// The last log id an election compares: term first, then index. An
    /// empty log ranks last.
    fn last_log_id(&self) -> (Option<u64>, Option<u64>) {
        (self.log.last_log_term, self.log.last_log_index)
    }
}

/// A recovery plan for one group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupPlan {
    pub leader: u64,
    pub point: u64,
    pub cut: bool,
}

/// Why the tool does not plan a group now.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    #[error("no replica of the group answered")]
    NoReplica,
    #[error(
        "only {answered} of the group's {voters} voters answered; a plan needs a majority of them"
    )]
    NoMajority { answered: usize, voters: usize },
    #[error(
        "node {node_id}'s gate is {status:?}: it may still open through a leader's barrier \
         without losing anything, or it is running a plan's cut; wait and observe again"
    )]
    NotStalled {
        node_id: u64,
        status: RecoveryGateStatus,
    },
    #[error("node {leader} leads the group: it has a live leader and needs no plan")]
    HasLeader { leader: u64 },
}

/// Computes the plan from the replicas that answered (operations guide,
/// "Recovering after a host crash"):
///
/// - `floor` is the highest vote term among the restricted replicas.
/// - The leader is the replica with the greatest last log id, and the
///   plan's point is its last log index, without a cut.
/// - When that leader's last log term is below `floor`, its log may hold a
///   stale entry: every member cuts its log to its committed entry, and the
///   leader is the replica with the greatest committed index.
pub fn plan_group(members: &[PlanMember]) -> Result<GroupPlan, PlanError> {
    let voters = members
        .iter()
        .flat_map(|member| member.voter_ids.iter().copied())
        .collect::<std::collections::BTreeSet<_>>();
    let answered = members
        .iter()
        .filter(|member| voters.contains(&member.node_id))
        .count();
    if members.is_empty() {
        return Err(PlanError::NoReplica);
    }
    if answered.saturating_mul(2) <= voters.len() {
        return Err(PlanError::NoMajority {
            answered,
            voters: voters.len(),
        });
    }
    if let Some(member) = members.iter().find(|member| {
        matches!(
            member.gate,
            RecoveryGateStatus::AwaitingBarrier
                | RecoveryGateStatus::CatchingUp
                | RecoveryGateStatus::Cutting
        )
    }) {
        return Err(PlanError::NotStalled {
            node_id: member.node_id,
            status: member.gate,
        });
    }
    let floor = members
        .iter()
        .filter(|member| member.restricted())
        .map(|member| member.log.current_term)
        .max()
        .unwrap_or(0);
    let longest = members
        .iter()
        .max_by_key(|member| (member.last_log_id(), member.node_id))
        .ok_or(PlanError::NoReplica)?;
    if members.len() > 1 && longest.log.last_log_term.unwrap_or(0) < floor {
        let leader = members
            .iter()
            .max_by_key(|member| (member.committed, member.node_id))
            .ok_or(PlanError::NoReplica)?;
        Ok(GroupPlan {
            leader: leader.node_id,
            point: leader.committed.unwrap_or(0),
            cut: true,
        })
    } else {
        Ok(GroupPlan {
            leader: longest.node_id,
            point: longest.log.last_log_index.unwrap_or(0),
            cut: false,
        })
    }
}

/// Why a plan is wedged and must be planned again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wedge {
    /// A member holding the plan has a longer log than the plan's leader:
    /// the leader lost entries it had replicated, and the member never
    /// votes for it.
    LeaderBehind { member: u64, leader: u64 },
    /// A replica holding the plan applied nothing for the stall timeout.
    Stalled { node_id: u64 },
}

/// Whether the plan electing `leader` is wedged, from the replicas the tool
/// reached.
pub fn plan_wedged(leader: u64, members: &[PlanMember]) -> Option<Wedge> {
    let planned = |member: &&PlanMember| member.plan_leader == Some(leader);
    if let Some(lead) = members
        .iter()
        .filter(planned)
        .find(|member| member.node_id == leader)
        && let Some(member) = members
            .iter()
            .filter(planned)
            .find(|member| member.last_log_id() > lead.last_log_id())
    {
        return Some(Wedge::LeaderBehind {
            member: member.node_id,
            leader,
        });
    }
    members
        .iter()
        .filter(planned)
        .find(|member| member.gate == RecoveryGateStatus::Stalled)
        .map(|member| Wedge::Stalled {
            node_id: member.node_id,
        })
}

/// Whether the plan with `point` is satisfied on every replica the tool
/// reached: each applied a history event above the point and holds no plan,
/// and the group has a leader.
pub fn plan_satisfied(point: u64, members: &[PlanMember]) -> bool {
    !members.is_empty()
        && members.iter().any(|member| member.current_leader.is_some())
        && members.iter().all(|member| {
            member.plan_leader.is_none()
                && member
                    .history_event_index
                    .is_some_and(|index| index > point)
        })
}

/// How [`recover_group`] runs.
#[derive(Debug, Clone)]
pub struct RecoverOptions {
    /// How many plans the tool sends at most, the first included.
    pub max_plans: usize,
    /// How long one plan may take to be satisfied before the tool gives up.
    pub settle_timeout: Duration,
    pub poll_interval: Duration,
    /// Compute and print the plan, and send nothing.
    pub dry_run: bool,
}

/// What [`recover_group`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoverOutcome {
    /// The group needs no plan: it has a leader and no replica holds one.
    Healthy,
    /// The plan the tool would send.
    DryRun(GroupPlan),
    /// The last plan sent was satisfied everywhere the tool reached.
    Satisfied { plan: GroupPlan, plans_sent: usize },
}

/// Reads every reachable replica of `raft_group_id`.
async fn observe(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    raft_group_id: u64,
) -> Vec<(NodeInfo, PlanMember)> {
    let mut members = Vec::new();
    for node in nodes {
        match client.fetch_node(node).await {
            Ok(view) => {
                if let Some(group) = view.group(raft_group_id) {
                    members.push((node.clone(), PlanMember::from_metrics(group)));
                }
            }
            Err(error) => {
                tracing::warn!(node_id = node.id, %error, "recovery: node did not answer");
            }
        }
    }
    members
}

/// Sends one replica its part of the plan.
async fn send(
    client: &MetricsClient,
    node: &NodeInfo,
    raft_group_id: u64,
    request: &RecoveryPlanRequest,
) -> Result<RecoveryPlanReport> {
    let url = node
        .admin_url
        .join(&format!(
            "/__ursula/raft/{raft_group_id}/recovery/accept-unsynced-loss"
        ))
        .with_context(|| format!("compose recovery url at node {}", node.id))?;
    let response = client
        .admin_request(node, Method::POST, url.clone())
        .await?
        .json(request)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        bail!(
            "node {} refused the recovery plan for group {raft_group_id} ({status}): {body}",
            node.id
        );
    }
    serde_json::from_str(&body).with_context(|| format!("decode recovery report: {body}"))
}

/// Sends `plan` to the leader first, then to every other member with the
/// leader's plan term. A refusal of the leader fails the round. A member's
/// refusal does not: the members that took the plan vote only for its
/// leader, which holds it, and a member it reached once elected refuses and
/// rejoins through it. Whether the plan then holds is up to the wait.
async fn send_plan(
    client: &MetricsClient,
    raft_group_id: u64,
    plan: GroupPlan,
    members: &[(NodeInfo, PlanMember)],
) -> Result<u64> {
    let request = |member: &PlanMember, step: PlanStep| RecoveryPlanRequest {
        leader: plan.leader,
        point: plan.point,
        cut: plan.cut,
        step,
        expected: member.log,
    };
    let (leader_node, leader) = members
        .iter()
        .find(|(_, member)| member.node_id == plan.leader)
        .context("the plan's leader did not answer")?;
    let report = send(
        client,
        leader_node,
        raft_group_id,
        &request(leader, PlanStep::Lead),
    )
    .await?;
    let leader_term = report.leader_term;
    for (node, member) in members
        .iter()
        .filter(|(_, member)| member.node_id != plan.leader)
    {
        if let Err(error) = send(
            client,
            node,
            raft_group_id,
            &request(member, PlanStep::Follow { leader_term }),
        )
        .await
        {
            tracing::warn!(
                raft_group_id,
                node_id = node.id,
                %error,
                "recovery: a member refused the plan; its leader holds it"
            );
        }
    }
    Ok(leader_term)
}

/// Plans `raft_group_id` and waits until the plan is satisfied, planning
/// again when a plan wedges or a member refuses, up to
/// [`RecoverOptions::max_plans`] plans.
pub async fn recover_group(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    raft_group_id: u64,
    options: &RecoverOptions,
) -> Result<RecoverOutcome> {
    let mut plans_sent = 0_usize;
    loop {
        let observed = observe(client, nodes, raft_group_id).await;
        let members = observed
            .iter()
            .map(|(_, member)| member.clone())
            .collect::<Vec<_>>();
        if members.iter().all(|member| member.plan_leader.is_none())
            && let Some(leader) = members.iter().find_map(|member| {
                member
                    .current_leader
                    .filter(|leader| *leader == member.node_id)
            })
        {
            if plans_sent == 0 {
                return Err(PlanError::HasLeader { leader }.into());
            }
            return Ok(RecoverOutcome::Healthy);
        }
        let plan = plan_group(&members)?;
        if options.dry_run {
            return Ok(RecoverOutcome::DryRun(plan));
        }
        if plans_sent >= options.max_plans {
            bail!("group {raft_group_id}: {plans_sent} recovery plans sent and none was satisfied");
        }
        plans_sent = plans_sent.saturating_add(1);
        tracing::info!(
            raft_group_id,
            leader = plan.leader,
            point = plan.point,
            cut = plan.cut,
            plans_sent,
            "recovery: sending the plan, the leader first"
        );
        if let Err(error) = send_plan(client, raft_group_id, plan, &observed).await {
            tracing::warn!(
                raft_group_id,
                %error,
                "recovery: the plan's leader refused the plan; observing again"
            );
            tokio::time::sleep(options.poll_interval).await;
            continue;
        }
        let deadline = Instant::now()
            .checked_add(options.settle_timeout)
            .context("settle timeout overflows the clock")?;
        loop {
            tokio::time::sleep(options.poll_interval).await;
            let members = observe(client, nodes, raft_group_id)
                .await
                .into_iter()
                .map(|(_, member)| member)
                .collect::<Vec<_>>();
            if plan_satisfied(plan.point, &members) {
                return Ok(RecoverOutcome::Satisfied { plan, plans_sent });
            }
            if let Some(wedge) = plan_wedged(plan.leader, &members) {
                tracing::warn!(
                    raft_group_id,
                    ?wedge,
                    "recovery: the plan is wedged; planning again"
                );
                break;
            }
            if Instant::now() >= deadline {
                bail!(
                    "group {raft_group_id}: the recovery plan electing node {} was not satisfied \
                     within {:?}",
                    plan.leader,
                    options.settle_timeout
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use ursula_proto::admin::RecoveryGateStatus;
    use ursula_proto::admin::ReplicaLogView;

    use super::GroupPlan;
    use super::PlanError;
    use super::PlanMember;
    use super::Wedge;
    use super::plan_group;
    use super::plan_satisfied;
    use super::plan_wedged;

    fn member(
        node_id: u64,
        last: Option<(u64, u64)>,
        current_term: u64,
        committed: Option<u64>,
        gate: RecoveryGateStatus,
    ) -> PlanMember {
        PlanMember {
            node_id,
            log: ReplicaLogView {
                last_log_index: last.map(|(index, _)| index),
                last_log_term: last.map(|(_, term)| term),
                current_term,
            },
            committed,
            gate,
            plan_leader: None,
            history_event_index: Some(1),
            current_leader: None,
            voter_ids: vec![1, 2, 3],
        }
    }

    #[test]
    fn the_longest_log_leads_without_a_cut() {
        // Node 1 survived open and lags; 2 and 3 lost their tail but hold
        // no vote term above node 1's last log term.
        let members = [
            member(1, Some((12, 3)), 9, Some(12), RecoveryGateStatus::Open),
            member(2, Some((10, 3)), 3, Some(10), RecoveryGateStatus::Stalled),
            member(3, Some((9, 3)), 3, Some(9), RecoveryGateStatus::Stalled),
        ];
        assert_eq!(
            plan_group(&members),
            Ok(GroupPlan {
                leader: 1,
                point: 12,
                cut: false
            })
        );
    }

    #[test]
    fn a_vote_term_above_the_leaders_last_log_term_cuts() {
        let members = [
            member(1, Some((14, 3)), 4, Some(12), RecoveryGateStatus::Open),
            member(2, Some((12, 3)), 5, Some(12), RecoveryGateStatus::Stalled),
            member(3, Some((11, 3)), 5, Some(11), RecoveryGateStatus::Stalled),
        ];
        assert_eq!(
            plan_group(&members),
            Ok(GroupPlan {
                leader: 2,
                point: 12,
                cut: true
            })
        );
    }

    #[test]
    fn a_minority_or_a_barrier_in_flight_is_not_planned() {
        let one = [member(
            1,
            Some((12, 3)),
            3,
            Some(12),
            RecoveryGateStatus::Stalled,
        )];
        assert_eq!(
            plan_group(&one),
            Err(PlanError::NoMajority {
                answered: 1,
                voters: 3
            })
        );
        let catching_up = [
            member(1, Some((12, 3)), 3, Some(12), RecoveryGateStatus::Stalled),
            member(
                2,
                Some((12, 3)),
                3,
                Some(12),
                RecoveryGateStatus::CatchingUp,
            ),
        ];
        assert_eq!(
            plan_group(&catching_up),
            Err(PlanError::NotStalled {
                node_id: 2,
                status: RecoveryGateStatus::CatchingUp
            })
        );
    }

    /// The wedge of #581 section 4.5: the plan's leader lost entries it had
    /// replicated, so its members never elect it. The leader's failed
    /// campaigns raised its vote term, which counts in the floor, so the
    /// next plan cuts every log to its committed entry and elects a member.
    #[test]
    fn a_leader_behind_its_members_wedges_the_plan_and_the_next_plan_elects_a_member() {
        let mut leader = member(1, Some((12, 3)), 8, Some(12), RecoveryGateStatus::LeadsPlan);
        leader.plan_leader = Some(1);
        let mut ahead = member(2, Some((13, 7)), 7, Some(12), RecoveryGateStatus::Planned);
        ahead.plan_leader = Some(1);
        let mut other = member(3, Some((13, 7)), 7, Some(12), RecoveryGateStatus::Planned);
        other.plan_leader = Some(1);
        let members = [leader, ahead, other];
        assert_eq!(
            plan_wedged(1, &members),
            Some(Wedge::LeaderBehind {
                member: 2,
                leader: 1
            })
        );
        assert_eq!(
            plan_group(&members),
            Ok(GroupPlan {
                leader: 3,
                point: 12,
                cut: true
            })
        );
    }

    #[test]
    fn a_stalled_planned_gate_wedges_the_plan() {
        let mut leader = member(1, Some((12, 3)), 8, Some(12), RecoveryGateStatus::LeadsPlan);
        leader.plan_leader = Some(1);
        let mut stalled = member(2, Some((12, 3)), 7, Some(12), RecoveryGateStatus::Stalled);
        stalled.plan_leader = Some(1);
        assert_eq!(
            plan_wedged(1, &[leader.clone(), stalled]),
            Some(Wedge::Stalled { node_id: 2 })
        );
        assert_eq!(plan_wedged(1, &[leader]), None);
    }

    #[test]
    fn a_plan_is_satisfied_once_every_replica_applied_its_event() {
        let mut members = [
            member(1, Some((13, 9)), 9, Some(13), RecoveryGateStatus::Open),
            member(2, Some((13, 9)), 9, Some(13), RecoveryGateStatus::Open),
        ];
        assert!(!plan_satisfied(12, &members), "no leader yet");
        members[0].current_leader = Some(1);
        members[1].current_leader = Some(1);
        assert!(!plan_satisfied(12, &members), "no event above the point");
        members[0].history_event_index = Some(13);
        members[1].history_event_index = Some(13);
        assert!(plan_satisfied(12, &members));
        members[1].plan_leader = Some(1);
        assert!(!plan_satisfied(12, &members), "a replica still holds it");
    }
}
