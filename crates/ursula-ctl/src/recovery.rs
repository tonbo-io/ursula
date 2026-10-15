//! Recovery plans for a stalled group after an accepted loss (#581 section
//! 4.5), over `POST /__ursula/raft/{group}/recovery/accept-unsynced-loss`.
//!
//! The tool reads every reachable replica of the group, computes the plan
//! ([`plan_group`]), and sends it to the plan's leader first with the
//! highest vote term it read ([`send_plan`]). The leader answers its plan
//! term, and every other member takes the plan with it: a member then votes
//! for the leader only in a campaign above the plan term. The tool waits
//! until the plan's history event applied on every replica it reached. A
//! plan whose leader lost entries it had replicated wedges (its members'
//! logs are longer than its own), and the tool plans again
//! ([`plan_wedged`]). The deterministic simulation runs the same planner and
//! sender over its own [`PlanTransport`].

use std::future::Future;
use std::time::Duration;

use reqwest::Method;
use tokio::time::Instant;
use ursula_proto::admin::PlanStep;
use ursula_proto::admin::RaftGroupMetrics;
use ursula_proto::admin::RaftGroupRecovery;
use ursula_proto::admin::RecoveryGateStatus;
use ursula_proto::admin::RecoveryPlanRefusal;
use ursula_proto::admin::RecoveryPlanRefused;
use ursula_proto::admin::RecoveryPlanReport;
use ursula_proto::admin::RecoveryPlanRequest;
use ursula_proto::admin::ReplicaLogView;

use crate::metrics::MetricsClient;
use crate::provider::NodeInfo;

/// One replica of the group as the tool read it from its metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanMember {
    pub node_id: u64,
    pub log: ReplicaLogView,
    pub committed: Option<u64>,
    pub gate: RecoveryGateStatus,
    /// The recovery plan this replica holds, if any.
    pub plan: Option<RaftGroupRecovery>,
    /// The replica leads with a quorum that acknowledged it lately.
    pub leads_live: bool,
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
            plan: group.recovery,
            leads_live: group.leads_live,
            history_event_index: group.history.as_ref().map(|history| history.event_index),
            current_leader: group.current_leader,
            voter_ids: group.voter_ids.clone(),
        }
    }

    /// The leader of the plan this replica holds, if any.
    pub fn plan_leader(&self) -> Option<u64> {
        self.plan.map(|plan| plan.leader)
    }

    /// Whether this replica reports itself the group's leader.
    fn reports_leading(&self) -> bool {
        self.current_leader == Some(self.node_id)
    }

    /// The gate restricts this replica's vote: it kept its vote while it may
    /// have lost entries, so its vote term bounds the terms of entries the
    /// group may have lost (`floor`).
    fn restricted(&self) -> bool {
        self.gate != RecoveryGateStatus::Open || self.plan.is_some()
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
    /// The highest vote term among the replicas the tool reached. The
    /// leader's plan term is at least this, so it is at least every vote
    /// term a plan member holds.
    pub highest_term: u64,
    /// The highest vote term among the restricted replicas: when the
    /// longest voter log's last term is below it, the plan cuts.
    pub floor: u64,
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
    let voters = voters_of(members);
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
    let highest_term = members
        .iter()
        .map(|member| member.log.current_term)
        .max()
        .unwrap_or(0);
    // Only a voter leads: a learner is refused as the plan's leader.
    let candidates = || {
        members
            .iter()
            .filter(|member| voters.contains(&member.node_id))
    };
    let longest = candidates()
        .max_by_key(|member| (member.last_log_id(), member.node_id))
        .ok_or(PlanError::NoReplica)?;
    // A single voter never cuts: no other voter could have overwritten an
    // entry it holds (#581 section 4.5).
    if voters.len() > 1 && longest.log.last_log_term.unwrap_or(0) < floor {
        let leader = candidates()
            .max_by_key(|member| (member.committed, member.node_id))
            .ok_or(PlanError::NoReplica)?;
        Ok(GroupPlan {
            leader: leader.node_id,
            point: leader.committed.unwrap_or(0),
            cut: true,
            highest_term,
            floor,
        })
    } else {
        Ok(GroupPlan {
            leader: longest.node_id,
            point: longest.log.last_log_index.unwrap_or(0),
            cut: false,
            highest_term,
            floor,
        })
    }
}

/// The voters the replicas report: the union of their memberships.
fn voters_of(members: &[PlanMember]) -> std::collections::BTreeSet<u64> {
    members
        .iter()
        .flat_map(|member| member.voter_ids.iter().copied())
        .collect()
}

/// What a dry run shows before an operator accepts the loss.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanExplanation {
    pub plan: GroupPlan,
    /// The longest voter log's last term: the plan cuts when it is below
    /// [`GroupPlan::floor`].
    pub longest_last_log_term: Option<u64>,
    /// Each replica the tool reached, with the log the plan expects of it.
    pub members: Vec<PlanMember>,
    /// The voters that did not answer: the plan goes ahead without them.
    pub unreached_voters: Vec<u64>,
}

/// Explains `plan` over the replicas it was computed from.
pub fn explain_plan(plan: GroupPlan, members: &[PlanMember]) -> PlanExplanation {
    let voters = voters_of(members);
    let longest = members
        .iter()
        .filter(|member| voters.contains(&member.node_id))
        .max_by_key(|member| (member.last_log_id(), member.node_id));
    PlanExplanation {
        plan,
        longest_last_log_term: longest.and_then(|member| member.log.last_log_term),
        members: members.to_vec(),
        unreached_voters: voters
            .into_iter()
            .filter(|voter| !members.iter().any(|member| member.node_id == *voter))
            .collect(),
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
/// reached. One observation reads the replicas one after another, so a
/// caller acts only on a wedge it saw in two observations in a row.
pub fn plan_wedged(leader: u64, members: &[PlanMember]) -> Option<Wedge> {
    // A member whose cut has not run yet is not behind or stalled: its
    // restart is under way.
    let planned = |member: &&PlanMember| {
        member.plan_leader() == Some(leader) && !member.plan.is_some_and(|plan| plan.cut_pending)
    };
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
            member.plan.is_none()
                && member
                    .history_event_index
                    .is_some_and(|index| index > point)
        })
}

/// Why one replica did not take its part of a plan.
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    /// The replica refused it and changed nothing.
    #[error("node {node_id} refused the recovery plan: {}", .refused.message)]
    Refused {
        node_id: u64,
        refused: RecoveryPlanRefused,
    },
    /// The plan names a leader the tool did not observe.
    #[error("the plan's leader, node {leader}, did not answer the observation")]
    LeaderNotObserved { leader: u64 },
    /// The node is not in the manifest.
    #[error("node {node_id} is not in the manifest")]
    NotInManifest { node_id: u64 },
    /// The node's admin URL does not take the route.
    #[error("node {node_id}'s admin URL: {source}")]
    Url {
        node_id: u64,
        #[source]
        source: url::ParseError,
    },
    /// The node's pinned process identity could not be used.
    #[error("node {node_id}'s process identity: {source}")]
    Identity {
        node_id: u64,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// The request did not complete: its outcome is unknown.
    #[error("node {node_id} did not answer the recovery plan: {source}")]
    Http {
        node_id: u64,
        #[source]
        source: reqwest::Error,
    },
    /// The node answered something other than a report or a refusal.
    #[error("node {node_id} answered {status}: {body}")]
    Status {
        node_id: u64,
        status: u16,
        body: String,
    },
    /// The node's answer could not be read.
    #[error("node {node_id}'s answer: {source}")]
    Decode {
        node_id: u64,
        #[source]
        source: serde_json::Error,
    },
    /// The replica could not take the plan for another reason, such as a
    /// failed write: its outcome is unknown.
    #[error("node {node_id} failed to take the recovery plan: {source}")]
    Failed {
        node_id: u64,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl SendError {
    /// The refusal, when the replica refused.
    pub fn refusal(&self) -> Option<RecoveryPlanRefusal> {
        match self {
            Self::Refused { refused, .. } => Some(refused.refusal),
            Self::LeaderNotObserved { .. }
            | Self::NotInManifest { .. }
            | Self::Url { .. }
            | Self::Identity { .. }
            | Self::Http { .. }
            | Self::Status { .. }
            | Self::Decode { .. }
            | Self::Failed { .. } => None,
        }
    }

    /// The replica the error came from, when it names one.
    pub fn node_id(&self) -> Option<u64> {
        match self {
            Self::Refused { node_id, .. }
            | Self::NotInManifest { node_id }
            | Self::Url { node_id, .. }
            | Self::Identity { node_id, .. }
            | Self::Http { node_id, .. }
            | Self::Status { node_id, .. }
            | Self::Decode { node_id, .. }
            | Self::Failed { node_id, .. } => Some(*node_id),
            Self::LeaderNotObserved { .. } => None,
        }
    }

    /// Whether the replica may have taken the plan although the tool got
    /// no answer: a refusal changed nothing, and an unsent request did not
    /// reach it.
    fn outcome_unknown(&self) -> bool {
        match self {
            Self::Http { .. } | Self::Status { .. } | Self::Decode { .. } | Self::Failed { .. } => {
                true
            }
            Self::Refused { .. }
            | Self::LeaderNotObserved { .. }
            | Self::NotInManifest { .. }
            | Self::Url { .. }
            | Self::Identity { .. } => false,
        }
    }
}

/// How the tool reaches one replica's `accept-unsynced-loss`.
pub trait PlanTransport {
    fn send(
        &mut self,
        node_id: u64,
        request: &RecoveryPlanRequest,
    ) -> impl Future<Output = Result<RecoveryPlanReport, SendError>>;
}

/// What [`send_plan`] did.
#[derive(Debug)]
pub struct PlanSent {
    /// The plan term the leader answered.
    pub leader_term: u64,
    /// The members that took the plan, the leader included.
    pub taken: Vec<u64>,
    /// The members that refused for another reason than following the
    /// plan's leader, which it elected meanwhile.
    pub refused: Vec<SendError>,
}

/// Sends `plan` to its leader first, with the highest vote term the tool
/// read, then to every other member with the leader's plan term
/// ([`send_follows`]). A refusal of the leader fails the round, and nothing
/// changed.
pub async fn send_plan<T: PlanTransport>(
    transport: &mut T,
    plan: GroupPlan,
    members: &[PlanMember],
) -> Result<PlanSent, SendError> {
    let leader = members
        .iter()
        .find(|member| member.node_id == plan.leader)
        .ok_or(SendError::LeaderNotObserved {
            leader: plan.leader,
        })?;
    let report = transport
        .send(plan.leader, &RecoveryPlanRequest {
            leader: plan.leader,
            point: plan.point,
            cut: plan.cut,
            step: PlanStep::Lead {
                highest_term: plan.highest_term,
            },
            expected: leader.log,
        })
        .await?;
    Ok(send_follows(transport, plan, report.leader_term, members).await)
}

/// Sends `plan` to every member but its leader with the plan term
/// `leader_term`, which the leader answered or recorded. A member's refusal
/// does not undo the leader's plan. A member that follows the plan's leader
/// refuses because the members that took the plan elected it, and rejoins
/// through it (m3, #581 review): it is skipped. Every other refusal is
/// returned.
pub async fn send_follows<T: PlanTransport>(
    transport: &mut T,
    plan: GroupPlan,
    leader_term: u64,
    members: &[PlanMember],
) -> PlanSent {
    let mut taken = vec![plan.leader];
    let mut refused = Vec::new();
    for member in members
        .iter()
        .filter(|member| member.node_id != plan.leader)
    {
        let request = RecoveryPlanRequest {
            leader: plan.leader,
            point: plan.point,
            cut: plan.cut,
            step: PlanStep::Follow { leader_term },
            expected: member.log,
        };
        match transport.send(member.node_id, &request).await {
            Ok(_report) => taken.push(member.node_id),
            Err(error)
                if error.refusal()
                    == Some(RecoveryPlanRefusal::FollowsLeader {
                        leader: plan.leader,
                    }) =>
            {
                tracing::info!(
                    node_id = member.node_id,
                    leader = plan.leader,
                    "recovery: a member follows the plan's leader already"
                );
                taken.push(member.node_id);
            }
            Err(error) => refused.push(error),
        }
    }
    PlanSent {
        leader_term,
        taken,
        refused,
    }
}

/// How [`recover_group`] runs.
#[derive(Debug, Clone)]
pub struct RecoverOptions {
    /// How many plans the tool sends at most, the first included.
    pub max_plans: usize,
    /// How long the tool waits for a stalled group, or for one plan to be
    /// satisfied, before it gives up.
    pub settle_timeout: Duration,
    pub poll_interval: Duration,
    /// How long one `accept-unsynced-loss` may take: a cut or a demotion
    /// restarts the group before the answer.
    pub accept_timeout: Duration,
    /// Send the plan, accepting the loss of every write acknowledged after
    /// the last `fsync` of the replicas it keeps. Without it the tool
    /// computes and explains the plan, and sends nothing.
    pub accept_loss: bool,
}

/// What [`recover_group`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoverOutcome {
    /// The group needs no plan: it has a leader and no replica holds one.
    Healthy,
    /// The plan the tool would send, explained, without `accept_loss`.
    DryRun(PlanExplanation),
    /// The last plan sent was satisfied everywhere the tool reached.
    Satisfied { plan: GroupPlan, plans_sent: usize },
}

/// Why [`recover_group`] stopped without a satisfied plan.
#[derive(Debug, thiserror::Error)]
pub enum RecoverError {
    #[error(transparent)]
    Plan(#[from] PlanError),
    /// A replica refused for a reason no plan fixes.
    #[error("no recovery plan can fix this: {source}")]
    Unfixable {
        #[source]
        source: SendError,
    },
    #[error("{plans_sent} recovery plans sent and none was satisfied")]
    MaxPlans { plans_sent: usize },
    #[error("nothing was settled within {settle_timeout:?}")]
    Timeout { settle_timeout: Duration },
}

/// The last plan the tool sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SentPlan {
    pub plan: GroupPlan,
    /// The plan term the leader answered. `None` while the answer to `lead`
    /// is unknown, until the leader's recorded plan shows it.
    pub leader_term: Option<u64>,
}

/// What the planning loop knows between observations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoopState {
    pub sent: Option<SentPlan>,
    pub plans_sent: usize,
    /// The plan must be planned again, once the group has no leader.
    pub replan: bool,
    /// The last observation saw the plan wedged.
    pub wedged_before: bool,
    /// Replicas that refused a plan for a reason no plan fixes: the next
    /// plan leaves them out.
    pub excluded: std::collections::BTreeSet<u64>,
    /// A replica refused a plan because it follows this live leader, which
    /// the tool may not reach: the tool waits instead of planning again.
    pub follows_leader: Option<u64>,
}

/// What the planning loop does after one observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// The last plan was satisfied, or the group needs none.
    Done(RecoverOutcome),
    /// Observe again after the poll interval.
    Wait,
    /// Send this new plan.
    Plan(GroupPlan),
    /// The leader recorded the last plan, whose answer was lost: send it to
    /// the other members with the recorded plan term.
    Resume { plan: GroupPlan, leader_term: u64 },
}

/// The planning loop's decision for one observation of the replicas the
/// tool reached (m1 and m2, #581 review). Pure, so it is tested alone.
pub fn next_step(
    state: &mut LoopState,
    members: &[PlanMember],
    max_plans: usize,
) -> Result<Step, RecoverError> {
    if let Some(sent) = state.sent
        && plan_satisfied(sent.plan.point, members)
    {
        return Ok(Step::Done(RecoverOutcome::Satisfied {
            plan: sent.plan,
            plans_sent: state.plans_sent,
        }));
    }
    // The answer to `lead` was lost: the leader's record is the answer.
    if let Some(sent) = state.sent
        && sent.leader_term.is_none()
    {
        let leader = members
            .iter()
            .find(|member| member.node_id == sent.plan.leader);
        if let Some(recorded) = leader.and_then(|leader| leader.plan)
            && recorded.leader == sent.plan.leader
            && recorded.point == sent.plan.point
        {
            return Ok(Step::Resume {
                plan: sent.plan,
                leader_term: recorded.leader_term,
            });
        }
        if leader.is_some_and(|leader| leader.gate == RecoveryGateStatus::Cutting) {
            return Ok(Step::Wait);
        }
        // It did not take the plan: plan again.
        state.sent = None;
    }
    let holds_plan = members.iter().any(|member| member.plan.is_some());
    // A replica follows a live leader the tool did not reach, or said so
    // in a refusal: the group has a leader, and a plan would strand the
    // replicas that take it from that leader's replication.
    let follows_unreached = |member: &PlanMember| {
        member.gate == RecoveryGateStatus::Open
            && member.current_leader.is_some_and(|leader| {
                leader != member.node_id && !members.iter().any(|reached| reached.node_id == leader)
            })
    };
    if let Some(leader) = state.follows_leader {
        if members
            .iter()
            .any(|member| member.current_leader == Some(leader))
        {
            return Ok(Step::Wait);
        }
        state.follows_leader = None;
    }
    if members.iter().any(follows_unreached) {
        return Ok(Step::Wait);
    }
    // m2(a): only a leader whose quorum acknowledged it lately leads.
    if let Some(leader) = members
        .iter()
        .find(|member| member.reports_leading() && member.leads_live)
    {
        if state.sent.is_none() && !holds_plan {
            return Err(PlanError::HasLeader {
                leader: leader.node_id,
            }
            .into());
        }
        if !holds_plan {
            return Ok(Step::Done(RecoverOutcome::Healthy));
        }
        return Ok(Step::Wait);
    }
    // m1: a replica that leads above the last plan's term was elected
    // under it. A wedge seen under it is a partition, not a dead plan. The
    // plan terms replicas report count too, from plans of another run.
    let last_plan_term = state
        .sent
        .and_then(|sent| sent.leader_term)
        .into_iter()
        .chain(
            members
                .iter()
                .filter_map(|member| member.plan.map(|plan| plan.leader_term)),
        )
        .max();
    if let Some(term) = last_plan_term
        && members
            .iter()
            .any(|member| member.reports_leading() && member.log.current_term > term)
    {
        state.wedged_before = false;
        return Ok(Step::Wait);
    }
    let wedge = state
        .sent
        .and_then(|sent| plan_wedged(sent.plan.leader, members));
    match wedge {
        Some(wedge) if state.wedged_before => {
            tracing::warn!(?wedge, "recovery: the plan is wedged");
            state.replan = true;
        }
        Some(_) => {
            state.wedged_before = true;
            return Ok(Step::Wait);
        }
        None => state.wedged_before = false,
    }
    if state.sent.is_some() && !state.replan {
        return Ok(Step::Wait);
    }
    // m2(c): the intent to plan again holds through a wait for a gate
    // that may still open. Replicas that refused for good are left out.
    let candidates = members
        .iter()
        .filter(|member| !state.excluded.contains(&member.node_id))
        .cloned()
        .collect::<Vec<_>>();
    let plan = match plan_group(&candidates) {
        Ok(plan) => plan,
        Err(PlanError::NotStalled { .. }) => return Ok(Step::Wait),
        Err(error) => return Err(error.into()),
    };
    if state.plans_sent >= max_plans {
        return Err(RecoverError::MaxPlans {
            plans_sent: state.plans_sent,
        });
    }
    state.replan = false;
    state.wedged_before = false;
    Ok(Step::Plan(plan))
}

/// Records what sending `plan` did in `state`: the plan term and the
/// members that took the plan, or the leader's error. Only the leader's
/// refusal that no plan fixes stops the tool; a member that refuses so is
/// left out of the next plan, and a plan its members can elect stands.
pub fn plan_sent(
    state: &mut LoopState,
    plan: GroupPlan,
    sent: Result<PlanSent, SendError>,
    voters: usize,
) -> Result<(), RecoverError> {
    match sent {
        Ok(sent) => {
            state.sent = Some(SentPlan {
                plan,
                leader_term: Some(sent.leader_term),
            });
            for error in &sent.refused {
                match error.refusal() {
                    Some(RecoveryPlanRefusal::FollowsLeader { leader }) => {
                        state.follows_leader = Some(leader);
                    }
                    Some(refusal) if refusal.no_plan_fixes() => {
                        if let Some(node_id) = error.node_id() {
                            state.excluded.insert(node_id);
                        }
                    }
                    Some(_) | None => {}
                }
            }
            // m2(b): members that took the plan and form a majority with its
            // leader elect it; only a plan that cannot win is planned again,
            // and not while a replica follows another live leader.
            if sent.taken.len().saturating_mul(2) <= voters && state.follows_leader.is_none() {
                state.replan = true;
            }
            Ok(())
        }
        Err(error)
            if error
                .refusal()
                .is_some_and(|refusal| refusal.no_plan_fixes()) =>
        {
            Err(RecoverError::Unfixable { source: error })
        }
        Err(error) => match error.refusal() {
            Some(RecoveryPlanRefusal::FollowsLeader { leader }) => {
                tracing::warn!(%error, "recovery: the plan's leader follows a live leader");
                state.follows_leader = Some(leader);
                Ok(())
            }
            // Another job runs on the leader: wait, and count no plan.
            Some(RecoveryPlanRefusal::PlanInFlight) => {
                state.plans_sent = state.plans_sent.saturating_sub(1);
                state.replan = true;
                Ok(())
            }
            // m2(d): the leader may have taken the plan: its record says.
            None if error.outcome_unknown() => {
                tracing::warn!(
                    %error,
                    "recovery: the plan's leader did not answer; reading its record"
                );
                state.sent = Some(SentPlan {
                    plan,
                    leader_term: None,
                });
                Ok(())
            }
            Some(_) | None => {
                tracing::warn!(%error, "recovery: the plan's leader refused the plan");
                state.replan = true;
                Ok(())
            }
        },
    }
}

/// Reads every reachable replica of `raft_group_id`.
async fn observe(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    raft_group_id: u64,
) -> Vec<PlanMember> {
    let mut members = Vec::new();
    for node in nodes {
        match client.fetch_node(node).await {
            Ok(view) => {
                if let Some(group) = view.group(raft_group_id) {
                    members.push(PlanMember::from_metrics(group));
                }
            }
            Err(error) => {
                tracing::warn!(node_id = node.id, %error, "recovery: node did not answer");
            }
        }
    }
    members
}

/// [`PlanTransport`] over the nodes' admin planes.
struct HttpTransport<'a> {
    client: &'a MetricsClient,
    nodes: &'a [NodeInfo],
    raft_group_id: u64,
    timeout: Duration,
}

impl HttpTransport<'_> {
    async fn post(
        &self,
        node_id: u64,
        request: &RecoveryPlanRequest,
    ) -> Result<RecoveryPlanReport, SendError> {
        let node = self
            .nodes
            .iter()
            .find(|node| node.id == node_id)
            .ok_or(SendError::NotInManifest { node_id })?;
        let url = node
            .admin_url
            .join(&format!(
                "/__ursula/raft/{}/recovery/accept-unsynced-loss",
                self.raft_group_id
            ))
            .map_err(|source| SendError::Url { node_id, source })?;
        let response = self
            .client
            .admin_request(node, Method::POST, url)
            .await
            .map_err(|source| SendError::Identity {
                node_id,
                source: source.into(),
            })?
            .timeout(self.timeout)
            .json(request)
            .send()
            .await
            .map_err(|source| SendError::Http { node_id, source })?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|source| SendError::Http { node_id, source })?;
        if status == reqwest::StatusCode::CONFLICT {
            let refused: RecoveryPlanRefused = serde_json::from_slice(&body)
                .map_err(|source| SendError::Decode { node_id, source })?;
            return Err(SendError::Refused { node_id, refused });
        }
        if !status.is_success() {
            return Err(SendError::Status {
                node_id,
                status: status.as_u16(),
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }
        serde_json::from_slice(&body).map_err(|source| SendError::Decode { node_id, source })
    }
}

impl PlanTransport for HttpTransport<'_> {
    fn send(
        &mut self,
        node_id: u64,
        request: &RecoveryPlanRequest,
    ) -> impl Future<Output = Result<RecoveryPlanReport, SendError>> {
        self.post(node_id, request)
    }
}

/// Plans `raft_group_id` and waits until the plan is satisfied, as
/// [`next_step`] decides on every observation, up to
/// [`RecoverOptions::max_plans`] plans.
pub async fn recover_group(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    raft_group_id: u64,
    options: &RecoverOptions,
) -> Result<RecoverOutcome, RecoverError> {
    let mut transport = HttpTransport {
        client,
        nodes,
        raft_group_id,
        timeout: options.accept_timeout,
    };
    let mut state = LoopState::default();
    let settle = |now: Instant| now.checked_add(options.settle_timeout).unwrap_or(now);
    let mut deadline = settle(Instant::now());
    loop {
        let members = observe(client, nodes, raft_group_id).await;
        let voters = members
            .iter()
            .flat_map(|member| member.voter_ids.iter().copied())
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        match next_step(&mut state, &members, options.max_plans)? {
            Step::Done(outcome) => return Ok(outcome),
            Step::Wait => {}
            Step::Plan(plan) if !options.accept_loss => {
                return Ok(RecoverOutcome::DryRun(explain_plan(plan, &members)));
            }
            Step::Plan(plan) => {
                state.plans_sent = state.plans_sent.saturating_add(1);
                tracing::info!(
                    raft_group_id,
                    leader = plan.leader,
                    point = plan.point,
                    cut = plan.cut,
                    highest_term = plan.highest_term,
                    plans_sent = state.plans_sent,
                    "recovery: sending the plan, the leader first"
                );
                deadline = settle(Instant::now());
                let sent = send_plan(&mut transport, plan, &members).await;
                if let Ok(sent) = &sent {
                    for error in &sent.refused {
                        tracing::warn!(raft_group_id, %error, "recovery: a member refused the plan");
                    }
                }
                plan_sent(&mut state, plan, sent, voters)?;
            }
            Step::Resume { plan, leader_term } => {
                tracing::info!(
                    raft_group_id,
                    leader_term,
                    "recovery: the plan's leader recorded the plan; sending it to the members"
                );
                let sent = send_follows(&mut transport, plan, leader_term, &members).await;
                plan_sent(&mut state, plan, Ok(sent), voters)?;
            }
        }
        if Instant::now() >= deadline {
            return Err(RecoverError::Timeout {
                settle_timeout: options.settle_timeout,
            });
        }
        tokio::time::sleep(options.poll_interval).await;
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
            plan: None,
            leads_live: false,
            history_event_index: Some(1),
            current_leader: None,
            voter_ids: vec![1, 2, 3],
        }
    }

    /// A plan for leader `leader` at point 12, plan term 9.
    fn planned(leader: u64) -> ursula_proto::admin::RaftGroupRecovery {
        ursula_proto::admin::RaftGroupRecovery {
            leader,
            point: 12,
            leader_term: 9,
            cut_pending: false,
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
                cut: false,
                highest_term: 9,
                floor: 3
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
                cut: true,
                highest_term: 5,
                floor: 5
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
        leader.plan = Some(planned(1));
        let mut ahead = member(2, Some((13, 7)), 7, Some(12), RecoveryGateStatus::Planned);
        ahead.plan = Some(planned(1));
        let mut other = member(3, Some((13, 7)), 7, Some(12), RecoveryGateStatus::Planned);
        other.plan = Some(planned(1));
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
                cut: true,
                highest_term: 8,
                floor: 8
            })
        );
    }

    #[test]
    fn a_stalled_planned_gate_wedges_the_plan() {
        let mut leader = member(1, Some((12, 3)), 8, Some(12), RecoveryGateStatus::LeadsPlan);
        leader.plan = Some(planned(1));
        let mut stalled = member(2, Some((12, 3)), 7, Some(12), RecoveryGateStatus::Stalled);
        stalled.plan = Some(planned(1));
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
        members[1].plan = Some(planned(1));
        assert!(!plan_satisfied(12, &members), "a replica still holds it");
    }

    /// A transport whose replicas answer from a table.
    struct Table(std::collections::BTreeMap<u64, Option<ursula_proto::admin::RecoveryPlanRefusal>>);

    impl super::PlanTransport for Table {
        fn send(
            &mut self,
            node_id: u64,
            request: &ursula_proto::admin::RecoveryPlanRequest,
        ) -> impl std::future::Future<
            Output = Result<ursula_proto::admin::RecoveryPlanReport, super::SendError>,
        > {
            let answer = match self.0.get(&node_id).copied().flatten() {
                None => Ok(ursula_proto::admin::RecoveryPlanReport {
                    raft_group_id: 0,
                    node_id,
                    outcome: ursula_proto::admin::RecoveryPlanOutcome::Follows,
                    leader: request.leader,
                    point: request.point,
                    leader_term: match request.step {
                        ursula_proto::admin::PlanStep::Lead { highest_term } => highest_term,
                        ursula_proto::admin::PlanStep::Follow { leader_term } => leader_term,
                    },
                }),
                Some(refusal) => Err(super::SendError::Refused {
                    node_id,
                    refused: ursula_proto::admin::RecoveryPlanRefused {
                        raft_group_id: 0,
                        node_id,
                        refusal,
                        message: String::new(),
                    },
                }),
            };
            std::future::ready(answer)
        }
    }

    /// m3 (#581 review): only a member that follows the plan's leader is
    /// skipped; any other refusal is returned so the tool plans again, and
    /// the leader's refusal fails the round.
    #[tokio::test]
    async fn only_a_member_following_the_plans_leader_is_skipped() {
        use ursula_proto::admin::RecoveryPlanRefusal;
        let members = [
            member(1, Some((12, 3)), 9, Some(12), RecoveryGateStatus::Open),
            member(2, Some((10, 3)), 3, Some(10), RecoveryGateStatus::Stalled),
            member(3, Some((9, 3)), 3, Some(9), RecoveryGateStatus::Stalled),
        ];
        let plan = plan_group(&members).expect("a plan");
        let mut table = Table(
            [
                (2, Some(RecoveryPlanRefusal::FollowsLeader { leader: 1 })),
                (3, Some(RecoveryPlanRefusal::ReplicaChanged)),
            ]
            .into(),
        );
        let sent = super::send_plan(&mut table, plan, &members)
            .await
            .expect("the leader took it");
        assert_eq!(sent.leader_term, 9);
        assert_eq!(sent.refused.len(), 1);
        assert_eq!(
            sent.refused[0].refusal(),
            Some(RecoveryPlanRefusal::ReplicaChanged)
        );
        // A member following another leader is not skipped.
        table
            .0
            .insert(3, Some(RecoveryPlanRefusal::FollowsLeader { leader: 2 }));
        let sent = super::send_plan(&mut table, plan, &members)
            .await
            .expect("the leader took it");
        assert_eq!(sent.refused.len(), 1);
        table.0.insert(1, Some(RecoveryPlanRefusal::LeadsLive));
        let refused = super::send_plan(&mut table, plan, &members)
            .await
            .expect_err("the leader refused");
        assert_eq!(refused.refusal(), Some(RecoveryPlanRefusal::LeadsLive));
    }

    /// A stalled three-voter group: node 1 survived open with the longest
    /// log, 2 and 3 lost their tail.
    fn stalled() -> Vec<PlanMember> {
        vec![
            member(1, Some((12, 3)), 9, Some(12), RecoveryGateStatus::Open),
            member(2, Some((10, 3)), 3, Some(10), RecoveryGateStatus::Stalled),
            member(3, Some((9, 3)), 3, Some(9), RecoveryGateStatus::Stalled),
        ]
    }

    fn sent(plan: super::GroupPlan, leader_term: Option<u64>) -> super::LoopState {
        super::LoopState {
            sent: Some(super::SentPlan { plan, leader_term }),
            plans_sent: 1,
            ..Default::default()
        }
    }

    fn refused(
        node_id: u64,
        refusal: ursula_proto::admin::RecoveryPlanRefusal,
    ) -> super::SendError {
        super::SendError::Refused {
            node_id,
            refused: ursula_proto::admin::RecoveryPlanRefused {
                raft_group_id: 0,
                node_id,
                refusal,
                message: String::new(),
            },
        }
    }

    /// m2(a): a leader that lost its quorum stays a leader in its own
    /// metrics; it does not stop the tool. A live one does.
    #[test]
    fn only_a_live_leader_stops_the_tool() {
        let mut members = stalled();
        members[0].current_leader = Some(1);
        let mut state = super::LoopState::default();
        let step = super::next_step(&mut state, &members, 3).expect("a step");
        assert!(
            matches!(step, super::Step::Plan(plan) if plan.leader == 1),
            "{step:?}"
        );
        members[0].leads_live = true;
        let mut state = super::LoopState::default();
        assert!(matches!(
            super::next_step(&mut state, &members, 3),
            Err(super::RecoverError::Plan(PlanError::HasLeader {
                leader: 1
            }))
        ));
    }

    /// m1: while a replica leads above the last plan's term, which only a
    /// leader elected under the plan does, the tool plans no more, even
    /// when a member reports the plan wedged twice.
    #[test]
    fn no_plan_while_a_replica_leads_above_the_last_plans_term() {
        let plan = plan_group(&stalled()).expect("a plan");
        let mut members = stalled();
        members[0].current_leader = Some(1);
        members[0].log.current_term = 10;
        members[0].plan = Some(planned(1));
        members[2].plan = Some(planned(1));
        members[2].gate = RecoveryGateStatus::Stalled;
        let mut state = sent(plan, Some(9));
        for _ in 0..3 {
            assert_eq!(
                super::next_step(&mut state, &members, 3).expect("a step"),
                super::Step::Wait
            );
        }
        // Without that leader, the wedge seen twice plans again.
        members[0].current_leader = None;
        members[0].log.current_term = 9;
        assert_eq!(
            super::next_step(&mut state, &members, 3).expect("a step"),
            super::Step::Wait
        );
        assert!(matches!(
            super::next_step(&mut state, &members, 3).expect("a step"),
            super::Step::Plan(_)
        ));
    }

    /// m2(b): the leader's refusal that no plan fixes stops the tool,
    /// typed. A member's refusal plans again only when the members that
    /// took the plan cannot elect its leader.
    #[test]
    fn refusals_no_plan_fixes_stop_the_tool_and_others_replan_only_without_a_majority() {
        use ursula_proto::admin::RecoveryPlanRefusal;
        let plan = plan_group(&stalled()).expect("a plan");
        // The plan's leader refusing for good stops the tool.
        let mut state = super::LoopState::default();
        assert!(matches!(
            super::plan_sent(
                &mut state,
                plan,
                Err(refused(1, RecoveryPlanRefusal::LeaderNotVoter)),
                3
            ),
            Err(super::RecoverError::Unfixable { .. })
        ));
        // Leader and one member are a majority of three: wait for it.
        let mut state = super::LoopState::default();
        super::plan_sent(
            &mut state,
            plan,
            Ok(super::PlanSent {
                leader_term: 9,
                taken: vec![1, 2],
                refused: vec![refused(3, RecoveryPlanRefusal::ReplicaChanged)],
            }),
            3,
        )
        .expect("recorded");
        assert!(!state.replan);
        // The leader alone is not: plan again.
        let mut state = super::LoopState::default();
        super::plan_sent(
            &mut state,
            plan,
            Ok(super::PlanSent {
                leader_term: 9,
                taken: vec![1],
                refused: vec![
                    refused(2, RecoveryPlanRefusal::ReplicaChanged),
                    refused(3, RecoveryPlanRefusal::ReplicaChanged),
                ],
            }),
            3,
        )
        .expect("recorded");
        assert!(state.replan);
    }

    /// m2(c): waiting for a gate that may still open keeps the intent to
    /// plan again.
    #[test]
    fn a_wait_for_a_gate_that_may_still_open_keeps_the_intent_to_plan_again() {
        let plan = plan_group(&stalled()).expect("a plan");
        let mut state = sent(plan, Some(9));
        state.replan = true;
        let mut members = stalled();
        members[2].gate = RecoveryGateStatus::AwaitingBarrier;
        assert_eq!(
            super::next_step(&mut state, &members, 3).expect("a step"),
            super::Step::Wait
        );
        assert!(state.replan);
        members[2].gate = RecoveryGateStatus::Stalled;
        assert!(matches!(
            super::next_step(&mut state, &members, 3).expect("a step"),
            super::Step::Plan(_)
        ));
        assert!(!state.replan);
    }

    /// m2(d): a `lead` whose answer was lost is resumed from the leader's
    /// recorded plan, waited for while it restarts, and planned again only
    /// when it recorded nothing.
    #[test]
    fn a_lead_without_an_answer_resumes_from_the_leaders_record() {
        let plan = plan_group(&stalled()).expect("a plan");
        let mut state = super::LoopState::default();
        let timeout = super::SendError::Status {
            node_id: 1,
            status: 504,
            body: String::new(),
        };
        super::plan_sent(&mut state, plan, Err(timeout), 3).expect("recorded");
        let mut members = stalled();
        members[0].gate = RecoveryGateStatus::Cutting;
        assert_eq!(
            super::next_step(&mut state, &members, 3).expect("a step"),
            super::Step::Wait
        );
        members[0].gate = RecoveryGateStatus::LeadsPlan;
        members[0].plan = Some(ursula_proto::admin::RaftGroupRecovery {
            leader: 1,
            point: plan.point,
            leader_term: 11,
            cut_pending: false,
        });
        assert_eq!(
            super::next_step(&mut state, &members, 3).expect("a step"),
            super::Step::Resume {
                plan,
                leader_term: 11
            }
        );
        // Nothing recorded and not restarting: it never took the plan.
        let mut state = super::LoopState::default();
        super::plan_sent(
            &mut state,
            plan,
            Err(super::SendError::Status {
                node_id: 1,
                status: 503,
                body: String::new(),
            }),
            3,
        )
        .expect("recorded");
        assert!(matches!(
            super::next_step(&mut state, &stalled(), 3).expect("a step"),
            super::Step::Plan(_)
        ));
    }

    /// Item 3 of the #612 review: a learner never leads, even with the
    /// longest log, and a group with one voter never cuts.
    #[test]
    fn a_learner_never_leads_and_one_voter_never_cuts() {
        let mut learner = member(4, Some((20, 9)), 9, Some(20), RecoveryGateStatus::Open);
        learner.voter_ids = vec![1, 2, 3];
        let mut members = stalled();
        members.push(learner);
        assert_eq!(plan_group(&members).expect("a plan").leader, 1);

        // One voter, gated, whose log ends below the floor a learner's vote
        // term sets: no cut.
        let mut voter = member(1, Some((12, 3)), 3, Some(10), RecoveryGateStatus::Stalled);
        voter.voter_ids = vec![1];
        let mut learner = member(2, Some((12, 3)), 7, Some(12), RecoveryGateStatus::Stalled);
        learner.voter_ids = vec![1];
        let plan = plan_group(&[voter, learner]).expect("a plan");
        assert_eq!(plan.leader, 1);
        assert!(!plan.cut, "{plan:?}");
        assert_eq!(plan.point, 12);
    }

    /// Item 2: the dry run names the floor, the longest log, each member's
    /// expected log and the voters that did not answer.
    #[test]
    fn a_dry_run_explains_the_plan() {
        let mut members = stalled();
        members.pop();
        let plan = plan_group(&members).expect("a plan");
        let explained = super::explain_plan(plan, &members);
        assert_eq!(explained.plan, plan);
        assert_eq!(explained.longest_last_log_term, Some(3));
        assert_eq!(explained.members, members);
        assert_eq!(explained.unreached_voters, vec![3]);
    }

    /// Item 4: a member's refusal that no plan fixes does not abort a plan
    /// a majority took; the member is left out of the next plan.
    #[test]
    fn a_permanent_member_refusal_leaves_the_member_out_and_keeps_a_plan_a_majority_took() {
        use ursula_proto::admin::RecoveryPlanRefusal;
        let plan = plan_group(&stalled()).expect("a plan");
        let mut state = super::LoopState::default();
        super::plan_sent(
            &mut state,
            plan,
            Ok(super::PlanSent {
                leader_term: 9,
                taken: vec![1, 2],
                refused: vec![refused(3, RecoveryPlanRefusal::MissingVoteFloor)],
            }),
            3,
        )
        .expect("the plan stands");
        assert!(!state.replan);
        assert!(state.excluded.contains(&3));
        // A later plan leaves node 3 out: two of three voters still answer.
        state.replan = true;
        let step = super::next_step(&mut state, &stalled(), 3).expect("a step");
        assert!(matches!(step, super::Step::Plan(_)), "{step:?}");
        // Without a majority left, the tool stops.
        state.excluded.insert(2);
        state.replan = true;
        assert!(matches!(
            super::next_step(&mut state, &stalled(), 3),
            Err(super::RecoverError::Plan(PlanError::NoMajority { .. }))
        ));
    }

    /// Item 5: a refusal naming another live leader waits instead of
    /// planning again, as does a reached open replica that follows a leader
    /// the tool did not reach.
    #[test]
    fn a_live_leader_out_of_reach_makes_the_tool_wait() {
        use ursula_proto::admin::RecoveryPlanRefusal;
        let plan = plan_group(&stalled()).expect("a plan");
        let mut state = super::LoopState::default();
        super::plan_sent(
            &mut state,
            plan,
            Ok(super::PlanSent {
                leader_term: 9,
                taken: vec![1],
                refused: vec![refused(2, RecoveryPlanRefusal::FollowsLeader { leader: 5 })],
            }),
            3,
        )
        .expect("recorded");
        assert!(!state.replan);
        assert_eq!(state.follows_leader, Some(5));
        let mut members = stalled();
        members[1].current_leader = Some(5);
        assert_eq!(
            super::next_step(&mut state, &members, 3).expect("a step"),
            super::Step::Wait
        );
        // A fresh run sees the open replica follow the unreached leader.
        let mut members = stalled();
        members[0].current_leader = Some(5);
        let mut fresh = super::LoopState::default();
        assert_eq!(
            super::next_step(&mut fresh, &members, 3).expect("a step"),
            super::Step::Wait
        );
    }

    /// Item 5: the plan terms replicas report bound the m1 guard too, for a
    /// plan another run sent.
    #[test]
    fn a_leader_above_a_plan_term_a_replica_reports_stops_new_plans() {
        let mut members = stalled();
        members[0].current_leader = Some(1);
        members[0].log.current_term = 10;
        members[0].plan = Some(planned(1));
        members[2].plan = Some(planned(1));
        let mut state = super::LoopState::default();
        assert_eq!(
            super::next_step(&mut state, &members, 3).expect("a step"),
            super::Step::Wait
        );
    }

    /// A `lead` refused because another job runs there costs no plan.
    #[test]
    fn a_plan_in_flight_on_the_leader_is_not_counted() {
        use ursula_proto::admin::RecoveryPlanRefusal;
        let plan = plan_group(&stalled()).expect("a plan");
        let mut state = super::LoopState {
            plans_sent: 1,
            ..Default::default()
        };
        super::plan_sent(
            &mut state,
            plan,
            Err(refused(1, RecoveryPlanRefusal::PlanInFlight)),
            3,
        )
        .expect("recorded");
        assert_eq!(state.plans_sent, 0);
        assert!(state.replan);
    }
}
