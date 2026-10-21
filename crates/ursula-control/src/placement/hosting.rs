//! Placement-driven hosting: what a node runs, what it tears down, who may
//! initialize a seeded group, and whom a node asks about a group.
//!
//! [`plan_hosting`] is the one decision of what a node runs. A node runs
//! what its journal holds and what its placement gives it, and starts
//! erasing a replica only from a confirmed view that gives it no role. The
//! group then decides the erasure: [`Teardown`] passes its first and its
//! last step only on a live leader's answer that the node is no member.
//!
//! An unconfirmed view, or one that has not bootstrapped, only opens what
//! the journal holds: views are not monotonic across restarts, so an old
//! one may only delay work.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use ursula_proto::placement::MembershipAnswer;
use ursula_proto::placement::MetaLogId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::ReplicaRole;
use ursula_proto::placement::TeardownStage;
use ursula_proto::placement::VoterSet;
use ursula_shard::RaftGroupId;

use super::reconcile::MembershipConfig;
use super::rules::Rule;
use super::rules::enforced;
use super::state::Cluster;
use super::state::GroupPlacement;
use super::view::ConfirmedView;
use super::view::PlacementView;

/// The placement view a plan reads.
#[derive(Debug, Clone, Copy)]
pub enum ViewInput<'a> {
    Unconfirmed(&'a PlacementView),
    Confirmed(&'a ConfirmedView),
}

impl ViewInput<'_> {
    pub fn view(&self) -> &PlacementView {
        match *self {
            Self::Unconfirmed(view) => view,
            Self::Confirmed(confirmed) => confirmed.view(),
        }
    }
}

/// Whether the node knows itself removed, from a registration answer. A
/// view that shows the node `Removed` counts too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Standing {
    Member,
    Removed,
}

/// What the node's journal holds of a group. A group the journal does not
/// name is absent from [`HostingInput::local`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LocalLog {
    /// The group's core failed to open: every group of that core is
    /// `Unknown`, never created, torn down or collected.
    Unknown,
    /// The replica holds no history yet.
    Empty,
    Holding,
    /// A teardown retired the log: the replica is gone.
    Retired,
}

impl LocalLog {
    /// Whether the node holds a replica: every log but a retired one.
    pub fn is_held(self) -> bool {
        match self {
            Self::Unknown | Self::Empty | Self::Holding => true,
            Self::Retired => false,
        }
    }
}

/// Whether a replica counts as a voter (the shell's one voter predicate). A
/// replica closed by a restart counts until its group answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Counts {
    Voter,
    NotVoter,
}

/// A group the node's journal names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LocalReplica {
    pub log: LocalLog,
    pub counts: Counts,
    /// The replica's running teardown, if any.
    pub teardown: Option<Teardown>,
}

impl LocalReplica {
    /// Whether the replica may be open: it is held and no teardown passed
    /// its first step.
    fn admissible(&self) -> bool {
        self.log.is_held() && !matches!(self.teardown, Some(Teardown::Running(_)))
    }
}

/// One planning input of the hosting driver.
#[derive(Debug, Clone, Copy)]
pub struct HostingInput<'a> {
    pub me: NodeId,
    pub standing: Standing,
    pub view: ViewInput<'a>,
    pub local: &'a BTreeMap<RaftGroupId, LocalReplica>,
}

/// How a replica may open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Admission {
    /// Open a held log, never creating one.
    Open,
    /// Create a fresh log for an absent group, or reopen a retired one.
    Create,
}

/// Work the hosting driver starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HostingStep {
    /// Confirm the view first: confirming would change what the plan
    /// admits or starts.
    ConfirmView,
    /// Start tearing the replica down, at its first step.
    TearDown { group: RaftGroupId },
    /// End a teardown still at its first step: the group gave the node a
    /// role again, or the node was removed.
    CancelTeardown { group: RaftGroupId },
    /// Report the teardown the retirement created at `created_at` named.
    ReportTornDown {
        group: RaftGroupId,
        created_at: MetaLogId,
    },
}

/// What a node runs and what it starts.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HostingPlan {
    /// Every replica the node may run. The driver closes every running one
    /// this map leaves out.
    pub admitted: BTreeMap<RaftGroupId, Admission>,
    /// In order: `ConfirmView` first when present, then by group.
    pub steps: Vec<HostingStep>,
    /// Held replicas without a role in the bootstrapped view, or, on a
    /// removed node, the replicas it keeps open. Readiness reads it.
    pub unplaced: BTreeSet<RaftGroupId>,
}

/// What the node runs and starts now.
///
/// - A removed node admits `Open` every held replica that counts as a
///   voter and reports each as unplaced. It never creates, starts a
///   teardown or reports one, and it cancels a teardown still at its first
///   step. A teardown past it finishes.
/// - Elsewhere an unconfirmed view, or one that has not bootstrapped,
///   admits `Open` every held replica and plans nothing, but a confirmation
///   when confirming the same view would plan otherwise.
/// - A confirmed bootstrapped view applies the rules per group: without a
///   role an `Empty` or `Holding` log is torn down and a retirement is
///   reported once the log is retired or absent. A `Voter` or `Incoming`
///   role opens a held log and otherwise creates one, and an `Outgoing` or
///   `Abandoned` role only opens a held log. A role cancels a teardown at
///   its first step.
///
/// A replica whose teardown passed its first step is never admitted.
pub fn plan_hosting(input: &HostingInput<'_>) -> HostingPlan {
    let view = input.view.view();
    let shown_removed = view
        .cluster()
        .and_then(|cluster| cluster.node(input.me))
        .is_some_and(|record| record.lifecycle == NodeLifecycle::Removed);
    if input.standing == Standing::Removed || shown_removed {
        return plan_removed(input.local);
    }
    match input.view {
        ViewInput::Confirmed(confirmed) => plan_member(input.me, confirmed.view(), input.local),
        ViewInput::Unconfirmed(view) => {
            let mut plan = plan_held(view, input.me, input.local);
            let confirmed = plan_member(input.me, view, input.local);
            #[cfg(test)]
            if !enforced(Rule::ConfirmedCreation) {
                for (group, admission) in &confirmed.admitted {
                    if *admission == Admission::Create {
                        plan.admitted.insert(*group, Admission::Create);
                    }
                }
            }
            if confirmed.admitted != plan.admitted || confirmed.steps != plan.steps {
                plan.steps.insert(0, HostingStep::ConfirmView);
            }
            plan
        }
    }
}

/// A removed node's plan.
fn plan_removed(local: &BTreeMap<RaftGroupId, LocalReplica>) -> HostingPlan {
    let mut plan = HostingPlan::default();
    for (group, replica) in local {
        if replica.admissible()
            && replica.counts == Counts::Voter
            && enforced(Rule::RemovedNodeCloses)
        {
            plan.admitted.insert(*group, Admission::Open);
            plan.unplaced.insert(*group);
        }
        if replica.teardown == Some(Teardown::Asking) {
            plan.steps
                .push(HostingStep::CancelTeardown { group: *group });
        }
    }
    plan
}

/// Every admissible held replica opens, and nothing else is planned.
fn plan_held(
    view: &PlacementView,
    me: NodeId,
    local: &BTreeMap<RaftGroupId, LocalReplica>,
) -> HostingPlan {
    let mut plan = HostingPlan::default();
    for (group, replica) in local {
        if replica.admissible() {
            plan.admitted.insert(*group, Admission::Open);
        }
        if let Some(cluster) = view.cluster()
            && replica.log.is_held()
            && cluster.role(*group, me).is_none()
        {
            plan.unplaced.insert(*group);
        }
    }
    plan
}

/// A member's plan from `view` taken as confirmed.
fn plan_member(
    me: NodeId,
    view: &PlacementView,
    local: &BTreeMap<RaftGroupId, LocalReplica>,
) -> HostingPlan {
    let Some(cluster) = view.cluster() else {
        return plan_held(view, me, local);
    };
    let mut groups: BTreeSet<RaftGroupId> = local.keys().copied().collect();
    groups.extend(
        cluster
            .groups()
            .iter()
            .filter(|(_, placement)| placement.role(me).is_some())
            .map(|(group, _)| group),
    );
    groups.extend(
        cluster
            .retirements()
            .iter()
            .filter(|(_, records)| records.contains_key(&me))
            .map(|(group, _)| *group),
    );
    let mut plan = HostingPlan::default();
    for group in groups {
        plan_group(cluster, me, group, local.get(&group), &mut plan);
    }
    plan
}

/// The rules for one group of a confirmed bootstrapped view.
fn plan_group(
    cluster: &Cluster,
    me: NodeId,
    group: RaftGroupId,
    replica: Option<&LocalReplica>,
    plan: &mut HostingPlan,
) {
    let held = replica.is_some_and(|replica| replica.log.is_held());
    let teardown = replica.and_then(|replica| replica.teardown);
    if replica.is_some_and(LocalReplica::admissible) {
        plan.admitted.insert(group, Admission::Open);
    }
    match cluster.role(group, me) {
        None => {
            if held {
                plan.unplaced.insert(group);
            }
            match (replica.map(|replica| replica.log), teardown) {
                (Some(LocalLog::Empty | LocalLog::Holding), None) => {
                    plan.steps.push(HostingStep::TearDown { group });
                }
                // An unknown log is never torn down.
                (Some(LocalLog::Unknown), Some(Teardown::Asking)) => {
                    plan.steps.push(HostingStep::CancelTeardown { group });
                }
                (None | Some(LocalLog::Retired), None) => {
                    if let Some(record) = cluster.retirement(group, me) {
                        plan.steps.push(HostingStep::ReportTornDown {
                            group,
                            created_at: record.created_at,
                        });
                    }
                }
                (Some(_) | None, Some(_)) | (Some(LocalLog::Unknown), None) => {}
            }
        }
        Some(role) => {
            if teardown == Some(Teardown::Asking) {
                plan.steps.push(HostingStep::CancelTeardown { group });
            }
            let creates = matches!(role, ReplicaRole::Voter | ReplicaRole::Incoming);
            if creates && !held && teardown.is_none() {
                plan.admitted.insert(group, Admission::Create);
            }
        }
    }
}

/// A seed driver's role in a seeded group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SeedRoleOf {
    /// Runs `Initialize` once every seed voter answered empty.
    Initializer,
    /// Waits for the initializer.
    Voter,
}

/// The node's seed role in `group`, from a confirmed view: `None` unless
/// the view shows the group `Seeded` with the node among its voters. Only
/// `Initializer` may initialize.
pub fn seed_role(view: &ConfirmedView, me: NodeId, group: RaftGroupId) -> Option<SeedRoleOf> {
    shown_seed_role(view.view(), me, group)
}

/// The seed role a view shows, confirmed or not, by [`seed_role`]'s rule.
/// An unconfirmed role only decides whether a seed driver starts: the
/// driver acts on [`seed_role`] of a confirmed view.
pub fn shown_seed_role(view: &PlacementView, me: NodeId, group: RaftGroupId) -> Option<SeedRoleOf> {
    let placement = view.cluster()?.group(group)?;
    match placement {
        GroupPlacement::Seeded {
            voters,
            initializer,
            ..
        } => {
            if *initializer == me {
                Some(SeedRoleOf::Initializer)
            } else if voters.contains(me) {
                Some(SeedRoleOf::Voter)
            } else {
                None
            }
        }
        GroupPlacement::Live { .. } => None,
    }
}

/// The nodes that may lead a group, in tiers asked in order: the
/// placement's voters of both sides of a move (or its stable or seed
/// voters), the rest of its source, its target, then the other members of
/// the local membership, then every other configured peer. Each node
/// appears once, in its first tier, the asking node in none, and empty
/// tiers are left out.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LeaderCandidates {
    tiers: Vec<BTreeSet<NodeId>>,
}

impl LeaderCandidates {
    pub fn tiers(&self) -> &[BTreeSet<NodeId>] {
        &self.tiers
    }

    /// The tier a `NotHosted` answer names a node from.
    pub fn first_tier(&self) -> Option<&BTreeSet<NodeId>> {
        self.tiers.first()
    }

    /// Every candidate in asking order.
    pub fn iter(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.tiers.iter().flat_map(|tier| tier.iter().copied())
    }

    fn push(&mut self, me: NodeId, nodes: impl IntoIterator<Item = NodeId>) {
        let tier: BTreeSet<NodeId> = nodes
            .into_iter()
            .filter(|node| *node != me && !self.tiers.iter().any(|tier| tier.contains(node)))
            .collect();
        if !tier.is_empty() {
            self.tiers.push(tier);
        }
    }
}

/// Whose leader a node looks for: a data group's, or the meta group's with
/// the meta voters this node claims at genesis.
#[derive(Debug, Clone, Copy)]
pub enum CandidatesOf<'a> {
    Group(RaftGroupId),
    Meta { claimed: &'a VoterSet },
}

/// Who `me` asks about a group's leader, from its placement view (confirmed
/// or not), the membership of its open replica if any, and the ids of
/// `raft.peers`.
///
/// For a data group the first tiers come from its placement. For the meta
/// group the first tier is meta's voters: the voters of the local meta
/// membership, else the view's `meta_voters` once bootstrapped, else the
/// claimed ones. Then come the other members of the local membership, then
/// every other peer.
pub fn leader_candidates(
    me: NodeId,
    of: CandidatesOf<'_>,
    view: &PlacementView,
    membership: Option<&MembershipConfig>,
    peers: &BTreeSet<NodeId>,
) -> LeaderCandidates {
    let mut candidates = LeaderCandidates::default();
    match of {
        CandidatesOf::Group(group) => {
            if let Some(placement) = view.cluster().and_then(|cluster| cluster.group(group)) {
                match placement.moving() {
                    Some(moving) => {
                        candidates.push(
                            me,
                            moving.from.iter().filter(|node| moving.to.contains(*node)),
                        );
                        candidates.push(me, moving.from.iter());
                        candidates.push(me, moving.to.iter());
                    }
                    None => candidates.push(me, placement.current_voters().iter()),
                }
            }
        }
        CandidatesOf::Meta { claimed } => match (membership, view.cluster()) {
            (Some(membership), _) => candidates.push(me, membership.voters()),
            (None, Some(cluster)) => candidates.push(me, cluster.params().meta_voters.iter()),
            (None, None) => candidates.push(me, claimed.iter()),
        },
    }
    if let Some(membership) = membership {
        candidates.push(me, membership.members());
    }
    candidates.push(me, peers.iter().copied());
    candidates
}

/// A running teardown of one replica, step by step:
///
/// 0. Ask: the replica's membership asker waits for a live leader's answer.
///    Anything but `NotMember` changes nothing, and the next ask waits.
/// 1. Fence: drop the group from the hosting set, so no request reopens it.
/// 2. Unregister the group's handle.
/// 3. Stop the group's engine.
/// 4. Wait until nothing holds the store or acknowledges.
/// 5. Ask again: a live leader's `NotMember` retires the log, anything else
///    admits the replica again with its log intact.
/// 6. Forget the group's gauges and open back-off.
///
/// A teardown is cancelled only at step 0. Past it, it finishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Teardown {
    /// Step 0.
    Asking,
    /// Steps 1 to 6.
    Running(TeardownStep),
}

/// The steps of a teardown past its first answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TeardownStep {
    Fence,
    Unregister,
    Stop,
    AwaitIdle,
    AskAgain,
    Retire,
    ForgetMetrics,
}

/// What the teardown executor observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TeardownEvent {
    /// An ask ended: a live leader's classification of this node, or `None`
    /// when no live leader answered.
    Answered(Option<MembershipAnswer>),
    /// The current step that is not an ask completed.
    StepDone,
    /// The plan cancels the teardown.
    Cancel,
}

/// Where a teardown went.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TeardownOutcome {
    Continue(Teardown),
    Ended(TeardownEnd),
}

/// How a teardown ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TeardownEnd {
    /// The log is retired and the replica gone.
    Retired,
    /// The second answer was not `NotMember`: the replica is admitted
    /// again with its log intact.
    Readmitted,
    /// Cancelled at step 0: nothing was fenced.
    Cancelled,
}

impl Teardown {
    /// A teardown starts at its first ask.
    pub fn start() -> Self {
        Self::Asking
    }

    /// The stage the replica's status reports.
    pub fn stage(self) -> TeardownStage {
        match self {
            Self::Asking => TeardownStage::Ask,
            Self::Running(_) => TeardownStage::Fence,
        }
    }

    /// The teardown after `event`. An event the current step does not wait
    /// for changes nothing: an answer while fencing, a completion while
    /// asking, a cancel past step 0.
    pub fn advance(self, event: TeardownEvent) -> TeardownOutcome {
        let not_member = event == TeardownEvent::Answered(Some(MembershipAnswer::NotMember));
        #[cfg(test)]
        let not_member = not_member
            || (!enforced(Rule::RetireAnswer) && matches!(event, TeardownEvent::Answered(_)));
        match (self, event) {
            (Self::Asking, TeardownEvent::Cancel) => TeardownOutcome::Ended(TeardownEnd::Cancelled),
            (Self::Asking, TeardownEvent::Answered(_)) if not_member => {
                TeardownOutcome::Continue(Self::Running(TeardownStep::Fence))
            }
            (Self::Running(TeardownStep::AskAgain), TeardownEvent::Answered(_)) => {
                if not_member {
                    TeardownOutcome::Continue(Self::Running(TeardownStep::Retire))
                } else {
                    TeardownOutcome::Ended(TeardownEnd::Readmitted)
                }
            }
            (Self::Running(step), TeardownEvent::StepDone) => match step {
                TeardownStep::Fence => {
                    TeardownOutcome::Continue(Self::Running(TeardownStep::Unregister))
                }
                TeardownStep::Unregister => {
                    TeardownOutcome::Continue(Self::Running(TeardownStep::Stop))
                }
                TeardownStep::Stop => {
                    TeardownOutcome::Continue(Self::Running(TeardownStep::AwaitIdle))
                }
                TeardownStep::AwaitIdle => {
                    TeardownOutcome::Continue(Self::Running(TeardownStep::AskAgain))
                }
                TeardownStep::Retire => {
                    TeardownOutcome::Continue(Self::Running(TeardownStep::ForgetMetrics))
                }
                TeardownStep::ForgetMetrics => TeardownOutcome::Ended(TeardownEnd::Retired),
                TeardownStep::AskAgain => TeardownOutcome::Continue(self),
            },
            (Self::Asking, TeardownEvent::Answered(_) | TeardownEvent::StepDone)
            | (Self::Running(_), TeardownEvent::Answered(_) | TeardownEvent::Cancel) => {
                TeardownOutcome::Continue(self)
            }
        }
    }
}

#[cfg(test)]
mod proptests;
#[cfg(test)]
mod tests;
