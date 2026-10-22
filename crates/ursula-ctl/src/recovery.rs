//! Recovery plans for a stalled group after an accepted loss (#581 section
//! 4.5), over `POST /__ursula/raft/{target}/recovery/accept-unsynced-loss`,
//! where the target is the meta group or a data group ([`RecoverTarget`]).
//!
//! Every plan carries the floor of its plane ([`PlanFloor`]). The meta
//! group's is `meta`. A data group's is read from a confirmed meta view
//! ([`group_floor`]), so meta is recovered first: without a confirmed view
//! the tool refuses to plan a data group and names `--target meta`. A
//! seeded group's floor is `seeded`. A live group's is `certified`, the
//! membership entry its last convergence certified committed, and its plan
//! keeps that entry (design 7.4): every replica that holds it keeps it
//! through a cut, and the plan's leader is one of its voters that holds it.
//!
//! The tool reads every reachable replica of the group, computes the plan
//! ([`plan_group`]), leaving out every replica that lost its vote and holds
//! no vote floor yet, and sends it to the plan's leader first with the
//! highest vote term it read ([`send_plan`]). The leader answers its plan
//! term, and every other member takes the plan with it: a member then votes
//! for the leader only in a campaign above the plan term. The tool waits
//! until the plan's history event applied on every replica it reached. A
//! plan whose leader lost entries it had replicated wedges (its members'
//! logs are longer than its own), and the tool plans again
//! ([`plan_wedged`]). The deterministic simulation runs the same planner and
//! sender over its own [`PlanTransport`].

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::future::Future;
use std::time::Duration;

use reqwest::Method;
use tokio::time::Instant;
use ursula_control::ClusterView;
use ursula_control::GroupPlacement;
use ursula_proto::admin::EntryId;
use ursula_proto::admin::PlanFloor;
use ursula_proto::admin::PlanStep;
use ursula_proto::admin::RaftGroupMetrics;
use ursula_proto::admin::RaftGroupRecovery;
use ursula_proto::admin::RecoveryGateStatus;
use ursula_proto::admin::RecoveryPlanRefusal;
use ursula_proto::admin::RecoveryPlanRefused;
use ursula_proto::admin::RecoveryPlanReport;
use ursula_proto::admin::RecoveryPlanRequest;
use ursula_proto::admin::ReplicaLogView;
use ursula_proto::placement::MembershipLogId;

use crate::metrics::MetricsClient;
use crate::provider::NodeInfo;

/// The group `recover-group` plans: the meta group, or a data group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoverTarget {
    Meta,
    Group(u32),
}

/// A `--target` that is neither `meta` nor a raft group id.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid recovery target `{raw}`: `meta` or a raft group id")]
pub struct InvalidRecoverTarget {
    pub raw: String,
}

impl std::str::FromStr for RecoverTarget {
    type Err = InvalidRecoverTarget;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        if raw == "meta" {
            return Ok(Self::Meta);
        }
        raw.parse::<u32>()
            .map(Self::Group)
            .map_err(|_invalid| InvalidRecoverTarget {
                raw: raw.to_owned(),
            })
    }
}

impl std::fmt::Display for RecoverTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Meta => f.write_str("meta"),
            Self::Group(group) => write!(f, "group {group}"),
        }
    }
}

impl RecoverTarget {
    /// The `{target}` of the admin routes.
    fn path_segment(self) -> String {
        match self {
            Self::Meta => "meta".to_owned(),
            Self::Group(group) => group.to_string(),
        }
    }

    /// This target's replica in one node's metrics: `meta`, or the group
    /// in `groups`, where the meta group never is.
    fn replica(self, view: &crate::metrics::NodeMetricsView) -> Option<&RaftGroupMetrics> {
        match self {
            Self::Meta => view.meta.as_ref(),
            Self::Group(group) => view.group(u64::from(group)),
        }
    }
}

/// Why the tool does not plan a data group: its floor comes from a
/// confirmed meta view ([`group_floor`]).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FloorError {
    #[error(
        "no node answered a confirmed placement view, so group {group} has no plan floor: \
         recover the meta group first with `recover-group --target meta`"
    )]
    NoConfirmedView { group: u32 },
    #[error(
        "the confirmed placement view shows no bootstrapped cluster, so group {group} has no \
         plan floor: recover the meta group first with `recover-group --target meta`"
    )]
    NotBootstrapped { group: u32 },
    #[error("group {group} is not in the confirmed placement view")]
    UnknownGroup { group: u32 },
}

/// A live group's membership as its last convergence certified it, read
/// from a confirmed meta view: the membership entry, committed in the
/// group, and its voters, the placement's current voters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertifiedMembership {
    pub entry: MembershipLogId,
    pub voters: BTreeSet<u64>,
}

impl CertifiedMembership {
    fn index(&self) -> u64 {
        self.entry.get().index
    }

    /// Whether `member`'s log holds the certified entry, by the rule the
    /// server's plan accept checks exactly (`certified_held` in
    /// `ursula-raft`): its last log id is at least the entry's in both term
    /// and index. A log that reaches the entry's index at a lower last term
    /// holds another entry there, an uncommitted tail of an older leader.
    /// Under Raft's log matching and leader completeness the two agree: a
    /// last entry of the entry's term or a later one was written by a
    /// leader that held the committed entry.
    fn held_by(&self, member: &PlanMember) -> bool {
        let id = self.entry.get();
        member
            .log
            .last_log_index
            .is_some_and(|last| last >= id.index)
            && member.log.last_log_term.is_some_and(|term| term >= id.term)
    }

    /// The entry's id as a plan's `point_entry` names it.
    fn entry_id(&self) -> EntryId {
        let id = self.entry.get();
        EntryId {
            term: id.term,
            node_id: id.leader.get(),
        }
    }

    /// `members` as a plan under this certified membership sees them. The
    /// certified entry is committed, so a membership older than it is no
    /// newest committed membership: a replica whose log does not hold the
    /// entry ([`Self::held_by`]) reports only older ones, and one that holds
    /// it but whose commit marker lies below it keeps the entry through a
    /// cut, so it keeps the certified membership. Replicas whose marker
    /// reaches the entry are seen as they report themselves.
    fn view<'a>(&self, members: &'a [PlanMember]) -> Cow<'a, [PlanMember]> {
        let index = self.index();
        let reaches = |at: Option<u64>| at.is_some_and(|at| at >= index);
        if members.iter().all(|member| reaches(member.committed)) {
            return Cow::Borrowed(members);
        }
        Cow::Owned(
            members
                .iter()
                .map(|member| {
                    let mut member = member.clone();
                    if !self.held_by(&member) {
                        member.voter_configs.clear();
                        member.committed_voter_configs.clear();
                    } else if !reaches(member.committed) {
                        member.committed_voter_configs = vec![self.voters.clone()];
                    }
                    member
                })
                .collect(),
        )
    }
}

/// What a plan keeps at least: the floor the tool sends ([`PlanFloor`]),
/// and for a live group the certified membership the planner keeps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Floor {
    Meta,
    Seeded,
    Certified(CertifiedMembership),
}

impl Floor {
    /// The floor the plan's requests carry.
    pub fn plan_floor(&self) -> PlanFloor {
        match self {
            Self::Meta => PlanFloor::Meta,
            Self::Seeded => PlanFloor::Seeded,
            Self::Certified(certified) => PlanFloor::Certified(certified.entry),
        }
    }

    /// The membership the plan keeps, under a `certified` floor.
    pub fn certified(&self) -> Option<&CertifiedMembership> {
        match self {
            Self::Certified(certified) => Some(certified),
            Self::Meta | Self::Seeded => None,
        }
    }

    /// `members` as a plan under this floor sees them
    /// ([`CertifiedMembership::view`]): as they report themselves unless
    /// the floor certifies a membership.
    fn view<'a>(&self, members: &'a [PlanMember]) -> Cow<'a, [PlanMember]> {
        match self.certified() {
            Some(certified) => certified.view(members),
            None => Cow::Borrowed(members),
        }
    }
}

/// The floor of a plan of data group `group`, from the confirmed meta view
/// `confirmed` (`None` when no node answered one): `seeded` while the view
/// shows the group seeded, `certified` with its `converged_at` and current
/// voters once it is live. A view that is not bootstrapped and no view at
/// all are refused.
pub fn group_floor(group: u32, confirmed: Option<&ClusterView>) -> Result<Floor, FloorError> {
    let cluster = match confirmed {
        None => return Err(FloorError::NoConfirmedView { group }),
        Some(ClusterView::Bootstrapped(cluster)) => cluster,
        Some(
            ClusterView::MetaGenesis { .. }
            | ClusterView::MetaCatchingUp
            | ClusterView::Registering { .. }
            | ClusterView::FormationCut,
        ) => return Err(FloorError::NotBootstrapped { group }),
    };
    match cluster.group(ursula_shard::RaftGroupId(group)) {
        None => Err(FloorError::UnknownGroup { group }),
        Some(GroupPlacement::Seeded { .. }) => Ok(Floor::Seeded),
        // The kernel keeps `converged_at` with the voters it certified: a
        // converge sets both, and a move keeps both as its source.
        Some(placement @ GroupPlacement::Live { converged_at, .. }) => {
            Ok(Floor::Certified(CertifiedMembership {
                entry: *converged_at,
                voters: placement
                    .current_voters()
                    .iter()
                    .map(ursula_proto::placement::NodeId::get)
                    .collect(),
            }))
        }
    }
}

/// The plan floor of `target` ([`group_floor`] for a data group).
pub fn plan_floor(
    target: RecoverTarget,
    confirmed: Option<&ClusterView>,
) -> Result<Floor, FloorError> {
    match target {
        RecoverTarget::Meta => Ok(Floor::Meta),
        RecoverTarget::Group(group) => group_floor(group, confirmed),
    }
}

/// One replica of the group as the tool read it from its metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanMember {
    pub node_id: u64,
    pub log: ReplicaLogView,
    pub committed: Option<u64>,
    /// The id of the committed entry, with `committed` as its index.
    pub committed_entry: Option<EntryId>,
    /// The last index the replica applied: committed, also on a replica
    /// whose group stopped and reports no committed index.
    pub last_applied: Option<u64>,
    pub gate: RecoveryGateStatus,
    /// The recovery plan this replica holds, if any.
    pub plan: Option<RaftGroupRecovery>,
    /// The replica leads with a quorum that acknowledged it lately.
    pub leads_live: bool,
    /// The log index of the history event it applied last.
    pub history_event_index: Option<u64>,
    pub current_leader: Option<u64>,
    /// The voter configs of the replica's effective membership: one, or
    /// two while a change is joint.
    pub voter_configs: Vec<BTreeSet<u64>>,
    /// The voter configs of the membership the replica keeps when a plan
    /// cuts its log back to its committed entry.
    pub committed_voter_configs: Vec<BTreeSet<u64>>,
    pub learner_ids: Vec<u64>,
    /// The replica's group runs. One whose apply stopped reports no
    /// membership and votes for no one: it never counts toward a majority.
    pub running: bool,
    /// The replica lost its vote and holds no vote floor: it refuses every
    /// plan until a live leader's barrier proves a vote, so no plan counts
    /// it or is sent to it.
    pub needs_vote_floor: bool,
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
            committed_entry: group
                .committed_term
                .zip(group.committed_node)
                .map(|(term, node_id)| EntryId { term, node_id }),
            last_applied: group.last_applied_index,
            gate: group.recovery_gate,
            plan: group.recovery,
            leads_live: group.leads_live,
            history_event_index: group.history.as_ref().map(|history| history.event_index),
            current_leader: group.current_leader,
            voter_configs: group.voter_configs.clone(),
            committed_voter_configs: group.committed_voter_configs.clone(),
            learner_ids: group.learner_ids.clone(),
            running: group.maintenance.running,
            needs_vote_floor: group.needs_vote_floor,
        }
    }

    /// Whether the replica's own membership makes it a voter: without a
    /// cut its effective membership, with a cut the one it keeps. OpenRaft
    /// lets only a voter campaign.
    fn votes_in_own_membership(&self, cut: bool) -> bool {
        let configs = if cut {
            &self.committed_voter_configs
        } else {
            &self.voter_configs
        };
        configs.iter().any(|config| config.contains(&self.node_id))
    }

    /// Every node the replica's effective membership names, learners
    /// included: the replicas its leadership replicates to.
    fn membership(&self) -> BTreeSet<u64> {
        self.voter_configs
            .iter()
            .flatten()
            .chain(self.learner_ids.iter())
            .copied()
            .collect()
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
    /// With a cut whose point is a later committed entry another replica
    /// reported than the leader's own commit marker: that entry's id, which
    /// the leader must hold (F3 of the third #613 review).
    pub point_entry: Option<EntryId>,
}

/// Why the tool does not plan a group now.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    #[error("no replica of the group answered")]
    NoReplica,
    #[error(
        "only {answered} of the voters {voters:?} answered with a vote floor (without one, \
         waiting for a live leader's barrier: {without_vote_floor:?}); a plan needs a majority \
         of every voter set a replica reports"
    )]
    NoMajority {
        answered: usize,
        voters: BTreeSet<u64>,
        /// Replicas of the set that answered but lost their vote and hold
        /// no vote floor, which no plan counts.
        without_vote_floor: BTreeSet<u64>,
    },
    #[error("no replica the plan may elect is a voter of the membership it would campaign under")]
    NoEligibleLeader,
    /// The plan would continue below an entry a reached replica knows is
    /// committed, and that replica's committed entries would be overwritten
    /// (M-A of the #613 review). No replica the plan may elect knows as much.
    #[error(
        "node {node_id} knows entry {committed} is committed, but the plan would continue from \
         {point}: no replica the plan may elect knows as much"
    )]
    BelowCommitted {
        node_id: u64,
        committed: u64,
        point: u64,
    },
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
    /// The group's last convergence certified a membership entry, and no
    /// voter of that membership the plan may count has a log that holds it
    /// (its last log id reaches the entry's term and index): every plan
    /// would drop it (design 7.4).
    #[error(
        "the group's last convergence certified membership entry {certified}, but no voter of \
         that membership the tool reached with a vote floor has a log that holds it"
    )]
    CertifiedNotReached { certified: MembershipLogId },
}

/// Computes the plan from the replicas that answered (operations guide,
/// "Recovering after a host crash"), as [`plan_group_excluding`] with no
/// replica left out and a floor that certifies no membership.
pub fn plan_group(members: &[PlanMember]) -> Result<GroupPlan, PlanError> {
    plan_group_excluding(members, &BTreeSet::new(), &Floor::Seeded)
}

/// Computes the plan from the replicas that answered, leaving `excluded`
/// out of it (#613):
///
/// - Every voter set any replica reports, `excluded` ones included, needs a
///   majority among the planned replicas. Each may be the newest committed
///   membership, whose quorums the floor must meet, and the plan's leader
///   must win one. A replica whose group stopped is never planned, nor is
///   one that lost its vote and holds no vote floor, which refuses every
///   plan (f3b gap 2): the voter sets it reports still count.
/// - `floor` is the highest vote term among the restricted replicas.
/// - The leader is the voter with the greatest last log id, and the plan's
///   point is its last log index, without a cut.
/// - When that log's last term is below `floor`, its log may hold a stale
///   entry. When the replica holding it is no voter of its own membership,
///   it cannot lead. Either way every member cuts its log to its committed
///   entry, and the leader is the replica with the greatest committed index
///   among those the membership they keep makes a voter.
/// - When a replica reports a later committed entry than that leader's
///   marker, a learner's say, the leader is instead a voter whose log
///   reaches it, and keeps its log up to that entry, which it must hold
///   exactly ([`GroupPlan::point_entry`]).
/// - The point is never below a committed or applied index any reached
///   replica reports, planned or not, stopped and excluded ones included:
///   otherwise the plan is refused as [`PlanError::BelowCommitted`].
/// - Under a `certified` floor the certified membership entry is committed
///   too (design 7.4), and the planner sees the replicas as such a plan
///   leaves them ([`CertifiedMembership::view`]). A cut whose leader's
///   marker lies below the entry keeps every log up to it, led by a voter
///   of that membership whose log holds it, so the point is never below
///   it. Without such a voter the plan is refused as
///   [`PlanError::CertifiedNotReached`]. Commit markers are not synced on
///   their own, so after a full power loss every marker may lie below a
///   membership change that teardowns already followed.
///
/// Why a majority of each reported voter set is enough. A replica syncs a
/// membership entry before it acknowledges it, so every replica that
/// acknowledged a change still reports it after a power loss. OpenRaft
/// proposes one change at a time: a joint configuration `C` commits under
/// the configuration `L` before it, and `C`'s last configuration is `L`'s
/// successor only once `C` commits. Whatever the loss rolled back, the
/// newest committed membership is either the last one some replica reports
/// or the one before it, `L`, which every replica holding `C` also reports
/// as `C`'s old half. A plan with a majority of every reported set holds a
/// majority of `L` and of any newer committed membership, so its leader is
/// elected by the same quorums Raft would require, and the floor reaches
/// every vote a quorum of them granted. This assumes no earlier plan's cut
/// dropped a committed membership entry: plans are serialized by their
/// terms, and each one checks the same rule.
pub fn plan_group_excluding(
    members: &[PlanMember],
    excluded: &BTreeSet<u64>,
    floor: &Floor,
) -> Result<GroupPlan, PlanError> {
    let certified = floor.certified();
    let members = &*floor.view(members);
    let planned = members
        .iter()
        .filter(|member| {
            member.running && !member.needs_vote_floor && !excluded.contains(&member.node_id)
        })
        .collect::<Vec<_>>();
    if planned.is_empty() {
        return Err(PlanError::NoReplica);
    }
    for voters in voter_sets(members, certified) {
        let answered = planned
            .iter()
            .filter(|member| voters.contains(&member.node_id))
            .count();
        if answered.saturating_mul(2) <= voters.len() {
            let without_vote_floor = members
                .iter()
                .filter(|member| member.needs_vote_floor && voters.contains(&member.node_id))
                .map(|member| member.node_id)
                .collect();
            return Err(PlanError::NoMajority {
                answered,
                voters,
                without_vote_floor,
            });
        }
    }
    if let Some(member) = planned.iter().find(|member| {
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
    // Design 7.4: only a replica that holds the certified entry may lead,
    // and the cut keeps it, so the plan's point is never below it.
    if let Some(certified) = certified
        && !planned
            .iter()
            .any(|member| certified.voters.contains(&member.node_id) && certified.held_by(member))
    {
        return Err(PlanError::CertifiedNotReached {
            certified: certified.entry,
        });
    }
    let floor = planned
        .iter()
        .filter(|member| member.restricted())
        .map(|member| member.log.current_term)
        .max()
        .unwrap_or(0);
    let highest_term = planned
        .iter()
        .map(|member| member.log.current_term)
        .max()
        .unwrap_or(0);
    let voters = voters_of(members, certified);
    // Only a voter leads: a learner is refused as the plan's leader.
    let candidates = || {
        planned
            .iter()
            .copied()
            .filter(|member| voters.contains(&member.node_id))
    };
    let longest = candidates()
        .max_by_key(|member| {
            (
                member.last_log_id(),
                member.votes_in_own_membership(false),
                member.node_id,
            )
        })
        .ok_or(PlanError::NoReplica)?;
    // A single voter never cuts: no other voter could have overwritten an
    // entry it holds (#581 section 4.5). The server decides it the same way.
    let single = ursula_proto::admin::single_voter(
        longest
            .voter_configs
            .iter()
            .chain(longest.committed_voter_configs.iter()),
    );
    let stale_possible = longest.log.last_log_term.unwrap_or(0) < floor;
    // R7 (#613): the newest committed index any reached replica reports,
    // planned or not, learners and stopped replicas included. A stopped
    // replica reports what it applied, which was committed.
    let reported = members
        .iter()
        .flat_map(|member| {
            [
                member
                    .committed
                    .map(|index| (index, member.committed_entry, member.node_id)),
                member
                    .last_applied
                    .map(|index| (index, None, member.node_id)),
            ]
        })
        .flatten()
        .max_by_key(|(index, entry, node_id)| (*index, entry.is_some(), *node_id));
    let plan = if !single && (stale_possible || !longest.votes_in_own_membership(false)) {
        let eligible = || candidates().filter(|member| member.votes_in_own_membership(true));
        let by_marker = eligible().max_by_key(|member| (member.committed, member.node_id));
        let marker = by_marker.and_then(|leader| leader.committed);
        let reaches = |index: u64| {
            move |member: &&PlanMember| member.log.last_log_index.is_some_and(|last| last >= index)
        };
        // A replica knows a later entry is committed than the leader's
        // marker: lead from a replica whose log may hold it, keeping up to
        // it. The leader verifies it holds exactly that entry. With no such
        // log, the ordinary cut below is refused as `BelowCommitted`.
        let reported_raise = reported.and_then(|(index, entry, _)| {
            let entry = entry?;
            if marker.is_some_and(|committed| committed >= index) {
                return None;
            }
            // The server checks the leader against its membership at the
            // point, which lies between the one it keeps at its marker and
            // its effective one: a voter of both is a voter there (N4 of the
            // fifth #613 review).
            let leader = eligible()
                .filter(|member| member.votes_in_own_membership(false))
                .filter(reaches(index))
                .max_by_key(|member| (member.committed, member.node_id))?;
            Some((index, false, leader.node_id, entry))
        });
        // Design 7.4: the certified entry is committed too. The membership
        // at it is the certified one, whose voters the view names, so the
        // leader is one of them whose log holds it, whatever its marker
        // keeps. A log that reaches its index at a lower last term holds an
        // older leader's tail there, and as the leader it would refuse.
        let certified_raise = certified.and_then(|certified| {
            let index = certified.index();
            if marker.is_some_and(|committed| committed >= index) {
                return None;
            }
            let leader = candidates()
                .filter(|member| certified.voters.contains(&member.node_id))
                .filter(|member| certified.held_by(member))
                .max_by_key(|member| (member.committed, member.node_id))?;
            Some((index, true, leader.node_id, certified.entry_id()))
        });
        let raised = [reported_raise, certified_raise]
            .into_iter()
            .flatten()
            .max_by_key(|(index, certified, _, _)| (*index, *certified));
        match raised {
            Some((point, _, leader, entry)) => GroupPlan {
                leader,
                point,
                cut: true,
                highest_term,
                floor,
                point_entry: Some(entry),
            },
            None => {
                let leader = by_marker.ok_or(PlanError::NoEligibleLeader)?;
                GroupPlan {
                    leader: leader.node_id,
                    point: leader.committed.unwrap_or(0),
                    cut: true,
                    highest_term,
                    floor,
                    point_entry: None,
                }
            }
        }
    } else {
        GroupPlan {
            leader: longest.node_id,
            point: longest.log.last_log_index.unwrap_or(0),
            cut: false,
            highest_term,
            floor,
            point_entry: None,
        }
    };
    // M-A of the second #613 review: a plan never continues below an entry
    // any reached replica knows is committed, or that replica's committed
    // entries would be overwritten.
    if let Some((index, _, node_id)) = reported
        && index > plan.point
    {
        return Err(PlanError::BelowCommitted {
            node_id,
            committed: index,
            point: plan.point,
        });
    }
    Ok(plan)
}

/// Every voter set the replicas report: each config of each replica's
/// effective membership and of the membership it keeps after a cut, and
/// the certified membership's voters, which are committed.
fn voter_sets(
    members: &[PlanMember],
    certified: Option<&CertifiedMembership>,
) -> BTreeSet<BTreeSet<u64>> {
    members
        .iter()
        .flat_map(|member| {
            member
                .voter_configs
                .iter()
                .chain(member.committed_voter_configs.iter())
        })
        .chain(certified.map(|certified| &certified.voters))
        .filter(|voters| !voters.is_empty())
        .cloned()
        .collect()
}

/// The voters the replicas report: the union of their voter sets.
fn voters_of(members: &[PlanMember], certified: Option<&CertifiedMembership>) -> BTreeSet<u64> {
    voter_sets(members, certified)
        .into_iter()
        .flatten()
        .collect()
}

/// The voter sets `plan`'s leader campaigns under, as the tool read it: a
/// majority of each must take the plan to elect it. A cut to a later entry
/// than the leader's marker keeps the membership at the point, which the
/// leader reports neither as kept (at its marker) nor as effective (at its
/// last entry). It is one of the two or a change between them, so both
/// count (D of the fourth #613 review). A cut to the certified entry keeps
/// the certified membership, whose voters `floor` names.
pub fn electorate(plan: &GroupPlan, members: &[PlanMember], floor: &Floor) -> Vec<BTreeSet<u64>> {
    if let Some(certified) = floor.certified()
        && plan.cut
        && plan.point == certified.index()
        && plan.point_entry == Some(certified.entry_id())
    {
        return vec![certified.voters.clone()];
    }
    floor
        .view(members)
        .iter()
        .find(|member| member.node_id == plan.leader)
        .map(|leader| {
            if plan.point_entry.is_some() {
                let mut sets = leader.committed_voter_configs.clone();
                for voters in &leader.voter_configs {
                    if !sets.contains(voters) {
                        sets.push(voters.clone());
                    }
                }
                sets
            } else if plan.cut {
                leader.committed_voter_configs.clone()
            } else {
                leader.voter_configs.clone()
            }
        })
        .unwrap_or_default()
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
    /// The replicas that answered without a vote floor: the plan neither
    /// counts them nor is sent to them.
    pub without_vote_floor: Vec<u64>,
    /// The floor the plan carries.
    pub floor: PlanFloor,
}

/// Explains `plan` under `floor` over the replicas it was computed from.
pub fn explain_plan(plan: GroupPlan, members: &[PlanMember], floor: &Floor) -> PlanExplanation {
    let voters = voters_of(&floor.view(members), floor.certified());
    let longest = members
        .iter()
        .filter(|member| voters.contains(&member.node_id))
        .max_by_key(|member| {
            (
                member.last_log_id(),
                member.votes_in_own_membership(false),
                member.node_id,
            )
        });
    PlanExplanation {
        plan,
        longest_last_log_term: longest.and_then(|member| member.log.last_log_term),
        members: members.to_vec(),
        unreached_voters: voters
            .into_iter()
            .filter(|voter| !members.iter().any(|member| member.node_id == *voter))
            .collect(),
        without_vote_floor: members
            .iter()
            .filter(|member| member.needs_vote_floor)
            .map(|member| member.node_id)
            .collect(),
        floor: floor.plan_floor(),
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
/// reached in the leader's membership: each applied a history event above
/// the point and holds no plan, and the group has a leader. A replica
/// outside the membership of a leader the tool reached never receives the
/// event ([`outside_leaders_membership`]).
pub fn plan_satisfied(point: u64, members: &[PlanMember]) -> bool {
    let outside = outside_leaders_membership(members);
    // m8: a replica whose group stopped applies nothing: never waited for.
    let within = members
        .iter()
        .filter(|member| member.running && !outside.contains(&member.node_id))
        .collect::<Vec<_>>();
    !within.is_empty()
        && members.iter().any(|member| member.current_leader.is_some())
        && within.iter().all(|member| {
            member.plan.is_none()
                && member
                    .history_event_index
                    .is_some_and(|index| index > point)
        })
}

/// The reached replicas outside the membership of the leader the tool
/// reached, if it reached one. The leader replicates nothing to them, so
/// they keep the history the plan replaced, and answer reads with `503`
/// while they hold the plan.
pub fn outside_leaders_membership(members: &[PlanMember]) -> Vec<u64> {
    let Some(leader) = members.iter().find(|member| member.reports_leading()) else {
        return Vec::new();
    };
    let membership = leader.membership();
    members
        .iter()
        .map(|member| member.node_id)
        .filter(|node_id| !membership.contains(node_id))
        .collect()
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

/// Sends `plan` with `floor` to its leader first, with the highest vote
/// term the tool read, then to every other member with the leader's plan term
/// ([`send_follows`]). A refusal of the leader fails the round, and nothing
/// changed.
pub async fn send_plan<T: PlanTransport>(
    transport: &mut T,
    plan: GroupPlan,
    floor: PlanFloor,
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
            point_entry: plan.point_entry,
            leader: plan.leader,
            point: plan.point,
            cut: plan.cut,
            step: PlanStep::Lead {
                highest_term: plan.highest_term,
            },
            floor,
            expected: leader.log,
        })
        .await?;
    Ok(send_follows(transport, plan, floor, report.leader_term, members).await)
}

/// Sends `plan` to every member but its leader with the plan term
/// `leader_term`, which the leader answered or recorded, and but the
/// members without a vote floor, which refuse every plan and rejoin through
/// the leader's barrier. A member's refusal does not undo the leader's
/// plan. A member that follows the plan's leader refuses because the
/// members that took the plan elected it, and rejoins through it (m3, #581
/// review): it is skipped. Every other refusal is returned.
pub async fn send_follows<T: PlanTransport>(
    transport: &mut T,
    plan: GroupPlan,
    floor: PlanFloor,
    leader_term: u64,
    members: &[PlanMember],
) -> PlanSent {
    let mut taken = vec![plan.leader];
    let mut refused = Vec::new();
    for member in members
        .iter()
        .filter(|member| member.node_id != plan.leader && !member.needs_vote_floor)
    {
        let request = RecoveryPlanRequest {
            point_entry: plan.point_entry,
            leader: plan.leader,
            point: plan.point,
            cut: plan.cut,
            step: PlanStep::Follow { leader_term },
            floor,
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
    /// The last plan sent was satisfied everywhere the tool reached in the
    /// leader's membership. `outside` are the reached replicas outside it,
    /// which keep the replaced history.
    Satisfied {
        plan: GroupPlan,
        plans_sent: usize,
        outside: Vec<u64>,
        /// Reached replicas whose group stopped: not waited for.
        stopped: Vec<u64>,
    },
}

/// Why [`recover_group`] stopped without a satisfied plan.
#[derive(Debug, thiserror::Error)]
pub enum RecoverError {
    #[error(transparent)]
    Plan(#[from] PlanError),
    #[error(transparent)]
    Floor(#[from] FloorError),
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
/// tool reached (m1 and m2, #581 review), under `floor`. Pure, so it is
/// tested alone.
pub fn next_step(
    state: &mut LoopState,
    members: &[PlanMember],
    floor: &Floor,
    max_plans: usize,
) -> Result<Step, RecoverError> {
    if let Some(sent) = state.sent
        && plan_satisfied(sent.plan.point, members)
    {
        return Ok(Step::Done(RecoverOutcome::Satisfied {
            plan: sent.plan,
            plans_sent: state.plans_sent,
            outside: outside_leaders_membership(members),
            stopped: members
                .iter()
                .filter(|member| !member.running)
                .map(|member| member.node_id)
                .collect(),
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
    // that may still open. Replicas that refused for good are left out,
    // but the voter sets they report still count (#613).
    let plan = match plan_group_excluding(members, &state.excluded, floor) {
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
/// left out of the next plan, and a plan its members can elect stands:
/// they hold a majority of every voter set in `electorate`, the sets the
/// leader campaigns under ([`electorate`]). A `leader_not_voter` refusal of
/// the leader changed nothing: the membership moved since the tool read
/// it, so the tool reads it again and plans again (#613). Nor did a
/// `floor_not_held` refusal: the next plan leaves that replica out, so a
/// voter of the certified membership that holds its entry leads.
pub fn plan_sent(
    state: &mut LoopState,
    plan: GroupPlan,
    sent: Result<PlanSent, SendError>,
    electorate: &[BTreeSet<u64>],
) -> Result<(), RecoverError> {
    match sent {
        Ok(sent) => {
            state.sent = Some(SentPlan {
                plan,
                leader_term: Some(sent.leader_term),
            });
            let mut replan_now = false;
            for error in &sent.refused {
                match error.refusal() {
                    Some(RecoveryPlanRefusal::FollowsLeader { leader }) => {
                        state.follows_leader = Some(leader);
                    }
                    // M-A of the #613 review: the member knows more is
                    // committed than the point. The plan picked the wrong
                    // leader: plan again from a new read.
                    Some(RecoveryPlanRefusal::RecoveryPointBehind) => {
                        replan_now = true;
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
            let elects = !electorate.is_empty()
                && electorate.iter().all(|voters| {
                    sent.taken
                        .iter()
                        .filter(|node_id| voters.contains(node_id))
                        .count()
                        .saturating_mul(2)
                        > voters.len()
                });
            if (replan_now || !elects) && state.follows_leader.is_none() {
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
            // The leader does not hold the certified membership entry: it
            // changed nothing, and another voter of that membership may
            // hold it. The next plan leaves this replica out.
            Some(RecoveryPlanRefusal::FloorNotHeld) => {
                tracing::warn!(%error, "recovery: the plan's leader lacks the certified entry");
                state.excluded.insert(plan.leader);
                state.replan = true;
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

/// Reads every reachable replica of `target`.
async fn observe(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    target: RecoverTarget,
) -> Vec<PlanMember> {
    let mut members = Vec::new();
    for node in nodes {
        match client.fetch_node(node).await {
            Ok(view) => {
                if let Some(replica) = target.replica(&view) {
                    members.push(PlanMember::from_metrics(replica));
                }
            }
            Err(error) => {
                tracing::warn!(node_id = node.id, %error, "recovery: node did not answer");
            }
        }
    }
    members
}

/// The confirmed meta view of the first manifest node that answers one,
/// `None` when none does.
async fn confirmed_view(client: &MetricsClient, nodes: &[NodeInfo]) -> Option<ClusterView> {
    crate::placement::confirmed_status(client, nodes)
        .await
        .map(|(_, status)| status.view)
}

/// [`PlanTransport`] over the nodes' admin planes.
struct HttpTransport<'a> {
    client: &'a MetricsClient,
    nodes: &'a [NodeInfo],
    target: RecoverTarget,
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
                self.target.path_segment()
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

/// Plans `target` and waits until the plan is satisfied, as [`next_step`]
/// decides on every observation, up to [`RecoverOptions::max_plans`]
/// plans. A data group's floor is read once, first ([`plan_floor`]), so a
/// data group without a confirmed meta view is refused before anything is
/// read or sent.
pub async fn recover_group(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    target: RecoverTarget,
    options: &RecoverOptions,
) -> Result<RecoverOutcome, RecoverError> {
    let confirmed = match target {
        RecoverTarget::Meta => None,
        RecoverTarget::Group(_) => confirmed_view(client, nodes).await,
    };
    let floor = plan_floor(target, confirmed.as_ref())?;
    let mut transport = HttpTransport {
        client,
        nodes,
        target,
        timeout: options.accept_timeout,
    };
    let mut state = LoopState::default();
    let settle = |now: Instant| now.checked_add(options.settle_timeout).unwrap_or(now);
    let mut deadline = settle(Instant::now());
    loop {
        let members = observe(client, nodes, target).await;
        match next_step(&mut state, &members, &floor, options.max_plans)? {
            Step::Done(outcome) => return Ok(outcome),
            Step::Wait => {}
            Step::Plan(plan) if !options.accept_loss => {
                return Ok(RecoverOutcome::DryRun(explain_plan(plan, &members, &floor)));
            }
            Step::Plan(plan) => {
                state.plans_sent = state.plans_sent.saturating_add(1);
                tracing::info!(
                    %target,
                    leader = plan.leader,
                    point = plan.point,
                    cut = plan.cut,
                    highest_term = plan.highest_term,
                    plans_sent = state.plans_sent,
                    "recovery: sending the plan, the leader first"
                );
                deadline = settle(Instant::now());
                let sent = send_plan(&mut transport, plan, floor.plan_floor(), &members).await;
                if let Ok(sent) = &sent {
                    for error in &sent.refused {
                        tracing::warn!(%target, %error, "recovery: a member refused the plan");
                    }
                }
                plan_sent(&mut state, plan, sent, &electorate(&plan, &members, &floor))?;
            }
            Step::Resume { plan, leader_term } => {
                tracing::info!(
                    %target,
                    leader_term,
                    "recovery: the plan's leader recorded the plan; sending it to the members"
                );
                let sent = send_follows(
                    &mut transport,
                    plan,
                    floor.plan_floor(),
                    leader_term,
                    &members,
                )
                .await;
                plan_sent(
                    &mut state,
                    plan,
                    Ok(sent),
                    &electorate(&plan, &members, &floor),
                )?;
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
    use ursula_proto::admin::PlanFloor;
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
            committed_entry: committed.map(|_| ursula_proto::admin::EntryId {
                term: 3,
                node_id: 1,
            }),
            last_applied: committed,
            gate,
            plan: None,
            leads_live: false,
            history_event_index: Some(1),
            current_leader: None,
            voter_configs: vec![[1, 2, 3].into()],
            committed_voter_configs: vec![[1, 2, 3].into()],
            learner_ids: vec![],
            running: true,
            needs_vote_floor: false,
        }
    }

    /// The voter set of a three-voter group, as the leader campaigns.
    fn three() -> Vec<std::collections::BTreeSet<u64>> {
        vec![[1, 2, 3].into()]
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
                floor: 3,
                point_entry: None
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
                floor: 5,
                point_entry: None
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
                voters: [1, 2, 3].into(),
                without_vote_floor: [].into(),
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
                floor: 8,
                point_entry: None
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
        let sent = super::send_plan(&mut table, plan, PlanFloor::Seeded, &members)
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
        let sent = super::send_plan(&mut table, plan, PlanFloor::Seeded, &members)
            .await
            .expect("the leader took it");
        assert_eq!(sent.refused.len(), 1);
        table.0.insert(1, Some(RecoveryPlanRefusal::LeadsLive));
        let refused = super::send_plan(&mut table, plan, PlanFloor::Seeded, &members)
            .await
            .expect_err("the leader refused");
        assert_eq!(refused.refusal(), Some(RecoveryPlanRefusal::LeadsLive));
    }

    /// The plan floor of each target and confirmed view: meta always plans
    /// with `meta`, and a data group only with `seeded` from a confirmed,
    /// bootstrapped view that shows it seeded.
    #[test]
    fn the_plan_floor_comes_from_the_target_and_a_confirmed_view() {
        use std::collections::BTreeMap;
        use std::collections::BTreeSet;

        use ursula_control::ClusterView;
        use ursula_control::ControlPlaneState;
        use ursula_control::samples::sample_state;

        use super::CertifiedMembership;
        use super::Floor;
        use super::FloorError;
        use super::RecoverTarget;
        use super::group_floor;
        use super::plan_floor;
        // Group 0 of the sample is seeded, group 1 live and stable on
        // {1, 3}, group 2 live and moving from {1, 3} to {1, 2}.
        let ControlPlaneState::Bootstrapped(cluster) = sample_state() else {
            panic!("the sample state is bootstrapped");
        };
        let converged_at = |group| match cluster.group(ursula_shard::RaftGroupId(group)) {
            Some(ursula_control::GroupPlacement::Live { converged_at, .. }) => *converged_at,
            other => panic!("group {group} of the sample is live: {other:?}"),
        };
        let (stable, moving) = (converged_at(1), converged_at(2));
        assert_ne!(stable, moving);
        let bootstrapped = ClusterView::Bootstrapped(cluster);
        assert_eq!(group_floor(0, Some(&bootstrapped)), Ok(Floor::Seeded));
        // A live group's floor certifies its `converged_at`, with the voters
        // it converged on: a moving group's are its source, not its target.
        for (live, entry) in [(1, stable), (2, moving)] {
            let floor = group_floor(live, Some(&bootstrapped));
            assert_eq!(
                floor,
                Ok(Floor::Certified(CertifiedMembership {
                    entry,
                    voters: [1, 3].into(),
                }))
            );
            assert_eq!(
                floor.map(|floor| floor.plan_floor()),
                Ok(PlanFloor::Certified(entry))
            );
        }
        assert_eq!(
            group_floor(3, Some(&bootstrapped)),
            Err(FloorError::UnknownGroup { group: 3 })
        );
        assert_eq!(
            group_floor(0, None),
            Err(FloorError::NoConfirmedView { group: 0 })
        );
        for unbootstrapped in [
            ClusterView::MetaGenesis {
                unanswered: BTreeSet::new(),
                genesis_mismatch: BTreeMap::new(),
            },
            ClusterView::MetaCatchingUp,
            ClusterView::Registering {
                registered: std::collections::BTreeMap::new(),
                waiting_for: BTreeSet::new(),
            },
            ClusterView::FormationCut,
        ] {
            assert_eq!(
                group_floor(0, Some(&unbootstrapped)),
                Err(FloorError::NotBootstrapped { group: 0 })
            );
        }
        // Refusals of a data group name the meta group's recovery first.
        for refused in [
            FloorError::NoConfirmedView { group: 0 },
            FloorError::NotBootstrapped { group: 0 },
        ] {
            assert!(refused.to_string().contains("--target meta"), "{refused}");
        }
        // Meta needs no view, and ignores one.
        assert_eq!(plan_floor(RecoverTarget::Meta, None), Ok(Floor::Meta));
        assert_eq!(
            plan_floor(RecoverTarget::Meta, Some(&bootstrapped)),
            Ok(Floor::Meta)
        );
        assert_eq!(
            plan_floor(RecoverTarget::Group(0), Some(&bootstrapped)),
            Ok(Floor::Seeded)
        );
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
        let step =
            super::next_step(&mut state, &members, &super::Floor::Seeded, 3).expect("a step");
        assert!(
            matches!(step, super::Step::Plan(plan) if plan.leader == 1),
            "{step:?}"
        );
        members[0].leads_live = true;
        let mut state = super::LoopState::default();
        assert!(matches!(
            super::next_step(&mut state, &members, &super::Floor::Seeded, 3),
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
                super::next_step(&mut state, &members, &super::Floor::Seeded, 3).expect("a step"),
                super::Step::Wait
            );
        }
        // Without that leader, the wedge seen twice plans again.
        members[0].current_leader = None;
        members[0].log.current_term = 9;
        assert_eq!(
            super::next_step(&mut state, &members, &super::Floor::Seeded, 3).expect("a step"),
            super::Step::Wait
        );
        assert!(matches!(
            super::next_step(&mut state, &members, &super::Floor::Seeded, 3).expect("a step"),
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
                Err(refused(1, RecoveryPlanRefusal::MissingVoteFloor)),
                &three()
            ),
            Err(super::RecoverError::Unfixable { .. })
        ));
        // #613: the leader's `leader_not_voter` changed nothing: read the
        // membership again and plan again, leaving no replica out.
        let mut state = super::LoopState::default();
        super::plan_sent(
            &mut state,
            plan,
            Err(refused(1, RecoveryPlanRefusal::LeaderNotVoter)),
            &three(),
        )
        .expect("planned again");
        assert!(state.replan);
        assert!(state.excluded.is_empty());
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
            &three(),
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
            &three(),
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
            super::next_step(&mut state, &members, &super::Floor::Seeded, 3).expect("a step"),
            super::Step::Wait
        );
        assert!(state.replan);
        members[2].gate = RecoveryGateStatus::Stalled;
        assert!(matches!(
            super::next_step(&mut state, &members, &super::Floor::Seeded, 3).expect("a step"),
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
        super::plan_sent(&mut state, plan, Err(timeout), &three()).expect("recorded");
        let mut members = stalled();
        members[0].gate = RecoveryGateStatus::Cutting;
        assert_eq!(
            super::next_step(&mut state, &members, &super::Floor::Seeded, 3).expect("a step"),
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
            super::next_step(&mut state, &members, &super::Floor::Seeded, 3).expect("a step"),
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
            &three(),
        )
        .expect("recorded");
        assert!(matches!(
            super::next_step(&mut state, &stalled(), &super::Floor::Seeded, 3).expect("a step"),
            super::Step::Plan(_)
        ));
    }

    /// Item 3 of the #612 review: a learner never leads, even with the
    /// longest log, and a group with one voter never cuts.
    #[test]
    fn a_learner_never_leads_and_one_voter_never_cuts() {
        let mut learner = member(4, Some((20, 9)), 9, Some(12), RecoveryGateStatus::Open);
        learner.learner_ids = vec![4];
        let mut members = stalled();
        members.push(learner);
        assert_eq!(plan_group(&members).expect("a plan").leader, 1);

        // One voter, gated, whose log ends below the floor a learner's vote
        // term sets: no cut.
        let mut voter = member(1, Some((12, 3)), 3, Some(10), RecoveryGateStatus::Stalled);
        voter.voter_configs = vec![[1].into()];
        voter.committed_voter_configs = vec![[1].into()];
        voter.learner_ids = vec![2];
        let mut learner = member(2, Some((12, 3)), 7, Some(12), RecoveryGateStatus::Stalled);
        learner.voter_configs = vec![[1].into()];
        learner.committed_voter_configs = vec![[1].into()];
        learner.learner_ids = vec![2];
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
        let explained = super::explain_plan(plan, &members, &super::Floor::Seeded);
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
            &three(),
        )
        .expect("the plan stands");
        assert!(!state.replan);
        assert!(state.excluded.contains(&3));
        // A later plan leaves node 3 out: two of three voters still answer.
        state.replan = true;
        let step =
            super::next_step(&mut state, &stalled(), &super::Floor::Seeded, 3).expect("a step");
        assert!(matches!(step, super::Step::Plan(_)), "{step:?}");
        // Without a majority left, the tool stops.
        state.excluded.insert(2);
        state.replan = true;
        assert!(matches!(
            super::next_step(&mut state, &stalled(), &super::Floor::Seeded, 3),
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
            &three(),
        )
        .expect("recorded");
        assert!(!state.replan);
        assert_eq!(state.follows_leader, Some(5));
        let mut members = stalled();
        members[1].current_leader = Some(5);
        assert_eq!(
            super::next_step(&mut state, &members, &super::Floor::Seeded, 3).expect("a step"),
            super::Step::Wait
        );
        // A fresh run sees the open replica follow the unreached leader.
        let mut members = stalled();
        members[0].current_leader = Some(5);
        let mut fresh = super::LoopState::default();
        assert_eq!(
            super::next_step(&mut fresh, &members, &super::Floor::Seeded, 3).expect("a step"),
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
            super::next_step(&mut state, &members, &super::Floor::Seeded, 3).expect("a step"),
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
            &three(),
        )
        .expect("recorded");
        assert_eq!(state.plans_sent, 0);
        assert!(state.replan);
    }

    /// A rolled-back promotion of node 4 (#613): the leader `1` and node 4
    /// hold the joint change, voters 2 and 3 never appended it.
    fn promoted(cut_off: bool) -> Vec<PlanMember> {
        let joint = vec![[1, 2, 3].into(), [1, 2, 3, 4].into()];
        let gate = if cut_off {
            RecoveryGateStatus::Open
        } else {
            RecoveryGateStatus::Stalled
        };
        let mut leader = member(1, Some((13, 3)), 3, Some(11), gate);
        leader.voter_configs = joint.clone();
        leader.learner_ids = vec![];
        let mut added = member(4, Some((13, 3)), 3, Some(11), RecoveryGateStatus::Open);
        added.voter_configs = joint;
        // With a cut it keeps the learner it was before the promotion.
        added.committed_voter_configs = vec![[1, 2, 3].into()];
        added.learner_ids = vec![];
        let term = if cut_off { 5 } else { 3 };
        let mut members = vec![
            leader,
            member(
                2,
                Some((11, 3)),
                term,
                Some(11),
                RecoveryGateStatus::Stalled,
            ),
            member(
                3,
                Some((11, 3)),
                term,
                Some(11),
                RecoveryGateStatus::Stalled,
            ),
            added,
        ];
        for member in members.iter_mut().skip(1).take(2) {
            member.learner_ids = vec![4];
        }
        members[0].committed_voter_configs = vec![[1, 2, 3].into()];
        members
    }

    /// #613: without a cut the added voter may lead under the joint
    /// change; with a cut it keeps no vote, so a voter of the membership
    /// it keeps leads instead, and the plan needs a majority of both
    /// halves of the joint change.
    #[test]
    fn a_rolled_back_promotion_is_planned_under_the_membership_each_leader_campaigns_under() {
        let plan = plan_group(&promoted(false)).expect("a plan");
        assert_eq!((plan.leader, plan.cut, plan.point), (4, false, 13));
        assert_eq!(
            super::electorate(&plan, &promoted(false), &super::Floor::Seeded),
            vec![[1, 2, 3].into(), [1, 2, 3, 4].into()]
        );
        let plan = plan_group(&promoted(true)).expect("a plan");
        assert!(plan.cut, "{plan:?}");
        assert_eq!(
            plan.leader, 3,
            "the greatest committed voter that keeps its vote"
        );
        assert_eq!(
            super::electorate(&plan, &promoted(true), &super::Floor::Seeded),
            three()
        );
        let members = promoted(false);
        assert_eq!(
            plan_group(&[members[0].clone(), members[3].clone(), members[1].clone()])
                .map(|plan| (plan.leader, plan.cut)),
            Ok((4, false))
        );
        // One of three voters of the joint change's first half: no plan.
        assert!(matches!(
            super::plan_group_excluding(&members, &[2, 3].into(), &super::Floor::Seeded),
            Err(PlanError::NoMajority { .. })
        ));
    }

    /// #613: the replicas outside the leader's membership never receive the
    /// event: the plan is satisfied without them, and they are named.
    #[test]
    fn a_replica_outside_the_leaders_membership_is_not_waited_for() {
        let mut members = stalled();
        for member in &mut members {
            member.history_event_index = Some(13);
            member.current_leader = Some(1);
        }
        let mut removed = member(4, Some((12, 3)), 3, Some(12), RecoveryGateStatus::Planned);
        removed.plan = Some(planned(1));
        removed.voter_configs = vec![[1, 2, 3, 4].into()];
        members.push(removed);
        assert!(plan_satisfied(12, &members));
        assert_eq!(super::outside_leaders_membership(&members), vec![4]);
        members[0].learner_ids = vec![4];
        assert!(
            !plan_satisfied(12, &members),
            "a learner of the leader waits"
        );
    }

    /// m1 of the #613 review: R1 against the union rule. Five voters shrank
    /// to `{1,2,3}`, committed. The tool reaches 3, which holds the shrink,
    /// and 4 and 5, which hold only its joint step. The union counts 3 of 5;
    /// R1 sees that only one of `{1,2,3}` answered, and the quorum `{1,2}`
    /// that may have committed every later entry is out of reach.
    #[test]
    fn every_reported_voter_set_needs_half_its_voters_not_the_union() {
        let joint = vec![[1, 2, 3, 4, 5].into(), [1, 2, 3].into()];
        let mut shrunk = member(3, Some((20, 4)), 4, Some(20), RecoveryGateStatus::Stalled);
        shrunk.voter_configs = vec![[1, 2, 3].into()];
        shrunk.committed_voter_configs = vec![[1, 2, 3].into()];
        let mut members = vec![shrunk];
        for node_id in [4, 5] {
            let mut removed = member(
                node_id,
                Some((18, 4)),
                4,
                Some(17),
                RecoveryGateStatus::Stalled,
            );
            removed.voter_configs = joint.clone();
            removed.committed_voter_configs = vec![[1, 2, 3, 4, 5].into()];
            members.push(removed);
        }
        assert!(matches!(
            plan_group(&members),
            Err(PlanError::NoMajority { answered: 1, ref voters, .. }) if *voters == [1, 2, 3].into()
        ));
        // The union rule would have planned: 3 of 5 answered.
        let union = members
            .iter()
            .flat_map(|member| member.voter_configs.iter().flatten())
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert!(members.len().saturating_mul(2) > union.len());
        // With node 2 reached too, two of `{1,2,3}` meet every quorum of it.
        let mut two = member(2, Some((20, 4)), 4, Some(20), RecoveryGateStatus::Stalled);
        two.voter_configs = vec![[1, 2, 3].into()];
        members.push(two);
        plan_group(&members).expect("two of {1,2,3} meet every quorum");
    }

    /// m6 reverted: an even voter set needs a strict majority too. A
    /// promotion of node 4 with nodes 3 and 4 down: 1 and 2 are not planned,
    /// since such a plan could not win.
    #[test]
    fn an_even_voter_set_still_needs_a_majority() {
        let joint = vec![[1, 2, 3].into(), [1, 2, 3, 4].into()];
        let members = [1, 2].map(|node_id| {
            let mut kept = member(
                node_id,
                Some((12, 3)),
                5,
                Some(10),
                RecoveryGateStatus::Stalled,
            );
            kept.voter_configs = joint.clone();
            kept
        });
        assert!(matches!(
            plan_group(&members),
            Err(PlanError::NoMajority { answered: 2, ref voters, .. }) if voters.len() == 4
        ));
    }

    /// M-A of the #613 review: a replica that knows a later committed entry
    /// than the replica the plan would elect, here a promoted voter that a
    /// cut makes a learner again, refuses the plan, typed, instead of having
    /// its committed entries overwritten.
    #[test]
    fn a_plan_never_continues_below_a_reached_committed_index() {
        let mut members = promoted(true);
        // Node 4 knows entry 12 is committed; the voters only 11. Node 1's
        // log reaches 13, so the cut keeps up to 12 from it, and node 1
        // must hold exactly node 4's committed entry (F3 of the third #613
        // review).
        members[3].committed = Some(12);
        assert_eq!(
            plan_group(&members).map(|plan| (plan.leader, plan.point, plan.point_entry)),
            Ok((
                1,
                12,
                Some(ursula_proto::admin::EntryId {
                    term: 3,
                    node_id: 1
                })
            ))
        );
        // No eligible log reaches it: refused, typed.
        members[0].log.last_log_index = Some(11);
        members[0].log.last_log_term = Some(3);
        assert_eq!(
            plan_group(&members),
            Err(PlanError::BelowCommitted {
                node_id: 4,
                committed: 12,
                point: 11
            })
        );
        // Without that knowledge, the cut leads from the voters.
        members[3].committed = Some(11);
        members[3].last_applied = Some(11);
        assert_eq!(plan_group(&members).map(|plan| plan.point), Ok(11));
    }

    /// D of the fourth #613 review: a cut to a later entry than the
    /// leader's marker counts both the membership it keeps at the marker and
    /// its effective one.
    #[test]
    fn a_raised_cut_counts_the_kept_and_the_effective_voter_sets() {
        let mut members = promoted(true);
        members[3].committed = Some(12);
        let plan = plan_group(&members).expect("a raised cut");
        assert!(plan.point_entry.is_some());
        let leader = &members[0];
        let sets = super::electorate(&plan, &members, &super::Floor::Seeded);
        for voters in leader
            .committed_voter_configs
            .iter()
            .chain(leader.voter_configs.iter())
        {
            assert!(sets.contains(voters), "{sets:?} lacks {voters:?}");
        }
    }

    /// F2 of the third #613 review: a replica whose group stopped reports
    /// no committed index but what it applied, which was committed: the
    /// plan never continues below it, though the replica is not planned.
    #[test]
    fn a_stopped_replicas_applied_index_bounds_the_point() {
        let mut members = stalled();
        let mut stopped = member(4, Some((20, 3)), 3, None, RecoveryGateStatus::Open);
        stopped.running = false;
        stopped.last_applied = Some(15);
        stopped.voter_configs = vec![];
        stopped.committed_voter_configs = vec![];
        members.push(stopped);
        assert!(matches!(
            plan_group(&members),
            Err(PlanError::BelowCommitted {
                node_id: 4,
                committed: 15,
                ..
            })
        ));
    }

    /// M-A: a member's `recovery_point_behind` plans again, even when the
    /// members that took the plan could elect its leader.
    #[test]
    fn a_member_behind_the_point_plans_again() {
        use ursula_proto::admin::RecoveryPlanRefusal;
        let plan = plan_group(&stalled()).expect("a plan");
        let mut state = super::LoopState::default();
        super::plan_sent(
            &mut state,
            plan,
            Ok(super::PlanSent {
                leader_term: 9,
                taken: vec![1, 2],
                refused: vec![refused(3, RecoveryPlanRefusal::RecoveryPointBehind)],
            }),
            &three(),
        )
        .expect("recorded");
        assert!(state.replan);
    }

    /// m8: a replica whose apply stopped never counts.
    #[test]
    fn a_stopped_replica_never_counts_toward_a_voter_set() {
        let mut members = stalled();
        members.pop();
        members[1].running = false;
        assert!(matches!(
            plan_group(&members),
            Err(PlanError::NoMajority { answered: 1, .. })
        ));
    }

    /// m4: a single voter's own sets decide, as on the server: a shrink to
    /// node 1 whose holder still reports the joint step is cut, and node 1
    /// alone is not.
    #[test]
    fn the_single_voter_rule_reads_the_leaders_own_voter_sets() {
        let mut alone = member(1, Some((12, 3)), 3, Some(10), RecoveryGateStatus::Stalled);
        alone.voter_configs = vec![[1].into()];
        alone.committed_voter_configs = vec![[1].into()];
        let mut floor = member(2, Some((11, 3)), 7, Some(10), RecoveryGateStatus::Stalled);
        floor.voter_configs = vec![[1].into()];
        floor.committed_voter_configs = vec![[1].into()];
        let plan = plan_group(&[alone.clone(), floor.clone()]).expect("a plan");
        assert!(!plan.cut, "{plan:?}");
        alone.committed_voter_configs = vec![[1, 2].into(), [1].into()];
        let plan = plan_group(&[alone, floor]).expect("a plan");
        assert!(plan.cut, "{plan:?}");
    }

    /// m8: a replica whose group stopped is not waited for.
    #[test]
    fn a_stopped_replica_is_not_waited_for() {
        let mut members = stalled();
        for member in &mut members {
            member.history_event_index = Some(13);
            member.current_leader = Some(1);
        }
        members[2].history_event_index = None;
        assert!(!plan_satisfied(12, &members));
        members[2].running = false;
        assert!(plan_satisfied(12, &members));
    }

    /// Design 7.4's group: moved from {1, 2, 3} to {4, 5, 6} and the old
    /// voters torn down. After a full power loss under `fsync = never`
    /// every new voter's commit marker (15) lies below the joint change, so
    /// the membership each keeps at its marker is the old one, while each
    /// log holds the uniform entry at index 20 that the last convergence
    /// certified, written by node 5 at term 4.
    fn moved_after_power_loss() -> Vec<PlanMember> {
        [(4, 22), (5, 21), (6, 20)]
            .into_iter()
            .map(|(node_id, last)| {
                let mut moved = member(
                    node_id,
                    Some((last, 4)),
                    6,
                    Some(15),
                    RecoveryGateStatus::Stalled,
                );
                moved.voter_configs = vec![[4, 5, 6].into()];
                moved.committed_voter_configs = vec![[1, 2, 3].into()];
                moved
            })
            .collect()
    }

    fn certified_at_20() -> super::Floor {
        use ursula_proto::placement::MembershipLogId;
        use ursula_proto::placement::NodeId;
        use ursula_proto::placement::RaftLogId;
        super::Floor::Certified(super::CertifiedMembership {
            entry: MembershipLogId::new(RaftLogId {
                term: 4,
                leader: NodeId::new(5),
                index: 20,
            }),
            voters: [4, 5, 6].into(),
        })
    }

    /// Design 7.4: a cutting plan of a moved group keeps the membership
    /// its last convergence certified. Without the certified floor the
    /// planner counts the torn-down old voters, which kept markers make
    /// look current, and finds no plan; with it, the cut keeps every log up
    /// to the certified entry, a voter of that membership leads, and only
    /// that membership elects it.
    #[test]
    fn a_cut_of_a_moved_group_keeps_its_certified_membership() {
        let members = moved_after_power_loss();
        assert!(matches!(
            plan_group(&members),
            Err(PlanError::NoMajority { answered: 0, ref voters, .. }) if *voters == [1, 2, 3].into()
        ));
        let floor = certified_at_20();
        let plan = super::plan_group_excluding(&members, &Default::default(), &floor)
            .expect("a certified plan");
        assert_eq!(plan, GroupPlan {
            leader: 6,
            point: 20,
            cut: true,
            highest_term: 6,
            floor: 6,
            point_entry: Some(ursula_proto::admin::EntryId {
                term: 4,
                node_id: 5
            }),
        });
        assert_eq!(super::electorate(&plan, &members, &floor), vec![
            [4, 5, 6].into()
        ]);
        // The certified voters' sets alone count: two of three answering
        // still plan, one does not.
        assert_eq!(
            super::plan_group_excluding(&members[..2], &Default::default(), &floor)
                .map(|plan| (plan.leader, plan.point)),
            Ok((5, 20))
        );
        assert!(matches!(
            super::plan_group_excluding(&members[..1], &Default::default(), &floor),
            Err(PlanError::NoMajority { answered: 1, .. })
        ));
        // A replica whose marker reaches the entry is seen as it reports
        // itself, and a plan from it needs no raise.
        let mut caught_up = members.clone();
        caught_up[1].committed = Some(21);
        caught_up[1].last_applied = Some(21);
        caught_up[1].committed_voter_configs = vec![[4, 5, 6].into()];
        let plan = super::plan_group_excluding(&caught_up, &Default::default(), &floor)
            .expect("a certified plan");
        assert_eq!((plan.leader, plan.point, plan.point_entry), (5, 21, None));
    }

    /// Design 7.4: no voter of the certified membership whose log reaches
    /// the certified entry means every plan would drop it, and the tool
    /// refuses before it sends anything.
    #[test]
    fn a_certified_entry_no_voter_reaches_is_refused() {
        let mut members = moved_after_power_loss();
        for member in &mut members {
            member.log.last_log_index = Some(19);
        }
        assert!(matches!(
            super::plan_group_excluding(&members, &Default::default(), &certified_at_20()),
            Err(PlanError::CertifiedNotReached { certified }) if certified.get().index == 20
        ));
    }

    /// Design 7.4: holding the certified entry takes its term too, as the
    /// server's `certified_held` checks the exact log id. Certified voters
    /// {1, 2, 4} with the entry at index 20 written by node 2 at term 4.
    /// Node 1, an old term-3 leader, has an uncommitted tail to 30 and its
    /// marker at 18. Node 2 holds the entry with its marker at 15. Node 4
    /// does not answer. A planner that read "my log reaches index 20" as
    /// holding the entry would see node 1 keep the certified membership and
    /// raise the cut from node 1's greater marker, and node 1 would refuse
    /// with `floor_not_held`. Node 2 leads, and two of the three certified
    /// voters take it.
    #[test]
    fn a_certified_voter_with_a_longer_tail_at_a_lower_term_is_not_chosen() {
        use ursula_proto::placement::MembershipLogId;
        use ursula_proto::placement::NodeId;
        use ursula_proto::placement::RaftLogId;
        let floor = super::Floor::Certified(super::CertifiedMembership {
            entry: MembershipLogId::new(RaftLogId {
                term: 4,
                leader: NodeId::new(2),
                index: 20,
            }),
            voters: [1, 2, 4].into(),
        });
        let members = [(1, (30, 3), 18), (2, (20, 4), 15)]
            .into_iter()
            .map(|(node_id, last, marker)| {
                let mut stalled = member(
                    node_id,
                    Some(last),
                    5,
                    Some(marker),
                    RecoveryGateStatus::Stalled,
                );
                stalled.voter_configs = vec![[1, 2, 4].into()];
                stalled
            })
            .collect::<Vec<_>>();
        let plan = super::plan_group_excluding(&members, &Default::default(), &floor)
            .expect("a certified plan");
        assert_eq!(
            (plan.leader, plan.point, plan.cut, plan.point_entry),
            (
                2,
                20,
                true,
                Some(ursula_proto::admin::EntryId {
                    term: 4,
                    node_id: 2
                })
            )
        );
        assert_eq!(super::electorate(&plan, &members, &floor), vec![
            [1, 2, 4].into()
        ]);
        // Without the certified voter that holds the entry, node 1's longer
        // log does not stand in for it, with node 4 answering short of it.
        let mut without_two = members.clone();
        let mut short = member(4, Some((19, 4)), 5, Some(15), RecoveryGateStatus::Stalled);
        short.voter_configs = vec![[1, 2, 4].into()];
        without_two.push(short);
        assert!(matches!(
            super::plan_group_excluding(&without_two, &[2].into(), &floor),
            Err(PlanError::CertifiedNotReached { certified }) if certified.get().index == 20
        ));
    }

    /// The plan's leader that lacks the certified entry refuses and changes
    /// nothing (`floor_not_held`, no refusal no plan fixes). The tool then
    /// leaves it out and plans again with another voter of the certified
    /// membership, instead of stopping or sending the same plan again.
    #[test]
    fn a_leader_without_the_certified_entry_is_left_out_of_the_next_plan() {
        use ursula_proto::admin::RecoveryPlanRefusal;
        assert!(!RecoveryPlanRefusal::FloorNotHeld.no_plan_fixes());
        let members = moved_after_power_loss();
        let floor = certified_at_20();
        let mut state = super::LoopState::default();
        let super::Step::Plan(plan) =
            super::next_step(&mut state, &members, &floor, 3).expect("a step")
        else {
            panic!("a plan");
        };
        assert_eq!(plan.leader, 6);
        state.plans_sent = 1;
        super::plan_sent(
            &mut state,
            plan,
            Err(refused(6, RecoveryPlanRefusal::FloorNotHeld)),
            &super::electorate(&plan, &members, &floor),
        )
        .expect("planned again, not stopped");
        assert!(state.replan);
        assert_eq!(state.excluded, [6].into());
        let step = super::next_step(&mut state, &members, &floor, 3).expect("a step");
        assert!(
            matches!(step, super::Step::Plan(plan) if plan.leader == 5 && plan.point == 20),
            "{step:?}"
        );
    }

    /// f3b gap 2: a replica that lost its vote and holds no vote floor
    /// refuses every plan, so the planner never counts it and the tool
    /// sends it nothing. Counted, a dry run showed a plan no member could
    /// take, and with `--accept-loss` the leader took it before the members
    /// refused.
    #[tokio::test]
    async fn replicas_without_a_vote_floor_are_neither_counted_nor_sent_to() {
        use ursula_proto::admin::RecoveryPlanRefusal;
        let mut members = stalled();
        members[1].needs_vote_floor = true;
        members[2].needs_vote_floor = true;
        assert_eq!(
            plan_group(&members),
            Err(PlanError::NoMajority {
                answered: 1,
                voters: [1, 2, 3].into(),
                without_vote_floor: [2, 3].into(),
            })
        );
        // One of three without a floor: the other two plan, and the plan
        // goes to them only.
        members[1].needs_vote_floor = false;
        let plan = plan_group(&members).expect("a plan");
        let mut table = Table([(3, Some(RecoveryPlanRefusal::MissingVoteFloor))].into());
        let sent = super::send_plan(&mut table, plan, PlanFloor::Seeded, &members)
            .await
            .expect("the leader took it");
        assert_eq!(sent.taken, vec![1, 2]);
        assert!(sent.refused.is_empty(), "{:?}", sent.refused);
        let explained = super::explain_plan(plan, &members, &super::Floor::Seeded);
        assert_eq!(explained.without_vote_floor, vec![3]);
    }
}
