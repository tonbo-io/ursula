//! The membership reconciler's planner: the one step a group leader takes
//! next toward its target.
//!
//! Each call re-derives the step from the committed membership, so a lost
//! response, a leadership change or a rolled-back view leaves nothing to
//! reconcile. The planner is pure: the shell around it reads the leader's
//! state, executes the step under a membership CAS on `expected`, and keeps
//! the step's future until OpenRaft answers.
//!
//! The rules, in order:
//!
//! 0. A replica that does not lead live, transfers, has a step pending or
//!    sees an uncommitted membership waits, and so does one in no voter
//!    config.
//! 1. A joint config is flattened onto one of its sides, the target when it
//!    has a live majority, else the side with more live voters. The leader
//!    hands off first when it is not on that side. Flattening adds no voter
//!    and needs no confirmed view.
//! 2. A uniform config equal to the target, or under a seeded target, only
//!    adjusts learners and reports. A settled target changes no voter.
//!    Under a moving target the leader adds the missing targets as
//!    learners, confirms its view, and then changes one voter per step:
//!    the dropped voter is never itself (it hands off instead), and the
//!    promoted one is a caught-up, listed learner with an open gate.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use ursula_proto::placement::LearnerLag;
use ursula_proto::placement::MembershipAnswer;
use ursula_proto::placement::MembershipLogId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::StepKind;
use ursula_proto::placement::VoterSet;
use ursula_proto::placement::WaitReason;

use super::state::GroupPlacement;
use super::state::Target;

/// Whether the leader's placement view is confirmed within its current data
/// leadership and meta history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ViewFreshness {
    Unconfirmed,
    Confirmed,
}

/// A learner's recovery gate, as its participation answer reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PeerGate {
    /// The learner applied a leader's barrier and may vote.
    Open,
    /// The learner refuses votes until a barrier opens its gate.
    Closed,
    /// No answer arrived.
    Unknown,
}

/// One OpenRaft membership config: the voters of one or two configs, and
/// the learners.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MembershipConfig {
    Uniform {
        voters: VoterSet,
        learners: BTreeSet<NodeId>,
    },
    /// A joint config, whose quorum is a majority of each side.
    Joint {
        old: VoterSet,
        new: VoterSet,
        learners: BTreeSet<NodeId>,
    },
}

impl MembershipConfig {
    /// The voter configs: one, or the two sides of a joint config.
    pub fn voter_configs(&self) -> impl Iterator<Item = &VoterSet> + '_ {
        let (first, second) = match self {
            Self::Uniform { voters, .. } => (voters, None),
            Self::Joint { old, new, .. } => (old, Some(new)),
        };
        std::iter::once(first).chain(second)
    }

    pub fn learners(&self) -> &BTreeSet<NodeId> {
        match self {
            Self::Uniform { learners, .. } | Self::Joint { learners, .. } => learners,
        }
    }

    /// Whether `node` is a voter of a config.
    pub fn is_voter(&self, node: NodeId) -> bool {
        self.voter_configs().any(|voters| voters.contains(node))
    }

    /// Whether `node` is a voter or a learner.
    pub fn is_member(&self, node: NodeId) -> bool {
        self.is_voter(node) || self.learners().contains(&node)
    }

    /// The voters of every config.
    pub fn voters(&self) -> BTreeSet<NodeId> {
        self.voter_configs()
            .flat_map(|voters| voters.iter())
            .collect()
    }

    /// The voters of every config and the learners.
    pub fn members(&self) -> BTreeSet<NodeId> {
        let mut members = self.voters();
        members.extend(self.learners().iter().copied());
        members
    }
}

/// How a live group leader classifies `node` from its committed and its
/// effective memberships: a voter of a config of either is `Voter`, another
/// member of either is `Learner`, and a node in neither is `NotMember`.
/// Every asker (the membership asker, the teardown and the removed-node
/// sweep) classifies with this one function.
pub fn membership_answer(
    node: NodeId,
    committed: &MembershipConfig,
    effective: &MembershipConfig,
) -> MembershipAnswer {
    if committed.is_voter(node) || effective.is_voter(node) {
        MembershipAnswer::Voter
    } else if committed.is_member(node) || effective.is_member(node) {
        MembershipAnswer::Learner
    } else {
        MembershipAnswer::NotMember
    }
}

/// The leader's effective membership.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EffectiveMembership {
    /// The effective membership is committed.
    Committed {
        log_id: MembershipLogId,
        config: MembershipConfig,
    },
    /// The effective membership is appended and not committed yet.
    Uncommitted { log_id: MembershipLogId },
}

/// What a leading replica knows when it plans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderView {
    pub membership: EffectiveMembership,
    pub committed_index: u64,
    /// Each member's matched log index, `None` when it matches nothing.
    pub matched: BTreeMap<NodeId, Option<u64>>,
    /// Members that acknowledged within the handoff ack window. The leader
    /// counts as live whether listed or not.
    pub live: BTreeSet<NodeId>,
    /// Voters a leadership handoff accepts now.
    pub handoff_ready: BTreeSet<NodeId>,
    /// The gates of caught-up learners.
    pub gates: BTreeMap<NodeId, PeerGate>,
    /// Targets this node's address book does not list.
    pub unlisted: BTreeSet<NodeId>,
    pub view: ViewFreshness,
}

impl LeaderView {
    /// Whether `node` matched the committed index.
    fn caught_up(&self, node: NodeId) -> bool {
        matches!(self.matched.get(&node), Some(Some(index)) if *index >= self.committed_index)
    }

    /// The smallest of `nodes` other than `me` that a handoff accepts now.
    fn handoff_target(&self, me: NodeId, nodes: impl Iterator<Item = NodeId>) -> Option<NodeId> {
        nodes
            .filter(|node| *node != me)
            .find(|node| self.handoff_ready.contains(node))
    }
}

/// Where a replica stands in its group's leadership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Leadership {
    /// Not the leader, or a leader that is not live.
    NotLeading,
    /// A leadership transfer to `to` is pending.
    Transferring {
        to: NodeId,
    },
    /// A step's future has not answered yet.
    StepPending,
    Leading(Box<LeaderView>),
}

/// The planner's input about the local replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipView {
    pub me: NodeId,
    pub leadership: Leadership,
}

/// What the placement asks of the group's membership.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MembershipTarget {
    pub voters: VoterSet,
    /// Learners the target keeps: the meta group's registered nodes. A data
    /// group's target has none.
    pub learners: BTreeSet<NodeId>,
    pub stage: TargetStage,
}

impl MembershipTarget {
    /// A data group's target: its placement's target voters, no learners,
    /// `Seeded` or `Moving` at the placement's key, `Settled` when stable.
    pub fn of_placement(placement: &GroupPlacement) -> Self {
        let since = placement.since();
        let stage = match placement {
            GroupPlacement::Seeded { .. } => TargetStage::Seeded { since },
            GroupPlacement::Live {
                target: Target::Moving(_),
                ..
            } => TargetStage::Moving { since },
            GroupPlacement::Live {
                target: Target::Stable { .. },
                ..
            } => TargetStage::Settled,
        };
        Self {
            voters: placement.target_voters().clone(),
            learners: BTreeSet::new(),
            stage,
        }
    }
}

/// How the target came to be, and so what reaching it means.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TargetStage {
    /// The Bootstrap seed: the group reports its voters as they are.
    Seeded { since: PlacementEpoch },
    /// A move: voters change toward the target, which is reported once
    /// reached.
    Moving { since: PlacementEpoch },
    /// A stable target: nothing is reported and no voter changes.
    Settled,
}

/// The one step a reconciler takes next. Every membership step names the
/// membership log id it was planned from, its CAS key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MembershipStep {
    AddLearners {
        expected: MembershipLogId,
        nodes: BTreeSet<NodeId>,
    },
    /// Replaces every voter: one joint and one uniform entry from a uniform
    /// config, or one entry from a joint config onto one of its sides.
    ChangeVoters {
        expected: MembershipLogId,
        voters: VoterSet,
    },
    RemoveLearners {
        expected: MembershipLogId,
        nodes: BTreeSet<NodeId>,
    },
    TransferLeadership {
        to: NodeId,
    },
    /// Confirm the placement view before changing voters.
    ConfirmView,
    /// The committed uniform membership has the target's voters: report
    /// them, after a leadership proof, unless the target is settled.
    Converged {
        voters: VoterSet,
    },
    Wait(WaitReason),
}

impl MembershipStep {
    /// The kind a reconcile status names, `None` for a wait.
    pub fn kind(&self) -> Option<StepKind> {
        match self {
            Self::AddLearners { .. } => Some(StepKind::AddLearners),
            Self::ChangeVoters { .. } => Some(StepKind::ChangeVoters),
            Self::RemoveLearners { .. } => Some(StepKind::RemoveLearners),
            Self::TransferLeadership { .. } => Some(StepKind::TransferLeadership),
            Self::ConfirmView => Some(StepKind::ConfirmView),
            Self::Converged { .. } => Some(StepKind::ReportConverged),
            Self::Wait(_) => None,
        }
    }
}

/// The next step of the replica `view` describes toward `target`.
pub fn plan_membership_step(view: &MembershipView, target: &MembershipTarget) -> MembershipStep {
    let leader = match &view.leadership {
        Leadership::NotLeading => return MembershipStep::Wait(WaitReason::NotLeader),
        Leadership::Transferring { .. } => {
            return MembershipStep::Wait(WaitReason::TransferPending);
        }
        Leadership::StepPending => return MembershipStep::Wait(WaitReason::StepPending),
        Leadership::Leading(leader) => leader,
    };
    let (expected, config) = match &leader.membership {
        EffectiveMembership::Uncommitted { .. } => {
            return MembershipStep::Wait(WaitReason::MembershipUncommitted);
        }
        EffectiveMembership::Committed { log_id, config } => (*log_id, config),
    };
    let me = view.me;
    if !config.is_voter(me) {
        return MembershipStep::Wait(WaitReason::NotAMember);
    }
    let mut live = leader.live.clone();
    live.insert(me);
    match config {
        MembershipConfig::Joint { old, new, .. } => {
            plan_joint_exit(me, expected, [old, new], &live, leader, target)
        }
        MembershipConfig::Uniform { voters, learners } => {
            let current = Current {
                me,
                expected,
                voters,
                learners,
                live: &live,
            };
            plan_uniform(&current, leader, target)
        }
    }
}

/// How many of `voters` are live.
fn live_count(voters: &VoterSet, live: &BTreeSet<NodeId>) -> usize {
    voters.iter().filter(|node| live.contains(node)).count()
}

/// Whether the live voters of `voters` are a majority of them.
fn has_live_majority(voters: &VoterSet, live: &BTreeSet<NodeId>) -> bool {
    live_count(voters, live).saturating_mul(2) > voters.len()
}

/// Rule 1: flatten the joint config `[old, new]` onto one side.
fn plan_joint_exit(
    me: NodeId,
    expected: MembershipLogId,
    sides: [&VoterSet; 2],
    live: &BTreeSet<NodeId>,
    leader: &LeaderView,
    target: &MembershipTarget,
) -> MembershipStep {
    let Some(exit) = joint_exit(sides, live, &target.voters) else {
        return MembershipStep::Wait(WaitReason::NoLiveQuorum);
    };
    if exit.contains(me) {
        return MembershipStep::ChangeVoters {
            expected,
            voters: exit.clone(),
        };
    }
    match leader.handoff_target(me, exit.iter()) {
        Some(to) => MembershipStep::TransferLeadership { to },
        None => MembershipStep::Wait(WaitReason::NoHandoffTarget),
    }
}

/// The side a joint config `[old, new]` exits onto: among the sides with a
/// live majority, the target, else the one with more live voters, a tie
/// going to the one sharing more voters with the target, then to `new`.
/// `None` when no side has a live majority.
fn joint_exit<'a>(
    [old, new]: [&'a VoterSet; 2],
    live: &BTreeSet<NodeId>,
    target: &VoterSet,
) -> Option<&'a VoterSet> {
    let old_ok = has_live_majority(old, live);
    let new_ok = has_live_majority(new, live);
    match (old_ok, new_ok) {
        (false, false) => None,
        (true, false) => Some(old),
        (false, true) => Some(new),
        (true, true) => {
            if new == target {
                return Some(new);
            }
            if old == target {
                return Some(old);
            }
            let (old_live, new_live) = (live_count(old, live), live_count(new, live));
            if old_live != new_live {
                return Some(if old_live > new_live { old } else { new });
            }
            let shared =
                |side: &VoterSet| side.iter().filter(|node| target.contains(*node)).count();
            Some(if shared(old) > shared(new) { old } else { new })
        }
    }
}

/// The leader's committed uniform config.
struct Current<'a> {
    me: NodeId,
    expected: MembershipLogId,
    voters: &'a VoterSet,
    learners: &'a BTreeSet<NodeId>,
    live: &'a BTreeSet<NodeId>,
}

/// Rule 2: from a uniform config.
fn plan_uniform(
    current: &Current<'_>,
    leader: &LeaderView,
    target: &MembershipTarget,
) -> MembershipStep {
    let Current {
        me,
        expected,
        voters,
        learners,
        live,
    } = *current;
    let reached = voters == &target.voters;
    if reached || matches!(target.stage, TargetStage::Seeded { .. }) {
        let extra: BTreeSet<NodeId> = learners.difference(&target.learners).copied().collect();
        if !extra.is_empty() {
            return MembershipStep::RemoveLearners {
                expected,
                nodes: extra,
            };
        }
        let absent: BTreeSet<NodeId> = target
            .learners
            .iter()
            .copied()
            .filter(|node| !voters.contains(*node) && !learners.contains(node))
            .collect();
        if !absent.is_empty() {
            return MembershipStep::AddLearners {
                expected,
                nodes: absent,
            };
        }
        return MembershipStep::Converged {
            voters: voters.clone(),
        };
    }
    if target.stage == TargetStage::Settled {
        return MembershipStep::Wait(WaitReason::TargetSettled);
    }
    let missing: BTreeSet<NodeId> = target
        .voters
        .iter()
        .filter(|node| !voters.contains(*node) && !learners.contains(node))
        .collect();
    if !missing.is_empty() {
        return MembershipStep::AddLearners {
            expected,
            nodes: missing,
        };
    }
    if leader.view == ViewFreshness::Unconfirmed {
        return MembershipStep::ConfirmView;
    }
    let add: BTreeSet<NodeId> = target
        .voters
        .iter()
        .filter(|node| !voters.contains(*node))
        .collect();
    let dropped: BTreeSet<NodeId> = voters
        .iter()
        .filter(|node| !target.voters.contains(*node))
        .collect();
    // `y`: the learner this step promotes, the smallest ready one.
    let y = add.iter().copied().find(|node| {
        !leader.unlisted.contains(node)
            && leader.caught_up(*node)
            && leader.gates.get(node) == Some(&PeerGate::Open)
    });
    // The RF 1 step replaces the only voter, the leader itself, by `y`.
    if voters.len() == 1 && target.voters.len() == 1 {
        return match y {
            Some(y) => MembershipStep::ChangeVoters {
                expected,
                voters: VoterSet::single(y),
            },
            None => MembershipStep::Wait(not_ready(&add, leader)),
        };
    }
    // `x`: the voter this step drops, never the leader: the smallest dead
    // one, else the smallest.
    let others: Vec<NodeId> = dropped.iter().copied().filter(|node| *node != me).collect();
    let x = others
        .iter()
        .copied()
        .find(|node| !live.contains(node))
        .or_else(|| others.first().copied());
    let (needs_x, needs_y) = match voters.len().cmp(&target.voters.len()) {
        std::cmp::Ordering::Greater => (true, false),
        std::cmp::Ordering::Less => (false, true),
        std::cmp::Ordering::Equal => (true, true),
    };
    // A missing `x` means the leader is the only voter to drop: it hands
    // off first, before waiting for any learner.
    if needs_x && x.is_none() {
        let staying = voters.iter().filter(|node| target.voters.contains(*node));
        return match leader.handoff_target(me, staying) {
            Some(to) => MembershipStep::TransferLeadership { to },
            None => MembershipStep::Wait(WaitReason::NoHandoffTarget),
        };
    }
    if needs_y && y.is_none() {
        return MembershipStep::Wait(not_ready(&add, leader));
    }
    let mut next: BTreeSet<NodeId> = voters.to_set();
    if let (true, Some(x)) = (needs_x, x) {
        next.remove(&x);
    }
    if let (true, Some(y)) = (needs_y, y) {
        next.insert(y);
    }
    MembershipStep::ChangeVoters {
        expected,
        voters: VoterSet::from_first(me, next),
    }
}

/// Why no target in `add` can be promoted, by a fixed precedence: an
/// unlisted address, then a closed gate, then a gate that did not answer,
/// then a learner still catching up.
fn not_ready(add: &BTreeSet<NodeId>, leader: &LeaderView) -> WaitReason {
    let unlisted: BTreeSet<NodeId> = add.intersection(&leader.unlisted).copied().collect();
    if !unlisted.is_empty() {
        return WaitReason::PeerNotListed { nodes: unlisted };
    }
    let caught_up: Vec<NodeId> = add
        .iter()
        .copied()
        .filter(|node| leader.caught_up(*node))
        .collect();
    let closed: BTreeSet<NodeId> = caught_up
        .iter()
        .copied()
        .filter(|node| leader.gates.get(node) == Some(&PeerGate::Closed))
        .collect();
    if !closed.is_empty() {
        return WaitReason::GateClosed { nodes: closed };
    }
    let unknown: BTreeSet<NodeId> = caught_up
        .iter()
        .copied()
        .filter(|node| leader.gates.get(node) != Some(&PeerGate::Open))
        .collect();
    if !unknown.is_empty() {
        return WaitReason::GateUnknown { nodes: unknown };
    }
    WaitReason::CatchingUp {
        committed: leader.committed_index,
        behind: add
            .iter()
            .filter(|node| !leader.caught_up(**node))
            .map(|node| LearnerLag {
                node: *node,
                matched: leader.matched.get(node).copied().flatten(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod model;
#[cfg(test)]
mod proptests;
#[cfg(test)]
mod tests;
