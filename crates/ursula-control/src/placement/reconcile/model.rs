//! An abstract Raft group for the reconciler's tests: one membership log
//! with OpenRaft's change semantics as section 5.10 of the design lists
//! them, a leader, live nodes, catch-up and gates.
//!
//! - A voter change from a uniform config appends a joint entry and, from
//!   the same call, the uniform one (unless the test drops the second
//!   step). `retain = false` drops old voters from the membership.
//! - An entry takes effect when appended. It commits when appended, or
//!   later in a fair round or on an election, once its leader is live and
//!   every config of it has a live majority. Until then it stays pending.
//! - A leader that is no voter of the committed uniform config steps down,
//!   and an election picks the smallest live voter of the effective
//!   membership when every config of it has a live majority, committing a
//!   pending entry it can. A live leader keeps leading after its quorum
//!   dies, as OpenRaft's does.
//! - Every append leaves the other members behind until the environment
//!   catches them up. A new learner starts unmatched, and so does a node
//!   that restarts, with its gate closed.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use ursula_proto::placement::MembershipLogId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::RaftLogId;
use ursula_proto::placement::VoterSet;

use super::EffectiveMembership;
use super::LeaderView;
use super::Leadership;
use super::MembershipConfig;
use super::MembershipStep;
use super::MembershipTarget;
use super::MembershipView;
use super::PeerGate;
use super::ViewFreshness;
use super::plan_membership_step;

pub(super) fn node(id: u64) -> NodeId {
    NodeId::new(id)
}

pub(super) fn voters(ids: &[u64]) -> VoterSet {
    VoterSet::new(ids.iter().copied().map(NodeId::new).collect()).unwrap()
}

pub(super) fn ids(values: &[u64]) -> BTreeSet<NodeId> {
    values.iter().copied().map(NodeId::new).collect()
}

/// Whether every config of `config` has a live majority among `alive`.
pub(super) fn quorum_live(config: &MembershipConfig, alive: &BTreeSet<NodeId>) -> bool {
    config.voter_configs().all(|side| {
        let live = side.iter().filter(|node| alive.contains(node)).count();
        live.checked_mul(2).unwrap() > side.len()
    })
}

/// `config` with `learners` as its learners.
fn with_learners(config: &MembershipConfig, learners: BTreeSet<NodeId>) -> MembershipConfig {
    match config {
        MembershipConfig::Uniform { voters, .. } => MembershipConfig::Uniform {
            voters: voters.clone(),
            learners,
        },
        MembershipConfig::Joint { old, new, .. } => MembershipConfig::Joint {
            old: old.clone(),
            new: new.clone(),
            learners,
        },
    }
}

#[derive(Debug, Clone)]
pub(super) struct Group {
    pub(super) alive: BTreeSet<NodeId>,
    pub(super) committed: MembershipConfig,
    pub(super) committed_id: MembershipLogId,
    /// An appended entry that has not committed.
    pub(super) pending: Option<(MembershipConfig, MembershipLogId)>,
    pub(super) index: u64,
    pub(super) term: u64,
    pub(super) leader: Option<NodeId>,
    /// Members matched up to the last index.
    pub(super) caught_up: BTreeSet<NodeId>,
    pub(super) gates_open: BTreeSet<NodeId>,
    pub(super) unlisted: BTreeSet<NodeId>,
    /// Whether the leader's placement view is confirmed in its leadership.
    pub(super) confirmed: bool,
    /// The next voter change loses its second step to a leadership change.
    pub(super) drop_second_step: bool,
    /// Every config that committed, the first one included.
    pub(super) committed_configs: Vec<MembershipConfig>,
    /// Every voter change the planner proposed, with its proposer.
    pub(super) proposals: Vec<(NodeId, VoterSet)>,
    /// How many pending entries a new leader committed on its election.
    pub(super) election_commits: usize,
}

/// One planner call of a round: the view it planned from and its step.
#[derive(Debug, Clone)]
pub(super) struct Planned {
    pub(super) view: MembershipView,
    pub(super) step: MembershipStep,
}

impl Group {
    /// A group whose committed uniform config is `voters`, led by `leader`,
    /// with every node of `alive` live, caught up and gated open.
    pub(super) fn new(voters: &VoterSet, leader: NodeId, alive: BTreeSet<NodeId>) -> Self {
        let committed = MembershipConfig::Uniform {
            voters: voters.clone(),
            learners: BTreeSet::new(),
        };
        let committed_id = MembershipLogId::new(RaftLogId {
            term: 1,
            leader,
            index: 1,
        });
        Self {
            caught_up: alive.clone(),
            gates_open: alive.clone(),
            alive,
            committed_configs: vec![committed.clone()],
            committed,
            committed_id,
            pending: None,
            index: 1,
            term: 1,
            leader: Some(leader),
            unlisted: BTreeSet::new(),
            confirmed: false,
            drop_second_step: false,
            proposals: Vec::new(),
            election_commits: 0,
        }
    }

    pub(super) fn effective(&self) -> &MembershipConfig {
        self.pending
            .as_ref()
            .map_or(&self.committed, |(config, _)| config)
    }

    fn leader_live(&self) -> bool {
        self.leader
            .is_some_and(|leader| self.alive.contains(&leader))
    }

    fn append(&mut self, config: MembershipConfig) {
        let Some(leader) = self.leader else {
            return;
        };
        self.index = self.index.checked_add(1).unwrap();
        let id = MembershipLogId::new(RaftLogId {
            term: self.term,
            leader,
            index: self.index,
        });
        self.caught_up = BTreeSet::from([leader]);
        if self.leader_live() && quorum_live(&config, &self.alive) {
            self.commit(config, id);
        } else {
            self.pending = Some((config, id));
        }
    }

    fn commit(&mut self, config: MembershipConfig, id: MembershipLogId) {
        self.committed_configs.push(config.clone());
        self.committed = config;
        self.committed_id = id;
        self.pending = None;
        if let Some(leader) = self.leader
            && matches!(self.committed, MembershipConfig::Uniform { .. })
            && !self.committed.is_voter(leader)
        {
            // OpenRaft's step-down of a leader outside the committed config.
            self.leader = None;
        }
    }

    /// Elects a leader when the current one is gone, and commits a pending
    /// entry the new leader can.
    pub(super) fn elect(&mut self) {
        if self.leader_live()
            && self
                .leader
                .is_some_and(|leader| self.effective().is_voter(leader))
        {
            return;
        }
        self.leader = None;
        if !quorum_live(self.effective(), &self.alive) {
            return;
        }
        let Some(leader) = self
            .effective()
            .voters()
            .into_iter()
            .find(|node| self.alive.contains(node))
        else {
            return;
        };
        self.elect_as(leader);
    }

    /// `leader` wins an election.
    pub(super) fn elect_as(&mut self, leader: NodeId) {
        self.leader = Some(leader);
        self.term = self.term.checked_add(1).unwrap();
        self.confirmed = false;
        if let Some((config, id)) = self.pending.clone()
            && quorum_live(&config, &self.alive)
        {
            self.election_commits = self.election_commits.checked_add(1).unwrap();
            self.commit(config, id);
        }
    }

    /// `node` crashes. A leader stays leader until an election replaces it.
    pub(super) fn kill(&mut self, node: NodeId) {
        self.alive.remove(&node);
    }

    /// `node` restarts unmatched, with its gate closed.
    pub(super) fn revive(&mut self, node: NodeId) {
        self.alive.insert(node);
        self.caught_up.remove(&node);
        self.gates_open.remove(&node);
    }

    /// The fair environment: an election when needed, a pending entry
    /// committed once its leader has a live majority of every config of it,
    /// and every live member caught up, gated open and listed.
    pub(super) fn settle(&mut self) {
        self.elect();
        if self.leader_live()
            && let Some((config, id)) = self.pending.clone()
            && quorum_live(&config, &self.alive)
        {
            self.commit(config, id);
            self.elect();
        }
        let members = self.effective().members();
        self.caught_up = members
            .iter()
            .copied()
            .filter(|node| self.alive.contains(node))
            .collect();
        self.gates_open.extend(self.caught_up.iter().copied());
        self.unlisted.clear();
    }

    /// What `me` plans from.
    pub(super) fn view(&self, me: NodeId) -> MembershipView {
        if self.leader != Some(me) || !self.alive.contains(&me) {
            return MembershipView {
                me,
                leadership: Leadership::NotLeading,
            };
        }
        let membership = match &self.pending {
            Some((_, log_id)) => EffectiveMembership::Uncommitted { log_id: *log_id },
            None => EffectiveMembership::Committed {
                log_id: self.committed_id,
                config: self.committed.clone(),
            },
        };
        let effective = self.effective();
        let members = effective.members();
        let matched: BTreeMap<NodeId, Option<u64>> = members
            .iter()
            .filter(|node| **node != me)
            .map(|node| {
                let matched = self.caught_up.contains(node).then_some(self.index);
                (*node, matched)
            })
            .collect();
        let live = members
            .iter()
            .copied()
            .filter(|node| self.alive.contains(node))
            .collect();
        let handoff_ready = effective
            .voters()
            .into_iter()
            .filter(|node| {
                *node != me
                    && self.alive.contains(node)
                    && self.caught_up.contains(node)
                    && self.gates_open.contains(node)
            })
            .collect();
        let gates = effective
            .learners()
            .iter()
            .filter(|node| self.caught_up.contains(node))
            .map(|node| {
                let gate = if self.gates_open.contains(node) {
                    PeerGate::Open
                } else {
                    PeerGate::Closed
                };
                (*node, gate)
            })
            .collect();
        let view = if self.confirmed {
            ViewFreshness::Confirmed
        } else {
            ViewFreshness::Unconfirmed
        };
        MembershipView {
            me,
            leadership: Leadership::Leading(Box::new(LeaderView {
                membership,
                committed_index: self.index,
                matched,
                live,
                handoff_ready,
                gates,
                unlisted: self.unlisted.clone(),
                view,
            })),
        }
    }

    /// Executes `step` as the leader would, each membership change under
    /// its CAS.
    pub(super) fn execute(&mut self, step: &MembershipStep) {
        let Some(leader) = self.leader else {
            return;
        };
        let cas = |expected: &MembershipLogId, group: &Self| {
            group.pending.is_none() && *expected == group.committed_id
        };
        match step {
            MembershipStep::AddLearners { expected, nodes } => {
                if cas(expected, self) {
                    let mut learners = self.committed.learners().clone();
                    learners.extend(nodes.iter().copied());
                    let next = with_learners(&self.committed, learners);
                    self.append(next);
                }
            }
            MembershipStep::RemoveLearners { expected, nodes } => {
                if cas(expected, self) {
                    let learners = self
                        .committed
                        .learners()
                        .difference(nodes)
                        .copied()
                        .collect();
                    let next = with_learners(&self.committed, learners);
                    self.append(next);
                }
            }
            MembershipStep::ChangeVoters { expected, voters } => {
                if cas(expected, self) {
                    self.proposals.push((leader, voters.clone()));
                    self.change_voters(voters);
                }
            }
            MembershipStep::TransferLeadership { to } => {
                if self.alive.contains(to)
                    && self.effective().is_voter(*to)
                    && self.caught_up.contains(to)
                {
                    self.leader = Some(*to);
                    self.term = self.term.checked_add(1).unwrap();
                    self.confirmed = false;
                }
            }
            MembershipStep::ConfirmView => self.confirmed = true,
            MembershipStep::Converged { .. } | MembershipStep::Wait(_) => {}
        }
    }

    /// `change_membership(ReplaceAllVoters, retain = false)`.
    fn change_voters(&mut self, target: &VoterSet) {
        let learners_without = |learners: &BTreeSet<NodeId>| -> BTreeSet<NodeId> {
            learners
                .iter()
                .copied()
                .filter(|node| !target.contains(*node))
                .collect()
        };
        match self.committed.clone() {
            MembershipConfig::Uniform { voters, learners } => {
                if &voters == target {
                    return;
                }
                let learners = learners_without(&learners);
                self.append(MembershipConfig::Joint {
                    old: voters,
                    new: target.clone(),
                    learners: learners.clone(),
                });
                if self.pending.is_some() {
                    return;
                }
                if self.drop_second_step {
                    self.drop_second_step = false;
                    self.leader = None;
                    return;
                }
                self.append(MembershipConfig::Uniform {
                    voters: target.clone(),
                    learners,
                });
            }
            MembershipConfig::Joint { old, new, learners } => {
                if &old == target || &new == target {
                    self.append(MembershipConfig::Uniform {
                        voters: target.clone(),
                        learners: learners_without(&learners),
                    });
                }
            }
        }
    }
}

/// The outcome of [`run`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Ended {
    /// The leader answered `Converged` with the target's voters.
    Converged,
    /// No convergence within the round limit.
    Stuck,
}

/// The environment's half of a round: [`Group::settle`] when `fair`, else
/// only an election.
pub(super) fn environment(group: &mut Group, fair: bool) {
    if fair {
        group.settle();
    } else {
        group.elect();
    }
}

/// The leader's half of a round: it plans and executes. `None` without a
/// leader. A leader that crashed plans from a `NotLeading` view.
pub(super) fn act(group: &mut Group, target: &MembershipTarget) -> Option<Planned> {
    let leader = group.leader?;
    let view = group.view(leader);
    let step = plan_membership_step(&view, target);
    group.execute(&step);
    Some(Planned { view, step })
}

/// One round: the environment acts, then the leader plans and executes.
pub(super) fn round(group: &mut Group, target: &MembershipTarget, fair: bool) -> Option<Planned> {
    environment(group, fair);
    act(group, target)
}

/// Runs fair rounds until the leader reports the target's voters, at most
/// `limit` rounds, calling `check` after every round.
pub(super) fn run(
    group: &mut Group,
    target: &MembershipTarget,
    limit: usize,
    mut check: impl FnMut(&Group, Option<&Planned>) -> Result<(), String>,
) -> Result<Ended, String> {
    for _ in 0..limit {
        let planned = round(group, target, true);
        check(group, planned.as_ref())?;
        if let Some(Planned {
            step: MembershipStep::Converged { voters },
            ..
        }) = &planned
            && voters == &target.voters
            && group.committed.learners().is_subset(&target.learners)
        {
            return Ok(Ended::Converged);
        }
    }
    Ok(Ended::Stuck)
}
