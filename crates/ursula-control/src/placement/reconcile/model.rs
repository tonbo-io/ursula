//! An abstract Raft group for the reconciler's tests, following OpenRaft
//! 0.10.0-alpha.37 as section 5.10 of the design lists it.
//!
//! - Every node keeps its own log, whose entries are membership configs or
//!   leaders' no-ops. A leader's append reaches at once every live member of
//!   its effective membership it can reach, and its matched metrics lag
//!   until the environment's round catches them up.
//! - The leader's log commits once its last entry, of its own term, is held
//!   by a majority of every config of its effective membership that
//!   acknowledges: live, reachable voters whose gate is not `Empty`. A new
//!   leader commits what it holds through its no-op, so a pending entry the
//!   new leader does not hold is truncated, and the membership reverts.
//! - A node campaigns only with an open gate and as a voter of its effective
//!   or the committed membership. It wins with the votes of a majority of
//!   every config of its effective membership: live, reachable voters with
//!   an open gate (or a plan naming it) whose log is not newer than its own.
//!   So an outgoing voter holding the uncommitted uniform entry can win, and
//!   its no-op commits that entry.
//! - A leader outside its committed effective membership keeps leading
//!   (`calc_server_state`, `raft_state/mod.rs:416-421`), so it plans from a
//!   committed config without itself. Only OpenRaft's step-down watcher
//!   hands off (`removed_leader_step_down`, default `After(150)`, which
//!   Ursula keeps): in the environment's next fair round it transfers to the
//!   voter with the greatest match, and reverts to a learner when the
//!   transfer takes no effect (`core/step_down_watcher.rs:171-199`).
//! - A voter change from a uniform config appends the joint entry, and its
//!   kept future appends the uniform one once the joint entry committed in
//!   the same leadership, unless the test drops that second step or delays
//!   it to the environment's next round. Design 5.5 passes the planning
//!   leadership's `CommittedLeaderId` with `LastMembershipLogId(expected)`,
//!   and OpenRaft carries it into the second step
//!   (`raft/api/management.rs:155-170`), so no other leadership runs it, the
//!   proposer's later ones included. OpenRaft drains the future only when its entry applies or is
//!   truncated on the proposer (`core/raft_core.rs:1113,2475,2497`), not on
//!   a step-down, but a deposed proposer plans from `NotLeading`, and a
//!   re-elected one commits the joint entry with its no-op, which ends the
//!   future with `PreconditionFailed`. So the model drops the future with
//!   the leadership.
//! - From a joint config onto one of its sides a voter change appends one
//!   uniform entry, and onto a third set OpenRaft bridges `[A, B]` to
//!   `[B, C]` first. `retain = false` drops old voters. A new voter that is
//!   no member fails with `LearnerNotFound`, and the run with it, since no
//!   correct planner asks for one.
//! - A step whose entry has not committed keeps its future pending
//!   (`StepPending`). A transfer waits for the environment (`Transferring`)
//!   and completes once its target holds the leader's log with an open gate
//!   and wins, else it is abandoned.
//! - A crash keeps the log. A restarted node comes back `Recovering` (it
//!   acknowledges, but neither votes nor campaigns), a new member `Empty`
//!   (it acknowledges nothing), and a barrier of a leader with a quorum
//!   opens either. A restarted leader comes back a follower. A group no
//!   candidate can lead takes the operator's recovery plan in a fair round:
//!   the freshest voter's gate opens and every other member votes only for
//!   it.
//! - A leader cut off from every other node keeps leading until the others
//!   elect one, and a live leader keeps leading after its quorum died, as
//!   OpenRaft's do. A cut keeps the node's gate: only a restart or a lost
//!   log closes it. The reconciler's adversary strands a node until the
//!   next fair round, and the closed loop partitions one until it
//!   reconnects it.
//! - An operator's data recovery plan keeps a committed membership whose
//!   entry a live, reachable voter of it holds: that voter, the one with
//!   the newest log, leads it, its log cut after the entry, and every other
//!   reachable member votes only for it. The tool refuses a plan no such
//!   voter can take, or whose quorum would not commit it.

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

pub(crate) fn node(id: u64) -> NodeId {
    NodeId::new(id)
}

pub(crate) fn voters(ids: &[u64]) -> VoterSet {
    VoterSet::new(ids.iter().copied().map(NodeId::new).collect()).unwrap()
}

pub(crate) fn ids(values: &[u64]) -> BTreeSet<NodeId> {
    values.iter().copied().map(NodeId::new).collect()
}

/// Whether more than half of `side` is `counted`.
fn majority(side: &VoterSet, counted: impl Fn(NodeId) -> bool) -> bool {
    let count = side.iter().filter(|node| counted(*node)).count();
    count.checked_mul(2).unwrap() > side.len()
}

/// Whether every config of `config` has a live majority among `alive`.
pub(crate) fn quorum_live(config: &MembershipConfig, alive: &BTreeSet<NodeId>) -> bool {
    config
        .voter_configs()
        .all(|side| majority(side, |node| alive.contains(&node)))
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

/// The planner a group's leaders plan with: the real one, or a broken one a
/// test swaps in to show that the run catches it.
pub(crate) type Planner = fn(&MembershipView, &MembershipTarget) -> MembershipStep;

/// One log entry: a membership config, or a leader's no-op.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Entry {
    pub(crate) id: RaftLogId,
    pub(crate) config: Option<MembershipConfig>,
}

/// A node's recovery gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Gate {
    Open,
    /// Restarted after a crash: acknowledges appends, and neither votes nor
    /// campaigns.
    Recovering,
    /// Never held the group: acknowledges nothing, votes for no one and
    /// does not campaign.
    Empty,
    /// Took an operator's recovery plan: votes only for its `leader`.
    Planned {
        leader: NodeId,
    },
}

impl Gate {
    fn acknowledges(self) -> bool {
        !matches!(self, Self::Empty)
    }

    fn votes_for(self, candidate: NodeId) -> bool {
        match self {
            Self::Open => true,
            Self::Planned { leader } => leader == candidate,
            Self::Recovering | Self::Empty => false,
        }
    }
}

/// The future of a membership step, kept until OpenRaft answers.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StepFuture {
    leader: NodeId,
    term: u64,
    /// The entry it waits on.
    awaiting: RaftLogId,
    /// The uniform entry it appends once the joint one committed.
    second: Option<MembershipConfig>,
    /// The second step waits for the environment's next round.
    delayed: bool,
}

/// A step OpenRaft refuses that no correct planner takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Refused {
    /// A new voter that is no member of the effective membership.
    LearnerNotFound { node: NodeId },
}

/// A voter change OpenRaft accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Proposal {
    pub(crate) proposer: NodeId,
    /// The effective membership it changed.
    pub(crate) from: MembershipConfig,
    pub(crate) voters: VoterSet,
}

impl Proposal {
    /// Whether the proposer keeps itself as a voter, or is the RF 1 step:
    /// the only voter of a uniform config replaced by one other.
    pub(crate) fn keeps_proposer(&self) -> bool {
        let rf1_step = matches!(
            &self.from,
            MembershipConfig::Uniform { voters, .. }
                if voters.len() == 1 && voters.contains(self.proposer)
        ) && self.voters.len() == 1;
        self.voters.contains(self.proposer) || rf1_step
    }
}

/// The elections and plans a run reached.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Elections {
    /// The winner held an uncommitted membership entry, which its no-op
    /// commits once a quorum holds it.
    pub(crate) pending_kept: usize,
    /// Another node held an uncommitted membership entry the winner lacks,
    /// which its leadership truncates.
    pub(crate) pending_truncated: usize,
    /// The winner is no voter of its effective membership.
    pub(crate) outgoing: usize,
    /// A recovery plan elected a leader.
    pub(crate) plans: usize,
    /// Leaders a voter returning from a long cut deposed with its higher
    /// vote.
    pub(crate) deposed: usize,
}

/// What heals a cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Cut {
    /// The environment's next fair round: the reconciler's adversary.
    UntilRound,
    /// Only [`Group::reconnect`]: the closed loop's data-plane partition.
    UntilReconnected,
}

/// How often a run lost or delayed a voter change's second step.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SecondSteps {
    pub(crate) dropped: usize,
    pub(crate) delayed: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct Group {
    pub(crate) alive: BTreeSet<NodeId>,
    /// Nodes cut off from every other node, each with what heals its cut.
    /// A cut node keeps its gate.
    pub(crate) cut: BTreeMap<NodeId, Cut>,
    pub(crate) logs: BTreeMap<NodeId, Vec<Entry>>,
    pub(crate) gates: BTreeMap<NodeId, Gate>,
    pub(crate) committed: MembershipConfig,
    pub(crate) committed_id: MembershipLogId,
    /// The index of the last committed entry.
    pub(crate) commit_index: u64,
    pub(crate) term: u64,
    pub(crate) leader: Option<NodeId>,
    /// Members the leader's metrics show matched up to its last entry.
    pub(crate) caught_up: BTreeSet<NodeId>,
    pub(crate) unlisted: BTreeSet<NodeId>,
    /// Whether the leader's placement view is confirmed in its leadership.
    pub(crate) confirmed: bool,
    future: Option<StepFuture>,
    /// The target of a transfer the leader started.
    pub(crate) transferring: Option<NodeId>,
    /// The next second step is lost to a leadership change.
    pub(crate) drop_second_step: bool,
    /// The next second step waits for the environment's next round.
    pub(crate) delay_second_step: bool,
    /// Every config that committed, the first one included.
    pub(crate) committed_configs: Vec<(MembershipLogId, MembershipConfig)>,
    /// Every voter change OpenRaft accepted.
    pub(crate) proposals: Vec<Proposal>,
    pub(crate) elections: Elections,
    pub(crate) second_steps: SecondSteps,
    pub(crate) planner: Planner,
}

/// One planner call of a round: the view it planned from and its step.
#[derive(Debug, Clone)]
pub(crate) struct Planned {
    pub(crate) view: MembershipView,
    pub(crate) step: MembershipStep,
}

impl Group {
    /// A group whose committed uniform config is `voters`, led by `leader`,
    /// with every node of `alive` live. The voters hold the config with
    /// open gates and are caught up. Every other node is `Empty`.
    pub(crate) fn new(voters: &VoterSet, leader: NodeId, alive: BTreeSet<NodeId>) -> Self {
        let committed = MembershipConfig::Uniform {
            voters: voters.clone(),
            learners: BTreeSet::new(),
        };
        let id = RaftLogId {
            term: 1,
            leader,
            index: 1,
        };
        let entry = Entry {
            id,
            config: Some(committed.clone()),
        };
        let logs = voters
            .iter()
            .map(|voter| (voter, vec![entry.clone()]))
            .collect();
        let gates = alive
            .iter()
            .copied()
            .chain(voters.iter())
            .map(|node| {
                let gate = if voters.contains(node) {
                    Gate::Open
                } else {
                    Gate::Empty
                };
                (node, gate)
            })
            .collect();
        Self {
            caught_up: voters.to_set(),
            alive,
            cut: BTreeMap::new(),
            logs,
            gates,
            committed_configs: vec![(MembershipLogId::new(id), committed.clone())],
            committed,
            committed_id: MembershipLogId::new(id),
            commit_index: 1,
            term: 1,
            leader: Some(leader),
            unlisted: BTreeSet::new(),
            confirmed: false,
            future: None,
            transferring: None,
            drop_second_step: false,
            delay_second_step: false,
            proposals: Vec::new(),
            elections: Elections::default(),
            second_steps: SecondSteps::default(),
            planner: plan_membership_step,
        }
    }

    pub(crate) fn log(&self, node: NodeId) -> &[Entry] {
        self.logs.get(&node).map(Vec::as_slice).unwrap_or_default()
    }

    fn last_id(&self, node: NodeId) -> Option<RaftLogId> {
        self.log(node).last().map(|entry| entry.id)
    }

    pub(crate) fn gate(&self, node: NodeId) -> Gate {
        self.gates.get(&node).copied().unwrap_or(Gate::Empty)
    }

    fn holds(&self, node: NodeId, id: RaftLogId) -> bool {
        self.log(node).iter().any(|entry| entry.id == id)
    }

    /// The last membership entry of `node`'s log: its effective membership.
    fn membership_of(&self, node: NodeId) -> Option<(&MembershipConfig, RaftLogId)> {
        self.log(node)
            .iter()
            .rev()
            .find_map(|entry| entry.config.as_ref().map(|config| (config, entry.id)))
    }

    /// The node whose log the group's effective membership is read from: a
    /// live leader, else the live node with the newest log, else any.
    fn reference(&self) -> Option<NodeId> {
        if let Some(leader) = self.leader
            && self.alive.contains(&leader)
        {
            return Some(leader);
        }
        let newest = |nodes: &mut dyn Iterator<Item = NodeId>| -> Option<NodeId> {
            let mut best: Option<(RaftLogId, NodeId)> = None;
            for node in nodes {
                let Some(last) = self.last_id(node) else {
                    continue;
                };
                if best.is_none_or(|(best_last, _)| last > best_last) {
                    best = Some((last, node));
                }
            }
            best.map(|(_, node)| node)
        };
        newest(&mut self.alive.iter().copied()).or_else(|| newest(&mut self.logs.keys().copied()))
    }

    pub(crate) fn effective(&self) -> &MembershipConfig {
        self.reference()
            .and_then(|node| self.membership_of(node))
            .map_or(&self.committed, |(config, _)| config)
    }

    /// The effective membership when it has not committed.
    pub(crate) fn pending(&self) -> Option<(&MembershipConfig, MembershipLogId)> {
        let (config, id) = self.reference().and_then(|node| self.membership_of(node))?;
        (id.index > self.commit_index).then_some((config, MembershipLogId::new(id)))
    }

    fn reachable(&self, from: NodeId, to: NodeId) -> bool {
        from == to || (!self.cut.contains_key(&from) && !self.cut.contains_key(&to))
    }

    /// Whether `to` is live and `from` reaches it.
    fn up(&self, from: NodeId, to: NodeId) -> bool {
        self.alive.contains(&to) && self.reachable(from, to)
    }

    /// The live nodes `from` reaches, itself included when live.
    pub(crate) fn live_from(&self, from: NodeId) -> BTreeSet<NodeId> {
        self.alive
            .iter()
            .copied()
            .filter(|node| self.reachable(from, *node))
            .collect()
    }

    fn lose_leadership(&mut self) {
        self.leader = None;
        self.future = None;
        self.transferring = None;
        self.confirmed = false;
        self.caught_up.clear();
    }

    /// The live leader's log reaches the members of its effective
    /// membership it can reach, truncating what they hold beyond it.
    fn replicate(&mut self) {
        let Some(leader) = self.leader else {
            return;
        };
        if !self.alive.contains(&leader) {
            return;
        }
        let Some(members) = self
            .membership_of(leader)
            .map(|(config, _)| config.members())
        else {
            return;
        };
        let log = self.log(leader).to_vec();
        for member in members {
            if member != leader && self.up(leader, member) {
                self.logs.insert(member, log.clone());
            }
        }
    }

    /// The leader appends `config`, or a no-op for `None`, and its members
    /// receive it. Its metrics then show only itself matched.
    fn append(&mut self, config: Option<MembershipConfig>) -> Option<RaftLogId> {
        let leader = self.leader?;
        let log = self.logs.entry(leader).or_default();
        let index = u64::try_from(log.len()).ok()?.checked_add(1)?;
        let id = RaftLogId {
            term: self.term,
            leader,
            index,
        };
        log.push(Entry { id, config });
        self.caught_up = BTreeSet::from([leader]);
        self.replicate();
        Some(id)
    }

    /// Whether `node` acknowledges `leader`'s last entry.
    fn acknowledges(&self, leader: NodeId, node: NodeId) -> bool {
        node == leader
            || (self.up(leader, node)
                && self.gate(node).acknowledges()
                && self.log(node) == self.log(leader))
    }

    /// Whether a majority of every config of `leader`'s effective membership
    /// acknowledges its last entry.
    fn quorum_acknowledges(&self, leader: NodeId) -> bool {
        if !self.alive.contains(&leader) {
            return false;
        }
        let Some((config, _)) = self.membership_of(leader) else {
            return false;
        };
        config
            .voter_configs()
            .all(|side| majority(side, |node| self.acknowledges(leader, node)))
    }

    /// Commits the leader's log once a quorum acknowledges its last entry,
    /// which is of its own term: every leadership starts with a no-op.
    fn try_commit(&mut self) {
        let Some(leader) = self.leader else {
            return;
        };
        if !self.quorum_acknowledges(leader) {
            return;
        }
        let index = u64::try_from(self.log(leader).len()).unwrap();
        self.commit_index = self.commit_index.max(index);
        if let Some((config, id)) = self.membership_of(leader)
            && MembershipLogId::new(id) != self.committed_id
        {
            let config = config.clone();
            self.committed_configs
                .push((MembershipLogId::new(id), config.clone()));
            self.committed = config;
            self.committed_id = MembershipLogId::new(id);
        }
    }

    /// OpenRaft's step-down watcher: a live leader outside its committed
    /// effective membership transfers leadership to the voter with the
    /// greatest match (`voter_ids().max_by_key(matching)`, the last one on
    /// a tie), then reverts to a learner if the transfer took no effect.
    fn step_down_removed_leader(&mut self) {
        let Some(leader) = self.leader.filter(|leader| self.alive.contains(leader)) else {
            return;
        };
        let Some((config, id)) = self.membership_of(leader) else {
            return;
        };
        if id.index > self.commit_index || config.is_member(leader) {
            return;
        }
        let to = config
            .voters()
            .into_iter()
            .max_by_key(|voter| self.caught_up.contains(voter));
        if let Some(to) = to {
            self.transferring = Some(to);
            self.resolve_transfer();
        }
        if self.leader == Some(leader) {
            self.lose_leadership();
        }
    }

    /// Advances the kept future once its entry committed: it is done, or it
    /// appends its second step under the joint entry's
    /// `LastMembershipLogId` and the planning leadership's
    /// `CommittedLeaderId`, so only in that leadership. A delayed second
    /// step runs only `in_round`, the environment's half.
    fn advance_future(&mut self, in_round: bool) {
        let Some(future) = self.future.clone() else {
            return;
        };
        if self.leader != Some(future.leader) || self.term != future.term {
            self.future = None;
            return;
        }
        if future.awaiting.index > self.commit_index {
            return;
        }
        let Some(second) = future.second.clone() else {
            self.future = None;
            return;
        };
        if self.drop_second_step {
            self.drop_second_step = false;
            self.second_steps.dropped = self.second_steps.dropped.checked_add(1).unwrap();
            self.lose_leadership();
            return;
        }
        if self.delay_second_step && !in_round {
            self.delay_second_step = false;
            self.second_steps.delayed = self.second_steps.delayed.checked_add(1).unwrap();
            self.future = Some(StepFuture {
                delayed: true,
                ..future
            });
            return;
        }
        if future.delayed && !in_round {
            return;
        }
        if self.membership_of(future.leader).map(|(_, id)| id) != Some(future.awaiting) {
            self.future = None;
            return;
        }
        self.future = self.append(Some(second)).map(|awaiting| StepFuture {
            leader: future.leader,
            term: future.term,
            awaiting,
            second: None,
            delayed: false,
        });
    }

    /// Commits what a quorum holds and advances the kept future until
    /// neither changes.
    fn sync(&mut self, in_round: bool) {
        for _ in 0..4 {
            let before = (self.commit_index, self.future.clone(), self.leader);
            self.try_commit();
            self.advance_future(in_round);
            if (self.commit_index, self.future.clone(), self.leader) == before {
                break;
            }
        }
    }

    /// The live leader's heartbeat: it replicates its log and commits.
    fn heartbeat(&mut self) {
        self.replicate();
        self.sync(true);
    }

    /// Whether `candidate` may campaign and would collect its votes.
    pub(crate) fn can_win(&self, candidate: NodeId) -> bool {
        if !self.alive.contains(&candidate) || self.gate(candidate) != Gate::Open {
            return false;
        }
        let Some((config, _)) = self.membership_of(candidate) else {
            return false;
        };
        if !config.is_voter(candidate) && !self.committed.is_voter(candidate) {
            return false;
        }
        let last = self.last_id(candidate);
        config.voter_configs().all(|side| {
            majority(side, |voter| {
                self.up(candidate, voter)
                    && self.gate(voter).votes_for(candidate)
                    && self.last_id(voter) <= last
            })
        })
    }

    /// `candidate` wins: it leads in a new term and appends its no-op.
    fn win(&mut self, candidate: NodeId) {
        let uncommitted: BTreeSet<RaftLogId> = self
            .logs
            .values()
            .flatten()
            .filter(|entry| entry.config.is_some() && entry.id.index > self.commit_index)
            .map(|entry| entry.id)
            .collect();
        if uncommitted.iter().any(|id| self.holds(candidate, *id)) {
            self.elections.pending_kept = self.elections.pending_kept.checked_add(1).unwrap();
        }
        if uncommitted.iter().any(|id| !self.holds(candidate, *id)) {
            self.elections.pending_truncated =
                self.elections.pending_truncated.checked_add(1).unwrap();
        }
        if !self
            .membership_of(candidate)
            .is_some_and(|(config, _)| config.is_voter(candidate))
        {
            self.elections.outgoing = self.elections.outgoing.checked_add(1).unwrap();
        }
        self.lose_leadership();
        self.leader = Some(candidate);
        self.term = self.term.checked_add(1).unwrap();
        self.append(None);
        self.sync(true);
    }

    /// `candidate` campaigns, and leads when it wins.
    pub(crate) fn campaign(&mut self, candidate: NodeId) -> bool {
        let won = self.can_win(candidate);
        if won {
            self.win(candidate);
        }
        won
    }

    /// An election when the leader is gone or cut off: the smallest node
    /// that wins leads. A cut-off leader no one replaces keeps leading.
    pub(crate) fn elect(&mut self) {
        let current = self.leader.filter(|leader| self.alive.contains(leader));
        if current.is_some_and(|leader| !self.cut.contains_key(&leader)) {
            return;
        }
        let winner = self
            .alive
            .iter()
            .copied()
            .find(|node| Some(*node) != current && self.can_win(*node));
        match winner {
            Some(winner) => self.win(winner),
            None if current.is_none() => self.lose_leadership(),
            None => {}
        }
    }

    /// The operator's recovery plan for a group without a leader: the live,
    /// uncut node with the newest log that may campaign leads, the smallest
    /// on a tie, its gate opens and every other such node holding the group
    /// votes only for it.
    fn accept_recovery_plan(&mut self) {
        let reachable: Vec<NodeId> = self
            .alive
            .iter()
            .copied()
            .filter(|node| !self.cut.contains_key(node))
            .collect();
        let mut best: Option<(RaftLogId, NodeId)> = None;
        for node in reachable.iter().copied() {
            let Some((config, last)) = self
                .membership_of(node)
                .map(|(config, _)| config)
                .zip(self.last_id(node))
            else {
                continue;
            };
            let may_campaign = config.is_voter(node) || self.committed.is_voter(node);
            if may_campaign && best.is_none_or(|(best_last, _)| last > best_last) {
                best = Some((last, node));
            }
        }
        let Some((_, leader)) = best else {
            return;
        };
        let members: Vec<NodeId> = reachable
            .into_iter()
            .filter(|node| !self.log(*node).is_empty())
            .collect();
        for node in members {
            let gate = if node == leader {
                Gate::Open
            } else {
                Gate::Planned { leader }
            };
            self.gates.insert(node, gate);
        }
        if self.campaign(leader) {
            self.elections.plans = self.elections.plans.checked_add(1).unwrap();
        }
    }

    /// A pending transfer completes once its target holds the leader's log
    /// with an open gate and wins, and is abandoned otherwise.
    fn resolve_transfer(&mut self) {
        let Some(to) = self.transferring.take() else {
            return;
        };
        let Some(leader) = self.leader else {
            return;
        };
        let ready = self.alive.contains(&leader)
            && self.up(leader, to)
            && self.gate(to) == Gate::Open
            && self.log(to) == self.log(leader)
            && self
                .membership_of(leader)
                .is_some_and(|(config, _)| config.is_voter(to));
        if ready {
            self.campaign(to);
        }
    }

    /// The barrier of a leader with a quorum opens the gate of every member
    /// holding its log, and its metrics catch up with every member that
    /// acknowledges.
    fn barrier(&mut self) {
        let Some(leader) = self.leader else {
            return;
        };
        if !self.alive.contains(&leader) {
            return;
        }
        let Some(members) = self
            .membership_of(leader)
            .map(|(config, _)| config.members())
        else {
            return;
        };
        let holding: BTreeSet<NodeId> = members
            .into_iter()
            .filter(|member| {
                *member == leader
                    || (self.up(leader, *member) && self.log(*member) == self.log(leader))
            })
            .collect();
        if self.quorum_acknowledges(leader) {
            for member in &holding {
                self.gates.insert(*member, Gate::Open);
            }
        }
        self.caught_up = holding
            .into_iter()
            .filter(|member| self.acknowledges(leader, *member))
            .collect();
    }

    /// `node` destroys its replica: it is down, and comes back `Empty`
    /// without a log.
    pub(crate) fn wipe(&mut self, node: NodeId) {
        self.kill(node);
        self.logs.remove(&node);
        self.gates.insert(node, Gate::Empty);
    }

    /// An operator's data recovery plan keeping the committed membership
    /// `id`, whose config is `config`, as 7.4's tool sends it. The plan's
    /// leader is a live voter of `config` that the tool reaches on the data
    /// plane and whose log holds the entry `id`, the one with the newest log
    /// (the smallest on a tie). Its log is cut after that entry, which
    /// everything up to it committed, and the plan appends `config` again in
    /// a new term. Its gate opens, every other reachable member holding the
    /// group votes only for it, and it campaigns. The tool refuses, and
    /// nothing changes, when no voter holds the entry or the plan's quorum
    /// would not commit it.
    pub(crate) fn recover_to(
        &mut self,
        id: MembershipLogId,
        config: MembershipConfig,
    ) -> Option<MembershipLogId> {
        let entry = id.get();
        let mut best: Option<(RaftLogId, NodeId)> = None;
        for voter in config.voters() {
            let reached = self.alive.contains(&voter) && !self.cut.contains_key(&voter);
            let Some(last) = self
                .last_id(voter)
                .filter(|_| reached && self.holds(voter, entry))
            else {
                continue;
            };
            if best.is_none_or(|(best_last, _)| last > best_last) {
                best = Some((last, voter));
            }
        }
        let (_, leader) = best?;
        let mut planned = self.clone();
        planned.lose_leadership();
        planned.term = planned.term.checked_add(1)?;
        let mut log = self.log(leader).to_vec();
        let kept = log
            .iter()
            .position(|held| held.id == entry)?
            .checked_add(1)?;
        log.truncate(kept);
        let point = u64::try_from(kept).ok()?;
        let plan_id = RaftLogId {
            term: planned.term,
            leader,
            index: point.checked_add(1)?,
        };
        log.push(Entry {
            id: plan_id,
            config: Some(config.clone()),
        });
        planned.logs.insert(leader, log);
        planned.commit_index = point;
        planned.committed = config;
        planned.committed_id = id;
        let takers: Vec<NodeId> = planned
            .alive
            .iter()
            .copied()
            .filter(|node| {
                *node != leader && !planned.cut.contains_key(node) && !planned.log(*node).is_empty()
            })
            .collect();
        planned.gates.insert(leader, Gate::Open);
        for node in takers {
            planned.gates.insert(node, Gate::Planned { leader });
        }
        let plan_id = MembershipLogId::new(plan_id);
        if !planned.campaign(leader) || planned.committed_id != plan_id {
            return None;
        }
        // The plan's own election is no election the counters follow.
        planned.elections = self.elections.clone();
        *self = planned;
        Some(plan_id)
    }

    /// `node` crashes. A leader stays leader until an election replaces it.
    pub(crate) fn kill(&mut self, node: NodeId) {
        self.alive.remove(&node);
    }

    /// `node` restarts with its log, `Recovering`, or `Empty` without one,
    /// and a leader as a follower.
    pub(crate) fn revive(&mut self, node: NodeId) {
        self.alive.insert(node);
        self.caught_up.remove(&node);
        let gate = if self.log(node).is_empty() {
            Gate::Empty
        } else {
            Gate::Recovering
        };
        self.gates.insert(node, gate);
        if self.leader == Some(node) {
            self.lose_leadership();
        }
    }

    /// The leader is cut off from every other node until the next fair
    /// round.
    pub(crate) fn strand(&mut self) {
        if let Some(leader) = self.leader {
            self.cut.insert(leader, Cut::UntilRound);
        }
    }

    /// `node` is cut off from every other node until `cut` heals it. It
    /// keeps its gate, and a leader keeps leading until another wins.
    pub(crate) fn isolate(&mut self, node: NodeId, cut: Cut) {
        self.cut.insert(node, cut);
    }

    /// The fair round heals its cuts.
    pub(crate) fn heal(&mut self) {
        self.cut.retain(|_, cut| *cut == Cut::UntilReconnected);
    }

    /// `node` reaches the others again, with the gate it had.
    pub(crate) fn reconnect(&mut self, node: NodeId) {
        self.cut.remove(&node);
    }

    /// `node` reaches the others again after a cut longer than an election
    /// timeout. Ursula runs OpenRaft with pre-vote off, so a node that may
    /// campaign (a voter of its effective or committed membership, its gate
    /// open) campaigned during the cut and comes back with a higher vote.
    /// A live leader replicating to it refuses that vote on its newer log,
    /// its next append to `node` is rejected by the higher vote, and it
    /// steps down (`Notification::HigherVote`) before anything it appended
    /// reaches `node`. An election follows, which a voter of the committed
    /// joint config holding the uncommitted uniform entry may win.
    pub(crate) fn reconnect_after_timeout(&mut self, node: NodeId) {
        self.reconnect(node);
        let campaigned = self.alive.contains(&node)
            && self.gate(node) == Gate::Open
            && (self
                .membership_of(node)
                .is_some_and(|(config, _)| config.is_voter(node))
                || self.committed.is_voter(node));
        let Some(leader) = self.leader else {
            return;
        };
        let replicates_to = self.alive.contains(&leader)
            && self.reachable(leader, node)
            && self
                .membership_of(leader)
                .is_some_and(|(config, _)| config.is_member(node));
        if !campaigned || leader == node || !replicates_to {
            return;
        }
        self.elections.deposed = self.elections.deposed.checked_add(1).unwrap();
        self.lose_leadership();
        self.elect();
    }

    /// The voters of the live leader's uncommitted membership entry that a
    /// cut keeps from it: a long cut of one of them, healed, deposes the
    /// leader before the entry reaches it.
    pub(crate) fn held_back(&self) -> BTreeSet<NodeId> {
        let Some(leader) = self.leader.filter(|leader| self.alive.contains(leader)) else {
            return BTreeSet::new();
        };
        let Some((config, id)) = self.membership_of(leader) else {
            return BTreeSet::new();
        };
        if id.index <= self.commit_index {
            return BTreeSet::new();
        }
        config
            .voters()
            .into_iter()
            .filter(|voter| self.cut.contains_key(voter) && !self.holds(*voter, id))
            .collect()
    }

    /// The incoming voters of a voter change whose second step waits for
    /// the next fair round, which no cut holds off yet: cutting off a
    /// majority of them holds the uniform entry back once it is appended.
    pub(crate) fn incoming_awaiting_round(&self) -> BTreeSet<NodeId> {
        let Some(future) = self.future.as_ref().filter(|future| future.delayed) else {
            return BTreeSet::new();
        };
        let Some((MembershipConfig::Joint { old, new, .. }, _)) = self.membership_of(future.leader)
        else {
            return BTreeSet::new();
        };
        new.iter()
            .filter(|node| !old.contains(*node) && !self.cut.contains_key(node))
            .collect()
    }

    /// The fair environment: a leader a past round removed steps down, an
    /// election the partition allowed runs, the network heals, the leader
    /// replicates and commits, a pending transfer resolves, an election runs
    /// when needed (a recovery plan when no candidate wins), and the
    /// leader's barrier opens gates and catches its metrics up. Every target
    /// is listed. A leader removed in this round leads through it and steps
    /// down in the next fair one.
    pub(crate) fn settle(&mut self) {
        self.step_down_removed_leader();
        self.elect();
        self.heal();
        self.heartbeat();
        self.resolve_transfer();
        for _ in 0..2 {
            self.elect();
            if self.leader.is_none() {
                self.accept_recovery_plan();
            }
            self.heartbeat();
        }
        self.barrier();
        self.unlisted.clear();
    }

    /// The environment that holds catch-up, gates and listings back: a
    /// delayed second step runs and an election runs when needed, and a
    /// pending transfer stays pending.
    pub(crate) fn hold_back(&mut self) {
        self.sync(true);
        self.elect();
    }

    /// What `me` plans from.
    pub(crate) fn view(&self, me: NodeId) -> MembershipView {
        let not_leading = MembershipView {
            me,
            leadership: Leadership::NotLeading,
        };
        if self.leader != Some(me) || !self.alive.contains(&me) {
            return not_leading;
        }
        if let Some(to) = self.transferring {
            return MembershipView {
                me,
                leadership: Leadership::Transferring { to },
            };
        }
        if self.future.is_some() {
            return MembershipView {
                me,
                leadership: Leadership::StepPending,
            };
        }
        let Some((config, id)) = self.membership_of(me) else {
            return not_leading;
        };
        let log_id = MembershipLogId::new(id);
        let membership = if id.index > self.commit_index {
            EffectiveMembership::Uncommitted { log_id }
        } else {
            EffectiveMembership::Committed {
                log_id,
                config: config.clone(),
            }
        };
        let last_index = u64::try_from(self.log(me).len()).unwrap();
        let members = config.members();
        let matched: BTreeMap<NodeId, Option<u64>> = members
            .iter()
            .filter(|node| **node != me)
            .map(|node| (*node, self.caught_up.contains(node).then_some(last_index)))
            .collect();
        let live = members
            .iter()
            .copied()
            .filter(|node| self.up(me, *node))
            .collect();
        let handoff_ready = config
            .voters()
            .into_iter()
            .filter(|node| {
                *node != me
                    && self.up(me, *node)
                    && self.caught_up.contains(node)
                    && self.gate(*node) == Gate::Open
            })
            .collect();
        let gates = config
            .learners()
            .iter()
            .filter(|node| self.caught_up.contains(node))
            .map(|node| {
                let gate = if self.gate(*node) == Gate::Open {
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
                committed_index: self.commit_index,
                matched,
                live,
                handoff_ready,
                gates,
                unlisted: self.unlisted.clone(),
                view,
            })),
        }
    }

    /// Executes `step` as the live leader would, each membership change
    /// under its CAS: `expected` must be the effective membership's log id,
    /// committed, with no step or transfer pending, else OpenRaft answers
    /// `PreconditionFailed` or `InProgress` and nothing changes.
    pub(crate) fn execute(&mut self, step: &MembershipStep) -> Result<(), Refused> {
        let Some(leader) = self.leader.filter(|leader| self.alive.contains(leader)) else {
            return Ok(());
        };
        let Some((config, id)) = self
            .membership_of(leader)
            .map(|(config, id)| (config.clone(), id))
        else {
            return Ok(());
        };
        let open =
            id.index <= self.commit_index && self.future.is_none() && self.transferring.is_none();
        let cas = |expected: &MembershipLogId| open && *expected == MembershipLogId::new(id);
        match step {
            MembershipStep::AddLearners { expected, nodes } => {
                if cas(expected) {
                    let mut learners = config.learners().clone();
                    learners.extend(nodes.iter().copied());
                    self.propose(with_learners(&config, learners), None);
                }
            }
            MembershipStep::RemoveLearners { expected, nodes } => {
                if cas(expected) {
                    let learners = config.learners().difference(nodes).copied().collect();
                    self.propose(with_learners(&config, learners), None);
                }
            }
            MembershipStep::ChangeVoters { expected, voters } => {
                if cas(expected) {
                    let members = config.members();
                    if let Some(node) = voters.iter().find(|node| !members.contains(node)) {
                        return Err(Refused::LearnerNotFound { node });
                    }
                    self.proposals.push(Proposal {
                        proposer: leader,
                        from: config.clone(),
                        voters: voters.clone(),
                    });
                    self.change_voters(&config, voters);
                }
            }
            MembershipStep::TransferLeadership { to } => {
                if self.transferring.is_none() {
                    self.transferring = Some(*to);
                }
            }
            MembershipStep::ConfirmView => self.confirmed = true,
            MembershipStep::Converged { .. } | MembershipStep::Wait(_) => {}
        }
        Ok(())
    }

    /// The leader appends `config` and keeps the step's future, with the
    /// uniform `second` step of a joint entry.
    fn propose(&mut self, config: MembershipConfig, second: Option<MembershipConfig>) {
        let Some(leader) = self.leader else {
            return;
        };
        let term = self.term;
        self.future = self.append(Some(config)).map(|awaiting| StepFuture {
            leader,
            term,
            awaiting,
            second,
            delayed: false,
        });
        self.sync(false);
    }

    /// `change_membership(ReplaceAllVoters(target), retain = false)` from
    /// `config`, the effective membership.
    fn change_voters(&mut self, config: &MembershipConfig, target: &VoterSet) {
        let learners: BTreeSet<NodeId> = config
            .learners()
            .iter()
            .copied()
            .filter(|node| !target.contains(*node))
            .collect();
        let uniform = MembershipConfig::Uniform {
            voters: target.clone(),
            learners: learners.clone(),
        };
        let old = match config {
            MembershipConfig::Uniform { voters, .. } => {
                if voters == target {
                    return;
                }
                voters.clone()
            }
            MembershipConfig::Joint { old, new, .. } => {
                if old == target || new == target {
                    self.propose(uniform, None);
                    return;
                }
                // `bridge_to`: `[A, B]` toward `C` goes through `[B, C]`.
                new.clone()
            }
        };
        let joint = MembershipConfig::Joint {
            old,
            new: target.clone(),
            learners,
        };
        self.propose(joint, Some(uniform));
    }
}

/// The outcome of [`run`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Ended {
    /// The leader answered `Converged` with the target's voters.
    Converged,
    /// No convergence within the round limit.
    Stuck,
}

/// The environment's half of a round: [`Group::settle`] when `fair`, else
/// [`Group::hold_back`].
pub(crate) fn environment(group: &mut Group, fair: bool) {
    if fair {
        group.settle();
    } else {
        group.hold_back();
    }
}

/// The leader's half of a round: it plans with the group's planner and
/// executes. `None` without a leader. A leader that crashed plans from a
/// `NotLeading` view. A step OpenRaft refuses fails the run.
pub(crate) fn act(group: &mut Group, target: &MembershipTarget) -> Result<Option<Planned>, String> {
    let Some(leader) = group.leader else {
        return Ok(None);
    };
    let view = group.view(leader);
    let step = (group.planner)(&view, target);
    group
        .execute(&step)
        .map_err(|refused| format!("OpenRaft refused {step:?}: {refused:?}"))?;
    Ok(Some(Planned { view, step }))
}

/// One round: the environment acts, then the leader plans and executes.
pub(crate) fn round(
    group: &mut Group,
    target: &MembershipTarget,
    fair: bool,
) -> Result<Option<Planned>, String> {
    environment(group, fair);
    act(group, target)
}

/// Runs fair rounds until the leader reports the target's voters, at most
/// `limit` rounds, calling `check` after every round.
pub(crate) fn run(
    group: &mut Group,
    target: &MembershipTarget,
    limit: usize,
    mut check: impl FnMut(&Group, Option<&Planned>) -> Result<(), String>,
) -> Result<Ended, String> {
    for _ in 0..limit {
        let planned = round(group, target, true)?;
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
