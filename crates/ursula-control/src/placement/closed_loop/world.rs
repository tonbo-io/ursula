//! The closed loop's model.
//!
//! - **Meta** is one log of entries, each with the state the kernel's
//!   dispatcher left after it. A recovery plan keeps a prefix and begins a
//!   new history with [`ControlPlaneState::begin_history`].
//! - **A node** has a placement view (a state of the meta log, possibly
//!   lagging or regressed), its journal's replicas with their log state
//!   (7.2), open state and teardowns, and the snapshot objects it holds. It
//!   may crash, restart with an older view, be cut off on the data plane,
//!   or believe itself removed.
//! - **A data group** is the abstract group of the reconciler's tests
//!   ([`Group`]): per-node logs, a CAS on the membership log id, a joint
//!   then a uniform entry per voter change with kept futures, lost and
//!   delayed second steps, commits only with an acknowledging majority of
//!   every config, elections by vote with recovery gates, an outgoing leader
//!   elected again on its uniform entry, a leader outside the committed
//!   config leading until its step-down, transfers, catch-up and gates. On
//!   top of it the model keeps leaders cut off on the data plane that still
//!   lead their own copy, step futures kept past their leadership, and data
//!   recovery plans.
//!
//! Every [`Action`] is one step of one actor or of the adversary, and
//! [`World::step`] checks C1 to C4 and C7 to C9 after each. C5 is
//! [`World::quiescent`], once the faults stop.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU8;
use std::num::NonZeroU32;
use std::sync::Arc;

use ursula_proto::placement::GroupCount;
use ursula_proto::placement::MembershipAnswer;
use ursula_proto::placement::MembershipLogId;
use ursula_proto::placement::MetaLogId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::NodeSet;
use ursula_proto::placement::RaftLogId;
use ursula_proto::placement::ReplicationFactor;
use ursula_proto::placement::TeardownStage;
use ursula_proto::placement::ViewStage;
use ursula_proto::placement::VoterSet;
use ursula_proto::placement::WaitReason;
use ursula_shard::RaftGroupId;

use crate::placement::Admission;
use crate::placement::ClusterParams;
use crate::placement::ConfirmedStamp;
use crate::placement::ConfirmedView;
use crate::placement::ControlApplied;
use crate::placement::ControlCommand;
use crate::placement::ControlError;
use crate::placement::ControlPlaneState;
use crate::placement::ControlResponse;
use crate::placement::Counts;
use crate::placement::GroupPlacement;
use crate::placement::HistoryCause;
use crate::placement::HostingInput;
use crate::placement::HostingPlan;
use crate::placement::HostingStep;
use crate::placement::LifecycleChange;
use crate::placement::LocalLog;
use crate::placement::LocalReplica;
use crate::placement::MaintenanceAuthority;
use crate::placement::MembershipConfig;
use crate::placement::MembershipStep;
use crate::placement::MembershipTarget;
use crate::placement::MoveLimit;
use crate::placement::NodeEligibility;
use crate::placement::PlacementView;
use crate::placement::SeedRoleOf;
use crate::placement::Standing;
use crate::placement::Target;
use crate::placement::TargetStage;
use crate::placement::Teardown;
use crate::placement::TeardownEnd;
use crate::placement::TeardownEvent;
use crate::placement::TeardownOutcome;
use crate::placement::TeardownStep;
use crate::placement::ViewInput;
use crate::placement::default_meta_voters;
use crate::placement::effective_replication_factor;
use crate::placement::group_ids;
use crate::placement::membership_answer;
use crate::placement::plan_decommission;
use crate::placement::plan_hosting;
use crate::placement::plan_membership_step;
use crate::placement::reconcile::model::Group;
use crate::placement::reconcile::model::Proposal;
use crate::placement::reconcile::model::Refused;
use crate::placement::reconcile::model::quorum_live;
use crate::placement::rules::Rule;
use crate::placement::rules::enforced;
use crate::placement::seed_role;

/// The move limit every model cluster bootstraps with, and the runbook
/// restores.
const DEFAULT_MOVE_LIMIT: u32 = 4;

/// How many actions a failure message lists.
const HISTORY: usize = 48;

/// How many host and teardown steps a teardown takes at most: its seven
/// steps, each after a hosting tick, and the report.
const TEARDOWN_STEPS: usize = 16;

pub(super) fn node(id: u64) -> NodeId {
    NodeId::new(id)
}

pub(super) fn voters(ids: &[u64]) -> VoterSet {
    VoterSet::new(ids.iter().copied().map(NodeId::new).collect()).expect("voters are not empty")
}

/// A meta log id in `term`. Node 1 leads every meta entry: who leads meta
/// does not matter here.
fn meta_id(term: u64, index: u64) -> MetaLogId {
    MetaLogId::new(RaftLogId {
        term,
        leader: NodeId::new(1),
        index,
    })
}

/// The closed loop's checks (15.1). `Script` is a scripted trace that did
/// not reach the state it drives the model to: a broken script, not a
/// broken rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Invariant {
    C1,
    C2,
    C3,
    C4,
    C5,
    C7,
    C8,
    C9,
    /// OpenRaft refused a step no correct planner takes.
    Refused,
    Script,
}

/// A failed check.
#[derive(Debug, Clone)]
pub(super) struct Violation {
    pub(super) invariant: Invariant,
    pub(super) detail: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.invariant, self.detail)
    }
}

fn violation(invariant: Invariant, detail: String) -> Violation {
    Violation { invariant, detail }
}

/// Which state of the meta log a view shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ViewAt {
    /// The state after the entry at this position of the current log,
    /// clamped to its end.
    Entry(usize),
    Latest,
}

/// One step of an actor or of the adversary.
#[derive(Debug, Clone)]
pub(super) enum Action {
    /// The node's hosting driver plans once, confirming its view first when
    /// the plan asks, and carries the plan out.
    Host(NodeId),
    /// The node's running teardown of the group takes its next step.
    Teardown(NodeId, RaftGroupId),
    /// The node's reconciler of the group plans once, as its leader or as a
    /// leader cut off on the data plane, and executes the step. With `hold`
    /// a membership step's future is kept instead and a report stays in
    /// flight.
    Reconcile {
        node: NodeId,
        group: RaftGroupId,
        hold: bool,
    },
    /// The node's seed driver: it initializes the group when a confirmed
    /// view shows it the initializer and every seed voter answers empty.
    Seed(NodeId, RaftGroupId),
    /// The fair environment of the group: the step-down of a leader a past
    /// action removed, an election when needed, the leader's log replicated
    /// and committed, a pending transfer resolved, and the barrier's gates
    /// and catch-up.
    Settle(RaftGroupId),
    /// Only an election when needed.
    Elect(RaftGroupId),
    /// The group's next voter change loses its second step.
    DropSecondStep(RaftGroupId),
    /// The group's next second step waits for its next settle.
    DelaySecondStep(RaftGroupId),
    /// The node campaigns and leads when it would win: an outgoing voter
    /// holding the uncommitted uniform entry included.
    Campaign(RaftGroupId, NodeId),
    /// The leader starts a transfer to the node, which the group's next
    /// settle completes when the node holds the leader's log and wins.
    Transfer(RaftGroupId, NodeId),
    /// Every kept step future resolves now: the step of the planner that
    /// leads runs, possibly in a later leadership, and the others fail.
    DeliverStep(RaftGroupId),
    /// Every report in flight reaches meta now.
    DeliverReports,
    /// An operator command, or a driver's.
    Command(ControlCommand),
    /// One pass of the decommission driver.
    Decommission,
    /// One pass of the removed-node sweep.
    Sweep,
    Crash(NodeId),
    /// A crashed node starts again, its view at the given state.
    Restart(NodeId, ViewAt),
    /// The node is cut off from every other node on the data plane. Meta
    /// still reaches it.
    Partition(NodeId),
    Heal(NodeId),
    /// The node's meta replica shows the given state, and lags there until
    /// a later action shows it the latest.
    View(NodeId, ViewAt),
    /// A meta recovery plan keeps the first `keep` entries and begins a
    /// history.
    MetaPlan {
        keep: usize,
    },
    /// A data recovery plan commits the group's committed membership at
    /// position `at` of its history again.
    DataPlan {
        group: RaftGroupId,
        at: usize,
    },
    /// Meta answers, or not.
    MetaAvailable(bool),
}

/// One meta entry and the state after it.
#[derive(Debug, Clone)]
struct MetaEntry {
    id: MetaLogId,
    /// The history event that began this entry's history.
    history: MetaLogId,
    state: Arc<ControlPlaneState>,
}

/// The meta group's log.
#[derive(Debug, Clone)]
pub(super) struct Meta {
    entries: Vec<MetaEntry>,
    term: u64,
    pub(super) up: bool,
}

impl Meta {
    fn genesis() -> Self {
        let id = meta_id(1, 1);
        let mut state = ControlPlaneState::default();
        let _begun = state.begin_history(id, HistoryCause::Genesis);
        Self {
            entries: vec![MetaEntry {
                id,
                history: id,
                state: Arc::new(state),
            }],
            term: 1,
            up: true,
        }
    }

    fn last(&self) -> &MetaEntry {
        self.entries.last().expect("meta holds its genesis event")
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn state(&self) -> &ControlPlaneState {
        &self.last().state
    }

    fn history(&self) -> MetaLogId {
        self.last().history
    }

    fn next_id(&self) -> MetaLogId {
        let index = self.last().id.get().index.checked_add(1).expect("index");
        meta_id(self.term, index)
    }

    /// Applies `command` as the next entry: a refused command is an entry
    /// too.
    fn apply(&mut self, command: ControlCommand) -> (MetaLogId, ControlResponse) {
        let at = self.next_id();
        let mut state = ControlPlaneState::clone(&self.last().state);
        let response = state.apply(at, command);
        let history = self.history();
        self.entries.push(MetaEntry {
            id: at,
            history,
            state: Arc::new(state),
        });
        (at, response)
    }

    /// A recovery plan: keep the first `keep` entries, and begin a history
    /// in a new term.
    fn plan(&mut self, keep: usize) {
        let keep = keep.clamp(1, self.entries.len());
        self.entries.truncate(keep);
        self.term = self.term.checked_add(1).expect("term");
        let at = self.next_id();
        let mut state = ControlPlaneState::clone(&self.last().state);
        let _begun = state.begin_history(at, HistoryCause::Plan);
        self.entries.push(MetaEntry {
            id: at,
            history: at,
            state: Arc::new(state),
        });
    }

    fn view_at(&self, position: usize) -> Arc<PlacementView> {
        let last = self.entries.len().saturating_sub(1);
        let entry = &self.entries[position.min(last)];
        Arc::new(PlacementView {
            stage: ViewStage::Applied {
                applied: entry.id,
                history: entry.history,
            },
            state: Arc::clone(&entry.state),
        })
    }

    fn latest_view(&self) -> Arc<PlacementView> {
        self.view_at(self.entries.len())
    }

    /// What a meta read barrier answers now.
    fn stamp(&self) -> ConfirmedStamp {
        ConfirmedStamp {
            history: self.history(),
            applied: self.last().id,
        }
    }

    /// Whether `view` shows a state of the current log, in its history.
    fn current(&self, view: &PlacementView) -> bool {
        match view.stage {
            ViewStage::Empty => false,
            ViewStage::Applied { applied, history } => {
                history == self.history() && self.entries.iter().any(|entry| entry.id == applied)
            }
        }
    }
}

/// What the node's journal records of a group: 7.2's `GroupLogState`,
/// from which the shell reads the [`LocalLog`] the hosting planner takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Journal {
    /// Created and never initialized: the only state the seed probe
    /// answers as empty.
    Empty,
    /// Holds the group's history and its snapshot objects.
    Initialized,
    /// A `Retired` log `Create` reopened (7.3): recorded `Recovering` with
    /// an empty log and no vote, its gate closed. It is held and never
    /// answers the probe as empty, and holds the group's data and objects
    /// only once it catches up.
    Recovering,
    /// A teardown retired the log.
    Retired,
}

impl Journal {
    fn local(self) -> LocalLog {
        match self {
            Self::Empty => LocalLog::Empty,
            Self::Initialized | Self::Recovering => LocalLog::Holding,
            Self::Retired => LocalLog::Retired,
        }
    }

    pub(super) fn is_held(self) -> bool {
        self.local().is_held()
    }
}

/// A group the node's journal names.
#[derive(Debug, Clone)]
pub(super) struct Replica {
    pub(super) log: Journal,
    /// The replica runs: it votes, acknowledges and catches up.
    pub(super) open: bool,
    pub(super) teardown: Option<Teardown>,
    /// The node's removal closed it.
    pub(super) closed_by_removal: bool,
}

impl Replica {
    fn created() -> Self {
        Self {
            log: Journal::Empty,
            open: true,
            teardown: None,
            closed_by_removal: false,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct NodeState {
    pub(super) alive: bool,
    pub(super) partitioned: bool,
    pub(super) view: Arc<PlacementView>,
    /// The node's meta replica receives nothing: its view moves only by a
    /// confirmation or a [`Action::View`].
    pub(super) lagging: bool,
    /// The node learned its removal, from its view or its registration,
    /// and believes it until it restarts.
    pub(super) removed: bool,
    pub(super) replicas: BTreeMap<RaftGroupId, Replica>,
    /// The groups whose snapshot objects the node holds.
    pub(super) objects: BTreeSet<RaftGroupId>,
}

impl NodeState {
    fn started(view: Arc<PlacementView>) -> Self {
        Self {
            alive: true,
            partitioned: false,
            view,
            lagging: false,
            removed: false,
            replicas: BTreeMap::new(),
            objects: BTreeSet::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct DataGroup {
    /// `None` until the group initializes.
    pub(super) raft: Option<Group>,
    /// How often a seed driver initialized the group: C4 fails on a second
    /// `Initialize`.
    initializations: u32,
    /// The leader's placement confirmation: the term of its leadership and
    /// the barrier's stamp.
    confirmation: Option<(u64, ConfirmedStamp)>,
    /// Leaders cut off on the data plane, each leading its own copy of the
    /// group, which commits nothing that needs another node.
    deposed: BTreeMap<NodeId, Group>,
    /// The membership steps whose futures were kept, one per planner. A
    /// kept step may run in a later leadership of its planner, which 5.5's
    /// `CommittedLeaderId` refuses: the adversary is stronger than the
    /// design, so the membership CAS alone must keep such a step safe.
    kept: BTreeMap<NodeId, MembershipStep>,
}

/// How often a run reached what only a long or faulty run reaches.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Tally {
    /// Moves that converged and retired their source.
    pub(super) moves: usize,
    /// Teardowns that retired their log.
    pub(super) retired: usize,
    pub(super) meta_plans: usize,
    pub(super) data_plans: usize,
    /// Nodes meta removed (C9).
    pub(super) removals: usize,
    /// Snapshot objects the removed-node sweep deleted.
    pub(super) sweeps: usize,
    /// Reports meta refused with `StalePlacement` (the placement CAS).
    pub(super) stale_reports: usize,
    /// `Converged` answers no leadership proof backed, so never reported.
    pub(super) unproven_reports: usize,
    /// Leaders that planned from a committed config without themselves and
    /// waited (`NotAMember`, step 0 of 5.3).
    pub(super) not_a_member: usize,
    /// Elections an outgoing voter won on the uncommitted uniform entry.
    pub(super) outgoing_wins: usize,
}

impl Tally {
    pub(super) fn merge(self, other: Self) -> Self {
        Self {
            moves: self.moves.saturating_add(other.moves),
            retired: self.retired.saturating_add(other.retired),
            meta_plans: self.meta_plans.saturating_add(other.meta_plans),
            data_plans: self.data_plans.saturating_add(other.data_plans),
            removals: self.removals.saturating_add(other.removals),
            sweeps: self.sweeps.saturating_add(other.sweeps),
            stale_reports: self.stale_reports.saturating_add(other.stale_reports),
            unproven_reports: self.unproven_reports.saturating_add(other.unproven_reports),
            not_a_member: self.not_a_member.saturating_add(other.not_a_member),
            outgoing_wins: self.outgoing_wins.saturating_add(other.outgoing_wins),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct World {
    pub(super) meta: Meta,
    pub(super) nodes: BTreeMap<NodeId, NodeState>,
    pub(super) groups: BTreeMap<RaftGroupId, DataGroup>,
    /// Proven reports meta has not applied yet: paused reporters.
    in_flight: Vec<ControlCommand>,
    /// The membership each accepted converge certified, by group and
    /// converge entry: the retirements' claims (6.2) start there.
    certified: BTreeMap<(RaftGroupId, MetaLogId), MembershipLogId>,
    /// Groups a data plan cut below what a report or a teardown relied on:
    /// C1's destruction clause and C3's claims skip them from then on
    /// (P0-1, I8's exception).
    uncertified: BTreeSet<RaftGroupId>,
    /// Snapshot objects the removed-node sweep deleted.
    swept: BTreeSet<(RaftGroupId, NodeId)>,
    /// When each node was first seen removed: each group's history length
    /// then.
    removed_at: BTreeMap<NodeId, BTreeMap<RaftGroupId, usize>>,
    /// Violations an action itself produced: C2, C3's destructions, C4, C8
    /// and C9's voter changes.
    found: Vec<Violation>,
    log: Vec<Action>,
    /// How many meta entries the bootstrap left: a meta plan keeps at least
    /// these.
    pub(super) boot_len: usize,
    /// The replication factor the cluster bootstrapped with.
    rf: usize,
    tally: Tally,
}

impl World {
    /// A cluster of nodes `1..=node_count`, registered and bootstrapped
    /// with `group_count` seeded groups at replication factor `rf` (at most
    /// the node count). No group has initialized.
    pub(super) fn new(node_count: u64, group_count: u32, rf: u8) -> Self {
        let ids: BTreeSet<NodeId> = (1..=node_count).map(NodeId::new).collect();
        let nodes = NodeSet::new(ids.clone()).expect("a cluster has a node");
        let mut meta = Meta::genesis();
        for id in &ids {
            meta.apply(ControlCommand::RegisterNode {
                node: *id,
                now_ms: 0,
            })
            .1
            .expect("a node registers");
        }
        let configured = ReplicationFactor::new(NonZeroU8::new(rf).expect("a replication factor"));
        let replication_factor = effective_replication_factor(configured, &nodes);
        let count = GroupCount::new(NonZeroU32::new(group_count).expect("a group"));
        let params = ClusterParams {
            group_count: count,
            replication_factor,
            meta_voters: default_meta_voters(&nodes),
            max_concurrent_moves: MoveLimit::new(DEFAULT_MOVE_LIMIT),
            maintenance: MaintenanceAuthority::Placement,
        };
        meta.apply(ControlCommand::Bootstrap {
            params,
            nodes,
            now_ms: 0,
        })
        .1
        .expect("the cluster bootstraps");
        let boot_len = meta.len();
        let view = meta.latest_view();
        Self {
            nodes: ids
                .iter()
                .map(|id| (*id, NodeState::started(Arc::clone(&view))))
                .collect(),
            groups: group_ids(count)
                .map(|group| (group, DataGroup::default()))
                .collect(),
            meta,
            in_flight: Vec::new(),
            certified: BTreeMap::new(),
            uncertified: BTreeSet::new(),
            swept: BTreeSet::new(),
            removed_at: BTreeMap::new(),
            found: Vec::new(),
            log: Vec::new(),
            boot_len,
            rf: usize::from(replication_factor.get()),
            tally: Tally::default(),
        }
    }

    /// Takes `action`, then checks C1 to C4 and C7 to C9.
    pub(super) fn step(&mut self, action: Action) -> Result<(), Violation> {
        self.log.push(action.clone());
        self.sync();
        self.act(action);
        self.sync();
        self.observe();
        let found = std::mem::take(&mut self.found);
        if let Some(first) = found.into_iter().next() {
            return Err(first);
        }
        self.check()
    }

    fn act(&mut self, action: Action) {
        match action {
            Action::Host(node) => self.host(node),
            Action::Teardown(node, group) => self.teardown(node, group),
            Action::Reconcile { node, group, hold } => self.reconcile(node, group, hold),
            Action::Seed(node, group) => self.seed(node, group),
            Action::Settle(group) => {
                if let Some(raft) = self.raft_mut(group) {
                    raft.settle();
                }
            }
            Action::Elect(group) => {
                if let Some(raft) = self.raft_mut(group) {
                    raft.elect();
                }
            }
            Action::DropSecondStep(group) => {
                if let Some(raft) = self.raft_mut(group) {
                    raft.drop_second_step = true;
                }
            }
            Action::DelaySecondStep(group) => {
                if let Some(raft) = self.raft_mut(group) {
                    raft.delay_second_step = true;
                }
            }
            Action::Campaign(group, node) => {
                if let Some(raft) = self.raft_mut(group) {
                    let _won = raft.campaign(node);
                }
            }
            Action::Transfer(group, to) => {
                let refused = self.raft_mut(group).and_then(|raft| {
                    raft.execute(&MembershipStep::TransferLeadership { to })
                        .err()
                });
                self.refused(group, refused);
            }
            Action::DeliverStep(group) => self.deliver_step(group),
            Action::DeliverReports => self.deliver_reports(),
            Action::Command(command) => {
                let _response = self.submit(command);
            }
            Action::Decommission => self.decommission(),
            Action::Sweep => self.sweep(),
            Action::Crash(node) => self.crash(node),
            Action::Restart(node, at) => self.restart(node, at),
            Action::Partition(node) => self.partition(node),
            Action::Heal(node) => self.heal(node),
            Action::View(node, at) => {
                let view = self.meta_view(at);
                if let Some(state) = self.nodes.get_mut(&node) {
                    state.view = view;
                    state.lagging = at != ViewAt::Latest;
                }
            }
            Action::MetaPlan { keep } => self.meta_plan(keep),
            Action::DataPlan { group, at } => self.data_plan(group, at),
            Action::MetaAvailable(up) => self.meta.up = up,
        }
    }

    // Accessors.

    pub(super) fn raft(&self, group: RaftGroupId) -> Option<&Group> {
        self.groups.get(&group).and_then(|data| data.raft.as_ref())
    }

    fn raft_mut(&mut self, group: RaftGroupId) -> Option<&mut Group> {
        self.groups
            .get_mut(&group)
            .and_then(|data| data.raft.as_mut())
    }

    pub(super) fn leader(&self, group: RaftGroupId) -> Option<NodeId> {
        self.raft(group).and_then(|raft| raft.leader)
    }

    /// A leader of the group cut off on the data plane, if any.
    pub(super) fn deposed_leader(&self, group: RaftGroupId) -> Option<NodeId> {
        self.groups
            .get(&group)
            .and_then(|data| data.deposed.keys().next().copied())
    }

    pub(super) fn replica(&self, node: NodeId, group: RaftGroupId) -> Option<&Replica> {
        self.nodes
            .get(&node)
            .and_then(|state| state.replicas.get(&group))
    }

    fn replica_mut(&mut self, node: NodeId, group: RaftGroupId) -> Option<&mut Replica> {
        self.nodes
            .get_mut(&node)
            .and_then(|state| state.replicas.get_mut(&group))
    }

    /// The group's placement in meta's latest state.
    pub(super) fn placement(&self, group: RaftGroupId) -> Option<GroupPlacement> {
        self.meta
            .state()
            .cluster()
            .and_then(|cluster| cluster.group(group))
            .cloned()
    }

    /// The voters of the group's committed membership, when it is uniform.
    pub(super) fn committed_voters(&self, group: RaftGroupId) -> Option<VoterSet> {
        match &self.raft(group)?.committed {
            MembershipConfig::Uniform { voters, .. } => Some(voters.clone()),
            MembershipConfig::Joint { .. } => None,
        }
    }

    pub(super) fn learners(&self, group: RaftGroupId) -> BTreeSet<NodeId> {
        self.raft(group)
            .map(|raft| raft.committed.learners().clone())
            .unwrap_or_default()
    }

    pub(super) fn caught_up(&self, group: RaftGroupId, id: u64) -> bool {
        self.raft(group)
            .is_some_and(|raft| raft.caught_up.contains(&node(id)))
    }

    /// Whether the group's placement is stable on `ids`.
    pub(super) fn is_stable(&self, group: RaftGroupId, ids: &[u64]) -> bool {
        matches!(
            self.placement(group),
            Some(GroupPlacement::Live { target: Target::Stable { voters: stable }, .. }) if stable == voters(ids)
        )
    }

    pub(super) fn teardown_stage(&self, id: u64, group: RaftGroupId) -> Option<TeardownStage> {
        self.replica(node(id), group)
            .and_then(|replica| replica.teardown)
            .map(Teardown::stage)
    }

    /// The first position of the group's history whose config is uniform on
    /// `ids` without learners.
    pub(super) fn position(&self, group: RaftGroupId, ids: &[u64]) -> Option<usize> {
        let wanted = voters(ids);
        self.raft(group)?
            .committed_configs
            .iter()
            .position(|(_, config)| {
                matches!(config, MembershipConfig::Uniform { voters, learners } if *voters == wanted && learners.is_empty())
            })
    }

    /// The first group whose teardown runs on the node.
    pub(super) fn teardown_of(&self, node: NodeId) -> Option<RaftGroupId> {
        self.nodes.get(&node).and_then(|state| {
            state
                .replicas
                .iter()
                .find(|(_, replica)| replica.teardown.is_some())
                .map(|(group, _)| *group)
        })
    }

    fn running_teardowns(&self) -> Vec<(NodeId, RaftGroupId)> {
        self.nodes
            .iter()
            .filter(|(_, state)| state.alive)
            .flat_map(|(node, state)| {
                state
                    .replicas
                    .iter()
                    .filter(|(_, replica)| replica.teardown.is_some())
                    .map(move |(group, _)| (*node, *group))
            })
            .collect()
    }

    pub(super) fn alive(&self, node: NodeId) -> bool {
        self.nodes.get(&node).is_some_and(|state| state.alive)
    }

    pub(super) fn partitioned(&self, node: NodeId) -> bool {
        self.nodes.get(&node).is_some_and(|state| state.partitioned)
    }

    /// Whether the node runs and reaches the others on the data plane.
    fn reaches(&self, node: NodeId) -> bool {
        self.nodes
            .get(&node)
            .is_some_and(|state| state.alive && !state.partitioned)
    }

    fn view_of(&self, node: NodeId) -> Arc<PlacementView> {
        self.nodes
            .get(&node)
            .map_or_else(|| self.meta.latest_view(), |state| Arc::clone(&state.view))
    }

    fn meta_view(&self, at: ViewAt) -> Arc<PlacementView> {
        match at {
            ViewAt::Entry(position) => self.meta.view_at(position),
            ViewAt::Latest => self.meta.latest_view(),
        }
    }

    fn standing(&self, node: NodeId) -> Standing {
        if self.nodes.get(&node).is_some_and(|state| state.removed) {
            Standing::Removed
        } else {
            Standing::Member
        }
    }

    fn removed_nodes(&self) -> BTreeSet<NodeId> {
        self.meta
            .state()
            .cluster()
            .map(|cluster| {
                cluster
                    .nodes()
                    .iter()
                    .filter(|(_, record)| record.lifecycle == NodeLifecycle::Removed)
                    .map(|(node, _)| *node)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Whether a report of the group is in flight.
    pub(super) fn report_in_flight(&self, group: RaftGroupId) -> bool {
        self.in_flight
            .iter()
            .any(|report| report.group() == Some(group))
    }

    /// The lowest position of the group's history a data plan may keep: the
    /// last certified membership, the only floor 7.4's tool takes.
    pub(super) fn plan_floor(&self, group: RaftGroupId) -> usize {
        let Some(raft) = self.raft(group) else {
            return 0;
        };
        self.certified
            .iter()
            .filter(|((reported, _), _)| *reported == group)
            .filter_map(|(_, membership)| {
                raft.committed_configs
                    .iter()
                    .position(|(id, _)| id == membership)
            })
            .max()
            .unwrap_or(0)
    }

    /// The run's tally, with the outgoing voters' wins of every group.
    pub(super) fn tally(&self) -> Tally {
        let outgoing_wins = self
            .groups
            .values()
            .filter_map(|data| data.raft.as_ref())
            .map(|raft| raft.elections.outgoing)
            .sum();
        Tally {
            outgoing_wins,
            ..self.tally
        }
    }

    /// Nodes that run and reach the others with an open, held replica of
    /// the group: the group's live members.
    fn reachable_replicas(&self, group: RaftGroupId) -> BTreeSet<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, state)| {
                state.alive
                    && !state.partitioned
                    && state
                        .replicas
                        .get(&group)
                        .is_some_and(|replica| replica.open && replica.log.is_held())
            })
            .map(|(node, _)| *node)
            .collect()
    }

    /// A live leader's answer about `node`, from its committed and
    /// effective memberships. `None` when no leader that holds a quorum
    /// answers.
    fn answer_about(&self, node: NodeId, group: RaftGroupId) -> Option<MembershipAnswer> {
        let raft = self.raft(group)?;
        let leader = raft.leader?;
        let holds_quorum = self.reaches(leader)
            && raft.alive.contains(&leader)
            && quorum_live(&raft.committed, &raft.alive)
            && quorum_live(raft.effective(), &raft.alive);
        holds_quorum.then(|| membership_answer(node, &raft.committed, raft.effective()))
    }

    /// What the node hears when it asks its group: nothing when cut off.
    fn ask(&self, node: NodeId, group: RaftGroupId) -> Option<MembershipAnswer> {
        if self.reaches(node) {
            self.answer_about(node, group)
        } else {
            None
        }
    }

    /// The shell's one voter predicate (S10), as the model sees it: a live
    /// leader's answer, and a voter until a leader answers.
    fn counts(&self, node: NodeId, group: RaftGroupId) -> Counts {
        match self.answer_about(node, group) {
            Some(MembershipAnswer::Voter) | None => Counts::Voter,
            Some(MembershipAnswer::Learner | MembershipAnswer::NotMember) => Counts::NotVoter,
        }
    }

    fn local(&self, node: NodeId) -> BTreeMap<RaftGroupId, LocalReplica> {
        let Some(state) = self.nodes.get(&node) else {
            return BTreeMap::new();
        };
        state
            .replicas
            .iter()
            .map(|(group, replica)| {
                let local = LocalReplica {
                    log: replica.log.local(),
                    counts: self.counts(node, *group),
                    teardown: replica.teardown,
                };
                (*group, local)
            })
            .collect()
    }

    // Meta.

    /// Applies `command` when meta answers, recording what a converge
    /// certified.
    fn submit(&mut self, command: ControlCommand) -> Option<ControlResponse> {
        if !self.meta.up {
            return None;
        }
        let moving = command
            .group()
            .and_then(|group| self.placement(group))
            .is_some_and(|placement| placement.moving().is_some());
        let (at, response) = self.meta.apply(command.clone());
        if let ControlCommand::ReportConverged {
            group, membership, ..
        } = command
        {
            match &response {
                Ok(ControlApplied::Converged { .. }) => {
                    self.certified.insert((group, at), membership);
                    if moving {
                        self.tally.moves = self.tally.moves.saturating_add(1);
                    }
                }
                Err(ControlError::StalePlacement { .. }) => {
                    self.tally.stale_reports = self.tally.stale_reports.saturating_add(1);
                    // 5.7: a stale report forces a confirmation before the
                    // next plan.
                    if let Some(data) = self.groups.get_mut(&group) {
                        data.confirmation = None;
                    }
                }
                Ok(_) | Err(_) => {}
            }
        }
        Some(response)
    }

    /// A meta read barrier for the node: the stamp, with the node's view
    /// caught up to it unless the view already covers it.
    fn confirm(&mut self, node: NodeId) -> Option<ConfirmedStamp> {
        if !self.meta.up {
            return None;
        }
        let stamp = self.meta.stamp();
        let latest = self.meta.latest_view();
        let state = self.nodes.get_mut(&node)?;
        if ConfirmedView::new(Arc::clone(&state.view), stamp).is_err() {
            state.view = latest;
        }
        Some(stamp)
    }

    /// Meta replicates to the node, unless its replica lags.
    fn replicate(&mut self, node: NodeId) {
        if !self.meta.up {
            return;
        }
        let latest = self.meta.latest_view();
        if let Some(state) = self.nodes.get_mut(&node)
            && !state.lagging
        {
            state.view = latest;
        }
    }

    /// Whether a confirmed view is in the current meta history and log.
    fn in_current_history(&self, confirmed: &ConfirmedView) -> bool {
        self.meta.current(confirmed.view()) && confirmed.stamp().history == self.meta.history()
    }

    // Hosting.

    fn host(&mut self, me: NodeId) {
        if !self.alive(me) {
            return;
        }
        self.replicate(me);
        let view = self.view_of(me);
        if shows_removed(&view, me)
            && let Some(state) = self.nodes.get_mut(&me)
        {
            state.removed = true;
        }
        let standing = self.standing(me);
        let local = self.local(me);
        let mut plan = plan_hosting(&HostingInput {
            me,
            standing,
            view: ViewInput::Unconfirmed(view.as_ref()),
            local: &local,
        });
        let mut fresh = false;
        let wants_confirmation = plan.steps.first() == Some(&HostingStep::ConfirmView);
        if wants_confirmation
            && let Some(stamp) = self.confirm(me)
            && let Ok(confirmed) = ConfirmedView::new(self.view_of(me), stamp)
        {
            fresh = self.in_current_history(&confirmed);
            plan = plan_hosting(&HostingInput {
                me,
                standing,
                view: ViewInput::Confirmed(&confirmed),
                local: &local,
            });
        }
        self.carry_out(me, standing, &plan, fresh);
    }

    /// The hosting driver carries `plan` out: it closes what the plan does
    /// not admit, opens and creates what it does, and starts, cancels and
    /// reports teardowns. `fresh`: the plan's view was confirmed in the
    /// current meta history.
    fn carry_out(&mut self, me: NodeId, standing: Standing, plan: &HostingPlan, fresh: bool) {
        let mut created = Vec::new();
        let mut reports = Vec::new();
        let Some(state) = self.nodes.get_mut(&me) else {
            return;
        };
        for (group, replica) in &mut state.replicas {
            if replica.open && !plan.admitted.contains_key(group) {
                replica.open = false;
                replica.closed_by_removal = standing == Standing::Removed;
            }
        }
        for (group, admission) in &plan.admitted {
            match admission {
                Admission::Open => {
                    if let Some(replica) = state.replicas.get_mut(group)
                        && replica.log.is_held()
                    {
                        replica.open = true;
                        replica.closed_by_removal = false;
                    }
                }
                Admission::Create => match state.replicas.get_mut(group) {
                    None => {
                        state.replicas.insert(*group, Replica::created());
                        created.push(*group);
                    }
                    Some(replica) => {
                        // 7.3's reopen: a retired log comes back
                        // initialized, never empty.
                        if replica.log == Journal::Retired {
                            replica.log = Journal::Recovering;
                            created.push(*group);
                        }
                        replica.open = true;
                        replica.closed_by_removal = false;
                    }
                },
            }
        }
        for step in &plan.steps {
            match *step {
                HostingStep::ConfirmView => {}
                HostingStep::TearDown { group } => {
                    if let Some(replica) = state.replicas.get_mut(&group)
                        && replica.teardown.is_none()
                    {
                        replica.teardown = Some(Teardown::start());
                    }
                }
                HostingStep::CancelTeardown { group } => {
                    if let Some(replica) = state.replicas.get_mut(&group)
                        && let Some(teardown) = replica.teardown
                    {
                        replica.teardown = match teardown.advance(TeardownEvent::Cancel) {
                            TeardownOutcome::Continue(next) => Some(next),
                            TeardownOutcome::Ended(_) => None,
                        };
                    }
                }
                HostingStep::ReportTornDown { group, created_at } => {
                    reports.push(ControlCommand::ReportTornDown {
                        group,
                        node: me,
                        created_at,
                    });
                }
            }
        }
        if !fresh {
            for group in created {
                self.found.push(violation(
                    Invariant::C8,
                    format!(
                        "node {me} created its replica of group {} from a view not confirmed in \
                         the current meta history",
                        group.0
                    ),
                ));
            }
        }
        for report in reports {
            let _response = self.submit(report);
        }
    }

    fn teardown(&mut self, me: NodeId, group: RaftGroupId) {
        if !self.alive(me) {
            return;
        }
        let Some(teardown) = self.replica(me, group).and_then(|replica| replica.teardown) else {
            return;
        };
        let event = match teardown {
            Teardown::Asking | Teardown::Running(TeardownStep::AskAgain) => {
                TeardownEvent::Answered(self.ask(me, group))
            }
            Teardown::Running(_) => TeardownEvent::StepDone,
        };
        if let (Teardown::Running(done), TeardownEvent::StepDone) = (teardown, event) {
            match done {
                TeardownStep::Stop => {
                    if let Some(replica) = self.replica_mut(me, group) {
                        replica.open = false;
                    }
                }
                TeardownStep::Retire => self.retire(me, group),
                TeardownStep::Fence
                | TeardownStep::Unregister
                | TeardownStep::AwaitIdle
                | TeardownStep::AskAgain
                | TeardownStep::ForgetMetrics => {}
            }
        }
        let next = match teardown.advance(event) {
            TeardownOutcome::Continue(next) => Some(next),
            TeardownOutcome::Ended(end) => {
                if end == TeardownEnd::Retired {
                    self.tally.retired = self.tally.retired.saturating_add(1);
                }
                None
            }
        };
        if let Some(replica) = self.replica_mut(me, group) {
            replica.teardown = next;
        }
    }

    /// Step 5 passed: the log is retired and its objects deleted. C3: the
    /// group lists the node as a voter in no config.
    fn retire(&mut self, me: NodeId, group: RaftGroupId) {
        let listed = self
            .raft(group)
            .filter(|raft| raft.committed.is_voter(me) || raft.effective().is_voter(me))
            .map(|raft| format!("{:?}", raft.effective()));
        if let Some(membership) = listed {
            self.found.push(violation(
                Invariant::C3,
                format!(
                    "node {me} destroyed its replica of group {} while the group lists it as a \
                     voter: {membership}",
                    group.0
                ),
            ));
        }
        if let Some(state) = self.nodes.get_mut(&me) {
            if let Some(replica) = state.replicas.get_mut(&group) {
                replica.log = Journal::Retired;
                replica.open = false;
            }
            state.objects.remove(&group);
        }
        if let Some(raft) = self.raft_mut(group) {
            raft.wipe(me);
        }
    }

    // Reconciliation.

    fn reconcile(&mut self, me: NodeId, group: RaftGroupId, hold: bool) {
        if !self.alive(me) {
            return;
        }
        self.replicate(me);
        let view = self.view_of(me);
        let Some(placement) = view.cluster().and_then(|cluster| cluster.group(group)) else {
            return;
        };
        let target = MembershipTarget::of_placement(placement);
        let removed = self.removed_nodes();
        let Some(data) = self.groups.get_mut(&group) else {
            return;
        };
        if data.kept.contains_key(&me) {
            // A step of this node is pending: it plans nothing.
            return;
        }
        let deposed = data.deposed.contains_key(&me);
        let confirmation = data.confirmation;
        let seat = if deposed {
            data.deposed.get_mut(&me)
        } else {
            data.raft.as_mut()
        };
        let Some(seat) = seat else {
            return;
        };
        if !deposed {
            seat.confirmed = confirmation.is_some_and(|(term, stamp)| {
                term == seat.term && ConfirmedView::new(Arc::clone(&view), stamp).is_ok()
            });
        }
        let planned = seat.view(me);
        let step = plan_membership_step(&planned, &target);
        let from = seat.committed.clone();
        let membership = seat.committed_id;
        let term = seat.term;
        // The leadership proof: a live leader of the real group whose
        // committed config has a live majority.
        let proven = !deposed
            && seat.leader == Some(me)
            && seat.alive.contains(&me)
            && quorum_live(&seat.committed, &seat.alive);
        self.check_voter_change(me, group, &step, &from, &removed);
        if !deposed && matches!(step, MembershipStep::Wait(WaitReason::NotAMember)) {
            self.tally.not_a_member = self.tally.not_a_member.saturating_add(1);
        }
        match step {
            MembershipStep::ConfirmView => {
                let Some(stamp) = self.confirm(me) else {
                    return;
                };
                let Some(data) = self.groups.get_mut(&group) else {
                    return;
                };
                if deposed {
                    if let Some(copy) = data.deposed.get_mut(&me) {
                        copy.confirmed = true;
                    }
                } else {
                    data.confirmation = Some((term, stamp));
                }
            }
            MembershipStep::Converged { voters } => {
                let since = match target.stage {
                    TargetStage::Seeded { since } | TargetStage::Moving { since } => since,
                    TargetStage::Settled => return,
                };
                if !proven {
                    self.tally.unproven_reports = self.tally.unproven_reports.saturating_add(1);
                    if enforced(Rule::LeadershipProof) {
                        return;
                    }
                }
                let report = ControlCommand::ReportConverged {
                    group,
                    since,
                    voters,
                    membership,
                    now_ms: 0,
                };
                if hold {
                    self.in_flight.push(report);
                } else {
                    let _response = self.submit(report);
                }
            }
            MembershipStep::Wait(_) => {}
            step @ (MembershipStep::AddLearners { .. }
            | MembershipStep::ChangeVoters { .. }
            | MembershipStep::RemoveLearners { .. }
            | MembershipStep::TransferLeadership { .. }) => {
                let Some(data) = self.groups.get_mut(&group) else {
                    return;
                };
                let refused = if deposed {
                    data.deposed
                        .get_mut(&me)
                        .and_then(|copy| copy.execute(&step).err())
                } else if hold {
                    data.kept.insert(me, step);
                    None
                } else {
                    data.raft
                        .as_mut()
                        .and_then(|raft| raft.execute(&step).err())
                };
                self.refused(group, refused);
            }
        }
    }

    /// A step OpenRaft refused, which no correct planner takes.
    fn refused(&mut self, group: RaftGroupId, refused: Option<Refused>) {
        if let Some(refused) = refused {
            self.found.push(violation(
                Invariant::Refused,
                format!("group {}: {refused:?}", group.0),
            ));
        }
    }

    /// C2 and C9 on a planned voter change from `from`, the planner's
    /// committed config: C2 with the reconciler's tests' one predicate,
    /// [`Proposal::keeps_proposer`].
    fn check_voter_change(
        &mut self,
        me: NodeId,
        group: RaftGroupId,
        step: &MembershipStep,
        from: &MembershipConfig,
        removed: &BTreeSet<NodeId>,
    ) {
        let MembershipStep::ChangeVoters { voters, .. } = step else {
            return;
        };
        let current = match from {
            MembershipConfig::Uniform { voters, .. } => Some(voters),
            MembershipConfig::Joint { .. } => None,
        };
        let proposal = Proposal {
            proposer: me,
            from: from.clone(),
            voters: voters.clone(),
        };
        if !proposal.keeps_proposer() {
            self.found.push(violation(
                Invariant::C2,
                format!("node {me} proposed {voters:?} for group {}", group.0),
            ));
        }
        let added: BTreeSet<NodeId> = voters
            .iter()
            .filter(|voter| {
                current.is_none_or(|current| !current.contains(*voter)) && removed.contains(voter)
            })
            .collect();
        if !added.is_empty() {
            self.found.push(violation(
                Invariant::C9,
                format!(
                    "node {me} made the removed nodes {added:?} voters of group {}",
                    group.0
                ),
            ));
        }
    }

    fn deliver_step(&mut self, group: RaftGroupId) {
        let Some(data) = self.groups.get_mut(&group) else {
            return;
        };
        // Every kept future resolves: the live leader's step runs, and the
        // others' fail, since their planners do not lead.
        let kept = std::mem::take(&mut data.kept);
        let refused = data.raft.as_mut().and_then(|raft| {
            let leader = raft.leader.filter(|leader| raft.alive.contains(leader))?;
            let step = kept.get(&leader)?;
            raft.execute(step).err()
        });
        self.refused(group, refused);
    }

    fn deliver_reports(&mut self) {
        if !self.meta.up {
            return;
        }
        for report in std::mem::take(&mut self.in_flight) {
            let _response = self.submit(report);
        }
    }

    // Seeding.

    /// The node's seed driver, which runs whether or not the group has
    /// initialized: only its probe keeps it from initializing again.
    fn seed(&mut self, me: NodeId, group: RaftGroupId) {
        if !self.alive(me) {
            return;
        }
        // Only a replica that never held the group initializes it.
        if !self
            .replica(me, group)
            .is_some_and(|replica| replica.open && replica.log == Journal::Empty)
        {
            return;
        }
        let local = self.view_of(me);
        let initializes = matches!(
            local.cluster().and_then(|cluster| cluster.group(group)),
            Some(GroupPlacement::Seeded { initializer, .. }) if *initializer == me
        );
        if !initializes {
            return;
        }
        let Some(stamp) = self.confirm(me) else {
            return;
        };
        let Ok(confirmed) = ConfirmedView::new(self.view_of(me), stamp) else {
            return;
        };
        if seed_role(&confirmed, me, group) != Some(SeedRoleOf::Initializer) {
            return;
        }
        let Some(GroupPlacement::Seeded { voters, .. }) = confirmed
            .view()
            .cluster()
            .and_then(|cluster| cluster.group(group))
        else {
            return;
        };
        let reachable = self.reachable_replicas(group);
        let empty = voters.iter().all(|voter| {
            reachable.contains(&voter)
                && self
                    .replica(voter, group)
                    .is_some_and(|replica| replica.log == Journal::Empty)
        });
        if !empty {
            return;
        }
        self.initialize(me, group, &confirmed, Group::new(voters, me, reachable));
    }

    /// The seed driver's `Initialize`. C4: the confirmed view is in the
    /// current meta history, meta's latest state shows the group `Seeded`
    /// with `me` its initializer, and the group never initialized before.
    fn initialize(
        &mut self,
        me: NodeId,
        group: RaftGroupId,
        confirmed: &ConfirmedView,
        raft: Group,
    ) {
        let mut found = Vec::new();
        if !self.in_current_history(confirmed) {
            found.push(format!(
                "node {me} initialized group {} from a view not confirmed in the current meta \
                 history",
                group.0
            ));
        }
        let latest = self.placement(group);
        if !matches!(
            latest,
            Some(GroupPlacement::Seeded { initializer, .. }) if initializer == me
        ) {
            found.push(format!(
                "node {me} initialized group {} while meta's latest state places it {latest:?}",
                group.0
            ));
        }
        if let Some(data) = self.groups.get_mut(&group) {
            data.initializations = data.initializations.saturating_add(1);
            if data.initializations > 1 {
                found.push(format!(
                    "node {me} initialized group {} a second time: {} initializations",
                    group.0, data.initializations
                ));
            }
            data.raft = Some(raft);
        }
        self.found.extend(
            found
                .into_iter()
                .map(|detail| violation(Invariant::C4, detail)),
        );
    }

    // Drivers and operators.

    fn decommission(&mut self) {
        if !self.meta.up {
            return;
        }
        let Some(cluster) = self.meta.state().cluster() else {
            return;
        };
        // A node that is not live has no entry.
        let eligibility: BTreeMap<NodeId, NodeEligibility> = cluster
            .nodes()
            .keys()
            .filter(|node| self.reaches(**node))
            .map(|node| (*node, NodeEligibility::Eligible))
            .collect();
        let plan = plan_decommission(cluster, &eligibility);
        for planned in plan.moves {
            let _response = self.submit(ControlCommand::RequestMove {
                group: planned.group,
                expected: planned.expected,
                to: planned.to,
                now_ms: 0,
            });
        }
    }

    fn sweep(&mut self) {
        if !self.meta.up {
            return;
        }
        for node in self.removed_nodes() {
            let groups: Vec<RaftGroupId> = self
                .nodes
                .get(&node)
                .map(|state| state.objects.iter().copied().collect())
                .unwrap_or_default();
            for group in groups {
                let not_member =
                    self.answer_about(node, group) == Some(MembershipAnswer::NotMember);
                if not_member || !enforced(Rule::SweepAnswer) {
                    if let Some(state) = self.nodes.get_mut(&node) {
                        state.objects.remove(&group);
                    }
                    self.swept.insert((group, node));
                    self.tally.sweeps = self.tally.sweeps.saturating_add(1);
                }
            }
        }
    }

    // Faults.

    fn crash(&mut self, node: NodeId) {
        let Some(state) = self.nodes.get_mut(&node) else {
            return;
        };
        state.alive = false;
        for replica in state.replicas.values_mut() {
            replica.open = false;
            replica.teardown = None;
        }
        for data in self.groups.values_mut() {
            data.deposed.remove(&node);
            data.kept.remove(&node);
        }
    }

    fn restart(&mut self, node: NodeId, at: ViewAt) {
        if self.alive(node) {
            return;
        }
        let view = self.meta_view(at);
        if let Some(state) = self.nodes.get_mut(&node) {
            state.alive = true;
            state.partitioned = false;
            state.removed = false;
            state.lagging = at != ViewAt::Latest;
            state.view = view;
        }
        let answer = self.submit(ControlCommand::RegisterNode { node, now_ms: 0 });
        if let Some(Err(ControlError::NodeRemoved { .. })) = answer
            && let Some(state) = self.nodes.get_mut(&node)
        {
            state.removed = true;
        }
    }

    fn partition(&mut self, node: NodeId) {
        if let Some(state) = self.nodes.get_mut(&node) {
            state.partitioned = true;
        }
        for data in self.groups.values_mut() {
            if let Some(raft) = data.raft.as_ref()
                && raft.leader == Some(node)
                && raft.alive.contains(&node)
                && !data.deposed.contains_key(&node)
            {
                let mut copy = raft.clone();
                copy.alive = BTreeSet::from([node]);
                data.deposed.insert(node, copy);
            }
        }
    }

    fn heal(&mut self, node: NodeId) {
        if let Some(state) = self.nodes.get_mut(&node) {
            state.partitioned = false;
        }
        for data in self.groups.values_mut() {
            data.deposed.remove(&node);
        }
    }

    fn meta_plan(&mut self, keep: usize) {
        self.meta.plan(keep);
        self.meta.up = true;
        self.tally.meta_plans = self.tally.meta_plans.saturating_add(1);
        let removed = self.removed_nodes();
        self.swept.retain(|(_, node)| removed.contains(node));
        self.removed_at.retain(|node, _| removed.contains(node));
    }

    fn data_plan(&mut self, group: RaftGroupId, at: usize) {
        let floor = self.plan_floor(group);
        let in_flight: Vec<MembershipLogId> = self
            .in_flight
            .iter()
            .filter_map(|report| match report {
                ControlCommand::ReportConverged {
                    group: reported,
                    membership,
                    ..
                } if *reported == group => Some(*membership),
                ControlCommand::RegisterNode { .. }
                | ControlCommand::Bootstrap { .. }
                | ControlCommand::RequestMove { .. }
                | ControlCommand::ReportConverged { .. }
                | ControlCommand::SetNodeLifecycle { .. }
                | ControlCommand::SetMoveLimit { .. }
                | ControlCommand::ReportTornDown { .. } => None,
            })
            .collect();
        let Some(raft) = self.raft_mut(group) else {
            return;
        };
        let Some(last) = raft.committed_configs.len().checked_sub(1) else {
            return;
        };
        let at = at.min(last);
        let config = raft.committed_configs[at].1.clone();
        let cuts_a_report = in_flight.iter().any(|membership| {
            raft.committed_configs
                .iter()
                .position(|(id, _)| id == membership)
                .is_some_and(|position| position > at)
        });
        raft.recover_to(config);
        if let Some(data) = self.groups.get_mut(&group) {
            data.confirmation = None;
            data.kept.clear();
            data.deposed.clear();
        }
        if at < floor || cuts_a_report {
            self.uncertified.insert(group);
        }
        self.tally.data_plans = self.tally.data_plans.saturating_add(1);
    }

    // Bookkeeping after every action.

    /// Every group's live members are the nodes that reach the others with
    /// an open replica. A replica that comes back starts unmatched with
    /// its gate closed.
    fn sync(&mut self) {
        let groups: Vec<RaftGroupId> = self.groups.keys().copied().collect();
        for group in groups {
            let reachable = self.reachable_replicas(group);
            if let Some(raft) = self.raft_mut(group) {
                let gone: Vec<NodeId> = raft.alive.difference(&reachable).copied().collect();
                for node in gone {
                    raft.kill(node);
                }
                let back: Vec<NodeId> = reachable.difference(&raft.alive).copied().collect();
                for node in back {
                    raft.revive(node);
                }
            }
        }
    }

    /// A caught-up empty or reopened replica holds the group's data and
    /// snapshot objects, and a newly removed node is noted with each
    /// group's history length.
    fn observe(&mut self) {
        for node in self.removed_nodes() {
            if self.removed_at.contains_key(&node) {
                continue;
            }
            self.tally.removals = self.tally.removals.saturating_add(1);
            let starts: BTreeMap<RaftGroupId, usize> = self
                .groups
                .iter()
                .map(|(group, data)| {
                    let length = data
                        .raft
                        .as_ref()
                        .map_or(0, |raft| raft.committed_configs.len());
                    (*group, length)
                })
                .collect();
            self.removed_at.insert(node, starts);
        }
        let mut holding = Vec::new();
        for (group, data) in &self.groups {
            if let Some(raft) = &data.raft {
                for node in &raft.caught_up {
                    holding.push((*group, *node));
                }
            }
        }
        for (group, node) in holding {
            if let Some(state) = self.nodes.get_mut(&node)
                && let Some(replica) = state.replicas.get_mut(&group)
                && replica.open
                && matches!(replica.log, Journal::Empty | Journal::Recovering)
            {
                replica.log = Journal::Initialized;
                state.objects.insert(group);
            }
        }
    }

    // Invariants.

    fn check(&self) -> Result<(), Violation> {
        self.check_voters()?;
        self.check_records()?;
        self.check_sizes()?;
        self.check_removed()
    }

    /// C1: every voter of every config of every group's committed
    /// membership is a node that, unless crashed, holds its replica, not
    /// closed by its removal, and whose objects the sweep did not delete.
    fn check_voters(&self) -> Result<(), Violation> {
        for (group, data) in &self.groups {
            let Some(raft) = &data.raft else {
                continue;
            };
            for voter in raft.committed.voters() {
                let fail = |what: &str| {
                    Err(violation(
                        Invariant::C1,
                        format!(
                            "voter {voter} of group {} {what}: {:?}",
                            group.0, raft.committed
                        ),
                    ))
                };
                if self.swept.contains(&(*group, voter)) {
                    return fail("lost its snapshot objects to the sweep");
                }
                let Some(state) = self.nodes.get(&voter) else {
                    return fail("is no node");
                };
                if !state.alive {
                    continue;
                }
                match state.replicas.get(group) {
                    Some(replica) if replica.closed_by_removal => {
                        return fail("was closed by its node's removal");
                    }
                    Some(replica) if replica.log.is_held() => {}
                    Some(_) | None => {
                        if !self.uncertified.contains(group) {
                            return fail("holds no replica");
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// C3: every retirement's claim (6.2) holds: from the membership its
    /// converge certified on, no committed membership lists the node as a
    /// voter.
    fn check_records(&self) -> Result<(), Violation> {
        let Some(cluster) = self.meta.state().cluster() else {
            return Ok(());
        };
        for (group, records) in cluster.retirements() {
            if self.uncertified.contains(group) {
                continue;
            }
            let Some(raft) = self.raft(*group) else {
                continue;
            };
            for (node, record) in records {
                let fail = |what: String| {
                    Err(violation(
                        Invariant::C3,
                        format!("the retirement of node {node} in group {} {what}", group.0),
                    ))
                };
                let Some(membership) = self.certified.get(&(*group, record.created_at)) else {
                    return fail("has no certified membership".to_owned());
                };
                let Some(position) = raft
                    .committed_configs
                    .iter()
                    .position(|(id, _)| id == membership)
                else {
                    return fail(format!("names {membership}, which never committed"));
                };
                if let Some((id, config)) = raft
                    .committed_configs
                    .iter()
                    .skip(position)
                    .find(|(_, config)| config.is_voter(*node))
                {
                    return fail(format!(
                        "certified {membership}, but {id} lists it as a voter: {config:?}"
                    ));
                }
            }
        }
        Ok(())
    }

    /// C7: every committed config of every group has the replication
    /// factor's voters on each side.
    fn check_sizes(&self) -> Result<(), Violation> {
        for (group, data) in &self.groups {
            let Some(raft) = &data.raft else {
                continue;
            };
            for (id, config) in &raft.committed_configs {
                if config.voter_configs().any(|side| side.len() != self.rf) {
                    return Err(violation(
                        Invariant::C7,
                        format!(
                            "group {} committed {id} with a side of another size than {}: \
                             {config:?}",
                            group.0, self.rf
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    /// C9: a removed node has no role, and no membership committed after
    /// its removal makes it a voter.
    fn check_removed(&self) -> Result<(), Violation> {
        let Some(cluster) = self.meta.state().cluster() else {
            return Ok(());
        };
        for (node, starts) in &self.removed_at {
            if let Some((group, _)) = cluster
                .groups()
                .iter()
                .find(|(_, placement)| placement.role(*node).is_some())
            {
                return Err(violation(
                    Invariant::C9,
                    format!("removed node {node} has a role in group {}", group.0),
                ));
            }
            for (group, start) in starts {
                if self.uncertified.contains(group) {
                    continue;
                }
                let Some(raft) = self.raft(*group) else {
                    continue;
                };
                let earlier = raft.committed_configs.iter();
                let later = raft.committed_configs.iter().skip(1);
                for (position, ((_, before), (id, after))) in earlier.zip(later).enumerate() {
                    if position.saturating_add(1) >= *start
                        && !before.is_voter(*node)
                        && after.is_voter(*node)
                    {
                        return Err(violation(
                            Invariant::C9,
                            format!("group {} made removed node {node} a voter at {id}", group.0),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// C5, once the faults stopped and the runbook's moves were made: every
    /// group stable with its committed uniform voters equal to the target
    /// and no learner, every retirement reported, no node that is not
    /// removed holding a log without a role, no removed node holding a
    /// replica open, and every removed node swept.
    pub(super) fn quiescent(&self) -> Result<(), Violation> {
        let fail = |what: String| Err(violation(Invariant::C5, what));
        let Some(cluster) = self.meta.state().cluster() else {
            return fail("the cluster is not bootstrapped".to_owned());
        };
        for (group, placement) in cluster.groups().iter() {
            let GroupPlacement::Live {
                target: Target::Stable { voters },
                ..
            } = placement
            else {
                return fail(format!("group {} is {placement:?}", group.0));
            };
            let Some(raft) = self.raft(group) else {
                return fail(format!("group {} never initialized", group.0));
            };
            let settled = raft.pending().is_none()
                && matches!(
                    &raft.committed,
                    MembershipConfig::Uniform { voters: committed, learners }
                        if committed == voters && learners.is_empty()
                );
            if !settled {
                return fail(format!(
                    "group {} is stable on {voters:?} with the membership {:?}, pending {:?}",
                    group.0,
                    raft.committed,
                    raft.pending()
                ));
            }
        }
        if !cluster.retirements().is_empty() {
            return fail(format!(
                "retirements are not reported: {:?}",
                cluster.retirements()
            ));
        }
        for (node, state) in &self.nodes {
            let removed = cluster
                .node(*node)
                .is_some_and(|record| record.lifecycle == NodeLifecycle::Removed);
            for (group, replica) in &state.replicas {
                if removed && replica.open {
                    return fail(format!("removed node {node} holds group {} open", group.0));
                }
                if !removed && replica.log.is_held() && cluster.role(*group, *node).is_none() {
                    return fail(format!(
                        "node {node} holds a log of group {} without a role",
                        group.0
                    ));
                }
            }
            if removed && !state.objects.is_empty() {
                return fail(format!(
                    "removed node {node} still holds the objects of {:?}",
                    state.objects
                ));
            }
        }
        Ok(())
    }

    // Composite steps.

    /// One fair round of every actor: every node hosts, every seed driver
    /// and leader acts after its group settled, every teardown steps, and
    /// the decommission driver, the sweep and the reports in flight run.
    pub(super) fn round(&mut self) -> Result<(), Violation> {
        let nodes: Vec<NodeId> = self.nodes.keys().copied().collect();
        let groups: Vec<RaftGroupId> = self.groups.keys().copied().collect();
        for node in &nodes {
            self.step(Action::Host(*node))?;
        }
        for group in &groups {
            // The seed drivers run while meta places the group `Seeded`,
            // whether or not it initialized (a meta plan may seed it again).
            let seeded = matches!(self.placement(*group), Some(GroupPlacement::Seeded { .. }));
            if seeded {
                for node in &nodes {
                    self.step(Action::Seed(*node, *group))?;
                }
            }
            self.step(Action::Settle(*group))?;
            self.step(Action::DeliverStep(*group))?;
            if let Some(leader) = self.leader(*group) {
                self.step(Action::Reconcile {
                    node: leader,
                    group: *group,
                    hold: false,
                })?;
            }
        }
        for (node, group) in self.running_teardowns() {
            self.step(Action::Teardown(node, group))?;
        }
        self.step(Action::Decommission)?;
        self.step(Action::Sweep)?;
        self.step(Action::DeliverReports)
    }

    /// The runbook's moves after a recovery plan: a stable group whose
    /// committed voters differ from its placement moves to them,
    /// uncordoning any it needs.
    fn runbook(&mut self) -> Result<(), Violation> {
        let Some(cluster) = self.meta.state().cluster() else {
            return Ok(());
        };
        let mut commands = Vec::new();
        for (group, placement) in cluster.groups().iter() {
            let GroupPlacement::Live {
                since,
                target: Target::Stable { voters },
                ..
            } = placement
            else {
                continue;
            };
            let Some(committed) = self.committed_voters(group) else {
                continue;
            };
            if committed == *voters {
                continue;
            }
            for member in committed.iter() {
                let inactive = cluster
                    .node(member)
                    .is_some_and(|record| record.lifecycle != NodeLifecycle::Active);
                if inactive && !voters.contains(member) {
                    commands.push(ControlCommand::SetNodeLifecycle {
                        node: member,
                        change: LifecycleChange::Uncordon,
                        now_ms: 0,
                    });
                }
            }
            commands.push(ControlCommand::RequestMove {
                group,
                expected: *since,
                to: committed,
                now_ms: 0,
            });
        }
        for command in commands {
            self.step(Action::Command(command))?;
        }
        Ok(())
    }

    /// Faults stop: meta answers with its default move limit, partitions
    /// heal, crashed nodes and nodes misled about their removal restart
    /// (12.2's discard step), lagging views catch up, kept steps run or are
    /// dropped and every report in flight lands.
    pub(super) fn stop_faults(&mut self) -> Result<(), Violation> {
        self.step(Action::MetaAvailable(true))?;
        self.step(Action::Command(ControlCommand::SetMoveLimit {
            limit: MoveLimit::new(DEFAULT_MOVE_LIMIT),
        }))?;
        let removed = self.removed_nodes();
        let nodes: Vec<NodeId> = self.nodes.keys().copied().collect();
        for node in nodes {
            let misled = self
                .nodes
                .get(&node)
                .is_some_and(|state| state.removed && !removed.contains(&node));
            if self.partitioned(node) {
                self.step(Action::Heal(node))?;
            }
            if self.alive(node) && misled {
                self.step(Action::Crash(node))?;
            }
            if self.alive(node) {
                self.step(Action::View(node, ViewAt::Latest))?;
            } else {
                self.step(Action::Restart(node, ViewAt::Latest))?;
            }
        }
        let groups: Vec<RaftGroupId> = self.groups.keys().copied().collect();
        for group in groups {
            if let Some(raft) = self.raft_mut(group) {
                raft.drop_second_step = false;
                raft.delay_second_step = false;
            }
            self.step(Action::DeliverStep(group))?;
        }
        self.step(Action::DeliverReports)
    }

    /// Fair rounds with the runbook's moves until C5 holds, at most
    /// `rounds`.
    pub(super) fn quiesce(&mut self, rounds: usize) -> Result<(), Violation> {
        for _ in 0..rounds {
            if self.quiescent().is_ok() {
                return Ok(());
            }
            self.round()?;
            self.runbook()?;
        }
        self.quiescent()
    }

    /// Rounds of hosting on `hosts`, the group settling and its leader
    /// acting, until `done`, at most `rounds`.
    pub(super) fn drive(
        &mut self,
        group: RaftGroupId,
        hosts: &[u64],
        rounds: usize,
        done: impl Fn(&Self) -> bool,
    ) -> Result<(), Violation> {
        for _ in 0..rounds {
            if done(self) {
                return Ok(());
            }
            for host in hosts {
                self.step(Action::Host(node(*host)))?;
            }
            self.step(Action::Settle(group))?;
            if done(self) {
                return Ok(());
            }
            if let Some(leader) = self.leader(group) {
                self.step(Action::Reconcile {
                    node: leader,
                    group,
                    hold: false,
                })?;
            }
        }
        if done(self) {
            Ok(())
        } else {
            Err(violation(
                Invariant::Script,
                format!("group {} did not get there in {rounds} rounds", group.0),
            ))
        }
    }

    /// The node hosts and steps its teardown of the group until the log is
    /// retired and the retirement reported.
    pub(super) fn finish_teardown(&mut self, id: u64, group: RaftGroupId) -> Result<(), Violation> {
        let me = node(id);
        for _ in 0..TEARDOWN_STEPS {
            self.step(Action::Host(me))?;
            if self.torn_down(me, group) {
                return Ok(());
            }
            self.step(Action::Teardown(me, group))?;
        }
        Err(violation(
            Invariant::Script,
            format!("node {me} did not finish its teardown of group {}", group.0),
        ))
    }

    fn torn_down(&self, me: NodeId, group: RaftGroupId) -> bool {
        let reported = self
            .meta
            .state()
            .cluster()
            .is_some_and(|cluster| cluster.retirement(group, me).is_none());
        let gone = self
            .replica(me, group)
            .is_none_or(|replica| replica.log == Journal::Retired && replica.teardown.is_none());
        reported && gone
    }

    /// The operator moves the group to `to` from its current placement.
    pub(super) fn request_move(&mut self, group: RaftGroupId, to: &[u64]) -> Result<(), Violation> {
        let to = voters(to);
        let Some(expected) = self.placement(group).map(|placement| placement.since()) else {
            return Err(violation(
                Invariant::Script,
                format!("group {} has no placement", group.0),
            ));
        };
        self.step(Action::Command(ControlCommand::RequestMove {
            group,
            expected,
            to: to.clone(),
            now_ms: 0,
        }))?;
        let accepted = self.placement(group).is_some_and(|placement| {
            *placement.target_voters() == to && placement.since() != expected
        });
        if accepted {
            Ok(())
        } else {
            Err(violation(
                Invariant::Script,
                format!(
                    "meta refused the move of group {} to {to:?}: {:?}",
                    group.0,
                    self.placement(group)
                ),
            ))
        }
    }

    /// The node's view stays at meta's latest state from now on.
    pub(super) fn freeze(&mut self, id: u64) -> Result<(), Violation> {
        let end = self.meta.len().saturating_sub(1);
        self.step(Action::View(node(id), ViewAt::Entry(end)))
    }

    /// A scripted expectation.
    pub(super) fn ensure(&self, holds: bool, what: &str) -> Result<(), Violation> {
        if holds {
            Ok(())
        } else {
            Err(violation(Invariant::Script, what.to_owned()))
        }
    }

    /// The last actions and the state they left, for failure messages.
    pub(super) fn history(&self) -> String {
        let start = self.log.len().saturating_sub(HISTORY);
        let actions: Vec<String> = self
            .log
            .iter()
            .enumerate()
            .skip(start)
            .map(|(index, action)| format!("{index}: {action:?}"))
            .collect();
        let groups: Vec<String> = self
            .groups
            .iter()
            .map(|(group, data)| {
                format!(
                    "group {}: placement {:?}, membership {:?}",
                    group.0,
                    self.placement(*group),
                    data.raft
                        .as_ref()
                        .map(|raft| (&raft.committed, raft.leader))
                )
            })
            .collect();
        format!("{}\n{}", actions.join("\n"), groups.join("\n"))
    }
}

/// Whether the view shows `node` removed.
fn shows_removed(view: &PlacementView, node: NodeId) -> bool {
    view.cluster()
        .and_then(|cluster| cluster.node(node))
        .is_some_and(|record| record.lifecycle == NodeLifecycle::Removed)
}
