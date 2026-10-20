//! The meta group's replicated control-plane state: desired placement only.
//!
//! A group's committed membership is the only truth of where it lives now,
//! and a node's journal the only truth of what it holds. This state says
//! where each group should live, the lifecycle of every node id ever
//! registered, and the retirements a converged move owes.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_proto::placement::GroupCount;
use ursula_proto::placement::MembershipLogId;
use ursula_proto::placement::MetaLogId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeRecord;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::ReplicaRole;
use ursula_proto::placement::ReplicationFactor;
use ursula_proto::placement::VoterSet;
use ursula_shard::RaftGroupId;

/// The meta group's state machine value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlPlaneState {
    /// Meta formed and nodes register. Nothing is placed.
    Forming {
        registered: BTreeMap<NodeId, Registration>,
    },
    /// A recovery plan's history event ended the formation, which may have
    /// lost a `Bootstrap` whose groups initialized: no `Bootstrap` is
    /// accepted again.
    FormationCut,
    /// Every group is placed.
    Bootstrapped(Cluster),
}

impl Default for ControlPlaneState {
    fn default() -> Self {
        Self::Forming {
            registered: BTreeMap::new(),
        }
    }
}

impl ControlPlaneState {
    /// The bootstrapped cluster, if any.
    pub fn cluster(&self) -> Option<&Cluster> {
        match self {
            Self::Bootstrapped(cluster) => Some(cluster),
            Self::Forming { .. } | Self::FormationCut => None,
        }
    }
}

/// A node registered before Bootstrap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Registration {
    pub since_ms: u64,
}

/// A bootstrapped cluster's desired state. Fields are read through accessors
/// and change only through the dispatcher.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cluster {
    pub(crate) params: ClusterParams,
    pub(crate) nodes: BTreeMap<NodeId, NodeRecord>,
    pub(crate) groups: Placements,
    /// Never beside a role of the same node in the same group.
    pub(crate) retirements: BTreeMap<RaftGroupId, BTreeMap<NodeId, Retirement>>,
}

impl Cluster {
    pub fn params(&self) -> &ClusterParams {
        &self.params
    }

    /// Every node id ever registered, with its lifecycle.
    pub fn nodes(&self) -> &BTreeMap<NodeId, NodeRecord> {
        &self.nodes
    }

    pub fn node(&self, node: NodeId) -> Option<&NodeRecord> {
        self.nodes.get(&node)
    }

    /// Every group's placement: exactly `group_count` of them.
    pub fn groups(&self) -> &Placements {
        &self.groups
    }

    pub fn group(&self, group: RaftGroupId) -> Option<&GroupPlacement> {
        self.groups.get(group)
    }

    /// The retirements a converged move owes, by group and then node.
    pub fn retirements(&self) -> &BTreeMap<RaftGroupId, BTreeMap<NodeId, Retirement>> {
        &self.retirements
    }

    pub fn retirement(&self, group: RaftGroupId, node: NodeId) -> Option<&Retirement> {
        self.retirements
            .get(&group)
            .and_then(|records| records.get(&node))
    }

    /// The node's role in the group, `None` when it has none.
    pub fn role(&self, group: RaftGroupId, node: NodeId) -> Option<ReplicaRole> {
        self.group(group).and_then(|placement| placement.role(node))
    }

    /// Whether the node has a role in any group.
    pub fn has_any_role(&self, node: NodeId) -> bool {
        self.groups
            .iter()
            .any(|(_, placement)| placement.role(node).is_some())
    }

    /// How many groups are moving.
    pub fn moving(&self) -> u32 {
        let moving = self
            .groups
            .iter()
            .filter(|(_, placement)| placement.moving().is_some())
            .count();
        u32::try_from(moving).unwrap_or(u32::MAX)
    }
}

/// Every group's placement, keyed by group id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Placements(pub(crate) BTreeMap<RaftGroupId, GroupPlacement>);

impl Placements {
    pub fn get(&self, group: RaftGroupId) -> Option<&GroupPlacement> {
        self.0.get(&group)
    }

    /// The placements in ascending group id.
    pub fn iter(&self) -> impl Iterator<Item = (RaftGroupId, &GroupPlacement)> + '_ {
        self.0.iter().map(|(group, placement)| (*group, placement))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// What a cluster fixed at Bootstrap, and its move limit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterParams {
    pub group_count: GroupCount,
    /// The effective replication factor, at most the node count at
    /// Bootstrap.
    pub replication_factor: ReplicationFactor,
    /// The meta group's committed voters at Bootstrap.
    pub meta_voters: VoterSet,
    pub max_concurrent_moves: MoveLimit,
    pub maintenance: MaintenanceAuthority,
}

/// Every group id of a cluster of `count` groups, ascending.
pub fn group_ids(count: GroupCount) -> impl Iterator<Item = RaftGroupId> {
    (0..count.get()).map(RaftGroupId)
}

/// How many groups may move at once. A limit below the moving count only
/// stops new moves, and zero stops them all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MoveLimit(u32);

impl MoveLimit {
    pub const fn new(limit: u32) -> Self {
        Self(limit)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Who changes placement: the meta group, or the startup reservation, under
/// which the state machine refuses moves and decommissions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceAuthority {
    Placement,
    Reservation,
}

/// Where a group should live.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupPlacement {
    /// The Bootstrap assignment: the only state in which the group may
    /// initialize, on its recorded initializer.
    Seeded {
        since: PlacementEpoch,
        voters: VoterSet,
        initializer: NodeId,
    },
    /// The group formed and reported its voters at least once.
    Live {
        /// The placement key.
        since: PlacementEpoch,
        /// The membership the last converge certified.
        converged_at: MembershipLogId,
        target: Target,
    },
}

impl GroupPlacement {
    /// The placement key every command acting on this placement names.
    pub fn since(&self) -> PlacementEpoch {
        match self {
            Self::Seeded { since, .. } | Self::Live { since, .. } => *since,
        }
    }

    /// The voters the group is placed on now: the seed or stable voters, or
    /// a move's source.
    pub fn current_voters(&self) -> &VoterSet {
        match self {
            Self::Seeded { voters, .. }
            | Self::Live {
                target: Target::Stable { voters },
                ..
            } => voters,
            Self::Live {
                target: Target::Moving(moving),
                ..
            } => &moving.from,
        }
    }

    /// The voters the group should end on: the seed or stable voters, or a
    /// move's target.
    pub fn target_voters(&self) -> &VoterSet {
        match self {
            Self::Seeded { voters, .. }
            | Self::Live {
                target: Target::Stable { voters },
                ..
            } => voters,
            Self::Live {
                target: Target::Moving(moving),
                ..
            } => &moving.to,
        }
    }

    /// The move in progress, if any.
    pub fn moving(&self) -> Option<&Move> {
        match self {
            Self::Live {
                target: Target::Moving(moving),
                ..
            } => Some(moving),
            Self::Seeded { .. }
            | Self::Live {
                target: Target::Stable { .. },
                ..
            } => None,
        }
    }

    /// The node's role in this group, `None` when it has none.
    pub fn role(&self, node: NodeId) -> Option<ReplicaRole> {
        match self {
            Self::Seeded { voters, .. }
            | Self::Live {
                target: Target::Stable { voters },
                ..
            } => voters.contains(node).then_some(ReplicaRole::Voter),
            Self::Live {
                target: Target::Moving(moving),
                ..
            } => moving.role(node),
        }
    }
}

/// A live group's target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Target {
    /// The group should stay on `voters`.
    Stable { voters: VoterSet },
    /// The group moves from `from` to `to`.
    Moving(Move),
}

/// A move in progress. `from` and `to` have the same size, and a cancel is
/// a move whose `to` is its `from`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Move {
    pub from: VoterSet,
    pub to: VoterSet,
    /// Earlier targets of this move in neither `from` nor `to`.
    pub abandoned: BTreeSet<NodeId>,
    /// When the move started (display only).
    pub since_ms: u64,
}

impl Move {
    /// The node's role in this move, `None` when it has none.
    pub fn role(&self, node: NodeId) -> Option<ReplicaRole> {
        match (self.from.contains(node), self.to.contains(node)) {
            (true, true) => Some(ReplicaRole::Voter),
            (false, true) => Some(ReplicaRole::Incoming),
            (true, false) => Some(ReplicaRole::Outgoing),
            (false, false) => self
                .abandoned
                .contains(&node)
                .then_some(ReplicaRole::Abandoned),
        }
    }
}

/// A replica a converged move retired: its node tears it down and reports.
/// While it exists the node cannot be placed in the group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Retirement {
    /// The converge entry that created it.
    pub created_at: MetaLogId,
}
