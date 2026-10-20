//! The meta group's commands and their typed answers.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_proto::placement::MembershipLogId;
use ursula_proto::placement::MetaLogId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::NodeSet;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::Registered;
use ursula_proto::placement::VoterSet;
use ursula_shard::RaftGroupId;

use super::state::ClusterParams;
use super::state::GroupPlacement;
use super::state::MoveLimit;

/// A command the meta group replicates. `now_ms` is display only: no
/// decision reads it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlCommand {
    /// A node announces itself, at every start.
    RegisterNode {
        node: NodeId,
        now_ms: u64,
    },
    /// Places every group over `nodes`, once.
    Bootstrap {
        params: ClusterParams,
        nodes: NodeSet,
        now_ms: u64,
    },
    /// Starts, retargets or cancels a move of `group` to `to`, when its
    /// placement is still at `expected`. A cancel is a move whose `to` is the
    /// move's source.
    RequestMove {
        group: RaftGroupId,
        expected: PlacementEpoch,
        to: VoterSet,
        now_ms: u64,
    },
    /// A proven leader of `group` reports that its committed uniform
    /// membership at `membership` has `voters`, while the placement it
    /// pursued was at `since`.
    ReportConverged {
        group: RaftGroupId,
        since: PlacementEpoch,
        voters: VoterSet,
        membership: MembershipLogId,
    },
    SetNodeLifecycle {
        node: NodeId,
        change: LifecycleChange,
        now_ms: u64,
    },
    SetMoveLimit {
        limit: MoveLimit,
    },
    /// `node` tore down its replica of `group` that the retirement created at
    /// `created_at` named.
    ReportTornDown {
        group: RaftGroupId,
        node: NodeId,
        created_at: MetaLogId,
    },
}

impl ControlCommand {
    /// The group the command names, if any.
    pub fn group(&self) -> Option<RaftGroupId> {
        match self {
            Self::RequestMove { group, .. }
            | Self::ReportConverged { group, .. }
            | Self::ReportTornDown { group, .. } => Some(*group),
            Self::RegisterNode { .. }
            | Self::Bootstrap { .. }
            | Self::SetNodeLifecycle { .. }
            | Self::SetMoveLimit { .. } => None,
        }
    }
}

/// An operator's lifecycle change of a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleChange {
    /// `Active` to `Cordoned`.
    Cordon,
    /// `Cordoned` or `Decommissioning` to `Active`.
    Uncordon,
    /// `Active` or `Cordoned` to `Decommissioning`, and to `Removed` once
    /// the node has no role.
    Decommission,
}

/// What began a meta history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryCause {
    /// The meta group's first leader.
    Genesis,
    /// An accepted recovery plan that may have cut the meta log.
    Plan,
}

/// What a history event did to the state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryBegun {
    /// The cluster is still forming.
    Forming,
    /// The formation ended without a `Bootstrap`, by this event or an
    /// earlier one.
    FormationCut,
    /// Every placement's key moved to the event, and these retirements were
    /// dropped.
    Rebased {
        dropped: BTreeMap<RaftGroupId, BTreeSet<NodeId>>,
    },
}

/// The answer to a command: what it did, or why the state machine refused
/// it, in which case the state is unchanged.
pub type ControlResponse = Result<ControlApplied, ControlError>;

/// What an accepted command did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlApplied {
    NodeRegistered(Registered),
    Bootstrapped,
    MoveAccepted {
        since: PlacementEpoch,
    },
    /// The group is stable at `since`. `retired` owe a teardown, and
    /// `removed` were decommissioning and lost their last role.
    Converged {
        since: PlacementEpoch,
        retired: BTreeSet<NodeId>,
        removed: BTreeSet<NodeId>,
    },
    /// The node's lifecycle after the change.
    LifecycleChanged {
        lifecycle: NodeLifecycle,
    },
    MoveLimitSet {
        limit: MoveLimit,
    },
    TornDown,
    /// No retirement matched the report: it was reported, dropped, or is a
    /// newer one.
    NothingToTearDown,
}

/// Why the state machine refused a command. The first failing check is the
/// error, and a refused command changes nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum ControlError {
    #[error("the cluster has not bootstrapped")]
    NotBootstrapped,
    #[error("the cluster has already bootstrapped")]
    AlreadyBootstrapped,
    #[error(
        "a meta recovery plan ended the cluster's formation: it never bootstraps, restore it into \
         a new cluster"
    )]
    FormationCut,
    #[error("invalid bootstrap: {problem}")]
    InvalidBootstrap { problem: BootstrapProblem },
    #[error("group {} does not exist", .group.0)]
    UnknownGroup { group: RaftGroupId },
    #[error("node {node} is not registered")]
    UnknownNode { node: NodeId },
    #[error("node {node} is removed")]
    NodeRemoved { node: NodeId },
    #[error("the placement of group {} is at {}", .group.0, .current.since())]
    StalePlacement {
        group: RaftGroupId,
        current: Box<GroupPlacement>,
    },
    #[error("group {} has not formed yet", .group.0)]
    GroupNotLive { group: RaftGroupId },
    #[error("group {} is not moving", .group.0)]
    NotMoving { group: RaftGroupId },
    #[error("group {} already targets these voters", .group.0)]
    NoChange { group: RaftGroupId },
    #[error(
        "group {} has {current} voters and the request names {requested}: a move keeps the \
         replication factor",
        .group.0
    )]
    ReplicationFactorChange {
        group: RaftGroupId,
        current: u8,
        requested: u8,
    },
    #[error("node {node} is {lifecycle:?} and cannot join group {}", .group.0)]
    IneligibleTarget {
        group: RaftGroupId,
        node: NodeId,
        lifecycle: NodeLifecycle,
    },
    #[error("node {node} has not reported the teardown of its earlier replica of group {}", .group.0)]
    RetirementPending { group: RaftGroupId, node: NodeId },
    #[error("{moving} groups are moving and the move limit is {limit}")]
    MoveLimit { limit: u32, moving: u32 },
    #[error("group {} does not target the reported voters", .group.0)]
    NotTarget {
        group: RaftGroupId,
        reported: VoterSet,
    },
    #[error("node {node} is a meta voter, which is replaced under its id, never decommissioned")]
    MetaVoter { node: NodeId },
    #[error("the startup reservation changes placement in this cluster")]
    ReservationAuthority,
    #[error("node {node} is decommissioning: uncordon it first")]
    Decommissioning { node: NodeId },
}

/// Why a `Bootstrap` is invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapProblem {
    #[error("meta voter {node} is not one of the nodes")]
    MetaVoterNotANode { node: NodeId },
    #[error("the replication factor exceeds the node count")]
    ReplicationFactorAboveNodes,
    #[error(
        "the startup reservation needs nodes and meta voters 1, 2 and 3 and replication factor 3"
    )]
    ReservationTopology,
}
