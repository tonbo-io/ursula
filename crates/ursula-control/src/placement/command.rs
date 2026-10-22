//! The meta group's commands and their typed answers.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_proto::placement::GroupCount;
use ursula_proto::placement::MAX_GROUP_COUNT;
use ursula_proto::placement::MembershipLogId;
use ursula_proto::placement::MetaLogId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::NodeRecord;
use ursula_proto::placement::NodeSet;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::Registered;
use ursula_proto::placement::RootId;
use ursula_proto::placement::RootSlot;
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
    /// A node announces itself and the root it runs on, at every start.
    /// `empty` says that the meta group never bound the root, as its WAL
    /// identity file records: its data replicas took part in nothing, so it
    /// holds nothing a bound process left, whatever its meta replica holds.
    /// Only an empty root takes a released slot.
    RegisterNode {
        node: NodeId,
        root: RootId,
        empty: bool,
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
    /// pursued was at `since`. `now_ms` dates the removals it causes.
    ReportConverged {
        group: RaftGroupId,
        since: PlacementEpoch,
        voters: VoterSet,
        membership: MembershipLogId,
        now_ms: u64,
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
    /// An operator releases `root`, the root `node` is bound to, once its
    /// disk is revoked (`replace-disk`): a compare-and-set on the slot.
    /// While the slot names it, the released root never binds again, and
    /// the release rule (its disk revoked) keeps a root released earlier
    /// out. It carries no time, since a slot change dates nothing.
    ReleaseRoot {
        node: NodeId,
        root: RootId,
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
            | Self::SetMoveLimit { .. }
            | Self::ReleaseRoot { .. } => None,
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
    /// The node's record with its released root. Before Bootstrap it is
    /// the record Bootstrap gives the node: `Active`, since its
    /// registration.
    RootReleased(NodeRecord),
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
    #[error("node {node} is {lifecycle} and cannot join group {}", .group.0)]
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
    #[error("node {node} is decommissioning: uncordon it first")]
    Decommissioning { node: NodeId },
    /// A registration of a root the node id is not bound to: another root
    /// holds the slot, the root was released, or a root that holds state
    /// met a released slot.
    #[error(
        "node {node} is not bound to this root, its slot is {slot}: an operator releases a root \
         with replace-disk once its disk is revoked"
    )]
    OtherRoot { node: NodeId, slot: RootSlot },
    /// A release of a root the node id is not bound to.
    #[error("node {node} is not bound to the root named, its slot is {slot}: read the slot again")]
    StaleRoot { node: NodeId, slot: RootSlot },
}

/// What a caller does after the state machine refused its command. Never
/// encoded: a process classifies a refusal it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Requeue {
    /// The same command may apply later, unchanged: retry it after a pause.
    Retry,
    /// The command was planned from a view the state has moved past:
    /// confirm a view and plan again from it.
    ConfirmView,
    /// The command never applies as it is: stop asking.
    Permanent,
}

impl ControlError {
    /// What a caller does after this refusal (see [`Requeue`]). One
    /// exhaustive match, so a new refusal names its class.
    pub fn requeue(&self) -> Requeue {
        match self {
            // An unbound root registers until an operator releases the slot.
            Self::NotBootstrapped
            | Self::RetirementPending { .. }
            | Self::MoveLimit { .. }
            | Self::OtherRoot { .. } => Requeue::Retry,
            Self::StalePlacement { .. }
            | Self::StaleRoot { .. }
            | Self::UnknownNode { .. }
            | Self::GroupNotLive { .. }
            | Self::NotMoving { .. }
            | Self::IneligibleTarget { .. } => Requeue::ConfirmView,
            Self::AlreadyBootstrapped
            | Self::FormationCut
            | Self::InvalidBootstrap { .. }
            | Self::UnknownGroup { .. }
            | Self::NodeRemoved { .. }
            | Self::NoChange { .. }
            | Self::ReplicationFactorChange { .. }
            | Self::NotTarget { .. }
            | Self::MetaVoter { .. }
            | Self::Decommissioning { .. } => Requeue::Permanent,
        }
    }
}

/// Why a `Bootstrap` is invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapProblem {
    #[error("{} groups exceed the bound of {}", .count.get(), MAX_GROUP_COUNT)]
    GroupCountAboveMax { count: GroupCount },
    #[error("meta voter {node} is not one of the nodes")]
    MetaVoterNotANode { node: NodeId },
    #[error("the replication factor exceeds the node count")]
    ReplicationFactorAboveNodes,
}
