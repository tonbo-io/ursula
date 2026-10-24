//! Admin JSON types of the placement routes, shared by the server and
//! `ursulactl`.
//!
//! `ursulactl` reads the kernel's own types ([`Cluster`], [`GroupPlacement`],
//! [`ControlError`]) through these, so no rule or shape is mirrored. They are
//! the admin protocol, compatible within a minor, and not part of the meta
//! log. They follow the admin JSON rules of `ursula_proto::placement`: the
//! planner enums they carry (`NodeEligibility`, `BlockReason`) decode an
//! unknown variant to `Unknown`, and an `Option` field is required, also
//! when `null`.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_proto::admin::RaftGroupMetrics;
use ursula_proto::admin::required_option;
use ursula_proto::placement::ClusterStage;
use ursula_proto::placement::GenesisClaim;
use ursula_proto::placement::MetaLogId;
pub use ursula_proto::placement::MetaRole;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::Registered;
use ursula_proto::placement::RootId;
use ursula_proto::placement::RootSlot;
use ursula_proto::placement::VoterSet;
use ursula_proto::placement::WriteEffect;
use ursula_shard::RaftGroupId;

use super::command::ControlError;
use super::decommission::BlockedMove;
use super::decommission::NodeEligibility;
use super::hosting::Standing;
use super::state::Cluster;
use super::state::GroupPlacement;
use super::state::MoveLimit;

/// What a node knows of the cluster's placement, one variant per stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterView {
    /// The node's meta replica applied nothing and no peer it probed holds
    /// meta history: meta has not formed. `unanswered` are `raft.peers` ids
    /// whose probe answer has not arrived or was not empty, and
    /// `genesis_mismatch` peers whose genesis claim differs from this
    /// node's.
    MetaGenesis {
        unanswered: BTreeSet<NodeId>,
        genesis_mismatch: BTreeMap<NodeId, GenesisClaim>,
    },
    /// Meta formed elsewhere and this node's replica applied nothing yet.
    MetaCatchingUp,
    /// Meta formed and nodes register: the registered ids with the root
    /// each id is bound to, and the `raft.peers` ids that have not
    /// registered yet.
    Registering {
        registered: BTreeMap<NodeId, RootSlot>,
        waiting_for: BTreeSet<NodeId>,
    },
    /// A meta recovery plan ended the formation.
    FormationCut,
    Bootstrapped(Cluster),
}

impl ClusterView {
    /// The view's stage, without its details.
    pub fn stage(&self) -> ClusterStage {
        match self {
            Self::MetaGenesis { .. } => ClusterStage::MetaGenesis,
            Self::MetaCatchingUp => ClusterStage::MetaCatchingUp,
            Self::Registering { .. } => ClusterStage::Registering,
            Self::FormationCut => ClusterStage::FormationCut,
            Self::Bootstrapped(_) => ClusterStage::Bootstrapped,
        }
    }
}

/// What only the meta leader knows, filled by the meta leader alone. A
/// node's placement status names the meta leader, so a client reads this
/// report from that node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaLeaderReport {
    pub leader: NodeId,
    /// When this node became the meta leader (display only).
    pub leader_since_ms: u64,
    /// Every node's eligibility as a move target. A node it leaves out is
    /// not live.
    pub eligibility: BTreeMap<NodeId, NodeEligibility>,
    /// The decommissioning nodes' groups the last decommission pass could
    /// not move.
    pub decommission_blocked: Vec<BlockedMove>,
    /// What each removed node's removal still waits for, as the last
    /// removal-completion pass found it. A node removed after that pass is
    /// absent until the next pass, and a node whose removal completed is
    /// `vacant` and absent.
    pub removal_pending: BTreeMap<NodeId, PendingRemoval>,
    /// Why the meta leader's last `Bootstrap` was refused, if it was.
    #[serde(deserialize_with = "required_option")]
    pub last_bootstrap_refusal: Option<ControlError>,
    /// The age of the oldest move in progress (display only).
    #[serde(deserialize_with = "required_option")]
    pub oldest_move_age_ms: Option<u64>,
}

/// What a removed node's removal still waits for before the meta leader
/// marks its id `vacant` (`ControlCommand::ReportRemovalComplete`). The
/// removal completes once every field says so: every group confirmed,
/// the directory swept, the meta memberships free of the node and no
/// process of the removed root answering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingRemoval {
    /// The root of the removed life.
    pub root: RootId,
    /// The groups whose live leader has not answered, in this meta
    /// leadership, that the node is no member of them: no live leader
    /// answered (the group is down or lost its quorum), or the answer still
    /// counts the node.
    pub unconfirmed: BTreeSet<RaftGroupId>,
    /// The node's snapshot directory.
    pub directory: RemovalDirectory,
    /// Whether the meta group's committed or effective membership still
    /// lists the node.
    pub meta_member: bool,
    /// Whether a process of the removed root still answers at the node's
    /// `raft.peers` address: stop it for the removal to complete.
    pub process_answers: bool,
}

impl PendingRemoval {
    /// Whether nothing holds the removal any more.
    pub fn is_complete(&self) -> bool {
        self.unconfirmed.is_empty()
            && self.directory == RemovalDirectory::Swept
            && !self.meta_member
            && !self.process_answers
    }
}

/// A removed node's snapshot directory, as the last pass found it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemovalDirectory {
    /// The directory did not list.
    Unlisted,
    /// The directory still holds the node's objects of these groups.
    Unswept(BTreeSet<RaftGroupId>),
    /// The directory holds no object of the node.
    Swept,
}

/// Why this node runs no meta replica in this run, while it keeps its meta
/// directory for the operator (design 12.2). Closed: a reader of the same
/// minor knows every variant, and a new one comes only with a new minor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetaDamageKind {
    /// The meta journal could not be read or opened at start.
    Journal,
    /// The meta snapshot record's pointer is not the meta group's own.
    Pointer,
    /// The meta snapshot record did not restore against the meta log.
    Restore,
    /// The meta Raft or its recovery gate did not start.
    Start,
    /// The commit screen refused, for good, what the current meta leader
    /// sent: this replica holds committed entries the group no longer has.
    CommitScreen,
}

/// `GET /__ursula/placement`: what this node knows of the placement and
/// how its meta replica runs. With `?confirmed=true` (admin listener only)
/// the view is one a meta read barrier confirmed current.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementStatus {
    pub node: NodeId,
    pub view: ClusterView,
    /// The last meta entry the view includes, `null` for an empty view.
    #[serde(deserialize_with = "required_option")]
    pub applied: Option<MetaLogId>,
    pub meta: MetaReplicaStatus,
    pub registration: RegistrationStatus,
    /// Why the last `Bootstrap` this node submitted was refused, if it was.
    #[serde(deserialize_with = "required_option")]
    pub last_bootstrap_refusal: Option<ControlError>,
    /// The meta leader's report, when this node leads the meta group with
    /// a settled history, else `null`.
    #[serde(deserialize_with = "required_option")]
    pub leader_report: Option<MetaLeaderReport>,
}

/// How this node's meta replica runs. Closed, like [`MetaDamageKind`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetaReplicaStatus {
    /// The replica runs, with this node's role in meta's effective
    /// membership and the meta leader it knows.
    Running {
        role: MetaRole,
        #[serde(deserialize_with = "required_option")]
        leader: Option<NodeId>,
    },
    /// No meta replica runs in this run, for `kind`. `message` is the
    /// rendered error.
    Damaged {
        kind: MetaDamageKind,
        message: String,
    },
    /// No replica is published at the moment: a recovery plan restarts it,
    /// or the node stops.
    Absent,
}

/// Where this node's registration with the meta group stands in this run.
/// Closed, like [`MetaDamageKind`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistrationStatus {
    /// No answer yet.
    Pending,
    /// The meta group's last answer.
    Answered(Registered),
    /// The meta group binds this node id to `slot`'s root, not to the
    /// unbound root this node runs on: the node's data replicas grant no
    /// vote and keep their recovery gates closed, and the node registers
    /// again until an operator releases that root
    /// (`ursulactl node replace-disk`).
    RootRefused { slot: RootSlot },
    /// The meta group refused the unbound root this node runs on because
    /// its node id is removed and the removal has not completed: the node's
    /// data replicas grant no vote and keep their recovery gates closed, and
    /// the node registers again until the meta leader marks the id vacant,
    /// then binds with no restart.
    RemovalPending,
    /// The meta group refused the node for good: it registers again only
    /// after a restart.
    Refused(ControlError),
    /// The meta group refused the node as removed: it treats itself as
    /// removed until it restarts.
    Removed,
}

impl RegistrationStatus {
    /// What the answers say of the node's standing: removed only after a
    /// `NodeRemoved` refusal.
    pub fn standing(&self) -> Standing {
        match self {
            Self::Removed => Standing::Removed,
            Self::Pending
            | Self::Answered(_)
            | Self::RootRefused { .. }
            | Self::RemovalPending
            | Self::Refused(_) => Standing::Member,
        }
    }
}

/// `POST /__ursula/placement/groups/{group}/move`: move the group to `to`
/// when its placement is still at `expected`. A cancel names the move's
/// source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveRequest {
    pub expected: PlacementEpoch,
    pub to: VoterSet,
}

/// The answer to an accepted move: the group's new placement key, which a
/// later move or cancel of the group names as `expected`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveResponse {
    pub since: PlacementEpoch,
}

/// `GET /__ursula/placement/groups/{group}`: the group's placement in the
/// node's view, confirmed by a meta read barrier with `?confirmed=true`
/// (admin listener only), and the node's replica of the group as its
/// metrics show it: its membership, its leader and, under `reconcile`, what
/// its membership reconcile shell does, the step a leader takes toward the
/// placement or why it waits. `replica` is `null` when the node hosts no
/// replica of the group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementGroupStatus {
    pub node: NodeId,
    pub placement: GroupPlacement,
    #[serde(deserialize_with = "required_option")]
    pub replica: Option<RaftGroupMetrics>,
}

/// `POST /__ursula/placement/nodes/{node}/replace-disk`: release the root
/// the node id is bound to when it is still `expect_root`, once that root's
/// disk is revoked. The answer is the node's record with its released root.
/// A root released never binds again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplaceDiskRequest {
    pub expect_root: RootId,
}

/// `POST /__ursula/placement/move-limit`: set the most groups that may
/// move at once ([`MoveLimit`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveLimitRequest {
    pub limit: MoveLimit,
}

/// The answer to `POST /__ursula/placement/move-limit`: the limit the meta
/// group applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveLimitResponse {
    pub limit: MoveLimit,
}

/// The answer to `POST /__ursula/placement/nodes/{node}/cordon`,
/// `.../uncordon` and `.../decommission`, which carry no body: the node's
/// lifecycle after the change. A decommission answers `removed` when the
/// node had no role left.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleResponse {
    pub lifecycle: NodeLifecycle,
}

/// Why a placement write did not answer with its result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementApiReason {
    /// The meta state machine refused the command.
    Refused(ControlError),
    /// No meta leader is known.
    NoLeader,
    /// The forward to the meta leader failed.
    ForwardFailed { leader: NodeId },
    /// The meta leader answered with an engine error.
    LeaderRefused { leader: NodeId },
    /// The local meta replica stopped.
    Stopped,
    /// A meta read barrier completed, and the local replica did not apply
    /// up to it in time, or its view is in another meta history.
    Unconfirmed,
}

/// Every refusal of a placement route, with whether the write may have
/// applied and the group's placement as the answering node sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementApiError {
    pub reason: PlacementApiReason,
    pub write_effect: WriteEffect,
    #[serde(deserialize_with = "required_option")]
    pub current: Option<GroupPlacement>,
    /// The rendered source chain.
    pub message: String,
}
