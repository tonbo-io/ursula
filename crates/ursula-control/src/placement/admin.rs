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
use ursula_proto::admin::required_option;
use ursula_proto::placement::GenesisClaim;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::VoterSet;
use ursula_proto::placement::WriteEffect;
use ursula_shard::RaftGroupId;

use super::command::ControlError;
use super::decommission::BlockedMove;
use super::decommission::NodeEligibility;
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
    /// Meta formed and nodes register: the registered ids, and the
    /// `raft.peers` ids that have not registered yet.
    Registering {
        registered: BTreeSet<NodeId>,
        waiting_for: BTreeSet<NodeId>,
    },
    /// A meta recovery plan ended the formation.
    FormationCut,
    Bootstrapped(Cluster),
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
    /// The groups whose snapshot objects the removed-node sweep has not
    /// deleted yet, by removed node.
    pub sweep_pending: BTreeMap<NodeId, BTreeSet<RaftGroupId>>,
    /// Why the meta leader's last `Bootstrap` was refused, if it was.
    #[serde(deserialize_with = "required_option")]
    pub last_bootstrap_refusal: Option<ControlError>,
    /// The age of the oldest move in progress (display only).
    #[serde(deserialize_with = "required_option")]
    pub oldest_move_age_ms: Option<u64>,
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

/// `POST /__ursula/placement/groups/{group}/move`: move the group to `to`
/// when its placement is still at `expected`. A cancel names the move's
/// source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveRequest {
    pub expected: PlacementEpoch,
    pub to: VoterSet,
}

/// `POST /__ursula/placement/move-limit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveLimitRequest {
    pub limit: MoveLimit,
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
