//! Admin JSON types of the placement routes, shared by the server and
//! `ursulactl`.
//!
//! `ursulactl` reads the kernel's own types ([`Cluster`], [`GroupPlacement`],
//! [`ControlError`]) through these, so no rule or shape is mirrored. They are
//! the admin protocol, compatible within a minor, and not part of the meta
//! log.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::VoterSet;
use ursula_proto::placement::WriteEffect;

use super::command::ControlError;
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
        genesis_mismatch: BTreeMap<NodeId, GenesisClaimView>,
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

/// The values a node proposes meta's genesis with: they must agree on every
/// node before meta initializes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenesisClaimView {
    pub voters: BTreeSet<NodeId>,
    /// The digest of the node's `raft.peers`, in lowercase hex.
    pub peers_digest: String,
    pub group_count: u32,
    pub replication_factor: u8,
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
    pub current: Option<GroupPlacement>,
    /// The rendered source chain.
    pub message: String,
}
