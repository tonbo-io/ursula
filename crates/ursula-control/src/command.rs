use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::RaftGroupId;

use crate::OperationId;
use crate::model::NodeId;
use crate::model::NodeState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlCommand {
    RegisterNode {
        node_id: NodeId,
        client_url: String,
        cluster_url: String,
        #[serde(default)]
        labels: BTreeMap<String, String>,
        now_ms: u64,
    },
    SetNodeState {
        node_id: NodeId,
        state: NodeState,
        now_ms: u64,
    },
    SeedPlacement {
        raft_group_id: RaftGroupId,
        voters: BTreeSet<NodeId>,
        now_ms: u64,
    },
    Operation {
        command: crate::OperationCommand,
        now_ms: u64,
    },
}

impl ControlCommand {
    pub fn now_ms(&self) -> u64 {
        match self {
            Self::RegisterNode { now_ms, .. }
            | Self::SetNodeState { now_ms, .. }
            | Self::SeedPlacement { now_ms, .. }
            | Self::Operation { now_ms, .. } => *now_ms,
        }
    }
}

impl fmt::Display for ControlCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::RegisterNode { .. } => "register_node",
            Self::SetNodeState { .. } => "set_node_state",
            Self::SeedPlacement { .. } => "seed_placement",
            Self::Operation { .. } => "operation",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlResponse {
    Ok,
    Operation(Result<crate::OperationOutcome, crate::OperationError>),
    Rejected { reason: ControlError },
}

impl ControlResponse {
    pub fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected { .. } | Self::Operation(Err(_)))
    }
}

impl fmt::Display for ControlResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Ok => "ok",
            Self::Operation(_) => "operation",
            Self::Rejected { .. } => "rejected",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeEndpoint {
    Client,
    Cluster,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum ControlError {
    #[error("node {node_id} has an empty {endpoint:?} address")]
    EmptyAddress {
        node_id: NodeId,
        endpoint: NodeEndpoint,
    },
    #[error("node {node_id} is not registered")]
    UnknownNode { node_id: NodeId },
    #[error("node {node_id} has been removed")]
    RemovedNode { node_id: NodeId },
    #[error("node {node_id} removal requires the decommission operation")]
    RemovalRequiresOperation { node_id: NodeId },
    #[error("node {node_id} is not eligible: {state:?}")]
    IneligibleNode { node_id: NodeId, state: NodeState },
    #[error("group {raft_group_id:?} requires at least one voter")]
    EmptyVoters { raft_group_id: RaftGroupId },
    #[error("group {raft_group_id:?} already has a placement")]
    PlacementExists { raft_group_id: RaftGroupId },
    /// Only an address refresh of a registered node is accepted while an
    /// operation is active.
    #[error("maintenance operation {operation_id:?} is active")]
    OperationActive { operation_id: OperationId },
}
