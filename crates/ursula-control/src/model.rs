use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::RaftGroupId;

pub type NodeId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Active,
    Draining,
    Disabled,
    Removed,
}

/// Nodes that are not `Removed`, with their lifecycle state, as the
/// operation kernel sees them.
pub(crate) type NodeStates = BTreeMap<NodeId, NodeState>;

impl NodeState {
    /// Whether the node may receive a new replica, as a seeded voter, a
    /// learner or a promoted voter. A node being drained may be in any state
    /// except `Removed`.
    pub fn accepts_new_replicas(self) -> bool {
        matches!(self, Self::Active)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterNode {
    pub node_id: NodeId,
    pub client_url: String,
    pub cluster_url: String,
    pub state: NodeState,
    pub registered_at_ms: u64,
    pub updated_at_ms: u64,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataGroupPlacement {
    pub raft_group_id: RaftGroupId,
    pub voters: BTreeSet<NodeId>,
    pub learners: BTreeSet<NodeId>,
    pub draining: BTreeSet<NodeId>,
    pub epoch: u64,
    pub updated_at_ms: u64,
}

impl DataGroupPlacement {
    pub fn empty(raft_group_id: RaftGroupId) -> Self {
        Self {
            raft_group_id,
            voters: BTreeSet::new(),
            learners: BTreeSet::new(),
            draining: BTreeSet::new(),
            epoch: 0,
            updated_at_ms: 0,
        }
    }

    pub fn hosts(&self, node_id: NodeId) -> bool {
        self.voters.contains(&node_id) || self.learners.contains(&node_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaConfig {
    pub initial_meta_voters: BTreeSet<NodeId>,
    pub default_replication_factor: u32,
    pub autopilot_enabled: bool,
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            initial_meta_voters: BTreeSet::new(),
            default_replication_factor: 3,
            autopilot_enabled: false,
        }
    }
}
