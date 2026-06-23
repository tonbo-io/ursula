use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::RaftGroupId;

use crate::model::NodeId;
use crate::model::NodeState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementNode {
    pub node_id: NodeId,
    pub client_url: String,
    pub cluster_url: String,
    pub state: NodeState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupPlacementView {
    pub raft_group_id: RaftGroupId,
    pub voters: BTreeSet<NodeId>,
    pub learners: BTreeSet<NodeId>,
    pub draining: BTreeSet<NodeId>,
    pub epoch: u64,
    pub nodes: BTreeMap<NodeId, PlacementNode>,
}

impl GroupPlacementView {
    pub fn hosts(&self, node_id: NodeId) -> bool {
        self.voters.contains(&node_id) || self.learners.contains(&node_id)
    }

    pub fn serves_client_traffic(&self, node_id: NodeId) -> bool {
        self.voters.contains(&node_id)
            && !self.draining.contains(&node_id)
            && self
                .nodes
                .get(&node_id)
                .is_some_and(|node| node.state == NodeState::Active)
    }

    pub fn cluster_endpoints(&self) -> BTreeMap<NodeId, String> {
        self.nodes
            .iter()
            .filter(|(node_id, _)| self.hosts(**node_id))
            .map(|(node_id, node)| (*node_id, node.cluster_url.clone()))
            .collect()
    }

    pub fn active_voter_client_url(&self, exclude: Option<NodeId>) -> Option<(NodeId, String)> {
        let mut fallback: Option<(NodeId, String)> = None;
        for node_id in &self.voters {
            if !self.serves_client_traffic(*node_id) {
                continue;
            }
            let Some(node) = self.nodes.get(node_id) else {
                continue;
            };
            let candidate = (*node_id, node.client_url.clone());
            if Some(*node_id) == exclude {
                fallback.get_or_insert(candidate);
            } else {
                return Some(candidate);
            }
        }
        fallback
    }
}
