use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::RaftGroupId;

use crate::command::ControlCommand;
use crate::command::ControlResponse;
use crate::model::ClusterNode;
use crate::model::DataGroupPlacement;
use crate::model::MetaConfig;
use crate::model::NodeId;
use crate::model::NodeState;
use crate::view::GroupPlacementView;
use crate::view::PlacementNode;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlPlaneState {
    pub nodes: BTreeMap<NodeId, ClusterNode>,
    pub placements: BTreeMap<RaftGroupId, DataGroupPlacement>,
    pub operations: crate::OperationState,
    pub config: MetaConfig,
}

impl Default for ControlPlaneState {
    fn default() -> Self {
        Self::new(MetaConfig::default())
    }
}

impl ControlPlaneState {
    pub fn new(config: MetaConfig) -> Self {
        Self {
            nodes: BTreeMap::new(),
            placements: BTreeMap::new(),
            operations: crate::OperationState::default(),
            config,
        }
    }

    pub fn apply(&mut self, command: ControlCommand) -> ControlResponse {
        if let Some(operation) = &self.operations.active
            && !matches!(command, ControlCommand::Operation { .. })
            && !self.refreshes_addresses_only(&command)
        {
            return reject(crate::ControlError::OperationActive {
                operation_id: operation.token.operation_id,
            });
        }
        match command {
            ControlCommand::RegisterNode {
                node_id,
                client_url,
                cluster_url,
                labels,
                now_ms,
            } => self.register_node(node_id, client_url, cluster_url, labels, now_ms),
            ControlCommand::SetNodeState {
                node_id,
                state,
                now_ms,
            } => self.set_node_state(node_id, state, now_ms),
            ControlCommand::SeedPlacement {
                raft_group_id,
                voters,
                now_ms,
            } => self.seed_placement(raft_group_id, voters, now_ms),
            ControlCommand::Operation { command, now_ms } => self.apply_operation(command, now_ms),
        }
    }

    /// Re-registration of a registered node that may change only its URLs.
    /// No operation invariant reads node addresses: operations pin process
    /// and replica identities, so a node may refresh them mid-operation.
    fn refreshes_addresses_only(&self, command: &ControlCommand) -> bool {
        let ControlCommand::RegisterNode {
            node_id, labels, ..
        } = command
        else {
            return false;
        };
        self.nodes
            .get(node_id)
            .is_some_and(|node| node.state != NodeState::Removed && &node.labels == labels)
    }

    fn apply_operation(
        &mut self,
        command: crate::OperationCommand,
        now_ms: u64,
    ) -> ControlResponse {
        let nodes = self
            .nodes
            .iter()
            .filter(|(_, node)| node.state != NodeState::Removed)
            .map(|(id, node)| (*id, node.state))
            .collect();
        let decommissioned = self
            .operations
            .active
            .as_ref()
            .and_then(|operation| match operation.kind {
                crate::OperationKind::DecommissionNode { node_id, .. } => Some(node_id),
                _ => None,
            });
        let result = self
            .operations
            .apply(command, now_ms, &nodes, &mut self.placements);
        if matches!(result, Ok(crate::OperationOutcome::Completed))
            && let Some(node_id) = decommissioned
            && let Some(node) = self.nodes.get_mut(&node_id)
        {
            node.state = NodeState::Removed;
            node.updated_at_ms = now_ms;
        }
        ControlResponse::Operation(result)
    }

    pub fn placement_view(&self, raft_group_id: RaftGroupId) -> Option<GroupPlacementView> {
        let placement = self.placements.get(&raft_group_id)?;
        let nodes = placement
            .voters
            .iter()
            .filter_map(|node_id| {
                self.nodes.get(node_id).map(|node| {
                    (*node_id, PlacementNode {
                        node_id: *node_id,
                        client_url: node.client_url.clone(),
                        cluster_url: node.cluster_url.clone(),
                        state: node.state,
                    })
                })
            })
            .collect();

        Some(GroupPlacementView {
            raft_group_id,
            voters: placement.voters.clone(),
            epoch: placement.epoch,
            nodes,
        })
    }

    fn register_node(
        &mut self,
        node_id: NodeId,
        client_url: String,
        cluster_url: String,
        labels: BTreeMap<String, String>,
        now_ms: u64,
    ) -> ControlResponse {
        let client_url = normalize_url(client_url);
        let cluster_url = normalize_url(cluster_url);
        if client_url.is_empty() {
            return reject(crate::ControlError::EmptyAddress {
                node_id,
                endpoint: crate::NodeEndpoint::Client,
            });
        }
        if cluster_url.is_empty() {
            return reject(crate::ControlError::EmptyAddress {
                node_id,
                endpoint: crate::NodeEndpoint::Cluster,
            });
        }

        if self
            .nodes
            .get(&node_id)
            .is_some_and(|node| node.state == NodeState::Removed)
        {
            return reject(crate::ControlError::RemovedNode { node_id });
        }
        let (registered_at_ms, state) = self
            .nodes
            .get(&node_id)
            .map_or((now_ms, NodeState::Active), |node| {
                (node.registered_at_ms, node.state)
            });
        self.nodes.insert(node_id, ClusterNode {
            node_id,
            client_url,
            cluster_url,
            state,
            registered_at_ms,
            updated_at_ms: now_ms,
            labels,
        });
        ControlResponse::Ok
    }

    fn set_node_state(
        &mut self,
        node_id: NodeId,
        state: NodeState,
        now_ms: u64,
    ) -> ControlResponse {
        let Some(node) = self.nodes.get_mut(&node_id) else {
            return reject(crate::ControlError::UnknownNode { node_id });
        };
        if node.state == NodeState::Removed {
            return reject(crate::ControlError::RemovedNode { node_id });
        }
        if state == NodeState::Removed {
            return reject(crate::ControlError::RemovalRequiresOperation { node_id });
        }
        node.state = state;
        node.updated_at_ms = now_ms;
        ControlResponse::Ok
    }

    fn seed_placement(
        &mut self,
        raft_group_id: RaftGroupId,
        voters: BTreeSet<NodeId>,
        now_ms: u64,
    ) -> ControlResponse {
        if voters.is_empty() {
            return reject(crate::ControlError::EmptyVoters { raft_group_id });
        }

        for node_id in &voters {
            let Some(node) = self.nodes.get(node_id) else {
                return reject(crate::ControlError::UnknownNode { node_id: *node_id });
            };
            if !node.state.accepts_new_replicas() {
                return reject(crate::ControlError::IneligibleNode {
                    node_id: *node_id,
                    state: node.state,
                });
            }
        }
        if let Some(existing) = self.placements.get(&raft_group_id) {
            return if existing.voters == voters {
                ControlResponse::Ok
            } else {
                reject(crate::ControlError::PlacementExists { raft_group_id })
            };
        }
        self.placements.insert(raft_group_id, DataGroupPlacement {
            raft_group_id,
            voters,
            epoch: 0,
            updated_at_ms: now_ms,
        });
        ControlResponse::Ok
    }
}

fn normalize_url(value: String) -> String {
    value.trim().trim_end_matches('/').to_owned()
}

fn reject(reason: crate::ControlError) -> ControlResponse {
    ControlResponse::Rejected { reason }
}
