use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::RaftGroupId;

use crate::command::ControlCommand;
use crate::command::ControlResponse;
use crate::model::ClusterNode;
use crate::model::DataGroupPlacement;
use crate::model::GroupMigration;
use crate::model::LearnerStatus;
use crate::model::MetaConfig;
use crate::model::MigrationPhase;
use crate::model::NodeId;
use crate::model::NodeState;
use crate::view::GroupPlacementView;
use crate::view::PlacementNode;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlPlaneState {
    pub nodes: BTreeMap<NodeId, ClusterNode>,
    pub placements: BTreeMap<RaftGroupId, DataGroupPlacement>,
    pub migrations: BTreeMap<u64, GroupMigration>,
    pub active_migration: Option<u64>,
    pub next_migration_id: u64,
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
            migrations: BTreeMap::new(),
            active_migration: None,
            next_migration_id: 1,
            config,
        }
    }

    pub fn apply(&mut self, command: ControlCommand) -> ControlResponse {
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
            ControlCommand::CommitPlacement {
                raft_group_id,
                voters,
                learners,
                draining,
                now_ms,
            } => self.commit_placement(raft_group_id, voters, learners, draining, now_ms),
            ControlCommand::BeginMigration {
                raft_group_id,
                target_voters,
                retain_removed,
                now_ms,
            } => self.begin_migration(raft_group_id, target_voters, retain_removed, now_ms),
            ControlCommand::AdvanceMigration {
                migration_id,
                phase,
                now_ms,
            } => self.advance_migration(migration_id, phase, now_ms),
            ControlCommand::SetLearnerStatus {
                migration_id,
                node_id,
                status,
                now_ms,
            } => self.set_learner_status(migration_id, node_id, status, now_ms),
            ControlCommand::RecordMigrationError {
                migration_id,
                error,
                now_ms,
            } => self.record_migration_error(migration_id, error, now_ms),
            ControlCommand::FinishMigration {
                migration_id,
                success,
                now_ms,
            } => self.finish_migration(migration_id, success, now_ms),
            ControlCommand::EvictLearner {
                raft_group_id,
                node_id,
                now_ms,
            } => self.evict_learner(raft_group_id, node_id, now_ms),
        }
    }

    pub fn active_migration(&self) -> Option<&GroupMigration> {
        self.active_migration
            .and_then(|id| self.migrations.get(&id))
    }

    pub fn placement_view(&self, raft_group_id: RaftGroupId) -> Option<GroupPlacementView> {
        let placement = self.placements.get(&raft_group_id)?;
        let node_ids = placement
            .voters
            .iter()
            .chain(placement.learners.iter())
            .chain(placement.draining.iter());
        let nodes = node_ids
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
            learners: placement.learners.clone(),
            draining: placement.draining.clone(),
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
            return reject("client_url must not be empty".to_owned());
        }
        if cluster_url.is_empty() {
            return reject("cluster_url must not be empty".to_owned());
        }

        let (registered_at_ms, state) =
            self.nodes
                .get(&node_id)
                .map_or((now_ms, NodeState::Active), |node| {
                    let state = if node.state == NodeState::Removed {
                        NodeState::Active
                    } else {
                        node.state
                    };
                    (node.registered_at_ms, state)
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
            return reject(format!("node {node_id} is not registered"));
        };
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
            return reject("placement voters must not be empty".to_owned());
        }

        self.placements.insert(raft_group_id, DataGroupPlacement {
            raft_group_id,
            voters,
            learners: BTreeSet::new(),
            draining: BTreeSet::new(),
            epoch: 0,
            updated_at_ms: now_ms,
        });
        ControlResponse::Ok
    }

    fn commit_placement(
        &mut self,
        raft_group_id: RaftGroupId,
        voters: BTreeSet<NodeId>,
        learners: BTreeSet<NodeId>,
        draining: BTreeSet<NodeId>,
        now_ms: u64,
    ) -> ControlResponse {
        if voters.is_empty() {
            return reject("placement voters must not be empty".to_owned());
        }
        if let Some(response) = self.validate_placement_nodes(&voters, &learners, &draining) {
            return response;
        }

        let placement = self
            .placements
            .entry(raft_group_id)
            .or_insert_with(|| DataGroupPlacement::empty(raft_group_id));
        if placement.voters != voters {
            placement.epoch = placement.epoch.saturating_add(1);
        }
        placement.voters = voters;
        placement.learners = learners;
        placement.draining = draining;
        placement.updated_at_ms = now_ms;
        ControlResponse::Ok
    }

    fn validate_placement_nodes(
        &self,
        voters: &BTreeSet<NodeId>,
        learners: &BTreeSet<NodeId>,
        draining: &BTreeSet<NodeId>,
    ) -> Option<ControlResponse> {
        if let Some(node_id) = voters.intersection(learners).next() {
            return Some(reject(format!(
                "node {node_id} cannot be both voter and learner"
            )));
        }
        if let Some(response) = self.validate_registered_nodes("voter", voters, true) {
            return Some(response);
        }
        if let Some(response) = self.validate_registered_nodes("learner", learners, false) {
            return Some(response);
        }
        self.validate_registered_nodes("draining", draining, false)
    }

    fn validate_registered_nodes(
        &self,
        role: &str,
        node_ids: &BTreeSet<NodeId>,
        require_migration_eligible: bool,
    ) -> Option<ControlResponse> {
        for node_id in node_ids {
            let Some(node) = self.nodes.get(node_id) else {
                return Some(reject(format!("{role} node {node_id} is not registered")));
            };
            if require_migration_eligible && !node.state.is_migration_eligible() {
                return Some(reject(format!(
                    "{role} node {node_id} is not migration eligible: {:?}",
                    node.state
                )));
            }
        }
        None
    }

    fn begin_migration(
        &mut self,
        raft_group_id: RaftGroupId,
        target_voters: BTreeSet<NodeId>,
        retain_removed: bool,
        now_ms: u64,
    ) -> ControlResponse {
        if let Some(active) = self.active_migration {
            return reject(format!("migration {active} is already running"));
        }
        if target_voters.is_empty() {
            return reject("target voters must not be empty".to_owned());
        }
        for node_id in &target_voters {
            let Some(node) = self.nodes.get(node_id) else {
                return reject(format!("node {node_id} is not registered"));
            };
            if !node.state.is_migration_eligible() {
                return reject(format!(
                    "node {node_id} is not migration eligible: {:?}",
                    node.state
                ));
            }
        }

        let Some(placement) = self.placements.get(&raft_group_id) else {
            return reject(format!("group {} has no placement", raft_group_id.0));
        };
        let from_voters = placement.voters.clone();
        let added_nodes = target_voters
            .difference(&from_voters)
            .copied()
            .collect::<BTreeSet<_>>();
        let removed_voters = from_voters
            .difference(&target_voters)
            .copied()
            .collect::<BTreeSet<_>>();
        let per_node_learner_status = added_nodes
            .iter()
            .copied()
            .map(|node_id| (node_id, LearnerStatus::Pending))
            .collect();

        let migration_id = self.next_migration_id.max(1);
        self.next_migration_id = migration_id.saturating_add(1);
        self.migrations.insert(migration_id, GroupMigration {
            migration_id,
            raft_group_id,
            from_voters,
            target_voters,
            added_nodes,
            removed_voters,
            retain_removed,
            phase: MigrationPhase::Validating,
            per_node_learner_status,
            last_error: None,
            retry_count: 0,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        });
        self.active_migration = Some(migration_id);

        ControlResponse::MigrationStarted { migration_id }
    }

    fn advance_migration(
        &mut self,
        migration_id: u64,
        phase: MigrationPhase,
        now_ms: u64,
    ) -> ControlResponse {
        if !phase.is_running() {
            return reject(format!(
                "migration {migration_id} must finish through FinishMigration"
            ));
        }
        let Some(migration) = self.migrations.get_mut(&migration_id) else {
            return reject(format!("migration {migration_id} does not exist"));
        };
        if migration.phase == phase {
            return ControlResponse::Ok;
        }
        if !migration.phase.can_advance_to(phase) {
            return reject(format!(
                "migration {migration_id} cannot advance from {:?} to {:?}",
                migration.phase, phase
            ));
        }
        migration.phase = phase;
        migration.updated_at_ms = now_ms;
        ControlResponse::Ok
    }

    fn set_learner_status(
        &mut self,
        migration_id: u64,
        node_id: NodeId,
        status: LearnerStatus,
        now_ms: u64,
    ) -> ControlResponse {
        let Some(migration) = self.migrations.get_mut(&migration_id) else {
            return reject(format!("migration {migration_id} does not exist"));
        };
        if !migration.is_running() {
            return reject(format!("migration {migration_id} is not running"));
        }
        if !migration.added_nodes.contains(&node_id) {
            return reject(format!(
                "node {node_id} is not an added learner for migration {migration_id}"
            ));
        }
        migration.per_node_learner_status.insert(node_id, status);
        migration.updated_at_ms = now_ms;
        ControlResponse::Ok
    }

    fn record_migration_error(
        &mut self,
        migration_id: u64,
        error: String,
        now_ms: u64,
    ) -> ControlResponse {
        let Some(migration) = self.migrations.get_mut(&migration_id) else {
            return reject(format!("migration {migration_id} does not exist"));
        };
        migration.last_error = Some(error);
        migration.retry_count = migration.retry_count.saturating_add(1);
        migration.updated_at_ms = now_ms;
        ControlResponse::Ok
    }

    fn finish_migration(
        &mut self,
        migration_id: u64,
        success: bool,
        now_ms: u64,
    ) -> ControlResponse {
        let Some(migration) = self.migrations.get_mut(&migration_id) else {
            return reject(format!("migration {migration_id} does not exist"));
        };
        if self.active_migration != Some(migration_id) {
            return reject(format!("migration {migration_id} is not active"));
        }
        if !migration.is_running() {
            return reject(format!("migration {migration_id} is not running"));
        }
        migration.phase = if success {
            MigrationPhase::Succeeded
        } else {
            MigrationPhase::Failed
        };
        migration.updated_at_ms = now_ms;
        self.active_migration = None;
        ControlResponse::Ok
    }

    fn evict_learner(
        &mut self,
        raft_group_id: RaftGroupId,
        node_id: NodeId,
        now_ms: u64,
    ) -> ControlResponse {
        let Some(placement) = self.placements.get_mut(&raft_group_id) else {
            return reject(format!("group {} has no placement", raft_group_id.0));
        };
        if placement.voters.contains(&node_id) {
            return reject(format!(
                "node {node_id} is a voter of group {} and cannot be evicted as a learner",
                raft_group_id.0
            ));
        }
        placement.learners.remove(&node_id);
        placement.draining.remove(&node_id);
        placement.updated_at_ms = now_ms;
        ControlResponse::Ok
    }
}

fn normalize_url(value: String) -> String {
    value.trim().trim_end_matches('/').to_owned()
}

fn reject(reason: String) -> ControlResponse {
    ControlResponse::Rejected { reason }
}
