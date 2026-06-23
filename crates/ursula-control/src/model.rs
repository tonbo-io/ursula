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

impl NodeState {
    pub fn is_migration_eligible(self) -> bool {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LearnerStatus {
    Pending,
    Adding,
    CaughtUp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MigrationPhase {
    Validating,
    PreparingLocalEngines,
    AddingLearners,
    ChangingVoters,
    VerifyingMembership,
    CommittingPlacement,
    Finalizing,
    Succeeded,
    Failed,
}

impl MigrationPhase {
    pub fn is_running(self) -> bool {
        !matches!(self, Self::Succeeded | Self::Failed)
    }

    pub fn can_advance_to(self, next: Self) -> bool {
        if !self.is_running() || !next.is_running() {
            return false;
        }
        self < next
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupMigration {
    pub migration_id: u64,
    pub raft_group_id: RaftGroupId,
    pub from_voters: BTreeSet<NodeId>,
    pub target_voters: BTreeSet<NodeId>,
    pub added_nodes: BTreeSet<NodeId>,
    pub removed_voters: BTreeSet<NodeId>,
    pub retain_removed: bool,
    pub phase: MigrationPhase,
    pub per_node_learner_status: BTreeMap<NodeId, LearnerStatus>,
    pub last_error: Option<String>,
    pub retry_count: u32,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl GroupMigration {
    pub fn is_running(&self) -> bool {
        self.phase.is_running()
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
