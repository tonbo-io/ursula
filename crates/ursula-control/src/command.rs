use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::RaftGroupId;

use crate::model::LearnerStatus;
use crate::model::MigrationPhase;
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
    BeginMigration {
        raft_group_id: RaftGroupId,
        target_voters: BTreeSet<NodeId>,
        retain_removed: bool,
        now_ms: u64,
    },
    AdvanceMigration {
        migration_id: u64,
        phase: MigrationPhase,
        now_ms: u64,
    },
    SetLearnerStatus {
        migration_id: u64,
        node_id: NodeId,
        status: LearnerStatus,
        now_ms: u64,
    },
    RecordMigrationError {
        migration_id: u64,
        error: String,
        now_ms: u64,
    },
    CommitPlacement {
        raft_group_id: RaftGroupId,
        voters: BTreeSet<NodeId>,
        learners: BTreeSet<NodeId>,
        draining: BTreeSet<NodeId>,
        now_ms: u64,
    },
    FinishMigration {
        migration_id: u64,
        success: bool,
        now_ms: u64,
    },
    EvictLearner {
        raft_group_id: RaftGroupId,
        node_id: NodeId,
        now_ms: u64,
    },
}

impl ControlCommand {
    pub fn now_ms(&self) -> u64 {
        match self {
            Self::RegisterNode { now_ms, .. }
            | Self::SetNodeState { now_ms, .. }
            | Self::SeedPlacement { now_ms, .. }
            | Self::BeginMigration { now_ms, .. }
            | Self::AdvanceMigration { now_ms, .. }
            | Self::SetLearnerStatus { now_ms, .. }
            | Self::RecordMigrationError { now_ms, .. }
            | Self::CommitPlacement { now_ms, .. }
            | Self::FinishMigration { now_ms, .. }
            | Self::EvictLearner { now_ms, .. } => *now_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlResponse {
    Ok,
    MigrationStarted { migration_id: u64 },
    Rejected { reason: String },
}

impl ControlResponse {
    pub fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected { .. })
    }
}
