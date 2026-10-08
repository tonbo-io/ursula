use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::RaftGroupId;

use crate::NodeId;
use crate::identity::ProcessIncarnation;
use crate::identity::ReplicaIdentity;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessIdentity {
    pub epoch: u64,
    pub incarnation: ProcessIncarnation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessState {
    Active(ProcessIdentity),
    Retired {
        epoch: u64,
        reason: RetirementReason,
    },
}

/// Data admission follows the durable WAL lifetime, independently of each boot.
/// A replacement remains pending until every affected group has ordered its
/// new identity after the old replica's removal from the voter configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaState {
    Active {
        identity: ReplicaIdentity,
        installed_groups: BTreeMap<RaftGroupId, u64>,
    },
    Retired(ReplicaIdentity),
    Pending {
        previous: ReplicaIdentity,
        replacement: ReplicaIdentity,
        installed_groups: BTreeMap<RaftGroupId, u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetirementReason {
    Rebuild,
    Decommission,
}

impl ProcessState {
    pub fn epoch(&self) -> u64 {
        match self {
            Self::Active(identity) => identity.epoch,
            Self::Retired { epoch, .. } => *epoch,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationKind {
    MoveReplicas {
        source: NodeId,
        target: NodeId,
        groups: BTreeSet<RaftGroupId>,
    },
    RebuildReplica {
        node_id: NodeId,
    },
    DecommissionNode {
        node_id: NodeId,
        replacements: BTreeMap<RaftGroupId, NodeId>,
    },
}

impl OperationKind {
    pub fn source(&self) -> NodeId {
        match self {
            Self::MoveReplicas { source, .. } => *source,
            Self::RebuildReplica { node_id } | Self::DecommissionNode { node_id, .. } => *node_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationToken {
    pub operation_id: OperationId,
    pub generation: ExecutorGeneration,
    pub executor: ProcessIncarnation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationPhase {
    Preparing,
    Retired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaEvidence {
    pub process: ProcessIdentity,
    pub applied_index: u64,
    #[serde(default)]
    pub installed_replica_identities: BTreeMap<NodeId, ReplicaIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrefixEvidence {
    pub raft_group_id: RaftGroupId,
    pub leader: NodeId,
    pub term: u64,
    pub committed_index: u64,
    pub voters: BTreeSet<NodeId>,
    pub joint: bool,
    pub replicas: BTreeMap<NodeId, ReplicaEvidence>,
    pub observed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceOperation {
    pub token: OperationToken,
    pub kind: OperationKind,
    pub phase: OperationPhase,
    pub participants: BTreeMap<NodeId, ProcessIdentity>,
    pub meta_voters: BTreeSet<NodeId>,
    pub previous: BTreeMap<RaftGroupId, BTreeSet<NodeId>>,
    pub desired: BTreeMap<RaftGroupId, BTreeSet<NodeId>>,
    pub evidence: BTreeMap<RaftGroupId, PrefixEvidence>,
    /// Retained across executor takeover and replacement claims.
    pub prefix_floor: BTreeMap<RaftGroupId, u64>,
    pub pending_action: Option<PendingAction>,
    pub last_action_sequence: ActionSequence,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationState {
    pub processes: BTreeMap<NodeId, ProcessState>,
    #[serde(default)]
    pub replicas: BTreeMap<NodeId, ReplicaState>,
    pub active: Option<MaintenanceOperation>,
    pub last_operation_id: OperationId,
}

/// An unresolved effect is retained across executor takeover. Only the bound
/// data leader process may execute it; completion cannot pass this receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationAction {
    pub sequence: ActionSequence,
    pub group: RaftGroupId,
    pub leader: NodeId,
    pub process: ProcessIdentity,
    pub action: MembershipAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipAction {
    PrepareReplica,
    AddLearner {
        node_id: NodeId,
    },
    ChangeVoters,
    RetireReplica,
    InstallReplicaIdentity {
        node_id: NodeId,
        identity: ReplicaIdentity,
    },
}

/// A dispatched effect stays unresolved across takeover and participant restart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PendingAction {
    Prepared(OperationAction),
    OutcomeUnknown(OperationAction),
}

impl PendingAction {
    pub fn receipt(&self) -> &OperationAction {
        match self {
            Self::Prepared(receipt) | Self::OutcomeUnknown(receipt) => receipt,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionOutcome {
    NotDispatched,
    Completed,
    Unknown,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct OperationId(pub u64);

impl OperationId {
    pub(super) fn checked_add(self, increment: u64) -> Option<Self> {
        self.0.checked_add(increment).map(Self)
    }
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct ExecutorGeneration(pub u64);

impl ExecutorGeneration {
    pub(super) fn checked_add(self, increment: u64) -> Option<Self> {
        self.0.checked_add(increment).map(Self)
    }
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct ActionSequence(pub u64);

impl ActionSequence {
    pub(super) fn checked_add(self, increment: u64) -> Option<Self> {
        self.0.checked_add(increment).map(Self)
    }
}
