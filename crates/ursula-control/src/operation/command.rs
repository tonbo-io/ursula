use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::RaftGroupId;

use super::model::ActionOutcome;
use super::model::ActionSequence;
use super::model::MembershipAction;
use super::model::OperationAction;
use super::model::OperationKind;
use super::model::OperationToken;
use super::model::PrefixEvidence;
use super::model::ProcessIdentity;
use crate::NodeId;
use crate::identity::ProcessIncarnation;
use crate::identity::ReplicaIdentity;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationCommand {
    /// Persist before invoking the external effect. A lost response then has
    /// a durable unknown outcome, rather than permission to run it twice.
    MarkActionDispatched {
        token: OperationToken,
        sequence: ActionSequence,
    },
    /// Only a never-dispatched receipt can be discarded without reconciliation.
    CancelPreparedAction {
        token: OperationToken,
        sequence: ActionSequence,
    },
    ReassignAction {
        token: OperationToken,
        leader: NodeId,
        drained: Option<OperationAction>,
    },
    PrepareAction {
        token: OperationToken,
        group: RaftGroupId,
        leader: NodeId,
        action: MembershipAction,
    },
    FinishAction {
        token: OperationToken,
        sequence: ActionSequence,
    },
    FinishReplicaFence {
        token: OperationToken,
        sequence: ActionSequence,
        committed_index: u64,
    },
    ClaimProcess {
        node_id: NodeId,
        expected_epoch: u64,
        incarnation: ProcessIncarnation,
    },
    /// A boot using the same exclusively owned WAL may refresh maintenance
    /// observations without changing the data replica's admission identity.
    RestartProcess {
        node_id: NodeId,
        previous: ProcessIdentity,
        incarnation: ProcessIncarnation,
        replica: ReplicaIdentity,
    },
    RegisterReplica {
        node_id: NodeId,
        process: ProcessIdentity,
        identity: ReplicaIdentity,
    },
    ActivateReplica {
        token: OperationToken,
        node_id: NodeId,
        identity: ReplicaIdentity,
    },
    Begin {
        kind: OperationKind,
        executor: ProcessIncarnation,
        participants: BTreeMap<NodeId, ProcessIdentity>,
        meta_voters: BTreeSet<NodeId>,
    },
    TakeOver {
        expected: OperationToken,
        executor: ProcessIncarnation,
    },
    Observe {
        token: OperationToken,
        evidence: PrefixEvidence,
    },
    RetireSource {
        token: OperationToken,
    },
    Complete {
        token: OperationToken,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationOutcome {
    ActionPrepared(OperationAction),
    ActionOutcome(ActionOutcome),
    ProcessClaimed(ProcessIdentity),
    ReplicaRegistered,
    ReplicaActivated,
    Acquired(OperationToken),
    EvidenceRecorded,
    SourceRetired,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum OperationError {
    #[error("another maintenance operation is active")]
    Busy,
    #[error("unknown node {node_id}")]
    UnknownNode { node_id: NodeId },
    #[error("process epoch precondition failed for node {node_id}")]
    ProcessChanged { node_id: NodeId },
    #[error("durable replica identity precondition failed for node {node_id}")]
    ReplicaChanged { node_id: NodeId },
    #[error("operation executor precondition failed")]
    StaleExecutor,
    #[error("maintenance operation does not cover the exact affected inventory")]
    InventoryMismatch,
    #[error("invalid operation transition")]
    InvalidTransition,
    #[error("action {sequence:?} has an unresolved outcome")]
    ActionOutcomeUnknown { sequence: ActionSequence },
    #[error("epoch counter exhausted")]
    EpochExhausted,
    #[error("group {raft_group_id:?} lacks fresh quorum and apply evidence")]
    MissingEvidence { raft_group_id: RaftGroupId },
}
