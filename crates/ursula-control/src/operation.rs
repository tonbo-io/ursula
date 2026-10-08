//! Replicated maintenance ownership, process fencing and membership evidence.
//!
//! Transport adapters authenticate evidence before submission. Every state
//! transition additionally binds it to the committed process and executor epochs.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_proto::admin::ProcessIncarnation;
use ursula_proto::admin::ReplicaIdentity;
use ursula_shard::RaftGroupId;

use crate::DataGroupPlacement;
use crate::NodeId;

const EVIDENCE_MAX_AGE_MS: u64 = 30_000;

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
    ActionRecovery,
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
    pub operation_id: u64,
    pub generation: u64,
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
    pub pending_action: Option<OperationAction>,
    pub last_action_sequence: u64,
    pub recovering_processes: BTreeSet<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationCommand {
    RetireActionExecutor {
        token: OperationToken,
        drained: OperationAction,
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
        sequence: u64,
    },
    FinishReplicaFence {
        token: OperationToken,
        sequence: u64,
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
    ExecutorRetired { node_id: NodeId },
    ActionPrepared(OperationAction),
    ActionFinished,
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
    #[error("another maintenance or migration operation is active")]
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
    #[error("epoch counter exhausted")]
    EpochExhausted,
    #[error("group {raft_group_id:?} lacks fresh quorum and apply evidence")]
    MissingEvidence { raft_group_id: RaftGroupId },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationState {
    pub processes: BTreeMap<NodeId, ProcessState>,
    #[serde(default)]
    pub replicas: BTreeMap<NodeId, ReplicaState>,
    pub active: Option<MaintenanceOperation>,
    pub last_operation_id: u64,
}

impl OperationState {
    pub fn accepts_process(&self, node_id: NodeId, identity: &ProcessIdentity) -> bool {
        self.processes.get(&node_id) == Some(&ProcessState::Active(identity.clone()))
    }

    pub(crate) fn apply(
        &mut self,
        command: OperationCommand,
        now_ms: u64,
        nodes: &BTreeSet<NodeId>,
        placements: &mut BTreeMap<RaftGroupId, DataGroupPlacement>,
    ) -> Result<OperationOutcome, OperationError> {
        match command {
            OperationCommand::RetireActionExecutor { token, drained } => {
                let bound = self
                    .active
                    .as_ref()
                    .and_then(|operation| operation.pending_action.as_ref())
                    .cloned()
                    .ok_or(OperationError::InvalidTransition)?;
                if drained != bound {
                    return Err(OperationError::InvalidTransition);
                }
                if !self.accepts_process(bound.leader, &bound.process) {
                    return Err(OperationError::ProcessChanged {
                        node_id: bound.leader,
                    });
                }
                let operation = self.authorized(&token)?;
                let pending = operation
                    .pending_action
                    .as_ref()
                    .ok_or(OperationError::InvalidTransition)?;
                let node_id = pending.leader;
                // Fencing is process-wide: never certify only a subset of its
                // hosted groups before retiring that process's protocol epoch.
                if placements.iter().any(|(group, placement)| {
                    placement.hosts(node_id) && !operation.previous.contains_key(group)
                }) {
                    return Err(OperationError::InventoryMismatch);
                }
                for (group, previous) in &operation.previous {
                    let missing = || OperationError::MissingEvidence {
                        raft_group_id: *group,
                    };
                    let evidence = operation.evidence.get(group).ok_or_else(missing)?;
                    let desired = operation.desired.get(group).ok_or_else(missing)?;
                    if (&evidence.voters != previous && &evidence.voters != desired)
                        || now_ms.saturating_sub(evidence.observed_at_ms) > EVIDENCE_MAX_AGE_MS
                    {
                        return Err(missing());
                    }
                    let survivors = evidence
                        .voters
                        .iter()
                        .filter(|id| **id != node_id)
                        .collect::<Vec<_>>();
                    if survivors.len() <= evidence.voters.len() / 2
                        || !survivors
                            .iter()
                            .all(|id| evidence.replicas.contains_key(id))
                    {
                        return Err(missing());
                    }
                }
                let epoch = pending.process.epoch;
                operation.recovering_processes.insert(node_id);
                operation.evidence.clear();
                self.processes.insert(node_id, ProcessState::Retired {
                    epoch,
                    reason: RetirementReason::ActionRecovery,
                });
                Ok(OperationOutcome::ExecutorRetired { node_id })
            }
            OperationCommand::ReassignAction {
                token,
                leader,
                drained,
            } => {
                let process = match self.processes.get(&leader) {
                    Some(ProcessState::Active(identity)) => identity.clone(),
                    _ => return Err(OperationError::ProcessChanged { node_id: leader }),
                };
                let old = self
                    .active
                    .as_ref()
                    .and_then(|operation| operation.pending_action.as_ref())
                    .cloned()
                    .ok_or(OperationError::InvalidTransition)?;
                if drained.as_ref().is_some_and(|receipt| receipt != &old) {
                    return Err(OperationError::InvalidTransition);
                }
                if self.accepts_process(old.leader, &old.process) && drained.is_none() {
                    return Err(OperationError::Busy);
                }
                let operation = self.authorized(&token)?;
                if matches!(old.action, MembershipAction::PrepareReplica) && leader != old.leader {
                    return Err(OperationError::InventoryMismatch);
                }
                if !operation
                    .previous
                    .get(&old.group)
                    .is_some_and(|voters| voters.contains(&leader))
                    && !operation
                        .desired
                        .get(&old.group)
                        .is_some_and(|voters| voters.contains(&leader))
                {
                    return Err(OperationError::InventoryMismatch);
                }
                if operation.participants.get(&leader) != Some(&process) {
                    return Err(OperationError::ProcessChanged { node_id: leader });
                }
                let sequence = operation
                    .last_action_sequence
                    .checked_add(1)
                    .ok_or(OperationError::EpochExhausted)?;
                let receipt = OperationAction {
                    sequence,
                    leader,
                    process,
                    ..old
                };
                operation.last_action_sequence = sequence;
                operation.pending_action = Some(receipt.clone());
                Ok(OperationOutcome::ActionPrepared(receipt))
            }
            OperationCommand::PrepareAction {
                token,
                group,
                leader,
                action,
            } => {
                self.validate_replica_action(&token, group, leader, &action)?;
                let current_process = match self.processes.get(&leader) {
                    Some(ProcessState::Active(identity)) => identity.clone(),
                    _ => return Err(OperationError::ProcessChanged { node_id: leader }),
                };
                let operation = self.authorized(&token)?;
                if let Some(pending) = &operation.pending_action {
                    if pending.group == group
                        && pending.leader == leader
                        && pending.action == action
                    {
                        return Ok(OperationOutcome::ActionPrepared(pending.clone()));
                    }
                    return Err(OperationError::Busy);
                }
                let desired = operation
                    .desired
                    .get(&group)
                    .ok_or(OperationError::InventoryMismatch)?;
                let previous = operation
                    .previous
                    .get(&group)
                    .ok_or(OperationError::InventoryMismatch)?;
                if !previous.contains(&leader) && !desired.contains(&leader) {
                    return Err(OperationError::InventoryMismatch);
                }
                if matches!(action, MembershipAction::PrepareReplica) && !desired.contains(&leader)
                {
                    return Err(OperationError::InventoryMismatch);
                }
                if matches!(action, MembershipAction::RetireReplica)
                    && (operation.phase != OperationPhase::Preparing
                        || !matches!(operation.kind, OperationKind::RebuildReplica { .. }))
                {
                    return Err(OperationError::InvalidTransition);
                }
                if let MembershipAction::AddLearner { node_id } = &action
                    && (!desired.contains(node_id)
                        || (previous.contains(node_id)
                            && !matches!(operation.kind, OperationKind::RebuildReplica { node_id: source } if source == *node_id)))
                {
                    return Err(OperationError::InventoryMismatch);
                }
                let process = operation
                    .participants
                    .get(&leader)
                    .cloned()
                    .ok_or(OperationError::InventoryMismatch)?;
                if process != current_process {
                    return Err(OperationError::ProcessChanged { node_id: leader });
                }
                let sequence = operation
                    .last_action_sequence
                    .checked_add(1)
                    .ok_or(OperationError::EpochExhausted)?;
                let receipt = OperationAction {
                    sequence,
                    group,
                    leader,
                    process,
                    action,
                };
                operation.last_action_sequence = sequence;
                operation.pending_action = Some(receipt.clone());
                Ok(OperationOutcome::ActionPrepared(receipt))
            }
            OperationCommand::FinishAction { token, sequence } => {
                let operation = self.authorized(&token)?;
                if !operation.pending_action.as_ref().is_some_and(|action| {
                    action.sequence == sequence
                        && !matches!(
                            action.action,
                            MembershipAction::InstallReplicaIdentity { .. }
                        )
                }) {
                    return Err(OperationError::InvalidTransition);
                }
                operation.pending_action = None;
                operation.evidence.clear();
                Ok(OperationOutcome::ActionFinished)
            }
            OperationCommand::FinishReplicaFence {
                token,
                sequence,
                committed_index,
            } => self.finish_replica_fence(&token, sequence, committed_index, now_ms),
            OperationCommand::ClaimProcess {
                node_id,
                expected_epoch,
                incarnation,
            } => self.claim_process(node_id, expected_epoch, incarnation, nodes),
            OperationCommand::RestartProcess {
                node_id,
                previous,
                incarnation,
                replica,
            } => self.restart_process(node_id, &previous, incarnation, &replica),
            OperationCommand::RegisterReplica {
                node_id,
                process,
                identity,
            } => self.register_replica(node_id, &process, identity, nodes),
            OperationCommand::ActivateReplica {
                token,
                node_id,
                identity,
            } => self.activate_replica(&token, node_id, identity),
            OperationCommand::Begin {
                kind,
                executor,
                participants,
                meta_voters,
            } => self.begin(kind, executor, participants, meta_voters, nodes, placements),
            OperationCommand::TakeOver { expected, executor } => {
                let operation = self.authorized(&expected)?;
                operation.token.generation = operation
                    .token
                    .generation
                    .checked_add(1)
                    .ok_or(OperationError::EpochExhausted)?;
                operation.token.executor = executor;
                operation.evidence.clear();
                Ok(OperationOutcome::Acquired(operation.token.clone()))
            }
            OperationCommand::Observe { token, evidence } => self.observe(&token, evidence, now_ms),
            OperationCommand::RetireSource { token } => self.retire(&token, now_ms),
            OperationCommand::Complete { token } => self.complete(&token, now_ms, placements),
        }
    }

    fn finish_replica_fence(
        &mut self,
        token: &OperationToken,
        sequence: u64,
        committed_index: u64,
        now_ms: u64,
    ) -> Result<OperationOutcome, OperationError> {
        let operation = self.authorized(token)?;
        let Some(OperationAction {
            group,
            sequence: expected_sequence,
            action: MembershipAction::InstallReplicaIdentity { node_id, identity },
            ..
        }) = operation.pending_action.as_ref()
        else {
            return Err(OperationError::InvalidTransition);
        };
        if sequence != *expected_sequence || committed_index == 0 {
            return Err(OperationError::InvalidTransition);
        }
        let missing = || OperationError::MissingEvidence {
            raft_group_id: *group,
        };
        let evidence = operation.evidence.get(group).ok_or_else(missing)?;
        let survivors: BTreeSet<_> = operation
            .previous
            .get(group)
            .ok_or_else(missing)?
            .iter()
            .copied()
            .filter(|voter| voter != node_id)
            .collect();
        let installed = survivors
            .iter()
            .filter(|voter| {
                evidence.replicas.get(voter).is_some_and(|replica| {
                    replica.installed_replica_identities.get(node_id) == Some(identity)
                        && replica.applied_index >= committed_index
                })
            })
            .count();
        if evidence.joint
            || evidence.voters != survivors
            || evidence.committed_index < committed_index
            || evidence.observed_at_ms > now_ms
            || now_ms.saturating_sub(evidence.observed_at_ms) > EVIDENCE_MAX_AGE_MS
            || installed != survivors.len()
        {
            return Err(missing());
        }
        let group = *group;
        let node_id = *node_id;
        let identity = identity.clone();
        match self.replicas.get_mut(&node_id) {
            Some(ReplicaState::Pending {
                replacement,
                installed_groups,
                ..
            }) if replacement == &identity => {
                installed_groups.insert(group, committed_index);
            }
            Some(ReplicaState::Active {
                identity: current,
                installed_groups,
            }) if current == &identity => {
                installed_groups
                    .entry(group)
                    .and_modify(|index| *index = (*index).max(committed_index))
                    .or_insert(committed_index);
            }
            _ => return Err(OperationError::ReplicaChanged { node_id }),
        }
        let operation = self.authorized(token)?;
        operation.pending_action = None;
        operation.evidence.clear();
        Ok(OperationOutcome::ActionFinished)
    }

    fn register_replica(
        &mut self,
        node_id: NodeId,
        process: &ProcessIdentity,
        identity: ReplicaIdentity,
        nodes: &BTreeSet<NodeId>,
    ) -> Result<OperationOutcome, OperationError> {
        if !nodes.contains(&node_id) {
            return Err(OperationError::UnknownNode { node_id });
        }
        if !self.accepts_process(node_id, process) {
            return Err(OperationError::ProcessChanged { node_id });
        }
        if identity.generation == 0 || identity.generation > process.epoch {
            return Err(OperationError::ReplicaChanged { node_id });
        }
        match self.replicas.get(&node_id) {
            Some(ReplicaState::Active {
                identity: current, ..
            }) if current == &identity => {
                return Ok(OperationOutcome::ReplicaRegistered);
            }
            Some(ReplicaState::Pending { replacement, .. }) if replacement == &identity => {
                return Ok(OperationOutcome::ReplicaRegistered);
            }
            Some(ReplicaState::Retired(previous)) => {
                let operation = self
                    .active
                    .as_ref()
                    .ok_or(OperationError::InvalidTransition)?;
                if !matches!(operation.kind, OperationKind::RebuildReplica { node_id: source } if source == node_id)
                    || operation.phase != OperationPhase::Retired
                    || identity.generation <= previous.generation
                    || identity.incarnation == previous.incarnation
                {
                    return Err(OperationError::ReplicaChanged { node_id });
                }
                self.replicas.insert(node_id, ReplicaState::Pending {
                    previous: previous.clone(),
                    replacement: identity,
                    installed_groups: BTreeMap::new(),
                });
            }
            Some(_) => return Err(OperationError::ReplicaChanged { node_id }),
            None => {
                if self.active.is_some() {
                    return Err(OperationError::Busy);
                }
                if identity.generation != process.epoch {
                    return Err(OperationError::ReplicaChanged { node_id });
                }
                self.replicas.insert(node_id, ReplicaState::Active {
                    identity,
                    installed_groups: BTreeMap::new(),
                });
            }
        }
        Ok(OperationOutcome::ReplicaRegistered)
    }

    fn validate_replica_action(
        &self,
        token: &OperationToken,
        group: RaftGroupId,
        leader: NodeId,
        action: &MembershipAction,
    ) -> Result<(), OperationError> {
        let operation = self
            .active
            .as_ref()
            .filter(|operation| &operation.token == token)
            .ok_or(OperationError::StaleExecutor)?;
        match action {
            MembershipAction::InstallReplicaIdentity { node_id, identity } => {
                let replacement = operation.phase == OperationPhase::Retired
                    && matches!(operation.kind, OperationKind::RebuildReplica { node_id: source } if source == *node_id)
                    && matches!(self.replicas.get(node_id), Some(ReplicaState::Pending { replacement, .. }) if replacement == identity);
                let admission = operation.phase == OperationPhase::Preparing
                    && matches!(
                        operation.kind,
                        OperationKind::MoveReplicas { .. } | OperationKind::DecommissionNode { .. }
                    )
                    && operation
                        .desired
                        .get(&group)
                        .is_some_and(|voters| voters.contains(node_id))
                    && operation
                        .previous
                        .get(&group)
                        .is_some_and(|voters| !voters.contains(node_id))
                    && matches!(self.replicas.get(node_id), Some(ReplicaState::Active { identity: current, .. }) if current == identity);
                if leader == *node_id || !(replacement || admission) {
                    return Err(OperationError::ReplicaChanged { node_id: *node_id });
                }
            }
            MembershipAction::ChangeVoters => {
                if let OperationKind::RebuildReplica { node_id } = operation.kind
                    && self
                        .replicas
                        .get(&node_id)
                        .is_some_and(|state| !matches!(state, ReplicaState::Active { .. }))
                {
                    return Err(OperationError::ReplicaChanged { node_id });
                }
                if let (Some(previous), Some(desired)) = (
                    operation.previous.get(&group),
                    operation.desired.get(&group),
                ) {
                    for node_id in desired.difference(previous) {
                        if let Some(state) = self.replicas.get(node_id)
                            && !matches!(state, ReplicaState::Active { installed_groups, .. } if installed_groups.contains_key(&group))
                        {
                            return Err(OperationError::ReplicaChanged { node_id: *node_id });
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn activate_replica(
        &mut self,
        token: &OperationToken,
        node_id: NodeId,
        identity: ReplicaIdentity,
    ) -> Result<OperationOutcome, OperationError> {
        let operation = self.authorized(token)?;
        if operation.phase != OperationPhase::Retired
            || !matches!(operation.kind, OperationKind::RebuildReplica { node_id: source } if source == node_id)
            || operation.pending_action.is_some()
        {
            return Err(OperationError::InvalidTransition);
        }
        let required: BTreeSet<_> = operation.previous.keys().copied().collect();
        match self.replicas.get(&node_id) {
            Some(ReplicaState::Active {
                identity: current, ..
            }) if current == &identity => {}
            Some(ReplicaState::Pending {
                replacement,
                installed_groups,
                ..
            }) if replacement == &identity
                && installed_groups.keys().copied().collect::<BTreeSet<_>>() == required =>
            {
                self.replicas.insert(node_id, ReplicaState::Active {
                    identity,
                    installed_groups: installed_groups.clone(),
                });
            }
            _ => return Err(OperationError::ReplicaChanged { node_id }),
        }
        Ok(OperationOutcome::ReplicaActivated)
    }

    fn authorized(
        &mut self,
        token: &OperationToken,
    ) -> Result<&mut MaintenanceOperation, OperationError> {
        self.active
            .as_mut()
            .filter(|operation| &operation.token == token)
            .ok_or(OperationError::StaleExecutor)
    }

    fn restart_process(
        &mut self,
        node_id: NodeId,
        previous: &ProcessIdentity,
        incarnation: ProcessIncarnation,
        replica: &ReplicaIdentity,
    ) -> Result<OperationOutcome, OperationError> {
        if !self.accepts_process(node_id, previous) {
            return Err(OperationError::ProcessChanged { node_id });
        }
        let admitted = match self.replicas.get(&node_id) {
            Some(ReplicaState::Active { identity, .. }) => identity == replica,
            Some(ReplicaState::Pending { replacement, .. }) => replacement == replica,
            _ => false,
        };
        if !admitted {
            return Err(OperationError::ReplicaChanged { node_id });
        }
        if let Some(operation) = &self.active
            && operation.participants.get(&node_id) != Some(previous)
        {
            return Err(OperationError::ProcessChanged { node_id });
        }
        let identity = ProcessIdentity {
            epoch: previous
                .epoch
                .checked_add(1)
                .ok_or(OperationError::EpochExhausted)?,
            incarnation,
        };
        self.processes
            .insert(node_id, ProcessState::Active(identity.clone()));
        if let Some(operation) = &mut self.active {
            operation.participants.insert(node_id, identity.clone());
            operation.evidence.clear();
            // Keep any unresolved action bound to its old process. Reassignment
            // must explicitly resolve that receipt; a reboot cannot erase it.
        }
        Ok(OperationOutcome::ProcessClaimed(identity))
    }

    fn claim_process(
        &mut self,
        node_id: NodeId,
        expected_epoch: u64,
        incarnation: ProcessIncarnation,
        nodes: &BTreeSet<NodeId>,
    ) -> Result<OperationOutcome, OperationError> {
        if !nodes.contains(&node_id) {
            return Err(OperationError::UnknownNode { node_id });
        }
        let current = self.processes.get(&node_id);
        if current.map_or(0, ProcessState::epoch) != expected_epoch {
            return Err(OperationError::ProcessChanged { node_id });
        }
        if self.active.is_none()
            && matches!(
                current,
                Some(ProcessState::Retired {
                    reason: RetirementReason::Decommission,
                    ..
                })
            )
        {
            return Err(OperationError::InvalidTransition);
        }
        // A pending replacement has no data authority yet and can restart while
        // waiting for its fences. Its WAL identity still has to match exactly.
        // An active executor cannot silently refresh its action's process pins.
        if let Some(operation) = &self.active {
            let rebuild = matches!(operation.kind, OperationKind::RebuildReplica { node_id: source } if source == node_id)
                && operation.phase == OperationPhase::Retired;
            let retired = matches!(current, Some(ProcessState::Retired { .. }));
            let pending = matches!(
                self.replicas.get(&node_id),
                Some(ReplicaState::Pending { .. })
            );
            if !(rebuild && (retired || pending)
                || operation.recovering_processes.contains(&node_id) && retired)
            {
                return Err(OperationError::Busy);
            }
        }
        let identity = ProcessIdentity {
            epoch: expected_epoch
                .checked_add(1)
                .ok_or(OperationError::EpochExhausted)?,
            incarnation,
        };
        self.processes
            .insert(node_id, ProcessState::Active(identity.clone()));
        if let Some(operation) = &mut self.active {
            operation.participants.insert(node_id, identity.clone());
            operation.recovering_processes.remove(&node_id);
            operation.evidence.clear();
        }
        Ok(OperationOutcome::ProcessClaimed(identity))
    }

    fn begin(
        &mut self,
        kind: OperationKind,
        executor: ProcessIncarnation,
        participants: BTreeMap<NodeId, ProcessIdentity>,
        meta_voters: BTreeSet<NodeId>,
        nodes: &BTreeSet<NodeId>,
        placements: &BTreeMap<RaftGroupId, DataGroupPlacement>,
    ) -> Result<OperationOutcome, OperationError> {
        if self.active.is_some() {
            return Err(OperationError::Busy);
        }
        let source = kind.source();
        if !nodes.contains(&source) {
            return Err(OperationError::UnknownNode { node_id: source });
        }
        let hosted = placements
            .iter()
            .filter(|(_, placement)| placement.voters.contains(&source))
            .map(|(id, placement)| (*id, placement.voters.clone()))
            .collect::<BTreeMap<_, _>>();
        let previous = match &kind {
            OperationKind::MoveReplicas { target, groups, .. } => {
                if groups.is_empty()
                    || *target == source
                    || !nodes.contains(target)
                    || groups.iter().any(|group| !hosted.contains_key(group))
                {
                    return Err(OperationError::InventoryMismatch);
                }
                hosted
                    .into_iter()
                    .filter(|(group, _)| groups.contains(group))
                    .collect()
            }
            OperationKind::RebuildReplica { .. } => hosted,
            OperationKind::DecommissionNode { replacements, .. } => {
                if replacements.keys().copied().collect::<BTreeSet<_>>()
                    != hosted.keys().copied().collect()
                {
                    return Err(OperationError::InventoryMismatch);
                }
                hosted
            }
        };
        if previous.is_empty() && !matches!(kind, OperationKind::DecommissionNode { .. }) {
            return Err(OperationError::InventoryMismatch);
        }
        let mut desired = previous.clone();
        for (group, voters) in &mut desired {
            let target = match &kind {
                OperationKind::MoveReplicas { target, .. } => Some(*target),
                OperationKind::RebuildReplica { .. } => None,
                OperationKind::DecommissionNode { replacements, .. } => {
                    replacements.get(group).copied()
                }
            };
            if let Some(target) = target {
                if !nodes.contains(&target) || voters.contains(&target) {
                    return Err(OperationError::InventoryMismatch);
                }
                voters.remove(&source);
                voters.insert(target);
            }
        }
        let mut required = previous
            .values()
            .chain(desired.values())
            .flatten()
            .copied()
            .collect::<BTreeSet<_>>();
        required.insert(source);
        if meta_voters.is_empty() || !meta_voters.is_subset(nodes) {
            return Err(OperationError::InventoryMismatch);
        }
        if participants.keys().copied().collect::<BTreeSet<_>>() != required {
            return Err(OperationError::InventoryMismatch);
        }
        for (node_id, identity) in &participants {
            if !self.accepts_process(*node_id, identity) {
                return Err(OperationError::ProcessChanged { node_id: *node_id });
            }
        }
        let operation_id = self
            .last_operation_id
            .checked_add(1)
            .ok_or(OperationError::EpochExhausted)?;
        let token = OperationToken {
            operation_id,
            generation: 1,
            executor,
        };
        self.active = Some(MaintenanceOperation {
            token: token.clone(),
            kind,
            phase: OperationPhase::Preparing,
            participants,
            meta_voters,
            previous,
            desired,
            evidence: BTreeMap::new(),
            prefix_floor: BTreeMap::new(),
            pending_action: None,
            last_action_sequence: 0,
            recovering_processes: BTreeSet::new(),
        });
        self.last_operation_id = operation_id;
        Ok(OperationOutcome::Acquired(token))
    }

    fn observe(
        &mut self,
        token: &OperationToken,
        evidence: PrefixEvidence,
        now_ms: u64,
    ) -> Result<OperationOutcome, OperationError> {
        let processes = &self.processes;
        let operation = self
            .active
            .as_mut()
            .filter(|operation| &operation.token == token)
            .ok_or(OperationError::StaleExecutor)?;
        let group = evidence.raft_group_id;
        if !operation.previous.contains_key(&group)
            || evidence.joint
            || !evidence.voters.contains(&evidence.leader)
            || evidence.observed_at_ms > now_ms
            || now_ms.saturating_sub(evidence.observed_at_ms) > EVIDENCE_MAX_AGE_MS
            || evidence.committed_index < operation.prefix_floor.get(&group).copied().unwrap_or(0)
        {
            return Err(OperationError::MissingEvidence {
                raft_group_id: group,
            });
        }
        for (node_id, replica) in &evidence.replicas {
            if operation.participants.get(node_id) != Some(&replica.process)
                || processes.get(node_id) != Some(&ProcessState::Active(replica.process.clone()))
                || replica.applied_index < evidence.committed_index
            {
                return Err(OperationError::ProcessChanged { node_id: *node_id });
            }
        }
        operation
            .prefix_floor
            .insert(group, evidence.committed_index);
        operation.evidence.insert(group, evidence);
        Ok(OperationOutcome::EvidenceRecorded)
    }

    fn require_evidence(
        operation: &MaintenanceOperation,
        now_ms: u64,
        retiring: bool,
    ) -> Result<(), OperationError> {
        for (group, desired) in &operation.desired {
            let missing = || OperationError::MissingEvidence {
                raft_group_id: *group,
            };
            let evidence = operation.evidence.get(group).ok_or_else(missing)?;
            if now_ms.saturating_sub(evidence.observed_at_ms) > EVIDENCE_MAX_AGE_MS {
                return Err(missing());
            }
            let required =
                if retiring && matches!(operation.kind, OperationKind::DecommissionNode { .. }) {
                    // Drain a node only after its replacements are already voters
                    // and have applied a fresh committed prefix in every group.
                    if &evidence.voters != desired {
                        return Err(missing());
                    }
                    desired.clone()
                } else if retiring {
                    let previous = operation.previous.get(group).ok_or_else(missing)?;
                    let survivors = previous
                        .iter()
                        .filter(|id| **id != operation.kind.source())
                        .copied()
                        .collect::<BTreeSet<_>>();
                    if &evidence.voters != previous
                        && (!matches!(operation.kind, OperationKind::RebuildReplica { .. })
                            || evidence.voters != survivors)
                    {
                        return Err(missing());
                    }
                    if survivors.len() <= previous.len() / 2 {
                        return Err(missing());
                    }
                    survivors
                } else {
                    if &evidence.voters != desired {
                        return Err(missing());
                    }
                    desired.clone()
                };
            if !required.iter().all(|id| evidence.replicas.contains_key(id)) {
                return Err(missing());
            }
        }
        Ok(())
    }

    fn retire(
        &mut self,
        token: &OperationToken,
        now_ms: u64,
    ) -> Result<OperationOutcome, OperationError> {
        let operation = self.authorized(token)?;
        if operation.phase != OperationPhase::Preparing
            || matches!(operation.kind, OperationKind::MoveReplicas { .. })
        {
            return Err(OperationError::InvalidTransition);
        }
        if operation.pending_action.is_some() {
            return Err(OperationError::Busy);
        }
        Self::require_evidence(operation, now_ms, true)?;
        let source = operation.kind.source();
        let epoch = operation
            .participants
            .get(&source)
            .ok_or(OperationError::InventoryMismatch)?
            .epoch;
        let reason = if matches!(operation.kind, OperationKind::DecommissionNode { .. }) {
            RetirementReason::Decommission
        } else {
            RetirementReason::Rebuild
        };
        operation.phase = OperationPhase::Retired;
        operation.evidence.clear();
        self.processes
            .insert(source, ProcessState::Retired { epoch, reason });
        if let Some(ReplicaState::Active { identity, .. }) = self.replicas.get(&source) {
            self.replicas
                .insert(source, ReplicaState::Retired(identity.clone()));
        }
        Ok(OperationOutcome::SourceRetired)
    }

    fn complete(
        &mut self,
        token: &OperationToken,
        now_ms: u64,
        placements: &mut BTreeMap<RaftGroupId, DataGroupPlacement>,
    ) -> Result<OperationOutcome, OperationError> {
        if let Some(operation) = &self.active
            && let OperationKind::RebuildReplica { node_id } = operation.kind
            && self
                .replicas
                .get(&node_id)
                .is_some_and(|state| !matches!(state, ReplicaState::Active { .. }))
        {
            return Err(OperationError::ReplicaChanged { node_id });
        }
        let operation = self.authorized(token)?;
        if !matches!(operation.kind, OperationKind::MoveReplicas { .. })
            && operation.phase != OperationPhase::Retired
        {
            return Err(OperationError::InvalidTransition);
        }
        if operation.pending_action.is_some() {
            return Err(OperationError::Busy);
        }
        Self::require_evidence(operation, now_ms, false)?;
        // Validate every epoch before changing any placement.
        for group in operation.desired.keys() {
            placements
                .get(group)
                .ok_or(OperationError::InventoryMismatch)?
                .epoch
                .checked_add(1)
                .ok_or(OperationError::EpochExhausted)?;
        }
        for (group, voters) in &operation.desired {
            if let Some(placement) = placements.get_mut(group) {
                placement.voters = voters.clone();
                placement.learners.retain(|node| !voters.contains(node));
                placement.draining.clear();
                placement.epoch = placement.epoch.saturating_add(1);
                placement.updated_at_ms = now_ms;
            }
        }
        self.active = None;
        Ok(OperationOutcome::Completed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(epoch: u64, node: u64) -> ProcessIdentity {
        ProcessIdentity {
            epoch,
            incarnation: ProcessIncarnation::from_bits(u128::from(node)),
        }
    }

    fn setup() -> (
        OperationState,
        BTreeSet<NodeId>,
        BTreeMap<RaftGroupId, DataGroupPlacement>,
    ) {
        let nodes = BTreeSet::from([1, 2, 3, 4]);
        let state = OperationState {
            processes: nodes
                .iter()
                .map(|id| (*id, ProcessState::Active(identity(1, *id))))
                .collect(),
            ..OperationState::default()
        };
        let placement = DataGroupPlacement {
            raft_group_id: RaftGroupId(0),
            voters: BTreeSet::from([1, 2, 3]),
            learners: BTreeSet::new(),
            draining: BTreeSet::new(),
            epoch: 1,
            updated_at_ms: 0,
        };
        (state, nodes, BTreeMap::from([(RaftGroupId(0), placement)]))
    }

    fn begin(
        state: &mut OperationState,
        nodes: &BTreeSet<NodeId>,
        placements: &mut BTreeMap<RaftGroupId, DataGroupPlacement>,
        kind: OperationKind,
        ids: &[u64],
    ) -> OperationToken {
        let command = OperationCommand::Begin {
            kind,
            executor: ProcessIncarnation::from_bits(99),
            participants: ids.iter().map(|id| (*id, identity(1, *id))).collect(),
            meta_voters: nodes.clone(),
        };
        let OperationOutcome::Acquired(token) =
            state.apply(command, 10, nodes, placements).unwrap()
        else {
            panic!("acquisition result");
        };
        token
    }

    fn evidence(voters: &[u64], replicas: &[u64], index: u64) -> PrefixEvidence {
        PrefixEvidence {
            raft_group_id: RaftGroupId(0),
            leader: 2,
            term: 1,
            committed_index: index,
            voters: voters.iter().copied().collect(),
            joint: false,
            replicas: replicas
                .iter()
                .map(|id| {
                    (*id, ReplicaEvidence {
                        process: identity(1, *id),
                        applied_index: index,
                        installed_replica_identities: BTreeMap::new(),
                    })
                })
                .collect(),
            observed_at_ms: 10,
        }
    }

    #[test]
    fn replica_fence_requires_every_survivor_not_only_a_majority() {
        let (mut state, mut nodes, mut placements) = setup();
        nodes.insert(5);
        state
            .processes
            .insert(5, ProcessState::Active(identity(1, 5)));
        placements.get_mut(&RaftGroupId(0)).unwrap().voters = BTreeSet::from([1, 2, 3, 4]);
        let admitted = ReplicaIdentity {
            generation: 1,
            incarnation: ProcessIncarnation::from_bits(55),
        };
        state
            .apply(
                OperationCommand::RegisterReplica {
                    node_id: 5,
                    process: identity(1, 5),
                    identity: admitted.clone(),
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap();
        let token = begin(
            &mut state,
            &nodes,
            &mut placements,
            OperationKind::MoveReplicas {
                source: 1,
                target: 5,
                groups: BTreeSet::from([RaftGroupId(0)]),
            },
            &[1, 2, 3, 4, 5],
        );
        let OperationOutcome::ActionPrepared(action) = state
            .apply(
                OperationCommand::PrepareAction {
                    token: token.clone(),
                    group: RaftGroupId(0),
                    leader: 2,
                    action: MembershipAction::InstallReplicaIdentity {
                        node_id: 5,
                        identity: admitted.clone(),
                    },
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap()
        else {
            panic!("action receipt");
        };
        for replicas in [&[1, 2, 3][..], &[1, 2, 3, 4][..]] {
            let mut proof = evidence(&[1, 2, 3, 4], replicas, 100);
            for replica in proof.replicas.values_mut() {
                replica
                    .installed_replica_identities
                    .insert(5, admitted.clone());
            }
            state
                .apply(
                    OperationCommand::Observe {
                        token: token.clone(),
                        evidence: proof,
                    },
                    10,
                    &nodes,
                    &mut placements,
                )
                .unwrap();
            let finished = state.apply(
                OperationCommand::FinishReplicaFence {
                    token: token.clone(),
                    sequence: action.sequence,
                    committed_index: 100,
                },
                10,
                &nodes,
                &mut placements,
            );
            if replicas.len() == 3 {
                assert!(matches!(
                    finished,
                    Err(OperationError::MissingEvidence {
                        raft_group_id: RaftGroupId(0),
                    })
                ));
                assert_eq!(
                    state.active.as_ref().unwrap().pending_action.as_ref(),
                    Some(&action)
                );
                assert!(matches!(state.replicas.get(&5), Some(ReplicaState::Active {
                    installed_groups, ..
                }) if installed_groups.is_empty()));
            } else {
                assert_eq!(finished.unwrap(), OperationOutcome::ActionFinished);
                assert!(matches!(state.replicas.get(&5), Some(ReplicaState::Active {
                    installed_groups, ..
                }) if installed_groups.get(&RaftGroupId(0)) == Some(&100)));
            }
        }
    }

    #[test]
    fn rebuild_retirement_requires_survivors_and_fences_the_old_process() {
        let (mut state, nodes, mut placements) = setup();
        let token = begin(
            &mut state,
            &nodes,
            &mut placements,
            OperationKind::RebuildReplica { node_id: 1 },
            &[1, 2, 3],
        );
        let retire = OperationCommand::RetireSource {
            token: token.clone(),
        };
        assert!(matches!(
            state.apply(retire.clone(), 10, &nodes, &mut placements),
            Err(OperationError::MissingEvidence { .. })
        ));
        state
            .apply(
                OperationCommand::Observe {
                    token: token.clone(),
                    evidence: evidence(&[1, 2, 3], &[2, 3], 100),
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap();
        let OperationOutcome::ActionPrepared(receipt) = state
            .apply(
                OperationCommand::PrepareAction {
                    token: token.clone(),
                    group: RaftGroupId(0),
                    leader: 2,
                    action: MembershipAction::RetireReplica,
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap()
        else {
            panic!("retire receipt");
        };
        state
            .apply(
                OperationCommand::FinishAction {
                    token: token.clone(),
                    sequence: receipt.sequence,
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap();
        state
            .apply(
                OperationCommand::Observe {
                    token: token.clone(),
                    evidence: evidence(&[2, 3], &[2, 3], 101),
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap();
        state.apply(retire, 10, &nodes, &mut placements).unwrap();
        assert!(!state.accepts_process(1, &identity(1, 1)));
        assert!(matches!(
            state.apply(
                OperationCommand::ClaimProcess {
                    node_id: 2,
                    expected_epoch: 1,
                    incarnation: ProcessIncarnation::from_bits(22)
                },
                11,
                &nodes,
                &mut placements
            ),
            Err(OperationError::Busy)
        ));
        let OperationOutcome::ProcessClaimed(replacement) = state
            .apply(
                OperationCommand::ClaimProcess {
                    node_id: 1,
                    expected_epoch: 1,
                    incarnation: ProcessIncarnation::from_bits(11),
                },
                11,
                &nodes,
                &mut placements,
            )
            .unwrap()
        else {
            panic!("claim result");
        };
        assert_eq!(replacement.epoch, 2);
        assert!(state.accepts_process(1, &replacement));
        let mut proof = evidence(&[1, 2, 3], &[1, 2, 3], 101);
        proof.replicas.get_mut(&1).unwrap().process = replacement;
        state
            .apply(
                OperationCommand::Observe {
                    token: token.clone(),
                    evidence: proof,
                },
                11,
                &nodes,
                &mut placements,
            )
            .unwrap();
        state
            .apply(
                OperationCommand::Complete { token },
                11,
                &nodes,
                &mut placements,
            )
            .unwrap();
        assert!(state.active.is_none());
        assert_eq!(placements[&RaftGroupId(0)].epoch, 2);
    }

    #[test]
    fn takeover_invalidates_executor_and_evidence_but_retains_prefix_floor() {
        let (mut state, nodes, mut placements) = setup();
        let token = begin(
            &mut state,
            &nodes,
            &mut placements,
            OperationKind::MoveReplicas {
                source: 1,
                target: 4,
                groups: BTreeSet::from([RaftGroupId(0)]),
            },
            &[1, 2, 3, 4],
        );
        state
            .apply(
                OperationCommand::Observe {
                    token: token.clone(),
                    evidence: evidence(&[2, 3, 4], &[2, 3, 4], 100),
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap();
        let OperationOutcome::Acquired(next) = state
            .apply(
                OperationCommand::TakeOver {
                    expected: token.clone(),
                    executor: ProcessIncarnation::from_bits(100),
                },
                11,
                &nodes,
                &mut placements,
            )
            .unwrap()
        else {
            panic!("takeover result");
        };
        assert_eq!(next.generation, 2);
        assert!(matches!(
            state.apply(
                OperationCommand::Complete { token },
                11,
                &nodes,
                &mut placements
            ),
            Err(OperationError::StaleExecutor)
        ));
        assert!(matches!(
            state.apply(
                OperationCommand::Complete {
                    token: next.clone()
                },
                11,
                &nodes,
                &mut placements
            ),
            Err(OperationError::MissingEvidence { .. })
        ));
        assert!(matches!(
            state.apply(
                OperationCommand::Observe {
                    token: next.clone(),
                    evidence: evidence(&[2, 3, 4], &[2, 3, 4], 99)
                },
                11,
                &nodes,
                &mut placements
            ),
            Err(OperationError::MissingEvidence { .. })
        ));
        state
            .apply(
                OperationCommand::Observe {
                    token: next.clone(),
                    evidence: evidence(&[2, 3, 4], &[2, 3, 4], 101),
                },
                11,
                &nodes,
                &mut placements,
            )
            .unwrap();
        state
            .apply(
                OperationCommand::Complete { token: next },
                11,
                &nodes,
                &mut placements,
            )
            .unwrap();
        assert_eq!(
            placements[&RaftGroupId(0)].voters,
            BTreeSet::from([2, 3, 4])
        );
        assert!(
            state.accepts_process(1, &identity(1, 1)),
            "moving replicas does not retire the whole process"
        );
    }

    #[test]
    fn stale_joint_or_incomplete_proof_never_allows_retirement() {
        let (mut state, nodes, mut placements) = setup();
        let token = begin(
            &mut state,
            &nodes,
            &mut placements,
            OperationKind::RebuildReplica { node_id: 1 },
            &[1, 2, 3],
        );
        let mut proof = evidence(&[1, 2, 3], &[2], 100);
        proof.joint = true;
        assert!(matches!(
            state.apply(
                OperationCommand::Observe {
                    token: token.clone(),
                    evidence: proof
                },
                10,
                &nodes,
                &mut placements
            ),
            Err(OperationError::MissingEvidence { .. })
        ));
        state
            .apply(
                OperationCommand::Observe {
                    token: token.clone(),
                    evidence: evidence(&[1, 2, 3], &[2], 100),
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap();
        assert!(matches!(
            state.apply(
                OperationCommand::RetireSource {
                    token: token.clone()
                },
                10,
                &nodes,
                &mut placements
            ),
            Err(OperationError::MissingEvidence { .. })
        ));
        state
            .apply(
                OperationCommand::Observe {
                    token: token.clone(),
                    evidence: evidence(&[1, 2, 3], &[2, 3], 100),
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap();
        assert!(matches!(
            state.apply(
                OperationCommand::RetireSource { token },
                40_011,
                &nodes,
                &mut placements
            ),
            Err(OperationError::MissingEvidence { .. })
        ));
        assert!(state.accepts_process(1, &identity(1, 1)));
    }

    #[test]
    fn decommission_cannot_reclaim_retired_identity() {
        let (mut state, nodes, mut placements) = setup();
        let token = begin(
            &mut state,
            &nodes,
            &mut placements,
            OperationKind::DecommissionNode {
                node_id: 1,
                replacements: BTreeMap::from([(RaftGroupId(0), 4)]),
            },
            &[1, 2, 3, 4],
        );
        state
            .apply(
                OperationCommand::Observe {
                    token: token.clone(),
                    evidence: evidence(&[1, 2, 3], &[2, 3], 100),
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap();
        assert_eq!(
            state.apply(
                OperationCommand::RetireSource {
                    token: token.clone()
                },
                10,
                &nodes,
                &mut placements
            ),
            Err(OperationError::MissingEvidence {
                raft_group_id: RaftGroupId(0)
            })
        );
        state
            .apply(
                OperationCommand::Observe {
                    token: token.clone(),
                    evidence: evidence(&[2, 3, 4], &[2, 3, 4], 100),
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap();
        state
            .apply(
                OperationCommand::RetireSource {
                    token: token.clone(),
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap();
        state
            .apply(
                OperationCommand::Observe {
                    token: token.clone(),
                    evidence: evidence(&[2, 3, 4], &[2, 3, 4], 100),
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap();
        state
            .apply(
                OperationCommand::Complete { token },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap();
        assert!(matches!(
            state.apply(
                OperationCommand::ClaimProcess {
                    node_id: 1,
                    expected_epoch: 1,
                    incarnation: ProcessIncarnation::from_bits(11)
                },
                11,
                &nodes,
                &mut placements
            ),
            Err(OperationError::InvalidTransition)
        ));
    }
    #[test]
    fn unresolved_action_survives_takeover_and_blocks_completion() {
        let (mut state, nodes, mut placements) = setup();
        let token = begin(
            &mut state,
            &nodes,
            &mut placements,
            OperationKind::MoveReplicas {
                source: 1,
                target: 4,
                groups: BTreeSet::from([RaftGroupId(0)]),
            },
            &[1, 2, 3, 4],
        );
        let OperationOutcome::ActionPrepared(action) = state
            .apply(
                OperationCommand::PrepareAction {
                    token: token.clone(),
                    group: RaftGroupId(0),
                    leader: 2,
                    action: MembershipAction::AddLearner { node_id: 4 },
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap()
        else {
            panic!("action receipt");
        };
        let OperationOutcome::Acquired(next) = state
            .apply(
                OperationCommand::TakeOver {
                    expected: token.clone(),
                    executor: ProcessIncarnation::from_bits(100),
                },
                11,
                &nodes,
                &mut placements,
            )
            .unwrap()
        else {
            panic!("takeover");
        };
        assert_eq!(
            state.active.as_ref().unwrap().pending_action,
            Some(action.clone())
        );
        assert!(matches!(
            state.apply(
                OperationCommand::Complete {
                    token: next.clone()
                },
                11,
                &nodes,
                &mut placements
            ),
            Err(OperationError::Busy)
        ));
        assert!(matches!(
            state.apply(
                OperationCommand::FinishAction {
                    token,
                    sequence: action.sequence
                },
                11,
                &nodes,
                &mut placements
            ),
            Err(OperationError::StaleExecutor)
        ));
        state
            .apply(
                OperationCommand::FinishAction {
                    token: next,
                    sequence: action.sequence,
                },
                11,
                &nodes,
                &mut placements,
            )
            .unwrap();
        assert!(state.active.as_ref().unwrap().pending_action.is_none());
    }

    #[test]
    fn drained_action_reassigns_without_retiring_the_healthy_process() {
        let (mut state, nodes, mut placements) = setup();
        let token = begin(
            &mut state,
            &nodes,
            &mut placements,
            OperationKind::MoveReplicas {
                source: 1,
                target: 4,
                groups: BTreeSet::from([RaftGroupId(0)]),
            },
            &[1, 2, 3, 4],
        );
        let OperationOutcome::ActionPrepared(receipt) = state
            .apply(
                OperationCommand::PrepareAction {
                    token: token.clone(),
                    group: RaftGroupId(0),
                    leader: 2,
                    action: MembershipAction::AddLearner { node_id: 4 },
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap()
        else {
            panic!("receipt");
        };
        let mut wrong = receipt.clone();
        wrong.sequence = 100;
        assert!(matches!(
            state.apply(
                OperationCommand::ReassignAction {
                    token: token.clone(),
                    leader: 3,
                    drained: Some(wrong)
                },
                10,
                &nodes,
                &mut placements
            ),
            Err(OperationError::InvalidTransition)
        ));
        let OperationOutcome::ActionPrepared(next) = state
            .apply(
                OperationCommand::ReassignAction {
                    token,
                    leader: 3,
                    drained: Some(receipt.clone()),
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap()
        else {
            panic!("replacement receipt");
        };
        assert_eq!(next.action, receipt.action);
        assert_eq!(next.sequence, receipt.sequence.checked_add(1).unwrap());
        assert_eq!(next.leader, 3);
        assert!(state.accepts_process(2, &receipt.process));
    }

    #[test]
    fn lost_action_executor_is_fenced_before_receipt_reassignment() {
        let (mut state, nodes, mut placements) = setup();
        let token = begin(
            &mut state,
            &nodes,
            &mut placements,
            OperationKind::MoveReplicas {
                source: 1,
                target: 4,
                groups: BTreeSet::from([RaftGroupId(0)]),
            },
            &[1, 2, 3, 4],
        );
        let OperationOutcome::ActionPrepared(old) = state
            .apply(
                OperationCommand::PrepareAction {
                    token: token.clone(),
                    group: RaftGroupId(0),
                    leader: 2,
                    action: MembershipAction::AddLearner { node_id: 4 },
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap()
        else {
            panic!("receipt");
        };
        assert!(matches!(
            state.apply(
                OperationCommand::ReassignAction {
                    drained: None,
                    token: token.clone(),
                    leader: 3
                },
                10,
                &nodes,
                &mut placements
            ),
            Err(OperationError::Busy)
        ));
        assert!(matches!(
            state.apply(
                OperationCommand::RetireActionExecutor {
                    token: token.clone(),
                    drained: old.clone(),
                },
                10,
                &nodes,
                &mut placements
            ),
            Err(OperationError::MissingEvidence { .. })
        ));
        let mut proof = evidence(&[1, 2, 3], &[1, 3], 100);
        proof.leader = 3;
        state
            .apply(
                OperationCommand::Observe {
                    token: token.clone(),
                    evidence: proof,
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap();
        state
            .apply(
                OperationCommand::RetireActionExecutor {
                    token: token.clone(),
                    drained: old.clone(),
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap();
        assert!(!state.accepts_process(2, &identity(1, 2)));
        let OperationOutcome::ActionPrepared(next) = state
            .apply(
                OperationCommand::ReassignAction {
                    drained: None,
                    token: token.clone(),
                    leader: 3,
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap()
        else {
            panic!("reassigned receipt");
        };
        assert!(next.sequence > old.sequence);
        assert!(matches!(
            state.apply(
                OperationCommand::FinishAction {
                    token: token.clone(),
                    sequence: old.sequence
                },
                10,
                &nodes,
                &mut placements
            ),
            Err(OperationError::InvalidTransition)
        ));
        let OperationOutcome::ProcessClaimed(restarted) = state
            .apply(
                OperationCommand::ClaimProcess {
                    node_id: 2,
                    expected_epoch: 1,
                    incarnation: ProcessIncarnation::from_bits(22),
                },
                11,
                &nodes,
                &mut placements,
            )
            .unwrap()
        else {
            panic!("recovered process");
        };
        assert_eq!(restarted.epoch, 2);
        assert!(state.accepts_process(2, &restarted));
        state
            .apply(
                OperationCommand::FinishAction {
                    token,
                    sequence: next.sequence,
                },
                11,
                &nodes,
                &mut placements,
            )
            .unwrap();
    }

    proptest::proptest! {
        #[test]
        fn rejected_commands_preserve_the_entire_replicated_state(actions in proptest::collection::vec(0_u8..7, 1..64)) {
            let (mut state, nodes, mut placements) = setup();
            let token = begin(&mut state, &nodes, &mut placements, OperationKind::RebuildReplica { node_id: 1 }, &[1, 2, 3]);
            for action in actions {
                let command = match action {
                    0 => OperationCommand::ClaimProcess { node_id: 2, expected_epoch: 1, incarnation: ProcessIncarnation::from_bits(100) },
                    1 => OperationCommand::RetireSource { token: token.clone() },
                    2 => OperationCommand::Complete { token: token.clone() },
                    3 => OperationCommand::RegisterReplica { node_id: 1, process: identity(1, 1), identity: ReplicaIdentity { generation: 2, incarnation: ProcessIncarnation::from_bits(100) } },
                    4 => OperationCommand::ActivateReplica { token: token.clone(), node_id: 1, identity: ReplicaIdentity { generation: 2, incarnation: ProcessIncarnation::from_bits(100) } },
                    5 => OperationCommand::FinishReplicaFence { token: token.clone(), sequence: 1, committed_index: 100 },
                    _ => OperationCommand::TakeOver { expected: OperationToken { generation: token.generation.saturating_add(1), ..token.clone() }, executor: ProcessIncarnation::from_bits(100) },
                };
                let before = state.clone();
                let before_placements = placements.clone();
                state.apply(command, 10, &nodes, &mut placements).expect_err("invalid transition");
                proptest::prop_assert_eq!(&state, &before);
                proptest::prop_assert_eq!(&placements, &before_placements);
            }
            let encoded = serde_json::to_vec(&state).unwrap();
            let restored: OperationState = serde_json::from_slice(&encoded).unwrap();
            proptest::prop_assert_eq!(state, restored);
        }
    }
}

/// Thin adapters submit intent; prefix evidence is collected by the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationRequest {
    /// Register a fresh node and its non-voting meta replica before data moves.
    JoinNode {
        node_id: NodeId,
        client_url: String,
        cluster_url: String,
        meta_url: String,
    },
    RecoverAction {
        token: OperationToken,
    },
    Reconcile {
        token: OperationToken,
    },
    Begin {
        kind: OperationKind,
        executor: ProcessIncarnation,
    },
    TakeOver {
        expected: OperationToken,
        executor: ProcessIncarnation,
    },
    CollectEvidence {
        token: OperationToken,
    },
    RetireSource {
        token: OperationToken,
    },
    Complete {
        token: OperationToken,
    },
}

/// An unresolved effect is retained across executor takeover. Only the bound
/// data leader process may execute it; completion cannot pass this receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationAction {
    pub sequence: u64,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionRequest {
    pub token: OperationToken,
    pub action: OperationAction,
}

/// A drained receipt can be reassigned; an unclassified failure remains pending.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ActionRejection {
    Drained,
    LeadershipChanged { detail: String },
}
