use std::collections::BTreeMap;
use std::collections::BTreeSet;

use super::command::OperationError;
use super::command::OperationOutcome;
use super::model::OperationBlock;
use super::model::OperationKind;
use super::model::OperationPhase;
use super::model::OperationState;
use super::model::OperationToken;
use super::model::ProcessIdentity;
use super::model::ProcessState;
use super::model::ReplicaState;
use super::model::RetirementReason;
use crate::NodeId;
use crate::identity::ProcessIncarnation;
use crate::identity::ReplicaIdentity;
use crate::model::NodeStates;

impl OperationState {
    pub(super) fn restart_process(
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
            && operation
                .participants
                .get(&node_id)
                .is_some_and(|pinned| pinned != previous)
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
        if let Some(operation) = &mut self.active
            && let Some(participant) = operation.participants.get_mut(&node_id)
        {
            *participant = identity.clone();
            operation.evidence.clear();
            // Keep any unresolved action bound to its old process. Reassignment
            // must explicitly resolve that receipt; a reboot cannot erase it.
        }
        Ok(OperationOutcome::ProcessClaimed(identity))
    }

    pub(super) fn claim_process(
        &mut self,
        node_id: NodeId,
        expected_epoch: u64,
        incarnation: ProcessIncarnation,
        nodes: &NodeStates,
    ) -> Result<OperationOutcome, OperationError> {
        if !nodes.contains_key(&node_id) {
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
        // An active executor cannot silently refresh its action's process pins:
        // a node completion depends on that claims a new process instead of
        // restarting is recorded as blocking the operation.
        let blocked_pin = match &self.active {
            None => None,
            Some(operation) => {
                let rebuild = matches!(operation.kind, OperationKind::RebuildReplica { node_id: source } if source == node_id)
                    && operation.phase == OperationPhase::Retired;
                let retired = matches!(current, Some(ProcessState::Retired { .. }));
                let pending = matches!(
                    self.replicas.get(&node_id),
                    Some(ReplicaState::Pending { .. })
                );
                if rebuild && (retired || pending) {
                    None
                } else if operation.required_replicas().contains(&node_id) {
                    Some(
                        operation
                            .participants
                            .get(&node_id)
                            .cloned()
                            .ok_or(OperationError::InventoryMismatch)?,
                    )
                } else {
                    return Err(OperationError::Busy);
                }
            }
        };
        let identity = ProcessIdentity {
            epoch: expected_epoch
                .checked_add(1)
                .ok_or(OperationError::EpochExhausted)?,
            incarnation,
        };
        self.processes
            .insert(node_id, ProcessState::Active(identity.clone()));
        if let Some(operation) = &mut self.active {
            match blocked_pin {
                Some(pinned) => {
                    operation
                        .blocked
                        .insert(node_id, OperationBlock::ParticipantReplaced {
                            pinned,
                            claimed: identity.clone(),
                        });
                }
                None => {
                    operation.participants.insert(node_id, identity.clone());
                }
            }
            operation.evidence.clear();
        }
        Ok(OperationOutcome::ProcessClaimed(identity))
    }

    pub(super) fn register_replica(
        &mut self,
        node_id: NodeId,
        process: &ProcessIdentity,
        identity: ReplicaIdentity,
        nodes: &NodeStates,
    ) -> Result<OperationOutcome, OperationError> {
        if !nodes.contains_key(&node_id) {
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

    pub(super) fn activate_replica(
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
}
