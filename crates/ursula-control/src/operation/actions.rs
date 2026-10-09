use ursula_shard::RaftGroupId;

use super::command::OperationError;
use super::command::OperationOutcome;
use super::model::ActionOutcome;
use super::model::ActionSequence;
use super::model::MembershipAction;
use super::model::OperationAction;
use super::model::OperationKind;
use super::model::OperationPhase;
use super::model::OperationState;
use super::model::OperationToken;
use super::model::PendingAction;
use super::model::ProcessState;
use super::model::ReplicaState;
use crate::NodeId;

impl OperationState {
    pub(super) fn mark_action_dispatched(
        &mut self,
        token: OperationToken,
        sequence: ActionSequence,
    ) -> Result<OperationOutcome, OperationError> {
        let operation = self.authorized(&token)?;
        let pending = operation
            .pending_action
            .clone()
            .ok_or(OperationError::InvalidTransition)?;
        if pending.receipt().sequence != sequence {
            return Err(OperationError::InvalidTransition);
        }
        if matches!(pending, PendingAction::OutcomeUnknown(_)) {
            return Err(OperationError::ActionOutcomeUnknown { sequence });
        }
        if !self.accepts_process(pending.receipt().leader, &pending.receipt().process) {
            return Err(OperationError::ProcessChanged {
                node_id: pending.receipt().leader,
            });
        }
        self.authorized(&token)?.pending_action =
            Some(PendingAction::OutcomeUnknown(pending.receipt().clone()));
        Ok(OperationOutcome::ActionOutcome(ActionOutcome::Unknown))
    }

    pub(super) fn cancel_prepared_action(
        &mut self,
        token: OperationToken,
        sequence: ActionSequence,
    ) -> Result<OperationOutcome, OperationError> {
        let operation = self.authorized(&token)?;
        if !matches!(&operation.pending_action, Some(PendingAction::Prepared(receipt)) if receipt.sequence == sequence)
        {
            return Err(OperationError::InvalidTransition);
        }
        operation.pending_action = None;
        Ok(OperationOutcome::ActionOutcome(
            ActionOutcome::NotDispatched,
        ))
    }

    pub(super) fn reassign_action(
        &mut self,
        token: OperationToken,
        leader: NodeId,
        drained: Option<OperationAction>,
    ) -> Result<OperationOutcome, OperationError> {
        let old = self
            .authorized(&token)?
            .pending_action
            .as_ref()
            .map(PendingAction::receipt)
            .cloned()
            .ok_or(OperationError::InvalidTransition)?;
        if drained.as_ref().is_some_and(|receipt| receipt != &old) {
            return Err(OperationError::InvalidTransition);
        }
        if drained.is_none() {
            return Err(OperationError::ActionOutcomeUnknown {
                sequence: old.sequence,
            });
        }
        if matches!(old.action, MembershipAction::PrepareReplica) && leader != old.leader {
            return Err(OperationError::InventoryMismatch);
        }
        // The new leader must pass the same action policy as a fresh prepare.
        self.validate_replica_action(&token, old.group, leader, &old.action)?;
        let process = match self.processes.get(&leader) {
            Some(ProcessState::Active(identity)) => identity.clone(),
            _ => return Err(OperationError::ProcessChanged { node_id: leader }),
        };
        let operation = self.authorized(&token)?;
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
        operation.pending_action = Some(PendingAction::Prepared(receipt.clone()));
        Ok(OperationOutcome::ActionPrepared(receipt))
    }

    pub(super) fn prepare_action(
        &mut self,
        token: OperationToken,
        group: RaftGroupId,
        leader: NodeId,
        action: MembershipAction,
    ) -> Result<OperationOutcome, OperationError> {
        self.validate_replica_action(&token, group, leader, &action)?;
        let current_process = match self.processes.get(&leader) {
            Some(ProcessState::Active(identity)) => identity.clone(),
            _ => return Err(OperationError::ProcessChanged { node_id: leader }),
        };
        let operation = self.authorized(&token)?;
        if let Some(pending) = &operation.pending_action {
            let receipt = pending.receipt();
            if receipt.group == group && receipt.leader == leader && receipt.action == action {
                return Ok(match pending {
                    PendingAction::Prepared(receipt) => {
                        OperationOutcome::ActionPrepared(receipt.clone())
                    }
                    PendingAction::OutcomeUnknown(_) => {
                        OperationOutcome::ActionOutcome(ActionOutcome::Unknown)
                    }
                });
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
        if matches!(action, MembershipAction::PrepareReplica) && !desired.contains(&leader) {
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
        operation.pending_action = Some(PendingAction::Prepared(receipt.clone()));
        Ok(OperationOutcome::ActionPrepared(receipt))
    }

    pub(super) fn finish_action(
        &mut self,
        token: OperationToken,
        sequence: ActionSequence,
    ) -> Result<OperationOutcome, OperationError> {
        let operation = self.authorized(&token)?;
        if !operation.pending_action.as_ref().is_some_and(|pending| {
            let PendingAction::OutcomeUnknown(action) = pending else {
                return false;
            };
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
        Ok(OperationOutcome::ActionOutcome(ActionOutcome::Completed))
    }

    pub(super) fn validate_replica_action(
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
            MembershipAction::ChangeVoters => self.require_replica_admissions(operation, group)?,
            _ => {}
        }
        Ok(())
    }
    pub(super) fn require_replica_admissions(
        &self,
        operation: &super::model::MaintenanceOperation,
        group: RaftGroupId,
    ) -> Result<(), OperationError> {
        let previous = operation
            .previous
            .get(&group)
            .ok_or(OperationError::InventoryMismatch)?;
        let desired = operation
            .desired
            .get(&group)
            .ok_or(OperationError::InventoryMismatch)?;
        for node_id in desired {
            let replacement = matches!(operation.kind, OperationKind::RebuildReplica { node_id: source } if source == *node_id);
            if (replacement || !previous.contains(node_id))
                && !matches!(self.replicas.get(node_id), Some(ReplicaState::Active { installed_groups, .. }) if installed_groups.get(&group).is_some_and(|index| *index > 0))
            {
                return Err(OperationError::ReplicaChanged { node_id: *node_id });
            }
        }
        Ok(())
    }
}
