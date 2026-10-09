use std::collections::BTreeMap;
use std::collections::BTreeSet;

use ursula_shard::RaftGroupId;

use super::command::OperationError;
use super::command::OperationOutcome;
use super::model::ActionSequence;
use super::model::ExecutorGeneration;
use super::model::MaintenanceOperation;
use super::model::OperationKind;
use super::model::OperationPhase;
use super::model::OperationState;
use super::model::OperationToken;
use super::model::ProcessIdentity;
use super::model::ProcessState;
use super::model::ReplicaState;
use super::model::RetirementReason;
use crate::DataGroupPlacement;
use crate::NodeId;
use crate::identity::ProcessIncarnation;
use crate::model::NodeStates;

impl OperationState {
    pub(super) fn begin(
        &mut self,
        kind: OperationKind,
        executor: ProcessIncarnation,
        participants: BTreeMap<NodeId, ProcessIdentity>,
        nodes: &NodeStates,
        placements: &BTreeMap<RaftGroupId, DataGroupPlacement>,
    ) -> Result<OperationOutcome, OperationError> {
        if self.active.is_some() {
            return Err(OperationError::Busy);
        }
        let source = kind.source();
        if !nodes.contains_key(&source) {
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
                    || !nodes.contains_key(target)
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
                if !nodes.contains_key(&target) || voters.contains(&target) {
                    return Err(OperationError::InventoryMismatch);
                }
                voters.remove(&source);
                voters.insert(target);
            }
        }
        // Every node that gains a replica must already have an admitted one:
        // replica registration is refused while an operation is active.
        let joining: BTreeSet<NodeId> = match &kind {
            OperationKind::MoveReplicas { target, .. } => BTreeSet::from([*target]),
            OperationKind::RebuildReplica { node_id } => BTreeSet::from([*node_id]),
            OperationKind::DecommissionNode { replacements, .. } => {
                replacements.values().copied().collect()
            }
        };
        for node_id in joining {
            // Sources may be in any state but `Removed`; new replicas go only to
            // nodes that accept them.
            let state = nodes
                .get(&node_id)
                .copied()
                .ok_or(OperationError::UnknownNode { node_id })?;
            if !state.accepts_new_replicas() {
                return Err(OperationError::IneligibleNode { node_id, state });
            }
            if !matches!(
                self.replicas.get(&node_id),
                Some(ReplicaState::Active { .. })
            ) {
                return Err(OperationError::InactiveReplica { node_id });
            }
        }
        let mut required = previous
            .values()
            .chain(desired.values())
            .flatten()
            .copied()
            .collect::<BTreeSet<_>>();
        required.insert(source);
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
            generation: ExecutorGeneration(1),
            executor,
        };
        self.active = Some(MaintenanceOperation {
            token: token.clone(),
            kind,
            phase: OperationPhase::Preparing,
            participants,
            previous,
            desired,
            evidence: BTreeMap::new(),
            prefix_floor: BTreeMap::new(),
            pending_action: None,
            last_action_sequence: ActionSequence(0),
            blocked: BTreeMap::new(),
        });
        self.last_operation_id = operation_id;
        Ok(OperationOutcome::Acquired(token))
    }

    pub(super) fn retire(
        &mut self,
        token: &OperationToken,
        now_ms: u64,
    ) -> Result<OperationOutcome, OperationError> {
        let operation = self.authorized(token)?;
        operation.ensure_unblocked()?;
        if !operation.phase.before_retirement()
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
        // Replacements are admitted only before retirement, so a decommission
        // that retired without them could never complete.
        if reason == RetirementReason::Decommission {
            let operation = self.active.as_ref().ok_or(OperationError::StaleExecutor)?;
            for group in operation.desired.keys() {
                self.require_replica_admissions(operation, *group)?;
            }
        }
        let operation = self.authorized(token)?;
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

    pub(super) fn complete(
        &mut self,
        token: &OperationToken,
        now_ms: u64,
        placements: &mut BTreeMap<RaftGroupId, DataGroupPlacement>,
    ) -> Result<OperationOutcome, OperationError> {
        let operation = self.authorized(token)?;
        operation.ensure_unblocked()?;
        if !matches!(operation.kind, OperationKind::MoveReplicas { .. })
            && operation.phase != OperationPhase::Retired
        {
            return Err(OperationError::InvalidTransition);
        }
        if operation.pending_action.is_some() {
            return Err(OperationError::Busy);
        }
        Self::require_evidence(operation, now_ms, false)?;
        let operation = self.active.as_ref().ok_or(OperationError::StaleExecutor)?;
        for group in operation.desired.keys() {
            self.require_replica_admissions(operation, *group)?;
        }
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
                placement.epoch = placement.epoch.saturating_add(1);
                placement.updated_at_ms = now_ms;
            }
        }
        self.active = None;
        Ok(OperationOutcome::Completed)
    }

    /// Discards a `Preparing` operation. Its pending action, if any, was
    /// either never dispatched or cannot change membership, so `previous`
    /// still describes every group and placement stays unchanged.
    pub(super) fn abort(
        &mut self,
        token: &OperationToken,
    ) -> Result<OperationOutcome, OperationError> {
        let operation = self.authorized(token)?;
        if operation.phase != OperationPhase::Preparing {
            return Err(OperationError::Irreversible {
                phase: operation.phase,
            });
        }
        self.active = None;
        Ok(OperationOutcome::Aborted)
    }
}
