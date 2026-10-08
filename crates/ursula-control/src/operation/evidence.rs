use std::collections::BTreeSet;

use super::command::OperationError;
use super::command::OperationOutcome;
use super::model::ActionOutcome;
use super::model::ActionSequence;
use super::model::MaintenanceOperation;
use super::model::MembershipAction;
use super::model::OperationAction;
use super::model::OperationKind;
use super::model::OperationState;
use super::model::OperationToken;
use super::model::PendingAction;
use super::model::PrefixEvidence;
use super::model::ProcessState;
use super::model::ReplicaState;

pub(super) const EVIDENCE_MAX_AGE_MS: u64 = 30_000;

impl OperationState {
    pub(super) fn finish_replica_fence(
        &mut self,
        token: &OperationToken,
        sequence: ActionSequence,
        committed_index: u64,
        now_ms: u64,
    ) -> Result<OperationOutcome, OperationError> {
        let operation = self.authorized(token)?;
        let Some(OperationAction {
            group,
            sequence: expected_sequence,
            action: MembershipAction::InstallReplicaIdentity { node_id, identity },
            ..
        }) = operation
            .pending_action
            .as_ref()
            .and_then(|pending| match pending {
                PendingAction::OutcomeUnknown(receipt) => Some(receipt),
                PendingAction::Prepared(_) => None,
            })
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
        Ok(OperationOutcome::ActionOutcome(ActionOutcome::Completed))
    }

    pub(super) fn observe(
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

    pub(super) fn require_evidence(
        operation: &MaintenanceOperation,
        now_ms: u64,
        retiring: bool,
    ) -> Result<(), OperationError> {
        for (group, desired) in &operation.desired {
            let missing = || OperationError::MissingEvidence {
                raft_group_id: *group,
            };
            let evidence = operation.evidence.get(group).ok_or_else(missing)?;
            if evidence.observed_at_ms > now_ms
                || now_ms.saturating_sub(evidence.observed_at_ms) > EVIDENCE_MAX_AGE_MS
            {
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
}
