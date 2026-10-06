//! Durable node-local control authority and replica retirement tombstones.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_proto::admin::ProcessIncarnation;
use ursula_shard::RaftGroupId;

use crate::CommittedGroupConfiguration;
use crate::MembershipLogId;
use crate::MigrationToken;
use crate::ReceiverProcess;
use crate::ReplicaRetirementEvidence;

/// Immutable description of possibly admitted work. Legacy opaque membership
/// remains fail-closed; typed steps reconcile actual committed configuration
/// rather than inferring completion from a queue barrier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ReceiverMutationKind {
    Membership,
    ManagedMembership {
        step: MembershipStep,
    },
    PrepareReplica {
        epoch: u64,
    },
    ReleaseReplica {
        epoch: u64,
        membership_log_id: MembershipLogId,
    },
}

/// Exact logical membership action authorized by the immutable meta intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum MembershipStep {
    TransferLeader {
        epoch: u64,
        node_id: u64,
    },
    AddLearner {
        epoch: u64,
        node_id: u64,
        prefix: MembershipLogId,
    },
    ChangeVoters {
        epoch: u64,
        target_voters: BTreeSet<u64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipOutcome {
    Applied,
    /// A takeover observed committed state but did not resubmit the old action.
    /// The executor must continue from this configuration after recertification.
    Reconciled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedMembershipMutation {
    pub request: PendingReceiverMutation,
    pub process: ReceiverProcess,
    pub configuration: CommittedGroupConfiguration,
    pub outcome: MembershipOutcome,
}

/// RF<=5 needs at most five learner actions, one voter change and a handoff.
/// Repeated failed/reconciled attempts consume this bounded generation budget;
/// the executor can obtain a new generation rather than discard old request IDs.
pub const MAX_MEMBERSHIP_RECEIPTS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReceiverFencePhase {
    Activating,
    Active,
    Retiring,
    Retired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiverFenceRecord {
    pub token: MigrationToken,
    pub process: ProcessIncarnation,
    pub phase: ReceiverFencePhase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaAssignmentPhase {
    Preparing,
    Hosted,
    Retiring,
    Retired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaAssignment {
    pub epoch: u64,
    pub migration_id: u64,
    pub generation: u64,
    pub phase: ReplicaAssignmentPhase,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingReceiverMutation {
    pub token: MigrationToken,
    pub raft_group_id: RaftGroupId,
    pub request_id: String,
    pub process: ProcessIncarnation,
    pub operation: ReceiverMutationKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaMutationResult {
    Prepared { process: ReceiverProcess },
    Released { evidence: ReplicaRetirementEvidence },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedReceiverMutation {
    pub request: PendingReceiverMutation,
    pub result: ReplicaMutationResult,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiverLedger {
    pub revision: u64,
    pub assignments_seeded: bool,
    pub high_water_generation: u64,
    pub fence: Option<ReceiverFenceRecord>,
    pub assignments: BTreeMap<RaftGroupId, ReplicaAssignment>,
    /// Written before submitting asynchronous work; unresolved work survives
    /// process replacement and cannot be cleared by an activation alone.
    pub pending: Option<PendingReceiverMutation>,
    /// One bounded receipt slot; old generations are rejected by admission.
    /// A lost response to the current operation replays the exact durable result.
    #[serde(default)]
    pub completed: Option<CompletedReceiverMutation>,
    #[serde(default)]
    pub membership_completed: BTreeMap<String, CompletedMembershipMutation>,
}

impl ReceiverLedger {
    pub fn validate(&self, group_count: u32) -> Result<(), String> {
        if !self.assignments_seeded && !self.assignments.is_empty() {
            return Err("replica assignments lack explicit bootstrap".to_owned());
        }
        if self.fence.as_ref().map_or(0, |f| f.token.generation) != self.high_water_generation
            || self.fence.as_ref().is_some_and(|f| {
                f.token.generation == 0
                    || f.token.migration_id == 0
                    || f.token.executor.node_id == 0
            })
        {
            return Err("receiver high water differs from its durable token".to_owned());
        }
        for (group, assignment) in &self.assignments {
            if group.0 >= group_count
                || assignment.generation > self.high_water_generation
                || (assignment.generation == 0) != (assignment.migration_id == 0)
            {
                return Err("invalid durable replica assignment".to_owned());
            }
            if assignment.generation != 0
                && assignment.generation == self.high_water_generation
                && self
                    .fence
                    .as_ref()
                    .is_none_or(|f| f.token.migration_id != assignment.migration_id)
            {
                return Err("replica assignment differs from current receiver intent".to_owned());
            }
        }
        if let Some(pending) = &self.pending {
            self.validate_request(pending, group_count)?;
            let fence = self
                .fence
                .as_ref()
                .ok_or("pending work lacks a receiver fence")?;
            if pending.token.generation == self.high_water_generation {
                if pending.token != fence.token
                    || pending.process != fence.process
                    || !matches!(
                        fence.phase,
                        ReceiverFencePhase::Active | ReceiverFencePhase::Activating
                    )
                {
                    return Err("pending work differs from its receiving process".to_owned());
                }
            } else if fence.phase != ReceiverFencePhase::Activating {
                return Err(
                    "older pending work requires an activation reconciliation barrier".to_owned(),
                );
            }
            self.validate_replica_assignment(pending, false)?;
        }
        if let Some(completed) = &self.completed {
            self.validate_request(&completed.request, group_count)?;
            let valid = match (&completed.request.operation, &completed.result) {
                (
                    ReceiverMutationKind::PrepareReplica { .. },
                    ReplicaMutationResult::Prepared { process },
                ) => process.node_id != 0 && process.incarnation == completed.request.process,
                (
                    ReceiverMutationKind::ReleaseReplica {
                        epoch,
                        membership_log_id,
                    },
                    ReplicaMutationResult::Released { evidence },
                ) => {
                    evidence.process.node_id != 0
                        && evidence.process.incarnation == completed.request.process
                        && evidence.placement_epoch == *epoch
                        && evidence.membership_log_id == *membership_log_id
                        && evidence.work_drained
                        && evidence.snapshot_references_retired
                        && evidence.local_records_reclaimed
                }
                _ => false,
            };
            if !valid {
                return Err("replica receipt differs from its immutable request".to_owned());
            }
            if completed.request.token.generation == self.high_water_generation {
                self.validate_replica_assignment(&completed.request, true)?;
            }
        }
        if self.membership_completed.len() > MAX_MEMBERSHIP_RECEIPTS {
            return Err("membership receipt generation budget exhausted".to_owned());
        }
        for (key, receipt) in &self.membership_completed {
            self.validate_request(&receipt.request, group_count)?;
            receipt.configuration.validate()?;
            let ReceiverMutationKind::ManagedMembership { step } = &receipt.request.operation
            else {
                return Err("membership receipt has no typed membership action".to_owned());
            };
            if key != &receipt.request.request_id
                || receipt.request.token.generation != self.high_water_generation
                || receipt.process.node_id == 0
                || receipt.process.incarnation != receipt.request.process
                || receipt.configuration.raft_group_id != receipt.request.raft_group_id
                || self.completed.as_ref().is_some_and(|replica| {
                    replica.request.token == receipt.request.token
                        && replica.request.request_id == *key
                })
            {
                return Err(
                    "membership receipt differs from receiving-process authority".to_owned(),
                );
            }
            if receipt.outcome == MembershipOutcome::Applied {
                let voters: BTreeSet<_> = receipt
                    .configuration
                    .voter_sets
                    .iter()
                    .flatten()
                    .copied()
                    .collect();
                let valid = match step {
                    MembershipStep::TransferLeader { node_id, .. } => {
                        receipt.configuration.leader_id == *node_id
                    }
                    MembershipStep::AddLearner {
                        node_id, prefix, ..
                    } => {
                        (voters.contains(node_id)
                            || receipt.configuration.learners.contains(node_id))
                            && covers(&receipt.configuration.applied_log_id, prefix)
                    }
                    MembershipStep::ChangeVoters { target_voters, .. } => {
                        receipt.configuration.voter_sets.as_slice() == [target_voters.clone()]
                            && receipt.configuration.learners.is_empty()
                    }
                };
                if !valid {
                    return Err(
                        "membership action lacks its applied configuration result".to_owned()
                    );
                }
            }
        }
        Ok(())
    }

    fn validate_request(
        &self,
        request: &PendingReceiverMutation,
        group_count: u32,
    ) -> Result<(), String> {
        if request.raft_group_id.0 >= group_count
            || request.token.generation == 0
            || request.token.generation > self.high_water_generation
            || request.token.migration_id == 0
            || request.token.executor.node_id == 0
            || request.request_id.is_empty()
            || request.request_id.len() > 128
        {
            return Err("invalid receiver mutation description".to_owned());
        }
        if matches!(&request.operation, ReceiverMutationKind::ReleaseReplica { membership_log_id, .. } if membership_log_id.node_id == 0)
        {
            return Err("release witness has no membership leader".to_owned());
        }
        if let ReceiverMutationKind::ManagedMembership { step } = &request.operation {
            let valid = match step {
                MembershipStep::TransferLeader { node_id, .. } => *node_id != 0,
                MembershipStep::AddLearner {
                    node_id, prefix, ..
                } => *node_id != 0 && prefix.node_id != 0,
                MembershipStep::ChangeVoters { target_voters, .. } => {
                    matches!(target_voters.len(), 3 | 5) && !target_voters.contains(&0)
                }
            };
            if !valid {
                return Err("invalid typed membership action".to_owned());
            }
        }
        if request.token.generation == self.high_water_generation
            && !self
                .fence
                .as_ref()
                .is_some_and(|f| f.token == request.token && f.process == request.process)
        {
            return Err("current receipt or request differs from its process fence".to_owned());
        }
        Ok(())
    }

    fn validate_replica_assignment(
        &self,
        request: &PendingReceiverMutation,
        complete: bool,
    ) -> Result<(), String> {
        let phases = match (&request.operation, complete) {
            (
                ReceiverMutationKind::Membership | ReceiverMutationKind::ManagedMembership { .. },
                _,
            ) => return Ok(()),
            (ReceiverMutationKind::PrepareReplica { .. }, false) => &[
                ReplicaAssignmentPhase::Preparing,
                ReplicaAssignmentPhase::Hosted,
            ][..],
            (ReceiverMutationKind::PrepareReplica { .. }, true) => {
                &[ReplicaAssignmentPhase::Hosted][..]
            }
            (ReceiverMutationKind::ReleaseReplica { .. }, false) => &[
                ReplicaAssignmentPhase::Retiring,
                ReplicaAssignmentPhase::Retired,
            ][..],
            (ReceiverMutationKind::ReleaseReplica { .. }, true) => {
                &[ReplicaAssignmentPhase::Retired][..]
            }
        };
        let epoch = match request.operation {
            ReceiverMutationKind::PrepareReplica { epoch }
            | ReceiverMutationKind::ReleaseReplica { epoch, .. } => epoch,
            ReceiverMutationKind::Membership | ReceiverMutationKind::ManagedMembership { .. } => {
                return Ok(());
            }
        };
        if !self
            .assignments
            .get(&request.raft_group_id)
            .is_some_and(|assignment| {
                (assignment.epoch == epoch
                    || (matches!(
                        request.operation,
                        ReceiverMutationKind::PrepareReplica { .. }
                    ) && assignment.epoch > epoch))
                    && assignment.migration_id == request.token.migration_id
                    && assignment.generation == request.token.generation
                    && phases.contains(&assignment.phase)
            })
        {
            return Err("replica mutation differs from its durable assignment".to_owned());
        }
        Ok(())
    }

    /// Compare before disk publication. Generations/epochs never roll back;
    /// retirement is reusable only by an explicitly newer assignment intent.
    pub fn validate_successor(&self, next: &Self, group_count: u32) -> Result<(), String> {
        next.validate(group_count)?;
        if self.assignments_seeded && !next.assignments_seeded {
            return Err("receiver bootstrap cannot be reset".to_owned());
        }
        if next == self {
            return Ok(());
        }
        if next.high_water_generation < self.high_water_generation {
            return Err("receiver generation regressed".to_owned());
        }
        match (&self.pending, &next.pending) {
            (Some(old), Some(new)) if old != new => {
                if old.token.migration_id != new.token.migration_id
                    || old.raft_group_id != new.raft_group_id
                    || old.request_id != new.request_id
                    || old.operation != new.operation
                    || matches!(old.operation, ReceiverMutationKind::Membership)
                    || new.token.generation <= old.token.generation
                    || !next.fence.as_ref().is_some_and(|f| {
                        f.phase == ReceiverFencePhase::Activating
                            && f.token == new.token
                            && f.process == new.process
                    })
                {
                    return Err("pending work cannot be replaced by another operation".to_owned());
                }
            }
            (Some(old), None) => {
                let complete = match &old.operation {
                    ReceiverMutationKind::ManagedMembership { .. } => next
                        .membership_completed
                        .get(&old.request_id)
                        .is_some_and(|done| &done.request == old),
                    _ => next
                        .completed
                        .as_ref()
                        .is_some_and(|done| &done.request == old),
                };
                if !complete {
                    return Err("pending work cannot clear without its durable receipt".to_owned());
                }
                next.validate_replica_assignment(old, true)?;
            }
            (None, Some(new)) => {
                if matches!(
                    new.operation,
                    ReceiverMutationKind::ManagedMembership { .. }
                ) && (self.membership_completed.len() >= MAX_MEMBERSHIP_RECEIPTS
                    || self.membership_completed.contains_key(&new.request_id))
                {
                    return Err("membership receipt budget/key already consumed".to_owned());
                }
                if self.completed.as_ref().is_some_and(|done| {
                    done.request.token == new.token
                        && (done.request.request_id == new.request_id
                            || !matches!(
                                new.operation,
                                ReceiverMutationKind::ManagedMembership { .. }
                            ))
                        && done.request != *new
                }) || self
                    .membership_completed
                    .get(&new.request_id)
                    .is_some_and(|done| done.request != *new)
                {
                    return Err(
                        "one replica action per receiver generation; retry the original request"
                            .to_owned(),
                    );
                }
                if !next.fence.as_ref().is_some_and(|f| {
                    f.phase == ReceiverFencePhase::Active
                        && f.token == new.token
                        && f.process == new.process
                }) {
                    return Err("new work requires active receiving-process authority".to_owned());
                }
            }
            _ => {}
        }
        if self.completed != next.completed
            && !self.pending.as_ref().is_some_and(|pending| {
                next.pending.is_none()
                    && next
                        .completed
                        .as_ref()
                        .is_some_and(|done| &done.request == pending)
            })
        {
            return Err("receipt can change only when completing its pending operation".to_owned());
        }
        if next.high_water_generation == self.high_water_generation {
            for (key, old) in &self.membership_completed {
                if next.membership_completed.get(key) != Some(old) {
                    return Err(
                        "membership receipts cannot be forgotten or rewritten within a generation"
                            .to_owned(),
                    );
                }
            }
            for (key, new) in &next.membership_completed {
                if !self.membership_completed.contains_key(key)
                    && !(next.pending.is_none()
                        && self
                            .pending
                            .as_ref()
                            .is_some_and(|pending| &new.request == pending))
                {
                    return Err(
                        "membership receipt requires completion of its exact pending action"
                            .to_owned(),
                    );
                }
            }
        } else if !next.membership_completed.is_empty() {
            return Err(
                "new receiver generation must discard only its old-generation membership replies"
                    .to_owned(),
            );
        }
        if next.high_water_generation == self.high_water_generation {
            match (&self.fence, &next.fence) {
                (None, None) => {}
                (Some(old), Some(new)) if old.token == new.token && old.process == new.process => {
                    let valid = old.phase == new.phase
                        || matches!(
                            (old.phase, new.phase),
                            (ReceiverFencePhase::Activating, ReceiverFencePhase::Active)
                                | (ReceiverFencePhase::Active, ReceiverFencePhase::Retiring)
                                | (ReceiverFencePhase::Retiring, ReceiverFencePhase::Retired)
                        );
                    if !valid {
                        return Err(
                            "receiver authority cannot reopen or skip its barrier".to_owned()
                        );
                    }
                }
                _ => {
                    return Err(
                        "receiver token or process changed at the same generation".to_owned()
                    );
                }
            }
        } else if !next
            .fence
            .as_ref()
            .is_some_and(|f| f.phase == ReceiverFencePhase::Activating)
        {
            return Err("new generation must begin with a durable activation barrier".to_owned());
        }
        for (group, old) in &self.assignments {
            let new = next
                .assignments
                .get(group)
                .ok_or("replica tombstones cannot be deleted")?;
            if new.epoch < old.epoch || new.generation < old.generation {
                return Err("replica assignment authority regressed".to_owned());
            }
            if old.phase == ReplicaAssignmentPhase::Retired
                && new.phase != ReplicaAssignmentPhase::Retired
                && !(new.phase == ReplicaAssignmentPhase::Preparing
                    && new.generation > old.generation
                    && new.migration_id != old.migration_id
                    && next
                        .fence
                        .as_ref()
                        .is_some_and(|f| f.phase == ReceiverFencePhase::Active))
            {
                return Err("retired replica requires an explicit new prepare intent".to_owned());
            }
            if old.phase != new.phase
                && old.phase != ReplicaAssignmentPhase::Retired
                && !matches!(
                    (old.phase, new.phase),
                    (
                        ReplicaAssignmentPhase::Preparing,
                        ReplicaAssignmentPhase::Hosted
                    ) | (
                        ReplicaAssignmentPhase::Preparing,
                        ReplicaAssignmentPhase::Retiring
                    ) | (
                        ReplicaAssignmentPhase::Hosted,
                        ReplicaAssignmentPhase::Retiring
                    ) | (
                        ReplicaAssignmentPhase::Retiring,
                        ReplicaAssignmentPhase::Retired
                    )
                )
            {
                return Err("replica assignment cannot skip or reverse retirement".to_owned());
            }
        }
        for (group, new) in &next.assignments {
            if !self.assignments.contains_key(group)
                && self.assignments_seeded
                && !(new.phase == ReplicaAssignmentPhase::Preparing
                    && new.generation > 0
                    && new.generation == next.high_water_generation)
            {
                return Err("new replica requires explicit prepare authority".to_owned());
            }
        }
        Ok(())
    }

    /// An absent assignment is never permission to lazily create a managed
    /// replica. Bootstrap seeds existing assignments explicitly once.
    pub fn may_restore(&self, group: RaftGroupId) -> bool {
        self.assignments.get(&group).is_some_and(|assignment| {
            matches!(
                assignment.phase,
                ReplicaAssignmentPhase::Preparing | ReplicaAssignmentPhase::Hosted
            )
        })
    }
}

#[cfg(test)]
#[path = "receiver_tests.rs"]
mod tests;

fn covers(applied: &MembershipLogId, prefix: &MembershipLogId) -> bool {
    applied == prefix || (applied.index > prefix.index && applied.term >= prefix.term)
}
