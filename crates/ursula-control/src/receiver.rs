//! Durable node-local control authority and replica retirement tombstones.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde::Serialize;
use ursula_proto::admin::ProcessIncarnation;
use ursula_shard::RaftGroupId;

use crate::MigrationToken;

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
        if self.pending.as_ref().is_some_and(|pending| {
            pending.raft_group_id.0 >= group_count
                || pending.token.generation == 0
                || pending.token.generation > self.high_water_generation
                || pending.request_id.is_empty()
                || pending.request_id.len() > 128
        }) {
            return Err("invalid unresolved receiver mutation".to_owned());
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
