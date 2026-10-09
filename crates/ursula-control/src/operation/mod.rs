//! Pure maintenance operation transitions. Inputs carry externally verified
//! observations; this module performs no I/O and does not authenticate peers.
//!
//! Module map:
//! - `model`: operation, process, replica and evidence state.
//! - `command`: typed inputs, outcomes and rejection reasons.
//! - `actions`: durable action ownership and recovery.
//! - `process`: process claims and replica admission state.
//! - `evidence`: committed-prefix and all-survivor checks.
//! - `placement`: begin, retire, complete and abort transitions.
//!
//! Recovery boundary: an operation is reversible only while it is
//! `Preparing`, that is before any membership transition (`AddLearner`,
//! `ChangeVoters`, `RetireReplica`) is dispatched. `Abort` then discards the
//! intent and any pending action, because nothing it leaves behind can change
//! membership. Once a membership transition is dispatched (`Reconfiguring`) or
//! the source retires (`Retired`), `Abort` is refused with
//! `OperationError::Irreversible` and recovery reconciles forward. This model
//! has no reverse membership transition. A node whose replica completion
//! depends on, and that claims a new process instead of restarting with its
//! pinned one, is recorded in `MaintenanceOperation::blocked`. A blocked
//! operation dispatches nothing new and cannot retire or complete. Before the
//! point of no return it can be aborted. After it, it stays blocked until a
//! later model can rebuild the lost participant within the operation.

mod actions;
mod command;
mod evidence;
#[cfg(test)]
mod lifecycle_tests;
mod model;
mod placement;
mod process;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

pub use command::OperationCommand;
pub use command::OperationError;
pub use command::OperationOutcome;
pub use model::ActionOutcome;
pub use model::MaintenanceOperation;
pub use model::MembershipAction;
pub use model::OperationAction;
pub use model::OperationBlock;
pub use model::OperationKind;
pub use model::OperationPhase;
pub use model::OperationState;
pub use model::OperationToken;
pub use model::PendingAction;
pub use model::PrefixEvidence;
pub use model::ProcessIdentity;
pub use model::ProcessState;
pub use model::ReplicaEvidence;
pub use model::ReplicaState;
pub use model::RetirementReason;
use ursula_shard::RaftGroupId;

use crate::DataGroupPlacement;
use crate::NodeId;
use crate::identity::ProcessIncarnation;
use crate::model::NodeStates;

impl OperationState {
    pub fn accepts_process(&self, node_id: NodeId, identity: &ProcessIdentity) -> bool {
        self.processes.get(&node_id) == Some(&ProcessState::Active(identity.clone()))
    }

    pub(crate) fn apply(
        &mut self,
        command: OperationCommand,
        now_ms: u64,
        nodes: &NodeStates,
        placements: &mut BTreeMap<RaftGroupId, DataGroupPlacement>,
    ) -> Result<OperationOutcome, OperationError> {
        match command {
            OperationCommand::MarkActionDispatched { token, sequence } => {
                self.mark_action_dispatched(token, sequence)
            }
            OperationCommand::CancelPreparedAction { token, sequence } => {
                self.cancel_prepared_action(token, sequence)
            }
            OperationCommand::ReassignAction {
                token,
                leader,
                drained,
            } => self.reassign_action(token, leader, drained),
            OperationCommand::PrepareAction {
                token,
                group,
                leader,
                action,
            } => self.prepare_action(token, group, leader, action),
            OperationCommand::FinishAction { token, sequence } => {
                self.finish_action(token, sequence)
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
            } => self.begin(kind, executor, participants, nodes, placements),
            OperationCommand::TakeOver { expected, executor } => self.take_over(expected, executor),
            OperationCommand::Observe { token, evidence } => self.observe(&token, evidence, now_ms),
            OperationCommand::RetireSource { token } => self.retire(&token, now_ms),
            OperationCommand::Complete { token } => self.complete(&token, now_ms, placements),
            OperationCommand::Abort { token } => self.abort(&token),
        }
    }

    pub(super) fn authorized(
        &mut self,
        token: &OperationToken,
    ) -> Result<&mut MaintenanceOperation, OperationError> {
        self.active
            .as_mut()
            .filter(|operation| &operation.token == token)
            .ok_or(OperationError::StaleExecutor)
    }

    pub(super) fn take_over(
        &mut self,
        expected: OperationToken,
        executor: ProcessIncarnation,
    ) -> Result<OperationOutcome, OperationError> {
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
}

pub use model::ActionSequence;
pub use model::ExecutorGeneration;
pub use model::OperationId;
