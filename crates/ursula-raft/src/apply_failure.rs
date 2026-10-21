//! Fatal committed-application diagnostics; recovery replays the intact WAL.
use std::sync::Arc;
use std::sync::Mutex;

pub use ursula_proto::admin::ApplyFailure;
pub use ursula_proto::admin::ApplyFailureKind;
pub use ursula_runtime::ApplyStopKind;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_shard::RaftTarget;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ApplyError {
    #[error("committed application panicked: {message}")]
    Panic { message: String },
    #[error("committed application failed: {0}")]
    InvariantViolation(#[source] GroupEngineError),
}
impl ApplyError {
    pub(crate) fn kind(&self) -> ApplyStopKind {
        match self {
            Self::Panic { .. } => ApplyStopKind::Panic,
            Self::InvariantViolation(_) => ApplyStopKind::InvariantViolation,
        }
    }
}
#[derive(Debug, Default, Clone)]
pub(crate) struct ApplyHealth(Arc<Mutex<Option<StoppedApply>>>);

/// A group whose committed apply stopped: the entry it stopped at and why.
#[derive(Debug, Clone)]
pub(crate) struct StoppedApply {
    pub(crate) term: u64,
    pub(crate) index: u64,
    pub(crate) kind: ApplyStopKind,
    pub(crate) message: String,
    pub(crate) last_applied: Option<openraft::alias::LogIdOf<crate::types::UrsulaRaftTypeConfig>>,
}
impl StoppedApply {
    /// The stop as the group metrics show it, with the admin plane's form of
    /// its kind: the one conversion of [`ApplyStopKind`].
    pub(crate) fn failure(&self) -> ApplyFailure {
        ApplyFailure {
            term: self.term,
            index: self.index,
            kind: self.kind.into(),
            message: self.message.clone(),
        }
    }

    /// The refusal of every request to the stopped group.
    pub(crate) fn error(&self, group: RaftTarget) -> GroupEngineError {
        GroupEngineError::Infra(GroupInfraError::ApplyStopped {
            group,
            term: self.term,
            index: self.index,
            kind: self.kind,
        })
    }
}
impl ApplyHealth {
    pub(crate) fn failure(&self) -> Option<ApplyFailure> {
        self.stopped().map(|stopped| stopped.failure())
    }
    pub(crate) fn stopped(&self) -> Option<StoppedApply> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    /// Records the first stop; a later one leaves it.
    pub(crate) fn stop(&self, stopped: StoppedApply) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert(stopped);
    }
    pub(crate) fn check(&self, group: RaftTarget) -> Result<(), GroupEngineError> {
        match self.stopped() {
            Some(stopped) => Err(stopped.error(group)),
            None => Ok(()),
        }
    }
}

/// A deterministic application bug at one committed index, hit after the
/// command mutated state. Tests and simulations run it as faulty code.
#[cfg(any(test, madsim, feature = "fault-injection"))]
#[derive(Debug, Clone, Copy)]
pub enum ApplyFault {
    PanicAfterMutation { index: u64 },
    InvariantAfterMutation { index: u64 },
}

#[cfg(any(test, madsim, feature = "fault-injection"))]
impl ApplyFault {
    /// The invariant violation [`ApplyFault::InvariantAfterMutation`]
    /// injects: an applied `Append` that answered nothing, which no correct
    /// apply returns.
    pub const INJECTED_INVARIANT_VIOLATION: ursula_runtime::GroupInfraError =
        ursula_runtime::GroupInfraError::UnexpectedResponse {
            expected: ursula_runtime::ResponseKind::Append,
            actual: ursula_runtime::ResponseKind::Missing,
        };
}
