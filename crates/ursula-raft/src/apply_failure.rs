//! Fatal committed-application diagnostics; recovery replays the intact WAL.
use std::sync::Arc;
use std::sync::Mutex;

pub use ursula_proto::admin::ApplyFailure;
pub use ursula_proto::admin::ApplyFailureKind;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_shard::RaftGroupId;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ApplyError {
    #[error("committed application panicked: {message}")]
    Panic { message: String },
    #[error("committed application failed: {0}")]
    InvariantViolation(#[source] GroupEngineError),
}
impl ApplyError {
    pub(crate) fn kind(&self) -> ApplyFailureKind {
        match self {
            Self::Panic { .. } => ApplyFailureKind::Panic,
            Self::InvariantViolation(_) => ApplyFailureKind::InvariantViolation,
        }
    }
}
#[derive(Debug, Default, Clone)]
pub(crate) struct ApplyHealth(Arc<Mutex<Option<StoppedApply>>>);

#[derive(Debug, Clone)]
pub(crate) struct StoppedApply {
    pub(crate) failure: ApplyFailure,
    pub(crate) last_applied: Option<openraft::alias::LogIdOf<crate::types::UrsulaRaftTypeConfig>>,
}
impl ApplyHealth {
    pub(crate) fn from_stopped(stopped: StoppedApply) -> Self {
        Self(Arc::new(Mutex::new(Some(stopped))))
    }
    pub(crate) fn failure(&self) -> Option<ApplyFailure> {
        self.stopped().map(|stopped| stopped.failure)
    }
    pub(crate) fn stopped(&self) -> Option<StoppedApply> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    pub(crate) fn stop(
        &self,
        failure: ApplyFailure,
        last_applied: Option<openraft::alias::LogIdOf<crate::types::UrsulaRaftTypeConfig>>,
    ) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert(StoppedApply {
                failure,
                last_applied,
            });
    }
    pub(crate) fn check(&self, raft_group_id: RaftGroupId) -> Result<(), GroupEngineError> {
        if let Some(failure) = self.failure() {
            Err(GroupEngineError::Infra(GroupInfraError::ApplyStopped {
                raft_group_id,
                term: failure.term,
                index: failure.index,
                kind: failure.kind,
            }))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum ApplyFault {
    PanicAfterMutation { index: u64 },
    InvariantAfterMutation { index: u64 },
}
