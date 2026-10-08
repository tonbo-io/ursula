//! Fatal committed-application diagnostics; recovery replays the intact WAL.
use std::sync::Arc;
use std::sync::Mutex;

use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_shard::RaftGroupId;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ApplyFailure {
    pub term: u64,
    pub index: u64,
    pub kind: ApplyFailureKind,
    pub message: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplyFailureKind {
    Panic,
    Infrastructure,
}
#[derive(Debug, thiserror::Error)]
pub(crate) enum ApplyError {
    #[error("committed application panicked: {message}")]
    Panic { message: String },
    #[error("committed application failed: {0}")]
    Infrastructure(#[source] GroupEngineError),
}
impl ApplyError {
    pub(crate) fn kind(&self) -> ApplyFailureKind {
        match self {
            Self::Panic { .. } => ApplyFailureKind::Panic,
            Self::Infrastructure(_) => ApplyFailureKind::Infrastructure,
        }
    }
}
#[derive(Debug, Default, Clone)]
pub(crate) struct ApplyHealth(Arc<Mutex<Option<ApplyFailure>>>);
impl ApplyHealth {
    pub(crate) fn failure(&self) -> Option<ApplyFailure> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    pub(crate) fn stop(&self, failure: ApplyFailure) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert(failure);
    }
    pub(crate) fn check(&self, raft_group_id: RaftGroupId) -> Result<(), GroupEngineError> {
        if self.failure().is_some() {
            Err(GroupEngineError::Infra(GroupInfraError::ApplyStopped {
                raft_group_id,
            }))
        } else {
            Ok(())
        }
    }
}
