//! Typed fatal application failures. A group stops without skipping its WAL record.

use ursula_runtime::GroupEngineError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ApplyError {
    #[error("committed application panicked: {message}")]
    Panic { message: String },
    #[error("committed application failed: {0}")]
    Infrastructure(#[source] GroupEngineError),
}

impl ApplyError {
    pub(crate) fn kind(&self) -> ursula_proto::admin::RaftApplyFailureKind {
        match self {
            Self::Panic { .. } => ursula_proto::admin::RaftApplyFailureKind::Panic,
            Self::Infrastructure(_) => ursula_proto::admin::RaftApplyFailureKind::Infrastructure,
        }
    }
}
