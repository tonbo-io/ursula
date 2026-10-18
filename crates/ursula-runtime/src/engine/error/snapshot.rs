//! [`GroupSnapshotError`]: why a group snapshot that crosses the engine
//! boundary (a restore at engine open, [`crate::GroupEngine::install_snapshot`],
//! a snapshot build) failed. Install-path refusals that never leave the Raft
//! crate stay in its own install error.

use serde::Deserialize;
use serde::Serialize;
use ursula_stream::StreamSnapshotError;

use super::WriteEffect;
use crate::error::ErrorStatus;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum GroupSnapshotError {
    /// The snapshot's stream state does not restore.
    #[error("snapshot content does not restore")]
    InvalidContent(#[source] StreamSnapshotError),
}

impl GroupSnapshotError {
    /// See [`super::GroupEngineError::status`].
    pub fn status(&self) -> ErrorStatus {
        match self {
            Self::InvalidContent(_) => ErrorStatus::Persistent,
        }
    }

    /// See [`super::GroupEngineError::write_effect`].
    pub fn write_effect(&self) -> WriteEffect {
        match self {
            Self::InvalidContent(_) => WriteEffect::Unknown,
        }
    }

    /// See [`super::GroupEngineError::peer_backs_off`].
    pub fn peer_backs_off(&self) -> bool {
        match self {
            Self::InvalidContent(_) => false,
        }
    }
}
