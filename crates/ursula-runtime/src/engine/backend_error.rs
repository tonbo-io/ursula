//! Transport-neutral infrastructure stages with locally preserved error sources.
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackendOperation {
    ValidateConfig,
    CheckInitialized,
    Initialize,
    WaitForLeader,
    RestoreSnapshot,
    CreateGroup,
    Shutdown,
    AccessStateMachine,
    BuildSnapshot,
    ColdIndex,
    RollbackColdIndex,
    ForwardRead,
    ForwardWrite,
    DecodeWire,
    Connect,
    Endpoint,
    WriteBatch,
    WriteBatchResponse,
    Write,
    ReadIndex,
    Vote,
    OpenJournal,
    OpenLog,
    AdmitSnapshotBuild,
    AdmitSnapshotInstall,
    AllowLogReversion,
}

/// The local source stays downcastable and keeps its complete error chain.
/// Crossing the wire necessarily projects it to diagnostic text; policy uses
/// the enclosing typed operation/variant, never this text. Equality follows
/// that same wire projection so local and decoded errors compare consistently.
#[derive(Debug, Clone)]
pub struct BackendErrorSource(Arc<dyn Error + Send + Sync>);

impl BackendErrorSource {
    pub fn new(error: impl Error + Send + Sync + 'static) -> Self {
        Self(Arc::new(error))
    }
}
impl fmt::Display for BackendErrorSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl Error for BackendErrorSource {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
impl PartialEq for BackendErrorSource {
    fn eq(&self, other: &Self) -> bool {
        self.to_string() == other.to_string()
    }
}
impl Eq for BackendErrorSource {}
impl Serialize for BackendErrorSource {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct RemoteError(String);
impl<'de> Deserialize<'de> for BackendErrorSource {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::new(RemoteError(String::deserialize(deserializer)?)))
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::BackendOperation;
    use crate::GroupEngineError;
    use crate::GroupInfraError;

    #[derive(Debug, thiserror::Error)]
    #[error("specific backend failure at index {index}")]
    struct LocalFailure {
        index: u64,
    }

    #[test]
    fn local_sources_survive_until_the_wire_boundary() {
        let error = GroupEngineError::backend(BackendOperation::Write, LocalFailure { index: 42 });
        let source = error
            .source()
            .expect("infra")
            .source()
            .expect("backend wrapper")
            .source()
            .expect("original source");
        assert_eq!(
            source
                .downcast_ref::<LocalFailure>()
                .expect("typed original")
                .index,
            42
        );
        let encoded = serde_json::to_vec(&error).expect("wire error");
        let decoded: GroupEngineError = serde_json::from_slice(&encoded).expect("remote error");
        assert_eq!(decoded, error);
        assert!(matches!(
            decoded,
            GroupEngineError::Infra(GroupInfraError::Backend {
                operation: BackendOperation::Write,
                ..
            })
        ));
    }
}
