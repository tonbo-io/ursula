//! Typed registry failures and their gRPC classification.
use ursula_shard::RaftGroupId;

#[derive(Debug, Clone, Copy)]
pub enum SnapshotStage {
    DecodePointer,
    Pin,
    Download,
    DecodeBody,
    EncodePointer,
    Publish,
}

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("Raft group {group:?} is shutting down")]
    ShuttingDown { group: RaftGroupId },
    #[error("Raft group {group:?} is not registered on this node")]
    NotRegistered { group: RaftGroupId },
    #[error("Raft group {group:?} has no persisted recovery vote floor for this leader")]
    RecoveryVoteFloor { group: RaftGroupId },
    #[error("Raft group {group:?} cannot accept leadership while its recovery gate is closed")]
    RecoveryGateClosed { group: RaftGroupId },
    #[error("OpenRaft request failed for group {group:?}: {source}")]
    Request {
        group: RaftGroupId,
        #[source]
        source: openraft::error::RaftError<crate::UrsulaRaftTypeConfig>,
    },
    #[error("OpenRaft group {group:?} stopped: {source}")]
    Raft {
        group: RaftGroupId,
        #[source]
        source: openraft::error::Fatal<crate::UrsulaRaftTypeConfig>,
    },
    #[error("snapshot {stage:?} failed for group {group:?}: {source}")]
    Snapshot {
        group: RaftGroupId,
        stage: SnapshotStage,
        #[source]
        source: ursula_runtime::SnapshotStoreError,
    },
    #[error("snapshot build failed for group {group:?}: {source}")]
    SnapshotIo {
        group: RaftGroupId,
        #[source]
        source: std::io::Error,
    },
    #[error("snapshot admission failed: {0}")]
    Admission(#[from] ursula_runtime::GroupEngineError),
    #[cfg(not(madsim))]
    #[error("snapshot install task stopped: {0}")]
    Task(#[source] tokio::task::JoinError),
    #[cfg(madsim)]
    #[error("snapshot install task stopped: {0}")]
    Task(#[source] sim_tokio::task::JoinError),
}

impl From<RegistryError> for tonic::Status {
    fn from(error: RegistryError) -> Self {
        let code = match &error {
            RegistryError::ShuttingDown { .. } => tonic::Code::Unavailable,
            RegistryError::NotRegistered { .. } => tonic::Code::NotFound,
            RegistryError::RecoveryVoteFloor { .. } | RegistryError::RecoveryGateClosed { .. } => {
                tonic::Code::FailedPrecondition
            }
            RegistryError::Snapshot {
                stage: SnapshotStage::DecodePointer | SnapshotStage::DecodeBody,
                ..
            } => tonic::Code::InvalidArgument,
            RegistryError::Snapshot { .. } | RegistryError::Admission(_) => {
                tonic::Code::Unavailable
            }
            RegistryError::Request { .. }
            | RegistryError::Raft { .. }
            | RegistryError::SnapshotIo { .. }
            | RegistryError::Task(_) => tonic::Code::Internal,
        };
        tonic::Status::new(code, error.to_string())
    }
}
