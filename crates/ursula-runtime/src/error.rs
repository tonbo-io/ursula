use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardMapError;
use ursula_shard::ShardPlacement;
use ursula_stream::StreamErrorCode;
use ursula_stream::StreamErrorContext;

use crate::engine::GroupEngineError;
use crate::engine::GroupLeaderHint;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorStatus {
    Permanent,
    Temporary,
    // Persistent is reserved for non-retryable service-side failures. HTTP currently
    // treats it like Permanent; keeping it distinct leaves room for logging/alerting.
    Persistent,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RuntimeError {
    #[error("invalid shard runtime config: {0}")]
    InvalidConfig(#[from] ShardMapError),
    #[error("raft group {} is outside configured range 0..{raft_group_count}", .raft_group_id.0)]
    InvalidRaftGroup {
        raft_group_id: RaftGroupId,
        raft_group_count: u32,
    },
    #[error(
        "snapshot placement for raft group {} is core {}, expected core {}",
        .actual.raft_group_id.0,
        .actual.core_id.0,
        .expected.core_id.0
    )]
    SnapshotPlacementMismatch {
        expected: ShardPlacement,
        actual: ShardPlacement,
    },
    #[error("append payload must be non-empty")]
    EmptyAppend,
    #[error("invalid cold store config: {message}")]
    ColdStoreConfig { message: String },
    #[error("invalid static Raft membership config: {message}")]
    StaticMembershipConfig { message: String },
    #[error("cold store IO error: {message}")]
    ColdStoreIo { message: String },
    #[error(
        "core {} live read waiters at {current_waiters} would exceed limit {limit}",
        .core_id.0
    )]
    LiveReadBackpressure {
        core_id: CoreId,
        current_waiters: u64,
        limit: u64,
    },
    #[error("core {} does not host raft group {}", .core_id.0, .raft_group_id.0)]
    GroupNotHosted {
        core_id: CoreId,
        raft_group_id: RaftGroupId,
    },
    #[error(
        "core {} raft group {} operation failed: {}",
        .core_id.0,
        .raft_group_id.0,
        .error.message()
    )]
    GroupEngine {
        core_id: CoreId,
        raft_group_id: RaftGroupId,
        error: GroupEngineError,
    },
    #[error("core {} mailbox is closed", .core_id.0)]
    MailboxClosed { core_id: CoreId },
    #[error("core {} dropped append response", .core_id.0)]
    ResponseDropped { core_id: CoreId },
    #[error("failed to spawn core {} thread: {message}", .core_id.0)]
    SpawnCoreThread { core_id: CoreId, message: String },
}

impl RuntimeError {
    pub(crate) fn group_engine(placement: ShardPlacement, err: GroupEngineError) -> Self {
        Self::GroupEngine {
            core_id: placement.core_id,
            raft_group_id: placement.raft_group_id,
            error: err,
        }
    }

    pub fn stream_error_code(&self) -> Option<StreamErrorCode> {
        match self {
            Self::GroupEngine { error, .. } => error.code(),
            _ => None,
        }
    }

    pub fn stream_next_offset(&self) -> Option<u64> {
        match self {
            Self::GroupEngine { error, .. } => error.next_offset(),
            _ => None,
        }
    }

    pub fn stream_error_context(&self) -> &[StreamErrorContext] {
        match self {
            Self::GroupEngine { error, .. } => error.context(),
            _ => &[],
        }
    }

    pub fn leader_hint(&self) -> Option<&GroupLeaderHint> {
        match self {
            Self::GroupEngine { error, .. } => error.leader_hint(),
            _ => None,
        }
    }

    pub fn status(&self) -> ErrorStatus {
        match self {
            Self::LiveReadBackpressure { .. } | Self::GroupNotHosted { .. } => {
                ErrorStatus::Temporary
            }
            Self::GroupEngine { error, .. } if error.leader_hint().is_some() => {
                ErrorStatus::Temporary
            }
            Self::GroupEngine { error, .. } if error.is_backpressure() => ErrorStatus::Temporary,
            Self::GroupEngine { error, .. } if error.code().is_some() => ErrorStatus::Permanent,
            Self::GroupEngine { .. } => ErrorStatus::Persistent,
            _ => ErrorStatus::Permanent,
        }
    }
}
