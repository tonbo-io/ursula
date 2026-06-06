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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    InvalidConfig(ShardMapError),
    InvalidRaftGroup {
        raft_group_id: RaftGroupId,
        raft_group_count: u32,
    },
    SnapshotPlacementMismatch {
        expected: ShardPlacement,
        actual: ShardPlacement,
    },
    EmptyAppend,
    ColdStoreConfig {
        message: String,
    },
    StaticMembershipConfig {
        message: String,
    },
    ColdStoreIo {
        message: String,
    },
    LiveReadBackpressure {
        core_id: CoreId,
        current_waiters: u64,
        limit: u64,
    },
    GroupNotHosted {
        core_id: CoreId,
        raft_group_id: RaftGroupId,
    },
    GroupEngine {
        core_id: CoreId,
        raft_group_id: RaftGroupId,
        error: GroupEngineError,
    },
    MailboxClosed {
        core_id: CoreId,
    },
    ResponseDropped {
        core_id: CoreId,
    },
    SpawnCoreThread {
        core_id: CoreId,
        message: String,
    },
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

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfig(err) => write!(f, "invalid shard runtime config: {err}"),
            Self::InvalidRaftGroup {
                raft_group_id,
                raft_group_count,
            } => write!(
                f,
                "raft group {} is outside configured range 0..{}",
                raft_group_id.0, raft_group_count
            ),
            Self::SnapshotPlacementMismatch { expected, actual } => write!(
                f,
                "snapshot placement for raft group {} is core {}, expected core {}",
                actual.raft_group_id.0, actual.core_id.0, expected.core_id.0
            ),
            Self::EmptyAppend => f.write_str("append payload must be non-empty"),
            Self::ColdStoreConfig { message } => {
                write!(f, "invalid cold store config: {message}")
            }
            Self::StaticMembershipConfig { message } => {
                write!(f, "invalid static Raft membership config: {message}")
            }
            Self::ColdStoreIo { message } => write!(f, "cold store IO error: {message}"),
            Self::LiveReadBackpressure {
                core_id,
                current_waiters,
                limit,
            } => write!(
                f,
                "core {} live read waiters at {} would exceed limit {}",
                core_id.0, current_waiters, limit
            ),
            Self::GroupNotHosted {
                core_id,
                raft_group_id,
            } => write!(
                f,
                "core {} does not host raft group {}",
                core_id.0, raft_group_id.0
            ),
            Self::GroupEngine {
                core_id,
                raft_group_id,
                error,
                ..
            } => write!(
                f,
                "core {} raft group {} operation failed: {}",
                core_id.0,
                raft_group_id.0,
                error.message()
            ),
            Self::MailboxClosed { core_id } => {
                write!(f, "core {} mailbox is closed", core_id.0)
            }
            Self::ResponseDropped { core_id } => {
                write!(f, "core {} dropped append response", core_id.0)
            }
            Self::SpawnCoreThread { core_id, message } => {
                write!(f, "failed to spawn core {} thread: {message}", core_id.0)
            }
        }
    }
}

impl std::error::Error for RuntimeError {}

impl From<ShardMapError> for RuntimeError {
    fn from(value: ShardMapError) -> Self {
        Self::InvalidConfig(value)
    }
}

pub(crate) fn map_fork_source_ref_error(
    err: RuntimeError,
    placement: ShardPlacement,
) -> RuntimeError {
    if err.stream_error_code() == Some(StreamErrorCode::StreamGone) {
        return RuntimeError::group_engine(
            placement,
            GroupEngineError::stream(
                StreamErrorCode::StreamAlreadyExistsConflict,
                "source stream is gone and cannot be forked",
            ),
        );
    }
    err
}
