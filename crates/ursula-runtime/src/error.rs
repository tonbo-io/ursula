use ursula_shard::{CoreId, RaftGroupId, ShardMapError, ShardPlacement};
use ursula_stream::StreamErrorCode;

use crate::engine::{GroupEngineError, GroupLeaderHint};

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
    ColdStoreIo {
        message: String,
    },
    LiveReadBackpressure {
        core_id: CoreId,
        current_waiters: u64,
        limit: u64,
    },
    GroupEngine {
        core_id: CoreId,
        raft_group_id: RaftGroupId,
        message: String,
        next_offset: Option<u64>,
        leader_hint: Option<GroupLeaderHint>,
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
            message: err.message().to_owned(),
            next_offset: err.next_offset(),
            leader_hint: err.leader_hint().cloned(),
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
            Self::GroupEngine {
                core_id,
                raft_group_id,
                message,
                ..
            } => write!(
                f,
                "core {} raft group {} append failed: {message}",
                core_id.0, raft_group_id.0
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
    if let RuntimeError::GroupEngine { message, .. } = &err
        && message.contains("StreamGone")
    {
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
