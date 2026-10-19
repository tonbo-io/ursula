use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardMapError;
use ursula_shard::ShardPlacement;
use ursula_stream::StreamErrorCode;
use ursula_stream::StreamErrorContext;

use crate::engine::GroupEngineError;
use crate::engine::LeadershipRefusal;

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
    /// The task that restarted one group's engine ended without finishing:
    /// the group may be stopped on this node until a request creates it.
    #[error("the restart of raft group {} did not finish", .raft_group_id.0)]
    GroupRestartAborted { raft_group_id: RaftGroupId },
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
        .error
    )]
    GroupEngine {
        core_id: CoreId,
        raft_group_id: RaftGroupId,
        error: GroupEngineError,
    },
    /// The core already runs an engine for the group: a second install is
    /// refused, and the running engine stays.
    #[error("core {} already runs an engine for raft group {}", .core_id.0, .raft_group_id.0)]
    GroupAlreadyInstalled {
        core_id: CoreId,
        raft_group_id: RaftGroupId,
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

    /// The leadership refusal a group engine answered, if it did.
    pub fn not_leader(&self) -> Option<&LeadershipRefusal> {
        match self {
            Self::GroupEngine { error, .. } => error.not_leader(),
            _ => None,
        }
    }

    /// Whether nothing of a failed write applied
    /// ([`GroupEngineError::applied_nothing`]). Only a group engine's error
    /// tells: every other runtime error counts as an unknown outcome.
    pub fn applied_nothing(&self) -> bool {
        match self {
            Self::GroupEngine { error, .. } => error.applied_nothing(),
            _ => false,
        }
    }

    pub fn status(&self) -> ErrorStatus {
        match self {
            Self::LiveReadBackpressure { .. } | Self::GroupNotHosted { .. } => {
                ErrorStatus::Temporary
            }
            Self::GroupEngine { error, .. } => error.status(),
            Self::GroupAlreadyInstalled { .. } => ErrorStatus::Persistent,
            Self::GroupRestartAborted { .. }
            | Self::InvalidConfig(_)
            | Self::InvalidRaftGroup { .. }
            | Self::SnapshotPlacementMismatch { .. }
            | Self::ColdStoreConfig { .. }
            | Self::StaticMembershipConfig { .. }
            | Self::ColdStoreIo { .. }
            | Self::MailboxClosed { .. }
            | Self::ResponseDropped { .. }
            | Self::SpawnCoreThread { .. } => ErrorStatus::Permanent,
        }
    }
}

/// Renders an error with its chain of sources, `error: source: source`, for
/// logs and for the message of a transport's answer. A typed error keeps its
/// cause as a source rather than in its own text, so a log that prints only
/// the error loses the cause.
pub struct ErrorChain<'a>(pub &'a (dyn std::error::Error + 'static));

impl std::fmt::Display for ErrorChain<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)?;
        let mut source = self.0.source();
        while let Some(error) = source {
            write!(f, ": {error}")?;
            source = error.source();
        }
        Ok(())
    }
}
