//! [`GroupSnapshotError`]: why a group snapshot that crosses the engine
//! boundary (a restore at engine open, [`crate::GroupEngine::install_snapshot`],
//! a snapshot build) failed. Install-path refusals that never leave the Raft
//! crate stay in its own install error.

use std::fmt;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::RaftGroupId;
use ursula_stream::StreamSnapshotError;

use super::WriteEffect;
use crate::error::ErrorStatus;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum GroupSnapshotError {
    /// The snapshot's stream state does not restore.
    #[error("snapshot content does not restore")]
    InvalidContent(#[source] StreamSnapshotError),
    /// The snapshot this node persisted names an object outside the
    /// namespace this node owns for the group, as when the node started
    /// with another node's data directory or with another snapshot prefix.
    #[error(
        "raft group {raft_group_id:?}: the persisted snapshot names {key}, outside this node's \
         snapshot namespace {prefix}. Start the node with an empty raft.wal.path to rejoin as \
         a wiped node"
    )]
    ForeignKey {
        raft_group_id: RaftGroupId,
        key: String,
        prefix: String,
    },
    /// The snapshot object the persisted pointer names is gone.
    #[error("raft group {raft_group_id:?}: snapshot object {key} is missing")]
    ObjectMissing {
        raft_group_id: RaftGroupId,
        key: String,
    },
    /// The snapshot object the persisted pointer names fails a check.
    #[error("raft group {raft_group_id:?}: snapshot object {key} is corrupt: {check}")]
    ObjectCorrupt {
        raft_group_id: RaftGroupId,
        key: String,
        check: SnapshotObjectCheck,
    },
    /// This node's persisted snapshot pointer cannot be read: its metadata
    /// file does not read, decode or become durable. The cause is logged
    /// where it is converted.
    #[error("raft group {raft_group_id:?}: the persisted snapshot pointer is unreadable")]
    PointerUnreadable { raft_group_id: RaftGroupId },
    /// The snapshot store did not answer `operation` on `key`. The cause is
    /// logged where it is converted. Retryable.
    #[error("raft group {raft_group_id:?}: snapshot store {operation} of {key} failed")]
    StoreUnavailable {
        raft_group_id: RaftGroupId,
        key: String,
        operation: SnapshotStoreOperation,
    },
}

/// The check a snapshot object failed ([`GroupSnapshotError::ObjectCorrupt`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotObjectCheck {
    /// The stored object's size differs from the pointer's.
    StoredSize { expected: u64, actual: u64 },
    /// The decompressed body's size differs from the pointer's.
    LogicalSize { expected: u64, actual: u64 },
    /// The stored object does not decompress.
    Decompress,
    /// The body does not decode as a group snapshot.
    Decode,
}

impl fmt::Display for SnapshotObjectCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StoredSize { expected, actual } => {
                write!(f, "stored size {actual}, expected {expected}")
            }
            Self::LogicalSize { expected, actual } => {
                write!(f, "logical size {actual}, expected {expected}")
            }
            Self::Decompress => f.write_str("it does not decompress"),
            Self::Decode => f.write_str("it does not decode as a group snapshot"),
        }
    }
}

/// A request to the snapshot store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotStoreOperation {
    /// Reading an object.
    Get,
    /// Reading an object's metadata.
    Head,
    /// Writing an object.
    Put,
    /// Listing a prefix.
    List,
    /// Deleting an object.
    Delete,
}

impl fmt::Display for SnapshotStoreOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Get => "GET",
            Self::Head => "HEAD",
            Self::Put => "PUT",
            Self::List => "LIST",
            Self::Delete => "DELETE",
        })
    }
}

impl GroupSnapshotError {
    /// See [`super::GroupEngineError::status`]. Only an unavailable store
    /// may answer on a retry: every other refusal holds until an operator
    /// acts.
    pub fn status(&self) -> ErrorStatus {
        match self {
            Self::StoreUnavailable { .. } => ErrorStatus::Temporary,
            Self::InvalidContent(_)
            | Self::ForeignKey { .. }
            | Self::ObjectMissing { .. }
            | Self::ObjectCorrupt { .. }
            | Self::PointerUnreadable { .. } => ErrorStatus::Persistent,
        }
    }

    /// See [`super::GroupEngineError::write_effect`]. A snapshot refusal
    /// answers no write, so it claims nothing about one.
    pub fn write_effect(&self) -> WriteEffect {
        match self {
            Self::InvalidContent(_)
            | Self::ForeignKey { .. }
            | Self::ObjectMissing { .. }
            | Self::ObjectCorrupt { .. }
            | Self::PointerUnreadable { .. }
            | Self::StoreUnavailable { .. } => WriteEffect::Unknown,
        }
    }

    /// See [`super::GroupEngineError::peer_backs_off`]. A replica that
    /// cannot restore its own snapshot takes nothing from a peer until it
    /// does, so the peer backs off.
    pub fn peer_backs_off(&self) -> bool {
        match self {
            Self::InvalidContent(_) => false,
            Self::ForeignKey { .. }
            | Self::ObjectMissing { .. }
            | Self::ObjectCorrupt { .. }
            | Self::PointerUnreadable { .. }
            | Self::StoreUnavailable { .. } => true,
        }
    }
}
