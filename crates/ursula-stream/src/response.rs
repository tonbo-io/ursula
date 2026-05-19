use serde::{Deserialize, Serialize};
use ursula_shard::BucketStreamId;

use crate::model::ProducerRequest;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamResponse {
    BucketCreated {
        bucket_id: String,
    },
    BucketAlreadyExists {
        bucket_id: String,
    },
    BucketDeleted {
        bucket_id: String,
    },
    Created {
        stream_id: BucketStreamId,
        next_offset: u64,
        closed: bool,
    },
    AlreadyExists {
        next_offset: u64,
        closed: bool,
        content_type: String,
        stream_ttl_seconds: Option<u64>,
        stream_expires_at_ms: Option<u64>,
    },
    Appended {
        offset: u64,
        next_offset: u64,
        closed: bool,
        deduplicated: bool,
        producer: Option<ProducerRequest>,
    },
    Closed {
        next_offset: u64,
        deduplicated: bool,
        producer: Option<ProducerRequest>,
    },
    Deleted {
        hard_deleted: bool,
        parent_to_release: Option<BucketStreamId>,
    },
    ForkRefAdded {
        fork_ref_count: u64,
    },
    ForkRefReleased {
        hard_deleted: bool,
        fork_ref_count: u64,
        parent_to_release: Option<BucketStreamId>,
    },
    ColdFlushed {
        hot_start_offset: u64,
    },
    SnapshotPublished {
        snapshot_offset: u64,
    },
    Accessed {
        changed: bool,
        expired: bool,
    },
    Error {
        code: StreamErrorCode,
        message: String,
        next_offset: Option<u64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamErrorCode {
    InvalidBucketId,
    InvalidStreamId,
    BucketNotFound,
    BucketNotEmpty,
    StreamNotFound,
    StreamGone,
    StreamAlreadyExistsConflict,
    MissingContentType,
    ContentTypeMismatch,
    EmptyAppend,
    StreamClosed,
    StreamSeqConflict,
    InvalidProducer,
    ProducerEpochStale,
    ProducerSeqConflict,
    InvalidRetention,
    InvalidFork,
    OffsetOutOfRange,
    InvalidColdFlush,
    InvalidSnapshot,
    SnapshotNotFound,
    SnapshotConflict,
}

impl StreamResponse {
    pub(crate) fn error(code: StreamErrorCode, message: impl Into<String>) -> Self {
        Self::Error {
            code,
            message: message.into(),
            next_offset: None,
        }
    }

    pub(crate) fn error_with_next_offset(
        code: StreamErrorCode,
        message: impl Into<String>,
        next_offset: u64,
    ) -> Self {
        Self::Error {
            code,
            message: message.into(),
            next_offset: Some(next_offset),
        }
    }
}
