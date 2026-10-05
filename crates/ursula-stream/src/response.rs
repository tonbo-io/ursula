use serde::Deserialize;
use serde::Serialize;
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
        /// A duplicate whose receipt the stream's receipt window evicted
        /// (F3). It is answered as deduplicated without
        /// byte ranges: `offset` and `next_offset` are the stream tail and
        /// carry no information about the original append.
        receipt_evicted: bool,
    },
    Closed {
        next_offset: u64,
        deduplicated: bool,
        producer: Option<ProducerRequest>,
    },
    Deleted,
    ColdFlushed {
        hot_start_offset: u64,
    },
    ColdCompacted {
        compacted_chunks: u64,
        compacted_bytes: u64,
    },
    SnapshotPublished {
        snapshot_offset: u64,
        snapshot_digest: String,
    },
    RetentionAdvanced {
        retained_offset: u64,
    },
    Accessed {
        changed: bool,
        expired: bool,
    },
    ColdGcAcked {
        removed: u64,
    },
    /// Result of [`StreamCommand::DeferColdGc`]: the entry's new sequence
    /// number, or `None` when no pending entry had the given `seq`.
    ///
    /// [`StreamCommand::DeferColdGc`]: crate::StreamCommand::DeferColdGc
    ColdGcDeferred {
        new_seq: Option<u64>,
    },
    /// A whole-bucket purge accepted by [`StreamCommand::PurgeBucket`].
    ///
    /// [`StreamCommand::PurgeBucket`]: crate::StreamCommand::PurgeBucket
    BucketPurged {
        bucket_id: String,
        removed_streams: u64,
        pending_cold_gc_entries: u64,
    },
    /// A whole-group state import accepted by [`StreamCommand::ImportSnapshot`].
    ///
    /// [`StreamCommand::ImportSnapshot`]: crate::StreamCommand::ImportSnapshot
    SnapshotImported {
        buckets: u64,
        streams: u64,
    },
    /// Result of [`StreamCommand::TidyStream`]: whether the stream still has
    /// normalization debt after this bounded step.
    ///
    /// [`StreamCommand::TidyStream`]: crate::StreamCommand::TidyStream
    StreamTidied {
        debt_remaining: bool,
    },
    /// Result of [`StreamCommand::OffloadColdRefs`]: how many of the listed
    /// refs were still in state and removed, and how many state-held external
    /// refs the stream keeps.
    ///
    /// [`StreamCommand::OffloadColdRefs`]: crate::StreamCommand::OffloadColdRefs
    ColdRefsOffloaded {
        removed: u64,
        remaining: u64,
    },
    Error {
        code: StreamErrorCode,
        message: String,
        next_offset: Option<u64>,
        context: Vec<StreamErrorContext>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamErrorCode {
    InvalidBucketId,
    InvalidStreamId,
    BucketNotFound,
    /// The bucket was permanently purged and its name is fenced from reuse.
    BucketErased,
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
    OffsetOutOfRange,
    InvalidColdFlush,
    InvalidSnapshot,
    SnapshotNotFound,
    SnapshotConflict,
    InvalidRecordBoundaries,
    /// A JSON snapshot or retention offset whose preceding byte is not in
    /// the hot buffer, proposed without an `expected_incarnation` that
    /// matches the stream. The proposer verifies the byte is LF by reading
    /// it, then proposes again with the incarnation the error carries in
    /// [`StreamErrorContext::StreamIncarnation`].
    JsonBoundaryUnverified,
    /// A state import targeted a group that already holds buckets or streams.
    ImportConflict,
    /// A state import payload failed snapshot validation.
    ImportInvalid,
    /// A new producer would exceed the stream's producer cap and no producer
    /// has been idle long enough to evict (F3).
    ProducerLimit,
    /// A `Stream-Incarnation` precondition (D12) failed: the stream is
    /// another incarnation (its current one is in
    /// [`StreamErrorContext::StreamIncarnation`]), or a create found no
    /// stream. Nothing was applied.
    IncarnationMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamErrorContext {
    StreamClosed,
    StaleColdFlushCandidate,
    ProducerEpochStale {
        current_epoch: u64,
    },
    ProducerSeqConflict {
        expected_seq: u64,
        received_seq: u64,
    },
    /// The stream incarnation (`created_at_ms`) a
    /// [`StreamErrorCode::JsonBoundaryUnverified`] or
    /// [`StreamErrorCode::IncarnationMismatch`] refusal saw.
    StreamIncarnation {
        incarnation: u64,
    },
}

impl StreamResponse {
    pub(crate) fn error(code: StreamErrorCode, message: impl Into<String>) -> Self {
        Self::error_with_context(code, message, Vec::new())
    }

    pub(crate) fn error_with_context(
        code: StreamErrorCode,
        message: impl Into<String>,
        context: Vec<StreamErrorContext>,
    ) -> Self {
        Self::Error {
            code,
            message: message.into(),
            next_offset: None,
            context,
        }
    }

    pub(crate) fn error_with_next_offset(
        code: StreamErrorCode,
        message: impl Into<String>,
        next_offset: u64,
    ) -> Self {
        Self::error_with_next_offset_and_context(code, message, next_offset, Vec::new())
    }

    pub(crate) fn error_with_next_offset_and_context(
        code: StreamErrorCode,
        message: impl Into<String>,
        next_offset: u64,
        context: Vec<StreamErrorContext>,
    ) -> Self {
        Self::Error {
            code,
            message: message.into(),
            next_offset: Some(next_offset),
            context,
        }
    }
}
