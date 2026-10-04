use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;

use crate::model::ProducerRequest;
use crate::record_index::StreamRecordRange;

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
        /// (F3, feature level 1). It is answered as deduplicated without
        /// byte or record ranges: `offset` and `next_offset` are the stream
        /// tail and carry no information about the original append.
        receipt_evicted: bool,
        /// Records of this append as apply computed them, or the stored
        /// receipt's range for a duplicate (F1, RC-10/RC-11). Never derived
        /// from the record index afterwards, which may have sealed them.
        record_range: Option<StreamRecordRange>,
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
        record_range: Option<StreamRecordRange>,
    },
    RetentionAdvanced {
        retained_offset: u64,
        record_range: Option<StreamRecordRange>,
    },
    Accessed {
        changed: bool,
        expired: bool,
    },
    AttrsUpdated {
        changed: bool,
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
    /// Result of [`StreamCommand::SetFeatureLevel`]: the group's level after
    /// apply and the level it held before.
    ///
    /// [`StreamCommand::SetFeatureLevel`]: crate::StreamCommand::SetFeatureLevel
    FeatureLevelSet {
        level: u32,
        previous_level: u32,
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
    InvalidStreamAttrs,
    InvalidRecordBoundaries,
    RecordPreconditionFailed,
    /// A state import targeted a group that already holds buckets or streams.
    ImportConflict,
    /// A state import payload failed snapshot validation.
    ImportInvalid,
    /// The command needs a higher group feature level than the group holds.
    /// Deterministic: every replica rejects it the same way.
    FeatureNotEnabled,
    /// A new producer would exceed the stream's producer cap and no producer
    /// has been idle long enough to evict (F3, feature level 1).
    ProducerLimit,
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
    RecordTailMismatch {
        current_record: u64,
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
