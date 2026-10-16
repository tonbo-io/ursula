use std::num::NonZeroU64;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;

use crate::model::ClosedEpoch;
use crate::model::WriteReceipt;

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
    /// An append that applied now, or a producer write that applied
    /// before ([`ProducerAck::delivery`]). For a repeat, `offset` and
    /// `next_offset` are the original write's range, or the stream's tail
    /// when the range was not kept ([`ProducerDelivery::Evicted`]).
    Appended {
        offset: u64,
        next_offset: u64,
        closed: bool,
        producer: Option<ProducerAck>,
    },
    /// A close that applied now, of a stream already closed (without
    /// producer headers), or a producer write that applied before.
    Closed {
        next_offset: u64,
        producer: Option<ProducerAck>,
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
        /// The history the import started.
        history: NonZeroU64,
        /// The group already held this import: nothing changed.
        already: bool,
    },
    /// A history event applied by [`StreamCommand::NewHistory`].
    ///
    /// [`StreamCommand::NewHistory`]: crate::StreamCommand::NewHistory
    HistoryStarted {
        history: NonZeroU64,
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
    /// A producer write of an epoch that a claim closed (`403
    /// epoch-closed`).
    ProducerEpochClosed,
    /// A producer write at or below the stream's epoch floor (`403
    /// producer-expired`): the stream keeps no memory of its epoch.
    ProducerExpired,
    /// A producer write of an epoch below the producer's current one, other
    /// than the epoch its claim closed (`403 stale-epoch`, the base
    /// protocol's stale epoch).
    ProducerEpochStale,
    /// A producer write whose key holds a write with another identity (`403
    /// key-conflict`).
    ProducerKeyConflict,
    /// A producer write that would apply, whose body does not match its
    /// `Producer-Digest` (`400`).
    ProducerDigestMismatch,
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
    /// A create at a `now_ms` at or above 2^52, which no token can carry
    /// ([`crate::MAX_CREATED_AT_MS`]): the proposer's clock is far ahead.
    CreatedAtOutOfRange,
    /// A `Stream-Incarnation` precondition (D12) failed: the stream is
    /// another incarnation (its current one is in
    /// [`StreamErrorContext::StreamIncarnation`]), or a create found no
    /// stream. Nothing was applied.
    IncarnationMismatch,
    /// The group has no history yet: its genesis has not applied (#581
    /// section 4.3). Every command but the history event is refused at
    /// apply, so a write that raced the genesis applies nothing. Retryable.
    HistoryPending,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamErrorContext {
    StreamClosed,
    StaleColdFlushCandidate,
    /// The typed detail of a producer refusal (#581 section 2.3.1).
    Producer(ProducerRefusal),
    ProducerSeqConflict {
        expected_seq: u64,
        received_seq: u64,
    },
    /// The stream's token (`Stream-Incarnation`) that a
    /// [`StreamErrorCode::JsonBoundaryUnverified`] or
    /// [`StreamErrorCode::IncarnationMismatch`] refusal saw, or that decided
    /// a read's [`StreamErrorCode::StreamGone`] or
    /// [`StreamErrorCode::OffsetOutOfRange`], or the `SnapshotNotFound` of a
    /// stream that exists. The read answers render it as
    /// `Stream-Incarnation`.
    StreamIncarnation {
        incarnation: u64,
    },
    /// The stream's latest snapshot, which a [`StreamErrorCode::SnapshotConflict`]
    /// of a publish names: a later offset means the publish is superseded,
    /// the same offset means another snapshot is there (#581 section 3).
    LatestSnapshot {
        offset: u64,
        digest: String,
    },
    /// The stream's last accepted `Stream-Seq`, which a
    /// [`StreamErrorCode::StreamSeqConflict`] names, so a client using the
    /// sequence as a compare-and-set retries from it.
    LastStreamSeq {
        seq: String,
    },
}

/// What closed an epoch, the `by` of `epoch-closed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClosedBy {
    /// A claim of a later epoch.
    Claim,
    /// A history event: a restore or an accepted loss (G2). It closes the
    /// producer's current epoch.
    History,
}

/// A producer refusal with what its answer reports. Every variant but
/// [`Self::EpochNotClaimed`] renders `Producer-Epoch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProducerRefusal {
    /// `403 epoch-closed`: `closed` is closed; `epoch` is the producer's
    /// current epoch.
    EpochClosed {
        epoch: u64,
        by: ClosedBy,
        closed: ClosedEpoch,
    },
    /// `403 producer-expired`: `epoch` is the stream's epoch floor.
    Expired { epoch: u64 },
    /// `403 stale-epoch`: `epoch` is the producer's current epoch.
    StaleEpoch { epoch: u64 },
    /// `403 key-conflict`.
    KeyConflict { epoch: u64 },
    /// `400` with `Producer-Expected-Seq: 0`: a seq above 0 of an epoch
    /// above the producer's, which no claim opened.
    EpochNotClaimed,
}

impl ProducerRefusal {
    /// The error code of the refusal: the one source the late path's status
    /// comes from.
    pub fn code(&self) -> StreamErrorCode {
        match self {
            Self::EpochClosed { .. } => StreamErrorCode::ProducerEpochClosed,
            Self::Expired { .. } => StreamErrorCode::ProducerExpired,
            Self::StaleEpoch { .. } => StreamErrorCode::ProducerEpochStale,
            Self::KeyConflict { .. } => StreamErrorCode::ProducerKeyConflict,
            Self::EpochNotClaimed => StreamErrorCode::InvalidProducer,
        }
    }
}

/// The answer to a tokened producer write that does not depend on its body
/// (#581 section 2.5, W7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProducerEarlyAnswer {
    /// Rows 9 and 10: the key applied before.
    Repeat(ProducerAck),
    /// Row 8: the key holds a write with another identity.
    KeyConflict { epoch: u64 },
}

/// How a producer write applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProducerDelivery {
    /// This command applied the write.
    Applied(WriteReceipt),
    /// The producer's newest write, applied before.
    Repeat(WriteReceipt),
    /// An older write of the epoch, applied before. The stream keeps no
    /// range for it (`receipt-evicted`).
    Evicted,
}

/// The answer to a producer write that applied, now or before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerAck {
    pub epoch: u64,
    /// The epoch's highest applied seq (base protocol `Producer-Seq`).
    pub seq: u64,
    /// On a claim and its repeats: the epoch the claim closed.
    pub closed_epoch: Option<ClosedEpoch>,
    pub delivery: ProducerDelivery,
}

impl ProducerAck {
    /// Whether the write applied before this command, so this command
    /// changed nothing.
    pub fn is_repeat(&self) -> bool {
        !matches!(self.delivery, ProducerDelivery::Applied(_))
    }
}

impl StreamResponse {
    /// Whether this is a repeat of a producer write that applied before.
    pub fn is_producer_repeat(&self) -> bool {
        match self {
            Self::Appended { producer, .. } | Self::Closed { producer, .. } => {
                producer.as_ref().is_some_and(ProducerAck::is_repeat)
            }
            _ => false,
        }
    }

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
