use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;

use crate::integrity::StreamIntegritySnapshot;
use crate::model::ColdChunkRef;
use crate::model::ColdGcEntry;
use crate::model::HotPayloadSegment;
use crate::model::ObjectPayloadRef;
use crate::model::ProducerSnapshot;
use crate::model::StreamAttrs;
use crate::model::StreamMessageRecord;
use crate::model::StreamMetadata;
use crate::model::StreamVisibleSnapshot;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSnapshot {
    pub buckets: Vec<String>,
    pub streams: Vec<StreamSnapshotEntry>,
    #[serde(default)]
    pub pending_cold_gc: Vec<ColdGcEntry>,
    #[serde(default)]
    pub next_cold_gc_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSnapshotEntry {
    pub metadata: StreamMetadata,
    #[serde(default)]
    pub attrs: Option<StreamAttrs>,
    pub hot_start_offset: u64,
    pub payload: Vec<u8>,
    pub hot_segments: Vec<HotPayloadSegment>,
    #[serde(default)]
    pub cold_frontier_offset: u64,
    #[serde(default)]
    pub cold_index_generation: u64,
    pub cold_chunks: Vec<ColdChunkRef>,
    pub external_segments: Vec<ObjectPayloadRef>,
    pub message_records: Vec<StreamMessageRecord>,
    pub integrity: StreamIntegritySnapshot,
    pub visible_snapshot: Option<StreamVisibleSnapshot>,
    pub producer_states: Vec<ProducerSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamSnapshotError {
    DuplicateBucket(String),
    DuplicateStream(BucketStreamId),
    DuplicateProducer {
        stream_id: BucketStreamId,
        producer_id: String,
    },
    MissingBucket(BucketStreamId),
    PayloadLengthMismatch {
        stream_id: BucketStreamId,
        tail_offset: u64,
        payload_len: usize,
    },
    MessageBoundaryMismatch {
        stream_id: BucketStreamId,
    },
    IntegrityMismatch {
        stream_id: BucketStreamId,
    },
    SnapshotOffsetOutOfRange {
        stream_id: BucketStreamId,
        snapshot_offset: u64,
        tail_offset: u64,
    },
}

impl std::fmt::Display for StreamSnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateBucket(bucket_id) => {
                write!(f, "snapshot contains duplicate bucket '{bucket_id}'")
            }
            Self::DuplicateStream(stream_id) => {
                write!(f, "snapshot contains duplicate stream '{stream_id}'")
            }
            Self::DuplicateProducer {
                stream_id,
                producer_id,
            } => write!(
                f,
                "snapshot stream '{stream_id}' contains duplicate producer '{producer_id}'"
            ),
            Self::MissingBucket(stream_id) => {
                write!(
                    f,
                    "snapshot stream '{stream_id}' references a missing bucket"
                )
            }
            Self::PayloadLengthMismatch {
                stream_id,
                tail_offset,
                payload_len,
            } => write!(
                f,
                "snapshot stream '{stream_id}' tail offset {tail_offset} does not match payload length {payload_len}"
            ),
            Self::MessageBoundaryMismatch { stream_id } => write!(
                f,
                "snapshot stream '{stream_id}' has inconsistent message boundaries"
            ),
            Self::IntegrityMismatch { stream_id } => write!(
                f,
                "snapshot stream '{stream_id}' has inconsistent integrity setsums"
            ),
            Self::SnapshotOffsetOutOfRange {
                stream_id,
                snapshot_offset,
                tail_offset,
            } => write!(
                f,
                "snapshot stream '{stream_id}' visible snapshot offset {snapshot_offset} is beyond tail offset {tail_offset}"
            ),
        }
    }
}

impl std::error::Error for StreamSnapshotError {}
