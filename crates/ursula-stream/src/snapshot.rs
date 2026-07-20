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
use crate::record_index::StreamRecordIndex;

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
    #[serde(default)]
    pub record_index: Option<StreamRecordIndex>,
    pub integrity: StreamIntegritySnapshot,
    pub visible_snapshot: Option<StreamVisibleSnapshot>,
    pub producer_states: Vec<ProducerSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StreamSnapshotError {
    #[error("snapshot contains duplicate bucket '{0}'")]
    DuplicateBucket(String),
    #[error("snapshot contains duplicate stream '{0}'")]
    DuplicateStream(BucketStreamId),
    #[error("snapshot stream '{stream_id}' contains duplicate producer '{producer_id}'")]
    DuplicateProducer {
        stream_id: BucketStreamId,
        producer_id: String,
    },
    #[error("snapshot stream '{0}' references a missing bucket")]
    MissingBucket(BucketStreamId),
    #[error(
        "snapshot stream '{stream_id}' tail offset {tail_offset} does not match payload length {payload_len}"
    )]
    PayloadLengthMismatch {
        stream_id: BucketStreamId,
        tail_offset: u64,
        payload_len: usize,
    },
    #[error("snapshot stream '{stream_id}' has inconsistent message boundaries")]
    MessageBoundaryMismatch { stream_id: BucketStreamId },
    #[error("snapshot stream '{stream_id}' has inconsistent record boundaries")]
    RecordBoundaryMismatch { stream_id: BucketStreamId },
    #[error("snapshot stream '{stream_id}' has inconsistent integrity setsums")]
    IntegrityMismatch { stream_id: BucketStreamId },
    #[error(
        "snapshot stream '{stream_id}' visible snapshot offset {snapshot_offset} is beyond tail offset {tail_offset}"
    )]
    SnapshotOffsetOutOfRange {
        stream_id: BucketStreamId,
        snapshot_offset: u64,
        tail_offset: u64,
    },
}
