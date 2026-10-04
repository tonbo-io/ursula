use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;

use crate::integrity::StreamIntegritySnapshot;
use crate::model::BucketUsageSnapshot;
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
    /// Permanent bucket-erasure fences. Absent in legacy snapshots.
    #[serde(default)]
    pub erased_buckets: Vec<String>,
    pub streams: Vec<StreamSnapshotEntry>,
    #[serde(default)]
    pub pending_cold_gc: Vec<ColdGcEntry>,
    #[serde(default)]
    pub next_cold_gc_seq: u64,
    /// Bucket erasure owners retained until a shared physical object is
    /// deleted. Absent in snapshots written before bucket-scoped proof.
    #[serde(default)]
    pub shared_cold_object_owners: Vec<SharedColdObjectOwnersSnapshot>,
    /// Monotonic per-bucket usage counters. Absent in legacy snapshots, in
    /// which case the monotonic counters restart from the restored gauges.
    #[serde(default)]
    pub bucket_usage: Vec<BucketUsageSnapshot>,
    /// Replicated group feature level (C0). Absent in legacy snapshots,
    /// which decode as level 0.
    #[serde(default)]
    pub feature_level: u32,
    /// Largest `created_at_ms` assigned at feature level 1 or later (C7,
    /// F14g). Absent in legacy snapshots, which decode as 0.
    #[serde(default)]
    pub last_created_at_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedColdObjectOwnersSnapshot {
    pub s3_path: String,
    pub bucket_ids: Vec<String>,
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
    /// Legacy message boundaries. Empty from feature level 4 (F4b) once
    /// the stream converted; snapshots then carry `hot_append_starts`.
    pub message_records: Vec<StreamMessageRecord>,
    /// F4b (level 4): start offsets of the messages at or above the seal
    /// point, for streams without a record index. Absent in older
    /// snapshots.
    #[serde(default)]
    pub hot_append_starts: Vec<u64>,
    #[serde(default)]
    pub record_index: Option<StreamRecordIndex>,
    pub integrity: StreamIntegritySnapshot,
    /// Independent destructive-retention floor. `None` denotes a legacy
    /// snapshot where the visible snapshot offset also implied retention.
    #[serde(default)]
    pub retained_offset: Option<u64>,
    pub visible_snapshot: Option<StreamVisibleSnapshot>,
    pub producer_states: Vec<ProducerSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StreamSnapshotError {
    #[error("snapshot contains duplicate bucket '{0}'")]
    DuplicateBucket(String),
    #[error("snapshot contains duplicate erased bucket '{0}'")]
    DuplicateErasedBucket(String),
    #[error("snapshot bucket '{0}' is both active and erased")]
    ActiveBucketErased(String),
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
    #[error(
        "snapshot feature level {level} exceeds this binary's supported level {supported}; \
         a binary that cannot apply that level must not run this group"
    )]
    UnsupportedFeatureLevel { level: u32, supported: u32 },
}
