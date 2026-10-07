use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;

use crate::model::BucketUsageSnapshot;
use crate::model::ColdChunkRef;
use crate::model::ColdGcEntry;
use crate::model::HotPayloadSegment;
use crate::model::ObjectPayloadRef;
use crate::model::ProducerSnapshot;
use crate::model::StreamMetadata;
use crate::model::StreamVisibleSnapshot;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSnapshot {
    /// Stream snapshot version ([`crate::STREAM_SNAPSHOT_VERSION`]). The
    /// MessagePack key stays `format_epoch`, the name Ursula 0.6 wrote it
    /// under, so 0.6 backups decode. Deliberately not `serde(default)`: a
    /// snapshot from Ursula 0.5.x has no such field and fails to decode
    /// instead of being misread.
    #[serde(rename = "format_epoch")]
    pub version: u32,
    pub buckets: Vec<String>,
    /// Permanent bucket-erasure fences.
    pub erased_buckets: Vec<String>,
    pub streams: Vec<StreamSnapshotEntry>,
    pub pending_cold_gc: Vec<ColdGcEntry>,
    pub next_cold_gc_seq: u64,
    /// Monotonic per-bucket usage counters.
    pub bucket_usage: Vec<BucketUsageSnapshot>,
    /// Largest `created_at_ms` this group assigned (C7, F14g).
    pub last_created_at_ms: u64,
}

/// An empty snapshot of this binary's version. Fixtures restore from it.
impl Default for StreamSnapshot {
    fn default() -> Self {
        Self {
            version: crate::STREAM_SNAPSHOT_VERSION,
            buckets: Vec::new(),
            erased_buckets: Vec::new(),
            streams: Vec::new(),
            pending_cold_gc: Vec::new(),
            next_cold_gc_seq: 0,
            bucket_usage: Vec::new(),
            last_created_at_ms: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSnapshotEntry {
    pub metadata: StreamMetadata,
    pub hot_start_offset: u64,
    pub payload: Vec<u8>,
    pub hot_segments: Vec<HotPayloadSegment>,
    pub cold_index_generation: u64,
    pub cold_chunks: Vec<ColdChunkRef>,
    pub external_segments: Vec<ObjectPayloadRef>,
    /// Independent destructive-retention floor.
    pub retained_offset: u64,
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
    #[error(
        "snapshot stream '{stream_id}' visible snapshot offset {snapshot_offset} is beyond tail offset {tail_offset}"
    )]
    SnapshotOffsetOutOfRange {
        stream_id: BucketStreamId,
        snapshot_offset: u64,
        tail_offset: u64,
    },
    #[error(
        "stream snapshot is version {found}; this binary reads stream snapshot version {} only",
        crate::STREAM_SNAPSHOT_VERSION
    )]
    UnsupportedVersion { found: u32 },
    #[error("snapshot cold GC entry {seq} has no owning bucket")]
    UnattributedColdGc { seq: u64 },
    #[error("snapshot cold GC entry {seq} targets a stream without a cold generation")]
    ColdGcWithoutGeneration { seq: u64 },
}
