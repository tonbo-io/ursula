use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;

use crate::history::HistoryEvent;
use crate::history::InheritedHistory;
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
    /// Backup format of this snapshot ([`crate::BACKUP_FORMAT_VERSION`]),
    /// under the wire key `format_epoch` that every backup format has used.
    /// Deliberately not `serde(default)`: a MessagePack snapshot from Ursula
    /// 0.5.x (epoch 1) has no such field and fails to decode instead of being
    /// misread.
    #[serde(rename = "format_epoch")]
    pub format_version: u32,
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
    /// The group's latest history event; `None` before its genesis.
    pub history: Option<HistoryEvent>,
}

/// An empty snapshot of this binary's backup format. Fixtures restore from it.
impl Default for StreamSnapshot {
    fn default() -> Self {
        Self {
            format_version: crate::BACKUP_FORMAT_VERSION,
            buckets: Vec::new(),
            erased_buckets: Vec::new(),
            streams: Vec::new(),
            pending_cold_gc: Vec::new(),
            next_cold_gc_seq: 0,
            bucket_usage: Vec::new(),
            last_created_at_ms: 0,
            history: None,
        }
    }
}

impl StreamSnapshot {
    /// Refuses a snapshot document of another backup format before anything
    /// else of it is trusted. `found` is its `format_epoch` field, `None` when
    /// it has none (Ursula 0.5.x). Restore, the import route and `ursulactl`
    /// share this one check.
    pub fn check_format(found: Option<u32>) -> Result<(), StreamSnapshotError> {
        match found {
            Some(crate::BACKUP_FORMAT_VERSION) => Ok(()),
            found => Err(StreamSnapshotError::BackupFormat { found }),
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
    /// The stream's epoch floor: the highest epoch of any producer record it
    /// retired.
    pub retired_epoch_watermark: Option<u64>,
    /// The group's history id when the stream was created or last written.
    pub history: u64,
    /// What the stream's current history began from, recorded by its first
    /// write after a history event.
    pub inherited: Option<InheritedHistory>,
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
    #[error("{}", snapshot_backup_format_message(.found))]
    BackupFormat { found: Option<u32> },
    #[error("snapshot cold GC entry {seq} has no owning bucket")]
    UnattributedColdGc { seq: u64 },
    #[error("snapshot cold GC entry {seq} targets a stream without a cold generation")]
    ColdGcWithoutGeneration { seq: u64 },
}

fn snapshot_backup_format_message(found: &Option<u32>) -> String {
    let found = match found {
        Some(found) => format!("is backup format {found}"),
        None => "has no format_epoch field".to_owned(),
    };
    crate::backup_format_refusal("snapshot", &found)
}
