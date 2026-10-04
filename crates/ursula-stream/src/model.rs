use serde::Deserialize;
use serde::Serialize;
use ursula_proto::ColdChunkRefV1;
use ursula_proto::ExternalPayloadRefV1;
use ursula_proto::ProducerRequestV1;
use ursula_shard::BucketStreamId;

pub const COLD_INDEX_PAGE_SPAN_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamStatus {
    Open,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMetadata {
    pub stream_id: BucketStreamId,
    pub content_type: String,
    pub status: StreamStatus,
    pub tail_offset: u64,
    pub last_stream_seq: Option<String>,
    pub stream_ttl_seconds: Option<u64>,
    pub stream_expires_at_ms: Option<u64>,
    pub created_at_ms: u64,
    pub last_ttl_touch_at_ms: u64,
}

pub type ProducerRequest = ProducerRequestV1;

#[derive(Debug)]
pub struct AppendStreamInput<'a> {
    pub stream_id: BucketStreamId,
    pub content_type: Option<&'a str>,
    pub payload: &'a [u8],
    pub close_after: bool,
    pub stream_seq: Option<String>,
    pub producer: Option<ProducerRequest>,
    pub now_ms: u64,
    pub record_match: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct AppendExternalInput<'a> {
    pub(crate) stream_id: BucketStreamId,
    pub(crate) content_type: Option<&'a str>,
    pub(crate) payload: ExternalPayloadRef,
    pub(crate) record_ends: Vec<u64>,
    pub(crate) close_after: bool,
    pub(crate) stream_seq: Option<String>,
    pub(crate) producer: Option<ProducerRequest>,
    pub(crate) now_ms: u64,
    pub(crate) record_match: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerSnapshot {
    pub producer_id: String,
    pub producer_epoch: u64,
    pub producer_seq: u64,
    pub last_start_offset: u64,
    pub last_next_offset: u64,
    pub last_closed: bool,
    /// Exact response history for delayed retries, oldest first, bounded by
    /// the stream's receipt window (F3). A producer's newest receipt is never
    /// evicted, so it is never empty.
    pub receipts: Vec<ProducerReceipt>,
    /// `now_ms` of the producer's newest accepted write (F3 idle expiry).
    pub last_seen_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerReceipt {
    pub producer_seq: u64,
    pub start_offset: u64,
    pub next_offset: u64,
    pub closed: bool,
    pub items: Vec<ProducerAppendRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerAppendRecord {
    pub start_offset: u64,
    pub next_offset: u64,
    pub closed: bool,
    #[serde(default)]
    pub record_start: Option<u64>,
    #[serde(default)]
    pub record_next: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProducerState {
    pub(crate) producer_epoch: u64,
    pub(crate) producer_seq: u64,
    pub(crate) last_start_offset: u64,
    pub(crate) last_next_offset: u64,
    pub(crate) last_closed: bool,
    /// Receipts of the current epoch in sequence order, with contiguous
    /// sequences, so a duplicate's receipt sits at `seq - front.seq`.
    pub(crate) receipts: std::collections::VecDeque<ProducerReceipt>,
    /// See [`ProducerSnapshot::last_seen_ms`].
    pub(crate) last_seen_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRead {
    pub offset: u64,
    pub next_offset: u64,
    pub content_type: String,
    pub payload: Vec<u8>,
    pub up_to_date: bool,
    pub closed: bool,
}

pub type ColdChunkRef = ColdChunkRefV1;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectPayloadRef {
    pub start_offset: u64,
    pub end_offset: u64,
    pub s3_path: String,
    pub object_size: u64,
    #[serde(default)]
    pub object_offset: u64,
}

impl From<&ColdChunkRef> for ObjectPayloadRef {
    fn from(chunk: &ColdChunkRef) -> Self {
        Self {
            start_offset: chunk.start_offset,
            end_offset: chunk.end_offset,
            s3_path: chunk.s3_path.clone(),
            object_size: chunk.object_size,
            object_offset: chunk.object_offset,
        }
    }
}

pub type ExternalPayloadRef = ExternalPayloadRefV1;

/// One unit of deferred cold-storage reclamation. Enqueued deterministically in
/// the state machine when a stream's cold objects become unreferenced, drained
/// asynchronously by the leader's background GC worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdGcEntry {
    pub seq: u64,
    /// Bucket-scoped erasure domain owning every byte referenced by `target`.
    /// Physical packs never cross this boundary.
    pub bucket_id: String,
    /// Earliest wall-clock timestamp at which the physical object may be
    /// reclaimed. Zero preserves the immediate behavior of legacy entries.
    #[serde(default)]
    pub not_before_ms: u64,
    pub target: ColdGcTarget,
    /// Cold generation of the removed incarnation for a
    /// [`ColdGcTarget::Stream`] entry (F14g step 2): the worker deletes only
    /// that generation's cold-index pages, the objects they reference, and
    /// chunk names scoped to it. Always set for `Stream` targets and always
    /// `None` for `Paths` targets; the worker refuses a `Stream` entry
    /// without a generation as corrupt and deletes nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cold_generation: Option<u64>,
    /// How many times the leader's GC worker deferred this entry after a
    /// failure (`DeferColdGc`, F14b). The worker backs off
    /// exponentially in it.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub defer_attempts: u32,
}

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

/// A pending GC entry as planned for the leader's GC worker, with the cold
/// generation of the live stream that currently holds the entry's name, if
/// any. Not replicated: the worker uses it to avoid deleting a live
/// incarnation's objects (F14g step 1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdGcPlanEntry {
    pub entry: ColdGcEntry,
    pub live_cold_generation: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColdGcTarget {
    /// Every cold object owned by a fully removed stream incarnation. The
    /// worker deletes only object names Ursula writes for that stream and
    /// never recurses into another stream's namespace (F14g).
    Stream(BucketStreamId),
    /// Specific cold object paths dropped while the stream lives on (snapshot
    /// retention compaction).
    Paths(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HotPayloadSegment {
    pub start_offset: u64,
    pub end_offset: u64,
    pub payload_start: usize,
    pub payload_end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdFlushCandidate {
    pub stream_id: BucketStreamId,
    /// Cold generation of the planned incarnation (F14g); the chunk name
    /// and cold-index pages written for this candidate use it.
    pub cold_generation: u64,
    pub start_offset: u64,
    pub end_offset: u64,
    pub payload: Vec<u8>,
    pub payload_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamReadColdSegment {
    pub chunk: ColdChunkRef,
    pub read_start_offset: u64,
    pub len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamReadObjectSegment {
    pub object: ObjectPayloadRef,
    pub read_start_offset: u64,
    pub len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamReadColdIndexSegment {
    pub generation: u64,
    pub page_id: u64,
    pub read_start_offset: u64,
    pub len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamReadSegment {
    ColdIndex(StreamReadColdIndexSegment),
    Object(StreamReadObjectSegment),
    Hot(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamReadPlan {
    pub offset: u64,
    pub next_offset: u64,
    pub content_type: String,
    pub segments: Vec<StreamReadSegment>,
    pub up_to_date: bool,
    pub closed: bool,
    pub retained_record_range: Option<crate::StreamRecordRange>,
    pub record_range: Option<crate::StreamRecordRange>,
    /// Set on a bracketed record read (F1): the segments cover a byte
    /// window around the requested records, and materialization trims it
    /// by counting LFs, then rewrites `offset`, `next_offset`,
    /// `record_range` and `up_to_date`. Until then `up_to_date` is false.
    pub record_trim: Option<Box<crate::RecordTrim>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMessageRecord {
    pub start_offset: u64,
    pub end_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamVisibleSnapshot {
    pub offset: u64,
    pub content_type: String,
    /// Inline body. Empty when `object` holds the body.
    pub payload: Vec<u8>,
    /// BLAKE3 digest over the content type and body. Empty only when
    /// decoding legacy snapshots; restore recomputes it.
    #[serde(default)]
    pub digest: String,
    /// Cold-tier object holding the whole body (bounded-state F16). `payload_len` is the body length.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<ExternalPayloadRef>,
}

/// Largest snapshot body Ursula stores as a cold-tier object (F16). Inline
/// bodies stay bounded by the HTTP body cap.
pub const MAX_COLD_SNAPSHOT_BYTES: u64 = 1024 * 1024 * 1024;

/// Incremental form of the snapshot digest: BLAKE3 over the content type's
/// length, the content type and the body. The HTTP layer feeds a staged
/// body through it piece by piece; apply uses it for inline bodies.
#[derive(Debug, Clone)]
pub struct SnapshotDigest(blake3::Hasher);

impl SnapshotDigest {
    pub fn new(content_type: &str) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&(content_type.len() as u64).to_le_bytes());
        hasher.update(content_type.as_bytes());
        Self(hasher)
    }

    pub fn update(&mut self, body: &[u8]) {
        self.0.update(body);
    }

    pub fn finalize(&self) -> String {
        self.0.finalize().to_hex().to_string()
    }
}

/// Default cap on the update bytes one `/bootstrap` response carries
/// (bounded-stream-state F11, the 8 MiB server read cap).
pub const BOOTSTRAP_MAX_UPDATE_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamBootstrapPlan {
    pub snapshot: Option<StreamVisibleSnapshot>,
    pub updates: Vec<StreamMessageRecord>,
    pub next_offset: u64,
    pub content_type: String,
    pub up_to_date: bool,
    pub closed: bool,
}

/// Per-bucket committed usage inside one Raft group's replicated state.
///
/// `committed_append_bytes`, `committed_records`, and
/// `committed_write_units` are monotonic: they count
/// committed (non-deduplicated) appends and survive restarts through the
/// snapshot. `retained_bytes` and `stream_count` are gauges derived from live
/// stream state and are recomputed from the restored slots, so drift cannot
/// accumulate across snapshot cycles. Purging a bucket zeros the gauges but
/// retains the monotonic counters: otherwise committed writes can disappear
/// before an asynchronous accounting reader observes them. A bucket-wide total
/// is the sum of this value across every Raft group.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketUsage {
    pub committed_append_bytes: u64,
    pub committed_records: u64,
    /// Committed write operations measured in the unit declared by the usage
    /// API, rounded up per operation and never below one. The API describes
    /// the unit separately so this state is a usage fact rather than a price
    /// name. Kept in replicated state because a gateway cannot distinguish
    /// "commit succeeded, response was lost" from "the write never committed".
    #[serde(default, alias = "committed_write_units_10kib")]
    pub committed_write_units: u64,
    pub retained_bytes: u64,
    pub stream_count: u64,
}

/// One bucket's usage as reported by a group or persisted in a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketUsageSnapshot {
    pub bucket_id: String,
    pub usage: BucketUsage,
}
