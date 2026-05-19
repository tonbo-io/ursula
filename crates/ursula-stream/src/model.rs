use serde::{Deserialize, Serialize};
use ursula_proto::{ColdChunkRefV1, ExternalPayloadRefV1, ProducerRequestV1};
use ursula_shard::BucketStreamId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamStatus {
    Open,
    Closed,
    SoftDeleted,
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
    pub forked_from: Option<BucketStreamId>,
    pub fork_offset: Option<u64>,
    pub fork_ref_count: u64,
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
}

#[derive(Debug)]
pub(crate) struct AppendExternalInput<'a> {
    pub(crate) stream_id: BucketStreamId,
    pub(crate) content_type: Option<&'a str>,
    pub(crate) payload: ExternalPayloadRef,
    pub(crate) close_after: bool,
    pub(crate) stream_seq: Option<String>,
    pub(crate) producer: Option<ProducerRequest>,
    pub(crate) now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerSnapshot {
    pub producer_id: String,
    pub producer_epoch: u64,
    pub producer_seq: u64,
    pub last_start_offset: u64,
    pub last_next_offset: u64,
    pub last_closed: bool,
    pub last_items: Vec<ProducerAppendRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerAppendRecord {
    pub start_offset: u64,
    pub next_offset: u64,
    pub closed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProducerState {
    pub(crate) producer_epoch: u64,
    pub(crate) producer_seq: u64,
    pub(crate) last_start_offset: u64,
    pub(crate) last_next_offset: u64,
    pub(crate) last_closed: bool,
    pub(crate) last_items: Vec<ProducerAppendRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamBatchAppend {
    pub items: Vec<StreamBatchAppendItem>,
    pub deduplicated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamBatchAppendItem {
    pub offset: u64,
    pub next_offset: u64,
    pub closed: bool,
    pub deduplicated: bool,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectPayloadRef {
    pub start_offset: u64,
    pub end_offset: u64,
    pub s3_path: String,
    pub object_size: u64,
}

impl From<&ColdChunkRef> for ObjectPayloadRef {
    fn from(chunk: &ColdChunkRef) -> Self {
        Self {
            start_offset: chunk.start_offset,
            end_offset: chunk.end_offset,
            s3_path: chunk.s3_path.clone(),
            object_size: chunk.object_size,
        }
    }
}

pub type ExternalPayloadRef = ExternalPayloadRefV1;

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
    pub start_offset: u64,
    pub end_offset: u64,
    pub payload: Vec<u8>,
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
pub enum StreamReadSegment {
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
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamBootstrapPlan {
    pub snapshot: Option<StreamVisibleSnapshot>,
    pub updates: Vec<StreamMessageRecord>,
    pub next_offset: u64,
    pub content_type: String,
    pub up_to_date: bool,
    pub closed: bool,
}
