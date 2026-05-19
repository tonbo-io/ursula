use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use ursula_proto::{ColdChunkRefV1, ExternalPayloadRefV1, ProducerRequestV1};
use ursula_shard::BucketStreamId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamCommand {
    CreateBucket {
        bucket_id: String,
    },
    DeleteBucket {
        bucket_id: String,
    },
    CreateStream {
        stream_id: BucketStreamId,
        content_type: String,
        initial_payload: Vec<u8>,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        stream_ttl_seconds: Option<u64>,
        stream_expires_at_ms: Option<u64>,
        forked_from: Option<BucketStreamId>,
        fork_offset: Option<u64>,
        now_ms: u64,
    },
    CreateExternal {
        stream_id: BucketStreamId,
        content_type: String,
        initial_payload: ExternalPayloadRef,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        stream_ttl_seconds: Option<u64>,
        stream_expires_at_ms: Option<u64>,
        forked_from: Option<BucketStreamId>,
        fork_offset: Option<u64>,
        now_ms: u64,
    },
    Append {
        stream_id: BucketStreamId,
        content_type: Option<String>,
        payload: Vec<u8>,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    },
    AppendExternal {
        stream_id: BucketStreamId,
        content_type: Option<String>,
        payload: ExternalPayloadRef,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    },
    AppendBatch {
        stream_id: BucketStreamId,
        content_type: Option<String>,
        payloads: Vec<Vec<u8>>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    },
    PublishSnapshot {
        stream_id: BucketStreamId,
        snapshot_offset: u64,
        content_type: String,
        payload: Vec<u8>,
        now_ms: u64,
    },
    TouchStreamAccess {
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
    },
    AddForkRef {
        stream_id: BucketStreamId,
        now_ms: u64,
    },
    ReleaseForkRef {
        stream_id: BucketStreamId,
    },
    FlushCold {
        stream_id: BucketStreamId,
        chunk: ColdChunkRef,
    },
    Close {
        stream_id: BucketStreamId,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    },
    DeleteStream {
        stream_id: BucketStreamId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamResponse {
    BucketCreated {
        bucket_id: String,
    },
    BucketAlreadyExists {
        bucket_id: String,
    },
    BucketDeleted {
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
    },
    Closed {
        next_offset: u64,
        deduplicated: bool,
        producer: Option<ProducerRequest>,
    },
    Deleted {
        hard_deleted: bool,
        parent_to_release: Option<BucketStreamId>,
    },
    ForkRefAdded {
        fork_ref_count: u64,
    },
    ForkRefReleased {
        hard_deleted: bool,
        fork_ref_count: u64,
        parent_to_release: Option<BucketStreamId>,
    },
    ColdFlushed {
        hot_start_offset: u64,
    },
    SnapshotPublished {
        snapshot_offset: u64,
    },
    Accessed {
        changed: bool,
        expired: bool,
    },
    Error {
        code: StreamErrorCode,
        message: String,
        next_offset: Option<u64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamErrorCode {
    InvalidBucketId,
    InvalidStreamId,
    BucketNotFound,
    BucketNotEmpty,
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
    InvalidFork,
    OffsetOutOfRange,
    InvalidColdFlush,
    InvalidSnapshot,
    SnapshotNotFound,
    SnapshotConflict,
}

impl StreamResponse {
    fn error(code: StreamErrorCode, message: impl Into<String>) -> Self {
        Self::Error {
            code,
            message: message.into(),
            next_offset: None,
        }
    }

    fn error_with_next_offset(
        code: StreamErrorCode,
        message: impl Into<String>,
        next_offset: u64,
    ) -> Self {
        Self::Error {
            code,
            message: message.into(),
            next_offset: Some(next_offset),
        }
    }
}

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
struct AppendExternalInput<'a> {
    stream_id: BucketStreamId,
    content_type: Option<&'a str>,
    payload: ExternalPayloadRef,
    close_after: bool,
    stream_seq: Option<String>,
    producer: Option<ProducerRequest>,
    now_ms: u64,
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
struct ProducerState {
    producer_epoch: u64,
    producer_seq: u64,
    last_start_offset: u64,
    last_next_offset: u64,
    last_closed: bool,
    last_items: Vec<ProducerAppendRecord>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSnapshot {
    pub buckets: Vec<String>,
    pub streams: Vec<StreamSnapshotEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSnapshotEntry {
    pub metadata: StreamMetadata,
    pub hot_start_offset: u64,
    pub payload: Vec<u8>,
    pub hot_segments: Vec<HotPayloadSegment>,
    pub cold_chunks: Vec<ColdChunkRef>,
    pub external_segments: Vec<ObjectPayloadRef>,
    pub message_records: Vec<StreamMessageRecord>,
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

#[derive(Debug, Clone, Default)]
pub struct StreamStateMachine {
    buckets: HashSet<String>,
    streams: HashMap<BucketStreamId, StreamMetadata>,
    payloads: HashMap<BucketStreamId, Vec<u8>>,
    hot_segments: HashMap<BucketStreamId, Vec<HotPayloadSegment>>,
    hot_start_offsets: HashMap<BucketStreamId, u64>,
    cold_chunks: HashMap<BucketStreamId, Vec<ColdChunkRef>>,
    external_segments: HashMap<BucketStreamId, Vec<ObjectPayloadRef>>,
    message_records: HashMap<BucketStreamId, Vec<StreamMessageRecord>>,
    visible_snapshots: HashMap<BucketStreamId, StreamVisibleSnapshot>,
    producers: HashMap<BucketStreamId, HashMap<String, ProducerState>>,
}

impl StreamStateMachine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply(&mut self, command: StreamCommand) -> StreamResponse {
        match command {
            StreamCommand::CreateBucket { bucket_id } => self.create_bucket(bucket_id),
            StreamCommand::DeleteBucket { bucket_id } => self.delete_bucket(&bucket_id),
            StreamCommand::CreateStream {
                stream_id,
                content_type,
                initial_payload,
                close_after,
                stream_seq,
                producer,
                stream_ttl_seconds,
                stream_expires_at_ms,
                forked_from,
                fork_offset,
                now_ms,
            } => self.create_stream(CreateStreamInput {
                stream_id,
                content_type,
                initial_payload,
                close_after,
                stream_seq,
                producer,
                stream_ttl_seconds,
                stream_expires_at_ms,
                forked_from,
                fork_offset,
                now_ms,
            }),
            StreamCommand::CreateExternal {
                stream_id,
                content_type,
                initial_payload,
                close_after,
                stream_seq,
                producer,
                stream_ttl_seconds,
                stream_expires_at_ms,
                forked_from,
                fork_offset,
                now_ms,
            } => self.create_external_stream(CreateExternalStreamInput {
                stream_id,
                content_type,
                initial_payload,
                close_after,
                stream_seq,
                producer,
                stream_ttl_seconds,
                stream_expires_at_ms,
                forked_from,
                fork_offset,
                now_ms,
            }),
            StreamCommand::Append {
                stream_id,
                content_type,
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => self.append_borrowed(AppendStreamInput {
                stream_id,
                content_type: content_type.as_deref(),
                payload: &payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            }),
            StreamCommand::AppendExternal {
                stream_id,
                content_type,
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => self.append_external(AppendExternalInput {
                stream_id,
                content_type: content_type.as_deref(),
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            }),
            StreamCommand::AppendBatch {
                stream_id,
                content_type,
                payloads,
                producer,
                now_ms,
            } => match self.append_batch_borrowed(
                stream_id,
                content_type.as_deref(),
                &payloads.iter().map(Vec::as_slice).collect::<Vec<_>>(),
                producer,
                now_ms,
            ) {
                Ok(batch) => batch
                    .items
                    .last()
                    .map(|item| StreamResponse::Appended {
                        offset: item.offset,
                        next_offset: item.next_offset,
                        closed: item.closed,
                        deduplicated: item.deduplicated,
                        producer: None,
                    })
                    .unwrap_or_else(|| {
                        StreamResponse::error(
                            StreamErrorCode::EmptyAppend,
                            "append batch must contain at least one payload",
                        )
                    }),
                Err(response) => response,
            },
            StreamCommand::PublishSnapshot {
                stream_id,
                snapshot_offset,
                content_type,
                payload,
                now_ms,
            } => self.publish_snapshot(stream_id, snapshot_offset, content_type, payload, now_ms),
            StreamCommand::TouchStreamAccess {
                stream_id,
                now_ms,
                renew_ttl,
            } => self.touch_stream_access(&stream_id, now_ms, renew_ttl),
            StreamCommand::AddForkRef { stream_id, now_ms } => {
                self.add_fork_ref(&stream_id, now_ms)
            }
            StreamCommand::ReleaseForkRef { stream_id } => self.release_fork_ref(&stream_id),
            StreamCommand::FlushCold { stream_id, chunk } => self.flush_cold(stream_id, chunk),
            StreamCommand::Close {
                stream_id,
                stream_seq,
                producer,
                now_ms,
            } => self.close(stream_id, stream_seq, producer, now_ms),
            StreamCommand::DeleteStream { stream_id } => self.delete_stream(&stream_id),
        }
    }

    pub fn head(&self, stream_id: &BucketStreamId) -> Option<&StreamMetadata> {
        self.streams.get(stream_id)
    }

    pub fn head_at(&mut self, stream_id: &BucketStreamId, now_ms: u64) -> Option<&StreamMetadata> {
        self.expire_stream_if_due(stream_id, now_ms);
        self.streams.get(stream_id)
    }

    pub fn access_requires_write(
        &self,
        stream_id: &BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
    ) -> Result<bool, StreamResponse> {
        self.validate_stream_scope(stream_id)?;
        let Some(stream) = self.streams.get(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        if is_soft_deleted(stream) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
        if stream_is_expired(stream, now_ms) {
            return Ok(true);
        }
        Ok(renew_ttl
            && stream.stream_ttl_seconds.is_some()
            && stream.last_ttl_touch_at_ms != now_ms)
    }

    pub fn hot_start_offset(&self, stream_id: &BucketStreamId) -> u64 {
        self.hot_start_offsets.get(stream_id).copied().unwrap_or(0)
    }

    pub fn cold_chunks(&self, stream_id: &BucketStreamId) -> &[ColdChunkRef] {
        self.cold_chunks
            .get(stream_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn external_segments(&self, stream_id: &BucketStreamId) -> &[ObjectPayloadRef] {
        self.external_segments
            .get(stream_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn hot_segments(&self, stream_id: &BucketStreamId) -> &[HotPayloadSegment] {
        self.hot_segments
            .get(stream_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn hot_payload_len(&self, stream_id: &BucketStreamId) -> Result<u64, StreamResponse> {
        let Some(stream) = self.streams.get(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        if is_soft_deleted(stream) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
        let payload = self
            .payloads
            .get(stream_id)
            .expect("payload vector exists for stream metadata");
        Ok(u64::try_from(payload.len()).expect("payload len fits u64"))
    }

    pub fn total_hot_payload_bytes(&self) -> u64 {
        self.payloads
            .values()
            .map(|payload| u64::try_from(payload.len()).expect("payload len fits u64"))
            .sum()
    }

    pub fn plan_cold_flush(
        &self,
        stream_id: &BucketStreamId,
        min_hot_bytes: usize,
        max_flush_bytes: usize,
    ) -> Result<Option<ColdFlushCandidate>, StreamResponse> {
        if max_flush_bytes == 0 {
            return Ok(None);
        }
        let Some(stream) = self.streams.get(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        if is_soft_deleted(stream) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
        let Some(first_segment) = self.hot_segments(stream_id).first() else {
            return Ok(None);
        };
        let mut payload_end = first_segment.payload_start;
        let mut end_offset = first_segment.start_offset;
        let mut flush_len = 0usize;
        for segment in self.hot_segments(stream_id) {
            if segment.start_offset != end_offset || segment.payload_start != payload_end {
                break;
            }
            let remaining = max_flush_bytes.saturating_sub(flush_len);
            if remaining == 0 {
                break;
            }
            let segment_len = segment.payload_end - segment.payload_start;
            let take = segment_len.min(remaining);
            flush_len += take;
            payload_end += take;
            end_offset = end_offset.saturating_add(u64::try_from(take).expect("take fits u64"));
            if take < segment_len {
                break;
            }
        }
        if flush_len < min_hot_bytes {
            return Ok(None);
        }
        let payload = self
            .payloads
            .get(stream_id)
            .expect("payload vector exists for stream metadata");
        let start_offset = first_segment.start_offset;
        Ok(Some(ColdFlushCandidate {
            stream_id: stream_id.clone(),
            start_offset,
            end_offset,
            payload: payload[first_segment.payload_start..payload_end].to_vec(),
        }))
    }

    pub fn plan_next_cold_flush(
        &self,
        min_hot_bytes: usize,
        max_flush_bytes: usize,
    ) -> Result<Option<ColdFlushCandidate>, StreamResponse> {
        if max_flush_bytes == 0 {
            return Ok(None);
        }
        let mut stream_ids = self.streams.keys().cloned().collect::<Vec<_>>();
        stream_ids.sort_by(compare_stream_ids);
        for stream_id in stream_ids {
            match self.plan_cold_flush(&stream_id, min_hot_bytes, max_flush_bytes) {
                Ok(Some(candidate)) => return Ok(Some(candidate)),
                Ok(None) => {}
                Err(StreamResponse::Error {
                    code: StreamErrorCode::StreamGone | StreamErrorCode::StreamNotFound,
                    ..
                }) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(None)
    }

    pub fn plan_next_cold_flush_batch(
        &self,
        min_hot_bytes: usize,
        max_flush_bytes: usize,
        max_candidates: usize,
    ) -> Result<Vec<ColdFlushCandidate>, StreamResponse> {
        if max_candidates == 0 || max_flush_bytes == 0 {
            return Ok(Vec::new());
        }
        let mut preview = self.clone();
        let mut candidates = Vec::with_capacity(max_candidates);
        while candidates.len() < max_candidates {
            let Some(candidate) = preview.plan_next_cold_flush(min_hot_bytes, max_flush_bytes)?
            else {
                break;
            };
            let chunk = ColdChunkRef {
                start_offset: candidate.start_offset,
                end_offset: candidate.end_offset,
                s3_path: "planned-cold-flush-batch".to_owned(),
                object_size: u64::try_from(candidate.payload.len()).expect("payload len fits u64"),
            };
            match preview.flush_cold(candidate.stream_id.clone(), chunk) {
                StreamResponse::ColdFlushed { .. } => candidates.push(candidate),
                StreamResponse::Error { .. } => break,
                other => {
                    return Err(StreamResponse::error(
                        StreamErrorCode::InvalidColdFlush,
                        format!("unexpected cold flush planning response: {other:?}"),
                    ));
                }
            }
        }
        Ok(candidates)
    }

    pub fn bucket_exists(&self, bucket_id: &str) -> bool {
        self.buckets.contains(bucket_id)
    }

    pub fn snapshot(&self) -> StreamSnapshot {
        let mut buckets = self.buckets.iter().cloned().collect::<Vec<_>>();
        buckets.sort();

        let mut streams = self
            .streams
            .values()
            .cloned()
            .map(|metadata| {
                let stream_id = metadata.stream_id.clone();
                let payload = self
                    .payloads
                    .get(&stream_id)
                    .expect("payload vector exists for stream metadata")
                    .clone();
                let producer_states = self.producer_snapshot(&stream_id);
                StreamSnapshotEntry {
                    metadata,
                    hot_start_offset: self.hot_start_offset(&stream_id),
                    payload,
                    hot_segments: self
                        .hot_segments
                        .get(&stream_id)
                        .cloned()
                        .unwrap_or_default(),
                    cold_chunks: self
                        .cold_chunks
                        .get(&stream_id)
                        .cloned()
                        .unwrap_or_default(),
                    external_segments: self
                        .external_segments
                        .get(&stream_id)
                        .cloned()
                        .unwrap_or_default(),
                    message_records: self
                        .message_records
                        .get(&stream_id)
                        .cloned()
                        .unwrap_or_default(),
                    visible_snapshot: self.visible_snapshots.get(&stream_id).cloned(),
                    producer_states,
                }
            })
            .collect::<Vec<_>>();
        streams.sort_by(|left, right| {
            compare_stream_ids(&left.metadata.stream_id, &right.metadata.stream_id)
        });

        StreamSnapshot { buckets, streams }
    }

    pub fn restore(snapshot: StreamSnapshot) -> Result<Self, StreamSnapshotError> {
        let mut machine = Self::default();
        for bucket_id in snapshot.buckets {
            if !machine.buckets.insert(bucket_id.clone()) {
                return Err(StreamSnapshotError::DuplicateBucket(bucket_id));
            }
        }

        for entry in snapshot.streams {
            let stream_id = entry.metadata.stream_id.clone();
            if !machine.buckets.contains(&stream_id.bucket_id) {
                return Err(StreamSnapshotError::MissingBucket(stream_id));
            }
            if let Some(snapshot) = entry.visible_snapshot.as_ref()
                && snapshot.offset > entry.metadata.tail_offset
            {
                return Err(StreamSnapshotError::SnapshotOffsetOutOfRange {
                    stream_id,
                    snapshot_offset: snapshot.offset,
                    tail_offset: entry.metadata.tail_offset,
                });
            }
            let retained_offset = entry
                .visible_snapshot
                .as_ref()
                .map(|snapshot| snapshot.offset)
                .unwrap_or(0);
            let hot_segments = if entry.hot_segments.is_empty() && !entry.payload.is_empty() {
                vec![HotPayloadSegment {
                    start_offset: entry.hot_start_offset,
                    end_offset: entry.metadata.tail_offset,
                    payload_start: 0,
                    payload_end: entry.payload.len(),
                }]
            } else {
                entry.hot_segments
            };
            if !hot_segments_match_payload(&hot_segments, entry.payload.len())
                || !payload_sources_cover_retained_suffix(
                    &entry.cold_chunks,
                    &entry.external_segments,
                    &hot_segments,
                    retained_offset,
                    entry.metadata.tail_offset,
                )
            {
                return Err(StreamSnapshotError::PayloadLengthMismatch {
                    stream_id,
                    tail_offset: entry.metadata.tail_offset,
                    payload_len: entry.payload.len(),
                });
            }
            if !message_records_cover_retained_suffix(
                &entry.message_records,
                retained_offset,
                entry.metadata.tail_offset,
            ) {
                return Err(StreamSnapshotError::MessageBoundaryMismatch { stream_id });
            }
            if machine
                .streams
                .insert(entry.metadata.stream_id.clone(), entry.metadata)
                .is_some()
            {
                return Err(StreamSnapshotError::DuplicateStream(stream_id));
            }
            let producer_states = restore_producer_states(&stream_id, entry.producer_states)?;
            if !hot_segments.is_empty() {
                machine.hot_segments.insert(stream_id.clone(), hot_segments);
            }
            if !entry.cold_chunks.is_empty() {
                machine
                    .cold_chunks
                    .insert(stream_id.clone(), entry.cold_chunks);
            }
            if !entry.external_segments.is_empty() {
                machine
                    .external_segments
                    .insert(stream_id.clone(), entry.external_segments);
            }
            if !entry.message_records.is_empty() {
                machine
                    .message_records
                    .insert(stream_id.clone(), entry.message_records);
            }
            if let Some(snapshot) = entry.visible_snapshot {
                machine
                    .visible_snapshots
                    .insert(stream_id.clone(), snapshot);
            }
            machine.payloads.insert(stream_id.clone(), entry.payload);
            machine.producers.insert(stream_id.clone(), producer_states);
            machine.refresh_hot_start_offset(&stream_id);
        }

        Ok(machine)
    }

    pub fn read(
        &self,
        stream_id: &BucketStreamId,
        offset: u64,
        max_len: usize,
    ) -> Result<StreamRead, StreamResponse> {
        let plan = self.read_plan(stream_id, offset, max_len)?;
        if plan
            .segments
            .iter()
            .any(|segment| matches!(segment, StreamReadSegment::Object(_)))
        {
            return Err(StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                format!("stream '{stream_id}' read requires object payload store"),
                plan.next_offset,
            ));
        }
        let payload = plan
            .segments
            .iter()
            .flat_map(|segment| match segment {
                StreamReadSegment::Hot(payload) => payload.as_slice(),
                StreamReadSegment::Object(_) => unreachable!("object segments checked above"),
            })
            .copied()
            .collect();
        Ok(StreamRead {
            offset: plan.offset,
            next_offset: plan.next_offset,
            content_type: plan.content_type,
            payload,
            up_to_date: plan.up_to_date,
            closed: plan.closed,
        })
    }

    pub fn read_plan(
        &self,
        stream_id: &BucketStreamId,
        offset: u64,
        max_len: usize,
    ) -> Result<StreamReadPlan, StreamResponse> {
        self.read_plan_at(stream_id, offset, max_len, 0)
    }

    pub fn read_plan_at(
        &self,
        stream_id: &BucketStreamId,
        offset: u64,
        max_len: usize,
        now_ms: u64,
    ) -> Result<StreamReadPlan, StreamResponse> {
        let Some(stream) = self.streams.get(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        if is_soft_deleted(stream) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
        if stream_is_expired(stream, now_ms) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        }
        if offset > stream.tail_offset {
            return Err(StreamResponse::error_with_next_offset(
                StreamErrorCode::OffsetOutOfRange,
                format!(
                    "offset {offset} is beyond stream '{}' tail {}",
                    stream_id, stream.tail_offset
                ),
                stream.tail_offset,
            ));
        }
        let retained_offset = self.earliest_retained_offset(stream_id);
        if offset < retained_offset {
            return Err(StreamResponse::error_with_next_offset(
                StreamErrorCode::StreamGone,
                format!(
                    "offset {offset} is older than stream '{}' retained offset {retained_offset}",
                    stream_id
                ),
                retained_offset,
            ));
        }

        let max_len_u64 = u64::try_from(max_len).unwrap_or(u64::MAX);
        let next_offset = stream.tail_offset.min(offset.saturating_add(max_len_u64));
        let payload = self
            .payloads
            .get(stream_id)
            .expect("payload vector exists for stream metadata");
        let mut segments = Vec::<(u64, StreamReadSegment)>::new();
        for chunk in self.cold_chunks(stream_id) {
            let start = offset.max(chunk.start_offset);
            let end = next_offset.min(chunk.end_offset);
            if start < end {
                segments.push((
                    start,
                    StreamReadSegment::Object(StreamReadObjectSegment {
                        object: ObjectPayloadRef::from(chunk),
                        read_start_offset: start,
                        len: usize::try_from(end - start).expect("object read len fits usize"),
                    }),
                ));
            }
        }
        for object in self.external_segments(stream_id) {
            let start = offset.max(object.start_offset);
            let end = next_offset.min(object.end_offset);
            if start < end {
                segments.push((
                    start,
                    StreamReadSegment::Object(StreamReadObjectSegment {
                        object: object.clone(),
                        read_start_offset: start,
                        len: usize::try_from(end - start).expect("object read len fits usize"),
                    }),
                ));
            }
        }
        for segment in self.hot_segments(stream_id) {
            let start = offset.max(segment.start_offset);
            let end = next_offset.min(segment.end_offset);
            if start < end {
                let payload_start = segment.payload_start
                    + usize::try_from(start - segment.start_offset)
                        .expect("hot segment start fits usize");
                let payload_end = segment.payload_start
                    + usize::try_from(end - segment.start_offset)
                        .expect("hot segment end fits usize");
                segments.push((
                    start,
                    StreamReadSegment::Hot(payload[payload_start..payload_end].to_vec()),
                ));
            }
        }
        segments.sort_by_key(|(start, _)| *start);
        if !segments_cover_range(&segments, offset, next_offset) {
            return Err(StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                format!("stream '{stream_id}' has missing payload segment metadata"),
                next_offset,
            ));
        }
        Ok(StreamReadPlan {
            offset,
            next_offset,
            content_type: stream.content_type.clone(),
            segments: segments.into_iter().map(|(_, segment)| segment).collect(),
            up_to_date: next_offset == stream.tail_offset,
            closed: stream.status == StreamStatus::Closed,
        })
    }

    pub fn latest_snapshot(
        &self,
        stream_id: &BucketStreamId,
    ) -> Result<Option<StreamVisibleSnapshot>, StreamResponse> {
        let Some(stream) = self.streams.get(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        if is_soft_deleted(stream) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
        Ok(self.visible_snapshots.get(stream_id).cloned())
    }

    pub fn read_snapshot(
        &self,
        stream_id: &BucketStreamId,
        snapshot_offset: u64,
    ) -> Result<StreamVisibleSnapshot, StreamResponse> {
        let snapshot = self.latest_snapshot(stream_id)?;
        match snapshot {
            Some(snapshot) if snapshot.offset == snapshot_offset => Ok(snapshot),
            _ => Err(StreamResponse::error(
                StreamErrorCode::SnapshotNotFound,
                format!("snapshot {snapshot_offset} for stream '{stream_id}' does not exist"),
            )),
        }
    }

    pub fn delete_snapshot(
        &self,
        stream_id: &BucketStreamId,
        snapshot_offset: u64,
    ) -> StreamResponse {
        match self.latest_snapshot(stream_id) {
            Ok(Some(snapshot)) if snapshot.offset == snapshot_offset => StreamResponse::error(
                StreamErrorCode::SnapshotConflict,
                format!(
                    "snapshot {snapshot_offset} for stream '{stream_id}' is the latest visible snapshot"
                ),
            ),
            Ok(_) => StreamResponse::error(
                StreamErrorCode::SnapshotNotFound,
                format!("snapshot {snapshot_offset} for stream '{stream_id}' does not exist"),
            ),
            Err(err) => err,
        }
    }

    pub fn bootstrap_plan(
        &self,
        stream_id: &BucketStreamId,
    ) -> Result<StreamBootstrapPlan, StreamResponse> {
        let Some(stream) = self.streams.get(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        if is_soft_deleted(stream) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
        let snapshot = self.visible_snapshots.get(stream_id).cloned();
        let retained_offset = snapshot
            .as_ref()
            .map(|snapshot| snapshot.offset)
            .unwrap_or(0);
        let updates = self
            .message_records
            .get(stream_id)
            .map(|records| {
                records
                    .iter()
                    .filter(|record| record.start_offset >= retained_offset)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Ok(StreamBootstrapPlan {
            snapshot,
            updates,
            next_offset: stream.tail_offset,
            content_type: stream.content_type.clone(),
            up_to_date: true,
            closed: stream.status == StreamStatus::Closed,
        })
    }

    fn publish_snapshot(
        &mut self,
        stream_id: BucketStreamId,
        snapshot_offset: u64,
        content_type: String,
        payload: Vec<u8>,
        now_ms: u64,
    ) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(&stream_id) {
            return response;
        }
        if content_type.trim().is_empty() {
            return StreamResponse::error(
                StreamErrorCode::InvalidSnapshot,
                "snapshot content type must not be empty",
            );
        }
        let Some(stream) = self.streams.get(&stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        if is_soft_deleted(stream) {
            return StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            );
        }
        if stream_is_expired(stream, now_ms) {
            self.remove_stream_state(&stream_id);
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        }
        let tail_offset = stream.tail_offset;
        let retained_offset = self.earliest_retained_offset(&stream_id);
        if snapshot_offset < retained_offset {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::StreamGone,
                format!(
                    "snapshot offset {snapshot_offset} is older than stream '{}' retained offset {retained_offset}",
                    stream_id
                ),
                retained_offset,
            );
        }
        if snapshot_offset > tail_offset {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::SnapshotConflict,
                format!(
                    "snapshot offset {snapshot_offset} is beyond stream '{}' tail {tail_offset}",
                    stream_id
                ),
                tail_offset,
            );
        }
        if !self.snapshot_offset_aligned(&stream_id, snapshot_offset, retained_offset) {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidSnapshot,
                format!(
                    "snapshot offset {snapshot_offset} is not aligned to a committed message boundary for stream '{stream_id}'"
                ),
                tail_offset,
            );
        }

        self.visible_snapshots.insert(
            stream_id.clone(),
            StreamVisibleSnapshot {
                offset: snapshot_offset,
                content_type,
                payload,
            },
        );
        self.compact_retained_prefix(&stream_id, snapshot_offset);
        StreamResponse::SnapshotPublished { snapshot_offset }
    }

    fn flush_cold(&mut self, stream_id: BucketStreamId, chunk: ColdChunkRef) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(&stream_id) {
            return response;
        }
        if chunk.s3_path.trim().is_empty() {
            return StreamResponse::error(
                StreamErrorCode::InvalidColdFlush,
                "cold chunk S3 path must not be empty",
            );
        }
        if chunk.object_size == 0 {
            return StreamResponse::error(
                StreamErrorCode::InvalidColdFlush,
                "cold chunk object size must be greater than zero",
            );
        }
        let Some(stream) = self.streams.get(&stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        if is_soft_deleted(stream) {
            return StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            );
        }
        if chunk.end_offset <= chunk.start_offset {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                "cold chunk must cover at least one byte",
                stream.tail_offset,
            );
        }
        if chunk.end_offset > stream.tail_offset {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                format!(
                    "cold chunk end {} is beyond stream '{}' tail {}",
                    chunk.end_offset, stream_id, stream.tail_offset
                ),
                stream.tail_offset,
            );
        }
        let segments = self.hot_segments(&stream_id);
        let Some(segment_index) = segments
            .iter()
            .position(|segment| segment.start_offset == chunk.start_offset)
        else {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                format!(
                    "cold chunk for stream '{stream_id}' does not match the start of a hot payload segment"
                ),
                stream.tail_offset,
            );
        };

        let drain_start = segments[segment_index].payload_start;
        let mut covered_offset = chunk.start_offset;
        let mut flush_len = 0usize;
        for segment in segments.iter().skip(segment_index) {
            if segment.start_offset != covered_offset {
                break;
            }
            let segment_cover_end = chunk.end_offset.min(segment.end_offset);
            let segment_flush_len = match usize::try_from(segment_cover_end - segment.start_offset)
            {
                Ok(segment_flush_len) => segment_flush_len,
                Err(_) => {
                    return StreamResponse::error_with_next_offset(
                        StreamErrorCode::InvalidColdFlush,
                        "cold chunk length does not fit in memory",
                        stream.tail_offset,
                    );
                }
            };
            let Some(expected_payload_start) = drain_start.checked_add(flush_len) else {
                return StreamResponse::error_with_next_offset(
                    StreamErrorCode::InvalidColdFlush,
                    "cold chunk length does not fit in memory",
                    stream.tail_offset,
                );
            };
            if segment.payload_start != expected_payload_start {
                return StreamResponse::error_with_next_offset(
                    StreamErrorCode::InvalidColdFlush,
                    format!("stream '{stream_id}' has non-contiguous hot payload metadata"),
                    stream.tail_offset,
                );
            }
            let segment_payload_len = segment.payload_end - segment.payload_start;
            if segment_flush_len > segment_payload_len {
                return StreamResponse::error_with_next_offset(
                    StreamErrorCode::InvalidColdFlush,
                    format!("cold chunk length exceeds stream '{stream_id}' hot segment metadata"),
                    stream.tail_offset,
                );
            }
            let Some(new_flush_len) = flush_len.checked_add(segment_flush_len) else {
                return StreamResponse::error_with_next_offset(
                    StreamErrorCode::InvalidColdFlush,
                    "cold chunk length does not fit in memory",
                    stream.tail_offset,
                );
            };
            flush_len = new_flush_len;
            covered_offset = segment_cover_end;
            if covered_offset == chunk.end_offset {
                break;
            }
        }
        if covered_offset != chunk.end_offset {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                format!(
                    "cold chunk for stream '{stream_id}' does not cover contiguous hot payload segments"
                ),
                stream.tail_offset,
            );
        }
        let Some(drain_end) = drain_start.checked_add(flush_len) else {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                "cold chunk length does not fit in memory",
                stream.tail_offset,
            );
        };
        let payload_len = self
            .payloads
            .get(&stream_id)
            .expect("payload vector exists for stream metadata")
            .len();
        if drain_end > payload_len {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                format!("cold chunk length exceeds stream '{stream_id}' hot payload length"),
                stream.tail_offset,
            );
        };

        self.payloads
            .get_mut(&stream_id)
            .expect("payload vector exists for stream metadata")
            .drain(drain_start..drain_end);
        self.remove_drained_hot_range(
            &stream_id,
            segment_index,
            chunk.end_offset,
            drain_start,
            flush_len,
        );
        self.cold_chunks
            .entry(stream_id.clone())
            .or_default()
            .push(chunk.clone());
        self.refresh_hot_start_offset(&stream_id);
        StreamResponse::ColdFlushed {
            hot_start_offset: self.hot_start_offset(&stream_id),
        }
    }

    fn create_bucket(&mut self, bucket_id: String) -> StreamResponse {
        if let Err(message) = validate_bucket_id(&bucket_id) {
            return StreamResponse::error(StreamErrorCode::InvalidBucketId, message);
        }
        if !self.buckets.insert(bucket_id.clone()) {
            return StreamResponse::BucketAlreadyExists { bucket_id };
        }
        StreamResponse::BucketCreated { bucket_id }
    }

    fn delete_bucket(&mut self, bucket_id: &str) -> StreamResponse {
        if let Err(message) = validate_bucket_id(bucket_id) {
            return StreamResponse::error(StreamErrorCode::InvalidBucketId, message);
        }
        if !self.buckets.contains(bucket_id) {
            return StreamResponse::error(
                StreamErrorCode::BucketNotFound,
                format!("bucket '{bucket_id}' does not exist"),
            );
        }
        if self
            .streams
            .keys()
            .any(|stream_id| stream_id.bucket_id == bucket_id)
        {
            return StreamResponse::error(
                StreamErrorCode::BucketNotEmpty,
                format!("bucket '{bucket_id}' is not empty"),
            );
        }
        self.buckets.remove(bucket_id);
        StreamResponse::BucketDeleted {
            bucket_id: bucket_id.to_owned(),
        }
    }

    fn create_stream(&mut self, input: CreateStreamInput) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(&input.stream_id) {
            return response;
        }
        if let Err(response) =
            validate_retention(input.stream_ttl_seconds, input.stream_expires_at_ms)
        {
            return response;
        }
        if let Err(response) = validate_producer_request(input.producer.as_ref()) {
            return response;
        }
        if let Some(producer) = input.producer.as_ref()
            && producer.producer_seq != 0
        {
            return StreamResponse::error(
                StreamErrorCode::ProducerSeqConflict,
                format!(
                    "producer '{}' expected sequence 0, received {}",
                    producer.producer_id, producer.producer_seq
                ),
            );
        }
        if self
            .streams
            .get(&input.stream_id)
            .is_some_and(|existing| stream_is_expired(existing, input.now_ms))
        {
            self.remove_stream_state(&input.stream_id);
        }

        if let Some(existing) = self.streams.get(&input.stream_id) {
            if is_soft_deleted(existing) {
                return StreamResponse::error(
                    StreamErrorCode::StreamAlreadyExistsConflict,
                    format!(
                        "stream '{}' is gone and cannot be recreated yet",
                        input.stream_id
                    ),
                );
            }
            if existing.content_type == input.content_type
                && existing.status == status_from_closed(input.close_after)
                && existing.stream_ttl_seconds == input.stream_ttl_seconds
                && existing.stream_expires_at_ms == input.stream_expires_at_ms
                && existing.forked_from == input.forked_from
                && existing.fork_offset == input.fork_offset
            {
                return StreamResponse::AlreadyExists {
                    next_offset: existing.tail_offset,
                    closed: existing.status == StreamStatus::Closed,
                    content_type: existing.content_type.clone(),
                    stream_ttl_seconds: existing.stream_ttl_seconds,
                    stream_expires_at_ms: existing.stream_expires_at_ms,
                };
            }
            return StreamResponse::error(
                StreamErrorCode::StreamAlreadyExistsConflict,
                format!(
                    "stream '{}' already exists with different metadata",
                    input.stream_id
                ),
            );
        }

        let initial_len = input.initial_len();
        let metadata = StreamMetadata {
            stream_id: input.stream_id.clone(),
            content_type: input.content_type,
            status: status_from_closed(input.close_after),
            tail_offset: initial_len,
            last_stream_seq: input.stream_seq,
            stream_ttl_seconds: input.stream_ttl_seconds,
            stream_expires_at_ms: input.stream_expires_at_ms,
            created_at_ms: input.now_ms,
            last_ttl_touch_at_ms: input.now_ms,
            forked_from: input.forked_from,
            fork_offset: input.fork_offset,
            fork_ref_count: 0,
        };
        self.streams.insert(input.stream_id.clone(), metadata);
        self.payloads
            .insert(input.stream_id.clone(), input.initial_payload);
        if initial_len > 0 {
            self.hot_segments.insert(
                input.stream_id.clone(),
                vec![HotPayloadSegment {
                    start_offset: 0,
                    end_offset: initial_len,
                    payload_start: 0,
                    payload_end: usize::try_from(initial_len).expect("payload len fits usize"),
                }],
            );
            self.message_records.insert(
                input.stream_id.clone(),
                vec![StreamMessageRecord {
                    start_offset: 0,
                    end_offset: initial_len,
                }],
            );
        }
        let mut producer_states = HashMap::new();
        if let Some(producer) = input.producer {
            let last_item = ProducerAppendRecord {
                start_offset: 0,
                next_offset: initial_len,
                closed: input.close_after,
            };
            producer_states.insert(
                producer.producer_id,
                ProducerState {
                    producer_epoch: producer.producer_epoch,
                    producer_seq: producer.producer_seq,
                    last_start_offset: last_item.start_offset,
                    last_next_offset: last_item.next_offset,
                    last_closed: last_item.closed,
                    last_items: vec![last_item],
                },
            );
        }
        self.producers
            .insert(input.stream_id.clone(), producer_states);
        StreamResponse::Created {
            stream_id: input.stream_id,
            next_offset: initial_len,
            closed: input.close_after,
        }
    }

    fn create_external_stream(&mut self, input: CreateExternalStreamInput) -> StreamResponse {
        if let Err(response) = validate_external_payload_ref(&input.initial_payload) {
            return response;
        }
        if let Err(response) = self.validate_stream_scope(&input.stream_id) {
            return response;
        }
        if let Err(response) =
            validate_retention(input.stream_ttl_seconds, input.stream_expires_at_ms)
        {
            return response;
        }
        if let Err(response) = validate_producer_request(input.producer.as_ref()) {
            return response;
        }
        if let Some(producer) = input.producer.as_ref()
            && producer.producer_seq != 0
        {
            return StreamResponse::error(
                StreamErrorCode::ProducerSeqConflict,
                format!(
                    "producer '{}' expected sequence 0, received {}",
                    producer.producer_id, producer.producer_seq
                ),
            );
        }
        if self
            .streams
            .get(&input.stream_id)
            .is_some_and(|existing| stream_is_expired(existing, input.now_ms))
        {
            self.remove_stream_state(&input.stream_id);
        }

        if let Some(existing) = self.streams.get(&input.stream_id) {
            if is_soft_deleted(existing) {
                return StreamResponse::error(
                    StreamErrorCode::StreamAlreadyExistsConflict,
                    format!(
                        "stream '{}' is gone and cannot be recreated yet",
                        input.stream_id
                    ),
                );
            }
            if existing.content_type == input.content_type
                && existing.status == status_from_closed(input.close_after)
                && existing.stream_ttl_seconds == input.stream_ttl_seconds
                && existing.stream_expires_at_ms == input.stream_expires_at_ms
                && existing.forked_from == input.forked_from
                && existing.fork_offset == input.fork_offset
            {
                return StreamResponse::AlreadyExists {
                    next_offset: existing.tail_offset,
                    closed: existing.status == StreamStatus::Closed,
                    content_type: existing.content_type.clone(),
                    stream_ttl_seconds: existing.stream_ttl_seconds,
                    stream_expires_at_ms: existing.stream_expires_at_ms,
                };
            }
            return StreamResponse::error(
                StreamErrorCode::StreamAlreadyExistsConflict,
                format!(
                    "stream '{}' already exists with different metadata",
                    input.stream_id
                ),
            );
        }

        let initial_len = input.initial_payload.payload_len;
        let metadata = StreamMetadata {
            stream_id: input.stream_id.clone(),
            content_type: input.content_type,
            status: status_from_closed(input.close_after),
            tail_offset: initial_len,
            last_stream_seq: input.stream_seq,
            stream_ttl_seconds: input.stream_ttl_seconds,
            stream_expires_at_ms: input.stream_expires_at_ms,
            created_at_ms: input.now_ms,
            last_ttl_touch_at_ms: input.now_ms,
            forked_from: input.forked_from,
            fork_offset: input.fork_offset,
            fork_ref_count: 0,
        };
        self.streams.insert(input.stream_id.clone(), metadata);
        self.payloads.insert(input.stream_id.clone(), Vec::new());
        self.external_segments.insert(
            input.stream_id.clone(),
            vec![ObjectPayloadRef {
                start_offset: 0,
                end_offset: initial_len,
                s3_path: input.initial_payload.s3_path,
                object_size: input.initial_payload.object_size,
            }],
        );
        self.message_records.insert(
            input.stream_id.clone(),
            vec![StreamMessageRecord {
                start_offset: 0,
                end_offset: initial_len,
            }],
        );
        let mut producer_states = HashMap::new();
        if let Some(producer) = input.producer {
            let last_item = ProducerAppendRecord {
                start_offset: 0,
                next_offset: initial_len,
                closed: input.close_after,
            };
            producer_states.insert(
                producer.producer_id,
                ProducerState {
                    producer_epoch: producer.producer_epoch,
                    producer_seq: producer.producer_seq,
                    last_start_offset: last_item.start_offset,
                    last_next_offset: last_item.next_offset,
                    last_closed: last_item.closed,
                    last_items: vec![last_item],
                },
            );
        }
        self.producers
            .insert(input.stream_id.clone(), producer_states);
        StreamResponse::Created {
            stream_id: input.stream_id,
            next_offset: initial_len,
            closed: input.close_after,
        }
    }

    pub fn append_borrowed(&mut self, input: AppendStreamInput<'_>) -> StreamResponse {
        let AppendStreamInput {
            stream_id,
            content_type,
            payload,
            close_after,
            stream_seq,
            producer,
            now_ms,
        } = input;
        if let Err(response) = self.validate_stream_scope(&stream_id) {
            return response;
        }
        if let Err(response) = validate_producer_request(producer.as_ref()) {
            return response;
        }

        let Some(_) = self.streams.get(&stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        if self.expire_stream_if_due(&stream_id, now_ms) {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        }
        if self.streams.get(&stream_id).is_some_and(is_soft_deleted) {
            return StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            );
        }
        let producer_decision = match self.evaluate_producer(&stream_id, producer.as_ref()) {
            Ok(decision) => decision,
            Err(response) => return response,
        };
        if let ProducerDecision::Duplicate {
            offset,
            next_offset,
            closed,
            producer,
            ..
        } = producer_decision
        {
            if payload.is_empty() {
                return StreamResponse::Closed {
                    next_offset,
                    deduplicated: true,
                    producer: Some(producer),
                };
            }
            return StreamResponse::Appended {
                offset,
                next_offset,
                closed,
                deduplicated: true,
                producer: Some(producer),
            };
        }

        let Some(stream) = self.streams.get_mut(&stream_id) else {
            unreachable!("stream existence checked before producer evaluation");
        };

        if stream.status == StreamStatus::Closed {
            if close_after && payload.is_empty() {
                return StreamResponse::Closed {
                    next_offset: stream.tail_offset,
                    deduplicated: false,
                    producer: None,
                };
            }
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::StreamClosed,
                format!("stream '{stream_id}' is closed"),
                stream.tail_offset,
            );
        }

        if payload.is_empty() && !close_after {
            return StreamResponse::error(
                StreamErrorCode::EmptyAppend,
                "append payload must be non-empty unless closing the stream",
            );
        }

        if !payload.is_empty() {
            let Some(content_type) = content_type else {
                return StreamResponse::error(
                    StreamErrorCode::MissingContentType,
                    "append with a body must include content type",
                );
            };
            if content_type != stream.content_type {
                return StreamResponse::error_with_next_offset(
                    StreamErrorCode::ContentTypeMismatch,
                    format!(
                        "append content type '{content_type}' does not match stream content type '{}'",
                        stream.content_type
                    ),
                    stream.tail_offset,
                );
            }
        }

        if let Err(response) = check_stream_seq(stream, stream_seq.as_deref()) {
            return response;
        }

        let offset = stream.tail_offset;
        let payload_len = u64::try_from(payload.len()).expect("payload len fits u64");
        stream.tail_offset = stream.tail_offset.saturating_add(payload_len);
        if let Some(seq) = stream_seq {
            stream.last_stream_seq = Some(seq);
        }
        renew_stream_ttl(stream, now_ms);
        if close_after {
            stream.status = StreamStatus::Closed;
        }
        let closed = stream.status == StreamStatus::Closed;
        let next_offset = stream.tail_offset;
        let producer_ack = producer.clone();
        if let Some(producer) = producer {
            self.record_producer_success(
                stream_id.clone(),
                producer,
                ProducerAppendRecord {
                    start_offset: offset,
                    next_offset,
                    closed,
                },
                vec![ProducerAppendRecord {
                    start_offset: offset,
                    next_offset,
                    closed,
                }],
            );
        }

        if payload.is_empty() {
            StreamResponse::Closed {
                next_offset,
                deduplicated: false,
                producer: producer_ack,
            }
        } else {
            let payload_store = self
                .payloads
                .get_mut(&stream_id)
                .expect("payload vector exists for stream metadata");
            let payload_start = payload_store.len();
            payload_store.extend_from_slice(payload);
            let payload_end = payload_store.len();
            self.hot_segments
                .get_mut(&stream_id)
                .map(|segments| {
                    segments.push(HotPayloadSegment {
                        start_offset: offset,
                        end_offset: next_offset,
                        payload_start,
                        payload_end,
                    })
                })
                .unwrap_or_else(|| {
                    self.hot_segments.insert(
                        stream_id.clone(),
                        vec![HotPayloadSegment {
                            start_offset: offset,
                            end_offset: next_offset,
                            payload_start,
                            payload_end,
                        }],
                    );
                });
            self.refresh_hot_start_offset(&stream_id);
            self.message_records
                .entry(stream_id.clone())
                .or_default()
                .push(StreamMessageRecord {
                    start_offset: offset,
                    end_offset: next_offset,
                });
            StreamResponse::Appended {
                offset,
                next_offset,
                closed: close_after,
                deduplicated: false,
                producer: producer_ack,
            }
        }
    }

    fn append_external(&mut self, input: AppendExternalInput<'_>) -> StreamResponse {
        let AppendExternalInput {
            stream_id,
            content_type,
            payload,
            close_after,
            stream_seq,
            producer,
            now_ms,
        } = input;
        if let Err(response) = validate_external_payload_ref(&payload) {
            return response;
        }
        if let Err(response) = self.validate_stream_scope(&stream_id) {
            return response;
        }
        if let Err(response) = validate_producer_request(producer.as_ref()) {
            return response;
        }
        let Some(_) = self.streams.get(&stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        if self.expire_stream_if_due(&stream_id, now_ms) {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        }
        if self.streams.get(&stream_id).is_some_and(is_soft_deleted) {
            return StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            );
        }
        let producer_decision = match self.evaluate_producer(&stream_id, producer.as_ref()) {
            Ok(decision) => decision,
            Err(response) => return response,
        };
        if let ProducerDecision::Duplicate {
            offset,
            next_offset,
            closed,
            producer,
            ..
        } = producer_decision
        {
            return StreamResponse::Appended {
                offset,
                next_offset,
                closed,
                deduplicated: true,
                producer: Some(producer),
            };
        }

        let Some(stream) = self.streams.get(&stream_id) else {
            unreachable!("stream existence checked before producer evaluation");
        };
        if stream.status == StreamStatus::Closed {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::StreamClosed,
                format!("stream '{stream_id}' is closed"),
                stream.tail_offset,
            );
        }
        let Some(content_type) = content_type else {
            return StreamResponse::error(
                StreamErrorCode::MissingContentType,
                "append with a body must include content type",
            );
        };
        if content_type != stream.content_type {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::ContentTypeMismatch,
                format!(
                    "append content type '{content_type}' does not match stream content type '{}'",
                    stream.content_type
                ),
                stream.tail_offset,
            );
        }
        if let Err(response) = check_stream_seq(stream, stream_seq.as_deref()) {
            return response;
        }
        let offset = stream.tail_offset;
        let next_offset = offset.saturating_add(payload.payload_len);
        let stream = self
            .streams
            .get_mut(&stream_id)
            .expect("stream existence checked before external append mutation");
        stream.tail_offset = next_offset;
        if let Some(seq) = stream_seq {
            stream.last_stream_seq = Some(seq);
        }
        renew_stream_ttl(stream, now_ms);
        if close_after {
            stream.status = StreamStatus::Closed;
        }
        let closed = stream.status == StreamStatus::Closed;
        let producer_ack = producer.clone();
        if let Some(producer) = producer {
            self.record_producer_success(
                stream_id.clone(),
                producer,
                ProducerAppendRecord {
                    start_offset: offset,
                    next_offset,
                    closed,
                },
                vec![ProducerAppendRecord {
                    start_offset: offset,
                    next_offset,
                    closed,
                }],
            );
        }
        self.external_segments
            .entry(stream_id.clone())
            .or_default()
            .push(ObjectPayloadRef {
                start_offset: offset,
                end_offset: next_offset,
                s3_path: payload.s3_path,
                object_size: payload.object_size,
            });
        self.message_records
            .entry(stream_id.clone())
            .or_default()
            .push(StreamMessageRecord {
                start_offset: offset,
                end_offset: next_offset,
            });
        StreamResponse::Appended {
            offset,
            next_offset,
            closed: close_after,
            deduplicated: false,
            producer: producer_ack,
        }
    }

    pub fn append_batch_borrowed(
        &mut self,
        stream_id: BucketStreamId,
        content_type: Option<&str>,
        payloads: &[&[u8]],
        producer: Option<ProducerRequest>,
        now_ms: u64,
    ) -> Result<StreamBatchAppend, StreamResponse> {
        if payloads.is_empty() {
            return Err(StreamResponse::error(
                StreamErrorCode::EmptyAppend,
                "append batch must contain at least one payload",
            ));
        }
        self.validate_stream_scope(&stream_id)?;
        validate_producer_request(producer.as_ref())?;
        if self.expire_stream_if_due(&stream_id, now_ms) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        }
        if self.streams.get(&stream_id).is_some_and(is_soft_deleted) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
        let producer_decision = self.evaluate_producer(&stream_id, producer.as_ref())?;
        if let ProducerDecision::Duplicate { items, .. } = producer_decision {
            return Ok(StreamBatchAppend {
                items: items
                    .into_iter()
                    .map(|item| StreamBatchAppendItem {
                        offset: item.start_offset,
                        next_offset: item.next_offset,
                        closed: item.closed,
                        deduplicated: true,
                    })
                    .collect(),
                deduplicated: true,
            });
        }

        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        if stream.status == StreamStatus::Closed {
            return Err(StreamResponse::error_with_next_offset(
                StreamErrorCode::StreamClosed,
                format!("stream '{stream_id}' is closed"),
                stream.tail_offset,
            ));
        }
        let Some(content_type) = content_type else {
            return Err(StreamResponse::error(
                StreamErrorCode::MissingContentType,
                "append batch must include content type",
            ));
        };
        if content_type != stream.content_type {
            return Err(StreamResponse::error_with_next_offset(
                StreamErrorCode::ContentTypeMismatch,
                format!(
                    "append content type '{content_type}' does not match stream content type '{}'",
                    stream.content_type
                ),
                stream.tail_offset,
            ));
        }
        if payloads.iter().any(|payload| payload.is_empty()) {
            return Err(StreamResponse::error(
                StreamErrorCode::EmptyAppend,
                "append batch payloads must be non-empty",
            ));
        }

        let mut items = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let offset = stream.tail_offset;
            let payload_len = u64::try_from(payload.len()).expect("payload len fits u64");
            stream.tail_offset = stream.tail_offset.saturating_add(payload_len);
            items.push(ProducerAppendRecord {
                start_offset: offset,
                next_offset: stream.tail_offset,
                closed: false,
            });
        }
        let last = items
            .last()
            .expect("payloads checked non-empty before append")
            .clone();
        renew_stream_ttl(stream, now_ms);
        if let Some(producer) = producer {
            self.record_producer_success(stream_id.clone(), producer, last.clone(), items.clone());
        }
        let payload_store = self
            .payloads
            .get_mut(&stream_id)
            .expect("payload vector exists for stream metadata");
        let hot_segments = self.hot_segments.entry(stream_id.clone()).or_default();
        for (item, payload) in items.iter().zip(payloads.iter()) {
            let payload_start = payload_store.len();
            payload_store.extend_from_slice(payload);
            let payload_end = payload_store.len();
            hot_segments.push(HotPayloadSegment {
                start_offset: item.start_offset,
                end_offset: item.next_offset,
                payload_start,
                payload_end,
            });
        }
        self.refresh_hot_start_offset(&stream_id);
        self.message_records
            .entry(stream_id.clone())
            .or_default()
            .extend(items.iter().map(|item| StreamMessageRecord {
                start_offset: item.start_offset,
                end_offset: item.next_offset,
            }));
        Ok(StreamBatchAppend {
            items: items
                .into_iter()
                .map(|item| StreamBatchAppendItem {
                    offset: item.start_offset,
                    next_offset: item.next_offset,
                    closed: item.closed,
                    deduplicated: false,
                })
                .collect(),
            deduplicated: false,
        })
    }

    fn close(
        &mut self,
        stream_id: BucketStreamId,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    ) -> StreamResponse {
        self.append_borrowed(AppendStreamInput {
            stream_id,
            content_type: None,
            payload: &[],
            close_after: true,
            stream_seq,
            producer,
            now_ms,
        })
    }

    fn delete_stream(&mut self, stream_id: &BucketStreamId) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(stream_id) {
            return response;
        }
        let Some(stream) = self.streams.get_mut(stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        if is_soft_deleted(stream) {
            return StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            );
        }
        if stream.fork_ref_count > 0 {
            stream.status = StreamStatus::SoftDeleted;
            return StreamResponse::Deleted {
                hard_deleted: false,
                parent_to_release: None,
            };
        }
        let parent_to_release = stream.forked_from.clone();
        self.remove_stream_state(stream_id);
        StreamResponse::Deleted {
            hard_deleted: true,
            parent_to_release,
        }
    }

    fn add_fork_ref(&mut self, stream_id: &BucketStreamId, now_ms: u64) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(stream_id) {
            return response;
        }
        if self.expire_stream_if_due(stream_id, now_ms) {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        }
        let Some(stream) = self.streams.get_mut(stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        if is_soft_deleted(stream) {
            return StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            );
        }
        stream.fork_ref_count = stream.fork_ref_count.saturating_add(1);
        StreamResponse::ForkRefAdded {
            fork_ref_count: stream.fork_ref_count,
        }
    }

    fn release_fork_ref(&mut self, stream_id: &BucketStreamId) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(stream_id) {
            return response;
        }
        let Some(stream) = self.streams.get_mut(stream_id) else {
            return StreamResponse::ForkRefReleased {
                hard_deleted: false,
                fork_ref_count: 0,
                parent_to_release: None,
            };
        };
        if stream.fork_ref_count == 0 {
            return StreamResponse::error(
                StreamErrorCode::InvalidFork,
                format!("stream '{stream_id}' has no fork reference to release"),
            );
        }
        stream.fork_ref_count -= 1;
        if stream.fork_ref_count == 0 && is_soft_deleted(stream) {
            let parent_to_release = stream.forked_from.clone();
            self.remove_stream_state(stream_id);
            return StreamResponse::ForkRefReleased {
                hard_deleted: true,
                fork_ref_count: 0,
                parent_to_release,
            };
        }
        StreamResponse::ForkRefReleased {
            hard_deleted: false,
            fork_ref_count: stream.fork_ref_count,
            parent_to_release: None,
        }
    }

    fn touch_stream_access(
        &mut self,
        stream_id: &BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
    ) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(stream_id) {
            return response;
        }
        let Some(stream) = self.streams.get(stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        if is_soft_deleted(stream) {
            return StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            );
        }
        if stream_is_expired(stream, now_ms) {
            self.remove_stream_state(stream_id);
            return StreamResponse::Accessed {
                changed: true,
                expired: true,
            };
        }
        let changed = if renew_ttl && stream.stream_ttl_seconds.is_some() {
            let stream = self
                .streams
                .get_mut(stream_id)
                .expect("stream existence checked before TTL renewal");
            let previous = stream.last_ttl_touch_at_ms;
            renew_stream_ttl(stream, now_ms);
            stream.last_ttl_touch_at_ms != previous
        } else {
            false
        };
        StreamResponse::Accessed {
            changed,
            expired: false,
        }
    }

    fn expire_stream_if_due(&mut self, stream_id: &BucketStreamId, now_ms: u64) -> bool {
        if self
            .streams
            .get(stream_id)
            .is_some_and(|stream| stream_is_expired(stream, now_ms))
        {
            self.remove_stream_state(stream_id);
            return true;
        }
        false
    }

    fn remove_stream_state(&mut self, stream_id: &BucketStreamId) -> bool {
        if self.streams.remove(stream_id).is_some() {
            self.payloads.remove(stream_id);
            self.hot_segments.remove(stream_id);
            self.hot_start_offsets.remove(stream_id);
            self.cold_chunks.remove(stream_id);
            self.external_segments.remove(stream_id);
            self.message_records.remove(stream_id);
            self.visible_snapshots.remove(stream_id);
            self.producers.remove(stream_id);
            true
        } else {
            false
        }
    }

    fn validate_stream_scope(&self, stream_id: &BucketStreamId) -> Result<(), StreamResponse> {
        if let Err(message) = validate_bucket_id(&stream_id.bucket_id) {
            return Err(StreamResponse::error(
                StreamErrorCode::InvalidBucketId,
                message,
            ));
        }
        if let Err(message) = validate_stream_id(stream_id) {
            return Err(StreamResponse::error(
                StreamErrorCode::InvalidStreamId,
                message,
            ));
        }
        if !self.buckets.contains(&stream_id.bucket_id) {
            return Err(StreamResponse::error(
                StreamErrorCode::BucketNotFound,
                format!("bucket '{}' does not exist", stream_id.bucket_id),
            ));
        }
        Ok(())
    }

    fn earliest_retained_offset(&self, stream_id: &BucketStreamId) -> u64 {
        self.visible_snapshots
            .get(stream_id)
            .map(|snapshot| snapshot.offset)
            .unwrap_or(0)
    }

    fn snapshot_offset_aligned(
        &self,
        stream_id: &BucketStreamId,
        snapshot_offset: u64,
        retained_offset: u64,
    ) -> bool {
        snapshot_offset == retained_offset
            || self.message_records.get(stream_id).is_some_and(|records| {
                records
                    .iter()
                    .any(|record| record.end_offset == snapshot_offset)
            })
    }

    fn compact_retained_prefix(&mut self, stream_id: &BucketStreamId, retained_offset: u64) {
        if let Some(records) = self.message_records.get_mut(stream_id) {
            records.retain(|record| record.end_offset > retained_offset);
            if records.is_empty() {
                self.message_records.remove(stream_id);
            }
        }
        if let Some(chunks) = self.cold_chunks.get_mut(stream_id) {
            chunks.retain(|chunk| chunk.end_offset > retained_offset);
            if chunks.is_empty() {
                self.cold_chunks.remove(stream_id);
            }
        }
        if let Some(objects) = self.external_segments.get_mut(stream_id) {
            objects.retain(|object| object.end_offset > retained_offset);
            if objects.is_empty() {
                self.external_segments.remove(stream_id);
            }
        }

        self.discard_hot_prefix_before(stream_id, retained_offset);
    }

    fn refresh_hot_start_offset(&mut self, stream_id: &BucketStreamId) {
        let Some(hot_start_offset) = self
            .hot_segments
            .get(stream_id)
            .and_then(|segments| segments.iter().map(|segment| segment.start_offset).min())
        else {
            self.hot_start_offsets.remove(stream_id);
            return;
        };
        if hot_start_offset == 0 {
            self.hot_start_offsets.remove(stream_id);
        } else {
            self.hot_start_offsets
                .insert(stream_id.clone(), hot_start_offset);
        }
    }

    fn remove_drained_hot_range(
        &mut self,
        stream_id: &BucketStreamId,
        segment_index: usize,
        new_start_offset: u64,
        drain_start: usize,
        drained_len: usize,
    ) {
        let Some(segments) = self.hot_segments.get_mut(stream_id) else {
            self.hot_start_offsets.remove(stream_id);
            return;
        };
        if segment_index >= segments.len() {
            self.refresh_hot_start_offset(stream_id);
            return;
        }
        let drain_end = drain_start + drained_len;
        let mut updated_segments = Vec::with_capacity(segments.len());
        for (index, mut segment) in segments.drain(..).enumerate() {
            if index < segment_index || segment.payload_end <= drain_start {
                updated_segments.push(segment);
                continue;
            }
            if segment.payload_start >= drain_end {
                segment.payload_start -= drained_len;
                segment.payload_end -= drained_len;
                updated_segments.push(segment);
                continue;
            }
            if segment.payload_end <= drain_end {
                continue;
            }
            segment.start_offset = new_start_offset;
            segment.payload_start = drain_start;
            segment.payload_end -= drained_len;
            updated_segments.push(segment);
        }
        *segments = updated_segments;
        if segments.is_empty() {
            self.hot_segments.remove(stream_id);
        }
        self.refresh_hot_start_offset(stream_id);
    }

    fn discard_hot_prefix_before(&mut self, stream_id: &BucketStreamId, retained_offset: u64) {
        while let Some(segment_index) = self
            .hot_segments(stream_id)
            .iter()
            .position(|segment| segment.start_offset < retained_offset)
        {
            let segment = self.hot_segments(stream_id)[segment_index].clone();
            let new_start_offset = retained_offset.min(segment.end_offset);
            let drained_len = usize::try_from(new_start_offset - segment.start_offset)
                .expect("drain len fits usize");
            if drained_len == 0 {
                break;
            }
            let drain_start = segment.payload_start;
            let drain_end = drain_start + drained_len;
            self.payloads
                .get_mut(stream_id)
                .expect("payload vector exists for stream metadata")
                .drain(drain_start..drain_end);
            self.remove_drained_hot_range(
                stream_id,
                segment_index,
                new_start_offset,
                drain_start,
                drained_len,
            );
        }
        self.refresh_hot_start_offset(stream_id);
    }

    fn producer_snapshot(&self, stream_id: &BucketStreamId) -> Vec<ProducerSnapshot> {
        let mut producer_states = self
            .producers
            .get(stream_id)
            .into_iter()
            .flat_map(|states| states.iter())
            .map(|(producer_id, state)| ProducerSnapshot {
                producer_id: producer_id.clone(),
                producer_epoch: state.producer_epoch,
                producer_seq: state.producer_seq,
                last_start_offset: state.last_start_offset,
                last_next_offset: state.last_next_offset,
                last_closed: state.last_closed,
                last_items: state.last_items.clone(),
            })
            .collect::<Vec<_>>();
        producer_states.sort_by(|left, right| left.producer_id.cmp(&right.producer_id));
        producer_states
    }

    fn evaluate_producer(
        &self,
        stream_id: &BucketStreamId,
        producer: Option<&ProducerRequest>,
    ) -> Result<ProducerDecision, StreamResponse> {
        let Some(producer) = producer else {
            return Ok(ProducerDecision::Accept);
        };
        let Some(states) = self.producers.get(stream_id) else {
            return Ok(ProducerDecision::Accept);
        };
        let Some(state) = states.get(&producer.producer_id) else {
            if producer.producer_seq == 0 {
                return Ok(ProducerDecision::Accept);
            }
            return Err(StreamResponse::error(
                StreamErrorCode::ProducerSeqConflict,
                format!(
                    "producer '{}' expected sequence 0, received {}",
                    producer.producer_id, producer.producer_seq
                ),
            ));
        };

        if producer.producer_epoch < state.producer_epoch {
            return Err(StreamResponse::error(
                StreamErrorCode::ProducerEpochStale,
                format!(
                    "producer '{}' epoch {} is stale; current epoch is {}",
                    producer.producer_id, producer.producer_epoch, state.producer_epoch
                ),
            ));
        }
        if producer.producer_epoch > state.producer_epoch {
            if producer.producer_seq == 0 {
                return Ok(ProducerDecision::Accept);
            }
            return Err(StreamResponse::error(
                StreamErrorCode::InvalidProducer,
                format!(
                    "producer '{}' new epoch {} must start at sequence 0",
                    producer.producer_id, producer.producer_epoch
                ),
            ));
        }

        if producer.producer_seq <= state.producer_seq {
            return Ok(ProducerDecision::Duplicate {
                offset: state.last_start_offset,
                next_offset: state.last_next_offset,
                closed: state.last_closed,
                producer: ProducerRequest {
                    producer_id: producer.producer_id.clone(),
                    producer_epoch: state.producer_epoch,
                    producer_seq: state.producer_seq,
                },
                items: state.last_items.clone(),
            });
        }
        if producer.producer_seq == state.producer_seq + 1 {
            return Ok(ProducerDecision::Accept);
        }
        Err(StreamResponse::error(
            StreamErrorCode::ProducerSeqConflict,
            format!(
                "producer '{}' expected sequence {}, received {}",
                producer.producer_id,
                state.producer_seq + 1,
                producer.producer_seq
            ),
        ))
    }

    fn record_producer_success(
        &mut self,
        stream_id: BucketStreamId,
        producer: ProducerRequest,
        last: ProducerAppendRecord,
        last_items: Vec<ProducerAppendRecord>,
    ) {
        self.producers.entry(stream_id).or_default().insert(
            producer.producer_id,
            ProducerState {
                producer_epoch: producer.producer_epoch,
                producer_seq: producer.producer_seq,
                last_start_offset: last.start_offset,
                last_next_offset: last.next_offset,
                last_closed: last.closed,
                last_items,
            },
        );
    }
}

#[derive(Debug)]
struct CreateStreamInput {
    stream_id: BucketStreamId,
    content_type: String,
    initial_payload: Vec<u8>,
    close_after: bool,
    stream_seq: Option<String>,
    producer: Option<ProducerRequest>,
    stream_ttl_seconds: Option<u64>,
    stream_expires_at_ms: Option<u64>,
    forked_from: Option<BucketStreamId>,
    fork_offset: Option<u64>,
    now_ms: u64,
}

#[derive(Debug)]
struct CreateExternalStreamInput {
    stream_id: BucketStreamId,
    content_type: String,
    initial_payload: ExternalPayloadRef,
    close_after: bool,
    stream_seq: Option<String>,
    producer: Option<ProducerRequest>,
    stream_ttl_seconds: Option<u64>,
    stream_expires_at_ms: Option<u64>,
    forked_from: Option<BucketStreamId>,
    fork_offset: Option<u64>,
    now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProducerDecision {
    Accept,
    Duplicate {
        offset: u64,
        next_offset: u64,
        closed: bool,
        producer: ProducerRequest,
        items: Vec<ProducerAppendRecord>,
    },
}

impl CreateStreamInput {
    fn initial_len(&self) -> u64 {
        u64::try_from(self.initial_payload.len()).expect("payload len fits u64")
    }
}

fn status_from_closed(closed: bool) -> StreamStatus {
    if closed {
        StreamStatus::Closed
    } else {
        StreamStatus::Open
    }
}

fn is_soft_deleted(stream: &StreamMetadata) -> bool {
    stream.status == StreamStatus::SoftDeleted
}

fn validate_retention(
    stream_ttl_seconds: Option<u64>,
    stream_expires_at_ms: Option<u64>,
) -> Result<(), StreamResponse> {
    if stream_ttl_seconds.is_some() && stream_expires_at_ms.is_some() {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidRetention,
            "stream ttl and expires-at cannot both be set",
        ));
    }
    if let Some(ttl_seconds) = stream_ttl_seconds
        && ttl_seconds.checked_mul(1000).is_none()
    {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidRetention,
            "stream ttl overflows millisecond range",
        ));
    }
    Ok(())
}

fn stream_expiry_at_ms(stream: &StreamMetadata) -> Option<u64> {
    if let Some(expires_at_ms) = stream.stream_expires_at_ms {
        return Some(expires_at_ms);
    }
    stream.stream_ttl_seconds.map(|ttl_seconds| {
        stream
            .last_ttl_touch_at_ms
            .saturating_add(ttl_seconds.saturating_mul(1000))
    })
}

fn stream_is_expired(stream: &StreamMetadata, now_ms: u64) -> bool {
    stream_expiry_at_ms(stream).is_some_and(|expires_at_ms| now_ms >= expires_at_ms)
}

fn renew_stream_ttl(stream: &mut StreamMetadata, now_ms: u64) {
    if stream.stream_ttl_seconds.is_some() && stream.stream_expires_at_ms.is_none() {
        stream.last_ttl_touch_at_ms = now_ms;
    }
}

fn check_stream_seq(stream: &StreamMetadata, incoming: Option<&str>) -> Result<(), StreamResponse> {
    let Some(incoming) = incoming else {
        return Ok(());
    };
    if let Some(last) = stream.last_stream_seq.as_deref()
        && incoming <= last
    {
        return Err(StreamResponse::error_with_next_offset(
            StreamErrorCode::StreamSeqConflict,
            format!("stream sequence '{incoming}' is not greater than last sequence '{last}'"),
            stream.tail_offset,
        ));
    }
    Ok(())
}

fn validate_producer_request(producer: Option<&ProducerRequest>) -> Result<(), StreamResponse> {
    let Some(producer) = producer else {
        return Ok(());
    };
    if producer.producer_id.trim().is_empty() {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidProducer,
            "producer id must not be empty",
        ));
    }
    const MAX_JS_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
    if producer.producer_epoch > MAX_JS_SAFE_INTEGER {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidProducer,
            format!(
                "producer epoch {} exceeds maximum {}",
                producer.producer_epoch, MAX_JS_SAFE_INTEGER
            ),
        ));
    }
    if producer.producer_seq > MAX_JS_SAFE_INTEGER {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidProducer,
            format!(
                "producer sequence {} exceeds maximum {}",
                producer.producer_seq, MAX_JS_SAFE_INTEGER
            ),
        ));
    }
    Ok(())
}

fn validate_external_payload_ref(payload: &ExternalPayloadRef) -> Result<(), StreamResponse> {
    if payload.s3_path.trim().is_empty() {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidColdFlush,
            "external payload S3 path must not be empty",
        ));
    }
    if payload.payload_len == 0 {
        return Err(StreamResponse::error(
            StreamErrorCode::EmptyAppend,
            "external payload length must be greater than zero",
        ));
    }
    if payload.object_size < payload.payload_len {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidColdFlush,
            "external payload object size must cover payload length",
        ));
    }
    Ok(())
}

fn restore_producer_states(
    stream_id: &BucketStreamId,
    snapshots: Vec<ProducerSnapshot>,
) -> Result<HashMap<String, ProducerState>, StreamSnapshotError> {
    let mut states = HashMap::with_capacity(snapshots.len());
    for snapshot in snapshots {
        if states
            .insert(
                snapshot.producer_id.clone(),
                ProducerState {
                    producer_epoch: snapshot.producer_epoch,
                    producer_seq: snapshot.producer_seq,
                    last_start_offset: snapshot.last_start_offset,
                    last_next_offset: snapshot.last_next_offset,
                    last_closed: snapshot.last_closed,
                    last_items: snapshot.last_items,
                },
            )
            .is_some()
        {
            return Err(StreamSnapshotError::DuplicateProducer {
                stream_id: stream_id.clone(),
                producer_id: snapshot.producer_id,
            });
        }
    }
    Ok(states)
}

fn valid_cold_chunk_ref(chunk: &ColdChunkRef) -> bool {
    chunk.end_offset > chunk.start_offset
        && !chunk.s3_path.trim().is_empty()
        && chunk.object_size >= chunk.end_offset - chunk.start_offset
}

fn valid_object_payload_ref(object: &ObjectPayloadRef) -> bool {
    object.end_offset > object.start_offset
        && !object.s3_path.trim().is_empty()
        && object.object_size >= object.end_offset - object.start_offset
}

fn hot_segments_match_payload(segments: &[HotPayloadSegment], payload_len: usize) -> bool {
    let mut expected_payload_start = 0;
    for segment in segments {
        if segment.end_offset <= segment.start_offset
            || segment.payload_start != expected_payload_start
            || segment.payload_end <= segment.payload_start
            || segment.payload_end > payload_len
        {
            return false;
        }
        let Ok(logical_len) = usize::try_from(segment.end_offset - segment.start_offset) else {
            return false;
        };
        if logical_len != segment.payload_end - segment.payload_start {
            return false;
        }
        expected_payload_start = segment.payload_end;
    }
    expected_payload_start == payload_len
}

fn payload_sources_cover_retained_suffix(
    cold_chunks: &[ColdChunkRef],
    external_segments: &[ObjectPayloadRef],
    hot_segments: &[HotPayloadSegment],
    retained_offset: u64,
    tail_offset: u64,
) -> bool {
    if tail_offset < retained_offset {
        return false;
    }
    let mut ranges =
        Vec::with_capacity(cold_chunks.len() + external_segments.len() + hot_segments.len());
    for chunk in cold_chunks {
        if !valid_cold_chunk_ref(chunk) {
            return false;
        }
        ranges.push((chunk.start_offset, chunk.end_offset));
    }
    for object in external_segments {
        if !valid_object_payload_ref(object) {
            return false;
        }
        ranges.push((object.start_offset, object.end_offset));
    }
    for segment in hot_segments {
        if segment.end_offset <= segment.start_offset {
            return false;
        }
        ranges.push((segment.start_offset, segment.end_offset));
    }
    ranges.sort_unstable();

    let mut expected_start = retained_offset;
    for (start_offset, end_offset) in ranges {
        if end_offset <= expected_start {
            continue;
        }
        if start_offset > expected_start {
            return false;
        }
        expected_start = end_offset;
        if expected_start >= tail_offset {
            return true;
        }
    }
    expected_start == tail_offset
}

fn segments_cover_range(
    segments: &[(u64, StreamReadSegment)],
    offset: u64,
    next_offset: u64,
) -> bool {
    if next_offset < offset {
        return false;
    }
    let mut expected_start = offset;
    for (segment_start, segment) in segments {
        let Some(segment_end) = read_segment_end(*segment_start, segment) else {
            return false;
        };
        if segment_end <= expected_start {
            continue;
        }
        if *segment_start > expected_start {
            return false;
        }
        expected_start = segment_end;
        if expected_start >= next_offset {
            return true;
        }
    }
    expected_start == next_offset
}

fn read_segment_end(segment_start: u64, segment: &StreamReadSegment) -> Option<u64> {
    match segment {
        StreamReadSegment::Object(object) => {
            if object.len == 0
                || object.read_start_offset != segment_start
                || object.read_start_offset < object.object.start_offset
            {
                return None;
            }
            let len = u64::try_from(object.len).ok()?;
            let segment_end = object.read_start_offset.checked_add(len)?;
            if segment_end > object.object.end_offset {
                return None;
            }
            Some(segment_end)
        }
        StreamReadSegment::Hot(payload) => {
            if payload.is_empty() {
                return None;
            }
            let len = u64::try_from(payload.len()).ok()?;
            segment_start.checked_add(len)
        }
    }
}

fn message_records_cover_retained_suffix(
    records: &[StreamMessageRecord],
    retained_offset: u64,
    tail_offset: u64,
) -> bool {
    let mut expected_start = retained_offset;
    for record in records {
        if record.start_offset != expected_start || record.end_offset <= record.start_offset {
            return false;
        }
        expected_start = record.end_offset;
    }
    expected_start == tail_offset
}

fn compare_stream_ids(left: &BucketStreamId, right: &BucketStreamId) -> std::cmp::Ordering {
    left.bucket_id
        .cmp(&right.bucket_id)
        .then_with(|| left.stream_id.cmp(&right.stream_id))
}

pub fn validate_bucket_id(bucket_id: &str) -> Result<(), String> {
    if !(4..=64).contains(&bucket_id.len()) {
        return Err(format!(
            "bucket_id must be 4 to 64 bytes, got {} bytes",
            bucket_id.len()
        ));
    }
    if !bucket_id.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
    }) {
        return Err("bucket_id must match ^[a-z0-9_-]{4,64}$".to_owned());
    }
    Ok(())
}

pub fn validate_stream_id(stream_id: &BucketStreamId) -> Result<(), String> {
    let local = stream_id.stream_id.as_str();
    if local.is_empty() {
        return Err("stream_id must not be empty".to_owned());
    }
    if local.len() > 122 {
        return Err(format!(
            "stream_id must not exceed 122 bytes, got {} bytes",
            local.len()
        ));
    }
    if local == "streams" {
        return Err("stream_id 'streams' is reserved".to_owned());
    }
    if local.contains('/') || local.contains('\0') || local.contains("..") {
        return Err("stream_id must not contain '/', NUL, or '..'".to_owned());
    }
    let combined_len = stream_id.bucket_id.len() + 1 + local.len();
    if combined_len > 122 {
        return Err(format!(
            "bucket_id/stream_id must not exceed 122 bytes, got {combined_len} bytes"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream(id: &str) -> BucketStreamId {
        BucketStreamId::new("benchcmp", id)
    }

    fn create_bucket(machine: &mut StreamStateMachine) {
        assert_eq!(
            machine.apply(StreamCommand::CreateBucket {
                bucket_id: "benchcmp".to_owned(),
            }),
            StreamResponse::BucketCreated {
                bucket_id: "benchcmp".to_owned(),
            }
        );
    }

    fn create_stream(machine: &mut StreamStateMachine, id: &str) {
        assert_eq!(
            machine.apply(StreamCommand::CreateStream {
                stream_id: stream(id),
                content_type: "application/octet-stream".to_owned(),
                initial_payload: Vec::new(),
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
                forked_from: None,
                fork_offset: None,
                now_ms: 0,
            }),
            StreamResponse::Created {
                stream_id: stream(id),
                next_offset: 0,
                closed: false,
            }
        );
    }

    fn producer(id: &str, epoch: u64, seq: u64) -> ProducerRequest {
        ProducerRequest {
            producer_id: id.to_owned(),
            producer_epoch: epoch,
            producer_seq: seq,
        }
    }

    #[test]
    fn stream_create_requires_existing_bucket_and_valid_ids() {
        let mut machine = StreamStateMachine::new();

        assert!(matches!(
            machine.apply(StreamCommand::CreateBucket {
                bucket_id: "Bad".to_owned(),
            }),
            StreamResponse::Error {
                code: StreamErrorCode::InvalidBucketId,
                ..
            }
        ));
        assert!(matches!(
            machine.apply(StreamCommand::CreateStream {
                stream_id: stream("s-1"),
                content_type: "application/octet-stream".to_owned(),
                initial_payload: Vec::new(),
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
                forked_from: None,
                fork_offset: None,
                now_ms: 0,
            }),
            StreamResponse::Error {
                code: StreamErrorCode::BucketNotFound,
                ..
            }
        ));

        create_bucket(&mut machine);
        assert!(matches!(
            machine.apply(StreamCommand::CreateStream {
                stream_id: stream("streams"),
                content_type: "application/octet-stream".to_owned(),
                initial_payload: Vec::new(),
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
                forked_from: None,
                fork_offset: None,
                now_ms: 0,
            }),
            StreamResponse::Error {
                code: StreamErrorCode::InvalidStreamId,
                ..
            }
        ));
    }

    #[test]
    fn create_stream_is_idempotent_only_when_metadata_matches() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "s-1");

        assert_eq!(
            machine.apply(StreamCommand::CreateStream {
                stream_id: stream("s-1"),
                content_type: "application/octet-stream".to_owned(),
                initial_payload: vec![0; 99],
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
                forked_from: None,
                fork_offset: None,
                now_ms: 0,
            }),
            StreamResponse::AlreadyExists {
                next_offset: 0,
                closed: false,
                content_type: "application/octet-stream".to_owned(),
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
            }
        );

        assert!(matches!(
            machine.apply(StreamCommand::CreateStream {
                stream_id: stream("s-1"),
                content_type: "text/plain".to_owned(),
                initial_payload: Vec::new(),
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
                forked_from: None,
                fork_offset: None,
                now_ms: 0,
            }),
            StreamResponse::Error {
                code: StreamErrorCode::StreamAlreadyExistsConflict,
                ..
            }
        ));
    }

    #[test]
    fn append_advances_offsets_and_checks_content_type() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "s-1");

        assert_eq!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("s-1"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"abcdefg".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended {
                offset: 0,
                next_offset: 7,
                closed: false,
                deduplicated: false,
                producer: None,
            }
        );
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("s-1"),
                content_type: Some("text/plain".to_owned()),
                payload: b"x".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Error {
                code: StreamErrorCode::ContentTypeMismatch,
                next_offset: Some(7),
                ..
            }
        ));
        assert_eq!(machine.head(&stream("s-1")).expect("stream").tail_offset, 7);
    }

    #[test]
    fn catch_up_read_returns_payload_slice_and_bounds_errors() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "s-1");
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("s-1"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"abcdefg".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended { .. }
        ));

        assert_eq!(
            machine.read(&stream("s-1"), 2, 3).expect("read"),
            StreamRead {
                offset: 2,
                next_offset: 5,
                content_type: "application/octet-stream".to_owned(),
                payload: b"cde".to_vec(),
                up_to_date: false,
                closed: false,
            }
        );
        assert_eq!(
            machine.read(&stream("s-1"), 7, 16).expect("tail read"),
            StreamRead {
                offset: 7,
                next_offset: 7,
                content_type: "application/octet-stream".to_owned(),
                payload: Vec::new(),
                up_to_date: true,
                closed: false,
            }
        );
        assert!(matches!(
            machine.read(&stream("s-1"), 8, 1),
            Err(StreamResponse::Error {
                code: StreamErrorCode::OffsetOutOfRange,
                next_offset: Some(7),
                ..
            })
        ));
    }

    #[test]
    fn flush_cold_moves_hot_prefix_to_manifest_and_read_plan_splits() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "cold");
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("cold"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"abcdef".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended {
                offset: 0,
                next_offset: 6,
                ..
            }
        ));

        let candidate = machine
            .plan_cold_flush(&stream("cold"), 4, 4)
            .expect("plan cold flush")
            .expect("cold flush candidate");
        assert_eq!(candidate.start_offset, 0);
        assert_eq!(candidate.end_offset, 4);
        assert_eq!(candidate.payload, b"abcd");
        assert_eq!(
            machine.apply(StreamCommand::FlushCold {
                stream_id: stream("cold"),
                chunk: ColdChunkRef {
                    start_offset: candidate.start_offset,
                    end_offset: candidate.end_offset,
                    s3_path: "s3://bucket/cold/000000".to_owned(),
                    object_size: u64::try_from(candidate.payload.len()).unwrap(),
                },
            }),
            StreamResponse::ColdFlushed {
                hot_start_offset: 4,
            }
        );
        assert_eq!(machine.hot_start_offset(&stream("cold")), 4);
        assert_eq!(machine.cold_chunks(&stream("cold")).len(), 1);

        let plan = machine.read_plan(&stream("cold"), 2, 4).expect("read plan");
        assert_eq!(plan.next_offset, 6);
        assert_eq!(plan.segments.len(), 2);
        match &plan.segments[0] {
            StreamReadSegment::Object(segment) => {
                assert_eq!(segment.read_start_offset, 2);
                assert_eq!(segment.len, 2);
            }
            other => panic!("expected cold object segment, got {other:?}"),
        }
        match &plan.segments[1] {
            StreamReadSegment::Hot(payload) => assert_eq!(payload, b"ef"),
            other => panic!("expected hot segment, got {other:?}"),
        }
        assert_eq!(
            machine.read(&stream("cold"), 0, 6),
            Err(StreamResponse::Error {
                code: StreamErrorCode::InvalidColdFlush,
                message: "stream 'benchcmp/cold' read requires object payload store".to_owned(),
                next_offset: Some(6),
            })
        );
        assert_eq!(
            machine.read(&stream("cold"), 4, 8).expect("hot read"),
            StreamRead {
                offset: 4,
                next_offset: 6,
                content_type: "application/octet-stream".to_owned(),
                payload: b"ef".to_vec(),
                up_to_date: true,
                closed: false,
            }
        );

        let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore snapshot");
        assert_eq!(restored.hot_start_offset(&stream("cold")), 4);
        assert_eq!(restored.cold_chunks(&stream("cold")).len(), 1);
        assert_eq!(
            restored.read(&stream("cold"), 4, 8).expect("hot read"),
            StreamRead {
                offset: 4,
                next_offset: 6,
                content_type: "application/octet-stream".to_owned(),
                payload: b"ef".to_vec(),
                up_to_date: true,
                closed: false,
            }
        );
    }

    #[test]
    fn flush_cold_can_coalesce_contiguous_hot_segments() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "cold-coalesced");
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("cold-coalesced"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"abc".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended {
                offset: 0,
                next_offset: 3,
                ..
            }
        ));
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("cold-coalesced"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"de".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended {
                offset: 3,
                next_offset: 5,
                ..
            }
        ));

        assert_eq!(
            machine.apply(StreamCommand::FlushCold {
                stream_id: stream("cold-coalesced"),
                chunk: ColdChunkRef {
                    start_offset: 0,
                    end_offset: 5,
                    s3_path: "s3://bucket/cold-coalesced/000000".to_owned(),
                    object_size: 5,
                },
            }),
            StreamResponse::ColdFlushed {
                hot_start_offset: 0,
            }
        );
        assert!(machine.hot_segments(&stream("cold-coalesced")).is_empty());
        assert_eq!(machine.hot_payload_len(&stream("cold-coalesced")), Ok(0));
        assert_eq!(machine.cold_chunks(&stream("cold-coalesced")).len(), 1);

        let plan = machine
            .read_plan(&stream("cold-coalesced"), 0, 5)
            .expect("read plan");
        assert_eq!(plan.next_offset, 5);
        assert_eq!(plan.segments.len(), 1);
        match &plan.segments[0] {
            StreamReadSegment::Object(segment) => {
                assert_eq!(segment.read_start_offset, 0);
                assert_eq!(segment.len, 5);
            }
            other => panic!("expected cold object segment, got {other:?}"),
        }
    }

    #[test]
    fn plan_cold_flush_coalesces_contiguous_hot_segments() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "cold-planned-coalesced");
        for payload in [b"ab".as_slice(), b"cd".as_slice(), b"ef".as_slice()] {
            assert!(matches!(
                machine.apply(StreamCommand::Append {
                    stream_id: stream("cold-planned-coalesced"),
                    content_type: Some("application/octet-stream".to_owned()),
                    payload: payload.to_vec(),
                    close_after: false,
                    stream_seq: None,
                    producer: None,
                    now_ms: 0,
                }),
                StreamResponse::Appended { .. }
            ));
        }

        assert!(
            machine
                .plan_cold_flush(&stream("cold-planned-coalesced"), 4, 4)
                .expect("plan cold flush")
                .is_some(),
            "planner should consider contiguous small hot segments together"
        );
        let candidate = machine
            .plan_cold_flush(&stream("cold-planned-coalesced"), 5, 5)
            .expect("plan cold flush")
            .expect("candidate");
        assert_eq!(candidate.start_offset, 0);
        assert_eq!(candidate.end_offset, 5);
        assert_eq!(candidate.payload, b"abcde");
    }

    #[test]
    fn plan_next_cold_flush_selects_deterministic_eligible_stream() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "z-cold");
        create_stream(&mut machine, "a-cold");
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("z-cold"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"zzzz".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended { .. }
        ));
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("a-cold"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"aaaa".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended { .. }
        ));

        let candidate = machine
            .plan_next_cold_flush(4, 4)
            .expect("plan next cold flush")
            .expect("candidate");
        assert_eq!(candidate.stream_id, stream("a-cold"));
        assert_eq!(candidate.payload, b"aaaa");
    }

    #[test]
    fn plan_next_cold_flush_batch_advances_on_preview_state() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "batched-cold");
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("batched-cold"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"abcd".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended { .. }
        ));

        let candidates = machine
            .plan_next_cold_flush_batch(1, 1, 4)
            .expect("plan cold flush batch");
        assert_eq!(candidates.len(), 4);
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| (candidate.start_offset, candidate.end_offset))
                .collect::<Vec<_>>(),
            vec![(0, 1), (1, 2), (2, 3), (3, 4)]
        );
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.payload.as_slice())
                .collect::<Vec<_>>(),
            vec![
                b"a".as_slice(),
                b"b".as_slice(),
                b"c".as_slice(),
                b"d".as_slice()
            ]
        );
        assert_eq!(machine.hot_payload_len(&stream("batched-cold")), Ok(4));
        assert!(machine.cold_chunks(&stream("batched-cold")).is_empty());
    }

    #[test]
    fn stale_cold_flush_candidate_after_delete_recreate_is_invalid_without_mutation() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "stale-cold");
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("stale-cold"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"abcdefghijklmnopqr".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended {
                next_offset: 18,
                ..
            }
        ));
        let candidate = machine
            .plan_cold_flush(&stream("stale-cold"), 18, 18)
            .expect("plan cold flush")
            .expect("candidate");

        assert!(matches!(
            machine.apply(StreamCommand::DeleteStream {
                stream_id: stream("stale-cold")
            }),
            StreamResponse::Deleted {
                hard_deleted: true,
                ..
            }
        ));
        create_stream(&mut machine, "stale-cold");
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("stale-cold"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"abcdefghijklmnopq".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended {
                next_offset: 17,
                ..
            }
        ));

        match machine.apply(StreamCommand::FlushCold {
            stream_id: stream("stale-cold"),
            chunk: ColdChunkRef {
                start_offset: candidate.start_offset,
                end_offset: candidate.end_offset,
                s3_path: "s3://bucket/stale-cold/old-candidate".to_owned(),
                object_size: u64::try_from(candidate.payload.len()).unwrap(),
            },
        }) {
            StreamResponse::Error {
                code: StreamErrorCode::InvalidColdFlush,
                message,
                next_offset: Some(17),
            } => assert!(message.contains("beyond stream")),
            other => panic!("expected stale invalid cold flush, got {other:?}"),
        }

        assert_eq!(
            machine.read(&stream("stale-cold"), 0, 32).expect("read"),
            StreamRead {
                offset: 0,
                next_offset: 17,
                content_type: "application/octet-stream".to_owned(),
                payload: b"abcdefghijklmnopq".to_vec(),
                up_to_date: true,
                closed: false,
            }
        );
    }

    #[test]
    fn plan_next_cold_flush_skips_soft_deleted_streams() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "a-gone");
        create_stream(&mut machine, "b-live");
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("a-gone"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"gone".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended { .. }
        ));
        assert!(matches!(
            machine.apply(StreamCommand::AddForkRef {
                stream_id: stream("a-gone"),
                now_ms: 0,
            }),
            StreamResponse::ForkRefAdded { .. }
        ));
        assert_eq!(
            machine.apply(StreamCommand::DeleteStream {
                stream_id: stream("a-gone"),
            }),
            StreamResponse::Deleted {
                hard_deleted: false,
                parent_to_release: None,
            }
        );
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("b-live"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"live".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended { .. }
        ));

        let candidate = machine
            .plan_next_cold_flush(4, 4)
            .expect("plan next cold flush")
            .expect("candidate");
        assert_eq!(candidate.stream_id, stream("b-live"));
        assert_eq!(candidate.payload, b"live");
    }

    #[test]
    fn hot_payload_byte_metrics_follow_cold_flush() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "hot-a");
        create_stream(&mut machine, "hot-b");
        for (stream_name, payload) in [("hot-a", b"abcd".as_slice()), ("hot-b", b"xy".as_slice())] {
            assert!(matches!(
                machine.apply(StreamCommand::Append {
                    stream_id: stream(stream_name),
                    content_type: Some("application/octet-stream".to_owned()),
                    payload: payload.to_vec(),
                    close_after: false,
                    stream_seq: None,
                    producer: None,
                    now_ms: 0,
                }),
                StreamResponse::Appended { .. }
            ));
        }

        assert_eq!(machine.hot_payload_len(&stream("hot-a")), Ok(4));
        assert_eq!(machine.hot_payload_len(&stream("hot-b")), Ok(2));
        assert_eq!(machine.total_hot_payload_bytes(), 6);

        assert_eq!(
            machine.apply(StreamCommand::FlushCold {
                stream_id: stream("hot-a"),
                chunk: ColdChunkRef {
                    start_offset: 0,
                    end_offset: 3,
                    s3_path: "s3://bucket/hot-a/000000".to_owned(),
                    object_size: 3,
                },
            }),
            StreamResponse::ColdFlushed {
                hot_start_offset: 3,
            }
        );
        assert_eq!(machine.hot_payload_len(&stream("hot-a")), Ok(1));
        assert_eq!(machine.total_hot_payload_bytes(), 3);
    }

    #[test]
    fn snapshot_restore_round_trips_payload_metadata_and_stream_seq() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        assert_eq!(
            machine.apply(StreamCommand::CreateStream {
                stream_id: stream("snap-open"),
                content_type: "application/octet-stream".to_owned(),
                initial_payload: b"hi".to_vec(),
                close_after: false,
                stream_seq: Some("0001".to_owned()),
                producer: None,
                stream_ttl_seconds: Some(60),
                stream_expires_at_ms: None,
                forked_from: None,
                fork_offset: None,
                now_ms: 0,
            }),
            StreamResponse::Created {
                stream_id: stream("snap-open"),
                next_offset: 2,
                closed: false,
            }
        );
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("snap-open"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"abc".to_vec(),
                close_after: false,
                stream_seq: Some("0002".to_owned()),
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended {
                offset: 2,
                next_offset: 5,
                ..
            }
        ));
        assert_eq!(
            machine.apply(StreamCommand::CreateStream {
                stream_id: stream("snap-closed"),
                content_type: "application/octet-stream".to_owned(),
                initial_payload: b"x".to_vec(),
                close_after: true,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
                forked_from: None,
                fork_offset: None,
                now_ms: 0,
            }),
            StreamResponse::Created {
                stream_id: stream("snap-closed"),
                next_offset: 1,
                closed: true,
            }
        );

        let encoded = serde_json::to_vec(&machine.snapshot()).expect("serialize snapshot");
        let decoded =
            serde_json::from_slice::<StreamSnapshot>(&encoded).expect("deserialize snapshot");
        let mut restored = StreamStateMachine::restore(decoded).expect("restore snapshot");

        assert_eq!(
            restored.read(&stream("snap-open"), 0, 16).expect("read"),
            StreamRead {
                offset: 0,
                next_offset: 5,
                content_type: "application/octet-stream".to_owned(),
                payload: b"hiabc".to_vec(),
                up_to_date: true,
                closed: false,
            }
        );
        let metadata = restored.head(&stream("snap-open")).expect("metadata");
        assert_eq!(metadata.last_stream_seq.as_deref(), Some("0002"));
        assert_eq!(metadata.stream_ttl_seconds, Some(60));
        assert_eq!(metadata.stream_expires_at_ms, None);

        assert!(matches!(
            restored.apply(StreamCommand::Append {
                stream_id: stream("snap-open"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"bad".to_vec(),
                close_after: false,
                stream_seq: Some("0002".to_owned()),
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Error {
                code: StreamErrorCode::StreamSeqConflict,
                next_offset: Some(5),
                ..
            }
        ));
        assert_eq!(
            restored.apply(StreamCommand::Append {
                stream_id: stream("snap-open"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"!".to_vec(),
                close_after: false,
                stream_seq: Some("0003".to_owned()),
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended {
                offset: 5,
                next_offset: 6,
                closed: false,
                deduplicated: false,
                producer: None,
            }
        );
        assert!(matches!(
            restored.apply(StreamCommand::Append {
                stream_id: stream("snap-closed"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"!".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Error {
                code: StreamErrorCode::StreamClosed,
                next_offset: Some(1),
                ..
            }
        ));
    }

    #[test]
    fn snapshot_order_is_deterministic() {
        let mut machine = StreamStateMachine::new();
        for bucket_id in ["zzzz", "benchcmp", "aaaa"] {
            machine.apply(StreamCommand::CreateBucket {
                bucket_id: bucket_id.to_owned(),
            });
        }
        for stream_id in [
            BucketStreamId::new("zzzz", "stream-b"),
            BucketStreamId::new("benchcmp", "stream-b"),
            BucketStreamId::new("benchcmp", "stream-a"),
            BucketStreamId::new("aaaa", "stream-z"),
        ] {
            assert!(matches!(
                machine.apply(StreamCommand::CreateStream {
                    stream_id,
                    content_type: "application/octet-stream".to_owned(),
                    initial_payload: Vec::new(),
                    close_after: false,
                    stream_seq: None,
                    producer: None,
                    stream_ttl_seconds: None,
                    stream_expires_at_ms: None,
                    forked_from: None,
                    fork_offset: None,
                    now_ms: 0,
                }),
                StreamResponse::Created { .. }
            ));
        }

        let snapshot = machine.snapshot();
        assert_eq!(snapshot.buckets, ["aaaa", "benchcmp", "zzzz"]);
        assert_eq!(
            snapshot
                .streams
                .iter()
                .map(|entry| entry.metadata.stream_id.to_string())
                .collect::<Vec<_>>(),
            [
                "aaaa/stream-z",
                "benchcmp/stream-a",
                "benchcmp/stream-b",
                "zzzz/stream-b",
            ]
        );
    }

    #[test]
    fn snapshot_restore_rejects_invalid_entries() {
        assert_eq!(
            StreamStateMachine::restore(StreamSnapshot {
                buckets: vec!["benchcmp".to_owned(), "benchcmp".to_owned()],
                streams: Vec::new(),
            })
            .expect_err("duplicate bucket"),
            StreamSnapshotError::DuplicateBucket("benchcmp".to_owned())
        );

        assert!(matches!(
            StreamStateMachine::restore(StreamSnapshot {
                buckets: vec!["benchcmp".to_owned()],
                streams: vec![StreamSnapshotEntry {
                    metadata: StreamMetadata {
                        stream_id: BucketStreamId::new("missing", "stream"),
                        content_type: "application/octet-stream".to_owned(),
                        status: StreamStatus::Open,
                        tail_offset: 0,
                        last_stream_seq: None,
                        stream_ttl_seconds: None,
                        stream_expires_at_ms: None,
                        created_at_ms: 0,
                        last_ttl_touch_at_ms: 0,
                        forked_from: None,
                        fork_offset: None,
                        fork_ref_count: 0,
                    },
                    hot_start_offset: 0,
                    payload: Vec::new(),
                    hot_segments: Vec::new(),
                    cold_chunks: Vec::new(),
                    external_segments: Vec::new(),
                    message_records: Vec::new(),
                    visible_snapshot: None,
                    producer_states: Vec::new(),
                }],
            }),
            Err(StreamSnapshotError::MissingBucket(_))
        ));

        assert!(matches!(
            StreamStateMachine::restore(StreamSnapshot {
                buckets: vec!["benchcmp".to_owned()],
                streams: vec![StreamSnapshotEntry {
                    metadata: StreamMetadata {
                        stream_id: stream("bad-len"),
                        content_type: "application/octet-stream".to_owned(),
                        status: StreamStatus::Open,
                        tail_offset: 2,
                        last_stream_seq: None,
                        stream_ttl_seconds: None,
                        stream_expires_at_ms: None,
                        created_at_ms: 0,
                        last_ttl_touch_at_ms: 0,
                        forked_from: None,
                        fork_offset: None,
                        fork_ref_count: 0,
                    },
                    hot_start_offset: 0,
                    payload: b"x".to_vec(),
                    hot_segments: Vec::new(),
                    cold_chunks: Vec::new(),
                    external_segments: Vec::new(),
                    message_records: Vec::new(),
                    visible_snapshot: None,
                    producer_states: Vec::new(),
                }],
            }),
            Err(StreamSnapshotError::PayloadLengthMismatch { .. })
        ));

        assert!(matches!(
            StreamStateMachine::restore(StreamSnapshot {
                buckets: vec!["benchcmp".to_owned()],
                streams: vec![StreamSnapshotEntry {
                    metadata: StreamMetadata {
                        stream_id: stream("duplicate-producer"),
                        content_type: "application/octet-stream".to_owned(),
                        status: StreamStatus::Open,
                        tail_offset: 0,
                        last_stream_seq: None,
                        stream_ttl_seconds: None,
                        stream_expires_at_ms: None,
                        created_at_ms: 0,
                        last_ttl_touch_at_ms: 0,
                        forked_from: None,
                        fork_offset: None,
                        fork_ref_count: 0,
                    },
                    hot_start_offset: 0,
                    payload: Vec::new(),
                    hot_segments: Vec::new(),
                    cold_chunks: Vec::new(),
                    external_segments: Vec::new(),
                    message_records: Vec::new(),
                    visible_snapshot: None,
                    producer_states: vec![
                        ProducerSnapshot {
                            producer_id: "writer-1".to_owned(),
                            producer_epoch: 0,
                            producer_seq: 0,
                            last_start_offset: 0,
                            last_next_offset: 0,
                            last_closed: false,
                            last_items: Vec::new(),
                        },
                        ProducerSnapshot {
                            producer_id: "writer-1".to_owned(),
                            producer_epoch: 1,
                            producer_seq: 0,
                            last_start_offset: 0,
                            last_next_offset: 0,
                            last_closed: false,
                            last_items: Vec::new(),
                        },
                    ],
                }],
            }),
            Err(StreamSnapshotError::DuplicateProducer { .. })
        ));
    }

    #[test]
    fn close_is_monotonic_and_close_only_is_idempotent() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "s-1");

        assert_eq!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("s-1"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"abc".to_vec(),
                close_after: true,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended {
                offset: 0,
                next_offset: 3,
                closed: true,
                deduplicated: false,
                producer: None,
            }
        );
        assert_eq!(
            machine.apply(StreamCommand::Close {
                stream_id: stream("s-1"),
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Closed {
                next_offset: 3,
                deduplicated: false,
                producer: None,
            }
        );
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("s-1"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"x".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Error {
                code: StreamErrorCode::StreamClosed,
                next_offset: Some(3),
                ..
            }
        ));
    }

    #[test]
    fn stream_seq_must_strictly_increase() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "s-1");

        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("s-1"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"a".to_vec(),
                close_after: false,
                stream_seq: Some("0002".to_owned()),
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended { .. }
        ));
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("s-1"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"b".to_vec(),
                close_after: false,
                stream_seq: Some("0002".to_owned()),
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Error {
                code: StreamErrorCode::StreamSeqConflict,
                next_offset: Some(1),
                ..
            }
        ));
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("s-1"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"c".to_vec(),
                close_after: false,
                stream_seq: Some("0003".to_owned()),
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended {
                offset: 1,
                next_offset: 2,
                ..
            }
        ));
    }

    #[test]
    fn producer_headers_deduplicate_retries_and_fence_stale_epochs() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "producer-stream");

        assert_eq!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("producer-stream"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"a".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: Some(producer("writer-1", 0, 0)),
                now_ms: 0,
            }),
            StreamResponse::Appended {
                offset: 0,
                next_offset: 1,
                closed: false,
                deduplicated: false,
                producer: Some(producer("writer-1", 0, 0)),
            }
        );
        assert_eq!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("producer-stream"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"ignored-retry-body".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: Some(producer("writer-1", 0, 0)),
                now_ms: 0,
            }),
            StreamResponse::Appended {
                offset: 0,
                next_offset: 1,
                closed: false,
                deduplicated: true,
                producer: Some(producer("writer-1", 0, 0)),
            }
        );
        assert_eq!(
            machine
                .read(&stream("producer-stream"), 0, 16)
                .expect("read")
                .payload,
            b"a"
        );

        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("producer-stream"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"gap".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: Some(producer("writer-1", 0, 2)),
                now_ms: 0,
            }),
            StreamResponse::Error {
                code: StreamErrorCode::ProducerSeqConflict,
                ..
            }
        ));

        assert_eq!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("producer-stream"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"b".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: Some(producer("writer-1", 1, 0)),
                now_ms: 0,
            }),
            StreamResponse::Appended {
                offset: 1,
                next_offset: 2,
                closed: false,
                deduplicated: false,
                producer: Some(producer("writer-1", 1, 0)),
            }
        );
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("producer-stream"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"stale".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: Some(producer("writer-1", 0, 1)),
                now_ms: 0,
            }),
            StreamResponse::Error {
                code: StreamErrorCode::ProducerEpochStale,
                ..
            }
        ));
    }

    #[test]
    fn producer_append_batch_deduplicates_retries_without_partial_mutation() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "producer-batch");

        let first_payloads = [b"ab".as_slice(), b"c".as_slice()];
        let first = machine
            .append_batch_borrowed(
                stream("producer-batch"),
                Some("application/octet-stream"),
                &first_payloads,
                Some(producer("writer-1", 0, 0)),
                0,
            )
            .expect("first batch");
        assert_eq!(
            first.items,
            vec![
                StreamBatchAppendItem {
                    offset: 0,
                    next_offset: 2,
                    closed: false,
                    deduplicated: false,
                },
                StreamBatchAppendItem {
                    offset: 2,
                    next_offset: 3,
                    closed: false,
                    deduplicated: false,
                },
            ]
        );
        assert!(!first.deduplicated);

        let duplicate = machine
            .append_batch_borrowed(
                stream("producer-batch"),
                Some("application/octet-stream"),
                &first_payloads,
                Some(producer("writer-1", 0, 0)),
                0,
            )
            .expect("duplicate batch");
        assert!(duplicate.deduplicated);
        assert!(duplicate.items.iter().all(|item| item.deduplicated));
        assert_eq!(duplicate.items[0].offset, 0);
        assert_eq!(duplicate.items[1].next_offset, 3);
        assert_eq!(
            machine
                .read(&stream("producer-batch"), 0, 16)
                .expect("read")
                .payload,
            b"abc"
        );

        let invalid_payloads = [b"".as_slice()];
        assert!(matches!(
            machine.append_batch_borrowed(
                stream("producer-batch"),
                Some("application/octet-stream"),
                &invalid_payloads,
                Some(producer("writer-1", 0, 1)),
                0,
            ),
            Err(StreamResponse::Error {
                code: StreamErrorCode::EmptyAppend,
                ..
            })
        ));

        let next_payloads = [b"d".as_slice()];
        let next = machine
            .append_batch_borrowed(
                stream("producer-batch"),
                Some("application/octet-stream"),
                &next_payloads,
                Some(producer("writer-1", 0, 1)),
                0,
            )
            .expect("next batch");
        assert_eq!(next.items[0].offset, 3);
        assert_eq!(
            machine
                .read(&stream("producer-batch"), 0, 16)
                .expect("read")
                .payload,
            b"abcd"
        );
    }

    #[test]
    fn producer_state_survives_snapshot_restore() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "producer-snapshot");
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("producer-snapshot"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"a".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: Some(producer("writer-1", 0, 0)),
                now_ms: 0,
            }),
            StreamResponse::Appended {
                deduplicated: false,
                ..
            }
        ));

        let snapshot = machine.snapshot();
        assert_eq!(snapshot.streams[0].producer_states.len(), 1);
        assert_eq!(
            snapshot.streams[0].producer_states[0].last_items,
            vec![ProducerAppendRecord {
                start_offset: 0,
                next_offset: 1,
                closed: false,
            }]
        );
        let mut restored = StreamStateMachine::restore(snapshot).expect("restore snapshot");

        assert!(matches!(
            restored.apply(StreamCommand::Append {
                stream_id: stream("producer-snapshot"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"retry".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: Some(producer("writer-1", 0, 0)),
                now_ms: 0,
            }),
            StreamResponse::Appended {
                offset: 0,
                next_offset: 1,
                deduplicated: true,
                ..
            }
        ));
        assert_eq!(
            restored.apply(StreamCommand::Append {
                stream_id: stream("producer-snapshot"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"b".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: Some(producer("writer-1", 0, 1)),
                now_ms: 0,
            }),
            StreamResponse::Appended {
                offset: 1,
                next_offset: 2,
                closed: false,
                deduplicated: false,
                producer: Some(producer("writer-1", 0, 1)),
            }
        );
    }

    #[test]
    fn stream_ttl_uses_sliding_access_window() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        let stream_id = stream("ttl-window");

        assert_eq!(
            machine.apply(StreamCommand::CreateStream {
                stream_id: stream_id.clone(),
                content_type: "application/octet-stream".to_owned(),
                initial_payload: b"hi".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: Some(1),
                stream_expires_at_ms: None,
                forked_from: None,
                fork_offset: None,
                now_ms: 1_000,
            }),
            StreamResponse::Created {
                stream_id: stream_id.clone(),
                next_offset: 2,
                closed: false,
            }
        );

        assert_eq!(
            machine.access_requires_write(&stream_id, 1_500, false),
            Ok(false)
        );
        assert_eq!(
            machine
                .head_at(&stream_id, 1_500)
                .expect("head before ttl expiry")
                .last_ttl_touch_at_ms,
            1_000
        );
        assert_eq!(
            machine.access_requires_write(&stream_id, 1_500, true),
            Ok(true)
        );
        assert_eq!(
            machine.apply(StreamCommand::TouchStreamAccess {
                stream_id: stream_id.clone(),
                now_ms: 1_500,
                renew_ttl: true,
            }),
            StreamResponse::Accessed {
                changed: true,
                expired: false,
            }
        );

        assert!(machine.read_plan_at(&stream_id, 2, 16, 2_400).is_ok());
        assert_eq!(
            machine.apply(StreamCommand::Append {
                stream_id: stream_id.clone(),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"!".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 2_400,
            }),
            StreamResponse::Appended {
                offset: 2,
                next_offset: 3,
                closed: false,
                deduplicated: false,
                producer: None,
            }
        );
        assert!(machine.head_at(&stream_id, 3_399).is_some());
        assert!(machine.head_at(&stream_id, 3_400).is_none());
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream_id.clone(),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"late".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 3_401,
            }),
            StreamResponse::Error {
                code: StreamErrorCode::StreamNotFound,
                ..
            }
        ));
    }

    #[test]
    fn stream_expires_at_is_absolute_and_recreate_after_expiry() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        let stream_id = stream("absolute-expiry");

        assert!(matches!(
            machine.apply(StreamCommand::CreateStream {
                stream_id: stream_id.clone(),
                content_type: "application/octet-stream".to_owned(),
                initial_payload: Vec::new(),
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: Some(2_000),
                forked_from: None,
                fork_offset: None,
                now_ms: 1_000,
            }),
            StreamResponse::Created { .. }
        ));
        assert_eq!(
            machine.apply(StreamCommand::TouchStreamAccess {
                stream_id: stream_id.clone(),
                now_ms: 1_500,
                renew_ttl: true,
            }),
            StreamResponse::Accessed {
                changed: false,
                expired: false,
            }
        );
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream_id.clone(),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"body".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 1_600,
            }),
            StreamResponse::Appended { .. }
        ));
        assert!(machine.read_plan_at(&stream_id, 0, 16, 1_999).is_ok());
        assert!(matches!(
            machine.read_plan_at(&stream_id, 0, 16, 2_000),
            Err(StreamResponse::Error {
                code: StreamErrorCode::StreamNotFound,
                ..
            })
        ));
        assert_eq!(
            machine.apply(StreamCommand::CreateStream {
                stream_id: stream_id.clone(),
                content_type: "text/plain".to_owned(),
                initial_payload: Vec::new(),
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
                forked_from: None,
                fork_offset: None,
                now_ms: 2_001,
            }),
            StreamResponse::Created {
                stream_id,
                next_offset: 0,
                closed: false,
            }
        );
    }

    #[test]
    fn producer_duplicate_final_append_remains_idempotent_after_close() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "producer-close");

        assert_eq!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("producer-close"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"final".to_vec(),
                close_after: true,
                stream_seq: None,
                producer: Some(producer("writer-1", 0, 0)),
                now_ms: 0,
            }),
            StreamResponse::Appended {
                offset: 0,
                next_offset: 5,
                closed: true,
                deduplicated: false,
                producer: Some(producer("writer-1", 0, 0)),
            }
        );
        assert_eq!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("producer-close"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"final".to_vec(),
                close_after: true,
                stream_seq: None,
                producer: Some(producer("writer-1", 0, 0)),
                now_ms: 0,
            }),
            StreamResponse::Appended {
                offset: 0,
                next_offset: 5,
                closed: true,
                deduplicated: true,
                producer: Some(producer("writer-1", 0, 0)),
            }
        );
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("producer-close"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"too-late".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: Some(producer("writer-1", 0, 1)),
                now_ms: 0,
            }),
            StreamResponse::Error {
                code: StreamErrorCode::StreamClosed,
                next_offset: Some(5),
                ..
            }
        ));
    }

    #[test]
    fn append_conflict_precedence_reports_closed_before_mismatch_or_seq() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "closed-precedence");

        assert_eq!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("closed-precedence"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"final".to_vec(),
                close_after: true,
                stream_seq: Some("0002".to_owned()),
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended {
                offset: 0,
                next_offset: 5,
                closed: true,
                deduplicated: false,
                producer: None,
            }
        );

        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("closed-precedence"),
                content_type: Some("text/plain".to_owned()),
                payload: b"too-late".to_vec(),
                close_after: false,
                stream_seq: Some("0001".to_owned()),
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Error {
                code: StreamErrorCode::StreamClosed,
                next_offset: Some(5),
                ..
            }
        ));
    }

    #[test]
    fn bucket_delete_requires_empty_bucket() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "s-1");

        assert!(matches!(
            machine.apply(StreamCommand::DeleteBucket {
                bucket_id: "benchcmp".to_owned(),
            }),
            StreamResponse::Error {
                code: StreamErrorCode::BucketNotEmpty,
                ..
            }
        ));
        assert_eq!(
            machine.apply(StreamCommand::DeleteStream {
                stream_id: stream("s-1"),
            }),
            StreamResponse::Deleted {
                hard_deleted: true,
                parent_to_release: None,
            }
        );
        assert_eq!(
            machine.apply(StreamCommand::DeleteBucket {
                bucket_id: "benchcmp".to_owned(),
            }),
            StreamResponse::BucketDeleted {
                bucket_id: "benchcmp".to_owned(),
            }
        );
    }

    #[test]
    fn fork_refs_soft_delete_and_release_parent_on_last_child() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "source");
        create_stream(&mut machine, "fork");

        assert_eq!(
            machine.apply(StreamCommand::AddForkRef {
                stream_id: stream("source"),
                now_ms: 0,
            }),
            StreamResponse::ForkRefAdded { fork_ref_count: 1 }
        );
        assert_eq!(
            machine.apply(StreamCommand::DeleteStream {
                stream_id: stream("source"),
            }),
            StreamResponse::Deleted {
                hard_deleted: false,
                parent_to_release: None,
            }
        );
        assert!(matches!(
            machine.read_plan(&stream("source"), 0, 1),
            Err(StreamResponse::Error {
                code: StreamErrorCode::StreamGone,
                ..
            })
        ));
        assert_eq!(
            machine.apply(StreamCommand::DeleteStream {
                stream_id: stream("fork"),
            }),
            StreamResponse::Deleted {
                hard_deleted: true,
                parent_to_release: None,
            }
        );
        assert_eq!(
            machine.apply(StreamCommand::ReleaseForkRef {
                stream_id: stream("source"),
            }),
            StreamResponse::ForkRefReleased {
                hard_deleted: true,
                fork_ref_count: 0,
                parent_to_release: None,
            }
        );
        assert!(machine.head(&stream("source")).is_none());
    }

    #[test]
    fn publish_snapshot_advances_retention_on_message_boundary() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "snap");
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("snap"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"abc".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended {
                offset: 0,
                next_offset: 3,
                ..
            }
        ));
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("snap"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"de".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended {
                offset: 3,
                next_offset: 5,
                ..
            }
        ));

        assert_eq!(
            machine.apply(StreamCommand::PublishSnapshot {
                stream_id: stream("snap"),
                snapshot_offset: 3,
                content_type: "application/json".to_owned(),
                payload: br#"{"state":"abc"}"#.to_vec(),
                now_ms: 0,
            }),
            StreamResponse::SnapshotPublished { snapshot_offset: 3 }
        );
        assert!(matches!(
            machine.read_plan(&stream("snap"), 0, 1),
            Err(StreamResponse::Error {
                code: StreamErrorCode::StreamGone,
                next_offset: Some(3),
                ..
            })
        ));
        let read = machine.read(&stream("snap"), 3, 2).expect("retained read");
        assert_eq!(read.payload, b"de");
        let snapshot = machine
            .read_snapshot(&stream("snap"), 3)
            .expect("visible snapshot");
        assert_eq!(snapshot.content_type, "application/json");
        assert_eq!(snapshot.payload, br#"{"state":"abc"}"#);
        let bootstrap = machine.bootstrap_plan(&stream("snap")).expect("bootstrap");
        assert_eq!(
            bootstrap.snapshot.as_ref().map(|snapshot| snapshot.offset),
            Some(3)
        );
        assert_eq!(
            bootstrap.updates,
            vec![StreamMessageRecord {
                start_offset: 3,
                end_offset: 5,
            }]
        );
    }

    #[test]
    fn publish_snapshot_rejects_unaligned_offset() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "unaligned");
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("unaligned"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: b"abc".to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended { .. }
        ));
        assert!(matches!(
            machine.apply(StreamCommand::PublishSnapshot {
                stream_id: stream("unaligned"),
                snapshot_offset: 2,
                content_type: "application/octet-stream".to_owned(),
                payload: b"ab".to_vec(),
                now_ms: 0,
            }),
            StreamResponse::Error {
                code: StreamErrorCode::InvalidSnapshot,
                next_offset: Some(3),
                ..
            }
        ));
    }

    #[test]
    fn snapshot_restore_preserves_visible_snapshot_and_message_records() {
        let mut machine = StreamStateMachine::new();
        create_bucket(&mut machine);
        create_stream(&mut machine, "restore-snap");
        let _ = machine.apply(StreamCommand::Append {
            stream_id: stream("restore-snap"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abc".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        });
        let _ = machine.apply(StreamCommand::Append {
            stream_id: stream("restore-snap"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"de".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        });
        let _ = machine.apply(StreamCommand::PublishSnapshot {
            stream_id: stream("restore-snap"),
            snapshot_offset: 3,
            content_type: "application/octet-stream".to_owned(),
            payload: b"abc-state".to_vec(),
            now_ms: 0,
        });

        let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore");
        assert_eq!(
            restored
                .read_snapshot(&stream("restore-snap"), 3)
                .expect("snapshot")
                .payload,
            b"abc-state"
        );
        assert_eq!(
            restored
                .bootstrap_plan(&stream("restore-snap"))
                .expect("bootstrap")
                .updates,
            vec![StreamMessageRecord {
                start_offset: 3,
                end_offset: 5,
            }]
        );
    }
}
