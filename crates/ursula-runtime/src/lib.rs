use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use opendal::{Operator, Scheme};
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;
use ursula_shard::{
    BucketStreamId, CoreId, RaftGroupId, ShardId, ShardMapError, ShardPlacement, StaticShardMap,
};
use ursula_stream::{
    AppendStreamInput, ObjectPayloadRef, StreamCommand, StreamMessageRecord, StreamReadPlan,
    StreamReadSegment, StreamResponse, StreamSnapshot, StreamStateMachine,
};
pub use ursula_stream::{
    ColdChunkRef, ColdFlushCandidate, ExternalPayloadRef, ProducerRequest, StreamErrorCode,
};

const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
static COLD_CHUNK_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct ColdStore {
    operator: Operator,
}

pub type ColdStoreHandle = Arc<ColdStore>;

impl ColdStore {
    pub fn memory() -> io::Result<Self> {
        let operator = Operator::via_iter(Scheme::Memory, [])
            .map_err(|err| io::Error::other(err.to_string()))?;
        Ok(Self { operator })
    }

    pub fn fs(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref();
        fs::create_dir_all(root)?;
        let operator = Operator::via_iter(
            Scheme::Fs,
            [("root".to_owned(), root.to_string_lossy().to_string())],
        )
        .map_err(|err| io::Error::other(err.to_string()))?;
        Ok(Self { operator })
    }

    pub fn s3_from_env() -> io::Result<Self> {
        Self::s3_from_env_with_root(None)
    }

    pub fn s3_from_env_with_root(root_override: Option<&str>) -> io::Result<Self> {
        let bucket = std::env::var("URSULA_COLD_S3_BUCKET").map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "URSULA_COLD_S3_BUCKET is required when URSULA_COLD_BACKEND=s3",
            )
        })?;
        if bucket.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "URSULA_COLD_S3_BUCKET must not be empty",
            ));
        }

        let mut builder = opendal::services::S3::default().bucket(&bucket);
        if let Some(root) = root_override {
            if !root.trim().is_empty() {
                builder = builder.root(root);
            }
        } else if let Ok(root) = std::env::var("URSULA_COLD_ROOT")
            && !root.trim().is_empty()
        {
            builder = builder.root(&root);
        }
        if let Ok(region) = std::env::var("URSULA_COLD_S3_REGION")
            && !region.trim().is_empty()
        {
            builder = builder.region(&region);
        }
        if let Ok(endpoint) = std::env::var("URSULA_COLD_S3_ENDPOINT")
            && !endpoint.trim().is_empty()
        {
            builder = builder.endpoint(&endpoint);
        }
        if let Ok(access_key_id) = std::env::var("URSULA_COLD_S3_ACCESS_KEY_ID")
            && !access_key_id.trim().is_empty()
        {
            builder = builder.access_key_id(&access_key_id);
        }
        if let Ok(secret_access_key) = std::env::var("URSULA_COLD_S3_SECRET_ACCESS_KEY")
            && !secret_access_key.trim().is_empty()
        {
            builder = builder.secret_access_key(&secret_access_key);
        }
        if let Ok(session_token) = std::env::var("URSULA_COLD_S3_SESSION_TOKEN")
            && !session_token.trim().is_empty()
        {
            builder = builder.session_token(&session_token);
        }

        Ok(Self {
            operator: Operator::new(builder)
                .map_err(|err| io::Error::other(err.to_string()))?
                .finish(),
        })
    }

    pub fn from_env() -> io::Result<Option<ColdStoreHandle>> {
        let backend = std::env::var("URSULA_COLD_BACKEND")
            .unwrap_or_else(|_| "none".to_owned())
            .to_ascii_lowercase();
        let store = match backend.as_str() {
            "none" | "disabled" | "off" => return Ok(None),
            "memory" | "mem" | "inmem" => Self::memory()?,
            "fs" => {
                let root =
                    std::env::var("URSULA_COLD_ROOT").unwrap_or_else(|_| "data/cold".to_owned());
                Self::fs(root)?
            }
            "s3" => Self::s3_from_env()?,
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported URSULA_COLD_BACKEND '{other}'"),
                ));
            }
        };
        Ok(Some(Arc::new(store)))
    }

    pub async fn write_chunk(&self, path: &str, payload: &[u8]) -> io::Result<u64> {
        if path.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cold chunk path must not be empty",
            ));
        }
        self.operator
            .write(path, payload.to_vec())
            .await
            .map_err(|err| cold_store_io_error(path, err))?;
        Ok(u64::try_from(payload.len()).expect("payload len fits u64"))
    }

    pub async fn delete_chunk(&self, path: &str) -> io::Result<()> {
        if path.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cold chunk path must not be empty",
            ));
        }
        self.operator
            .delete(path)
            .await
            .map_err(|err| cold_store_io_error(path, err))
    }

    pub async fn remove_all(&self, path: &str) -> io::Result<()> {
        self.operator
            .remove_all(path)
            .await
            .map_err(|err| cold_store_io_error(path, err))
    }

    pub async fn read_chunk_range(
        &self,
        chunk: &ColdChunkRef,
        read_start_offset: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        let object = ObjectPayloadRef {
            start_offset: chunk.start_offset,
            end_offset: chunk.end_offset,
            s3_path: chunk.s3_path.clone(),
            object_size: chunk.object_size,
        };
        self.read_object_range(&object, read_start_offset, len)
            .await
    }

    pub async fn read_object_range(
        &self,
        object: &ObjectPayloadRef,
        read_start_offset: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let len_u64 = u64::try_from(len).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "cold read length exceeds u64")
        })?;
        let read_end = read_start_offset.checked_add(len_u64).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "cold read range overflow")
        })?;
        if read_start_offset < object.start_offset || read_end > object.end_offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cold read range [{read_start_offset}..{read_end}) is outside object segment [{}..{})",
                    object.start_offset, object.end_offset
                ),
            ));
        }
        let object_start = read_start_offset - object.start_offset;
        let object_end = object_start.checked_add(len_u64).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "cold read range overflow")
        })?;
        if object_end > object.object_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "cold read range [{object_start}..{object_end}) is outside object '{}' size {}",
                    object.s3_path, object.object_size
                ),
            ));
        }
        let bytes = self
            .operator
            .read_with(&object.s3_path)
            .range(object_start..object_end)
            .await
            .map_err(|err| cold_store_io_error(&object.s3_path, err))?
            .to_bytes();
        if bytes.len() != len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "cold object '{}' returned {} bytes for requested range [{}..{})",
                    object.s3_path,
                    bytes.len(),
                    object_start,
                    object_end
                ),
            ));
        }
        Ok(bytes.to_vec())
    }
}

fn cold_store_io_error(path: &str, err: opendal::Error) -> io::Error {
    io::Error::other(format!("cold object '{path}': {err}"))
}

pub fn new_cold_chunk_path(
    stream_id: &BucketStreamId,
    start_offset: u64,
    end_offset: u64,
) -> String {
    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let sequence = COLD_CHUNK_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!(
        "{stream_id}/chunks/{start_offset:016x}-{end_offset:016x}-{unix_nanos:032x}-{sequence:016x}.bin"
    )
}

pub fn new_external_payload_path(stream_id: &BucketStreamId) -> String {
    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let sequence = COLD_CHUNK_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{stream_id}/external/{unix_nanos:032x}-{sequence:016x}.bin")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStreamRequest {
    pub stream_id: BucketStreamId,
    pub content_type: String,
    pub content_type_explicit: bool,
    pub initial_payload: Bytes,
    pub close_after: bool,
    pub stream_seq: Option<String>,
    pub producer: Option<ProducerRequest>,
    pub stream_ttl_seconds: Option<u64>,
    pub stream_expires_at_ms: Option<u64>,
    pub forked_from: Option<BucketStreamId>,
    pub fork_offset: Option<u64>,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateStreamExternalRequest {
    pub stream_id: BucketStreamId,
    pub content_type: String,
    pub initial_payload: ExternalPayloadRef,
    pub close_after: bool,
    pub stream_seq: Option<String>,
    pub producer: Option<ProducerRequest>,
    pub stream_ttl_seconds: Option<u64>,
    pub stream_expires_at_ms: Option<u64>,
    pub forked_from: Option<BucketStreamId>,
    pub fork_offset: Option<u64>,
    pub now_ms: u64,
}

impl CreateStreamExternalRequest {
    pub fn from_create_request(
        request: CreateStreamRequest,
        initial_payload: ExternalPayloadRef,
    ) -> Self {
        Self {
            stream_id: request.stream_id,
            content_type: request.content_type,
            initial_payload,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            stream_ttl_seconds: request.stream_ttl_seconds,
            stream_expires_at_ms: request.stream_expires_at_ms,
            forked_from: request.forked_from,
            fork_offset: request.fork_offset,
            now_ms: request.now_ms,
        }
    }
}

impl CreateStreamRequest {
    pub fn new(stream_id: BucketStreamId, content_type: impl Into<String>) -> Self {
        Self {
            stream_id,
            content_type: content_type.into(),
            content_type_explicit: true,
            initial_payload: Bytes::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            forked_from: None,
            fork_offset: None,
            now_ms: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateStreamResponse {
    pub placement: ShardPlacement,
    pub next_offset: u64,
    pub closed: bool,
    pub already_exists: bool,
    pub group_commit_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadStreamRequest {
    pub stream_id: BucketStreamId,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadStreamResponse {
    pub placement: ShardPlacement,
    pub content_type: String,
    pub tail_offset: u64,
    pub closed: bool,
    pub stream_ttl_seconds: Option<u64>,
    pub stream_expires_at_ms: Option<u64>,
    pub snapshot_offset: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadStreamRequest {
    pub stream_id: BucketStreamId,
    pub offset: u64,
    pub max_len: usize,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadStreamResponse {
    pub placement: ShardPlacement,
    pub offset: u64,
    pub next_offset: u64,
    pub content_type: String,
    pub payload: Vec<u8>,
    pub up_to_date: bool,
    pub closed: bool,
}

pub enum GroupReadStreamBody {
    Materialized(Vec<u8>),
    Planned {
        stream_id: BucketStreamId,
        plan: StreamReadPlan,
        cold_store: Option<ColdStoreHandle>,
    },
    #[cfg(test)]
    Blocking {
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        payload: Vec<u8>,
    },
}

pub struct GroupReadStreamParts {
    pub placement: ShardPlacement,
    pub offset: u64,
    pub next_offset: u64,
    pub content_type: String,
    pub up_to_date: bool,
    pub closed: bool,
    pub body: GroupReadStreamBody,
}

impl GroupReadStreamParts {
    pub fn from_response(response: ReadStreamResponse) -> Self {
        Self {
            placement: response.placement,
            offset: response.offset,
            next_offset: response.next_offset,
            content_type: response.content_type,
            up_to_date: response.up_to_date,
            closed: response.closed,
            body: GroupReadStreamBody::Materialized(response.payload),
        }
    }

    pub fn from_plan(
        placement: ShardPlacement,
        stream_id: BucketStreamId,
        plan: StreamReadPlan,
        cold_store: Option<ColdStoreHandle>,
    ) -> Self {
        Self {
            placement,
            offset: plan.offset,
            next_offset: plan.next_offset,
            content_type: plan.content_type.clone(),
            up_to_date: plan.up_to_date,
            closed: plan.closed,
            body: GroupReadStreamBody::Planned {
                stream_id,
                plan,
                cold_store,
            },
        }
    }

    pub async fn into_response(self) -> Result<ReadStreamResponse, GroupEngineError> {
        let payload = match &self.body {
            GroupReadStreamBody::Materialized(payload) => payload.clone(),
            GroupReadStreamBody::Planned {
                stream_id,
                plan,
                cold_store,
            } => {
                InMemoryGroupEngine::read_payload_from_plan(cold_store.as_ref(), stream_id, plan)
                    .await?
            }
            #[cfg(test)]
            GroupReadStreamBody::Blocking {
                entered,
                release,
                payload,
            } => {
                entered.notify_one();
                release.notified().await;
                payload.clone()
            }
        };
        Ok(ReadStreamResponse {
            placement: self.placement,
            offset: self.offset,
            next_offset: self.next_offset,
            content_type: self.content_type,
            payload,
            up_to_date: self.up_to_date,
            closed: self.closed,
        })
    }

    fn payload_is_empty(&self) -> bool {
        match &self.body {
            GroupReadStreamBody::Materialized(payload) => payload.is_empty(),
            GroupReadStreamBody::Planned { plan, .. } => {
                plan.segments.iter().all(|segment| match segment {
                    StreamReadSegment::Hot(payload) => payload.is_empty(),
                    StreamReadSegment::Object(segment) => segment.len == 0,
                })
            }
            #[cfg(test)]
            GroupReadStreamBody::Blocking { payload, .. } => payload.is_empty(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishSnapshotRequest {
    pub stream_id: BucketStreamId,
    pub snapshot_offset: u64,
    pub content_type: String,
    pub payload: Bytes,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishSnapshotResponse {
    pub placement: ShardPlacement,
    pub snapshot_offset: u64,
    pub group_commit_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSnapshotRequest {
    pub stream_id: BucketStreamId,
    pub snapshot_offset: Option<u64>,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSnapshotResponse {
    pub placement: ShardPlacement,
    pub snapshot_offset: u64,
    pub next_offset: u64,
    pub content_type: String,
    pub payload: Vec<u8>,
    pub up_to_date: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteSnapshotRequest {
    pub stream_id: BucketStreamId,
    pub snapshot_offset: u64,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapStreamRequest {
    pub stream_id: BucketStreamId,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapUpdate {
    pub start_offset: u64,
    pub next_offset: u64,
    pub content_type: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapStreamResponse {
    pub placement: ShardPlacement,
    pub snapshot_offset: Option<u64>,
    pub snapshot_content_type: String,
    pub snapshot_payload: Vec<u8>,
    pub updates: Vec<BootstrapUpdate>,
    pub next_offset: u64,
    pub up_to_date: bool,
    pub closed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseStreamRequest {
    pub stream_id: BucketStreamId,
    pub stream_seq: Option<String>,
    pub producer: Option<ProducerRequest>,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloseStreamResponse {
    pub placement: ShardPlacement,
    pub next_offset: u64,
    pub group_commit_index: u64,
    pub deduplicated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteStreamRequest {
    pub stream_id: BucketStreamId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteStreamResponse {
    pub placement: ShardPlacement,
    pub group_commit_index: u64,
    pub hard_deleted: bool,
    pub parent_to_release: Option<BucketStreamId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkRefResponse {
    pub placement: ShardPlacement,
    pub fork_ref_count: u64,
    pub hard_deleted: bool,
    pub parent_to_release: Option<BucketStreamId>,
    pub group_commit_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushColdRequest {
    pub stream_id: BucketStreamId,
    pub chunk: ColdChunkRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlushColdResponse {
    pub placement: ShardPlacement,
    pub hot_start_offset: u64,
    pub group_commit_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TouchStreamAccessResponse {
    pub placement: ShardPlacement,
    pub changed: bool,
    pub expired: bool,
    pub group_commit_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanColdFlushRequest {
    pub stream_id: BucketStreamId,
    pub min_hot_bytes: usize,
    pub max_flush_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanGroupColdFlushRequest {
    pub min_hot_bytes: usize,
    pub max_flush_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdHotBacklog {
    pub stream_id: BucketStreamId,
    pub stream_hot_bytes: u64,
    pub group_hot_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ColdWriteAdmission {
    pub max_hot_bytes_per_group: Option<u64>,
}

impl ColdWriteAdmission {
    fn is_enabled(self) -> bool {
        self.max_hot_bytes_per_group.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRequest {
    pub stream_id: BucketStreamId,
    pub content_type: String,
    pub payload: Bytes,
    pub close_after: bool,
    pub stream_seq: Option<String>,
    pub producer: Option<ProducerRequest>,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendExternalRequest {
    pub stream_id: BucketStreamId,
    pub content_type: String,
    pub payload: ExternalPayloadRef,
    pub close_after: bool,
    pub stream_seq: Option<String>,
    pub producer: Option<ProducerRequest>,
    pub now_ms: u64,
}

impl AppendExternalRequest {
    pub fn from_append_request(request: AppendRequest, payload: ExternalPayloadRef) -> Self {
        Self {
            stream_id: request.stream_id,
            content_type: request.content_type,
            payload,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            now_ms: request.now_ms,
        }
    }
}

impl AppendRequest {
    pub fn new(stream_id: BucketStreamId, payload_len: u64) -> Self {
        Self {
            stream_id,
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            payload: Bytes::from(vec![
                0;
                usize::try_from(payload_len)
                    .expect("payload_len fits usize")
            ]),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }
    }

    pub fn from_bytes(stream_id: BucketStreamId, payload: impl Into<Bytes>) -> Self {
        Self {
            stream_id,
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            payload: payload.into(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }
    }

    pub fn payload_len(&self) -> u64 {
        u64::try_from(self.payload.len()).expect("payload len fits u64")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendBatchRequest {
    pub stream_id: BucketStreamId,
    pub content_type: String,
    pub payloads: Vec<Bytes>,
    pub producer: Option<ProducerRequest>,
    pub now_ms: u64,
}

impl AppendBatchRequest {
    pub fn new<P>(stream_id: BucketStreamId, payloads: Vec<P>) -> Self
    where
        P: Into<Bytes>,
    {
        Self {
            stream_id,
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            payloads: payloads.into_iter().map(Into::into).collect(),
            producer: None,
            now_ms: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendResponse {
    pub placement: ShardPlacement,
    pub start_offset: u64,
    pub next_offset: u64,
    pub stream_append_count: u64,
    pub group_commit_index: u64,
    pub closed: bool,
    pub deduplicated: bool,
    pub producer: Option<ProducerRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendBatchResponse {
    pub placement: ShardPlacement,
    pub items: Vec<Result<AppendResponse, RuntimeError>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamAppendCount {
    pub stream_id: BucketStreamId,
    pub append_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupSnapshot {
    pub placement: ShardPlacement,
    pub group_commit_index: u64,
    pub stream_snapshot: StreamSnapshot,
    pub stream_append_counts: Vec<StreamAppendCount>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum GroupWriteCommand {
    CreateStream {
        stream_id: BucketStreamId,
        content_type: String,
        initial_payload: Bytes,
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
        content_type: String,
        payload: Bytes,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    },
    AppendExternal {
        stream_id: BucketStreamId,
        content_type: String,
        payload: ExternalPayloadRef,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    },
    AppendBatch {
        stream_id: BucketStreamId,
        content_type: String,
        payloads: Vec<Bytes>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    },
    PublishSnapshot {
        stream_id: BucketStreamId,
        snapshot_offset: u64,
        content_type: String,
        payload: Bytes,
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
    CloseStream {
        stream_id: BucketStreamId,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    },
    DeleteStream {
        stream_id: BucketStreamId,
    },
    Batch {
        commands: Vec<GroupWriteCommand>,
    },
}

impl From<CreateStreamRequest> for GroupWriteCommand {
    fn from(request: CreateStreamRequest) -> Self {
        Self::CreateStream {
            stream_id: request.stream_id,
            content_type: request.content_type,
            initial_payload: request.initial_payload,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            stream_ttl_seconds: request.stream_ttl_seconds,
            stream_expires_at_ms: request.stream_expires_at_ms,
            forked_from: request.forked_from,
            fork_offset: request.fork_offset,
            now_ms: request.now_ms,
        }
    }
}

impl From<&CreateStreamRequest> for GroupWriteCommand {
    fn from(request: &CreateStreamRequest) -> Self {
        Self::CreateStream {
            stream_id: request.stream_id.clone(),
            content_type: request.content_type.clone(),
            initial_payload: request.initial_payload.clone(),
            close_after: request.close_after,
            stream_seq: request.stream_seq.clone(),
            producer: request.producer.clone(),
            stream_ttl_seconds: request.stream_ttl_seconds,
            stream_expires_at_ms: request.stream_expires_at_ms,
            forked_from: request.forked_from.clone(),
            fork_offset: request.fork_offset,
            now_ms: request.now_ms,
        }
    }
}

impl From<CreateStreamExternalRequest> for GroupWriteCommand {
    fn from(request: CreateStreamExternalRequest) -> Self {
        Self::CreateExternal {
            stream_id: request.stream_id,
            content_type: request.content_type,
            initial_payload: request.initial_payload,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            stream_ttl_seconds: request.stream_ttl_seconds,
            stream_expires_at_ms: request.stream_expires_at_ms,
            forked_from: request.forked_from,
            fork_offset: request.fork_offset,
            now_ms: request.now_ms,
        }
    }
}

impl From<&CreateStreamExternalRequest> for GroupWriteCommand {
    fn from(request: &CreateStreamExternalRequest) -> Self {
        Self::CreateExternal {
            stream_id: request.stream_id.clone(),
            content_type: request.content_type.clone(),
            initial_payload: request.initial_payload.clone(),
            close_after: request.close_after,
            stream_seq: request.stream_seq.clone(),
            producer: request.producer.clone(),
            stream_ttl_seconds: request.stream_ttl_seconds,
            stream_expires_at_ms: request.stream_expires_at_ms,
            forked_from: request.forked_from.clone(),
            fork_offset: request.fork_offset,
            now_ms: request.now_ms,
        }
    }
}

impl From<AppendRequest> for GroupWriteCommand {
    fn from(request: AppendRequest) -> Self {
        Self::Append {
            stream_id: request.stream_id,
            content_type: request.content_type,
            payload: request.payload,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            now_ms: request.now_ms,
        }
    }
}

impl From<&AppendRequest> for GroupWriteCommand {
    fn from(request: &AppendRequest) -> Self {
        Self::Append {
            stream_id: request.stream_id.clone(),
            content_type: request.content_type.clone(),
            payload: request.payload.clone(),
            close_after: request.close_after,
            stream_seq: request.stream_seq.clone(),
            producer: request.producer.clone(),
            now_ms: request.now_ms,
        }
    }
}

impl From<AppendExternalRequest> for GroupWriteCommand {
    fn from(request: AppendExternalRequest) -> Self {
        Self::AppendExternal {
            stream_id: request.stream_id,
            content_type: request.content_type,
            payload: request.payload,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            now_ms: request.now_ms,
        }
    }
}

impl From<&AppendExternalRequest> for GroupWriteCommand {
    fn from(request: &AppendExternalRequest) -> Self {
        Self::AppendExternal {
            stream_id: request.stream_id.clone(),
            content_type: request.content_type.clone(),
            payload: request.payload.clone(),
            close_after: request.close_after,
            stream_seq: request.stream_seq.clone(),
            producer: request.producer.clone(),
            now_ms: request.now_ms,
        }
    }
}

impl From<AppendBatchRequest> for GroupWriteCommand {
    fn from(request: AppendBatchRequest) -> Self {
        Self::AppendBatch {
            stream_id: request.stream_id,
            content_type: request.content_type,
            payloads: request.payloads,
            producer: request.producer,
            now_ms: request.now_ms,
        }
    }
}

impl From<&AppendBatchRequest> for GroupWriteCommand {
    fn from(request: &AppendBatchRequest) -> Self {
        Self::AppendBatch {
            stream_id: request.stream_id.clone(),
            content_type: request.content_type.clone(),
            payloads: request.payloads.clone(),
            producer: request.producer.clone(),
            now_ms: request.now_ms,
        }
    }
}

impl From<PublishSnapshotRequest> for GroupWriteCommand {
    fn from(request: PublishSnapshotRequest) -> Self {
        Self::PublishSnapshot {
            stream_id: request.stream_id,
            snapshot_offset: request.snapshot_offset,
            content_type: request.content_type,
            payload: request.payload,
            now_ms: request.now_ms,
        }
    }
}

impl From<&PublishSnapshotRequest> for GroupWriteCommand {
    fn from(request: &PublishSnapshotRequest) -> Self {
        Self::PublishSnapshot {
            stream_id: request.stream_id.clone(),
            snapshot_offset: request.snapshot_offset,
            content_type: request.content_type.clone(),
            payload: request.payload.clone(),
            now_ms: request.now_ms,
        }
    }
}

impl From<CloseStreamRequest> for GroupWriteCommand {
    fn from(request: CloseStreamRequest) -> Self {
        Self::CloseStream {
            stream_id: request.stream_id,
            stream_seq: request.stream_seq,
            producer: request.producer,
            now_ms: request.now_ms,
        }
    }
}

impl From<&CloseStreamRequest> for GroupWriteCommand {
    fn from(request: &CloseStreamRequest) -> Self {
        Self::CloseStream {
            stream_id: request.stream_id.clone(),
            stream_seq: request.stream_seq.clone(),
            producer: request.producer.clone(),
            now_ms: request.now_ms,
        }
    }
}

impl From<DeleteStreamRequest> for GroupWriteCommand {
    fn from(request: DeleteStreamRequest) -> Self {
        Self::DeleteStream {
            stream_id: request.stream_id,
        }
    }
}

impl From<&DeleteStreamRequest> for GroupWriteCommand {
    fn from(request: &DeleteStreamRequest) -> Self {
        Self::DeleteStream {
            stream_id: request.stream_id.clone(),
        }
    }
}

impl From<FlushColdRequest> for GroupWriteCommand {
    fn from(request: FlushColdRequest) -> Self {
        Self::FlushCold {
            stream_id: request.stream_id,
            chunk: request.chunk,
        }
    }
}

impl From<&FlushColdRequest> for GroupWriteCommand {
    fn from(request: &FlushColdRequest) -> Self {
        Self::FlushCold {
            stream_id: request.stream_id.clone(),
            chunk: request.chunk.clone(),
        }
    }
}

impl fmt::Display for GroupWriteCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateStream { stream_id, .. } => {
                write!(f, "create_stream:{stream_id}")
            }
            Self::CreateExternal {
                stream_id,
                initial_payload,
                ..
            } => {
                write!(
                    f,
                    "create_external:{stream_id}:{} bytes",
                    initial_payload.payload_len
                )
            }
            Self::Append {
                stream_id, payload, ..
            } => {
                write!(f, "append:{stream_id}:{} bytes", payload.len())
            }
            Self::AppendExternal {
                stream_id, payload, ..
            } => {
                write!(
                    f,
                    "append_external:{stream_id}:{} bytes",
                    payload.payload_len
                )
            }
            Self::AppendBatch {
                stream_id,
                payloads,
                ..
            } => {
                write!(f, "append_batch:{stream_id}:{} items", payloads.len())
            }
            Self::PublishSnapshot {
                stream_id,
                snapshot_offset,
                payload,
                ..
            } => {
                write!(
                    f,
                    "publish_snapshot:{stream_id}:{snapshot_offset}:{} bytes",
                    payload.len()
                )
            }
            Self::TouchStreamAccess {
                stream_id,
                renew_ttl,
                ..
            } => {
                write!(f, "touch_stream_access:{stream_id}:renew_ttl={renew_ttl}")
            }
            Self::AddForkRef { stream_id, .. } => {
                write!(f, "add_fork_ref:{stream_id}")
            }
            Self::ReleaseForkRef { stream_id } => {
                write!(f, "release_fork_ref:{stream_id}")
            }
            Self::FlushCold { stream_id, chunk } => {
                write!(
                    f,
                    "flush_cold:{stream_id}:{}..{}",
                    chunk.start_offset, chunk.end_offset
                )
            }
            Self::CloseStream { stream_id, .. } => {
                write!(f, "close_stream:{stream_id}")
            }
            Self::DeleteStream { stream_id } => {
                write!(f, "delete_stream:{stream_id}")
            }
            Self::Batch { commands } => {
                write!(f, "batch:{} commands", commands.len())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    InvalidConfig(ShardMapError),
    InvalidRaftGroup {
        raft_group_id: RaftGroupId,
        raft_group_count: u32,
    },
    SnapshotPlacementMismatch {
        expected: ShardPlacement,
        actual: ShardPlacement,
    },
    EmptyAppend,
    ColdStoreConfig {
        message: String,
    },
    ColdStoreIo {
        message: String,
    },
    LiveReadBackpressure {
        core_id: CoreId,
        current_waiters: u64,
        limit: u64,
    },
    GroupEngine {
        core_id: CoreId,
        raft_group_id: RaftGroupId,
        message: String,
        next_offset: Option<u64>,
        leader_hint: Option<GroupLeaderHint>,
    },
    MailboxClosed {
        core_id: CoreId,
    },
    ResponseDropped {
        core_id: CoreId,
    },
    SpawnCoreThread {
        core_id: CoreId,
        message: String,
    },
}

impl RuntimeError {
    fn group_engine(placement: ShardPlacement, err: GroupEngineError) -> Self {
        Self::GroupEngine {
            core_id: placement.core_id,
            raft_group_id: placement.raft_group_id,
            message: err.message().to_owned(),
            next_offset: err.next_offset(),
            leader_hint: err.leader_hint().cloned(),
        }
    }
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfig(err) => write!(f, "invalid shard runtime config: {err}"),
            Self::InvalidRaftGroup {
                raft_group_id,
                raft_group_count,
            } => write!(
                f,
                "raft group {} is outside configured range 0..{}",
                raft_group_id.0, raft_group_count
            ),
            Self::SnapshotPlacementMismatch { expected, actual } => write!(
                f,
                "snapshot placement for raft group {} is core {}, expected core {}",
                actual.raft_group_id.0, actual.core_id.0, expected.core_id.0
            ),
            Self::EmptyAppend => f.write_str("append payload must be non-empty"),
            Self::ColdStoreConfig { message } => {
                write!(f, "invalid cold store config: {message}")
            }
            Self::ColdStoreIo { message } => write!(f, "cold store IO error: {message}"),
            Self::LiveReadBackpressure {
                core_id,
                current_waiters,
                limit,
            } => write!(
                f,
                "core {} live read waiters at {} would exceed limit {}",
                core_id.0, current_waiters, limit
            ),
            Self::GroupEngine {
                core_id,
                raft_group_id,
                message,
                ..
            } => write!(
                f,
                "core {} raft group {} append failed: {message}",
                core_id.0, raft_group_id.0
            ),
            Self::MailboxClosed { core_id } => {
                write!(f, "core {} mailbox is closed", core_id.0)
            }
            Self::ResponseDropped { core_id } => {
                write!(f, "core {} dropped append response", core_id.0)
            }
            Self::SpawnCoreThread { core_id, message } => {
                write!(f, "failed to spawn core {} thread: {message}", core_id.0)
            }
        }
    }
}

impl std::error::Error for RuntimeError {}

impl From<ShardMapError> for RuntimeError {
    fn from(value: ShardMapError) -> Self {
        Self::InvalidConfig(value)
    }
}

fn map_fork_source_ref_error(err: RuntimeError, placement: ShardPlacement) -> RuntimeError {
    if let RuntimeError::GroupEngine { message, .. } = &err
        && message.contains("StreamGone")
    {
        return RuntimeError::group_engine(
            placement,
            GroupEngineError::stream(
                StreamErrorCode::StreamAlreadyExistsConflict,
                "source stream is gone and cannot be forked",
            ),
        );
    }
    err
}

pub type GroupAppendFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AppendResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupAppendBatchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<GroupAppendBatchResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupFlushColdFuture<'a> =
    Pin<Box<dyn Future<Output = Result<FlushColdResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupPlanColdFlushFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<ColdFlushCandidate>, GroupEngineError>> + Send + 'a>>;
pub type GroupPlanNextColdFlushFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<ColdFlushCandidate>, GroupEngineError>> + Send + 'a>>;
pub type GroupPlanNextColdFlushBatchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<ColdFlushCandidate>, GroupEngineError>> + Send + 'a>>;
pub type GroupColdHotBacklogFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ColdHotBacklog, GroupEngineError>> + Send + 'a>>;
pub type GroupCreateStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CreateStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupHeadStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<HeadStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupReadStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ReadStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupReadStreamPartsFuture<'a> =
    Pin<Box<dyn Future<Output = Result<GroupReadStreamParts, GroupEngineError>> + Send + 'a>>;
pub type GroupRequireLiveReadOwnerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), GroupEngineError>> + Send + 'a>>;
pub type GroupPublishSnapshotFuture<'a> =
    Pin<Box<dyn Future<Output = Result<PublishSnapshotResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupReadSnapshotFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ReadSnapshotResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupDeleteSnapshotFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), GroupEngineError>> + Send + 'a>>;
pub type GroupBootstrapStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<BootstrapStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupTouchStreamAccessFuture<'a> =
    Pin<Box<dyn Future<Output = Result<TouchStreamAccessResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupCloseStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CloseStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupDeleteStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<DeleteStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupForkRefFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ForkRefResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupSnapshotFuture<'a> =
    Pin<Box<dyn Future<Output = Result<GroupSnapshot, GroupEngineError>> + Send + 'a>>;
pub type GroupInstallSnapshotFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), GroupEngineError>> + Send + 'a>>;
pub type GroupWriteBatchFuture<'a> = Pin<
    Box<
        dyn Future<
                Output = Result<
                    Vec<Result<GroupWriteResponse, GroupEngineError>>,
                    GroupEngineError,
                >,
            > + Send
            + 'a,
    >,
>;
pub type GroupEngineCreateFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Box<dyn GroupEngine>, GroupEngineError>> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupAppendBatchResponse {
    pub placement: ShardPlacement,
    pub items: Vec<Result<AppendResponse, GroupEngineError>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupWriteResponse {
    CreateStream(CreateStreamResponse),
    Append(AppendResponse),
    AppendBatch(GroupAppendBatchResponse),
    PublishSnapshot(PublishSnapshotResponse),
    TouchStreamAccess(TouchStreamAccessResponse),
    AddForkRef(ForkRefResponse),
    ReleaseForkRef(ForkRefResponse),
    FlushCold(FlushColdResponse),
    CloseStream(CloseStreamResponse),
    DeleteStream(DeleteStreamResponse),
    Batch(Vec<Result<GroupWriteResponse, GroupEngineError>>),
}

pub trait GroupEngine: Send + 'static {
    fn accepts_local_writes(&self) -> bool {
        true
    }

    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a>;

    fn create_stream_external<'a>(
        &'a mut self,
        request: CreateStreamExternalRequest,
        _placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "external stream create is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a>;

    fn read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a>;

    fn read_stream_parts<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamPartsFuture<'a> {
        Box::pin(async move {
            let response = self.read_stream(request, placement).await?;
            Ok(GroupReadStreamParts::from_response(response))
        })
    }

    fn require_local_live_read_owner<'a>(
        &'a mut self,
        _placement: ShardPlacement,
    ) -> GroupRequireLiveReadOwnerFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn publish_snapshot<'a>(
        &'a mut self,
        request: PublishSnapshotRequest,
        _placement: ShardPlacement,
    ) -> GroupPublishSnapshotFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "snapshot publish is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn read_snapshot<'a>(
        &'a mut self,
        request: ReadSnapshotRequest,
        _placement: ShardPlacement,
    ) -> GroupReadSnapshotFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "snapshot read is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn delete_snapshot<'a>(
        &'a mut self,
        request: DeleteSnapshotRequest,
        _placement: ShardPlacement,
    ) -> GroupDeleteSnapshotFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "snapshot delete is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn bootstrap_stream<'a>(
        &'a mut self,
        request: BootstrapStreamRequest,
        _placement: ShardPlacement,
    ) -> GroupBootstrapStreamFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "bootstrap is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a>;

    fn add_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a>;

    fn release_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a>;

    fn close_stream<'a>(
        &'a mut self,
        request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a>;

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a>;

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a>;

    fn append_external<'a>(
        &'a mut self,
        request: AppendExternalRequest,
        _placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "external append is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn append_batch<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
    ) -> GroupAppendBatchFuture<'a>;

    fn create_stream_with_cold_admission<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        _admission: ColdWriteAdmission,
    ) -> GroupCreateStreamFuture<'a> {
        self.create_stream(request, placement)
    }

    fn append_with_cold_admission<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        _admission: ColdWriteAdmission,
    ) -> GroupAppendFuture<'a> {
        self.append(request, placement)
    }

    fn append_batch_with_cold_admission<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
        _admission: ColdWriteAdmission,
    ) -> GroupAppendBatchFuture<'a> {
        self.append_batch(request, placement)
    }

    fn append_batch_many_with_cold_admission<'a>(
        &'a mut self,
        requests: Vec<AppendBatchRequest>,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupWriteBatchFuture<'a> {
        Box::pin(async move {
            let mut responses = Vec::with_capacity(requests.len());
            for request in requests {
                let response = self
                    .append_batch_with_cold_admission(request, placement, admission)
                    .await
                    .map(GroupWriteResponse::AppendBatch);
                responses.push(response);
            }
            Ok(responses)
        })
    }

    fn flush_cold<'a>(
        &'a mut self,
        request: FlushColdRequest,
        _placement: ShardPlacement,
    ) -> GroupFlushColdFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "cold flush is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn plan_cold_flush<'a>(
        &'a mut self,
        request: PlanColdFlushRequest,
        _placement: ShardPlacement,
    ) -> GroupPlanColdFlushFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "cold flush planning is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn plan_next_cold_flush<'a>(
        &'a mut self,
        _request: PlanGroupColdFlushRequest,
        _placement: ShardPlacement,
    ) -> GroupPlanNextColdFlushFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(
                "group cold flush planning is not supported",
            ))
        })
    }

    fn plan_next_cold_flush_batch<'a>(
        &'a mut self,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
        max_candidates: usize,
    ) -> GroupPlanNextColdFlushBatchFuture<'a> {
        Box::pin(async move {
            match self.plan_next_cold_flush(request, placement).await? {
                Some(candidate) if max_candidates > 0 => Ok(vec![candidate]),
                _ => Ok(Vec::new()),
            }
        })
    }

    fn cold_hot_backlog<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        _placement: ShardPlacement,
    ) -> GroupColdHotBacklogFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "cold hot backlog is not supported for stream '{stream_id}'"
            )))
        })
    }

    fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a>;

    fn install_snapshot<'a>(
        &'a mut self,
        snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a>;

    fn write_batch<'a>(
        &'a mut self,
        commands: Vec<GroupWriteCommand>,
        placement: ShardPlacement,
    ) -> GroupWriteBatchFuture<'a> {
        Box::pin(async move {
            let mut responses = Vec::with_capacity(commands.len());
            for command in commands {
                let response = match command {
                    GroupWriteCommand::CreateStream {
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
                    } => self
                        .create_stream(
                            CreateStreamRequest {
                                stream_id,
                                content_type,
                                content_type_explicit: true,
                                initial_payload,
                                close_after,
                                stream_seq,
                                producer,
                                stream_ttl_seconds,
                                stream_expires_at_ms,
                                forked_from,
                                fork_offset,
                                now_ms,
                            },
                            placement,
                        )
                        .await
                        .map(GroupWriteResponse::CreateStream),
                    GroupWriteCommand::CreateExternal {
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
                    } => self
                        .create_stream_external(
                            CreateStreamExternalRequest {
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
                            },
                            placement,
                        )
                        .await
                        .map(GroupWriteResponse::CreateStream),
                    GroupWriteCommand::Append {
                        stream_id,
                        content_type,
                        payload,
                        close_after,
                        stream_seq,
                        producer,
                        now_ms,
                    } => self
                        .append(
                            AppendRequest {
                                stream_id,
                                content_type,
                                payload,
                                close_after,
                                stream_seq,
                                producer,
                                now_ms,
                            },
                            placement,
                        )
                        .await
                        .map(GroupWriteResponse::Append),
                    GroupWriteCommand::AppendExternal {
                        stream_id,
                        content_type,
                        payload,
                        close_after,
                        stream_seq,
                        producer,
                        now_ms,
                    } => self
                        .append_external(
                            AppendExternalRequest {
                                stream_id,
                                content_type,
                                payload,
                                close_after,
                                stream_seq,
                                producer,
                                now_ms,
                            },
                            placement,
                        )
                        .await
                        .map(GroupWriteResponse::Append),
                    GroupWriteCommand::AppendBatch {
                        stream_id,
                        content_type,
                        payloads,
                        producer,
                        now_ms,
                    } => self
                        .append_batch(
                            AppendBatchRequest {
                                stream_id,
                                content_type,
                                payloads,
                                producer,
                                now_ms,
                            },
                            placement,
                        )
                        .await
                        .map(GroupWriteResponse::AppendBatch),
                    GroupWriteCommand::PublishSnapshot {
                        stream_id,
                        snapshot_offset,
                        content_type,
                        payload,
                        now_ms,
                    } => self
                        .publish_snapshot(
                            PublishSnapshotRequest {
                                stream_id,
                                snapshot_offset,
                                content_type,
                                payload,
                                now_ms,
                            },
                            placement,
                        )
                        .await
                        .map(GroupWriteResponse::PublishSnapshot),
                    GroupWriteCommand::TouchStreamAccess {
                        stream_id,
                        now_ms,
                        renew_ttl,
                    } => self
                        .touch_stream_access(stream_id, now_ms, renew_ttl, placement)
                        .await
                        .map(GroupWriteResponse::TouchStreamAccess),
                    GroupWriteCommand::AddForkRef { stream_id, now_ms } => self
                        .add_fork_ref(stream_id, now_ms, placement)
                        .await
                        .map(GroupWriteResponse::AddForkRef),
                    GroupWriteCommand::ReleaseForkRef { stream_id } => self
                        .release_fork_ref(stream_id, placement)
                        .await
                        .map(GroupWriteResponse::ReleaseForkRef),
                    GroupWriteCommand::FlushCold { stream_id, chunk } => self
                        .flush_cold(FlushColdRequest { stream_id, chunk }, placement)
                        .await
                        .map(GroupWriteResponse::FlushCold),
                    GroupWriteCommand::CloseStream {
                        stream_id,
                        stream_seq,
                        producer,
                        now_ms,
                    } => self
                        .close_stream(
                            CloseStreamRequest {
                                stream_id,
                                stream_seq,
                                producer,
                                now_ms,
                            },
                            placement,
                        )
                        .await
                        .map(GroupWriteResponse::CloseStream),
                    GroupWriteCommand::DeleteStream { stream_id } => self
                        .delete_stream(DeleteStreamRequest { stream_id }, placement)
                        .await
                        .map(GroupWriteResponse::DeleteStream),
                    GroupWriteCommand::Batch { commands } => self
                        .write_batch(commands, placement)
                        .await
                        .map(GroupWriteResponse::Batch),
                };
                responses.push(response);
            }
            Ok(responses)
        })
    }
}

pub trait GroupEngineFactory: Send + Sync + 'static {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a>;
}

#[derive(Debug, Clone)]
pub struct GroupEngineMetrics {
    inner: Arc<RuntimeMetricsInner>,
}

impl GroupEngineMetrics {
    pub fn record_wal_batch(
        &self,
        placement: ShardPlacement,
        record_count: usize,
        write_ns: u64,
        sync_ns: u64,
    ) {
        self.inner.record_wal_batch(
            placement.core_id,
            placement.raft_group_id,
            u64::try_from(record_count).expect("record count fits u64"),
            write_ns,
            sync_ns,
        );
    }

    pub fn record_raft_write_many(
        &self,
        placement: ShardPlacement,
        command_count: usize,
        logical_command_count: usize,
        response_count: usize,
        submit_ns: u64,
        response_ns: u64,
    ) {
        self.inner.record_raft_write_many(
            placement.core_id,
            placement.raft_group_id,
            RaftWriteManySample {
                command_count: u64::try_from(command_count).expect("command count fits u64"),
                logical_command_count: u64::try_from(logical_command_count)
                    .expect("logical command count fits u64"),
                response_count: u64::try_from(response_count).expect("response count fits u64"),
                submit_ns,
                response_ns,
            },
        );
    }

    pub fn record_raft_apply_batch(
        &self,
        placement: ShardPlacement,
        entry_count: usize,
        apply_ns: u64,
    ) {
        self.inner.record_raft_apply_batch(
            placement.core_id,
            placement.raft_group_id,
            u64::try_from(entry_count).expect("entry count fits u64"),
            apply_ns,
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupLeaderHint {
    pub node_id: Option<u64>,
    pub address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupEngineError {
    message: String,
    code: Option<StreamErrorCode>,
    next_offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    leader_hint: Option<GroupLeaderHint>,
}

impl GroupEngineError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: None,
            next_offset: None,
            leader_hint: None,
        }
    }

    pub fn stream(code: StreamErrorCode, message: impl Into<String>) -> Self {
        Self::stream_with_next_offset(code, message, None)
    }

    pub fn stream_with_next_offset(
        code: StreamErrorCode,
        message: impl Into<String>,
        next_offset: Option<u64>,
    ) -> Self {
        Self {
            message: format!("{code:?}: {}", message.into()),
            code: Some(code),
            next_offset,
            leader_hint: None,
        }
    }

    pub fn forward_to_leader(
        message: impl Into<String>,
        node_id: Option<u64>,
        address: Option<String>,
    ) -> Self {
        Self {
            message: message.into(),
            code: None,
            next_offset: None,
            leader_hint: Some(GroupLeaderHint { node_id, address }),
        }
    }

    pub fn from_replicated_parts(
        message: impl Into<String>,
        code: Option<StreamErrorCode>,
        next_offset: Option<u64>,
        leader_hint: Option<GroupLeaderHint>,
    ) -> Self {
        Self {
            message: message.into(),
            code,
            next_offset,
            leader_hint,
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn code(&self) -> Option<StreamErrorCode> {
        self.code
    }

    pub fn next_offset(&self) -> Option<u64> {
        self.next_offset
    }

    pub fn leader_hint(&self) -> Option<&GroupLeaderHint> {
        self.leader_hint.as_ref()
    }
}

impl std::fmt::Display for GroupEngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for GroupEngineError {}

struct AppendPayloadInput<'a> {
    stream_id: BucketStreamId,
    content_type: Option<&'a str>,
    payload: &'a [u8],
    close_after: bool,
    stream_seq: Option<String>,
    producer: Option<ProducerRequest>,
    now_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryGroupEngine {
    commit_index: u64,
    state_machine: StreamStateMachine,
    stream_append_counts: HashMap<BucketStreamId, u64>,
    cold_store: Option<ColdStoreHandle>,
}

impl InMemoryGroupEngine {
    pub fn with_cold_store(cold_store: ColdStoreHandle) -> Self {
        Self {
            cold_store: Some(cold_store),
            ..Self::default()
        }
    }

    pub fn cold_store(&self) -> Option<ColdStoreHandle> {
        self.cold_store.clone()
    }

    pub fn apply_committed_write(
        &mut self,
        command: GroupWriteCommand,
        placement: ShardPlacement,
    ) -> Result<GroupWriteResponse, GroupEngineError> {
        match command {
            GroupWriteCommand::CreateStream {
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
            } => {
                ensure_bucket_exists(&mut self.state_machine, &stream_id)?;
                let response = self.state_machine.apply(StreamCommand::CreateStream {
                    stream_id,
                    content_type,
                    initial_payload: initial_payload.to_vec(),
                    close_after,
                    stream_seq,
                    producer,
                    stream_ttl_seconds,
                    stream_expires_at_ms,
                    forked_from,
                    fork_offset,
                    now_ms,
                });
                match response {
                    StreamResponse::Created {
                        next_offset,
                        closed,
                        ..
                    } => {
                        self.commit_index += 1;
                        Ok(GroupWriteResponse::CreateStream(CreateStreamResponse {
                            placement,
                            next_offset,
                            closed,
                            already_exists: false,
                            group_commit_index: self.commit_index,
                        }))
                    }
                    StreamResponse::AlreadyExists {
                        next_offset,
                        closed,
                        ..
                    } => Ok(GroupWriteResponse::CreateStream(CreateStreamResponse {
                        placement,
                        next_offset,
                        closed,
                        already_exists: true,
                        group_commit_index: self.commit_index,
                    })),
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected create stream response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::CreateExternal {
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
            } => {
                ensure_bucket_exists(&mut self.state_machine, &stream_id)?;
                let response = self.state_machine.apply(StreamCommand::CreateExternal {
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
                });
                match response {
                    StreamResponse::Created {
                        next_offset,
                        closed,
                        ..
                    } => {
                        self.commit_index += 1;
                        Ok(GroupWriteResponse::CreateStream(CreateStreamResponse {
                            placement,
                            next_offset,
                            closed,
                            already_exists: false,
                            group_commit_index: self.commit_index,
                        }))
                    }
                    StreamResponse::AlreadyExists {
                        next_offset,
                        closed,
                        ..
                    } => Ok(GroupWriteResponse::CreateStream(CreateStreamResponse {
                        placement,
                        next_offset,
                        closed,
                        already_exists: true,
                        group_commit_index: self.commit_index,
                    })),
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected create external stream response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::Append {
                stream_id,
                content_type,
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => self
                .append_payload(
                    AppendPayloadInput {
                        stream_id,
                        content_type: Some(&content_type),
                        payload: &payload,
                        close_after,
                        stream_seq,
                        producer,
                        now_ms,
                    },
                    placement,
                )
                .map(GroupWriteResponse::Append),
            GroupWriteCommand::AppendExternal {
                stream_id,
                content_type,
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => {
                let response = self.state_machine.apply(StreamCommand::AppendExternal {
                    stream_id: stream_id.clone(),
                    content_type: Some(content_type),
                    payload,
                    close_after,
                    stream_seq,
                    producer,
                    now_ms,
                });
                match response {
                    StreamResponse::Appended {
                        offset,
                        next_offset,
                        closed,
                        deduplicated,
                        producer,
                        ..
                    } => {
                        let stream_append_count =
                            self.stream_append_counts.entry(stream_id).or_insert(0);
                        if !deduplicated {
                            self.commit_index += 1;
                            *stream_append_count += 1;
                        }
                        Ok(GroupWriteResponse::Append(AppendResponse {
                            placement,
                            start_offset: offset,
                            next_offset,
                            stream_append_count: *stream_append_count,
                            group_commit_index: self.commit_index,
                            closed,
                            deduplicated,
                            producer,
                        }))
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected append external response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::AppendBatch {
                stream_id,
                content_type,
                payloads,
                producer,
                now_ms,
            } => {
                if producer.is_some() {
                    let payload_refs = payloads.iter().map(Bytes::as_ref).collect::<Vec<_>>();
                    let batch = self
                        .state_machine
                        .append_batch_borrowed(
                            stream_id.clone(),
                            Some(&content_type),
                            &payload_refs,
                            producer,
                            now_ms,
                        )
                        .map_err(stream_response_error)?;
                    let old_commit_index = self.commit_index;
                    let old_append_count = *self.stream_append_counts.get(&stream_id).unwrap_or(&0);
                    if !batch.deduplicated {
                        let count = u64::try_from(batch.items.len()).expect("item count fits u64");
                        self.commit_index += count;
                        *self.stream_append_counts.entry(stream_id).or_insert(0) += count;
                    }
                    let items = batch
                        .items
                        .into_iter()
                        .enumerate()
                        .map(|(index, item)| {
                            let item_index = u64::try_from(index + 1).expect("item index fits u64");
                            Ok(AppendResponse {
                                placement,
                                start_offset: item.offset,
                                next_offset: item.next_offset,
                                stream_append_count: if item.deduplicated {
                                    old_append_count
                                } else {
                                    old_append_count + item_index
                                },
                                group_commit_index: if item.deduplicated {
                                    old_commit_index
                                } else {
                                    old_commit_index + item_index
                                },
                                closed: item.closed,
                                deduplicated: item.deduplicated,
                                producer: None,
                            })
                        })
                        .collect();
                    return Ok(GroupWriteResponse::AppendBatch(GroupAppendBatchResponse {
                        placement,
                        items,
                    }));
                }

                let mut items = Vec::with_capacity(payloads.len());
                for payload in payloads {
                    if payload.is_empty() {
                        items.push(Err(GroupEngineError::stream(
                            StreamErrorCode::EmptyAppend,
                            "append payload must be non-empty",
                        )));
                        continue;
                    }
                    items.push(self.append_payload(
                        AppendPayloadInput {
                            stream_id: stream_id.clone(),
                            content_type: Some(&content_type),
                            payload: &payload,
                            close_after: false,
                            stream_seq: None,
                            producer: None,
                            now_ms,
                        },
                        placement,
                    ));
                }
                Ok(GroupWriteResponse::AppendBatch(GroupAppendBatchResponse {
                    placement,
                    items,
                }))
            }
            GroupWriteCommand::PublishSnapshot {
                stream_id,
                snapshot_offset,
                content_type,
                payload,
                now_ms,
            } => {
                let response = self.state_machine.apply(StreamCommand::PublishSnapshot {
                    stream_id,
                    snapshot_offset,
                    content_type,
                    payload: payload.to_vec(),
                    now_ms,
                });
                match response {
                    StreamResponse::SnapshotPublished { snapshot_offset } => {
                        self.commit_index += 1;
                        Ok(GroupWriteResponse::PublishSnapshot(
                            PublishSnapshotResponse {
                                placement,
                                snapshot_offset,
                                group_commit_index: self.commit_index,
                            },
                        ))
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected publish snapshot response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::TouchStreamAccess {
                stream_id,
                now_ms,
                renew_ttl,
            } => {
                let response = self.state_machine.apply(StreamCommand::TouchStreamAccess {
                    stream_id,
                    now_ms,
                    renew_ttl,
                });
                match response {
                    StreamResponse::Accessed { changed, expired } => {
                        if changed || expired {
                            self.commit_index += 1;
                        }
                        Ok(GroupWriteResponse::TouchStreamAccess(
                            TouchStreamAccessResponse {
                                placement,
                                changed,
                                expired,
                                group_commit_index: self.commit_index,
                            },
                        ))
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected touch stream access response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::AddForkRef { stream_id, now_ms } => {
                let response = self
                    .state_machine
                    .apply(StreamCommand::AddForkRef { stream_id, now_ms });
                match response {
                    StreamResponse::ForkRefAdded { fork_ref_count } => {
                        self.commit_index += 1;
                        Ok(GroupWriteResponse::AddForkRef(ForkRefResponse {
                            placement,
                            fork_ref_count,
                            hard_deleted: false,
                            parent_to_release: None,
                            group_commit_index: self.commit_index,
                        }))
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected add fork ref response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::ReleaseForkRef { stream_id } => {
                let response = self
                    .state_machine
                    .apply(StreamCommand::ReleaseForkRef { stream_id });
                match response {
                    StreamResponse::ForkRefReleased {
                        hard_deleted,
                        fork_ref_count,
                        parent_to_release,
                    } => {
                        self.commit_index += 1;
                        Ok(GroupWriteResponse::ReleaseForkRef(ForkRefResponse {
                            placement,
                            fork_ref_count,
                            hard_deleted,
                            parent_to_release,
                            group_commit_index: self.commit_index,
                        }))
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected release fork ref response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::FlushCold { stream_id, chunk } => {
                let response = self
                    .state_machine
                    .apply(StreamCommand::FlushCold { stream_id, chunk });
                match response {
                    StreamResponse::ColdFlushed { hot_start_offset } => {
                        self.commit_index += 1;
                        Ok(GroupWriteResponse::FlushCold(FlushColdResponse {
                            placement,
                            hot_start_offset,
                            group_commit_index: self.commit_index,
                        }))
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected flush cold response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::CloseStream {
                stream_id,
                stream_seq,
                producer,
                now_ms,
            } => {
                let response = self.state_machine.apply(StreamCommand::Close {
                    stream_id,
                    stream_seq,
                    producer,
                    now_ms,
                });
                match response {
                    StreamResponse::Closed {
                        next_offset,
                        deduplicated,
                        ..
                    } => {
                        if !deduplicated {
                            self.commit_index += 1;
                        }
                        Ok(GroupWriteResponse::CloseStream(CloseStreamResponse {
                            placement,
                            next_offset,
                            group_commit_index: self.commit_index,
                            deduplicated,
                        }))
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected close stream response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::DeleteStream { stream_id } => {
                let response = self
                    .state_machine
                    .apply(StreamCommand::DeleteStream { stream_id });
                match response {
                    StreamResponse::Deleted {
                        hard_deleted,
                        parent_to_release,
                    } => {
                        self.commit_index += 1;
                        Ok(GroupWriteResponse::DeleteStream(DeleteStreamResponse {
                            placement,
                            group_commit_index: self.commit_index,
                            hard_deleted,
                            parent_to_release,
                        }))
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected delete stream response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::Batch { commands } => Ok(GroupWriteResponse::Batch(
                self.apply_committed_write_batch(commands, placement),
            )),
        }
    }

    fn cold_hot_backlog_for(
        &self,
        stream_id: BucketStreamId,
    ) -> Result<ColdHotBacklog, GroupEngineError> {
        let stream_hot_bytes = self.state_machine.hot_payload_len(&stream_id).unwrap_or(0);
        Ok(ColdHotBacklog {
            stream_id,
            stream_hot_bytes,
            group_hot_bytes: self.state_machine.total_hot_payload_bytes(),
        })
    }

    fn enforce_cold_write_admission(
        &self,
        stream_id: &BucketStreamId,
        admission: ColdWriteAdmission,
        before_group_hot_bytes: u64,
        after_group_hot_bytes: u64,
        mutating: bool,
    ) -> Result<(), GroupEngineError> {
        let Some(limit) = admission.max_hot_bytes_per_group else {
            return Ok(());
        };
        if !mutating || after_group_hot_bytes <= limit {
            return Ok(());
        }
        Err(GroupEngineError::new(format!(
            "ColdBackpressure: stream '{stream_id}' would raise group hot bytes from {before_group_hot_bytes} to {after_group_hot_bytes}, above limit {limit}"
        )))
    }

    fn create_stream_with_admission_inner(
        &mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<CreateStreamResponse, GroupEngineError> {
        let stream_id = request.stream_id.clone();
        let command = GroupWriteCommand::from(request);
        let before = self.state_machine.total_hot_payload_bytes();
        let mut preview = self.clone();
        let response = match preview.apply_committed_write(command, placement)? {
            GroupWriteResponse::CreateStream(response) => response,
            other => {
                return Err(GroupEngineError::new(format!(
                    "unexpected create stream write response: {other:?}"
                )));
            }
        };
        preview.enforce_cold_write_admission(
            &stream_id,
            admission,
            before,
            preview.state_machine.total_hot_payload_bytes(),
            !response.already_exists,
        )?;
        *self = preview;
        Ok(response)
    }

    fn append_with_admission_inner(
        &mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<AppendResponse, GroupEngineError> {
        let stream_id = request.stream_id.clone();
        let command = GroupWriteCommand::from(request);
        let before = self.state_machine.total_hot_payload_bytes();
        let mut preview = self.clone();
        let response = match preview.apply_committed_write(command, placement)? {
            GroupWriteResponse::Append(response) => response,
            other => {
                return Err(GroupEngineError::new(format!(
                    "unexpected append write response: {other:?}"
                )));
            }
        };
        preview.enforce_cold_write_admission(
            &stream_id,
            admission,
            before,
            preview.state_machine.total_hot_payload_bytes(),
            !response.deduplicated,
        )?;
        *self = preview;
        Ok(response)
    }

    fn append_batch_with_admission_inner(
        &mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<GroupAppendBatchResponse, GroupEngineError> {
        let stream_id = request.stream_id.clone();
        let command = GroupWriteCommand::from(request);
        let before = self.state_machine.total_hot_payload_bytes();
        let mut preview = self.clone();
        let response = match preview.apply_committed_write(command, placement)? {
            GroupWriteResponse::AppendBatch(response) => response,
            other => {
                return Err(GroupEngineError::new(format!(
                    "unexpected append batch write response: {other:?}"
                )));
            }
        };
        let mutating = response
            .items
            .iter()
            .any(|item| matches!(item, Ok(response) if !response.deduplicated));
        preview.enforce_cold_write_admission(
            &stream_id,
            admission,
            before,
            preview.state_machine.total_hot_payload_bytes(),
            mutating,
        )?;
        *self = preview;
        Ok(response)
    }

    pub fn access_requires_write(
        &self,
        stream_id: &BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
    ) -> Result<bool, GroupEngineError> {
        self.state_machine
            .access_requires_write(stream_id, now_ms, renew_ttl)
            .map_err(stream_response_error)
    }

    fn apply_access_command(
        &mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> Result<TouchStreamAccessResponse, GroupEngineError> {
        match self.apply_committed_write(
            GroupWriteCommand::TouchStreamAccess {
                stream_id,
                now_ms,
                renew_ttl,
            },
            placement,
        )? {
            GroupWriteResponse::TouchStreamAccess(response) => Ok(response),
            other => Err(GroupEngineError::new(format!(
                "unexpected touch stream access write response: {other:?}"
            ))),
        }
    }

    fn ensure_stream_access(
        &mut self,
        stream_id: &BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> Result<Option<TouchStreamAccessResponse>, GroupEngineError> {
        if !self.access_requires_write(stream_id, now_ms, renew_ttl)? {
            return Ok(None);
        }
        let response =
            self.apply_access_command(stream_id.clone(), now_ms, renew_ttl, placement)?;
        if response.expired {
            return Err(GroupEngineError::stream(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        }
        Ok(Some(response))
    }

    pub fn apply_committed_write_batch(
        &mut self,
        commands: Vec<GroupWriteCommand>,
        placement: ShardPlacement,
    ) -> Vec<Result<GroupWriteResponse, GroupEngineError>> {
        commands
            .into_iter()
            .map(|command| self.apply_committed_write(command, placement))
            .collect()
    }

    fn apply_replayed_write_command(
        &mut self,
        command: GroupWriteCommand,
    ) -> Result<(), GroupEngineError> {
        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(0),
        };
        self.apply_committed_write(command, placement).map(|_| ())
    }

    fn apply_replayed_command(&mut self, command: StreamCommand) -> Result<(), GroupEngineError> {
        match command {
            StreamCommand::CreateBucket { bucket_id } => {
                match self
                    .state_machine
                    .apply(StreamCommand::CreateBucket { bucket_id })
                {
                    StreamResponse::BucketCreated { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    StreamResponse::BucketAlreadyExists { .. } => Ok(()),
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay create bucket response: {other:?}"
                    ))),
                }
            }
            StreamCommand::DeleteBucket { bucket_id } => {
                match self
                    .state_machine
                    .apply(StreamCommand::DeleteBucket { bucket_id })
                {
                    StreamResponse::BucketDeleted { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay delete bucket response: {other:?}"
                    ))),
                }
            }
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
            } => {
                ensure_bucket_exists(&mut self.state_machine, &stream_id)?;
                let response = self.state_machine.apply(StreamCommand::CreateStream {
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
                });
                match response {
                    StreamResponse::Created { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    StreamResponse::AlreadyExists { .. } => Ok(()),
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay create stream response: {other:?}"
                    ))),
                }
            }
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
            } => {
                ensure_bucket_exists(&mut self.state_machine, &stream_id)?;
                let response = self.state_machine.apply(StreamCommand::CreateExternal {
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
                });
                match response {
                    StreamResponse::Created { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    StreamResponse::AlreadyExists { .. } => Ok(()),
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay external create stream response: {other:?}"
                    ))),
                }
            }
            StreamCommand::Append {
                stream_id,
                content_type,
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => {
                let stream_count_key = stream_id.clone();
                let response = self.state_machine.apply(StreamCommand::Append {
                    stream_id,
                    content_type,
                    payload,
                    close_after,
                    stream_seq,
                    producer,
                    now_ms,
                });
                match response {
                    StreamResponse::Appended { deduplicated, .. } => {
                        if !deduplicated {
                            self.commit_index += 1;
                            *self
                                .stream_append_counts
                                .entry(stream_count_key)
                                .or_insert(0) += 1;
                        }
                        Ok(())
                    }
                    StreamResponse::Closed { deduplicated, .. } => {
                        if !deduplicated {
                            self.commit_index += 1;
                        }
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay append response: {other:?}"
                    ))),
                }
            }
            StreamCommand::AppendExternal {
                stream_id,
                content_type,
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => {
                let stream_count_key = stream_id.clone();
                let response = self.state_machine.apply(StreamCommand::AppendExternal {
                    stream_id,
                    content_type,
                    payload,
                    close_after,
                    stream_seq,
                    producer,
                    now_ms,
                });
                match response {
                    StreamResponse::Appended { deduplicated, .. } => {
                        if !deduplicated {
                            self.commit_index += 1;
                            *self
                                .stream_append_counts
                                .entry(stream_count_key)
                                .or_insert(0) += 1;
                        }
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay external append response: {other:?}"
                    ))),
                }
            }
            StreamCommand::AppendBatch {
                stream_id,
                content_type,
                payloads,
                producer,
                now_ms,
            } => {
                let stream_count_key = stream_id.clone();
                let payload_refs = payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();
                let response = self
                    .state_machine
                    .append_batch_borrowed(
                        stream_id,
                        content_type.as_deref(),
                        &payload_refs,
                        producer,
                        now_ms,
                    )
                    .map_err(stream_response_error)?;
                if !response.deduplicated {
                    let count = u64::try_from(response.items.len()).expect("item count fits u64");
                    self.commit_index += count;
                    *self
                        .stream_append_counts
                        .entry(stream_count_key)
                        .or_insert(0) += count;
                }
                Ok(())
            }
            StreamCommand::PublishSnapshot {
                stream_id,
                snapshot_offset,
                content_type,
                payload,
                now_ms,
            } => {
                let response = self.state_machine.apply(StreamCommand::PublishSnapshot {
                    stream_id,
                    snapshot_offset,
                    content_type,
                    payload,
                    now_ms,
                });
                match response {
                    StreamResponse::SnapshotPublished { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay publish snapshot response: {other:?}"
                    ))),
                }
            }
            StreamCommand::TouchStreamAccess {
                stream_id,
                now_ms,
                renew_ttl,
            } => {
                let response = self.state_machine.apply(StreamCommand::TouchStreamAccess {
                    stream_id,
                    now_ms,
                    renew_ttl,
                });
                match response {
                    StreamResponse::Accessed { changed, expired } => {
                        if changed || expired {
                            self.commit_index += 1;
                        }
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay touch stream access response: {other:?}"
                    ))),
                }
            }
            StreamCommand::AddForkRef { stream_id, now_ms } => {
                let response = self
                    .state_machine
                    .apply(StreamCommand::AddForkRef { stream_id, now_ms });
                match response {
                    StreamResponse::ForkRefAdded { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay add fork ref response: {other:?}"
                    ))),
                }
            }
            StreamCommand::ReleaseForkRef { stream_id } => {
                let response = self
                    .state_machine
                    .apply(StreamCommand::ReleaseForkRef { stream_id });
                match response {
                    StreamResponse::ForkRefReleased { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay release fork ref response: {other:?}"
                    ))),
                }
            }
            StreamCommand::FlushCold { stream_id, chunk } => {
                let response = self
                    .state_machine
                    .apply(StreamCommand::FlushCold { stream_id, chunk });
                match response {
                    StreamResponse::ColdFlushed { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay flush cold response: {other:?}"
                    ))),
                }
            }
            StreamCommand::Close {
                stream_id,
                stream_seq,
                producer,
                now_ms,
            } => {
                let response = self.state_machine.apply(StreamCommand::Close {
                    stream_id,
                    stream_seq,
                    producer,
                    now_ms,
                });
                match response {
                    StreamResponse::Closed { deduplicated, .. } => {
                        if !deduplicated {
                            self.commit_index += 1;
                        }
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay close stream response: {other:?}"
                    ))),
                }
            }
            StreamCommand::DeleteStream { stream_id } => {
                let response = self
                    .state_machine
                    .apply(StreamCommand::DeleteStream { stream_id });
                match response {
                    StreamResponse::Deleted { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay delete stream response: {other:?}"
                    ))),
                }
            }
        }
    }

    fn append_payload(
        &mut self,
        input: AppendPayloadInput<'_>,
        placement: ShardPlacement,
    ) -> Result<AppendResponse, GroupEngineError> {
        let AppendPayloadInput {
            stream_id,
            content_type,
            payload,
            close_after,
            stream_seq,
            producer,
            now_ms,
        } = input;
        let stream_count_key = stream_id.clone();
        let response = self.state_machine.append_borrowed(AppendStreamInput {
            stream_id,
            content_type,
            payload,
            close_after,
            stream_seq,
            producer,
            now_ms,
        });
        match response {
            StreamResponse::Appended {
                offset,
                next_offset,
                closed,
                deduplicated,
                producer,
                ..
            } => {
                let stream_append_count = self
                    .stream_append_counts
                    .entry(stream_count_key)
                    .or_insert(0);
                if !deduplicated {
                    self.commit_index += 1;
                    *stream_append_count += 1;
                }
                Ok(AppendResponse {
                    placement,
                    start_offset: offset,
                    next_offset,
                    stream_append_count: *stream_append_count,
                    group_commit_index: self.commit_index,
                    closed,
                    deduplicated,
                    producer,
                })
            }
            StreamResponse::Error {
                code,
                message,
                next_offset,
            } => Err(GroupEngineError::stream_with_next_offset(
                code,
                message,
                next_offset,
            )),
            other => Err(GroupEngineError::new(format!(
                "unexpected append response: {other:?}"
            ))),
        }
    }

    pub fn read_stream_plan(
        &mut self,
        request: &ReadStreamRequest,
        placement: ShardPlacement,
    ) -> Result<StreamReadPlan, GroupEngineError> {
        self.ensure_stream_access(&request.stream_id, request.now_ms, true, placement)?;
        self.read_stream_plan_after_access(request)
    }

    pub fn read_stream_plan_after_access(
        &self,
        request: &ReadStreamRequest,
    ) -> Result<StreamReadPlan, GroupEngineError> {
        self.state_machine
            .read_plan_at(
                &request.stream_id,
                request.offset,
                request.max_len,
                request.now_ms,
            )
            .map_err(stream_response_error)
    }

    pub async fn read_payload_from_plan(
        cold_store: Option<&ColdStoreHandle>,
        stream_id: &BucketStreamId,
        plan: &StreamReadPlan,
    ) -> Result<Vec<u8>, GroupEngineError> {
        let mut payload = Vec::new();
        for segment in &plan.segments {
            match segment {
                StreamReadSegment::Hot(bytes) => payload.extend_from_slice(bytes),
                StreamReadSegment::Object(segment) => {
                    let Some(cold_store) = cold_store else {
                        return Err(GroupEngineError::stream_with_next_offset(
                            StreamErrorCode::InvalidColdFlush,
                            format!("stream '{stream_id}' read requires object payload store"),
                            Some(plan.next_offset),
                        ));
                    };
                    let bytes = cold_store
                        .read_object_range(&segment.object, segment.read_start_offset, segment.len)
                        .await
                        .map_err(|err| GroupEngineError::new(err.to_string()))?;
                    payload.extend_from_slice(&bytes);
                }
            }
        }
        Ok(payload)
    }

    async fn read_own_payload_from_plan(
        &self,
        stream_id: &BucketStreamId,
        plan: &StreamReadPlan,
    ) -> Result<Vec<u8>, GroupEngineError> {
        Self::read_payload_from_plan(self.cold_store.as_ref(), stream_id, plan).await
    }

    async fn bootstrap_updates(
        &self,
        stream_id: &BucketStreamId,
        records: &[StreamMessageRecord],
        content_type: &str,
        now_ms: u64,
    ) -> Result<Vec<BootstrapUpdate>, GroupEngineError> {
        let mut updates = Vec::with_capacity(records.len());
        for record in records {
            let len = usize::try_from(record.end_offset - record.start_offset).map_err(|_| {
                GroupEngineError::stream(
                    StreamErrorCode::InvalidSnapshot,
                    format!(
                        "bootstrap message [{}..{}) for stream '{stream_id}' is too large",
                        record.start_offset, record.end_offset
                    ),
                )
            })?;
            let plan = self
                .state_machine
                .read_plan_at(stream_id, record.start_offset, len, now_ms)
                .map_err(stream_response_error)?;
            let payload = self.read_own_payload_from_plan(stream_id, &plan).await?;
            updates.push(BootstrapUpdate {
                start_offset: record.start_offset,
                next_offset: record.end_offset,
                content_type: content_type.to_owned(),
                payload,
            });
        }
        Ok(updates)
    }

    fn build_snapshot(&self, placement: ShardPlacement) -> GroupSnapshot {
        GroupSnapshot {
            placement,
            group_commit_index: self.commit_index,
            stream_snapshot: self.state_machine.snapshot(),
            stream_append_counts: self.stream_append_counts_snapshot(),
        }
    }

    fn stream_append_counts_snapshot(&self) -> Vec<StreamAppendCount> {
        let mut counts = self
            .stream_append_counts
            .iter()
            .map(|(stream_id, append_count)| StreamAppendCount {
                stream_id: stream_id.clone(),
                append_count: *append_count,
            })
            .collect::<Vec<_>>();
        counts.sort_by(|left, right| compare_stream_ids(&left.stream_id, &right.stream_id));
        counts
    }

    fn install_snapshot_inner(&mut self, snapshot: GroupSnapshot) -> Result<(), GroupEngineError> {
        let GroupSnapshot {
            placement: _,
            group_commit_index,
            stream_snapshot,
            stream_append_counts,
        } = snapshot;
        self.install_snapshot_parts(group_commit_index, stream_snapshot, stream_append_counts)
    }

    fn install_snapshot_parts(
        &mut self,
        group_commit_index: u64,
        stream_snapshot: StreamSnapshot,
        stream_append_counts: Vec<StreamAppendCount>,
    ) -> Result<(), GroupEngineError> {
        let stream_ids = stream_snapshot
            .streams
            .iter()
            .map(|entry| entry.metadata.stream_id.clone())
            .collect::<HashSet<_>>();
        let state_machine = StreamStateMachine::restore(stream_snapshot)
            .map_err(|err| GroupEngineError::new(format!("restore stream snapshot: {err}")))?;
        let stream_append_counts = restore_stream_append_counts(stream_append_counts, &stream_ids)?;

        self.commit_index = group_commit_index;
        self.state_machine = state_machine;
        self.stream_append_counts = stream_append_counts;
        Ok(())
    }
}

impl GroupEngine for InMemoryGroupEngine {
    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        let command = GroupWriteCommand::from(request);
        Box::pin(async move {
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::CreateStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected create stream write response: {other:?}"
                ))),
            }
        })
    }

    fn create_stream_with_cold_admission<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupCreateStreamFuture<'a> {
        if !admission.is_enabled() {
            return self.create_stream(request, placement);
        }
        Box::pin(
            async move { self.create_stream_with_admission_inner(request, placement, admission) },
        )
    }

    fn create_stream_external<'a>(
        &'a mut self,
        request: CreateStreamExternalRequest,
        placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        let command = GroupWriteCommand::from(request);
        Box::pin(async move {
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::CreateStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected external create stream write response: {other:?}"
                ))),
            }
        })
    }

    fn read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a> {
        Box::pin(async move {
            self.read_stream_parts(request, placement)
                .await?
                .into_response()
                .await
        })
    }

    fn read_stream_parts<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamPartsFuture<'a> {
        Box::pin(async move {
            let stream_id = request.stream_id.clone();
            let plan = self.read_stream_plan(&request, placement)?;
            Ok(GroupReadStreamParts::from_plan(
                placement,
                stream_id,
                plan,
                self.cold_store(),
            ))
        })
    }

    fn publish_snapshot<'a>(
        &'a mut self,
        request: PublishSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupPublishSnapshotFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request);
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::PublishSnapshot(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected publish snapshot write response: {other:?}"
                ))),
            }
        })
    }

    fn read_snapshot<'a>(
        &'a mut self,
        request: ReadSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupReadSnapshotFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, true, placement)?;
            let snapshot = match request.snapshot_offset {
                Some(offset) => self
                    .state_machine
                    .read_snapshot(&request.stream_id, offset)
                    .map_err(stream_response_error)?,
                None => self
                    .state_machine
                    .latest_snapshot(&request.stream_id)
                    .map_err(stream_response_error)?
                    .ok_or_else(|| {
                        GroupEngineError::stream(
                            StreamErrorCode::SnapshotNotFound,
                            format!("stream '{}' has no visible snapshot", request.stream_id),
                        )
                    })?,
            };
            let tail_offset = self
                .state_machine
                .head_at(&request.stream_id, request.now_ms)
                .map(|metadata| metadata.tail_offset)
                .unwrap_or(snapshot.offset);
            Ok(ReadSnapshotResponse {
                placement,
                snapshot_offset: snapshot.offset,
                next_offset: snapshot.offset,
                content_type: snapshot.content_type,
                payload: snapshot.payload,
                up_to_date: snapshot.offset == tail_offset,
            })
        })
    }

    fn delete_snapshot<'a>(
        &'a mut self,
        request: DeleteSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteSnapshotFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            match self
                .state_machine
                .delete_snapshot(&request.stream_id, request.snapshot_offset)
            {
                StreamResponse::Error {
                    code,
                    message,
                    next_offset,
                } => Err(GroupEngineError::stream_with_next_offset(
                    code,
                    message,
                    next_offset,
                )),
                other => Err(GroupEngineError::new(format!(
                    "unexpected delete snapshot response: {other:?}"
                ))),
            }
        })
    }

    fn bootstrap_stream<'a>(
        &'a mut self,
        request: BootstrapStreamRequest,
        placement: ShardPlacement,
    ) -> GroupBootstrapStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, true, placement)?;
            let plan = self
                .state_machine
                .bootstrap_plan(&request.stream_id)
                .map_err(stream_response_error)?;
            let snapshot_offset = plan.snapshot.as_ref().map(|snapshot| snapshot.offset);
            let snapshot_content_type = plan
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.content_type.clone())
                .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_owned());
            let snapshot_payload = plan
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.payload.clone())
                .unwrap_or_default();
            let updates = self
                .bootstrap_updates(
                    &request.stream_id,
                    &plan.updates,
                    &plan.content_type,
                    request.now_ms,
                )
                .await?;
            Ok(BootstrapStreamResponse {
                placement,
                snapshot_offset,
                snapshot_content_type,
                snapshot_payload,
                updates,
                next_offset: plan.next_offset,
                up_to_date: plan.up_to_date,
                closed: plan.closed,
            })
        })
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a> {
        Box::pin(async move { self.apply_access_command(stream_id, now_ms, renew_ttl, placement) })
    }

    fn add_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        Box::pin(async move {
            match self.apply_committed_write(
                GroupWriteCommand::AddForkRef { stream_id, now_ms },
                placement,
            )? {
                GroupWriteResponse::AddForkRef(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected add fork ref write response: {other:?}"
                ))),
            }
        })
    }

    fn release_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        Box::pin(async move {
            match self
                .apply_committed_write(GroupWriteCommand::ReleaseForkRef { stream_id }, placement)?
            {
                GroupWriteResponse::ReleaseForkRef(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected release fork ref write response: {other:?}"
                ))),
            }
        })
    }

    fn head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            let Some(metadata) = self
                .state_machine
                .head_at(&request.stream_id, request.now_ms)
            else {
                return Err(GroupEngineError::stream(
                    StreamErrorCode::StreamNotFound,
                    format!("stream '{}' does not exist", request.stream_id),
                ));
            };
            Ok(HeadStreamResponse {
                placement,
                content_type: metadata.content_type.clone(),
                tail_offset: metadata.tail_offset,
                closed: metadata.status == ursula_stream::StreamStatus::Closed,
                stream_ttl_seconds: metadata.stream_ttl_seconds,
                stream_expires_at_ms: metadata.stream_expires_at_ms,
                snapshot_offset: self
                    .state_machine
                    .latest_snapshot(&request.stream_id)
                    .map_err(stream_response_error)?
                    .map(|snapshot| snapshot.offset),
            })
        })
    }

    fn close_stream<'a>(
        &'a mut self,
        request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request);
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::CloseStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected close stream write response: {other:?}"
                ))),
            }
        })
    }

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a> {
        let command = GroupWriteCommand::from(request);
        Box::pin(async move {
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::DeleteStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected delete stream write response: {other:?}"
                ))),
            }
        })
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request);
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::Append(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected append write response: {other:?}"
                ))),
            }
        })
    }

    fn append_with_cold_admission<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendFuture<'a> {
        if !admission.is_enabled() {
            return self.append(request, placement);
        }
        Box::pin(async move { self.append_with_admission_inner(request, placement, admission) })
    }

    fn append_external<'a>(
        &'a mut self,
        request: AppendExternalRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request);
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::Append(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected external append write response: {other:?}"
                ))),
            }
        })
    }

    fn append_batch<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
    ) -> GroupAppendBatchFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request);
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::AppendBatch(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected append batch write response: {other:?}"
                ))),
            }
        })
    }

    fn append_batch_with_cold_admission<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendBatchFuture<'a> {
        if !admission.is_enabled() {
            return self.append_batch(request, placement);
        }
        Box::pin(
            async move { self.append_batch_with_admission_inner(request, placement, admission) },
        )
    }

    fn flush_cold<'a>(
        &'a mut self,
        request: FlushColdRequest,
        placement: ShardPlacement,
    ) -> GroupFlushColdFuture<'a> {
        let command = GroupWriteCommand::from(request);
        Box::pin(async move {
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::FlushCold(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected flush cold write response: {other:?}"
                ))),
            }
        })
    }

    fn plan_cold_flush<'a>(
        &'a mut self,
        request: PlanColdFlushRequest,
        _placement: ShardPlacement,
    ) -> GroupPlanColdFlushFuture<'a> {
        Box::pin(async move {
            self.state_machine
                .plan_cold_flush(
                    &request.stream_id,
                    request.min_hot_bytes,
                    request.max_flush_bytes,
                )
                .map_err(stream_response_error)
        })
    }

    fn plan_next_cold_flush<'a>(
        &'a mut self,
        request: PlanGroupColdFlushRequest,
        _placement: ShardPlacement,
    ) -> GroupPlanNextColdFlushFuture<'a> {
        Box::pin(async move {
            self.state_machine
                .plan_next_cold_flush(request.min_hot_bytes, request.max_flush_bytes)
                .map_err(stream_response_error)
        })
    }

    fn plan_next_cold_flush_batch<'a>(
        &'a mut self,
        request: PlanGroupColdFlushRequest,
        _placement: ShardPlacement,
        max_candidates: usize,
    ) -> GroupPlanNextColdFlushBatchFuture<'a> {
        Box::pin(async move {
            self.state_machine
                .plan_next_cold_flush_batch(
                    request.min_hot_bytes,
                    request.max_flush_bytes,
                    max_candidates,
                )
                .map_err(stream_response_error)
        })
    }

    fn cold_hot_backlog<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        _placement: ShardPlacement,
    ) -> GroupColdHotBacklogFuture<'a> {
        Box::pin(async move { self.cold_hot_backlog_for(stream_id) })
    }

    fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
        Box::pin(async move { Ok(self.build_snapshot(placement)) })
    }

    fn install_snapshot<'a>(
        &'a mut self,
        snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a> {
        Box::pin(async move { self.install_snapshot_inner(snapshot) })
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryGroupEngineFactory {
    cold_store: Option<ColdStoreHandle>,
}

impl InMemoryGroupEngineFactory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_cold_store(cold_store: Option<ColdStoreHandle>) -> Self {
        Self { cold_store }
    }
}

impl GroupEngineFactory for InMemoryGroupEngineFactory {
    fn create<'a>(
        &'a self,
        _placement: ShardPlacement,
        _metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let engine = InMemoryGroupEngine {
                cold_store: self.cold_store.clone(),
                ..InMemoryGroupEngine::default()
            };
            let engine: Box<dyn GroupEngine> = Box::new(engine);
            Ok(engine)
        })
    }
}

#[derive(Debug, Clone)]
pub struct WalGroupEngineFactory {
    root: PathBuf,
    cold_store: Option<ColdStoreHandle>,
}

impl WalGroupEngineFactory {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            cold_store: None,
        }
    }

    pub fn with_cold_store(root: impl Into<PathBuf>, cold_store: Option<ColdStoreHandle>) -> Self {
        Self {
            root: root.into(),
            cold_store,
        }
    }
}

impl GroupEngineFactory for WalGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let engine: Box<dyn GroupEngine> = Box::new(WalGroupEngine::open(
                &self.root,
                placement,
                metrics,
                self.cold_store.clone(),
            ));
            Ok(engine)
        })
    }
}

pub struct WalGroupEngine {
    inner: InMemoryGroupEngine,
    log_path: PathBuf,
    placement: ShardPlacement,
    metrics: GroupEngineMetrics,
    init_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "wal_record", rename_all = "snake_case")]
enum WalRecord {
    Command {
        command: Box<GroupWriteCommand>,
    },
    Snapshot {
        group_commit_index: u64,
        stream_snapshot: StreamSnapshot,
        stream_append_counts: Vec<StreamAppendCount>,
    },
}

impl WalGroupEngine {
    fn open(
        root: &Path,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
        cold_store: Option<ColdStoreHandle>,
    ) -> Self {
        let log_path = group_log_path(root, placement);
        match replay_group_log(&log_path) {
            Ok(mut inner) => {
                inner.cold_store = cold_store;
                Self {
                    inner,
                    log_path,
                    placement,
                    metrics,
                    init_error: None,
                }
            }
            Err(err) => Self {
                inner: InMemoryGroupEngine {
                    cold_store,
                    ..InMemoryGroupEngine::default()
                },
                log_path,
                placement,
                metrics,
                init_error: Some(err.message().to_owned()),
            },
        }
    }

    fn ensure_ready(&self) -> Result<(), GroupEngineError> {
        match &self.init_error {
            Some(message) => Err(GroupEngineError::new(message.clone())),
            None => Ok(()),
        }
    }

    fn append_record(&self, command: &GroupWriteCommand) -> Result<(), GroupEngineError> {
        self.append_records(std::slice::from_ref(command))
    }

    fn append_records(&self, commands: &[GroupWriteCommand]) -> Result<(), GroupEngineError> {
        if commands.is_empty() {
            return Ok(());
        }
        let Some(parent) = self.log_path.parent() else {
            return Err(GroupEngineError::new(format!(
                "WAL path '{}' has no parent directory",
                self.log_path.display()
            )));
        };
        fs::create_dir_all(parent).map_err(|err| {
            GroupEngineError::new(format!("create WAL dir '{}': {err}", parent.display()))
        })?;
        let write_started_at = Instant::now();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
            .map_err(|err| {
                GroupEngineError::new(format!("open WAL '{}': {err}", self.log_path.display()))
            })?;
        for command in commands {
            let record = WalRecord::Command {
                command: Box::new(command.clone()),
            };
            serde_json::to_writer(&mut file, &record).map_err(|err| {
                GroupEngineError::new(format!("encode WAL '{}': {err}", self.log_path.display()))
            })?;
            file.write_all(b"\n").map_err(|err| {
                GroupEngineError::new(format!("write WAL '{}': {err}", self.log_path.display()))
            })?;
        }
        let write_ns = elapsed_ns(write_started_at);
        let sync_started_at = Instant::now();
        file.sync_data().map_err(|err| {
            GroupEngineError::new(format!("sync WAL '{}': {err}", self.log_path.display()))
        })?;
        self.metrics.record_wal_batch(
            self.placement,
            commands.len(),
            write_ns,
            elapsed_ns(sync_started_at),
        );
        Ok(())
    }

    fn append_snapshot_record(&self, snapshot: &GroupSnapshot) -> Result<(), GroupEngineError> {
        let record = WalRecord::Snapshot {
            group_commit_index: snapshot.group_commit_index,
            stream_snapshot: snapshot.stream_snapshot.clone(),
            stream_append_counts: snapshot.stream_append_counts.clone(),
        };
        let Some(parent) = self.log_path.parent() else {
            return Err(GroupEngineError::new(format!(
                "WAL path '{}' has no parent directory",
                self.log_path.display()
            )));
        };
        fs::create_dir_all(parent).map_err(|err| {
            GroupEngineError::new(format!("create WAL dir '{}': {err}", parent.display()))
        })?;
        let write_started_at = Instant::now();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
            .map_err(|err| {
                GroupEngineError::new(format!("open WAL '{}': {err}", self.log_path.display()))
            })?;
        serde_json::to_writer(&mut file, &record).map_err(|err| {
            GroupEngineError::new(format!("encode WAL '{}': {err}", self.log_path.display()))
        })?;
        file.write_all(b"\n").map_err(|err| {
            GroupEngineError::new(format!("write WAL '{}': {err}", self.log_path.display()))
        })?;
        let write_ns = elapsed_ns(write_started_at);
        let sync_started_at = Instant::now();
        file.sync_data().map_err(|err| {
            GroupEngineError::new(format!("sync WAL '{}': {err}", self.log_path.display()))
        })?;
        self.metrics
            .record_wal_batch(self.placement, 1, write_ns, elapsed_ns(sync_started_at));
        Ok(())
    }

    fn commit_access_if_needed(
        &mut self,
        stream_id: &BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> Result<Option<TouchStreamAccessResponse>, GroupEngineError> {
        if !self
            .inner
            .access_requires_write(stream_id, now_ms, renew_ttl)?
        {
            return Ok(None);
        }
        let command = GroupWriteCommand::TouchStreamAccess {
            stream_id: stream_id.clone(),
            now_ms,
            renew_ttl,
        };
        let mut preview = self.inner.clone();
        let response = match preview.apply_committed_write(command.clone(), placement)? {
            GroupWriteResponse::TouchStreamAccess(response) => response,
            other => {
                return Err(GroupEngineError::new(format!(
                    "unexpected touch stream access write response: {other:?}"
                )));
            }
        };
        if response.changed || response.expired {
            self.append_record(&command)?;
        }
        self.inner = preview;
        if response.expired {
            return Err(GroupEngineError::stream(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        }
        Ok(Some(response))
    }
}

impl GroupEngine for WalGroupEngine {
    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            let command = GroupWriteCommand::from(request);
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::CreateStream(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected create stream write response: {other:?}"
                    )));
                }
            };
            if !response.already_exists {
                self.append_record(&command)?;
            }
            self.inner = preview;
            Ok(response)
        })
    }

    fn create_stream_with_cold_admission<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupCreateStreamFuture<'a> {
        if !admission.is_enabled() {
            return self.create_stream(request, placement);
        }
        Box::pin(async move {
            self.ensure_ready()?;
            let command = GroupWriteCommand::from(request.clone());
            let mut preview = self.inner.clone();
            let response =
                preview.create_stream_with_admission_inner(request, placement, admission)?;
            if !response.already_exists {
                self.append_record(&command)?;
            }
            self.inner = preview;
            Ok(response)
        })
    }

    fn head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, false, placement)?;
            self.inner.head_stream(request, placement).await
        })
    }

    fn read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, true, placement)?;
            self.inner.read_stream(request, placement).await
        })
    }

    fn publish_snapshot<'a>(
        &'a mut self,
        request: PublishSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupPublishSnapshotFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request);
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::PublishSnapshot(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected publish snapshot write response: {other:?}"
                    )));
                }
            };
            self.append_record(&command)?;
            self.inner = preview;
            Ok(response)
        })
    }

    fn read_snapshot<'a>(
        &'a mut self,
        request: ReadSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupReadSnapshotFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, true, placement)?;
            self.inner.read_snapshot(request, placement).await
        })
    }

    fn delete_snapshot<'a>(
        &'a mut self,
        request: DeleteSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteSnapshotFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, false, placement)?;
            self.inner.delete_snapshot(request, placement).await
        })
    }

    fn bootstrap_stream<'a>(
        &'a mut self,
        request: BootstrapStreamRequest,
        placement: ShardPlacement,
    ) -> GroupBootstrapStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, true, placement)?;
            self.inner.bootstrap_stream(request, placement).await
        })
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            let command = GroupWriteCommand::TouchStreamAccess {
                stream_id,
                now_ms,
                renew_ttl,
            };
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::TouchStreamAccess(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected touch stream access write response: {other:?}"
                    )));
                }
            };
            if response.changed || response.expired {
                self.append_record(&command)?;
            }
            self.inner = preview;
            Ok(response)
        })
    }

    fn add_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            let command = GroupWriteCommand::AddForkRef { stream_id, now_ms };
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::AddForkRef(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected add fork ref write response: {other:?}"
                    )));
                }
            };
            self.append_record(&command)?;
            self.inner = preview;
            Ok(response)
        })
    }

    fn release_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            let command = GroupWriteCommand::ReleaseForkRef { stream_id };
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::ReleaseForkRef(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected release fork ref write response: {other:?}"
                    )));
                }
            };
            self.append_record(&command)?;
            self.inner = preview;
            Ok(response)
        })
    }

    fn close_stream<'a>(
        &'a mut self,
        request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request);
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::CloseStream(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected close stream write response: {other:?}"
                    )));
                }
            };
            self.append_record(&command)?;
            self.inner = preview;
            Ok(response)
        })
    }

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            let command = GroupWriteCommand::from(request);
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::DeleteStream(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected delete stream write response: {other:?}"
                    )));
                }
            };
            self.append_record(&command)?;
            self.inner = preview;
            Ok(response)
        })
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request);
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::Append(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected append write response: {other:?}"
                    )));
                }
            };
            self.append_record(&command)?;
            self.inner = preview;
            Ok(response)
        })
    }

    fn append_with_cold_admission<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendFuture<'a> {
        if !admission.is_enabled() {
            return self.append(request, placement);
        }
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request.clone());
            let mut preview = self.inner.clone();
            let response = preview.append_with_admission_inner(request, placement, admission)?;
            if !response.deduplicated {
                self.append_record(&command)?;
            }
            self.inner = preview;
            Ok(response)
        })
    }

    fn append_batch<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
    ) -> GroupAppendBatchFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request);
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::AppendBatch(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected append batch write response: {other:?}"
                    )));
                }
            };
            if response
                .items
                .iter()
                .any(|item| matches!(item, Ok(response) if !response.deduplicated))
            {
                self.append_record(&command)?;
            }
            self.inner = preview;
            Ok(response)
        })
    }

    fn append_batch_with_cold_admission<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendBatchFuture<'a> {
        if !admission.is_enabled() {
            return self.append_batch(request, placement);
        }
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request.clone());
            let mut preview = self.inner.clone();
            let response =
                preview.append_batch_with_admission_inner(request, placement, admission)?;
            if response
                .items
                .iter()
                .any(|item| matches!(item, Ok(response) if !response.deduplicated))
            {
                self.append_record(&command)?;
            }
            self.inner = preview;
            Ok(response)
        })
    }

    fn flush_cold<'a>(
        &'a mut self,
        request: FlushColdRequest,
        placement: ShardPlacement,
    ) -> GroupFlushColdFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            let command = GroupWriteCommand::from(request);
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::FlushCold(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected flush cold write response: {other:?}"
                    )));
                }
            };
            self.append_record(&command)?;
            self.inner = preview;
            Ok(response)
        })
    }

    fn plan_cold_flush<'a>(
        &'a mut self,
        request: PlanColdFlushRequest,
        placement: ShardPlacement,
    ) -> GroupPlanColdFlushFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.inner.plan_cold_flush(request, placement).await
        })
    }

    fn plan_next_cold_flush<'a>(
        &'a mut self,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
    ) -> GroupPlanNextColdFlushFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.inner.plan_next_cold_flush(request, placement).await
        })
    }

    fn plan_next_cold_flush_batch<'a>(
        &'a mut self,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
        max_candidates: usize,
    ) -> GroupPlanNextColdFlushBatchFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.inner
                .plan_next_cold_flush_batch(request, placement, max_candidates)
                .await
        })
    }

    fn cold_hot_backlog<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> GroupColdHotBacklogFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.inner.cold_hot_backlog(stream_id, placement).await
        })
    }

    fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.inner.snapshot(placement).await
        })
    }

    fn install_snapshot<'a>(
        &'a mut self,
        snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            let mut preview = self.inner.clone();
            preview.install_snapshot(snapshot.clone()).await?;
            self.append_snapshot_record(&snapshot)?;
            self.inner = preview;
            Ok(())
        })
    }
}

fn group_log_path(root: &Path, placement: ShardPlacement) -> PathBuf {
    root.join(format!("core-{}", placement.core_id.0))
        .join(format!("group-{}.jsonl", placement.raft_group_id.0))
}

fn replay_group_log(log_path: &Path) -> Result<InMemoryGroupEngine, GroupEngineError> {
    if !log_path.exists() {
        return Ok(InMemoryGroupEngine::default());
    }

    let file = File::open(log_path).map_err(|err| {
        GroupEngineError::new(format!("open WAL '{}': {err}", log_path.display()))
    })?;
    let reader = BufReader::new(file);
    let mut inner = InMemoryGroupEngine::default();
    for (line_index, line) in reader.lines().enumerate() {
        let line = line.map_err(|err| {
            GroupEngineError::new(format!(
                "read WAL '{}' line {}: {err}",
                log_path.display(),
                line_index + 1
            ))
        })?;
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(record) = serde_json::from_str::<WalRecord>(&line) {
            match record {
                WalRecord::Command { command } => inner
                    .apply_replayed_write_command(*command)
                    .map_err(|err| {
                        GroupEngineError::new(format!(
                            "replay WAL command '{}' line {}: {err}",
                            log_path.display(),
                            line_index + 1
                        ))
                    })?,
                WalRecord::Snapshot {
                    group_commit_index,
                    stream_snapshot,
                    stream_append_counts,
                } => inner
                    .install_snapshot_parts(
                        group_commit_index,
                        stream_snapshot,
                        stream_append_counts,
                    )
                    .map_err(|err| {
                        GroupEngineError::new(format!(
                            "replay WAL snapshot '{}' line {}: {err}",
                            log_path.display(),
                            line_index + 1
                        ))
                    })?,
            }
            continue;
        }

        let command = serde_json::from_str::<StreamCommand>(&line).map_err(|err| {
            GroupEngineError::new(format!(
                "decode WAL '{}' line {}: {err}",
                log_path.display(),
                line_index + 1
            ))
        })?;
        inner.apply_replayed_command(command).map_err(|err| {
            GroupEngineError::new(format!(
                "replay WAL '{}' line {}: {err}",
                log_path.display(),
                line_index + 1
            ))
        })?;
    }
    Ok(inner)
}

fn ensure_bucket_exists(
    state_machine: &mut StreamStateMachine,
    stream_id: &BucketStreamId,
) -> Result<(), GroupEngineError> {
    if state_machine.bucket_exists(&stream_id.bucket_id) {
        return Ok(());
    }

    match state_machine.apply(StreamCommand::CreateBucket {
        bucket_id: stream_id.bucket_id.clone(),
    }) {
        StreamResponse::BucketCreated { .. } | StreamResponse::BucketAlreadyExists { .. } => Ok(()),
        StreamResponse::Error {
            code,
            message,
            next_offset,
        } => Err(GroupEngineError::stream_with_next_offset(
            code,
            message,
            next_offset,
        )),
        other => Err(GroupEngineError::new(format!(
            "unexpected create bucket response: {other:?}"
        ))),
    }
}

fn stream_response_error(response: StreamResponse) -> GroupEngineError {
    match response {
        StreamResponse::Error {
            code,
            message,
            next_offset,
        } => GroupEngineError::stream_with_next_offset(code, message, next_offset),
        other => GroupEngineError::new(format!("unexpected stream response error: {other:?}")),
    }
}

fn restore_stream_append_counts(
    counts: Vec<StreamAppendCount>,
    snapshot_stream_ids: &HashSet<BucketStreamId>,
) -> Result<HashMap<BucketStreamId, u64>, GroupEngineError> {
    let mut restored = HashMap::with_capacity(counts.len());
    for count in counts {
        if !snapshot_stream_ids.contains(&count.stream_id) {
            return Err(GroupEngineError::new(format!(
                "append count references missing snapshot stream '{}'",
                count.stream_id
            )));
        }
        if restored
            .insert(count.stream_id.clone(), count.append_count)
            .is_some()
        {
            return Err(GroupEngineError::new(format!(
                "snapshot contains duplicate append count for stream '{}'",
                count.stream_id
            )));
        }
    }
    Ok(restored)
}

fn compare_stream_ids(left: &BucketStreamId, right: &BucketStreamId) -> std::cmp::Ordering {
    left.bucket_id
        .cmp(&right.bucket_id)
        .then_with(|| left.stream_id.cmp(&right.stream_id))
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub core_count: usize,
    pub raft_group_count: usize,
    pub mailbox_capacity: usize,
    pub threading: RuntimeThreading,
    pub cold_max_hot_bytes_per_group: Option<u64>,
    pub live_read_max_waiters_per_core: Option<u64>,
}

impl RuntimeConfig {
    pub fn new(core_count: usize, raft_group_count: usize) -> Self {
        Self {
            core_count,
            raft_group_count,
            mailbox_capacity: 1024,
            threading: RuntimeThreading::ThreadPerCore,
            cold_max_hot_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        }
    }

    pub fn with_cold_max_hot_bytes_per_group(mut self, value: Option<u64>) -> Self {
        self.cold_max_hot_bytes_per_group = value;
        self
    }

    pub fn with_live_read_max_waiters_per_core(mut self, value: Option<u64>) -> Self {
        self.live_read_max_waiters_per_core = value;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeThreading {
    ThreadPerCore,
    HostedTokio,
}

#[derive(Debug, Clone)]
pub struct ShardRuntime {
    shard_map: StaticShardMap,
    mailboxes: Vec<CoreMailbox>,
    metrics: Arc<RuntimeMetricsInner>,
    next_waiter_id: Arc<AtomicU64>,
    cold_store: Option<ColdStoreHandle>,
}

impl ShardRuntime {
    pub fn spawn(config: RuntimeConfig) -> Result<Self, RuntimeError> {
        Self::spawn_with_engine_factory(config, InMemoryGroupEngineFactory::default())
    }

    pub fn spawn_with_engine_factory(
        config: RuntimeConfig,
        engine_factory: impl GroupEngineFactory,
    ) -> Result<Self, RuntimeError> {
        Self::spawn_with_engine_factory_and_cold_store(config, engine_factory, None)
    }

    pub fn spawn_with_engine_factory_and_cold_store(
        config: RuntimeConfig,
        engine_factory: impl GroupEngineFactory,
        cold_store: Option<ColdStoreHandle>,
    ) -> Result<Self, RuntimeError> {
        let shard_map = StaticShardMap::new(config.core_count, config.raft_group_count)?;
        let metrics = Arc::new(RuntimeMetricsInner::new(
            usize::from(shard_map.core_count()),
            usize::try_from(shard_map.raft_group_count()).expect("u32 fits usize"),
        ));
        let cold_write_admission = ColdWriteAdmission {
            max_hot_bytes_per_group: config.cold_max_hot_bytes_per_group,
        };
        let engine_factory: Arc<dyn GroupEngineFactory> = Arc::new(engine_factory);
        let read_materialization = Arc::new(Semaphore::new(config.mailbox_capacity.max(1)));
        let mut mailboxes = Vec::with_capacity(usize::from(shard_map.core_count()));
        for raw_core_id in 0..shard_map.core_count() {
            let core_id = CoreId(raw_core_id);
            let (tx, rx) = mpsc::channel(config.mailbox_capacity.max(1));
            let worker = CoreWorker {
                core_id,
                rx,
                engine_factory: engine_factory.clone(),
                groups: HashMap::new(),
                metrics: metrics.clone(),
                group_mailbox_capacity: config.mailbox_capacity.max(1),
                cold_write_admission,
                live_read_max_waiters_per_core: config.live_read_max_waiters_per_core,
                read_materialization: read_materialization.clone(),
            };
            spawn_core_worker(config.threading, worker)?;
            mailboxes.push(CoreMailbox { core_id, tx });
        }
        Ok(Self {
            shard_map,
            mailboxes,
            metrics,
            next_waiter_id: Arc::new(AtomicU64::new(1)),
            cold_store,
        })
    }

    pub fn locate(&self, stream_id: &BucketStreamId) -> ShardPlacement {
        self.shard_map.locate(stream_id)
    }

    pub fn has_cold_store(&self) -> bool {
        self.cold_store.is_some()
    }

    pub fn cold_store(&self) -> Option<ColdStoreHandle> {
        self.cold_store.clone()
    }

    pub async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> Result<CreateStreamResponse, RuntimeError> {
        if request.forked_from.is_some() {
            return self.create_fork_stream(request).await;
        }
        self.create_stream_on_owner(request).await
    }

    pub async fn create_stream_external(
        &self,
        request: CreateStreamExternalRequest,
    ) -> Result<CreateStreamResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::CreateExternal {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    async fn create_stream_on_owner(
        &self,
        request: CreateStreamRequest,
    ) -> Result<CreateStreamResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::CreateStream {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    async fn create_fork_stream(
        &self,
        mut request: CreateStreamRequest,
    ) -> Result<CreateStreamResponse, RuntimeError> {
        let source_id = request
            .forked_from
            .clone()
            .expect("forked_from checked before create_fork_stream");
        let now_ms = request.now_ms;
        let source_placement = self.shard_map.locate(&source_id);
        let source_head = self
            .head_stream(HeadStreamRequest {
                stream_id: source_id.clone(),
                now_ms,
            })
            .await
            .map_err(|err| map_fork_source_ref_error(err, source_placement))?;

        if request.content_type_explicit {
            if request.content_type != source_head.content_type {
                return Err(RuntimeError::group_engine(
                    source_placement,
                    GroupEngineError::stream(
                        StreamErrorCode::ContentTypeMismatch,
                        format!(
                            "fork content type '{}' does not match source content type '{}'",
                            request.content_type, source_head.content_type
                        ),
                    ),
                ));
            }
        } else {
            request.content_type.clone_from(&source_head.content_type);
        }

        let fork_offset = request.fork_offset.unwrap_or(source_head.tail_offset);
        if fork_offset > source_head.tail_offset {
            return Err(RuntimeError::group_engine(
                source_placement,
                GroupEngineError::stream(
                    StreamErrorCode::InvalidFork,
                    format!(
                        "fork offset {fork_offset} is beyond source stream '{}' tail {}",
                        source_id, source_head.tail_offset
                    ),
                ),
            ));
        }

        let max_len = usize::try_from(fork_offset).map_err(|_| {
            RuntimeError::group_engine(
                source_placement,
                GroupEngineError::stream(
                    StreamErrorCode::InvalidFork,
                    format!("fork offset {fork_offset} cannot fit in memory on this host"),
                ),
            )
        })?;
        request.initial_payload = if fork_offset == 0 {
            Bytes::new()
        } else {
            self.read_stream(ReadStreamRequest {
                stream_id: source_id.clone(),
                offset: 0,
                max_len,
                now_ms,
            })
            .await?
            .payload
            .into()
        };
        self.add_fork_ref_on_owner(source_id.clone(), now_ms)
            .await
            .map_err(|err| map_fork_source_ref_error(err, source_placement))?;
        request.close_after = false;
        request.stream_seq = None;
        request.producer = None;
        if request.stream_ttl_seconds.is_none() && request.stream_expires_at_ms.is_none() {
            request.stream_ttl_seconds = source_head.stream_ttl_seconds;
            request.stream_expires_at_ms = source_head.stream_expires_at_ms;
        }
        request.fork_offset = Some(fork_offset);
        match self.create_stream_on_owner(request).await {
            Ok(response) if response.already_exists => {
                self.release_fork_ref_cascade(source_id).await?;
                Ok(response)
            }
            Ok(response) => Ok(response),
            Err(err) => {
                let _ = self.release_fork_ref_cascade(source_id).await;
                Err(err)
            }
        }
    }

    pub async fn head_stream(
        &self,
        request: HeadStreamRequest,
    ) -> Result<HeadStreamResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::HeadStream {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn read_stream(
        &self,
        request: ReadStreamRequest,
    ) -> Result<ReadStreamResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::ReadStream {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn publish_snapshot(
        &self,
        request: PublishSnapshotRequest,
    ) -> Result<PublishSnapshotResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::PublishSnapshot {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn read_snapshot(
        &self,
        request: ReadSnapshotRequest,
    ) -> Result<ReadSnapshotResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::ReadSnapshot {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn delete_snapshot(
        &self,
        request: DeleteSnapshotRequest,
    ) -> Result<(), RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::DeleteSnapshot {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn bootstrap_stream(
        &self,
        request: BootstrapStreamRequest,
    ) -> Result<BootstrapStreamResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::BootstrapStream {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn wait_read_stream(
        &self,
        request: ReadStreamRequest,
    ) -> Result<ReadStreamResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let waiter_id = self.next_waiter_id.fetch_add(1, Ordering::Relaxed);
        let stream_id = request.stream_id.clone();
        let (response_tx, response_rx) = oneshot::channel();
        self.enqueue_core_command(
            mailbox,
            CoreCommand::WaitRead {
                request,
                placement,
                waiter_id,
                response_tx,
            },
        )
        .await?;
        let mut cancel = WaitReadCancel::new(mailbox.tx.clone(), stream_id, placement, waiter_id);
        let response = response_rx
            .await
            .map_err(|_| RuntimeError::ResponseDropped {
                core_id: mailbox.core_id,
            })?;
        cancel.disarm();
        response
    }

    pub async fn require_local_live_read_owner(
        &self,
        stream_id: &BucketStreamId,
    ) -> Result<(), RuntimeError> {
        let placement = self.shard_map.locate(stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::RequireLiveReadOwner {
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> Result<CloseStreamResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::CloseStream {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn delete_stream(
        &self,
        request: DeleteStreamRequest,
    ) -> Result<DeleteStreamResponse, RuntimeError> {
        let response = self.delete_stream_on_owner(request).await?;
        if let Some(parent_to_release) = response.parent_to_release.clone() {
            self.release_fork_ref_cascade(parent_to_release).await?;
        }
        Ok(response)
    }

    async fn delete_stream_on_owner(
        &self,
        request: DeleteStreamRequest,
    ) -> Result<DeleteStreamResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::DeleteStream {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    async fn add_fork_ref_on_owner(
        &self,
        stream_id: BucketStreamId,
        now_ms: u64,
    ) -> Result<ForkRefResponse, RuntimeError> {
        let placement = self.shard_map.locate(&stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::AddForkRef {
                stream_id,
                now_ms,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    async fn release_fork_ref_on_owner(
        &self,
        stream_id: BucketStreamId,
    ) -> Result<ForkRefResponse, RuntimeError> {
        let placement = self.shard_map.locate(&stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::ReleaseForkRef {
                stream_id,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    async fn release_fork_ref_cascade(
        &self,
        stream_id: BucketStreamId,
    ) -> Result<(), RuntimeError> {
        let mut next = Some(stream_id);
        while let Some(current) = next {
            let response = self.release_fork_ref_on_owner(current).await?;
            next = response.parent_to_release;
        }
        Ok(())
    }

    pub async fn flush_cold(
        &self,
        request: FlushColdRequest,
    ) -> Result<FlushColdResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::FlushCold {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn append_external(
        &self,
        request: AppendExternalRequest,
    ) -> Result<AppendResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::AppendExternal {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn plan_cold_flush(
        &self,
        request: PlanColdFlushRequest,
    ) -> Result<Option<ColdFlushCandidate>, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::PlanColdFlush {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn flush_cold_once(
        &self,
        request: PlanColdFlushRequest,
    ) -> Result<Option<FlushColdResponse>, RuntimeError> {
        let Some(candidate) = self.plan_cold_flush(request).await? else {
            return Ok(None);
        };
        self.flush_cold_candidate(candidate).await.map(Some)
    }

    pub async fn plan_next_cold_flush(
        &self,
        raft_group_id: RaftGroupId,
        request: PlanGroupColdFlushRequest,
    ) -> Result<Option<ColdFlushCandidate>, RuntimeError> {
        let placement = self.placement_for_group(raft_group_id)?;
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::PlanNextColdFlush {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn plan_next_cold_flush_batch(
        &self,
        raft_group_id: RaftGroupId,
        request: PlanGroupColdFlushRequest,
        max_candidates: usize,
    ) -> Result<Vec<ColdFlushCandidate>, RuntimeError> {
        let placement = self.placement_for_group(raft_group_id)?;
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::PlanNextColdFlushBatch {
                request,
                placement,
                max_candidates,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn flush_cold_group_once(
        &self,
        raft_group_id: RaftGroupId,
        request: PlanGroupColdFlushRequest,
    ) -> Result<Option<FlushColdResponse>, RuntimeError> {
        let Some(candidate) = self.plan_next_cold_flush(raft_group_id, request).await? else {
            return Ok(None);
        };
        match self.flush_cold_candidate(candidate).await {
            Ok(response) => Ok(Some(response)),
            Err(err) if is_stale_cold_flush_candidate_error(&err) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn flush_cold_group_batch_once(
        &self,
        raft_group_id: RaftGroupId,
        request: PlanGroupColdFlushRequest,
        max_candidates: usize,
    ) -> Result<Vec<FlushColdResponse>, RuntimeError> {
        let candidates = self
            .plan_next_cold_flush_batch(raft_group_id, request, max_candidates)
            .await?;
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        match self.flush_cold_candidates_batch(candidates).await {
            Ok(responses) => Ok(responses),
            Err(err) if is_stale_cold_flush_candidate_error(&err) => Ok(Vec::new()),
            Err(err) => Err(err),
        }
    }

    async fn flush_cold_candidate(
        &self,
        candidate: ColdFlushCandidate,
    ) -> Result<FlushColdResponse, RuntimeError> {
        let Some(cold_store) = self.cold_store.as_ref() else {
            return Err(RuntimeError::ColdStoreConfig {
                message: "URSULA_COLD_BACKEND must be configured before flushing cold chunks"
                    .to_owned(),
            });
        };
        let path = new_cold_chunk_path(
            &candidate.stream_id,
            candidate.start_offset,
            candidate.end_offset,
        );
        let upload_started_at = Instant::now();
        let object_size = cold_store
            .write_chunk(&path, &candidate.payload)
            .await
            .map_err(|err| RuntimeError::ColdStoreIo {
                message: err.to_string(),
            })?;
        self.metrics
            .record_cold_upload(object_size, elapsed_ns(upload_started_at));
        let publish_started_at = Instant::now();
        let publish = self
            .flush_cold(FlushColdRequest {
                stream_id: candidate.stream_id,
                chunk: ColdChunkRef {
                    start_offset: candidate.start_offset,
                    end_offset: candidate.end_offset,
                    s3_path: path.clone(),
                    object_size,
                },
            })
            .await;
        match publish {
            Ok(response) => {
                self.metrics
                    .record_cold_publish(object_size, elapsed_ns(publish_started_at));
                Ok(response)
            }
            Err(err) => {
                let cleanup_failed = cold_store.delete_chunk(&path).await.is_err();
                self.metrics
                    .record_cold_orphan_cleanup(object_size, cleanup_failed);
                Err(err)
            }
        }
    }

    async fn flush_cold_candidates_batch(
        &self,
        candidates: Vec<ColdFlushCandidate>,
    ) -> Result<Vec<FlushColdResponse>, RuntimeError> {
        let Some(cold_store) = self.cold_store.as_ref() else {
            return Err(RuntimeError::ColdStoreConfig {
                message: "URSULA_COLD_BACKEND must be configured before flushing cold chunks"
                    .to_owned(),
            });
        };
        let mut requests = Vec::with_capacity(candidates.len());
        let mut uploaded = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let path = new_cold_chunk_path(
                &candidate.stream_id,
                candidate.start_offset,
                candidate.end_offset,
            );
            let upload_started_at = Instant::now();
            let object_size = cold_store
                .write_chunk(&path, &candidate.payload)
                .await
                .map_err(|err| RuntimeError::ColdStoreIo {
                    message: err.to_string(),
                })?;
            self.metrics
                .record_cold_upload(object_size, elapsed_ns(upload_started_at));
            uploaded.push((path.clone(), object_size));
            requests.push(FlushColdRequest {
                stream_id: candidate.stream_id,
                chunk: ColdChunkRef {
                    start_offset: candidate.start_offset,
                    end_offset: candidate.end_offset,
                    s3_path: path,
                    object_size,
                },
            });
        }

        let placement = self.shard_map.locate(&requests[0].stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        let publish_started_at = Instant::now();
        let publish = self
            .send_core_command(
                mailbox,
                CoreCommand::FlushColdBatch {
                    requests,
                    placement,
                    response_tx,
                },
                response_rx,
            )
            .await;
        match publish {
            Ok(responses) => {
                let publish_ns = elapsed_ns(publish_started_at);
                let per_chunk_publish_ns =
                    publish_ns / u64::try_from(uploaded.len()).expect("uploaded len fits u64");
                for (_, object_size) in &uploaded {
                    self.metrics
                        .record_cold_publish(*object_size, per_chunk_publish_ns);
                }
                Ok(responses)
            }
            Err(err) => {
                for (path, object_size) in uploaded {
                    let cleanup_failed = cold_store.delete_chunk(&path).await.is_err();
                    self.metrics
                        .record_cold_orphan_cleanup(object_size, cleanup_failed);
                }
                Err(err)
            }
        }
    }

    pub async fn flush_cold_all_groups_once(
        &self,
        request: PlanGroupColdFlushRequest,
    ) -> Result<usize, RuntimeError> {
        self.flush_cold_all_groups_once_bounded(request, 1).await
    }

    pub async fn flush_cold_all_groups_once_bounded(
        &self,
        request: PlanGroupColdFlushRequest,
        max_concurrency: usize,
    ) -> Result<usize, RuntimeError> {
        let max_concurrency = max_concurrency.max(1);
        if max_concurrency == 1 {
            return self.flush_cold_all_groups_once_serial(request).await;
        }
        let mut flushed = 0;
        let mut next_group_id = 0;
        let group_count = self.shard_map.raft_group_count();
        let mut tasks = JoinSet::new();

        while next_group_id < group_count || !tasks.is_empty() {
            while next_group_id < group_count && tasks.len() < max_concurrency {
                let runtime = self.clone();
                let request = request.clone();
                let group_id = RaftGroupId(next_group_id);
                next_group_id += 1;
                tasks.spawn(async move {
                    runtime
                        .flush_cold_group_batch_once(
                            group_id,
                            request,
                            COLD_FLUSH_GROUP_BATCH_MAX_CHUNKS,
                        )
                        .await
                        .map(|responses| responses.len())
                });
            }
            if let Some(result) = tasks.join_next().await {
                match result {
                    Ok(Ok(count)) => flushed += count,
                    Ok(Err(err)) => return Err(err),
                    Err(err) => {
                        return Err(RuntimeError::ColdStoreIo {
                            message: format!("cold flush task failed: {err}"),
                        });
                    }
                }
            }
        }
        Ok(flushed)
    }

    async fn flush_cold_all_groups_once_serial(
        &self,
        request: PlanGroupColdFlushRequest,
    ) -> Result<usize, RuntimeError> {
        let mut flushed = 0;
        for group_id in 0..self.shard_map.raft_group_count() {
            flushed += self
                .flush_cold_group_batch_once(
                    RaftGroupId(group_id),
                    request.clone(),
                    COLD_FLUSH_GROUP_BATCH_MAX_CHUNKS,
                )
                .await?
                .len();
        }
        Ok(flushed)
    }

    pub async fn append(&self, request: AppendRequest) -> Result<AppendResponse, RuntimeError> {
        if request.payload.is_empty() {
            return Err(RuntimeError::EmptyAppend);
        }
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::Append {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn append_batch(
        &self,
        request: AppendBatchRequest,
    ) -> Result<AppendBatchResponse, RuntimeError> {
        if request.payloads.is_empty() {
            return Err(RuntimeError::EmptyAppend);
        }
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::AppendBatch {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn snapshot_group(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<GroupSnapshot, RuntimeError> {
        let placement = self.placement_for_group(raft_group_id)?;
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::SnapshotGroup {
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn install_group_snapshot(
        &self,
        snapshot: GroupSnapshot,
    ) -> Result<(), RuntimeError> {
        let expected = self.placement_for_group(snapshot.placement.raft_group_id)?;
        if snapshot.placement != expected {
            return Err(RuntimeError::SnapshotPlacementMismatch {
                expected,
                actual: snapshot.placement,
            });
        }
        let mailbox = &self.mailboxes[usize::from(expected.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::InstallGroupSnapshot {
                snapshot,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn warm_group(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<ShardPlacement, RuntimeError> {
        let placement = self.placement_for_group(raft_group_id)?;
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::WarmGroup {
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn warm_all_groups(&self) -> Result<(), RuntimeError> {
        for raw_group_id in 0..self.shard_map.raft_group_count() {
            self.warm_group(RaftGroupId(raw_group_id)).await?;
        }
        Ok(())
    }

    fn placement_for_group(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<ShardPlacement, RuntimeError> {
        if raft_group_id.0 >= self.shard_map.raft_group_count() {
            return Err(RuntimeError::InvalidRaftGroup {
                raft_group_id,
                raft_group_count: self.shard_map.raft_group_count(),
            });
        }
        Ok(ShardPlacement {
            core_id: CoreId(
                (raft_group_id.0 % u32::from(self.shard_map.core_count()))
                    .try_into()
                    .expect("core id fits u16"),
            ),
            shard_id: ShardId(raft_group_id.0),
            raft_group_id,
        })
    }

    async fn send_core_command<T>(
        &self,
        mailbox: &CoreMailbox,
        command: CoreCommand,
        response_rx: oneshot::Receiver<Result<T, RuntimeError>>,
    ) -> Result<T, RuntimeError> {
        self.enqueue_core_command(mailbox, command).await?;
        response_rx
            .await
            .map_err(|_| RuntimeError::ResponseDropped {
                core_id: mailbox.core_id,
            })?
    }

    async fn enqueue_core_command(
        &self,
        mailbox: &CoreMailbox,
        command: CoreCommand,
    ) -> Result<(), RuntimeError> {
        if mailbox.tx.capacity() == 0 {
            self.metrics.record_mailbox_full(mailbox.core_id);
        }
        let started_at = Instant::now();
        mailbox
            .tx
            .send(command)
            .await
            .map_err(|_| RuntimeError::MailboxClosed {
                core_id: mailbox.core_id,
            })?;
        self.metrics
            .record_routed_request(mailbox.core_id, elapsed_ns(started_at));
        Ok(())
    }

    pub fn metrics(&self) -> RuntimeMetrics {
        RuntimeMetrics {
            inner: self.metrics.clone(),
        }
    }

    pub fn mailbox_snapshot(&self) -> RuntimeMailboxSnapshot {
        let depths = self
            .mailboxes
            .iter()
            .map(CoreMailbox::depth)
            .collect::<Vec<_>>();
        let capacities = self
            .mailboxes
            .iter()
            .map(CoreMailbox::capacity)
            .collect::<Vec<_>>();
        RuntimeMailboxSnapshot { depths, capacities }
    }
}

fn spawn_core_worker(threading: RuntimeThreading, worker: CoreWorker) -> Result<(), RuntimeError> {
    let core_id = worker.core_id;
    match threading {
        RuntimeThreading::HostedTokio => {
            tokio::spawn(worker.run());
            Ok(())
        }
        RuntimeThreading::ThreadPerCore => std::thread::Builder::new()
            .name(format!("ursula-core-{}", core_id.0))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build per-core tokio runtime");
                runtime.block_on(worker.run());
            })
            .map(|_| ())
            .map_err(|err| RuntimeError::SpawnCoreThread {
                core_id,
                message: err.to_string(),
            }),
    }
}

#[derive(Debug, Clone)]
struct CoreMailbox {
    core_id: CoreId,
    tx: mpsc::Sender<CoreCommand>,
}

impl CoreMailbox {
    fn depth(&self) -> usize {
        self.tx.max_capacity() - self.tx.capacity()
    }

    fn capacity(&self) -> usize {
        self.tx.max_capacity()
    }
}

#[derive(Debug)]
enum CoreCommand {
    CreateStream {
        request: CreateStreamRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<CreateStreamResponse, RuntimeError>>,
    },
    CreateExternal {
        request: CreateStreamExternalRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<CreateStreamResponse, RuntimeError>>,
    },
    HeadStream {
        request: HeadStreamRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<HeadStreamResponse, RuntimeError>>,
    },
    ReadStream {
        request: ReadStreamRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<ReadStreamResponse, RuntimeError>>,
    },
    PublishSnapshot {
        request: PublishSnapshotRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<PublishSnapshotResponse, RuntimeError>>,
    },
    ReadSnapshot {
        request: ReadSnapshotRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<ReadSnapshotResponse, RuntimeError>>,
    },
    DeleteSnapshot {
        request: DeleteSnapshotRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    },
    BootstrapStream {
        request: BootstrapStreamRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<BootstrapStreamResponse, RuntimeError>>,
    },
    WaitRead {
        request: ReadStreamRequest,
        placement: ShardPlacement,
        waiter_id: u64,
        response_tx: oneshot::Sender<Result<ReadStreamResponse, RuntimeError>>,
    },
    RequireLiveReadOwner {
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    },
    CancelWaitRead {
        stream_id: BucketStreamId,
        placement: ShardPlacement,
        waiter_id: u64,
    },
    CloseStream {
        request: CloseStreamRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<CloseStreamResponse, RuntimeError>>,
    },
    AddForkRef {
        stream_id: BucketStreamId,
        now_ms: u64,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<ForkRefResponse, RuntimeError>>,
    },
    ReleaseForkRef {
        stream_id: BucketStreamId,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<ForkRefResponse, RuntimeError>>,
    },
    DeleteStream {
        request: DeleteStreamRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<DeleteStreamResponse, RuntimeError>>,
    },
    FlushCold {
        request: FlushColdRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<FlushColdResponse, RuntimeError>>,
    },
    FlushColdBatch {
        requests: Vec<FlushColdRequest>,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<Vec<FlushColdResponse>, RuntimeError>>,
    },
    PlanColdFlush {
        request: PlanColdFlushRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<Option<ColdFlushCandidate>, RuntimeError>>,
    },
    PlanNextColdFlush {
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<Option<ColdFlushCandidate>, RuntimeError>>,
    },
    PlanNextColdFlushBatch {
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
        max_candidates: usize,
        response_tx: oneshot::Sender<Result<Vec<ColdFlushCandidate>, RuntimeError>>,
    },
    Append {
        request: AppendRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<AppendResponse, RuntimeError>>,
    },
    AppendExternal {
        request: AppendExternalRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<AppendResponse, RuntimeError>>,
    },
    AppendBatch {
        request: AppendBatchRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<AppendBatchResponse, RuntimeError>>,
    },
    WarmGroup {
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<ShardPlacement, RuntimeError>>,
    },
    SnapshotGroup {
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<GroupSnapshot, RuntimeError>>,
    },
    InstallGroupSnapshot {
        snapshot: GroupSnapshot,
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    },
}

struct CoreWorker {
    core_id: CoreId,
    rx: mpsc::Receiver<CoreCommand>,
    engine_factory: Arc<dyn GroupEngineFactory>,
    groups: HashMap<RaftGroupId, GroupMailbox>,
    metrics: Arc<RuntimeMetricsInner>,
    group_mailbox_capacity: usize,
    cold_write_admission: ColdWriteAdmission,
    live_read_max_waiters_per_core: Option<u64>,
    read_materialization: Arc<Semaphore>,
}

#[derive(Clone)]
struct AppendBatchRuntime {
    metrics: Arc<RuntimeMetricsInner>,
    read_materialization: Arc<Semaphore>,
    placement: ShardPlacement,
}

type ReadWatchers = HashMap<BucketStreamId, Vec<ReadWatcher>>;
const GROUP_ACTOR_MAX_WRITE_BATCH: usize = 64;
const COLD_FLUSH_GROUP_BATCH_MAX_CHUNKS: usize = 64;

#[derive(Clone)]
struct GroupMailbox {
    group_id: RaftGroupId,
    tx: mpsc::Sender<GroupCommand>,
    metrics: Arc<RuntimeMetricsInner>,
}

impl GroupMailbox {
    async fn send(&self, command: GroupCommand) -> Result<(), Box<GroupCommand>> {
        match self.tx.try_send(command) {
            Ok(()) => {
                self.metrics.record_group_mailbox_enqueued(self.group_id);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(command)) => {
                self.metrics.record_group_mailbox_full(self.group_id);
                match self.tx.send(command).await {
                    Ok(()) => {
                        self.metrics.record_group_mailbox_enqueued(self.group_id);
                        Ok(())
                    }
                    Err(err) => Err(Box::new(err.0)),
                }
            }
            Err(mpsc::error::TrySendError::Closed(command)) => Err(Box::new(command)),
        }
    }
}

struct PendingAppendBatch {
    stream_id: BucketStreamId,
    incoming_bytes: u64,
    response_tx: oneshot::Sender<Result<AppendBatchResponse, RuntimeError>>,
    started_at: Instant,
}

#[derive(Debug)]
enum GroupCommand {
    CreateStream {
        request: CreateStreamRequest,
        response_tx: oneshot::Sender<Result<CreateStreamResponse, RuntimeError>>,
    },
    CreateExternal {
        request: CreateStreamExternalRequest,
        response_tx: oneshot::Sender<Result<CreateStreamResponse, RuntimeError>>,
    },
    HeadStream {
        request: HeadStreamRequest,
        response_tx: oneshot::Sender<Result<HeadStreamResponse, RuntimeError>>,
    },
    ReadStream {
        request: ReadStreamRequest,
        response_tx: oneshot::Sender<Result<ReadStreamResponse, RuntimeError>>,
    },
    PublishSnapshot {
        request: PublishSnapshotRequest,
        response_tx: oneshot::Sender<Result<PublishSnapshotResponse, RuntimeError>>,
    },
    ReadSnapshot {
        request: ReadSnapshotRequest,
        response_tx: oneshot::Sender<Result<ReadSnapshotResponse, RuntimeError>>,
    },
    DeleteSnapshot {
        request: DeleteSnapshotRequest,
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    },
    BootstrapStream {
        request: BootstrapStreamRequest,
        response_tx: oneshot::Sender<Result<BootstrapStreamResponse, RuntimeError>>,
    },
    WaitRead {
        request: ReadStreamRequest,
        waiter_id: u64,
        response_tx: oneshot::Sender<Result<ReadStreamResponse, RuntimeError>>,
    },
    CancelWaitRead {
        stream_id: BucketStreamId,
        waiter_id: u64,
    },
    RequireLiveReadOwner {
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    },
    CloseStream {
        request: CloseStreamRequest,
        response_tx: oneshot::Sender<Result<CloseStreamResponse, RuntimeError>>,
    },
    AddForkRef {
        stream_id: BucketStreamId,
        now_ms: u64,
        response_tx: oneshot::Sender<Result<ForkRefResponse, RuntimeError>>,
    },
    ReleaseForkRef {
        stream_id: BucketStreamId,
        response_tx: oneshot::Sender<Result<ForkRefResponse, RuntimeError>>,
    },
    DeleteStream {
        request: DeleteStreamRequest,
        response_tx: oneshot::Sender<Result<DeleteStreamResponse, RuntimeError>>,
    },
    FlushCold {
        request: FlushColdRequest,
        response_tx: oneshot::Sender<Result<FlushColdResponse, RuntimeError>>,
    },
    FlushColdBatch {
        requests: Vec<FlushColdRequest>,
        response_tx: oneshot::Sender<Result<Vec<FlushColdResponse>, RuntimeError>>,
    },
    PlanColdFlush {
        request: PlanColdFlushRequest,
        response_tx: oneshot::Sender<Result<Option<ColdFlushCandidate>, RuntimeError>>,
    },
    PlanNextColdFlush {
        request: PlanGroupColdFlushRequest,
        response_tx: oneshot::Sender<Result<Option<ColdFlushCandidate>, RuntimeError>>,
    },
    PlanNextColdFlushBatch {
        request: PlanGroupColdFlushRequest,
        max_candidates: usize,
        response_tx: oneshot::Sender<Result<Vec<ColdFlushCandidate>, RuntimeError>>,
    },
    Append {
        request: AppendRequest,
        response_tx: oneshot::Sender<Result<AppendResponse, RuntimeError>>,
    },
    AppendExternal {
        request: AppendExternalRequest,
        response_tx: oneshot::Sender<Result<AppendResponse, RuntimeError>>,
    },
    AppendBatch {
        request: AppendBatchRequest,
        response_tx: oneshot::Sender<Result<AppendBatchResponse, RuntimeError>>,
    },
    SnapshotGroup {
        response_tx: oneshot::Sender<Result<GroupSnapshot, RuntimeError>>,
    },
    InstallGroupSnapshot {
        snapshot: GroupSnapshot,
        response_tx: oneshot::Sender<Result<(), RuntimeError>>,
    },
}

impl GroupCommand {
    fn send_error(self, err: RuntimeError) {
        match self {
            Self::CreateStream { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::CreateExternal { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::HeadStream { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::ReadStream { response_tx, .. } | Self::WaitRead { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::CancelWaitRead { .. } => {}
            Self::RequireLiveReadOwner { response_tx } => {
                let _ = response_tx.send(Err(err));
            }
            Self::PublishSnapshot { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::ReadSnapshot { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::DeleteSnapshot { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::BootstrapStream { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::CloseStream { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::AddForkRef { response_tx, .. } | Self::ReleaseForkRef { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::DeleteStream { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::FlushCold { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::FlushColdBatch { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::PlanColdFlush { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::PlanNextColdFlush { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::PlanNextColdFlushBatch { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::Append { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::AppendExternal { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::AppendBatch { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
            Self::SnapshotGroup { response_tx } => {
                let _ = response_tx.send(Err(err));
            }
            Self::InstallGroupSnapshot { response_tx, .. } => {
                let _ = response_tx.send(Err(err));
            }
        }
    }
}

struct GroupActor {
    placement: ShardPlacement,
    engine: Box<dyn GroupEngine>,
    rx: mpsc::Receiver<GroupCommand>,
    read_watchers: ReadWatchers,
    metrics: Arc<RuntimeMetricsInner>,
    cold_write_admission: ColdWriteAdmission,
    live_read_max_waiters_per_core: Option<u64>,
    read_materialization: Arc<Semaphore>,
}

impl GroupActor {
    async fn run(mut self) {
        let mut pending = VecDeque::new();
        loop {
            let Some(command) = self.next_command(&mut pending).await else {
                break;
            };
            match command {
                GroupCommand::CreateStream {
                    request,
                    response_tx,
                } => {
                    let response = CoreWorker::create_stream(
                        &mut self.engine,
                        self.metrics.clone(),
                        request,
                        self.placement,
                        self.cold_write_admission,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::CreateExternal {
                    request,
                    response_tx,
                } => {
                    let response = CoreWorker::create_stream_external(
                        &mut self.engine,
                        self.metrics.clone(),
                        request,
                        self.placement,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::HeadStream {
                    request,
                    response_tx,
                } => {
                    let response = CoreWorker::head_stream(
                        &mut self.engine,
                        self.metrics.clone(),
                        request,
                        self.placement,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::ReadStream {
                    request,
                    response_tx,
                } => {
                    CoreWorker::read_stream(
                        &mut self.engine,
                        self.metrics.clone(),
                        self.read_materialization.clone(),
                        request,
                        self.placement,
                        response_tx,
                    )
                    .await;
                }
                GroupCommand::PublishSnapshot {
                    request,
                    response_tx,
                } => {
                    let response = CoreWorker::publish_snapshot(
                        &mut self.engine,
                        self.metrics.clone(),
                        self.read_materialization.clone(),
                        &mut self.read_watchers,
                        request,
                        self.placement,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::ReadSnapshot {
                    request,
                    response_tx,
                } => {
                    let response = CoreWorker::read_snapshot(
                        &mut self.engine,
                        self.metrics.clone(),
                        request,
                        self.placement,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::DeleteSnapshot {
                    request,
                    response_tx,
                } => {
                    let response = CoreWorker::delete_snapshot(
                        &mut self.engine,
                        self.metrics.clone(),
                        request,
                        self.placement,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::BootstrapStream {
                    request,
                    response_tx,
                } => {
                    let response = CoreWorker::bootstrap_stream(
                        &mut self.engine,
                        self.metrics.clone(),
                        request,
                        self.placement,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::WaitRead {
                    request,
                    waiter_id,
                    response_tx,
                } => {
                    let watcher = ReadWatcher {
                        waiter_id,
                        request,
                        response_tx,
                    };
                    CoreWorker::wait_read_stream(
                        &mut self.engine,
                        self.metrics.clone(),
                        self.read_materialization.clone(),
                        &mut self.read_watchers,
                        self.placement,
                        watcher,
                        self.live_read_max_waiters_per_core,
                    )
                    .await;
                }
                GroupCommand::CancelWaitRead {
                    stream_id,
                    waiter_id,
                } => {
                    CoreWorker::cancel_read_watcher(
                        &mut self.read_watchers,
                        self.metrics.clone(),
                        self.placement.core_id,
                        stream_id,
                        waiter_id,
                    );
                }
                GroupCommand::RequireLiveReadOwner { response_tx } => {
                    let response = self
                        .engine
                        .require_local_live_read_owner(self.placement)
                        .await
                        .map_err(|err| RuntimeError::group_engine(self.placement, err));
                    let _ = response_tx.send(response);
                }
                GroupCommand::CloseStream {
                    request,
                    response_tx,
                } => {
                    let response = CoreWorker::close_stream(
                        &mut self.engine,
                        self.metrics.clone(),
                        self.read_materialization.clone(),
                        &mut self.read_watchers,
                        request,
                        self.placement,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::AddForkRef {
                    stream_id,
                    now_ms,
                    response_tx,
                } => {
                    let response = CoreWorker::add_fork_ref(
                        &mut self.engine,
                        self.metrics.clone(),
                        stream_id,
                        now_ms,
                        self.placement,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::ReleaseForkRef {
                    stream_id,
                    response_tx,
                } => {
                    let response = CoreWorker::release_fork_ref(
                        &mut self.engine,
                        self.metrics.clone(),
                        self.read_materialization.clone(),
                        &mut self.read_watchers,
                        stream_id,
                        self.placement,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::DeleteStream {
                    request,
                    response_tx,
                } => {
                    let response = CoreWorker::delete_stream(
                        &mut self.engine,
                        self.metrics.clone(),
                        self.read_materialization.clone(),
                        &mut self.read_watchers,
                        request,
                        self.placement,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::FlushCold {
                    request,
                    response_tx,
                } => {
                    let response = CoreWorker::flush_cold(
                        &mut self.engine,
                        self.metrics.clone(),
                        self.read_materialization.clone(),
                        &mut self.read_watchers,
                        request,
                        self.placement,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::FlushColdBatch {
                    requests,
                    response_tx,
                } => {
                    let response = CoreWorker::flush_cold_batch(
                        &mut self.engine,
                        self.metrics.clone(),
                        self.read_materialization.clone(),
                        &mut self.read_watchers,
                        requests,
                        self.placement,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::PlanColdFlush {
                    request,
                    response_tx,
                } => {
                    let response = CoreWorker::plan_cold_flush(
                        &mut self.engine,
                        self.metrics.clone(),
                        request,
                        self.placement,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::PlanNextColdFlush {
                    request,
                    response_tx,
                } => {
                    let response = CoreWorker::plan_next_cold_flush(
                        &mut self.engine,
                        self.metrics.clone(),
                        request,
                        self.placement,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::PlanNextColdFlushBatch {
                    request,
                    max_candidates,
                    response_tx,
                } => {
                    let response = CoreWorker::plan_next_cold_flush_batch(
                        &mut self.engine,
                        self.metrics.clone(),
                        request,
                        self.placement,
                        max_candidates,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::Append {
                    request,
                    response_tx,
                } => {
                    let response = CoreWorker::apply_append(
                        &mut self.engine,
                        self.metrics.clone(),
                        self.read_materialization.clone(),
                        &mut self.read_watchers,
                        request,
                        self.placement,
                        self.cold_write_admission,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::AppendExternal {
                    request,
                    response_tx,
                } => {
                    let response = CoreWorker::apply_append_external(
                        &mut self.engine,
                        self.metrics.clone(),
                        self.read_materialization.clone(),
                        &mut self.read_watchers,
                        request,
                        self.placement,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::AppendBatch {
                    request,
                    response_tx,
                } => {
                    let mut batch = vec![(request, response_tx)];
                    self.collect_append_batch_commands(&mut pending, &mut batch);
                    if self.cold_write_admission.is_enabled() {
                        let (requests, pending_batch) =
                            CoreWorker::prepare_append_batch_requests(batch);
                        CoreWorker::apply_prepared_append_batch_requests_with_cold_admission(
                            &mut self.engine,
                            AppendBatchRuntime {
                                metrics: self.metrics.clone(),
                                read_materialization: self.read_materialization.clone(),
                                placement: self.placement,
                            },
                            &mut self.read_watchers,
                            pending_batch,
                            requests,
                            self.cold_write_admission,
                        )
                        .await;
                    } else {
                        let (commands, pending_batch) =
                            CoreWorker::prepare_append_batch_commands(batch);
                        CoreWorker::apply_prepared_append_batch_commands(
                            &mut self.engine,
                            AppendBatchRuntime {
                                metrics: self.metrics.clone(),
                                read_materialization: self.read_materialization.clone(),
                                placement: self.placement,
                            },
                            &mut self.read_watchers,
                            pending_batch,
                            commands,
                        )
                        .await;
                    }
                }
                GroupCommand::SnapshotGroup { response_tx } => {
                    let response = CoreWorker::snapshot_group(
                        &mut self.engine,
                        self.metrics.clone(),
                        self.placement,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
                GroupCommand::InstallGroupSnapshot {
                    snapshot,
                    response_tx,
                } => {
                    let response = CoreWorker::install_group_snapshot(
                        &mut self.engine,
                        self.metrics.clone(),
                        snapshot,
                    )
                    .await;
                    let _ = response_tx.send(response);
                }
            }
        }
    }

    async fn next_command(&mut self, pending: &mut VecDeque<GroupCommand>) -> Option<GroupCommand> {
        match pending.pop_front() {
            Some(command) => Some(command),
            None => {
                let command = self.rx.recv().await;
                if command.is_some() {
                    self.metrics
                        .record_group_mailbox_dequeued(self.placement.raft_group_id);
                }
                command
            }
        }
    }

    fn collect_append_batch_commands(
        &mut self,
        pending: &mut VecDeque<GroupCommand>,
        batch: &mut Vec<(
            AppendBatchRequest,
            oneshot::Sender<Result<AppendBatchResponse, RuntimeError>>,
        )>,
    ) {
        while batch.len() < GROUP_ACTOR_MAX_WRITE_BATCH {
            let command = match pending.pop_front() {
                Some(command) => Some(command),
                None => match self.rx.try_recv() {
                    Ok(command) => {
                        self.metrics
                            .record_group_mailbox_dequeued(self.placement.raft_group_id);
                        Some(command)
                    }
                    Err(_) => None,
                },
            };
            match command {
                Some(GroupCommand::AppendBatch {
                    request,
                    response_tx,
                }) => batch.push((request, response_tx)),
                Some(other) => {
                    pending.push_front(other);
                    break;
                }
                None => break,
            }
        }
    }
}

struct ReadWatcher {
    waiter_id: u64,
    request: ReadStreamRequest,
    response_tx: oneshot::Sender<Result<ReadStreamResponse, RuntimeError>>,
}

fn live_read_watcher_count(read_watchers: &HashMap<BucketStreamId, Vec<ReadWatcher>>) -> u64 {
    read_watchers
        .values()
        .map(|watchers| u64::try_from(watchers.len()).expect("watcher count fits u64"))
        .sum()
}

struct WaitReadCancel {
    tx: mpsc::Sender<CoreCommand>,
    stream_id: Option<BucketStreamId>,
    placement: ShardPlacement,
    waiter_id: u64,
}

impl WaitReadCancel {
    fn new(
        tx: mpsc::Sender<CoreCommand>,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
        waiter_id: u64,
    ) -> Self {
        Self {
            tx,
            stream_id: Some(stream_id),
            placement,
            waiter_id,
        }
    }

    fn disarm(&mut self) {
        self.stream_id = None;
    }
}

impl Drop for WaitReadCancel {
    fn drop(&mut self) {
        if let Some(stream_id) = self.stream_id.take() {
            // Drop cannot await. If the owner mailbox is full, the stale
            // waiter is still removed when the next stream notification
            // consumes the closed oneshot sender.
            let _ = self.tx.try_send(CoreCommand::CancelWaitRead {
                stream_id,
                placement: self.placement,
                waiter_id: self.waiter_id,
            });
        }
    }
}

impl CoreWorker {
    async fn run(mut self) {
        while let Some(command) = self.rx.recv().await {
            match command {
                CoreCommand::CreateStream {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::CreateStream {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::CreateExternal {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::CreateExternal {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::HeadStream {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::HeadStream {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::ReadStream {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::ReadStream {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::PublishSnapshot {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::PublishSnapshot {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::ReadSnapshot {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::ReadSnapshot {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::DeleteSnapshot {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::DeleteSnapshot {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::BootstrapStream {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::BootstrapStream {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::WaitRead {
                    request,
                    placement,
                    waiter_id,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::WaitRead {
                            request,
                            waiter_id,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::RequireLiveReadOwner {
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::RequireLiveReadOwner { response_tx },
                    )
                    .await;
                }
                CoreCommand::CancelWaitRead {
                    stream_id,
                    placement,
                    waiter_id,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::CancelWaitRead {
                            stream_id,
                            waiter_id,
                        },
                    )
                    .await;
                }
                CoreCommand::CloseStream {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::CloseStream {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::AddForkRef {
                    stream_id,
                    now_ms,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::AddForkRef {
                            stream_id,
                            now_ms,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::ReleaseForkRef {
                    stream_id,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::ReleaseForkRef {
                            stream_id,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::DeleteStream {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::DeleteStream {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::FlushCold {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::FlushCold {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::FlushColdBatch {
                    requests,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::FlushColdBatch {
                            requests,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::PlanColdFlush {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::PlanColdFlush {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::PlanNextColdFlush {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::PlanNextColdFlush {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::PlanNextColdFlushBatch {
                    request,
                    placement,
                    max_candidates,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::PlanNextColdFlushBatch {
                            request,
                            max_candidates,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::Append {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::Append {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::AppendExternal {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::AppendExternal {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::AppendBatch {
                    request,
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(
                        placement,
                        GroupCommand::AppendBatch {
                            request,
                            response_tx,
                        },
                    )
                    .await;
                }
                CoreCommand::WarmGroup {
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    let response = self.group(placement).await.map(|_| placement);
                    let _ = response_tx.send(response);
                }
                CoreCommand::SnapshotGroup {
                    placement,
                    response_tx,
                } => {
                    debug_assert_eq!(placement.core_id, self.core_id);
                    self.send_group_command(placement, GroupCommand::SnapshotGroup { response_tx })
                        .await;
                }
                CoreCommand::InstallGroupSnapshot {
                    snapshot,
                    response_tx,
                } => {
                    debug_assert_eq!(snapshot.placement.core_id, self.core_id);
                    self.send_group_command(
                        snapshot.placement,
                        GroupCommand::InstallGroupSnapshot {
                            snapshot,
                            response_tx,
                        },
                    )
                    .await;
                }
            }
        }
    }

    async fn send_group_command(&mut self, placement: ShardPlacement, command: GroupCommand) {
        let core_id = placement.core_id;
        match self.group(placement).await {
            Ok(group) => {
                if let Err(command) = group.send(command).await {
                    (*command).send_error(RuntimeError::MailboxClosed { core_id });
                }
            }
            Err(err) => command.send_error(err),
        }
    }

    async fn group(&mut self, placement: ShardPlacement) -> Result<GroupMailbox, RuntimeError> {
        if !self.groups.contains_key(&placement.raft_group_id) {
            let engine_factory = self.engine_factory.clone();
            let metrics = GroupEngineMetrics {
                inner: self.metrics.clone(),
            };
            let engine = engine_factory
                .create(placement, metrics)
                .await
                .map_err(|err| RuntimeError::group_engine(placement, err))?;
            let (tx, rx) = mpsc::channel(self.group_mailbox_capacity);
            let actor = GroupActor {
                placement,
                engine,
                rx,
                read_watchers: HashMap::new(),
                metrics: self.metrics.clone(),
                cold_write_admission: self.cold_write_admission,
                live_read_max_waiters_per_core: self.live_read_max_waiters_per_core,
                read_materialization: self.read_materialization.clone(),
            };
            tokio::spawn(actor.run());
            self.groups.insert(
                placement.raft_group_id,
                GroupMailbox {
                    group_id: placement.raft_group_id,
                    tx,
                    metrics: self.metrics.clone(),
                },
            );
        }
        Ok(self
            .groups
            .get(&placement.raft_group_id)
            .expect("group was just inserted")
            .clone())
    }

    async fn read_stream(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        request: ReadStreamRequest,
        placement: ShardPlacement,
        response_tx: oneshot::Sender<Result<ReadStreamResponse, RuntimeError>>,
    ) {
        let exec_started_at = Instant::now();
        let parts = group
            .read_stream_parts(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        match parts {
            Ok(parts) => {
                Self::send_read_parts_response(placement, read_materialization, parts, response_tx);
            }
            Err(err) => {
                let _ = response_tx.send(Err(err));
            }
        }
    }

    fn send_read_parts_response(
        placement: ShardPlacement,
        read_materialization: Arc<Semaphore>,
        parts: GroupReadStreamParts,
        response_tx: oneshot::Sender<Result<ReadStreamResponse, RuntimeError>>,
    ) {
        tokio::spawn(async move {
            let response = match read_materialization.acquire_owned().await {
                Ok(_permit) => parts
                    .into_response()
                    .await
                    .map_err(|err| RuntimeError::group_engine(placement, err)),
                Err(_) => Err(RuntimeError::MailboxClosed {
                    core_id: placement.core_id,
                }),
            };
            let _ = response_tx.send(response);
        });
    }

    fn send_read_parts_to_watchers(
        placement: ShardPlacement,
        read_materialization: Arc<Semaphore>,
        parts: GroupReadStreamParts,
        watchers: Vec<ReadWatcher>,
    ) {
        tokio::spawn(async move {
            let response = match read_materialization.acquire_owned().await {
                Ok(_permit) => parts
                    .into_response()
                    .await
                    .map_err(|err| RuntimeError::group_engine(placement, err)),
                Err(_) => Err(RuntimeError::MailboxClosed {
                    core_id: placement.core_id,
                }),
            };
            for watcher in watchers {
                let _ = watcher.response_tx.send(response.clone());
            }
        });
    }

    async fn publish_snapshot(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        request: PublishSnapshotRequest,
        placement: ShardPlacement,
    ) -> Result<PublishSnapshotResponse, RuntimeError> {
        let stream_id = request.stream_id.clone();
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .publish_snapshot(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        if response.is_ok() {
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
            record_cold_hot_backlog(group, &metrics, stream_id.clone(), placement).await;
            Self::notify_read_watchers(
                group,
                metrics,
                read_materialization,
                read_watchers,
                &stream_id,
                placement,
            )
            .await;
        }
        response
    }

    async fn read_snapshot(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: ReadSnapshotRequest,
        placement: ShardPlacement,
    ) -> Result<ReadSnapshotResponse, RuntimeError> {
        let exec_started_at = Instant::now();
        let response = group
            .read_snapshot(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    async fn delete_snapshot(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: DeleteSnapshotRequest,
        placement: ShardPlacement,
    ) -> Result<(), RuntimeError> {
        let exec_started_at = Instant::now();
        let response = group
            .delete_snapshot(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    async fn bootstrap_stream(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: BootstrapStreamRequest,
        placement: ShardPlacement,
    ) -> Result<BootstrapStreamResponse, RuntimeError> {
        let exec_started_at = Instant::now();
        let response = group
            .bootstrap_stream(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    async fn wait_read_stream(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        placement: ShardPlacement,
        watcher: ReadWatcher,
        live_read_max_waiters_per_core: Option<u64>,
    ) {
        let exec_started_at = Instant::now();
        let parts = group
            .read_stream_parts(watcher.request.clone(), placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        match parts {
            Ok(parts) if parts.payload_is_empty() && parts.up_to_date && !parts.closed => {
                if watcher.response_tx.is_closed() {
                    return;
                }
                let current_waiters = live_read_watcher_count(read_watchers);
                if let Some(limit) = live_read_max_waiters_per_core
                    && current_waiters >= limit
                {
                    metrics.record_live_read_backpressure(placement.core_id);
                    let _ = watcher
                        .response_tx
                        .send(Err(RuntimeError::LiveReadBackpressure {
                            core_id: placement.core_id,
                            current_waiters,
                            limit,
                        }));
                    return;
                }
                metrics.record_read_watcher_added(placement.core_id);
                read_watchers
                    .entry(watcher.request.stream_id.clone())
                    .or_default()
                    .push(watcher);
            }
            Ok(parts) => {
                Self::send_read_parts_response(
                    placement,
                    read_materialization.clone(),
                    parts,
                    watcher.response_tx,
                );
            }
            Err(err) => {
                let _ = watcher.response_tx.send(Err(err));
            }
        }
    }

    fn cancel_read_watcher(
        read_watchers: &mut ReadWatchers,
        metrics: Arc<RuntimeMetricsInner>,
        core_id: CoreId,
        stream_id: BucketStreamId,
        waiter_id: u64,
    ) {
        let Some(watchers) = read_watchers.get_mut(&stream_id) else {
            return;
        };
        let before = watchers.len();
        watchers.retain(|watcher| watcher.waiter_id != waiter_id);
        let removed = before - watchers.len();
        let is_empty = watchers.is_empty();
        if removed > 0 {
            metrics.record_read_watchers_removed(core_id, removed);
        }
        if is_empty {
            read_watchers.remove(&stream_id);
        }
    }

    async fn close_stream(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> Result<CloseStreamResponse, RuntimeError> {
        let stream_id = request.stream_id.clone();
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .close_stream(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        if response
            .as_ref()
            .is_ok_and(|response| !response.deduplicated)
        {
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
            Self::notify_read_watchers(
                group,
                metrics,
                read_materialization,
                read_watchers,
                &stream_id,
                placement,
            )
            .await;
        }
        response
    }

    async fn add_fork_ref(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        stream_id: BucketStreamId,
        now_ms: u64,
        placement: ShardPlacement,
    ) -> Result<ForkRefResponse, RuntimeError> {
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .add_fork_ref(stream_id, now_ms, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        if response.is_ok() {
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
        }
        response
    }

    async fn release_fork_ref(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> Result<ForkRefResponse, RuntimeError> {
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .release_fork_ref(stream_id.clone(), placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        if response.is_ok() {
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
            Self::notify_read_watchers(
                group,
                metrics,
                read_materialization,
                read_watchers,
                &stream_id,
                placement,
            )
            .await;
        }
        response
    }

    async fn delete_stream(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> Result<DeleteStreamResponse, RuntimeError> {
        let stream_id = request.stream_id.clone();
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .delete_stream(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        if response.is_ok() {
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
            record_cold_hot_backlog(group, &metrics, stream_id.clone(), placement).await;
            Self::notify_read_watchers(
                group,
                metrics,
                read_materialization,
                read_watchers,
                &stream_id,
                placement,
            )
            .await;
        }
        response
    }

    async fn flush_cold(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        request: FlushColdRequest,
        placement: ShardPlacement,
    ) -> Result<FlushColdResponse, RuntimeError> {
        let stream_id = request.stream_id.clone();
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .flush_cold(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        if response.is_ok() {
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
            record_cold_hot_backlog(group, &metrics, stream_id.clone(), placement).await;
            Self::notify_read_watchers(
                group,
                metrics,
                read_materialization,
                read_watchers,
                &stream_id,
                placement,
            )
            .await;
        }
        response
    }

    async fn flush_cold_batch(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        requests: Vec<FlushColdRequest>,
        placement: ShardPlacement,
    ) -> Result<Vec<FlushColdResponse>, RuntimeError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let stream_ids = requests
            .iter()
            .map(|request| request.stream_id.clone())
            .collect::<Vec<_>>();
        let commands = requests
            .into_iter()
            .map(GroupWriteCommand::from)
            .collect::<Vec<_>>();
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .write_batch(vec![GroupWriteCommand::Batch { commands }], placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        let mut outer = response?;
        let Some(batch_response) = outer.pop() else {
            return Err(RuntimeError::group_engine(
                placement,
                GroupEngineError::new("cold flush batch returned no response"),
            ));
        };
        let items =
            match batch_response.map_err(|err| RuntimeError::group_engine(placement, err))? {
                GroupWriteResponse::Batch(items) => items,
                other => {
                    return Err(RuntimeError::group_engine(
                        placement,
                        GroupEngineError::new(format!(
                            "unexpected cold flush batch response: {other:?}"
                        )),
                    ));
                }
            };
        let mut responses = Vec::with_capacity(items.len());
        let mutation_ns = elapsed_ns(started_at);
        for (index, item) in items.into_iter().enumerate() {
            match item.map_err(|err| RuntimeError::group_engine(placement, err))? {
                GroupWriteResponse::FlushCold(response) => {
                    metrics.record_applied_mutation(
                        placement.core_id,
                        placement.raft_group_id,
                        mutation_ns,
                    );
                    if let Some(stream_id) = stream_ids.get(index) {
                        record_cold_hot_backlog(group, &metrics, stream_id.clone(), placement)
                            .await;
                        Self::notify_read_watchers(
                            group,
                            metrics.clone(),
                            read_materialization.clone(),
                            read_watchers,
                            stream_id,
                            placement,
                        )
                        .await;
                    }
                    responses.push(response);
                }
                other => {
                    return Err(RuntimeError::group_engine(
                        placement,
                        GroupEngineError::new(format!(
                            "unexpected cold flush batch item response: {other:?}"
                        )),
                    ));
                }
            }
        }
        Ok(responses)
    }

    async fn plan_cold_flush(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: PlanColdFlushRequest,
        placement: ShardPlacement,
    ) -> Result<Option<ColdFlushCandidate>, RuntimeError> {
        let exec_started_at = Instant::now();
        let response = group
            .plan_cold_flush(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    async fn plan_next_cold_flush(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
    ) -> Result<Option<ColdFlushCandidate>, RuntimeError> {
        if !group.accepts_local_writes() {
            return Ok(None);
        }
        let exec_started_at = Instant::now();
        let response = group
            .plan_next_cold_flush(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    async fn plan_next_cold_flush_batch(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
        max_candidates: usize,
    ) -> Result<Vec<ColdFlushCandidate>, RuntimeError> {
        if !group.accepts_local_writes() {
            return Ok(Vec::new());
        }
        let exec_started_at = Instant::now();
        let response = group
            .plan_next_cold_flush_batch(request, placement, max_candidates)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    async fn head_stream(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> Result<HeadStreamResponse, RuntimeError> {
        let exec_started_at = Instant::now();
        let response = group
            .head_stream(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    async fn snapshot_group(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        placement: ShardPlacement,
    ) -> Result<GroupSnapshot, RuntimeError> {
        let exec_started_at = Instant::now();
        let response = group
            .snapshot(placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    async fn install_group_snapshot(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        snapshot: GroupSnapshot,
    ) -> Result<(), RuntimeError> {
        let placement = snapshot.placement;
        let exec_started_at = Instant::now();
        let response = group
            .install_snapshot(snapshot)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err));
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        response
    }

    async fn create_stream(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<CreateStreamResponse, RuntimeError> {
        let stream_id = request.stream_id.clone();
        let incoming_bytes =
            u64::try_from(request.initial_payload.len()).expect("payload len fits u64");
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .create_stream_with_cold_admission(request, placement, admission)
            .await
            .map_err(|err| {
                record_cold_backpressure_error(
                    &metrics,
                    placement,
                    incoming_bytes,
                    admission,
                    &err,
                );
                RuntimeError::group_engine(placement, err)
            })?;
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        if !response.already_exists {
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
            record_cold_hot_backlog(group, &metrics, stream_id, placement).await;
        }
        Ok(response)
    }

    async fn create_stream_external(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        request: CreateStreamExternalRequest,
        placement: ShardPlacement,
    ) -> Result<CreateStreamResponse, RuntimeError> {
        let stream_id = request.stream_id.clone();
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .create_stream_external(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err))?;
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        if !response.already_exists {
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
            record_cold_hot_backlog(group, &metrics, stream_id, placement).await;
        }
        Ok(response)
    }

    async fn apply_append(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<AppendResponse, RuntimeError> {
        let stream_id = request.stream_id.clone();
        let incoming_bytes = request.payload_len();
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .append_with_cold_admission(request, placement, admission)
            .await
            .map_err(|err| {
                record_cold_backpressure_error(
                    &metrics,
                    placement,
                    incoming_bytes,
                    admission,
                    &err,
                );
                RuntimeError::group_engine(placement, err)
            })?;
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );

        if !response.deduplicated {
            metrics.record_append(placement.core_id, placement.raft_group_id);
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
            record_cold_hot_backlog(group, &metrics, stream_id.clone(), placement).await;
            Self::notify_read_watchers(
                group,
                metrics,
                read_materialization,
                read_watchers,
                &stream_id,
                placement,
            )
            .await;
        }
        Ok(response)
    }

    async fn apply_append_external(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        request: AppendExternalRequest,
        placement: ShardPlacement,
    ) -> Result<AppendResponse, RuntimeError> {
        let stream_id = request.stream_id.clone();
        let started_at = Instant::now();
        let exec_started_at = Instant::now();
        let response = group
            .append_external(request, placement)
            .await
            .map_err(|err| RuntimeError::group_engine(placement, err))?;
        metrics.record_group_engine_exec(
            placement.core_id,
            placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );

        if !response.deduplicated {
            metrics.record_append(placement.core_id, placement.raft_group_id);
            metrics.record_applied_mutation(
                placement.core_id,
                placement.raft_group_id,
                elapsed_ns(started_at),
            );
            record_cold_hot_backlog(group, &metrics, stream_id.clone(), placement).await;
            Self::notify_read_watchers(
                group,
                metrics,
                read_materialization,
                read_watchers,
                &stream_id,
                placement,
            )
            .await;
        }
        Ok(response)
    }

    fn prepare_append_batch_commands(
        batch: Vec<(
            AppendBatchRequest,
            oneshot::Sender<Result<AppendBatchResponse, RuntimeError>>,
        )>,
    ) -> (Vec<GroupWriteCommand>, Vec<PendingAppendBatch>) {
        let mut commands = Vec::with_capacity(batch.len());
        let mut pending = Vec::with_capacity(batch.len());
        for (request, response_tx) in batch {
            pending.push(PendingAppendBatch {
                stream_id: request.stream_id.clone(),
                incoming_bytes: append_batch_payload_bytes(&request),
                response_tx,
                started_at: Instant::now(),
            });
            commands.push(GroupWriteCommand::from(request));
        }
        (commands, pending)
    }

    fn prepare_append_batch_requests(
        batch: Vec<(
            AppendBatchRequest,
            oneshot::Sender<Result<AppendBatchResponse, RuntimeError>>,
        )>,
    ) -> (Vec<AppendBatchRequest>, Vec<PendingAppendBatch>) {
        let mut requests = Vec::with_capacity(batch.len());
        let mut pending = Vec::with_capacity(batch.len());
        for (request, response_tx) in batch {
            pending.push(PendingAppendBatch {
                stream_id: request.stream_id.clone(),
                incoming_bytes: append_batch_payload_bytes(&request),
                response_tx,
                started_at: Instant::now(),
            });
            requests.push(request);
        }
        (requests, pending)
    }

    async fn apply_prepared_append_batch_requests_with_cold_admission(
        group: &mut Box<dyn GroupEngine>,
        runtime: AppendBatchRuntime,
        read_watchers: &mut ReadWatchers,
        pending: Vec<PendingAppendBatch>,
        requests: Vec<AppendBatchRequest>,
        admission: ColdWriteAdmission,
    ) {
        let exec_started_at = Instant::now();
        let responses = group
            .append_batch_many_with_cold_admission(requests, runtime.placement, admission)
            .await
            .map_err(|err| RuntimeError::group_engine(runtime.placement, err));
        runtime.metrics.record_group_engine_exec(
            runtime.placement.core_id,
            runtime.placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        Self::finish_append_batch_commands(
            group,
            runtime,
            read_watchers,
            pending,
            responses,
            Some(admission),
        )
        .await;
    }

    async fn apply_prepared_append_batch_commands(
        group: &mut Box<dyn GroupEngine>,
        runtime: AppendBatchRuntime,
        read_watchers: &mut ReadWatchers,
        pending: Vec<PendingAppendBatch>,
        commands: Vec<GroupWriteCommand>,
    ) {
        let exec_started_at = Instant::now();
        let responses = group
            .write_batch(commands, runtime.placement)
            .await
            .map_err(|err| RuntimeError::group_engine(runtime.placement, err));
        runtime.metrics.record_group_engine_exec(
            runtime.placement.core_id,
            runtime.placement.raft_group_id,
            elapsed_ns(exec_started_at),
        );
        Self::finish_append_batch_commands(group, runtime, read_watchers, pending, responses, None)
            .await;
    }

    async fn finish_append_batch_commands(
        group: &mut Box<dyn GroupEngine>,
        runtime: AppendBatchRuntime,
        read_watchers: &mut ReadWatchers,
        pending: Vec<PendingAppendBatch>,
        responses: Result<Vec<Result<GroupWriteResponse, GroupEngineError>>, RuntimeError>,
        admission: Option<ColdWriteAdmission>,
    ) {
        let placement = runtime.placement;
        let responses = match responses {
            Ok(responses) => responses,
            Err(err) => {
                for pending in pending {
                    if let Some(admission) = admission
                        && let RuntimeError::GroupEngine { message, .. } = &err
                        && message.contains("ColdBackpressure")
                    {
                        runtime.metrics.record_cold_backpressure(
                            placement.core_id,
                            placement.raft_group_id,
                            pending.incoming_bytes,
                            admission.max_hot_bytes_per_group.unwrap_or(0),
                        );
                    }
                    let _ = pending.response_tx.send(Err(err.clone()));
                }
                return;
            }
        };

        if responses.len() != pending.len() {
            let err = RuntimeError::GroupEngine {
                core_id: placement.core_id,
                raft_group_id: placement.raft_group_id,
                message: format!(
                    "batched append response count {} does not match request count {}",
                    responses.len(),
                    pending.len()
                ),
                next_offset: None,
                leader_hint: None,
            };
            for pending in pending {
                let _ = pending.response_tx.send(Err(err.clone()));
            }
            return;
        }

        for (pending, response) in pending.into_iter().zip(responses) {
            let response = match response {
                Ok(GroupWriteResponse::AppendBatch(response)) => Ok(response),
                Ok(other) => Err(RuntimeError::GroupEngine {
                    core_id: placement.core_id,
                    raft_group_id: placement.raft_group_id,
                    message: format!("unexpected batched append response: {other:?}"),
                    next_offset: None,
                    leader_hint: None,
                }),
                Err(err) => Err(RuntimeError::group_engine(placement, err)),
            };

            match response {
                Ok(response) => {
                    let success_count = response
                        .items
                        .iter()
                        .filter(|item| matches!(item, Ok(response) if !response.deduplicated))
                        .count();
                    if success_count > 0 {
                        let success_count = u64::try_from(success_count).expect("count fits u64");
                        runtime.metrics.record_append_batch(
                            placement.core_id,
                            placement.raft_group_id,
                            success_count,
                        );
                        runtime.metrics.record_applied_mutation_batch(
                            placement.core_id,
                            placement.raft_group_id,
                            success_count,
                            elapsed_ns(pending.started_at),
                        );
                        Self::notify_read_watchers(
                            group,
                            runtime.metrics.clone(),
                            runtime.read_materialization.clone(),
                            read_watchers,
                            &pending.stream_id,
                            placement,
                        )
                        .await;
                    }

                    let items = response
                        .items
                        .into_iter()
                        .map(|item| item.map_err(|err| RuntimeError::group_engine(placement, err)))
                        .collect();
                    let _ = pending
                        .response_tx
                        .send(Ok(AppendBatchResponse { placement, items }));
                }
                Err(err) => {
                    if let Some(admission) = admission
                        && let RuntimeError::GroupEngine { message, .. } = &err
                        && message.contains("ColdBackpressure")
                    {
                        runtime.metrics.record_cold_backpressure(
                            placement.core_id,
                            placement.raft_group_id,
                            pending.incoming_bytes,
                            admission.max_hot_bytes_per_group.unwrap_or(0),
                        );
                    }
                    let _ = pending.response_tx.send(Err(err));
                }
            }
        }
    }

    async fn notify_read_watchers(
        group: &mut Box<dyn GroupEngine>,
        metrics: Arc<RuntimeMetricsInner>,
        read_materialization: Arc<Semaphore>,
        read_watchers: &mut ReadWatchers,
        stream_id: &BucketStreamId,
        placement: ShardPlacement,
    ) {
        let Some(watchers) = read_watchers.remove(stream_id) else {
            return;
        };
        metrics.record_read_watchers_removed(placement.core_id, watchers.len());

        let mut request_groups: Vec<(ReadStreamRequest, Vec<ReadWatcher>)> = Vec::new();
        for watcher in watchers {
            if let Some((_, grouped)) = request_groups
                .iter_mut()
                .find(|(request, _)| *request == watcher.request)
            {
                grouped.push(watcher);
            } else {
                request_groups.push((watcher.request.clone(), vec![watcher]));
            }
        }

        let mut pending = Vec::new();
        for (request, watchers) in request_groups {
            let parts = group
                .read_stream_parts(request, placement)
                .await
                .map_err(|err| RuntimeError::group_engine(placement, err));
            match parts {
                Ok(parts) if parts.payload_is_empty() && parts.up_to_date && !parts.closed => {
                    pending.extend(watchers);
                }
                Ok(parts) => {
                    Self::send_read_parts_to_watchers(
                        placement,
                        read_materialization.clone(),
                        parts,
                        watchers,
                    );
                }
                Err(err) => {
                    for watcher in watchers {
                        let _ = watcher.response_tx.send(Err(err.clone()));
                    }
                }
            }
        }

        if !pending.is_empty() {
            metrics.record_read_watchers_added(placement.core_id, pending.len());
            read_watchers
                .entry(stream_id.clone())
                .or_default()
                .extend(pending);
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeMetrics {
    inner: Arc<RuntimeMetricsInner>,
}

impl RuntimeMetrics {
    pub fn snapshot(&self) -> RuntimeMetricsSnapshot {
        let per_core_appends = self
            .inner
            .per_core_appends
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let accepted_appends = per_core_appends.iter().sum();
        let per_group_appends = self
            .inner
            .per_group_appends
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_applied_mutations = self
            .inner
            .per_core_applied_mutations
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let applied_mutations = per_core_applied_mutations.iter().sum();
        let per_group_applied_mutations = self
            .inner
            .per_group_applied_mutations
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_mutation_apply_ns = self
            .inner
            .per_core_mutation_apply_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let mutation_apply_ns = per_core_mutation_apply_ns.iter().sum();
        let per_group_mutation_apply_ns = self
            .inner
            .per_group_mutation_apply_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_group_lock_wait_ns = self
            .inner
            .per_core_group_lock_wait_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let group_lock_wait_ns = per_core_group_lock_wait_ns.iter().sum();
        let per_group_group_lock_wait_ns = self
            .inner
            .per_group_group_lock_wait_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_group_engine_exec_ns = self
            .inner
            .per_core_group_engine_exec_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let group_engine_exec_ns = per_core_group_engine_exec_ns.iter().sum();
        let per_group_group_engine_exec_ns = self
            .inner
            .per_group_group_engine_exec_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_group_group_mailbox_depth = self
            .inner
            .per_group_group_mailbox_depth
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let group_mailbox_depth = per_group_group_mailbox_depth.iter().sum();
        let per_group_group_mailbox_max_depth = self
            .inner
            .per_group_group_mailbox_max_depth
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let group_mailbox_max_depth = per_group_group_mailbox_max_depth
            .iter()
            .copied()
            .max()
            .unwrap_or(0);
        let per_group_group_mailbox_full_events = self
            .inner
            .per_group_group_mailbox_full_events
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let group_mailbox_full_events = per_group_group_mailbox_full_events.iter().sum();
        let per_core_raft_write_many_batches = self
            .inner
            .per_core_raft_write_many_batches
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let raft_write_many_batches = per_core_raft_write_many_batches.iter().sum();
        let per_group_raft_write_many_batches = self
            .inner
            .per_group_raft_write_many_batches
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_raft_write_many_commands = self
            .inner
            .per_core_raft_write_many_commands
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let raft_write_many_commands = per_core_raft_write_many_commands.iter().sum();
        let per_group_raft_write_many_commands = self
            .inner
            .per_group_raft_write_many_commands
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_raft_write_many_logical_commands = self
            .inner
            .per_core_raft_write_many_logical_commands
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let raft_write_many_logical_commands =
            per_core_raft_write_many_logical_commands.iter().sum();
        let per_group_raft_write_many_logical_commands = self
            .inner
            .per_group_raft_write_many_logical_commands
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_raft_write_many_responses = self
            .inner
            .per_core_raft_write_many_responses
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let raft_write_many_responses = per_core_raft_write_many_responses.iter().sum();
        let per_group_raft_write_many_responses = self
            .inner
            .per_group_raft_write_many_responses
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_raft_write_many_submit_ns = self
            .inner
            .per_core_raft_write_many_submit_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let raft_write_many_submit_ns = per_core_raft_write_many_submit_ns.iter().sum();
        let per_group_raft_write_many_submit_ns = self
            .inner
            .per_group_raft_write_many_submit_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_raft_write_many_response_ns = self
            .inner
            .per_core_raft_write_many_response_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let raft_write_many_response_ns = per_core_raft_write_many_response_ns.iter().sum();
        let per_group_raft_write_many_response_ns = self
            .inner
            .per_group_raft_write_many_response_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_raft_apply_entries = self
            .inner
            .per_core_raft_apply_entries
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let raft_apply_entries = per_core_raft_apply_entries.iter().sum();
        let per_group_raft_apply_entries = self
            .inner
            .per_group_raft_apply_entries
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_raft_apply_ns = self
            .inner
            .per_core_raft_apply_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let raft_apply_ns = per_core_raft_apply_ns.iter().sum();
        let per_group_raft_apply_ns = self
            .inner
            .per_group_raft_apply_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_live_read_waiters = self
            .inner
            .per_core_live_read_waiters
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let live_read_waiters = per_core_live_read_waiters.iter().sum();
        let per_core_live_read_backpressure_events = self
            .inner
            .per_core_live_read_backpressure_events
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let live_read_backpressure_events = per_core_live_read_backpressure_events.iter().sum();
        let per_core_routed_requests = self
            .inner
            .per_core_routed_requests
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let routed_requests = per_core_routed_requests.iter().sum();
        let per_core_mailbox_send_wait_ns = self
            .inner
            .per_core_mailbox_send_wait_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let mailbox_send_wait_ns = per_core_mailbox_send_wait_ns.iter().sum();
        let per_core_mailbox_full_events = self
            .inner
            .per_core_mailbox_full_events
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let mailbox_full_events = per_core_mailbox_full_events.iter().sum();
        let per_core_wal_batches = self
            .inner
            .per_core_wal_batches
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let wal_batches = per_core_wal_batches.iter().sum();
        let per_group_wal_batches = self
            .inner
            .per_group_wal_batches
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_wal_records = self
            .inner
            .per_core_wal_records
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let wal_records = per_core_wal_records.iter().sum();
        let per_group_wal_records = self
            .inner
            .per_group_wal_records
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_wal_write_ns = self
            .inner
            .per_core_wal_write_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let wal_write_ns = per_core_wal_write_ns.iter().sum();
        let per_group_wal_write_ns = self
            .inner
            .per_group_wal_write_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let per_core_wal_sync_ns = self
            .inner
            .per_core_wal_sync_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let wal_sync_ns = per_core_wal_sync_ns.iter().sum();
        let per_group_wal_sync_ns = self
            .inner
            .per_group_wal_sync_ns
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let cold_flush_uploads = self.inner.cold_flush_uploads.load_relaxed();
        let cold_flush_upload_bytes = self.inner.cold_flush_upload_bytes.load_relaxed();
        let cold_flush_upload_ns = self.inner.cold_flush_upload_ns.load_relaxed();
        let cold_flush_publishes = self.inner.cold_flush_publishes.load_relaxed();
        let cold_flush_publish_bytes = self.inner.cold_flush_publish_bytes.load_relaxed();
        let cold_flush_publish_ns = self.inner.cold_flush_publish_ns.load_relaxed();
        let cold_orphan_cleanup_attempts = self.inner.cold_orphan_cleanup_attempts.load_relaxed();
        let cold_orphan_cleanup_errors = self.inner.cold_orphan_cleanup_errors.load_relaxed();
        let cold_orphan_bytes = self.inner.cold_orphan_bytes.load_relaxed();
        let per_group_cold_hot_bytes = self
            .inner
            .per_group_cold_hot_bytes
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let cold_hot_bytes = per_group_cold_hot_bytes.iter().sum();
        let per_group_cold_hot_bytes_max = self
            .inner
            .per_group_cold_hot_bytes_max
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let cold_hot_group_bytes_max = per_group_cold_hot_bytes_max
            .iter()
            .copied()
            .max()
            .unwrap_or(0);
        let cold_hot_stream_bytes_max = self.inner.cold_hot_stream_bytes_max.load_relaxed();
        let per_core_cold_backpressure_events = self
            .inner
            .per_core_cold_backpressure_events
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect::<Vec<_>>();
        let cold_backpressure_events = per_core_cold_backpressure_events.iter().sum();
        let per_group_cold_backpressure_events = self
            .inner
            .per_group_cold_backpressure_events
            .iter()
            .map(PaddedAtomicU64::load_relaxed)
            .collect();
        let cold_backpressure_bytes = self.inner.cold_backpressure_bytes.load_relaxed();

        RuntimeMetricsSnapshot {
            accepted_appends,
            per_core_appends,
            per_group_appends,
            applied_mutations,
            per_core_applied_mutations,
            per_group_applied_mutations,
            mutation_apply_ns,
            per_core_mutation_apply_ns,
            per_group_mutation_apply_ns,
            group_lock_wait_ns,
            per_core_group_lock_wait_ns,
            per_group_group_lock_wait_ns,
            group_engine_exec_ns,
            per_core_group_engine_exec_ns,
            per_group_group_engine_exec_ns,
            group_mailbox_depth,
            per_group_group_mailbox_depth,
            group_mailbox_max_depth,
            per_group_group_mailbox_max_depth,
            group_mailbox_full_events,
            per_group_group_mailbox_full_events,
            raft_write_many_batches,
            per_core_raft_write_many_batches,
            per_group_raft_write_many_batches,
            raft_write_many_commands,
            per_core_raft_write_many_commands,
            per_group_raft_write_many_commands,
            raft_write_many_logical_commands,
            per_core_raft_write_many_logical_commands,
            per_group_raft_write_many_logical_commands,
            raft_write_many_responses,
            per_core_raft_write_many_responses,
            per_group_raft_write_many_responses,
            raft_write_many_submit_ns,
            per_core_raft_write_many_submit_ns,
            per_group_raft_write_many_submit_ns,
            raft_write_many_response_ns,
            per_core_raft_write_many_response_ns,
            per_group_raft_write_many_response_ns,
            raft_apply_entries,
            per_core_raft_apply_entries,
            per_group_raft_apply_entries,
            raft_apply_ns,
            per_core_raft_apply_ns,
            per_group_raft_apply_ns,
            live_read_waiters,
            per_core_live_read_waiters,
            live_read_backpressure_events,
            per_core_live_read_backpressure_events,
            routed_requests,
            per_core_routed_requests,
            mailbox_send_wait_ns,
            per_core_mailbox_send_wait_ns,
            mailbox_full_events,
            per_core_mailbox_full_events,
            wal_batches,
            per_core_wal_batches,
            per_group_wal_batches,
            wal_records,
            per_core_wal_records,
            per_group_wal_records,
            wal_write_ns,
            per_core_wal_write_ns,
            per_group_wal_write_ns,
            wal_sync_ns,
            per_core_wal_sync_ns,
            per_group_wal_sync_ns,
            cold_flush_uploads,
            cold_flush_upload_bytes,
            cold_flush_upload_ns,
            cold_flush_publishes,
            cold_flush_publish_bytes,
            cold_flush_publish_ns,
            cold_orphan_cleanup_attempts,
            cold_orphan_cleanup_errors,
            cold_orphan_bytes,
            cold_hot_bytes,
            per_group_cold_hot_bytes,
            cold_hot_group_bytes_max,
            per_group_cold_hot_bytes_max,
            cold_hot_stream_bytes_max,
            cold_backpressure_events,
            per_core_cold_backpressure_events,
            per_group_cold_backpressure_events,
            cold_backpressure_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeMetricsSnapshot {
    pub accepted_appends: u64,
    pub per_core_appends: Vec<u64>,
    pub per_group_appends: Vec<u64>,
    pub applied_mutations: u64,
    pub per_core_applied_mutations: Vec<u64>,
    pub per_group_applied_mutations: Vec<u64>,
    pub mutation_apply_ns: u64,
    pub per_core_mutation_apply_ns: Vec<u64>,
    pub per_group_mutation_apply_ns: Vec<u64>,
    pub group_lock_wait_ns: u64,
    pub per_core_group_lock_wait_ns: Vec<u64>,
    pub per_group_group_lock_wait_ns: Vec<u64>,
    pub group_engine_exec_ns: u64,
    pub per_core_group_engine_exec_ns: Vec<u64>,
    pub per_group_group_engine_exec_ns: Vec<u64>,
    pub group_mailbox_depth: u64,
    pub per_group_group_mailbox_depth: Vec<u64>,
    pub group_mailbox_max_depth: u64,
    pub per_group_group_mailbox_max_depth: Vec<u64>,
    pub group_mailbox_full_events: u64,
    pub per_group_group_mailbox_full_events: Vec<u64>,
    pub raft_write_many_batches: u64,
    pub per_core_raft_write_many_batches: Vec<u64>,
    pub per_group_raft_write_many_batches: Vec<u64>,
    pub raft_write_many_commands: u64,
    pub per_core_raft_write_many_commands: Vec<u64>,
    pub per_group_raft_write_many_commands: Vec<u64>,
    pub raft_write_many_logical_commands: u64,
    pub per_core_raft_write_many_logical_commands: Vec<u64>,
    pub per_group_raft_write_many_logical_commands: Vec<u64>,
    pub raft_write_many_responses: u64,
    pub per_core_raft_write_many_responses: Vec<u64>,
    pub per_group_raft_write_many_responses: Vec<u64>,
    pub raft_write_many_submit_ns: u64,
    pub per_core_raft_write_many_submit_ns: Vec<u64>,
    pub per_group_raft_write_many_submit_ns: Vec<u64>,
    pub raft_write_many_response_ns: u64,
    pub per_core_raft_write_many_response_ns: Vec<u64>,
    pub per_group_raft_write_many_response_ns: Vec<u64>,
    pub raft_apply_entries: u64,
    pub per_core_raft_apply_entries: Vec<u64>,
    pub per_group_raft_apply_entries: Vec<u64>,
    pub raft_apply_ns: u64,
    pub per_core_raft_apply_ns: Vec<u64>,
    pub per_group_raft_apply_ns: Vec<u64>,
    pub live_read_waiters: u64,
    pub per_core_live_read_waiters: Vec<u64>,
    pub live_read_backpressure_events: u64,
    pub per_core_live_read_backpressure_events: Vec<u64>,
    pub routed_requests: u64,
    pub per_core_routed_requests: Vec<u64>,
    pub mailbox_send_wait_ns: u64,
    pub per_core_mailbox_send_wait_ns: Vec<u64>,
    pub mailbox_full_events: u64,
    pub per_core_mailbox_full_events: Vec<u64>,
    pub wal_batches: u64,
    pub per_core_wal_batches: Vec<u64>,
    pub per_group_wal_batches: Vec<u64>,
    pub wal_records: u64,
    pub per_core_wal_records: Vec<u64>,
    pub per_group_wal_records: Vec<u64>,
    pub wal_write_ns: u64,
    pub per_core_wal_write_ns: Vec<u64>,
    pub per_group_wal_write_ns: Vec<u64>,
    pub wal_sync_ns: u64,
    pub per_core_wal_sync_ns: Vec<u64>,
    pub per_group_wal_sync_ns: Vec<u64>,
    pub cold_flush_uploads: u64,
    pub cold_flush_upload_bytes: u64,
    pub cold_flush_upload_ns: u64,
    pub cold_flush_publishes: u64,
    pub cold_flush_publish_bytes: u64,
    pub cold_flush_publish_ns: u64,
    pub cold_orphan_cleanup_attempts: u64,
    pub cold_orphan_cleanup_errors: u64,
    pub cold_orphan_bytes: u64,
    pub cold_hot_bytes: u64,
    pub per_group_cold_hot_bytes: Vec<u64>,
    pub cold_hot_group_bytes_max: u64,
    pub per_group_cold_hot_bytes_max: Vec<u64>,
    pub cold_hot_stream_bytes_max: u64,
    pub cold_backpressure_events: u64,
    pub per_core_cold_backpressure_events: Vec<u64>,
    pub per_group_cold_backpressure_events: Vec<u64>,
    pub cold_backpressure_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeMailboxSnapshot {
    pub depths: Vec<usize>,
    pub capacities: Vec<usize>,
}

#[derive(Debug)]
struct RuntimeMetricsInner {
    per_core_appends: Vec<PaddedAtomicU64>,
    per_group_appends: Vec<PaddedAtomicU64>,
    per_core_applied_mutations: Vec<PaddedAtomicU64>,
    per_group_applied_mutations: Vec<PaddedAtomicU64>,
    per_core_mutation_apply_ns: Vec<PaddedAtomicU64>,
    per_group_mutation_apply_ns: Vec<PaddedAtomicU64>,
    per_core_group_lock_wait_ns: Vec<PaddedAtomicU64>,
    per_group_group_lock_wait_ns: Vec<PaddedAtomicU64>,
    per_core_group_engine_exec_ns: Vec<PaddedAtomicU64>,
    per_group_group_engine_exec_ns: Vec<PaddedAtomicU64>,
    per_group_group_mailbox_depth: Vec<PaddedAtomicU64>,
    per_group_group_mailbox_max_depth: Vec<PaddedAtomicU64>,
    per_group_group_mailbox_full_events: Vec<PaddedAtomicU64>,
    per_core_raft_write_many_batches: Vec<PaddedAtomicU64>,
    per_group_raft_write_many_batches: Vec<PaddedAtomicU64>,
    per_core_raft_write_many_commands: Vec<PaddedAtomicU64>,
    per_group_raft_write_many_commands: Vec<PaddedAtomicU64>,
    per_core_raft_write_many_logical_commands: Vec<PaddedAtomicU64>,
    per_group_raft_write_many_logical_commands: Vec<PaddedAtomicU64>,
    per_core_raft_write_many_responses: Vec<PaddedAtomicU64>,
    per_group_raft_write_many_responses: Vec<PaddedAtomicU64>,
    per_core_raft_write_many_submit_ns: Vec<PaddedAtomicU64>,
    per_group_raft_write_many_submit_ns: Vec<PaddedAtomicU64>,
    per_core_raft_write_many_response_ns: Vec<PaddedAtomicU64>,
    per_group_raft_write_many_response_ns: Vec<PaddedAtomicU64>,
    per_core_raft_apply_entries: Vec<PaddedAtomicU64>,
    per_group_raft_apply_entries: Vec<PaddedAtomicU64>,
    per_core_raft_apply_ns: Vec<PaddedAtomicU64>,
    per_group_raft_apply_ns: Vec<PaddedAtomicU64>,
    per_core_live_read_waiters: Vec<PaddedAtomicU64>,
    per_core_live_read_backpressure_events: Vec<PaddedAtomicU64>,
    per_core_routed_requests: Vec<PaddedAtomicU64>,
    per_core_mailbox_send_wait_ns: Vec<PaddedAtomicU64>,
    per_core_mailbox_full_events: Vec<PaddedAtomicU64>,
    per_core_wal_batches: Vec<PaddedAtomicU64>,
    per_group_wal_batches: Vec<PaddedAtomicU64>,
    per_core_wal_records: Vec<PaddedAtomicU64>,
    per_group_wal_records: Vec<PaddedAtomicU64>,
    per_core_wal_write_ns: Vec<PaddedAtomicU64>,
    per_group_wal_write_ns: Vec<PaddedAtomicU64>,
    per_core_wal_sync_ns: Vec<PaddedAtomicU64>,
    per_group_wal_sync_ns: Vec<PaddedAtomicU64>,
    cold_flush_uploads: PaddedAtomicU64,
    cold_flush_upload_bytes: PaddedAtomicU64,
    cold_flush_upload_ns: PaddedAtomicU64,
    cold_flush_publishes: PaddedAtomicU64,
    cold_flush_publish_bytes: PaddedAtomicU64,
    cold_flush_publish_ns: PaddedAtomicU64,
    cold_orphan_cleanup_attempts: PaddedAtomicU64,
    cold_orphan_cleanup_errors: PaddedAtomicU64,
    cold_orphan_bytes: PaddedAtomicU64,
    per_group_cold_hot_bytes: Vec<PaddedAtomicU64>,
    per_group_cold_hot_bytes_max: Vec<PaddedAtomicU64>,
    cold_hot_stream_bytes_max: PaddedAtomicU64,
    per_core_cold_backpressure_events: Vec<PaddedAtomicU64>,
    per_group_cold_backpressure_events: Vec<PaddedAtomicU64>,
    cold_backpressure_bytes: PaddedAtomicU64,
}

#[derive(Debug, Clone, Copy)]
struct RaftWriteManySample {
    command_count: u64,
    logical_command_count: u64,
    response_count: u64,
    submit_ns: u64,
    response_ns: u64,
}

impl RuntimeMetricsInner {
    fn new(core_count: usize, raft_group_count: usize) -> Self {
        Self {
            per_core_appends: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_appends: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_applied_mutations: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_applied_mutations: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_mutation_apply_ns: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_mutation_apply_ns: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_group_lock_wait_ns: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_group_lock_wait_ns: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_group_engine_exec_ns: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_group_engine_exec_ns: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_group_mailbox_depth: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_group_mailbox_max_depth: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_group_mailbox_full_events: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_raft_write_many_batches: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_raft_write_many_batches: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_raft_write_many_commands: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_raft_write_many_commands: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_raft_write_many_logical_commands: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_raft_write_many_logical_commands: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_raft_write_many_responses: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_raft_write_many_responses: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_raft_write_many_submit_ns: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_raft_write_many_submit_ns: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_raft_write_many_response_ns: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_raft_write_many_response_ns: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_raft_apply_entries: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_raft_apply_entries: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_raft_apply_ns: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_raft_apply_ns: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_live_read_waiters: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_core_live_read_backpressure_events: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_routed_requests: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_core_mailbox_send_wait_ns: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_mailbox_full_events: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_wal_batches: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_wal_batches: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_wal_records: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_wal_records: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_wal_write_ns: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_wal_write_ns: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_core_wal_sync_ns: (0..core_count).map(|_| PaddedAtomicU64::new(0)).collect(),
            per_group_wal_sync_ns: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            cold_flush_uploads: PaddedAtomicU64::new(0),
            cold_flush_upload_bytes: PaddedAtomicU64::new(0),
            cold_flush_upload_ns: PaddedAtomicU64::new(0),
            cold_flush_publishes: PaddedAtomicU64::new(0),
            cold_flush_publish_bytes: PaddedAtomicU64::new(0),
            cold_flush_publish_ns: PaddedAtomicU64::new(0),
            cold_orphan_cleanup_attempts: PaddedAtomicU64::new(0),
            cold_orphan_cleanup_errors: PaddedAtomicU64::new(0),
            cold_orphan_bytes: PaddedAtomicU64::new(0),
            per_group_cold_hot_bytes: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_cold_hot_bytes_max: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            cold_hot_stream_bytes_max: PaddedAtomicU64::new(0),
            per_core_cold_backpressure_events: (0..core_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            per_group_cold_backpressure_events: (0..raft_group_count)
                .map(|_| PaddedAtomicU64::new(0))
                .collect(),
            cold_backpressure_bytes: PaddedAtomicU64::new(0),
        }
    }

    fn record_routed_request(&self, core_id: CoreId, mailbox_send_wait_ns: u64) {
        let index = usize::from(core_id.0);
        self.per_core_routed_requests[index].fetch_add_relaxed(1);
        self.per_core_mailbox_send_wait_ns[index].fetch_add_relaxed(mailbox_send_wait_ns);
    }

    fn record_mailbox_full(&self, core_id: CoreId) {
        self.per_core_mailbox_full_events[usize::from(core_id.0)].fetch_add_relaxed(1);
    }

    fn record_append(&self, core_id: CoreId, group_id: RaftGroupId) {
        self.record_append_batch(core_id, group_id, 1);
    }

    fn record_append_batch(&self, core_id: CoreId, group_id: RaftGroupId, count: u64) {
        self.per_core_appends[usize::from(core_id.0)].fetch_add_relaxed(count);
        self.per_group_appends[usize::try_from(group_id.0).expect("u32 fits usize")]
            .fetch_add_relaxed(count);
    }

    fn record_applied_mutation(&self, core_id: CoreId, group_id: RaftGroupId, apply_ns: u64) {
        self.record_applied_mutation_batch(core_id, group_id, 1, apply_ns);
    }

    fn record_applied_mutation_batch(
        &self,
        core_id: CoreId,
        group_id: RaftGroupId,
        count: u64,
        apply_ns: u64,
    ) {
        let core_index = usize::from(core_id.0);
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_core_applied_mutations[core_index].fetch_add_relaxed(count);
        self.per_group_applied_mutations[group_index].fetch_add_relaxed(count);
        self.per_core_mutation_apply_ns[core_index].fetch_add_relaxed(apply_ns);
        self.per_group_mutation_apply_ns[group_index].fetch_add_relaxed(apply_ns);
    }

    fn record_group_engine_exec(&self, core_id: CoreId, group_id: RaftGroupId, exec_ns: u64) {
        let core_index = usize::from(core_id.0);
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_core_group_engine_exec_ns[core_index].fetch_add_relaxed(exec_ns);
        self.per_group_group_engine_exec_ns[group_index].fetch_add_relaxed(exec_ns);
    }

    fn record_group_mailbox_enqueued(&self, group_id: RaftGroupId) {
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        let depth = self.per_group_group_mailbox_depth[group_index].fetch_add_relaxed(1) + 1;
        self.per_group_group_mailbox_max_depth[group_index].fetch_max_relaxed(depth);
    }

    fn record_group_mailbox_dequeued(&self, group_id: RaftGroupId) {
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_group_group_mailbox_depth[group_index].fetch_sub_relaxed(1);
    }

    fn record_group_mailbox_full(&self, group_id: RaftGroupId) {
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_group_group_mailbox_full_events[group_index].fetch_add_relaxed(1);
    }

    fn record_raft_write_many(
        &self,
        core_id: CoreId,
        group_id: RaftGroupId,
        sample: RaftWriteManySample,
    ) {
        let core_index = usize::from(core_id.0);
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_core_raft_write_many_batches[core_index].fetch_add_relaxed(1);
        self.per_group_raft_write_many_batches[group_index].fetch_add_relaxed(1);
        self.per_core_raft_write_many_commands[core_index].fetch_add_relaxed(sample.command_count);
        self.per_group_raft_write_many_commands[group_index]
            .fetch_add_relaxed(sample.command_count);
        self.per_core_raft_write_many_logical_commands[core_index]
            .fetch_add_relaxed(sample.logical_command_count);
        self.per_group_raft_write_many_logical_commands[group_index]
            .fetch_add_relaxed(sample.logical_command_count);
        self.per_core_raft_write_many_responses[core_index]
            .fetch_add_relaxed(sample.response_count);
        self.per_group_raft_write_many_responses[group_index]
            .fetch_add_relaxed(sample.response_count);
        self.per_core_raft_write_many_submit_ns[core_index].fetch_add_relaxed(sample.submit_ns);
        self.per_group_raft_write_many_submit_ns[group_index].fetch_add_relaxed(sample.submit_ns);
        self.per_core_raft_write_many_response_ns[core_index].fetch_add_relaxed(sample.response_ns);
        self.per_group_raft_write_many_response_ns[group_index]
            .fetch_add_relaxed(sample.response_ns);
    }

    fn record_raft_apply_batch(
        &self,
        core_id: CoreId,
        group_id: RaftGroupId,
        entry_count: u64,
        apply_ns: u64,
    ) {
        let core_index = usize::from(core_id.0);
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_core_raft_apply_entries[core_index].fetch_add_relaxed(entry_count);
        self.per_group_raft_apply_entries[group_index].fetch_add_relaxed(entry_count);
        self.per_core_raft_apply_ns[core_index].fetch_add_relaxed(apply_ns);
        self.per_group_raft_apply_ns[group_index].fetch_add_relaxed(apply_ns);
    }

    fn record_wal_batch(
        &self,
        core_id: CoreId,
        group_id: RaftGroupId,
        record_count: u64,
        write_ns: u64,
        sync_ns: u64,
    ) {
        let core_index = usize::from(core_id.0);
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_core_wal_batches[core_index].fetch_add_relaxed(1);
        self.per_group_wal_batches[group_index].fetch_add_relaxed(1);
        self.per_core_wal_records[core_index].fetch_add_relaxed(record_count);
        self.per_group_wal_records[group_index].fetch_add_relaxed(record_count);
        self.per_core_wal_write_ns[core_index].fetch_add_relaxed(write_ns);
        self.per_group_wal_write_ns[group_index].fetch_add_relaxed(write_ns);
        self.per_core_wal_sync_ns[core_index].fetch_add_relaxed(sync_ns);
        self.per_group_wal_sync_ns[group_index].fetch_add_relaxed(sync_ns);
    }

    fn record_cold_upload(&self, bytes: u64, upload_ns: u64) {
        self.cold_flush_uploads.fetch_add_relaxed(1);
        self.cold_flush_upload_bytes.fetch_add_relaxed(bytes);
        self.cold_flush_upload_ns.fetch_add_relaxed(upload_ns);
    }

    fn record_cold_publish(&self, bytes: u64, publish_ns: u64) {
        self.cold_flush_publishes.fetch_add_relaxed(1);
        self.cold_flush_publish_bytes.fetch_add_relaxed(bytes);
        self.cold_flush_publish_ns.fetch_add_relaxed(publish_ns);
    }

    fn record_cold_orphan_cleanup(&self, bytes: u64, cleanup_failed: bool) {
        self.cold_orphan_cleanup_attempts.fetch_add_relaxed(1);
        if cleanup_failed {
            self.cold_orphan_cleanup_errors.fetch_add_relaxed(1);
            self.cold_orphan_bytes.fetch_add_relaxed(bytes);
        }
    }

    fn record_cold_hot_backlog(
        &self,
        group_id: RaftGroupId,
        stream_hot_bytes: u64,
        group_hot_bytes: u64,
    ) {
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_group_cold_hot_bytes[group_index].store_relaxed(group_hot_bytes);
        self.per_group_cold_hot_bytes_max[group_index].fetch_max_relaxed(group_hot_bytes);
        self.cold_hot_stream_bytes_max
            .fetch_max_relaxed(stream_hot_bytes);
    }

    fn record_cold_backpressure(
        &self,
        core_id: CoreId,
        group_id: RaftGroupId,
        incoming_bytes: u64,
        _limit: u64,
    ) {
        let core_index = usize::from(core_id.0);
        let group_index = usize::try_from(group_id.0).expect("u32 fits usize");
        self.per_core_cold_backpressure_events[core_index].fetch_add_relaxed(1);
        self.per_group_cold_backpressure_events[group_index].fetch_add_relaxed(1);
        self.cold_backpressure_bytes
            .fetch_add_relaxed(incoming_bytes);
    }

    fn record_read_watcher_added(&self, core_id: CoreId) {
        self.record_read_watchers_added(core_id, 1);
    }

    fn record_read_watchers_added(&self, core_id: CoreId, count: usize) {
        self.per_core_live_read_waiters[usize::from(core_id.0)]
            .fetch_add_relaxed(u64::try_from(count).expect("watcher count fits u64"));
    }

    fn record_read_watchers_removed(&self, core_id: CoreId, count: usize) {
        self.per_core_live_read_waiters[usize::from(core_id.0)]
            .fetch_sub_relaxed(u64::try_from(count).expect("watcher count fits u64"));
    }

    fn record_live_read_backpressure(&self, core_id: CoreId) {
        self.per_core_live_read_backpressure_events[usize::from(core_id.0)].fetch_add_relaxed(1);
    }
}

fn elapsed_ns(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn append_batch_payload_bytes(request: &AppendBatchRequest) -> u64 {
    request
        .payloads
        .iter()
        .map(|payload| u64::try_from(payload.len()).expect("payload len fits u64"))
        .sum()
}

fn record_cold_backpressure_error(
    metrics: &RuntimeMetricsInner,
    placement: ShardPlacement,
    incoming_bytes: u64,
    admission: ColdWriteAdmission,
    err: &GroupEngineError,
) {
    if !err.message().contains("ColdBackpressure") {
        return;
    }
    metrics.record_cold_backpressure(
        placement.core_id,
        placement.raft_group_id,
        incoming_bytes,
        admission.max_hot_bytes_per_group.unwrap_or(0),
    );
}

fn is_stale_cold_flush_candidate_error(err: &RuntimeError) -> bool {
    let RuntimeError::GroupEngine { message, .. } = err else {
        return false;
    };
    message.contains("StreamGone")
        || message.contains("StreamNotFound")
        || (message.contains("InvalidColdFlush")
            && (message.contains("beyond stream")
                || message.contains("does not match the start of a hot payload segment")
                || message.contains("does not cover contiguous hot payload segments")
                || message.contains("exceeds stream")
                || message.contains("non-contiguous hot payload metadata")))
}

async fn record_cold_hot_backlog(
    group: &mut Box<dyn GroupEngine>,
    metrics: &RuntimeMetricsInner,
    stream_id: BucketStreamId,
    placement: ShardPlacement,
) {
    if let Ok(backlog) = group.cold_hot_backlog(stream_id, placement).await {
        metrics.record_cold_hot_backlog(
            placement.raft_group_id,
            backlog.stream_hot_bytes,
            backlog.group_hot_bytes,
        );
    }
}

#[derive(Debug)]
#[repr(align(128))]
struct PaddedAtomicU64 {
    value: AtomicU64,
}

impl PaddedAtomicU64 {
    fn new(value: u64) -> Self {
        Self {
            value: AtomicU64::new(value),
        }
    }

    fn load_relaxed(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    fn fetch_add_relaxed(&self, value: u64) -> u64 {
        self.value.fetch_add(value, Ordering::Relaxed)
    }

    fn fetch_sub_relaxed(&self, value: u64) {
        self.value.fetch_sub(value, Ordering::Relaxed);
    }

    fn fetch_max_relaxed(&self, value: u64) {
        self.value.fetch_max(value, Ordering::Relaxed);
    }

    fn store_relaxed(&self, value: u64) {
        self.value.store(value, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use tokio::sync::Notify;

    fn runtime(core_count: usize, group_count: usize) -> ShardRuntime {
        ShardRuntime::spawn(RuntimeConfig {
            core_count,
            raft_group_count: group_count,
            mailbox_capacity: 128,
            threading: RuntimeThreading::HostedTokio,
            cold_max_hot_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        })
        .expect("spawn runtime")
    }

    fn stream_on_group(
        runtime: &ShardRuntime,
        group_id: RaftGroupId,
        prefix: &str,
    ) -> BucketStreamId {
        for index in 0..10_000 {
            let stream = BucketStreamId::new("benchcmp", format!("{prefix}-{index}"));
            if runtime.locate(&stream).raft_group_id == group_id {
                return stream;
            }
        }
        panic!("could not find stream for group {}", group_id.0);
    }

    async fn create_stream(
        runtime: &ShardRuntime,
        stream: &BucketStreamId,
    ) -> CreateStreamResponse {
        runtime
            .create_stream(CreateStreamRequest::new(
                stream.clone(),
                DEFAULT_CONTENT_TYPE,
            ))
            .await
            .expect("create stream")
    }

    fn producer(id: &str, epoch: u64, seq: u64) -> ProducerRequest {
        ProducerRequest {
            producer_id: id.to_owned(),
            producer_epoch: epoch,
            producer_seq: seq,
        }
    }

    fn placement() -> ShardPlacement {
        ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(0),
        }
    }

    #[test]
    fn group_write_command_round_trips_as_log_payload() {
        let command = GroupWriteCommand::AppendBatch {
            stream_id: BucketStreamId::new("benchcmp", "raft-log"),
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            payloads: vec![Bytes::from_static(b"ab"), Bytes::from_static(b"cd")],
            producer: Some(producer("writer-1", 7, 42)),
            now_ms: 0,
        };

        let encoded = serde_json::to_vec(&command).expect("encode command");
        let decoded =
            serde_json::from_slice::<GroupWriteCommand>(&encoded).expect("decode command");

        assert_eq!(decoded, command);
    }

    #[test]
    fn committed_write_command_is_state_machine_apply_boundary() {
        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(0),
        };
        let stream = BucketStreamId::new("benchcmp", "apply-command");
        let mut engine = InMemoryGroupEngine::default();

        let created = engine
            .apply_committed_write(
                GroupWriteCommand::CreateStream {
                    stream_id: stream.clone(),
                    content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                    initial_payload: Bytes::new(),
                    close_after: false,
                    stream_seq: None,
                    producer: None,
                    stream_ttl_seconds: None,
                    stream_expires_at_ms: None,
                    forked_from: None,
                    fork_offset: None,
                    now_ms: 0,
                },
                placement,
            )
            .expect("create stream");
        assert_eq!(
            created,
            GroupWriteResponse::CreateStream(CreateStreamResponse {
                placement,
                next_offset: 0,
                closed: false,
                already_exists: false,
                group_commit_index: 1,
            })
        );

        let appended = engine
            .apply_committed_write(
                GroupWriteCommand::Append {
                    stream_id: stream.clone(),
                    content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                    payload: Bytes::from_static(b"abc"),
                    close_after: false,
                    stream_seq: None,
                    producer: None,
                    now_ms: 0,
                },
                placement,
            )
            .expect("append");
        assert_eq!(
            appended,
            GroupWriteResponse::Append(AppendResponse {
                placement,
                start_offset: 0,
                next_offset: 3,
                stream_append_count: 1,
                group_commit_index: 2,
                closed: false,
                deduplicated: false,
                producer: None,
            })
        );

        let flushed = engine
            .apply_committed_write(
                GroupWriteCommand::FlushCold {
                    stream_id: stream.clone(),
                    chunk: ColdChunkRef {
                        start_offset: 0,
                        end_offset: 2,
                        s3_path: "s3://bucket/apply-command/000000".to_owned(),
                        object_size: 2,
                    },
                },
                placement,
            )
            .expect("flush cold");
        assert_eq!(
            flushed,
            GroupWriteResponse::FlushCold(FlushColdResponse {
                placement,
                hot_start_offset: 2,
                group_commit_index: 3,
            })
        );

        let read = engine
            .state_machine
            .read(&stream, 2, 16)
            .expect("read applied command");
        assert_eq!(read.payload, b"c");
        let plan = engine
            .state_machine
            .read_plan(&stream, 0, 16)
            .expect("read plan");
        assert_eq!(plan.segments.len(), 2);
        assert!(matches!(plan.segments[0], StreamReadSegment::Object(_)));
        assert_eq!(plan.segments[1], StreamReadSegment::Hot(b"c".to_vec()));
    }

    #[tokio::test]
    async fn cold_store_read_reassembles_cold_and_hot_segments() {
        let placement = placement();
        let stream = BucketStreamId::new("benchcmp", "cold-read");
        let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
        cold_store
            .write_chunk("benchcmp/cold-read/chunks/000000.bin", b"abcd")
            .await
            .expect("write cold object");
        let mut engine = InMemoryGroupEngine::with_cold_store(cold_store);

        engine
            .apply_committed_write(
                GroupWriteCommand::CreateStream {
                    stream_id: stream.clone(),
                    content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                    initial_payload: Bytes::new(),
                    close_after: false,
                    stream_seq: None,
                    producer: None,
                    stream_ttl_seconds: None,
                    stream_expires_at_ms: None,
                    forked_from: None,
                    fork_offset: None,
                    now_ms: 0,
                },
                placement,
            )
            .expect("create stream");
        engine
            .apply_committed_write(
                GroupWriteCommand::Append {
                    stream_id: stream.clone(),
                    content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                    payload: Bytes::from_static(b"abcdef"),
                    close_after: false,
                    stream_seq: None,
                    producer: None,
                    now_ms: 0,
                },
                placement,
            )
            .expect("append");
        engine
            .apply_committed_write(
                GroupWriteCommand::FlushCold {
                    stream_id: stream.clone(),
                    chunk: ColdChunkRef {
                        start_offset: 0,
                        end_offset: 4,
                        s3_path: "benchcmp/cold-read/chunks/000000.bin".to_owned(),
                        object_size: 4,
                    },
                },
                placement,
            )
            .expect("flush cold");

        let read = engine
            .read_stream(
                ReadStreamRequest {
                    stream_id: stream,
                    offset: 2,
                    max_len: 4,
                    now_ms: 0,
                },
                placement,
            )
            .await
            .expect("read cold and hot segments");
        assert_eq!(read.payload, b"cdef");
        assert_eq!(read.next_offset, 6);
        assert!(read.up_to_date);
    }

    #[tokio::test]
    async fn bootstrap_reads_retained_updates_from_cold_chunk_after_snapshot() {
        let placement = placement();
        let stream = BucketStreamId::new("benchcmp", "cold-bootstrap");
        let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
        cold_store
            .write_chunk("benchcmp/cold-bootstrap/chunks/000000.bin", b"abcde")
            .await
            .expect("write cold object");
        let mut engine = InMemoryGroupEngine::with_cold_store(cold_store);

        engine
            .create_stream(
                CreateStreamRequest::new(stream.clone(), DEFAULT_CONTENT_TYPE),
                placement,
            )
            .await
            .expect("create stream");
        engine
            .append(
                AppendRequest::from_bytes(stream.clone(), b"abc".to_vec()),
                placement,
            )
            .await
            .expect("append first message");
        engine
            .append(
                AppendRequest::from_bytes(stream.clone(), b"de".to_vec()),
                placement,
            )
            .await
            .expect("append second message");
        engine
            .flush_cold(
                FlushColdRequest {
                    stream_id: stream.clone(),
                    chunk: ColdChunkRef {
                        start_offset: 0,
                        end_offset: 5,
                        s3_path: "benchcmp/cold-bootstrap/chunks/000000.bin".to_owned(),
                        object_size: 5,
                    },
                },
                placement,
            )
            .await
            .expect("flush all hot bytes");
        engine
            .publish_snapshot(
                PublishSnapshotRequest {
                    stream_id: stream.clone(),
                    snapshot_offset: 3,
                    content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                    payload: Bytes::from_static(b"abc-state"),
                    now_ms: 0,
                },
                placement,
            )
            .await
            .expect("publish snapshot");

        let read = engine
            .read_stream(
                ReadStreamRequest {
                    stream_id: stream.clone(),
                    offset: 3,
                    max_len: 2,
                    now_ms: 0,
                },
                placement,
            )
            .await
            .expect("read retained update from cold chunk");
        assert_eq!(read.payload, b"de");

        let bootstrap = engine
            .bootstrap_stream(
                BootstrapStreamRequest {
                    stream_id: stream,
                    now_ms: 0,
                },
                placement,
            )
            .await
            .expect("bootstrap");
        assert_eq!(bootstrap.snapshot_offset, Some(3));
        assert_eq!(bootstrap.snapshot_payload, b"abc-state");
        assert_eq!(bootstrap.next_offset, 5);
        assert_eq!(bootstrap.updates.len(), 1);
        assert_eq!(bootstrap.updates[0].start_offset, 3);
        assert_eq!(bootstrap.updates[0].next_offset, 5);
        assert_eq!(bootstrap.updates[0].payload, b"de");
    }

    #[tokio::test]
    async fn cold_store_reads_only_requested_range() {
        let cold_store = ColdStore::memory().expect("memory cold store");
        cold_store
            .write_chunk("benchcmp/cold-range/chunks/000000.bin", b"abcdefgh")
            .await
            .expect("write cold object");
        let bytes = cold_store
            .read_chunk_range(
                &ColdChunkRef {
                    start_offset: 10,
                    end_offset: 18,
                    s3_path: "benchcmp/cold-range/chunks/000000.bin".to_owned(),
                    object_size: 8,
                },
                12,
                3,
            )
            .await
            .expect("read range");
        assert_eq!(bytes, b"cde");
    }

    #[tokio::test]
    async fn ttl_read_access_is_committed_and_expiry_removes_stream() {
        let placement = placement();
        let stream = BucketStreamId::new("benchcmp", "runtime-ttl");
        let mut engine = InMemoryGroupEngine::default();

        let mut create = CreateStreamRequest::new(stream.clone(), DEFAULT_CONTENT_TYPE);
        create.initial_payload = Bytes::from_static(b"abc");
        create.stream_ttl_seconds = Some(1);
        create.now_ms = 1_000;
        engine
            .create_stream(create, placement)
            .await
            .expect("create ttl stream");

        let read = engine
            .read_stream(
                ReadStreamRequest {
                    stream_id: stream.clone(),
                    offset: 0,
                    max_len: 16,
                    now_ms: 1_500,
                },
                placement,
            )
            .await
            .expect("read renews ttl");
        assert_eq!(read.payload, b"abc");
        assert_eq!(
            engine
                .snapshot(placement)
                .await
                .expect("snapshot")
                .group_commit_index,
            2
        );

        engine
            .head_stream(
                HeadStreamRequest {
                    stream_id: stream.clone(),
                    now_ms: 2_499,
                },
                placement,
            )
            .await
            .expect("head does not renew but stream is still live");
        assert_eq!(
            engine
                .snapshot(placement)
                .await
                .expect("snapshot")
                .group_commit_index,
            2
        );

        let err = engine
            .read_stream(
                ReadStreamRequest {
                    stream_id: stream.clone(),
                    offset: 0,
                    max_len: 16,
                    now_ms: 2_500,
                },
                placement,
            )
            .await
            .expect_err("expired stream read is not found");
        assert_eq!(err.code(), Some(StreamErrorCode::StreamNotFound));
        assert_eq!(
            engine
                .snapshot(placement)
                .await
                .expect("snapshot")
                .group_commit_index,
            3
        );

        let mut recreate = CreateStreamRequest::new(stream, "text/plain");
        recreate.now_ms = 2_501;
        let recreated = engine
            .create_stream(recreate, placement)
            .await
            .expect("recreate expired stream");
        assert!(!recreated.already_exists);
    }

    #[test]
    fn committed_write_batch_preserves_logical_command_responses() {
        let placement = placement();
        let stream = BucketStreamId::new("benchcmp", "apply-command-batch");
        let mut engine = InMemoryGroupEngine::default();

        let response = engine
            .apply_committed_write(
                GroupWriteCommand::Batch {
                    commands: vec![
                        GroupWriteCommand::from(CreateStreamRequest::new(
                            stream.clone(),
                            DEFAULT_CONTENT_TYPE,
                        )),
                        GroupWriteCommand::from(AppendBatchRequest::new(
                            stream.clone(),
                            vec![Bytes::from_static(b"ab"), Bytes::from_static(b"cd")],
                        )),
                    ],
                },
                placement,
            )
            .expect("apply command batch");

        let GroupWriteResponse::Batch(items) = response else {
            panic!("unexpected batch response: {response:?}");
        };
        assert_eq!(items.len(), 2);
        assert!(matches!(
            &items[0],
            Ok(GroupWriteResponse::CreateStream(CreateStreamResponse {
                group_commit_index: 1,
                ..
            }))
        ));
        match &items[1] {
            Ok(GroupWriteResponse::AppendBatch(response)) => {
                assert_eq!(response.items.len(), 2);
                assert_eq!(
                    response.items[0].as_ref().expect("first item").start_offset,
                    0
                );
                assert_eq!(
                    response.items[1]
                        .as_ref()
                        .expect("second item")
                        .start_offset,
                    2
                );
                assert_eq!(
                    response.items[1]
                        .as_ref()
                        .expect("second item")
                        .group_commit_index,
                    3
                );
            }
            other => panic!("unexpected append batch response: {other:?}"),
        }

        let read = engine
            .state_machine
            .read(&stream, 0, 16)
            .expect("read applied command batch");
        assert_eq!(read.payload, b"abcd");
    }

    async fn wait_for_live_waiters(runtime: &ShardRuntime, expected: u64) {
        for _ in 0..100 {
            if runtime.metrics().snapshot().live_read_waiters == expected {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!(
            "expected {expected} live waiters, got {}",
            runtime.metrics().snapshot().live_read_waiters
        );
    }

    async fn wait_for_mailbox_depth(runtime: &ShardRuntime, core_index: usize, expected: usize) {
        for _ in 0..100 {
            if runtime.mailbox_snapshot().depths[core_index] == expected {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!(
            "expected core {core_index} mailbox depth {expected}, got {}",
            runtime.mailbox_snapshot().depths[core_index]
        );
    }

    async fn wait_for_mailbox_full_events(runtime: &ShardRuntime, expected: u64) {
        for _ in 0..100 {
            if runtime.metrics().snapshot().mailbox_full_events == expected {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!(
            "expected {expected} mailbox full events, got {}",
            runtime.metrics().snapshot().mailbox_full_events
        );
    }

    async fn wait_for_group_mailbox_full_events(runtime: &ShardRuntime, expected: u64) {
        for _ in 0..100 {
            if runtime.metrics().snapshot().group_mailbox_full_events == expected {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!(
            "expected {expected} group mailbox full events, got {}",
            runtime.metrics().snapshot().group_mailbox_full_events
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn repeated_appends_to_one_stream_are_ordered() {
        let runtime = runtime(4, 32);
        let stream = BucketStreamId::new("benchcmp", "one-stream");
        create_stream(&runtime, &stream).await;
        for index in 0..100 {
            let response = runtime
                .append(AppendRequest::new(stream.clone(), 7))
                .await
                .expect("append");
            assert_eq!(response.start_offset, index * 7);
            assert_eq!(response.next_offset, (index + 1) * 7);
            assert_eq!(response.stream_append_count, index + 1);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn independent_streams_reach_all_cores_and_many_groups() {
        let runtime = runtime(4, 64);
        let mut tasks = Vec::new();
        for index in 0..4096 {
            let runtime = runtime.clone();
            tasks.push(tokio::spawn(async move {
                let stream = BucketStreamId::new("benchcmp", format!("stream-{index}"));
                create_stream(&runtime, &stream).await;
                runtime
                    .append(AppendRequest::new(stream, 1))
                    .await
                    .expect("append")
            }));
        }

        for task in tasks {
            let response = task.await.expect("task");
            assert_eq!(response.start_offset, 0);
            assert_eq!(response.next_offset, 1);
        }

        let snapshot = runtime.metrics().snapshot();
        assert_eq!(snapshot.accepted_appends, 4096);
        assert!(snapshot.per_core_appends.iter().all(|value| *value > 0));
        let active_groups = snapshot
            .per_group_appends
            .iter()
            .filter(|value| **value > 0)
            .count();
        assert!(active_groups > 48, "active_groups={active_groups}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn empty_append_is_rejected_before_routing() {
        let runtime = runtime(2, 8);
        let err = runtime
            .append(AppendRequest::new(BucketStreamId::new("b", "s"), 0))
            .await
            .expect_err("empty append rejected");
        assert_eq!(err, RuntimeError::EmptyAppend);
        assert_eq!(runtime.metrics().snapshot().accepted_appends, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn append_batch_routes_once_and_applies_each_payload_on_owner_core() {
        let runtime = runtime(2, 8);
        let stream = BucketStreamId::new("benchcmp", "batch-runtime");
        let owner_core = usize::from(runtime.locate(&stream).core_id.0);
        let owner_group =
            usize::try_from(runtime.locate(&stream).raft_group_id.0).expect("u32 fits usize");

        create_stream(&runtime, &stream).await;
        let response = runtime
            .append_batch(AppendBatchRequest::new(
                stream.clone(),
                vec![b"ab".to_vec(), b"c".to_vec(), b"def".to_vec()],
            ))
            .await
            .expect("append batch");
        assert_eq!(response.items.len(), 3);
        assert_eq!(response.items[0].as_ref().expect("first").start_offset, 0);
        assert_eq!(response.items[1].as_ref().expect("second").start_offset, 2);
        assert_eq!(response.items[2].as_ref().expect("third").start_offset, 3);

        let read = runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream.clone(),
                offset: 0,
                max_len: 16,
                now_ms: 0,
            })
            .await
            .expect("read");
        assert_eq!(read.payload, b"abcdef");

        let snapshot = runtime.metrics().snapshot();
        assert_eq!(snapshot.accepted_appends, 3);
        assert_eq!(snapshot.applied_mutations, 4);
        assert_eq!(snapshot.routed_requests, 3);
        assert_eq!(snapshot.per_core_appends[owner_core], 3);
        assert_eq!(snapshot.per_group_appends[owner_group], 3);
        assert_eq!(snapshot.per_core_applied_mutations[owner_core], 4);
        assert_eq!(snapshot.per_group_applied_mutations[owner_group], 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn append_batch_reports_item_errors_without_stopping_later_payloads() {
        let runtime = runtime(2, 8);
        let stream = BucketStreamId::new("benchcmp", "batch-partial");
        create_stream(&runtime, &stream).await;

        let response = runtime
            .append_batch(AppendBatchRequest::new(
                stream.clone(),
                vec![b"a".to_vec(), Vec::new(), b"b".to_vec()],
            ))
            .await
            .expect("append batch");
        assert!(response.items[0].is_ok());
        assert!(response.items[1].is_err());
        assert!(response.items[2].is_ok());
        assert_eq!(response.items[0].as_ref().expect("first").start_offset, 0);
        assert_eq!(response.items[2].as_ref().expect("third").start_offset, 1);

        let read = runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream,
                offset: 0,
                max_len: 16,
                now_ms: 0,
            })
            .await
            .expect("read");
        assert_eq!(read.payload, b"ab");

        let snapshot = runtime.metrics().snapshot();
        assert_eq!(snapshot.accepted_appends, 2);
        assert_eq!(snapshot.applied_mutations, 3);
        assert_eq!(snapshot.routed_requests, 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn producer_duplicate_append_returns_prior_offsets_without_mutating_metrics() {
        let runtime = runtime(2, 8);
        let stream = BucketStreamId::new("benchcmp", "producer-runtime");
        create_stream(&runtime, &stream).await;

        let mut first = AppendRequest::from_bytes(stream.clone(), b"a".to_vec());
        first.producer = Some(producer("writer-1", 0, 0));
        let first = runtime.append(first).await.expect("first append");
        assert_eq!(first.start_offset, 0);
        assert_eq!(first.next_offset, 1);
        assert_eq!(first.stream_append_count, 1);
        assert!(!first.deduplicated);

        let mut duplicate = AppendRequest::from_bytes(stream.clone(), b"ignored".to_vec());
        duplicate.producer = Some(producer("writer-1", 0, 0));
        let duplicate = runtime.append(duplicate).await.expect("duplicate append");
        assert_eq!(duplicate.start_offset, 0);
        assert_eq!(duplicate.next_offset, 1);
        assert_eq!(duplicate.stream_append_count, 1);
        assert!(duplicate.deduplicated);

        let mut next = AppendRequest::from_bytes(stream.clone(), b"b".to_vec());
        next.producer = Some(producer("writer-1", 0, 1));
        let next = runtime.append(next).await.expect("next append");
        assert_eq!(next.start_offset, 1);
        assert_eq!(next.next_offset, 2);
        assert_eq!(next.stream_append_count, 2);
        assert!(!next.deduplicated);

        let read = runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream,
                offset: 0,
                max_len: 16,
                now_ms: 0,
            })
            .await
            .expect("read");
        assert_eq!(read.payload, b"ab");

        let metrics = runtime.metrics().snapshot();
        assert_eq!(metrics.accepted_appends, 2);
        assert_eq!(metrics.applied_mutations, 3);
        assert_eq!(metrics.routed_requests, 5);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn producer_duplicate_append_batch_returns_prior_offsets_without_mutating_metrics() {
        let runtime = runtime(2, 8);
        let stream = BucketStreamId::new("benchcmp", "producer-batch-runtime");
        create_stream(&runtime, &stream).await;

        let mut first =
            AppendBatchRequest::new(stream.clone(), vec![b"ab".to_vec(), b"c".to_vec()]);
        first.producer = Some(producer("writer-1", 0, 0));
        let first = runtime.append_batch(first).await.expect("first batch");
        assert_eq!(first.items.len(), 2);
        let first_item = first.items[0].as_ref().expect("first item");
        let second_item = first.items[1].as_ref().expect("second item");
        assert_eq!(first_item.start_offset, 0);
        assert_eq!(first_item.next_offset, 2);
        assert_eq!(first_item.stream_append_count, 1);
        assert!(!first_item.deduplicated);
        assert_eq!(second_item.start_offset, 2);
        assert_eq!(second_item.next_offset, 3);
        assert_eq!(second_item.stream_append_count, 2);
        assert!(!second_item.deduplicated);

        let mut duplicate =
            AppendBatchRequest::new(stream.clone(), vec![b"ignored".to_vec(), b"body".to_vec()]);
        duplicate.producer = Some(producer("writer-1", 0, 0));
        let duplicate = runtime
            .append_batch(duplicate)
            .await
            .expect("duplicate batch");
        assert_eq!(duplicate.items.len(), 2);
        assert!(
            duplicate
                .items
                .iter()
                .all(|item| { item.as_ref().expect("deduplicated item").deduplicated })
        );
        assert_eq!(
            duplicate.items[0]
                .as_ref()
                .expect("first duplicate")
                .start_offset,
            0
        );
        assert_eq!(
            duplicate.items[1]
                .as_ref()
                .expect("second duplicate")
                .next_offset,
            3
        );

        let mut next = AppendBatchRequest::new(stream.clone(), vec![b"d".to_vec()]);
        next.producer = Some(producer("writer-1", 0, 1));
        let next = runtime.append_batch(next).await.expect("next batch");
        let next_item = next.items[0].as_ref().expect("next item");
        assert_eq!(next_item.start_offset, 3);
        assert_eq!(next_item.next_offset, 4);
        assert_eq!(next_item.stream_append_count, 3);
        assert!(!next_item.deduplicated);

        let read = runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream,
                offset: 0,
                max_len: 16,
                now_ms: 0,
            })
            .await
            .expect("read");
        assert_eq!(read.payload, b"abcd");

        let metrics = runtime.metrics().snapshot();
        assert_eq!(metrics.accepted_appends, 3);
        assert_eq!(metrics.applied_mutations, 4);
        assert_eq!(metrics.routed_requests, 5);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn snapshot_group_routes_to_owner_core_and_captures_only_group_state() {
        let runtime = runtime(2, 8);
        let first_stream = BucketStreamId::new("benchcmp", "snapshot-first");
        let first_placement = runtime.locate(&first_stream);
        let second_stream = (0..512)
            .map(|index| BucketStreamId::new("benchcmp", format!("snapshot-other-{index}")))
            .find(|stream| runtime.locate(stream).core_id != first_placement.core_id)
            .expect("stream on another core");

        create_stream(&runtime, &first_stream).await;
        runtime
            .append(AppendRequest::from_bytes(
                first_stream.clone(),
                b"first".to_vec(),
            ))
            .await
            .expect("append first stream");
        create_stream(&runtime, &second_stream).await;
        runtime
            .append(AppendRequest::from_bytes(
                second_stream.clone(),
                b"second".to_vec(),
            ))
            .await
            .expect("append second stream");

        let snapshot = runtime
            .snapshot_group(first_placement.raft_group_id)
            .await
            .expect("snapshot group");
        assert_eq!(snapshot.placement, first_placement);
        assert_eq!(snapshot.group_commit_index, 2);
        assert_eq!(snapshot.stream_snapshot.buckets, vec!["benchcmp"]);
        assert_eq!(
            snapshot
                .stream_snapshot
                .streams
                .iter()
                .map(|entry| entry.metadata.stream_id.clone())
                .collect::<Vec<_>>(),
            vec![first_stream.clone()]
        );

        let restored =
            StreamStateMachine::restore(snapshot.stream_snapshot).expect("restore group snapshot");
        let read = restored
            .read(&first_stream, 0, 16)
            .expect("read restored snapshot");
        assert_eq!(read.payload, b"first");
        assert_eq!(read.next_offset, 5);
        assert!(restored.read(&second_stream, 0, 16).is_err());

        let metrics = runtime.metrics().snapshot();
        assert_eq!(metrics.routed_requests, 5);
        assert_eq!(
            metrics.per_core_routed_requests[usize::from(first_placement.core_id.0)],
            3
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn snapshot_group_rejects_out_of_range_group_before_routing() {
        let runtime = runtime(2, 8);
        let err = runtime
            .snapshot_group(RaftGroupId(8))
            .await
            .expect_err("invalid group");
        assert_eq!(
            err,
            RuntimeError::InvalidRaftGroup {
                raft_group_id: RaftGroupId(8),
                raft_group_count: 8,
            }
        );
        assert_eq!(runtime.metrics().snapshot().routed_requests, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn install_group_snapshot_restores_group_state_and_append_counts() {
        let source = runtime(2, 8);
        let stream = BucketStreamId::new("benchcmp", "install-snapshot");
        let placement = source.locate(&stream);
        create_stream(&source, &stream).await;
        source
            .append(AppendRequest::from_bytes(stream.clone(), b"ab".to_vec()))
            .await
            .expect("append first");
        source
            .append(AppendRequest::from_bytes(stream.clone(), b"cd".to_vec()))
            .await
            .expect("append second");

        let snapshot = source
            .snapshot_group(placement.raft_group_id)
            .await
            .expect("snapshot group");
        assert_eq!(snapshot.group_commit_index, 3);
        assert_eq!(
            snapshot.stream_append_counts,
            vec![StreamAppendCount {
                stream_id: stream.clone(),
                append_count: 2,
            }]
        );

        let target = runtime(2, 8);
        target
            .install_group_snapshot(snapshot)
            .await
            .expect("install snapshot");

        let read = target
            .read_stream(ReadStreamRequest {
                stream_id: stream.clone(),
                offset: 0,
                max_len: 16,
                now_ms: 0,
            })
            .await
            .expect("read restored stream");
        assert_eq!(read.placement, placement);
        assert_eq!(read.payload, b"abcd");
        assert_eq!(read.next_offset, 4);

        let appended = target
            .append(AppendRequest::from_bytes(stream, b"ef".to_vec()))
            .await
            .expect("append after restore");
        assert_eq!(appended.start_offset, 4);
        assert_eq!(appended.next_offset, 6);
        assert_eq!(appended.stream_append_count, 3);
        assert_eq!(appended.group_commit_index, 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn install_group_snapshot_rejects_mismatched_placement_before_routing() {
        let runtime = runtime(2, 8);
        let snapshot = GroupSnapshot {
            placement: ShardPlacement {
                core_id: CoreId(1),
                shard_id: ShardId(0),
                raft_group_id: RaftGroupId(0),
            },
            group_commit_index: 0,
            stream_snapshot: StreamSnapshot {
                buckets: Vec::new(),
                streams: Vec::new(),
            },
            stream_append_counts: Vec::new(),
        };

        let err = runtime
            .install_group_snapshot(snapshot)
            .await
            .expect_err("mismatched placement rejected");
        assert_eq!(
            err,
            RuntimeError::SnapshotPlacementMismatch {
                expected: ShardPlacement {
                    core_id: CoreId(0),
                    shard_id: ShardId(0),
                    raft_group_id: RaftGroupId(0),
                },
                actual: ShardPlacement {
                    core_id: CoreId(1),
                    shard_id: ShardId(0),
                    raft_group_id: RaftGroupId(0),
                },
            }
        );
        assert_eq!(runtime.metrics().snapshot().routed_requests, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mailbox_snapshot_reports_per_core_depths_and_capacities() {
        let runtime = ShardRuntime::spawn(RuntimeConfig {
            core_count: 3,
            raft_group_count: 9,
            mailbox_capacity: 7,
            threading: RuntimeThreading::HostedTokio,
            cold_max_hot_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        })
        .expect("spawn runtime");

        let snapshot = runtime.mailbox_snapshot();
        assert_eq!(snapshot.depths, vec![0, 0, 0]);
        assert_eq!(snapshot.capacities, vec![7, 7, 7]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_metrics_track_owner_core_routing_and_mailbox_wait() {
        let runtime = runtime(2, 8);
        let stream = BucketStreamId::new("benchcmp", "routing-metrics");
        let owner_core = usize::from(runtime.locate(&stream).core_id.0);

        create_stream(&runtime, &stream).await;
        runtime
            .append(AppendRequest::from_bytes(stream.clone(), b"hello".to_vec()))
            .await
            .expect("append");
        runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream.clone(),
                offset: 0,
                max_len: 16,
                now_ms: 0,
            })
            .await
            .expect("read");

        let snapshot = runtime.metrics().snapshot();
        assert_eq!(snapshot.accepted_appends, 1);
        assert_eq!(snapshot.applied_mutations, 2);
        assert_eq!(snapshot.routed_requests, 3);
        assert_eq!(snapshot.per_core_routed_requests.len(), 2);
        assert_eq!(snapshot.per_core_routed_requests[owner_core], 3);
        assert_eq!(snapshot.per_core_applied_mutations[owner_core], 2);
        assert_eq!(
            snapshot.per_group_applied_mutations
                [usize::try_from(runtime.locate(&stream).raft_group_id.0).expect("u32 fits usize")],
            2
        );
        assert_eq!(
            snapshot.mutation_apply_ns,
            snapshot.per_core_mutation_apply_ns.iter().sum::<u64>()
        );
        assert_eq!(
            snapshot.mutation_apply_ns,
            snapshot.per_group_mutation_apply_ns.iter().sum::<u64>()
        );
        assert_eq!(
            snapshot.group_lock_wait_ns,
            snapshot.per_core_group_lock_wait_ns.iter().sum::<u64>()
        );
        assert_eq!(
            snapshot.group_lock_wait_ns,
            snapshot.per_group_group_lock_wait_ns.iter().sum::<u64>()
        );
        assert_eq!(
            snapshot.group_engine_exec_ns,
            snapshot.per_core_group_engine_exec_ns.iter().sum::<u64>()
        );
        assert_eq!(
            snapshot.group_engine_exec_ns,
            snapshot.per_group_group_engine_exec_ns.iter().sum::<u64>()
        );
        assert_eq!(
            snapshot.raft_write_many_batches,
            snapshot
                .per_core_raft_write_many_batches
                .iter()
                .sum::<u64>()
        );
        assert_eq!(
            snapshot.raft_write_many_batches,
            snapshot
                .per_group_raft_write_many_batches
                .iter()
                .sum::<u64>()
        );
        assert_eq!(
            snapshot.raft_write_many_commands,
            snapshot
                .per_core_raft_write_many_commands
                .iter()
                .sum::<u64>()
        );
        assert_eq!(
            snapshot.raft_write_many_commands,
            snapshot
                .per_group_raft_write_many_commands
                .iter()
                .sum::<u64>()
        );
        assert_eq!(
            snapshot.raft_write_many_logical_commands,
            snapshot
                .per_core_raft_write_many_logical_commands
                .iter()
                .sum::<u64>()
        );
        assert_eq!(
            snapshot.raft_write_many_logical_commands,
            snapshot
                .per_group_raft_write_many_logical_commands
                .iter()
                .sum::<u64>()
        );
        assert_eq!(
            snapshot.raft_write_many_responses,
            snapshot
                .per_core_raft_write_many_responses
                .iter()
                .sum::<u64>()
        );
        assert_eq!(
            snapshot.raft_write_many_responses,
            snapshot
                .per_group_raft_write_many_responses
                .iter()
                .sum::<u64>()
        );
        assert_eq!(
            snapshot.raft_write_many_submit_ns,
            snapshot
                .per_core_raft_write_many_submit_ns
                .iter()
                .sum::<u64>()
        );
        assert_eq!(
            snapshot.raft_write_many_submit_ns,
            snapshot
                .per_group_raft_write_many_submit_ns
                .iter()
                .sum::<u64>()
        );
        assert_eq!(
            snapshot.raft_write_many_response_ns,
            snapshot
                .per_core_raft_write_many_response_ns
                .iter()
                .sum::<u64>()
        );
        assert_eq!(
            snapshot.raft_write_many_response_ns,
            snapshot
                .per_group_raft_write_many_response_ns
                .iter()
                .sum::<u64>()
        );
        assert_eq!(
            snapshot.raft_apply_entries,
            snapshot.per_core_raft_apply_entries.iter().sum::<u64>()
        );
        assert_eq!(
            snapshot.raft_apply_entries,
            snapshot.per_group_raft_apply_entries.iter().sum::<u64>()
        );
        assert_eq!(
            snapshot.raft_apply_ns,
            snapshot.per_core_raft_apply_ns.iter().sum::<u64>()
        );
        assert_eq!(
            snapshot.raft_apply_ns,
            snapshot.per_group_raft_apply_ns.iter().sum::<u64>()
        );
        assert_eq!(
            snapshot.mailbox_send_wait_ns,
            snapshot.per_core_mailbox_send_wait_ns.iter().sum::<u64>()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn append_before_stream_setup_uses_stream_state_machine_error() {
        let runtime = runtime(2, 8);
        let stream = BucketStreamId::new("benchcmp", "missing-stream");
        let placement = runtime.locate(&stream);
        let err = runtime
            .append(AppendRequest::new(stream, 1))
            .await
            .expect_err("missing stream rejected");

        match err {
            RuntimeError::GroupEngine {
                core_id,
                raft_group_id,
                message,
                ..
            } => {
                assert_eq!(core_id, placement.core_id);
                assert_eq!(raft_group_id, placement.raft_group_id);
                assert!(message.contains("BucketNotFound"), "message={message}");
            }
            other => panic!("expected group engine error, got {other:?}"),
        }
        assert_eq!(runtime.metrics().snapshot().accepted_appends, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_stream_is_routed_and_idempotent_for_matching_metadata() {
        let runtime = runtime(2, 8);
        let stream = BucketStreamId::new("benchcmp", "create-stream");
        let placement = runtime.locate(&stream);

        let created = create_stream(&runtime, &stream).await;
        assert_eq!(created.placement, placement);
        assert_eq!(created.next_offset, 0);
        assert!(!created.closed);
        assert!(!created.already_exists);

        let existing = create_stream(&runtime, &stream).await;
        assert_eq!(existing.placement, placement);
        assert_eq!(existing.next_offset, 0);
        assert!(!existing.closed);
        assert!(existing.already_exists);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn head_stream_reflects_append_and_closed_state_on_owner_group() {
        let runtime = runtime(2, 8);
        let stream = BucketStreamId::new("benchcmp", "head-stream");
        let placement = runtime.locate(&stream);
        runtime
            .create_stream(CreateStreamRequest::new(stream.clone(), "text/plain"))
            .await
            .expect("create stream");

        let mut append = AppendRequest::new(stream.clone(), 3);
        append.content_type = "text/plain".to_owned();
        append.close_after = true;
        let response = runtime.append(append).await.expect("append");
        assert_eq!(response.start_offset, 0);
        assert_eq!(response.next_offset, 3);

        let head = runtime
            .head_stream(HeadStreamRequest {
                stream_id: stream,
                now_ms: 0,
            })
            .await
            .expect("head stream");
        assert_eq!(head.placement, placement);
        assert_eq!(head.content_type, "text/plain");
        assert_eq!(head.tail_offset, 3);
        assert!(head.closed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_stream_returns_payload_slice_from_owner_group() {
        let runtime = runtime(2, 8);
        let stream = BucketStreamId::new("benchcmp", "read-stream");
        let placement = runtime.locate(&stream);
        create_stream(&runtime, &stream).await;
        runtime
            .append(AppendRequest::from_bytes(
                stream.clone(),
                b"abcdefg".to_vec(),
            ))
            .await
            .expect("append");

        let read = runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream.clone(),
                offset: 2,
                max_len: 3,
                now_ms: 0,
            })
            .await
            .expect("read stream");
        assert_eq!(read.placement, placement);
        assert_eq!(read.offset, 2);
        assert_eq!(read.next_offset, 5);
        assert_eq!(read.payload, b"cde");
        assert!(!read.up_to_date);
        assert!(!read.closed);

        let tail = runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream,
                offset: 7,
                max_len: 16,
                now_ms: 0,
            })
            .await
            .expect("tail read");
        assert_eq!(tail.next_offset, 7);
        assert!(tail.payload.is_empty());
        assert!(tail.up_to_date);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_cold_publishes_chunk_metadata_on_owner_group() {
        let runtime = runtime(2, 8);
        let stream = BucketStreamId::new("benchcmp", "cold-runtime");
        let placement = runtime.locate(&stream);
        create_stream(&runtime, &stream).await;
        runtime
            .append(AppendRequest::from_bytes(
                stream.clone(),
                b"abcdef".to_vec(),
            ))
            .await
            .expect("append");

        let flushed = runtime
            .flush_cold(FlushColdRequest {
                stream_id: stream.clone(),
                chunk: ColdChunkRef {
                    start_offset: 0,
                    end_offset: 4,
                    s3_path: "s3://bucket/cold-runtime/000000".to_owned(),
                    object_size: 4,
                },
            })
            .await
            .expect("flush cold");
        assert_eq!(flushed.placement, placement);
        assert_eq!(flushed.hot_start_offset, 4);

        let hot = runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream.clone(),
                offset: 4,
                max_len: 16,
                now_ms: 0,
            })
            .await
            .expect("hot read");
        assert_eq!(hot.payload, b"ef");

        let err = runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream,
                offset: 0,
                max_len: 16,
                now_ms: 0,
            })
            .await
            .expect_err("cold read needs store");
        match err {
            RuntimeError::GroupEngine {
                message,
                next_offset: Some(6),
                ..
            } if message.contains("InvalidColdFlush") => {}
            other => panic!("expected cold read error, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_cold_once_uploads_outside_group_and_reads_back() {
        let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
        let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
            RuntimeConfig::new(2, 8),
            InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
            Some(cold_store),
        )
        .expect("spawn runtime");
        let stream = BucketStreamId::new("benchcmp", "cold-once");
        create_stream(&runtime, &stream).await;
        runtime
            .append(AppendRequest::from_bytes(
                stream.clone(),
                b"abcdef".to_vec(),
            ))
            .await
            .expect("append");

        let flushed = runtime
            .flush_cold_once(PlanColdFlushRequest {
                stream_id: stream.clone(),
                min_hot_bytes: 4,
                max_flush_bytes: 4,
            })
            .await
            .expect("flush once")
            .expect("candidate flushed");
        assert_eq!(flushed.hot_start_offset, 4);
        let metrics = runtime.metrics().snapshot();
        assert_eq!(metrics.cold_flush_uploads, 1);
        assert_eq!(metrics.cold_flush_upload_bytes, 4);
        assert_eq!(metrics.cold_flush_publishes, 1);
        assert_eq!(metrics.cold_flush_publish_bytes, 4);
        assert_eq!(metrics.cold_orphan_cleanup_attempts, 0);

        let read = runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream,
                offset: 0,
                max_len: 6,
                now_ms: 0,
            })
            .await
            .expect("read cold and hot");
        assert_eq!(read.payload, b"abcdef");
        assert_eq!(read.next_offset, 6);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_cold_group_batch_once_publishes_multiple_chunks() {
        let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
        let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
            RuntimeConfig::new(2, 8),
            InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
            Some(cold_store),
        )
        .expect("spawn runtime");
        let stream = BucketStreamId::new("benchcmp", "cold-batch");
        let placement = runtime.locate(&stream);
        create_stream(&runtime, &stream).await;
        runtime
            .append(AppendRequest::from_bytes(stream.clone(), b"abcd".to_vec()))
            .await
            .expect("append");

        let flushed = runtime
            .flush_cold_group_batch_once(
                placement.raft_group_id,
                PlanGroupColdFlushRequest {
                    min_hot_bytes: 1,
                    max_flush_bytes: 1,
                },
                4,
            )
            .await
            .expect("flush batch");
        assert_eq!(flushed.len(), 4);
        assert!(
            flushed
                .iter()
                .all(|response| response.placement == placement)
        );
        assert_eq!(
            flushed
                .iter()
                .map(|response| response.hot_start_offset)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 0]
        );

        let metrics = runtime.metrics().snapshot();
        assert_eq!(metrics.cold_flush_uploads, 4);
        assert_eq!(metrics.cold_flush_upload_bytes, 4);
        assert_eq!(metrics.cold_flush_publishes, 4);
        assert_eq!(metrics.cold_flush_publish_bytes, 4);
        assert_eq!(metrics.cold_hot_bytes, 0);

        let snapshot = runtime
            .snapshot_group(placement.raft_group_id)
            .await
            .expect("snapshot group");
        let entry = snapshot
            .stream_snapshot
            .streams
            .iter()
            .find(|entry| entry.metadata.stream_id == stream)
            .expect("stream snapshot");
        assert_eq!(entry.cold_chunks.len(), 4);
        assert!(entry.payload.is_empty());

        let read = runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream,
                offset: 0,
                max_len: 4,
                now_ms: 0,
            })
            .await
            .expect("read cold chunks");
        assert_eq!(read.payload, b"abcd");
        assert_eq!(read.next_offset, 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_cold_flush_batch_after_delete_recreate_is_classified_for_cleanup() {
        let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
        let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
            RuntimeConfig::new(2, 8),
            InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
            Some(cold_store),
        )
        .expect("spawn runtime");
        let stream = BucketStreamId::new("benchcmp", "stale-cold-runtime");
        let placement = runtime.locate(&stream);
        create_stream(&runtime, &stream).await;
        runtime
            .append(AppendRequest::from_bytes(
                stream.clone(),
                b"abcdefghijklmnopqr".to_vec(),
            ))
            .await
            .expect("append old stream");
        let candidates = runtime
            .plan_next_cold_flush_batch(
                placement.raft_group_id,
                PlanGroupColdFlushRequest {
                    min_hot_bytes: 18,
                    max_flush_bytes: 18,
                },
                1,
            )
            .await
            .expect("plan candidate");
        assert_eq!(candidates.len(), 1);

        runtime
            .delete_stream(DeleteStreamRequest {
                stream_id: stream.clone(),
            })
            .await
            .expect("delete old stream");
        create_stream(&runtime, &stream).await;
        runtime
            .append(AppendRequest::from_bytes(
                stream.clone(),
                b"abcdefghijklmnopq".to_vec(),
            ))
            .await
            .expect("append recreated stream");

        let err = runtime
            .flush_cold_candidates_batch(candidates)
            .await
            .expect_err("stale candidate should fail publish");
        assert!(is_stale_cold_flush_candidate_error(&err));
        let metrics = runtime.metrics().snapshot();
        assert_eq!(metrics.cold_flush_uploads, 1);
        assert_eq!(metrics.cold_flush_publishes, 0);
        assert_eq!(metrics.cold_orphan_cleanup_attempts, 1);
        assert_eq!(metrics.cold_orphan_cleanup_errors, 0);

        let read = runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream,
                offset: 0,
                max_len: 32,
                now_ms: 0,
            })
            .await
            .expect("read recreated stream");
        assert_eq!(read.payload, b"abcdefghijklmnopq");
        assert_eq!(read.next_offset, 17);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cold_write_admission_rejects_new_bytes_until_flush_catches_up() {
        let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
        let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
            RuntimeConfig::new(2, 8).with_cold_max_hot_bytes_per_group(Some(4)),
            InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
            Some(cold_store),
        )
        .expect("spawn runtime");
        let stream = BucketStreamId::new("benchcmp", "cold-admission");
        create_stream(&runtime, &stream).await;
        runtime
            .append(AppendRequest::from_bytes(stream.clone(), b"abcd".to_vec()))
            .await
            .expect("append below limit");

        let err = runtime
            .append(AppendRequest::from_bytes(stream.clone(), b"e".to_vec()))
            .await
            .expect_err("append should be backpressured");
        match err {
            RuntimeError::GroupEngine { message, .. } if message.contains("ColdBackpressure") => {}
            other => panic!("expected cold backpressure, got {other:?}"),
        }
        let metrics = runtime.metrics().snapshot();
        let group_index = usize::try_from(runtime.locate(&stream).raft_group_id.0).unwrap();
        assert_eq!(metrics.accepted_appends, 1);
        assert_eq!(metrics.cold_hot_bytes, 4);
        assert_eq!(metrics.per_group_cold_hot_bytes[group_index], 4);
        assert_eq!(metrics.cold_hot_group_bytes_max, 4);
        assert_eq!(metrics.cold_hot_stream_bytes_max, 4);
        assert_eq!(metrics.cold_backpressure_events, 1);
        assert_eq!(metrics.per_group_cold_backpressure_events[group_index], 1);
        assert_eq!(metrics.cold_backpressure_bytes, 1);

        runtime
            .flush_cold_once(PlanColdFlushRequest {
                stream_id: stream.clone(),
                min_hot_bytes: 4,
                max_flush_bytes: 4,
            })
            .await
            .expect("flush once")
            .expect("candidate flushed");
        assert_eq!(runtime.metrics().snapshot().cold_hot_bytes, 0);

        runtime
            .append(AppendRequest::from_bytes(stream.clone(), b"e".to_vec()))
            .await
            .expect("append after flush");
        let read = runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream,
                offset: 0,
                max_len: 5,
                now_ms: 0,
            })
            .await
            .expect("read cold and hot");
        assert_eq!(read.payload, b"abcde");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cold_write_admission_rejects_append_batch_without_partial_mutation() {
        let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
        let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
            RuntimeConfig::new(2, 8).with_cold_max_hot_bytes_per_group(Some(4)),
            InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
            Some(cold_store),
        )
        .expect("spawn runtime");
        let stream = BucketStreamId::new("benchcmp", "cold-admission-batch");
        create_stream(&runtime, &stream).await;
        runtime
            .append(AppendRequest::from_bytes(stream.clone(), b"abc".to_vec()))
            .await
            .expect("append below limit");

        let err = runtime
            .append_batch(AppendBatchRequest::new(
                stream.clone(),
                vec![b"d".to_vec(), b"e".to_vec()],
            ))
            .await
            .expect_err("batch should be backpressured");
        match err {
            RuntimeError::GroupEngine { message, .. } if message.contains("ColdBackpressure") => {}
            other => panic!("expected cold backpressure, got {other:?}"),
        }
        let read = runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream.clone(),
                offset: 0,
                max_len: 8,
                now_ms: 0,
            })
            .await
            .expect("read");
        assert_eq!(read.payload, b"abc");
        let metrics = runtime.metrics().snapshot();
        assert_eq!(metrics.accepted_appends, 1);
        assert_eq!(metrics.cold_backpressure_events, 1);
        assert_eq!(metrics.cold_backpressure_bytes, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_cold_group_once_selects_stream_inside_owner_group() {
        let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
        let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
            RuntimeConfig::new(2, 8),
            InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
            Some(cold_store),
        )
        .expect("spawn runtime");
        let group_id = RaftGroupId(3);
        let stream = stream_on_group(&runtime, group_id, "cold-group");
        create_stream(&runtime, &stream).await;
        runtime
            .append(AppendRequest::from_bytes(
                stream.clone(),
                b"abcdef".to_vec(),
            ))
            .await
            .expect("append");

        let flushed = runtime
            .flush_cold_group_once(
                group_id,
                PlanGroupColdFlushRequest {
                    min_hot_bytes: 4,
                    max_flush_bytes: 4,
                },
            )
            .await
            .expect("flush group")
            .expect("candidate flushed");
        assert_eq!(flushed.hot_start_offset, 4);

        let read = runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream,
                offset: 0,
                max_len: 6,
                now_ms: 0,
            })
            .await
            .expect("read cold and hot");
        assert_eq!(read.payload, b"abcdef");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_cold_all_groups_once_bounded_flushes_multiple_groups() {
        let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
        let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
            RuntimeConfig::new(2, 8),
            InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
            Some(cold_store),
        )
        .expect("spawn runtime");
        let first = stream_on_group(&runtime, RaftGroupId(1), "cold-bounded-a");
        let second = stream_on_group(&runtime, RaftGroupId(6), "cold-bounded-b");
        for stream in [&first, &second] {
            create_stream(&runtime, stream).await;
            runtime
                .append(AppendRequest::from_bytes(
                    stream.clone(),
                    b"abcdef".to_vec(),
                ))
                .await
                .expect("append");
        }

        let flushed = runtime
            .flush_cold_all_groups_once_bounded(
                PlanGroupColdFlushRequest {
                    min_hot_bytes: 4,
                    max_flush_bytes: 4,
                },
                2,
            )
            .await
            .expect("flush all bounded");
        assert_eq!(flushed, 2);
        let metrics = runtime.metrics().snapshot();
        assert_eq!(metrics.cold_flush_uploads, 2);
        assert_eq!(metrics.cold_flush_upload_bytes, 8);
        assert_eq!(metrics.cold_flush_publishes, 2);
        assert_eq!(metrics.cold_flush_publish_bytes, 8);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repeated_cold_flush_keeps_hot_bytes_bounded_while_writes_continue() {
        let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
        let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
            RuntimeConfig::new(2, 8).with_cold_max_hot_bytes_per_group(Some(16)),
            InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
            Some(cold_store),
        )
        .expect("spawn runtime");
        let streams = [
            stream_on_group(&runtime, RaftGroupId(0), "cold-steady-a"),
            stream_on_group(&runtime, RaftGroupId(3), "cold-steady-b"),
            stream_on_group(&runtime, RaftGroupId(5), "cold-steady-c"),
            stream_on_group(&runtime, RaftGroupId(7), "cold-steady-d"),
        ];
        for stream in &streams {
            create_stream(&runtime, stream).await;
        }

        let mut expected = Vec::new();
        for round in 0..8u8 {
            let payload = vec![b'a' + round; 4];
            expected.extend_from_slice(&payload);
            for stream in &streams {
                runtime
                    .append(AppendRequest::from_bytes(stream.clone(), payload.clone()))
                    .await
                    .expect("append while cold worker keeps up");
            }

            let metrics_before_flush = runtime.metrics().snapshot();
            assert!(
                metrics_before_flush.cold_hot_bytes <= 64,
                "hot bytes should stay within one unflushed batch per group before flush: {}",
                metrics_before_flush.cold_hot_bytes
            );

            let flushed = runtime
                .flush_cold_all_groups_once_bounded(
                    PlanGroupColdFlushRequest {
                        min_hot_bytes: 4,
                        max_flush_bytes: 4,
                    },
                    streams.len(),
                )
                .await
                .expect("flush all bounded");
            assert_eq!(flushed, streams.len());
            let metrics_after_flush = runtime.metrics().snapshot();
            assert_eq!(
                metrics_after_flush.cold_hot_bytes, 0,
                "all newly appended bytes should be offloaded after round {round}"
            );
            assert_eq!(
                metrics_after_flush.cold_flush_uploads,
                u64::try_from((usize::from(round) + 1) * streams.len()).expect("count fits u64")
            );
            assert_eq!(metrics_after_flush.cold_orphan_cleanup_attempts, 0);
            assert_eq!(metrics_after_flush.cold_backpressure_events, 0);
        }

        for stream in streams {
            let read = runtime
                .read_stream(ReadStreamRequest {
                    stream_id: stream,
                    offset: 0,
                    max_len: expected.len(),
                    now_ms: 0,
                })
                .await
                .expect("read cold-backed stream");
            assert_eq!(read.payload, expected);
            assert_eq!(read.next_offset, u64::try_from(expected.len()).unwrap());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_read_stream_completes_after_owner_append() {
        let runtime = runtime(2, 8);
        let stream = BucketStreamId::new("benchcmp", "wait-read");
        create_stream(&runtime, &stream).await;

        let wait = {
            let runtime = runtime.clone();
            let stream = stream.clone();
            tokio::spawn(async move {
                runtime
                    .wait_read_stream(ReadStreamRequest {
                        stream_id: stream,
                        offset: 0,
                        max_len: 16,
                        now_ms: 0,
                    })
                    .await
                    .expect("wait read")
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        runtime
            .append(AppendRequest::from_bytes(stream.clone(), b"hello".to_vec()))
            .await
            .expect("append");

        let read = tokio::time::timeout(std::time::Duration::from_secs(1), wait)
            .await
            .expect("wait read timeout")
            .expect("wait task");
        assert_eq!(read.payload, b"hello");
        assert_eq!(read.next_offset, 5);
        assert!(read.up_to_date);
        assert!(!read.closed);
        assert_eq!(runtime.metrics().snapshot().live_read_waiters, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_read_stream_completes_on_close_at_tail() {
        let runtime = runtime(2, 8);
        let stream = BucketStreamId::new("benchcmp", "wait-close");
        create_stream(&runtime, &stream).await;

        let wait = {
            let runtime = runtime.clone();
            let stream = stream.clone();
            tokio::spawn(async move {
                runtime
                    .wait_read_stream(ReadStreamRequest {
                        stream_id: stream,
                        offset: 0,
                        max_len: 16,
                        now_ms: 0,
                    })
                    .await
                    .expect("wait read")
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        runtime
            .close_stream(CloseStreamRequest {
                stream_id: stream,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            })
            .await
            .expect("close stream");

        let read = tokio::time::timeout(std::time::Duration::from_secs(1), wait)
            .await
            .expect("wait read timeout")
            .expect("wait task");
        assert!(read.payload.is_empty());
        assert_eq!(read.next_offset, 0);
        assert!(read.up_to_date);
        assert!(read.closed);
        assert_eq!(runtime.metrics().snapshot().live_read_waiters, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn canceled_wait_read_stream_removes_owner_waiter() {
        let runtime = runtime(2, 8);
        let stream = BucketStreamId::new("benchcmp", "wait-cancel");
        create_stream(&runtime, &stream).await;

        let wait = {
            let runtime = runtime.clone();
            let stream = stream.clone();
            tokio::spawn(async move {
                runtime
                    .wait_read_stream(ReadStreamRequest {
                        stream_id: stream,
                        offset: 0,
                        max_len: 16,
                        now_ms: 0,
                    })
                    .await
            })
        };
        wait_for_live_waiters(&runtime, 1).await;
        wait.abort();
        let _ = wait.await;
        wait_for_live_waiters(&runtime, 0).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_read_waiter_limit_rejects_excess_waiters_on_owner_core() {
        let runtime = ShardRuntime::spawn(
            RuntimeConfig::new(1, 1).with_live_read_max_waiters_per_core(Some(1)),
        )
        .expect("spawn runtime");
        let stream = BucketStreamId::new("benchcmp", "wait-limit");
        create_stream(&runtime, &stream).await;

        let first = {
            let runtime = runtime.clone();
            let stream = stream.clone();
            tokio::spawn(async move {
                runtime
                    .wait_read_stream(ReadStreamRequest {
                        stream_id: stream,
                        offset: 0,
                        max_len: 16,
                        now_ms: 0,
                    })
                    .await
            })
        };
        wait_for_live_waiters(&runtime, 1).await;

        let err = runtime
            .wait_read_stream(ReadStreamRequest {
                stream_id: stream.clone(),
                offset: 0,
                max_len: 16,
                now_ms: 0,
            })
            .await
            .expect_err("second waiter should hit owner-core limit");
        assert_eq!(
            err,
            RuntimeError::LiveReadBackpressure {
                core_id: CoreId(0),
                current_waiters: 1,
                limit: 1,
            }
        );
        let snapshot = runtime.metrics().snapshot();
        assert_eq!(snapshot.live_read_waiters, 1);
        assert_eq!(snapshot.live_read_backpressure_events, 1);
        assert_eq!(snapshot.per_core_live_read_backpressure_events, vec![1]);

        first.abort();
        let _ = first.await;
        wait_for_live_waiters(&runtime, 0).await;
    }

    #[test]
    fn cancel_read_watcher_removes_group_local_waiter() {
        let stream = BucketStreamId::new("benchcmp", "watcher-cancel-local");
        let mut read_watchers = ReadWatchers::new();
        let (first_tx, _first_rx) = oneshot::channel();
        let (second_tx, _second_rx) = oneshot::channel();
        read_watchers.insert(
            stream.clone(),
            vec![
                ReadWatcher {
                    waiter_id: 1,
                    request: ReadStreamRequest {
                        stream_id: stream.clone(),
                        offset: 0,
                        max_len: 16,
                        now_ms: 0,
                    },
                    response_tx: first_tx,
                },
                ReadWatcher {
                    waiter_id: 2,
                    request: ReadStreamRequest {
                        stream_id: stream.clone(),
                        offset: 0,
                        max_len: 16,
                        now_ms: 0,
                    },
                    response_tx: second_tx,
                },
            ],
        );

        let metrics = Arc::new(RuntimeMetricsInner::new(1, 1));
        metrics.record_read_watchers_added(CoreId(0), 2);
        CoreWorker::cancel_read_watcher(
            &mut read_watchers,
            metrics.clone(),
            CoreId(0),
            stream.clone(),
            1,
        );

        let watcher_ids = read_watchers
            .get(&stream)
            .expect("one watcher remains")
            .iter()
            .map(|watcher| watcher.waiter_id)
            .collect::<Vec<_>>();
        assert_eq!(watcher_ids, vec![2]);
        assert_eq!(metrics.per_core_live_read_waiters[0].load_relaxed(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn notify_read_watchers_shares_identical_reads_across_watchers() {
        let factory = BlockingReadFactory::default();
        let runtime = ShardRuntime::spawn_with_engine_factory(
            RuntimeConfig {
                core_count: 1,
                raft_group_count: 1,
                mailbox_capacity: 8,
                threading: RuntimeThreading::HostedTokio,
                cold_max_hot_bytes_per_group: None,
                live_read_max_waiters_per_core: Some(65_536),
            },
            factory.clone(),
        )
        .expect("spawn runtime");
        let stream = BucketStreamId::new("benchcmp", "watcher-shared-read");
        let placement = runtime.locate(&stream);
        let request = ReadStreamRequest {
            stream_id: stream.clone(),
            offset: 0,
            max_len: 16,
            now_ms: 0,
        };
        let mut read_watchers = ReadWatchers::new();
        let (first_tx, _first_rx) = oneshot::channel();
        let (second_tx, _second_rx) = oneshot::channel();
        read_watchers.insert(
            stream.clone(),
            vec![
                ReadWatcher {
                    waiter_id: 1,
                    request: request.clone(),
                    response_tx: first_tx,
                },
                ReadWatcher {
                    waiter_id: 2,
                    request,
                    response_tx: second_tx,
                },
            ],
        );

        let metrics = Arc::new(RuntimeMetricsInner::new(1, 1));
        let mut engine = factory
            .create(
                placement,
                GroupEngineMetrics {
                    inner: metrics.clone(),
                },
            )
            .await
            .expect("create engine");
        let notify = {
            let stream = stream.clone();
            tokio::spawn(async move {
                CoreWorker::notify_read_watchers(
                    &mut engine,
                    metrics,
                    Arc::new(Semaphore::new(8)),
                    &mut read_watchers,
                    &stream,
                    placement,
                )
                .await;
                read_watchers
            })
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            factory.entered.notified(),
        )
        .await
        .expect("notify issued one grouped read");
        factory.release.notify_one();
        let read_watchers = tokio::time::timeout(std::time::Duration::from_secs(1), notify)
            .await
            .expect("notify should finish after one read")
            .expect("notify task");

        let watcher_ids = read_watchers
            .get(&stream)
            .expect("pending watchers reinserted")
            .iter()
            .map(|watcher| watcher.waiter_id)
            .collect::<Vec<_>>();
        assert_eq!(watcher_ids, vec![1, 2]);
        assert_eq!(factory.read_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn close_stream_allows_close_only_and_rejects_later_appends() {
        let runtime = runtime(2, 8);
        let stream = BucketStreamId::new("benchcmp", "close-only");
        let placement = runtime.locate(&stream);
        create_stream(&runtime, &stream).await;

        let closed = runtime
            .close_stream(CloseStreamRequest {
                stream_id: stream.clone(),
                stream_seq: None,
                producer: None,
                now_ms: 0,
            })
            .await
            .expect("close stream");
        assert_eq!(closed.placement, placement);
        assert_eq!(closed.next_offset, 0);

        let err = runtime
            .append(AppendRequest::new(stream.clone(), 1))
            .await
            .expect_err("append after close rejected");
        match err {
            RuntimeError::GroupEngine { message, .. } => {
                assert!(message.contains("StreamClosed"), "message={message}");
            }
            other => panic!("expected group engine error, got {other:?}"),
        }

        let head = runtime
            .head_stream(HeadStreamRequest {
                stream_id: stream,
                now_ms: 0,
            })
            .await
            .expect("head stream");
        assert_eq!(head.tail_offset, 0);
        assert!(head.closed);
        assert_eq!(runtime.metrics().snapshot().accepted_appends, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_stream_removes_state_on_owner_group() {
        let runtime = runtime(2, 8);
        let stream = BucketStreamId::new("benchcmp", "delete-stream");
        let placement = runtime.locate(&stream);
        create_stream(&runtime, &stream).await;
        runtime
            .append(AppendRequest::from_bytes(
                stream.clone(),
                b"payload".to_vec(),
            ))
            .await
            .expect("append");

        let deleted = runtime
            .delete_stream(DeleteStreamRequest {
                stream_id: stream.clone(),
            })
            .await
            .expect("delete stream");
        assert_eq!(deleted.placement, placement);

        let err = runtime
            .head_stream(HeadStreamRequest {
                stream_id: stream.clone(),
                now_ms: 0,
            })
            .await
            .expect_err("head after delete rejected");
        match err {
            RuntimeError::GroupEngine { message, .. } => {
                assert!(message.contains("StreamNotFound"), "message={message}");
            }
            other => panic!("expected group engine error, got {other:?}"),
        }

        let err = runtime
            .append(AppendRequest::new(stream, 1))
            .await
            .expect_err("append after delete rejected");
        match err {
            RuntimeError::GroupEngine { message, .. } => {
                assert!(message.contains("StreamNotFound"), "message={message}");
            }
            other => panic!("expected group engine error, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fork_ref_keeps_deleted_source_gone_until_last_fork_delete() {
        let runtime = runtime(2, 8);
        let source = BucketStreamId::new("benchcmp", "fork-ref-source");
        let fork = BucketStreamId::new("benchcmp", "fork-ref-child");
        let mut source_create = CreateStreamRequest::new(source.clone(), DEFAULT_CONTENT_TYPE);
        source_create.initial_payload = Bytes::from_static(b"abc");
        runtime
            .create_stream(source_create)
            .await
            .expect("create source");

        let mut fork_create = CreateStreamRequest::new(fork.clone(), DEFAULT_CONTENT_TYPE);
        fork_create.forked_from = Some(source.clone());
        runtime
            .create_stream(fork_create)
            .await
            .expect("create fork");

        runtime
            .delete_stream(DeleteStreamRequest {
                stream_id: source.clone(),
            })
            .await
            .expect("delete source");
        let err = runtime
            .head_stream(HeadStreamRequest {
                stream_id: source.clone(),
                now_ms: 0,
            })
            .await
            .expect_err("soft-deleted source is gone");
        match err {
            RuntimeError::GroupEngine { message, .. } => {
                assert!(message.contains("StreamGone"), "message={message}");
            }
            other => panic!("expected group engine error, got {other:?}"),
        }

        let fork_read = runtime
            .read_stream(ReadStreamRequest {
                stream_id: fork.clone(),
                offset: 0,
                max_len: 16,
                now_ms: 0,
            })
            .await
            .expect("fork remains readable");
        assert_eq!(fork_read.payload, b"abc");

        runtime
            .delete_stream(DeleteStreamRequest { stream_id: fork })
            .await
            .expect("delete fork");
        let err = runtime
            .head_stream(HeadStreamRequest {
                stream_id: source,
                now_ms: 0,
            })
            .await
            .expect_err("source is hard-deleted after last fork");
        match err {
            RuntimeError::GroupEngine { message, .. } => {
                assert!(message.contains("StreamNotFound"), "message={message}");
            }
            other => panic!("expected group engine error, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn thread_per_core_runtime_reaches_all_configured_cores() {
        let mut config = RuntimeConfig::new(4, 32);
        config.mailbox_capacity = 128;
        assert_eq!(config.threading, RuntimeThreading::ThreadPerCore);
        let runtime = ShardRuntime::spawn(config).expect("spawn runtime");

        let mut tasks = Vec::new();
        for index in 0..1024 {
            let runtime = runtime.clone();
            tasks.push(tokio::spawn(async move {
                let stream = BucketStreamId::new("benchcmp", format!("thread-core-{index}"));
                create_stream(&runtime, &stream).await;
                runtime
                    .append(AppendRequest::new(stream, 1))
                    .await
                    .expect("append");
            }));
        }

        for task in tasks {
            task.await.expect("task");
        }

        let snapshot = runtime.metrics().snapshot();
        assert_eq!(snapshot.accepted_appends, 1024);
        assert!(snapshot.per_core_appends.iter().all(|value| *value > 0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn custom_group_engine_is_created_once_per_touched_group_on_owner_core() {
        let factory = RecordingFactory::default();
        let runtime = ShardRuntime::spawn_with_engine_factory(
            RuntimeConfig {
                core_count: 4,
                raft_group_count: 32,
                mailbox_capacity: 128,
                threading: RuntimeThreading::HostedTokio,
                cold_max_hot_bytes_per_group: None,
                live_read_max_waiters_per_core: Some(65_536),
            },
            factory.clone(),
        )
        .expect("spawn runtime");

        let mut touched_groups = HashSet::new();
        for index in 0..4096 {
            let stream = BucketStreamId::new("benchcmp", format!("engine-{index}"));
            let placement = runtime.locate(&stream);
            runtime
                .create_stream(CreateStreamRequest::new(stream, DEFAULT_CONTENT_TYPE))
                .await
                .expect("create stream");
            touched_groups.insert(placement.raft_group_id);
            if touched_groups.len() == 16 {
                break;
            }
        }

        let created = factory.created();
        let created_groups = created
            .iter()
            .map(|placement| placement.raft_group_id)
            .collect::<HashSet<_>>();
        assert_eq!(created_groups, touched_groups);
        for placement in created {
            assert_eq!(
                u32::from(placement.core_id.0),
                placement.raft_group_id.0 % 4
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn background_cold_flush_skips_groups_that_cannot_accept_local_writes() {
        let factory = RecordingFactory::without_local_writes();
        let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
        let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
            RuntimeConfig {
                core_count: 2,
                raft_group_count: 4,
                mailbox_capacity: 128,
                threading: RuntimeThreading::HostedTokio,
                cold_max_hot_bytes_per_group: None,
                live_read_max_waiters_per_core: Some(65_536),
            },
            factory.clone(),
            Some(cold_store),
        )
        .expect("spawn runtime");

        let flushed = runtime
            .flush_cold_all_groups_once_bounded(
                PlanGroupColdFlushRequest {
                    min_hot_bytes: 1,
                    max_flush_bytes: 1,
                },
                4,
            )
            .await
            .expect("flush all groups");

        assert_eq!(flushed, 0);
        assert_eq!(factory.created().len(), 4);
        let metrics = runtime.metrics().snapshot();
        assert_eq!(metrics.cold_flush_uploads, 0);
        assert_eq!(metrics.cold_flush_publishes, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn warm_group_instantiates_engine_on_owner_core_without_stream_mutation() {
        let factory = RecordingFactory::default();
        let runtime = ShardRuntime::spawn_with_engine_factory(
            RuntimeConfig {
                core_count: 2,
                raft_group_count: 4,
                mailbox_capacity: 128,
                threading: RuntimeThreading::HostedTokio,
                cold_max_hot_bytes_per_group: None,
                live_read_max_waiters_per_core: Some(65_536),
            },
            factory.clone(),
        )
        .expect("spawn runtime");

        let warmed = runtime
            .warm_group(RaftGroupId(3))
            .await
            .expect("warm group");
        assert_eq!(warmed.core_id, CoreId(1));
        assert_eq!(warmed.raft_group_id, RaftGroupId(3));

        runtime
            .warm_group(RaftGroupId(3))
            .await
            .expect("second warm is idempotent");

        let created = factory.created();
        assert_eq!(created, vec![warmed]);

        runtime.warm_all_groups().await.expect("warm all groups");
        let created_groups = factory
            .created()
            .into_iter()
            .map(|placement| placement.raft_group_id)
            .collect::<HashSet<_>>();
        assert_eq!(
            created_groups,
            [
                RaftGroupId(0),
                RaftGroupId(1),
                RaftGroupId(2),
                RaftGroupId(3)
            ]
            .into_iter()
            .collect()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn core_worker_dispatches_other_groups_while_one_group_waits() {
        let factory = BlockingFirstCreateEngineFactory::default();
        let runtime = ShardRuntime::spawn_with_engine_factory(
            RuntimeConfig {
                core_count: 1,
                raft_group_count: 2,
                mailbox_capacity: 128,
                threading: RuntimeThreading::HostedTokio,
                cold_max_hot_bytes_per_group: None,
                live_read_max_waiters_per_core: Some(65_536),
            },
            factory.clone(),
        )
        .expect("spawn runtime");

        let blocked_stream = stream_on_group(&runtime, RaftGroupId(0), "blocked-group");
        let free_stream = stream_on_group(&runtime, RaftGroupId(1), "free-group");
        let entered_wait = factory.entered.notified();
        let blocked_runtime = runtime.clone();
        let blocked =
            tokio::spawn(async move { create_stream(&blocked_runtime, &blocked_stream).await });

        tokio::time::timeout(std::time::Duration::from_secs(1), entered_wait)
            .await
            .expect("first group entered blocking create");

        let completed = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            create_stream(&runtime, &free_stream),
        )
        .await
        .expect("other group should complete while first group is blocked");
        assert_eq!(completed.placement.raft_group_id, RaftGroupId(1));

        factory.release.notify_one();
        blocked.await.expect("blocked task");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_read_uses_group_read_parts_fast_path() {
        let factory = BlockingReadFactory::default();
        let runtime = ShardRuntime::spawn_with_engine_factory(
            RuntimeConfig {
                core_count: 1,
                raft_group_count: 1,
                mailbox_capacity: 128,
                threading: RuntimeThreading::HostedTokio,
                cold_max_hot_bytes_per_group: None,
                live_read_max_waiters_per_core: Some(65_536),
            },
            factory.clone(),
        )
        .expect("spawn runtime");
        let stream = BucketStreamId::new("benchcmp", "read-offload");
        create_stream(&runtime, &stream).await;

        let read = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            runtime.read_stream(ReadStreamRequest {
                stream_id: stream.clone(),
                offset: 0,
                max_len: 16,
                now_ms: 0,
            }),
        )
        .await
        .expect("runtime read should not use blocking legacy read_stream")
        .expect("read stream");
        assert_eq!(read.placement.raft_group_id, RaftGroupId(0));
        assert_eq!(factory.read_count.load(Ordering::Relaxed), 1);

        let head = runtime
            .head_stream(HeadStreamRequest {
                stream_id: stream,
                now_ms: 0,
            })
            .await
            .expect("head stream");
        assert_eq!(head.placement.raft_group_id, RaftGroupId(0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_materialization_is_bounded_without_blocking_group_actor() {
        let factory = BlockingReadFactory::block_materialization();
        let mut config = RuntimeConfig::new(1, 1);
        config.mailbox_capacity = 1;
        config.threading = RuntimeThreading::HostedTokio;
        let runtime = ShardRuntime::spawn_with_engine_factory(config, factory.clone())
            .expect("spawn runtime");
        let first_stream = BucketStreamId::new("benchcmp", "materialize-bound-1");
        let second_stream = BucketStreamId::new("benchcmp", "materialize-bound-2");
        create_stream(&runtime, &first_stream).await;
        create_stream(&runtime, &second_stream).await;

        let first_runtime = runtime.clone();
        let first_stream_for_read = first_stream.clone();
        let first_read = tokio::spawn(async move {
            first_runtime
                .read_stream(ReadStreamRequest {
                    stream_id: first_stream_for_read,
                    offset: 0,
                    max_len: 16,
                    now_ms: 0,
                })
                .await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            factory.entered.notified(),
        )
        .await
        .expect("first materialization acquired the only permit");

        let second_runtime = runtime.clone();
        let second_stream_for_read = second_stream.clone();
        let second_read = tokio::spawn(async move {
            second_runtime
                .read_stream(ReadStreamRequest {
                    stream_id: second_stream_for_read,
                    offset: 0,
                    max_len: 16,
                    now_ms: 0,
                })
                .await
        });

        let head = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            runtime.head_stream(HeadStreamRequest {
                stream_id: first_stream,
                now_ms: 0,
            }),
        )
        .await
        .expect("group actor should keep serving metadata while materialization waits")
        .expect("head stream");
        assert_eq!(head.placement.raft_group_id, RaftGroupId(0));
        assert!(!second_read.is_finished());

        factory.release.notify_one();
        let first = first_read
            .await
            .expect("first read task")
            .expect("first read");
        assert_eq!(first.payload, b"ready");
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            factory.entered.notified(),
        )
        .await
        .expect("second materialization acquired permit after first released it");
        factory.release.notify_one();
        let second = second_read
            .await
            .expect("second read task")
            .expect("second read");
        assert_eq!(second.payload, b"ready");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn group_engine_errors_include_group_context_and_do_not_record_success_metrics() {
        let runtime = ShardRuntime::spawn_with_engine_factory(
            RuntimeConfig {
                core_count: 2,
                raft_group_count: 8,
                mailbox_capacity: 128,
                threading: RuntimeThreading::HostedTokio,
                cold_max_hot_bytes_per_group: None,
                live_read_max_waiters_per_core: Some(65_536),
            },
            FailingFactory,
        )
        .expect("spawn runtime");

        let stream = BucketStreamId::new("benchcmp", "failing-stream");
        let placement = runtime.locate(&stream);
        let err = runtime
            .append(AppendRequest::new(stream, 1))
            .await
            .expect_err("engine failure");

        assert_eq!(
            err,
            RuntimeError::GroupEngine {
                core_id: placement.core_id,
                raft_group_id: placement.raft_group_id,
                message: "proposal rejected".to_owned(),
                next_offset: None,
                leader_hint: None,
            }
        );
        assert_eq!(runtime.metrics().snapshot().accepted_appends, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mailbox_full_events_record_owner_core_backpressure() {
        let factory = BlockingOnceFactory::default();
        let runtime = ShardRuntime::spawn_with_engine_factory(
            RuntimeConfig {
                core_count: 1,
                raft_group_count: 1,
                mailbox_capacity: 1,
                threading: RuntimeThreading::HostedTokio,
                cold_max_hot_bytes_per_group: None,
                live_read_max_waiters_per_core: Some(65_536),
            },
            factory.clone(),
        )
        .expect("spawn runtime");

        let entered = factory.entered.clone();
        let entered_wait = entered.notified();
        let first_runtime = runtime.clone();
        let first = tokio::spawn(async move {
            create_stream(
                &first_runtime,
                &BucketStreamId::new("benchcmp", "backpressure-1"),
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), entered_wait)
            .await
            .expect("first create entered blocking engine factory");

        let second_runtime = runtime.clone();
        let second = tokio::spawn(async move {
            create_stream(
                &second_runtime,
                &BucketStreamId::new("benchcmp", "backpressure-2"),
            )
            .await
        });
        wait_for_mailbox_depth(&runtime, 0, 1).await;

        let third_runtime = runtime.clone();
        let third = tokio::spawn(async move {
            create_stream(
                &third_runtime,
                &BucketStreamId::new("benchcmp", "backpressure-3"),
            )
            .await
        });
        wait_for_mailbox_full_events(&runtime, 1).await;
        assert_eq!(
            runtime.metrics().snapshot().per_core_mailbox_full_events[0],
            1
        );

        factory.release.notify_one();
        first.await.expect("first task");
        second.await.expect("second task");
        third.await.expect("third task");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn group_mailbox_full_events_record_inner_actor_backpressure() {
        let factory = BlockingFirstCreateEngineFactory::default();
        let runtime = ShardRuntime::spawn_with_engine_factory(
            RuntimeConfig {
                core_count: 1,
                raft_group_count: 1,
                mailbox_capacity: 1,
                threading: RuntimeThreading::HostedTokio,
                cold_max_hot_bytes_per_group: None,
                live_read_max_waiters_per_core: Some(65_536),
            },
            factory.clone(),
        )
        .expect("spawn runtime");

        let first_runtime = runtime.clone();
        let first = tokio::spawn(async move {
            create_stream(
                &first_runtime,
                &BucketStreamId::new("benchcmp", "group-backpressure-1"),
            )
            .await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            factory.entered.notified(),
        )
        .await
        .expect("first append entered blocking group engine");

        let second_runtime = runtime.clone();
        let second = tokio::spawn(async move {
            create_stream(
                &second_runtime,
                &BucketStreamId::new("benchcmp", "group-backpressure-2"),
            )
            .await
        });
        for _ in 0..100 {
            if runtime.metrics().snapshot().group_mailbox_depth == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let third_runtime = runtime.clone();
        let third = tokio::spawn(async move {
            create_stream(
                &third_runtime,
                &BucketStreamId::new("benchcmp", "group-backpressure-3"),
            )
            .await
        });
        wait_for_group_mailbox_full_events(&runtime, 1).await;
        assert_eq!(
            runtime
                .metrics()
                .snapshot()
                .per_group_group_mailbox_full_events[0],
            1
        );

        factory.release.notify_one();
        first.await.expect("first task");
        second.await.expect("second task");
        third.await.expect("third task");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wal_group_engine_recovers_multiple_groups_from_per_group_logs() {
        let wal_root = std::env::temp_dir().join(format!(
            "ursula-wal-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&wal_root);
        let config = RuntimeConfig {
            core_count: 2,
            raft_group_count: 8,
            mailbox_capacity: 128,
            threading: RuntimeThreading::HostedTokio,
            cold_max_hot_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        };

        let (first_stream, second_stream) = {
            let runtime = ShardRuntime::spawn_with_engine_factory(
                config.clone(),
                WalGroupEngineFactory::new(&wal_root),
            )
            .expect("spawn runtime");

            let mut seen_groups = HashSet::new();
            let mut streams = Vec::new();
            for index in 0..256 {
                let stream = BucketStreamId::new("benchcmp", format!("wal-{index}"));
                if seen_groups.insert(runtime.locate(&stream).raft_group_id) {
                    streams.push(stream);
                }
                if streams.len() == 2 {
                    break;
                }
            }
            assert_eq!(streams.len(), 2, "expected streams on two groups");
            let first_stream = streams[0].clone();
            let second_stream = streams[1].clone();

            create_stream(&runtime, &first_stream).await;
            runtime
                .append(AppendRequest::from_bytes(
                    first_stream.clone(),
                    b"first-payload".to_vec(),
                ))
                .await
                .expect("append first stream");

            create_stream(&runtime, &second_stream).await;
            let mut append_second =
                AppendRequest::from_bytes(second_stream.clone(), b"second-payload".to_vec());
            append_second.close_after = true;
            runtime
                .append(append_second)
                .await
                .expect("append second stream");

            (first_stream, second_stream)
        };

        let recovered =
            ShardRuntime::spawn_with_engine_factory(config, WalGroupEngineFactory::new(&wal_root))
                .expect("spawn recovered runtime");

        let first_read = recovered
            .read_stream(ReadStreamRequest {
                stream_id: first_stream.clone(),
                offset: 0,
                max_len: 128,
                now_ms: 0,
            })
            .await
            .expect("read recovered first stream");
        assert_eq!(first_read.payload, b"first-payload");
        assert!(!first_read.closed);

        let second_read = recovered
            .read_stream(ReadStreamRequest {
                stream_id: second_stream.clone(),
                offset: 0,
                max_len: 128,
                now_ms: 0,
            })
            .await
            .expect("read recovered second stream");
        assert_eq!(second_read.payload, b"second-payload");
        assert!(second_read.closed);

        let mut wal_file_count = 0;
        for core_entry in std::fs::read_dir(&wal_root).expect("read WAL root") {
            let core_entry = core_entry.expect("read core WAL dir");
            for group_entry in std::fs::read_dir(core_entry.path()).expect("read group WAL dir") {
                let group_entry = group_entry.expect("read group WAL file");
                if group_entry
                    .path()
                    .extension()
                    .is_some_and(|ext| ext == "jsonl")
                {
                    wal_file_count += 1;
                }
            }
        }
        assert_eq!(wal_file_count, 2);

        drop(recovered);
        std::fs::remove_dir_all(&wal_root).expect("remove WAL root");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wal_group_engine_batches_append_records_and_recovers() {
        let wal_root = std::env::temp_dir().join(format!(
            "ursula-wal-batch-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&wal_root);
        let config = RuntimeConfig {
            core_count: 2,
            raft_group_count: 8,
            mailbox_capacity: 128,
            threading: RuntimeThreading::HostedTokio,
            cold_max_hot_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        };
        let stream = BucketStreamId::new("benchcmp", "wal-batch");
        let placement;

        {
            let runtime = ShardRuntime::spawn_with_engine_factory(
                config.clone(),
                WalGroupEngineFactory::new(&wal_root),
            )
            .expect("spawn runtime");
            placement = runtime.locate(&stream);
            create_stream(&runtime, &stream).await;
            let response = runtime
                .append_batch(AppendBatchRequest::new(
                    stream.clone(),
                    vec![b"ab".to_vec(), b"cd".to_vec(), b"ef".to_vec()],
                ))
                .await
                .expect("append batch");
            assert_eq!(response.items.len(), 3);
            assert!(response.items.iter().all(Result::is_ok));

            let read = runtime
                .read_stream(ReadStreamRequest {
                    stream_id: stream.clone(),
                    offset: 0,
                    max_len: 16,
                    now_ms: 0,
                })
                .await
                .expect("read");
            assert_eq!(read.payload, b"abcdef");

            let snapshot = runtime.metrics().snapshot();
            let core_index = usize::from(placement.core_id.0);
            let group_index = usize::try_from(placement.raft_group_id.0).expect("u32 fits usize");
            assert_eq!(snapshot.wal_batches, 2);
            assert_eq!(snapshot.wal_records, 2);
            assert_eq!(snapshot.per_core_wal_batches[core_index], 2);
            assert_eq!(snapshot.per_group_wal_batches[group_index], 2);
            assert_eq!(snapshot.per_core_wal_records[core_index], 2);
            assert_eq!(snapshot.per_group_wal_records[group_index], 2);
            assert!(snapshot.wal_write_ns > 0);
            assert!(snapshot.wal_sync_ns > 0);
            assert_eq!(
                snapshot.wal_write_ns,
                snapshot.per_core_wal_write_ns.iter().sum::<u64>()
            );
            assert_eq!(
                snapshot.wal_sync_ns,
                snapshot.per_group_wal_sync_ns.iter().sum::<u64>()
            );
        }

        let log_path = group_log_path(&wal_root, placement);
        let line_count = std::fs::read_to_string(&log_path)
            .expect("read WAL log")
            .lines()
            .count();
        assert_eq!(line_count, 2);

        let recovered =
            ShardRuntime::spawn_with_engine_factory(config, WalGroupEngineFactory::new(&wal_root))
                .expect("spawn recovered runtime");
        let read = recovered
            .read_stream(ReadStreamRequest {
                stream_id: stream,
                offset: 0,
                max_len: 16,
                now_ms: 0,
            })
            .await
            .expect("read recovered batch");
        assert_eq!(read.payload, b"abcdef");

        drop(recovered);
        std::fs::remove_dir_all(&wal_root).expect("remove WAL root");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wal_group_engine_persists_installed_snapshot() {
        let wal_root = std::env::temp_dir().join(format!(
            "ursula-wal-install-snapshot-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&wal_root);
        let config = RuntimeConfig {
            core_count: 2,
            raft_group_count: 8,
            mailbox_capacity: 128,
            threading: RuntimeThreading::HostedTokio,
            cold_max_hot_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        };
        let stream = BucketStreamId::new("benchcmp", "wal-installed-snapshot");
        let source = runtime(2, 8);
        let placement = source.locate(&stream);
        create_stream(&source, &stream).await;
        source
            .append(AppendRequest::from_bytes(
                stream.clone(),
                b"snapshot-payload".to_vec(),
            ))
            .await
            .expect("append source");
        let snapshot = source
            .snapshot_group(placement.raft_group_id)
            .await
            .expect("snapshot source");

        {
            let target = ShardRuntime::spawn_with_engine_factory(
                config.clone(),
                WalGroupEngineFactory::new(&wal_root),
            )
            .expect("spawn WAL runtime");
            target
                .install_group_snapshot(snapshot)
                .await
                .expect("install snapshot");
        }

        let recovered =
            ShardRuntime::spawn_with_engine_factory(config, WalGroupEngineFactory::new(&wal_root))
                .expect("spawn recovered WAL runtime");
        let read = recovered
            .read_stream(ReadStreamRequest {
                stream_id: stream.clone(),
                offset: 0,
                max_len: 32,
                now_ms: 0,
            })
            .await
            .expect("read recovered snapshot");
        assert_eq!(read.payload, b"snapshot-payload");

        let appended = recovered
            .append(AppendRequest::from_bytes(stream, b"-next".to_vec()))
            .await
            .expect("append after recovered snapshot");
        assert_eq!(appended.start_offset, 16);
        assert_eq!(appended.stream_append_count, 2);

        drop(recovered);
        std::fs::remove_dir_all(&wal_root).expect("remove WAL root");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wal_group_engine_recovers_producer_dedup_state() {
        let wal_root = std::env::temp_dir().join(format!(
            "ursula-wal-producer-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&wal_root);
        let config = RuntimeConfig {
            core_count: 2,
            raft_group_count: 8,
            mailbox_capacity: 128,
            threading: RuntimeThreading::HostedTokio,
            cold_max_hot_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        };
        let stream = BucketStreamId::new("benchcmp", "wal-producer");

        {
            let runtime = ShardRuntime::spawn_with_engine_factory(
                config.clone(),
                WalGroupEngineFactory::new(&wal_root),
            )
            .expect("spawn WAL runtime");
            create_stream(&runtime, &stream).await;
            let mut append = AppendRequest::from_bytes(stream.clone(), b"a".to_vec());
            append.producer = Some(producer("writer-1", 0, 0));
            runtime.append(append).await.expect("append");
        }

        let recovered =
            ShardRuntime::spawn_with_engine_factory(config, WalGroupEngineFactory::new(&wal_root))
                .expect("spawn recovered runtime");
        let mut duplicate = AppendRequest::from_bytes(stream.clone(), b"ignored".to_vec());
        duplicate.producer = Some(producer("writer-1", 0, 0));
        let duplicate = recovered
            .append(duplicate)
            .await
            .expect("deduplicated retry");
        assert!(duplicate.deduplicated);
        assert_eq!(duplicate.start_offset, 0);
        assert_eq!(duplicate.next_offset, 1);
        assert_eq!(duplicate.stream_append_count, 1);

        let mut next = AppendRequest::from_bytes(stream.clone(), b"b".to_vec());
        next.producer = Some(producer("writer-1", 0, 1));
        let next = recovered.append(next).await.expect("next append");
        assert_eq!(next.start_offset, 1);
        assert_eq!(next.next_offset, 2);
        assert_eq!(next.stream_append_count, 2);

        let read = recovered
            .read_stream(ReadStreamRequest {
                stream_id: stream,
                offset: 0,
                max_len: 16,
                now_ms: 0,
            })
            .await
            .expect("read");
        assert_eq!(read.payload, b"ab");

        drop(recovered);
        std::fs::remove_dir_all(&wal_root).expect("remove WAL root");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wal_group_engine_recovers_producer_append_batch_dedup_state() {
        let wal_root = std::env::temp_dir().join(format!(
            "ursula-wal-producer-batch-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&wal_root);
        let config = RuntimeConfig {
            core_count: 2,
            raft_group_count: 8,
            mailbox_capacity: 128,
            threading: RuntimeThreading::HostedTokio,
            cold_max_hot_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        };
        let stream = BucketStreamId::new("benchcmp", "wal-producer-batch");
        let placement;

        {
            let runtime = ShardRuntime::spawn_with_engine_factory(
                config.clone(),
                WalGroupEngineFactory::new(&wal_root),
            )
            .expect("spawn WAL runtime");
            placement = runtime.locate(&stream);
            create_stream(&runtime, &stream).await;

            let mut first =
                AppendBatchRequest::new(stream.clone(), vec![b"a".to_vec(), b"b".to_vec()]);
            first.producer = Some(producer("writer-1", 0, 0));
            let first = runtime.append_batch(first).await.expect("first batch");
            assert!(first.items.iter().all(Result::is_ok));

            let mut duplicate = AppendBatchRequest::new(stream.clone(), vec![b"ignored".to_vec()]);
            duplicate.producer = Some(producer("writer-1", 0, 0));
            let duplicate = runtime
                .append_batch(duplicate)
                .await
                .expect("duplicate batch");
            assert!(
                duplicate
                    .items
                    .iter()
                    .all(|item| { item.as_ref().expect("deduplicated item").deduplicated })
            );
        }

        let log_path = group_log_path(&wal_root, placement);
        let line_count = std::fs::read_to_string(&log_path)
            .expect("read WAL log")
            .lines()
            .count();
        assert_eq!(line_count, 2);

        let recovered =
            ShardRuntime::spawn_with_engine_factory(config, WalGroupEngineFactory::new(&wal_root))
                .expect("spawn recovered runtime");
        let mut duplicate = AppendBatchRequest::new(stream.clone(), vec![b"retry".to_vec()]);
        duplicate.producer = Some(producer("writer-1", 0, 0));
        let duplicate = recovered
            .append_batch(duplicate)
            .await
            .expect("deduplicated retry");
        assert_eq!(duplicate.items.len(), 2);
        assert!(
            duplicate
                .items
                .iter()
                .all(|item| { item.as_ref().expect("deduplicated item").deduplicated })
        );

        let mut next = AppendBatchRequest::new(stream.clone(), vec![b"c".to_vec()]);
        next.producer = Some(producer("writer-1", 0, 1));
        let next = recovered.append_batch(next).await.expect("next batch");
        assert_eq!(next.items[0].as_ref().expect("next item").start_offset, 2);

        let read = recovered
            .read_stream(ReadStreamRequest {
                stream_id: stream,
                offset: 0,
                max_len: 16,
                now_ms: 0,
            })
            .await
            .expect("read");
        assert_eq!(read.payload, b"abc");

        drop(recovered);
        std::fs::remove_dir_all(&wal_root).expect("remove WAL root");
    }

    #[derive(Debug, Clone)]
    struct RecordingFactory {
        created: Arc<Mutex<Vec<ShardPlacement>>>,
        accepts_local_writes: bool,
    }

    impl Default for RecordingFactory {
        fn default() -> Self {
            Self {
                created: Arc::default(),
                accepts_local_writes: true,
            }
        }
    }

    impl RecordingFactory {
        fn without_local_writes() -> Self {
            Self {
                accepts_local_writes: false,
                ..Self::default()
            }
        }

        fn created(&self) -> Vec<ShardPlacement> {
            self.created.lock().expect("lock created groups").clone()
        }
    }

    impl GroupEngineFactory for RecordingFactory {
        fn create<'a>(
            &'a self,
            placement: ShardPlacement,
            _metrics: GroupEngineMetrics,
        ) -> GroupEngineCreateFuture<'a> {
            Box::pin(async move {
                self.created
                    .lock()
                    .expect("lock created groups")
                    .push(placement);
                let engine: Box<dyn GroupEngine> = Box::new(RecordingEngine {
                    placement,
                    commit_index: 0,
                    accepts_local_writes: self.accepts_local_writes,
                });
                Ok(engine)
            })
        }
    }

    struct RecordingEngine {
        placement: ShardPlacement,
        commit_index: u64,
        accepts_local_writes: bool,
    }

    #[derive(Clone)]
    struct BlockingReadFactory {
        entered: Arc<Notify>,
        release: Arc<Notify>,
        read_count: Arc<AtomicU64>,
        block_parts: bool,
    }

    impl Default for BlockingReadFactory {
        fn default() -> Self {
            Self {
                entered: Arc::new(Notify::new()),
                release: Arc::new(Notify::new()),
                read_count: Arc::new(AtomicU64::new(0)),
                block_parts: false,
            }
        }
    }

    impl BlockingReadFactory {
        fn block_materialization() -> Self {
            Self {
                block_parts: true,
                ..Self::default()
            }
        }
    }

    impl GroupEngineFactory for BlockingReadFactory {
        fn create<'a>(
            &'a self,
            placement: ShardPlacement,
            _metrics: GroupEngineMetrics,
        ) -> GroupEngineCreateFuture<'a> {
            Box::pin(async move {
                let engine: Box<dyn GroupEngine> = Box::new(BlockingReadEngine {
                    inner: InMemoryGroupEngine::default(),
                    placement,
                    entered: self.entered.clone(),
                    release: self.release.clone(),
                    read_count: self.read_count.clone(),
                    block_parts: self.block_parts,
                });
                Ok(engine)
            })
        }
    }

    struct BlockingReadEngine {
        inner: InMemoryGroupEngine,
        placement: ShardPlacement,
        entered: Arc<Notify>,
        release: Arc<Notify>,
        read_count: Arc<AtomicU64>,
        block_parts: bool,
    }

    impl GroupEngine for BlockingReadEngine {
        fn create_stream<'a>(
            &'a mut self,
            request: CreateStreamRequest,
            placement: ShardPlacement,
        ) -> GroupCreateStreamFuture<'a> {
            self.inner.create_stream(request, placement)
        }

        fn head_stream<'a>(
            &'a mut self,
            request: HeadStreamRequest,
            placement: ShardPlacement,
        ) -> GroupHeadStreamFuture<'a> {
            self.inner.head_stream(request, placement)
        }

        fn read_stream<'a>(
            &'a mut self,
            request: ReadStreamRequest,
            placement: ShardPlacement,
        ) -> GroupReadStreamFuture<'a> {
            let entered = self.entered.clone();
            let release = self.release.clone();
            let read_count = self.read_count.clone();
            Box::pin(async move {
                assert_eq!(placement, self.placement);
                read_count.fetch_add(1, Ordering::Relaxed);
                entered.notify_one();
                release.notified().await;
                Ok(ReadStreamResponse {
                    placement,
                    offset: request.offset,
                    next_offset: request.offset,
                    content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                    payload: Vec::new(),
                    up_to_date: true,
                    closed: false,
                })
            })
        }

        fn read_stream_parts<'a>(
            &'a mut self,
            request: ReadStreamRequest,
            placement: ShardPlacement,
        ) -> GroupReadStreamPartsFuture<'a> {
            let entered = self.entered.clone();
            let read_count = self.read_count.clone();
            Box::pin(async move {
                assert_eq!(placement, self.placement);
                read_count.fetch_add(1, Ordering::Relaxed);
                entered.notify_one();
                if self.block_parts {
                    return Ok(GroupReadStreamParts {
                        placement,
                        offset: request.offset,
                        next_offset: request.offset
                            + u64::try_from(b"ready".len()).expect("payload len fits u64"),
                        content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                        up_to_date: true,
                        closed: false,
                        body: GroupReadStreamBody::Blocking {
                            entered: self.entered.clone(),
                            release: self.release.clone(),
                            payload: b"ready".to_vec(),
                        },
                    });
                }
                let response = ReadStreamResponse {
                    placement,
                    offset: request.offset,
                    next_offset: request.offset,
                    content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                    payload: Vec::new(),
                    up_to_date: true,
                    closed: false,
                };
                Ok(GroupReadStreamParts::from_response(response))
            })
        }

        fn touch_stream_access<'a>(
            &'a mut self,
            stream_id: BucketStreamId,
            now_ms: u64,
            renew_ttl: bool,
            placement: ShardPlacement,
        ) -> GroupTouchStreamAccessFuture<'a> {
            self.inner
                .touch_stream_access(stream_id, now_ms, renew_ttl, placement)
        }

        fn add_fork_ref<'a>(
            &'a mut self,
            stream_id: BucketStreamId,
            now_ms: u64,
            placement: ShardPlacement,
        ) -> GroupForkRefFuture<'a> {
            self.inner.add_fork_ref(stream_id, now_ms, placement)
        }

        fn release_fork_ref<'a>(
            &'a mut self,
            stream_id: BucketStreamId,
            placement: ShardPlacement,
        ) -> GroupForkRefFuture<'a> {
            self.inner.release_fork_ref(stream_id, placement)
        }

        fn close_stream<'a>(
            &'a mut self,
            request: CloseStreamRequest,
            placement: ShardPlacement,
        ) -> GroupCloseStreamFuture<'a> {
            self.inner.close_stream(request, placement)
        }

        fn delete_stream<'a>(
            &'a mut self,
            request: DeleteStreamRequest,
            placement: ShardPlacement,
        ) -> GroupDeleteStreamFuture<'a> {
            self.inner.delete_stream(request, placement)
        }

        fn append<'a>(
            &'a mut self,
            request: AppendRequest,
            placement: ShardPlacement,
        ) -> GroupAppendFuture<'a> {
            self.inner.append(request, placement)
        }

        fn append_batch<'a>(
            &'a mut self,
            request: AppendBatchRequest,
            placement: ShardPlacement,
        ) -> GroupAppendBatchFuture<'a> {
            self.inner.append_batch(request, placement)
        }

        fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
            Box::pin(async move {
                Ok(GroupSnapshot {
                    placement,
                    group_commit_index: 0,
                    stream_snapshot: StreamSnapshot {
                        buckets: Vec::new(),
                        streams: Vec::new(),
                    },
                    stream_append_counts: Vec::new(),
                })
            })
        }

        fn install_snapshot<'a>(
            &'a mut self,
            _snapshot: GroupSnapshot,
        ) -> GroupInstallSnapshotFuture<'a> {
            Box::pin(async { Ok(()) })
        }
    }

    impl GroupEngine for RecordingEngine {
        fn accepts_local_writes(&self) -> bool {
            self.accepts_local_writes
        }

        fn create_stream<'a>(
            &'a mut self,
            request: CreateStreamRequest,
            placement: ShardPlacement,
        ) -> GroupCreateStreamFuture<'a> {
            Box::pin(async move {
                assert_eq!(placement, self.placement);
                self.commit_index += 1;
                Ok(CreateStreamResponse {
                    placement,
                    next_offset: u64::try_from(request.initial_payload.len())
                        .expect("payload len fits u64"),
                    closed: request.close_after,
                    already_exists: false,
                    group_commit_index: self.commit_index,
                })
            })
        }

        fn head_stream<'a>(
            &'a mut self,
            request: HeadStreamRequest,
            placement: ShardPlacement,
        ) -> GroupHeadStreamFuture<'a> {
            Box::pin(async move {
                assert_eq!(placement, self.placement);
                Ok(HeadStreamResponse {
                    placement,
                    content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                    tail_offset: request.stream_id.stream_id.len() as u64,
                    closed: false,
                    stream_ttl_seconds: None,
                    stream_expires_at_ms: None,
                    snapshot_offset: None,
                })
            })
        }

        fn read_stream<'a>(
            &'a mut self,
            request: ReadStreamRequest,
            placement: ShardPlacement,
        ) -> GroupReadStreamFuture<'a> {
            Box::pin(async move {
                assert_eq!(placement, self.placement);
                Ok(ReadStreamResponse {
                    placement,
                    offset: request.offset,
                    next_offset: request.offset,
                    content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                    payload: Vec::new(),
                    up_to_date: true,
                    closed: false,
                })
            })
        }

        fn touch_stream_access<'a>(
            &'a mut self,
            _stream_id: BucketStreamId,
            _now_ms: u64,
            _renew_ttl: bool,
            placement: ShardPlacement,
        ) -> GroupTouchStreamAccessFuture<'a> {
            Box::pin(async move {
                assert_eq!(placement, self.placement);
                Ok(TouchStreamAccessResponse {
                    placement,
                    changed: false,
                    expired: false,
                    group_commit_index: self.commit_index,
                })
            })
        }

        fn add_fork_ref<'a>(
            &'a mut self,
            _stream_id: BucketStreamId,
            _now_ms: u64,
            placement: ShardPlacement,
        ) -> GroupForkRefFuture<'a> {
            Box::pin(async move {
                assert_eq!(placement, self.placement);
                self.commit_index += 1;
                Ok(ForkRefResponse {
                    placement,
                    fork_ref_count: 1,
                    hard_deleted: false,
                    parent_to_release: None,
                    group_commit_index: self.commit_index,
                })
            })
        }

        fn release_fork_ref<'a>(
            &'a mut self,
            _stream_id: BucketStreamId,
            placement: ShardPlacement,
        ) -> GroupForkRefFuture<'a> {
            Box::pin(async move {
                assert_eq!(placement, self.placement);
                self.commit_index += 1;
                Ok(ForkRefResponse {
                    placement,
                    fork_ref_count: 0,
                    hard_deleted: false,
                    parent_to_release: None,
                    group_commit_index: self.commit_index,
                })
            })
        }

        fn close_stream<'a>(
            &'a mut self,
            _request: CloseStreamRequest,
            placement: ShardPlacement,
        ) -> GroupCloseStreamFuture<'a> {
            Box::pin(async move {
                assert_eq!(placement, self.placement);
                self.commit_index += 1;
                Ok(CloseStreamResponse {
                    placement,
                    next_offset: self.commit_index,
                    group_commit_index: self.commit_index,
                    deduplicated: false,
                })
            })
        }

        fn delete_stream<'a>(
            &'a mut self,
            _request: DeleteStreamRequest,
            placement: ShardPlacement,
        ) -> GroupDeleteStreamFuture<'a> {
            Box::pin(async move {
                assert_eq!(placement, self.placement);
                self.commit_index += 1;
                Ok(DeleteStreamResponse {
                    placement,
                    group_commit_index: self.commit_index,
                    hard_deleted: true,
                    parent_to_release: None,
                })
            })
        }

        fn append<'a>(
            &'a mut self,
            request: AppendRequest,
            placement: ShardPlacement,
        ) -> GroupAppendFuture<'a> {
            Box::pin(async move {
                assert_eq!(placement, self.placement);
                let start_offset = self.commit_index;
                let next_offset = start_offset + request.payload_len();
                self.commit_index += 1;
                Ok(AppendResponse {
                    placement,
                    start_offset,
                    next_offset,
                    stream_append_count: self.commit_index,
                    group_commit_index: self.commit_index,
                    closed: request.close_after,
                    deduplicated: false,
                    producer: request.producer,
                })
            })
        }

        fn append_batch<'a>(
            &'a mut self,
            request: AppendBatchRequest,
            placement: ShardPlacement,
        ) -> GroupAppendBatchFuture<'a> {
            Box::pin(async move {
                assert_eq!(placement, self.placement);
                let AppendBatchRequest {
                    stream_id: _,
                    content_type: _,
                    payloads,
                    producer: _,
                    ..
                } = request;
                let mut items = Vec::with_capacity(payloads.len());
                for payload in payloads {
                    let start_offset = self.commit_index;
                    let next_offset =
                        start_offset + u64::try_from(payload.len()).expect("payload len fits u64");
                    self.commit_index += 1;
                    items.push(Ok(AppendResponse {
                        placement,
                        start_offset,
                        next_offset,
                        stream_append_count: self.commit_index,
                        group_commit_index: self.commit_index,
                        closed: false,
                        deduplicated: false,
                        producer: None,
                    }));
                }
                Ok(GroupAppendBatchResponse { placement, items })
            })
        }

        fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
            Box::pin(async move {
                assert_eq!(placement, self.placement);
                Ok(GroupSnapshot {
                    placement,
                    group_commit_index: self.commit_index,
                    stream_snapshot: StreamSnapshot {
                        buckets: Vec::new(),
                        streams: Vec::new(),
                    },
                    stream_append_counts: Vec::new(),
                })
            })
        }

        fn install_snapshot<'a>(
            &'a mut self,
            snapshot: GroupSnapshot,
        ) -> GroupInstallSnapshotFuture<'a> {
            Box::pin(async move {
                assert_eq!(snapshot.placement, self.placement);
                self.commit_index = snapshot.group_commit_index;
                Ok(())
            })
        }
    }

    #[derive(Debug, Clone)]
    struct BlockingFirstCreateEngineFactory {
        first_create_blocks: Arc<AtomicBool>,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl Default for BlockingFirstCreateEngineFactory {
        fn default() -> Self {
            Self {
                first_create_blocks: Arc::new(AtomicBool::new(true)),
                entered: Arc::new(Notify::new()),
                release: Arc::new(Notify::new()),
            }
        }
    }

    impl GroupEngineFactory for BlockingFirstCreateEngineFactory {
        fn create<'a>(
            &'a self,
            _placement: ShardPlacement,
            _metrics: GroupEngineMetrics,
        ) -> GroupEngineCreateFuture<'a> {
            Box::pin(async move {
                let engine: Box<dyn GroupEngine> = Box::new(BlockingFirstCreateEngine {
                    inner: InMemoryGroupEngine::default(),
                    first_create_blocks: self.first_create_blocks.clone(),
                    entered: self.entered.clone(),
                    release: self.release.clone(),
                });
                Ok(engine)
            })
        }
    }

    struct BlockingFirstCreateEngine {
        inner: InMemoryGroupEngine,
        first_create_blocks: Arc<AtomicBool>,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl GroupEngine for BlockingFirstCreateEngine {
        fn create_stream<'a>(
            &'a mut self,
            request: CreateStreamRequest,
            placement: ShardPlacement,
        ) -> GroupCreateStreamFuture<'a> {
            let should_block = self.first_create_blocks.swap(false, Ordering::SeqCst);
            let entered = self.entered.clone();
            let release = self.release.clone();
            Box::pin(async move {
                if should_block {
                    entered.notify_one();
                    release.notified().await;
                }
                self.inner.create_stream(request, placement).await
            })
        }

        fn head_stream<'a>(
            &'a mut self,
            request: HeadStreamRequest,
            placement: ShardPlacement,
        ) -> GroupHeadStreamFuture<'a> {
            self.inner.head_stream(request, placement)
        }

        fn read_stream<'a>(
            &'a mut self,
            request: ReadStreamRequest,
            placement: ShardPlacement,
        ) -> GroupReadStreamFuture<'a> {
            self.inner.read_stream(request, placement)
        }

        fn touch_stream_access<'a>(
            &'a mut self,
            stream_id: BucketStreamId,
            now_ms: u64,
            renew_ttl: bool,
            placement: ShardPlacement,
        ) -> GroupTouchStreamAccessFuture<'a> {
            self.inner
                .touch_stream_access(stream_id, now_ms, renew_ttl, placement)
        }

        fn add_fork_ref<'a>(
            &'a mut self,
            stream_id: BucketStreamId,
            now_ms: u64,
            placement: ShardPlacement,
        ) -> GroupForkRefFuture<'a> {
            self.inner.add_fork_ref(stream_id, now_ms, placement)
        }

        fn release_fork_ref<'a>(
            &'a mut self,
            stream_id: BucketStreamId,
            placement: ShardPlacement,
        ) -> GroupForkRefFuture<'a> {
            self.inner.release_fork_ref(stream_id, placement)
        }

        fn close_stream<'a>(
            &'a mut self,
            request: CloseStreamRequest,
            placement: ShardPlacement,
        ) -> GroupCloseStreamFuture<'a> {
            self.inner.close_stream(request, placement)
        }

        fn delete_stream<'a>(
            &'a mut self,
            request: DeleteStreamRequest,
            placement: ShardPlacement,
        ) -> GroupDeleteStreamFuture<'a> {
            self.inner.delete_stream(request, placement)
        }

        fn append<'a>(
            &'a mut self,
            request: AppendRequest,
            placement: ShardPlacement,
        ) -> GroupAppendFuture<'a> {
            self.inner.append(request, placement)
        }

        fn append_batch<'a>(
            &'a mut self,
            request: AppendBatchRequest,
            placement: ShardPlacement,
        ) -> GroupAppendBatchFuture<'a> {
            self.inner.append_batch(request, placement)
        }

        fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
            self.inner.snapshot(placement)
        }

        fn install_snapshot<'a>(
            &'a mut self,
            snapshot: GroupSnapshot,
        ) -> GroupInstallSnapshotFuture<'a> {
            self.inner.install_snapshot(snapshot)
        }
    }

    #[derive(Debug, Clone)]
    struct BlockingOnceFactory {
        first_create_blocks: Arc<AtomicBool>,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl Default for BlockingOnceFactory {
        fn default() -> Self {
            Self {
                first_create_blocks: Arc::new(AtomicBool::new(true)),
                entered: Arc::new(Notify::new()),
                release: Arc::new(Notify::new()),
            }
        }
    }

    impl GroupEngineFactory for BlockingOnceFactory {
        fn create<'a>(
            &'a self,
            _placement: ShardPlacement,
            _metrics: GroupEngineMetrics,
        ) -> GroupEngineCreateFuture<'a> {
            Box::pin(async move {
                if self.first_create_blocks.swap(false, Ordering::SeqCst) {
                    self.entered.notify_one();
                    self.release.notified().await;
                }
                let engine: Box<dyn GroupEngine> = Box::new(InMemoryGroupEngine::default());
                Ok(engine)
            })
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct FailingFactory;

    impl GroupEngineFactory for FailingFactory {
        fn create<'a>(
            &'a self,
            _placement: ShardPlacement,
            _metrics: GroupEngineMetrics,
        ) -> GroupEngineCreateFuture<'a> {
            Box::pin(async {
                let engine: Box<dyn GroupEngine> = Box::new(FailingEngine);
                Ok(engine)
            })
        }
    }

    struct FailingEngine;

    impl GroupEngine for FailingEngine {
        fn create_stream<'a>(
            &'a mut self,
            _request: CreateStreamRequest,
            _placement: ShardPlacement,
        ) -> GroupCreateStreamFuture<'a> {
            Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
        }

        fn head_stream<'a>(
            &'a mut self,
            _request: HeadStreamRequest,
            _placement: ShardPlacement,
        ) -> GroupHeadStreamFuture<'a> {
            Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
        }

        fn read_stream<'a>(
            &'a mut self,
            _request: ReadStreamRequest,
            _placement: ShardPlacement,
        ) -> GroupReadStreamFuture<'a> {
            Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
        }

        fn touch_stream_access<'a>(
            &'a mut self,
            _stream_id: BucketStreamId,
            _now_ms: u64,
            _renew_ttl: bool,
            _placement: ShardPlacement,
        ) -> GroupTouchStreamAccessFuture<'a> {
            Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
        }

        fn add_fork_ref<'a>(
            &'a mut self,
            _stream_id: BucketStreamId,
            _now_ms: u64,
            _placement: ShardPlacement,
        ) -> GroupForkRefFuture<'a> {
            Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
        }

        fn release_fork_ref<'a>(
            &'a mut self,
            _stream_id: BucketStreamId,
            _placement: ShardPlacement,
        ) -> GroupForkRefFuture<'a> {
            Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
        }

        fn close_stream<'a>(
            &'a mut self,
            _request: CloseStreamRequest,
            _placement: ShardPlacement,
        ) -> GroupCloseStreamFuture<'a> {
            Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
        }

        fn delete_stream<'a>(
            &'a mut self,
            _request: DeleteStreamRequest,
            _placement: ShardPlacement,
        ) -> GroupDeleteStreamFuture<'a> {
            Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
        }

        fn append<'a>(
            &'a mut self,
            _request: AppendRequest,
            _placement: ShardPlacement,
        ) -> GroupAppendFuture<'a> {
            Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
        }

        fn append_batch<'a>(
            &'a mut self,
            _request: AppendBatchRequest,
            _placement: ShardPlacement,
        ) -> GroupAppendBatchFuture<'a> {
            Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
        }

        fn snapshot<'a>(&'a mut self, _placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
            Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
        }

        fn install_snapshot<'a>(
            &'a mut self,
            _snapshot: GroupSnapshot,
        ) -> GroupInstallSnapshotFuture<'a> {
            Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
        }
    }
}
