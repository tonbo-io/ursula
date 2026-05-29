use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
#[cfg(not(madsim))]
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use opendal::layers::{RetryLayer, TimeoutLayer};
use opendal::{Operator, Scheme};
use ursula_shard::BucketStreamId;
use ursula_stream::{ColdChunkRef, ObjectPayloadRef};

pub(crate) const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
static COLD_CHUNK_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const DEFAULT_COLD_CACHE_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_COLD_CACHE_BLOCK_BYTES: usize = 1024 * 1024;
const DEFAULT_COLD_CACHE_READAHEAD_BLOCKS: usize = 4;
const DEFAULT_S3_OP_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_S3_MAX_RETRIES: usize = 3;

/// Wrap an S3 (opendal) operator with the resilience every external
/// object-store call needs.
///
/// 1. **Per-attempt timeout** ([`TimeoutLayer`], inner): a blackholed endpoint
///    — chaos `s3_unavailable`, or a "busy ESTAB" TCP socket whose future never
///    gets polled again — otherwise hangs the caller until
///    `net.ipv4.tcp_retries2` (~15 min). That is the original freeze: the raft
///    state-machine worker awaits S3 inside `install_snapshot` (`&mut self`),
///    and openraft type-level-serializes `apply` with it, so an unbounded S3
///    stall freezes apply. Bounding every attempt keeps the worker progressing.
/// 2. **Bounded retries** ([`RetryLayer`], outer): S3 answers `503 SlowDown`
///    while a fresh key prefix warms up (and on transient network blips). These
///    are `is_temporary()` errors; without retries a single 503 fails a
///    snapshot upload/download, stalling a restarted node's rejoin/catch-up.
///    Retries are bounded, so a sustained outage still fails fast enough (each
///    attempt is timeout-bounded) and the cluster keeps progressing on quorum.
///
/// Timeout is applied before retry so each retry attempt is itself bounded
/// (per opendal's layer-ordering requirement). Tunable via `URSULA_S3_TIMEOUT_MS`
/// and `URSULA_S3_MAX_RETRIES`.
pub(crate) fn with_s3_resilience(operator: Operator) -> Operator {
    let timeout_ms = std::env::var("URSULA_S3_TIMEOUT_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(DEFAULT_S3_OP_TIMEOUT_MS);
    let timeout = Duration::from_millis(timeout_ms);
    let max_retries = std::env::var("URSULA_S3_MAX_RETRIES")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(DEFAULT_S3_MAX_RETRIES);
    operator
        .layer(
            TimeoutLayer::new()
                .with_timeout(timeout)
                .with_io_timeout(timeout),
        )
        .layer(RetryLayer::new().with_max_times(max_retries).with_jitter())
}

#[derive(Clone)]
pub struct ColdStore {
    info: ColdStoreInfo,
    operator: Operator,
    read_cache: Option<Arc<ColdReadCache>>,
    observer: Arc<Mutex<Option<ColdStoreObserver>>>,
    fault_policy: Arc<Mutex<Option<ColdStoreFaultPolicy>>>,
    delay_fn: Arc<Mutex<ColdStoreDelayFn>>,
}

pub type ColdStoreHandle = Arc<ColdStore>;

type ColdStoreObserver = Arc<dyn Fn(ColdStoreEvent) + Send + Sync>;
type ColdStoreFaultPolicy =
    Arc<dyn Fn(&ColdStoreFaultContext) -> Option<ColdStoreFaultEffect> + Send + Sync>;
type ColdStoreDelayFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type ColdStoreDelayFn = Arc<dyn Fn(Duration) -> ColdStoreDelayFuture + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColdStoreOperation {
    WriteChunk,
    DeleteChunk,
    RemoveAll,
    ReadObjectRange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdStoreFaultContext {
    pub operation: ColdStoreOperation,
    pub stream_id: Option<BucketStreamId>,
    pub path: String,
    pub payload_len: Option<usize>,
    pub read_start_offset: Option<u64>,
    pub len: Option<usize>,
    pub object_start: Option<u64>,
    pub object_end: Option<u64>,
    pub cached: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdStoreFault {
    pub message: String,
}

impl ColdStoreFault {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColdStoreFaultEffect {
    pub delay: Option<Duration>,
    pub error: Option<ColdStoreFault>,
    pub truncate_read_to: Option<usize>,
}

impl ColdStoreFaultEffect {
    pub fn delay(duration: Duration) -> Self {
        Self {
            delay: Some(duration),
            error: None,
            truncate_read_to: None,
        }
    }

    pub fn fail(message: impl Into<String>) -> Self {
        Self {
            delay: None,
            error: Some(ColdStoreFault::new(message)),
            truncate_read_to: None,
        }
    }

    pub fn delay_then_fail(duration: Duration, message: impl Into<String>) -> Self {
        Self {
            delay: Some(duration),
            error: Some(ColdStoreFault::new(message)),
            truncate_read_to: None,
        }
    }

    pub fn truncate_read_to(len: usize) -> Self {
        Self {
            delay: None,
            error: None,
            truncate_read_to: Some(len),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColdStoreEvent {
    WriteChunkBegin {
        path: String,
        payload_len: usize,
    },
    WriteChunkComplete {
        path: String,
        object_size: u64,
    },
    DeleteChunkBegin {
        path: String,
    },
    DeleteChunkComplete {
        path: String,
    },
    RemoveAllBegin {
        path: String,
    },
    RemoveAllComplete {
        path: String,
    },
    ReadObjectRangeBegin {
        stream_id: Option<BucketStreamId>,
        path: String,
        read_start_offset: u64,
        len: usize,
        object_start: u64,
        object_end: u64,
        cached: bool,
    },
    ReadObjectRangeComplete {
        stream_id: Option<BucketStreamId>,
        path: String,
        read_start_offset: u64,
        len: usize,
        returned_len: usize,
        cached: bool,
    },
    FaultInjected {
        operation: ColdStoreOperation,
        stream_id: Option<BucketStreamId>,
        path: String,
        message: String,
    },
    DelayInjected {
        operation: ColdStoreOperation,
        stream_id: Option<BucketStreamId>,
        path: String,
        delay_ms: u64,
    },
    TruncateInjected {
        stream_id: Option<BucketStreamId>,
        path: String,
        requested_len: usize,
        returned_len: usize,
    },
}

impl fmt::Debug for ColdStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ColdStore")
            .field("info", &self.info)
            .field("operator", &self.operator)
            .field("read_cache", &self.read_cache)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColdStoreInfo {
    pub backend: &'static str,
    pub root: Option<String>,
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
}

impl ColdStore {
    pub fn memory() -> io::Result<Self> {
        let operator = Operator::via_iter(Scheme::Memory, [])
            .map_err(|err| io::Error::other(err.to_string()))?;
        Ok(Self::from_operator(
            operator,
            ColdStoreInfo {
                backend: "memory",
                root: None,
                bucket: None,
                region: None,
                endpoint: None,
            },
        ))
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
        let mut configured_root = None;
        if let Some(root) = root_override {
            if !root.trim().is_empty() {
                builder = builder.root(root);
                configured_root = Some(root.to_owned());
            }
        } else if let Ok(root) = std::env::var("URSULA_COLD_ROOT")
            && !root.trim().is_empty()
        {
            builder = builder.root(&root);
            configured_root = Some(root);
        }
        let mut configured_region = None;
        if let Ok(region) = std::env::var("URSULA_COLD_S3_REGION")
            && !region.trim().is_empty()
        {
            builder = builder.region(&region);
            configured_region = Some(region);
        }
        let mut configured_endpoint = None;
        if let Ok(endpoint) = std::env::var("URSULA_COLD_S3_ENDPOINT")
            && !endpoint.trim().is_empty()
        {
            builder = builder.endpoint(&endpoint);
            configured_endpoint = Some(endpoint);
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

        Ok(Self::from_operator(
            with_s3_resilience(
                Operator::new(builder)
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .finish(),
            ),
            ColdStoreInfo {
                backend: "s3",
                root: configured_root,
                bucket: Some(bucket),
                region: configured_region,
                endpoint: configured_endpoint,
            },
        ))
    }

    pub fn from_env() -> io::Result<Option<ColdStoreHandle>> {
        let backend = std::env::var("URSULA_COLD_BACKEND")
            .unwrap_or_else(|_| "none".to_owned())
            .to_ascii_lowercase();
        Self::from_backend(&backend)
    }

    fn from_backend(backend: &str) -> io::Result<Option<ColdStoreHandle>> {
        let store = match backend {
            "none" | "disabled" | "off" => return Ok(None),
            "memory" | "mem" | "inmem" => Self::memory()?,
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

    pub fn from_operator(operator: Operator, info: ColdStoreInfo) -> Self {
        Self {
            info,
            operator,
            read_cache: ColdReadCache::from_env().map(Arc::new),
            observer: Arc::new(Mutex::new(None)),
            fault_policy: Arc::new(Mutex::new(None)),
            delay_fn: Arc::new(Mutex::new(default_cold_store_delay_fn())),
        }
    }

    pub fn info(&self) -> &ColdStoreInfo {
        &self.info
    }

    pub fn with_read_cache(mut self, config: ColdReadCacheConfig) -> Self {
        self.read_cache = Some(Arc::new(ColdReadCache::new(config)));
        self
    }

    pub fn without_read_cache(mut self) -> Self {
        self.read_cache = None;
        self
    }

    pub fn set_observer(&self, observer: impl Fn(ColdStoreEvent) + Send + Sync + 'static) {
        *self.observer.lock().expect("cold store observer mutex") = Some(Arc::new(observer));
    }

    pub fn set_fault_policy(
        &self,
        policy: impl Fn(&ColdStoreFaultContext) -> Option<ColdStoreFaultEffect> + Send + Sync + 'static,
    ) {
        *self
            .fault_policy
            .lock()
            .expect("cold store fault policy mutex") = Some(Arc::new(policy));
    }

    pub fn clear_fault_policy(&self) {
        *self
            .fault_policy
            .lock()
            .expect("cold store fault policy mutex") = None;
    }

    pub fn set_delay_fn<F, Fut>(&self, delay_fn: F)
    where
        F: Fn(Duration) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        *self.delay_fn.lock().expect("cold store delay fn mutex") =
            Arc::new(move |duration| Box::pin(delay_fn(duration)));
    }

    #[cfg(test)]
    pub(crate) fn cached_block_count(&self) -> usize {
        self.read_cache
            .as_ref()
            .map(|cache| cache.block_count())
            .unwrap_or(0)
    }

    pub async fn write_chunk(&self, path: &str, payload: &[u8]) -> io::Result<u64> {
        if path.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cold chunk path must not be empty",
            ));
        }
        self.notify(ColdStoreEvent::WriteChunkBegin {
            path: path.to_owned(),
            payload_len: payload.len(),
        });
        let _applied_fault = self
            .maybe_apply_fault_effect(ColdStoreFaultContext {
                operation: ColdStoreOperation::WriteChunk,
                stream_id: None,
                path: path.to_owned(),
                payload_len: Some(payload.len()),
                read_start_offset: None,
                len: None,
                object_start: None,
                object_end: None,
                cached: None,
            })
            .await?;
        self.operator
            .write(path, payload.to_vec())
            .await
            .map_err(|err| cold_store_io_error(path, err))?;
        let object_size = u64::try_from(payload.len()).expect("payload len fits u64");
        self.notify(ColdStoreEvent::WriteChunkComplete {
            path: path.to_owned(),
            object_size,
        });
        Ok(object_size)
    }

    pub async fn delete_chunk(&self, path: &str) -> io::Result<()> {
        if path.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cold chunk path must not be empty",
            ));
        }
        self.notify(ColdStoreEvent::DeleteChunkBegin {
            path: path.to_owned(),
        });
        let _applied_fault = self
            .maybe_apply_fault_effect(ColdStoreFaultContext {
                operation: ColdStoreOperation::DeleteChunk,
                stream_id: None,
                path: path.to_owned(),
                payload_len: None,
                read_start_offset: None,
                len: None,
                object_start: None,
                object_end: None,
                cached: None,
            })
            .await?;
        self.operator
            .delete(path)
            .await
            .map_err(|err| cold_store_io_error(path, err))?;
        if let Some(cache) = &self.read_cache {
            cache.invalidate_path(path);
        }
        self.notify(ColdStoreEvent::DeleteChunkComplete {
            path: path.to_owned(),
        });
        Ok(())
    }

    pub async fn remove_all(&self, path: &str) -> io::Result<()> {
        self.notify(ColdStoreEvent::RemoveAllBegin {
            path: path.to_owned(),
        });
        let _applied_fault = self
            .maybe_apply_fault_effect(ColdStoreFaultContext {
                operation: ColdStoreOperation::RemoveAll,
                stream_id: None,
                path: path.to_owned(),
                payload_len: None,
                read_start_offset: None,
                len: None,
                object_start: None,
                object_end: None,
                cached: None,
            })
            .await?;
        self.operator
            .remove_all(path)
            .await
            .map_err(|err| cold_store_io_error(path, err))?;
        if let Some(cache) = &self.read_cache {
            cache.invalidate_prefix(path);
        }
        self.notify(ColdStoreEvent::RemoveAllComplete {
            path: path.to_owned(),
        });
        Ok(())
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

    pub async fn read_object_range_for_stream(
        &self,
        stream_id: &BucketStreamId,
        object: &ObjectPayloadRef,
        read_start_offset: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        self.read_object_range_inner(Some(stream_id), object, read_start_offset, len)
            .await
    }

    pub async fn read_object_range(
        &self,
        object: &ObjectPayloadRef,
        read_start_offset: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        self.read_object_range_inner(None, object, read_start_offset, len)
            .await
    }

    async fn read_object_range_inner(
        &self,
        stream_id: Option<&BucketStreamId>,
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
        let cached = self.read_cache.is_some();
        self.notify(ColdStoreEvent::ReadObjectRangeBegin {
            stream_id: stream_id.cloned(),
            path: object.s3_path.clone(),
            read_start_offset,
            len,
            object_start,
            object_end,
            cached,
        });
        let applied_fault = self
            .maybe_apply_fault_effect(ColdStoreFaultContext {
                operation: ColdStoreOperation::ReadObjectRange,
                stream_id: stream_id.cloned(),
                path: object.s3_path.clone(),
                payload_len: None,
                read_start_offset: Some(read_start_offset),
                len: Some(len),
                object_start: Some(object_start),
                object_end: Some(object_end),
                cached: Some(cached),
            })
            .await?;
        let mut bytes = if let Some(cache) = &self.read_cache {
            let bytes = self
                .read_object_range_cached(cache, object, object_start, object_end, len)
                .await?;
            if let Some(stream_id) = stream_id {
                let readahead_blocks = cache.record_stream_read(stream_id, read_start_offset, len);
                if readahead_blocks > 0 {
                    self.spawn_readahead(object.clone(), object_end, readahead_blocks);
                }
            }
            bytes
        } else {
            self.read_object_range_uncached(object, object_start, object_end, len)
                .await?
        };
        if let Some(returned_len) = applied_fault.truncate_read_to {
            let returned_len = returned_len.min(bytes.len());
            bytes.truncate(returned_len);
            self.notify(ColdStoreEvent::TruncateInjected {
                stream_id: stream_id.cloned(),
                path: object.s3_path.clone(),
                requested_len: len,
                returned_len,
            });
        }
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
        self.notify(ColdStoreEvent::ReadObjectRangeComplete {
            stream_id: stream_id.cloned(),
            path: object.s3_path.clone(),
            read_start_offset,
            len,
            returned_len: bytes.len(),
            cached,
        });
        Ok(bytes)
    }

    async fn read_object_range_uncached(
        &self,
        object: &ObjectPayloadRef,
        object_start: u64,
        object_end: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
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

    async fn read_object_range_cached(
        &self,
        cache: &ColdReadCache,
        object: &ObjectPayloadRef,
        object_start: u64,
        object_end: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        let mut payload = Vec::with_capacity(len);
        let block_size = cache.block_size();
        let first_block = object_start / block_size;
        let last_block = (object_end - 1) / block_size;
        for block_index in first_block..=last_block {
            let block_start = block_index * block_size;
            let block_end = block_start
                .saturating_add(block_size)
                .min(object.object_size);
            let block = self
                .read_cached_block(
                    cache,
                    object.s3_path.clone(),
                    object.object_size,
                    block_index,
                    block_start,
                    block_end,
                )
                .await?;
            let slice_start = usize::try_from(object_start.max(block_start) - block_start)
                .expect("cache slice start fits usize");
            let slice_end = usize::try_from(object_end.min(block_end) - block_start)
                .expect("cache slice end fits usize");
            payload.extend_from_slice(&block.slice(slice_start..slice_end));
        }
        if payload.len() != len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "cold object '{}' returned {} bytes for requested range [{}..{})",
                    object.s3_path,
                    payload.len(),
                    object_start,
                    object_end
                ),
            ));
        }
        Ok(payload)
    }

    async fn read_cached_block(
        &self,
        cache: &ColdReadCache,
        path: String,
        object_size: u64,
        block_index: u64,
        block_start: u64,
        block_end: u64,
    ) -> io::Result<Bytes> {
        if let Some(bytes) = cache.get(&path, block_index) {
            return Ok(bytes);
        }
        let bytes = self
            .operator
            .read_with(&path)
            .range(block_start..block_end)
            .await
            .map_err(|err| cold_store_io_error(&path, err))?
            .to_bytes();
        let expected_len = usize::try_from(block_end - block_start).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "cold cache block length exceeds usize",
            )
        })?;
        if bytes.len() != expected_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "cold object '{path}' returned {} bytes for cache block [{}..{}) of object size {object_size}",
                    bytes.len(),
                    block_start,
                    block_end
                ),
            ));
        }
        cache.insert(path, block_index, bytes.clone());
        Ok(bytes)
    }

    fn spawn_readahead(&self, object: ObjectPayloadRef, object_end: u64, readahead_blocks: usize) {
        let Some(cache) = self.read_cache.clone() else {
            return;
        };
        let block_size = cache.block_size();
        let mut block_index = object_end.div_ceil(block_size);
        let store = self.clone();
        crate::rt::spawn(async move {
            for _ in 0..readahead_blocks {
                let block_start = block_index * block_size;
                if block_start >= object.object_size {
                    break;
                }
                let block_end = block_start
                    .saturating_add(block_size)
                    .min(object.object_size);
                if cache.get(&object.s3_path, block_index).is_none() {
                    let _ = store
                        .read_cached_block(
                            &cache,
                            object.s3_path.clone(),
                            object.object_size,
                            block_index,
                            block_start,
                            block_end,
                        )
                        .await;
                }
                block_index += 1;
            }
        });
    }

    fn notify(&self, event: ColdStoreEvent) {
        let observer = self
            .observer
            .lock()
            .expect("cold store observer mutex")
            .clone();
        if let Some(observer) = observer {
            observer(event);
        }
    }

    async fn maybe_apply_fault_effect(
        &self,
        context: ColdStoreFaultContext,
    ) -> io::Result<ColdStoreAppliedFault> {
        let policy = self
            .fault_policy
            .lock()
            .expect("cold store fault policy mutex")
            .clone();
        let Some(policy) = policy else {
            return Ok(ColdStoreAppliedFault::default());
        };
        let Some(effect) = policy(&context) else {
            return Ok(ColdStoreAppliedFault::default());
        };
        if let Some(delay) = effect.delay {
            self.notify(ColdStoreEvent::DelayInjected {
                operation: context.operation,
                stream_id: context.stream_id.clone(),
                path: context.path.clone(),
                delay_ms: duration_ms(delay),
            });
            let delay_fn = self
                .delay_fn
                .lock()
                .expect("cold store delay fn mutex")
                .clone();
            delay_fn(delay).await;
        }
        if let Some(fault) = effect.error {
            self.notify(ColdStoreEvent::FaultInjected {
                operation: context.operation,
                stream_id: context.stream_id,
                path: context.path.clone(),
                message: fault.message.clone(),
            });
            return Err(io::Error::other(format!(
                "cold store fault injected for {} '{}': {}",
                context.operation.as_str(),
                context.path,
                fault.message
            )));
        }
        Ok(ColdStoreAppliedFault {
            truncate_read_to: effect.truncate_read_to,
        })
    }
}

#[derive(Debug, Default)]
struct ColdStoreAppliedFault {
    truncate_read_to: Option<usize>,
}

impl ColdStoreOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::WriteChunk => "write_chunk",
            Self::DeleteChunk => "delete_chunk",
            Self::RemoveAll => "remove_all",
            Self::ReadObjectRange => "read_object_range",
        }
    }
}

fn default_cold_store_delay_fn() -> ColdStoreDelayFn {
    Arc::new(|duration| Box::pin(crate::rt::time::sleep(duration)))
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[derive(Debug, Clone, Copy)]
pub struct ColdReadCacheConfig {
    pub max_bytes: usize,
    pub block_bytes: usize,
    pub max_readahead_blocks: usize,
}

#[derive(Debug)]
struct ColdReadCache {
    config: ColdReadCacheConfig,
    inner: Mutex<ColdReadCacheInner>,
}

#[derive(Debug, Default)]
struct ColdReadCacheInner {
    blocks: HashMap<ColdCacheKey, ColdCacheEntry>,
    lru: VecDeque<(ColdCacheKey, u64)>,
    current_bytes: usize,
    generation: u64,
    readers: HashMap<BucketStreamId, StreamReadState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ColdCacheKey {
    path: String,
    block_index: u64,
}

#[derive(Debug)]
struct ColdCacheEntry {
    bytes: Bytes,
    generation: u64,
}

#[derive(Debug, Default)]
struct StreamReadState {
    next_offset: u64,
    sequential_score: usize,
}

impl ColdReadCache {
    fn from_env() -> Option<Self> {
        let max_bytes = env_usize("URSULA_COLD_CACHE_BYTES").unwrap_or(DEFAULT_COLD_CACHE_BYTES);
        if max_bytes == 0 {
            return None;
        }
        let block_bytes =
            env_usize("URSULA_COLD_CACHE_BLOCK_BYTES").unwrap_or(DEFAULT_COLD_CACHE_BLOCK_BYTES);
        let max_readahead_blocks = env_usize("URSULA_COLD_CACHE_READAHEAD_BLOCKS")
            .unwrap_or(DEFAULT_COLD_CACHE_READAHEAD_BLOCKS);
        Some(Self::new(ColdReadCacheConfig {
            max_bytes,
            block_bytes,
            max_readahead_blocks,
        }))
    }

    fn new(config: ColdReadCacheConfig) -> Self {
        let block_bytes = config.block_bytes.max(1);
        Self {
            config: ColdReadCacheConfig {
                max_bytes: config.max_bytes,
                block_bytes,
                max_readahead_blocks: config.max_readahead_blocks,
            },
            inner: Mutex::new(ColdReadCacheInner::default()),
        }
    }

    fn block_size(&self) -> u64 {
        u64::try_from(self.config.block_bytes.max(1)).expect("cache block size fits u64")
    }

    fn get(&self, path: &str, block_index: u64) -> Option<Bytes> {
        let mut inner = self.inner.lock().expect("cold cache mutex poisoned");
        let key = ColdCacheKey {
            path: path.to_owned(),
            block_index,
        };
        let bytes = inner.blocks.get(&key)?.bytes.clone();
        Self::touch(&mut inner, key);
        Some(bytes)
    }

    fn insert(&self, path: String, block_index: u64, bytes: Bytes) {
        if bytes.len() > self.config.max_bytes || self.config.max_bytes == 0 {
            return;
        }
        let mut inner = self.inner.lock().expect("cold cache mutex poisoned");
        let key = ColdCacheKey { path, block_index };
        if let Some(previous) = inner.blocks.remove(&key) {
            inner.current_bytes = inner.current_bytes.saturating_sub(previous.bytes.len());
        }
        let generation = Self::next_generation(&mut inner);
        inner.current_bytes = inner.current_bytes.saturating_add(bytes.len());
        inner
            .blocks
            .insert(key.clone(), ColdCacheEntry { bytes, generation });
        inner.lru.push_back((key, generation));
        self.evict_locked(&mut inner);
    }

    fn record_stream_read(
        &self,
        stream_id: &BucketStreamId,
        read_start_offset: u64,
        len: usize,
    ) -> usize {
        let mut inner = self.inner.lock().expect("cold cache mutex poisoned");
        let state = inner.readers.entry(stream_id.clone()).or_default();
        if read_start_offset == state.next_offset {
            state.sequential_score = state
                .sequential_score
                .saturating_add(1)
                .min(self.config.max_readahead_blocks);
        } else {
            state.sequential_score = 0;
        }
        state.next_offset =
            read_start_offset.saturating_add(u64::try_from(len).unwrap_or(u64::MAX));
        state.sequential_score.min(self.config.max_readahead_blocks)
    }

    fn invalidate_path(&self, path: &str) {
        let mut inner = self.inner.lock().expect("cold cache mutex poisoned");
        let keys = inner
            .blocks
            .keys()
            .filter(|key| key.path == path)
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            if let Some(entry) = inner.blocks.remove(&key) {
                inner.current_bytes = inner.current_bytes.saturating_sub(entry.bytes.len());
            }
        }
    }

    fn invalidate_prefix(&self, prefix: &str) {
        let mut inner = self.inner.lock().expect("cold cache mutex poisoned");
        let keys = inner
            .blocks
            .keys()
            .filter(|key| key.path.starts_with(prefix))
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            if let Some(entry) = inner.blocks.remove(&key) {
                inner.current_bytes = inner.current_bytes.saturating_sub(entry.bytes.len());
            }
        }
    }

    #[cfg(test)]
    fn block_count(&self) -> usize {
        self.inner
            .lock()
            .expect("cold cache mutex poisoned")
            .blocks
            .len()
    }

    fn touch(inner: &mut ColdReadCacheInner, key: ColdCacheKey) {
        let generation = Self::next_generation(inner);
        if let Some(entry) = inner.blocks.get_mut(&key) {
            entry.generation = generation;
        }
        inner.lru.push_back((key, generation));
    }

    fn next_generation(inner: &mut ColdReadCacheInner) -> u64 {
        inner.generation = inner.generation.wrapping_add(1);
        inner.generation
    }

    fn evict_locked(&self, inner: &mut ColdReadCacheInner) {
        while inner.current_bytes > self.config.max_bytes {
            let Some((key, generation)) = inner.lru.pop_front() else {
                break;
            };
            let Some(entry) = inner.blocks.get(&key) else {
                continue;
            };
            if entry.generation != generation {
                continue;
            }
            let entry = inner
                .blocks
                .remove(&key)
                .expect("cache entry exists after lookup");
            inner.current_bytes = inner.current_bytes.saturating_sub(entry.bytes.len());
        }
    }
}

fn env_usize(name: &str) -> Option<usize> {
    let value = std::env::var(name).ok()?;
    value.parse::<usize>().ok()
}

fn cold_store_io_error(path: &str, err: opendal::Error) -> io::Error {
    io::Error::other(format!("cold object '{path}': {err}"))
}

#[cfg(not(madsim))]
fn cold_object_unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

#[cfg(madsim)]
fn cold_object_unix_nanos() -> u128 {
    0
}

pub fn new_cold_chunk_path(
    stream_id: &BucketStreamId,
    start_offset: u64,
    end_offset: u64,
) -> String {
    let unix_nanos = cold_object_unix_nanos();
    let sequence = COLD_CHUNK_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!(
        "{stream_id}/chunks/{start_offset:016x}-{end_offset:016x}-{unix_nanos:032x}-{sequence:016x}.bin"
    )
}

/// The prefix under which all of a stream's cold chunks live. Cold objects are
/// stream-exclusive, so removing this prefix reclaims every chunk for a fully
/// deleted stream in one sweep. Mirrors the layout of [`new_cold_chunk_path`].
pub fn cold_chunk_prefix(stream_id: &BucketStreamId) -> String {
    format!("{stream_id}/chunks/")
}

pub fn new_external_payload_path(stream_id: &BucketStreamId) -> String {
    let unix_nanos = cold_object_unix_nanos();
    let sequence = COLD_CHUNK_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{stream_id}/external/{unix_nanos:032x}-{sequence:016x}.bin")
}

/// Reset the global cold-object sequence counter. Only available under
/// `cfg(madsim)` so the simulator can clear state between scenarios when
/// running multiple seeds in one process (e.g. `Runtime::check_determinism`).
#[cfg(madsim)]
#[allow(dead_code)]
pub fn reset_cold_chunk_sequence_for_sim() {
    COLD_CHUNK_SEQUENCE.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::ColdStore;

    #[test]
    fn fs_backend_is_not_supported() {
        let err = ColdStore::from_backend("fs").expect_err("fs backend should be unsupported");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(err.to_string(), "unsupported URSULA_COLD_BACKEND 'fs'");
    }
}
