use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
#[cfg(not(madsim))]
use std::time::SystemTime;
#[cfg(not(madsim))]
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use futures_util::TryStreamExt;
use opendal::EntryMode;
use opendal::Operator;
use opendal::Scheme;
use opendal::layers::RetryLayer;
use opendal::layers::TimeoutLayer;
use ursula_config::config::ColdBackend;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_stream::ColdChunkRef;
use ursula_stream::ColdGcEntry;
use ursula_stream::ObjectPayloadRef;

use crate::ColdConfig;
use crate::ColdIndexPageKey;

pub(crate) const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

/// Part size of [`ColdObjectWriter`] uploads (above S3's 5 MiB multipart
/// minimum).
pub const COLD_OBJECT_WRITE_PART_BYTES: usize = 8 * 1024 * 1024;

/// Upload parts of one [`ColdObjectWriter`] in flight at once, so a large
/// object does not wait for each part in turn. A writer buffers at most this
/// many parts.
pub const COLD_OBJECT_WRITE_CONCURRENCY: usize = 2;

/// A streaming upload of one cold object. Dropping it without
/// [`Self::close`] leaves no object behind on S3 (the multipart upload is
/// never completed); [`Self::abort`] also releases the parts.
pub struct ColdObjectWriter {
    path: String,
    writer: opendal::Writer,
    written: u64,
}

impl ColdObjectWriter {
    pub fn path(&self) -> &str {
        &self.path
    }

    pub async fn write(&mut self, bytes: Bytes) -> io::Result<()> {
        self.written = self
            .written
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        self.writer
            .write(bytes)
            .await
            .map_err(|err| cold_store_io_error(&self.path, err))
    }

    /// Completes the upload and returns the object size.
    pub async fn close(mut self) -> io::Result<u64> {
        self.writer
            .close()
            .await
            .map_err(|err| cold_store_io_error(&self.path, err))?;
        Ok(self.written)
    }

    pub async fn abort(mut self) {
        if let Err(err) = self.writer.abort().await {
            tracing::warn!(path = %self.path, error = %err, "failed to abort cold object upload");
        }
    }
}
/// The sequence component of every new cold object name (chunks, packs,
/// external payloads, snapshot parts).
#[cfg(not(madsim))]
mod object_sequence {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use crossbeam_utils::CachePadded;

    // Keep this global atomic isolated from unrelated statics. This does not
    // remove contention on the counter itself, but avoids accidental false
    // sharing with adjacent data without adding a per-core sequence scheme to
    // this low-frequency object-key path.
    static SEQUENCE: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));

    pub(super) fn next() -> u64 {
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    }
}

/// The sequence component of every new cold object name, per thread. madsim
/// runs a simulation on one thread, and the harness runs each one on a fresh
/// thread, so every run numbers its objects from zero. A process-wide counter
/// would carry over from one run to the next and rename the objects of a
/// later seed (#638).
#[cfg(madsim)]
mod object_sequence {
    use std::cell::Cell;

    std::thread_local! {
        static SEQUENCE: Cell<u64> = const { Cell::new(0) };
    }

    pub(super) fn next() -> u64 {
        SEQUENCE.with(|sequence| {
            let next = sequence.get();
            sequence.set(next.wrapping_add(1));
            next
        })
    }
}

/// Applies the configured server-side encryption to an S3 builder and returns
/// the label recorded in [`ColdStoreInfo`]. Shared by the cold store and the
/// S3 snapshot store so backup objects inherit the same posture (#149).
pub(crate) fn apply_s3_encryption(
    builder: opendal::services::S3,
    s3: &ursula_config::S3Config,
) -> io::Result<(opendal::services::S3, &'static str)> {
    apply_s3_encryption_with_fetcher(builder, s3, crate::s3_sse::default_fetcher())
}

/// [`apply_s3_encryption`] over an explicit HTTP transport. With SSE on, the
/// transport is wrapped in [`crate::s3_sse::MultipartSseFetch`], which keeps
/// the SSE headers off UploadPart / CompleteMultipartUpload (AWS rejects
/// them there; they belong on CreateMultipartUpload only).
pub(crate) fn apply_s3_encryption_with_fetcher(
    builder: opendal::services::S3,
    s3: &ursula_config::S3Config,
    fetcher: opendal::raw::HttpClient,
) -> io::Result<(opendal::services::S3, &'static str)> {
    use ursula_config::S3ServerSideEncryption;
    let builder = match s3.server_side_encryption {
        S3ServerSideEncryption::None => builder,
        S3ServerSideEncryption::Aes256 | S3ServerSideEncryption::AwsKms => {
            match crate::s3_sse::MultipartSseFetch::new(s3, fetcher) {
                Some(fetch) => builder.http_client(opendal::raw::HttpClient::with(fetch)),
                None => builder,
            }
        }
    };
    match s3.server_side_encryption {
        S3ServerSideEncryption::Aes256 => {
            if s3.kms_key_id.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "s3.kms_key_id requires server_side_encryption = \"aws-kms\"",
                ));
            }
            Ok((builder.server_side_encryption_with_s3_key(), "aes256"))
        }
        S3ServerSideEncryption::AwsKms => match s3.kms_key_id.as_deref() {
            Some(key_id) if !key_id.trim().is_empty() => Ok((
                builder.server_side_encryption_with_customer_managed_kms_key(key_id),
                "aws-kms",
            )),
            _ => Ok((
                builder.server_side_encryption_with_aws_managed_kms_key(),
                "aws-kms",
            )),
        },
        S3ServerSideEncryption::None => {
            if s3.kms_key_id.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "s3.kms_key_id requires server_side_encryption = \"aws-kms\"",
                ));
            }
            Ok((builder, "none"))
        }
    }
}

/// Wrap an S3 (opendal) operator with timeout and bounded-retry layers.
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
pub(crate) fn with_s3_resilience(
    operator: Operator,
    timeout: Duration,
    max_retries: usize,
) -> Operator {
    operator
        .layer(
            TimeoutLayer::new()
                .with_timeout(timeout)
                .with_io_timeout(timeout),
        )
        .layer(
            RetryLayer::new()
                .with_min_delay(S3_RETRY_MIN_DELAY)
                .with_max_delay(S3_RETRY_MAX_DELAY)
                .with_factor(2.0)
                .with_max_times(max_retries)
                .with_jitter(),
        )
}

/// Backoff before the first S3 retry. Each later retry waits twice as long,
/// up to [`S3_RETRY_MAX_DELAY`], and the jitter adds a random share of up to
/// the same delay again.
const S3_RETRY_MIN_DELAY: Duration = Duration::from_secs(1);
const S3_RETRY_MAX_DELAY: Duration = Duration::from_secs(60);

/// The longest one S3 operation can take behind [`with_s3_resilience`]
/// before the caller sees it fail: each of its `max_retries + 1` attempts
/// runs into `timeout`, and each backoff before a retry runs to its jittered
/// maximum, twice its delay.
pub fn s3_operation_budget(timeout: Duration, max_retries: usize) -> Duration {
    let attempts = u32::try_from(max_retries)
        .unwrap_or(u32::MAX)
        .saturating_add(1);
    let mut backoff = Duration::ZERO;
    let mut delay = S3_RETRY_MIN_DELAY;
    let mut retries_left = max_retries;
    while retries_left > 0 && delay < S3_RETRY_MAX_DELAY {
        backoff = backoff.saturating_add(delay.saturating_mul(2));
        delay = delay.saturating_mul(2).min(S3_RETRY_MAX_DELAY);
        retries_left = retries_left.saturating_sub(1);
    }
    // The delay has reached its cap: every retry left waits that long.
    let capped_retries = u32::try_from(retries_left).unwrap_or(u32::MAX);
    backoff = backoff.saturating_add(delay.saturating_mul(2).saturating_mul(capped_retries));
    timeout.saturating_mul(attempts).saturating_add(backoff)
}

#[derive(Clone)]
pub struct ColdStore {
    info: ColdStoreInfo,
    operator: Operator,
    read_cache: Option<Arc<ColdReadCache>>,
    observer: Arc<Mutex<Option<ColdStoreObserver>>>,
    fault_policy: Arc<Mutex<Option<ColdStoreFaultPolicy>>>,
    delay_fn: Arc<Mutex<ColdStoreDelayFn>>,
    /// LIST requests issued (F14d: compaction discovery issues none).
    list_requests: Arc<std::sync::atomic::AtomicU64>,
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
    /// A prefix listing ([`ColdStore::list_file_sizes_with_prefix`]), once
    /// it is read and before it returns.
    ListPrefix,
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

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ColdStoreInfo {
    pub backend: &'static str,
    pub root: Option<String>,
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    /// Server-side encryption mode applied to object writes (S3 backend only).
    pub encryption: Option<&'static str>,
}

impl ColdStore {
    /// Lists the authoritative cold-index pages rather than every chunk. One
    /// page covers 64 MiB of one stream, so this is the bounded discovery
    /// surface used by the background chunk compactor.
    pub async fn list_cold_index_pages(&self) -> io::Result<Vec<ColdIndexPageKey>> {
        self.count_list_request();
        let mut lister = self
            .operator
            .lister_with("")
            .recursive(true)
            .await
            .map_err(|err| cold_store_io_error("", err))?;
        let mut pages = Vec::new();
        while let Some(entry) = lister
            .try_next()
            .await
            .map_err(|err| cold_store_io_error("", err))?
        {
            if entry.metadata().mode() != EntryMode::FILE {
                continue;
            }
            if let Some(key) = parse_cold_index_page_path(entry.path()) {
                pages.push(key);
            }
        }
        pages.sort_by(|left, right| {
            left.stream_id
                .bucket_id
                .cmp(&right.stream_id.bucket_id)
                .then_with(|| left.stream_id.stream_id.cmp(&right.stream_id.stream_id))
                .then_with(|| left.generation.cmp(&right.generation))
                .then_with(|| left.page_id.cmp(&right.page_id))
        });
        pages.dedup();
        Ok(pages)
    }

    pub fn memory() -> io::Result<Self> {
        let operator = Operator::via_iter(Scheme::Memory, [])
            .map_err(|err| io::Error::other(err.to_string()))?;
        Ok(Self::from_operator(operator, ColdStoreInfo {
            backend: "memory",
            root: None,
            bucket: None,
            region: None,
            endpoint: None,
            encryption: None,
        }))
    }

    /// Build a [`ColdStore`] from an explicit [`ColdConfig`].
    ///
    /// The bootstrap layer assembles the typed config before calling this method;
    /// this method is purely functional — it does not touch `std::env`.
    ///
    /// Returns `Err` when the backend is [`ColdBackend::None`];
    /// the caller should skip construction when cold storage is disabled.
    pub fn try_new(config: &ColdConfig) -> io::Result<Self> {
        let mut store = match config.backend {
            ColdBackend::None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "ColdStore::try_new called with backend=none; \
                     the caller should skip construction when cold storage is disabled",
                ));
            }
            ColdBackend::Memory => Self::memory()?,
            ColdBackend::S3 => Self::s3_from_config(config)?,
        };
        let cache = config.cache.clone().unwrap_or_default();
        if cache.max_size.as_bytes() > 0 {
            let cache_params = ColdReadCacheParams {
                max_bytes: cache.max_size.as_bytes() as usize,
                block_bytes: cache.block_size.as_bytes() as usize,
                max_readahead_blocks: cache.readahead_blocks,
            };
            store = store.with_read_cache(cache_params);
        }
        Ok(store)
    }

    fn s3_from_config(config: &ColdConfig) -> io::Result<Self> {
        let s3 = config.s3.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "s3 configuration is required when cold backend is s3",
            )
        })?;
        let bucket = s3.bucket.as_deref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "s3 bucket is required when cold backend is s3",
            )
        })?;
        if bucket.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "s3 bucket must not be empty",
            ));
        }

        let mut builder = opendal::services::S3::default().bucket(bucket);
        let mut configured_root = None;
        if let Some(root) = config.root.as_deref()
            && !root.trim().is_empty()
        {
            builder = builder.root(root);
            configured_root = Some(root.to_owned());
        }
        let mut configured_region = None;
        if let Some(region) = s3.region.as_deref()
            && !region.trim().is_empty()
        {
            builder = builder.region(region);
            configured_region = Some(region.to_owned());
        }
        let mut configured_endpoint = None;
        if let Some(endpoint) = s3.endpoint.as_deref()
            && !endpoint.trim().is_empty()
        {
            builder = builder.endpoint(endpoint);
            configured_endpoint = Some(endpoint.to_owned());
        }
        if let Some(access_key_id) = s3.access_key_id.as_deref()
            && !access_key_id.trim().is_empty()
        {
            builder = builder.access_key_id(access_key_id);
        }
        if let Some(secret_access_key) = s3.secret_access_key.as_deref()
            && !secret_access_key.trim().is_empty()
        {
            builder = builder.secret_access_key(secret_access_key);
        }
        if let Some(session_token) = s3.session_token.as_deref()
            && !session_token.trim().is_empty()
        {
            builder = builder.session_token(session_token);
        }
        let (builder, encryption) = apply_s3_encryption(builder, s3)?;

        Ok(Self::from_operator(
            with_s3_resilience(
                Operator::new(builder)
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .finish(),
                s3.timeout.as_duration(),
                s3.max_retries,
            ),
            ColdStoreInfo {
                backend: "s3",
                root: configured_root,
                bucket: Some(bucket.to_owned()),
                region: configured_region,
                endpoint: configured_endpoint,
                encryption: Some(encryption),
            },
        ))
    }

    fn from_operator(operator: Operator, info: ColdStoreInfo) -> Self {
        Self {
            info,
            operator,
            read_cache: None,
            observer: Arc::new(Mutex::new(None)),
            fault_policy: Arc::new(Mutex::new(None)),
            delay_fn: Arc::new(Mutex::new(default_cold_store_delay_fn())),
            list_requests: Arc::default(),
        }
    }

    /// LIST requests this store has issued.
    pub fn list_request_count(&self) -> u64 {
        self.list_requests
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn count_list_request(&self) {
        self.list_requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn info(&self) -> &ColdStoreInfo {
        &self.info
    }

    pub fn with_read_cache(mut self, config: ColdReadCacheParams) -> Self {
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

    /// Opens a streaming writer for one object, such as a cold snapshot
    /// body (bounded-state F16). The body goes up in parts of
    /// [`COLD_OBJECT_WRITE_PART_BYTES`], so memory stays bounded whatever
    /// the object's size.
    pub async fn open_object_writer(&self, path: &str) -> io::Result<ColdObjectWriter> {
        if path.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cold object path must not be empty",
            ));
        }
        let _applied_fault = self
            .maybe_apply_fault_effect(ColdStoreFaultContext {
                operation: ColdStoreOperation::WriteChunk,
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
        let writer = self
            .operator
            .writer_with(path)
            .chunk(COLD_OBJECT_WRITE_PART_BYTES)
            .concurrent(COLD_OBJECT_WRITE_CONCURRENCY)
            .await
            .map_err(|err| cold_store_io_error(path, err))?;
        Ok(ColdObjectWriter {
            path: path.to_owned(),
            writer,
            written: 0,
        })
    }

    /// Reads `len` bytes at `start` of a whole-object body of `size` bytes
    /// (a cold snapshot body) without the read cache: such bodies are read
    /// once, front to back, and must not evict blocks that serve stream
    /// reads.
    pub async fn read_whole_object_range(
        &self,
        path: &str,
        size: u64,
        start: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        let object = ObjectPayloadRef {
            start_offset: 0,
            end_offset: size,
            s3_path: path.to_owned(),
            object_size: size,
            object_offset: 0,
        };
        self.read_object_range_inner(None, &object, start, len, false)
            .await
    }

    pub(crate) async fn write_cold_index_page(
        &self,
        path: &str,
        payload: &[u8],
    ) -> io::Result<u64> {
        if path.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cold index page path must not be empty",
            ));
        }
        self.operator
            .write(path, payload.to_vec())
            .await
            .map_err(|err| cold_store_io_error(path, err))?;
        Ok(u64::try_from(payload.len()).expect("payload len fits u64"))
    }

    #[tracing::instrument(name = "cold.read_index", level = "debug", skip_all)]
    pub(crate) async fn read_cold_index_page(&self, path: &str) -> io::Result<Option<Vec<u8>>> {
        if path.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cold index page path must not be empty",
            ));
        }
        match self.operator.read(path).await {
            Ok(bytes) => Ok(Some(bytes.to_bytes().to_vec())),
            Err(err) if err.kind() == opendal::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(cold_store_io_error(path, err)),
        }
    }

    pub async fn delete_chunk(&self, path: &str) -> io::Result<()> {
        self.begin_delete(path).await?;
        self.operator
            .delete(path)
            .await
            .map_err(|err| cold_store_io_error(path, err))?;
        self.complete_delete(path);
        Ok(())
    }

    /// The checks and events before an object is deleted, one policy for
    /// [`Self::delete_chunk`] and [`Self::delete_objects`]: the empty-path
    /// guard, `DeleteChunkBegin`, and the `DeleteChunk` fault policy.
    async fn begin_delete(&self, path: &str) -> io::Result<()> {
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
        Ok(())
    }

    /// What follows a deleted object: its cached blocks go, and
    /// `DeleteChunkComplete`.
    fn complete_delete(&self, path: &str) {
        if let Some(cache) = &self.read_cache {
            cache.invalidate_path(path);
        }
        self.notify(ColdStoreEvent::DeleteChunkComplete {
            path: path.to_owned(),
        });
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

    /// Lists the file names directly below the directory `dir` (which ends
    /// in `/`), without recursing into subdirectories. Stream GC uses it so a
    /// sweep never reaches another stream's namespace (F14g).
    pub async fn list_file_names(&self, dir: &str) -> io::Result<Vec<String>> {
        self.list_file_names_up_to(dir, usize::MAX).await
    }

    /// The names of at most `max` files directly in `dir`, sorted. The
    /// listing stops at `max`, page by page, so a directory a tenant can
    /// grow costs a bounded listing.
    pub async fn list_file_names_up_to(&self, dir: &str, max: usize) -> io::Result<Vec<String>> {
        self.list_file_names_after(dir, None, max).await
    }

    /// The first `max` names of files directly in `dir` that sort after
    /// `start_after`, sorted: one page of a walk that resumes where the last
    /// one stopped. A backend that lists from a key (S3) starts there; any
    /// other is listed whole and filtered.
    pub async fn list_file_names_after(
        &self,
        dir: &str,
        start_after: Option<&str>,
        max: usize,
    ) -> io::Result<Vec<String>> {
        self.count_list_request();
        let ordered = self.operator.info().full_capability().list_with_start_after;
        let mut lister = self.operator.lister_with(dir);
        if ordered && let Some(after) = start_after {
            lister = lister.start_after(&format!("{dir}{after}"));
        }
        let mut lister = lister.await.map_err(|err| cold_store_io_error(dir, err))?;
        let mut names = Vec::new();
        while (!ordered || names.len() < max)
            && let Some(entry) = lister
                .try_next()
                .await
                .map_err(|err| cold_store_io_error(dir, err))?
        {
            if entry.metadata().mode() != EntryMode::FILE {
                continue;
            }
            let Some(name) = entry.path().strip_prefix(dir) else {
                continue;
            };
            if !name.is_empty()
                && !name.contains('/')
                && start_after.is_none_or(|after| name > after)
            {
                names.push(name.to_owned());
            }
        }
        names.sort();
        names.truncate(max);
        Ok(names)
    }

    /// The names and sizes of the files directly in `dir` whose names start
    /// with `prefix`, at most `max` of them, in no particular order
    /// ([`Self::fold_file_sizes_with_prefix`]).
    pub async fn list_file_sizes_with_prefix(
        &self,
        dir: &str,
        prefix: &str,
        max: usize,
    ) -> io::Result<Vec<(String, u64)>> {
        let mut files = Vec::new();
        self.fold_file_sizes_with_prefix(dir, prefix, |name, size| {
            files.push((name.to_owned(), size));
            files.len() < max
        })
        .await?;
        Ok(files)
    }

    /// Hands `visit` the name and size of each file directly in `dir` whose
    /// name starts with `prefix`, in no particular order, until it returns
    /// `false`. It is a prefix listing (S3's `prefix=`), so it costs the
    /// matching names, not the directory, on any store that honors a
    /// prefix, and it holds no list of names. A backend whose listing
    /// carries no size is asked for each. The fault policy sees the listing
    /// as [`ColdStoreOperation::ListPrefix`] once it is read, before it
    /// returns.
    pub async fn fold_file_sizes_with_prefix(
        &self,
        dir: &str,
        prefix: &str,
        mut visit: impl FnMut(&str, u64) -> bool,
    ) -> io::Result<()> {
        self.count_list_request();
        let path = format!("{dir}{prefix}");
        let has_length = self
            .operator
            .info()
            .full_capability()
            .list_has_content_length;
        let mut lister = self
            .operator
            .lister_with(&path)
            .await
            .map_err(|err| cold_store_io_error(&path, err))?;
        while let Some(entry) = lister
            .try_next()
            .await
            .map_err(|err| cold_store_io_error(&path, err))?
        {
            if entry.metadata().mode() != EntryMode::FILE {
                continue;
            }
            let Some(name) = entry.path().strip_prefix(dir) else {
                continue;
            };
            if name.contains('/') || !name.starts_with(prefix) {
                continue;
            }
            let size = if has_length {
                entry.metadata().content_length()
            } else {
                match self.object_size(entry.path()).await {
                    Ok(size) => size,
                    // Deleted since it was listed.
                    Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                    Err(err) => return Err(err),
                }
            };
            if !visit(name, size) {
                break;
            }
        }
        let _applied_fault = self
            .maybe_apply_fault_effect(ColdStoreFaultContext {
                operation: ColdStoreOperation::ListPrefix,
                stream_id: None,
                path,
                payload_len: None,
                read_start_offset: None,
                len: None,
                object_start: None,
                object_end: None,
                cached: None,
            })
            .await?;
        Ok(())
    }

    /// Deletes `paths` with the checks and events of
    /// [`Self::delete_chunk`] for each, in one batched call where the
    /// backend has one (S3's `DeleteObjects`). A path the checks refuse is
    /// kept; the others are deleted all the same, and the first refusal is
    /// returned. A missing object counts as deleted.
    pub async fn delete_objects(&self, paths: Vec<String>) -> io::Result<()> {
        let mut refused = None;
        let mut admitted = Vec::with_capacity(paths.len());
        for path in paths {
            match self.begin_delete(&path).await {
                Ok(()) => admitted.push(path),
                Err(err) => {
                    refused.get_or_insert(err);
                }
            }
        }
        if !admitted.is_empty() {
            self.operator
                .delete_iter(admitted.clone())
                .await
                .map_err(|err| cold_store_io_error("batch delete", err))?;
            for path in &admitted {
                self.complete_delete(path);
            }
        }
        refused.map_or(Ok(()), Err)
    }

    /// Proves that the object store has no file below `path`. Tenant purge
    /// uses this after recursive deletion; a successful delete request alone
    /// is not physical-absence evidence.
    pub async fn prefix_is_empty(&self, path: &str) -> io::Result<bool> {
        self.count_list_request();
        let mut lister = self
            .operator
            .lister_with(path)
            .recursive(true)
            .await
            .map_err(|err| cold_store_io_error(path, err))?;
        while let Some(entry) = lister
            .try_next()
            .await
            .map_err(|err| cold_store_io_error(path, err))?
        {
            if entry.metadata().mode() == EntryMode::FILE {
                return Ok(false);
            }
        }
        Ok(true)
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
            object_offset: chunk.object_offset,
        };
        self.read_object_range(&object, read_start_offset, len)
            .await
    }

    /// [`Self::read_chunk_range`] that bypasses the read cache: background
    /// rewrites such as F2 compaction read each slice once, and must not
    /// evict the blocks that serve client reads.
    pub async fn read_chunk_range_uncached(
        &self,
        chunk: &ColdChunkRef,
        read_start_offset: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        let object = ObjectPayloadRef::from(chunk);
        self.read_object_range_inner(None, &object, read_start_offset, len, false)
            .await
    }

    /// Size of the object at `path`.
    pub async fn object_size(&self, path: &str) -> io::Result<u64> {
        self.operator
            .stat(path)
            .await
            .map(|metadata| metadata.content_length())
            .map_err(|err| cold_store_io_error(path, err))
    }

    pub async fn read_object_range_for_stream(
        &self,
        stream_id: &BucketStreamId,
        object: &ObjectPayloadRef,
        read_start_offset: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        self.read_object_range_inner(Some(stream_id), object, read_start_offset, len, true)
            .await
    }

    pub async fn read_object_range(
        &self,
        object: &ObjectPayloadRef,
        read_start_offset: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        self.read_object_range_inner(None, object, read_start_offset, len, true)
            .await
    }

    #[tracing::instrument(
        name = "cold.read_chunk",
        level = "debug",
        skip_all,
        fields(
            start_offset = object.start_offset,
            end_offset = object.end_offset,
            object_size = object.object_size,
            len = len,
        ),
    )]
    async fn read_object_range_inner(
        &self,
        stream_id: Option<&BucketStreamId>,
        object: &ObjectPayloadRef,
        read_start_offset: u64,
        len: usize,
        use_cache: bool,
    ) -> io::Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let len_u64 = u64::try_from(len).map_err(|_overflow| {
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
        let object_start = read_start_offset
            .checked_sub(object.start_offset)
            .and_then(|within_segment| object.object_offset.checked_add(within_segment))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "cold read range overflow")
            })?;
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
        let read_cache = self.read_cache.as_ref().filter(|_| use_cache);
        let cached = read_cache.is_some();
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
        let mut bytes = if let Some(cache) = read_cache {
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
        let range_error = || {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "cold cache block range is out of bounds",
            )
        };
        let mut payload = Vec::with_capacity(len);
        let block_size = cache.block_size();
        let last_offset = object_end.checked_sub(1).ok_or_else(range_error)?;
        let first_block = object_start
            .checked_div(block_size)
            .ok_or_else(range_error)?;
        let last_block = last_offset
            .checked_div(block_size)
            .ok_or_else(range_error)?;
        for block_index in first_block..=last_block {
            let block_start = block_index
                .checked_mul(block_size)
                .ok_or_else(range_error)?;
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
            // `max(object_start, block_start) - block_start` never underflows.
            let slice_start = usize::try_from(object_start.saturating_sub(block_start))
                .expect("cache slice start fits usize");
            let slice_end = object_end
                .min(block_end)
                .checked_sub(block_start)
                .ok_or_else(range_error)?;
            let slice_end = usize::try_from(slice_end).expect("cache slice end fits usize");
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
        let block_len = block_end.checked_sub(block_start).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "cold cache block range is out of bounds",
            )
        })?;
        let bytes = self
            .operator
            .read_with(&path)
            .range(block_start..block_end)
            .await
            .map_err(|err| cold_store_io_error(&path, err))?
            .to_bytes();
        let expected_len = usize::try_from(block_len).map_err(|_overflow| {
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
                // An overflowing block start is past the end of the object.
                let Some(block_start) = block_index.checked_mul(block_size) else {
                    break;
                };
                if block_start >= object.object_size {
                    break;
                }
                let block_end = block_start
                    .saturating_add(block_size)
                    .min(object.object_size);
                if cache.get(&object.s3_path, block_index).is_none()
                    && let Err(err) = store
                        .read_cached_block(
                            &cache,
                            object.s3_path.clone(),
                            object.object_size,
                            block_index,
                            block_start,
                            block_end,
                        )
                        .await
                {
                    tracing::debug!(%err, block_index, "cold block prefetch failed");
                }
                block_index = block_index.saturating_add(1);
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

fn parse_cold_index_page_path(path: &str) -> Option<ColdIndexPageKey> {
    let parts = path.split('/').collect::<Vec<_>>();
    let [bucket, stream, "cold-index", generation, page] = parts.as_slice() else {
        return None;
    };
    if bucket.is_empty() || stream.is_empty() {
        return None;
    }
    Some(ColdIndexPageKey {
        stream_id: BucketStreamId::new(*bucket, *stream),
        generation: generation.parse().ok()?,
        page_id: page.strip_suffix(".idx")?.parse().ok()?,
    })
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
            Self::ListPrefix => "list_prefix",
        }
    }
}

fn default_cold_store_delay_fn() -> ColdStoreDelayFn {
    Arc::new(|duration| Box::pin(crate::rt::time::sleep(duration)))
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Runtime parameters for the optional cold-read cache.
///
/// Kept separate from the serde [`ursula_config::ColdCacheConfig`] because
/// `ColdReadCache` accesses these fields on the hot path.
#[derive(Debug, Clone, Copy)]
pub struct ColdReadCacheParams {
    pub max_bytes: usize,
    pub block_bytes: usize,
    pub max_readahead_blocks: usize,
}

#[derive(Debug)]
struct ColdReadCache {
    config: ColdReadCacheParams,
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
    /// Cache generation of the last read, for pruning idle readers (F13).
    last_read: u64,
}

/// Minimum number of readahead reader entries kept before idle ones are
/// pruned (F13).
const MIN_TRACKED_READERS: usize = 4_096;

impl ColdReadCache {
    fn new(config: ColdReadCacheParams) -> Self {
        let block_bytes = config.block_bytes.max(1);
        Self {
            config: ColdReadCacheParams {
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
        Self::compact_lru_if_needed(&mut inner);
    }

    fn record_stream_read(
        &self,
        stream_id: &BucketStreamId,
        read_start_offset: u64,
        len: usize,
    ) -> usize {
        let mut inner = self.inner.lock().expect("cold cache mutex poisoned");
        let generation = Self::next_generation(&mut inner);
        if !inner.readers.contains_key(stream_id) {
            self.prune_readers_if_needed(&mut inner);
        }
        let state = inner.readers.entry(stream_id.clone()).or_default();
        state.last_read = generation;
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

    /// Readahead state is kept per stream ever read; bound it at
    /// max(4 x cache blocks, 4,096) entries by dropping the least recently
    /// read half once the map is full. Amortized O(1) per new reader. A
    /// pruned stream only loses its readahead score.
    fn prune_readers_if_needed(&self, inner: &mut ColdReadCacheInner) {
        let capacity_blocks = self
            .config
            .max_bytes
            .checked_div(self.config.block_bytes.max(1))
            .unwrap_or_default();
        let limit = capacity_blocks.saturating_mul(4).max(MIN_TRACKED_READERS);
        if inner.readers.len() < limit {
            return;
        }
        let keep = limit / 2;
        let mut generations = inner
            .readers
            .values()
            .map(|state| state.last_read)
            .collect::<Vec<_>>();
        let drop_count = generations.len().saturating_sub(keep);
        let Some(threshold_index) = drop_count.checked_sub(1) else {
            return;
        };
        let (_, threshold, _) = generations.select_nth_unstable(threshold_index);
        let threshold = *threshold;
        inner.readers.retain(|_, state| state.last_read > threshold);
        inner.readers.shrink_to(limit);
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
        // Drop readahead state of every stream at or below the prefix: a
        // stream prefix (`bucket/stream/...`) or a whole bucket (`bucket/`).
        inner.readers.retain(|stream_id, _| {
            let stream_prefix = format!("{stream_id}/");
            !(prefix.starts_with(&stream_prefix) || stream_prefix.starts_with(prefix))
        });
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
        Self::compact_lru_if_needed(inner);
    }

    fn compact_lru_if_needed(inner: &mut ColdReadCacheInner) {
        // `touch` appends a fresh (key, generation) on every hit without removing
        // the stale prior entry, and `evict_locked` only reclaims those when the
        // cache is over `max_bytes`. With a working set at or below the cap but
        // repeated hits, the deque would otherwise grow without bound. Rebuild it
        // from the live blocks once it bloats past 2x the live entry count —
        // amortized O(1) per touch, since each rebuild shrinks it back to
        // `blocks.len()` so the next rebuild is `blocks.len()` touches away.
        if inner.lru.len() <= inner.blocks.len().saturating_mul(2).saturating_add(16) {
            return;
        }
        let mut live: Vec<(u64, ColdCacheKey)> = inner
            .blocks
            .iter()
            .map(|(key, entry)| (entry.generation, key.clone()))
            .collect();
        live.sort_unstable_by_key(|(generation, _)| *generation);
        inner.lru = live
            .into_iter()
            .map(|(generation, key)| (key, generation))
            .collect();
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

fn cold_store_io_error(path: &str, err: opendal::Error) -> io::Error {
    // Keep NotFound distinguishable: a read that names a compacted object
    // refreshes its cold-index page and retries (RT2).
    let kind = if err.kind() == opendal::ErrorKind::NotFound {
        io::ErrorKind::NotFound
    } else {
        io::ErrorKind::Other
    };
    io::Error::new(kind, format!("cold object '{path}': {err}"))
}

/// The S3 operator a [`ColdStore`] built from `config` would use, rooted at
/// `config.root`. The format-epoch marker check reads and writes through it
/// before the runtime builds its own stores.
pub(crate) fn s3_operator_for_config(config: &ColdConfig) -> io::Result<Operator> {
    Ok(ColdStore::s3_from_config(config)?.operator)
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

/// Names a new exclusive chunk of the stream incarnation whose cold
/// generation is `generation` (F14g). The `{generation:016x}/` component lets
/// stream GC delete one incarnation's chunks without touching another's.
pub fn new_cold_chunk_path_in_generation(
    stream_id: &BucketStreamId,
    generation: u64,
    start_offset: u64,
    end_offset: u64,
) -> String {
    let unix_nanos = cold_object_unix_nanos();
    let sequence = object_sequence::next();
    format!(
        "{}{start_offset:016x}-{end_offset:016x}-{unix_nanos:032x}-{sequence:016x}.bin",
        cold_chunk_dir(stream_id, generation)
    )
}

/// The directory holding the exclusive chunks of one stream incarnation
/// (F14g): `{stream}/chunks/{generation:016x}/`.
pub fn cold_chunk_dir(stream_id: &BucketStreamId, generation: u64) -> String {
    format!("{}{generation:016x}/", cold_chunk_prefix(stream_id))
}

/// The directory holding a stream's staged external payloads.
pub fn cold_external_dir(stream_id: &BucketStreamId) -> String {
    format!("{stream_id}/external/")
}

/// Whether `name` (a file name without directory) has the form Ursula uses
/// for exclusive chunks, `{start:016x}-{end:016x}-{nanos:032x}-{seq:016x}.bin`.
pub fn is_cold_chunk_file_name(name: &str) -> bool {
    hex_fields_with_suffix(name, ".bin", &[16, 16, 32, 16])
}

/// The `[start, end)` byte range an exclusive chunk's file name encodes, or
/// `None` for any other name.
pub fn cold_chunk_file_range(name: &str) -> Option<(u64, u64)> {
    if !is_cold_chunk_file_name(name) {
        return None;
    }
    let mut fields = name.split('-');
    let start = u64::from_str_radix(fields.next()?, 16).ok()?;
    let end = u64::from_str_radix(fields.next()?, 16).ok()?;
    Some((start, end))
}

/// Whether `name` has the form Ursula uses for staged external payloads,
/// `{nanos:032x}-{seq:016x}.bin`.
pub fn is_external_payload_file_name(name: &str) -> bool {
    hex_fields_with_suffix(name, ".bin", &[32, 16])
}

fn hex_fields_with_suffix(name: &str, suffix: &str, widths: &[usize]) -> bool {
    let Some(stem) = name.strip_suffix(suffix) else {
        return false;
    };
    let fields = stem.split('-').collect::<Vec<_>>();
    fields.len() == widths.len()
        && fields.iter().zip(widths).all(|(field, width)| {
            field.len() == *width && field.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

pub fn new_cold_pack_path(bucket_id: &str, raft_group_id: u32) -> String {
    let unix_nanos = cold_object_unix_nanos();
    let sequence = object_sequence::next();
    format!(
        "{}{unix_nanos:032x}-{sequence:016x}.bin",
        cold_pack_dir(bucket_id, raft_group_id)
    )
}

/// The directory holding one Raft group's packs for one bucket.
pub fn cold_pack_dir(bucket_id: &str, raft_group_id: u32) -> String {
    format!("{bucket_id}/_packs/{raft_group_id:08x}/")
}

/// The physical erasure domain for one tenant bucket. Every current chunk,
/// index, pack, and external payload path is nested below this prefix.
pub fn cold_bucket_prefix(bucket_id: &str) -> String {
    format!("{bucket_id}/")
}

/// The directory under which every incarnation of a stream keeps its
/// exclusive chunks, one [`cold_chunk_dir`] per cold generation.
pub fn cold_chunk_prefix(stream_id: &BucketStreamId) -> String {
    format!("{stream_id}/chunks/")
}

pub fn new_external_payload_path(stream_id: &BucketStreamId) -> String {
    let unix_nanos = cold_object_unix_nanos();
    let sequence = object_sequence::next();
    format!("{stream_id}/external/{unix_nanos:032x}-{sequence:016x}.bin")
}

/// A new snapshot part id (#581 section 3.1), `{nanos:032x}-{seq:016x}.part`.
/// The `.part` suffix keeps parts apart from the `.bin` objects that state
/// references, so a commit can name only parts, and deleting a part after
/// its commit can never remove data.
pub fn new_snapshot_part_id() -> String {
    let unix_nanos = cold_object_unix_nanos();
    let sequence = object_sequence::next();
    format!("{unix_nanos:032x}-{sequence:016x}{SNAPSHOT_PART_SUFFIX}")
}

const SNAPSHOT_PART_SUFFIX: &str = ".part";

/// Whether `id` has the form of a part id a client got from
/// [`new_snapshot_part_id`].
pub fn is_snapshot_part_id(id: &str) -> bool {
    hex_fields_with_suffix(id, SNAPSHOT_PART_SUFFIX, &[32, 16])
}

/// The directory of the snapshot parts staged for the streams of one Raft
/// group in one bucket: `{bucket}/_parts/{group:08x}/`. It does not depend on
/// any stream being live, so the orphan sweep lists it from the group's
/// buckets, as it lists the pack directories, and reclaims the parts of a
/// stream that was deleted or expired as well as the unused parts of a live
/// one. A bucket purge removes it with the bucket's prefix.
pub fn snapshot_part_dir(bucket_id: &str, raft_group_id: u32) -> String {
    format!("{bucket_id}/_parts/{raft_group_id:08x}/")
}

/// The object of part `id` staged for the incarnation `incarnation` of
/// `stream_id`, in [`snapshot_part_dir`]:
/// `{stream:032x}-{incarnation:016x}-{id}`, where `stream` is a hash of the
/// stream ([`snapshot_part_stream_prefix`]). A commit resolves an id only
/// with its own stream and token, so a manifest cannot name another
/// stream's part, nor one staged for an earlier incarnation or history of
/// its own stream.
pub fn snapshot_part_path(
    stream_id: &BucketStreamId,
    raft_group_id: u32,
    incarnation: u64,
    id: &str,
) -> String {
    format!(
        "{}{}{id}",
        snapshot_part_dir(&stream_id.bucket_id, raft_group_id),
        snapshot_part_owner_prefix(stream_id, incarnation),
    )
}

/// The file-name prefix, `{stream:032x}-`, of every part staged for any
/// incarnation of `stream_id` in its [`snapshot_part_dir`], so one prefix
/// listing ([`ColdStore::list_file_sizes_with_prefix`]) finds the parts of
/// the stream's live and retired incarnations. `stream` is 128 bits of a
/// hash of the stream id, so two streams of a directory never share it.
pub fn snapshot_part_stream_prefix(stream_id: &BucketStreamId) -> String {
    let mut owner = blake3::Hasher::new();
    owner.update(b"ursula snapshot part owner\0");
    owner.update(stream_id.to_string().as_bytes());
    let digest = owner.finalize();
    let owner = digest
        .as_bytes()
        .first_chunk::<16>()
        .map_or(0, |prefix| u128::from_be_bytes(*prefix));
    format!("{owner:032x}-")
}

/// The file-name prefix, `{stream:032x}-{incarnation:016x}-`, of every part
/// staged for the incarnation `incarnation` of `stream_id`.
pub fn snapshot_part_owner_prefix(stream_id: &BucketStreamId, incarnation: u64) -> String {
    format!(
        "{}{incarnation:016x}-",
        snapshot_part_stream_prefix(stream_id)
    )
}

/// Whether `name` (a file name in a [`snapshot_part_dir`]) has the form of
/// a staged part object,
/// `{stream:032x}-{incarnation:016x}-{nanos:032x}-{seq:016x}.part`. No
/// replicated state ever references one.
pub fn is_snapshot_part_object_name(name: &str) -> bool {
    hex_fields_with_suffix(name, SNAPSHOT_PART_SUFFIX, &[32, 16, 32, 16])
}

/// The incarnation a staged part object named `name` was staged for, when
/// `name` has the form [`is_snapshot_part_object_name`] checks.
pub fn snapshot_part_object_incarnation(name: &str) -> Option<u64> {
    if !is_snapshot_part_object_name(name) {
        return None;
    }
    let incarnation = name.split('-').nth(1)?;
    u64::from_str_radix(incarnation, 16).ok()
}

/// An object a `Paths` cold GC entry may delete (G1): exactly the objects
/// such an entry exists for, each named as its key function names it. A
/// snapshot object, a cold-index page, a staged part or anything else is
/// none of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ColdGcPathKind {
    /// An exclusive chunk, named by [`new_cold_chunk_path_in_generation`].
    ExclusiveChunk {
        stream_id: BucketStreamId,
        generation: u64,
    },
    /// A pack of one Raft group, named by [`new_cold_pack_path`].
    Pack { raft_group_id: RaftGroupId },
    /// An external payload, named by [`new_external_payload_path`].
    External { stream_id: BucketStreamId },
}

impl ColdGcPathKind {
    /// What `path` is, when it is an exclusive chunk, a pack or an external
    /// payload of bucket `bucket_id`. `None` for any other path, a path of
    /// another bucket included. The directory must be the one the key
    /// function builds and the file name must have the form it writes.
    pub(crate) fn parse(bucket_id: &str, path: &str) -> Option<Self> {
        ursula_stream::validate_bucket_id(bucket_id).ok()?;
        let rest = path.strip_prefix(&cold_bucket_prefix(bucket_id))?;
        let segments = rest.split('/').collect::<Vec<_>>();
        let (name, dirs) = segments.split_last()?;
        let kind = Self::pack(dirs)
            .or_else(|| Self::exclusive_chunk(bucket_id, dirs))
            .or_else(|| Self::external(bucket_id, dirs))?;
        let named = format!("{}{name}", kind.dir(bucket_id));
        (named == path && kind.accepts_file_name(name)).then_some(kind)
    }

    fn pack(dirs: &[&str]) -> Option<Self> {
        let ["_packs", group] = dirs else {
            return None;
        };
        let raft_group_id = RaftGroupId(u32::from_str_radix(group, 16).ok()?);
        Some(Self::Pack { raft_group_id })
    }

    fn exclusive_chunk(bucket_id: &str, dirs: &[&str]) -> Option<Self> {
        let [stream, "chunks", generation] = dirs else {
            return None;
        };
        Some(Self::ExclusiveChunk {
            stream_id: Self::stream_id(bucket_id, stream)?,
            generation: u64::from_str_radix(generation, 16).ok()?,
        })
    }

    fn external(bucket_id: &str, dirs: &[&str]) -> Option<Self> {
        let [stream, "external"] = dirs else {
            return None;
        };
        Some(Self::External {
            stream_id: Self::stream_id(bucket_id, stream)?,
        })
    }

    fn stream_id(bucket_id: &str, stream: &str) -> Option<BucketStreamId> {
        let stream_id = BucketStreamId::new(bucket_id, stream);
        ursula_stream::validate_stream_id(&stream_id).ok()?;
        Some(stream_id)
    }

    fn dir(&self, bucket_id: &str) -> String {
        match self {
            Self::ExclusiveChunk {
                stream_id,
                generation,
            } => cold_chunk_dir(stream_id, *generation),
            Self::Pack { raft_group_id } => cold_pack_dir(bucket_id, raft_group_id.0),
            Self::External { stream_id } => cold_external_dir(stream_id),
        }
    }

    fn accepts_file_name(&self, name: &str) -> bool {
        match self {
            Self::ExclusiveChunk { .. } => is_cold_chunk_file_name(name),
            // A pack is named as an external payload is (`new_cold_pack_path`).
            Self::Pack { .. } | Self::External { .. } => is_external_payload_file_name(name),
        }
    }

    /// The Raft group that owns the object: a pack's own group, or the
    /// group `locate` places the stream of a chunk or an external payload in.
    fn owner(&self, locate: impl Fn(&BucketStreamId) -> RaftGroupId) -> RaftGroupId {
        match self {
            Self::Pack { raft_group_id } => *raft_group_id,
            Self::ExclusiveChunk { stream_id, .. } | Self::External { stream_id } => {
                locate(stream_id)
            }
        }
    }
}

/// Why the cold GC worker did not delete a path a `Paths` entry names (G1).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ColdGcRefusal {
    /// The path is not an exclusive chunk, a pack or an external payload of
    /// the entry's bucket that the executing group owns.
    #[error(
        "cold GC entry {seq} of raft group {} names '{path}', which is not an exclusive chunk, \
         pack or external payload of bucket '{bucket_id}' owned by the group, so it is not deleted",
        .raft_group_id.0
    )]
    ForeignPath {
        raft_group_id: RaftGroupId,
        seq: u64,
        bucket_id: String,
        path: String,
    },
}

/// The one check at the cold GC `Paths` delete site (G1): `path` of `entry`
/// may be deleted by group `raft_group_id` only if it is an exclusive chunk,
/// a pack or an external payload of the entry's own bucket, and the pack's
/// group, or the group `locate` places the stream in, is `raft_group_id`.
pub(crate) fn check_cold_gc_path(
    raft_group_id: RaftGroupId,
    entry: &ColdGcEntry,
    path: &str,
    locate: impl Fn(&BucketStreamId) -> RaftGroupId,
) -> Result<ColdGcPathKind, ColdGcRefusal> {
    ColdGcPathKind::parse(&entry.bucket_id, path)
        .filter(|kind| kind.owner(locate) == raft_group_id)
        .ok_or_else(|| ColdGcRefusal::ForeignPath {
            raft_group_id,
            seq: entry.seq,
            bucket_id: entry.bucket_id.clone(),
            path: path.to_owned(),
        })
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use ursula_config::config::ColdBackend;
    use ursula_shard::BucketStreamId;
    use ursula_shard::RaftGroupId;
    use ursula_stream::ColdGcEntry;
    use ursula_stream::ColdGcTarget;

    use super::ColdGcPathKind;
    use super::ColdGcRefusal;
    use super::ColdReadCache;
    use super::ColdStore;
    use super::check_cold_gc_path;
    use super::new_cold_chunk_path_in_generation;
    use super::new_cold_pack_path;
    use super::new_external_payload_path;
    use super::parse_cold_index_page_path;
    use super::s3_operation_budget;
    use super::snapshot_part_path;
    use crate::ColdConfig;
    use crate::ColdIndexPageKey;
    use crate::ColdReadCacheParams;

    fn read_cache_params(store: &ColdStore) -> ColdReadCacheParams {
        store
            .read_cache
            .as_ref()
            .map(|cache| cache.config)
            .expect("read cache")
    }

    #[test]
    fn s3_operation_budget_covers_every_attempt_and_jittered_backoff() {
        use std::time::Duration;

        let secs = Duration::from_secs;
        assert_eq!(s3_operation_budget(secs(10), 0), secs(10));
        // The defaults: four attempts of 10 s, backoffs of at most 2, 4 and 8 s.
        assert_eq!(s3_operation_budget(secs(10), 3), secs(54));
        // Backoffs double from 1 s to the 60 s cap: 2+4+8+16+32+64 s, then
        // 120 s for each retry past the cap.
        assert_eq!(s3_operation_budget(secs(1), 8), secs(9 + 126 + 240));
        assert_eq!(
            s3_operation_budget(Duration::MAX, 3),
            Duration::MAX,
            "saturates instead of overflowing"
        );
    }

    const GC_BUCKET: &str = "tenant-a";
    const GC_GROUP: RaftGroupId = RaftGroupId(1);

    fn gc_entry(bucket_id: &str) -> ColdGcEntry {
        ColdGcEntry {
            seq: 9,
            bucket_id: bucket_id.to_owned(),
            not_before_ms: 0,
            target: ColdGcTarget::Paths(Vec::new()),
            cold_generation: None,
            defer_attempts: 0,
        }
    }

    /// Streams named `elsewhere` are placed in group 2, every other in
    /// [`GC_GROUP`].
    fn gc_locate(stream: &BucketStreamId) -> RaftGroupId {
        if stream.stream_id == "elsewhere" {
            RaftGroupId(2)
        } else {
            GC_GROUP
        }
    }

    fn gc_check(bucket_id: &str, path: &str) -> Result<ColdGcPathKind, ColdGcRefusal> {
        check_cold_gc_path(GC_GROUP, &gc_entry(bucket_id), path, gc_locate)
    }

    /// G1: every object a `Paths` entry exists for passes, as its key
    /// function names it, a stream named `_packs` included.
    #[test]
    fn cold_gc_guard_accepts_every_legal_shape() {
        for stream in ["s", "_packs", "chunks", "external"] {
            let stream_id = BucketStreamId::new(GC_BUCKET, stream);
            assert_eq!(
                gc_check(
                    GC_BUCKET,
                    &new_cold_chunk_path_in_generation(&stream_id, 7, 0, 4)
                ),
                Ok(ColdGcPathKind::ExclusiveChunk {
                    stream_id: stream_id.clone(),
                    generation: 7,
                })
            );
            assert_eq!(
                gc_check(GC_BUCKET, &new_external_payload_path(&stream_id)),
                Ok(ColdGcPathKind::External { stream_id })
            );
        }
        assert_eq!(
            gc_check(
                GC_BUCKET,
                &new_cold_chunk_path_in_generation(
                    &BucketStreamId::new(GC_BUCKET, "s"),
                    u64::MAX,
                    0,
                    4
                )
            ),
            Ok(ColdGcPathKind::ExclusiveChunk {
                stream_id: BucketStreamId::new(GC_BUCKET, "s"),
                generation: u64::MAX,
            })
        );
        assert_eq!(
            gc_check(GC_BUCKET, &new_cold_pack_path(GC_BUCKET, GC_GROUP.0)),
            Ok(ColdGcPathKind::Pack {
                raft_group_id: GC_GROUP
            })
        );
    }

    /// G1: a snapshot key, a page, a staged part, another bucket's object,
    /// another group's pack or stream, and malformed paths are refused with
    /// the entry's identity.
    #[test]
    fn cold_gc_guard_refuses_foreign_and_malformed_paths() {
        let stream_id = BucketStreamId::new(GC_BUCKET, "s");
        let chunk = new_cold_chunk_path_in_generation(&stream_id, 7, 0, 4);
        let chunk_name = chunk.rsplit_once('/').expect("chunk has a directory").1;
        let external = new_external_payload_path(&stream_id);
        let external_name = external
            .rsplit_once('/')
            .expect("external has a directory")
            .1;
        let refused = [
            // A snapshot object of this node, under the default prefix and
            // under a prefix inside the bucket.
            "snapshots/node-1/group-1/00000000000000000042-0123456789abcdef.snap".to_owned(),
            format!("{GC_BUCKET}/node-1/group-1/00000000000000000042-0123456789abcdef.snap"),
            format!("{GC_BUCKET}/s/node-1/group-1/{external_name}"),
            // A cold-index page and a staged snapshot part.
            ColdIndexPageKey {
                stream_id: stream_id.clone(),
                generation: 7,
                page_id: 0,
            }
            .path(),
            snapshot_part_path(&stream_id, GC_GROUP.0, 1, &super::new_snapshot_part_id()),
            // Another bucket's objects.
            new_external_payload_path(&BucketStreamId::new("tenant-b", "s")),
            new_cold_pack_path("tenant-b", GC_GROUP.0),
            new_cold_chunk_path_in_generation(&BucketStreamId::new("tenant-ab", "s"), 7, 0, 4),
            // Another group's pack, and the objects of a stream placed in
            // another group.
            new_cold_pack_path(GC_BUCKET, 2),
            new_cold_chunk_path_in_generation(
                &BucketStreamId::new(GC_BUCKET, "elsewhere"),
                7,
                0,
                4,
            ),
            new_external_payload_path(&BucketStreamId::new(GC_BUCKET, "elsewhere")),
            // Malformed: a generation or group not as the key function
            // writes it, a file name of another form, extra or missing
            // directories, escapes and an invalid stream name.
            format!("{GC_BUCKET}/s/chunks/7/{chunk_name}"),
            format!("{GC_BUCKET}/s/chunks/{:016X}/{chunk_name}", u64::MAX),
            format!("{GC_BUCKET}/s/chunks/+000000000000007/{chunk_name}"),
            format!("{GC_BUCKET}/_packs/1/{external_name}"),
            format!("{GC_BUCKET}/s/chunks/0000000000000007/{external_name}"),
            format!("{GC_BUCKET}/s/external/{chunk_name}"),
            format!("{GC_BUCKET}/s/external/object.bin"),
            format!("{GC_BUCKET}/s/external/nested/{external_name}"),
            format!("{GC_BUCKET}/s/{external_name}"),
            format!("{GC_BUCKET}/{external_name}"),
            format!("{GC_BUCKET}/s/external/../{external_name}"),
            format!("{GC_BUCKET}/../tenant-b/s/external/{external_name}"),
            format!("{GC_BUCKET}//external/{external_name}"),
            format!("{GC_BUCKET}/$s/external/{external_name}"),
            format!("{GC_BUCKET}/s/external/"),
            format!("/{external}"),
            format!("{GC_BUCKET}/s/chunks/0000000000000007/"),
            GC_BUCKET.to_owned(),
            String::new(),
        ];
        for path in refused {
            assert_eq!(
                gc_check(GC_BUCKET, &path),
                Err(ColdGcRefusal::ForeignPath {
                    raft_group_id: GC_GROUP,
                    seq: 9,
                    bucket_id: GC_BUCKET.to_owned(),
                    path: path.clone(),
                }),
                "{path}"
            );
        }
        // An entry of a reserved or invalid bucket deletes nothing, even a
        // path of the matching shape.
        for bucket in ["snapshots", "__ursula", "A"] {
            let path = new_external_payload_path(&BucketStreamId::new(bucket, "s"));
            assert!(
                matches!(
                    gc_check(bucket, &path),
                    Err(ColdGcRefusal::ForeignPath { .. })
                ),
                "{path}"
            );
        }
    }

    #[test]
    fn cold_index_paths_parse_only_bucket_and_stream() {
        let plain = parse_cold_index_page_path(
            "benchcmp/journal/cold-index/00000000000000000007/00000000000000000042.idx",
        )
        .expect("plain path");
        assert_eq!(plain.stream_id, BucketStreamId::new("benchcmp", "journal"));

        // The six-part grouped-stream layout is gone.
        assert_eq!(
            parse_cold_index_page_path(
                "benchcmp/run-42/journal/cold-index/00000000000000000007/00000000000000000042.idx",
            ),
            None
        );
    }

    #[test]
    fn try_new_omitted_cache_installs_default_cache() {
        let config = ColdConfig {
            backend: ColdBackend::Memory,
            cache: None,
            ..Default::default()
        };

        let store = ColdStore::try_new(&config).expect("memory cold store");
        let cache = read_cache_params(&store);

        assert_eq!(cache.max_bytes, 256 * 1024 * 1024);
        assert_eq!(cache.block_bytes, 1024 * 1024);
        assert_eq!(cache.max_readahead_blocks, 4);
    }

    #[test]
    fn try_new_zero_cache_disables_cache() {
        let config = ColdConfig {
            backend: ColdBackend::Memory,
            cache: Some(ursula_config::ColdCacheConfig {
                max_size: ursula_config::HumanSize::bytes(0),
                ..Default::default()
            }),
            ..Default::default()
        };

        let store = ColdStore::try_new(&config).expect("memory cold store");

        assert!(store.read_cache.is_none());
    }

    #[test]
    fn try_new_custom_cache_installs_cache() {
        let config = ColdConfig {
            backend: ColdBackend::Memory,
            cache: Some(ursula_config::ColdCacheConfig {
                max_size: ursula_config::HumanSize::mib(7),
                block_size: ursula_config::HumanSize::kib(512),
                readahead_blocks: 3,
            }),
            ..Default::default()
        };

        let store = ColdStore::try_new(&config).expect("memory cold store");
        let cache = read_cache_params(&store);

        assert_eq!(cache.max_bytes, 7 * 1024 * 1024);
        assert_eq!(cache.block_bytes, 512 * 1024);
        assert_eq!(cache.max_readahead_blocks, 3);
    }

    fn s3_test_config(
        encryption: ursula_config::S3ServerSideEncryption,
        kms_key_id: Option<&str>,
    ) -> ColdConfig {
        ColdConfig {
            backend: ColdBackend::S3,
            s3: Some(ursula_config::S3Config {
                bucket: Some("test-bucket".to_owned()),
                region: Some("us-east-1".to_owned()),
                server_side_encryption: encryption,
                kms_key_id: kms_key_id.map(str::to_owned),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn s3_store_defaults_to_sse_s3_and_reports_it() {
        let store = ColdStore::try_new(&s3_test_config(
            ursula_config::S3ServerSideEncryption::Aes256,
            None,
        ))
        .expect("s3 cold store");
        assert_eq!(store.info().encryption, Some("aes256"));
    }

    #[test]
    fn s3_store_reports_kms_and_disabled_modes() {
        let kms = ColdStore::try_new(&s3_test_config(
            ursula_config::S3ServerSideEncryption::AwsKms,
            Some("arn:aws:kms:us-east-1:111122223333:key/test"),
        ))
        .expect("s3 cold store with kms");
        assert_eq!(kms.info().encryption, Some("aws-kms"));

        let disabled = ColdStore::try_new(&s3_test_config(
            ursula_config::S3ServerSideEncryption::None,
            None,
        ))
        .expect("s3 cold store without sse");
        assert_eq!(disabled.info().encryption, Some("none"));
    }

    #[test]
    fn kms_key_without_kms_mode_is_rejected() {
        let err = ColdStore::try_new(&s3_test_config(
            ursula_config::S3ServerSideEncryption::Aes256,
            Some("arn:aws:kms:us-east-1:111122223333:key/test"),
        ))
        .expect_err("kms key without aws-kms mode");
        assert!(err.to_string().contains("aws-kms"), "got: {err}");
    }

    /// Records every S3 request opendal sends and answers it like S3 would.
    struct RecordingS3 {
        requests: std::sync::Mutex<Vec<(http::Method, String, http::HeaderMap)>>,
    }

    impl opendal::raw::HttpFetch for RecordingS3 {
        async fn fetch(
            &self,
            req: http::Request<opendal::Buffer>,
        ) -> opendal::Result<http::Response<opendal::raw::HttpBody>> {
            let query = req.uri().query().unwrap_or_default().to_owned();
            self.requests.lock().expect("requests").push((
                req.method().clone(),
                query.clone(),
                req.headers().clone(),
            ));
            let body = if query.contains("uploads") {
                "<InitiateMultipartUploadResult><Bucket>b</Bucket><Key>k</Key>\
                 <UploadId>upload-1</UploadId></InitiateMultipartUploadResult>"
            } else if req.method() == http::Method::POST {
                "<CompleteMultipartUploadResult><Bucket>b</Bucket><Key>k</Key>\
                 <ETag>\"done\"</ETag></CompleteMultipartUploadResult>"
            } else {
                ""
            };
            let body = opendal::Buffer::from(Bytes::from_static(body.as_bytes()));
            let len = body.len() as u64;
            Ok(http::Response::builder()
                .status(200)
                .header("etag", "\"part\"")
                .header("content-length", len)
                .body(opendal::raw::HttpBody::new(
                    futures_util::stream::iter(vec![Ok(body)]),
                    Some(len),
                ))
                .expect("response"))
        }
    }

    /// A prefix listing finds the files of one run of names directly in its
    /// directory, with their sizes, and nothing that only shares a part of
    /// the prefix or lies deeper.
    #[tokio::test]
    async fn a_prefix_listing_finds_one_run_of_names() {
        let store = ColdStore::memory().expect("memory");
        for (path, len) in [
            ("b/_parts/00000000/aa-1", 1),
            ("b/_parts/00000000/ab-1", 2),
            ("b/_parts/00000000/ab-2", 3),
            ("b/_parts/00000000/abc-1", 4),
            ("b/_parts/00000000/ac-1", 5),
            ("b/_parts/00000000/ab-/deeper", 6),
        ] {
            store.write_chunk(path, &vec![0; len]).await.expect("write");
        }
        let mut listed = store
            .list_file_sizes_with_prefix("b/_parts/00000000/", "ab-", usize::MAX)
            .await
            .expect("list");
        listed.sort();
        assert_eq!(listed, [("ab-1".to_owned(), 2), ("ab-2".to_owned(), 3)]);
        assert!(
            store
                .list_file_sizes_with_prefix("b/_parts/00000001/", "ab-", usize::MAX)
                .await
                .expect("list")
                .is_empty()
        );
    }

    /// Regression (AWS EKS run): with SSE on, opendal put
    /// `x-amz-server-side-encryption` on every UploadPart, which real S3
    /// rejects, so every cold snapshot of 8 MiB or more failed. The SSE
    /// headers must ride CreateMultipartUpload only, and the stripped
    /// requests must stay correctly signed.
    #[tokio::test]
    async fn multipart_upload_sends_sse_headers_on_create_only() {
        for (encryption, kms_key_id, expected) in [
            (
                ursula_config::S3ServerSideEncryption::Aes256,
                None,
                "AES256",
            ),
            (
                ursula_config::S3ServerSideEncryption::AwsKms,
                Some("arn:aws:kms:us-east-1:111122223333:key/test"),
                "aws:kms",
            ),
        ] {
            let mut config = s3_test_config(encryption, kms_key_id);
            let s3 = config.s3.as_mut().expect("s3 config");
            s3.endpoint = Some("http://127.0.0.1:9".to_owned());
            s3.access_key_id = Some("AKIDTEST".to_owned());
            s3.secret_access_key = Some("secret".to_owned());
            s3.session_token = Some("token".to_owned());
            let s3 = config.s3.as_ref().expect("s3 config");
            let recorder = std::sync::Arc::new(RecordingS3 {
                requests: std::sync::Mutex::default(),
            });
            let builder = opendal::services::S3::default()
                .bucket("test-bucket")
                .region("us-east-1")
                .endpoint("http://127.0.0.1:9")
                .access_key_id("AKIDTEST")
                .secret_access_key("secret")
                .session_token("token");
            let (builder, _) = super::apply_s3_encryption_with_fetcher(
                builder,
                s3,
                opendal::raw::HttpClient::with(recorder.clone()),
            )
            .expect("encryption");
            let store = ColdStore::from_operator(
                opendal::Operator::new(builder).expect("operator").finish(),
                super::ColdStoreInfo {
                    backend: "s3",
                    root: None,
                    bucket: None,
                    region: None,
                    endpoint: None,
                    encryption: None,
                },
            );

            let mut writer = store.open_object_writer("snap/body").await.expect("open");
            let body = vec![7u8; super::COLD_OBJECT_WRITE_PART_BYTES + 1];
            writer.write(Bytes::from(body)).await.expect("write");
            writer.close().await.expect("close");

            let requests = recorder.requests.lock().expect("requests");
            let kind = |method: &http::Method, query: &str| match *method {
                http::Method::POST if query.contains("uploads") => "create",
                http::Method::PUT if query.contains("partNumber") => "part",
                http::Method::POST if query.contains("uploadId") => "complete",
                _ => "other",
            };
            let kinds: Vec<_> = requests.iter().map(|(m, q, _)| kind(m, q)).collect();
            assert_eq!(kinds, ["create", "part", "part", "complete"], "{expected}");
            for (method, query, headers) in requests.iter() {
                let sse = headers
                    .get("x-amz-server-side-encryption")
                    .map(|v| v.to_str().expect("ascii"));
                let authorization = headers
                    .get("authorization")
                    .expect("signed")
                    .to_str()
                    .expect("ascii");
                let signed = authorization
                    .split("SignedHeaders=")
                    .nth(1)
                    .and_then(|rest| rest.split(',').next())
                    .expect("signed headers");
                if kind(method, query) == "create" {
                    assert_eq!(sse, Some(expected));
                    if kms_key_id.is_some() {
                        assert!(
                            headers.contains_key("x-amz-server-side-encryption-aws-kms-key-id")
                        );
                    }
                    continue;
                }
                assert!(
                    !headers
                        .keys()
                        .any(|name| name.as_str().starts_with("x-amz-server-side-encryption")),
                    "{expected}: {method} ?{query} carries SSE headers: {headers:?}"
                );
                // Re-signed: every signed header is present (host is
                // set by the HTTP client) and none is an SSE header.
                for name in signed.split(';') {
                    assert!(
                        name == "host" || headers.contains_key(name),
                        "{expected}: {method} ?{query} signs absent header {name}"
                    );
                }
            }
        }
    }

    #[test]
    fn lru_queue_stays_bounded_under_repeated_hits() {
        // Working set fits entirely in the cache (4 blocks == max_bytes), so there
        // is no eviction pressure and the recency deque is the only thing that
        // could grow. Before compaction it grew by one entry per hit (~40k here);
        // it must stay bounded to the live set instead.
        let cache = ColdReadCache::new(ColdReadCacheParams {
            max_bytes: 4 * 1024,
            block_bytes: 1024,
            max_readahead_blocks: 0,
        });
        for index in 0..4 {
            cache.insert("p".to_owned(), index, Bytes::from(vec![0u8; 1024]));
        }
        for _ in 0..10_000 {
            for index in 0..4 {
                assert!(cache.get("p", index).is_some());
            }
        }
        let inner = cache.inner.lock().expect("cache mutex");
        assert_eq!(inner.blocks.len(), 4, "live blocks unchanged");
        assert!(
            inner.lru.len() <= inner.blocks.len() * 2 + 16,
            "lru deque grew unbounded: {} entries for {} live blocks",
            inner.lru.len(),
            inner.blocks.len(),
        );
    }

    #[test]
    fn readahead_readers_are_pruned_and_invalidated() {
        // Measured before F13: one reader entry per stream ever read
        // (149 B each), never evicted.
        let cache = ColdReadCache::new(ColdReadCacheParams {
            max_bytes: 4 * 1024,
            block_bytes: 1024,
            max_readahead_blocks: 2,
        });
        for index in 0..20_000 {
            let stream_id = BucketStreamId::new("bucket", format!("s{index}"));
            cache.record_stream_read(&stream_id, 0, 8);
        }
        let readers = cache.inner.lock().expect("cache mutex").readers.len();
        assert!(readers <= 4_096, "{readers} reader entries retained");

        // The most recent reader keeps its sequential state (a fresh entry
        // reading at offset 8 would score 0).
        let recent = BucketStreamId::new("bucket", "s19999");
        assert_eq!(cache.record_stream_read(&recent, 8, 8), 2);

        let doomed = BucketStreamId::new("other", "gone");
        cache.record_stream_read(&doomed, 0, 8);
        cache.invalidate_prefix(&super::cold_chunk_prefix(&doomed));
        assert!(
            !cache
                .inner
                .lock()
                .expect("cache mutex")
                .readers
                .contains_key(&doomed)
        );
        cache.invalidate_prefix(&super::cold_bucket_prefix("bucket"));
        assert!(cache.inner.lock().expect("cache mutex").readers.is_empty());
    }

    #[test]
    fn s3_without_bucket_fails() {
        let config = ColdConfig {
            backend: ColdBackend::S3,
            ..Default::default()
        };
        let err = ColdStore::try_new(&config).expect_err("s3 without bucket should fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }
}
