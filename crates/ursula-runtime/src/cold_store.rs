use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
#[cfg(not(madsim))]
use std::time::SystemTime;
#[cfg(not(madsim))]
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use crossbeam_utils::CachePadded;
use futures_util::TryStreamExt;
use opendal::EntryMode;
use opendal::Operator;
use opendal::Scheme;
use opendal::layers::RetryLayer;
use opendal::layers::TimeoutLayer;
use ursula_config::config::ColdBackend;
use ursula_shard::BucketStreamId;
use ursula_stream::ColdChunkRef;
use ursula_stream::ObjectPayloadRef;

use crate::ColdConfig;
use crate::ColdIndexPageKey;

pub(crate) const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

/// Part size of [`ColdObjectWriter`] uploads (above S3's 5 MiB multipart
/// minimum).
pub const COLD_OBJECT_WRITE_PART_BYTES: usize = 8 * 1024 * 1024;

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
// Keep this global atomic isolated from unrelated statics. This does not remove
// contention on the counter itself, but avoids accidental false sharing with
// adjacent data without adding a per-core sequence scheme to this low-frequency
// object-key path.
static COLD_CHUNK_SEQUENCE: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));

/// Applies the configured server-side encryption to an S3 builder and returns
/// the label recorded in [`ColdStoreInfo`]. Shared by the cold store and the
/// S3 snapshot store so backup objects inherit the same posture (#149).
pub(crate) fn apply_s3_encryption(
    builder: opendal::services::S3,
    s3: &ursula_config::S3Config,
) -> io::Result<(opendal::services::S3, &'static str)> {
    use ursula_config::S3ServerSideEncryption;
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
                .then_with(|| {
                    left.stream_id
                        .affinity_key
                        .cmp(&right.stream_id.affinity_key)
                })
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

    /// Lists the file names directly below the directory `dir` (which ends
    /// in `/`), without recursing into subdirectories. Stream GC uses it so a
    /// sweep never reaches another stream's namespace (F14g).
    pub async fn list_file_names(&self, dir: &str) -> io::Result<Vec<String>> {
        self.count_list_request();
        let mut lister = self
            .operator
            .lister_with(dir)
            .await
            .map_err(|err| cold_store_io_error(dir, err))?;
        let mut names = Vec::new();
        while let Some(entry) = lister
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
            if !name.is_empty() && !name.contains('/') {
                names.push(name.to_owned());
            }
        }
        names.sort();
        Ok(names)
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
        let object_start = object
            .object_offset
            .checked_add(read_start_offset - object.start_offset)
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

fn parse_cold_index_page_path(path: &str) -> Option<ColdIndexPageKey> {
    let parts = path.split('/').collect::<Vec<_>>();
    let (stream_id, generation, page_id) = match parts.as_slice() {
        [bucket, stream, "cold-index", generation, page] => {
            (BucketStreamId::new(*bucket, *stream), *generation, *page)
        }
        [bucket, affinity, stream, "cold-index", generation, page] => (
            BucketStreamId::with_affinity(*bucket, *affinity, *stream),
            *generation,
            *page,
        ),
        _ => return None,
    };
    if stream_id.bucket_id.is_empty()
        || stream_id
            .affinity_key
            .as_ref()
            .is_some_and(String::is_empty)
        || stream_id.stream_id.is_empty()
    {
        return None;
    }
    Some(ColdIndexPageKey {
        stream_id,
        generation: generation.parse().ok()?,
        page_id: page_id.strip_suffix(".idx")?.parse().ok()?,
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
        let capacity_blocks = self.config.max_bytes / self.config.block_bytes.max(1);
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
        if drop_count == 0 {
            return;
        }
        let (_, threshold, _) = generations.select_nth_unstable(drop_count - 1);
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
        if inner.lru.len() <= inner.blocks.len() * 2 + 16 {
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
    new_cold_chunk_path_in_generation(stream_id, 0, start_offset, end_offset)
}

/// Names a new exclusive chunk of the stream incarnation whose cold
/// generation is `generation` (F14g). Generation 0 keeps the legacy name
/// directly under `{stream}/chunks/`; any other generation adds a
/// `{generation:016x}/` component, so stream GC can delete one incarnation's
/// chunks without touching another's.
pub fn new_cold_chunk_path_in_generation(
    stream_id: &BucketStreamId,
    generation: u64,
    start_offset: u64,
    end_offset: u64,
) -> String {
    let unix_nanos = cold_object_unix_nanos();
    let sequence = COLD_CHUNK_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!(
        "{}{start_offset:016x}-{end_offset:016x}-{unix_nanos:032x}-{sequence:016x}.bin",
        cold_chunk_dir(stream_id, generation)
    )
}

/// The directory holding the exclusive chunks of one stream incarnation
/// (F14g): `{stream}/chunks/` for generation 0, otherwise
/// `{stream}/chunks/{generation:016x}/`.
pub fn cold_chunk_dir(stream_id: &BucketStreamId, generation: u64) -> String {
    if generation == 0 {
        cold_chunk_prefix(stream_id)
    } else {
        format!("{stream_id}/chunks/{generation:016x}/")
    }
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
    let sequence = COLD_CHUNK_SEQUENCE.fetch_add(1, Ordering::Relaxed);
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

/// The directory of a stream's legacy (generation-0) exclusive chunks.
/// Mirrors the layout of [`new_cold_chunk_path`]. Never remove it
/// recursively: an affinity stream named `chunks` under this stream's name
/// lives below it (D4); stream GC lists it one level at a time instead.
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
    use bytes::Bytes;
    use ursula_config::config::ColdBackend;
    use ursula_shard::BucketStreamId;

    use super::ColdReadCache;
    use super::ColdStore;
    use super::parse_cold_index_page_path;
    use crate::ColdConfig;
    use crate::ColdReadCacheParams;

    fn read_cache_params(store: &ColdStore) -> ColdReadCacheParams {
        store
            .read_cache
            .as_ref()
            .map(|cache| cache.config)
            .expect("read cache")
    }

    #[test]
    fn cold_index_paths_preserve_optional_affinity() {
        let plain = parse_cold_index_page_path(
            "benchcmp/journal/cold-index/00000000000000000007/00000000000000000042.idx",
        )
        .expect("plain path");
        assert_eq!(plain.stream_id, BucketStreamId::new("benchcmp", "journal"));

        let grouped = parse_cold_index_page_path(
            "benchcmp/run-42/journal/cold-index/00000000000000000007/00000000000000000042.idx",
        )
        .expect("grouped path");
        assert_eq!(
            grouped.stream_id,
            BucketStreamId::with_affinity("benchcmp", "run-42", "journal")
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
