use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use opendal::{Operator, Scheme};
use ursula_shard::BucketStreamId;
use ursula_stream::{ColdChunkRef, ObjectPayloadRef};

pub(crate) const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
static COLD_CHUNK_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const DEFAULT_COLD_CACHE_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_COLD_CACHE_BLOCK_BYTES: usize = 1024 * 1024;
const DEFAULT_COLD_CACHE_READAHEAD_BLOCKS: usize = 4;

#[derive(Clone, Debug)]
pub struct ColdStore {
    operator: Operator,
    read_cache: Option<Arc<ColdReadCache>>,
}

pub type ColdStoreHandle = Arc<ColdStore>;

impl ColdStore {
    pub fn memory() -> io::Result<Self> {
        let operator = Operator::via_iter(Scheme::Memory, [])
            .map_err(|err| io::Error::other(err.to_string()))?;
        Ok(Self::new(operator))
    }

    pub fn fs(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref();
        fs::create_dir_all(root)?;
        let operator = Operator::via_iter(
            Scheme::Fs,
            [("root".to_owned(), root.to_string_lossy().to_string())],
        )
        .map_err(|err| io::Error::other(err.to_string()))?;
        Ok(Self::new(operator))
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

        Ok(Self::new(
            Operator::new(builder)
                .map_err(|err| io::Error::other(err.to_string()))?
                .finish(),
        ))
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

    fn new(operator: Operator) -> Self {
        Self {
            operator,
            read_cache: ColdReadCache::from_env().map(Arc::new),
        }
    }

    pub fn with_read_cache(mut self, config: ColdReadCacheConfig) -> Self {
        self.read_cache = Some(Arc::new(ColdReadCache::new(config)));
        self
    }

    pub fn without_read_cache(mut self) -> Self {
        self.read_cache = None;
        self
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
            .map_err(|err| cold_store_io_error(path, err))?;
        if let Some(cache) = &self.read_cache {
            cache.invalidate_path(path);
        }
        Ok(())
    }

    pub async fn remove_all(&self, path: &str) -> io::Result<()> {
        self.operator
            .remove_all(path)
            .await
            .map_err(|err| cold_store_io_error(path, err))?;
        if let Some(cache) = &self.read_cache {
            cache.invalidate_prefix(path);
        }
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
        let Some(cache) = &self.read_cache else {
            return self
                .read_object_range_uncached(object, object_start, object_end, len)
                .await;
        };
        let bytes = self
            .read_object_range_cached(cache, object, object_start, object_end, len)
            .await?;
        if let Some(stream_id) = stream_id {
            let readahead_blocks = cache.record_stream_read(stream_id, read_start_offset, len);
            if readahead_blocks > 0 {
                self.spawn_readahead(object.clone(), object_end, readahead_blocks);
            }
        }
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
        tokio::spawn(async move {
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
