use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;

use ursula_shard::BucketStreamId;
use ursula_stream::ColdChunkRef;
use ursula_stream::ObjectPayloadRef;
use ursula_stream::StreamReadColdIndexSegment;

use crate::cold_store::ColdStoreHandle;

pub type ColdIndexPageStoreFuture<'a, T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send + 'a>>;

const COLD_INDEX_PAGE_MAGIC: &[u8; 8] = b"UCIDX001";
const COLD_INDEX_PAGE_VERSION: u16 = 2;
const COLD_INDEX_ENTRY_COLD_CHUNK: u8 = 1;
const COLD_INDEX_ENTRY_EXTERNAL_SEGMENT: u8 = 2;
const FNV64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV64_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColdIndexPageKey {
    pub stream_id: BucketStreamId,
    pub generation: u64,
    pub page_id: u64,
}

impl ColdIndexPageKey {
    pub fn path(&self) -> String {
        format!(
            "{}/cold-index/{:020}/{:020}.idx",
            self.stream_id, self.generation, self.page_id
        )
    }
}

pub fn cold_index_prefix(stream_id: &BucketStreamId) -> String {
    format!("{stream_id}/cold-index/")
}

/// The directory holding one cold generation's pages of a stream (F14g).
pub fn cold_index_generation_dir(stream_id: &BucketStreamId, generation: u64) -> String {
    format!("{stream_id}/cold-index/{generation:020}/")
}

/// Parses a page file name inside a generation directory, `{page:020}.idx`.
pub fn parse_cold_index_page_file_name(name: &str) -> Option<u64> {
    let digits = name.strip_suffix(".idx")?;
    if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdIndexPage {
    pub start_offset: u64,
    pub end_offset: u64,
    pub cold_chunks: Vec<ColdChunkRef>,
    pub external_segments: Vec<ObjectPayloadRef>,
}

impl ColdIndexPage {
    pub fn covers(&self, offset: u64) -> bool {
        self.start_offset <= offset && offset < self.end_offset
    }
}

#[derive(Debug, Clone)]
pub struct ColdIndexPageRollback {
    key: ColdIndexPageKey,
    previous: Option<ColdIndexPage>,
    written_chunk: ColdChunkRef,
    clipped_entries: u64,
}

impl ColdIndexPageRollback {
    /// Entries this write removed because they overlapped the written range
    /// (F19 clip rule). Callers drop cached pages of the stream when any were
    /// removed, so a cached copy cannot keep serving them.
    pub fn clipped_entries(&self) -> u64 {
        self.clipped_entries
    }
}

/// Total entries clipped by a set of page writes.
pub fn clipped_entries(rollback: &[ColdIndexPageRollback]) -> u64 {
    rollback
        .iter()
        .map(ColdIndexPageRollback::clipped_entries)
        .fold(0, u64::saturating_add)
}

fn ranges_overlap(start: u64, end: u64, other_start: u64, other_end: u64) -> bool {
    start < other_end && other_start < end
}

/// F19 step 1, the clip rule: removes every entry other than `chunk` that
/// overlaps `chunk`'s range. Only call it for a range whose bytes state
/// proves: a flush of hot bytes, or a replacement of state-held refs. Such a
/// range is never covered by a committed external append or by another
/// committed chunk, so whatever overlaps it is a leftover of a proposal that
/// did not commit there (a rejected external append, a stale flush).
fn clip_page_for_proven_chunk(page: &mut ColdIndexPage, chunk: &ColdChunkRef) -> u64 {
    let before = page
        .cold_chunks
        .len()
        .saturating_add(page.external_segments.len());
    page.cold_chunks.retain(|existing| {
        (existing.start_offset == chunk.start_offset
            && existing.end_offset == chunk.end_offset
            && existing.s3_path == chunk.s3_path)
            || !ranges_overlap(
                existing.start_offset,
                existing.end_offset,
                chunk.start_offset,
                chunk.end_offset,
            )
    });
    page.external_segments.retain(|existing| {
        !ranges_overlap(
            existing.start_offset,
            existing.end_offset,
            chunk.start_offset,
            chunk.end_offset,
        )
    });
    let after = page
        .cold_chunks
        .len()
        .saturating_add(page.external_segments.len());
    u64::try_from(before.saturating_sub(after)).unwrap_or(u64::MAX)
}

fn encode_page(key: &ColdIndexPageKey, page: &ColdIndexPage) -> Vec<u8> {
    let mut body = Vec::new();
    put_string(&mut body, &key.stream_id.bucket_id);
    // Version-2 affinity marker: always 0, since grouped streams are gone.
    put_u8(&mut body, 0);
    put_string(&mut body, &key.stream_id.stream_id);
    put_u64(&mut body, key.generation);
    put_u64(&mut body, key.page_id);
    put_u64(&mut body, page.start_offset);
    put_u64(&mut body, page.end_offset);
    put_u32(
        &mut body,
        u32::try_from(page.cold_chunks.len()).expect("cold index cold chunk count fits u32"),
    );
    for chunk in &page.cold_chunks {
        put_u8(&mut body, COLD_INDEX_ENTRY_COLD_CHUNK);
        put_u64(&mut body, chunk.start_offset);
        put_u64(&mut body, chunk.end_offset);
        put_u64(&mut body, chunk.object_size);
        put_string(&mut body, &chunk.s3_path);
    }
    put_u32(
        &mut body,
        u32::try_from(page.external_segments.len())
            .expect("cold index external segment count fits u32"),
    );
    for object in &page.external_segments {
        put_u8(&mut body, COLD_INDEX_ENTRY_EXTERNAL_SEGMENT);
        put_u64(&mut body, object.start_offset);
        put_u64(&mut body, object.end_offset);
        put_u64(&mut body, object.object_size);
        put_string(&mut body, &object.s3_path);
    }

    let mut bytes = Vec::with_capacity(COLD_INDEX_PAGE_MAGIC.len() + 2 + 4 + body.len() + 8);
    bytes.extend_from_slice(COLD_INDEX_PAGE_MAGIC);
    put_u16(&mut bytes, COLD_INDEX_PAGE_VERSION);
    put_u32(
        &mut bytes,
        u32::try_from(body.len()).expect("cold index page body len fits u32"),
    );
    bytes.extend_from_slice(&body);
    put_u64(&mut bytes, checksum64(&body));
    bytes
}

fn decode_page(key: &ColdIndexPageKey, bytes: &[u8]) -> io::Result<ColdIndexPage> {
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.read_exact(COLD_INDEX_PAGE_MAGIC.len())?;
    if magic != COLD_INDEX_PAGE_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cold index page has invalid magic",
        ));
    }
    let version = cursor.read_u16()?;
    if version != COLD_INDEX_PAGE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported cold index page version {version}"),
        ));
    }
    let body_len = usize::try_from(cursor.read_u32()?).expect("u32 fits usize");
    let body = cursor.read_exact(body_len)?;
    let expected_checksum = cursor.read_u64()?;
    if cursor.remaining() != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cold index page has trailing bytes",
        ));
    }
    let actual_checksum = checksum64(body);
    if actual_checksum != expected_checksum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cold index page checksum mismatch",
        ));
    }

    let mut body = Cursor::new(body);
    let bucket_id = body.read_string()?;
    // The affinity marker is always 0; marker 1 (a grouped stream) is no
    // longer valid.
    if body.read_u8()? != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cold index page has invalid affinity marker",
        ));
    }
    let stream_id = body.read_string()?;
    let generation = body.read_u64()?;
    let page_id = body.read_u64()?;
    if bucket_id != key.stream_id.bucket_id
        || stream_id != key.stream_id.stream_id
        || generation != key.generation
        || page_id != key.page_id
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cold index page key metadata mismatch",
        ));
    }
    let start_offset = body.read_u64()?;
    let end_offset = body.read_u64()?;
    let cold_chunk_count = body.read_u32()?;
    let mut cold_chunks =
        Vec::with_capacity(usize::try_from(cold_chunk_count).expect("u32 fits usize"));
    for _ in 0..cold_chunk_count {
        let tag = body.read_u8()?;
        if tag != COLD_INDEX_ENTRY_COLD_CHUNK {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cold index page expected cold chunk entry",
            ));
        }
        cold_chunks.push(ColdChunkRef {
            start_offset: body.read_u64()?,
            end_offset: body.read_u64()?,
            object_size: body.read_u64()?,
            s3_path: body.read_string()?,
            object_offset: 0,
            shared_object: false,
            payload_digest: String::new(),
        });
    }
    let external_segment_count = body.read_u32()?;
    let mut external_segments =
        Vec::with_capacity(usize::try_from(external_segment_count).expect("u32 fits usize"));
    for _ in 0..external_segment_count {
        let tag = body.read_u8()?;
        if tag != COLD_INDEX_ENTRY_EXTERNAL_SEGMENT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cold index page expected external segment entry",
            ));
        }
        external_segments.push(ObjectPayloadRef {
            start_offset: body.read_u64()?,
            end_offset: body.read_u64()?,
            object_size: body.read_u64()?,
            s3_path: body.read_string()?,
            object_offset: 0,
        });
    }
    if body.remaining() != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cold index page body has trailing bytes",
        ));
    }
    Ok(ColdIndexPage {
        start_offset,
        end_offset,
        cold_chunks,
        external_segments,
    })
}

fn put_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_string(out: &mut Vec<u8>, value: &str) {
    put_u32(
        out,
        u32::try_from(value.len()).expect("cold index string len fits u32"),
    );
    out.extend_from_slice(value.as_bytes());
}

fn checksum64(bytes: &[u8]) -> u64 {
    let mut hash = FNV64_OFFSET_BASIS;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV64_PRIME);
    }
    hash
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn read_exact(&mut self, len: usize) -> io::Result<&'a [u8]> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "cold index page offset overflow",
            )
        })?;
        if end > self.bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "cold index page ended early",
            ));
        }
        let slice = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(slice)
    }

    fn read_u8(&mut self) -> io::Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> io::Result<u16> {
        let mut bytes = [0; 2];
        bytes.copy_from_slice(self.read_exact(2)?);
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u32(&mut self) -> io::Result<u32> {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(self.read_exact(4)?);
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> io::Result<u64> {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(self.read_exact(8)?);
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_string(&mut self) -> io::Result<String> {
        let len = usize::try_from(self.read_u32()?).expect("u32 fits usize");
        let bytes = self.read_exact(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("cold index page contains invalid UTF-8: {err}"),
            )
        })
    }
}

pub trait ColdIndexPageStore: Send + Sync {
    fn put_page<'a>(
        &'a self,
        key: &'a ColdIndexPageKey,
        page: &'a ColdIndexPage,
    ) -> ColdIndexPageStoreFuture<'a, ()>;

    fn get_page<'a>(
        &'a self,
        key: &'a ColdIndexPageKey,
    ) -> ColdIndexPageStoreFuture<'a, Option<ColdIndexPage>>;
}

/// Writes `chunk` into the pages of the stream incarnation whose pages live
/// under `generation` (F14g).
pub async fn write_cold_chunk_index_pages_in_generation<S: ColdIndexPageStore + ?Sized>(
    store: &S,
    stream_id: &BucketStreamId,
    generation: u64,
    chunk: &ColdChunkRef,
) -> io::Result<()> {
    write_cold_chunk_index_pages_with_rollback_in_generation(store, stream_id, generation, chunk)
        .await
        .map(|_| ())
}

/// Writes `chunk` into every page it spans. The range must be one whose bytes
/// state proves (a flush of hot bytes, or a replacement of state-held refs):
/// the write clips every other overlapping entry in the same
/// read-modify-write (F19 step 1). Rollback restores the previous pages.
pub async fn write_cold_chunk_index_pages_with_rollback_in_generation<
    S: ColdIndexPageStore + ?Sized,
>(
    store: &S,
    stream_id: &BucketStreamId,
    generation: u64,
    chunk: &ColdChunkRef,
) -> io::Result<Vec<ColdIndexPageRollback>> {
    if chunk.end_offset <= chunk.start_offset {
        return Ok(Vec::new());
    }
    let first_page_id = chunk.start_offset / ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES;
    let last_page_id = (chunk.end_offset - 1) / ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES;
    let mut rollback = Vec::new();
    for page_id in first_page_id..=last_page_id {
        let key = ColdIndexPageKey {
            stream_id: stream_id.clone(),
            generation,
            page_id,
        };
        let page_start = page_id.saturating_mul(ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES);
        let page_end = page_start.saturating_add(ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES);
        let previous = store.get_page(&key).await?;
        let mut page = previous.clone().unwrap_or_else(|| ColdIndexPage {
            start_offset: page_start,
            end_offset: page_end,
            cold_chunks: Vec::new(),
            external_segments: Vec::new(),
        });
        let clipped_entries = clip_page_for_proven_chunk(&mut page, chunk);
        rollback.push(ColdIndexPageRollback {
            key: key.clone(),
            previous,
            written_chunk: chunk.clone(),
            clipped_entries,
        });
        page.cold_chunks.retain(|existing| {
            existing.start_offset != chunk.start_offset || existing.end_offset != chunk.end_offset
        });
        page.cold_chunks.push(chunk.clone());
        page.cold_chunks.sort_by_key(|chunk| chunk.start_offset);
        store.put_page(&key, &page).await?;
    }
    Ok(rollback)
}

pub async fn rollback_cold_index_pages<S: ColdIndexPageStore + ?Sized>(
    store: &S,
    rollback: Vec<ColdIndexPageRollback>,
) -> io::Result<()> {
    for entry in rollback.into_iter().rev() {
        let Some(current) = store.get_page(&entry.key).await? else {
            continue;
        };
        let current_still_has_written_chunk = current.cold_chunks.iter().any(|chunk| {
            chunk.start_offset == entry.written_chunk.start_offset
                && chunk.end_offset == entry.written_chunk.end_offset
                && chunk.s3_path == entry.written_chunk.s3_path
        });
        if !current_still_has_written_chunk {
            continue;
        }
        let page = entry.previous.unwrap_or_else(|| {
            let page_start = entry
                .key
                .page_id
                .saturating_mul(ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES);
            let page_end = page_start.saturating_add(ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES);
            ColdIndexPage {
                start_offset: page_start,
                end_offset: page_end,
                cold_chunks: Vec::new(),
                external_segments: Vec::new(),
            }
        });
        store.put_page(&entry.key, &page).await?;
    }
    Ok(())
}

/// Bounded-state F5 offload: writes the page entries of a *committed*
/// state-held external ref under `generation`. State proves its bytes, so the
/// same read-modify-write clips every other entry overlapping it, chunk or
/// external, as F19's clip rule does for flushes. Idempotent: rewriting the
/// same ref leaves the pages unchanged. Returns the entries clipped.
pub async fn write_proven_external_index_pages<S: ColdIndexPageStore + ?Sized>(
    store: &S,
    stream_id: &BucketStreamId,
    generation: u64,
    object: &ObjectPayloadRef,
) -> io::Result<u64> {
    if object.end_offset <= object.start_offset {
        return Ok(0);
    }
    let first_page_id = object.start_offset / ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES;
    let last_page_id = (object.end_offset - 1) / ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES;
    let mut clipped = 0_u64;
    for page_id in first_page_id..=last_page_id {
        let key = ColdIndexPageKey {
            stream_id: stream_id.clone(),
            generation,
            page_id,
        };
        let page_start = page_id.saturating_mul(ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES);
        let page_end = page_start.saturating_add(ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES);
        let previous = store.get_page(&key).await?;
        let mut page = previous.clone().unwrap_or_else(|| ColdIndexPage {
            start_offset: page_start,
            end_offset: page_end,
            cold_chunks: Vec::new(),
            external_segments: Vec::new(),
        });
        let overlaps = |start: u64, end: u64| {
            ranges_overlap(start, end, object.start_offset, object.end_offset)
        };
        let before = page
            .cold_chunks
            .len()
            .saturating_add(page.external_segments.len());
        page.cold_chunks
            .retain(|chunk| !overlaps(chunk.start_offset, chunk.end_offset));
        page.external_segments.retain(|existing| {
            existing == object || !overlaps(existing.start_offset, existing.end_offset)
        });
        let kept = page
            .cold_chunks
            .len()
            .saturating_add(page.external_segments.len());
        clipped = clipped.saturating_add(u64::try_from(before.saturating_sub(kept)).unwrap_or(0));
        if !page
            .external_segments
            .iter()
            .any(|existing| existing == object)
        {
            page.external_segments.push(object.clone());
            page.external_segments
                .sort_by_key(|existing| existing.start_offset);
        }
        if previous.as_ref() != Some(&page) {
            store.put_page(&key, &page).await?;
        }
    }
    Ok(clipped)
}

#[derive(Debug, Default)]
pub struct InMemoryColdIndexPageStore {
    pages: Mutex<HashMap<ColdIndexPageKey, Vec<u8>>>,
}

#[derive(Debug, Clone)]
pub struct ColdStoreColdIndexPageStore {
    cold_store: ColdStoreHandle,
}

impl ColdStoreColdIndexPageStore {
    pub fn new(cold_store: ColdStoreHandle) -> Self {
        Self { cold_store }
    }
}

impl ColdIndexPageStore for ColdStoreColdIndexPageStore {
    fn put_page<'a>(
        &'a self,
        key: &'a ColdIndexPageKey,
        page: &'a ColdIndexPage,
    ) -> ColdIndexPageStoreFuture<'a, ()> {
        Box::pin(async move {
            let bytes = encode_page(key, page);
            self.cold_store
                .write_cold_index_page(&key.path(), &bytes)
                .await?;
            Ok(())
        })
    }

    fn get_page<'a>(
        &'a self,
        key: &'a ColdIndexPageKey,
    ) -> ColdIndexPageStoreFuture<'a, Option<ColdIndexPage>> {
        Box::pin(async move {
            self.cold_store
                .read_cold_index_page(&key.path())
                .await?
                .map(|bytes| decode_page(key, &bytes))
                .transpose()
        })
    }
}

impl InMemoryColdIndexPageStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ColdIndexPageStore for InMemoryColdIndexPageStore {
    fn put_page<'a>(
        &'a self,
        key: &'a ColdIndexPageKey,
        page: &'a ColdIndexPage,
    ) -> ColdIndexPageStoreFuture<'a, ()> {
        Box::pin(async move {
            let bytes = encode_page(key, page);
            self.pages
                .lock()
                .expect("cold index page store mutex poisoned")
                .insert(key.clone(), bytes);
            Ok(())
        })
    }

    fn get_page<'a>(
        &'a self,
        key: &'a ColdIndexPageKey,
    ) -> ColdIndexPageStoreFuture<'a, Option<ColdIndexPage>> {
        Box::pin(async move {
            self.pages
                .lock()
                .expect("cold index page store mutex poisoned")
                .get(key)
                .map(|bytes| decode_page(key, bytes))
                .transpose()
        })
    }
}

/// Default byte bound of a group's cold-index page cache (bounded-state
/// F13): pages hold one entry per flush or external append, so a page count
/// alone does not bound memory.
pub const DEFAULT_COLD_INDEX_CACHE_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug)]
pub struct ColdIndexPageCache<S: ColdIndexPageStore + ?Sized> {
    store: Arc<S>,
    capacity_pages: usize,
    capacity_bytes: usize,
    inner: Mutex<ColdIndexPageCacheInner>,
}

#[derive(Debug, Default)]
struct ColdIndexPageCacheInner {
    next_generation: u64,
    pages: HashMap<ColdIndexPageKey, ColdIndexPageCacheEntry>,
    lru: VecDeque<(ColdIndexPageKey, u64)>,
    /// Sum of the cached pages' approximate heap bytes.
    bytes: usize,
    /// Invalidation epochs, one per hash slot of stream ids (RT2). A reload
    /// captures its stream's epoch before fetching and caches the page only
    /// if no invalidation bumped it meanwhile. Slots are shared by hash, so a
    /// collision only skips a cache insert.
    invalidation_epochs: [u64; INVALIDATION_EPOCH_SLOTS],
}

const INVALIDATION_EPOCH_SLOTS: usize = 32;
const INVALIDATION_EPOCH_SLOTS_U64: u64 = 32;

fn invalidation_epoch_slot(stream_id: &BucketStreamId) -> usize {
    use std::hash::Hash;
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    stream_id.hash(&mut hasher);
    // The modulo bounds the slot below the array length.
    usize::try_from(hasher.finish() % INVALIDATION_EPOCH_SLOTS_U64).unwrap_or(0)
}

#[derive(Debug)]
struct ColdIndexPageCacheEntry {
    page: Arc<ColdIndexPage>,
    generation: u64,
    bytes: usize,
}

/// Approximate heap bytes of one cached page: the page, its entries and
/// their path strings, plus the key.
fn approximate_page_bytes(key: &ColdIndexPageKey, page: &ColdIndexPage) -> usize {
    let chunks = page.cold_chunks.iter().fold(0_usize, |total, chunk| {
        total
            .saturating_add(std::mem::size_of::<ColdChunkRef>())
            .saturating_add(chunk.s3_path.len())
            .saturating_add(chunk.payload_digest.len())
    });
    let externals = page
        .external_segments
        .iter()
        .fold(0_usize, |total, segment| {
            total
                .saturating_add(std::mem::size_of::<ObjectPayloadRef>())
                .saturating_add(segment.s3_path.len())
        });
    std::mem::size_of::<ColdIndexPage>()
        .saturating_add(std::mem::size_of::<ColdIndexPageCacheEntry>())
        .saturating_add(std::mem::size_of::<ColdIndexPageKey>())
        .saturating_add(key.stream_id.bucket_id.len())
        .saturating_add(key.stream_id.stream_id.len())
        .saturating_add(chunks)
        .saturating_add(externals)
}

impl ColdIndexPageCacheInner {
    fn remove_where(&mut self, mut remove: impl FnMut(&ColdIndexPageKey) -> bool) {
        let mut freed = 0_usize;
        self.pages.retain(|key, entry| {
            let drop = remove(key);
            if drop {
                freed = freed.saturating_add(entry.bytes);
            }
            !drop
        });
        self.bytes = self.bytes.saturating_sub(freed);
        self.lru.retain(|(key, _)| !remove(key));
    }

    fn invalidation_epoch(&self, stream_id: &BucketStreamId) -> u64 {
        self.invalidation_epochs
            .get(invalidation_epoch_slot(stream_id))
            .copied()
            .unwrap_or(0)
    }

    fn bump_invalidation_epoch(&mut self, stream_id: &BucketStreamId) {
        if let Some(epoch) = self
            .invalidation_epochs
            .get_mut(invalidation_epoch_slot(stream_id))
        {
            *epoch = epoch.wrapping_add(1);
        }
    }
}

impl<S: ColdIndexPageStore + ?Sized> ColdIndexPageCache<S> {
    pub fn new(store: Arc<S>, capacity_pages: usize) -> Self {
        Self::with_capacity_bytes(store, capacity_pages, DEFAULT_COLD_INDEX_CACHE_BYTES)
    }

    /// A cache bounded by both a page count and approximate heap bytes
    /// (bounded-state F13); the least recently used pages go first.
    pub fn with_capacity_bytes(
        store: Arc<S>,
        capacity_pages: usize,
        capacity_bytes: usize,
    ) -> Self {
        Self {
            store,
            capacity_pages,
            capacity_bytes,
            inner: Mutex::new(ColdIndexPageCacheInner::default()),
        }
    }

    /// Approximate heap bytes of the cached pages.
    pub fn cached_bytes(&self) -> usize {
        self.inner
            .lock()
            .expect("cold index page cache mutex poisoned")
            .bytes
    }

    pub async fn put_page(&self, key: &ColdIndexPageKey, page: &ColdIndexPage) -> io::Result<()> {
        self.store.put_page(key, page).await?;
        self.insert(key.clone(), Arc::new(page.clone()));
        Ok(())
    }

    pub async fn get_page(&self, key: &ColdIndexPageKey) -> io::Result<Option<Arc<ColdIndexPage>>> {
        if let Some(page) = self.get_cached(key) {
            return Ok(Some(page));
        }
        self.reload_page(key).await
    }

    /// Drops every cached generation/page for one stream. Compaction invokes
    /// this on every replica when the replicated replacement command applies.
    pub fn invalidate_stream(&self, stream_id: &BucketStreamId) {
        let mut inner = self.inner.lock().expect("cold index cache mutex poisoned");
        inner.bump_invalidation_epoch(stream_id);
        inner.remove_where(|key| &key.stream_id == stream_id);
    }

    /// Drops every cached page.
    pub fn clear(&self) {
        let mut inner = self.inner.lock().expect("cold index cache mutex poisoned");
        inner.pages.clear();
        inner.lru.clear();
        inner.bytes = 0;
        for epoch in &mut inner.invalidation_epochs {
            *epoch = epoch.wrapping_add(1);
        }
    }

    /// Drops the cached pages of one stream generation that cover
    /// `[start_offset, end_offset)`. A replicated `FlushCold` invokes this on
    /// every replica when it applies: the leader rewrote those pages (and the
    /// F19 clip rule may have removed entries from them), so a follower's
    /// cached copy must not keep serving the old entries.
    pub fn invalidate_range(
        &self,
        stream_id: &BucketStreamId,
        generation: u64,
        start_offset: u64,
        end_offset: u64,
    ) {
        if end_offset <= start_offset {
            return;
        }
        let span = ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES;
        let first_page = start_offset / span;
        let last_page = (end_offset - 1) / span;
        let covered = |key: &ColdIndexPageKey| {
            &key.stream_id == stream_id
                && key.generation == generation
                && (first_page..=last_page).contains(&key.page_id)
        };
        let mut inner = self.inner.lock().expect("cold index cache mutex poisoned");
        inner.bump_invalidation_epoch(stream_id);
        inner.remove_where(covered);
    }

    /// Fetches a page from the store. It is cached only when no
    /// invalidation of its stream ran while the fetch was in flight (RT2):
    /// otherwise the fetched copy may predate the invalidating command.
    async fn reload_page(&self, key: &ColdIndexPageKey) -> io::Result<Option<Arc<ColdIndexPage>>> {
        let epoch = self
            .inner
            .lock()
            .expect("cold index page cache mutex poisoned")
            .invalidation_epoch(&key.stream_id);
        let Some(page) = self.store.get_page(key).await? else {
            return Ok(None);
        };
        let page = Arc::new(page);
        self.insert_unless_invalidated(key.clone(), page.clone(), epoch);
        Ok(Some(page))
    }

    /// Drops one cached page and fetches it again (an object it named was
    /// missing, so the cached copy may predate a compaction or clip).
    pub async fn refresh_page(
        &self,
        key: &ColdIndexPageKey,
    ) -> io::Result<Option<Arc<ColdIndexPage>>> {
        self.inner
            .lock()
            .expect("cold index page cache mutex poisoned")
            .remove_where(|cached| cached == key);
        self.reload_page(key).await
    }

    pub async fn object_segments_for_read(
        &self,
        stream_id: &BucketStreamId,
        segment: &StreamReadColdIndexSegment,
    ) -> io::Result<Vec<ObjectPayloadRef>> {
        let key = ColdIndexPageKey {
            stream_id: stream_id.clone(),
            generation: segment.generation,
            page_id: segment.page_id,
        };
        let Some(page) = self.get_page(&key).await? else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("cold index page '{}' does not exist", key.path()),
            ));
        };
        let read_end = segment
            .read_start_offset
            .checked_add(u64::try_from(segment.len).expect("cold index read len fits u64"))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cold index read range overflows",
                )
            })?;
        let mut objects = objects_for_read(&page, segment.read_start_offset, read_end);
        if !objects_cover_range(&objects, segment.read_start_offset, read_end)
            && let Some(reloaded) = self.reload_page(&key).await?
        {
            objects = objects_for_read(&reloaded, segment.read_start_offset, read_end);
        }
        if !objects_cover_range(&objects, segment.read_start_offset, read_end) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cold index page does not cover requested read range",
            ));
        }
        Ok(objects)
    }

    pub fn cached_page_count(&self) -> usize {
        self.inner
            .lock()
            .expect("cold index page cache mutex poisoned")
            .pages
            .len()
    }

    fn get_cached(&self, key: &ColdIndexPageKey) -> Option<Arc<ColdIndexPage>> {
        let mut inner = self
            .inner
            .lock()
            .expect("cold index page cache mutex poisoned");
        let page = inner.pages.get(key)?.page.clone();
        Self::touch(&mut inner, key.clone());
        Self::compact_lru_if_needed(&mut inner);
        Some(page)
    }

    fn insert(&self, key: ColdIndexPageKey, page: Arc<ColdIndexPage>) {
        let mut inner = self
            .inner
            .lock()
            .expect("cold index page cache mutex poisoned");
        Self::insert_locked(
            &mut inner,
            key,
            page,
            self.capacity_pages,
            self.capacity_bytes,
        );
    }

    fn insert_unless_invalidated(
        &self,
        key: ColdIndexPageKey,
        page: Arc<ColdIndexPage>,
        epoch: u64,
    ) {
        let mut inner = self
            .inner
            .lock()
            .expect("cold index page cache mutex poisoned");
        if inner.invalidation_epoch(&key.stream_id) != epoch {
            return;
        }
        Self::insert_locked(
            &mut inner,
            key,
            page,
            self.capacity_pages,
            self.capacity_bytes,
        );
    }

    fn insert_locked(
        inner: &mut ColdIndexPageCacheInner,
        key: ColdIndexPageKey,
        page: Arc<ColdIndexPage>,
        capacity_pages: usize,
        capacity_bytes: usize,
    ) {
        let generation = Self::touch(inner, key.clone());
        let bytes = approximate_page_bytes(&key, &page);
        inner.bytes = inner.bytes.saturating_add(bytes);
        if let Some(replaced) = inner.pages.insert(key, ColdIndexPageCacheEntry {
            page,
            generation,
            bytes,
        }) {
            inner.bytes = inner.bytes.saturating_sub(replaced.bytes);
        }
        Self::evict_over_capacity(inner, capacity_pages, capacity_bytes);
        Self::compact_lru_if_needed(inner);
    }

    /// `touch` appends a fresh `(key, generation)` per lookup and leaves the
    /// stale one behind; eviction only reclaims those when the cache is over
    /// capacity. Rebuild the deque from the live pages, in recency order, once
    /// it holds more than twice as many entries (F13): amortized O(1) per
    /// lookup, since each rebuild leaves `pages.len()` entries.
    fn compact_lru_if_needed(inner: &mut ColdIndexPageCacheInner) {
        if inner.lru.len() <= inner.pages.len().saturating_mul(2).saturating_add(16) {
            return;
        }
        let mut live = inner
            .pages
            .iter()
            .map(|(key, entry)| (entry.generation, key.clone()))
            .collect::<Vec<_>>();
        live.sort_unstable_by_key(|(generation, _)| *generation);
        inner.lru = live
            .into_iter()
            .map(|(generation, key)| (key, generation))
            .collect();
    }

    fn touch(inner: &mut ColdIndexPageCacheInner, key: ColdIndexPageKey) -> u64 {
        let generation = inner.next_generation;
        inner.next_generation = inner.next_generation.saturating_add(1);
        if let Some(entry) = inner.pages.get_mut(&key) {
            entry.generation = generation;
        }
        inner.lru.push_back((key, generation));
        generation
    }

    fn evict_over_capacity(
        inner: &mut ColdIndexPageCacheInner,
        capacity_pages: usize,
        capacity_bytes: usize,
    ) {
        if capacity_pages == 0 {
            inner.pages.clear();
            inner.lru.clear();
            inner.bytes = 0;
            return;
        }
        while inner.pages.len() > capacity_pages || inner.bytes > capacity_bytes {
            let Some((key, generation)) = inner.lru.pop_front() else {
                break;
            };
            let stale = inner
                .pages
                .get(&key)
                .is_none_or(|entry| entry.generation != generation);
            if stale {
                continue;
            }
            if let Some(removed) = inner.pages.remove(&key) {
                inner.bytes = inner.bytes.saturating_sub(removed.bytes);
            }
        }
    }
}

/// Loads and de-duplicates the chunk references present in a stream's index
/// pages. Chunks crossing a 64 MiB page boundary intentionally appear in more
/// than one page.
pub async fn load_cold_chunks_from_pages<S: ColdIndexPageStore + ?Sized>(
    store: &S,
    keys: &[ColdIndexPageKey],
) -> io::Result<Vec<ColdChunkRef>> {
    let mut chunks = Vec::new();
    let mut seen = HashSet::new();
    for key in keys {
        let Some(page) = store.get_page(key).await? else {
            continue;
        };
        for chunk in page.cold_chunks {
            let identity = (chunk.start_offset, chunk.end_offset, chunk.s3_path.clone());
            if seen.insert(identity) {
                chunks.push(chunk);
            }
        }
    }
    chunks.sort_by(|left, right| {
        left.start_offset
            .cmp(&right.start_offset)
            .then_with(|| left.end_offset.cmp(&right.end_offset))
            .then_with(|| left.s3_path.cmp(&right.s3_path))
    });
    Ok(chunks)
}

/// Selects the oldest contiguous run of undersized raw chunks whose combined
/// payload reaches the byte target without exceeding the configured maximum.
pub fn select_cold_chunk_compaction(
    chunks: &[ColdChunkRef],
    target_bytes: u64,
    max_bytes: u64,
) -> Option<Vec<ColdChunkRef>> {
    if target_bytes == 0 || max_bytes < target_bytes {
        return None;
    }
    let mut candidate = Vec::new();
    let mut bytes = 0_u64;
    let mut next_offset = None;
    for chunk in chunks {
        let logical_bytes = chunk.end_offset.checked_sub(chunk.start_offset)?;
        let usable = logical_bytes > 0
            && chunk.object_size == logical_bytes
            && chunk.object_size < target_bytes;
        let contiguous = next_offset.is_none_or(|offset| offset == chunk.start_offset);
        let next_bytes = bytes.checked_add(chunk.object_size);
        if !usable || !contiguous || next_bytes.is_none_or(|total| total > max_bytes) {
            candidate.clear();
            bytes = 0;
            next_offset = None;
            if !usable {
                continue;
            }
        }
        bytes = bytes.checked_add(chunk.object_size)?;
        next_offset = Some(chunk.end_offset);
        candidate.push(chunk.clone());
        if candidate.len() >= 2 && bytes >= target_bytes {
            return Some(candidate);
        }
    }
    None
}

/// Atomically at the page-object level replaces a contiguous set of chunk
/// references with one equivalent object. Every rewritten page always points
/// at readable old or new bytes, so a retry after a partial S3 failure remains
/// safe.
pub async fn replace_cold_chunk_index_pages_with_rollback_in_generation<
    S: ColdIndexPageStore + ?Sized,
>(
    store: &S,
    stream_id: &BucketStreamId,
    generation: u64,
    old_chunks: &[ColdChunkRef],
    replacement: &ColdChunkRef,
) -> io::Result<Option<Vec<ColdIndexPageRollback>>> {
    if old_chunks.len() < 2 || replacement.end_offset <= replacement.start_offset {
        return Ok(None);
    }
    let first_page_id = replacement.start_offset / ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES;
    let last_page_id = (replacement.end_offset - 1) / ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES;
    let old_identities = old_chunks
        .iter()
        .map(|chunk| (chunk.start_offset, chunk.end_offset, chunk.s3_path.as_str()))
        .collect::<HashSet<_>>();
    let mut pages = Vec::new();
    let mut found = HashSet::new();
    for page_id in first_page_id..=last_page_id {
        let key = ColdIndexPageKey {
            stream_id: stream_id.clone(),
            generation,
            page_id,
        };
        let Some(mut page) = store.get_page(&key).await? else {
            return Ok(None);
        };
        let previous = page.clone();
        for chunk in &page.cold_chunks {
            let identity = (chunk.start_offset, chunk.end_offset, chunk.s3_path.as_str());
            if old_identities.contains(&identity) {
                found.insert((chunk.start_offset, chunk.end_offset, chunk.s3_path.clone()));
            }
        }
        page.cold_chunks.retain(|chunk| {
            !old_identities.contains(&(
                chunk.start_offset,
                chunk.end_offset,
                chunk.s3_path.as_str(),
            ))
        });
        page.cold_chunks.retain(|chunk| {
            chunk.start_offset != replacement.start_offset
                || chunk.end_offset != replacement.end_offset
        });
        page.cold_chunks.push(replacement.clone());
        page.cold_chunks.sort_by_key(|chunk| chunk.start_offset);
        pages.push((key, previous, page));
    }
    if found.len() != old_identities.len() {
        return Ok(None);
    }
    let mut rollback = Vec::with_capacity(pages.len());
    for (key, previous, page) in pages {
        if let Err(err) = store.put_page(&key, &page).await {
            rollback_cold_index_pages(store, rollback).await?;
            return Err(err);
        }
        rollback.push(ColdIndexPageRollback {
            key,
            previous: Some(previous),
            written_chunk: replacement.clone(),
            clipped_entries: 0,
        });
    }
    Ok(Some(rollback))
}

/// Objects of `page` that serve `[read_start, read_end)`, ordered by start.
///
/// Reads apply the page-local rules of F19 repair, so a page that repair
/// has not reached yet, or a stale cached copy of a page it rewrote, serves
/// the same bytes: of several external entries at one start only the
/// last-written is used, and an external entry overlapping a chunk entry is
/// ignored (chunk entries hold committed, proven bytes). Chunk entries come
/// first at a shared start.
fn objects_for_read(page: &ColdIndexPage, read_start: u64, read_end: u64) -> Vec<ObjectPayloadRef> {
    let mut objects = Vec::new();
    for chunk in &page.cold_chunks {
        if let Some(object) = intersect_object(&ObjectPayloadRef::from(chunk), read_start, read_end)
        {
            objects.push(object);
        }
    }
    for (index, object) in page.external_segments.iter().enumerate() {
        let superseded = page
            .external_segments
            .iter()
            .skip(index.saturating_add(1))
            .any(|later| later.start_offset == object.start_offset);
        let overlaps_chunk = page.cold_chunks.iter().any(|chunk| {
            ranges_overlap(
                object.start_offset,
                object.end_offset,
                chunk.start_offset,
                chunk.end_offset,
            )
        });
        if superseded || overlaps_chunk {
            continue;
        }
        if let Some(object) = intersect_object(object, read_start, read_end) {
            objects.push(object);
        }
    }
    objects.sort_by_key(|object| object.start_offset);
    objects
}

fn intersect_object(
    object: &ObjectPayloadRef,
    read_start: u64,
    read_end: u64,
) -> Option<ObjectPayloadRef> {
    let start = object.start_offset.max(read_start);
    let end = object.end_offset.min(read_end);
    (start < end).then(|| object.clone())
}

fn objects_cover_range(objects: &[ObjectPayloadRef], start: u64, end: u64) -> bool {
    let mut expected = start;
    for object in objects {
        if object.end_offset <= expected {
            continue;
        }
        if object.start_offset > expected {
            return false;
        }
        expected = object.end_offset;
        if expected >= end {
            return true;
        }
    }
    expected == end
}

/// How long an external object may predate its stream's creation before page
/// repair treats it as another incarnation's object (bounded-state D4).
const REPAIR_PREDATING_SLACK_MS: u64 = 60_000;

/// What replicated state proves about one stream, as page repair needs it
/// (bounded-state F19 step 2). Built on the group's leader from its applied
/// state, inside the group actor, so it is consistent with every page write
/// that the actor serializes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdIndexRepairInput {
    pub stream_id: BucketStreamId,
    /// Cold-index generation of the live incarnation; repair reads and
    /// writes only its pages.
    pub generation: u64,
    pub retained_offset: u64,
    pub tail_offset: u64,
    pub created_at_ms: u64,
    /// Ranges the hot buffer holds: committed inline bytes, never external.
    pub hot_ranges: Vec<(u64, u64)>,
    /// State-held object refs (shared pack slices, external refs).
    pub state_refs: Vec<ObjectPayloadRef>,
}

/// Counts from one repair pass. `pages_rewritten` pages changed; every
/// dropped entry is counted once, under the first rule that dropped it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ColdIndexRepairReport {
    pub streams_scanned: u64,
    pub pages_scanned: u64,
    pub pages_rewritten: u64,
    /// External entries followed by a later-written entry at the same start.
    pub superseded_entries_dropped: u64,
    /// External entries that start at or beyond the stream's tail.
    pub beyond_tail_entries_dropped: u64,
    /// External entries overlapping a chunk entry, hot bytes, or another
    /// object's state ref.
    pub overlapping_entries_dropped: u64,
    /// External entries whose object predates the stream's creation.
    pub predating_entries_dropped: u64,
}

impl ColdIndexRepairReport {
    pub fn entries_dropped(&self) -> u64 {
        self.superseded_entries_dropped
            .saturating_add(self.beyond_tail_entries_dropped)
            .saturating_add(self.overlapping_entries_dropped)
            .saturating_add(self.predating_entries_dropped)
    }

    pub fn add(&mut self, other: &Self) {
        self.streams_scanned = self.streams_scanned.saturating_add(other.streams_scanned);
        self.pages_scanned = self.pages_scanned.saturating_add(other.pages_scanned);
        self.pages_rewritten = self.pages_rewritten.saturating_add(other.pages_rewritten);
        self.superseded_entries_dropped = self
            .superseded_entries_dropped
            .saturating_add(other.superseded_entries_dropped);
        self.beyond_tail_entries_dropped = self
            .beyond_tail_entries_dropped
            .saturating_add(other.beyond_tail_entries_dropped);
        self.overlapping_entries_dropped = self
            .overlapping_entries_dropped
            .saturating_add(other.overlapping_entries_dropped);
        self.predating_entries_dropped = self
            .predating_entries_dropped
            .saturating_add(other.predating_entries_dropped);
    }
}

/// One step of the leader-side repair cursor over a group's streams.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepairColdIndexRequest {
    /// Resume after this stream id; `None` starts a new cycle.
    pub after: Option<BucketStreamId>,
    pub max_streams: usize,
    /// Repair only this stream and leave the cursor alone. The F2 driver
    /// repairs a stream's pages right before compacting its shared refs.
    pub stream: Option<BucketStreamId>,
    /// Wall-clock time of a cursor step, which also runs retention GC
    /// (F14f) over the streams it visits; `None` skips retention GC.
    pub retention_gc_now_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepairColdIndexResponse {
    pub report: ColdIndexRepairReport,
    /// Pages the step found holding at least two exclusive chunks below
    /// [`COMPACTION_DEBT_CHUNK_BYTES`]: compaction debt the leader's
    /// compactor drains (F14d). This is how idle streams, which no new
    /// flush records as debt, still get compacted after a failover.
    pub compaction_pages: Vec<ColdIndexPageKey>,
    /// Where the next step resumes; `None` starts a new cycle.
    pub next_after: Option<BucketStreamId>,
    /// This step ran on the leader and reached the end of the group's
    /// streams. A follower answers `false` and repairs nothing.
    pub cycle_completed: bool,
}

/// Millisecond timestamp encoded in a staged external object's name
/// (`{stream}/external/{unix_nanos:032x}-{sequence:016x}.bin`), or `None`
/// when the name carries none (other layouts, or the simulator's zero clock).
fn external_object_unix_ms(s3_path: &str) -> Option<u64> {
    let (prefix, file_name) = s3_path.rsplit_once('/')?;
    if !prefix.ends_with("/external") && prefix != "external" {
        return None;
    }
    let (nanos_hex, _) = file_name.strip_suffix(".bin")?.split_once('-')?;
    let nanos = u128::from_str_radix(nanos_hex, 16).ok()?;
    if nanos == 0 {
        return None;
    }
    u64::try_from(nanos / 1_000_000).ok()
}

/// F19 step 2 for one page: keeps only the last-written external entry at
/// each start offset, and drops external entries that start at or beyond the
/// tail, that overlap a chunk entry, hot bytes or another object's state ref,
/// or whose objects predate the stream's creation by more than a minute.
/// Chunk entries hold committed bytes and are kept. Within a leader's term
/// the group actor runs page writes one at a time, so a later entry at the
/// same start follows a proposal that did not commit there.
pub fn repair_cold_index_page(
    page: &mut ColdIndexPage,
    input: &ColdIndexRepairInput,
) -> ColdIndexRepairReport {
    let mut report = ColdIndexRepairReport {
        pages_scanned: 1,
        ..ColdIndexRepairReport::default()
    };
    let externals = std::mem::take(&mut page.external_segments);
    let mut kept = Vec::with_capacity(externals.len());
    for (index, entry) in externals.iter().enumerate() {
        let superseded = externals
            .iter()
            .skip(index.saturating_add(1))
            .any(|later| later.start_offset == entry.start_offset);
        if superseded {
            report.superseded_entries_dropped = report.superseded_entries_dropped.saturating_add(1);
            continue;
        }
        if entry.start_offset >= input.tail_offset {
            report.beyond_tail_entries_dropped =
                report.beyond_tail_entries_dropped.saturating_add(1);
            continue;
        }
        let overlaps =
            |start: u64, end: u64| ranges_overlap(entry.start_offset, entry.end_offset, start, end);
        let overlapping = page
            .cold_chunks
            .iter()
            .any(|chunk| overlaps(chunk.start_offset, chunk.end_offset))
            || input
                .hot_ranges
                .iter()
                .any(|(start, end)| overlaps(*start, *end))
            || input.state_refs.iter().any(|object| {
                object.s3_path != entry.s3_path && overlaps(object.start_offset, object.end_offset)
            });
        if overlapping {
            report.overlapping_entries_dropped =
                report.overlapping_entries_dropped.saturating_add(1);
            continue;
        }
        if external_object_unix_ms(&entry.s3_path).is_some_and(|object_ms| {
            object_ms.saturating_add(REPAIR_PREDATING_SLACK_MS) < input.created_at_ms
        }) {
            report.predating_entries_dropped = report.predating_entries_dropped.saturating_add(1);
            continue;
        }
        kept.push(entry.clone());
    }
    page.external_segments = kept;
    if report.entries_dropped() > 0 {
        report.pages_rewritten = 1;
    }
    report
}

/// Exclusive chunks below this size make a page compaction debt when the
/// repair cursor reads it (F14d); the compactor applies its configured
/// target when it drains the debt.
pub const COMPACTION_DEBT_CHUNK_BYTES: u64 = 8 << 20;

/// Whether `page` holds at least two exclusive chunk entries below
/// [`COMPACTION_DEBT_CHUNK_BYTES`].
pub fn page_has_compaction_debt(page: &ColdIndexPage) -> bool {
    page.cold_chunks
        .iter()
        .filter(|chunk| {
            !chunk.shared_object
                && chunk.end_offset.saturating_sub(chunk.start_offset) < COMPACTION_DEBT_CHUNK_BYTES
        })
        .nth(1)
        .is_some()
}

/// Repairs the pages of each stream in `inputs` and drops the cached pages
/// of every stream whose pages changed. Also returns the pages that hold
/// compaction debt (F14d).
pub async fn repair_cold_index_streams<S: ColdIndexPageStore + ?Sized>(
    store: &S,
    cache: Option<&ColdIndexPageCache<ColdStoreColdIndexPageStore>>,
    inputs: &[ColdIndexRepairInput],
) -> io::Result<(ColdIndexRepairReport, Vec<ColdIndexPageKey>)> {
    let mut report = ColdIndexRepairReport::default();
    let mut compaction_pages = Vec::new();
    for input in inputs {
        let stream_report =
            repair_stream_cold_index_pages_collecting(store, input, &mut compaction_pages).await?;
        if stream_report.pages_rewritten > 0 {
            if let Some(cache) = cache {
                cache.invalidate_stream(&input.stream_id);
            }
            tracing::info!(
                stream = %input.stream_id,
                pages_rewritten = stream_report.pages_rewritten,
                entries_dropped = stream_report.entries_dropped(),
                "repaired cold-index pages"
            );
        }
        report.add(&stream_report);
    }
    Ok((report, compaction_pages))
}

/// Repairs every cold-index page of one stream from the retained offset on:
/// the pages up to the one holding the tail, then any later pages that
/// entries spanning past the tail left behind. Writes only changed pages.
pub async fn repair_stream_cold_index_pages<S: ColdIndexPageStore + ?Sized>(
    store: &S,
    input: &ColdIndexRepairInput,
) -> io::Result<ColdIndexRepairReport> {
    repair_stream_cold_index_pages_collecting(store, input, &mut Vec::new()).await
}

/// [`repair_stream_cold_index_pages`], also collecting the pages that hold
/// compaction debt into `compaction_pages`.
pub async fn repair_stream_cold_index_pages_collecting<S: ColdIndexPageStore + ?Sized>(
    store: &S,
    input: &ColdIndexRepairInput,
    compaction_pages: &mut Vec<ColdIndexPageKey>,
) -> io::Result<ColdIndexRepairReport> {
    let span = ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES;
    let tail_page_id = input.tail_offset / span;
    let mut page_id = input.retained_offset / span;
    let mut report = ColdIndexRepairReport {
        streams_scanned: 1,
        ..ColdIndexRepairReport::default()
    };
    loop {
        let key = ColdIndexPageKey {
            stream_id: input.stream_id.clone(),
            generation: input.generation,
            page_id,
        };
        match store.get_page(&key).await? {
            Some(mut page) => {
                let page_report = repair_cold_index_page(&mut page, input);
                if page_report.pages_rewritten > 0 {
                    store.put_page(&key, &page).await?;
                }
                if page_has_compaction_debt(&page) {
                    compaction_pages.push(key);
                }
                report.add(&page_report);
            }
            None if page_id >= tail_page_id => break,
            None => {}
        }
        let Some(next) = page_id.checked_add(1) else {
            break;
        };
        page_id = next;
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(page_id: u64) -> ColdIndexPageKey {
        ColdIndexPageKey {
            stream_id: BucketStreamId::new("benchcmp", "cold-index"),
            generation: 7,
            page_id,
        }
    }

    fn page(start_offset: u64, end_offset: u64) -> ColdIndexPage {
        ColdIndexPage {
            start_offset,
            end_offset,
            cold_chunks: vec![ColdChunkRef {
                start_offset,
                end_offset,
                s3_path: format!("benchcmp/cold-index/chunks/{start_offset:020}.bin"),
                object_size: end_offset - start_offset,
                ..Default::default()
            }],
            external_segments: Vec::new(),
        }
    }

    /// F13: the page cache is bounded by approximate bytes, not only by a
    /// page count, because one page can hold thousands of entries.
    #[tokio::test]
    async fn page_cache_is_bounded_by_bytes_and_evicts_least_recently_used() {
        let store = Arc::new(InMemoryColdIndexPageStore::new());
        let one_page = approximate_page_bytes(&key(0), &page(0, 128));
        let cache = ColdIndexPageCache::with_capacity_bytes(store, 1024, one_page * 3);
        for page_id in 0..3 {
            cache
                .put_page(&key(page_id), &page(page_id * 128, (page_id + 1) * 128))
                .await
                .expect("put page");
        }
        assert_eq!(cache.cached_page_count(), 3);
        assert_eq!(cache.cached_bytes(), one_page * 3);
        // Touch page 0 so page 1 is the least recently used.
        assert!(cache.get_cached(&key(0)).is_some());
        cache
            .put_page(&key(3), &page(3 * 128, 4 * 128))
            .await
            .expect("put page");
        assert_eq!(cache.cached_page_count(), 3);
        assert!(cache.cached_bytes() <= one_page * 3);
        assert!(cache.get_cached(&key(1)).is_none(), "LRU page evicted");
        assert!(cache.get_cached(&key(0)).is_some());
        cache.invalidate_stream(&key(0).stream_id);
        assert_eq!(cache.cached_bytes(), 0);
        assert_eq!(cache.cached_page_count(), 0);
    }

    #[tokio::test]
    async fn page_cache_lru_stays_bounded_under_repeated_lookups() {
        // Measured before F13: every lookup appended a cloned key to the
        // recency deque and only an over-capacity insert drained it (1.1M
        // lookups of one page: +223 MiB).
        let store = Arc::new(InMemoryColdIndexPageStore::new());
        let cache = ColdIndexPageCache::new(store, 1024);
        for page_id in 0..4 {
            cache
                .put_page(&key(page_id), &page(page_id * 128, (page_id + 1) * 128))
                .await
                .expect("put page");
        }
        for _ in 0..10_000 {
            for page_id in 0..4 {
                assert!(cache.get_page(&key(page_id)).await.expect("get").is_some());
            }
        }
        {
            let inner = cache.inner.lock().expect("cache mutex");
            assert_eq!(inner.pages.len(), 4);
            assert!(
                inner.lru.len() <= inner.pages.len() * 2 + 16,
                "lru deque grew to {} entries for {} pages",
                inner.lru.len(),
                inner.pages.len()
            );
        }
        // Recency order survives compaction: page 0 is the oldest, so an
        // over-capacity insert into a 4-page cache evicts it first.
        let small = ColdIndexPageCache::new(Arc::new(InMemoryColdIndexPageStore::new()), 4);
        for page_id in 0..4 {
            small
                .put_page(&key(page_id), &page(page_id * 128, (page_id + 1) * 128))
                .await
                .expect("put page");
        }
        for _ in 0..100 {
            for page_id in 1..4 {
                assert!(small.get_page(&key(page_id)).await.expect("get").is_some());
            }
        }
        small
            .put_page(&key(9), &page(9 * 128, 10 * 128))
            .await
            .expect("put page");
        let inner = small.inner.lock().expect("cache mutex");
        assert!(!inner.pages.contains_key(&key(0)));
        assert!((1..4).all(|page_id| inner.pages.contains_key(&key(page_id))));
    }

    #[tokio::test]
    async fn memory_store_round_trips_pages() {
        let store = InMemoryColdIndexPageStore::new();
        let key = key(1);
        let page = page(0, 128);

        assert_eq!(
            key.path(),
            "benchcmp/cold-index/cold-index/00000000000000000007/00000000000000000001.idx"
        );
        assert_eq!(store.get_page(&key).await.expect("get missing"), None);
        store.put_page(&key, &page).await.expect("put page");
        assert_eq!(
            store.get_page(&key).await.expect("get page"),
            Some(page.clone())
        );
        assert!(page.covers(127));
        assert!(!page.covers(128));
    }

    #[tokio::test]
    async fn rollback_skips_page_updated_by_newer_writer() {
        let store = InMemoryColdIndexPageStore::new();
        let stream_id = BucketStreamId::new("benchcmp", "cold-index");
        let first = ColdChunkRef {
            start_offset: 0,
            end_offset: 128,
            s3_path: "benchcmp/cold-index/chunks/first.bin".to_owned(),
            object_size: 128,
            ..Default::default()
        };
        let stale = ColdChunkRef {
            start_offset: 0,
            end_offset: 128,
            s3_path: "benchcmp/cold-index/chunks/stale.bin".to_owned(),
            object_size: 128,
            ..Default::default()
        };
        let newer = ColdChunkRef {
            start_offset: 0,
            end_offset: 128,
            s3_path: "benchcmp/cold-index/chunks/newer.bin".to_owned(),
            object_size: 128,
            ..Default::default()
        };
        write_cold_chunk_index_pages_in_generation(&store, &stream_id, 0, &first)
            .await
            .expect("write first chunk");
        let rollback =
            write_cold_chunk_index_pages_with_rollback_in_generation(&store, &stream_id, 0, &stale)
                .await
                .expect("write stale chunk");
        write_cold_chunk_index_pages_in_generation(&store, &stream_id, 0, &newer)
            .await
            .expect("write newer chunk");

        rollback_cold_index_pages(&store, rollback)
            .await
            .expect("rollback stale chunk");

        let page = store
            .get_page(&ColdIndexPageKey {
                stream_id,
                generation: 0,
                page_id: 0,
            })
            .await
            .expect("get page")
            .expect("page exists");
        assert_eq!(page.cold_chunks, vec![newer]);
    }

    #[tokio::test]
    async fn compact_replacement_rollback_restores_input_chunks() {
        let store = InMemoryColdIndexPageStore::new();
        let stream_id = BucketStreamId::new("benchcmp", "cold-index");
        let first = ColdChunkRef {
            start_offset: 0,
            end_offset: 64,
            s3_path: "benchcmp/cold-index/chunks/first.bin".to_owned(),
            object_size: 64,
            ..Default::default()
        };
        let second = ColdChunkRef {
            start_offset: 64,
            end_offset: 128,
            s3_path: "benchcmp/cold-index/chunks/second.bin".to_owned(),
            object_size: 64,
            ..Default::default()
        };
        let replacement = ColdChunkRef {
            start_offset: 0,
            end_offset: 128,
            s3_path: "benchcmp/cold-index/chunks/compacted.bin".to_owned(),
            object_size: 128,
            ..Default::default()
        };
        for chunk in [&first, &second] {
            write_cold_chunk_index_pages_in_generation(&store, &stream_id, 0, chunk)
                .await
                .expect("write input chunk");
        }

        let rollback = replace_cold_chunk_index_pages_with_rollback_in_generation(
            &store,
            &stream_id,
            0,
            &[first.clone(), second.clone()],
            &replacement,
        )
        .await
        .expect("replace chunks")
        .expect("inputs still match");
        rollback_cold_index_pages(&store, rollback)
            .await
            .expect("rollback replacement");

        let page = store
            .get_page(&ColdIndexPageKey {
                stream_id,
                generation: 0,
                page_id: 0,
            })
            .await
            .expect("get page")
            .expect("page exists");
        assert_eq!(page.cold_chunks, vec![first, second]);
    }

    /// A store whose `get_page` reads the page, then parks until released,
    /// so a test can invalidate the cache while a fetch is in flight.
    struct GatedPageStore {
        inner: InMemoryColdIndexPageStore,
        fetched: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl ColdIndexPageStore for GatedPageStore {
        fn put_page<'a>(
            &'a self,
            key: &'a ColdIndexPageKey,
            page: &'a ColdIndexPage,
        ) -> ColdIndexPageStoreFuture<'a, ()> {
            self.inner.put_page(key, page)
        }

        fn get_page<'a>(
            &'a self,
            key: &'a ColdIndexPageKey,
        ) -> ColdIndexPageStoreFuture<'a, Option<ColdIndexPage>> {
            Box::pin(async move {
                let page = self.inner.get_page(key).await?;
                self.fetched.notify_one();
                self.release.notified().await;
                Ok(page)
            })
        }
    }

    /// RT2: a page fetched before a concurrent invalidation must not be
    /// cached, or a later read keeps serving entries the invalidating
    /// command removed (a compacted or clipped chunk).
    #[tokio::test]
    async fn page_fetched_across_an_invalidation_is_not_cached() {
        let store = Arc::new(GatedPageStore {
            inner: InMemoryColdIndexPageStore::new(),
            fetched: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let cache = Arc::new(ColdIndexPageCache::new(store.clone(), 8));
        let key = key(0);
        store
            .inner
            .put_page(&key, &page(0, 128))
            .await
            .expect("write old page");

        let reader = tokio::spawn({
            let cache = cache.clone();
            let key = key.clone();
            async move { cache.get_page(&key).await }
        });
        store.fetched.notified().await;
        store
            .inner
            .put_page(&key, &page(0, 64))
            .await
            .expect("rewrite page");
        cache.invalidate_range(&key.stream_id, key.generation, 0, 128);
        store.release.notify_one();
        let stale = reader
            .await
            .expect("reader task")
            .expect("read")
            .expect("page");
        assert_eq!(stale.end_offset, 128);
        assert_eq!(cache.cached_page_count(), 0);

        let reload = tokio::spawn({
            let cache = cache.clone();
            let key = key.clone();
            async move { cache.get_page(&key).await }
        });
        store.fetched.notified().await;
        store.release.notify_one();
        let fresh = reload
            .await
            .expect("reload task")
            .expect("reload")
            .expect("page");
        assert_eq!(fresh.end_offset, 64);
        assert_eq!(cache.cached_page_count(), 1);
    }

    #[tokio::test]
    async fn read_reload_repairs_stale_cached_page() {
        let store = Arc::new(InMemoryColdIndexPageStore::new());
        let stream_id = BucketStreamId::new("benchcmp", "cold-index");
        let cache = ColdIndexPageCache::new(store.clone(), 8);
        let first = ColdChunkRef {
            start_offset: 0,
            end_offset: 128,
            s3_path: "benchcmp/cold-index/chunks/first.bin".to_owned(),
            object_size: 128,
            ..Default::default()
        };
        write_cold_chunk_index_pages_in_generation(store.as_ref(), &stream_id, 0, &first)
            .await
            .expect("write first chunk");
        assert_eq!(
            cache
                .object_segments_for_read(&stream_id, &StreamReadColdIndexSegment {
                    generation: 0,
                    page_id: 0,
                    read_start_offset: 0,
                    len: 1,
                },)
                .await
                .expect("read first byte")
                .len(),
            1
        );

        let second = ColdChunkRef {
            start_offset: 128,
            end_offset: 256,
            s3_path: "benchcmp/cold-index/chunks/second.bin".to_owned(),
            object_size: 128,
            ..Default::default()
        };
        write_cold_chunk_index_pages_in_generation(store.as_ref(), &stream_id, 0, &second)
            .await
            .expect("write second chunk behind cache");

        let objects = cache
            .object_segments_for_read(&stream_id, &StreamReadColdIndexSegment {
                generation: 0,
                page_id: 0,
                read_start_offset: 128,
                len: 1,
            })
            .await
            .expect("reload stale page");
        assert_eq!(objects[0].s3_path, second.s3_path);
    }

    #[test]
    fn binary_page_format_round_trips_and_validates() {
        let key = key(42);
        let mut page = page(128, 256);
        page.external_segments.push(ObjectPayloadRef {
            start_offset: 256,
            end_offset: 300,
            s3_path: "benchcmp/cold-index/external/00000000000000000256.bin".to_owned(),
            object_size: 44,
            ..Default::default()
        });
        let bytes = encode_page(&key, &page);
        assert!(bytes.starts_with(COLD_INDEX_PAGE_MAGIC));

        assert_eq!(decode_page(&key, &bytes).expect("decode page"), page);

        let mut corrupted = bytes.clone();
        let last = corrupted.last_mut().expect("checksum byte");
        *last ^= 0xff;
        let err = decode_page(&key, &corrupted).expect_err("corrupt checksum");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let wrong_key = ColdIndexPageKey {
            stream_id: key.stream_id.clone(),
            generation: key.generation + 1,
            page_id: key.page_id,
        };
        let err = decode_page(&wrong_key, &bytes).expect_err("key mismatch");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn page_carrying_affinity_marker_one_is_refused() {
        let key = key(42);
        let mut body = Vec::new();
        put_string(&mut body, &key.stream_id.bucket_id);
        put_u8(&mut body, 1);
        put_string(&mut body, "run-42");
        put_string(&mut body, &key.stream_id.stream_id);
        put_u64(&mut body, key.generation);
        put_u64(&mut body, key.page_id);
        put_u64(&mut body, 0);
        put_u64(&mut body, 10);
        put_u32(&mut body, 0);
        put_u32(&mut body, 0);
        let mut bytes = COLD_INDEX_PAGE_MAGIC.to_vec();
        put_u16(&mut bytes, COLD_INDEX_PAGE_VERSION);
        put_u32(&mut bytes, u32::try_from(body.len()).expect("body len"));
        bytes.extend_from_slice(&body);
        put_u64(&mut bytes, checksum64(&body));

        let err = decode_page(&key, &bytes).expect_err("marker 1 is refused");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("affinity marker"), "{err}");
    }

    #[tokio::test]
    async fn page_cache_loads_on_miss_and_evicts_lru() {
        let store = Arc::new(InMemoryColdIndexPageStore::new());
        for page_id in 0..3 {
            store
                .put_page(&key(page_id), &page(page_id * 100, page_id * 100 + 100))
                .await
                .expect("put page");
        }
        let cache = ColdIndexPageCache::new(store, 2);

        assert_eq!(
            cache
                .get_page(&key(0))
                .await
                .expect("load page")
                .expect("page")
                .start_offset,
            0
        );
        assert_eq!(
            cache
                .get_page(&key(1))
                .await
                .expect("load page")
                .expect("page")
                .start_offset,
            100
        );
        assert_eq!(cache.cached_page_count(), 2);

        // Touch page 0 so page 1 becomes the eviction candidate.
        assert!(
            cache
                .get_page(&key(0))
                .await
                .expect("cached page")
                .is_some()
        );
        assert_eq!(
            cache
                .get_page(&key(2))
                .await
                .expect("load page")
                .expect("page")
                .start_offset,
            200
        );
        assert_eq!(cache.cached_page_count(), 2);
    }

    #[tokio::test]
    async fn zero_capacity_cache_does_not_retain_pages() {
        let store = Arc::new(InMemoryColdIndexPageStore::new());
        store
            .put_page(&key(0), &page(0, 64))
            .await
            .expect("put page");
        let cache = ColdIndexPageCache::new(store, 0);

        assert!(cache.get_page(&key(0)).await.expect("load page").is_some());
        assert_eq!(cache.cached_page_count(), 0);
    }

    #[tokio::test]
    async fn selects_and_replaces_target_sized_contiguous_chunks() {
        let store = InMemoryColdIndexPageStore::new();
        let stream_id = BucketStreamId::new("benchcmp", "compact");
        let chunks = (0..4)
            .map(|index| ColdChunkRef {
                start_offset: index * 2,
                end_offset: index * 2 + 2,
                object_size: 2,
                s3_path: format!("old-{index}"),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        for chunk in &chunks {
            write_cold_chunk_index_pages_in_generation(&store, &stream_id, 0, chunk)
                .await
                .expect("write chunk index");
        }
        let selected =
            select_cold_chunk_compaction(&chunks, 8, 16).expect("select compaction candidate");
        assert_eq!(selected, chunks);
        let replacement = ColdChunkRef {
            start_offset: 0,
            end_offset: 8,
            object_size: 8,
            s3_path: "replacement".to_owned(),
            ..Default::default()
        };
        assert!(
            replace_cold_chunk_index_pages_with_rollback_in_generation(
                &store,
                &stream_id,
                0,
                &selected,
                &replacement,
            )
            .await
            .expect("replace chunks")
            .is_some()
        );
        let loaded = load_cold_chunks_from_pages(&store, &[ColdIndexPageKey {
            stream_id,
            generation: 0,
            page_id: 0,
        }])
        .await
        .expect("load replacement");
        assert_eq!(loaded, vec![replacement]);
    }

    fn external(start_offset: u64, end_offset: u64, s3_path: &str) -> ObjectPayloadRef {
        ObjectPayloadRef {
            start_offset,
            end_offset,
            s3_path: s3_path.to_owned(),
            object_size: end_offset - start_offset,
            object_offset: 0,
        }
    }

    /// Wave-1 follow-up: reads prefer the last-written external entry at a
    /// start, matching F19 repair, instead of the first one (a stale entry
    /// left by a rejected external append).
    #[test]
    fn reads_use_the_last_written_external_entry_at_a_start() {
        let page = ColdIndexPage {
            start_offset: 0,
            end_offset: 64,
            cold_chunks: Vec::new(),
            external_segments: vec![
                external(0, 16, "s/external/rejected.bin"),
                external(0, 10, "s/external/committed.bin"),
                external(10, 20, "s/external/next.bin"),
            ],
        };
        let paths = objects_for_read(&page, 0, 20)
            .into_iter()
            .map(|object| object.s3_path)
            .collect::<Vec<_>>();
        assert_eq!(paths, vec![
            "s/external/committed.bin",
            "s/external/next.bin"
        ]);
    }

    #[test]
    fn reads_ignore_external_entries_overlapping_a_chunk_entry() {
        let mut page = page(0, 64);
        page.cold_chunks[0].end_offset = 32;
        page.cold_chunks[0].start_offset = 8;
        page.cold_chunks.insert(0, ColdChunkRef {
            start_offset: 0,
            end_offset: 8,
            s3_path: "s/chunks/head.bin".to_owned(),
            object_size: 8,
            ..Default::default()
        });
        page.external_segments = vec![
            external(0, 20, "s/external/stale.bin"),
            external(32, 40, "s/external/live.bin"),
        ];
        let objects = objects_for_read(&page, 0, 40);
        assert!(
            objects
                .iter()
                .all(|object| object.s3_path != "s/external/stale.bin"),
            "{objects:?}"
        );
        assert!(objects_cover_range(&objects, 0, 40));
    }

    /// A follower's cached copy of a page that repair rewrote on the leader
    /// serves the same objects as the repaired page.
    #[test]
    fn stale_cached_page_reads_like_its_repaired_copy() {
        let mut stale = page(0, 64);
        stale.cold_chunks[0].end_offset = 16;
        stale.external_segments = vec![
            external(8, 24, "s/external/overlaps-chunk.bin"),
            external(16, 32, "s/external/rejected.bin"),
            external(16, 32, "s/external/committed.bin"),
        ];
        let mut repaired = stale.clone();
        let input = ColdIndexRepairInput {
            stream_id: BucketStreamId::new("benchcmp", "cold-index"),
            generation: 7,
            retained_offset: 0,
            tail_offset: 32,
            created_at_ms: 0,
            hot_ranges: Vec::new(),
            state_refs: Vec::new(),
        };
        assert!(repair_cold_index_page(&mut repaired, &input).pages_rewritten > 0);
        assert_eq!(
            objects_for_read(&stale, 0, 32),
            objects_for_read(&repaired, 0, 32)
        );
    }
}
