use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use ursula_shard::BucketStreamId;
use ursula_stream::{
    ColdChunkRef, ExternalPayloadRef, ObjectPayloadRef, StreamReadColdIndexSegment,
};

use crate::cold_store::ColdStoreHandle;

pub type ColdIndexPageStoreFuture<'a, T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send + 'a>>;

const COLD_INDEX_PAGE_MAGIC: &[u8; 8] = b"UCIDX001";
const COLD_INDEX_PAGE_VERSION: u16 = 1;
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
            "{}/{}/cold-index/{:020}/{:020}.idx",
            self.stream_id.bucket_id, self.stream_id.stream_id, self.generation, self.page_id
        )
    }
}

pub fn cold_index_prefix(stream_id: &BucketStreamId) -> String {
    format!(
        "{}/{}/cold-index/",
        stream_id.bucket_id, stream_id.stream_id
    )
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
}

fn encode_page(key: &ColdIndexPageKey, page: &ColdIndexPage) -> Vec<u8> {
    let mut body = Vec::new();
    put_string(&mut body, &key.stream_id.bucket_id);
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

pub async fn write_cold_chunk_index_pages<S: ColdIndexPageStore + ?Sized>(
    store: &S,
    stream_id: &BucketStreamId,
    chunk: &ColdChunkRef,
) -> io::Result<()> {
    write_cold_chunk_index_pages_with_rollback(store, stream_id, chunk)
        .await
        .map(|_| ())
}

pub async fn write_cold_chunk_index_pages_with_rollback<S: ColdIndexPageStore + ?Sized>(
    store: &S,
    stream_id: &BucketStreamId,
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
            generation: 0,
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
        rollback.push(ColdIndexPageRollback {
            key: key.clone(),
            previous,
            written_chunk: chunk.clone(),
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

pub async fn write_external_segment_index_pages<S: ColdIndexPageStore + ?Sized>(
    store: &S,
    stream_id: &BucketStreamId,
    start_offset: u64,
    payload: &ExternalPayloadRef,
) -> io::Result<()> {
    let object = ObjectPayloadRef {
        start_offset,
        end_offset: start_offset.saturating_add(payload.payload_len),
        s3_path: payload.s3_path.clone(),
        object_size: payload.object_size,
    };
    write_object_index_pages(store, stream_id, object).await
}

async fn write_object_index_pages<S: ColdIndexPageStore + ?Sized>(
    store: &S,
    stream_id: &BucketStreamId,
    object: ObjectPayloadRef,
) -> io::Result<()> {
    if object.end_offset <= object.start_offset {
        return Ok(());
    }
    let first_page_id = object.start_offset / ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES;
    let last_page_id = (object.end_offset - 1) / ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES;
    for page_id in first_page_id..=last_page_id {
        let key = ColdIndexPageKey {
            stream_id: stream_id.clone(),
            generation: 0,
            page_id,
        };
        let page_start = page_id.saturating_mul(ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES);
        let page_end = page_start.saturating_add(ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES);
        let mut page = store
            .get_page(&key)
            .await?
            .unwrap_or_else(|| ColdIndexPage {
                start_offset: page_start,
                end_offset: page_end,
                cold_chunks: Vec::new(),
                external_segments: Vec::new(),
            });
        page.external_segments.retain(|existing| {
            existing.start_offset != object.start_offset || existing.end_offset != object.end_offset
        });
        page.external_segments.push(object.clone());
        page.external_segments
            .sort_by_key(|object| object.start_offset);
        store.put_page(&key, &page).await?;
    }
    Ok(())
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

#[derive(Debug)]
pub struct ColdIndexPageCache<S: ColdIndexPageStore + ?Sized> {
    store: Arc<S>,
    capacity_pages: usize,
    inner: Mutex<ColdIndexPageCacheInner>,
}

#[derive(Debug, Default)]
struct ColdIndexPageCacheInner {
    next_generation: u64,
    pages: HashMap<ColdIndexPageKey, ColdIndexPageCacheEntry>,
    lru: VecDeque<(ColdIndexPageKey, u64)>,
}

#[derive(Debug)]
struct ColdIndexPageCacheEntry {
    page: Arc<ColdIndexPage>,
    generation: u64,
}

impl<S: ColdIndexPageStore + ?Sized> ColdIndexPageCache<S> {
    pub fn new(store: Arc<S>, capacity_pages: usize) -> Self {
        Self {
            store,
            capacity_pages,
            inner: Mutex::new(ColdIndexPageCacheInner::default()),
        }
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
        let Some(page) = self.store.get_page(key).await? else {
            return Ok(None);
        };
        let page = Arc::new(page);
        self.insert(key.clone(), page.clone());
        Ok(Some(page))
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
        let mut objects = Vec::new();
        for chunk in &page.cold_chunks {
            if let Some(object) = intersect_object(
                &ObjectPayloadRef::from(chunk),
                segment.read_start_offset,
                read_end,
            ) {
                objects.push(object);
            }
        }
        for object in &page.external_segments {
            if let Some(object) = intersect_object(object, segment.read_start_offset, read_end) {
                objects.push(object);
            }
        }
        objects.sort_by_key(|object| object.start_offset);
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
        Some(page)
    }

    fn insert(&self, key: ColdIndexPageKey, page: Arc<ColdIndexPage>) {
        let mut inner = self
            .inner
            .lock()
            .expect("cold index page cache mutex poisoned");
        let generation = Self::touch(&mut inner, key.clone());
        inner
            .pages
            .insert(key, ColdIndexPageCacheEntry { page, generation });
        Self::evict_over_capacity(&mut inner, self.capacity_pages);
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

    fn evict_over_capacity(inner: &mut ColdIndexPageCacheInner, capacity_pages: usize) {
        if capacity_pages == 0 {
            inner.pages.clear();
            inner.lru.clear();
            return;
        }
        while inner.pages.len() > capacity_pages {
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
            inner.pages.remove(&key);
        }
    }
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
            }],
            external_segments: Vec::new(),
        }
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
        };
        let stale = ColdChunkRef {
            start_offset: 0,
            end_offset: 128,
            s3_path: "benchcmp/cold-index/chunks/stale.bin".to_owned(),
            object_size: 128,
        };
        let newer = ColdChunkRef {
            start_offset: 0,
            end_offset: 128,
            s3_path: "benchcmp/cold-index/chunks/newer.bin".to_owned(),
            object_size: 128,
        };
        write_cold_chunk_index_pages(&store, &stream_id, &first)
            .await
            .expect("write first chunk");
        let rollback = write_cold_chunk_index_pages_with_rollback(&store, &stream_id, &stale)
            .await
            .expect("write stale chunk");
        write_cold_chunk_index_pages(&store, &stream_id, &newer)
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

    #[test]
    fn binary_page_format_round_trips_and_validates() {
        let key = key(42);
        let mut page = page(128, 256);
        page.external_segments.push(ObjectPayloadRef {
            start_offset: 256,
            end_offset: 300,
            s3_path: "benchcmp/cold-index/external/00000000000000000256.bin".to_owned(),
            object_size: 44,
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
}
