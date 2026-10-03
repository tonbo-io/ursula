//! Keyed part v2 (design §6.1 U13): one immutable, key-sorted Parquet file.
//!
//! Columns: `key` (Binary, strictly ascending), `record` (UInt64), `del`
//! (Boolean, a point tombstone) and `value` (Binary JSON text, null exactly
//! when `del`). ZSTD, page statistics and the page index are always written.
//!
//! The footer's key-value metadata carries the part's range tombstones
//! (`ursula.keyed.tombstones`, `[[start,end,record],…]` with base64url keys)
//! and its verified-read layout (`ursula.keyed.layout`), so no separate
//! layout object exists. The layout hashes the data region `[0, data_bytes)`
//! in blocks; the manifest pins the tail `[data_bytes, bytes)` (page index
//! and footer) by its blake3 digest. A reader therefore verifies the tail
//! against the manifest, then every data block against the tail.
//!
//! Readers push a lower bound and an exclusive upper bound down to the page
//! index of `key`, and decode lazily in small batches, so `after`/`limit`
//! reads touch only the pages they return.
//!
//! A part's footer (Parquet metadata with its page index, the range
//! tombstones and, for the object-store reader, the verified tail and block
//! layout) is decoded and verified once and shared by every later read of
//! the part ([`PartFooter`], [`FooterCache`]), so a point read costs the
//! same whatever the size of the part or the namespace (design §9.3).

use std::collections::HashMap;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::BinaryArray;
use arrow_array::BooleanArray;
use arrow_array::RecordBatch;
use arrow_array::UInt64Array;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::basic::Compression;
use parquet::basic::ZstdLevel;
use parquet::errors::ParquetError;
use parquet::file::metadata::KeyValue;
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;
use serde::Deserialize;
use serde::Serialize;

use super::batch::decode_key;
use super::batch::encode_key;
use super::manifest::KeyedPartMeta;
use crate::EventIndexCache;
use crate::IndexError;
use crate::cache::VerifiedParquetReader;
use crate::cache::VerifiedRangeCache;
use crate::object_store::ObjectStore;
use crate::object_store::digest;
use crate::part::PartLayout;
use crate::part::PartUnit;

/// Footer key of the range tombstones.
pub const TOMBSTONES_METADATA_KEY: &str = "ursula.keyed.tombstones";
/// Footer key of the verified-read layout of the data region.
pub const LAYOUT_METADATA_KEY: &str = "ursula.keyed.layout";
/// Version of the footer layout document.
const LAYOUT_VERSION: u32 = 1;
/// Parquet footer: 4-byte metadata length plus the `PAR1` magic.
const FOOTER_SIZE: usize = 8;

/// One stored row of a part or run: a put, or a point tombstone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyedEntry {
    /// Key octets.
    pub key: Vec<u8>,
    /// Ordinal of the record holding the op.
    pub record: u64,
    /// The put's stored JSON text; `None` for a point tombstone (`del`).
    pub value: Option<String>,
}

impl KeyedEntry {
    pub(crate) fn weight(&self) -> usize {
        self.key
            .len()
            .saturating_add(self.value.as_ref().map_or(0, String::len))
            .saturating_add(16)
    }
}

/// A range tombstone: deletes keys in `[start, end)` of records `< record`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RangeTombstone {
    /// Inclusive lower bound.
    pub start: Vec<u8>,
    /// Exclusive upper bound, strictly above `start`.
    pub end: Vec<u8>,
    /// Ordinal of the record holding the range delete.
    pub record: u64,
}

impl RangeTombstone {
    /// Whether the tombstone covers `key`.
    pub fn covers(&self, key: &[u8]) -> bool {
        self.start.as_slice() <= key && key < self.end.as_slice()
    }
}

/// Coalesces the tombstones of one run: overlapping or touching ranges merge
/// and keep the smallest record.
///
/// Within a run no entry is shadowed by the run's own tombstones (the fold
/// or merge that produced the run dropped those), and every tombstone's
/// record is at least the run's first record, so `min` keeps both facts: a
/// coalesced tombstone still deletes every older run's rows in its range and
/// none of the run's own.
pub fn coalesce_tombstones(mut tombstones: Vec<RangeTombstone>) -> Vec<RangeTombstone> {
    tombstones.sort_unstable();
    let mut out: Vec<RangeTombstone> = Vec::with_capacity(tombstones.len());
    for tombstone in tombstones {
        if let Some(last) = out.last_mut()
            && tombstone.start <= last.end
        {
            if tombstone.end > last.end {
                last.end = tombstone.end;
            }
            last.record = last.record.min(tombstone.record);
            continue;
        }
        out.push(tombstone);
    }
    out
}

/// Encoding knobs of a part.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartOptions {
    /// Rows per data page (the page-index granularity of pushdown).
    pub data_page_rows: usize,
    /// Rows per row group.
    pub row_group_rows: usize,
    /// Block size of the verified-read layout of the data region.
    pub layout_block_bytes: u64,
    /// A run is cut into a new part once a part's estimated raw size
    /// (keys, values and fixed per-row overhead) reaches this.
    pub target_part_bytes: usize,
    /// Rows decoded per batch by readers.
    pub read_batch_rows: usize,
}

impl Default for PartOptions {
    fn default() -> Self {
        Self {
            data_page_rows: 64,
            row_group_rows: 16 * 1024,
            layout_block_bytes: 64 * 1024,
            target_part_bytes: 32 * 1024 * 1024,
            read_batch_rows: 64,
        }
    }
}

/// An encoded part ready to be stored under `meta.key`.
#[derive(Clone, Debug)]
pub struct EncodedPart {
    /// The part's manifest entry.
    pub meta: KeyedPartMeta,
    /// The Parquet file.
    pub bytes: Bytes,
}

#[derive(Debug, Deserialize, Serialize)]
struct FooterLayout {
    version: u32,
    data_bytes: u64,
    /// `[start, end, blake3]` per block, contiguous over `[0, data_bytes)`.
    units: Vec<(u64, u64, String)>,
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Binary, false),
        Field::new("record", DataType::UInt64, false),
        Field::new("del", DataType::Boolean, false),
        Field::new("value", DataType::Binary, true),
    ]))
}

fn invalid(message: impl Into<String>) -> IndexError {
    IndexError::InvalidKeyedState(message.into())
}

/// A fresh object key for a part, relative to its namespace:
/// `parts/{blake3}-{nonce}.parquet`. The nonce makes every encoded part's key
/// unique, so a key is never reused after a deleter could have scheduled it
/// (`manifest` module docs); readers verify the bytes against the manifest
/// entry, not the key.
pub fn part_object_key(bytes: &[u8]) -> String {
    format!(
        "parts/{}-{}.parquet",
        digest(bytes),
        super::manifest::unique_object_nonce()
    )
}

/// Encodes one part. `entries` must be strictly ascending by key, and
/// `tombstones` well-formed (`start < end`); at least one of them must be
/// non-empty. The manifest key range covers row keys and tombstone
/// endpoints.
pub fn encode_part(
    entries: &[KeyedEntry],
    tombstones: &[RangeTombstone],
    options: &PartOptions,
) -> Result<EncodedPart, IndexError> {
    if entries.windows(2).any(|pair| match pair {
        [a, b] => a.key >= b.key,
        _ => false,
    }) {
        return Err(invalid("part entries are not strictly ascending"));
    }
    if tombstones.iter().any(|t| t.start >= t.end) {
        return Err(invalid("part has an empty range tombstone"));
    }
    let row_min = entries.first().map(|entry| entry.key.as_slice());
    let row_max = entries.last().map(|entry| entry.key.as_slice());
    let tomb_min = tombstones.iter().map(|t| t.start.as_slice()).min();
    let tomb_max = tombstones.iter().map(|t| t.end.as_slice()).max();
    let min_key = match (row_min, tomb_min) {
        (Some(a), Some(b)) => a.min(b),
        (Some(a), None) | (None, Some(a)) => a,
        (None, None) => return Err(invalid("a part needs a row or a tombstone")),
    }
    .to_vec();
    let max_key = match (row_max, tomb_max) {
        (Some(a), Some(b)) => a.max(b),
        (Some(a), None) | (None, Some(a)) => a,
        (None, None) => return Err(invalid("a part needs a row or a tombstone")),
    }
    .to_vec();

    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_dictionary_enabled(false)
        .set_data_page_row_count_limit(options.data_page_rows.max(1))
        .set_write_batch_size(options.data_page_rows.max(1))
        .set_max_row_group_row_count(Some(options.row_group_rows.max(1)))
        .build();
    let mut writer = ArrowWriter::try_new(Vec::<u8>::new(), schema(), Some(properties))?;
    if !entries.is_empty() {
        let keys = BinaryArray::from_iter_values(entries.iter().map(|entry| entry.key.as_slice()));
        let records = UInt64Array::from_iter_values(entries.iter().map(|entry| entry.record));
        let dels = BooleanArray::from(
            entries
                .iter()
                .map(|entry| entry.value.is_none())
                .collect::<Vec<_>>(),
        );
        let values = BinaryArray::from(
            entries
                .iter()
                .map(|entry| entry.value.as_deref().map(str::as_bytes))
                .collect::<Vec<_>>(),
        );
        let batch = RecordBatch::try_new(schema(), vec![
            Arc::new(keys),
            Arc::new(records),
            Arc::new(dels),
            Arc::new(values),
        ])?;
        writer.write(&batch)?;
    }
    writer.flush()?;
    writer.sync()?;
    let data_len = writer.bytes_written();
    let data = writer.inner();
    if data.len() != data_len {
        return Err(invalid("parquet writer did not flush its data region"));
    }
    let data_bytes = u64::try_from(data_len).map_err(|_error| invalid("part is too large"))?;
    let block = usize::try_from(options.layout_block_bytes.max(1))
        .map_err(|_error| invalid("layout block is too large"))?;
    let mut units = Vec::new();
    let mut start = 0_usize;
    for chunk in data.chunks(block) {
        let end = start.saturating_add(chunk.len());
        units.push((
            u64::try_from(start).map_err(|_error| invalid("part is too large"))?,
            u64::try_from(end).map_err(|_error| invalid("part is too large"))?,
            digest(chunk),
        ));
        start = end;
    }
    let layout = FooterLayout {
        version: LAYOUT_VERSION,
        data_bytes,
        units,
    };
    let encoded_tombstones: Vec<(String, String, u64)> = tombstones
        .iter()
        .map(|t| (encode_key(&t.start), encode_key(&t.end), t.record))
        .collect();
    writer.append_key_value_metadata(KeyValue::new(
        TOMBSTONES_METADATA_KEY.to_owned(),
        serde_json::to_string(&encoded_tombstones)?,
    ));
    writer.append_key_value_metadata(KeyValue::new(
        LAYOUT_METADATA_KEY.to_owned(),
        serde_json::to_string(&layout)?,
    ));
    let bytes = Bytes::from(writer.into_inner()?);
    let tail = bytes
        .get(data_len..)
        .ok_or_else(|| invalid("part tail is missing"))?;
    let meta = KeyedPartMeta {
        key: part_object_key(&bytes),
        bytes: u64::try_from(bytes.len()).map_err(|_error| invalid("part is too large"))?,
        data_bytes,
        tail_hash: digest(tail),
        min_key,
        max_key,
        rows: u64::try_from(entries.len()).map_err(|_error| invalid("part is too large"))?,
        tombstones: u64::try_from(tombstones.len())
            .map_err(|_error| invalid("part is too large"))?,
    };
    Ok(EncodedPart { meta, bytes })
}

/// A part's decoded footer: its Parquet metadata (with the page index and
/// the Arrow schema) and its range tombstones. Decoded once per part and
/// shared by every read of it.
#[derive(Debug)]
pub struct PartFooter {
    metadata: ArrowReaderMetadata,
    tombstones: Vec<RangeTombstone>,
}

impl PartFooter {
    /// Decodes and checks the footer through `reader` (the page index is
    /// required).
    pub async fn load(reader: &mut Box<dyn AsyncFileReader>) -> Result<Self, IndexError> {
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let metadata = ArrowReaderMetadata::load_async(reader, options).await?;
        check_schema(metadata.schema())?;
        let tombstones = footer_tombstones(metadata.metadata())?;
        Ok(Self {
            metadata,
            tombstones,
        })
    }

    /// Approximate heap footprint, for cache weighting.
    fn weight(&self) -> usize {
        self.tombstones
            .iter()
            .map(|t| t.start.len().saturating_add(t.end.len()).saturating_add(64))
            .fold(
                self.metadata.metadata().memory_size(),
                usize::saturating_add,
            )
    }
}

/// A part opened for reading: a verified reader and its decoded footer.
pub struct OpenedPart {
    /// Reader over the part's bytes.
    pub reader: Box<dyn AsyncFileReader>,
    /// The part's footer.
    pub footer: Arc<PartFooter>,
}

impl OpenedPart {
    /// Opens `reader`, decoding its footer.
    pub async fn load(mut reader: Box<dyn AsyncFileReader>) -> Result<Self, IndexError> {
        let footer = Arc::new(PartFooter::load(&mut reader).await?);
        Ok(Self { reader, footer })
    }
}

/// Opens part files for readers. Implementations verify what they return.
pub trait PartOpener: Send + Sync {
    /// Returns a reader over the part's bytes and its decoded footer.
    fn open<'a>(&'a self, part: &'a KeyedPartMeta)
    -> BoxFuture<'a, Result<OpenedPart, IndexError>>;
}

/// Default capacity of a [`FooterCache`] made by an opener on its own.
pub const DEFAULT_FOOTER_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// A verified, decoded part tail as cached by the object-store reader.
#[derive(Debug)]
pub struct CachedTail {
    tail_hash: String,
    tail: Bytes,
    layout: Arc<PartLayout>,
    footer: Arc<PartFooter>,
}

impl CachedTail {
    fn weight(&self) -> usize {
        self.tail
            .len()
            .saturating_add(self.footer.weight())
            .saturating_add(self.layout.units.len().saturating_mul(96))
    }
}

/// Decoded part footers by object key, bounded by approximate bytes
/// (oldest inserted evicted first). Parts are immutable and
/// content-addressed, so an entry never goes stale; the manifest's tail
/// digest is still compared on every hit. One cache is shared by every
/// namespace of a pod. Deterministic (no hashing seeds, no clocks), so it
/// keeps simulation runs reproducible.
#[derive(Clone)]
pub struct FooterCache {
    capacity: usize,
    state: Arc<std::sync::Mutex<FooterCacheState>>,
}

#[derive(Default)]
struct FooterCacheState {
    entries: std::collections::BTreeMap<String, (Arc<CachedTail>, usize)>,
    order: VecDeque<String>,
    bytes: usize,
}

impl std::fmt::Debug for FooterCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FooterCache")
            .field("capacity", &self.capacity)
            .field("bytes", &self.lock().bytes)
            .finish_non_exhaustive()
    }
}

impl FooterCache {
    /// A cache holding about `capacity_bytes` of footers.
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity: capacity_bytes,
            state: Arc::new(std::sync::Mutex::new(FooterCacheState::default())),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FooterCacheState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn get(&self, object_key: &str, tail_hash: &str) -> Option<Arc<CachedTail>> {
        self.lock()
            .entries
            .get(object_key)
            .map(|(tail, _weight)| Arc::clone(tail))
            .filter(|tail| tail.tail_hash == tail_hash)
    }

    fn insert(&self, object_key: String, tail: Arc<CachedTail>) {
        let weight = object_key.len().saturating_add(tail.weight());
        if weight > self.capacity {
            return;
        }
        let mut state = self.lock();
        if let Some((_old, old_weight)) = state.entries.remove(&object_key) {
            state.bytes = state.bytes.saturating_sub(old_weight);
            state.order.retain(|key| *key != object_key);
        }
        state.bytes = state.bytes.saturating_add(weight);
        state.order.push_back(object_key.clone());
        state.entries.insert(object_key, (tail, weight));
        while state.bytes > self.capacity {
            let Some(oldest) = state.order.pop_front() else {
                break;
            };
            if let Some((_evicted, evicted_weight)) = state.entries.remove(&oldest) {
                state.bytes = state.bytes.saturating_sub(evicted_weight);
            }
        }
    }
}

/// Checks `bytes` (a whole part) against the manifest's size and tail
/// digest.
fn verify_whole_part(key: &str, bytes: &Bytes, part: &KeyedPartMeta) -> Result<(), IndexError> {
    let start = usize::try_from(part.data_bytes).map_err(|_error| invalid("part is too large"))?;
    let tail = bytes
        .get(start..)
        .ok_or_else(|| IndexError::PartSizeMismatch {
            file: key.to_owned(),
            expected: part.bytes,
            actual: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        })?;
    if u64::try_from(bytes.len()).ok() != Some(part.bytes) || digest(tail) != part.tail_hash {
        return Err(IndexError::ObjectHashMismatch(key.to_owned()));
    }
    Ok(())
}

/// A whole part held in memory with its footer decoded on first use (the
/// write cache and [`MemoryParts`]).
#[derive(Debug)]
pub(crate) struct ResidentPart {
    bytes: Bytes,
    /// Set once the bytes were verified against a manifest entry and the
    /// footer decoded; keyed by the tail digest it was verified against.
    footer: std::sync::OnceLock<(String, Arc<PartFooter>)>,
}

impl ResidentPart {
    pub(crate) fn new(bytes: Bytes) -> Self {
        Self {
            bytes,
            footer: std::sync::OnceLock::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Opens the part for `part`, verifying and decoding only on first use.
    pub(crate) async fn open(
        &self,
        key: &str,
        part: &KeyedPartMeta,
    ) -> Result<OpenedPart, IndexError> {
        let reader = Box::new(BytesReader(Bytes::clone(&self.bytes))) as Box<dyn AsyncFileReader>;
        if let Some((tail_hash, footer)) = self.footer.get()
            && *tail_hash == part.tail_hash
            && u64::try_from(self.bytes.len()).ok() == Some(part.bytes)
        {
            return Ok(OpenedPart {
                reader,
                footer: Arc::clone(footer),
            });
        }
        verify_whole_part(key, &self.bytes, part)?;
        let opened = OpenedPart::load(reader).await?;
        let _first = self
            .footer
            .set((part.tail_hash.clone(), Arc::clone(&opened.footer)));
        Ok(opened)
    }
}

/// An in-memory part source: parts freshly written (a cache filled on
/// write) or test fixtures. Bytes are checked against the manifest's size
/// and tail digest.
#[derive(Clone, Debug, Default)]
pub struct MemoryParts {
    parts: HashMap<String, Arc<ResidentPart>>,
}

impl MemoryParts {
    /// An empty source.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a part.
    pub fn insert(&mut self, part: &EncodedPart) {
        self.parts.insert(
            part.meta.key.clone(),
            Arc::new(ResidentPart::new(Bytes::clone(&part.bytes))),
        );
    }

    /// Removes a part.
    pub fn remove(&mut self, key: &str) {
        self.parts.remove(key);
    }
}

impl PartOpener for MemoryParts {
    fn open<'a>(
        &'a self,
        part: &'a KeyedPartMeta,
    ) -> BoxFuture<'a, Result<OpenedPart, IndexError>> {
        async move {
            let resident = self
                .parts
                .get(&part.key)
                .ok_or_else(|| IndexError::MissingObject(part.key.clone()))?;
            resident.open(&part.key, part).await
        }
        .boxed()
    }
}

/// Reads parts from an object-store namespace, verifying the tail against
/// the manifest and every data block against the footer layout. With a
/// verified range cache, blocks are served through it.
#[derive(Clone)]
pub struct StorePartOpener {
    store: ObjectStore,
    prefix: String,
    ranges: Option<VerifiedRangeCache>,
    footers: FooterCache,
}

impl StorePartOpener {
    /// A reader over `prefix` (the namespace, ending in `/`) of `store`.
    pub fn new(store: ObjectStore, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
            ranges: None,
            footers: FooterCache::new(DEFAULT_FOOTER_CACHE_BYTES),
        }
    }

    /// Keeps decoded footers in `footers` (shared across namespaces).
    #[must_use]
    pub fn with_footer_cache(mut self, footers: FooterCache) -> Self {
        self.footers = footers;
        self
    }

    /// Serves blocks through a serving cache's verified range cache.
    pub fn with_cache(mut self, cache: &EventIndexCache) -> Result<Self, IndexError> {
        self.ranges = Some(cache.0.ranges()?.clone());
        Ok(self)
    }
}

impl StorePartOpener {
    /// A verified reader over the part, given its verified tail and layout.
    fn reader(&self, tail: &CachedTail) -> Box<dyn AsyncFileReader> {
        match &self.ranges {
            Some(ranges) => Box::new(VerifiedParquetReader::new(
                self.store.clone(),
                ranges.clone(),
                Arc::clone(&tail.layout),
            )),
            None => Box::new(UncachedVerifiedReader {
                store: self.store.clone(),
                layout: Arc::clone(&tail.layout),
                tail_start: tail.layout.units.last().map_or(0, |unit| unit.start),
                tail: Bytes::clone(&tail.tail),
            }),
        }
    }

    /// Fetches and verifies the tail of `part`, then decodes its layout and
    /// footer.
    async fn load_tail(
        &self,
        object_key: &str,
        part: &KeyedPartMeta,
    ) -> Result<CachedTail, IndexError> {
        let tail = self
            .store
            .get_range(object_key, part.data_bytes..part.bytes)
            .await?
            .ok_or_else(|| IndexError::MissingObject(object_key.to_owned()))?;
        if digest(&tail) != part.tail_hash {
            return Err(IndexError::ObjectHashMismatch(object_key.to_owned()));
        }
        let layout = footer_layout(&tail, part)
            .map_err(|_error| IndexError::InvalidPartLayout(object_key.to_owned()))?;
        let mut units: Vec<PartUnit> = layout
            .units
            .into_iter()
            .map(|(start, end, hash)| PartUnit { start, end, hash })
            .collect();
        units.push(PartUnit {
            start: part.data_bytes,
            end: part.bytes,
            hash: part.tail_hash.clone(),
        });
        let layout = Arc::new(PartLayout {
            version: 1,
            part_key: object_key.to_owned(),
            bytes: part.bytes,
            units,
        });
        let tail = Bytes::from(tail);
        let mut reader: Box<dyn AsyncFileReader> = Box::new(UncachedVerifiedReader {
            store: self.store.clone(),
            layout: Arc::clone(&layout),
            tail_start: part.data_bytes,
            tail: Bytes::clone(&tail),
        });
        let footer = Arc::new(PartFooter::load(&mut reader).await?);
        Ok(CachedTail {
            tail_hash: part.tail_hash.clone(),
            tail,
            layout,
            footer,
        })
    }
}

impl PartOpener for StorePartOpener {
    fn open<'a>(
        &'a self,
        part: &'a KeyedPartMeta,
    ) -> BoxFuture<'a, Result<OpenedPart, IndexError>> {
        async move {
            let object_key = format!("{}{}", self.prefix, part.key);
            if part.data_bytes >= part.bytes {
                return Err(IndexError::InvalidPartLayout(object_key));
            }
            let tail = match self.footers.get(&object_key, &part.tail_hash) {
                Some(tail) if tail.layout.bytes == part.bytes => tail,
                _ => {
                    let tail = Arc::new(self.load_tail(&object_key, part).await?);
                    self.footers.insert(object_key, Arc::clone(&tail));
                    tail
                }
            };
            Ok(OpenedPart {
                reader: self.reader(&tail),
                footer: Arc::clone(&tail.footer),
            })
        }
        .boxed()
    }
}

/// Decodes and checks the footer layout from a verified tail.
fn footer_layout(tail: &[u8], part: &KeyedPartMeta) -> Result<FooterLayout, IndexError> {
    let metadata = decode_tail_metadata(tail)?;
    let text = footer_value(&metadata, LAYOUT_METADATA_KEY)?
        .ok_or_else(|| invalid("part footer has no layout"))?;
    let layout: FooterLayout = serde_json::from_str(text)?;
    if layout.version != LAYOUT_VERSION || layout.data_bytes != part.data_bytes {
        return Err(invalid("part footer layout does not match the manifest"));
    }
    let mut cursor = 0_u64;
    for (start, end, _hash) in &layout.units {
        if *start != cursor || start >= end {
            return Err(invalid("part footer layout is not contiguous"));
        }
        cursor = *end;
    }
    if cursor != part.data_bytes {
        return Err(invalid("part footer layout does not cover the data region"));
    }
    Ok(layout)
}

fn decode_tail_metadata(tail: &[u8]) -> Result<ParquetMetaData, IndexError> {
    let split = tail
        .len()
        .checked_sub(FOOTER_SIZE)
        .ok_or_else(|| invalid("part tail is shorter than a footer"))?;
    let (body, footer) = tail.split_at(split);
    let length_bytes: [u8; 4] = footer
        .get(..4)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| invalid("part footer is truncated"))?;
    if footer.get(4..) != Some(b"PAR1".as_slice()) {
        return Err(invalid("part footer has no PAR1 magic"));
    }
    let length = usize::try_from(u32::from_le_bytes(length_bytes))
        .map_err(|_error| invalid("part footer is too large"))?;
    let start = body
        .len()
        .checked_sub(length)
        .ok_or_else(|| invalid("part footer is longer than its tail"))?;
    let metadata = body
        .get(start..)
        .ok_or_else(|| invalid("part footer is longer than its tail"))?;
    Ok(ParquetMetaDataReader::decode_metadata(metadata)?)
}

fn footer_value<'m>(
    metadata: &'m ParquetMetaData,
    key: &str,
) -> Result<Option<&'m str>, IndexError> {
    let Some(pairs) = metadata.file_metadata().key_value_metadata() else {
        return Ok(None);
    };
    let mut found = None;
    for pair in pairs.iter().filter(|pair| pair.key == key) {
        if found.is_some() {
            return Err(invalid(format!("part footer repeats `{key}`")));
        }
        found = pair.value.as_deref();
    }
    Ok(found)
}

fn footer_tombstones(metadata: &ParquetMetaData) -> Result<Vec<RangeTombstone>, IndexError> {
    let Some(text) = footer_value(metadata, TOMBSTONES_METADATA_KEY)? else {
        return Err(invalid("part footer has no tombstone list"));
    };
    let raw: Vec<(String, String, u64)> = serde_json::from_str(text)?;
    raw.into_iter()
        .map(|(start, end, record)| {
            let start = decode_key(&start).map_err(|error| invalid(error.to_string()))?;
            let end = decode_key(&end).map_err(|error| invalid(error.to_string()))?;
            if start >= end {
                return Err(invalid("part footer has an empty range tombstone"));
            }
            Ok(RangeTombstone { start, end, record })
        })
        .collect()
}

/// An [`AsyncFileReader`] over bytes held in memory.
#[derive(Clone, Debug)]
pub struct BytesReader(pub Bytes);

impl BytesReader {
    fn slice(&self, range: Range<u64>) -> parquet::errors::Result<Bytes> {
        let start = usize::try_from(range.start)
            .map_err(|_error| ParquetError::General("range start overflows".to_owned()))?;
        let end = usize::try_from(range.end)
            .map_err(|_error| ParquetError::General("range end overflows".to_owned()))?;
        if start > end || end > self.0.len() {
            return Err(ParquetError::EOF(format!(
                "range {start}..{end} outside a {}-byte part",
                self.0.len()
            )));
        }
        Ok(self.0.slice(start..end))
    }
}

impl AsyncFileReader for BytesReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let result = self.slice(range);
        async move { result }.boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let file_bytes = u64::try_from(self.0.len())
                .map_err(|_error| ParquetError::General("part is too large".to_owned()))?;
            load_metadata(self, options, file_bytes).await
        }
        .boxed()
    }
}

async fn load_metadata<R: AsyncFileReader>(
    reader: &mut R,
    options: Option<&ArrowReaderOptions>,
    file_bytes: u64,
) -> parquet::errors::Result<Arc<ParquetMetaData>> {
    let metadata_options = options.map(|options| options.metadata_options().clone());
    let mut metadata_reader = ParquetMetaDataReader::new().with_metadata_options(metadata_options);
    if let Some(options) = options {
        metadata_reader = metadata_reader
            .with_column_index_policy(options.column_index_policy())
            .with_offset_index_policy(options.offset_index_policy());
    }
    metadata_reader
        .load_and_finish(reader, file_bytes)
        .await
        .map(Arc::new)
}

/// Verified reads without a cache (maintenance and compaction): each block
/// is fetched and checked against the footer layout; the tail is the bytes
/// already verified against the manifest.
struct UncachedVerifiedReader {
    store: ObjectStore,
    layout: Arc<PartLayout>,
    tail_start: u64,
    tail: Bytes,
}

impl UncachedVerifiedReader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes, IndexError> {
        let key = &self.layout.part_key;
        if range.start > range.end || range.end > self.layout.bytes {
            return Err(IndexError::InvalidPartLayout(key.clone()));
        }
        let mut pieces: Vec<Bytes> = Vec::new();
        for unit in self
            .layout
            .units
            .iter()
            .filter(|unit| unit.end > range.start && unit.start < range.end)
        {
            let bytes = if unit.start == self.tail_start {
                Bytes::clone(&self.tail)
            } else {
                let bytes = self
                    .store
                    .get_range(key, unit.start..unit.end)
                    .await?
                    .ok_or_else(|| IndexError::MissingObject(key.clone()))?;
                if digest(&bytes) != unit.hash {
                    return Err(IndexError::ObjectHashMismatch(key.clone()));
                }
                Bytes::from(bytes)
            };
            let from = usize::try_from(range.start.saturating_sub(unit.start))
                .map_err(|_error| IndexError::InvalidPartLayout(key.clone()))?;
            let to = usize::try_from(range.end.min(unit.end).saturating_sub(unit.start))
                .map_err(|_error| IndexError::InvalidPartLayout(key.clone()))?;
            if from > to || to > bytes.len() {
                return Err(IndexError::InvalidPartLayout(key.clone()));
            }
            pieces.push(bytes.slice(from..to));
        }
        Ok(join_pieces(pieces))
    }
}

impl AsyncFileReader for UncachedVerifiedReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            self.read(range)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let file_bytes = self.layout.bytes;
            load_metadata(self, options, file_bytes).await
        }
        .boxed()
    }
}

/// Concatenates verified block slices; one slice (the common case of a
/// page within one block) is returned without copying.
pub(crate) fn join_pieces(mut pieces: Vec<Bytes>) -> Bytes {
    if pieces.len() == 1 {
        return pieces.pop().unwrap_or_default();
    }
    let mut out = Vec::with_capacity(pieces.iter().map(Bytes::len).sum());
    for piece in &pieces {
        out.extend_from_slice(piece);
    }
    Bytes::from(out)
}

/// A lazily decoded scan of one part's rows within `[from, end)`.
pub struct PartScan {
    stream: Option<ParquetRecordBatchStream<Box<dyn AsyncFileReader>>>,
    buffered: VecDeque<KeyedEntry>,
    from: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    done: bool,
}

/// Opens a part for a scan of keys `>= from` and `< end`. Returns the scan
/// and the part's range tombstones. Pages of `key` entirely below `from` or
/// at or above `end` are skipped through the page index.
pub async fn open_part(
    opened: OpenedPart,
    from: Option<&[u8]>,
    end: Option<&[u8]>,
    options: &PartOptions,
) -> Result<(PartScan, Vec<RangeTombstone>), IndexError> {
    let OpenedPart { reader, footer } = opened;
    let builder =
        ParquetRecordBatchStreamBuilder::new_with_metadata(reader, footer.metadata.clone());
    let tombstones = footer.tombstones.clone();
    let selection = select_pages(footer.metadata.metadata(), from, end)?;
    let empty = !selection.selects_any();
    let stream = if empty {
        None
    } else {
        Some(
            builder
                .with_row_selection(selection)
                .with_batch_size(options.read_batch_rows.max(1))
                .build()?,
        )
    };
    Ok((
        PartScan {
            stream,
            buffered: VecDeque::new(),
            from: from.map(<[u8]>::to_vec),
            end: end.map(<[u8]>::to_vec),
            done: empty,
        },
        tombstones,
    ))
}

fn check_schema(schema: &Schema) -> Result<(), IndexError> {
    let expected = self::schema();
    let fields = schema.fields();
    if fields.len() != expected.fields().len()
        || fields.iter().zip(expected.fields()).any(|(field, want)| {
            field.name() != want.name() || field.data_type() != want.data_type()
        })
    {
        return Err(IndexError::InvalidPartSchema);
    }
    Ok(())
}

/// Row selection from the page index of `key`: a page is read unless its
/// maximum is below `from` or its minimum is at or above `end`. Truncated
/// statistics stay valid bounds (min rounds down, max rounds up).
fn select_pages(
    metadata: &ParquetMetaData,
    from: Option<&[u8]>,
    end: Option<&[u8]>,
) -> Result<RowSelection, IndexError> {
    if metadata.row_groups().is_empty() {
        // A tombstone-only part.
        return Ok(RowSelection::from(Vec::new()));
    }
    let offsets = metadata
        .offset_index()
        .ok_or_else(|| invalid("part has no offset index"))?;
    let columns = metadata.column_index();
    let mut ranges = Vec::new();
    let mut base = 0_usize;
    for (group_index, group) in metadata.row_groups().iter().enumerate() {
        let group_rows =
            usize::try_from(group.num_rows()).map_err(|_error| invalid("row group overflows"))?;
        let locations = offsets
            .get(group_index)
            .and_then(|columns| columns.first())
            .map(|index| index.page_locations())
            .ok_or_else(|| invalid("part has no offset index for `key`"))?;
        let stats = columns
            .and_then(|columns| columns.get(group_index))
            .and_then(|columns| columns.first());
        for (page, location) in locations.iter().enumerate() {
            let first = usize::try_from(location.first_row_index)
                .map_err(|_error| invalid("page row index overflows"))?;
            let next = match locations.get(page.saturating_add(1)) {
                Some(next) => usize::try_from(next.first_row_index)
                    .map_err(|_error| invalid("page row index overflows"))?,
                None => group_rows,
            };
            let keep = match stats {
                Some(ColumnIndexMetaData::BYTE_ARRAY(index)) => {
                    let below = match (from, index.max_value(page)) {
                        (Some(from), Some(max)) => max < from,
                        _ => false,
                    };
                    let above = match (end, index.min_value(page)) {
                        (Some(end), Some(min)) => min >= end,
                        _ => false,
                    };
                    !below && !above
                }
                _ => true,
            };
            if keep && first < next {
                ranges.push(base.saturating_add(first)..base.saturating_add(next));
            }
        }
        base = base.saturating_add(group_rows);
    }
    Ok(RowSelection::from_consecutive_ranges(
        ranges.into_iter(),
        base,
    ))
}

impl PartScan {
    /// The next row in range, decoding another batch when needed.
    pub async fn next(&mut self) -> Result<Option<KeyedEntry>, IndexError> {
        loop {
            if let Some(entry) = self.buffered.pop_front() {
                return Ok(Some(entry));
            }
            if self.done {
                return Ok(None);
            }
            let Some(stream) = self.stream.as_mut() else {
                self.done = true;
                return Ok(None);
            };
            match stream.next().await {
                Some(batch) => self.decode(&batch?)?,
                None => self.done = true,
            }
        }
    }

    fn decode(&mut self, batch: &RecordBatch) -> Result<(), IndexError> {
        let keys = column::<BinaryArray>(batch, 0, "key")?;
        let records = column::<UInt64Array>(batch, 1, "record")?;
        let dels = column::<BooleanArray>(batch, 2, "del")?;
        let values = column::<BinaryArray>(batch, 3, "value")?;
        for (((key, record), del), value) in keys
            .iter()
            .zip(records.iter())
            .zip(dels.iter())
            .zip(values.iter())
        {
            let (Some(key), Some(record), Some(del)) = (key, record, del) else {
                return Err(invalid("part has a null in a required column"));
            };
            if self.from.as_deref().is_some_and(|from| key < from) {
                continue;
            }
            if self.end.as_deref().is_some_and(|end| key >= end) {
                self.done = true;
                self.stream = None;
                break;
            }
            let value = match (del, value) {
                (true, None) => None,
                (false, Some(value)) => Some(
                    std::str::from_utf8(value)
                        .map_err(|_error| invalid("part value is not UTF-8"))?
                        .to_owned(),
                ),
                _ => return Err(invalid("part row's `del` and `value` disagree")),
            };
            self.buffered.push_back(KeyedEntry {
                key: key.to_vec(),
                record,
                value,
            });
        }
        Ok(())
    }
}

fn column<'b, T: 'static>(
    batch: &'b RecordBatch,
    index: usize,
    name: &str,
) -> Result<&'b T, IndexError> {
    batch
        .columns()
        .get(index)
        .and_then(|column| column.as_any().downcast_ref::<T>())
        .ok_or_else(|| invalid(format!("part column `{name}` has the wrong type")))
}

/// Reads a whole part: its rows and tombstones (verification, tests,
/// compaction of small inputs).
pub async fn read_part(
    opened: OpenedPart,
    options: &PartOptions,
) -> Result<(Vec<KeyedEntry>, Vec<RangeTombstone>), IndexError> {
    let (mut scan, tombstones) = open_part(opened, None, None, options).await?;
    let mut rows = Vec::new();
    while let Some(entry) = scan.next().await? {
        rows.push(entry);
    }
    Ok((rows, tombstones))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use super::*;

    fn put(key: &[u8], record: u64, value: &str) -> KeyedEntry {
        KeyedEntry {
            key: key.to_vec(),
            record,
            value: Some(value.to_owned()),
        }
    }

    fn many(n: u32) -> Vec<KeyedEntry> {
        (0..n)
            .map(|i| put(&i.to_be_bytes(), u64::from(i), &format!("{{\"v\":{i}}}")))
            .collect()
    }

    /// Counts the bytes a reader fetches.
    struct Counting {
        inner: BytesReader,
        fetched: Arc<AtomicU64>,
    }

    impl AsyncFileReader for Counting {
        fn get_bytes(
            &mut self,
            range: Range<u64>,
        ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
            self.fetched
                .fetch_add(range.end.saturating_sub(range.start), Ordering::Relaxed);
            self.inner.get_bytes(range)
        }

        fn get_metadata<'a>(
            &'a mut self,
            options: Option<&'a ArrowReaderOptions>,
        ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
            self.inner.get_metadata(options)
        }
    }

    #[tokio::test]
    async fn round_trips_rows_dels_and_tombstones() {
        let entries = vec![
            put(&[1], 3, "null"),
            KeyedEntry {
                key: vec![2],
                record: 4,
                value: None,
            },
            put(&[5], 7, "{\"a\":\"\\ud800\"}"),
        ];
        let tombstones = vec![RangeTombstone {
            start: vec![6],
            end: vec![9, 9],
            record: 8,
        }];
        let part = encode_part(&entries, &tombstones, &PartOptions::default()).unwrap();
        assert_eq!(part.meta.min_key, vec![1]);
        assert_eq!(
            part.meta.max_key,
            vec![9, 9],
            "key range covers tombstone ends"
        );
        let (rows, tombs) = read_part(
            OpenedPart::load(Box::new(BytesReader(part.bytes.clone())))
                .await
                .unwrap(),
            &PartOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(rows, entries);
        assert_eq!(tombs, tombstones);
    }

    #[tokio::test]
    async fn tombstone_only_part_round_trips() {
        let tombstones = vec![RangeTombstone {
            start: vec![0],
            end: vec![1],
            record: 2,
        }];
        let part = encode_part(&[], &tombstones, &PartOptions::default()).unwrap();
        assert_eq!(part.meta.rows, 0);
        assert_eq!(
            (part.meta.min_key.clone(), part.meta.max_key.clone()),
            (vec![0], vec![1])
        );
        let (rows, tombs) = read_part(
            OpenedPart::load(Box::new(BytesReader(part.bytes.clone())))
                .await
                .unwrap(),
            &PartOptions::default(),
        )
        .await
        .unwrap();
        assert!(rows.is_empty());
        assert_eq!(tombs, tombstones);
        encode_part(&[], &[], &PartOptions::default()).unwrap_err();
    }

    #[tokio::test]
    async fn lower_bound_is_pushed_down_to_the_page_index() {
        let options = PartOptions::default();
        let entries = many(20_000);
        let part = encode_part(&entries, &[], &options).unwrap();
        let fetched = Arc::new(AtomicU64::new(0));
        let reader = Counting {
            inner: BytesReader(part.bytes.clone()),
            fetched: Arc::clone(&fetched),
        };
        let from = 19_990_u32.to_be_bytes();
        let opened = OpenedPart::load(Box::new(reader)).await.unwrap();
        let (mut scan, _) = open_part(opened, Some(&from), None, &options)
            .await
            .unwrap();
        let mut keys = Vec::new();
        while let Some(entry) = scan.next().await.unwrap() {
            keys.push(entry.key);
        }
        assert_eq!(keys.len(), 10);
        assert_eq!(keys.first().unwrap(), &from.to_vec());
        let total = part.meta.bytes;
        let read = fetched.load(Ordering::Relaxed);
        assert!(
            read * 4 < total,
            "a tail read fetched {read} of {total} bytes"
        );
    }

    #[tokio::test]
    async fn store_opener_verifies_tail_and_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let fs = crate::FsObjectStore::new(dir.path()).unwrap();
        let store = ObjectStore::from(fs);
        let options = PartOptions {
            layout_block_bytes: 1024,
            ..PartOptions::default()
        };
        let part = encode_part(&many(2_000), &[], &options).unwrap();
        let key = format!("ns/{}", part.meta.key);
        store.put_if_absent(&key, &part.bytes).await.unwrap();
        let opener = StorePartOpener::new(store.clone(), "ns/");
        let (rows, _) = read_part(opener.open(&part.meta).await.unwrap(), &options)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2_000);

        // A manifest pinning another tail is refused before any parsing.
        let mut wrong = part.meta.clone();
        wrong.tail_hash = digest(b"other");
        assert!(matches!(
            opener.open(&wrong).await,
            Err(IndexError::ObjectHashMismatch(_))
        ));

        // A corrupted data block is refused by the footer layout.
        let mut corrupt = part.bytes.to_vec();
        if let Some(byte) = corrupt.get_mut(100) {
            *byte ^= 0xff;
        }
        let path = dir.path().join(&key);
        std::fs::write(path, &corrupt).unwrap();
        let reader = opener.open(&part.meta).await.unwrap();
        read_part(reader, &options).await.unwrap_err();
    }

    #[tokio::test]
    async fn store_opener_decodes_a_footer_once() {
        let dir = tempfile::tempdir().unwrap();
        let fs = crate::FsObjectStore::new(dir.path()).unwrap();
        let store = ObjectStore::from(fs.clone());
        let options = PartOptions {
            layout_block_bytes: 1024,
            ..PartOptions::default()
        };
        let part = encode_part(&many(5_000), &[], &options).unwrap();
        store
            .put_if_absent(&format!("ns/{}", part.meta.key), &part.bytes)
            .await
            .unwrap();
        let footers = FooterCache::new(DEFAULT_FOOTER_CACHE_BYTES);
        let opener = StorePartOpener::new(store.clone(), "ns/").with_footer_cache(footers.clone());
        let first = opener.open(&part.meta).await.unwrap();
        let tail_reads = fs.range_read_count();
        let tail_bytes = fs.range_read_bytes();
        // Later opens, also through another opener sharing the cache, fetch
        // no tail: only the data blocks a scan touches.
        let other = StorePartOpener::new(store, "ns/").with_footer_cache(footers);
        for opener in [&opener, &other] {
            let opened = opener.open(&part.meta).await.unwrap();
            assert!(Arc::ptr_eq(&opened.footer, &first.footer));
        }
        assert_eq!(fs.range_read_count(), tail_reads);
        let key = 4_321_u32.to_be_bytes();
        let mut end = key.to_vec();
        end.push(0);
        let (mut scan, _) = open_part(first, Some(&key), Some(&end), &options)
            .await
            .unwrap();
        assert_eq!(scan.next().await.unwrap().unwrap().key, key.to_vec());
        assert!(scan.next().await.unwrap().is_none());
        let read = fs.range_read_bytes() - tail_bytes;
        assert!(
            read * 8 < part.meta.data_bytes,
            "a point read fetched {read} of {} data bytes",
            part.meta.data_bytes
        );

        // A manifest entry pinning another tail misses the cache.
        let mut wrong = part.meta.clone();
        wrong.tail_hash = digest(b"other");
        assert!(matches!(
            opener.open(&wrong).await,
            Err(IndexError::ObjectHashMismatch(_))
        ));
    }

    #[tokio::test]
    async fn store_opener_reads_through_the_range_cache() {
        let dir = tempfile::tempdir().unwrap();
        let store = ObjectStore::from(crate::FsObjectStore::new(dir.path().join("s3")).unwrap());
        let cache = EventIndexCache::serving(dir.path().join("cache"), 64 * 1024 * 1024).unwrap();
        let options = PartOptions::default();
        let part = encode_part(&many(500), &[], &options).unwrap();
        store
            .put_if_absent(&format!("ns/{}", part.meta.key), &part.bytes)
            .await
            .unwrap();
        let opener = StorePartOpener::new(store, "ns/")
            .with_cache(&cache)
            .unwrap();
        for _ in 0..2 {
            let (rows, _) = read_part(opener.open(&part.meta).await.unwrap(), &options)
                .await
                .unwrap();
            assert_eq!(rows, many(500));
        }
        let maintenance =
            EventIndexCache::maintenance(dir.path().join("m"), 64 * 1024 * 1024).unwrap();
        let refused = StorePartOpener::new(
            ObjectStore::from(crate::FsObjectStore::new(dir.path().join("x")).unwrap()),
            "ns/",
        )
        .with_cache(&maintenance);
        assert!(refused.is_err(), "a maintenance cache cannot serve reads");
    }

    #[test]
    fn coalescing_keeps_the_smallest_record() {
        let t = |s: u8, e: u8, r: u64| RangeTombstone {
            start: vec![s],
            end: vec![e],
            record: r,
        };
        assert_eq!(
            coalesce_tombstones(vec![t(5, 7, 9), t(1, 3, 4), t(3, 4, 6), t(8, 9, 2)]),
            vec![t(1, 4, 4), t(5, 7, 9), t(8, 9, 2)]
        );
    }
}
