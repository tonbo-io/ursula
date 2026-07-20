use serde::Deserialize;
use serde::Serialize;

use crate::EventEntry;
use crate::IndexError;
use crate::IndexStatus;
use crate::object_store::ObjectStore;
use crate::object_store::digest;

pub(crate) const FORMAT_VERSION: u32 = 5;
pub(crate) const CURRENT_KEY: &str = "CURRENT";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompletedRecordRange {
    pub start_record: u64,
    pub end_record: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecordSegmentLease {
    pub start_record: u64,
    pub end_record: u64,
    pub worker_id: String,
    pub expires_at_ms: u64,
    #[serde(skip)]
    pub(crate) key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct PartMeta {
    pub(crate) key: String,
    pub(crate) layout_key: String,
    pub(crate) level: u8,
    pub(crate) partition_start_ms: i64,
    pub(crate) entries: u64,
    pub(crate) min_captured_at_ms: i64,
    pub(crate) max_captured_at_ms: i64,
    pub(crate) min_record: u64,
    pub(crate) max_record: u64,
    pub(crate) bytes: u64,
}

impl PartMeta {
    pub(crate) fn overlaps(&self, from_ms: i64, until_ms: i64) -> bool {
        self.max_captured_at_ms >= from_ms && self.min_captured_at_ms < until_ms
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Manifest {
    pub(crate) version: u32,
    pub(crate) source_id: String,
    pub(crate) generation: u64,
    pub(crate) indexed_from_record: u64,
    pub(crate) durable_through_record: u64,
    pub(crate) completed_record_ranges: Vec<CompletedRecordRange>,
    pub(crate) status: IndexStatus,
    pub(crate) parts: Vec<PartMeta>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ManifestIdentity {
    pub(crate) version: u32,
    pub(crate) source_id: String,
}

impl Manifest {
    pub(crate) fn new(source_id: String, indexed_from_record: u64) -> Self {
        Self {
            version: FORMAT_VERSION,
            source_id,
            generation: 0,
            indexed_from_record,
            durable_through_record: indexed_from_record,
            completed_record_ranges: Vec::new(),
            status: IndexStatus::Ready,
            parts: Vec::new(),
        }
    }

    /// Serialize this manifest into its content-addressed object plus the
    /// matching `CURRENT` pointer bytes.
    pub(crate) fn encode(&self) -> Result<(String, Vec<u8>, Vec<u8>), IndexError> {
        let bytes = serde_json::to_vec(self)?;
        let key = format!("manifests/{:020}-{}.json", self.generation, digest(&bytes));
        let pointer_bytes = serde_json::to_vec(&CurrentPointer {
            version: FORMAT_VERSION,
            generation: self.generation,
            manifest: key.clone(),
        })?;
        Ok((key, bytes, pointer_bytes))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CurrentPointer {
    version: u32,
    generation: u64,
    manifest: String,
}

#[derive(Clone, Debug)]
pub(crate) struct PublishedManifest {
    pub(crate) pointer_etag: String,
    pub(crate) manifest_key: String,
    pub(crate) manifest: Manifest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GarbageCollectionReport {
    pub deleted_parts: usize,
    pub deleted_layouts: usize,
    pub deleted_manifests: usize,
    pub deleted_claims: usize,
}

pub(crate) async fn initialize(
    store: &ObjectStore,
    source_id: &str,
    indexed_from_record: u64,
) -> Result<(), IndexError> {
    if store.get(CURRENT_KEY).await?.is_some() {
        return Ok(());
    }
    let (key, bytes, pointer_bytes) =
        Manifest::new(source_id.to_owned(), indexed_from_record).encode()?;
    let _manifest_write = store.put_if_absent(&key, &bytes).await?;
    let _pointer_write = store.put_if_absent(CURRENT_KEY, &pointer_bytes).await?;
    Ok(())
}

pub(crate) async fn load_published(
    store: &ObjectStore,
    configured_source: &str,
) -> Result<PublishedManifest, IndexError> {
    let current = store
        .get(CURRENT_KEY)
        .await?
        .ok_or_else(|| IndexError::MissingObject(CURRENT_KEY.to_owned()))?;
    let pointer: CurrentPointer = serde_json::from_slice(&current.bytes)?;
    if pointer.version != FORMAT_VERSION {
        return Err(IndexError::ManifestVersion(pointer.version));
    }
    let manifest_object = store
        .get(&pointer.manifest)
        .await?
        .ok_or_else(|| IndexError::MissingObject(pointer.manifest.clone()))?;
    let manifest_hash = pointer
        .manifest
        .strip_prefix("manifests/")
        .and_then(|value| value.strip_suffix(".json"))
        .and_then(|value| value.split_once('-'))
        .map(|(_, hash)| hash)
        .ok_or_else(|| IndexError::InvalidObjectKey(pointer.manifest.clone()))?;
    if digest(&manifest_object.bytes) != manifest_hash {
        return Err(IndexError::ObjectHashMismatch(pointer.manifest));
    }
    let manifest: Manifest = serde_json::from_slice(&manifest_object.bytes)?;
    if manifest.version != FORMAT_VERSION {
        return Err(IndexError::ManifestVersion(manifest.version));
    }
    if manifest.generation != pointer.generation {
        return Err(IndexError::InvalidSourceResponse(
            "CURRENT generation does not match manifest",
        ));
    }
    if manifest.source_id != configured_source {
        return Err(IndexError::SourceMismatch {
            stored: manifest.source_id,
            configured: configured_source.to_owned(),
        });
    }
    if manifest.durable_through_record < manifest.indexed_from_record
        || manifest
            .completed_record_ranges
            .iter()
            .any(|range| range.start_record < manifest.indexed_from_record)
    {
        return Err(IndexError::InvalidSourceResponse(
            "manifest record ranges precede the index base",
        ));
    }
    Ok(PublishedManifest {
        pointer_etag: current.etag,
        manifest_key: pointer.manifest,
        manifest,
    })
}

pub(crate) fn record_is_covered(ranges: &[CompletedRecordRange], record: u64) -> bool {
    ranges
        .iter()
        .any(|range| range.start_record <= record && record < range.end_record)
}

pub(crate) fn completed_ranges_for_entries(
    entries: &[EventEntry],
) -> Result<Vec<CompletedRecordRange>, IndexError> {
    let mut ranges = Vec::<CompletedRecordRange>::new();
    for entry in entries {
        let end_record = entry
            .record
            .checked_add(1)
            .ok_or(IndexError::InvalidConfig("record segment end overflowed"))?;
        if let Some(previous) = ranges.last_mut()
            && previous.end_record == entry.record
        {
            previous.end_record = end_record;
        } else {
            ranges.push(CompletedRecordRange {
                start_record: entry.record,
                end_record,
            });
        }
    }
    Ok(ranges)
}

pub(crate) fn normalize_completed_ranges(ranges: &mut Vec<CompletedRecordRange>) {
    ranges.sort_unstable_by_key(|range| (range.start_record, range.end_record));
    let mut normalized = Vec::<CompletedRecordRange>::with_capacity(ranges.len());
    for range in ranges
        .drain(..)
        .filter(|range| range.start_record < range.end_record)
    {
        if let Some(previous) = normalized.last_mut()
            && range.start_record <= previous.end_record
        {
            previous.end_record = previous.end_record.max(range.end_record);
            continue;
        }
        normalized.push(range);
    }
    *ranges = normalized;
}

pub(crate) fn contiguous_watermark(
    ranges: &[CompletedRecordRange],
    indexed_from_record: u64,
) -> u64 {
    let mut watermark = indexed_from_record;
    for range in ranges {
        if range.start_record > watermark {
            break;
        }
        watermark = watermark.max(range.end_record);
    }
    watermark
}

pub(crate) fn first_uncovered_record(
    ranges: &[CompletedRecordRange],
    indexed_from_record: u64,
    tail_record: u64,
) -> u64 {
    let mut next = indexed_from_record;
    for range in ranges {
        if range.start_record > next {
            break;
        }
        next = next.max(range.end_record);
        if next >= tail_record {
            return tail_record;
        }
    }
    next.min(tail_record)
}

pub(crate) fn manifest_generation(key: &str) -> Option<u64> {
    key.strip_prefix("manifests/")?
        .split_once('-')?
        .0
        .parse()
        .ok()
}
