use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;
use std::time::SystemTime;

use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex as AsyncMutex;

use crate::EventEntry;
use crate::EventIndexConfig;
use crate::IndexError;
use crate::IndexStatus;
use crate::QueryCursor;
use crate::QueryResult;
use crate::SourceEnvelope;
use crate::object_store::ConditionalWrite;
use crate::object_store::FsObjectStore;
use crate::object_store::ObjectStore;
use crate::object_store::S3ObjectStore;
use crate::object_store::digest;
use crate::part;

const FORMAT_VERSION: u32 = 4;
const CURRENT_KEY: &str = "CURRENT";
const MAX_PUBLISH_ATTEMPTS: usize = 8;
const EVENT_TIME_PARTITION_MS: i64 = 24 * 60 * 60 * 1_000;

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
    key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PartMeta {
    key: String,
    level: u8,
    partition_start_ms: i64,
    entries: u64,
    min_captured_at_ms: i64,
    max_captured_at_ms: i64,
    min_record: u64,
    max_record: u64,
    bytes: u64,
}

impl PartMeta {
    fn overlaps(&self, from_ms: i64, until_ms: i64) -> bool {
        self.max_captured_at_ms >= from_ms && self.min_captured_at_ms < until_ms
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Manifest {
    version: u32,
    source_id: String,
    generation: u64,
    indexed_from_record: u64,
    durable_through_record: u64,
    completed_record_ranges: Vec<CompletedRecordRange>,
    status: IndexStatus,
    parts: Vec<PartMeta>,
}

#[derive(Debug, Deserialize)]
struct ManifestIdentity {
    version: u32,
    source_id: String,
}

impl Manifest {
    fn new(source_id: String, indexed_from_record: u64) -> Self {
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
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CurrentPointer {
    version: u32,
    generation: u64,
    manifest: String,
}

#[derive(Clone, Debug)]
struct PublishedManifest {
    pointer_etag: String,
    manifest_key: String,
    manifest: Manifest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GarbageCollectionReport {
    pub deleted_parts: usize,
    pub deleted_manifests: usize,
    pub deleted_claims: usize,
}

#[derive(Clone, Debug)]
struct LocalCache {
    root: PathBuf,
    max_bytes: u64,
    pins: Arc<Mutex<HashMap<PathBuf, usize>>>,
    budget: Arc<AsyncMutex<CacheBudget>>,
    installs: Arc<AsyncMutex<HashMap<PathBuf, Weak<AsyncMutex<()>>>>>,
}

#[derive(Debug, Default)]
struct CacheBudget {
    reserved_bytes: u64,
}

struct MaterializedPart {
    path: PathBuf,
    pins: Arc<Mutex<HashMap<PathBuf, usize>>>,
}

impl Deref for MaterializedPart {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl Drop for MaterializedPart {
    fn drop(&mut self) {
        let Ok(mut pins) = self.pins.lock() else {
            return;
        };
        let Some(count) = pins.get_mut(&self.path) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            pins.remove(&self.path);
        }
    }
}

#[derive(Clone, Debug)]
pub struct EventIndexCache(LocalCache);

impl EventIndexCache {
    pub fn new(root: impl AsRef<Path>, max_bytes: u64) -> Result<Self, IndexError> {
        Ok(Self(LocalCache::new(root, max_bytes)?))
    }
}

impl LocalCache {
    fn new(root: impl AsRef<Path>, max_bytes: u64) -> Result<Self, IndexError> {
        if max_bytes == 0 {
            return Err(IndexError::InvalidConfig(
                "cache_max_bytes must be positive",
            ));
        }
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            max_bytes,
            pins: Arc::new(Mutex::new(HashMap::new())),
            budget: Arc::new(AsyncMutex::new(CacheBudget::default())),
            installs: Arc::new(AsyncMutex::new(HashMap::new())),
        })
    }

    async fn materialize(
        &self,
        store: &ObjectStore,
        meta: &PartMeta,
    ) -> Result<MaterializedPart, IndexError> {
        fs::create_dir_all(&self.root)?;
        let path = self
            .root
            .join(format!("{}.parquet", digest(meta.key.as_bytes())));
        let pinned = self.pin(path.clone())?;
        if validate_cached_part(path.clone(), meta.clone()).await? {
            return Ok(pinned);
        }
        drop(pinned);
        let install_lock = self.install_lock(&path).await;
        let _install_guard = install_lock.lock().await;
        let pinned = self.pin(path.clone())?;
        if validate_cached_part(path.clone(), meta.clone()).await? {
            return Ok(pinned);
        }
        drop(pinned);
        let object = store
            .get(&meta.key)
            .await?
            .ok_or_else(|| IndexError::MissingObject(meta.key.clone()))?;
        let object_key = meta.key.clone();
        let expected_bytes = meta.bytes;
        let bytes = tokio::task::spawn_blocking(move || {
            validate_downloaded_part(&object_key, expected_bytes, &object.bytes)?;
            Ok::<_, IndexError>(object.bytes)
        })
        .await
        .map_err(|_error| IndexError::WorkerFailed)??;
        self.reserve(meta.bytes, &path).await?;
        let installation_pin = match self.pin(path.clone()) {
            Ok(pin) => pin,
            Err(error) => {
                self.release_reservation(meta.bytes).await;
                return Err(error);
            }
        };
        let root = self.root.clone();
        let install_path = path.clone();
        let install = match tokio::task::spawn_blocking(move || {
            let temporary = root.join(format!("{}.tmp", digest(&bytes)));
            let result = (|| -> Result<(), IndexError> {
                let mut file = File::create(&temporary)?;
                file.write_all(&bytes)?;
                file.sync_all()?;
                fs::rename(&temporary, &install_path)?;
                part::validate(&install_path)?;
                Ok(())
            })();
            if result.is_err() {
                let _remove = fs::remove_file(&temporary);
                let _remove = fs::remove_file(&install_path);
            }
            result
        })
        .await
        {
            Ok(result) => result,
            Err(_error) => Err(IndexError::WorkerFailed),
        };
        match install {
            Ok(()) => {
                self.release_reservation(meta.bytes).await;
                Ok(installation_pin)
            }
            Err(error) => {
                drop(installation_pin);
                self.release_reservation(meta.bytes).await;
                Err(error)
            }
        }
    }

    fn pin(&self, path: PathBuf) -> Result<MaterializedPart, IndexError> {
        let mut pins = self
            .pins
            .lock()
            .map_err(|_error| IndexError::LockPoisoned)?;
        Ok(pin_locked(&mut pins, path, &self.pins))
    }

    async fn install_lock(&self, path: &Path) -> Arc<AsyncMutex<()>> {
        let mut installs = self.installs.lock().await;
        installs.retain(|_path, install| install.strong_count() > 0);
        if let Some(install) = installs.get(path).and_then(Weak::upgrade) {
            return install;
        }
        let install = Arc::new(AsyncMutex::new(()));
        installs.insert(path.to_path_buf(), Arc::downgrade(&install));
        install
    }

    async fn reserve(&self, incoming: u64, protected: &Path) -> Result<(), IndexError> {
        if incoming > self.max_bytes {
            return Err(IndexError::CacheCapacity {
                capacity: self.max_bytes,
                object_size: incoming,
            });
        }
        let mut budget = self.budget.lock().await;
        let pins = Arc::clone(&self.pins);
        let root = self.root.clone();
        let max_bytes = self.max_bytes;
        let reserved_bytes = budget.reserved_bytes;
        let protected = protected.to_path_buf();
        tokio::task::spawn_blocking(move || {
            make_cache_room(
                &root,
                max_bytes,
                incoming,
                reserved_bytes,
                Some(&protected),
                &pins,
            )
        })
        .await
        .map_err(|_error| IndexError::WorkerFailed)??;
        budget.reserved_bytes = budget.reserved_bytes.saturating_add(incoming);
        Ok(())
    }

    async fn release_reservation(&self, bytes: u64) {
        let mut budget = self.budget.lock().await;
        budget.reserved_bytes = budget.reserved_bytes.saturating_sub(bytes);
    }
}

async fn validate_cached_part(path: PathBuf, meta: PartMeta) -> Result<bool, IndexError> {
    tokio::task::spawn_blocking(move || valid_cached_part(&path, &meta))
        .await
        .map_err(|_error| IndexError::WorkerFailed)?
}

fn validate_downloaded_part(
    key: &str,
    expected_bytes: u64,
    bytes: &[u8],
) -> Result<(), IndexError> {
    if u64::try_from(bytes.len()).ok() != Some(expected_bytes) {
        return Err(IndexError::PartSizeMismatch {
            file: key.to_owned(),
            expected: expected_bytes,
            actual: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        });
    }
    let expected_hash = key
        .strip_prefix("parts/")
        .and_then(|value| value.strip_suffix(".parquet"))
        .ok_or_else(|| IndexError::InvalidObjectKey(key.to_owned()))?;
    if digest(bytes) != expected_hash {
        return Err(IndexError::ObjectHashMismatch(key.to_owned()));
    }
    Ok(())
}

fn make_cache_room(
    root: &Path,
    max_bytes: u64,
    incoming: u64,
    reserved: u64,
    protected: Option<&Path>,
    pins: &Arc<Mutex<HashMap<PathBuf, usize>>>,
) -> Result<(), IndexError> {
    let mut files = Vec::new();
    let mut total = 0_u64;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() || protected.is_some_and(|protected| protected == path) {
            continue;
        }
        let metadata = entry.metadata()?;
        total = total.saturating_add(metadata.len());
        if pins
            .lock()
            .map_err(|_error| IndexError::LockPoisoned)?
            .contains_key(&path)
        {
            continue;
        }
        files.push((
            metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            metadata.len(),
            path,
        ));
    }
    files.sort_unstable_by_key(|(modified, _, _)| *modified);
    for (_, bytes, path) in files {
        if total.saturating_add(reserved).saturating_add(incoming) <= max_bytes {
            break;
        }
        if remove_cache_file_if_unpinned(&path, pins)? {
            total = total.saturating_sub(bytes);
        }
    }
    if total.saturating_add(reserved).saturating_add(incoming) > max_bytes {
        return Err(IndexError::CacheCapacity {
            capacity: max_bytes,
            object_size: incoming,
        });
    }
    Ok(())
}

fn remove_cache_file_if_unpinned(
    path: &Path,
    pins: &Arc<Mutex<HashMap<PathBuf, usize>>>,
) -> Result<bool, IndexError> {
    let pins = pins.lock().map_err(|_error| IndexError::LockPoisoned)?;
    if pins.contains_key(path) {
        return Ok(false);
    }
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn pin_locked(
    pins: &mut HashMap<PathBuf, usize>,
    path: PathBuf,
    shared_pins: &Arc<Mutex<HashMap<PathBuf, usize>>>,
) -> MaterializedPart {
    let count = pins.entry(path.clone()).or_default();
    *count = count.saturating_add(1);
    MaterializedPart {
        path,
        pins: Arc::clone(shared_pins),
    }
}

pub struct ServerlessEventIndex {
    store: ObjectStore,
    cache: LocalCache,
    config: EventIndexConfig,
    published: PublishedManifest,
    active: Vec<EventEntry>,
}

impl ServerlessEventIndex {
    pub async fn open_fs(
        store: FsObjectStore,
        cache_dir: impl AsRef<Path>,
        cache_max_bytes: u64,
        config: EventIndexConfig,
    ) -> Result<Self, IndexError> {
        let cache = EventIndexCache::new(cache_dir, cache_max_bytes)?;
        Self::open(store.into(), cache, config, 0).await
    }

    pub async fn open_fs_with_cache(
        store: FsObjectStore,
        cache: EventIndexCache,
        config: EventIndexConfig,
    ) -> Result<Self, IndexError> {
        Self::open(store.into(), cache, config, 0).await
    }

    pub async fn open_s3(
        store: S3ObjectStore,
        cache_dir: impl AsRef<Path>,
        cache_max_bytes: u64,
        config: EventIndexConfig,
    ) -> Result<Self, IndexError> {
        let cache = EventIndexCache::new(cache_dir, cache_max_bytes)?;
        Self::open(store.into(), cache, config, 0).await
    }

    pub async fn open_s3_with_cache(
        store: S3ObjectStore,
        cache: EventIndexCache,
        config: EventIndexConfig,
    ) -> Result<Self, IndexError> {
        Self::open(store.into(), cache, config, 0).await
    }

    pub async fn open_fs_with_cache_from_record(
        store: FsObjectStore,
        cache: EventIndexCache,
        config: EventIndexConfig,
        indexed_from_record: u64,
    ) -> Result<Self, IndexError> {
        Self::open(store.into(), cache, config, indexed_from_record).await
    }

    pub async fn open_s3_with_cache_from_record(
        store: S3ObjectStore,
        cache: EventIndexCache,
        config: EventIndexConfig,
        indexed_from_record: u64,
    ) -> Result<Self, IndexError> {
        Self::open(store.into(), cache, config, indexed_from_record).await
    }

    async fn open(
        store: ObjectStore,
        cache: EventIndexCache,
        config: EventIndexConfig,
        indexed_from_record: u64,
    ) -> Result<Self, IndexError> {
        validate_config(&config)?;
        initialize(&store, &config.source_id, indexed_from_record).await?;
        let published = load_published(&store, &config.source_id).await?;
        if published.manifest.indexed_from_record != indexed_from_record {
            return Err(IndexError::IndexBaseMismatch {
                stored: published.manifest.indexed_from_record,
                configured: indexed_from_record,
            });
        }
        Ok(Self {
            store,
            cache: cache.0,
            config,
            published,
            active: Vec::new(),
        })
    }

    pub fn status(&self) -> &IndexStatus {
        &self.published.manifest.status
    }

    pub fn durable_through_record(&self) -> u64 {
        self.published.manifest.durable_through_record
    }

    pub fn indexed_from_record(&self) -> u64 {
        self.published.manifest.indexed_from_record
    }

    pub fn indexed_through_record(&self) -> u64 {
        self.active
            .last()
            .map_or(self.published.manifest.durable_through_record, |entry| {
                entry.record.saturating_add(1)
            })
    }

    pub fn part_count(&self) -> usize {
        self.published.manifest.parts.len()
    }

    pub fn completed_record_ranges(&self) -> &[CompletedRecordRange] {
        &self.published.manifest.completed_record_ranges
    }

    /// Claim the oldest currently-uncovered source range. Claims coordinate
    /// work but are not a correctness boundary: duplicate processing remains
    /// safe because segment publication is immutable and manifest-CAS guarded.
    pub async fn claim_next_segment(
        &mut self,
        tail_record: u64,
        segment_records: u64,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
    ) -> Result<Option<RecordSegmentLease>, IndexError> {
        if segment_records == 0 || lease_ms == 0 || worker_id.is_empty() {
            return Err(IndexError::InvalidConfig(
                "segment records, lease duration, and worker id must be non-empty",
            ));
        }
        for _attempt in 0..MAX_PUBLISH_ATTEMPTS {
            self.refresh().await?;
            ensure_ready(&self.published.manifest.status)?;
            let mut covered = self.published.manifest.completed_record_ranges.clone();
            for object in self.store.list("claims/").await? {
                if !object.key.ends_with(".json") {
                    continue;
                }
                let Some(stored) = self.store.get(&object.key).await? else {
                    continue;
                };
                let claim: RecordSegmentLease = serde_json::from_slice(&stored.bytes)?;
                if claim.expires_at_ms > now_ms {
                    covered.push(CompletedRecordRange {
                        start_record: claim.start_record,
                        end_record: claim.end_record,
                    });
                }
            }
            normalize_completed_ranges(&mut covered);
            let start_record = first_uncovered_record(
                &covered,
                self.published.manifest.indexed_from_record,
                tail_record,
            );
            if start_record >= tail_record {
                return Ok(None);
            }
            let next_covered_record = covered
                .iter()
                .find(|range| range.start_record > start_record)
                .map_or(tail_record, |range| range.start_record);
            let end_record = start_record
                .saturating_add(segment_records)
                .min(tail_record)
                .min(next_covered_record);
            let key = format!("claims/{start_record:020}.json");
            let claim = RecordSegmentLease {
                start_record,
                end_record,
                worker_id: worker_id.to_owned(),
                expires_at_ms: now_ms.saturating_add(lease_ms),
                key: key.clone(),
            };
            let bytes = serde_json::to_vec(&claim)?;
            match self.store.put_if_absent(&key, &bytes).await? {
                ConditionalWrite::Written => return Ok(Some(claim)),
                ConditionalWrite::Conflict => {
                    let Some(stored) = self.store.get(&key).await? else {
                        continue;
                    };
                    let existing: RecordSegmentLease = serde_json::from_slice(&stored.bytes)?;
                    if existing.expires_at_ms > now_ms {
                        continue;
                    }
                    match self
                        .store
                        .compare_and_swap(&key, &stored.etag, &bytes)
                        .await?
                    {
                        ConditionalWrite::Written => return Ok(Some(claim)),
                        ConditionalWrite::Conflict => continue,
                    }
                }
            }
        }
        Err(IndexError::PublishConflict)
    }

    pub async fn finish_segment(
        &mut self,
        claim: &RecordSegmentLease,
        envelopes: Vec<SourceEnvelope>,
    ) -> Result<(), IndexError> {
        let maximum_len = claim.end_record.saturating_sub(claim.start_record);
        let actual_len = u64::try_from(envelopes.len())
            .map_err(|_error| IndexError::InvalidConfig("record segment is too large"))?;
        if actual_len == 0 || actual_len > maximum_len {
            return Err(IndexError::InvalidSourceResponse(
                "source returned an invalid claimed record segment length",
            ));
        }
        self.commit_envelopes(claim.start_record, envelopes).await?;
        self.store.delete(&claim.key).await
    }

    pub fn needs_partition_compaction(&self, fan_in: usize, max_entries: u64) -> bool {
        fan_in >= 2
            && max_entries > 0
            && select_compaction(&self.published.manifest.parts, fan_in, max_entries).is_some()
    }

    pub async fn refresh(&mut self) -> Result<(), IndexError> {
        let latest = load_published(&self.store, &self.config.source_id).await?;
        if latest.pointer_etag != self.published.pointer_etag {
            self.published = latest;
            self.reconcile_active().await?;
        }
        Ok(())
    }

    async fn reconcile_active(&mut self) -> Result<(), IndexError> {
        let checkpoint = self.published.manifest.durable_through_record;
        if self
            .active
            .first()
            .is_none_or(|entry| entry.record >= checkpoint)
        {
            return Ok(());
        }
        let first_record = self.active.first().map_or(checkpoint, |entry| entry.record);
        let mut committed = Vec::new();
        for meta in self
            .published
            .manifest
            .parts
            .iter()
            .filter(|meta| meta.max_record >= first_record && meta.min_record < checkpoint)
        {
            let path = self.cache.materialize(&self.store, meta).await?;
            committed.extend(part::read_all(&path)?);
        }
        for active in self.active.iter().filter(|entry| entry.record < checkpoint) {
            let matches = committed.iter().any(|committed| committed == active);
            if !matches {
                return Err(IndexError::RecordConflict {
                    record: active.record,
                });
            }
        }
        self.active.retain(|entry| entry.record >= checkpoint);
        Ok(())
    }

    pub async fn ingest_envelope(&mut self, envelope: SourceEnvelope) -> Result<(), IndexError> {
        let timestamp = envelope
            .value
            .get(&self.config.timestamp_field)
            .and_then(super::store::parse_timestamp);
        let captured_at_ms = timestamp.ok_or_else(|| IndexError::InvalidTimestamp {
            record: envelope.record,
            field: self.config.timestamp_field.clone(),
        })?;
        self.ingest(EventEntry {
            captured_at_ms,
            record: envelope.record,
        })
        .await
    }

    pub async fn ingest(&mut self, entry: EventEntry) -> Result<(), IndexError> {
        let expected = self.indexed_through_record();
        if entry.record != expected {
            return Err(IndexError::UnexpectedRecord {
                expected,
                actual: entry.record,
            });
        }
        ensure_ready(&self.published.manifest.status)?;
        self.active.push(entry);
        if self.active.len() >= self.config.flush_entries {
            self.flush().await?;
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), IndexError> {
        for _attempt in 0..MAX_PUBLISH_ATTEMPTS {
            if self.active.is_empty() {
                return Ok(());
            }
            self.refresh().await?;
            if self.active.is_empty() {
                return Ok(());
            }
            ensure_ready(&self.published.manifest.status)?;
            let start_record = self.active.first().map(|entry| entry.record).ok_or(
                IndexError::InvalidSourceResponse("active event buffer unexpectedly became empty"),
            )?;
            let checkpoint = self.indexed_through_record();
            let mut partitions = BTreeMap::<i64, Vec<EventEntry>>::new();
            for entry in self.active.iter().copied() {
                partitions
                    .entry(event_time_partition(entry.captured_at_ms))
                    .or_default()
                    .push(entry);
            }
            let mut metas = Vec::with_capacity(partitions.len());
            for (partition_start_ms, mut entries) in partitions {
                entries.sort_unstable();
                metas.push(
                    self.write_and_upload_part(&entries, 0, partition_start_ms)
                        .await?,
                );
            }
            let mut next = self.published.manifest.clone();
            next.generation = next.generation.saturating_add(1);
            next.completed_record_ranges.push(CompletedRecordRange {
                start_record,
                end_record: checkpoint,
            });
            normalize_completed_ranges(&mut next.completed_record_ranges);
            next.durable_through_record =
                contiguous_watermark(&next.completed_record_ranges, next.indexed_from_record);
            next.parts.extend(metas);
            if self.publish(next).await? {
                self.active.clear();
                return Ok(());
            }
        }
        Err(IndexError::PublishConflict)
    }

    /// Publish one independently processed source-record segment. Segments may
    /// complete out of order; the durable watermark advances only across a
    /// gap-free prefix starting at record zero.
    pub async fn commit_envelopes(
        &mut self,
        start_record: u64,
        envelopes: Vec<SourceEnvelope>,
    ) -> Result<(), IndexError> {
        if start_record < self.published.manifest.indexed_from_record {
            return Err(IndexError::InvalidSourceResponse(
                "record segment starts before the index base",
            ));
        }
        if envelopes.is_empty() {
            return Err(IndexError::InvalidSourceResponse(
                "record segment must not be empty",
            ));
        }
        let mut entries = Vec::with_capacity(envelopes.len());
        for (index, envelope) in envelopes.into_iter().enumerate() {
            let relative = u64::try_from(index)
                .map_err(|_error| IndexError::InvalidConfig("record segment is too large"))?;
            let expected = start_record
                .checked_add(relative)
                .ok_or(IndexError::InvalidConfig("record segment end overflowed"))?;
            if envelope.record != expected {
                return Err(IndexError::UnexpectedRecord {
                    expected,
                    actual: envelope.record,
                });
            }
            let captured_at_ms = envelope
                .value
                .get(&self.config.timestamp_field)
                .and_then(super::store::parse_timestamp)
                .ok_or_else(|| IndexError::InvalidTimestamp {
                    record: envelope.record,
                    field: self.config.timestamp_field.clone(),
                })?;
            entries.push(EventEntry {
                captured_at_ms,
                record: envelope.record,
            });
        }
        for _attempt in 0..MAX_PUBLISH_ATTEMPTS {
            self.refresh().await?;
            ensure_ready(&self.published.manifest.status)?;
            let (committed, uncovered): (Vec<_>, Vec<_>) =
                entries.iter().copied().partition(|entry| {
                    record_is_covered(
                        &self.published.manifest.completed_record_ranges,
                        entry.record,
                    )
                });
            self.verify_committed_entries(&committed).await?;
            if uncovered.is_empty() {
                return Ok(());
            }
            let new_ranges = completed_ranges_for_entries(&uncovered)?;
            let mut partitions = BTreeMap::<i64, Vec<EventEntry>>::new();
            for entry in uncovered {
                partitions
                    .entry(event_time_partition(entry.captured_at_ms))
                    .or_default()
                    .push(entry);
            }
            let mut uploaded_parts = Vec::with_capacity(partitions.len());
            for (partition_start_ms, mut partition_entries) in partitions {
                partition_entries.sort_unstable();
                uploaded_parts.push(
                    self.write_and_upload_part(&partition_entries, 0, partition_start_ms)
                        .await?,
                );
            }
            let mut next = self.published.manifest.clone();
            next.generation = next.generation.saturating_add(1);
            next.completed_record_ranges.extend(new_ranges);
            normalize_completed_ranges(&mut next.completed_record_ranges);
            next.durable_through_record =
                contiguous_watermark(&next.completed_record_ranges, next.indexed_from_record);
            next.parts.extend(uploaded_parts);
            if self.publish(next).await? {
                return Ok(());
            }
        }
        Err(IndexError::PublishConflict)
    }

    async fn verify_committed_entries(&self, expected: &[EventEntry]) -> Result<(), IndexError> {
        let Some(first) = expected.first() else {
            return Ok(());
        };
        let Some(last) = expected.last() else {
            return Ok(());
        };
        let mut committed = Vec::new();
        for meta in self
            .published
            .manifest
            .parts
            .iter()
            .filter(|meta| meta.max_record >= first.record && meta.min_record <= last.record)
        {
            let path = self.cache.materialize(&self.store, meta).await?;
            committed.extend(part::read_all(&path)?);
        }
        for entry in expected {
            if !committed.iter().any(|candidate| candidate == entry) {
                return Err(IndexError::RecordConflict {
                    record: entry.record,
                });
            }
        }
        Ok(())
    }

    pub async fn mark_retention_gap(
        &mut self,
        first_available_record: u64,
    ) -> Result<(), IndexError> {
        self.flush().await?;
        let expected_record = self.indexed_through_record();
        if first_available_record <= expected_record {
            return Err(IndexError::InvalidSourceResponse(
                "retention gap does not advance beyond expected record",
            ));
        }
        self.publish_status(IndexStatus::RetentionGap {
            expected_record,
            first_available_record,
        })
        .await?;
        Err(IndexError::RetentionGap {
            expected_record,
            first_available_record,
        })
    }

    pub async fn mark_blocked(&mut self, record: u64, reason: String) -> Result<(), IndexError> {
        self.flush().await?;
        if record != self.indexed_through_record() {
            return Err(IndexError::UnexpectedRecord {
                expected: self.indexed_through_record(),
                actual: record,
            });
        }
        self.publish_status(IndexStatus::Blocked { record, reason })
            .await
    }

    pub async fn mark_segment_blocked(
        &mut self,
        record: u64,
        reason: String,
    ) -> Result<(), IndexError> {
        self.refresh().await?;
        self.publish_status(IndexStatus::Blocked { record, reason })
            .await
    }

    pub async fn clear_blocked(&mut self) -> Result<(), IndexError> {
        self.refresh().await?;
        match self.published.manifest.status {
            IndexStatus::Ready => Ok(()),
            IndexStatus::Blocked { .. } => self.publish_status(IndexStatus::Ready).await,
            IndexStatus::RetentionGap { .. } => Err(IndexError::CannotResume(
                "a retention gap requires rebuilding the index",
            )),
        }
    }

    pub async fn query(
        &mut self,
        from_ms: i64,
        until_ms: i64,
        after: Option<QueryCursor>,
        through_record: Option<u64>,
        limit: usize,
    ) -> Result<QueryResult, IndexError> {
        if from_ms >= until_ms || limit == 0 {
            return Err(IndexError::InvalidQuery);
        }
        self.refresh().await?;
        if let IndexStatus::RetentionGap {
            expected_record,
            first_available_record,
        } = self.published.manifest.status
        {
            return Err(IndexError::RetentionGap {
                expected_record,
                first_available_record,
            });
        }
        let indexed_through_record = self.indexed_through_record();
        let through_record = through_record.unwrap_or(indexed_through_record);
        if through_record < self.published.manifest.indexed_from_record
            || through_record > indexed_through_record
        {
            return Err(IndexError::InvalidQuery);
        }
        let mut entries = Vec::new();
        for meta in self
            .published
            .manifest
            .parts
            .iter()
            .filter(|meta| meta.overlaps(from_ms, until_ms))
        {
            let path = self.cache.materialize(&self.store, meta).await?;
            entries.extend(part::read_part_range(&path, from_ms, until_ms)?);
        }
        entries.extend(
            self.active
                .iter()
                .copied()
                .filter(|entry| entry.captured_at_ms >= from_ms && entry.captured_at_ms < until_ms),
        );
        entries.retain(|entry| entry.record < through_record);
        if let Some(after) = after {
            entries.retain(|entry| {
                (entry.captured_at_ms, entry.record) > (after.captured_at_ms, after.record)
            });
        }
        entries.sort_unstable();
        entries.dedup_by_key(|entry| entry.record);
        let has_more = entries.len() > limit;
        entries.truncate(limit);
        let next = has_more
            .then(|| entries.last().copied().map(QueryCursor::from))
            .flatten();
        Ok(QueryResult {
            indexed_from_record: self.published.manifest.indexed_from_record,
            indexed_through_record,
            durable_through_record: self.published.manifest.durable_through_record,
            through_record,
            records: entries,
            next,
        })
    }

    pub async fn compact_partition_once(
        &mut self,
        fan_in: usize,
        max_entries: u64,
    ) -> Result<bool, IndexError> {
        if fan_in < 2 {
            return Err(IndexError::InvalidConfig(
                "compaction fan-in must be at least 2",
            ));
        }
        if max_entries == 0 {
            return Err(IndexError::InvalidConfig(
                "compaction max entries must be positive",
            ));
        }
        for _attempt in 0..MAX_PUBLISH_ATTEMPTS {
            self.refresh().await?;
            let Some(candidate) =
                select_compaction(&self.published.manifest.parts, fan_in, max_entries)
            else {
                return Ok(false);
            };
            let candidate_entries = candidate.iter().try_fold(0_u64, |total, meta| {
                total
                    .checked_add(meta.entries)
                    .ok_or(IndexError::InvalidConfig(
                        "compaction candidate entry count overflowed",
                    ))
            })?;
            let mut entries =
                Vec::with_capacity(usize::try_from(candidate_entries).map_err(|_error| {
                    IndexError::CompactionTooLarge {
                        entries: candidate_entries,
                        max_entries,
                    }
                })?);
            let candidate_keys = candidate
                .iter()
                .map(|meta| meta.key.as_str())
                .collect::<HashSet<_>>();
            for meta in &candidate {
                let path = self.cache.materialize(&self.store, meta).await?;
                entries.extend(part::read_all(&path)?);
            }
            entries.sort_unstable();
            entries.dedup_by_key(|entry| entry.record);
            let partition_start_ms = candidate
                .first()
                .map(|meta| meta.partition_start_ms)
                .ok_or(IndexError::InvalidQuery)?;
            let meta = self
                .write_and_upload_part(&entries, 1, partition_start_ms)
                .await?;
            let mut next = self.published.manifest.clone();
            next.generation = next.generation.saturating_add(1);
            next.parts
                .retain(|part| !candidate_keys.contains(part.key.as_str()));
            next.parts.push(meta);
            if self.publish(next).await? {
                return Ok(true);
            }
        }
        Err(IndexError::PublishConflict)
    }

    pub async fn garbage_collect(
        &mut self,
        retain_generations: u64,
        grace: Duration,
        now: SystemTime,
    ) -> Result<GarbageCollectionReport, IndexError> {
        if retain_generations == 0 {
            return Err(IndexError::InvalidConfig(
                "GC retained generations must be positive",
            ));
        }
        self.refresh().await?;
        let current_generation = self.published.manifest.generation;
        let minimum_generation = current_generation
            .saturating_add(1)
            .saturating_sub(retain_generations);
        let cutoff = now.checked_sub(grace).unwrap_or(SystemTime::UNIX_EPOCH);
        let manifest_objects = self.store.list("manifests/").await?;
        let mut retained_manifests = HashSet::new();
        retained_manifests.insert(self.published.manifest_key.clone());
        for object in &manifest_objects {
            if !object.key.ends_with(".json") {
                continue;
            }
            let Some(generation) = manifest_generation(&object.key) else {
                continue;
            };
            if generation >= minimum_generation && generation < current_generation {
                retained_manifests.insert(object.key.clone());
            }
        }
        let mut retained_parts = HashSet::new();
        let retained_manifest_keys = retained_manifests.iter().cloned().collect::<Vec<_>>();
        for key in &retained_manifest_keys {
            let object = self
                .store
                .get(key)
                .await?
                .ok_or_else(|| IndexError::MissingObject(key.clone()))?;
            let identity: ManifestIdentity = serde_json::from_slice(&object.bytes)?;
            if identity.version != FORMAT_VERSION || identity.source_id != self.config.source_id {
                tracing::warn!(
                    manifest = %key,
                    version = identity.version,
                    source_id = %identity.source_id,
                    "skipping incompatible manifest during event-index garbage collection"
                );
                retained_manifests.remove(key);
                continue;
            }
            let manifest: Manifest = serde_json::from_slice(&object.bytes)?;
            retained_parts.extend(manifest.parts.into_iter().map(|part| part.key));
        }
        let part_objects = self.store.list("parts/").await?;
        let latest = load_published(&self.store, &self.config.source_id).await?;
        let durable_through_record = latest.manifest.durable_through_record;
        retained_manifests.insert(latest.manifest_key);
        retained_parts.extend(latest.manifest.parts.into_iter().map(|part| part.key));
        let stale_manifests = manifest_objects
            .into_iter()
            .filter(|object| object.key.ends_with(".json"))
            .filter(|object| eligible_for_gc(object.modified, cutoff))
            .filter(|object| !retained_manifests.contains(&object.key))
            .map(|object| object.key)
            .collect::<Vec<_>>();
        let stale_parts = part_objects
            .into_iter()
            .filter(|object| object.key.ends_with(".parquet"))
            .filter(|object| eligible_for_gc(object.modified, cutoff))
            .filter(|object| !retained_parts.contains(&object.key))
            .map(|object| object.key)
            .collect::<Vec<_>>();
        let now_ms = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .and_then(|duration| u64::try_from(duration.as_millis()).ok());
        let mut stale_claims = Vec::new();
        for object in self.store.list("claims/").await? {
            if !object.key.ends_with(".json") {
                continue;
            }
            let Some(stored) = self.store.get(&object.key).await? else {
                continue;
            };
            let claim: RecordSegmentLease = serde_json::from_slice(&stored.bytes)?;
            if claim.end_record <= durable_through_record
                || now_ms.is_some_and(|now_ms| claim.expires_at_ms <= now_ms)
            {
                stale_claims.push(object.key);
            }
        }
        for key in &stale_manifests {
            self.store.delete(key).await?;
        }
        for key in &stale_parts {
            self.store.delete(key).await?;
        }
        for key in &stale_claims {
            self.store.delete(key).await?;
        }
        self.refresh().await?;
        Ok(GarbageCollectionReport {
            deleted_parts: stale_parts.len(),
            deleted_manifests: stale_manifests.len(),
            deleted_claims: stale_claims.len(),
        })
    }

    async fn publish_status(&mut self, status: IndexStatus) -> Result<(), IndexError> {
        for _attempt in 0..MAX_PUBLISH_ATTEMPTS {
            self.refresh().await?;
            let mut next = self.published.manifest.clone();
            next.generation = next.generation.saturating_add(1);
            next.status = status.clone();
            if self.publish(next).await? {
                return Ok(());
            }
        }
        Err(IndexError::PublishConflict)
    }

    async fn write_and_upload_part(
        &self,
        entries: &[EventEntry],
        level: u8,
        partition_start_ms: i64,
    ) -> Result<PartMeta, IndexError> {
        if entries
            .iter()
            .any(|entry| event_time_partition(entry.captured_at_ms) != partition_start_ms)
        {
            return Err(IndexError::InvalidSourceResponse(
                "part entries cross an event-time partition boundary",
            ));
        }
        let temporary = tempfile::NamedTempFile::new_in(&self.cache.root)?;
        part::write_part(temporary.path(), entries, self.config.row_group_entries)?;
        let bytes = fs::read(temporary.path())?;
        let hash = digest(&bytes);
        let key = format!("parts/{hash}.parquet");
        let _write = self.store.put_if_absent(&key, &bytes).await?;
        let first = entries.first().ok_or(IndexError::InvalidQuery)?;
        let last = entries.last().ok_or(IndexError::InvalidQuery)?;
        Ok(PartMeta {
            key,
            level,
            partition_start_ms,
            entries: u64::try_from(entries.len())
                .map_err(|_error| IndexError::InvalidConfig("part is too large"))?,
            min_captured_at_ms: first.captured_at_ms,
            max_captured_at_ms: last.captured_at_ms,
            min_record: entries
                .iter()
                .map(|entry| entry.record)
                .min()
                .ok_or(IndexError::InvalidQuery)?,
            max_record: entries
                .iter()
                .map(|entry| entry.record)
                .max()
                .ok_or(IndexError::InvalidQuery)?,
            bytes: u64::try_from(bytes.len())
                .map_err(|_error| IndexError::InvalidConfig("part is too large"))?,
        })
    }

    async fn publish(&mut self, next: Manifest) -> Result<bool, IndexError> {
        let bytes = serde_json::to_vec(&next)?;
        let key = format!("manifests/{:020}-{}.json", next.generation, digest(&bytes));
        let _write = self.store.put_if_absent(&key, &bytes).await?;
        let pointer = CurrentPointer {
            version: FORMAT_VERSION,
            generation: next.generation,
            manifest: key,
        };
        let pointer_bytes = serde_json::to_vec(&pointer)?;
        match self
            .store
            .compare_and_swap(CURRENT_KEY, &self.published.pointer_etag, &pointer_bytes)
            .await?
        {
            ConditionalWrite::Written => {
                self.published = load_published(&self.store, &self.config.source_id).await?;
                Ok(true)
            }
            ConditionalWrite::Conflict => {
                self.refresh().await?;
                Ok(false)
            }
        }
    }
}

async fn initialize(
    store: &ObjectStore,
    source_id: &str,
    indexed_from_record: u64,
) -> Result<(), IndexError> {
    if store.get(CURRENT_KEY).await?.is_some() {
        return Ok(());
    }
    let manifest = Manifest::new(source_id.to_owned(), indexed_from_record);
    let bytes = serde_json::to_vec(&manifest)?;
    let manifest_key = format!("manifests/{:020}-{}.json", 0, digest(&bytes));
    let _manifest_write = store.put_if_absent(&manifest_key, &bytes).await?;
    let pointer = CurrentPointer {
        version: FORMAT_VERSION,
        generation: 0,
        manifest: manifest_key,
    };
    let pointer_bytes = serde_json::to_vec(&pointer)?;
    let _pointer_write = store.put_if_absent(CURRENT_KEY, &pointer_bytes).await?;
    Ok(())
}

async fn load_published(
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

fn event_time_partition(captured_at_ms: i64) -> i64 {
    captured_at_ms
        .div_euclid(EVENT_TIME_PARTITION_MS)
        .saturating_mul(EVENT_TIME_PARTITION_MS)
}

fn record_is_covered(ranges: &[CompletedRecordRange], record: u64) -> bool {
    ranges
        .iter()
        .any(|range| range.start_record <= record && record < range.end_record)
}

fn completed_ranges_for_entries(
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

fn normalize_completed_ranges(ranges: &mut Vec<CompletedRecordRange>) {
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

fn contiguous_watermark(ranges: &[CompletedRecordRange], indexed_from_record: u64) -> u64 {
    let mut watermark = indexed_from_record;
    for range in ranges {
        if range.start_record > watermark {
            break;
        }
        watermark = watermark.max(range.end_record);
    }
    watermark
}

fn first_uncovered_record(
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

fn select_compaction(parts: &[PartMeta], fan_in: usize, max_entries: u64) -> Option<Vec<PartMeta>> {
    let mut partitions = BTreeMap::<i64, Vec<&PartMeta>>::new();
    for part in parts.iter().filter(|part| part.level == 0) {
        partitions
            .entry(part.partition_start_ms)
            .or_default()
            .push(part);
    }
    partitions.into_iter().find_map(|(_, mut parts)| {
        if parts.len() < fan_in {
            return None;
        }
        parts.sort_unstable_by(|left, right| {
            left.entries
                .cmp(&right.entries)
                .then_with(|| left.key.cmp(&right.key))
        });
        let mut total = 0_u64;
        let mut selected = Vec::with_capacity(fan_in);
        for part in parts {
            if selected.len() == fan_in {
                break;
            }
            let Some(next_total) = total.checked_add(part.entries) else {
                continue;
            };
            if next_total > max_entries {
                continue;
            }
            total = next_total;
            selected.push(part.clone());
        }
        (selected.len() >= 2).then_some(selected)
    })
}

fn eligible_for_gc(modified: Option<SystemTime>, cutoff: SystemTime) -> bool {
    modified.is_some_and(|modified| modified <= cutoff)
}

fn manifest_generation(key: &str) -> Option<u64> {
    key.strip_prefix("manifests/")?
        .split_once('-')?
        .0
        .parse()
        .ok()
}

fn valid_cached_part(path: &Path, meta: &PartMeta) -> Result<bool, IndexError> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if metadata.len() != meta.bytes {
        fs::remove_file(path)?;
        return Ok(false);
    }
    let expected_hash = meta
        .key
        .strip_prefix("parts/")
        .and_then(|value| value.strip_suffix(".parquet"))
        .ok_or_else(|| IndexError::InvalidObjectKey(meta.key.clone()))?;
    if digest(&fs::read(path)?) != expected_hash {
        fs::remove_file(path)?;
        return Ok(false);
    }
    match part::validate(path) {
        Ok(()) => Ok(true),
        Err(_) => {
            fs::remove_file(path)?;
            Ok(false)
        }
    }
}

fn validate_config(config: &EventIndexConfig) -> Result<(), IndexError> {
    if config.flush_entries == 0 {
        return Err(IndexError::InvalidConfig("flush_entries must be positive"));
    }
    if config.row_group_entries == 0 {
        return Err(IndexError::InvalidConfig(
            "row_group_entries must be positive",
        ));
    }
    if config.source_id.is_empty() {
        return Err(IndexError::InvalidConfig("source_id must not be empty"));
    }
    Ok(())
}

fn ensure_ready(status: &IndexStatus) -> Result<(), IndexError> {
    match status {
        IndexStatus::Ready => Ok(()),
        IndexStatus::Blocked { record, reason } => Err(IndexError::Blocked {
            record: *record,
            reason: reason.clone(),
        }),
        IndexStatus::RetentionGap {
            expected_record,
            first_available_record,
        } => Err(IndexError::RetentionGap {
            expected_record: *expected_record,
            first_available_record: *first_available_record,
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::SystemTime;

    use super::LocalCache;
    use super::eligible_for_gc;
    use super::pin_locked;
    use super::remove_cache_file_if_unpinned;
    use crate::IndexError;

    #[test]
    fn missing_modification_time_is_not_eligible_for_gc() {
        assert!(!eligible_for_gc(None, SystemTime::UNIX_EPOCH));
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the test combines fallible setup with assertions"
    )]
    async fn shared_cache_never_evicts_a_pinned_part_or_exceeds_its_budget() -> anyhow::Result<()> {
        let directory = tempfile::TempDir::new()?;
        let cache = LocalCache::new(directory.path(), 10)?;
        let path = directory.path().join("part.parquet");
        fs::write(&path, [0_u8; 6])?;
        let guard = {
            let mut pins = cache
                .pins
                .lock()
                .map_err(|_error| anyhow::anyhow!("cache pin lock poisoned"))?;
            pin_locked(&mut pins, path.clone(), &cache.pins)
        };
        let incoming = directory.path().join("incoming.parquet");
        let error = cache
            .reserve(6, &incoming)
            .await
            .expect_err("a pinned part must not be evicted to exceed the cache bound");
        assert!(matches!(error, IndexError::CacheCapacity { .. }));
        assert!(path.exists());

        drop(guard);
        cache.reserve(6, &incoming).await?;
        cache.release_reservation(6).await;
        assert!(!path.exists());
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the test combines fallible setup with assertions"
    )]
    fn eviction_rechecks_a_new_pin_immediately_before_unlink() -> anyhow::Result<()> {
        let directory = tempfile::TempDir::new()?;
        let cache = LocalCache::new(directory.path(), 10)?;
        let path = directory.path().join("part.parquet");
        fs::write(&path, [0_u8; 6])?;
        let guard = cache.pin(path.clone())?;

        assert!(!remove_cache_file_if_unpinned(&path, &cache.pins)?);
        assert!(path.exists());
        drop(guard);
        assert!(remove_cache_file_if_unpinned(&path, &cache.pins)?);
        assert!(!path.exists());
        Ok(())
    }
}
