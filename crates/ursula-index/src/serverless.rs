use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::SystemTime;

use serde::Deserialize;
use serde::Serialize;

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

const FORMAT_VERSION: u32 = 2;
const CURRENT_KEY: &str = "CURRENT";
const MAX_PUBLISH_ATTEMPTS: usize = 8;
const EVENT_TIME_PARTITION_MS: i64 = 24 * 60 * 60 * 1_000;

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
    durable_through_record: u64,
    status: IndexStatus,
    parts: Vec<PartMeta>,
}

#[derive(Debug, Deserialize)]
struct ManifestIdentity {
    version: u32,
    source_id: String,
}

impl Manifest {
    fn new(source_id: String) -> Self {
        Self {
            version: FORMAT_VERSION,
            source_id,
            generation: 0,
            durable_through_record: 0,
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
}

#[derive(Debug)]
struct LocalCache {
    root: PathBuf,
    max_bytes: u64,
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
        Ok(Self { root, max_bytes })
    }

    async fn materialize(
        &self,
        store: &ObjectStore,
        meta: &PartMeta,
    ) -> Result<PathBuf, IndexError> {
        fs::create_dir_all(&self.root)?;
        let path = self
            .root
            .join(format!("{}.parquet", digest(meta.key.as_bytes())));
        if valid_cached_part(&path, meta)? {
            return Ok(path);
        }
        let object = store
            .get(&meta.key)
            .await?
            .ok_or_else(|| IndexError::MissingObject(meta.key.clone()))?;
        if u64::try_from(object.bytes.len()).ok() != Some(meta.bytes) {
            return Err(IndexError::PartSizeMismatch {
                file: meta.key.clone(),
                expected: meta.bytes,
                actual: u64::try_from(object.bytes.len()).unwrap_or(u64::MAX),
            });
        }
        let expected_hash = meta
            .key
            .strip_prefix("parts/")
            .and_then(|value| value.strip_suffix(".parquet"))
            .ok_or_else(|| IndexError::InvalidObjectKey(meta.key.clone()))?;
        if digest(&object.bytes) != expected_hash {
            return Err(IndexError::ObjectHashMismatch(meta.key.clone()));
        }
        self.make_room(meta.bytes, Some(&path))?;
        let temporary = self.root.join(format!("{}.tmp", digest(&object.bytes)));
        let mut file = File::create(&temporary)?;
        file.write_all(&object.bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, &path)?;
        part::validate(&path)?;
        Ok(path)
    }

    fn make_room(&self, incoming: u64, protected: Option<&Path>) -> Result<(), IndexError> {
        if incoming > self.max_bytes {
            return Err(IndexError::CacheCapacity {
                capacity: self.max_bytes,
                object_size: incoming,
            });
        }
        let mut files = Vec::new();
        let mut total = 0_u64;
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() || protected.is_some_and(|protected| protected == path) {
                continue;
            }
            let metadata = entry.metadata()?;
            total = total.saturating_add(metadata.len());
            files.push((
                metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                metadata.len(),
                path,
            ));
        }
        files.sort_unstable_by_key(|(modified, _, _)| *modified);
        for (_, bytes, path) in files {
            if total.saturating_add(incoming) <= self.max_bytes {
                break;
            }
            match fs::remove_file(path) {
                Ok(()) => total = total.saturating_sub(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
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
        Self::open(store.into(), cache_dir, cache_max_bytes, config).await
    }

    pub async fn open_s3(
        store: S3ObjectStore,
        cache_dir: impl AsRef<Path>,
        cache_max_bytes: u64,
        config: EventIndexConfig,
    ) -> Result<Self, IndexError> {
        Self::open(store.into(), cache_dir, cache_max_bytes, config).await
    }

    async fn open(
        store: ObjectStore,
        cache_dir: impl AsRef<Path>,
        cache_max_bytes: u64,
        config: EventIndexConfig,
    ) -> Result<Self, IndexError> {
        validate_config(&config)?;
        let cache = LocalCache::new(cache_dir, cache_max_bytes)?;
        initialize(&store, &config.source_id).await?;
        let published = load_published(&store, &config.source_id).await?;
        Ok(Self {
            store,
            cache,
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
            next.durable_through_record = checkpoint;
            next.parts.extend(metas);
            if self.publish(next).await? {
                self.active.clear();
                return Ok(());
            }
        }
        Err(IndexError::PublishConflict)
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
        if through_record > indexed_through_record {
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
        for key in &stale_manifests {
            self.store.delete(key).await?;
        }
        for key in &stale_parts {
            self.store.delete(key).await?;
        }
        self.refresh().await?;
        Ok(GarbageCollectionReport {
            deleted_parts: stale_parts.len(),
            deleted_manifests: stale_manifests.len(),
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

async fn initialize(store: &ObjectStore, source_id: &str) -> Result<(), IndexError> {
    if store.get(CURRENT_KEY).await?.is_some() {
        return Ok(());
    }
    let manifest = Manifest::new(source_id.to_owned());
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
    use std::time::SystemTime;

    use super::eligible_for_gc;

    #[test]
    fn missing_modification_time_is_not_eligible_for_gc() {
        assert!(!eligible_for_gc(None, SystemTime::UNIX_EPOCH));
    }
}
