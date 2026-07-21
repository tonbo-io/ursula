use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fs;
use std::time::Duration;
use std::time::SystemTime;

use crate::EventEntry;
use crate::EventIndexConfig;
use crate::IndexError;
use crate::IndexStatus;
use crate::QueryCursor;
use crate::QueryResult;
use crate::SourceEnvelope;
use crate::cache::EventIndexCache;
use crate::cache::IndexCaches;
use crate::cache::VerifiedParquetReader;
use crate::manifest;
use crate::manifest::CompletedRecordRange;
use crate::manifest::GarbageCollectionReport;
use crate::manifest::Manifest;
use crate::manifest::ManifestIdentity;
use crate::manifest::PartMeta;
use crate::manifest::PublishedManifest;
use crate::manifest::RecordSegmentLease;
use crate::object_store::ConditionalWrite;
use crate::object_store::ObjectInfo;
use crate::object_store::ObjectStore;
use crate::object_store::digest;
use crate::part;

const MAX_PUBLISH_ATTEMPTS: usize = 8;
const EVENT_TIME_PARTITION_MS: i64 = 24 * 60 * 60 * 1_000;

/// S3-authoritative event-time index over one JSON record stream.
///
/// S3 (or the filesystem development backend) holds the only durable state;
/// the local cache is disposable. Concurrent instances coordinate through
/// immutable content-addressed objects and an ETag compare-and-swap on the
/// `CURRENT` manifest pointer.
pub struct EventIndex {
    store: ObjectStore,
    cache: IndexCaches,
    config: EventIndexConfig,
    published: PublishedManifest,
    active: Vec<EventEntry>,
}

impl EventIndex {
    /// Open an index whose base is source record zero.
    pub async fn open(
        store: impl Into<ObjectStore>,
        cache: EventIndexCache,
        config: EventIndexConfig,
    ) -> Result<Self, IndexError> {
        Self::open_from_record(store, cache, config, 0).await
    }

    /// Open an index over a retained stream whose oldest readable source
    /// record is `indexed_from_record`.
    pub async fn open_from_record(
        store: impl Into<ObjectStore>,
        cache: EventIndexCache,
        config: EventIndexConfig,
        indexed_from_record: u64,
    ) -> Result<Self, IndexError> {
        let store = store.into();
        validate_config(&config)?;
        manifest::initialize(&store, &config.source_id, indexed_from_record).await?;
        let published = manifest::load_published(&store, &config.source_id).await?;
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
            for (_key, claim) in self.load_claims().await? {
                if claim.expires_at_ms > now_ms {
                    covered.push(CompletedRecordRange {
                        start_record: claim.start_record,
                        end_record: claim.end_record,
                    });
                }
            }
            manifest::normalize_completed_ranges(&mut covered);
            let start_record = manifest::first_uncovered_record(
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
        let latest = manifest::load_published(&self.store, &self.config.source_id).await?;
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
        let committed = self
            .committed_entries_between(first_record, checkpoint.saturating_sub(1))
            .await?;
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
        let entry = self.envelope_entry(&envelope)?;
        self.ingest(entry).await
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
        if self.active.is_empty() {
            return Ok(());
        }
        let entries = self.active.clone();
        self.publish_entries(&entries).await?;
        self.active.clear();
        Ok(())
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
            entries.push(self.envelope_entry(&envelope)?);
        }
        self.publish_entries(&entries).await
    }

    /// Publish entries idempotently: already-covered records are verified
    /// against their committed values, the rest are uploaded and added to the
    /// manifest under conditional-publish retry.
    async fn publish_entries(&mut self, entries: &[EventEntry]) -> Result<(), IndexError> {
        for _attempt in 0..MAX_PUBLISH_ATTEMPTS {
            self.refresh().await?;
            ensure_ready(&self.published.manifest.status)?;
            let (committed, uncovered): (Vec<_>, Vec<_>) =
                entries.iter().copied().partition(|entry| {
                    manifest::record_is_covered(
                        &self.published.manifest.completed_record_ranges,
                        entry.record,
                    )
                });
            self.verify_committed_entries(&committed).await?;
            if uncovered.is_empty() {
                return Ok(());
            }
            let new_ranges = manifest::completed_ranges_for_entries(&uncovered)?;
            let uploaded_parts = self.upload_day_partitions(&uncovered).await?;
            let mut next = self.draft_manifest();
            next.completed_record_ranges.extend(new_ranges);
            manifest::normalize_completed_ranges(&mut next.completed_record_ranges);
            next.durable_through_record = manifest::contiguous_watermark(
                &next.completed_record_ranges,
                next.indexed_from_record,
            );
            next.parts.extend(uploaded_parts);
            if self.publish(next).await? {
                return Ok(());
            }
        }
        Err(IndexError::PublishConflict)
    }

    fn envelope_entry(&self, envelope: &SourceEnvelope) -> Result<EventEntry, IndexError> {
        let captured_at_ms = envelope
            .value
            .get(&self.config.timestamp_field)
            .and_then(crate::store::parse_timestamp)
            .ok_or_else(|| IndexError::InvalidTimestamp {
                record: envelope.record,
                field: self.config.timestamp_field.clone(),
            })?;
        Ok(EventEntry {
            captured_at_ms,
            record: envelope.record,
        })
    }

    /// Clone the published manifest as the base of the next generation.
    fn draft_manifest(&self) -> Manifest {
        let mut next = self.published.manifest.clone();
        next.generation = next.generation.saturating_add(1);
        next
    }

    /// Split entries into UTC-day event-time partitions and upload one sorted
    /// level-0 part per partition.
    async fn upload_day_partitions(
        &self,
        entries: &[EventEntry],
    ) -> Result<Vec<PartMeta>, IndexError> {
        let mut partitions = BTreeMap::<i64, Vec<EventEntry>>::new();
        for entry in entries.iter().copied() {
            partitions
                .entry(event_time_partition(entry.captured_at_ms))
                .or_default()
                .push(entry);
        }
        let mut metas = Vec::with_capacity(partitions.len());
        for (partition_start_ms, mut partition_entries) in partitions {
            partition_entries.sort_unstable();
            metas.push(
                self.write_and_upload_part(&partition_entries, 0, partition_start_ms)
                    .await?,
            );
        }
        Ok(metas)
    }

    /// Materialize every published part overlapping the inclusive record range
    /// and return its entries.
    async fn committed_entries_between(
        &self,
        min_record: u64,
        max_record: u64,
    ) -> Result<Vec<EventEntry>, IndexError> {
        let mut committed = Vec::new();
        for meta in self
            .published
            .manifest
            .parts
            .iter()
            .filter(|meta| meta.max_record >= min_record && meta.min_record <= max_record)
        {
            let path = self.cache.parts().materialize(&self.store, meta).await?;
            committed.extend(part::read_all(&path)?);
        }
        Ok(committed)
    }

    async fn verify_committed_entries(&self, expected: &[EventEntry]) -> Result<(), IndexError> {
        let (Some(first), Some(last)) = (expected.first(), expected.last()) else {
            return Ok(());
        };
        let committed = self
            .committed_entries_between(first.record, last.record)
            .await?;
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
            let ranges = self.cache.ranges()?;
            let layout = ranges.layout(&self.store, meta).await?;
            let reader = VerifiedParquetReader::new(self.store.clone(), ranges.clone(), layout);
            entries.extend(part::read_part_range_async(reader, from_ms, until_ms).await?);
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
                let path = self.cache.parts().materialize(&self.store, meta).await?;
                entries.extend(part::read_all(&path)?);
            }
            entries.sort_unstable();
            entries.dedup_by_key(|entry| entry.record);
            let partition_start_ms = candidate
                .first()
                .map(|meta| meta.partition_start_ms)
                .ok_or(IndexError::InvalidQuery)?;
            let output_level = candidate
                .first()
                .and_then(|meta| meta.level.checked_add(1))
                .ok_or(IndexError::InvalidQuery)?;
            let meta = self
                .write_and_upload_part(&entries, output_level, partition_start_ms)
                .await?;
            let mut next = self.draft_manifest();
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
            let Some(generation) = manifest::manifest_generation(&object.key) else {
                continue;
            };
            if generation >= minimum_generation && generation < current_generation {
                retained_manifests.insert(object.key.clone());
            }
        }
        let mut retained_parts = HashSet::new();
        let mut retained_layouts = HashSet::new();
        let retained_manifest_keys = retained_manifests.iter().cloned().collect::<Vec<_>>();
        for key in &retained_manifest_keys {
            let object = self
                .store
                .get(key)
                .await?
                .ok_or_else(|| IndexError::MissingObject(key.clone()))?;
            let identity: ManifestIdentity = serde_json::from_slice(&object.bytes)?;
            if identity.version != manifest::FORMAT_VERSION
                || identity.source_id != self.config.source_id
            {
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
            for part in manifest.parts {
                retained_parts.insert(part.key);
                retained_layouts.insert(part.layout_key);
            }
        }
        let part_objects = self.store.list("parts/").await?;
        let layout_objects = self.store.list("layouts/").await?;
        let latest = manifest::load_published(&self.store, &self.config.source_id).await?;
        let durable_through_record = latest.manifest.durable_through_record;
        retained_manifests.insert(latest.manifest_key);
        for part in latest.manifest.parts {
            retained_parts.insert(part.key);
            retained_layouts.insert(part.layout_key);
        }
        let stale_manifests = stale_keys(manifest_objects, ".json", cutoff, &retained_manifests);
        let stale_parts = stale_keys(part_objects, ".parquet", cutoff, &retained_parts);
        let stale_layouts = stale_keys(layout_objects, ".json", cutoff, &retained_layouts);
        let now_ms = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .and_then(|duration| u64::try_from(duration.as_millis()).ok());
        let mut stale_claims = Vec::new();
        for (key, claim) in self.load_claims().await? {
            if claim.end_record <= durable_through_record
                || now_ms.is_some_and(|now_ms| claim.expires_at_ms <= now_ms)
            {
                stale_claims.push(key);
            }
        }
        for key in stale_manifests
            .iter()
            .chain(&stale_parts)
            .chain(&stale_layouts)
            .chain(&stale_claims)
        {
            self.store.delete(key).await?;
        }
        self.refresh().await?;
        Ok(GarbageCollectionReport {
            deleted_parts: stale_parts.len(),
            deleted_layouts: stale_layouts.len(),
            deleted_manifests: stale_manifests.len(),
            deleted_claims: stale_claims.len(),
        })
    }

    async fn load_claims(&self) -> Result<Vec<(String, RecordSegmentLease)>, IndexError> {
        let mut claims = Vec::new();
        for object in self.store.list("claims/").await? {
            if !object.key.ends_with(".json") {
                continue;
            }
            let Some(stored) = self.store.get(&object.key).await? else {
                continue;
            };
            let claim: RecordSegmentLease = serde_json::from_slice(&stored.bytes)?;
            claims.push((object.key, claim));
        }
        Ok(claims)
    }

    async fn publish_status(&mut self, status: IndexStatus) -> Result<(), IndexError> {
        for _attempt in 0..MAX_PUBLISH_ATTEMPTS {
            self.refresh().await?;
            let mut next = self.draft_manifest();
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
        let temporary = tempfile::NamedTempFile::new_in(self.cache.parts().directory())?;
        part::write_part(temporary.path(), entries, self.config.row_group_entries)?;
        let bytes = fs::read(temporary.path())?;
        let hash = digest(&bytes);
        let key = format!("parts/{hash}.parquet");
        let layout = part::build_layout(temporary.path(), key.clone(), &bytes)?;
        let layout_bytes = serde_json::to_vec(&layout)?;
        let layout_key = format!("layouts/{}.json", digest(&layout_bytes));
        let _write = self.store.put_if_absent(&key, &bytes).await?;
        let _layout_write = self.store.put_if_absent(&layout_key, &layout_bytes).await?;
        let first = entries.first().ok_or(IndexError::InvalidQuery)?;
        let last = entries.last().ok_or(IndexError::InvalidQuery)?;
        Ok(PartMeta {
            key,
            layout_key,
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
        let (key, bytes, pointer_bytes) = next.encode()?;
        let _write = self.store.put_if_absent(&key, &bytes).await?;
        match self
            .store
            .compare_and_swap(
                manifest::CURRENT_KEY,
                &self.published.pointer_etag,
                &pointer_bytes,
            )
            .await?
        {
            ConditionalWrite::Written => {
                self.published =
                    manifest::load_published(&self.store, &self.config.source_id).await?;
                Ok(true)
            }
            ConditionalWrite::Conflict => {
                self.refresh().await?;
                Ok(false)
            }
        }
    }
}

fn event_time_partition(captured_at_ms: i64) -> i64 {
    captured_at_ms
        .div_euclid(EVENT_TIME_PARTITION_MS)
        .saturating_mul(EVENT_TIME_PARTITION_MS)
}

fn select_compaction(parts: &[PartMeta], fan_in: usize, max_entries: u64) -> Option<Vec<PartMeta>> {
    let mut tiers = BTreeMap::<(i64, u8), Vec<&PartMeta>>::new();
    for part in parts {
        tiers
            .entry((part.partition_start_ms, part.level))
            .or_default()
            .push(part);
    }
    tiers.into_iter().find_map(|(_, mut parts)| {
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

fn stale_keys(
    objects: Vec<ObjectInfo>,
    suffix: &str,
    cutoff: SystemTime,
    retained: &HashSet<String>,
) -> Vec<String> {
    objects
        .into_iter()
        .filter(|object| object.key.ends_with(suffix))
        .filter(|object| eligible_for_gc(object.modified, cutoff))
        .filter(|object| !retained.contains(&object.key))
        .map(|object| object.key)
        .collect()
}

fn eligible_for_gc(modified: Option<SystemTime>, cutoff: SystemTime) -> bool {
    modified.is_some_and(|modified| modified <= cutoff)
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

    use crate::EventEntry;
    use crate::EventIndexConfig;
    use crate::cache::EventIndexCache;
    use crate::index::EventIndex;
    use crate::index::eligible_for_gc;
    use crate::object_store::FsObjectStore;

    #[test]
    fn missing_modification_time_is_not_eligible_for_gc() {
        assert!(!eligible_for_gc(None, SystemTime::UNIX_EPOCH));
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the test combines fallible setup with assertions"
    )]
    async fn narrow_query_reads_verified_parquet_pages_instead_of_the_whole_part()
    -> anyhow::Result<()> {
        let objects = tempfile::TempDir::new()?;
        let cache = tempfile::TempDir::new()?;
        let store = FsObjectStore::new(objects.path())?;
        let mut index = EventIndex::open(
            store.clone(),
            EventIndexCache::serving(cache.path(), 16 * 1024 * 1024)?,
            EventIndexConfig {
                source_id: "narrow-range-test".to_owned(),
                flush_entries: 10_000,
                row_group_entries: 1_000,
                timestamp_field: "captured_at".to_owned(),
            },
        )
        .await?;
        for record in 0..10_000_u64 {
            index
                .ingest(EventEntry {
                    captured_at_ms: i64::try_from(record)?,
                    record,
                })
                .await?;
        }
        let part_bytes = index
            .published
            .manifest
            .parts
            .first()
            .ok_or_else(|| anyhow::anyhow!("flush did not publish a part"))?
            .bytes;
        let result = index.query(4_500, 4_510, None, None, 100).await?;
        assert_eq!(result.records.len(), 10);
        assert!(store.range_read_bytes() < part_bytes);
        Ok(())
    }
}
