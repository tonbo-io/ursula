use std::cmp::Reverse;
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
use crate::cache::EventIndexCache;
use crate::cache::IndexCaches;
use crate::cache::VerifiedParquetReader;
use crate::manifest;
use crate::manifest::CLAIM_KEY;
use crate::manifest::GarbageCollectionReport;
use crate::manifest::Manifest;
use crate::manifest::ManifestBinding;
use crate::manifest::ManifestIdentity;
use crate::manifest::PartMeta;
use crate::manifest::PublishedManifest;
use crate::manifest::SegmentLease;
use crate::object_store::ConditionalWrite;
use crate::object_store::ObjectInfo;
use crate::object_store::ObjectStore;
use crate::object_store::digest;
use crate::part;
use crate::part::PartFilter;
use crate::source::OversizeScan;
use crate::store::Coverage;
use crate::store::IndexBase;
use crate::store::MatchMode;
use crate::store::QueryRequest;
use crate::store::Segment;
use crate::store::SkipCounts;
use crate::store::SkipKind;
use crate::store::SourceBinding;

const MAX_PUBLISH_ATTEMPTS: usize = 8;
const EVENT_TIME_PARTITION_MS: i64 = 24 * 60 * 60 * 1_000;

/// S3-authoritative event-time index over one source stream.
///
/// S3 (or the filesystem development backend) holds the only durable state;
/// the local cache is disposable. Concurrent instances coordinate through
/// immutable content-addressed objects and an ETag compare-and-swap on the
/// `CURRENT` manifest pointer.
pub struct EventIndex {
    store: ObjectStore,
    cache: IndexCaches,
    config: EventIndexConfig,
    binding: ManifestBinding,
    published: PublishedManifest,
    /// In-memory scheduling hint, never persisted: the offset at which this
    /// handle last read source bytes and found no complete message.
    stalled_at: Option<u64>,
    /// In-memory, never persisted: how far this handle's last read scanned
    /// an unterminated oversize line, so the next read resumes there.
    oversize_scan: Option<OversizeScan>,
}

impl EventIndex {
    /// Open the index in `store`, creating it at `base` if the namespace is
    /// empty. An existing index keeps its own base.
    pub async fn open(
        store: impl Into<ObjectStore>,
        cache: EventIndexCache,
        config: EventIndexConfig,
        base: IndexBase,
    ) -> Result<Self, IndexError> {
        let store = store.into();
        validate_config(&config)?;
        let binding = ManifestBinding {
            stream_url: config.source_url.clone(),
            extractor_digest: config.extractor.digest().to_owned(),
        };
        manifest::initialize(&store, &binding, &base).await?;
        let published = manifest::load_published(&store, &binding).await?;
        Ok(Self {
            store,
            cache: cache.0,
            config,
            binding,
            published,
            stalled_at: None,
            oversize_scan: None,
        })
    }

    pub fn config(&self) -> &EventIndexConfig {
        &self.config
    }

    pub fn status(&self) -> &IndexStatus {
        &self.published.manifest.status
    }

    pub fn source(&self) -> &SourceBinding {
        &self.published.manifest.source
    }

    pub fn indexed_from_offset(&self) -> u64 {
        self.published.manifest.indexed_from_offset
    }

    pub fn floor_offset(&self) -> u64 {
        self.published.manifest.floor_offset
    }

    pub fn durable_offset(&self) -> u64 {
        self.published.manifest.durable_offset
    }

    /// A restart point that may not be a message boundary.
    pub fn resync_offset(&self) -> Option<u64> {
        self.published.manifest.resync_offset
    }

    /// Whether this handle's last read found only an unterminated message
    /// and the durable offset has not moved since.
    pub(crate) fn is_stalled(&self) -> bool {
        self.stalled_at == Some(self.published.manifest.durable_offset)
    }

    pub(crate) fn note_stalled(&mut self, offset: u64) {
        self.stalled_at = Some(offset);
    }

    pub(crate) fn oversize_scan(&self) -> Option<OversizeScan> {
        self.oversize_scan
    }

    pub(crate) fn note_oversize_scan(&mut self, scan: Option<OversizeScan>) {
        self.oversize_scan = scan;
    }

    pub fn trimmed_bytes(&self) -> u64 {
        self.published.manifest.trimmed_bytes
    }

    pub fn skipped(&self) -> SkipCounts {
        self.published.manifest.skipped
    }

    pub fn part_count(&self) -> usize {
        self.published.manifest.parts.len()
    }

    pub fn coverage(&self) -> Coverage {
        let manifest = &self.published.manifest;
        Coverage {
            from: manifest.indexed_from_offset,
            floor: manifest.floor_offset,
            through: manifest.durable_offset,
            durable: manifest.durable_offset,
            complete: manifest.trimmed_bytes == 0,
            trimmed_bytes: manifest.trimmed_bytes,
        }
    }

    /// Take the stream's single claim, which starts at the first uncovered
    /// offset and stays open until it commits. Returns `None` when there is
    /// nothing to index yet (or only a partial tail and `allow_partial` is
    /// off) or another worker holds a live claim.
    pub async fn claim_segment(
        &mut self,
        tail: u64,
        segment_bytes: u64,
        allow_partial: bool,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
    ) -> Result<Option<SegmentLease>, IndexError> {
        if segment_bytes == 0 || lease_ms == 0 || worker_id.is_empty() {
            return Err(IndexError::InvalidConfig(
                "segment bytes, lease duration, and worker id must be non-empty",
            ));
        }
        for _attempt in 0..MAX_PUBLISH_ATTEMPTS {
            self.refresh().await?;
            ensure_ready(&self.published.manifest.status)?;
            let start_offset = self.published.manifest.durable_offset;
            if tail <= start_offset
                || (!allow_partial && tail.saturating_sub(start_offset) < segment_bytes)
            {
                return Ok(None);
            }
            let claim = SegmentLease {
                start_offset,
                worker_id: worker_id.to_owned(),
                expires_at_ms: now_ms.saturating_add(lease_ms),
            };
            let bytes = serde_json::to_vec(&claim)?;
            match self.store.put_if_absent(CLAIM_KEY, &bytes).await? {
                ConditionalWrite::Written => return Ok(Some(claim)),
                ConditionalWrite::Conflict => {
                    let Some(stored) = self.store.get(CLAIM_KEY).await? else {
                        continue;
                    };
                    let existing: SegmentLease = serde_json::from_slice(&stored.bytes)?;
                    if existing.expires_at_ms > now_ms && existing.worker_id != worker_id {
                        return Ok(None);
                    }
                    match self
                        .store
                        .compare_and_swap(CLAIM_KEY, &stored.etag, &bytes)
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

    /// Delete the claim if it is still `claim`; a claim another worker took
    /// over after expiry is left alone.
    pub async fn release_claim(&self, claim: &SegmentLease) -> Result<(), IndexError> {
        let Some(stored) = self.store.get(CLAIM_KEY).await? else {
            return Ok(());
        };
        let existing: SegmentLease = serde_json::from_slice(&stored.bytes)?;
        if &existing == claim {
            self.store.delete(CLAIM_KEY).await?;
        }
        Ok(())
    }

    pub async fn finish_segment(
        &mut self,
        claim: &SegmentLease,
        segment: Segment,
    ) -> Result<(), IndexError> {
        if segment.start != claim.start_offset {
            return Err(IndexError::InvalidSourceResponse(
                "segment does not start at its claim",
            ));
        }
        self.commit_segment(segment).await?;
        self.release_claim(claim).await
    }

    /// Publish one segment idempotently. Bytes another worker already
    /// covered must have produced exactly the same entries; only the
    /// uncovered suffix adds entries, skip counts and trimmed bytes. A
    /// segment that starts below the floor is stale and dropped: the next
    /// claim starts at the floor. A fragment discarded at the base belongs
    /// to a message that began before the indexed range, so it is covered
    /// without counting as trimmed history.
    pub async fn commit_segment(&mut self, segment: Segment) -> Result<(), IndexError> {
        validate_segment(&segment)?;
        if segment.end == segment.start {
            return Ok(());
        }
        for _attempt in 0..MAX_PUBLISH_ATTEMPTS {
            self.refresh().await?;
            ensure_ready(&self.published.manifest.status)?;
            let base = self.published.manifest.indexed_from_offset;
            let floor = self.published.manifest.floor_offset;
            let durable = self.published.manifest.durable_offset;
            if segment.start < floor {
                return Ok(());
            }
            if segment.start > durable {
                return Err(IndexError::InvalidSourceResponse(
                    "segment starts beyond the durable offset",
                ));
            }
            let covered_end = durable.min(segment.end);
            if covered_end > segment.start {
                if !segment.is_boundary(covered_end) {
                    return Err(IndexError::EntryConflict {
                        offset: covered_end,
                    });
                }
                self.verify_committed(&segment, covered_end).await?;
            }
            if covered_end >= segment.end {
                return Ok(());
            }
            let entries = segment
                .entries
                .iter()
                .copied()
                .filter(|entry| entry.offset >= covered_end)
                .collect::<Vec<_>>();
            let parts = self.upload_day_partitions(&entries).await?;
            let mut next = self.draft_manifest();
            next.durable_offset = segment.end;
            for skip in segment
                .skips
                .iter()
                .filter(|skip| skip.offset >= covered_end)
            {
                if skip.kind == SkipKind::Trimmed {
                    if skip.offset != base {
                        next.trimmed_bytes = next.trimmed_bytes.saturating_add(skip.len);
                    }
                } else {
                    next.skipped.add(skip.kind);
                }
            }
            if next
                .resync_offset
                .is_some_and(|offset| offset < next.durable_offset)
            {
                next.resync_offset = None;
            }
            next.parts.extend(parts);
            if self.publish(next).await? {
                return Ok(());
            }
        }
        Err(IndexError::PublishConflict)
    }

    /// Require the committed entries in `[segment.start, covered_end)` to
    /// equal this segment's entries there exactly.
    async fn verify_committed(
        &self,
        segment: &Segment,
        covered_end: u64,
    ) -> Result<(), IndexError> {
        let in_range =
            |entry: &EventEntry| entry.offset >= segment.start && entry.offset < covered_end;
        let mut mine = segment
            .entries
            .iter()
            .copied()
            .filter(in_range)
            .collect::<Vec<_>>();
        let mut committed = Vec::new();
        for meta in self
            .published
            .manifest
            .parts
            .iter()
            .filter(|meta| meta.overlaps_offsets(segment.start, covered_end))
        {
            let path = self.cache.parts().materialize(&self.store, meta).await?;
            committed.extend(part::read_all(&path)?.into_iter().filter(in_range));
        }
        mine.sort_unstable_by_key(|entry| entry.offset);
        committed.sort_unstable_by_key(|entry| entry.offset);
        committed.dedup();
        if mine == committed {
            return Ok(());
        }
        let offset = mine
            .iter()
            .zip(&committed)
            .find(|(left, right)| left != right)
            .map(|(left, right)| left.offset.min(right.offset))
            .or_else(|| {
                mine.get(committed.len())
                    .or_else(|| committed.get(mine.len()))
                    .map(|entry| entry.offset)
            })
            .unwrap_or(segment.start);
        Err(IndexError::EntryConflict { offset })
    }

    /// Follow the source's retained offset. Entries before the floor are no
    /// longer returned. If retention passed bytes that were never indexed,
    /// they are counted as trimmed and indexing restarts at `retained`,
    /// which may not be a message boundary.
    pub async fn advance_floor(&mut self, retained: u64) -> Result<(), IndexError> {
        for _attempt in 0..MAX_PUBLISH_ATTEMPTS {
            self.refresh().await?;
            if retained <= self.published.manifest.floor_offset {
                return Ok(());
            }
            let mut next = self.draft_manifest();
            next.floor_offset = retained;
            if retained > next.durable_offset {
                next.trimmed_bytes = next
                    .trimmed_bytes
                    .saturating_add(retained.saturating_sub(next.durable_offset));
                next.durable_offset = retained;
                next.resync_offset = Some(retained);
            }
            if self.publish(next).await? {
                return Ok(());
            }
        }
        Err(IndexError::PublishConflict)
    }

    pub fn needs_partition_compaction(&self, fan_in: usize, max_entries: u64) -> bool {
        fan_in >= 2
            && max_entries > 0
            && select_compaction(&self.published.manifest.parts, fan_in, max_entries).is_some()
    }

    pub async fn refresh(&mut self) -> Result<(), IndexError> {
        let latest = manifest::load_published(&self.store, &self.binding).await?;
        if latest.pointer_etag != self.published.pointer_etag {
            self.published = latest;
        }
        Ok(())
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
                .entry(event_time_partition(entry.t_ms))
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

    pub async fn mark_blocked(&mut self, offset: u64, reason: String) -> Result<(), IndexError> {
        self.refresh().await?;
        self.publish_status(IndexStatus::Blocked { offset, reason })
            .await
    }

    /// Record that the source answered 404 (`gone`) or answers again.
    pub async fn set_source_gone(&mut self, gone: bool) -> Result<(), IndexError> {
        self.refresh().await?;
        let status = match (&self.published.manifest.status, gone) {
            (IndexStatus::Ready, true) => IndexStatus::SourceGone,
            (IndexStatus::SourceGone, false) => IndexStatus::Ready,
            _ => return Ok(()),
        };
        self.publish_status(status).await
    }

    pub async fn clear_blocked(&mut self) -> Result<(), IndexError> {
        self.refresh().await?;
        match self.published.manifest.status {
            IndexStatus::Ready => Ok(()),
            IndexStatus::Blocked { .. } => self.publish_status(IndexStatus::Ready).await,
            IndexStatus::SourceGone => Err(IndexError::CannotResume(
                "the source stream is gone; indexing resumes when it answers again",
            )),
        }
    }

    /// Start over in place for a recreated source stream: publish an empty
    /// manifest for `incarnation` at offset 0, since every byte of the new
    /// stream was appended after this index was created. The old parts
    /// become unreferenced and GC removes them after the grace period. A
    /// no-op if another instance already restarted onto `incarnation`.
    pub async fn restart(&mut self, incarnation: Option<String>) -> Result<(), IndexError> {
        let base = IndexBase {
            offset: 0,
            incarnation,
        };
        self.stalled_at = None;
        self.oversize_scan = None;
        for _attempt in 0..MAX_PUBLISH_ATTEMPTS {
            self.refresh().await?;
            if self.published.manifest.source.incarnation == base.incarnation {
                return Ok(());
            }
            let generation = self.published.manifest.generation.saturating_add(1);
            if self
                .publish(Manifest::new(&self.binding, &base, generation))
                .await?
            {
                return self.store.delete(CLAIM_KEY).await;
            }
        }
        Err(IndexError::PublishConflict)
    }

    pub async fn query(&mut self, request: QueryRequest) -> Result<QueryResult, IndexError> {
        if request.from_ms >= request.until_ms || request.limit == 0 {
            return Err(IndexError::InvalidQuery);
        }
        self.refresh().await?;
        let manifest = &self.published.manifest;
        let through = request.through.unwrap_or(manifest.durable_offset);
        if through > manifest.durable_offset || through < manifest.indexed_from_offset {
            return Err(IndexError::InvalidQuery);
        }
        let filter = PartFilter {
            from_ms: request.from_ms,
            until_ms: request.until_ms,
            overlap: request.match_mode == MatchMode::Overlap,
            floor: manifest.floor_offset,
            through,
            after: request.after.map(|after| (after.t_ms, after.offset)),
        };
        let mut entries = Vec::new();
        for meta in manifest.parts.iter().filter(|meta| meta.may_match(&filter)) {
            let ranges = self.cache.ranges()?;
            let layout = ranges.layout(&self.store, meta).await?;
            let reader = VerifiedParquetReader::new(self.store.clone(), ranges.clone(), layout);
            entries.extend(part::read_part_range_async(reader, filter).await?);
        }
        entries.sort_unstable();
        entries.dedup_by_key(|entry| entry.offset);
        let has_more = entries.len() > request.limit;
        entries.truncate(request.limit);
        let next = has_more
            .then(|| entries.last().copied().map(QueryCursor::from))
            .flatten();
        let mut coverage = self.coverage();
        coverage.through = through;
        Ok(QueryResult {
            source: self.published.manifest.source.clone(),
            coverage,
            skipped: self.published.manifest.skipped,
            entries,
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
            // Entries are unique by offset; those below the floor are no
            // longer fetchable and are dropped.
            let floor = self.published.manifest.floor_offset;
            entries.sort_unstable_by_key(|entry| entry.offset);
            entries.dedup_by_key(|entry| entry.offset);
            entries.retain(|entry| entry.offset >= floor);
            entries.sort_unstable();
            let partition_start_ms = candidate
                .first()
                .map(|meta| meta.partition_start_ms)
                .ok_or(IndexError::InvalidQuery)?;
            let output_level = candidate
                .first()
                .and_then(|meta| meta.level.checked_add(1))
                .ok_or(IndexError::InvalidQuery)?;
            let mut next = self.draft_manifest();
            next.parts
                .retain(|part| !candidate_keys.contains(part.key.as_str()));
            if !entries.is_empty() {
                next.parts.push(
                    self.write_and_upload_part(&entries, output_level, partition_start_ms)
                        .await?,
                );
            }
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
            if !identity.matches(&self.binding) {
                tracing::warn!(
                    manifest = %key,
                    version = identity.version,
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
        let latest = manifest::load_published(&self.store, &self.binding).await?;
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
        if let Some(stored) = self.store.get(CLAIM_KEY).await? {
            match serde_json::from_slice::<SegmentLease>(&stored.bytes) {
                Ok(claim) if now_ms.is_some_and(|now_ms| claim.expires_at_ms <= now_ms) => {
                    stale_claims.push(CLAIM_KEY.to_owned());
                }
                Ok(_) => {}
                Err(error) => tracing::warn!(%error, "skipping an unreadable event-index claim"),
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
            .any(|entry| event_time_partition(entry.t_ms) != partition_start_ms)
        {
            return Err(IndexError::InvalidSourceResponse(
                "part entries cross an event-time partition boundary",
            ));
        }
        let first = entries.first().ok_or(IndexError::InvalidQuery)?;
        let last = entries.last().ok_or(IndexError::InvalidQuery)?;
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
        Ok(PartMeta {
            key,
            layout_key,
            level,
            partition_start_ms,
            entries: u64::try_from(entries.len())
                .map_err(|_error| IndexError::InvalidConfig("part is too large"))?,
            min_t_ms: first.t_ms,
            max_t_ms: last.t_ms,
            max_t_end_ms: entries
                .iter()
                .map(|entry| entry.t_end_ms)
                .max()
                .unwrap_or(last.t_ms),
            min_offset: entries
                .iter()
                .map(|entry| entry.offset)
                .min()
                .unwrap_or(first.offset),
            max_offset: entries
                .iter()
                .map(|entry| entry.offset)
                .max()
                .unwrap_or(last.offset),
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
                self.published = manifest::load_published(&self.store, &self.binding).await?;
                Ok(true)
            }
            ConditionalWrite::Conflict => {
                self.refresh().await?;
                Ok(false)
            }
        }
    }
}

/// Every entry and skip lies inside `[start, end)`.
fn validate_segment(segment: &Segment) -> Result<(), IndexError> {
    let inside = |offset: u64, len: u64| {
        offset >= segment.start
            && offset
                .checked_add(len)
                .is_some_and(|end| end <= segment.end)
    };
    if segment.start > segment.end
        || !segment
            .entries
            .iter()
            .all(|entry| inside(entry.offset, entry.len))
        || !segment
            .skips
            .iter()
            .all(|skip| inside(skip.offset, skip.len))
    {
        return Err(IndexError::InvalidSourceResponse(
            "segment entries lie outside the segment",
        ));
    }
    Ok(())
}

fn event_time_partition(t_ms: i64) -> i64 {
    t_ms.div_euclid(EVENT_TIME_PARTITION_MS)
        .saturating_mul(EVENT_TIME_PARTITION_MS)
}

fn select_compaction(parts: &[PartMeta], fan_in: usize, max_entries: u64) -> Option<Vec<PartMeta>> {
    // Drain higher levels before accepting more work from L0. A continuously
    // written partition otherwise keeps L0 eligible forever and starves every
    // higher level, allowing the manifest and its immutable parts to grow
    // without bound. Within one level, compact older partitions first.
    let mut tiers = BTreeMap::<(Reverse<u8>, i64), Vec<&PartMeta>>::new();
    for part in parts {
        tiers
            .entry((Reverse(part.level), part.partition_start_ms))
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
    if config.row_group_entries == 0 {
        return Err(IndexError::InvalidConfig(
            "row_group_entries must be positive",
        ));
    }
    if config.source_url.is_empty() {
        return Err(IndexError::InvalidConfig("source URL must not be empty"));
    }
    Ok(())
}

fn ensure_ready(status: &IndexStatus) -> Result<(), IndexError> {
    match status {
        IndexStatus::Ready => Ok(()),
        IndexStatus::Blocked { offset, reason } => Err(IndexError::Blocked {
            offset: *offset,
            reason: reason.clone(),
        }),
        IndexStatus::SourceGone => Err(IndexError::SourceGone),
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use crate::EventEntry;
    use crate::EventIndexConfig;
    use crate::cache::EventIndexCache;
    use crate::extract::Extractor;
    use crate::index::EventIndex;
    use crate::index::eligible_for_gc;
    use crate::index::select_compaction;
    use crate::manifest::PartMeta;
    use crate::object_store::FsObjectStore;
    use crate::store::IndexBase;
    use crate::store::QueryRequest;
    use crate::store::Segment;

    #[test]
    fn missing_modification_time_is_not_eligible_for_gc() {
        assert!(!eligible_for_gc(None, SystemTime::UNIX_EPOCH));
    }

    #[test]
    fn compaction_prioritizes_higher_levels_under_continuous_l0_writes() {
        let mut parts = Vec::new();
        for level in [0_u8, 1_u8] {
            for ordinal in 0..8_u64 {
                parts.push(PartMeta {
                    key: format!("level-{level}-part-{ordinal}"),
                    layout_key: format!("level-{level}-layout-{ordinal}"),
                    level,
                    partition_start_ms: 0,
                    entries: 1,
                    min_t_ms: 0,
                    max_t_ms: 0,
                    max_t_end_ms: 0,
                    min_offset: ordinal,
                    max_offset: ordinal,
                    bytes: 1,
                });
            }
        }

        let selected = select_compaction(&parts, 8, 8).expect("both levels are eligible");
        assert_eq!(selected.len(), 8);
        assert!(selected.iter().all(|part| part.level == 1));
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
        let mut config =
            EventIndexConfig::new("narrow-range-test", Extractor::timestamp_field("t")?);
        config.row_group_entries = 1_000;
        let mut index = EventIndex::open(
            store.clone(),
            EventIndexCache::serving(cache.path(), 16 * 1024 * 1024)?,
            config,
            IndexBase::default(),
        )
        .await?;
        let entries = (0..10_000_u64)
            .map(|ordinal| -> anyhow::Result<EventEntry> {
                Ok(EventEntry {
                    t_ms: i64::try_from(ordinal)?,
                    t_end_ms: i64::try_from(ordinal)?,
                    offset: ordinal.saturating_mul(10),
                    len: 10,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        index
            .commit_segment(Segment {
                start: 0,
                end: 100_000,
                entries,
                skips: Vec::new(),
            })
            .await?;
        let part_bytes = index
            .published
            .manifest
            .parts
            .first()
            .ok_or_else(|| anyhow::anyhow!("commit did not publish a part"))?
            .bytes;
        let result = index.query(QueryRequest::window(4_500, 4_510, 100)).await?;
        assert_eq!(result.entries.len(), 10);
        assert!(store.range_read_bytes() < part_bytes);
        Ok(())
    }
}
