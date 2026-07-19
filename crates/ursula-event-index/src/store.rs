use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use chrono::DateTime;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

use crate::part;

const MANIFEST_VERSION: u32 = 1;
const MANIFEST_FILE: &str = "manifest.json";

#[derive(Clone, Debug)]
pub struct EventIndexConfig {
    pub source_id: String,
    pub flush_entries: usize,
    pub row_group_entries: usize,
    pub timestamp_field: String,
}

impl Default for EventIndexConfig {
    fn default() -> Self {
        Self {
            source_id: "default".to_owned(),
            flush_entries: 65_536,
            row_group_entries: 16_384,
            timestamp_field: "captured_at".to_owned(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct EventEntry {
    pub captured_at_ms: i64,
    pub record: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct QueryCursor {
    pub captured_at_ms: i64,
    pub record: u64,
}

impl From<EventEntry> for QueryCursor {
    fn from(value: EventEntry) -> Self {
        Self {
            captured_at_ms: value.captured_at_ms,
            record: value.record,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum IndexStatus {
    Ready,
    Blocked {
        record: u64,
        reason: String,
    },
    RetentionGap {
        expected_record: u64,
        first_available_record: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SourceEnvelope {
    pub record: u64,
    pub value: Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QueryResult {
    pub indexed_through_record: u64,
    pub durable_through_record: u64,
    pub through_record: u64,
    pub records: Vec<EventEntry>,
    pub next: Option<QueryCursor>,
}

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("manifest JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("source HTTP error: {0}")]
    SourceHttp(#[from] reqwest::Error),
    #[error("source returned HTTP {0}")]
    SourceStatus(u16),
    #[error("invalid source response: {0}")]
    InvalidSourceResponse(&'static str),
    #[error("event index lock poisoned")]
    LockPoisoned,
    #[error("blocking event-index worker failed")]
    WorkerFailed,
    #[error("event index is blocked at source record {record}: {reason}")]
    Blocked { record: u64, reason: String },
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("unsupported manifest version {0}")]
    ManifestVersion(u32),
    #[error("index source is `{stored}`, not configured source `{configured}`")]
    SourceMismatch { stored: String, configured: String },
    #[error("event index data directory is already open by another writer")]
    AlreadyOpen,
    #[error("invalid configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("expected source record {expected}, received {actual}")]
    UnexpectedRecord { expected: u64, actual: u64 },
    #[error("record {record} has no valid `{field}` timestamp")]
    InvalidTimestamp { record: u64, field: String },
    #[error(
        "source retention gap: expected record {expected_record}, first available is {first_available_record}"
    )]
    RetentionGap {
        expected_record: u64,
        first_available_record: u64,
    },
    #[error("invalid query range or watermark")]
    InvalidQuery,
    #[error("index part is missing: {0}")]
    MissingPart(String),
    #[error("index part size changed for {file}: manifest={expected}, actual={actual}")]
    PartSizeMismatch {
        file: String,
        expected: u64,
        actual: u64,
    },
    #[error("index Parquet part has an incompatible schema")]
    InvalidPartSchema,
    #[error("object store error: {0}")]
    ObjectStore(String),
    #[error("object `{0}` has no entity tag; conditional updates are unavailable")]
    MissingEtag(String),
    #[error("invalid object key: {0}")]
    InvalidObjectKey(String),
    #[error("event-index object is missing: {0}")]
    MissingObject(String),
    #[error("event-index object failed its content hash: {0}")]
    ObjectHashMismatch(String),
    #[error("conditional manifest publication repeatedly conflicted")]
    PublishConflict,
    #[error("record {record} differs from the value already committed by another indexer")]
    RecordConflict { record: u64 },
    #[error("cache capacity {capacity} bytes cannot hold a {object_size}-byte part")]
    CacheCapacity { capacity: u64, object_size: u64 },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PartMeta {
    file: String,
    level: u8,
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

impl Manifest {
    fn new(source_id: String) -> Self {
        Self {
            version: MANIFEST_VERSION,
            source_id,
            generation: 0,
            durable_through_record: 0,
            status: IndexStatus::Ready,
            parts: Vec::new(),
        }
    }
}

pub struct LocalEventIndex {
    _lock: File,
    root: PathBuf,
    config: EventIndexConfig,
    manifest: Manifest,
    active: Vec<EventEntry>,
}

impl LocalEventIndex {
    pub fn open(root: impl AsRef<Path>, config: EventIndexConfig) -> Result<Self, IndexError> {
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
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("LOCK"))?;
        lock.try_lock().map_err(|_error| IndexError::AlreadyOpen)?;
        let manifest_path = root.join(MANIFEST_FILE);
        let manifest = if manifest_path.exists() {
            serde_json::from_slice::<Manifest>(&fs::read(&manifest_path)?)?
        } else {
            Manifest::new(config.source_id.clone())
        };
        if manifest.version != MANIFEST_VERSION {
            return Err(IndexError::ManifestVersion(manifest.version));
        }
        if manifest.source_id != config.source_id {
            return Err(IndexError::SourceMismatch {
                stored: manifest.source_id,
                configured: config.source_id,
            });
        }
        for meta in &manifest.parts {
            let path = root.join(&meta.file);
            if !path.is_file() {
                return Err(IndexError::MissingPart(meta.file.clone()));
            }
            let actual = fs::metadata(&path)?.len();
            if actual != meta.bytes {
                return Err(IndexError::PartSizeMismatch {
                    file: meta.file.clone(),
                    expected: meta.bytes,
                    actual,
                });
            }
            part::validate(&path)?;
        }
        cleanup_orphans(&root, &manifest)?;
        Ok(Self {
            _lock: lock,
            root,
            config,
            manifest,
            active: Vec::new(),
        })
    }

    pub fn status(&self) -> &IndexStatus {
        &self.manifest.status
    }

    pub fn durable_through_record(&self) -> u64 {
        self.manifest.durable_through_record
    }

    pub fn part_count(&self) -> usize {
        self.manifest.parts.len()
    }

    pub fn indexed_through_record(&self) -> u64 {
        self.active
            .last()
            .map_or(self.manifest.durable_through_record, |entry| {
                entry.record.saturating_add(1)
            })
    }

    pub fn ingest_envelope(&mut self, envelope: SourceEnvelope) -> Result<(), IndexError> {
        let timestamp = envelope
            .value
            .get(&self.config.timestamp_field)
            .and_then(parse_timestamp);
        let captured_at_ms = timestamp.ok_or_else(|| IndexError::InvalidTimestamp {
            record: envelope.record,
            field: self.config.timestamp_field.clone(),
        })?;
        self.ingest(EventEntry {
            captured_at_ms,
            record: envelope.record,
        })
    }

    pub fn ingest(&mut self, entry: EventEntry) -> Result<(), IndexError> {
        let expected = self.indexed_through_record();
        if entry.record != expected {
            return Err(IndexError::UnexpectedRecord {
                expected,
                actual: entry.record,
            });
        }
        if !matches!(self.manifest.status, IndexStatus::Ready) {
            return Err(match &self.manifest.status {
                IndexStatus::Ready => IndexError::InvalidQuery,
                IndexStatus::Blocked { record, reason } => IndexError::Blocked {
                    record: *record,
                    reason: reason.clone(),
                },
                IndexStatus::RetentionGap {
                    expected_record,
                    first_available_record,
                } => IndexError::RetentionGap {
                    expected_record: *expected_record,
                    first_available_record: *first_available_record,
                },
            });
        }
        self.active.push(entry);
        if self.active.len() >= self.config.flush_entries {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), IndexError> {
        if self.active.is_empty() {
            return Ok(());
        }
        let mut entries = self.active.clone();
        entries.sort_unstable();
        let generation = self.manifest.generation.saturating_add(1);
        let file = format!("part-{generation:020}-l0.parquet");
        let temporary = self.root.join(format!("{file}.tmp"));
        let final_path = self.root.join(&file);
        part::write_part(&temporary, &entries, self.config.row_group_entries)?;
        fs::rename(&temporary, &final_path)?;
        sync_directory(&self.root)?;
        let metadata = fs::metadata(&final_path)?;
        let first = entries.first().ok_or(IndexError::InvalidQuery)?;
        let last = entries.last().ok_or(IndexError::InvalidQuery)?;
        let min_record = entries
            .iter()
            .map(|entry| entry.record)
            .min()
            .ok_or(IndexError::InvalidQuery)?;
        let max_record = entries
            .iter()
            .map(|entry| entry.record)
            .max()
            .ok_or(IndexError::InvalidQuery)?;
        let mut next = self.manifest.clone();
        next.generation = generation;
        next.durable_through_record = self.indexed_through_record();
        next.parts.push(PartMeta {
            file,
            level: 0,
            entries: u64::try_from(entries.len())
                .map_err(|_error| IndexError::InvalidConfig("part is too large"))?,
            min_captured_at_ms: first.captured_at_ms,
            max_captured_at_ms: last.captured_at_ms,
            min_record,
            max_record,
            bytes: metadata.len(),
        });
        publish_manifest(&self.root, &next)?;
        self.manifest = next;
        self.active.clear();
        Ok(())
    }

    pub fn mark_retention_gap(&mut self, first_available_record: u64) -> Result<(), IndexError> {
        self.flush()?;
        let expected_record = self.indexed_through_record();
        if first_available_record <= expected_record {
            return Err(IndexError::InvalidSourceResponse(
                "retention gap does not advance beyond expected record",
            ));
        }
        let mut next = self.manifest.clone();
        next.generation = next.generation.saturating_add(1);
        next.status = IndexStatus::RetentionGap {
            expected_record,
            first_available_record,
        };
        publish_manifest(&self.root, &next)?;
        self.manifest = next;
        Err(IndexError::RetentionGap {
            expected_record,
            first_available_record,
        })
    }

    pub fn mark_blocked(&mut self, record: u64, reason: String) -> Result<(), IndexError> {
        self.flush()?;
        if record != self.indexed_through_record() {
            return Err(IndexError::UnexpectedRecord {
                expected: self.indexed_through_record(),
                actual: record,
            });
        }
        let mut next = self.manifest.clone();
        next.generation = next.generation.saturating_add(1);
        next.status = IndexStatus::Blocked { record, reason };
        publish_manifest(&self.root, &next)?;
        self.manifest = next;
        Ok(())
    }

    pub fn query(
        &self,
        from_ms: i64,
        until_ms: i64,
        after: Option<QueryCursor>,
        through_record: Option<u64>,
        limit: usize,
    ) -> Result<QueryResult, IndexError> {
        if from_ms >= until_ms || limit == 0 {
            return Err(IndexError::InvalidQuery);
        }
        if let IndexStatus::RetentionGap {
            expected_record,
            first_available_record,
        } = self.manifest.status
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
            .manifest
            .parts
            .iter()
            .filter(|meta| meta.overlaps(from_ms, until_ms))
        {
            entries.extend(part::read_part_range(
                &self.root.join(&meta.file),
                from_ms,
                until_ms,
            )?);
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
            durable_through_record: self.manifest.durable_through_record,
            through_record,
            records: entries,
            next,
        })
    }

    pub fn compact_all(&mut self) -> Result<(), IndexError> {
        self.flush()?;
        if self.manifest.parts.len() <= 1 {
            return Ok(());
        }
        let old_parts = self.manifest.parts.clone();
        let mut entries = Vec::new();
        for meta in &old_parts {
            entries.extend(part::read_all(&self.root.join(&meta.file))?);
        }
        entries.sort_unstable();
        entries.dedup_by_key(|entry| entry.record);
        let generation = self.manifest.generation.saturating_add(1);
        let file = format!("part-{generation:020}-l1.parquet");
        let temporary = self.root.join(format!("{file}.tmp"));
        let final_path = self.root.join(&file);
        part::write_part(&temporary, &entries, self.config.row_group_entries)?;
        fs::rename(&temporary, &final_path)?;
        sync_directory(&self.root)?;
        let metadata = fs::metadata(&final_path)?;
        let first = entries.first().ok_or(IndexError::InvalidQuery)?;
        let last = entries.last().ok_or(IndexError::InvalidQuery)?;
        let min_record = entries
            .iter()
            .map(|entry| entry.record)
            .min()
            .ok_or(IndexError::InvalidQuery)?;
        let max_record = entries
            .iter()
            .map(|entry| entry.record)
            .max()
            .ok_or(IndexError::InvalidQuery)?;
        let mut next = self.manifest.clone();
        next.generation = generation;
        next.parts = vec![PartMeta {
            file,
            level: 1,
            entries: u64::try_from(entries.len())
                .map_err(|_error| IndexError::InvalidConfig("part is too large"))?,
            min_captured_at_ms: first.captured_at_ms,
            max_captured_at_ms: last.captured_at_ms,
            min_record,
            max_record,
            bytes: metadata.len(),
        }];
        publish_manifest(&self.root, &next)?;
        self.manifest = next;
        for meta in old_parts {
            match fs::remove_file(self.root.join(&meta.file)) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(IndexError::Io(error)),
            }
        }
        sync_directory(&self.root)?;
        Ok(())
    }
}

pub(crate) fn parse_timestamp(value: &Value) -> Option<i64> {
    match value {
        Value::String(value) => DateTime::parse_from_rfc3339(value)
            .ok()
            .map(|value| value.timestamp_millis()),
        Value::Number(value) => value.as_i64(),
        _ => None,
    }
}

fn publish_manifest(root: &Path, manifest: &Manifest) -> Result<(), IndexError> {
    let temporary = root.join("manifest.json.tmp");
    let final_path = root.join(MANIFEST_FILE);
    let bytes = serde_json::to_vec_pretty(manifest)?;
    let mut file = File::create(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, &final_path)?;
    sync_directory(root)?;
    Ok(())
}

fn cleanup_orphans(root: &Path, manifest: &Manifest) -> Result<(), IndexError> {
    let referenced = manifest
        .parts
        .iter()
        .map(|meta| meta.file.as_str())
        .collect::<HashSet<_>>();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let temporary = name.ends_with(".tmp");
        let orphan_part = name.ends_with(".parquet") && !referenced.contains(name.as_ref());
        if temporary || orphan_part {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), IndexError> {
    File::open(path)?.sync_all()?;
    Ok(())
}
