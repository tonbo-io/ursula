use chrono::DateTime;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

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
    pub indexed_from_record: u64,
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
    #[error("source response did not advertise json-record-coordinates-v1")]
    MissingRecordCoordinates,
    #[error("event index lock poisoned")]
    LockPoisoned,
    #[error("blocking event-index worker failed")]
    WorkerFailed,
    #[error("event index is blocked at source record {record}: {reason}")]
    Blocked { record: u64, reason: String },
    #[error("index status cannot be resumed: {0}")]
    CannotResume(&'static str),
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("unsupported manifest version {0}")]
    ManifestVersion(u32),
    #[error("index source is `{stored}`, not configured source `{configured}`")]
    SourceMismatch { stored: String, configured: String },
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
    #[error("index part size changed for {file}: manifest={expected}, actual={actual}")]
    PartSizeMismatch {
        file: String,
        expected: u64,
        actual: u64,
    },
    #[error("index Parquet part has an incompatible schema")]
    InvalidPartSchema,
    #[error("index Parquet part has an invalid verified-range layout: {0}")]
    InvalidPartLayout(String),
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
    #[error(
        "compaction candidate has {entries} entries, exceeding configured maximum {max_entries}"
    )]
    CompactionTooLarge { entries: u64, max_entries: u64 },
    #[error("record {record} differs from the value already committed by another indexer")]
    RecordConflict { record: u64 },
    #[error("cache capacity {capacity} bytes cannot hold a {object_size}-byte part")]
    CacheCapacity { capacity: u64, object_size: u64 },
    #[error("index registration `{0}` already exists with different settings")]
    RegistrationConflict(String),
    #[error("index registration `{0}` does not exist")]
    UnknownIndex(String),
    #[error("index starts at source record {stored}, not configured record {configured}")]
    IndexBaseMismatch { stored: u64, configured: u64 },
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
