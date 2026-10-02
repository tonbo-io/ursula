use chrono::DateTime;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::de::IgnoredAny;
use serde::de::MapAccess;
use serde::de::Visitor;
use serde_json::Value;
use serde_json::value::RawValue;
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

/// One record of the source's envelope view. `value` is the stored JSON
/// message text, kept raw: under JSON Message Text (P1) it may contain escapes
/// of unpaired surrogates, which `serde_json::Value` cannot represent.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SourceEnvelope {
    pub record: u64,
    pub value: Box<RawValue>,
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
    #[error("keyed record {record} cannot be applied: {reason}")]
    InvalidKeyedRecord { record: u64, reason: String },
    #[error("invalid keyed projection state: {0}")]
    InvalidKeyedState(String),
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

/// Event time of a record: the `field` member of a top-level JSON object,
/// parsed by [`parse_timestamp`]. Only that member's value is decoded; other
/// members (and keys) are skipped lexically, so a lone-surrogate escape
/// elsewhere in the record does not fail the ingest. With duplicate members
/// the last one wins, as in `JSON.parse`.
pub(crate) fn record_timestamp(value: &RawValue, field: &str) -> Option<i64> {
    let mut deserializer = serde_json::Deserializer::from_str(value.get());
    let member = (&mut deserializer)
        .deserialize_map(MemberVisitor { field })
        .ok()
        .flatten()?;
    let member: Value = serde_json::from_str(member.get()).ok()?;
    parse_timestamp(&member)
}

struct MemberVisitor<'f> {
    field: &'f str,
}

impl<'de> Visitor<'de> for MemberVisitor<'_> {
    type Value = Option<&'de RawValue>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where A: MapAccess<'de> {
        let mut found = None;
        while let Some(key) = map.next_key::<&'de RawValue>()? {
            if key_matches(key.get(), self.field) {
                found = Some(map.next_value::<&'de RawValue>()?);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(found)
    }
}

/// Compare a raw JSON string literal (quotes included) with `field`.
fn key_matches(raw: &str, field: &str) -> bool {
    let inner = raw
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
        .unwrap_or(raw);
    if !inner.contains('\\') {
        return inner == field;
    }
    // Escaped keys are decoded; one that encodes a lone surrogate cannot
    // equal a Rust string and fails to decode, which is a non-match.
    serde_json::from_str::<String>(raw).is_ok_and(|key| key == field)
}

#[cfg(test)]
mod tests {
    use serde_json::value::RawValue;

    use super::record_timestamp;

    fn raw(text: &str) -> Box<RawValue> {
        RawValue::from_string(text.to_owned()).unwrap()
    }

    #[test]
    fn extracts_the_timestamp_member_lexically() {
        let value =
            raw(r#"{"note":"\ud800 lone","\udc00":1,"captured_at":"2026-01-02T03:04:05Z"}"#);
        assert_eq!(
            record_timestamp(&value, "captured_at"),
            Some(1_767_323_045_000)
        );
        let value = raw(r#"{"captured_at":1,"nested":{"captured_at":2},"captured_at":3}"#);
        assert_eq!(record_timestamp(&value, "captured_at"), Some(3));
        let value = raw(r#"{"captured\u005fat":42}"#);
        assert_eq!(record_timestamp(&value, "captured_at"), Some(42));
        // A deep sibling does not hit serde_json's recursion limit.
        let deep = format!(
            r#"{{"d":{}{},"captured_at":7}}"#,
            "[".repeat(200),
            "]".repeat(200)
        );
        assert_eq!(record_timestamp(&raw(&deep), "captured_at"), Some(7));
    }

    #[test]
    fn missing_or_invalid_timestamps_are_none() {
        for text in [
            r#"{"other":1}"#,
            r#"{"captured_at":"\ud800"}"#,
            r#"{"captured_at":"not-a-time"}"#,
            r#"{"captured_at":1.5}"#,
            r#"[{"captured_at":1}]"#,
            "12",
        ] {
            assert_eq!(record_timestamp(&raw(text), "captured_at"), None, "{text}");
        }
    }
}
