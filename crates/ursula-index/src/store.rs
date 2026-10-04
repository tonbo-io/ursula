use std::cmp::Ordering;

use serde::Deserialize;
use serde::Serialize;
use serde::Serializer;
use thiserror::Error;

use crate::extract::Extractor;

/// Render an offset as the 20-digit token Ursula itself sends. Clients treat
/// it as opaque and only echo or compare it.
pub(crate) fn offset_token(offset: u64) -> String {
    format!("{offset:020}")
}

/// Parse an offset token that this indexer (or Ursula) minted.
pub(crate) fn parse_offset_token(value: &str) -> Option<u64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

/// Serde adapter that stores and returns offsets as 20-digit tokens.
pub(crate) mod offset_string {
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serializer;
    use serde::de::Error;

    pub(crate) fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(&super::offset_token(*value))
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where D: Deserializer<'de> {
        let value = String::deserialize(deserializer)?;
        super::parse_offset_token(&value).ok_or_else(|| D::Error::custom("invalid offset token"))
    }
}

#[derive(Clone, Debug)]
pub struct EventIndexConfig {
    /// The source stream URL this index is bound to.
    pub source_url: String,
    /// How each source message yields its event time.
    pub extractor: Extractor,
    pub row_group_entries: usize,
}

impl EventIndexConfig {
    pub fn new(source_url: impl Into<String>, extractor: Extractor) -> Self {
        Self {
            source_url: source_url.into(),
            extractor,
            row_group_entries: 16_384,
        }
    }
}

/// Where a new index starts reading and which incarnation of the source
/// stream it describes. Only used when the index namespace is empty.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexBase {
    pub offset: u64,
    pub incarnation: Option<String>,
}

/// The source stream an index describes. The incarnation is Ursula's opaque
/// `Stream-Incarnation` token, compared only for equality.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceBinding {
    pub stream_url: String,
    pub incarnation: Option<String>,
}

/// One indexed message: its event time span and its locator. `offset` is the
/// message's first byte and `len` its stored length including the LF, so
/// `GET {stream}?offset=<offset>&max_bytes=<len>` (continued from
/// `Stream-Next-Offset` until `len` bytes arrive) returns exactly the message.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct EventEntry {
    pub t_ms: i64,
    pub t_end_ms: i64,
    #[serde(with = "offset_string")]
    pub offset: u64,
    pub len: u64,
}

impl Ord for EventEntry {
    /// Event time first; ties sort by offset.
    fn cmp(&self, other: &Self) -> Ordering {
        (self.t_ms, self.offset, self.t_end_ms, self.len).cmp(&(
            other.t_ms,
            other.offset,
            other.t_end_ms,
            other.len,
        ))
    }
}

impl PartialOrd for EventEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Opaque pagination cursor: the last returned `(t_ms, offset)`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryCursor {
    pub t_ms: i64,
    pub offset: u64,
}

const SIGN_BIT: u64 = 1 << 63;

impl QueryCursor {
    /// 32 hex digits; the token sorts like the cursor it encodes.
    pub fn encode(&self) -> String {
        let time = u64::from_be_bytes(self.t_ms.to_be_bytes()) ^ SIGN_BIT;
        format!("{time:016x}{:016x}", self.offset)
    }

    pub fn decode(value: &str) -> Option<Self> {
        if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        let time = u64::from_str_radix(value.get(..16)?, 16).ok()?;
        let offset = u64::from_str_radix(value.get(16..)?, 16).ok()?;
        Some(Self {
            t_ms: i64::from_be_bytes((time ^ SIGN_BIT).to_be_bytes()),
            offset,
        })
    }
}

impl Serialize for QueryCursor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(&self.encode())
    }
}

impl From<EventEntry> for QueryCursor {
    fn from(value: EventEntry) -> Self {
        Self {
            t_ms: value.t_ms,
            offset: value.offset,
        }
    }
}

/// How an entry matches a `[from, until)` window.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MatchMode {
    /// The event starts inside the window.
    #[default]
    Start,
    /// The event's `[t_ms, t_end_ms]` span intersects the window.
    Overlap,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryRequest {
    pub from_ms: i64,
    pub until_ms: i64,
    pub match_mode: MatchMode,
    pub after: Option<QueryCursor>,
    /// Pin pagination to entries before this offset (a previous
    /// `coverage.through`). Defaults to the durable offset.
    pub through: Option<u64>,
    pub limit: usize,
}

impl QueryRequest {
    pub fn window(from_ms: i64, until_ms: i64, limit: usize) -> Self {
        Self {
            from_ms,
            until_ms,
            match_mode: MatchMode::Start,
            after: None,
            through: None,
            limit,
        }
    }
}

/// Messages that produced no entry, by reason. Counted once per message: a
/// retried or overlapping commit adds only the counts for bytes it newly
/// covers.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SkipCounts {
    pub missing: u64,
    pub invalid: u64,
    pub unparseable: u64,
    pub oversize: u64,
}

impl SkipCounts {
    pub(crate) fn add(&mut self, kind: SkipKind) {
        let counter = match kind {
            SkipKind::Missing => &mut self.missing,
            SkipKind::Invalid => &mut self.invalid,
            SkipKind::Unparseable => &mut self.unparseable,
            SkipKind::Oversize => &mut self.oversize,
            SkipKind::Trimmed | SkipKind::Blank => return,
        };
        *counter = counter.saturating_add(1);
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.missing = self.missing.saturating_add(other.missing);
        self.invalid = self.invalid.saturating_add(other.invalid);
        self.unparseable = self.unparseable.saturating_add(other.unparseable);
        self.oversize = self.oversize.saturating_add(other.oversize);
    }
}

/// Why a message produced no entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SkipKind {
    /// No time value, `null`, or 0.
    Missing,
    /// A time value of the wrong type or format for the declared unit.
    Invalid,
    /// The message is not JSON.
    Unparseable,
    /// The message is longer than the indexer assembles.
    Oversize,
    /// Bytes discarded to resynchronize on a message boundary after a
    /// restart at an offset that was not one; counted as trimmed bytes.
    Trimmed,
    /// An empty or whitespace-only line; not an event and not counted.
    Blank,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Skip {
    pub offset: u64,
    pub len: u64,
    pub kind: SkipKind,
}

/// The result of reading `[start, end)` from the source: one entry or one
/// skip per complete message. `end` is always a message boundary.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Segment {
    pub start: u64,
    pub end: u64,
    pub entries: Vec<EventEntry>,
    pub skips: Vec<Skip>,
}

impl Segment {
    /// Whether `offset` is a message boundary inside this segment.
    pub(crate) fn is_boundary(&self, offset: u64) -> bool {
        offset == self.start
            || offset == self.end
            || self.entries.iter().any(|entry| entry.offset == offset)
            || self.skips.iter().any(|skip| skip.offset == offset)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum IndexStatus {
    Ready,
    /// Two workers produced different entries for the same source bytes.
    Blocked {
        #[serde(with = "offset_string")]
        offset: u64,
        reason: String,
    },
    /// The source stream answered 404. Cleared when it answers again with
    /// the same incarnation; a new incarnation restarts the index.
    SourceGone,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct Coverage {
    /// Where the index started reading.
    #[serde(with = "offset_string")]
    pub from: u64,
    /// The source's retained offset as last observed; entries before it are
    /// no longer fetchable and are not returned.
    #[serde(with = "offset_string")]
    pub floor: u64,
    /// Entries returned are before this offset; pass it back as `through` to
    /// pin later pages.
    #[serde(with = "offset_string")]
    pub through: u64,
    /// Every complete message before this offset is indexed or counted.
    #[serde(with = "offset_string")]
    pub durable: u64,
    /// False once source bytes were trimmed by retention before they were
    /// indexed.
    pub complete: bool,
    pub trimmed_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QueryResult {
    pub source: SourceBinding,
    pub coverage: Coverage,
    pub skipped: SkipCounts,
    pub entries: Vec<EventEntry>,
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
    #[error("the source stream does not exist")]
    SourceGone,
    #[error("event index lock poisoned")]
    LockPoisoned,
    #[error("blocking event-index worker failed")]
    WorkerFailed,
    #[error("event index is blocked at source offset {offset}: {reason}")]
    Blocked { offset: u64, reason: String },
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
    #[error("invalid extractor: {0}")]
    InvalidExtractor(String),
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
    #[error(
        "source bytes at offset {offset} index differently from the entries another indexer committed"
    )]
    RecordConflict { offset: u64 },
    #[error("cache capacity {capacity} bytes cannot hold a {object_size}-byte part")]
    CacheCapacity { capacity: u64, object_size: u64 },
    #[error("index registration `{0}` already exists with different settings")]
    RegistrationConflict(String),
    #[error("index registration `{0}` does not exist")]
    UnknownIndex(String),
}

#[cfg(test)]
mod tests {
    use super::QueryCursor;
    use super::offset_token;
    use super::parse_offset_token;

    #[test]
    fn cursor_tokens_round_trip_and_sort_like_their_cursor() {
        let cursors = [
            QueryCursor {
                t_ms: -5,
                offset: 9,
            },
            QueryCursor { t_ms: 0, offset: 0 },
            QueryCursor {
                t_ms: 1_759_482_001_000,
                offset: 3,
            },
            QueryCursor {
                t_ms: 1_759_482_001_000,
                offset: 4,
            },
        ];
        let tokens = cursors.iter().map(QueryCursor::encode).collect::<Vec<_>>();
        let mut sorted = tokens.clone();
        sorted.sort();
        assert_eq!(sorted, tokens);
        for (cursor, token) in cursors.iter().zip(&tokens) {
            assert_eq!(QueryCursor::decode(token), Some(*cursor));
        }
        assert_eq!(QueryCursor::decode("not-a-cursor"), None);
    }

    #[test]
    fn offset_tokens_are_twenty_digits() {
        assert_eq!(offset_token(42), "00000000000000000042");
        assert_eq!(parse_offset_token("00000000000000000042"), Some(42));
        assert_eq!(parse_offset_token("-1"), None);
        assert_eq!(parse_offset_token(""), None);
    }
}
