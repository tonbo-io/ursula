//! Shared fixtures for ursula-index integration tests. Every test binary
//! that includes this module uses all of it.

use tempfile::TempDir;
use ursula_index::EventEntry;
use ursula_index::EventIndex;
use ursula_index::EventIndexCache;
use ursula_index::EventIndexConfig;
use ursula_index::Extractor;
use ursula_index::FsObjectStore;
use ursula_index::IndexBase;
use ursula_index::Segment;
use ursula_index::Skip;
use ursula_index::SkipKind;

const CACHE_BYTES: u64 = 16 * 1024 * 1024;
/// Every fixture message is 10 bytes long.
pub const LEN: u64 = 10;

pub fn config(source_url: &str) -> EventIndexConfig {
    let mut config = EventIndexConfig::new(
        source_url,
        Extractor::timestamp_field("captured_at").expect("valid extractor"),
    );
    config.row_group_entries = 16;
    config
}

pub fn entry(offset: u64, t_ms: i64) -> EventEntry {
    EventEntry {
        t_ms,
        t_end_ms: t_ms,
        offset,
        len: LEN,
    }
}

/// Consecutive 10-byte messages from `start`, one per timestamp; `None` is a
/// message without a timestamp.
pub fn segment(start: u64, times: &[Option<i64>]) -> Segment {
    let mut segment = Segment {
        start,
        end: start,
        entries: Vec::new(),
        skips: Vec::new(),
    };
    for time in times {
        let offset = segment.end;
        match time {
            Some(t_ms) => segment.entries.push(entry(offset, *t_ms)),
            None => segment.skips.push(Skip {
                offset,
                len: LEN,
                kind: SkipKind::Missing,
            }),
        }
        segment.end = offset.saturating_add(LEN);
    }
    segment
}

/// [`segment`] where every message has a timestamp.
pub fn timed(start: u64, times: &[i64]) -> Segment {
    segment(start, &times.iter().copied().map(Some).collect::<Vec<_>>())
}

/// A filesystem object store in a fresh temporary directory. The returned
/// [`TempDir`] owns the objects and must outlive the store.
pub fn fs_store() -> anyhow::Result<(TempDir, FsObjectStore)> {
    let objects = TempDir::new()?;
    let store = FsObjectStore::new(objects.path())?;
    Ok((objects, store))
}

/// Open an index over `store` with a fresh serving cache. The returned
/// [`TempDir`] owns the cache directory and must outlive the index.
pub async fn open(
    store: &FsObjectStore,
    config: EventIndexConfig,
    base_offset: u64,
) -> anyhow::Result<(TempDir, EventIndex)> {
    let cache = TempDir::new()?;
    let index = EventIndex::open(
        store.clone(),
        EventIndexCache::serving(cache.path(), CACHE_BYTES)?,
        config,
        IndexBase {
            offset: base_offset,
            incarnation: Some("1".to_owned()),
        },
    )
    .await?;
    Ok((cache, index))
}
