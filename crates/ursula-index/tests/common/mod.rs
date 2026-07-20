//! Shared fixtures for ursula-index integration tests.

use tempfile::TempDir;
use ursula_index::EventEntry;
use ursula_index::EventIndex;
use ursula_index::EventIndexCache;
use ursula_index::EventIndexConfig;
use ursula_index::FsObjectStore;

const CACHE_BYTES: u64 = 16 * 1024 * 1024;

pub fn config(source_id: &str, flush_entries: usize, row_group_entries: usize) -> EventIndexConfig {
    EventIndexConfig {
        source_id: source_id.to_owned(),
        flush_entries,
        row_group_entries,
        timestamp_field: "captured_at".to_owned(),
    }
}

pub fn entry(record: u64, captured_at_ms: i64) -> EventEntry {
    EventEntry {
        captured_at_ms,
        record,
    }
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
    indexed_from_record: u64,
) -> anyhow::Result<(TempDir, EventIndex)> {
    let cache = TempDir::new()?;
    let index = EventIndex::open_from_record(
        store.clone(),
        EventIndexCache::serving(cache.path(), CACHE_BYTES)?,
        config,
        indexed_from_record,
    )
    .await?;
    Ok((cache, index))
}
