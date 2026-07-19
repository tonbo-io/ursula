#![expect(
    clippy::panic_in_result_fn,
    reason = "integration tests combine fallible setup with assertions"
)]

use tempfile::TempDir;
use ursula_event_index::EventEntry;
use ursula_event_index::EventIndexConfig;
use ursula_event_index::FsObjectStore;
use ursula_event_index::ServerlessEventIndex;

fn config() -> EventIndexConfig {
    EventIndexConfig {
        source_id: "https://example.test/v1/stream".to_owned(),
        flush_entries: 64,
        row_group_entries: 16,
        timestamp_field: "captured_at".to_owned(),
    }
}

#[tokio::test]
async fn cache_is_disposable_and_rebuilt_from_authoritative_objects() -> anyhow::Result<()> {
    let object_dir = TempDir::new()?;
    let first_cache = TempDir::new()?;
    let store = FsObjectStore::new(object_dir.path())?;
    let mut writer = ServerlessEventIndex::open_fs(
        store.clone(),
        first_cache.path(),
        16 * 1024 * 1024,
        config(),
    )
    .await?;
    writer
        .ingest(EventEntry {
            captured_at_ms: 200,
            record: 0,
        })
        .await?;
    writer
        .ingest(EventEntry {
            captured_at_ms: 100,
            record: 1,
        })
        .await?;
    writer.flush().await?;
    drop(writer);
    drop(first_cache);

    let empty_cache = TempDir::new()?;
    let mut reader =
        ServerlessEventIndex::open_fs(store, empty_cache.path(), 16 * 1024 * 1024, config())
            .await?;
    let result = reader.query(0, 1_000, None, None, 10).await?;
    assert_eq!(result.durable_through_record, 2);
    assert_eq!(
        result
            .records
            .iter()
            .map(|entry| entry.record)
            .collect::<Vec<_>>(),
        vec![1, 0]
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_writers_converge_on_one_checkpoint() -> anyhow::Result<()> {
    let object_dir = TempDir::new()?;
    let cache_a = TempDir::new()?;
    let cache_b = TempDir::new()?;
    let store = FsObjectStore::new(object_dir.path())?;
    let mut first =
        ServerlessEventIndex::open_fs(store.clone(), cache_a.path(), 16 * 1024 * 1024, config())
            .await?;
    let mut second =
        ServerlessEventIndex::open_fs(store.clone(), cache_b.path(), 16 * 1024 * 1024, config())
            .await?;
    for record in 0..8 {
        let entry = EventEntry {
            captured_at_ms: 1_000_i64.saturating_sub(i64::try_from(record)?),
            record,
        };
        first.ingest(entry).await?;
        second.ingest(entry).await?;
    }
    let (first_result, second_result) = tokio::join!(first.flush(), second.flush());
    first_result?;
    second_result?;

    let verify_cache = TempDir::new()?;
    let mut verify =
        ServerlessEventIndex::open_fs(store, verify_cache.path(), 16 * 1024 * 1024, config())
            .await?;
    let result = verify.query(0, 2_000, None, None, 32).await?;
    assert_eq!(result.durable_through_record, 8);
    assert_eq!(result.records.len(), 8);
    let mut records = result
        .records
        .iter()
        .map(|entry| entry.record)
        .collect::<Vec<_>>();
    records.sort_unstable();
    assert_eq!(records, (0..8).collect::<Vec<_>>());
    Ok(())
}

#[tokio::test]
async fn source_binding_is_stored_in_s3_manifest() -> anyhow::Result<()> {
    let object_dir = TempDir::new()?;
    let cache = TempDir::new()?;
    let store = FsObjectStore::new(object_dir.path())?;
    let _index =
        ServerlessEventIndex::open_fs(store.clone(), cache.path(), 16 * 1024 * 1024, config())
            .await?;
    let other_cache = TempDir::new()?;
    let mut other = config();
    other.source_id = "https://other.example/v1/stream".to_owned();
    let error = ServerlessEventIndex::open_fs(store, other_cache.path(), 16 * 1024 * 1024, other)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected source mismatch"))?;
    assert!(error.to_string().contains("not configured source"));
    Ok(())
}

#[tokio::test]
async fn conflicting_writer_cannot_hide_different_event_time() -> anyhow::Result<()> {
    let object_dir = TempDir::new()?;
    let cache_a = TempDir::new()?;
    let cache_b = TempDir::new()?;
    let store = FsObjectStore::new(object_dir.path())?;
    let mut first =
        ServerlessEventIndex::open_fs(store.clone(), cache_a.path(), 16 * 1024 * 1024, config())
            .await?;
    let mut second =
        ServerlessEventIndex::open_fs(store, cache_b.path(), 16 * 1024 * 1024, config()).await?;
    first
        .ingest(EventEntry {
            captured_at_ms: 100,
            record: 0,
        })
        .await?;
    second
        .ingest(EventEntry {
            captured_at_ms: 200,
            record: 0,
        })
        .await?;
    first.flush().await?;
    let error = second
        .flush()
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected conflicting record"))?;
    assert!(error.to_string().contains("differs from the value"));
    Ok(())
}

#[tokio::test]
async fn loser_can_publish_the_suffix_after_a_partial_concurrent_advance() -> anyhow::Result<()> {
    let object_dir = TempDir::new()?;
    let cache_a = TempDir::new()?;
    let cache_b = TempDir::new()?;
    let store = FsObjectStore::new(object_dir.path())?;
    let mut prefix_writer =
        ServerlessEventIndex::open_fs(store.clone(), cache_a.path(), 16 * 1024 * 1024, config())
            .await?;
    let mut full_writer =
        ServerlessEventIndex::open_fs(store.clone(), cache_b.path(), 16 * 1024 * 1024, config())
            .await?;
    for record in 0..4 {
        let entry = EventEntry {
            captured_at_ms: i64::try_from(record)?,
            record,
        };
        full_writer.ingest(entry).await?;
        if record < 2 {
            prefix_writer.ingest(entry).await?;
        }
    }
    prefix_writer.flush().await?;
    full_writer.flush().await?;

    let verify_cache = TempDir::new()?;
    let mut verify =
        ServerlessEventIndex::open_fs(store, verify_cache.path(), 16 * 1024 * 1024, config())
            .await?;
    let result = verify.query(-1, 10, None, None, 10).await?;
    assert_eq!(result.durable_through_record, 4);
    assert_eq!(result.records.len(), 4);
    Ok(())
}

#[tokio::test]
async fn serverless_compaction_survives_cache_loss() -> anyhow::Result<()> {
    let object_dir = TempDir::new()?;
    let cache = TempDir::new()?;
    let store = FsObjectStore::new(object_dir.path())?;
    let mut compact_config = config();
    compact_config.flush_entries = 2;
    let mut index = ServerlessEventIndex::open_fs(
        store.clone(),
        cache.path(),
        16 * 1024 * 1024,
        compact_config.clone(),
    )
    .await?;
    for record in 0..6 {
        index
            .ingest(EventEntry {
                captured_at_ms: 10_i64.saturating_sub(i64::try_from(record)?),
                record,
            })
            .await?;
    }
    assert_eq!(index.part_count(), 3);
    index.compact_all().await?;
    assert_eq!(index.part_count(), 1);
    drop(index);
    drop(cache);

    let fresh_cache = TempDir::new()?;
    let mut reopened =
        ServerlessEventIndex::open_fs(store, fresh_cache.path(), 16 * 1024 * 1024, compact_config)
            .await?;
    let result = reopened.query(0, 20, None, None, 10).await?;
    assert_eq!(result.records.len(), 6);
    assert_eq!(result.durable_through_record, 6);
    Ok(())
}
