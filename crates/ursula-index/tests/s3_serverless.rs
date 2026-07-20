#![expect(
    clippy::panic_in_result_fn,
    reason = "the integration test combines fallible setup with assertions"
)]

use std::time::SystemTime;

use anyhow::Context;
use opendal::Operator;
use tempfile::TempDir;
use ursula_index::EventEntry;
use ursula_index::EventIndexConfig;
use ursula_index::S3ObjectStore;
use ursula_index::S3ObjectStoreConfig;
use ursula_index::ServerlessEventIndex;

#[tokio::test]
async fn real_s3_conditional_publish_and_cache_recovery() -> anyhow::Result<()> {
    if std::env::var("URSULA_EVENT_INDEX_S3_INTEGRATION")
        .ok()
        .as_deref()
        != Some("1")
    {
        return Ok(());
    }
    let bucket = std::env::var("URSULA_EVENT_INDEX_S3_BUCKET")
        .context("URSULA_EVENT_INDEX_S3_BUCKET is required")?;
    let region = std::env::var("URSULA_EVENT_INDEX_S3_REGION").ok();
    let endpoint = std::env::var("URSULA_EVENT_INDEX_S3_ENDPOINT").ok();
    let unique = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_nanos();
    let root = format!("ursula-index-integration/{}-{unique}", std::process::id());
    let store_config = S3ObjectStoreConfig {
        bucket: bucket.clone(),
        root: root.clone(),
        region: region.clone(),
        endpoint: endpoint.clone(),
    };
    let source_config = EventIndexConfig {
        source_id: "s3-integration-source".to_owned(),
        flush_entries: 1,
        row_group_entries: 8,
        timestamp_field: "captured_at".to_owned(),
    };
    let first_cache = TempDir::new()?;
    let mut writer = ServerlessEventIndex::open_s3(
        S3ObjectStore::new(store_config.clone())?,
        first_cache.path(),
        16 * 1024 * 1024,
        source_config.clone(),
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
    assert!(writer.compact_partition_once(2, 2).await?);
    let gc = writer
        .garbage_collect(1, std::time::Duration::ZERO, SystemTime::now())
        .await?;
    assert_eq!(gc.deleted_parts, 2);
    assert_eq!(gc.deleted_layouts, 2);
    drop(writer);
    drop(first_cache);

    let empty_cache = TempDir::new()?;
    let mut reader = ServerlessEventIndex::open_s3(
        S3ObjectStore::new(store_config)?,
        empty_cache.path(),
        16 * 1024 * 1024,
        source_config,
    )
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

    let mut builder = opendal::services::S3::default().bucket(&bucket).root(&root);
    if let Some(region) = region {
        builder = builder.region(&region);
    }
    if let Some(endpoint) = endpoint {
        builder = builder.endpoint(&endpoint);
    }
    Operator::new(builder)?.finish().remove_all("/").await?;
    Ok(())
}
