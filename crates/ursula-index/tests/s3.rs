#![expect(
    clippy::panic_in_result_fn,
    reason = "the integration test combines fallible setup with assertions"
)]

use std::time::SystemTime;

use anyhow::Context;
use opendal::Operator;
use tempfile::TempDir;
use ursula_index::EventEntry;
use ursula_index::EventIndex;
use ursula_index::EventIndexCache;
use ursula_index::EventIndexConfig;
use ursula_index::Extractor;
use ursula_index::IndexBase;
use ursula_index::QueryRequest;
use ursula_index::S3ObjectStore;
use ursula_index::S3ObjectStoreConfig;
use ursula_index::Segment;

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
    // CI scopes each job's credentials to URSULA_S3_PREFIX.
    let root = format!(
        "{}ursula-index-integration/{}-{unique}",
        std::env::var("URSULA_S3_PREFIX")
            .map(|prefix| format!("{prefix}/"))
            .unwrap_or_default(),
        std::process::id()
    );
    let store_config = S3ObjectStoreConfig {
        bucket: bucket.clone(),
        root: root.clone(),
        region: region.clone(),
        endpoint: endpoint.clone(),
    };
    let mut source_config = EventIndexConfig::new(
        "s3-integration-source",
        Extractor::timestamp_field("captured_at")?,
    );
    source_config.row_group_entries = 8;
    let first_cache = TempDir::new()?;
    let mut writer = EventIndex::open(
        S3ObjectStore::new(store_config.clone())?,
        EventIndexCache::serving(first_cache.path(), 16 * 1024 * 1024)?,
        source_config.clone(),
        IndexBase::default(),
    )
    .await?;
    for (offset, t_ms) in [(0_u64, 200_i64), (10, 100)] {
        writer
            .commit_segment(Segment {
                start: offset,
                end: offset.saturating_add(10),
                entries: vec![EventEntry {
                    t_ms,
                    t_end_ms: t_ms,
                    offset,
                    len: 10,
                }],
                skips: Vec::new(),
            })
            .await?;
    }
    assert!(writer.compact_partition_once(2, 2).await?);
    // S3 stamps LastModified with its own clock, which may run ahead of the runner's: judge the
    // zero-grace cutoff a minute later so the parts just written count as old enough.
    let gc = writer
        .garbage_collect(
            1,
            std::time::Duration::ZERO,
            SystemTime::now() + std::time::Duration::from_secs(60),
        )
        .await?;
    assert_eq!(gc.deleted_parts, 2);
    assert_eq!(gc.deleted_layouts, 2);
    drop(writer);
    drop(first_cache);

    let empty_cache = TempDir::new()?;
    let mut reader = EventIndex::open(
        S3ObjectStore::new(store_config)?,
        EventIndexCache::serving(empty_cache.path(), 16 * 1024 * 1024)?,
        source_config,
        IndexBase::default(),
    )
    .await?;
    let result = reader.query(QueryRequest::window(0, 1_000, 10)).await?;
    assert_eq!(result.coverage.durable, 20);
    assert_eq!(
        result
            .entries
            .iter()
            .map(|entry| entry.offset)
            .collect::<Vec<_>>(),
        vec![10, 0]
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
