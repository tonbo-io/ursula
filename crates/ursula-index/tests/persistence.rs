#![expect(
    clippy::panic_in_result_fn,
    reason = "integration tests combine fallible setup with assertions"
)]

use ursula_index::EventIndexConfig;
use ursula_index::IndexBase;
use ursula_index::IndexError;
use ursula_index::IndexStatus;
use ursula_index::QueryRequest;

mod common;

use common::LEN;
use common::entry;
use common::open;
use common::segment;
use common::timed;

fn config() -> EventIndexConfig {
    common::config("persistence-test")
}

fn window(from_ms: i64, until_ms: i64) -> QueryRequest {
    QueryRequest::window(from_ms, until_ms, 10)
}

#[tokio::test]
async fn out_of_order_time_and_skip_counts_survive_restart() -> anyhow::Result<()> {
    let (_objects, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    index
        .commit_segment(segment(0, &[Some(300), None, Some(100), Some(100)]))
        .await?;
    assert_eq!(index.durable_offset(), 4_u64.saturating_mul(LEN));
    assert_eq!(index.part_count(), 1);
    let expected = vec![entry(20, 100), entry(30, 100), entry(0, 300)];
    assert_eq!(index.query(window(0, 400)).await?.entries, expected);
    drop(index);

    let (_fresh_cache, mut reopened) = open(&store, config(), 0).await?;
    let result = reopened.query(window(0, 400)).await?;
    assert_eq!(result.entries, expected);
    assert_eq!(result.skipped.missing, 1);
    Ok(())
}

#[tokio::test]
async fn compaction_preserves_order_and_checkpoint() -> anyhow::Result<()> {
    let (_objects, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    index.commit_segment(timed(0, &[400, 100])).await?;
    index.commit_segment(timed(20, &[300, 200])).await?;
    assert_eq!(index.part_count(), 2);
    let before = index.query(window(0, 500)).await?;
    assert!(index.compact_partition_once(2, 4).await?);
    assert_eq!(index.part_count(), 1);
    assert_eq!(index.durable_offset(), 40);
    assert_eq!(index.query(window(0, 500)).await?.entries, before.entries);
    Ok(())
}

#[tokio::test]
async fn restart_in_place_starts_over_for_a_recreated_source() -> anyhow::Result<()> {
    let (_objects, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    index.commit_segment(timed(0, &[100, 200])).await?;
    index
        .restart(IndexBase {
            offset: 5,
            incarnation: Some("2".to_owned()),
        })
        .await?;
    assert_eq!(index.source().incarnation.as_deref(), Some("2"));
    assert_eq!(index.indexed_from_offset(), 5);
    assert_eq!(index.durable_offset(), 5);
    assert_eq!(index.resync_offset(), Some(5));
    assert_eq!(index.part_count(), 0);
    assert!(index.query(window(0, 400)).await?.entries.is_empty());
    drop(index);

    let (_fresh_cache, reopened) = open(&store, config(), 0).await?;
    assert_eq!(reopened.source().incarnation.as_deref(), Some("2"));
    assert_eq!(reopened.durable_offset(), 5);
    Ok(())
}

#[tokio::test]
async fn a_gone_source_pauses_indexing_until_it_answers_again() -> anyhow::Result<()> {
    let (_objects, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    index.set_source_gone(true).await?;
    assert_eq!(index.status(), &IndexStatus::SourceGone);
    assert!(matches!(
        index.commit_segment(timed(0, &[100])).await,
        Err(IndexError::SourceGone)
    ));
    assert!(matches!(
        index.clear_blocked().await,
        Err(IndexError::CannotResume(_))
    ));
    // The committed index stays queryable.
    assert!(index.query(window(0, 400)).await?.entries.is_empty());
    index.set_source_gone(false).await?;
    assert_eq!(index.status(), &IndexStatus::Ready);
    index.commit_segment(timed(0, &[100])).await?;
    Ok(())
}

#[tokio::test]
async fn corrupt_referenced_part_is_rejected_on_query() -> anyhow::Result<()> {
    let (objects, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    index.commit_segment(timed(0, &[100])).await?;
    drop(index);

    let part = std::fs::read_dir(objects.path().join("parts"))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "parquet")
        })
        .ok_or_else(|| anyhow::anyhow!("test part is missing"))?;
    std::fs::OpenOptions::new()
        .write(true)
        .open(part)?
        .set_len(8)?;

    let (_fresh_cache, mut reopened) = open(&store, config(), 0).await?;
    let _error = reopened
        .query(window(0, 200))
        .await
        .expect_err("a corrupt referenced part must not be queryable");
    Ok(())
}
