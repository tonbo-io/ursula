#![expect(
    clippy::panic_in_result_fn,
    reason = "integration tests combine fallible setup with assertions"
)]

use ursula_index::EventIndexConfig;
use ursula_index::IndexError;
use ursula_index::IndexStatus;
use ursula_index::QueryCursor;
use ursula_index::SourceEnvelope;

mod common;

use common::entry;
use common::open;

fn config(flush_entries: usize) -> EventIndexConfig {
    common::config("persistence-test", flush_entries, 2)
}

#[tokio::test]
async fn out_of_order_time_is_queryable_before_and_after_restart() -> anyhow::Result<()> {
    let (_objects, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(3), 0).await?;
    index.ingest(entry(0, 300)).await?;
    index.ingest(entry(1, 100)).await?;
    index.ingest(entry(2, 100)).await?;
    assert_eq!(index.durable_through_record(), 3);
    assert_eq!(index.part_count(), 1);
    assert_eq!(index.query(0, 400, None, None, 10).await?.records, vec![
        entry(1, 100),
        entry(2, 100),
        entry(0, 300)
    ]);
    drop(index);

    let (_fresh_cache, mut reopened) = open(&store, config(3), 0).await?;
    assert_eq!(reopened.query(0, 400, None, None, 10).await?.records, vec![
        entry(1, 100),
        entry(2, 100),
        entry(0, 300)
    ]);
    Ok(())
}

#[tokio::test]
async fn unflushed_entries_replay_from_the_durable_checkpoint() -> anyhow::Result<()> {
    let (_objects, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(10), 0).await?;
    index.ingest(entry(0, 200)).await?;
    index.ingest(entry(1, 100)).await?;
    assert_eq!(index.indexed_through_record(), 2);
    assert_eq!(index.durable_through_record(), 0);
    drop(index);

    let (_fresh_cache, mut reopened) = open(&store, config(10), 0).await?;
    assert_eq!(reopened.indexed_through_record(), 0);
    assert!(
        reopened
            .query(0, 300, None, None, 10)
            .await?
            .records
            .is_empty()
    );
    reopened.ingest(entry(0, 200)).await?;
    reopened.ingest(entry(1, 100)).await?;
    reopened.flush().await?;
    assert_eq!(reopened.durable_through_record(), 2);
    Ok(())
}

#[tokio::test]
async fn pagination_pins_a_record_watermark() -> anyhow::Result<()> {
    let (_objects, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(10), 0).await?;
    index.ingest(entry(0, 100)).await?;
    index.ingest(entry(1, 200)).await?;
    let first = index.query(0, 1_000, None, None, 1).await?;
    assert_eq!(first.through_record, 2);
    assert_eq!(first.records, vec![entry(0, 100)]);
    assert_eq!(
        first.next,
        Some(QueryCursor {
            captured_at_ms: 100,
            record: 0,
        })
    );

    index.ingest(entry(2, 150)).await?;
    let second = index
        .query(0, 1_000, first.next, Some(first.through_record), 10)
        .await?;
    assert_eq!(second.records, vec![entry(1, 200)]);
    Ok(())
}

#[tokio::test]
async fn compaction_preserves_order_and_checkpoint() -> anyhow::Result<()> {
    let (_objects, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(2), 0).await?;
    for (record, captured_at_ms) in [(0, 400), (1, 100), (2, 300), (3, 200)] {
        index.ingest(entry(record, captured_at_ms)).await?;
    }
    assert_eq!(index.part_count(), 2);
    let before = index.query(0, 500, None, None, 10).await?;
    assert!(index.compact_partition_once(2, 4).await?);
    assert_eq!(index.part_count(), 1);
    assert_eq!(index.durable_through_record(), 4);
    assert_eq!(
        index.query(0, 500, None, None, 10).await?.records,
        before.records
    );
    Ok(())
}

#[tokio::test]
async fn retention_gap_flushes_prefix_and_survives_restart() -> anyhow::Result<()> {
    let (_objects, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(10), 0).await?;
    index.ingest(entry(0, 100)).await?;
    assert!(matches!(
        index.mark_retention_gap(5).await,
        Err(IndexError::RetentionGap {
            expected_record: 1,
            first_available_record: 5,
        })
    ));
    assert_eq!(index.durable_through_record(), 1);
    drop(index);

    let (_fresh_cache, mut reopened) = open(&store, config(10), 0).await?;
    assert_eq!(reopened.status(), &IndexStatus::RetentionGap {
        expected_record: 1,
        first_available_record: 5,
    });
    assert!(matches!(
        reopened.query(0, 200, None, None, 10).await,
        Err(IndexError::RetentionGap { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn invalid_timestamp_does_not_advance_the_source_record() -> anyhow::Result<()> {
    let (_objects, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(10), 0).await?;
    let result = index
        .ingest_envelope(SourceEnvelope {
            record: 0,
            value: serde_json::json!({"captured_at": "not-a-time"}),
        })
        .await;
    assert!(matches!(
        result,
        Err(IndexError::InvalidTimestamp { record: 0, .. })
    ));
    assert_eq!(index.indexed_through_record(), 0);
    Ok(())
}

#[tokio::test]
async fn deterministic_source_error_can_be_persisted_as_blocked() -> anyhow::Result<()> {
    let (_objects, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(10), 0).await?;
    index
        .mark_blocked(0, "invalid captured_at".to_owned())
        .await?;
    drop(index);

    let (_fresh_cache, mut reopened) = open(&store, config(10), 0).await?;
    assert_eq!(reopened.status(), &IndexStatus::Blocked {
        record: 0,
        reason: "invalid captured_at".to_owned(),
    });
    assert!(matches!(
        reopened.ingest(entry(0, 100)).await,
        Err(IndexError::Blocked { record: 0, .. })
    ));
    Ok(())
}

#[tokio::test]
async fn manifest_is_bound_to_one_source_stream() -> anyhow::Result<()> {
    let (_objects, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(1), 0).await?;
    index.ingest(entry(0, 100)).await?;
    drop(index);

    let mut other = config(1);
    other.source_id = "another-stream".to_owned();
    let error = open(&store, other, 0)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected source mismatch"))?;
    assert!(
        error
            .downcast_ref::<IndexError>()
            .is_some_and(|error| matches!(error, IndexError::SourceMismatch { .. }))
    );
    Ok(())
}

#[tokio::test]
async fn corrupt_referenced_part_is_rejected_on_query() -> anyhow::Result<()> {
    let (objects, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(1), 0).await?;
    index.ingest(entry(0, 100)).await?;
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

    let (_fresh_cache, mut reopened) = open(&store, config(1), 0).await?;
    let _error = reopened
        .query(0, 200, None, None, 10)
        .await
        .expect_err("a corrupt referenced part must not be queryable");
    Ok(())
}
