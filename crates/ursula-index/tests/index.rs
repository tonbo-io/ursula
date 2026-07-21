#![expect(
    clippy::panic_in_result_fn,
    reason = "integration tests combine fallible setup with assertions"
)]

use ursula_index::EventIndexConfig;
use ursula_index::IndexStatus;
use ursula_index::SourceEnvelope;

mod common;

use common::entry;
use common::open;

fn config() -> EventIndexConfig {
    common::config("https://example.test/v1/stream", 64, 16)
}

fn envelopes(start: u64, timestamps: &[i64]) -> Vec<SourceEnvelope> {
    timestamps
        .iter()
        .enumerate()
        .map(|(index, timestamp)| SourceEnvelope {
            record: start.saturating_add(u64::try_from(index).unwrap_or(u64::MAX)),
            value: serde_json::json!({
                "captured_at": chrono::DateTime::from_timestamp_millis(*timestamp)
                    .map(|value| value.to_rfc3339())
                    .unwrap_or_default()
            }),
        })
        .collect()
}

#[tokio::test]
async fn retained_stream_starts_at_an_explicit_record_base() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 41).await?;

    assert_eq!(index.indexed_from_record(), 41);
    assert_eq!(index.durable_through_record(), 41);
    let claim = index
        .claim_next_segment(45, 2, "worker-a", 1_000, 60_000)
        .await?
        .ok_or_else(|| anyhow::anyhow!("retained range was not claimed"))?;
    assert_eq!((claim.start_record, claim.end_record), (41, 43));
    index
        .finish_segment(&claim, envelopes(41, &[1_000, 2_000]))
        .await?;
    assert_eq!(index.durable_through_record(), 43);
    let result = index.query(0, 3_000, None, None, 10).await?;
    assert_eq!(result.indexed_from_record, 41);
    assert_eq!(result.records.len(), 2);
    let error = index
        .query(0, 3_000, None, Some(40), 10)
        .await
        .expect_err("a query watermark cannot precede indexed_from_record");
    assert!(matches!(error, ursula_index::IndexError::InvalidQuery));

    let (_fresh_cache, reopened) = open(&store, config(), 41).await?;
    assert_eq!(reopened.indexed_from_record(), 41);
    assert_eq!(reopened.durable_through_record(), 43);
    Ok(())
}

#[tokio::test]
async fn out_of_order_record_segments_advance_only_the_contiguous_watermark() -> anyhow::Result<()>
{
    let (_object_dir, store) = common::fs_store()?;
    let (_cache_a, mut first) = open(&store, config(), 0).await?;
    let (_cache_b, mut second) = open(&store, config(), 0).await?;

    second
        .commit_envelopes(2, envelopes(2, &[3_000, 2_000]))
        .await?;
    assert_eq!(second.durable_through_record(), 0);
    assert_eq!(second.completed_record_ranges(), &[
        ursula_index::CompletedRecordRange {
            start_record: 2,
            end_record: 4,
        }
    ]);

    first
        .commit_envelopes(0, envelopes(0, &[4_000, 1_000]))
        .await?;
    first.refresh().await?;
    assert_eq!(first.durable_through_record(), 4);
    assert_eq!(first.completed_record_ranges(), &[
        ursula_index::CompletedRecordRange {
            start_record: 0,
            end_record: 4,
        }
    ]);
    let result = first.query(0, 5_000, None, None, 10).await?;
    assert_eq!(result.records.len(), 4);
    assert_eq!(result.durable_through_record, 4);
    Ok(())
}

#[tokio::test]
async fn workers_claim_distinct_ranges_and_publish_out_of_order() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (_cache_a, mut first) = open(&store, config(), 0).await?;
    let (_cache_b, mut second) = open(&store, config(), 0).await?;

    let first_claim = first
        .claim_next_segment(6, 2, "worker-a", 1_000, 60_000)
        .await?
        .ok_or_else(|| anyhow::anyhow!("first range was not claimed"))?;
    let second_claim = second
        .claim_next_segment(6, 2, "worker-b", 1_000, 60_000)
        .await?
        .ok_or_else(|| anyhow::anyhow!("second range was not claimed"))?;
    assert_eq!((first_claim.start_record, first_claim.end_record), (0, 2));
    assert_eq!((second_claim.start_record, second_claim.end_record), (2, 4));

    second
        .finish_segment(&second_claim, envelopes(2, &[3_000, 2_000]))
        .await?;
    assert_eq!(second.durable_through_record(), 0);
    first
        .finish_segment(&first_claim, envelopes(0, &[4_000, 1_000]))
        .await?;
    assert_eq!(first.durable_through_record(), 4);

    let third_claim = second
        .claim_next_segment(6, 2, "worker-b", 1_001, 60_000)
        .await?
        .ok_or_else(|| anyhow::anyhow!("third range was not claimed"))?;
    assert_eq!((third_claim.start_record, third_claim.end_record), (4, 6));
    Ok(())
}

#[tokio::test]
async fn claims_stop_before_the_next_completed_range() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    index
        .commit_envelopes(4, envelopes(4, &[4_000, 5_000]))
        .await?;

    let claim = index
        .claim_next_segment(10, 10, "worker-a", 1_000, 60_000)
        .await?
        .ok_or_else(|| anyhow::anyhow!("gap before completed range was not claimed"))?;
    assert_eq!((claim.start_record, claim.end_record), (0, 4));
    Ok(())
}

#[tokio::test]
async fn garbage_collection_removes_expired_crashed_worker_claims() -> anyhow::Result<()> {
    let (object_dir, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    let claim = index
        .claim_next_segment(4, 2, "crashed-worker", 1_000, 100)
        .await?
        .ok_or_else(|| anyhow::anyhow!("range was not claimed"))?;
    assert!(
        object_dir
            .path()
            .join(format!("claims/{:020}.json", claim.start_record))
            .exists()
    );

    let report = index
        .garbage_collect(
            1,
            std::time::Duration::ZERO,
            std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(2_000),
        )
        .await?;
    assert_eq!(report.deleted_claims, 1);
    assert!(
        !object_dir
            .path()
            .join("claims")
            .join("00000000000000000000.json")
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_workers_split_one_hot_stream_into_distinct_ranges() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (_cache_a, mut first) = open(&store, config(), 0).await?;
    let (_cache_b, mut second) = open(&store, config(), 0).await?;
    let (_cache_c, mut third) = open(&store, config(), 0).await?;
    let (_cache_d, mut fourth) = open(&store, config(), 0).await?;

    let claims = tokio::join!(
        first.claim_next_segment(8, 2, "worker-a", 1_000, 60_000),
        second.claim_next_segment(8, 2, "worker-b", 1_000, 60_000),
        third.claim_next_segment(8, 2, "worker-c", 1_000, 60_000),
        fourth.claim_next_segment(8, 2, "worker-d", 1_000, 60_000),
    );
    let mut ranges = [claims.0?, claims.1?, claims.2?, claims.3?]
        .into_iter()
        .map(|claim| {
            claim
                .map(|claim| (claim.start_record, claim.end_record))
                .ok_or_else(|| anyhow::anyhow!("worker did not claim a hot-stream range"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    ranges.sort_unstable();
    assert_eq!(ranges, vec![(0, 2), (2, 4), (4, 6), (6, 8)]);
    Ok(())
}

#[tokio::test]
async fn an_expired_long_claim_can_commit_around_an_already_published_prefix() -> anyhow::Result<()>
{
    let (_object_dir, store) = common::fs_store()?;
    let (_cache_a, mut short_reader) = open(&store, config(), 0).await?;
    let (_cache_b, mut long_reader) = open(&store, config(), 0).await?;

    short_reader
        .commit_envelopes(0, envelopes(0, &[1_000, 2_000]))
        .await?;
    long_reader
        .commit_envelopes(0, envelopes(0, &[1_000, 2_000, 3_000, 4_000]))
        .await?;
    assert_eq!(long_reader.durable_through_record(), 4);
    assert_eq!(
        long_reader
            .query(0, 5_000, None, None, 10)
            .await?
            .records
            .len(),
        4
    );

    let error = long_reader
        .commit_envelopes(0, envelopes(0, &[9_000, 2_000, 3_000, 4_000]))
        .await
        .expect_err("a retried covered record must match its committed timestamp");
    assert!(matches!(error, ursula_index::IndexError::RecordConflict {
        record: 0
    }));
    Ok(())
}

#[tokio::test]
async fn cache_is_disposable_and_rebuilt_from_authoritative_objects() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (first_cache, mut writer) = open(&store, config(), 0).await?;
    writer.ingest(entry(0, 200)).await?;
    writer.ingest(entry(1, 100)).await?;
    writer.flush().await?;
    drop(writer);
    drop(first_cache);

    let (_empty_cache, mut reader) = open(&store, config(), 0).await?;
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
    let (_object_dir, store) = common::fs_store()?;
    let (_cache_a, mut first) = open(&store, config(), 0).await?;
    let (_cache_b, mut second) = open(&store, config(), 0).await?;
    for record in 0..8 {
        let event = entry(record, 1_000_i64.saturating_sub(i64::try_from(record)?));
        first.ingest(event).await?;
        second.ingest(event).await?;
    }
    let (first_result, second_result) = tokio::join!(first.flush(), second.flush());
    first_result?;
    second_result?;

    let gc = first
        .garbage_collect(1, std::time::Duration::ZERO, std::time::SystemTime::now())
        .await?;
    assert!(gc.deleted_manifests >= 1);

    let (_verify_cache, mut verify) = open(&store, config(), 0).await?;
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
    let (_object_dir, store) = common::fs_store()?;
    let (_cache, _index) = open(&store, config(), 0).await?;
    let mut other = config();
    other.source_id = "https://other.example/v1/stream".to_owned();
    let error = open(&store, other, 0)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected source mismatch"))?;
    assert!(error.to_string().contains("not configured source"));
    Ok(())
}

#[tokio::test]
async fn conflicting_writer_cannot_hide_different_event_time() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (_cache_a, mut first) = open(&store, config(), 0).await?;
    let (_cache_b, mut second) = open(&store, config(), 0).await?;
    first.ingest(entry(0, 100)).await?;
    second.ingest(entry(0, 200)).await?;
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
    let (_object_dir, store) = common::fs_store()?;
    let (_cache_a, mut prefix_writer) = open(&store, config(), 0).await?;
    let (_cache_b, mut full_writer) = open(&store, config(), 0).await?;
    for record in 0..4 {
        let event = entry(record, i64::try_from(record)?);
        full_writer.ingest(event).await?;
        if record < 2 {
            prefix_writer.ingest(event).await?;
        }
    }
    prefix_writer.flush().await?;
    full_writer.flush().await?;

    let (_verify_cache, mut verify) = open(&store, config(), 0).await?;
    let result = verify.query(-1, 10, None, None, 10).await?;
    assert_eq!(result.durable_through_record, 4);
    assert_eq!(result.records.len(), 4);
    Ok(())
}

#[tokio::test]
async fn compaction_survives_cache_loss() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let compact_config = common::config("https://example.test/v1/stream", 2, 16);
    let (cache, mut index) = open(&store, compact_config.clone(), 0).await?;
    for record in 0..6 {
        index
            .ingest(entry(record, 10_i64.saturating_sub(i64::try_from(record)?)))
            .await?;
    }
    assert_eq!(index.part_count(), 3);
    assert!(index.compact_partition_once(3, 100).await?);
    assert_eq!(index.part_count(), 1);
    drop(index);
    drop(cache);

    let (_fresh_cache, mut reopened) = open(&store, compact_config, 0).await?;
    let result = reopened.query(0, 20, None, None, 10).await?;
    assert_eq!(result.records.len(), 6);
    assert_eq!(result.durable_through_record, 6);
    Ok(())
}

#[tokio::test]
async fn compaction_rewrites_full_higher_levels() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let compact_config = common::config("https://example.test/v1/stream", 1, 16);
    let (_cache, mut index) = open(&store, compact_config, 0).await?;
    for record in 0..64 {
        index.ingest(entry(record, i64::try_from(record)?)).await?;
    }

    while index.compact_partition_once(4, 64).await? {}

    assert_eq!(index.part_count(), 1);
    assert_eq!(
        index.query(-1, 100, None, None, 100).await?.records.len(),
        64
    );
    Ok(())
}

#[tokio::test]
async fn compaction_is_bounded_to_one_event_time_partition() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let compact_config = common::config("https://example.test/v1/stream", 1, 16);
    let (_cache, mut index) = open(&store, compact_config, 0).await?;
    const DAY_MS: i64 = 24 * 60 * 60 * 1_000;
    for record in 0..6 {
        let day = i64::try_from(record / 3)?;
        index
            .ingest(entry(
                record,
                day.saturating_mul(DAY_MS) + i64::try_from(record)?,
            ))
            .await?;
    }
    assert_eq!(index.part_count(), 6);

    assert!(index.compact_partition_once(3, 3).await?);
    assert_eq!(index.part_count(), 4);
    assert!(index.compact_partition_once(3, 3).await?);
    assert_eq!(index.part_count(), 2);
    assert!(!index.compact_partition_once(3, 3).await?);

    for record in 6..9 {
        index.ingest(entry(record, i64::try_from(record)?)).await?;
    }
    assert_eq!(index.part_count(), 5);
    assert!(index.compact_partition_once(3, 3).await?);
    assert_eq!(index.part_count(), 3);

    let first_day = index.query(0, DAY_MS, None, None, 10).await?;
    assert_eq!(first_day.records.len(), 6);
    let second_day = index.query(DAY_MS, DAY_MS * 2, None, None, 10).await?;
    assert_eq!(second_day.records.len(), 3);
    Ok(())
}

#[tokio::test]
async fn compaction_reduces_fan_in_to_stay_within_the_memory_bound() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let compact_config = common::config("https://example.test/v1/stream", 1, 16);
    let (_cache, mut index) = open(&store, compact_config, 0).await?;
    for record in 0..3 {
        index.ingest(entry(record, i64::try_from(record)?)).await?;
    }

    assert!(index.compact_partition_once(3, 2).await?);
    assert_eq!(index.part_count(), 2);
    assert_eq!(index.query(-1, 10, None, None, 10).await?.records.len(), 3);
    Ok(())
}

#[tokio::test]
async fn oversized_old_partition_does_not_block_later_partition() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let large_config = common::config("https://example.test/v1/stream", 3, 16);
    let (_large_cache, mut large) = open(&store, large_config, 0).await?;
    for record in 0..6 {
        large.ingest(entry(record, i64::try_from(record)?)).await?;
    }
    drop(large);

    let small_config = common::config("https://example.test/v1/stream", 1, 16);
    let (_small_cache, mut small) = open(&store, small_config, 0).await?;
    const DAY_MS: i64 = 24 * 60 * 60 * 1_000;
    for record in 6..8 {
        small
            .ingest(entry(record, DAY_MS + i64::try_from(record)?))
            .await?;
    }

    assert!(small.compact_partition_once(2, 2).await?);
    assert_eq!(small.part_count(), 3);
    assert_eq!(
        small
            .query(DAY_MS, DAY_MS * 2, None, None, 10)
            .await?
            .records
            .len(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn garbage_collection_reclaims_unreferenced_parts_and_manifests() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let compact_config = common::config("https://example.test/v1/stream", 1, 16);
    let (_cache, mut index) = open(&store, compact_config.clone(), 0).await?;
    for record in 0..3 {
        index.ingest(entry(record, i64::try_from(record)?)).await?;
    }
    assert!(index.compact_partition_once(3, 3).await?);

    let retained = index
        .garbage_collect(2, std::time::Duration::ZERO, std::time::SystemTime::now())
        .await?;
    assert_eq!(retained.deleted_parts, 0);
    assert_eq!(retained.deleted_layouts, 0);
    let reclaimed = index
        .garbage_collect(1, std::time::Duration::ZERO, std::time::SystemTime::now())
        .await?;
    assert_eq!(reclaimed.deleted_parts, 3);
    assert_eq!(reclaimed.deleted_layouts, 3);
    assert!(reclaimed.deleted_manifests >= 1);

    drop(index);
    let (_fresh_cache, mut reopened) = open(&store, compact_config, 0).await?;
    let result = reopened.query(-1, 10, None, None, 10).await?;
    assert_eq!(result.records.len(), 3);
    Ok(())
}

#[tokio::test]
async fn garbage_collection_skips_and_reclaims_incompatible_manifests() -> anyhow::Result<()> {
    let (object_dir, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    let legacy = object_dir
        .path()
        .join("manifests/00000000000000000000-legacy-v1.json");
    std::fs::write(
        &legacy,
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "source_id": "https://example.test/v1/stream",
            "generation": 0,
            "durable_through_record": 0,
            "status": {"state": "ready"},
            "parts": []
        }))?,
    )?;

    let report = index
        .garbage_collect(8, std::time::Duration::ZERO, std::time::SystemTime::now())
        .await?;
    assert!(report.deleted_manifests >= 1);
    assert!(!legacy.exists());
    Ok(())
}

#[tokio::test]
async fn blocked_status_can_be_cleared_by_an_operator() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    index
        .mark_blocked(0, "repaired source event".to_owned())
        .await?;
    assert!(matches!(index.status(), IndexStatus::Blocked { .. }));

    index.clear_blocked().await?;
    assert_eq!(index.status(), &IndexStatus::Ready);
    index.clear_blocked().await?;
    assert_eq!(index.status(), &IndexStatus::Ready);

    let (_fresh_cache, reopened) = open(&store, config(), 0).await?;
    assert_eq!(reopened.status(), &IndexStatus::Ready);
    Ok(())
}
