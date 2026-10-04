#![expect(
    clippy::panic_in_result_fn,
    reason = "integration tests combine fallible setup with assertions"
)]

use ursula_index::EventEntry;
use ursula_index::EventIndexConfig;
use ursula_index::IndexError;
use ursula_index::IndexStatus;
use ursula_index::MatchMode;
use ursula_index::QueryRequest;
use ursula_index::Skip;
use ursula_index::SkipKind;

mod common;

use common::LEN;
use common::entry;
use common::open;
use common::segment;
use common::timed;

const SOURCE: &str = "https://example.test/v1/stream";
const DAY_MS: i64 = 24 * 60 * 60 * 1_000;

fn config() -> EventIndexConfig {
    common::config(SOURCE)
}

fn window(from_ms: i64, until_ms: i64) -> QueryRequest {
    QueryRequest::window(from_ms, until_ms, 100)
}

#[tokio::test]
async fn retained_stream_starts_at_an_explicit_base() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 40).await?;

    assert_eq!(index.indexed_from_offset(), 40);
    assert_eq!(index.durable_offset(), 40);
    let claim = index
        .claim_segment(80, 20, true, "worker-a", 1_000, 60_000)
        .await?
        .ok_or_else(|| anyhow::anyhow!("retained range was not claimed"))?;
    assert_eq!(claim.start_offset, 40);
    index
        .finish_segment(&claim, timed(40, &[1_000, 2_000]))
        .await?;
    assert_eq!(index.durable_offset(), 60);
    let result = index.query(window(0, 3_000)).await?;
    assert_eq!(result.coverage.from, 40);
    assert_eq!(result.entries, vec![entry(40, 1_000), entry(50, 2_000)]);
    let mut below_base = window(0, 3_000);
    below_base.through = Some(30);
    assert!(matches!(
        index.query(below_base).await,
        Err(IndexError::InvalidQuery)
    ));

    // An existing index keeps its own base.
    let (_fresh_cache, reopened) = open(&store, config(), 70).await?;
    assert_eq!(reopened.indexed_from_offset(), 40);
    assert_eq!(reopened.durable_offset(), 60);
    Ok(())
}

#[tokio::test]
async fn a_fragment_at_the_base_is_covered_but_not_trimmed() -> anyhow::Result<()> {
    // A registration may start inside a message (an NDJSON tail): the
    // fragment before the first boundary predates the indexed range.
    let (_object_dir, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 40).await?;
    assert_eq!(index.resync_offset(), Some(40));
    let mut segment = timed(44, &[100]);
    segment.start = 40;
    segment.skips.push(Skip {
        offset: 40,
        len: 4,
        kind: SkipKind::Trimmed,
    });
    index.commit_segment(segment).await?;
    assert_eq!(index.durable_offset(), 54);
    assert_eq!(index.trimmed_bytes(), 0);
    assert!(index.coverage().complete);
    Ok(())
}

#[tokio::test]
async fn one_open_ended_claim_per_stream_starts_at_the_first_uncovered_offset() -> anyhow::Result<()>
{
    let (_object_dir, store) = common::fs_store()?;
    let (_cache_a, mut first) = open(&store, config(), 0).await?;
    let (_cache_b, mut second) = open(&store, config(), 0).await?;

    assert!(
        first
            .claim_segment(30, 40, false, "worker-a", 1_000, 60_000)
            .await?
            .is_none(),
        "a partial tail waits for the tail-flush interval"
    );
    let claim = first
        .claim_segment(30, 40, true, "worker-a", 1_000, 60_000)
        .await?
        .ok_or_else(|| anyhow::anyhow!("the stream was not claimed"))?;
    assert!(
        second
            .claim_segment(30, 40, true, "worker-b", 1_000, 60_000)
            .await?
            .is_none(),
        "a live claim excludes every other worker"
    );
    first
        .finish_segment(&claim, timed(0, &[3_000, 2_000]))
        .await?;
    let next = second
        .claim_segment(30, 40, true, "worker-b", 1_001, 60_000)
        .await?
        .ok_or_else(|| anyhow::anyhow!("the released stream was not claimed"))?;
    assert_eq!(next.start_offset, 20);
    assert!(
        second
            .claim_segment(20, 40, true, "worker-b", 1_002, 60_000)
            .await?
            .is_none(),
        "nothing is left to claim at the tail"
    );
    Ok(())
}

#[tokio::test]
async fn a_lease_that_expires_mid_commit_neither_duplicates_entries_nor_skip_counts()
-> anyhow::Result<()> {
    let (object_dir, store) = common::fs_store()?;
    let (_cache_a, mut slow) = open(&store, config(), 0).await?;
    let (_cache_b, mut fast) = open(&store, config(), 0).await?;

    let slow_claim = slow
        .claim_segment(40, 40, true, "worker-slow", 1_000, 100)
        .await?
        .ok_or_else(|| anyhow::anyhow!("slow worker did not claim"))?;
    let fast_claim = fast
        .claim_segment(40, 40, true, "worker-fast", 2_000, 60_000)
        .await?
        .ok_or_else(|| anyhow::anyhow!("an expired claim was not taken over"))?;
    assert_eq!(fast_claim.start_offset, 0);

    // The slow worker still commits what it read, then leaves the claim it
    // lost alone.
    slow.finish_segment(&slow_claim, segment(0, &[Some(100), None]))
        .await?;
    assert!(object_dir.path().join("claims/current.json").exists());

    // The fast worker read further. Its overlap must match exactly; only its
    // new suffix adds entries and skip counts.
    fast.finish_segment(&fast_claim, segment(0, &[Some(100), None, None, Some(400)]))
        .await?;
    assert!(!object_dir.path().join("claims/current.json").exists());
    assert_eq!(fast.durable_offset(), 40);
    assert_eq!(fast.skipped().missing, 2);
    let result = fast.query(window(0, 1_000)).await?;
    assert_eq!(result.entries, vec![entry(0, 100), entry(30, 400)]);

    // A full retry of committed bytes is a verified no-op.
    fast.commit_segment(segment(0, &[Some(100), None, None, Some(400)]))
        .await?;
    assert_eq!(fast.skipped().missing, 2);
    assert_eq!(fast.query(window(0, 1_000)).await?.entries.len(), 2);
    Ok(())
}

#[tokio::test]
async fn overlapping_commits_must_match_exactly() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (_cache_a, mut first) = open(&store, config(), 0).await?;
    let (_cache_b, mut second) = open(&store, config(), 0).await?;
    first.commit_segment(timed(0, &[100, 200])).await?;

    let different_time = second
        .commit_segment(timed(0, &[100, 999, 300]))
        .await
        .expect_err("a different event time for committed bytes conflicts");
    assert!(matches!(different_time, IndexError::RecordConflict {
        offset: 10
    }));
    let missing_entry = second
        .commit_segment(segment(0, &[Some(100), None, Some(300)]))
        .await
        .expect_err("dropping a committed entry conflicts; a subset is not enough");
    assert!(matches!(missing_entry, IndexError::RecordConflict {
        offset: 10
    }));
    let mut misaligned = timed(0, &[100, 200]);
    misaligned.entries[0].len = 15;
    misaligned.entries[1].offset = 15;
    misaligned.end = 25;
    let misaligned = second
        .commit_segment(misaligned)
        .await
        .expect_err("covered bytes must end on one of this segment's boundaries");
    assert!(matches!(misaligned, IndexError::RecordConflict {
        offset: 20
    }));

    second.commit_segment(timed(0, &[100, 200, 300])).await?;
    assert_eq!(second.durable_offset(), 30);
    let gap = second
        .commit_segment(timed(40, &[500]))
        .await
        .expect_err("a segment cannot start beyond the durable offset");
    assert!(matches!(gap, IndexError::InvalidSourceResponse(_)));
    Ok(())
}

#[tokio::test]
async fn the_floor_follows_retention_and_reports_trimmed_history() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    index.commit_segment(timed(0, &[100, 200, 300])).await?;

    // Retention inside the indexed range hides entries before the floor.
    index.advance_floor(10).await?;
    assert_eq!(index.durable_offset(), 30);
    let result = index.query(window(0, 1_000)).await?;
    assert_eq!(result.entries, vec![entry(10, 200), entry(20, 300)]);
    assert!(result.coverage.complete);
    assert_eq!(index.resync_offset(), None);

    // Retention past unindexed bytes counts them and restarts there.
    index.advance_floor(55).await?;
    assert_eq!(index.floor_offset(), 55);
    assert_eq!(index.durable_offset(), 55);
    assert_eq!(index.trimmed_bytes(), 25);
    assert_eq!(index.resync_offset(), Some(55));
    let result = index.query(window(0, 1_000)).await?;
    assert!(result.entries.is_empty());
    assert!(!result.coverage.complete);
    assert_eq!(result.coverage.trimmed_bytes, 25);

    // A segment read before the floor moved is stale and dropped.
    index.commit_segment(timed(30, &[400])).await?;
    assert_eq!(index.durable_offset(), 55);
    index.commit_segment(timed(55, &[500])).await?;
    assert_eq!(index.durable_offset(), 65);
    assert_eq!(index.resync_offset(), None);
    assert_eq!(index.query(window(0, 1_000)).await?.entries, vec![entry(
        55, 500
    )]);
    Ok(())
}

#[tokio::test]
async fn overlap_queries_match_event_spans_and_pages_pin_a_watermark() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    let mut spans = timed(0, &[100, 250, 260]);
    spans.entries[0].t_end_ms = 500;
    index.commit_segment(spans).await?;

    let starts = index.query(window(200, 300)).await?;
    assert_eq!(starts.entries.len(), 2);
    let mut overlap = window(200, 300);
    overlap.match_mode = MatchMode::Overlap;
    let overlapping = index.query(overlap).await?;
    assert_eq!(overlapping.entries.len(), 3);
    assert_eq!(overlapping.entries[0].t_end_ms, 500);

    let mut first_page = window(0, 1_000);
    first_page.limit = 1;
    let first = index.query(first_page).await?;
    assert_eq!(first.entries, vec![EventEntry {
        t_ms: 100,
        t_end_ms: 500,
        offset: 0,
        len: LEN,
    }]);
    assert_eq!(first.coverage.through, 30);
    index.commit_segment(timed(30, &[150])).await?;
    let mut second_page = window(0, 1_000);
    second_page.after = first.next;
    second_page.through = Some(first.coverage.through);
    let second = index.query(second_page).await?;
    assert_eq!(second.entries, vec![entry(10, 250), entry(20, 260)]);
    assert!(second.next.is_none());
    Ok(())
}

#[tokio::test]
async fn garbage_collection_removes_an_expired_crashed_worker_claim() -> anyhow::Result<()> {
    let (object_dir, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    let _claim = index
        .claim_segment(40, 20, true, "crashed-worker", 1_000, 100)
        .await?
        .ok_or_else(|| anyhow::anyhow!("range was not claimed"))?;
    assert!(object_dir.path().join("claims/current.json").exists());

    let report = index
        .garbage_collect(
            1,
            std::time::Duration::ZERO,
            std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(2_000),
        )
        .await?;
    assert_eq!(report.deleted_claims, 1);
    assert!(!object_dir.path().join("claims/current.json").exists());
    Ok(())
}

#[tokio::test]
async fn cache_is_disposable_and_rebuilt_from_authoritative_objects() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (first_cache, mut writer) = open(&store, config(), 0).await?;
    writer.commit_segment(timed(0, &[200, 100])).await?;
    drop(writer);
    drop(first_cache);

    let (_empty_cache, mut reader) = open(&store, config(), 0).await?;
    let result = reader.query(window(0, 1_000)).await?;
    assert_eq!(result.coverage.durable, 20);
    assert_eq!(result.entries, vec![entry(10, 100), entry(0, 200)]);
    Ok(())
}

#[tokio::test]
async fn concurrent_writers_converge_on_one_checkpoint() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (_cache_a, mut first) = open(&store, config(), 0).await?;
    let (_cache_b, mut second) = open(&store, config(), 0).await?;
    let times = (0..8_i64).map(|ordinal| 1_000_i64.saturating_sub(ordinal));
    let times = times.collect::<Vec<_>>();
    let (first_result, second_result) = tokio::join!(
        first.commit_segment(timed(0, &times)),
        second.commit_segment(timed(0, &times))
    );
    first_result?;
    second_result?;

    let gc = first
        .garbage_collect(1, std::time::Duration::ZERO, std::time::SystemTime::now())
        .await?;
    assert!(gc.deleted_manifests >= 1);

    let (_verify_cache, mut verify) = open(&store, config(), 0).await?;
    let result = verify.query(window(0, 2_000)).await?;
    assert_eq!(result.coverage.durable, 80);
    assert_eq!(result.entries.len(), 8);
    Ok(())
}

#[tokio::test]
async fn the_manifest_is_bound_to_one_source_and_extractor() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (_cache, _index) = open(&store, config(), 0).await?;
    let other_source = common::config("https://other.example/v1/stream");
    let error = open(&store, other_source, 0)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected source mismatch"))?;
    assert!(error.to_string().contains("not configured source"));

    let mut other_extractor = config();
    other_extractor.extractor = ursula_index::Extractor::timestamp_field("other")?;
    let error = open(&store, other_extractor, 0)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected extractor mismatch"))?;
    assert!(error.to_string().contains("extractor"));
    Ok(())
}

#[tokio::test]
async fn compaction_survives_cache_loss_and_drops_entries_below_the_floor() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (cache, mut index) = open(&store, config(), 0).await?;
    for (ordinal, start) in [0_u64, 10, 20, 30, 40, 50].into_iter().enumerate() {
        let time = 10_i64.saturating_sub(i64::try_from(ordinal)?);
        index.commit_segment(timed(start, &[time])).await?;
    }
    assert_eq!(index.part_count(), 6);
    index.advance_floor(20).await?;
    assert!(index.compact_partition_once(6, 100).await?);
    assert_eq!(index.part_count(), 1);
    drop(index);
    drop(cache);

    let (_fresh_cache, mut reopened) = open(&store, config(), 0).await?;
    let result = reopened.query(window(0, 20)).await?;
    assert_eq!(result.entries.len(), 4);
    assert_eq!(result.coverage.durable, 60);
    Ok(())
}

#[tokio::test]
async fn compaction_rewrites_full_higher_levels() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    for ordinal in 0..64_u64 {
        index
            .commit_segment(timed(ordinal.saturating_mul(LEN), &[i64::try_from(
                ordinal,
            )?]))
            .await?;
    }

    while index.compact_partition_once(4, 64).await? {}

    assert_eq!(index.part_count(), 1);
    assert_eq!(index.query(window(-1, 100)).await?.entries.len(), 64);
    Ok(())
}

#[tokio::test]
async fn compaction_is_bounded_to_one_event_time_partition() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    for ordinal in 0..6_u64 {
        let day = i64::try_from(ordinal / 3)?;
        let time = day
            .saturating_mul(DAY_MS)
            .saturating_add(i64::try_from(ordinal)?);
        index
            .commit_segment(timed(ordinal.saturating_mul(LEN), &[time]))
            .await?;
    }
    assert_eq!(index.part_count(), 6);

    assert!(index.compact_partition_once(3, 3).await?);
    assert_eq!(index.part_count(), 4);
    assert!(index.compact_partition_once(3, 3).await?);
    assert_eq!(index.part_count(), 2);
    assert!(!index.compact_partition_once(3, 3).await?);

    for ordinal in 6..9_u64 {
        index
            .commit_segment(timed(ordinal.saturating_mul(LEN), &[i64::try_from(
                ordinal,
            )?]))
            .await?;
    }
    assert_eq!(index.part_count(), 5);
    assert!(index.compact_partition_once(3, 3).await?);
    assert_eq!(index.part_count(), 3);

    assert_eq!(index.query(window(0, DAY_MS)).await?.entries.len(), 6);
    assert_eq!(
        index
            .query(window(DAY_MS, DAY_MS.saturating_mul(2)))
            .await?
            .entries
            .len(),
        3
    );
    Ok(())
}

#[tokio::test]
async fn compaction_reduces_fan_in_to_stay_within_the_memory_bound() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    for ordinal in 0..3_u64 {
        index
            .commit_segment(timed(ordinal.saturating_mul(LEN), &[i64::try_from(
                ordinal,
            )?]))
            .await?;
    }

    assert!(index.compact_partition_once(3, 2).await?);
    assert_eq!(index.part_count(), 2);
    assert_eq!(index.query(window(-1, 10)).await?.entries.len(), 3);
    Ok(())
}

#[tokio::test]
async fn oversized_old_partition_does_not_block_later_partition() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    index.commit_segment(timed(0, &[0, 1, 2])).await?;
    index.commit_segment(timed(30, &[3, 4, 5])).await?;
    index
        .commit_segment(timed(60, &[DAY_MS.saturating_add(6)]))
        .await?;
    index
        .commit_segment(timed(70, &[DAY_MS.saturating_add(7)]))
        .await?;

    // Day 0's parts are each too large to merge; day 1 is still compacted.
    assert!(index.compact_partition_once(2, 2).await?);
    assert_eq!(index.part_count(), 3);
    assert_eq!(
        index
            .query(window(DAY_MS, DAY_MS.saturating_mul(2)))
            .await?
            .entries
            .len(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn garbage_collection_reclaims_unreferenced_parts_and_manifests() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    for ordinal in 0..3_u64 {
        index
            .commit_segment(timed(ordinal.saturating_mul(LEN), &[i64::try_from(
                ordinal,
            )?]))
            .await?;
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
    let (_fresh_cache, mut reopened) = open(&store, config(), 0).await?;
    assert_eq!(reopened.query(window(-1, 10)).await?.entries.len(), 3);
    Ok(())
}

#[tokio::test]
async fn garbage_collection_skips_and_reclaims_incompatible_manifests() -> anyhow::Result<()> {
    let (object_dir, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    let legacy = object_dir
        .path()
        .join("manifests/00000000000000000000-legacy-v5.json");
    std::fs::write(
        &legacy,
        serde_json::to_vec(&serde_json::json!({
            "version": 5,
            "source_id": SOURCE,
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
async fn blocked_status_survives_restart_and_can_be_cleared_by_an_operator() -> anyhow::Result<()> {
    let (_object_dir, store) = common::fs_store()?;
    let (_cache, mut index) = open(&store, config(), 0).await?;
    index.mark_blocked(10, "conflict".to_owned()).await?;
    drop(index);

    let (_fresh_cache, mut reopened) = open(&store, config(), 0).await?;
    assert_eq!(reopened.status(), &IndexStatus::Blocked {
        offset: 10,
        reason: "conflict".to_owned(),
    });
    assert!(matches!(
        reopened.commit_segment(timed(0, &[100])).await,
        Err(IndexError::Blocked { offset: 10, .. })
    ));
    reopened.clear_blocked().await?;
    assert_eq!(reopened.status(), &IndexStatus::Ready);
    reopened.clear_blocked().await?;
    assert_eq!(reopened.status(), &IndexStatus::Ready);
    Ok(())
}
