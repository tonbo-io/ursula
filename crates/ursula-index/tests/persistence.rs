use std::fs;

use tempfile::TempDir;
use ursula_index::EventEntry;
use ursula_index::EventIndexConfig;
use ursula_index::IndexError;
use ursula_index::IndexStatus;
use ursula_index::LocalEventIndex as EventIndex;
use ursula_index::QueryCursor;
use ursula_index::SourceEnvelope;

fn config(flush_entries: usize) -> EventIndexConfig {
    EventIndexConfig {
        source_id: "persistence-test".to_owned(),
        flush_entries,
        row_group_entries: 2,
        timestamp_field: "captured_at".to_owned(),
    }
}

fn entry(record: u64, captured_at_ms: i64) -> EventEntry {
    EventEntry {
        captured_at_ms,
        record,
    }
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test combines fallible setup with assertions"
)]
fn out_of_order_time_is_queryable_before_and_after_restart() -> Result<(), IndexError> {
    let directory = TempDir::new()?;
    let mut index = EventIndex::open(directory.path(), config(3))?;
    index.ingest(entry(0, 300))?;
    index.ingest(entry(1, 100))?;
    index.ingest(entry(2, 100))?;
    assert_eq!(index.durable_through_record(), 3);
    assert_eq!(index.part_count(), 1);
    let result = index.query(0, 400, None, None, 10)?;
    assert_eq!(result.records, vec![
        entry(1, 100),
        entry(2, 100),
        entry(0, 300)
    ]);
    drop(index);

    let reopened = EventIndex::open(directory.path(), config(3))?;
    let result = reopened.query(0, 400, None, None, 10)?;
    assert_eq!(result.records, vec![
        entry(1, 100),
        entry(2, 100),
        entry(0, 300)
    ]);
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test combines fallible setup with assertions"
)]
fn unflushed_entries_replay_from_the_durable_checkpoint() -> Result<(), IndexError> {
    let directory = TempDir::new()?;
    let mut index = EventIndex::open(directory.path(), config(10))?;
    index.ingest(entry(0, 200))?;
    index.ingest(entry(1, 100))?;
    assert_eq!(index.indexed_through_record(), 2);
    assert_eq!(index.durable_through_record(), 0);
    drop(index);

    let mut reopened = EventIndex::open(directory.path(), config(10))?;
    assert_eq!(reopened.indexed_through_record(), 0);
    assert!(reopened.query(0, 300, None, None, 10)?.records.is_empty());
    reopened.ingest(entry(0, 200))?;
    reopened.ingest(entry(1, 100))?;
    reopened.flush()?;
    assert_eq!(reopened.durable_through_record(), 2);
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test combines fallible setup with assertions"
)]
fn pagination_pins_a_record_watermark() -> Result<(), IndexError> {
    let directory = TempDir::new()?;
    let mut index = EventIndex::open(directory.path(), config(10))?;
    index.ingest(entry(0, 100))?;
    index.ingest(entry(1, 200))?;
    let first = index.query(0, 1_000, None, None, 1)?;
    assert_eq!(first.through_record, 2);
    assert_eq!(first.records, vec![entry(0, 100)]);
    assert_eq!(
        first.next,
        Some(QueryCursor {
            captured_at_ms: 100,
            record: 0
        })
    );

    index.ingest(entry(2, 150))?;
    let second = index.query(0, 1_000, first.next, Some(first.through_record), 10)?;
    assert_eq!(second.records, vec![entry(1, 200)]);
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test combines fallible setup with assertions"
)]
fn compaction_preserves_order_and_checkpoint() -> Result<(), IndexError> {
    let directory = TempDir::new()?;
    let mut index = EventIndex::open(directory.path(), config(2))?;
    for (record, captured_at_ms) in [(0, 400), (1, 100), (2, 300), (3, 200)] {
        index.ingest(entry(record, captured_at_ms))?;
    }
    assert_eq!(index.part_count(), 2);
    let before = index.query(0, 500, None, None, 10)?;
    index.compact_all()?;
    assert_eq!(index.part_count(), 1);
    assert_eq!(index.durable_through_record(), 4);
    assert_eq!(index.query(0, 500, None, None, 10)?.records, before.records);
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test combines fallible setup with assertions"
)]
fn retention_gap_flushes_prefix_and_survives_restart() -> Result<(), IndexError> {
    let directory = TempDir::new()?;
    let mut index = EventIndex::open(directory.path(), config(10))?;
    index.ingest(entry(0, 100))?;
    assert!(matches!(
        index.mark_retention_gap(5),
        Err(IndexError::RetentionGap {
            expected_record: 1,
            first_available_record: 5
        })
    ));
    assert_eq!(index.durable_through_record(), 1);
    drop(index);

    let reopened = EventIndex::open(directory.path(), config(10))?;
    assert_eq!(reopened.status(), &IndexStatus::RetentionGap {
        expected_record: 1,
        first_available_record: 5,
    });
    assert!(matches!(
        reopened.query(0, 200, None, None, 10),
        Err(IndexError::RetentionGap { .. })
    ));
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test combines fallible setup with assertions"
)]
fn invalid_timestamp_does_not_advance_the_source_record() -> Result<(), IndexError> {
    let directory = TempDir::new()?;
    let mut index = EventIndex::open(directory.path(), config(10))?;
    let result = index.ingest_envelope(SourceEnvelope {
        record: 0,
        value: serde_json::json!({"captured_at": "not-a-time"}),
    });
    assert!(matches!(
        result,
        Err(IndexError::InvalidTimestamp { record: 0, .. })
    ));
    assert_eq!(index.indexed_through_record(), 0);
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test combines fallible setup with assertions"
)]
fn deterministic_source_error_can_be_persisted_as_blocked() -> Result<(), IndexError> {
    let directory = TempDir::new()?;
    let mut index = EventIndex::open(directory.path(), config(10))?;
    index.mark_blocked(0, "invalid captured_at".to_owned())?;
    drop(index);

    let mut reopened = EventIndex::open(directory.path(), config(10))?;
    assert_eq!(reopened.status(), &IndexStatus::Blocked {
        record: 0,
        reason: "invalid captured_at".to_owned(),
    });
    assert!(matches!(
        reopened.ingest(entry(0, 100)),
        Err(IndexError::Blocked { record: 0, .. })
    ));
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test combines fallible setup with assertions"
)]
fn startup_removes_temporary_and_unreferenced_parts() -> Result<(), IndexError> {
    let directory = TempDir::new()?;
    fs::write(directory.path().join("partial.tmp"), b"partial")?;
    fs::write(directory.path().join("orphan.parquet"), b"orphan")?;
    let _index = EventIndex::open(directory.path(), config(10))?;
    assert!(!directory.path().join("partial.tmp").exists());
    assert!(!directory.path().join("orphan.parquet").exists());
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test combines fallible setup with assertions"
)]
fn data_directory_allows_only_one_writer() -> Result<(), IndexError> {
    let directory = TempDir::new()?;
    let _first = EventIndex::open(directory.path(), config(10))?;
    assert!(matches!(
        EventIndex::open(directory.path(), config(10)),
        Err(IndexError::AlreadyOpen)
    ));
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test combines fallible setup with assertions"
)]
fn manifest_is_bound_to_one_source_stream() -> Result<(), IndexError> {
    let directory = TempDir::new()?;
    let mut index = EventIndex::open(directory.path(), config(1))?;
    index.ingest(entry(0, 100))?;
    drop(index);

    let mut other = config(1);
    other.source_id = "another-stream".to_owned();
    assert!(matches!(
        EventIndex::open(directory.path(), other),
        Err(IndexError::SourceMismatch { .. })
    ));
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test combines fallible setup with assertions"
)]
fn startup_rejects_truncated_referenced_part() -> Result<(), IndexError> {
    let directory = TempDir::new()?;
    let mut index = EventIndex::open(directory.path(), config(1))?;
    index.ingest(entry(0, 100))?;
    drop(index);
    let part = fs::read_dir(directory.path())?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "parquet")
        })
        .ok_or(IndexError::MissingPart("test part".to_owned()))?;
    fs::OpenOptions::new().write(true).open(part)?.set_len(8)?;
    assert!(matches!(
        EventIndex::open(directory.path(), config(1)),
        Err(IndexError::PartSizeMismatch { .. })
    ));
    Ok(())
}
