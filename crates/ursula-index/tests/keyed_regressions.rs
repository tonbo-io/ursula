//! Regressions found by the keyed indexer simulation (design §6.1 U21).
//!
//! - Garbage collection against content-addressed parts that a writer
//!   re-references: a publish that loses its CAS queues its parts for
//!   deletion after the grace period; when the next attempt rebuilds the
//!   same records it produces byte-identical parts under the same keys,
//!   finds them present and publishes a manifest that references them. The
//!   queued deletion must not remove them.
//! - A continuity rebuild split by `max_ingest_bytes` must not publish a `D`
//!   below the one it replaces while the source still holds that many
//!   records (`D` is monotone per namespace).

#![allow(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::panic,
    reason = "test scaffolding"
)]

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use ursula_index::MemoryObjectStore;
use ursula_index::ObjectStore;
use ursula_index::clock::Clock;
use ursula_index::keyed::IncarnationState;
use ursula_index::keyed::KeyedEngine;
use ursula_index::keyed::KeyedEngineConfig;
use ursula_index::keyed::KeyedManifest;
use ursula_index::keyed::KeyedNamespace;
use ursula_index::keyed::KeyedReadOutcome;
use ursula_index::keyed::KeyedReadRequest;
use ursula_index::keyed::KeyedSource;
use ursula_index::keyed::KeyedState;
use ursula_index::keyed::Lower;
use ursula_index::keyed::RangeQuery;
use ursula_index::keyed::Selection;
use ursula_index::keyed::SourceClient;
use ursula_index::keyed::SourceError;
use ursula_index::keyed::SourcePage;
use ursula_index::keyed::encode_key;
use ursula_index::memory_store::AppliedChange;
use ursula_index::memory_store::FaultDecision;
use ursula_index::memory_store::MemoryStoreHooks;
use ursula_index::memory_store::ObjectFault;
use ursula_index::memory_store::ObjectOp;

const GRACE: Duration = Duration::from_secs(10);

/// Wall clock on tokio's (paused) time.
struct TestClock(tokio::time::Instant);

impl Clock for TestClock {
    fn now_ms(&self) -> u64 {
        1_700_000_000_000 + u64::try_from(self.0.elapsed().as_millis()).unwrap()
    }
}

/// An in-memory source log that can be made to fail.
#[derive(Default)]
struct FakeLog {
    records: Mutex<Vec<String>>,
    failing: AtomicBool,
}

impl SourceClient for FakeLog {
    fn read<'a>(
        &'a self,
        _bucket: &'a str,
        _key: &'a str,
        record: u64,
        _max_bytes: u64,
        max_records: Option<u64>,
        _leader: bool,
    ) -> BoxFuture<'a, Result<SourcePage, SourceError>> {
        async move {
            if self.failing.load(Ordering::SeqCst) {
                return Err(SourceError::Transient("injected source outage".to_owned()));
            }
            let records = self.records.lock().unwrap();
            let next_record = records.len() as u64;
            if record > next_record {
                return Err(SourceError::BeyondTail { next_record });
            }
            let end = max_records.map_or(next_record, |limit| next_record.min(record + limit));
            Ok(SourcePage {
                start_record: record,
                next_record: end,
                records: records[record as usize..end as usize].to_vec(),
            })
        }
        .boxed()
    }

    fn incarnation<'a>(
        &'a self,
        _bucket: &'a str,
        _key: &'a str,
        _incarnation: u64,
    ) -> BoxFuture<'a, Result<IncarnationState, SourceError>> {
        async { Ok(IncarnationState::Present) }.boxed()
    }
}

/// Scripted store faults: one spurious CAS conflict that also takes the
/// source down, and a delay on deletions.
struct Script {
    log: Arc<FakeLog>,
    conflict_next_cas: AtomicBool,
    delete_delay: Mutex<Duration>,
    deleted: Mutex<Vec<String>>,
}

impl MemoryStoreHooks for Script {
    fn decide(&self, op: ObjectOp, _key: &str) -> FaultDecision {
        match op {
            ObjectOp::CompareAndSwap if self.conflict_next_cas.swap(false, Ordering::SeqCst) => {
                self.log.failing.store(true, Ordering::SeqCst);
                FaultDecision {
                    delay: Duration::ZERO,
                    fault: ObjectFault::Conflict,
                }
            }
            ObjectOp::Delete => FaultDecision {
                delay: *self.delete_delay.lock().unwrap(),
                fault: ObjectFault::None,
            },
            _ => FaultDecision::default(),
        }
    }

    fn applied(&self, change: AppliedChange<'_>) {
        if let AppliedChange::Delete { key } = change {
            self.deleted.lock().unwrap().push(key.to_owned());
        }
    }
}

fn record(index: usize) -> String {
    format!(
        r#"{{"ops":[["p","{}",{index}]]}}"#,
        encode_key(format!("k{}", index % 3).as_bytes())
    )
}

fn all_rows() -> RangeQuery {
    RangeQuery {
        lower: Lower::First,
        end: None,
        limit: 1_000,
        budget: None,
    }
}

fn source() -> KeyedSource {
    KeyedSource {
        bucket: "b".to_owned(),
        key: "s".to_owned(),
        incarnation: 7,
    }
}

async fn read(engine: &KeyedEngine, min: u64, timeout: Duration) -> KeyedReadOutcome {
    engine
        .read(KeyedReadRequest {
            source: source(),
            source_next: min,
            selection: Selection::Range(all_rows()),
            min_through_record: Some(min),
            timeout,
        })
        .await
}

#[tokio::test(start_paused = true)]
async fn queued_deletion_spares_a_part_republished_by_a_retry() {
    let log = Arc::new(FakeLog::default());
    let script = Arc::new(Script {
        log: Arc::clone(&log),
        conflict_next_cas: AtomicBool::new(false),
        delete_delay: Mutex::new(Duration::ZERO),
        deleted: Mutex::new(Vec::new()),
    });
    let clock: Arc<dyn Clock> = Arc::new(TestClock(tokio::time::Instant::now()));
    let raw = MemoryObjectStore::new(Arc::clone(&clock));
    let store = raw.with_hooks(script.clone());
    let engine = KeyedEngine::with_clock(
        ObjectStore::from(store.clone()),
        Arc::clone(&log),
        None,
        KeyedEngineConfig {
            min_publish_interval: Duration::ZERO,
            gc_grace: GRACE,
            // Reads go to the store, so a deleted part is noticed.
            write_cache_bytes: 0,
            ..KeyedEngineConfig::default()
        },
        clock,
    );

    // Generation 1 covers records 0..2.
    log.records.lock().unwrap().extend((0..2).map(record));
    let KeyedReadOutcome::Rows { through: 2, .. } = read(&engine, 2, Duration::from_secs(5)).await
    else {
        panic!("first publication");
    };

    // The publication of records 2..4 loses its CAS (a spurious conflict),
    // which queues its parts for deletion, and the source goes down, so the
    // retry fails.
    log.records.lock().unwrap().extend((2..4).map(record));
    script.conflict_next_cas.store(true, Ordering::SeqCst);
    let outcome = read(&engine, 4, Duration::from_secs(1)).await;
    assert!(
        !matches!(outcome, KeyedReadOutcome::Rows { through: 4, .. }),
        "{outcome:?}"
    );

    // After the grace period the queued deletion runs, slowly, while the
    // source is back and a new attempt republishes the same parts.
    tokio::time::sleep(GRACE + Duration::from_secs(1)).await;
    log.failing.store(false, Ordering::SeqCst);
    *script.delete_delay.lock().unwrap() = Duration::from_secs(3);
    let gc = tokio::spawn({
        let engine = engine.clone();
        async move { engine.collect_garbage().await }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    let KeyedReadOutcome::Rows { through: 4, .. } = read(&engine, 4, Duration::from_secs(5)).await
    else {
        panic!("second publication");
    };
    let _deleted = gc.await.unwrap();

    // Every object the current manifest references still exists, and the
    // keyed state is readable and equals the fold.
    let published = KeyedNamespace::new(ObjectStore::from(raw.without_hooks()), source())
        .load()
        .await
        .unwrap()
        .unwrap();
    let existing: Vec<String> = raw
        .snapshot()
        .into_iter()
        .map(|(key, _modified)| key)
        .collect();
    for part in published.manifest.part_keys() {
        assert!(
            existing.iter().any(|key| key.ends_with(part)),
            "referenced part {part} was deleted (deleted: {:?})",
            script.deleted.lock().unwrap()
        );
    }
    let KeyedReadOutcome::Rows { through: 4, page } =
        read(&engine, 4, Duration::from_secs(5)).await
    else {
        panic!("keyed state is unreadable");
    };
    let records = log.records.lock().unwrap().clone();
    let state = KeyedState::fold(records.iter().map(String::as_str)).unwrap();
    assert_eq!(page.body(), state.range(&all_rows()).body());
}

fn manifests_by_generation(raw: &MemoryObjectStore) -> Vec<(u64, u64)> {
    let mut published = Vec::new();
    for (key, _modified) in raw.snapshot() {
        if !key.contains("/manifests/") {
            continue;
        }
        let bytes = raw.object_bytes(&key).unwrap();
        let manifest: KeyedManifest = serde_json::from_slice(&bytes).unwrap();
        published.push((manifest.generation, manifest.through_record));
    }
    published.sort_unstable();
    published
}

#[tokio::test(start_paused = true)]
async fn split_continuity_rebuild_never_publishes_a_lower_d() {
    let log = Arc::new(FakeLog::default());
    let clock: Arc<dyn Clock> = Arc::new(TestClock(tokio::time::Instant::now()));
    let raw = MemoryObjectStore::new(Arc::clone(&clock));
    let engine = KeyedEngine::with_clock(
        ObjectStore::from(raw.clone()),
        Arc::clone(&log),
        None,
        KeyedEngineConfig {
            min_publish_interval: Duration::ZERO,
            gc_grace: GRACE,
            // A few records per publication.
            max_ingest_bytes: 64,
            ..KeyedEngineConfig::default()
        },
        clock,
    );
    log.records.lock().unwrap().extend((0..12).map(record));
    let KeyedReadOutcome::Rows { through: 12, .. } =
        read(&engine, 12, Duration::from_secs(5)).await
    else {
        panic!("initial publications");
    };
    // A restore: the log keeps records 0..3 and diverges after them.
    {
        let mut records = log.records.lock().unwrap();
        records.truncate(3);
        records.extend((100..112).map(record));
    }
    let tail = log.records.lock().unwrap().len() as u64;
    let mut reached = false;
    for _ in 0..20 {
        if let KeyedReadOutcome::Rows { through, .. } =
            read(&engine, tail, Duration::from_secs(5)).await
            && through == tail
        {
            reached = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(reached, "the rebuild reaches the restored tail");
    let published = manifests_by_generation(&raw);
    for pair in published.windows(2) {
        assert!(
            pair[1].1 >= pair[0].1,
            "D went back from {:?} to {:?} (all: {published:?})",
            pair[0],
            pair[1]
        );
    }
}
