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
//! - Cross-pod reuse: pins only order one engine's own GC. Another pod's
//!   GC (or a sweep run as a separate process) can delete an old orphan
//!   that a writer reuses through the content-addressed put-if-absent
//!   dedupe; the writer must not publish a manifest whose part is gone.
//!   Deleters therefore act only on fresh observations: a sweep whose
//!   LIST is older than the decision TTL observes an object again before
//!   deleting it.

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
use ursula_index::AppliedChange;
use ursula_index::FaultDecision;
use ursula_index::MemoryObjectStore;
use ursula_index::ObjectFault;
use ursula_index::ObjectHooks;
use ursula_index::ObjectOp;
use ursula_index::ObjectStore;
use ursula_index::clock::Clock;
use ursula_index::keyed::IncarnationState;
use ursula_index::keyed::KeyedEngine;
use ursula_index::keyed::KeyedEngineConfig;
use ursula_index::keyed::KeyedEntry;
use ursula_index::keyed::KeyedManifest;
use ursula_index::keyed::KeyedNamespace;
use ursula_index::keyed::KeyedReadOutcome;
use ursula_index::keyed::KeyedReadRequest;
use ursula_index::keyed::KeyedSource;
use ursula_index::keyed::KeyedState;
use ursula_index::keyed::Lower;
use ursula_index::keyed::PartOptions;
use ursula_index::keyed::RangeQuery;
use ursula_index::keyed::Selection;
use ursula_index::keyed::SourceClient;
use ursula_index::keyed::SourceError;
use ursula_index::keyed::SourcePage;
use ursula_index::keyed::encode_key;
use ursula_index::keyed::manifest::ObjectWrite;
use ursula_index::keyed::manifest::delete_decision_ttl;
use ursula_index::keyed::part::encode_part;

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

impl ObjectHooks for Script {
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
    let store = ObjectStore::from(raw.clone()).with_hooks(script.clone());
    let engine = KeyedEngine::with_clock(
        store.clone(),
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
    let published = KeyedNamespace::new(ObjectStore::from(raw.clone()), source())
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

/// Every object `CURRENT` references, and whether it exists.
fn referenced_objects(raw: &MemoryObjectStore) -> Vec<(String, bool)> {
    let existing: Vec<String> = raw.snapshot().into_iter().map(|(key, _)| key).collect();
    let current = existing
        .iter()
        .find(|key| key.ends_with("/CURRENT"))
        .expect("a published namespace")
        .clone();
    let prefix = current.trim_end_matches("CURRENT").to_owned();
    let pointer: serde_json::Value =
        serde_json::from_slice(&raw.object_bytes(&current).unwrap()).unwrap();
    let manifest_key = pointer["manifest"].as_str().unwrap().to_owned();
    let manifest: KeyedManifest = serde_json::from_slice(
        &raw.object_bytes(&format!("{prefix}{manifest_key}"))
            .unwrap(),
    )
    .unwrap();
    std::iter::once(manifest_key)
        .chain(manifest.part_keys().map(str::to_owned))
        .map(|key| {
            let object = format!("{prefix}{key}");
            let exists = existing.contains(&object);
            (key, exists)
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn another_pods_gc_cannot_delete_a_part_reused_by_dedupe() {
    let log = Arc::new(FakeLog::default());
    let clock: Arc<dyn Clock> = Arc::new(TestClock(tokio::time::Instant::now()));
    let raw = MemoryObjectStore::new(Arc::clone(&clock));
    let config = KeyedEngineConfig {
        min_publish_interval: Duration::ZERO,
        gc_grace: GRACE,
        gc_tick: Duration::from_secs(3_600),
        // Reads go to the store, so a deleted part is noticed.
        write_cache_bytes: 0,
        ..KeyedEngineConfig::default()
    };
    // Pod A serves; pod B loses a CAS and later collects its garbage, with
    // its DELETEs in flight for a while.
    let pod_a = KeyedEngine::with_clock(
        ObjectStore::from(raw.clone()),
        Arc::clone(&log),
        None,
        config.clone(),
        Arc::clone(&clock),
    );
    let script = Arc::new(Script {
        log: Arc::clone(&log),
        conflict_next_cas: AtomicBool::new(false),
        delete_delay: Mutex::new(Duration::ZERO),
        deleted: Mutex::new(Vec::new()),
    });
    let pod_b = KeyedEngine::with_clock(
        ObjectStore::from(raw.clone()).with_hooks(script.clone()),
        Arc::clone(&log),
        None,
        config,
        Arc::clone(&clock),
    );

    // Generation 1 covers records 0..2.
    log.records.lock().unwrap().extend((0..2).map(record));
    let KeyedReadOutcome::Rows { through: 2, .. } = read(&pod_a, 2, Duration::from_secs(5)).await
    else {
        panic!("first publication");
    };

    // Pod B folds records 2..4 and loses its CAS: its parts are orphans in
    // its GC queue, due after the grace period.
    log.records.lock().unwrap().extend((2..4).map(record));
    script.conflict_next_cas.store(true, Ordering::SeqCst);
    let outcome = read(&pod_b, 4, Duration::from_secs(1)).await;
    assert!(
        !matches!(outcome, KeyedReadOutcome::Rows { through: 4, .. }),
        "{outcome:?}"
    );
    let orphans: Vec<String> = raw
        .snapshot()
        .into_iter()
        .map(|(key, _)| key)
        .filter(|key| key.contains("/parts/"))
        .collect();

    // Past the grace period, pod B's GC decides to delete them (they are
    // old and unreferenced); its DELETEs take a second to land.
    tokio::time::sleep(GRACE + Duration::from_secs(1)).await;
    *script.delete_delay.lock().unwrap() = Duration::from_secs(1);
    let gc = tokio::spawn({
        let pod_b = pod_b.clone();
        async move { pod_b.collect_garbage().await }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Meanwhile pod A folds the same records into byte-identical parts,
    // finds them present and reuses them.
    log.failing.store(false, Ordering::SeqCst);
    let outcome = read(&pod_a, 4, Duration::from_secs(30)).await;
    let deleted = gc.await.unwrap();
    assert!(deleted > 0, "pod B's GC ran");
    assert!(
        script
            .deleted
            .lock()
            .unwrap()
            .iter()
            .any(|key| orphans.contains(key)),
        "pod B deleted a part pod A was reusing"
    );
    let KeyedReadOutcome::Rows { through: 4, .. } = outcome else {
        panic!("second publication: {outcome:?}");
    };
    let missing: Vec<String> = referenced_objects(&raw)
        .into_iter()
        .filter(|(_, exists)| !exists)
        .map(|(key, _)| key)
        .collect();
    assert!(missing.is_empty(), "CURRENT references missing {missing:?}");
    let KeyedReadOutcome::Rows { through: 4, page } = read(&pod_a, 4, Duration::from_secs(5)).await
    else {
        panic!("keyed state is unreadable");
    };
    let records = log.records.lock().unwrap().clone();
    let state = KeyedState::fold(records.iter().map(String::as_str)).unwrap();
    assert_eq!(page.body(), state.range(&all_rows()).body());
    assert!(pod_a.metrics().reused_settled > 0);
}

/// Delays every DELETE.
struct SlowDeletes(Duration);

impl ObjectHooks for SlowDeletes {
    fn decide(&self, op: ObjectOp, _key: &str) -> FaultDecision {
        FaultDecision {
            delay: if op == ObjectOp::Delete {
                self.0
            } else {
                Duration::ZERO
            },
            fault: ObjectFault::None,
        }
    }
}

#[tokio::test(start_paused = true)]
async fn sweep_observes_again_before_acting_on_a_stale_listing() {
    let clock: Arc<dyn Clock> = Arc::new(TestClock(tokio::time::Instant::now()));
    let raw = MemoryObjectStore::new(Arc::clone(&clock));
    let writer = KeyedNamespace::new(ObjectStore::from(raw.clone()), source());
    let options = PartOptions::default();
    let mut parts: Vec<_> = (0..2_u8)
        .map(|index| {
            let entry = KeyedEntry {
                key: vec![index],
                record: u64::from(index),
                value: Some(index.to_string()),
            };
            encode_part(&[entry], &[], &options).unwrap()
        })
        .collect();
    parts.sort_by(|a, b| a.meta.key.cmp(&b.meta.key));
    for part in &parts {
        assert_eq!(writer.put_part(part).await.unwrap(), ObjectWrite::Created);
    }
    tokio::time::sleep(GRACE + Duration::from_secs(1)).await;

    // The sweep lists both orphans as old; its first DELETE takes longer
    // than the decision TTL, and meanwhile a writer reuses the second one.
    assert!(Duration::from_secs(3) > delete_decision_ttl(GRACE));
    let sweeper = KeyedNamespace::new(
        ObjectStore::from(raw.clone()).with_hooks(Arc::new(SlowDeletes(Duration::from_secs(3)))),
        source(),
    );
    let sweep = tokio::spawn({
        let clock = Arc::clone(&clock);
        async move { sweeper.sweep(&*clock, GRACE, false).await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        writer.put_part(&parts[1]).await.unwrap(),
        ObjectWrite::Refreshed
    );
    let report = sweep.await.unwrap().unwrap();
    assert_eq!(report.deleted, vec![parts[0].meta.key.clone()]);
    let existing: Vec<String> = raw.snapshot().into_iter().map(|(key, _)| key).collect();
    assert!(
        existing.iter().any(|key| key.ends_with(&parts[1].meta.key)),
        "the reused part survived the sweep"
    );
}

/// IX1: a pod holding an older manifest serves reads with
/// `min_through_record <= D` without revalidating `CURRENT`. After another
/// pod's compaction and GC removed that manifest's parts, the read must
/// reload `CURRENT` and retry instead of answering 503 indefinitely.
#[tokio::test(start_paused = true)]
async fn stale_pod_reloads_current_when_its_parts_were_collected() {
    let log = Arc::new(FakeLog::default());
    let clock: Arc<dyn Clock> = Arc::new(TestClock(tokio::time::Instant::now()));
    let raw = MemoryObjectStore::new(Arc::clone(&clock));
    let config = KeyedEngineConfig {
        min_publish_interval: Duration::ZERO,
        gc_grace: GRACE,
        gc_tick: Duration::from_secs(3_600),
        // Reads go to the store, so a deleted part is noticed.
        write_cache_bytes: 0,
        footer_cache_bytes: 0,
        // Two runs merge at once.
        policy: ursula_index::keyed::CompactionPolicy {
            ratio: 1_000,
            width: 2,
            amp_percent: 1,
            max_runs: 8,
        },
        ..KeyedEngineConfig::default()
    };
    let pod_a = KeyedEngine::with_clock(
        ObjectStore::from(raw.clone()),
        Arc::clone(&log),
        None,
        config.clone(),
        Arc::clone(&clock),
    );
    let pod_b = KeyedEngine::with_clock(
        ObjectStore::from(raw.clone()),
        Arc::clone(&log),
        None,
        config,
        Arc::clone(&clock),
    );

    log.records.lock().unwrap().extend((0..2).map(record));
    let KeyedReadOutcome::Rows { through: 2, .. } = read(&pod_a, 2, Duration::from_secs(5)).await
    else {
        panic!("pod A publishes generation 1");
    };
    let first_parts: Vec<String> = raw
        .snapshot()
        .into_iter()
        .map(|(key, _)| key)
        .filter(|key| key.contains("/parts/"))
        .collect();
    assert!(!first_parts.is_empty());

    // Pod B ingests more records, compacts the two runs into one and, past
    // the grace period, collects generation 1's parts.
    log.records.lock().unwrap().extend((2..4).map(record));
    let KeyedReadOutcome::Rows { through: 4, .. } = read(&pod_b, 4, Duration::from_secs(5)).await
    else {
        panic!("pod B publishes");
    };
    tokio::time::sleep(Duration::from_secs(1)).await;
    tokio::time::sleep(GRACE + Duration::from_secs(1)).await;
    pod_b.collect_garbage().await;
    let remaining: Vec<String> = raw.snapshot().into_iter().map(|(key, _)| key).collect();
    assert!(
        first_parts.iter().any(|part| !remaining.contains(part)),
        "pod B's compaction and GC removed a part of generation 1"
    );

    // Pod A still holds generation 1; a read it can answer from that view
    // must reload CURRENT and succeed.
    let KeyedReadOutcome::Rows { through, page } = read(&pod_a, 2, Duration::from_secs(5)).await
    else {
        panic!("stale pod cannot read after another pod's GC");
    };
    assert_eq!(through, 4);
    let records = log.records.lock().unwrap().clone();
    let state = KeyedState::fold(records.iter().map(String::as_str)).unwrap();
    assert_eq!(page.body(), state.range(&all_rows()).body());
}

/// IX2: engine GC checked a queued orphan only against `CURRENT`. Parts are
/// content-addressed, so another pod can publish the same part in a later
/// manifest; once a newer manifest replaces that one, the part is still in
/// use by readers of the recent manifest for the grace period. The orphan's
/// deletion must respect every manifest written within the grace.
#[tokio::test(start_paused = true)]
async fn gc_spares_an_orphan_referenced_by_a_recent_manifest() {
    let log = Arc::new(FakeLog::default());
    let clock: Arc<dyn Clock> = Arc::new(TestClock(tokio::time::Instant::now()));
    let raw = MemoryObjectStore::new(Arc::clone(&clock));
    let config = KeyedEngineConfig {
        min_publish_interval: Duration::ZERO,
        gc_grace: GRACE,
        gc_tick: Duration::from_secs(3_600),
        write_cache_bytes: 0,
        // Tier merges never include the oldest run: the third run merges
        // with the second (the reused one), two runs never merge.
        policy: ursula_index::keyed::CompactionPolicy {
            ratio: 1_000,
            width: 2,
            amp_percent: 1_000_000,
            max_runs: 8,
        },
        ..KeyedEngineConfig::default()
    };
    let pod_a = KeyedEngine::with_clock(
        ObjectStore::from(raw.clone()),
        Arc::clone(&log),
        None,
        config.clone(),
        Arc::clone(&clock),
    );
    let script = Arc::new(Script {
        log: Arc::clone(&log),
        conflict_next_cas: AtomicBool::new(false),
        delete_delay: Mutex::new(Duration::ZERO),
        deleted: Mutex::new(Vec::new()),
    });
    let pod_b = KeyedEngine::with_clock(
        ObjectStore::from(raw.clone()).with_hooks(script.clone()),
        Arc::clone(&log),
        None,
        config,
        Arc::clone(&clock),
    );

    log.records.lock().unwrap().extend((0..2).map(record));
    let KeyedReadOutcome::Rows { through: 2, .. } = read(&pod_a, 2, Duration::from_secs(5)).await
    else {
        panic!("generation 1");
    };
    let parts = || -> Vec<String> {
        raw.snapshot()
            .into_iter()
            .map(|(key, _)| key)
            .filter(|key| key.contains("/parts/"))
            .collect()
    };
    let before = parts();
    // Pod B folds records 2..4 and loses its CAS: its parts are orphans in
    // its GC queue.
    log.records.lock().unwrap().extend((2..4).map(record));
    script.conflict_next_cas.store(true, Ordering::SeqCst);
    let _lost = read(&pod_b, 4, Duration::from_secs(1)).await;
    log.failing.store(false, Ordering::SeqCst);
    let orphans: Vec<String> = parts()
        .into_iter()
        .filter(|key| !before.contains(key))
        .collect();
    assert!(!orphans.is_empty());

    // Pod A publishes the same records, reusing pod B's parts.
    let KeyedReadOutcome::Rows { through: 4, .. } = read(&pod_a, 4, Duration::from_secs(30)).await
    else {
        panic!("generation 2");
    };
    // Shortly before pod B's GC, pod A ingests more and compacts the three
    // runs: CURRENT no longer references the reused parts, but the manifest
    // just before the compaction (written within the grace) does.
    tokio::time::sleep(GRACE.checked_sub(Duration::from_secs(1)).unwrap()).await;
    log.records.lock().unwrap().extend((4..6).map(record));
    let KeyedReadOutcome::Rows { through: 6, .. } = read(&pod_a, 6, Duration::from_secs(30)).await
    else {
        panic!("generation 3");
    };
    tokio::time::sleep(Duration::from_secs(1)).await;
    let current_parts = referenced_objects(&raw);
    let reused_in_current = orphans.iter().any(|orphan| {
        current_parts
            .iter()
            .any(|(key, _)| orphan.ends_with(key.as_str()))
    });
    assert!(
        !reused_in_current,
        "the compaction replaced the reused parts"
    );

    tokio::time::sleep(Duration::from_secs(2)).await;
    pod_b.collect_garbage().await;
    let deleted = script.deleted.lock().unwrap().clone();
    assert!(
        !orphans.iter().any(|orphan| deleted.contains(orphan)),
        "pod B deleted a part a manifest within the grace references: {deleted:?}"
    );
}
