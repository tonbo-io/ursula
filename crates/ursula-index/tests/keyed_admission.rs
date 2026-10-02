//! Process-wide admission of the keyed engine: many namespaces cold-start
//! at once under a small byte budget without exceeding it, a full
//! admission queue answers 503 with `Retry-After`, and compaction under
//! contention waits and completes.

#![allow(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::panic,
    reason = "test scaffolding"
)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use futures_util::future::join_all;
use tokio::sync::watch;
use ursula_index::MemoryObjectStore;
use ursula_index::ObjectStore;
use ursula_index::clock::Clock;
use ursula_index::keyed::CompactionPolicy;
use ursula_index::keyed::IncarnationState;
use ursula_index::keyed::KeyedEngine;
use ursula_index::keyed::KeyedEngineConfig;
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
use ursula_index::keyed::http::router;

const BUCKET: &str = "b";

/// Wall clock on tokio's time.
struct TestClock(tokio::time::Instant);

impl Clock for TestClock {
    fn now_ms(&self) -> u64 {
        1_700_000_000_000 + u64::try_from(self.0.elapsed().as_millis()).unwrap()
    }
}

/// In-memory source logs, one per stream, honouring `max_bytes`; reads can
/// be held at a gate.
struct FakeLogs {
    streams: Mutex<HashMap<String, Vec<String>>>,
    /// Reads wait while this is false.
    open: watch::Sender<bool>,
}

impl FakeLogs {
    fn new() -> Self {
        Self {
            streams: Mutex::new(HashMap::new()),
            open: watch::Sender::new(true),
        }
    }

    fn append(&self, key: &str, records: impl IntoIterator<Item = String>) -> u64 {
        let mut streams = self.streams.lock().unwrap();
        let log = streams.entry(key.to_owned()).or_default();
        log.extend(records);
        log.len() as u64
    }

    fn records(&self, key: &str) -> Vec<String> {
        self.streams.lock().unwrap()[key].clone()
    }
}

impl SourceClient for FakeLogs {
    fn read<'a>(
        &'a self,
        _bucket: &'a str,
        key: &'a str,
        record: u64,
        max_bytes: u64,
        max_records: Option<u64>,
        _leader: bool,
    ) -> BoxFuture<'a, Result<SourcePage, SourceError>> {
        async move {
            let mut open = self.open.subscribe();
            if open.wait_for(|open| *open).await.is_err() {
                return Err(SourceError::Transient("gate closed".to_owned()));
            }
            // Interleave the namespaces' ingests.
            tokio::time::sleep(Duration::from_millis(1)).await;
            let streams = self.streams.lock().unwrap();
            let log = streams.get(key).ok_or(SourceError::NotFound)?;
            let next_record = log.len() as u64;
            if record > next_record {
                return Err(SourceError::BeyondTail { next_record });
            }
            let mut records = Vec::new();
            let mut bytes = 0_u64;
            let mut next = record;
            while next < next_record && max_records.is_none_or(|limit| next - record < limit) {
                let text = &log[next as usize];
                let size = text.len() as u64 + 1;
                if next > record && bytes + size > max_bytes {
                    break;
                }
                bytes += size;
                records.push(text.clone());
                next += 1;
            }
            Ok(SourcePage {
                start_record: record,
                next_record: next,
                records,
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

fn record(index: usize) -> String {
    format!(
        r#"{{"ops":[["p","{}",{{"v":{index},"pad":"{}"}}]]}}"#,
        encode_key(format!("k{}", index % 17).as_bytes()),
        "x".repeat(24)
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

fn source(key: &str) -> KeyedSource {
    KeyedSource {
        bucket: BUCKET.to_owned(),
        key: key.to_owned(),
        incarnation: 1,
    }
}

async fn read(engine: &KeyedEngine, key: &str, min: u64, timeout: Duration) -> KeyedReadOutcome {
    engine
        .read(KeyedReadRequest {
            source: source(key),
            source_next: min,
            selection: Selection::Range(all_rows()),
            min_through_record: Some(min),
            timeout,
        })
        .await
}

fn expected(logs: &FakeLogs, key: &str) -> String {
    let records = logs.records(key);
    KeyedState::fold(records.iter().map(String::as_str))
        .unwrap()
        .range(&all_rows())
        .body()
}

fn config() -> KeyedEngineConfig {
    KeyedEngineConfig {
        min_publish_interval: Duration::ZERO,
        gc_grace: Duration::from_secs(60),
        gc_tick: Duration::from_secs(3600),
        current_revalidate: Duration::ZERO,
        source_page_bytes: 1024,
        part_options: PartOptions {
            data_page_rows: 2,
            row_group_rows: 8,
            layout_block_bytes: 256,
            target_part_bytes: 512,
            read_batch_rows: 2,
        },
        ..KeyedEngineConfig::default()
    }
}

fn engine(logs: &Arc<FakeLogs>, config: KeyedEngineConfig) -> KeyedEngine {
    let clock: Arc<dyn Clock> = Arc::new(TestClock(tokio::time::Instant::now()));
    let store = ObjectStore::from(MemoryObjectStore::new(Arc::clone(&clock)));
    KeyedEngine::with_clock(store, Arc::clone(logs), None, config, clock)
}

#[tokio::test(start_paused = true)]
async fn many_cold_namespaces_stay_within_the_budget() {
    const NAMESPACES: usize = 64;
    const RECORDS: usize = 200;
    const BUDGET: u64 = 8 * 1024;
    let logs = Arc::new(FakeLogs::new());
    for namespace in 0..NAMESPACES {
        logs.append(&format!("s{namespace}"), (0..RECORDS).map(record));
    }
    let engine = engine(&logs, KeyedEngineConfig {
        admission_budget_bytes: BUDGET,
        max_concurrent_ingests: 8,
        max_concurrent_compactions: 2,
        admission_queue: NAMESPACES,
        ..config()
    });

    let outcomes = join_all((0..NAMESPACES).map(|namespace| {
        let engine = engine.clone();
        async move {
            let key = format!("s{namespace}");
            let outcome = read(&engine, &key, RECORDS as u64, Duration::from_secs(600)).await;
            (key, outcome)
        }
    }))
    .await;
    for (key, outcome) in outcomes {
        let KeyedReadOutcome::Rows { through, page } = outcome else {
            panic!("{key}: {outcome:?}");
        };
        assert_eq!(through, RECORDS as u64, "{key}");
        assert_eq!(page.body(), expected(&logs, &key), "{key}");
    }

    let admission = engine.metrics().admission;
    assert_eq!(admission.budget_bytes, BUDGET);
    assert!(
        admission.peak_in_use_bytes <= BUDGET,
        "reserved {} of a {BUDGET} B budget",
        admission.peak_in_use_bytes
    );
    // The budget was contended: reservations waited, or ingests published
    // early to release it.
    assert!(
        admission.budget_waits + admission.budget_truncations > 0,
        "{admission:?}"
    );
    assert_eq!(admission.rejections, 0, "{admission:?}");
    // Every reservation, slot and queue place is returned (workers finish
    // their compaction passes in the background).
    for _ in 0..1_000 {
        let admission = engine.metrics().admission;
        if admission.in_use_bytes == 0
            && admission.queue_depth == 0
            && admission.ingests_running == 0
            && admission.compactions_running == 0
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "admission was not released: {:?}",
        engine.metrics().admission
    );
}

#[tokio::test]
async fn a_full_admission_queue_answers_503_with_retry_after() {
    let logs = Arc::new(FakeLogs::new());
    for key in ["s0", "s1", "s2"] {
        logs.append(key, (0..10).map(record));
    }
    let engine = engine(&logs, KeyedEngineConfig {
        max_concurrent_ingests: 1,
        admission_queue: 1,
        ..config()
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(axum::serve(listener, router(engine.clone())).into_future());

    // s0 ingests (its source reads are held); s1 waits in the queue.
    logs.open.send_replace(false);
    let first = tokio::spawn({
        let engine = engine.clone();
        async move { read(&engine, "s0", 10, Duration::from_secs(30)).await }
    });
    wait_for(&engine, |admission| admission.ingests_running == 1).await;
    let second = tokio::spawn({
        let engine = engine.clone();
        async move { read(&engine, "s1", 10, Duration::from_secs(30)).await }
    });
    wait_for(&engine, |admission| admission.queue_depth == 1).await;

    // s2 needs a new worker: the queue is full.
    let url = format!(
        "http://{address}/v1/keyed/{BUCKET}/s2?incarnation=1&source_next=10&min_through_record=10"
    );
    let response = reqwest::get(&url).await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get("retry-after")
            .map(|value| value.to_str().unwrap()),
        Some("1")
    );
    assert_eq!(engine.metrics().admission.rejections, 1);

    // Joining a namespace whose worker is already admitted needs no place.
    let joined = tokio::spawn({
        let engine = engine.clone();
        async move { read(&engine, "s1", 10, Duration::from_secs(30)).await }
    });

    logs.open.send_replace(true);
    for (key, outcome) in [
        ("s0", first.await.unwrap()),
        ("s1", second.await.unwrap()),
        ("s1", joined.await.unwrap()),
    ] {
        let KeyedReadOutcome::Rows { through: 10, page } = outcome else {
            panic!("{key}: {outcome:?}");
        };
        assert_eq!(page.body(), expected(&logs, key));
    }
    // The client's retry succeeds once the queue drains.
    let response = reqwest::get(&url).await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), expected(&logs, "s2"));
}

async fn wait_for(
    engine: &KeyedEngine,
    condition: impl Fn(&ursula_index::keyed::AdmissionMetrics) -> bool,
) {
    for _ in 0..1_000 {
        if condition(&engine.metrics().admission) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("condition not reached: {:?}", engine.metrics().admission);
}

#[tokio::test(start_paused = true)]
async fn compaction_under_contention_completes() {
    const NAMESPACES: usize = 16;
    const ROUNDS: usize = 6;
    const BUDGET: u64 = 4 * 1024;
    let logs = Arc::new(FakeLogs::new());
    let engine = engine(&logs, KeyedEngineConfig {
        admission_budget_bytes: BUDGET,
        max_concurrent_ingests: 4,
        max_concurrent_compactions: 1,
        admission_queue: NAMESPACES,
        policy: CompactionPolicy {
            amp_percent: 1,
            ..CompactionPolicy::default()
        },
        ..config()
    });
    for round in 0..ROUNDS {
        let outcomes = join_all((0..NAMESPACES).map(|namespace| {
            let engine = engine.clone();
            let logs = Arc::clone(&logs);
            async move {
                let key = format!("s{namespace}");
                let tail = logs.append(&key, (round * 20..(round + 1) * 20).map(record));
                (
                    key,
                    read(
                        &engine,
                        &format!("s{namespace}"),
                        tail,
                        Duration::from_secs(600),
                    )
                    .await,
                )
            }
        }))
        .await;
        for (key, outcome) in outcomes {
            let KeyedReadOutcome::Rows { page, .. } = outcome else {
                panic!("{key}: {outcome:?}");
            };
            assert_eq!(page.body(), expected(&logs, &key), "{key}");
        }
    }
    // Let the last compaction passes finish.
    for _ in 0..1_000 {
        let admission = engine.metrics().admission;
        if admission.ingests_running == 0 && admission.compactions_running == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let metrics = engine.metrics();
    assert!(metrics.compaction_publishes > 0, "{metrics:?}");
    assert!(metrics.admission.peak_in_use_bytes <= BUDGET, "{metrics:?}");
    assert_eq!(metrics.admission.in_use_bytes, 0, "{metrics:?}");
    for namespace in 0..NAMESPACES {
        let key = format!("s{namespace}");
        let tail = logs.records(&key).len() as u64;
        let KeyedReadOutcome::Rows { through, page } =
            read(&engine, &key, tail, Duration::from_secs(600)).await
        else {
            panic!("{key}");
        };
        assert_eq!(through, tail);
        assert_eq!(page.body(), expected(&logs, &key), "{key}");
    }
}
