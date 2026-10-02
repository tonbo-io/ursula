//! Service-level tests of the keyed engine (design §3.4, §5.5, §6.1 U16,
//! U17): the internal `/v1/keyed` API over an in-process fake source that
//! speaks the node's record-read, HEAD and bucket-listing surfaces.

#![allow(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::panic,
    reason = "test helpers outside #[test] functions index and do arithmetic on generated data"
)]

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Router;
use axum::extract::Path;
use axum::extract::RawQuery;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use reqwest::Url;
use ursula_index::FsObjectStore;
use ursula_index::ObjectStore;
use ursula_index::keyed::CompactionPolicy;
use ursula_index::keyed::KeyedEngine;
use ursula_index::keyed::KeyedEngineConfig;
use ursula_index::keyed::KeyedNamespace;
use ursula_index::keyed::KeyedSource;
use ursula_index::keyed::KeyedSourceClient;
use ursula_index::keyed::KeyedState;
use ursula_index::keyed::Lower;
use ursula_index::keyed::PartOptions;
use ursula_index::keyed::RangeQuery;
use ursula_index::keyed::encode_key;
use ursula_index::keyed::http::router;
use ursula_index::keyed::manifest::namespace_prefix;

const BUCKET: &str = "b1";

// ---------------------------------------------------------------- fake source

#[derive(Clone, Debug)]
struct FakeStream {
    incarnation: u64,
    records: Vec<String>,
}

#[derive(Default)]
struct FakeState {
    streams: HashMap<(String, String), FakeStream>,
    /// Listing answers this incarnation instead of the stream's (a recreate).
    listing_override: Option<u64>,
    /// Old node: record reads with `max_bytes` answer 400.
    reject_max_bytes: bool,
    /// Followers lag: non-leader reads see at most this many records.
    follower_tail: Option<u64>,
    /// Non-leader reads from record 0 answer 503 (a rebuild's source fails).
    fail_record_zero: bool,
}

#[derive(Clone, Default)]
struct FakeSource {
    state: Arc<Mutex<FakeState>>,
    reads: Arc<AtomicUsize>,
    heads: Arc<AtomicUsize>,
}

impl FakeSource {
    fn create(&self, key: &str, incarnation: u64) {
        self.state.lock().unwrap().streams.insert(
            (BUCKET.to_owned(), key.to_owned()),
            FakeStream {
                incarnation,
                records: Vec::new(),
            },
        );
    }

    fn append(&self, key: &str, record: String) -> u64 {
        let mut state = self.state.lock().unwrap();
        let stream = state
            .streams
            .get_mut(&(BUCKET.to_owned(), key.to_owned()))
            .unwrap();
        stream.records.push(record);
        stream.records.len() as u64
    }

    fn records(&self, key: &str) -> Vec<String> {
        self.state.lock().unwrap().streams[&(BUCKET.to_owned(), key.to_owned())]
            .records
            .clone()
    }

    fn replace(&self, key: &str, records: Vec<String>) {
        self.state
            .lock()
            .unwrap()
            .streams
            .get_mut(&(BUCKET.to_owned(), key.to_owned()))
            .unwrap()
            .records = records;
    }
}

fn params(raw: Option<&str>) -> HashMap<String, String> {
    url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes())
        .into_owned()
        .collect()
}

async fn fake_get(
    State(fake): State<FakeSource>,
    Path((bucket, rest)): Path<(String, String)>,
    RawQuery(raw): RawQuery,
) -> Response {
    let query = params(raw.as_deref());
    let state = fake.state.lock().unwrap();
    if rest == "streams" {
        let prefix = query.get("prefix").cloned().unwrap_or_default();
        let mut streams: Vec<_> = state
            .streams
            .iter()
            .filter(|((b, key), _)| *b == bucket && key.starts_with(&prefix))
            .map(|((_, key), stream)| {
                serde_json::json!({
                    "stream_id": key,
                    "created_at_ms": state.listing_override.unwrap_or(stream.incarnation),
                })
            })
            .collect();
        streams.sort_by_key(|entry| entry["stream_id"].as_str().unwrap().to_owned());
        streams.truncate(1);
        return axum::Json(serde_json::json!({ "streams": streams })).into_response();
    }
    let Some(stream) = state.streams.get(&(bucket, rest)) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    fake.reads.fetch_add(1, Ordering::SeqCst);
    let leader = query
        .get("consistency")
        .is_some_and(|value| value == "leader");
    let mut tail = stream.records.len() as u64;
    if !leader && let Some(follower) = state.follower_tail {
        tail = tail.min(follower);
    }
    let record: u64 = query["record"].parse().unwrap();
    if record == 0 && !leader && state.fail_record_zero {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let mut headers = HeaderMap::new();
    headers.insert(
        "stream-extensions",
        "json-record-coordinates-v1, keyed-batch-v1"
            .parse()
            .unwrap(),
    );
    headers.insert("stream-record-first", "0".parse().unwrap());
    if record > tail {
        headers.insert("stream-record-next", tail.into());
        return (StatusCode::BAD_REQUEST, headers).into_response();
    }
    if query.contains_key("max_bytes") && state.reject_max_bytes {
        return (StatusCode::BAD_REQUEST, "max_bytes is not allowed").into_response();
    }
    let max_bytes: u64 = query
        .get("max_bytes")
        .map_or(u64::MAX, |raw| raw.parse().unwrap());
    let max_records: u64 = query
        .get("max_records")
        .map_or(u64::MAX, |raw| raw.parse().unwrap());
    let mut body = String::new();
    let mut next = record;
    while next < tail && next - record < max_records {
        let line = &stream.records[next as usize];
        if next > record && (body.len() + line.len() + 1) as u64 > max_bytes {
            break;
        }
        body.push_str(line);
        body.push('\n');
        next += 1;
    }
    headers.insert("stream-record-start", record.into());
    headers.insert("stream-record-next", next.into());
    (StatusCode::OK, headers, body).into_response()
}

async fn fake_head(
    State(fake): State<FakeSource>,
    Path((bucket, rest)): Path<(String, String)>,
) -> StatusCode {
    fake.heads.fetch_add(1, Ordering::SeqCst);
    if fake
        .state
        .lock()
        .unwrap()
        .streams
        .contains_key(&(bucket, rest))
    {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn serve(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(axum::serve(listener, app).into_future());
    format!("http://{address}")
}

// ---------------------------------------------------------------- harness

struct Harness {
    fake: FakeSource,
    engine: KeyedEngine,
    store: ObjectStore,
    indexer: String,
    client: reqwest::Client,
    _dir: tempfile::TempDir,
}

fn test_config() -> KeyedEngineConfig {
    KeyedEngineConfig {
        min_publish_interval: Duration::ZERO,
        gc_grace: Duration::ZERO,
        gc_tick: Duration::from_secs(3600),
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

async fn harness_with(config: KeyedEngineConfig, fake: FakeSource) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let store = ObjectStore::from(FsObjectStore::new(dir.path()).unwrap());
    let source_base = serve(
        Router::new()
            .route("/{bucket}/{*rest}", get(fake_get).head(fake_head))
            .with_state(fake.clone()),
    )
    .await;
    let source = KeyedSourceClient::new(Url::parse(&source_base).unwrap()).unwrap();
    let engine = KeyedEngine::new(store.clone(), source, None, config);
    let indexer = serve(router(engine.clone())).await;
    Harness {
        fake,
        engine,
        store,
        indexer,
        client: reqwest::Client::new(),
        _dir: dir,
    }
}

async fn harness(config: KeyedEngineConfig) -> Harness {
    harness_with(config, FakeSource::default()).await
}

struct Answer {
    status: StatusCode,
    through: Option<u64>,
    after: Option<String>,
    body: String,
}

impl Harness {
    fn source(&self, key: &str) -> KeyedSource {
        let incarnation = self.fake.state.lock().unwrap().streams
            [&(BUCKET.to_owned(), key.to_owned())]
            .incarnation;
        KeyedSource {
            bucket: BUCKET.to_owned(),
            key: key.to_owned(),
            incarnation,
        }
    }

    fn url(&self, key: &str, incarnation: u64, source_next: u64, extra: &[(&str, &str)]) -> Url {
        let mut url = Url::parse(&self.indexer).unwrap();
        url.path_segments_mut()
            .unwrap()
            .extend(["v1", "keyed", BUCKET, key]);
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("incarnation", &incarnation.to_string());
            query.append_pair("source_next", &source_next.to_string());
            for (name, value) in extra {
                query.append_pair(name, value);
            }
        }
        url
    }

    async fn get_url(&self, url: Url) -> Answer {
        let response = self.client.get(url).send().await.unwrap();
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .map(|value| value.to_str().unwrap().to_owned())
        };
        let through = header("stream-keyed-through").map(|value| value.parse().unwrap());
        let after = header("stream-keyed-after");
        let status = response.status();
        Answer {
            status,
            through,
            after,
            body: response.text().await.unwrap(),
        }
    }

    /// A keyed-state read as the node forwards it, with the source's tail.
    async fn read(&self, key: &str, extra: &[(&str, &str)]) -> Answer {
        let source = self.source(key);
        let tail = self.fake.records(key).len() as u64;
        self.get_url(self.url(key, source.incarnation, tail, extra))
            .await
    }

    /// Reads every row with `after` pagination at `limit`, requiring
    /// `D >= wanted` on each page; returns the concatenated body and the
    /// `D` of each page.
    async fn scan(&self, key: &str, wanted: u64, limit: usize) -> (String, Vec<u64>) {
        let mut body = String::new();
        let mut throughs = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let wanted = wanted.to_string();
            let limit = limit.to_string();
            let mut extra = vec![
                ("min_through_record", wanted.as_str()),
                ("timeout_ms", "10000"),
                ("limit", limit.as_str()),
            ];
            if let Some(after) = &after {
                extra.push(("after", after.as_str()));
            }
            let answer = self.read(key, &extra).await;
            assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
            throughs.push(answer.through.unwrap());
            body.push_str(&answer.body);
            match answer.after {
                Some(next) => after = Some(next),
                None => return (body, throughs),
            }
        }
    }

    /// Object keys of a namespace, relative to it.
    fn objects(&self, source: &KeyedSource) -> HashSet<String> {
        let root = self._dir.path().join(namespace_prefix(source, 1));
        let mut keys = HashSet::new();
        let mut pending = vec![root.clone()];
        while let Some(directory) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(&directory) else {
                continue;
            };
            for entry in entries {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                } else if !path.to_string_lossy().ends_with("-lock") {
                    // `*.create-lock` and `*.cas-lock` are the filesystem
                    // store's own lock files.
                    let relative = path.strip_prefix(&root).unwrap();
                    keys.insert(relative.to_string_lossy().replace('\\', "/"));
                }
            }
        }
        keys
    }

    /// Object keys the published manifest references, plus `CURRENT`.
    async fn referenced(&self, source: &KeyedSource) -> HashSet<String> {
        let namespace = KeyedNamespace::new(self.store.clone(), source.clone());
        let published = namespace.load().await.unwrap().unwrap();
        let mut keys: HashSet<String> = published.manifest.part_keys().map(str::to_owned).collect();
        keys.insert(published.manifest_key.clone());
        keys.insert("CURRENT".to_owned());
        keys
    }

    async fn generation(&self, source: &KeyedSource) -> u64 {
        let namespace = KeyedNamespace::new(self.store.clone(), source.clone());
        namespace
            .load()
            .await
            .unwrap()
            .map_or(0, |published| published.manifest.generation)
    }
}

fn fold_body(records: &[String], through: u64, query: &RangeQuery) -> String {
    let state = KeyedState::fold(records[..through as usize].iter().map(String::as_str)).unwrap();
    state.range(query).body()
}

fn full_query() -> RangeQuery {
    RangeQuery {
        lower: Lower::First,
        end: None,
        limit: usize::MAX,
        budget: None,
    }
}

fn key(id: u8) -> String {
    encode_key(&[b'k', id])
}

fn put(id: u8, value: &str) -> String {
    format!(r#"{{"ops":[["p","{}",{value}]]}}"#, key(id))
}

struct Lcg(u64);

impl Lcg {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % bound
    }
}

fn random_record(rng: &mut Lcg) -> String {
    let count = 1 + rng.below(4);
    let mut ops = Vec::new();
    for _ in 0..count {
        let id = rng.below(24) as u8;
        ops.push(match rng.below(10) {
            0..=5 => {
                let value = match rng.below(4) {
                    0 => "null".to_owned(),
                    1 => rng.below(1000).to_string(),
                    2 => format!(r#"{{"n":{}, "s":"é"}}"#, rng.below(9)),
                    _ => format!("\"{}\"", "v".repeat(rng.below(160) as usize)),
                };
                format!(r#"["p","{}",{value}]"#, key(id))
            }
            6 | 7 => format!(r#"["d","{}"]"#, key(id)),
            _ => {
                let end = id.saturating_add(1 + rng.below(6) as u8);
                format!(r#"["x","{}","{}"]"#, key(id), key(end))
            }
        });
    }
    format!(r#"{{"ops":[{}],"meta":1}}"#, ops.join(","))
}

// ---------------------------------------------------------------- tests

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_read_builds_state_from_record_zero() {
    let h = harness(test_config()).await;
    h.fake.create("aff/s1", 7);
    for id in 0..5 {
        h.fake.append("aff/s1", put(id, &format!("{{\"v\":{id}}}")));
    }
    // A missing namespace is state(0).
    let answer = h.read("aff/s1", &[]).await;
    assert_eq!(answer.status, StatusCode::OK);
    assert_eq!(answer.through, Some(0));
    assert!(answer.body.is_empty());

    let answer = h.read("aff/s1", &[("min_through_record", "5")]).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    assert_eq!(answer.through, Some(5));
    let records = h.fake.records("aff/s1");
    assert_eq!(answer.body, fold_body(&records, 5, &full_query()));

    let point = key(3);
    let answer = h.read("aff/s1", &[("key", point.as_str())]).await;
    assert_eq!(
        answer.body,
        format!("{{\"key\":\"{point}\",\"record\":3,\"value\":{{\"v\":3}}}}\n")
    );
    // The namespace is the single-component affinity path.
    let source = h.source("aff/s1");
    assert!(h.objects(&source).contains("CURRENT"));
    assert!(
        h._dir
            .path()
            .join(".keyed/b1/aff%2Fs1/0000000000000007/v1/CURRENT")
            .exists()
    );

    let answer = h.read("aff/s1", &[("min_through_record", "6")]).await;
    assert_eq!(answer.status, StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_times_out_with_204_then_catches_up() {
    let h = harness(test_config()).await;
    h.fake.create("s", 1);
    for id in 0..5 {
        h.fake.append("s", put(id, "1"));
    }
    h.fake.state.lock().unwrap().follower_tail = Some(2);
    let answer = h
        .read("s", &[("min_through_record", "5"), ("timeout_ms", "300")])
        .await;
    assert_eq!(answer.status, StatusCode::NO_CONTENT, "{}", answer.body);
    assert_eq!(answer.through, Some(2));

    h.fake.state.lock().unwrap().follower_tail = None;
    let answer = h
        .read("s", &[("min_through_record", "5"), ("timeout_ms", "5000")])
        .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    assert_eq!(answer.through, Some(5));
    assert_eq!(
        answer.body,
        fold_body(&h.fake.records("s"), 5, &full_query())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_waiters_share_one_ingest() {
    let h = harness(test_config()).await;
    h.fake.create("s", 1);
    for id in 0..6 {
        h.fake.append("s", put(id, "true"));
    }
    let reads =
        (0..16).map(|_| h.read("s", &[("min_through_record", "6"), ("timeout_ms", "5000")]));
    for answer in futures_util::future::join_all(reads).await {
        assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
        assert_eq!(answer.through, Some(6));
    }
    assert_eq!(h.generation(&h.source("s")).await, 1);
    assert_eq!(h.fake.reads.load(Ordering::SeqCst), 1, "one source page");
    // One post-CAS incarnation HEAD per publish.
    assert_eq!(h.fake.heads.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_interval_coalesces_waiters() {
    let h = harness(KeyedEngineConfig {
        min_publish_interval: Duration::from_millis(400),
        ..test_config()
    })
    .await;
    h.fake.create("s", 1);
    for id in 0..3 {
        h.fake.append("s", put(id, "1"));
    }
    let answer = h.read("s", &[("min_through_record", "3")]).await;
    assert_eq!(answer.through, Some(3));
    let published = std::time::Instant::now();
    for id in 3..6 {
        h.fake.append("s", put(id, "2"));
    }
    let reads = (0..8).map(|_| h.read("s", &[("min_through_record", "6"), ("timeout_ms", "5000")]));
    for answer in futures_util::future::join_all(reads).await {
        assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
        assert_eq!(answer.through, Some(6));
    }
    assert!(published.elapsed() >= Duration::from_millis(300));
    assert_eq!(h.generation(&h.source("s")).await, 2);
}

/// Polls until a read with `min_through_record` answers 200.
async fn read_until_ok(h: &Harness, key: &str, wanted: u64) -> Answer {
    let wanted = wanted.to_string();
    for _ in 0..100 {
        let answer = h
            .read(key, &[
                ("min_through_record", wanted.as_str()),
                ("timeout_ms", "2000"),
            ])
            .await;
        if answer.status == StatusCode::OK {
            return answer;
        }
        assert_eq!(
            answer.status,
            StatusCode::SERVICE_UNAVAILABLE,
            "{}",
            answer.body
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("keyed state never became readable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continuity_failure_rebuilds_from_record_zero() {
    let h = harness(test_config()).await;
    h.fake.create("s", 1);
    for id in 0..5 {
        h.fake.append("s", put(id, "1"));
    }
    assert_eq!(read_until_ok(&h, "s", 5).await.through, Some(5));

    // A restore rewinds the log below D: 503 while D > N, then the
    // namespace is rebuilt from the restored log.
    let rewound: Vec<String> = (0..3).map(|id| put(id + 10, "\"r\"")).collect();
    h.fake.replace("s", rewound.clone());
    let answer = h.read("s", &[]).await;
    assert_eq!(answer.status, StatusCode::SERVICE_UNAVAILABLE);
    let answer = read_until_ok(&h, "s", 3).await;
    assert_eq!(answer.through, Some(3));
    assert_eq!(answer.body, fold_body(&rewound, 3, &full_query()));

    // Record D−1 differs (same length): the digest check rebuilds.
    let mut diverged: Vec<String> = (0..6).map(|id| put(id + 20, "2")).collect();
    diverged[0] = rewound[0].clone();
    h.fake.replace("s", diverged.clone());
    let answer = read_until_ok(&h, "s", 6).await;
    assert_eq!(answer.through, Some(6));
    assert_eq!(answer.body, fold_body(&diverged, 6, &full_query()));

    // GC leaves exactly the rebuilt namespace's objects.
    h.engine.collect_garbage().await;
    let source = h.source("s");
    assert_eq!(h.objects(&source), h.referenced(&source).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_and_gc_preserve_state_at_every_d() {
    let h = harness(test_config()).await;
    h.fake.create("c", 3);
    let source = h.source("c");
    let mut rng = Lcg(0x5eed);
    for step in 0..48 {
        for _ in 0..1 + rng.below(3) {
            h.fake.append("c", random_record(&mut rng));
        }
        let records = h.fake.records("c");
        let tail = records.len() as u64;
        let (body, throughs) = h.scan("c", tail, 1 + step % 7).await;
        assert!(throughs.iter().all(|through| *through == tail));
        assert_eq!(
            body,
            fold_body(&records, tail, &full_query()),
            "step {step}"
        );
        if step % 5 == 4 {
            h.engine.collect_garbage().await;
            let objects = h.objects(&source);
            let referenced = h.referenced(&source).await;
            assert!(
                referenced.is_subset(&objects),
                "GC deleted a referenced object"
            );
        }
    }
    let namespace = KeyedNamespace::new(h.store.clone(), source.clone());
    let published = namespace.load().await.unwrap().unwrap();
    assert!(published.manifest.runs.len() <= CompactionPolicy::default().max_runs);
    assert!(
        published.manifest.generation > 48,
        "compactions published: generation {}",
        published.manifest.generation
    );
    h.engine.collect_garbage().await;
    assert_eq!(h.objects(&source), h.referenced(&source).await);
    let records = h.fake.records("c");
    let tail = records.len() as u64;
    let (body, _) = h.scan("c", tail, 1000).await;
    assert_eq!(body, fold_body(&records, tail, &full_query()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_pods_race_and_leave_no_orphans() {
    let a = harness(test_config()).await;
    // Pod B shares the store and the source.
    let source_base = serve(
        Router::new()
            .route("/{bucket}/{*rest}", get(fake_get).head(fake_head))
            .with_state(a.fake.clone()),
    )
    .await;
    let engine_b = KeyedEngine::new(
        a.store.clone(),
        KeyedSourceClient::new(Url::parse(&source_base).unwrap()).unwrap(),
        None,
        test_config(),
    );
    let b = Harness {
        fake: a.fake.clone(),
        engine: engine_b.clone(),
        store: a.store.clone(),
        indexer: serve(router(engine_b)).await,
        client: reqwest::Client::new(),
        _dir: tempfile::tempdir().unwrap(),
    };
    a.fake.create("r", 9);
    let source = a.source("r");
    let mut rng = Lcg(42);
    for _ in 0..24 {
        for _ in 0..1 + rng.below(3) {
            a.fake.append("r", random_record(&mut rng));
        }
        let records = a.fake.records("r");
        let tail = records.len() as u64;
        let (left, right) =
            tokio::join!(read_until_ok(&a, "r", tail), read_until_ok(&b, "r", tail));
        for answer in [left, right] {
            let through = answer.through.unwrap();
            assert!(through >= tail);
            assert_eq!(
                answer.body,
                fold_body(&records, through, &RangeQuery {
                    limit: 100,
                    ..full_query()
                })
            );
        }
    }
    a.engine.collect_garbage().await;
    b.engine.collect_garbage().await;
    assert_eq!(a.objects(&source), a.referenced(&source).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_blocks_new_work() {
    let h = harness(test_config()).await;
    h.fake.create("d", 1);
    h.fake.append("d", put(1, "1"));
    h.fake.append("d", put(2, "2"));
    // Followers never catch up: the waiter keeps waiting.
    h.fake.state.lock().unwrap().follower_tail = Some(0);
    let waiter = h.read("d", &[("min_through_record", "2"), ("timeout_ms", "20000")]);
    let drain = async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let started = std::time::Instant::now();
        let response = h
            .client
            .post(format!("{}/v1/keyed/drain", h.indexer))
            .header("content-type", "application/json")
            .body(serde_json::json!({ "bucket": BUCKET }).to_string())
            .send()
            .await
            .unwrap();
        (response.status(), started.elapsed())
    };
    let (waited, (status, elapsed)) = tokio::join!(waiter, drain);
    assert_eq!(status, StatusCode::OK);
    assert!(elapsed < Duration::from_secs(5));
    assert_eq!(waited.status, StatusCode::SERVICE_UNAVAILABLE);
    let answer = h.read("d", &[]).await;
    assert_eq!(answer.status, StatusCode::SERVICE_UNAVAILABLE);
    // Draining again is idempotent.
    h.engine.drain(BUCKET).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gone_incarnation_deletes_its_namespace() {
    let h = harness(test_config()).await;
    h.fake.create("g", 5);
    h.fake.append("g", put(1, "1"));
    // The listing shows a recreated stream at the same path.
    h.fake.state.lock().unwrap().listing_override = Some(6);
    let _answer = h.read("g", &[("min_through_record", "1")]).await;
    let source = h.source("g");
    for _ in 0..100 {
        if h.objects(&source).is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the gone incarnation's namespace was not deleted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_node_without_max_bytes_falls_back_to_max_records() {
    let h = harness(test_config()).await;
    h.fake.create("o", 1);
    h.fake.state.lock().unwrap().reject_max_bytes = true;
    for id in 0..4 {
        h.fake.append("o", put(id, "[1]"));
    }
    let answer = read_until_ok(&h, "o", 4).await;
    assert_eq!(answer.through, Some(4));
    assert_eq!(
        answer.body,
        fold_body(&h.fake.records("o"), 4, &full_query())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_parameters_and_gzip() {
    let h = harness(test_config()).await;
    h.fake.create("p", 1);
    for id in 0..20 {
        h.fake.append("p", put(id, "\"some longer value text\""));
    }
    let k = key(1);
    for extra in [
        vec![("limit", "1"), ("limit", "2")],
        vec![("key", k.as_str()), ("limit", "2")],
        vec![("start", k.as_str()), ("after", k.as_str())],
        vec![("limit", "0")],
        vec![("limit", "1001")],
        vec![("start", "AA==")],
        vec![("min_through_record", "x")],
    ] {
        let answer = h.read("p", &extra).await;
        assert_eq!(answer.status, StatusCode::BAD_REQUEST, "{extra:?}");
    }
    let mut url = Url::parse(&h.indexer).unwrap();
    url.set_path("/v1/keyed/b1/p");
    url.set_query(Some("source_next=1"));
    assert_eq!(h.get_url(url).await.status, StatusCode::BAD_REQUEST);

    read_until_ok(&h, "p", 20).await;
    let source = h.source("p");
    let response = h
        .client
        .get(h.url("p", source.incarnation, 20, &[]))
        .header("accept-encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-encoding"], "gzip");
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(
        response.headers()["content-type"],
        "application/vnd.durable-stream-keyed-rows+ndjson"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backlog_is_published_in_steps_without_the_interval() {
    let h = harness(KeyedEngineConfig {
        min_publish_interval: Duration::from_millis(500),
        max_ingest_bytes: 120,
        ..test_config()
    })
    .await;
    h.fake.create("l", 1);
    for id in 0..20 {
        h.fake.append("l", put(id, "1"));
    }
    let started = std::time::Instant::now();
    let answer = h
        .read("l", &[
            ("min_through_record", "20"),
            ("timeout_ms", "10000"),
        ])
        .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    assert_eq!(answer.through, Some(20));
    assert!(h.generation(&h.source("l")).await >= 3);
    assert!(started.elapsed() < Duration::from_millis(1500));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unappliable_source_record_answers_500() {
    let h = harness(test_config()).await;
    h.fake.create("bad", 1);
    h.fake.append("bad", put(1, "1"));
    h.fake.append("bad", r#"{"not":"a batch"}"#.to_owned());
    let answer = h
        .read("bad", &[
            ("min_through_record", "2"),
            ("timeout_ms", "5000"),
        ])
        .await;
    assert_eq!(answer.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(answer.body.contains("record 1"), "{}", answer.body);
    // State below the bad record is still served.
    let answer = h.read("bad", &[]).await;
    assert_eq!(answer.status, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_state_is_not_served_while_its_rebuild_fails() {
    let h = harness(test_config()).await;
    h.fake.create("i", 1);
    for id in 0..5 {
        h.fake.append("i", put(id, "1"));
    }
    assert_eq!(read_until_ok(&h, "i", 5).await.through, Some(5));
    // Record 4 changes; the rebuild's reads fail for a while.
    let mut diverged = h.fake.records("i");
    diverged[4] = put(40, "4");
    diverged.push(put(41, "5"));
    h.fake.replace("i", diverged.clone());
    h.fake.state.lock().unwrap().fail_record_zero = true;
    let answer = h
        .read("i", &[("min_through_record", "6"), ("timeout_ms", "2000")])
        .await;
    assert_eq!(
        answer.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{}",
        answer.body
    );
    for _ in 0..3 {
        let answer = h.read("i", &[]).await;
        assert_eq!(
            answer.status,
            StatusCode::SERVICE_UNAVAILABLE,
            "{}",
            answer.body
        );
    }
    h.fake.state.lock().unwrap().fail_record_zero = false;
    let answer = read_until_ok(&h, "i", 6).await;
    assert_eq!(answer.body, fold_body(&diverged, 6, &full_query()));
}
