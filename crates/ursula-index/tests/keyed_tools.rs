//! Keyed maintenance tools, metrics and crash injection (design §6.1 U20,
//! U24; §10 M2 exits; P3.6):
//!
//! - object-store crashes between part write, manifest put and CAS, after
//!   the CAS, mid-compaction and mid-GC leave a servable state, orphans
//!   bounded by the crashed attempt, and `sweep` reclaims them;
//! - a lost CAS leaves no orphans after the grace period;
//! - served `D` never goes below the published `CURRENT` across pods;
//! - `verify`, `rebuild` (blue/green, with catch-up), `sweep` and `dump`;
//! - the metrics snapshot and its HTTP endpoint.

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
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
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
use ursula_index::IndexError;
use ursula_index::ObjectFaults;
use ursula_index::ObjectOp;
use ursula_index::ObjectStore;
use ursula_index::keyed::CompactionPolicy;
use ursula_index::keyed::KeyedEngine;
use ursula_index::keyed::KeyedEngineConfig;
use ursula_index::keyed::KeyedNamespace;
use ursula_index::keyed::KeyedReadOutcome;
use ursula_index::keyed::KeyedReadRequest;
use ursula_index::keyed::KeyedSource;
use ursula_index::keyed::KeyedSourceClient;
use ursula_index::keyed::KeyedState;
use ursula_index::keyed::Lower;
use ursula_index::keyed::PartOptions;
use ursula_index::keyed::RangeQuery;
use ursula_index::keyed::Selection;
use ursula_index::keyed::encode_key;
use ursula_index::keyed::http::router;
use ursula_index::keyed::manifest::namespace_prefix;
use ursula_index::keyed::metrics::INDEXER_METRICS_PATH;
use ursula_index::keyed::tools;

const BUCKET: &str = "b1";
const KEY: &str = "aff/s";
const INCARNATION: u64 = 11;

// ---------------------------------------------------------------- fake source

/// One stream's log, served with the node's record-read, HEAD and listing
/// surfaces.
#[derive(Clone, Default)]
struct FakeLog {
    records: Arc<Mutex<Vec<String>>>,
}

impl FakeLog {
    fn append(&self, record: String) -> u64 {
        let mut records = self.records.lock().unwrap();
        records.push(record);
        records.len() as u64
    }

    fn records(&self) -> Vec<String> {
        self.records.lock().unwrap().clone()
    }

    fn tail(&self) -> u64 {
        self.records.lock().unwrap().len() as u64
    }
}

fn params(raw: Option<&str>) -> HashMap<String, String> {
    url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes())
        .into_owned()
        .collect()
}

async fn fake_get(
    State(log): State<FakeLog>,
    Path((bucket, rest)): Path<(String, String)>,
    RawQuery(raw): RawQuery,
) -> Response {
    let query = params(raw.as_deref());
    if bucket != BUCKET {
        return StatusCode::NOT_FOUND.into_response();
    }
    if rest == "streams" {
        return axum::Json(serde_json::json!({
            "streams": [{ "stream_id": KEY, "created_at_ms": INCARNATION }]
        }))
        .into_response();
    }
    if rest != KEY {
        return StatusCode::NOT_FOUND.into_response();
    }
    let records = log.records.lock().unwrap();
    let tail = records.len() as u64;
    let record: u64 = query["record"].parse().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(
        "stream-extensions",
        "json-record-coordinates-v1, keyed-batch-v1"
            .parse()
            .unwrap(),
    );
    if record > tail {
        headers.insert("stream-record-next", tail.into());
        return (StatusCode::BAD_REQUEST, headers).into_response();
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
        let line = &records[next as usize];
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

async fn fake_head(Path((bucket, rest)): Path<(String, String)>) -> StatusCode {
    if bucket == BUCKET && rest == KEY {
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

// ---------------------------------------------------------------- faults

type Trigger = Box<dyn Fn(ObjectOp, &str) -> bool + Send + Sync>;

/// Kills the "process" at the first operation `trigger` selects once armed:
/// that operation and every later one fail. Records the keys written
/// between arming and the crash (the crashed attempt's objects).
struct Crash {
    armed: AtomicBool,
    dead: AtomicBool,
    trigger: Trigger,
    written: Mutex<HashSet<String>>,
}

impl Crash {
    fn new(trigger: Trigger) -> Arc<Self> {
        Arc::new(Self {
            armed: AtomicBool::new(false),
            dead: AtomicBool::new(false),
            trigger,
            written: Mutex::new(HashSet::new()),
        })
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn is_dead(&self) -> bool {
        self.dead.load(Ordering::SeqCst)
    }

    /// Keys written by the crashed attempt, relative to the namespace.
    fn written(&self) -> HashSet<String> {
        let prefix = namespace_prefix(&source(), 1);
        self.written
            .lock()
            .unwrap()
            .iter()
            .filter_map(|key| key.strip_prefix(&prefix).map(str::to_owned))
            .collect()
    }
}

impl ObjectFaults for Crash {
    fn check(&self, op: ObjectOp, key: &str) -> Result<(), IndexError> {
        if self.dead.load(Ordering::SeqCst) {
            return Err(IndexError::ObjectStore("injected: process is dead".into()));
        }
        if !self.armed.load(Ordering::SeqCst) {
            return Ok(());
        }
        if (self.trigger)(op, key) {
            self.dead.store(true, Ordering::SeqCst);
            return Err(IndexError::ObjectStore("injected crash".into()));
        }
        if op == ObjectOp::Put {
            self.written
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(key.to_owned());
        }
        Ok(())
    }
}

/// Runs `action` once, synchronously, before the first operation `trigger`
/// selects (an interleaving point for another writer).
struct Interleave {
    trigger: Trigger,
    action: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl ObjectFaults for Interleave {
    fn check(&self, op: ObjectOp, key: &str) -> Result<(), IndexError> {
        if (self.trigger)(op, key) {
            let action = self
                .action
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(action) = action {
                tokio::task::block_in_place(action);
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------- harness

fn source() -> KeyedSource {
    KeyedSource {
        bucket: BUCKET.to_owned(),
        key: KEY.to_owned(),
        incarnation: INCARNATION,
    }
}

fn part_options() -> PartOptions {
    PartOptions {
        data_page_rows: 2,
        row_group_rows: 8,
        layout_block_bytes: 256,
        target_part_bytes: 512,
        read_batch_rows: 2,
    }
}

fn config() -> KeyedEngineConfig {
    KeyedEngineConfig {
        min_publish_interval: Duration::ZERO,
        gc_grace: Duration::ZERO,
        gc_tick: Duration::from_secs(3600),
        current_revalidate: Duration::ZERO,
        part_options: part_options(),
        ..KeyedEngineConfig::default()
    }
}

/// Merges every new run into the oldest one.
fn eager_compaction() -> KeyedEngineConfig {
    KeyedEngineConfig {
        policy: CompactionPolicy {
            amp_percent: 1,
            ..CompactionPolicy::default()
        },
        ..config()
    }
}

struct World {
    dir: tempfile::TempDir,
    raw: ObjectStore,
    log: FakeLog,
    client: KeyedSourceClient,
}

impl World {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let raw = ObjectStore::from(FsObjectStore::new(dir.path()).unwrap());
        let log = FakeLog::default();
        let base = serve(
            Router::new()
                .route("/{bucket}/{*rest}", get(fake_get).head(fake_head))
                .with_state(log.clone()),
        )
        .await;
        let client = KeyedSourceClient::new(Url::parse(&base).unwrap()).unwrap();
        Self {
            dir,
            raw,
            log,
            client,
        }
    }

    fn pod(&self, store: ObjectStore, config: KeyedEngineConfig) -> KeyedEngine {
        KeyedEngine::new(store, self.client.clone(), None, config)
    }

    fn append(&self, count: usize, rng: &mut Lcg) {
        for _ in 0..count {
            self.log.append(random_record(rng));
        }
    }

    /// Object keys of the namespace, relative to it (lock files excluded).
    fn objects(&self) -> HashSet<String> {
        let root = self.dir.path().join(namespace_prefix(&source(), 1));
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
                    let relative = path.strip_prefix(&root).unwrap();
                    keys.insert(relative.to_string_lossy().replace('\\', "/"));
                }
            }
        }
        keys
    }

    async fn published(&self) -> ursula_index::keyed::PublishedKeyedManifest {
        KeyedNamespace::new(self.raw.clone(), source())
            .load()
            .await
            .unwrap()
            .unwrap()
    }

    /// `CURRENT`, its manifest and every part it references.
    async fn referenced(&self) -> HashSet<String> {
        let published = self.published().await;
        let mut keys: HashSet<String> = published.manifest.part_keys().map(str::to_owned).collect();
        keys.insert(published.manifest_key.clone());
        keys.insert("CURRENT".to_owned());
        keys
    }

    /// Verifies the published namespace against the log.
    async fn verify(&self) -> tools::VerifyReport {
        tools::verify(
            self.raw.clone(),
            &self.client,
            &source(),
            &tools::VerifyOptions {
                read: read_options(),
                ..tools::VerifyOptions::default()
            },
        )
        .await
        .unwrap()
    }

    fn expected(&self, through: u64) -> String {
        let records = self.log.records();
        let state =
            KeyedState::fold(records[..through as usize].iter().map(String::as_str)).unwrap();
        state.range(&full_query()).body()
    }
}

fn read_options() -> tools::SourceReadOptions {
    tools::SourceReadOptions {
        page_bytes: 700,
        part_options: part_options(),
        ..tools::SourceReadOptions::default()
    }
}

fn full_query() -> RangeQuery {
    RangeQuery {
        lower: Lower::First,
        end: None,
        limit: usize::MAX,
        budget: None,
    }
}

/// A keyed-state read as the node forwards it at the log's tail.
async fn read(pod: &KeyedEngine, world: &World, min: Option<u64>) -> KeyedReadOutcome {
    pod.read(KeyedReadRequest {
        source: source(),
        source_next: world.log.tail(),
        selection: Selection::Range(RangeQuery {
            limit: 1000,
            ..full_query()
        }),
        min_through_record: min,
        timeout: Duration::from_secs(5),
    })
    .await
}

/// Reads at `D ≥ min` and checks the rows against the reference fold.
async fn read_checked(pod: &KeyedEngine, world: &World, min: u64) -> u64 {
    match read(pod, world, Some(min)).await {
        KeyedReadOutcome::Rows { through, page } => {
            assert!(through >= min);
            assert_eq!(page.body(), world.expected(through));
            through
        }
        other => panic!("expected rows at D >= {min}, got {other:?}"),
    }
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

fn key(id: u8) -> String {
    encode_key(&[b'k', id])
}

fn random_record(rng: &mut Lcg) -> String {
    let count = 1 + rng.below(4);
    let mut ops = Vec::new();
    for _ in 0..count {
        let id = rng.below(24) as u8;
        ops.push(match rng.below(10) {
            0..=6 => {
                let value = format!("\"{}\"", "v".repeat(rng.below(120) as usize));
                format!(r#"["p","{}",{value}]"#, key(id))
            }
            7 | 8 => format!(r#"["d","{}"]"#, key(id)),
            _ => {
                let end = id.saturating_add(1 + rng.below(6) as u8);
                format!(r#"["x","{}","{}"]"#, key(id), key(end))
            }
        });
    }
    format!(r#"{{"ops":[{}]}}"#, ops.join(","))
}

// ---------------------------------------------------------------- crashes

/// A crash scenario: pod 1 publishes `D = 6`, then a fault armed before its
/// next work kills it. The namespace must stay servable and verifiable,
/// leave only the crashed attempt's objects (plus the delta its last
/// publication made obsolete), and `sweep` reclaims them after the grace
/// period; a restarted pod then catches up.
async fn crash_scenario(
    name: &str,
    trigger: Trigger,
    pod_config: KeyedEngineConfig,
    // Whether orphans are bounded by the crashed attempt's own writes (a
    // GC crash instead leaves earlier garbage, which only sweep reclaims).
    bounded: bool,
    // Work done while armed; returns once the crash happened.
    work: impl AsyncFnOnce(&KeyedEngine, &World),
) {
    let world = World::new().await;
    let crash = Crash::new(trigger);
    let pod = world.pod(world.raw.clone().with_faults(crash.clone()), pod_config);
    let mut rng = Lcg(0xc0ffee);
    world.append(6, &mut rng);
    assert_eq!(read_checked(&pod, &world, 6).await, 6);
    world.append(6, &mut rng);
    crash.arm();
    work(&pod, &world).await;
    assert!(crash.is_dead(), "{name}: the injected crash never happened");
    drop(pod);

    // State is a valid publication, matching the log at its D.
    let report = world.verify().await;
    assert!(report.is_ok(), "{name}: {:?}", report.mismatches);
    let published = world.published().await;
    let orphans: HashSet<String> = world
        .objects()
        .difference(&world.referenced().await)
        .cloned()
        .collect();
    // Orphans are bounded by the crashed attempt (and the obsolete delta of
    // the last publication, which its GC queue held).
    let mut allowed = crash.written();
    allowed.extend(published.manifest.obsoleted.iter().cloned());
    assert!(
        !bounded || orphans.is_subset(&allowed),
        "{name}: orphans {orphans:?} beyond the crashed attempt {allowed:?}"
    );

    // Sweep keeps everything younger than the grace period ...
    let young = tools::sweep(
        world.raw.clone(),
        &source(),
        Duration::from_secs(3600),
        std::time::SystemTime::now(),
        false,
    )
    .await
    .unwrap();
    assert!(young.deleted.is_empty(), "{name}: {young:?}");
    // ... and reclaims the orphans once they are older.
    let swept = tools::sweep(
        world.raw.clone(),
        &source(),
        Duration::ZERO,
        std::time::SystemTime::now(),
        false,
    )
    .await
    .unwrap();
    assert_eq!(
        swept.deleted.iter().cloned().collect::<HashSet<_>>(),
        orphans,
        "{name}"
    );
    assert_eq!(world.objects(), world.referenced().await, "{name}");
    assert!(world.verify().await.is_ok(), "{name}");

    // A restarted pod catches up and leaves no garbage.
    let restarted = world.pod(world.raw.clone(), config());
    assert!(read_checked(&restarted, &world, 12).await >= 12);
    restarted.collect_garbage().await;
    assert_eq!(world.objects(), world.referenced().await, "{name}");
}

fn is_put(op: ObjectOp, key: &str, needle: &str) -> bool {
    op == ObjectOp::Put && key.contains(needle)
}

/// Ingest to `D = 12` while armed; the crash makes it fail.
async fn failed_ingest(pod: &KeyedEngine, world: &World) {
    let outcome = read(pod, world, Some(12)).await;
    assert!(
        !matches!(outcome, KeyedReadOutcome::Rows { .. }),
        "{outcome:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_at_part_write_leaves_state_unchanged() {
    crash_scenario(
        "part",
        Box::new(|op, key| is_put(op, key, "/parts/")),
        config(),
        true,
        failed_ingest,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_between_part_and_manifest_leaves_state_unchanged() {
    crash_scenario(
        "manifest",
        Box::new(|op, key| is_put(op, key, "/manifests/")),
        config(),
        true,
        failed_ingest,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_between_manifest_and_cas_leaves_state_unchanged() {
    crash_scenario(
        "cas",
        Box::new(|op, key| is_put(op, key, "/CURRENT")),
        config(),
        true,
        failed_ingest,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_after_cas_before_read_back_publishes_a_valid_state() {
    let cas_done = AtomicBool::new(false);
    crash_scenario(
        "read-back",
        Box::new(move |op, key| {
            if !key.ends_with("/CURRENT") {
                return false;
            }
            match op {
                ObjectOp::Put => {
                    cas_done.store(true, Ordering::SeqCst);
                    false
                }
                ObjectOp::Get => cas_done.load(Ordering::SeqCst),
                _ => false,
            }
        }),
        config(),
        true,
        failed_ingest,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_mid_compaction_leaves_state_unchanged() {
    // The ingest publishes; the compaction that follows dies before its
    // manifest is written.
    let ingest_published = AtomicBool::new(false);
    crash_scenario(
        "compaction",
        Box::new(move |op, key| {
            if is_put(op, key, "/CURRENT") {
                ingest_published.store(true, Ordering::SeqCst);
                return false;
            }
            ingest_published.load(Ordering::SeqCst) && is_put(op, key, "/manifests/")
        }),
        eager_compaction(),
        true,
        async |pod, world| {
            assert_eq!(read_checked(pod, world, 12).await, 12);
            // The compaction runs in the worker after the publication.
            for _ in 0..250 {
                if pod.metrics().compaction_input_bytes > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(pod.metrics().compaction_input_bytes > 0);
            assert_eq!(pod.metrics().compaction_publishes, 0);
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_mid_gc_leaves_state_unchanged() {
    let deletes = AtomicUsize::new(0);
    crash_scenario(
        "gc",
        Box::new(move |op, _key| {
            op == ObjectOp::Delete && deletes.fetch_add(1, Ordering::SeqCst) == 1
        }),
        config(),
        false,
        async |pod, world| {
            // Several publications fill the GC queue, then GC dies after
            // one delete. (The arming also covers these publications; only
            // deletes can trigger.)
            let mut rng = Lcg(7);
            assert_eq!(read_checked(pod, world, 12).await, 12);
            world.append(2, &mut rng);
            assert_eq!(read_checked(pod, world, 14).await, 14);
            world.append(2, &mut rng);
            assert_eq!(read_checked(pod, world, 16).await, 16);
            let before = pod.metrics();
            assert!(before.gc_backlog >= 2, "{before:?}");
            pod.collect_garbage().await;
            assert_eq!(pod.metrics().gc_deleted, 1);
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_cas_leaves_no_orphans_after_grace() {
    let world = World::new().await;
    let mut rng = Lcg(99);
    world.append(5, &mut rng);
    // Before pod 1's second CAS, another writer publishes (a no-op edit of
    // the same state at the next generation), so the CAS is lost.
    let cas_puts = Arc::new(AtomicUsize::new(0));
    let raw = world.raw.clone();
    let interleave = Arc::new(Interleave {
        trigger: Box::new({
            let cas_puts = Arc::clone(&cas_puts);
            move |op, key| {
                is_put(op, key, "/CURRENT") && cas_puts.fetch_add(1, Ordering::SeqCst) == 1
            }
        }),
        action: Mutex::new(Some(Box::new(move || {
            tokio::runtime::Handle::current().block_on(async move {
                let namespace = KeyedNamespace::new(raw, source());
                let base = namespace.load().await.unwrap().unwrap();
                let mut edit = base.manifest.clone();
                edit.published_at_ms += 1;
                edit.obsoleted = vec![base.manifest_key.clone()];
                namespace.publish(Some(&base), &edit).await.unwrap();
            });
        }))),
    });
    let pod = world.pod(world.raw.clone().with_faults(interleave), config());
    assert_eq!(read_checked(&pod, &world, 5).await, 5);
    let first_manifest = world.published().await.manifest_key;
    world.append(5, &mut rng);
    assert_eq!(read_checked(&pod, &world, 10).await, 10);
    let metrics = pod.metrics();
    assert_eq!(metrics.cas_conflicts, 1, "{metrics:?}");
    // Pod 1 (D = 5), the other writer, pod 1 again (D = 10).
    assert_eq!(world.published().await.manifest.generation, 3);
    // After the grace period (zero here), GC leaves exactly the published
    // objects: the lost attempt's manifest is gone, its parts are reused.
    pod.collect_garbage().await;
    let leftover: HashSet<String> = world
        .objects()
        .difference(&world.referenced().await)
        .cloned()
        .collect();
    // The other writer's own obsolete manifest is its GC's business.
    assert!(
        leftover.is_subset(&HashSet::from([first_manifest])),
        "{leftover:?}"
    );
    assert!(world.verify().await.is_ok());
}

// ---------------------------------------------------------------- P3.6

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_pod_never_serves_below_published_current() {
    let world = World::new().await;
    let mut rng = Lcg(3);
    world.append(3, &mut rng);
    let pod_a = world.pod(world.raw.clone(), config());
    let pod_b = world.pod(world.raw.clone(), config());
    assert_eq!(read_checked(&pod_a, &world, 3).await, 3);
    world.append(3, &mut rng);
    assert_eq!(read_checked(&pod_b, &world, 6).await, 6);
    // Pod A's cached view is at D = 3; CURRENT is at 6.
    match read(&pod_a, &world, None).await {
        KeyedReadOutcome::Rows { through, page } => {
            assert_eq!(through, 6, "pod A served a D below the published CURRENT");
            assert_eq!(page.body(), world.expected(6));
        }
        other => panic!("{other:?}"),
    }
    assert!(pod_a.metrics().current_revalidations >= 1);
    // A restarted pod loads CURRENT afresh.
    let restarted = world.pod(world.raw.clone(), config());
    match read(&restarted, &world, None).await {
        KeyedReadOutcome::Rows { through, .. } => assert_eq!(through, 6),
        other => panic!("{other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revalidation_is_bounded_by_its_interval() {
    let world = World::new().await;
    let mut rng = Lcg(4);
    world.append(2, &mut rng);
    let pod = world.pod(world.raw.clone(), KeyedEngineConfig {
        current_revalidate: Duration::from_secs(3600),
        ..config()
    });
    assert_eq!(read_checked(&pod, &world, 2).await, 2);
    for _ in 0..5 {
        assert!(matches!(
            read(&pod, &world, None).await,
            KeyedReadOutcome::Rows { through: 2, .. }
        ));
    }
    assert_eq!(pod.metrics().current_revalidations, 0);
}

// ---------------------------------------------------------------- tools

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_matches_and_detects_divergence() {
    let world = World::new().await;
    let mut rng = Lcg(5);
    let pod = world.pod(world.raw.clone(), eager_compaction());
    for step in 0..6 {
        world.append(3, &mut rng);
        read_checked(&pod, &world, 3 * (step + 1)).await;
    }
    let report = world.verify().await;
    assert!(report.is_ok(), "{:?}", report.mismatches);
    assert_eq!(report.through_record, 18);
    assert!(report.matched_rows > 0);
    let sampled = tools::verify(
        world.raw.clone(),
        &world.client,
        &source(),
        &tools::VerifyOptions {
            sample_modulus: 3,
            read: read_options(),
        },
    )
    .await
    .unwrap();
    assert!(sampled.is_ok());
    assert!(sampled.matched_rows < report.matched_rows);

    // The log diverges from what the namespace was built from.
    {
        let mut records = world.log.records.lock().unwrap();
        records[17] = format!(r#"{{"ops":[["p","{}","diverged"]]}}"#, key(200));
    }
    let report = world.verify().await;
    assert!(!report.is_ok());
    assert!(
        report
            .mismatches
            .iter()
            .any(|m| m.starts_with("continuity")),
        "{:?}",
        report.mismatches
    );
    assert!(report.mismatch_count >= 2, "{:?}", report.mismatches);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebuild_swaps_blue_green_and_catches_up() {
    let world = World::new().await;
    let mut rng = Lcg(6);
    let pod = world.pod(world.raw.clone(), config());
    for step in 0..4 {
        world.append(3, &mut rng);
        read_checked(&pod, &world, 3 * (step + 1)).await;
    }
    let before = world.published().await;
    assert_eq!(before.manifest.through_record, 12);

    // While the rebuild writes its manifest, the old namespace advances to
    // D = 15 (the pod keeps serving and publishing it).
    let rebuild_store = world.raw.clone().with_faults(Arc::new(Interleave {
        trigger: Box::new(|op, key| is_put(op, key, "/manifests/")),
        action: Mutex::new(Some(Box::new({
            let pod = pod.clone();
            let log = world.log.clone();
            move || {
                let mut rng = Lcg(66);
                for _ in 0..3 {
                    log.append(random_record(&mut rng));
                }
                tokio::runtime::Handle::current().block_on(async move {
                    let outcome = pod
                        .read(KeyedReadRequest {
                            source: source(),
                            source_next: 15,
                            selection: Selection::Point(vec![b'k']),
                            min_through_record: Some(15),
                            timeout: Duration::from_secs(5),
                        })
                        .await;
                    assert!(matches!(outcome, KeyedReadOutcome::Rows {
                        through: 15,
                        ..
                    }));
                });
            }
        }))),
    }));
    let report = tools::rebuild(
        rebuild_store,
        &world.client,
        &source(),
        &tools::RebuildOptions {
            parallelism: 3,
            read: tools::SourceReadOptions {
                max_bytes_per_second: Some(1 << 30),
                ..read_options()
            },
            ..tools::RebuildOptions::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(report.previous_through, 12);
    assert_eq!(report.through_record, 15, "{report:?}");
    assert_eq!(report.orphans.len(), 1, "the first attempt lost its CAS");
    assert!(report.runs >= 2, "{report:?}");
    let after = world.published().await;
    assert_eq!(after.manifest.generation, report.generation);
    assert!(world.verify().await.is_ok());

    // The pod adopts the rebuilt manifest (served D never decreases) and
    // garbage-collects the replaced objects.
    match read(&pod, &world, None).await {
        KeyedReadOutcome::Rows { through, page } => {
            assert_eq!(through, 15);
            assert_eq!(page.body(), world.expected(15));
        }
        other => panic!("{other:?}"),
    }
    pod.collect_garbage().await;
    let swept = tools::sweep(
        world.raw.clone(),
        &source(),
        Duration::ZERO,
        std::time::SystemTime::now(),
        false,
    )
    .await
    .unwrap();
    assert_eq!(
        swept.deleted.len(),
        1,
        "only the lost attempt's manifest: {swept:?}"
    );
    assert_eq!(world.objects(), world.referenced().await);
    let rows = read_checked(&pod, &world, 15).await;
    assert_eq!(rows, 15);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sweep_keeps_what_readers_may_hold() {
    let world = World::new().await;
    let mut rng = Lcg(8);
    // GC is disabled (long grace), so superseded manifests and parts stay.
    let pod = world.pod(world.raw.clone(), KeyedEngineConfig {
        gc_grace: Duration::from_secs(3600),
        ..eager_compaction()
    });
    for step in 0..4 {
        world.append(3, &mut rng);
        read_checked(&pod, &world, 3 * (step + 1)).await;
    }
    // Let the last compaction land.
    for _ in 0..250 {
        if world.published().await.manifest.runs.len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(world.published().await.manifest.runs.len(), 1);
    let objects = world.objects();
    let referenced = world.referenced().await;
    assert!(objects.len() > referenced.len());
    // Everything is younger than an hour: a sweep at that grace keeps the
    // superseded manifests and their parts.
    let report = tools::sweep(
        world.raw.clone(),
        &source(),
        Duration::from_secs(3600),
        std::time::SystemTime::now(),
        true,
    )
    .await
    .unwrap();
    assert!(report.deleted.is_empty(), "{report:?}");
    assert_eq!(world.objects(), objects);
    // A dry run at zero grace names the unreferenced objects without
    // deleting them; the real run deletes exactly those.
    let dry = tools::sweep(
        world.raw.clone(),
        &source(),
        Duration::ZERO,
        std::time::SystemTime::now(),
        true,
    )
    .await
    .unwrap();
    let unreferenced: HashSet<String> = objects.difference(&referenced).cloned().collect();
    assert_eq!(
        dry.deleted.iter().cloned().collect::<HashSet<_>>(),
        unreferenced
    );
    assert_eq!(world.objects(), objects);
    tools::sweep(
        world.raw.clone(),
        &source(),
        Duration::ZERO,
        std::time::SystemTime::now(),
        false,
    )
    .await
    .unwrap();
    assert_eq!(world.objects(), referenced);
    assert!(world.verify().await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sweep_protects_the_manifest_current_at_the_grace_horizon() {
    let world = World::new().await;
    let mut rng = Lcg(12);
    let pod = world.pod(world.raw.clone(), KeyedEngineConfig {
        gc_grace: Duration::from_secs(3600),
        ..eager_compaction()
    });
    let settle = async || {
        for _ in 0..250 {
            if world.published().await.manifest.runs.len() == 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("compaction never settled");
    };
    for step in 0..3 {
        world.append(3, &mut rng);
        read_checked(&pod, &world, 3 * (step + 1)).await;
        settle().await;
    }
    // Everything so far is two hours old; the manifest published last was
    // CURRENT when the one-hour grace period began.
    let horizon = world.published().await;
    let two_hours_ago = std::time::SystemTime::now() - Duration::from_secs(7200);
    let mut pending = vec![world.dir.path().to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                std::fs::File::options()
                    .write(true)
                    .open(&path)
                    .unwrap()
                    .set_modified(two_hours_ago)
                    .unwrap();
            }
        }
    }
    world.append(3, &mut rng);
    read_checked(&pod, &world, 12).await;
    settle().await;
    let report = tools::sweep(
        world.raw.clone(),
        &source(),
        Duration::from_secs(3600),
        std::time::SystemTime::now(),
        false,
    )
    .await
    .unwrap();
    assert!(!report.deleted.is_empty(), "older garbage is reclaimed");
    let objects = world.objects();
    assert!(objects.contains(&horizon.manifest_key), "{report:?}");
    for part in horizon.manifest.part_keys() {
        assert!(objects.contains(part), "{part} of the horizon manifest");
    }
    assert!(world.referenced().await.is_subset(&objects));
    assert!(world.verify().await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dump_prints_manifest_and_rows() {
    let world = World::new().await;
    let mut rng = Lcg(9);
    world.append(4, &mut rng);
    let pod = world.pod(world.raw.clone(), config());
    read_checked(&pod, &world, 4).await;
    let mut out = Vec::new();
    tools::dump(
        world.raw.clone(),
        &source(),
        true,
        &part_options(),
        &mut out,
    )
    .await
    .unwrap();
    let text = String::from_utf8(out).unwrap();
    let (header, rows) = text.split_once('\n').unwrap();
    let header: serde_json::Value = serde_json::from_str(header).unwrap();
    assert_eq!(header["manifest"]["through_record"], 4);
    assert_eq!(header["namespace"], namespace_prefix(&source(), 1));
    assert_eq!(rows, world.expected(4));
}

// ---------------------------------------------------------------- metrics

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_report_lag_publishes_runs_and_requests() {
    let world = World::new().await;
    let mut rng = Lcg(10);
    let pod = world.pod(world.raw.clone(), config());
    world.append(4, &mut rng);
    read_checked(&pod, &world, 4).await;
    world.append(3, &mut rng);
    read_checked(&pod, &world, 7).await;
    world.append(5, &mut rng);
    // A read that does not ask for the new records sees the lag.
    let _rows = read(&pod, &world, None).await;
    let metrics = pod.metrics();
    assert_eq!(metrics.publishes, 2);
    assert_eq!(metrics.cas_conflicts, 0);
    assert_eq!(metrics.namespaces, 1);
    assert_eq!(metrics.lag_records_max, 5);
    assert_eq!(metrics.runs_max, 2);
    assert!(metrics.gc_backlog >= 1);
    let namespace = &metrics.namespace_detail[0];
    assert_eq!(namespace.key, KEY);
    assert_eq!((namespace.through_record, namespace.source_next), (7, 12));
    assert!(namespace.s3_requests.put_class >= 2 * 3, "{namespace:?}");
    assert!(namespace.s3_requests.get_class >= 1);
    assert_eq!(namespace.s3_requests.counts.list, 0);
    // The pod counts at least what its namespaces did.
    assert!(metrics.s3_requests.counts.put >= namespace.s3_requests.counts.put);

    let base = serve(router(pod.clone())).await;
    let text = reqwest::get(format!("{base}{INDEXER_METRICS_PATH}"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["publishes"], 2);
    assert_eq!(body["lag_records_max"], 5);
    assert_eq!(body["waiters"], 0);
    assert!(body["s3_requests"]["put_class"].as_u64().unwrap() >= 6);
    assert_eq!(body["namespace_detail"][0]["incarnation"], INCARNATION);
}
