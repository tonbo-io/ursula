//! Keyed indexer deterministic simulation (keyed-streams design §6.1 U21,
//! §11.7, §11.8; bounded-stream-state §7.4).
//!
//! A keyed stream lives on a three-node Raft group behind the real HTTP
//! router. A keyed engine runs on its own madsim node, reads the log through
//! a [`SourceClient`] over that router, and stores its namespace in a
//! [`MemoryObjectStore`] whose hooks inject seeded S3 faults: latency,
//! failures, spurious CAS conflicts and ambiguous writes and deletes. The
//! workload interleaves keyed appends and keyed-state reads with Raft leader
//! changes, indexer crashes (the node is killed mid-anything: between part,
//! manifest and CAS, mid-compaction, mid-GC) and a restore that swaps the
//! log under the namespace, which the engine must catch with its continuity
//! check and rebuild from record 0.
//!
//! Invariants:
//!
//! - every published namespace (every `CURRENT` ever written) equals the
//!   reference fold of `log[0, D)` of a log it was built from, and every
//!   keyed-state answer equals the fold at its `D`;
//! - `D` (and the generation) never decreases per namespace;
//! - no object referenced by the current `CURRENT` is deleted, no object is
//!   deleted before the grace period after the publication that dropped it,
//!   and no `CURRENT` references a missing object;
//! - after quiescence, GC and the orphan sweep, no unreferenced part or
//!   manifest older than the grace period remains;
//! - liveness: with faults off, keyed state reaches the log tail.

use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::body::to_bytes;
use axum::http::Request as HttpRequest;
use axum::http::StatusCode;
use serde::Deserialize;
use serde::Serialize;
use tower::ServiceExt;
use ursula::HttpState;
use ursula::WallClock;
use ursula::router_with_http_state;
use ursula_index::AppliedChange;
use ursula_index::FaultDecision;
use ursula_index::MemoryObjectStore;
use ursula_index::ObjectFault;
use ursula_index::ObjectHooks;
use ursula_index::ObjectOp;
use ursula_index::ObjectStore;
use ursula_index::clock::Clock;
use ursula_index::keyed::CompactionPolicy;
use ursula_index::keyed::EncodedPart;
use ursula_index::keyed::IncarnationState;
use ursula_index::keyed::KeyedEngine;
use ursula_index::keyed::KeyedEngineConfig;
use ursula_index::keyed::KeyedManifest;
use ursula_index::keyed::KeyedReadOutcome;
use ursula_index::keyed::KeyedReadRequest;
use ursula_index::keyed::KeyedSource;
use ursula_index::keyed::KeyedState;
use ursula_index::keyed::Lower;
use ursula_index::keyed::MemoryParts;
use ursula_index::keyed::PartOptions;
use ursula_index::keyed::RangeQuery;
use ursula_index::keyed::Selection;
use ursula_index::keyed::SourceClient;
use ursula_index::keyed::SourceError;
use ursula_index::keyed::SourcePage;
use ursula_index::keyed::encode_key;
use ursula_index::keyed::read_range;
use ursula_index::keyed::read_response;
use ursula_index::keyed::record_digest;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::RuntimeThreading;
use ursula_runtime::ShardRuntime;
use ursula_shard::ShardPlacement;

use super::MadsimOpenRaftRuntime;
use super::MadsimRuntimeRaftNetworkFactory;
use super::SimEvent;
use super::SimTrace;
use super::SplitMix64;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::sim_network_policy;

const KEYED_CONTENT_TYPE: &str = "application/json; profile=keyed-batch-v1";
/// Wall-clock origin of the simulation's virtual clock.
const CLOCK_ORIGIN_MS: u64 = 1_700_000_000_000;
const GC_GRACE: Duration = Duration::from_millis(1_000);
const GC_TICK: Duration = Duration::from_millis(100);
const ALL_ROWS: usize = 100_000;

/// Seeded shape of one keyed-indexer run, recorded in the schedule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyedIndexerPlan {
    pub rounds: u32,
    /// Rounds that start with a Raft leader change.
    pub leader_changes: Vec<u32>,
    /// Rounds that start by killing the indexer node and starting a new one.
    pub indexer_crashes: Vec<u32>,
    /// Round that starts with a restore: the log under the namespace is
    /// replaced by one that keeps a prefix and diverges after it.
    pub restore_round: Option<u32>,
    pub s3_fail_permille: u64,
    pub s3_conflict_permille: u64,
    pub s3_ambiguous_permille: u64,
    pub s3_delay_permille: u64,
    pub s3_max_delay_ms: u64,
    pub source_page_bytes: u64,
    pub max_ingest_bytes: u64,
    pub write_cache: bool,
    pub key_space: u64,
}

fn pick_rounds(rng: &mut SplitMix64, rounds: u32, count: u64) -> Vec<u32> {
    let mut picked: Vec<u32> = (0..count)
        .map(|_| 1 + u32::try_from(rng.next_bounded(u64::from(rounds - 1))).expect("round"))
        .collect();
    picked.sort_unstable();
    picked.dedup();
    picked
}

impl KeyedIndexerPlan {
    pub(super) fn from_seed(seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed ^ 0x6b65_7965_645f_6978);
        let rounds = 10 + u32::try_from(rng.next_bounded(8)).expect("rounds");
        let leader_changes = {
            let count = 1 + rng.next_bounded(2);
            pick_rounds(&mut rng, rounds, count)
        };
        let indexer_crashes = {
            let count = rng.next_bounded(3);
            pick_rounds(&mut rng, rounds, count)
        };
        let restore_round = (rng.next_bounded(3) == 0).then(|| {
            rounds / 2 + u32::try_from(rng.next_bounded(u64::from(rounds / 2))).expect("round")
        });
        Self {
            rounds,
            leader_changes,
            indexer_crashes,
            restore_round,
            s3_fail_permille: 5 + rng.next_bounded(35),
            s3_conflict_permille: 20 + rng.next_bounded(100),
            s3_ambiguous_permille: 5 + rng.next_bounded(30),
            s3_delay_permille: 100 + rng.next_bounded(300),
            s3_max_delay_ms: 5 + rng.next_bounded(35),
            source_page_bytes: 256 + rng.next_bounded(3_840),
            max_ingest_bytes: 1_024 + rng.next_bounded(15_360),
            write_cache: rng.next_bounded(2) == 0,
            key_space: 4 + rng.next_bounded(20),
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

/// Virtual-time wall clock shared by the node (stream `created_at_ms`), the
/// engine and the object store.
struct VirtualClock {
    start: madsim::time::Instant,
}

impl VirtualClock {
    fn ms(&self) -> u64 {
        CLOCK_ORIGIN_MS
            + u64::try_from(self.start.elapsed().as_millis()).expect("virtual time fits u64")
    }
}

impl Clock for VirtualClock {
    fn now_ms(&self) -> u64 {
        self.ms()
    }
}

impl WallClock for VirtualClock {
    fn unix_time_ms(&self) -> u64 {
        self.ms()
    }
}

// ---------------------------------------------------------------------------
// Object-store checker and fault hooks
// ---------------------------------------------------------------------------

#[derive(Default)]
struct CheckerState {
    /// Every part and manifest ever written, by namespace-relative key.
    archive: HashMap<String, Bytes>,
    /// Namespace-relative keys that currently exist.
    live: HashSet<String>,
    /// Every `CURRENT` written, in the order the writes took effect.
    publications: Vec<Vec<u8>>,
    /// Objects the current `CURRENT` references (manifest and parts).
    referenced: HashSet<String>,
    /// `published_at_ms` of the publication that stopped referencing a key.
    unreferenced_at: HashMap<String, u64>,
    deleted: u64,
    violations: Vec<String>,
}

struct Checker {
    prefix: String,
    clock: Arc<VirtualClock>,
    state: Mutex<CheckerState>,
}

#[derive(Deserialize)]
struct Pointer {
    generation: u64,
    manifest: String,
}

fn parse_manifest(
    state: &CheckerState,
    pointer: &[u8],
) -> Result<(Pointer, KeyedManifest), String> {
    let pointer: Pointer =
        serde_json::from_slice(pointer).map_err(|error| format!("CURRENT is not JSON: {error}"))?;
    let bytes = state
        .archive
        .get(&pointer.manifest)
        .ok_or_else(|| format!("CURRENT names unwritten manifest {}", pointer.manifest))?;
    let manifest: KeyedManifest = serde_json::from_slice(bytes)
        .map_err(|error| format!("manifest {} is not JSON: {error}", pointer.manifest))?;
    Ok((pointer, manifest))
}

impl Checker {
    fn applied(&self, change: AppliedChange<'_>) {
        let mut state = lock(&self.state);
        match change {
            AppliedChange::Put { key, bytes } => {
                let Some(name) = key.strip_prefix(&self.prefix) else {
                    return;
                };
                if name == "CURRENT" {
                    self.published(&mut state, bytes);
                } else {
                    state
                        .archive
                        .insert(name.to_owned(), Bytes::copy_from_slice(bytes));
                    state.live.insert(name.to_owned());
                }
            }
            AppliedChange::Delete { key } => {
                let Some(name) = key.strip_prefix(&self.prefix) else {
                    return;
                };
                state.deleted += 1;
                state.live.remove(name);
                let now = self.clock.ms();
                if name == "CURRENT" {
                    state
                        .violations
                        .push("CURRENT of a live stream incarnation was deleted".to_owned());
                } else if state.referenced.contains(name) {
                    state
                        .violations
                        .push(format!("deleted {name} while CURRENT references it"));
                } else if let Some(at) = state.unreferenced_at.get(name).copied()
                    && now < at + u64::try_from(GC_GRACE.as_millis()).expect("grace")
                {
                    state.violations.push(format!(
                        "deleted {name} at {now}, within the grace period of the publication \
                         at {at} that dropped it"
                    ));
                }
            }
        }
    }

    fn published(&self, state: &mut CheckerState, bytes: &[u8]) {
        state.publications.push(bytes.to_vec());
        let (pointer, manifest) = match parse_manifest(state, bytes) {
            Ok(parsed) => parsed,
            Err(error) => {
                state.violations.push(error);
                return;
            }
        };
        let mut referenced: HashSet<String> = manifest.part_keys().map(str::to_owned).collect();
        referenced.insert(pointer.manifest.clone());
        for key in &referenced {
            if !state.live.contains(key) {
                state.violations.push(format!(
                    "CURRENT generation {} references missing object {key}",
                    pointer.generation
                ));
            }
            state.unreferenced_at.remove(key);
        }
        let dropped: Vec<String> = state.referenced.difference(&referenced).cloned().collect();
        for key in dropped {
            state.unreferenced_at.insert(key, manifest.published_at_ms);
        }
        state.referenced = referenced;
    }
}

/// Generation of a manifest key, `manifests/{generation:020}-{hash}.json`.
fn manifest_generation(name: &str) -> Option<u64> {
    name.strip_prefix("manifests/")?
        .split_once('-')?
        .0
        .parse()
        .ok()
}

/// What the orphan sweep must keep (the U20 rule, `KeyedNamespace::sweep`):
/// everything the published manifest references, plus every manifest up to
/// the published generation written within the grace period, and the newest
/// one written before it (it may have been `CURRENT` when the period began),
/// with the parts they reference.
fn readers_may_hold(
    state: &CheckerState,
    at_rest: &[(String, u64)],
    now: u64,
    grace_ms: u64,
) -> HashSet<String> {
    let mut keep = state.referenced.clone();
    let current_generation = state
        .publications
        .last()
        .and_then(|pointer| serde_json::from_slice::<Pointer>(pointer).ok())
        .map_or(0, |pointer| pointer.generation);
    let manifests: Vec<(u64, &str, bool)> = at_rest
        .iter()
        .filter_map(|(name, modified_ms)| {
            manifest_generation(name)
                .filter(|generation| *generation <= current_generation)
                .map(|generation| {
                    let young = now.saturating_sub(*modified_ms) < grace_ms;
                    (generation, name.as_str(), young)
                })
        })
        .collect();
    let window_start = manifests
        .iter()
        .filter(|(_, _, young)| !young)
        .map(|(generation, _, _)| *generation)
        .max();
    for (generation, name, young) in manifests {
        let at_window_start = Some(generation) == window_start && generation < current_generation;
        if !(young || at_window_start) {
            continue;
        }
        keep.insert(name.to_owned());
        if let Some(manifest) = state
            .archive
            .get(name)
            .and_then(|bytes| serde_json::from_slice::<KeyedManifest>(bytes).ok())
        {
            keep.extend(manifest.part_keys().map(str::to_owned));
        }
    }
    keep
}

struct S3Faults {
    plan: KeyedIndexerPlan,
    enabled: AtomicBool,
    rng: Mutex<SplitMix64>,
    injected: Mutex<HashMap<&'static str, u64>>,
    checker: Arc<Checker>,
}

impl ObjectHooks for S3Faults {
    fn decide(&self, op: ObjectOp, _key: &str) -> FaultDecision {
        if !self.enabled.load(Ordering::SeqCst) {
            return FaultDecision::default();
        }
        let mut rng = lock(&self.rng);
        let delay = if rng.next_bounded(1_000) < self.plan.s3_delay_permille {
            Duration::from_millis(1 + rng.next_bounded(self.plan.s3_max_delay_ms))
        } else {
            Duration::ZERO
        };
        let roll = rng.next_bounded(1_000);
        let fail = self.plan.s3_fail_permille;
        let conflict = fail + self.plan.s3_conflict_permille;
        let ambiguous = conflict + self.plan.s3_ambiguous_permille;
        let (fault, name) = if roll < fail {
            (ObjectFault::Fail, "fail")
        } else if op == ObjectOp::CompareAndSwap && roll < conflict {
            (ObjectFault::Conflict, "cas_conflict")
        } else if op.is_mutation() && roll >= conflict && roll < ambiguous {
            (ObjectFault::Ambiguous, "ambiguous")
        } else {
            (ObjectFault::None, "none")
        };
        drop(rng);
        if fault != ObjectFault::None {
            *lock(&self.injected).entry(name).or_default() += 1;
        }
        FaultDecision { delay, fault }
    }

    fn applied(&self, change: AppliedChange<'_>) {
        self.checker.applied(change);
    }
}

// ---------------------------------------------------------------------------
// Simulated source: the SourceClient over the in-process node router
// ---------------------------------------------------------------------------

struct SimSource {
    app: Router,
    /// Logical stream name (the namespace key) → stream actually read; a
    /// restore points the namespace at another log.
    routes: Mutex<HashMap<String, String>>,
    /// Set while the Raft group swaps its leader engine: reads answer as a
    /// node without a leader would (unavailable). The harness swaps engines
    /// only with no source request inside the runtime, since a request
    /// between shutdown and install would make the runtime create a fresh
    /// engine.
    paused: AtomicBool,
    in_flight: AtomicUsize,
}

/// Counts a source request inside the runtime.
struct InFlight<'a>(&'a AtomicUsize);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl SimSource {
    async fn send(
        &self,
        request: HttpRequest<Body>,
    ) -> Result<axum::response::Response, SourceError> {
        let unavailable =
            || SourceError::Transient("the raft group is changing leaders".to_owned());
        if self.paused.load(Ordering::SeqCst) {
            return Err(unavailable());
        }
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        let _in_flight = InFlight(&self.in_flight);
        if self.paused.load(Ordering::SeqCst) {
            return Err(unavailable());
        }
        Ok(self
            .app
            .clone()
            .oneshot(request)
            .await
            .expect("router is infallible"))
    }

    /// Stops new source requests and waits (bounded) for those inside the
    /// runtime.
    async fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
        for _ in 0..5_000 {
            if self.in_flight.load(Ordering::SeqCst) == 0 {
                return;
            }
            madsim::time::sleep(Duration::from_millis(1)).await;
        }
    }

    fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }

    fn path(&self, bucket: &str, key: &str) -> String {
        let physical = lock(&self.routes)
            .get(key)
            .cloned()
            .unwrap_or_else(|| key.to_owned());
        format!("/{bucket}/{physical}")
    }
}

type SourceFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, SourceError>> + Send + 'a>>;

impl SourceClient for SimSource {
    fn read<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        record: u64,
        max_bytes: u64,
        max_records: Option<u64>,
        leader: bool,
    ) -> SourceFuture<'a, SourcePage> {
        Box::pin(async move {
            let mut uri = format!(
                "{}?record={record}&max_bytes={}",
                self.path(bucket, key),
                max_bytes.max(1)
            );
            if let Some(records) = max_records {
                uri.push_str(&format!("&max_records={records}"));
            }
            if leader {
                uri.push_str("&consistency=leader");
            }
            let request = HttpRequest::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .expect("source read request");
            let response = self.send(request).await?;
            let status = response.status();
            let headers = response.headers().clone();
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .map_err(|error| SourceError::Transient(error.to_string()))?;
            let body = String::from_utf8(body.to_vec())
                .map_err(|error| SourceError::Transient(error.to_string()))?;
            read_response(record, status, &headers, &body)
        })
    }

    /// A HEAD of the stream the namespace currently reads: 404 is gone,
    /// anything else successful is present.
    fn incarnation<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        _incarnation: u64,
    ) -> SourceFuture<'a, IncarnationState> {
        Box::pin(async move {
            let request = HttpRequest::builder()
                .method("HEAD")
                .uri(self.path(bucket, key))
                .body(Body::empty())
                .expect("source head request");
            let response = self.send(request).await?;
            match response.status() {
                StatusCode::NOT_FOUND => Ok(IncarnationState::Gone),
                status if status.is_success() => Ok(IncarnationState::Present),
                status => Err(SourceError::Transient(format!("HEAD returned {status}"))),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Indexer node
// ---------------------------------------------------------------------------

struct Indexer {
    node: madsim::runtime::NodeHandle,
    engine: KeyedEngine,
    seed: u64,
}

impl Indexer {
    fn start(
        generation: u32,
        seed: u64,
        store: &ObjectStore,
        source: &Arc<SimSource>,
        config: &KeyedEngineConfig,
        clock: &Arc<VirtualClock>,
    ) -> Self {
        let node = madsim::runtime::Handle::current()
            .create_node()
            .name(format!("keyed-indexer-{generation}"))
            .build();
        let engine = KeyedEngine::with_clock(
            store.clone(),
            SharedSource(Arc::clone(source)),
            None,
            config.clone(),
            Arc::clone(clock) as Arc<dyn Clock>,
        );
        let gc = engine.clone();
        let _gc_loop = node.spawn(MadsimOpenRaftRuntime::scope(seed, async move {
            loop {
                madsim::time::sleep(GC_TICK).await;
                let _deleted = gc.collect_garbage().await;
            }
        }));
        Self { node, engine, seed }
    }

    async fn run<T, F, Fut>(&self, work: F) -> T
    where
        F: FnOnce(KeyedEngine) -> Fut,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.node
            .spawn(MadsimOpenRaftRuntime::scope(
                self.seed,
                work(self.engine.clone()),
            ))
            .await
            .expect("indexer task")
    }

    fn crash(self) {
        madsim::runtime::Handle::current().kill(self.node.id());
    }
}

/// The engine owns its source; the scenario keeps a handle to re-route it.
struct SharedSource(Arc<SimSource>);

impl SourceClient for SharedSource {
    fn read<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        record: u64,
        max_bytes: u64,
        max_records: Option<u64>,
        leader: bool,
    ) -> SourceFuture<'a, SourcePage> {
        self.0
            .read(bucket, key, record, max_bytes, max_records, leader)
    }

    fn incarnation<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        incarnation: u64,
    ) -> SourceFuture<'a, IncarnationState> {
        self.0.incarnation(bucket, key, incarnation)
    }
}

// ---------------------------------------------------------------------------
// Workload helpers
// ---------------------------------------------------------------------------

async fn send(app: &Router, method: &str, uri: &str, body: String) -> (StatusCode, Bytes) {
    let request = HttpRequest::builder()
        .method(method)
        .uri(uri)
        .header("content-type", KEYED_CONTENT_TYPE)
        .body(Body::from(body))
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("router");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (status, body)
}

const VALUES: &[&str] = &[
    "null",
    "true",
    "1.50e3",
    "\"s\"",
    "\"\\ud800\"",
    "[1,\"v\"]",
    "{\"b\":1,\"a\":[2]}",
];

fn sim_key(rng: &mut SplitMix64, key_space: u64) -> String {
    encode_key(format!("k{:02}", rng.next_bounded(key_space)).as_bytes())
}

/// One `keyed-batch-v1` record, minified (its stored text equals it).
fn record_text(rng: &mut SplitMix64, key_space: u64, marker: Option<(u32, usize)>) -> String {
    let mut ops = Vec::new();
    if let Some((restore, index)) = marker {
        ops.push(format!(
            "[\"p\",\"{}\",{{\"restore\":{restore},\"i\":{index}}}]",
            encode_key(b"restore")
        ));
    }
    for _ in 0..1 + rng.next_bounded(3) {
        let roll = rng.next_bounded(10);
        let op = if roll < 6 {
            let value = VALUES
                .get(usize::try_from(rng.next_bounded(VALUES.len() as u64)).expect("index"))
                .expect("value");
            format!("[\"p\",\"{}\",{value}]", sim_key(rng, key_space))
        } else if roll < 8 {
            format!("[\"d\",\"{}\"]", sim_key(rng, key_space))
        } else {
            let a = rng.next_bounded(key_space);
            let b = rng.next_bounded(key_space);
            let (low, high) = (a.min(b), a.max(b) + 1);
            format!(
                "[\"x\",\"{}\",\"{}\"]",
                encode_key(format!("k{low:02}").as_bytes()),
                encode_key(format!("k{high:02}").as_bytes())
            )
        };
        ops.push(op);
    }
    format!("{{\"ops\":[{}]}}", ops.join(","))
}

/// Appends `records` as one POST (a JSON array is flattened into one
/// message per element).
async fn append(app: &Router, path: &str, records: &[String]) {
    let body = match records {
        [one] => one.clone(),
        many => format!("[{}]", many.join(",")),
    };
    let (status, body) = send(app, "POST", path, body).await;
    assert!(
        status.is_success(),
        "keyed append failed with {status}: {}",
        String::from_utf8_lossy(&body)
    );
}

fn full_range() -> RangeQuery {
    RangeQuery {
        lower: Lower::First,
        end: None,
        limit: ALL_ROWS,
        budget: None,
    }
}

fn fold_body(log: &[String], through: u64) -> Option<String> {
    let records = log.get(..usize::try_from(through).ok()?)?;
    let state = KeyedState::fold(records.iter().map(String::as_str)).expect("fold");
    Some(state.range(&full_range()).body())
}

fn sim_part_options() -> PartOptions {
    PartOptions {
        data_page_rows: 2,
        row_group_rows: 5,
        layout_block_bytes: 97,
        target_part_bytes: 120,
        read_batch_rows: 3,
    }
}

async fn change_leader(
    factory: &MadsimRuntimeRaftNetworkFactory,
    runtime: &ShardRuntime,
    placement: ShardPlacement,
) -> u64 {
    let old_leader_id = factory
        .unregister_current_leader(placement.raft_group_id)
        .expect("unregister current leader");
    runtime
        .shutdown_group_engine_for_simulation(placement)
        .await
        .expect("shutdown leader engine");
    let (new_leader_id, engine) = factory
        .take_current_leader_engine(placement.raft_group_id)
        .await
        .expect("take replacement leader engine");
    runtime
        .install_group_engine_for_simulation(placement, engine)
        .await
        .expect("install replacement leader engine");
    factory
        .restart_follower(old_leader_id)
        .await
        .expect("restart old leader as follower");
    new_leader_id
}

// ---------------------------------------------------------------------------
// The scenario
// ---------------------------------------------------------------------------

pub(super) async fn run_keyed_indexer_inner(
    config: ThreeNodeRaftSimConfig,
    plan: KeyedIndexerPlan,
) -> ThreeNodeRaftSimOutcome {
    let seed = config.seed;
    let mut trace = SimTrace::default();
    let mut rng = SplitMix64::new(seed ^ 0x6b69_6478_776c_6f64);
    let clock = Arc::new(VirtualClock {
        start: madsim::time::Instant::now(),
    });

    let factory = MadsimRuntimeRaftNetworkFactory::new(seed, sim_network_policy());
    let mut runtime_config = RuntimeConfig::new(1, 1);
    runtime_config.threading = RuntimeThreading::HostedTokio;
    let runtime = ShardRuntime::spawn_with_engine_factory(runtime_config, factory.clone())
        .expect("spawn hosted runtime over a three-node raft group");
    trace.push(SimEvent::ClusterBuilt { seed });
    for (group, result) in runtime.set_feature_level_all_groups(1).await {
        result.unwrap_or_else(|error| panic!("raise group {} to level 1: {error}", group.0));
    }
    let app = router_with_http_state(
        HttpState::new(runtime.clone())
            .with_wall_clock_handle(Arc::clone(&clock) as Arc<dyn WallClock>),
    );

    let bucket = config.stream.bucket_id.clone();
    let logical = config.stream.stream_id.clone();
    let placement = runtime.locate(&config.stream);
    let mut leader_id = factory
        .leader_id(placement.raft_group_id)
        .expect("initial leader");
    trace.push(SimEvent::LeaderElected { leader_id });

    let mut writer_path = format!("/{bucket}/{logical}");
    let (status, _) = send(&app, "PUT", &writer_path, String::new()).await;
    assert_eq!(status, StatusCode::CREATED, "create keyed stream");
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });
    // The incarnation is the stream's `created_at_ms`, as the node's
    // keyed-state proxy forwards it.
    let incarnation = runtime
        .head_stream(HeadStreamRequest {
            stream_id: config.stream.clone(),
            now_ms: clock.ms(),
        })
        .await
        .expect("head keyed stream")
        .created_at_ms
        .expect("created_at_ms");
    let source_id = KeyedSource {
        bucket: bucket.clone(),
        key: logical.clone(),
        incarnation,
    };

    let checker = Arc::new(Checker {
        prefix: ursula_index::keyed::manifest::namespace_prefix(
            &source_id,
            ursula_index::keyed::KEYED_PROJECTION_FORMAT,
        ),
        clock: Arc::clone(&clock),
        state: Mutex::new(CheckerState::default()),
    });
    let faults = Arc::new(S3Faults {
        plan: plan.clone(),
        enabled: AtomicBool::new(true),
        rng: Mutex::new(SplitMix64::new(seed ^ 0x7333_6661_756c_7473)),
        injected: Mutex::new(HashMap::new()),
        checker: Arc::clone(&checker),
    });
    let raw_store = MemoryObjectStore::new(Arc::clone(&clock) as Arc<dyn Clock>);
    let store = ObjectStore::from(raw_store.clone())
        .with_hooks(Arc::clone(&faults) as Arc<dyn ObjectHooks>);
    let source = Arc::new(SimSource {
        app: app.clone(),
        routes: Mutex::new(HashMap::new()),
        paused: AtomicBool::new(false),
        in_flight: AtomicUsize::new(0),
    });
    let engine_config = KeyedEngineConfig {
        min_publish_interval: Duration::from_millis(20),
        gc_grace: GC_GRACE,
        gc_tick: GC_TICK,
        source_page_bytes: plan.source_page_bytes,
        max_ingest_bytes: plan.max_ingest_bytes,
        write_cache_bytes: if plan.write_cache { 1 << 20 } else { 0 },
        idle_namespace: Duration::from_secs(3_600),
        part_options: sim_part_options(),
        policy: CompactionPolicy::default(),
        ..KeyedEngineConfig::default()
    };
    let mut indexer_generation = 0_u32;
    let mut indexer = Indexer::start(
        indexer_generation,
        seed,
        &store,
        &source,
        &engine_config,
        &clock,
    );

    // Every log the namespace was ever built from; the last is current.
    let mut logs: Vec<Vec<String>> = vec![Vec::new()];
    let mut violations: Vec<String> = Vec::new();
    let mut seen_through = 0_u64;
    let mut restores = 0_u32;

    for round in 0..plan.rounds {
        if plan.leader_changes.contains(&round) {
            source.pause().await;
            leader_id = change_leader(&factory, &runtime, placement).await;
            source.resume();
            trace.push(SimEvent::KeyedIndexerFault {
                round,
                fault: "raft_leader_change".to_owned(),
            });
            trace.push(SimEvent::LeaderElected { leader_id });
        }
        if plan.indexer_crashes.contains(&round) {
            indexer.crash();
            indexer_generation += 1;
            indexer = Indexer::start(
                indexer_generation,
                seed,
                &store,
                &source,
                &engine_config,
                &clock,
            );
            trace.push(SimEvent::KeyedIndexerFault {
                round,
                fault: "indexer_crash".to_owned(),
            });
        }
        if plan.restore_round == Some(round) {
            restores += 1;
            let current = logs.last().expect("current log").clone();
            let kept = usize::try_from(rng.next_bounded(current.len() as u64 + 1)).expect("kept");
            let mut restored: Vec<String> = current.get(..kept).expect("prefix").to_vec();
            let fresh = current.len() - kept + 1 + usize::try_from(rng.next_bounded(3)).expect("n");
            for index in 0..fresh {
                restored.push(record_text(
                    &mut rng,
                    plan.key_space,
                    Some((restores, kept + index)),
                ));
            }
            let physical = format!("{logical}-restored-{restores}");
            writer_path = format!("/{bucket}/{physical}");
            let (status, _) = send(&app, "PUT", &writer_path, String::new()).await;
            assert_eq!(status, StatusCode::CREATED, "create restored log");
            for chunk in restored.chunks(8) {
                append(&app, &writer_path, chunk).await;
            }
            lock(&source.routes).insert(logical.clone(), physical);
            logs.push(restored);
            trace.push(SimEvent::KeyedIndexerFault {
                round,
                fault: "restore".to_owned(),
            });
        }

        for _ in 0..1 + rng.next_bounded(3) {
            let batch: Vec<String> = (0..1 + rng.next_bounded(3))
                .map(|_| record_text(&mut rng, plan.key_space, None))
                .collect();
            append(&app, &writer_path, &batch).await;
            logs.last_mut().expect("current log").extend(batch);
        }
        let tail = logs.last().expect("current log").len() as u64;
        let min_through_record = match rng.next_bounded(10) {
            0 => None,
            1 | 2 => Some(rng.next_bounded(tail + 1)),
            _ => Some(tail),
        };
        let request = KeyedReadRequest {
            source: source_id.clone(),
            source_next: tail,
            selection: Selection::Range(full_range()),
            min_through_record,
            timeout: Duration::from_millis(rng.next_bounded(400)),
        };
        let outcome = indexer
            .run(move |engine| async move { engine.read(request).await })
            .await;
        let through = match outcome {
            KeyedReadOutcome::Rows { through, page } => {
                if through < seen_through {
                    violations.push(format!(
                        "round {round}: keyed state went back from D={seen_through} to {through}"
                    ));
                }
                seen_through = seen_through.max(through);
                if min_through_record.is_some_and(|wanted| through < wanted) {
                    violations.push(format!(
                        "round {round}: answered D={through} below min_through_record \
                         {min_through_record:?}"
                    ));
                }
                let body = page.body();
                if !logs
                    .iter()
                    .any(|log| fold_body(log, through).as_ref() == Some(&body))
                {
                    violations.push(format!(
                        "round {round}: keyed state at D={through} differs from the fold of \
                         every log"
                    ));
                }
                Some(through)
            }
            KeyedReadOutcome::NotYet { .. } | KeyedReadOutcome::Unavailable(_) => None,
            other => {
                violations.push(format!("round {round}: unexpected outcome {other:?}"));
                None
            }
        };
        trace.push(SimEvent::KeyedIndexerRound {
            round,
            records: tail,
            through,
        });
        madsim::time::sleep(Duration::from_millis(rng.next_bounded(150))).await;
    }

    // Quiesce: faults off, keyed state must reach the tail.
    faults.enabled.store(false, Ordering::SeqCst);
    let log = logs.last().expect("current log").clone();
    let tail = log.len() as u64;
    let mut reached = None;
    for _ in 0..40 {
        let request = KeyedReadRequest {
            source: source_id.clone(),
            source_next: tail,
            selection: Selection::Range(full_range()),
            min_through_record: Some(tail),
            timeout: Duration::from_secs(2),
        };
        let outcome = indexer
            .run(move |engine| async move { engine.read(request).await })
            .await;
        if let KeyedReadOutcome::Rows { through, page } = outcome
            && through == tail
        {
            reached = Some(page.body());
            break;
        }
        madsim::time::sleep(Duration::from_millis(100)).await;
    }
    match reached {
        Some(body) if fold_body(&log, tail).as_ref() == Some(&body) => {}
        Some(_) => violations.push(format!(
            "final keyed state at D={tail} differs from the fold"
        )),
        None => violations.push(format!("keyed state never reached the tail {tail}")),
    }

    // The node holds exactly the acknowledged log.
    let mut stored = Vec::new();
    while (stored.len() as u64) < tail {
        let page = source
            .read(&bucket, &logical, stored.len() as u64, 1 << 20, None, true)
            .await
            .expect("read back the log");
        stored.extend(page.records);
    }
    assert_eq!(stored, log, "the node's log equals the acknowledged log");

    // GC after the grace period, then the orphan sweep.
    madsim::time::sleep(GC_GRACE + GC_TICK * 3).await;
    let collected = indexer
        .run(|engine| async move { engine.collect_garbage().await })
        .await;
    let sweep_source = source_id.clone();
    let swept = indexer
        .run(move |engine| async move { engine.sweep(&sweep_source).await })
        .await
        .expect("orphan sweep");
    let now = clock.ms();
    let grace_ms = u64::try_from(GC_GRACE.as_millis()).expect("grace");
    // Stop the indexer, then check its store at rest.
    indexer.crash();
    let state = std::mem::take(&mut *lock(&checker.state));
    let at_rest: Vec<(String, u64)> = raw_store
        .snapshot()
        .into_iter()
        .filter_map(|(key, modified_ms)| {
            key.strip_prefix(&checker.prefix)
                .map(|name| (name.to_owned(), modified_ms))
        })
        .collect();
    let may_hold = readers_may_hold(&state, &at_rest, now, grace_ms);
    for (name, modified_ms) in &at_rest {
        let name = name.as_str();
        let data = name.starts_with("parts/") || name.starts_with("manifests/");
        if data && !may_hold.contains(name) && now.saturating_sub(*modified_ms) >= grace_ms {
            violations.push(format!(
                "orphan {name} older than the grace period after the sweep"
            ));
        }
    }

    // Every publication: generation and D monotone, contents equal the fold.
    let mut last: Option<(u64, u64)> = None;
    let mut max_through = 0_u64;
    for pointer in &state.publications {
        let (pointer, manifest) = match parse_manifest(&state, pointer) {
            Ok(parsed) => parsed,
            Err(error) => {
                violations.push(error);
                continue;
            }
        };
        let d = manifest.through_record;
        if let Some((generation, through)) = last
            && (pointer.generation <= generation || d < through)
        {
            violations.push(format!(
                "publication went from generation {generation} D={through} to generation {} \
                 D={d}",
                pointer.generation
            ));
        }
        last = Some((pointer.generation, d));
        max_through = max_through.max(d);
        let mut parts = MemoryParts::new();
        for run in &manifest.runs {
            for meta in &run.parts {
                match state.archive.get(&meta.key) {
                    Some(bytes) => parts.insert(&EncodedPart {
                        meta: meta.clone(),
                        bytes: bytes.clone(),
                    }),
                    None => violations.push(format!("part {} was never written", meta.key)),
                }
            }
        }
        let body = read_range(&parts, &manifest.runs, &full_range(), &sim_part_options())
            .await
            .map(|page| page.body());
        let matches = logs.iter().any(|log| {
            let continuous = match (d.checked_sub(1), &manifest.through_digest) {
                (None, _) => true,
                (Some(previous), Some(expected)) => usize::try_from(previous)
                    .ok()
                    .and_then(|index| log.get(index))
                    .is_some_and(|text| record_digest(format!("{text}\n").as_bytes()) == *expected),
                (Some(_), None) => false,
            };
            continuous
                && body
                    .as_ref()
                    .is_ok_and(|body| fold_body(log, d).as_ref() == Some(body))
        });
        if !matches {
            violations.push(format!(
                "publication generation {} at D={d} does not equal the fold of its log",
                pointer.generation
            ));
        }
    }
    violations.extend(state.violations.iter().cloned());
    let publications = state.publications.len() as u64;
    let deleted = state.deleted;

    let injected = lock(&faults.injected).clone();
    trace.push(SimEvent::KeyedIndexerVerified {
        publications,
        through: max_through,
        deleted,
        swept: swept.deleted.len() as u64,
        collected: collected as u64,
    });
    assert!(
        violations.is_empty(),
        "keyed indexer invariants failed for seed {seed} (plan {plan:?}, injected {injected:?}):\n{}",
        violations.join("\n")
    );
    assert!(publications > 0, "the engine published at least once");

    ThreeNodeRaftSimOutcome {
        seed,
        leader_id,
        target_node_id: None,
        appended_log_index: tail,
        trace,
    }
}
