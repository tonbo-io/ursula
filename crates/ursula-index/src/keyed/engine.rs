//! The keyed projection engine (design §3.4, §5.5, §6.1 U16 and the state
//! half of U17).
//!
//! One [`KeyedEngine`] serves every namespace of a pod. A namespace is
//! `(bucket, key, incarnation)` at the current projection format; its
//! published manifest lives in a `watch` cell, so readers take a snapshot
//! without locking the engine and waiters wake on each publication.
//!
//! Ingestion is on demand: a read whose `min_through_record` exceeds the
//! published `D` registers a want and starts the namespace's single worker
//! if none runs. The worker waits until `CURRENT.published_at_ms +
//! min_publish_interval`, reloads `CURRENT`, reads `[D−1, N)` from the
//! source in P7 pages, checks record `D−1` against `through_digest`, folds
//! the rest into one run and publishes it (parts, manifest put-if-absent,
//! CAS of `CURRENT`, read-back). Every waiter of the namespace is served by
//! that one publication. After each CAS the worker confirms that the stream
//! incarnation still exists and deletes the namespace when it does not.
//!
//! A continuity failure (record `D−1` differs, or is beyond the source tail)
//! is confirmed with a leader read, then the namespace is rebuilt from
//! record 0 by a publication that replaces every run; reads answer 503
//! meanwhile. A namespace ahead of the request's source tail answers 503 and
//! triggers the same check.
//!
//! Compaction follows a publication (size-tiered, `plan_compaction`); its
//! manifest edit is rebased onto newer publications while all inputs
//! remain. Garbage collection is delta-driven: each manifest lists the
//! objects its edit made unreachable, and they are deleted after the grace
//! period unless the then-current manifest references them. A writer's own
//! unpublished objects (lost CAS, abandoned compaction) go through the same
//! queue, and so does the delta of every manifest adopted from `CURRENT`
//! that another writer published (another pod, a previous process, a
//! rebuild).
//!
//! A read without `min_through_record` revalidates `CURRENT` (one HEAD)
//! when the view is older than `current_revalidate`, so a pod never serves
//! a `D` below what another pod or a previous process published (P3.6).
//! Object-store requests are counted per namespace and per pod (U24).
//!
//! Resource admission is process-wide ([`super::admission`]): a worker
//! takes a place in a bounded admission queue (a full queue answers 503
//! with `Retry-After` to the read that would start it), an ingest slot
//! before it reloads `CURRENT`, and reservations from the byte budget for
//! each source page and the records it has folded; a compaction takes a
//! compaction slot and reserves its buffers, waiting rather than failing.
//!
//! Objects a crash leaves behind are removed by the orphan sweep
//! ([`KeyedEngine::sweep`], or the `keyed sweep` tool).
//!
//! Every part and manifest is stored under a key unique to its write
//! (content hash plus a random nonce, `manifest` module docs), so no key is
//! ever reused after a deletion of it could have been decided, and a
//! delayed DELETE can only remove an object nobody references any more. A
//! writer still pins the keys it is about to reference, and a queued
//! deletion decided before the latest pin of its key is dropped.
//!
//! The source log ([`SourceClient`]), the object store and the wall clock
//! ([`Clock`]) are injected, and tasks and timers go through the crate's
//! task seam, so the engine runs under the deterministic simulator (U21).

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use tokio::sync::Notify;
use tokio::sync::watch;

use super::admission::Admission;
use super::admission::AdmissionLimits;
use super::admission::QueueTicket;
use super::admission::Reservation;
use super::admission::Slot;
use super::fold::RangeQuery;
use super::manifest::KEYED_PROJECTION_FORMAT;
use super::manifest::KeyedManifest;
use super::manifest::KeyedNamespace;
use super::manifest::KeyedPartMeta;
use super::manifest::KeyedSource;
use super::manifest::PublishOutcome;
use super::manifest::PublishedKeyedManifest;
use super::manifest::SweepReport;
use super::manifest::delete_decision_ttl;
use super::merge::KeyedPage;
use super::merge::get;
use super::merge::read_range;
use super::metrics::KeyedMetrics;
use super::metrics::KeyedMetricsSnapshot;
use super::metrics::NamespaceMetrics;
use super::metrics::bump;
use super::part::EncodedPart;
use super::part::FooterCache;
use super::part::OpenedPart;
use super::part::PartOpener;
use super::part::PartOptions;
use super::part::ResidentPart;
use super::part::StorePartOpener;
use super::run::CompactionPolicy;
use super::run::RunBuilder;
use super::run::compact;
use super::run::plan_compaction;
use super::source::IncarnationState;
use super::source::SourceClient;
use super::source::SourceError;
use crate::EventIndexCache;
use crate::IndexError;
use crate::clock::Clock;
use crate::clock::SystemClock;
use crate::object_store::ObjectRequestCounters;
use crate::object_store::ObjectStore;
use crate::rt;
use crate::rt::time::Instant;

/// Compactions attempted after one publication.
const COMPACTIONS_PER_PASS: usize = 4;
/// CAS attempts of one compaction's rebased manifest edit.
const COMPACTION_COMMIT_ATTEMPTS: usize = 3;
/// Pause before retrying a cycle that made no progress (a lagging replica).
const NO_PROGRESS_BACKOFF: Duration = Duration::from_millis(100);
/// Default process-wide admission budget: 1 GiB.
pub const DEFAULT_ADMISSION_BUDGET_BYTES: u64 = 1 << 30;
/// Default bound of the admission queue.
pub const DEFAULT_ADMISSION_QUEUE: usize = 4_096;

/// Tuning of the engine. Defaults follow the design.
#[derive(Clone, Debug)]
pub struct KeyedEngineConfig {
    /// Minimum spacing of publications, measured from
    /// `CURRENT.published_at_ms` (§3.4 step 3).
    pub min_publish_interval: Duration,
    /// Objects made unreachable are deleted only after this long, which
    /// must exceed the longest request.
    pub gc_grace: Duration,
    /// Period of the garbage-collection and idle-namespace sweep.
    pub gc_tick: Duration,
    /// Maximum concurrent waiting reads per pod; more answer 503.
    pub max_waiters: usize,
    /// `max_bytes` of one source page (P7).
    pub source_page_bytes: u64,
    /// Source bytes folded into one publication; a longer backlog is
    /// published in several steps without the publish interval.
    pub max_ingest_bytes: u64,
    /// Largest input of one compaction (the per-namespace compaction byte
    /// budget); larger plans are skipped.
    pub compaction_budget_bytes: u64,
    /// Bytes of freshly written parts kept in memory for reads.
    pub write_cache_bytes: usize,
    /// Bytes of decoded part footers (metadata, page index, tombstones and
    /// verified tails) kept in memory, shared by every namespace.
    pub footer_cache_bytes: usize,
    /// A namespace with no activity for this long is dropped from memory.
    pub idle_namespace: Duration,
    /// How long a drained bucket stays blocked (the purge erases it
    /// meanwhile; a recreated bucket is served again afterwards).
    pub drain_hold: Duration,
    /// Part encoding and read knobs.
    pub part_options: PartOptions,
    /// Size-tiered compaction policy.
    pub policy: CompactionPolicy,
    /// A read without `min_through_record` revalidates `CURRENT` (one
    /// HEAD, and a reload when it changed) when the namespace's view was
    /// last checked longer ago than this, so another pod's or a previous
    /// process's publication is never served below (P3.6).
    pub current_revalidate: Duration,
    /// Projection format of the namespaces this pod reads and writes
    /// (`v{fmt}/`). A pod at another format builds its own namespaces from
    /// record 0 next to the served ones: the blue/green rebuild (§6.1 U20).
    pub projection_format: u32,
    /// Process-wide byte budget of ingest pages, folded-but-unpublished
    /// records and compaction buffers ([`super::admission`]). A single
    /// reservation larger than this is clamped to it and runs alone.
    pub admission_budget_bytes: u64,
    /// Ingests (source reads, fold, publication) running at once.
    pub max_concurrent_ingests: usize,
    /// Compactions running at once.
    pub max_concurrent_compactions: usize,
    /// Namespaces admitted for ingestion but not yet ingesting; when full,
    /// reads that need ingestion answer 503 with `Retry-After`.
    pub admission_queue: usize,
    /// Longest one ingest cycle or one compaction may run once admitted
    /// (source reads, folding, part writes and the CAS). Past it the work
    /// is dropped, releasing its slot and budget; waiters get 503 and the
    /// next read retries.
    pub work_deadline: Duration,
}

/// Default concurrency: the host's CPU count (fixed under the simulator, so
/// runs reproduce on any host).
fn default_parallelism() -> usize {
    #[cfg(madsim)]
    {
        4
    }
    #[cfg(not(madsim))]
    {
        std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get)
    }
}

impl Default for KeyedEngineConfig {
    fn default() -> Self {
        Self {
            min_publish_interval: Duration::from_millis(5_000),
            gc_grace: Duration::from_secs(600),
            gc_tick: Duration::from_secs(30),
            max_waiters: 10_000,
            source_page_bytes: 16 * 1024 * 1024,
            max_ingest_bytes: 256 * 1024 * 1024,
            compaction_budget_bytes: 4 * 1024 * 1024 * 1024,
            write_cache_bytes: 256 * 1024 * 1024,
            footer_cache_bytes: super::part::DEFAULT_FOOTER_CACHE_BYTES,
            idle_namespace: Duration::from_secs(600),
            drain_hold: Duration::from_secs(600),
            part_options: PartOptions::default(),
            policy: CompactionPolicy::default(),
            current_revalidate: Duration::from_secs(1),
            projection_format: KEYED_PROJECTION_FORMAT,
            admission_budget_bytes: DEFAULT_ADMISSION_BUDGET_BYTES,
            max_concurrent_ingests: default_parallelism(),
            max_concurrent_compactions: default_parallelism().div_ceil(2),
            admission_queue: DEFAULT_ADMISSION_QUEUE,
            work_deadline: Duration::from_secs(120),
        }
    }
}

/// What a read selects.
#[derive(Clone, Debug)]
pub enum Selection {
    /// `key=k`.
    Point(Vec<u8>),
    /// `start|after`, `end`, `limit` and the response budget.
    Range(RangeQuery),
}

/// One keyed-state read as forwarded by a node.
#[derive(Clone, Debug)]
pub struct KeyedReadRequest {
    /// The stream incarnation.
    pub source: KeyedSource,
    /// The node's record tail `N`.
    pub source_next: u64,
    /// The selection.
    pub selection: Selection,
    /// `min_through_record`.
    pub min_through_record: Option<u64>,
    /// How long to wait for `min_through_record`.
    pub timeout: Duration,
}

/// The answer to a [`KeyedReadRequest`] (P3 statuses).
#[derive(Clone, Debug)]
pub enum KeyedReadOutcome {
    /// 200: rows of `state(through)`.
    Rows {
        /// `D`.
        through: u64,
        /// The rows and `Stream-Keyed-After`.
        page: KeyedPage,
    },
    /// 204: the wait timed out at `D = through`.
    NotYet {
        /// `D`.
        through: u64,
    },
    /// 400 with `Stream-Record-Next`.
    BeyondTail {
        /// The source tail.
        next_record: u64,
    },
    /// 500: a permanent condition, naming the record.
    Failed(String),
    /// 503: temporarily unavailable.
    Unavailable(String),
}

type NamespaceId = (String, String, u64);

fn namespace_id(source: &KeyedSource) -> NamespaceId {
    (
        source.bucket.clone(),
        source.key.clone(),
        source.incarnation,
    )
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// blake3 of a record's stored bytes: its message text plus the LF.
pub(crate) fn stored_digest(text: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(text.as_bytes());
    hasher.update(b"\n");
    hasher.finalize().to_hex().to_string()
}

#[derive(Clone, Debug, Default)]
enum Status {
    #[default]
    Ready,
    /// The last attempt failed transiently: 503 for waiters.
    Unavailable(String),
    /// A permanent failure: 500 for reads beyond `D`.
    Failed(String),
    /// The namespace failed its continuity check: 503 until rebuilt.
    Rebuilding,
}

#[derive(Clone, Debug, Default)]
struct View {
    published: Option<Arc<PublishedKeyedManifest>>,
    status: Status,
    /// `published` failed its continuity check: it is not served until a
    /// newer publication replaces it.
    invalid: bool,
}

impl View {
    fn through(&self) -> u64 {
        self.published
            .as_ref()
            .map_or(0, |published| published.manifest.through_record)
    }

    fn generation(&self) -> u64 {
        self.published
            .as_ref()
            .map_or(0, |published| published.manifest.generation)
    }
}

#[derive(Debug, Default)]
struct Work {
    running: bool,
    want_record: u64,
    want_next: u64,
    want_until: Option<Instant>,
    verify: bool,
}

struct Namespace {
    source: KeyedSource,
    namespace: KeyedNamespace,
    opener: LayeredOpener,
    view: watch::Sender<Arc<View>>,
    loaded: tokio::sync::Mutex<bool>,
    work: Mutex<Work>,
    last_used: Mutex<Instant>,
    /// The last ingest stopped at `max_ingest_bytes` before its target: the
    /// next one starts without the publish interval.
    backlog: AtomicBool,
    /// A part of the published manifest is missing although `CURRENT` did
    /// not move: the next cycle rebuilds the namespace from the source log.
    needs_rebuild: AtomicBool,
    /// When `CURRENT` was last read or revalidated.
    checked: Mutex<Instant>,
    /// The highest source tail `N` seen in a request (lag metric).
    source_next: AtomicU64,
    /// Object-store requests issued for this namespace.
    requests: Arc<ObjectRequestCounters>,
}

impl Namespace {
    fn view(&self) -> Arc<View> {
        Arc::clone(&self.view.borrow())
    }

    fn published(&self) -> Option<Arc<PublishedKeyedManifest>> {
        self.view().published.clone()
    }

    fn through(&self) -> u64 {
        self.view().through()
    }

    fn touch(&self) {
        *lock(&self.last_used) = Instant::now();
    }

    /// Adopts a publication newer than the one held; a new publication
    /// clears transient and rebuilding states. Returns whether it did.
    fn adopt(&self, published: Arc<PublishedKeyedManifest>) -> bool {
        self.view.send_if_modified(|view| {
            if view.published.is_some() && published.manifest.generation <= view.generation() {
                return false;
            }
            let status = match &view.status {
                Status::Failed(reason) => Status::Failed(reason.clone()),
                _ => Status::Ready,
            };
            *view = Arc::new(View {
                published: Some(published),
                status,
                invalid: false,
            });
            true
        })
    }

    fn set_status(&self, status: Status) {
        self.view.send_modify(|view| {
            *view = Arc::new(View {
                published: view.published.clone(),
                invalid: view.invalid || matches!(status, Status::Rebuilding),
                status,
            });
        });
    }

    /// Clears a transient failure so that a new want is not answered by an
    /// older attempt's 503.
    fn clear_unavailable(&self) {
        self.view.send_if_modified(|view| {
            if !matches!(view.status, Status::Unavailable(_)) {
                return false;
            }
            *view = Arc::new(View {
                published: view.published.clone(),
                status: Status::Ready,
                invalid: view.invalid,
            });
            true
        });
    }

    fn stop_work(&self) {
        let mut work = lock(&self.work);
        *work = Work::default();
    }

    fn mark_checked(&self, at: Instant) {
        let mut checked = lock(&self.checked);
        *checked = (*checked).max(at);
    }

    fn metrics(&self) -> NamespaceMetrics {
        let view = self.view();
        let through = view.through();
        let source_next = self.source_next.load(Ordering::Relaxed);
        NamespaceMetrics {
            bucket: self.source.bucket.clone(),
            key: self.source.key.clone(),
            incarnation: self.source.incarnation,
            through_record: through,
            source_next,
            lag_records: source_next.saturating_sub(through),
            runs: view
                .published
                .as_ref()
                .map_or(0, |published| published.manifest.runs.len()),
            generation: view.generation(),
            s3_requests: self.requests.snapshot().into(),
        }
    }
}

/// Freshly written parts, kept in memory so the next reads and compactions
/// need no object-store round trip (caches filled on write).
#[derive(Debug)]
struct WrittenParts {
    capacity: usize,
    state: Mutex<WrittenState>,
}

#[derive(Debug, Default)]
struct WrittenState {
    parts: HashMap<String, Arc<ResidentPart>>,
    order: VecDeque<String>,
    bytes: usize,
}

impl WrittenParts {
    fn insert(&self, key: String, bytes: Bytes) {
        if bytes.len() > self.capacity {
            return;
        }
        let mut state = lock(&self.state);
        if state.parts.contains_key(&key) {
            return;
        }
        state.bytes = state.bytes.saturating_add(bytes.len());
        state.order.push_back(key.clone());
        state.parts.insert(key, Arc::new(ResidentPart::new(bytes)));
        while state.bytes > self.capacity {
            let Some(oldest) = state.order.pop_front() else {
                break;
            };
            if let Some(evicted) = state.parts.remove(&oldest) {
                state.bytes = state.bytes.saturating_sub(evicted.len());
            }
        }
    }

    fn get(&self, key: &str) -> Option<Arc<ResidentPart>> {
        lock(&self.state).parts.get(key).cloned()
    }
}

/// Reads parts from the write cache when present (its footer decoded once
/// per part), else through the verified object-store reader and the pod's
/// footer cache.
#[derive(Clone)]
struct LayeredOpener {
    prefix: String,
    written: Arc<WrittenParts>,
    store: StorePartOpener,
}

impl PartOpener for LayeredOpener {
    fn open<'a>(
        &'a self,
        part: &'a KeyedPartMeta,
    ) -> BoxFuture<'a, Result<OpenedPart, IndexError>> {
        async move {
            let object_key = format!("{}{}", self.prefix, part.key);
            if let Some(resident) = self.written.get(&object_key) {
                return resident.open(&object_key, part).await;
            }
            self.store.open(part).await
        }
        .boxed()
    }
}

#[derive(Debug, Default)]
struct BucketState {
    busy: usize,
    drained_until: Option<Instant>,
}

/// A queued deletion: the namespace-relative key and the
/// [`KeyGuards::sequence`] at which it was decided.
type Decided = (String, u64);

struct GcItem {
    namespace: Arc<Namespace>,
    key: String,
    due: Instant,
    /// [`KeyGuards::sequence`] when the deletion was decided.
    decided: u64,
}

/// Coordination of writers and garbage collection over object keys.
#[derive(Debug, Default)]
struct KeyGuards {
    /// Orders deletion decisions and pins.
    sequence: u64,
    /// Keys a writer is about to reference (full object keys), counted.
    pinned: HashMap<String, usize>,
    /// The sequence of each key's latest pin.
    last_pinned: HashMap<String, u64>,
    /// Keys whose deletion is in flight.
    deleting: HashSet<String>,
}

impl KeyGuards {
    fn next(&mut self) -> u64 {
        self.sequence = self.sequence.saturating_add(1);
        self.sequence
    }
}

/// Keys pinned by a writer; unpinned on drop.
struct Pins {
    guards: Arc<Mutex<KeyGuards>>,
    keys: Vec<String>,
}

impl Drop for Pins {
    fn drop(&mut self) {
        let mut guards = lock(&self.guards);
        for key in &self.keys {
            if let Some(count) = guards.pinned.get_mut(key) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    guards.pinned.remove(key);
                }
            }
        }
    }
}

/// Why a worker cycle stopped.
#[derive(Debug)]
enum CycleError {
    /// 503 for the current waiters; a later read retries.
    Transient(String),
    /// 500 for reads beyond `D`.
    Permanent(String),
}

fn transient(error: impl std::fmt::Display) -> CycleError {
    CycleError::Transient(error.to_string())
}

fn source_failure(error: SourceError, record: u64) -> CycleError {
    match error {
        SourceError::Retained { first_record } => CycleError::Permanent(format!(
            "keyed state needs source record {record}, but records below {first_record} are no \
             longer retained"
        )),
        SourceError::NotKeyed => {
            CycleError::Permanent("the source stream does not advertise keyed-batch-v1".to_owned())
        }
        SourceError::NotFound => CycleError::Transient("the source stream is absent".to_owned()),
        other => CycleError::Transient(other.to_string()),
    }
}

/// Outcome of folding source records.
enum Folded {
    Done,
    /// Record `D−1` is missing or differs: the reason.
    Discontinuity(String),
}

struct Inner {
    store: ObjectStore,
    source: Arc<dyn SourceClient>,
    clock: Arc<dyn Clock>,
    cache: Option<EventIndexCache>,
    written: Arc<WrittenParts>,
    /// Decoded part footers of every namespace of the pod.
    footers: FooterCache,
    config: KeyedEngineConfig,
    namespaces: Mutex<HashMap<NamespaceId, Arc<Namespace>>>,
    buckets: Mutex<HashMap<String, BucketState>>,
    bucket_idle: Notify,
    waiters: AtomicUsize,
    gc: Mutex<Vec<GcItem>>,
    metrics: KeyedMetrics,
    guards: Arc<Mutex<KeyGuards>>,
    /// Signalled when a deletion in flight finishes.
    deletions: Notify,
    /// Garbage-collection passes run one at a time.
    gc_pass: tokio::sync::Mutex<()>,
    /// Process-wide byte budget, ingest and compaction slots, and the
    /// admission queue.
    admission: Admission,
    /// Set by [`KeyedEngine::shutdown`]: reads answer 503 and no worker
    /// starts.
    closing: AtomicBool,
    /// Set when shutdown cancels the workers still running, with
    /// `cancelled` signalled. (A `Notify`, not a `watch`: a watch picks
    /// its wait slot from tokio's thread-local RNG, which simulation
    /// replays do not reset.)
    cancel: AtomicBool,
    cancelled: Notify,
    /// Background workers alive (spawned and not yet finished).
    workers: AtomicUsize,
    /// Signalled when a worker finishes.
    worker_done: Notify,
    /// Test hook run inside the blocking run encode (see
    /// [`KeyedEngine::set_blocking_encode_hook`]).
    encode_hook: Mutex<Option<BlockingHook>>,
}

/// A callback run on the blocking pool; a test seam.
pub type BlockingHook = Arc<dyn Fn() + Send + Sync>;

/// Counts one background worker; uncounted (and announced) on drop, when
/// its task ends or is cancelled.
struct WorkerGuard(Arc<Inner>);

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        self.0.workers.fetch_sub(1, Ordering::SeqCst);
        self.0.worker_done.notify_waiters();
    }
}

/// The keyed projection engine of one indexer pod.
#[derive(Clone)]
pub struct KeyedEngine {
    inner: Arc<Inner>,
}

/// Keeps a bucket busy (a read or a worker in flight) for `drain`.
struct BusyGuard {
    inner: Arc<Inner>,
    bucket: String,
}

impl Drop for BusyGuard {
    fn drop(&mut self) {
        let mut buckets = lock(&self.inner.buckets);
        if let Some(state) = buckets.get_mut(&self.bucket) {
            state.busy = state.busy.saturating_sub(1);
            if state.busy == 0 && state.drained_until.is_none() {
                buckets.remove(&self.bucket);
            }
        }
        drop(buckets);
        self.inner.bucket_idle.notify_waiters();
    }
}

struct WaiterGuard<'a>(&'a AtomicUsize);

impl Drop for WaiterGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl KeyedEngine {
    /// An engine storing namespaces in `store` (whose root holds `.keyed/`)
    /// and reading sources through `source`, on the host's wall clock.
    /// `cache` is a serving cache for verified part ranges.
    pub fn new(
        store: ObjectStore,
        source: impl SourceClient + 'static,
        cache: Option<EventIndexCache>,
        config: KeyedEngineConfig,
    ) -> Self {
        Self::with_clock(store, source, cache, config, Arc::new(SystemClock))
    }

    /// [`Self::new`] on an injected wall clock (the simulator's virtual
    /// time).
    pub fn with_clock(
        store: ObjectStore,
        source: impl SourceClient + 'static,
        cache: Option<EventIndexCache>,
        config: KeyedEngineConfig,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let written = Arc::new(WrittenParts {
            capacity: config.write_cache_bytes,
            state: Mutex::new(WrittenState::default()),
        });
        let metrics = KeyedMetrics::default();
        let store = store.counted(Arc::clone(&metrics.requests));
        let admission = Admission::new(AdmissionLimits {
            budget_bytes: config.admission_budget_bytes,
            ingests: config.max_concurrent_ingests,
            compactions: config.max_concurrent_compactions,
            queue: config.admission_queue,
        });
        Self {
            inner: Arc::new(Inner {
                store,
                source: Arc::new(source),
                clock,
                cache,
                written,
                footers: FooterCache::new(config.footer_cache_bytes),
                config,
                namespaces: Mutex::new(HashMap::new()),
                buckets: Mutex::new(HashMap::new()),
                bucket_idle: Notify::new(),
                waiters: AtomicUsize::new(0),
                gc: Mutex::new(Vec::new()),
                metrics,
                guards: Arc::new(Mutex::new(KeyGuards::default())),
                deletions: Notify::new(),
                gc_pass: tokio::sync::Mutex::new(()),
                admission,
                closing: AtomicBool::new(false),
                cancel: AtomicBool::new(false),
                cancelled: Notify::new(),
                workers: AtomicUsize::new(0),
                worker_done: Notify::new(),
                encode_hook: Mutex::new(None),
            }),
        }
    }

    /// The engine configuration.
    pub fn config(&self) -> &KeyedEngineConfig {
        &self.inner.config
    }

    /// Serves one keyed-state read.
    pub async fn read(&self, request: KeyedReadRequest) -> KeyedReadOutcome {
        Inner::read(&self.inner, request).await
    }

    /// Blocks new work for `bucket`, waits for its in-flight reads and
    /// workers, and forgets its namespaces. Idempotent; the block lasts
    /// `drain_hold`.
    pub async fn drain(&self, bucket: &str) {
        Inner::drain(&self.inner, bucket).await;
    }

    /// A snapshot of the engine's metrics (U24).
    pub fn metrics(&self) -> KeyedMetricsSnapshot {
        let detail: Vec<NamespaceMetrics> = lock(&self.inner.namespaces)
            .values()
            .map(|namespace| namespace.metrics())
            .collect();
        self.inner.metrics.snapshot(
            self.inner.waiters.load(Ordering::SeqCst),
            lock(&self.inner.gc).len(),
            self.inner.admission.metrics(),
            detail,
        )
    }

    /// Stops the engine: new reads answer 503 and no new work starts; the
    /// background workers in flight get up to `grace` to finish, and the
    /// rest are cancelled (their waiters answer 503). When this returns,
    /// no background worker is running. Idempotent.
    pub async fn shutdown(&self, grace: Duration) {
        let inner = &self.inner;
        inner.closing.store(true, Ordering::SeqCst);
        let deadline = Instant::now()
            .checked_add(grace)
            .unwrap_or_else(Instant::now);
        if !inner.wait_workers(Some(deadline)).await {
            tracing::warn!(
                workers = inner.workers.load(Ordering::SeqCst),
                "keyed workers did not finish within the shutdown grace; cancelling them"
            );
            inner.cancel.store(true, Ordering::SeqCst);
            inner.cancelled.notify_waiters();
            let _idle = inner.wait_workers(None).await;
        }
    }

    /// Test seam: runs `hook` inside every blocking run encode, before the
    /// encode itself, so a test can hold one past the work deadline.
    #[doc(hidden)]
    pub fn set_blocking_encode_hook(&self, hook: Option<BlockingHook>) {
        *lock(&self.inner.encode_hook) = hook;
    }

    /// Background workers running now (including blocking work a deadline
    /// abandoned that is still running).
    pub fn background_workers(&self) -> usize {
        self.inner.workers.load(Ordering::SeqCst)
    }

    /// Deletes due garbage now; returns the number of objects deleted.
    pub async fn collect_garbage(&self) -> usize {
        self.inner.collect_garbage().await
    }

    /// The orphan sweep of `source`'s namespace ([`KeyedNamespace::sweep`],
    /// the rule shared with `ursula indexer keyed sweep`): deletes parts and
    /// manifests older than the GC grace period that no manifest a reader
    /// may still hold references (objects a crash left behind, or whose
    /// queued deletion a crash lost).
    pub async fn sweep(&self, source: &KeyedSource) -> Result<SweepReport, IndexError> {
        let grace = self.inner.config.gc_grace;
        KeyedNamespace::with_format(
            self.inner.store.clone(),
            source.clone(),
            self.inner.config.projection_format,
        )
        .with_grace(grace)
        .sweep(&*self.inner.clock, grace, false)
        .await
    }

    /// Runs garbage collection every `gc_tick` until `shutdown` turns true.
    pub async fn run_maintenance(&self, mut shutdown: watch::Receiver<bool>) {
        loop {
            // Biased: a fixed poll order keeps simulation runs reproducible.
            tokio::select! {
                biased;
                () = rt::time::sleep(self.inner.config.gc_tick) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
            let deleted = self.inner.collect_garbage().await;
            if deleted > 0 {
                tracing::debug!(deleted, "keyed GC deleted unreachable objects");
            }
        }
    }
}

impl Inner {
    fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    fn enter(self: &Arc<Self>, bucket: &str) -> Option<BusyGuard> {
        let mut buckets = lock(&self.buckets);
        let state = buckets.entry(bucket.to_owned()).or_default();
        if let Some(until) = state.drained_until {
            if until > Instant::now() {
                return None;
            }
            state.drained_until = None;
        }
        state.busy = state.busy.saturating_add(1);
        Some(BusyGuard {
            inner: Arc::clone(self),
            bucket: bucket.to_owned(),
        })
    }

    fn draining(&self, bucket: &str) -> bool {
        lock(&self.buckets)
            .get(bucket)
            .and_then(|state| state.drained_until)
            .is_some_and(|until| until > Instant::now())
    }

    fn namespace(&self, source: &KeyedSource) -> Result<Arc<Namespace>, IndexError> {
        let id = namespace_id(source);
        let mut namespaces = lock(&self.namespaces);
        if let Some(namespace) = namespaces.get(&id) {
            namespace.touch();
            return Ok(Arc::clone(namespace));
        }
        let requests = Arc::new(ObjectRequestCounters::default());
        let namespace = KeyedNamespace::with_format(
            self.store.clone().counted(Arc::clone(&requests)),
            source.clone(),
            self.config.projection_format,
        )
        .with_grace(self.config.gc_grace);
        let mut store = namespace.opener().with_footer_cache(self.footers.clone());
        if let Some(cache) = &self.cache {
            store = store.with_cache(cache)?;
        }
        let (view, _receiver) = watch::channel(Arc::new(View::default()));
        let created = Arc::new(Namespace {
            source: source.clone(),
            opener: LayeredOpener {
                prefix: namespace.prefix().to_owned(),
                written: Arc::clone(&self.written),
                store,
            },
            namespace,
            view,
            loaded: tokio::sync::Mutex::new(false),
            work: Mutex::new(Work::default()),
            last_used: Mutex::new(Instant::now()),
            backlog: AtomicBool::new(false),
            needs_rebuild: AtomicBool::new(false),
            checked: Mutex::new(Instant::now()),
            source_next: AtomicU64::new(0),
            requests,
        });
        namespaces.insert(id, Arc::clone(&created));
        Ok(created)
    }

    /// Loads `CURRENT` once per namespace (later loads happen in workers
    /// and GC). The loaded manifest's obsoleted objects join the GC queue.
    async fn ensure_loaded(&self, namespace: &Arc<Namespace>) -> Result<(), IndexError> {
        let mut loaded = namespace.loaded.lock().await;
        if *loaded {
            return Ok(());
        }
        let started = Instant::now();
        if let Some(published) = namespace.namespace.load().await? {
            self.adopt_loaded(namespace, published);
        }
        namespace.mark_checked(started);
        *loaded = true;
        Ok(())
    }

    /// Adopts a manifest loaded from `CURRENT`. When it is newer than the
    /// view, it was published elsewhere (another pod, a previous process,
    /// a rebuild), so the objects its edit made obsolete join this pod's GC
    /// queue, due a grace period after its publication.
    fn adopt_loaded(&self, namespace: &Arc<Namespace>, published: PublishedKeyedManifest) {
        let published = Arc::new(published);
        let obsoleted = published.manifest.obsoleted.clone();
        let remaining = published
            .manifest
            .published_at_ms
            .saturating_add(u64::try_from(self.config.gc_grace.as_millis()).unwrap_or(u64::MAX))
            .saturating_sub(self.now_ms());
        if namespace.adopt(published) {
            let due = Instant::now()
                .checked_add(Duration::from_millis(remaining))
                .unwrap_or_else(Instant::now);
            self.schedule_gc(namespace, obsoleted, due);
        }
    }

    /// Revalidates `CURRENT` when the namespace's view was last checked
    /// longer ago than `current_revalidate`: one HEAD, and a reload when
    /// the entity tag differs from the view's (P3.6 across pods and
    /// restarts). Single flight per namespace.
    async fn revalidate(&self, namespace: &Arc<Namespace>) -> Result<(), IndexError> {
        let bound = self.config.current_revalidate;
        if lock(&namespace.checked).elapsed() < bound {
            return Ok(());
        }
        let _loading = namespace.loaded.lock().await;
        if lock(&namespace.checked).elapsed() < bound {
            return Ok(());
        }
        let started = Instant::now();
        bump(&self.metrics.current_revalidations, 1);
        let current = namespace.namespace.current_etag().await?;
        let held = namespace
            .published()
            .map(|published| published.pointer_etag.clone());
        if current.is_some()
            && current != held
            && let Some(published) = namespace.namespace.load().await?
        {
            self.adopt_loaded(namespace, published);
        }
        namespace.mark_checked(started);
        Ok(())
    }

    async fn reload(
        &self,
        namespace: &Arc<Namespace>,
    ) -> Result<Option<Arc<PublishedKeyedManifest>>, CycleError> {
        let started = Instant::now();
        if let Some(published) = namespace.namespace.load().await.map_err(transient)? {
            self.adopt_loaded(namespace, published);
        }
        namespace.mark_checked(started);
        Ok(namespace.published())
    }

    /// Resolves once shutdown cancels the background workers.
    async fn wait_cancelled(&self) {
        loop {
            let cancelled = self.cancelled.notified();
            tokio::pin!(cancelled);
            cancelled.as_mut().enable();
            if self.cancel.load(Ordering::SeqCst) {
                return;
            }
            cancelled.await;
        }
    }

    /// Waits until no background worker runs, or `until` passes; returns
    /// whether none runs.
    async fn wait_workers(&self, until: Option<Instant>) -> bool {
        loop {
            let done = self.worker_done.notified();
            tokio::pin!(done);
            done.as_mut().enable();
            if self.workers.load(Ordering::SeqCst) == 0 {
                return true;
            }
            match until {
                None => done.await,
                Some(until) => {
                    // Biased: simulation determinism.
                    tokio::select! {
                        biased;
                        () = &mut done => {}
                        () = rt::time::sleep_until(until) => {
                            return self.workers.load(Ordering::SeqCst) == 0;
                        }
                    }
                }
            }
        }
    }

    async fn read(self: &Arc<Self>, request: KeyedReadRequest) -> KeyedReadOutcome {
        if self.closing.load(Ordering::SeqCst) {
            return KeyedReadOutcome::Unavailable("the keyed indexer is shutting down".to_owned());
        }
        let bucket = request.source.bucket.clone();
        let Some(_busy) = self.enter(&bucket) else {
            return KeyedReadOutcome::Unavailable("the bucket is draining".to_owned());
        };
        if let Some(record) = request.min_through_record
            && record > request.source_next
        {
            return KeyedReadOutcome::BeyondTail {
                next_record: request.source_next,
            };
        }
        let namespace = match self.namespace(&request.source) {
            Ok(namespace) => namespace,
            Err(error) => return KeyedReadOutcome::Unavailable(error.to_string()),
        };
        namespace
            .source_next
            .fetch_max(request.source_next, Ordering::Relaxed);
        if let Err(error) = self.ensure_loaded(&namespace).await {
            tracing::warn!(%error, bucket, key = %request.source.key, "keyed namespace load failed");
            return KeyedReadOutcome::Unavailable("keyed state cannot be loaded".to_owned());
        }
        if request.min_through_record.is_none()
            && let Err(error) = self.revalidate(&namespace).await
        {
            tracing::warn!(%error, bucket, key = %request.source.key, "keyed CURRENT revalidation failed");
            return KeyedReadOutcome::Unavailable("keyed state cannot be loaded".to_owned());
        }
        let mut receiver = namespace.view.subscribe();
        let view = Arc::clone(&receiver.borrow_and_update());
        if view.invalid {
            // Restart the rebuild if its worker stopped (a transient failure).
            let _admitted = self.request_work(&namespace, None, true);
            return match &view.status {
                Status::Failed(reason) => KeyedReadOutcome::Failed(reason.clone()),
                _ => KeyedReadOutcome::Unavailable("keyed state is being rebuilt".to_owned()),
            };
        }
        if view.through() > request.source_next {
            let _admitted = self.request_work(&namespace, None, true);
            return KeyedReadOutcome::Unavailable(
                "keyed state is ahead of its source log and is being re-validated".to_owned(),
            );
        }
        let Some(wanted) = request
            .min_through_record
            .filter(|wanted| *wanted > view.through())
        else {
            return self.serve(&namespace, &view, &request.selection).await;
        };
        if let Status::Failed(reason) = &view.status {
            return KeyedReadOutcome::Failed(reason.clone());
        }
        if self.waiters.fetch_add(1, Ordering::SeqCst) >= self.config.max_waiters {
            self.waiters.fetch_sub(1, Ordering::SeqCst);
            return KeyedReadOutcome::Unavailable("too many waiting keyed-state reads".to_owned());
        }
        let _waiter = WaiterGuard(&self.waiters);
        let deadline = Instant::now()
            .checked_add(request.timeout)
            .unwrap_or_else(Instant::now);
        if !self.request_work(
            &namespace,
            Some((wanted, request.source_next, deadline)),
            false,
        ) {
            return KeyedReadOutcome::Unavailable(
                "the keyed indexer's admission queue is full".to_owned(),
            );
        }
        loop {
            let view = Arc::clone(&receiver.borrow_and_update());
            match &view.status {
                Status::Failed(reason) => return KeyedReadOutcome::Failed(reason.clone()),
                Status::Unavailable(reason) => {
                    return KeyedReadOutcome::Unavailable(reason.clone());
                }
                Status::Rebuilding => {
                    return KeyedReadOutcome::Unavailable(
                        "keyed state is being rebuilt".to_owned(),
                    );
                }
                Status::Ready => {}
            }
            if view.through() >= wanted {
                return self.serve(&namespace, &view, &request.selection).await;
            }
            // Biased: a publication and the deadline at the same instant
            // resolve the same way on every run (simulation determinism).
            tokio::select! {
                biased;
                changed = receiver.changed() => {
                    if changed.is_err() {
                        return KeyedReadOutcome::Unavailable("keyed namespace closed".to_owned());
                    }
                }
                () = rt::time::sleep_until(deadline) => {
                    let view = Arc::clone(&receiver.borrow());
                    if matches!(view.status, Status::Ready) && view.through() >= wanted {
                        return self.serve(&namespace, &view, &request.selection).await;
                    }
                    return KeyedReadOutcome::NotYet { through: view.through() };
                }
            }
        }
    }

    /// Serves `selection` from `view`. When the read fails, the view's parts
    /// may have been removed by another pod's compaction and GC after this
    /// pod last loaded `CURRENT` (IX1): reload `CURRENT` once and, when it
    /// is newer, retry on it before answering 503. When a part is missing
    /// and `CURRENT` has not moved, the published state is damaged; the
    /// namespace is rebuilt from the source log (503 meanwhile) instead of
    /// failing every read forever.
    async fn serve(
        self: &Arc<Self>,
        namespace: &Arc<Namespace>,
        view: &View,
        selection: &Selection,
    ) -> KeyedReadOutcome {
        let error = match self.serve_view(namespace, view, selection).await {
            Ok(outcome) => return outcome,
            Err(error) => error,
        };
        let held = view.generation();
        let started = Instant::now();
        match namespace.namespace.load().await {
            Ok(Some(published)) if published.manifest.generation > held => {
                self.adopt_loaded(namespace, published);
                namespace.mark_checked(started);
                let reloaded = namespace.view();
                if reloaded.generation() > held && reloaded.through() >= view.through() {
                    match self.serve_view(namespace, &reloaded, selection).await {
                        Ok(outcome) => return outcome,
                        Err(retry_error) => {
                            return self.read_failed(namespace, &retry_error);
                        }
                    }
                }
            }
            Ok(Some(published))
                if published.manifest.generation == held
                    && matches!(error, IndexError::MissingObject(_)) =>
            {
                return self.rebuild_damaged(namespace, &error);
            }
            Ok(_) => {}
            Err(load_error) => {
                tracing::warn!(
                    error = %load_error,
                    bucket = %namespace.source.bucket,
                    key = %namespace.source.key,
                    "keyed CURRENT reload after a failed read failed"
                );
            }
        }
        self.read_failed(namespace, &error)
    }

    /// `CURRENT` references a part that is gone: schedules a rebuild of the
    /// namespace from the source log and answers 503 until it publishes.
    fn rebuild_damaged(
        self: &Arc<Self>,
        namespace: &Arc<Namespace>,
        error: &IndexError,
    ) -> KeyedReadOutcome {
        tracing::error!(
            %error,
            bucket = %namespace.source.bucket,
            key = %namespace.source.key,
            "keyed CURRENT references a missing object; rebuilding the namespace"
        );
        bump(&self.metrics.damaged_rebuilds, 1);
        namespace.needs_rebuild.store(true, Ordering::SeqCst);
        namespace.set_status(Status::Rebuilding);
        let _admitted = self.request_work(namespace, None, true);
        KeyedReadOutcome::Unavailable("keyed state is being rebuilt".to_owned())
    }

    fn read_failed(&self, namespace: &Namespace, error: &IndexError) -> KeyedReadOutcome {
        tracing::warn!(
            %error,
            bucket = %namespace.source.bucket,
            key = %namespace.source.key,
            "keyed-state read failed"
        );
        KeyedReadOutcome::Unavailable("keyed state cannot be read".to_owned())
    }

    async fn serve_view(
        &self,
        namespace: &Namespace,
        view: &View,
        selection: &Selection,
    ) -> Result<KeyedReadOutcome, IndexError> {
        let through = view.through();
        let runs = view
            .published
            .as_ref()
            .map_or(&[][..], |published| published.manifest.runs.as_slice());
        let options = &self.config.part_options;
        let page = match selection {
            Selection::Point(key) => {
                get(&namespace.opener, runs, key, options)
                    .await
                    .map(|row| KeyedPage {
                        rows: row.into_iter().collect(),
                        after: None,
                    })
            }
            Selection::Range(query) => read_range(&namespace.opener, runs, query, options).await,
        }?;
        Ok(KeyedReadOutcome::Rows { through, page })
    }

    /// Registers a want (or a re-validation) and starts the namespace's
    /// worker unless one runs (single flight). A new worker needs a place in
    /// the admission queue; returns false, registering nothing, when the
    /// queue is full.
    fn request_work(
        self: &Arc<Self>,
        namespace: &Arc<Namespace>,
        want: Option<(u64, u64, Instant)>,
        verify: bool,
    ) -> bool {
        if self.closing.load(Ordering::SeqCst) {
            return false;
        }
        let mut work = lock(&namespace.work);
        let ticket = if work.running {
            None
        } else {
            let Some(ticket) = self.admission.try_enqueue() else {
                return false;
            };
            Some(ticket)
        };
        if let Some((record, next, until)) = want {
            work.want_record = work.want_record.max(record);
            work.want_next = work.want_next.max(next);
            work.want_until = Some(work.want_until.map_or(until, |current| current.max(until)));
            namespace.clear_unavailable();
        }
        work.verify |= verify;
        if work.running {
            return true;
        }
        work.running = true;
        drop(work);
        // Counted before the closing check, so shutdown either sees this
        // worker or this call sees shutdown.
        self.workers.fetch_add(1, Ordering::SeqCst);
        let guard = WorkerGuard(Arc::clone(self));
        if self.closing.load(Ordering::SeqCst) {
            *lock(&namespace.work) = Work::default();
            return false;
        }
        let inner = Arc::clone(self);
        let namespace = Arc::clone(namespace);
        let _worker = rt::spawn(async move {
            let _guard = guard;
            // Biased: a cancellation wins over further work.
            tokio::select! {
                biased;
                () = inner.wait_cancelled() => {
                    let mut work = lock(&namespace.work);
                    namespace.set_status(Status::Unavailable(
                        "the keyed indexer is shutting down".to_owned(),
                    ));
                    *work = Work::default();
                }
                () = Arc::clone(&inner).run_worker(Arc::clone(&namespace), ticket) => {}
            }
        });
        true
    }

    /// The namespace's worker. `ticket` is its place in the admission
    /// queue, left when the first ingest slot is granted.
    async fn run_worker(
        self: Arc<Self>,
        namespace: Arc<Namespace>,
        mut ticket: Option<QueueTicket>,
    ) {
        let Some(_busy) = self.enter(&namespace.source.bucket) else {
            namespace.stop_work();
            return;
        };
        loop {
            if self.draining(&namespace.source.bucket) {
                namespace.stop_work();
                return;
            }
            let through = namespace.through();
            let (want_next, verify) = {
                let mut work = lock(&namespace.work);
                if work.want_until.is_some_and(|until| until <= Instant::now()) {
                    work.want_record = 0;
                    work.want_until = None;
                }
                if work.want_record <= through && !work.verify {
                    *work = Work::default();
                    return;
                }
                let verify = work.verify;
                work.verify = false;
                (work.want_next, verify)
            };
            match self.cycle(&namespace, want_next, verify, &mut ticket).await {
                Ok(true) => {}
                Ok(false) => rt::time::sleep(NO_PROGRESS_BACKOFF).await,
                Err(error) => {
                    let status = match error {
                        CycleError::Transient(reason) => {
                            tracing::warn!(
                                bucket = %namespace.source.bucket,
                                key = %namespace.source.key,
                                reason,
                                "keyed ingest attempt failed"
                            );
                            Status::Unavailable(reason)
                        }
                        CycleError::Permanent(reason) => {
                            tracing::error!(
                                bucket = %namespace.source.bucket,
                                key = %namespace.source.key,
                                reason,
                                "keyed namespace cannot advance"
                            );
                            Status::Failed(reason)
                        }
                    };
                    // Under the work lock, so a want registered meanwhile
                    // either sees this status cleared or starts a worker.
                    let mut work = lock(&namespace.work);
                    namespace.set_status(status);
                    *work = Work::default();
                    return;
                }
            }
            let pending = lock(&namespace.work).want_record > namespace.through();
            let runs = namespace
                .published()
                .map_or(0, |published| published.manifest.runs.len());
            if (!pending || runs > self.config.policy.max_runs)
                && let Err(error) = self.compact_pass(&namespace).await
            {
                tracing::warn!(
                    bucket = %namespace.source.bucket,
                    key = %namespace.source.key,
                    ?error,
                    "keyed compaction failed"
                );
            }
        }
    }

    /// Waits for the publish interval and an ingest slot, reloads `CURRENT`
    /// and ingests up to `want_next`. Returns whether `D` advanced (or a
    /// re-validation ran).
    async fn cycle(
        self: &Arc<Self>,
        namespace: &Arc<Namespace>,
        want_next: u64,
        verify: bool,
        ticket: &mut Option<QueueTicket>,
    ) -> Result<bool, CycleError> {
        let before = namespace.through();
        if !namespace.backlog.load(Ordering::SeqCst)
            && let Some(published) = namespace.published()
        {
            let interval =
                u64::try_from(self.config.min_publish_interval.as_millis()).unwrap_or(u64::MAX);
            let wait = published
                .manifest
                .published_at_ms
                .saturating_add(interval)
                .saturating_sub(self.now_ms())
                .min(interval);
            if wait > 0 {
                rt::time::sleep(Duration::from_millis(wait)).await;
            }
        }
        // Shared with blocking work, which keeps it past a deadline.
        let slot = Arc::new(self.admission.ingest_slot().await);
        drop(ticket.take());
        self.within("ingest", async {
            let base = self.reload(namespace).await?;
            let through = base
                .as_ref()
                .map_or(0, |published| published.manifest.through_record);
            if !verify && through >= lock(&namespace.work).want_record {
                return Ok(true);
            }
            self.ingest(namespace, base, want_next.max(through), &slot)
                .await?;
            Ok(verify || namespace.through() > before)
        })
        .await
    }

    async fn ingest(
        self: &Arc<Self>,
        namespace: &Arc<Namespace>,
        base: Option<Arc<PublishedKeyedManifest>>,
        target: u64,
        slot: &Arc<Slot>,
    ) -> Result<(), CycleError> {
        if base.is_some() && namespace.needs_rebuild.load(Ordering::SeqCst) {
            // The published state is damaged (a missing part).
            namespace.set_status(Status::Rebuilding);
            return match self
                .fold(namespace, base.as_ref(), true, target, slot)
                .await?
            {
                Folded::Done => {
                    namespace.needs_rebuild.store(false, Ordering::SeqCst);
                    Ok(())
                }
                Folded::Discontinuity(reason) => Err(CycleError::Transient(reason)),
            };
        }
        let Folded::Discontinuity(reason) = self
            .fold(namespace, base.as_ref(), false, target, slot)
            .await?
        else {
            return Ok(());
        };
        let Some(published) = base.as_ref() else {
            return Err(CycleError::Transient(reason));
        };
        // Confirm with the leader before retiring the namespace: a lagging
        // replica can be behind a namespace built from the leader.
        let d = published.manifest.through_record;
        let previous = d.saturating_sub(1);
        let _page = self.admission.reserve(self.config.source_page_bytes).await;
        match self
            .source
            .read(
                &namespace.source.bucket,
                &namespace.source.key,
                previous,
                self.config.source_page_bytes,
                Some(1),
                true,
            )
            .await
        {
            Ok(page)
                if page.records.first().is_some_and(|text| {
                    Some(stored_digest(text)) == published.manifest.through_digest
                }) =>
            {
                return Err(CycleError::Transient(
                    "the source replica is behind keyed state".to_owned(),
                ));
            }
            Ok(_) | Err(SourceError::BeyondTail { .. }) => {}
            Err(error) => return Err(source_failure(error, previous)),
        }
        tracing::warn!(
            bucket = %namespace.source.bucket,
            key = %namespace.source.key,
            incarnation = namespace.source.incarnation,
            through = d,
            reason,
            "keyed namespace failed its continuity check; rebuilding from record 0"
        );
        namespace.set_status(Status::Rebuilding);
        match self
            .fold(namespace, base.as_ref(), true, target, slot)
            .await?
        {
            Folded::Done => Ok(()),
            Folded::Discontinuity(reason) => Err(CycleError::Transient(reason)),
        }
    }

    /// Reads `[D−1, target)` (from 0 for a rebuild or `D = 0`), checks the
    /// continuity record, folds the rest into one run and publishes it.
    async fn fold(
        self: &Arc<Self>,
        namespace: &Arc<Namespace>,
        base: Option<&Arc<PublishedKeyedManifest>>,
        rebuild: bool,
        target: u64,
        slot: &Arc<Slot>,
    ) -> Result<Folded, CycleError> {
        let (d, mut expected) = match base {
            Some(published) if !rebuild => (
                published.manifest.through_record,
                published.manifest.through_digest.clone(),
            ),
            _ => (0, None),
        };
        // A rebuild replaces a publication at `D`: it may not publish less
        // while the source holds that many records, so `max_ingest_bytes`
        // splits it only above the old `D` (D is monotone per namespace).
        let floor = match base {
            Some(published) if rebuild => published.manifest.through_record,
            _ => 0,
        };
        let drop_tombstones =
            rebuild || base.is_none_or(|published| published.manifest.runs.is_empty());
        let mut builder = RunBuilder::new(d, drop_tombstones);
        let mut cursor = if expected.is_some() {
            d.saturating_sub(1)
        } else {
            d
        };
        let mut last_digest = None;
        let mut bytes = 0_u64;
        let mut truncated = false;
        // Admission: the folded records this ingest holds plus the next
        // page, kept until the publication is done.
        let mut held: Option<Reservation> = None;
        let bucket = namespace.source.bucket.as_str();
        let key = namespace.source.key.as_str();
        'pages: loop {
            if expected.is_none() && cursor >= target {
                break;
            }
            if expected.is_none() && bytes >= self.config.max_ingest_bytes && cursor >= floor {
                truncated = true;
                break;
            }
            let needed = bytes.saturating_add(self.config.source_page_bytes);
            let grown = held
                .as_mut()
                .is_some_and(|reservation| self.admission.try_grow(reservation, needed));
            if !grown {
                if held.is_some() && expected.is_none() && last_digest.is_some() && cursor >= floor
                {
                    // The budget is taken: publish what is folded and
                    // continue in the next cycle without the interval.
                    self.admission.truncated();
                    truncated = true;
                    break;
                }
                // Never wait while holding a reservation.
                drop(held.take());
                held = Some(self.admission.reserve(needed).await);
            }
            let page = match self
                .source
                .read(
                    bucket,
                    key,
                    cursor,
                    self.config.source_page_bytes,
                    None,
                    false,
                )
                .await
            {
                Ok(page) => page,
                Err(SourceError::BeyondTail { next_record }) if expected.is_some() => {
                    return Ok(Folded::Discontinuity(format!(
                        "record {cursor} is beyond the source tail {next_record}"
                    )));
                }
                Err(SourceError::BeyondTail { .. }) => break,
                Err(error) => return Err(source_failure(error, cursor)),
            };
            if page.records.is_empty() {
                if expected.is_some() {
                    return Ok(Folded::Discontinuity(format!(
                        "record {cursor} is at the source tail"
                    )));
                }
                break;
            }
            for (record, text) in (page.start_record..).zip(&page.records) {
                if let Some(digest) = expected.take() {
                    if stored_digest(text) != digest {
                        return Ok(Folded::Discontinuity(format!(
                            "record {record} differs from the record keyed state was built from"
                        )));
                    }
                    continue;
                }
                if record >= target {
                    break 'pages;
                }
                if bytes >= self.config.max_ingest_bytes && record >= floor {
                    truncated = true;
                    break 'pages;
                }
                builder
                    .apply_message(record, text)
                    .map_err(|error| match error {
                        IndexError::InvalidKeyedRecord { record, reason } => CycleError::Permanent(
                            format!("source record {record} cannot be applied: {reason}"),
                        ),
                        other => transient(other),
                    })?;
                last_digest = Some(stored_digest(text));
                bytes = bytes
                    .saturating_add(u64::try_from(text.len()).unwrap_or(u64::MAX))
                    .saturating_add(1);
            }
            cursor = page.next_record;
        }
        namespace.backlog.store(truncated, Ordering::SeqCst);
        let empty_base =
            || KeyedManifest::empty_at(namespace.source.clone(), namespace.namespace.format());
        let mut manifest = if rebuild {
            empty_base()
        } else {
            base.map_or_else(empty_base, |published| published.manifest.clone())
        };
        let mut new_keys = Vec::new();
        let mut pins = None;
        if let Some(digest) = last_digest {
            let through = builder.next_record();
            let options = self.config.part_options;
            // The blocking encode cannot be cancelled: when the work
            // deadline drops this future it keeps running, so it owns the
            // reservation, the ingest slot and a worker count until it
            // ends, and shutdown waits for it like any worker.
            self.workers.fetch_add(1, Ordering::SeqCst);
            let hold = (held.take(), Arc::clone(slot), WorkerGuard(Arc::clone(self)));
            let hook = lock(&self.encode_hook).clone();
            let (built, hold) = rt::run_blocking(move || {
                if let Some(hook) = hook {
                    hook();
                }
                (builder.finish(&options), hold)
            })
            .await
            .map_err(transient)?;
            let (reservation, _slot, _worker) = hold;
            held = reservation;
            let built = built.map_err(transient)?;
            pins = Some(self.store_parts(namespace, &built.parts).await?);
            new_keys = built
                .parts
                .iter()
                .map(|part| part.meta.key.clone())
                .collect();
            manifest = manifest
                .after_ingest(Some(built.meta), through, digest, self.now_ms())
                .map_err(transient)?;
        } else if !rebuild {
            return Ok(Folded::Done);
        } else {
            manifest.published_at_ms = self.now_ms();
        }
        if let Some(published) = base {
            manifest.obsoleted.push(published.manifest_key.clone());
            if rebuild {
                let kept: HashSet<&str> = manifest.part_keys().collect();
                let retired: Vec<String> = published
                    .manifest
                    .part_keys()
                    .filter(|key| !kept.contains(key))
                    .map(str::to_owned)
                    .collect();
                manifest.obsoleted.extend(retired);
            }
        }
        self.commit(namespace, base, manifest, new_keys).await?;
        drop(pins);
        drop(held);
        Ok(Folded::Done)
    }

    /// Pins `keys` (namespace-relative) for a writer that is about to
    /// reference them: waits for deletions of them in flight, then records
    /// the pin, which voids deletions decided earlier.
    async fn pin<'k>(
        &self,
        namespace: &Namespace,
        keys: impl IntoIterator<Item = &'k str>,
    ) -> Pins {
        let prefix = namespace.namespace.prefix();
        let keys: Vec<String> = keys
            .into_iter()
            .map(|key| format!("{prefix}{key}"))
            .collect();
        loop {
            let finished = self.deletions.notified();
            tokio::pin!(finished);
            finished.as_mut().enable();
            {
                let mut guards = lock(&self.guards);
                if !keys.iter().any(|key| guards.deleting.contains(key)) {
                    let sequence = guards.next();
                    for key in &keys {
                        let count = guards.pinned.entry(key.clone()).or_default();
                        *count = count.saturating_add(1);
                        guards.last_pinned.insert(key.clone(), sequence);
                    }
                    return Pins {
                        guards: Arc::clone(&self.guards),
                        keys,
                    };
                }
            }
            finished.await;
        }
    }

    /// Pins and stores `parts`; the pins last until the caller's commit
    /// has published them or queued them for deletion. Every part has a key
    /// of its own (`manifest` module docs), so nothing is reused.
    async fn store_parts(
        &self,
        namespace: &Namespace,
        parts: &[EncodedPart],
    ) -> Result<Pins, CycleError> {
        let pins = self
            .pin(namespace, parts.iter().map(|part| part.meta.key.as_str()))
            .await;
        for part in parts {
            let object = format!("{}{}", namespace.namespace.prefix(), part.meta.key);
            namespace
                .namespace
                .put_part(part)
                .await
                .map_err(transient)?;
            self.written.insert(object, Bytes::clone(&part.bytes));
        }
        Ok(pins)
    }

    /// Publishes `manifest` on `base`. On success, schedules the delta's GC
    /// and checks the incarnation; on a lost CAS, schedules this attempt's
    /// own objects for deletion and reloads. Returns whether it published.
    async fn commit(
        &self,
        namespace: &Arc<Namespace>,
        base: Option<&Arc<PublishedKeyedManifest>>,
        manifest: KeyedManifest,
        new_keys: Vec<String>,
    ) -> Result<bool, CycleError> {
        // The manifest's key is unique to this write, so no deletion of it
        // can be queued: it needs no pin.
        let outcome = namespace
            .namespace
            .publish(base.map(Arc::as_ref), &manifest)
            .await
            .map_err(transient)?;
        let due = Instant::now()
            .checked_add(self.config.gc_grace)
            .unwrap_or_else(Instant::now);
        match outcome {
            PublishOutcome::Published(published) => {
                bump(&self.metrics.publishes, 1);
                let published = Arc::new(*published);
                self.schedule_gc(namespace, published.manifest.obsoleted.clone(), due);
                let _adopted = namespace.adopt(published);
                self.check_incarnation(namespace).await?;
                Ok(true)
            }
            PublishOutcome::Conflict { manifest_key } => {
                bump(&self.metrics.cas_conflicts, 1);
                let mut orphans = new_keys;
                orphans.push(manifest_key);
                self.schedule_gc(namespace, orphans, due);
                let _current = self.reload(namespace).await?;
                Ok(false)
            }
        }
    }

    /// The post-CAS incarnation check (§3.4 step 6).
    async fn check_incarnation(&self, namespace: &Arc<Namespace>) -> Result<(), CycleError> {
        let source = &namespace.source;
        match self
            .source
            .incarnation(&source.bucket, &source.key, source.incarnation)
            .await
        {
            Ok(IncarnationState::Present) => Ok(()),
            Ok(IncarnationState::Gone) => {
                tracing::info!(
                    bucket = %source.bucket,
                    key = %source.key,
                    incarnation = source.incarnation,
                    "keyed source incarnation is gone; deleting its namespace"
                );
                let deleted = namespace.namespace.delete_all().await.map_err(transient)?;
                tracing::debug!(deleted, "deleted keyed namespace objects");
                let id = namespace_id(source);
                let mut namespaces = lock(&self.namespaces);
                if namespaces
                    .get(&id)
                    .is_some_and(|held| Arc::ptr_eq(held, namespace))
                {
                    namespaces.remove(&id);
                }
                Err(CycleError::Transient(
                    "the source stream incarnation is gone".to_owned(),
                ))
            }
            Err(error) => {
                tracing::warn!(%error, "keyed incarnation check failed; the next publish retries");
                Ok(())
            }
        }
    }

    async fn compact_pass(&self, namespace: &Arc<Namespace>) -> Result<(), CycleError> {
        for _ in 0..COMPACTIONS_PER_PASS {
            let Some(base) = namespace.published() else {
                return Ok(());
            };
            let runs = &base.manifest.runs;
            let Some(range) = plan_compaction(runs, &self.config.policy) else {
                return Ok(());
            };
            let input_bytes = runs
                .get(range.clone())
                .unwrap_or_default()
                .iter()
                .map(super::manifest::KeyedRunMeta::bytes)
                .fold(0_u64, u64::saturating_add);
            if input_bytes > self.config.compaction_budget_bytes {
                tracing::warn!(
                    input_bytes,
                    budget = self.config.compaction_budget_bytes,
                    "keyed compaction exceeds the per-namespace budget; skipped"
                );
                return Ok(());
            }
            // Admission: a compaction slot, then its input and output
            // buffers (each at most the input size) from the budget. Both
            // wait rather than fail; a stale plan is caught by the rebase.
            let _slot = self.admission.compaction_slot().await;
            let _buffers = self.admission.reserve(input_bytes.saturating_mul(2)).await;
            if !self
                .within(
                    "compaction",
                    self.compact_one(namespace, Arc::clone(&base), range, input_bytes),
                )
                .await?
            {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Runs one planned compaction and commits it, rebasing onto newer
    /// publications. Returns whether it committed.
    async fn compact_one(
        &self,
        namespace: &Arc<Namespace>,
        base: Arc<PublishedKeyedManifest>,
        range: std::ops::Range<usize>,
        input_bytes: u64,
    ) -> Result<bool, CycleError> {
        let runs = &base.manifest.runs;
        let output = compact(&namespace.opener, runs, range, &self.config.part_options)
            .await
            .map_err(transient)?;
        bump(&self.metrics.compaction_input_bytes, input_bytes);
        bump(
            &self.metrics.compaction_output_bytes,
            output
                .output
                .parts
                .iter()
                .map(|part| part.meta.bytes)
                .fold(0_u64, u64::saturating_add),
        );
        let _pins = self.store_parts(namespace, &output.output.parts).await?;
        let new_keys: Vec<String> = output
            .output
            .parts
            .iter()
            .map(|part| part.meta.key.clone())
            .collect();
        let due = Instant::now()
            .checked_add(self.config.gc_grace)
            .unwrap_or_else(Instant::now);
        let mut attempt = Arc::clone(&base);
        for _ in 0..COMPACTION_COMMIT_ATTEMPTS {
            let Some(mut next) = attempt
                .manifest
                .after_compaction(
                    &output.inputs,
                    &output.output.meta,
                    output.into_oldest,
                    self.now_ms(),
                )
                .map_err(transient)?
            else {
                break;
            };
            next.obsoleted.push(attempt.manifest_key.clone());
            if self
                .commit(namespace, Some(&attempt), next, Vec::new())
                .await?
            {
                bump(&self.metrics.compaction_publishes, 1);
                return Ok(true);
            }
            let Some(latest) = namespace.published() else {
                break;
            };
            attempt = latest;
        }
        // Abandoned: the inputs changed, or the CAS kept losing.
        self.schedule_gc(namespace, new_keys, due);
        Ok(false)
    }

    /// Runs `work` within the per-work deadline: a stalled source or store
    /// cannot hold the namespace's worker, its slot and its admission
    /// budget forever. Dropping `work` releases them, except what a
    /// blocking encode still running holds (it keeps its reservation, slot
    /// and worker count until it ends).
    async fn within<T>(
        &self,
        what: &str,
        work: impl std::future::Future<Output = Result<T, CycleError>>,
    ) -> Result<T, CycleError> {
        let deadline = self.config.work_deadline;
        // Biased: completion and the deadline at the same instant resolve
        // the same way on every run (simulation determinism).
        tokio::select! {
            biased;
            result = work => result,
            () = rt::time::sleep(deadline) => {
                bump(&self.metrics.work_deadlines, 1);
                Err(CycleError::Transient(format!(
                    "keyed {what} exceeded its {} ms deadline",
                    deadline.as_millis()
                )))
            }
        }
    }

    fn schedule_gc(&self, namespace: &Arc<Namespace>, keys: Vec<String>, due: Instant) {
        if keys.is_empty() {
            return;
        }
        let decided = lock(&self.guards).next();
        self.requeue_gc(
            namespace,
            keys.into_iter().map(|key| (key, decided)).collect(),
            due,
        );
    }

    /// Queues deletions with the sequence at which each was decided.
    fn requeue_gc(&self, namespace: &Arc<Namespace>, keys: Vec<Decided>, due: Instant) {
        let mut gc = lock(&self.gc);
        gc.extend(keys.into_iter().map(|(key, decided)| GcItem {
            namespace: Arc::clone(namespace),
            key,
            due,
            decided,
        }));
    }

    /// Deletes due objects that the namespace's current manifest does not
    /// reference and no writer re-referenced since the deletion was
    /// decided, and forgets idle namespaces.
    async fn collect_garbage(&self) -> usize {
        let _pass = self.gc_pass.lock().await;
        let now = Instant::now();
        let due: Vec<GcItem> = {
            let mut gc = lock(&self.gc);
            let (due, later): (Vec<GcItem>, Vec<GcItem>) =
                gc.drain(..).partition(|item| item.due <= now);
            *gc = later;
            due
        };
        let mut by_namespace: Vec<(Arc<Namespace>, Vec<Decided>)> = Vec::new();
        for item in due {
            match by_namespace
                .iter_mut()
                .find(|(namespace, _)| Arc::ptr_eq(namespace, &item.namespace))
            {
                Some((_, keys)) => keys.push((item.key, item.decided)),
                None => by_namespace.push((item.namespace, vec![(item.key, item.decided)])),
            }
        }
        let mut deleted = 0_usize;
        let retry = now.checked_add(self.config.gc_tick).unwrap_or(now);
        let ttl = delete_decision_ttl(self.config.gc_grace);
        for (namespace, keys) in by_namespace {
            if self.draining(&namespace.source.bucket) {
                continue;
            }
            let mut loaded_at = Instant::now();
            let mut referenced = match self.gc_referenced(&namespace).await {
                Ok(referenced) => referenced,
                Err(error) => {
                    tracing::warn!(%error, "keyed GC cannot load CURRENT; retrying later");
                    self.requeue_gc(&namespace, keys, retry);
                    continue;
                }
            };
            let mut failed = Vec::new();
            let mut keys = keys.into_iter();
            while let Some((key, decided)) = keys.next() {
                if referenced.contains(&key) {
                    continue;
                }
                // Observe the object's age, then CURRENT when that load is
                // stale, and delete within the decision TTL of both
                // (`manifest` module docs).
                let observed = Instant::now();
                let modified = match namespace.namespace.modified_ms(&key).await {
                    Ok(Some(modified)) => modified,
                    // Gone, or of unknown age: nothing to do.
                    Ok(None) => continue,
                    Err(error) => {
                        tracing::warn!(%error, key, "keyed GC cannot stat; retrying later");
                        failed.push((key, decided));
                        continue;
                    }
                };
                let age = Duration::from_millis(self.now_ms().saturating_sub(modified));
                if let Some(young) = self
                    .config
                    .gc_grace
                    .checked_sub(age)
                    .filter(|d| !d.is_zero())
                {
                    // Not yet old (its writer may still publish it): due
                    // again once it is, unless a manifest references it.
                    let due = Instant::now().checked_add(young).unwrap_or(retry);
                    self.requeue_gc(&namespace, vec![(key, decided)], due);
                    continue;
                }
                if loaded_at.elapsed() > ttl {
                    loaded_at = Instant::now();
                    match self.gc_referenced(&namespace).await {
                        Ok(fresh) => referenced = fresh,
                        Err(error) => {
                            tracing::warn!(%error, "keyed GC cannot load CURRENT; retrying later");
                            failed.push((key, decided));
                            failed.extend(keys.by_ref());
                            break;
                        }
                    }
                    if referenced.contains(&key) {
                        continue;
                    }
                }
                if observed.elapsed() > ttl {
                    failed.push((key, decided));
                    continue;
                }
                let object = format!("{}{key}", namespace.namespace.prefix());
                {
                    let mut guards = lock(&self.guards);
                    if guards
                        .last_pinned
                        .get(&object)
                        .is_some_and(|pinned| *pinned >= decided)
                    {
                        // Re-referenced since: its new life schedules its own
                        // deletion.
                        continue;
                    }
                    if guards.pinned.contains_key(&object) {
                        failed.push((key, decided));
                        continue;
                    }
                    guards.deleting.insert(object.clone());
                }
                let result = namespace.namespace.delete(&key).await;
                lock(&self.guards).deleting.remove(&object);
                self.deletions.notify_waiters();
                match result {
                    Ok(()) => deleted = deleted.saturating_add(1),
                    Err(error) => {
                        tracing::warn!(%error, key, "keyed GC delete failed; retrying later");
                        failed.push((key, decided));
                    }
                }
            }
            self.requeue_gc(&namespace, failed, retry);
        }
        // A pin matters only to deletions decided before it.
        let oldest = lock(&self.gc).iter().map(|item| item.decided).min();
        lock(&self.guards)
            .last_pinned
            .retain(|_, pinned| oldest.is_some_and(|oldest| *pinned >= oldest));
        self.forget_idle();
        bump(
            &self.metrics.gc_deleted,
            u64::try_from(deleted).unwrap_or(u64::MAX),
        );
        deleted
    }

    /// The objects a reader may still use: what `CURRENT` references
    /// (adopting it when newer), plus every manifest written within the
    /// grace period and what it references (IX2): a reader may still hold
    /// a recent manifest other than `CURRENT`.
    async fn gc_referenced(
        &self,
        namespace: &Arc<Namespace>,
    ) -> Result<HashSet<String>, IndexError> {
        let started = Instant::now();
        let Some(published) = namespace.namespace.load().await? else {
            return Ok(HashSet::new());
        };
        namespace.mark_checked(started);
        let mut referenced: HashSet<String> =
            published.manifest.part_keys().map(str::to_owned).collect();
        referenced.insert(published.manifest_key.clone());
        self.adopt_loaded(namespace, published);
        referenced.extend(
            namespace
                .namespace
                .protected_now(self.now_ms(), self.config.gc_grace)
                .await?,
        );
        Ok(referenced)
    }

    fn forget_idle(&self) {
        let idle = self.config.idle_namespace;
        let mut namespaces = lock(&self.namespaces);
        namespaces.retain(|_, namespace| {
            Arc::strong_count(namespace) > 1
                || lock(&namespace.work).running
                || lock(&namespace.last_used).elapsed() < idle
        });
    }

    async fn drain(self: &Arc<Self>, bucket: &str) {
        {
            let mut buckets = lock(&self.buckets);
            let state = buckets.entry(bucket.to_owned()).or_default();
            state.drained_until = Instant::now().checked_add(self.config.drain_hold);
        }
        let draining: Vec<Arc<Namespace>> = lock(&self.namespaces)
            .values()
            .filter(|namespace| namespace.source.bucket == bucket)
            .cloned()
            .collect();
        for namespace in &draining {
            namespace.set_status(Status::Unavailable("the bucket is draining".to_owned()));
        }
        drop(draining);
        loop {
            let idle = self.bucket_idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            let busy = lock(&self.buckets)
                .get(bucket)
                .map_or(0, |state| state.busy);
            if busy == 0 {
                break;
            }
            idle.await;
        }
        lock(&self.namespaces).retain(|(namespace_bucket, _, _), _| namespace_bucket != bucket);
        lock(&self.gc).retain(|item| item.namespace.source.bucket != bucket);
    }
}
