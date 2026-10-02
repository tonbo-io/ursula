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
//! queue.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
#[cfg(not(madsim))]
use std::time::SystemTime;
#[cfg(not(madsim))]
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use parquet::arrow::async_reader::AsyncFileReader;
use tokio::sync::Notify;
use tokio::sync::watch;
use tokio::time::Instant;

use super::fold::RangeQuery;
use super::manifest::KEYED_PROJECTION_FORMAT;
use super::manifest::KeyedManifest;
use super::manifest::KeyedNamespace;
use super::manifest::KeyedPartMeta;
use super::manifest::KeyedSource;
use super::manifest::PublishOutcome;
use super::manifest::PublishedKeyedManifest;
use super::merge::KeyedPage;
use super::merge::get;
use super::merge::read_range;
use super::part::BytesReader;
use super::part::EncodedPart;
use super::part::PartOpener;
use super::part::PartOptions;
use super::part::StorePartOpener;
use super::run::CompactionPolicy;
use super::run::RunBuilder;
use super::run::compact;
use super::run::plan_compaction;
use super::source::IncarnationState;
use super::source::KeyedSourceClient;
use super::source::SourceError;
use crate::EventIndexCache;
use crate::IndexError;
use crate::object_store::ObjectStore;
use crate::object_store::digest;

/// Compactions attempted after one publication.
const COMPACTIONS_PER_PASS: usize = 4;
/// CAS attempts of one compaction's rebased manifest edit.
const COMPACTION_COMMIT_ATTEMPTS: usize = 3;
/// Pause before retrying a cycle that made no progress (a lagging replica).
const NO_PROGRESS_BACKOFF: Duration = Duration::from_millis(100);

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
    /// A namespace with no activity for this long is dropped from memory.
    pub idle_namespace: Duration,
    /// How long a drained bucket stays blocked (the purge erases it
    /// meanwhile; a recreated bucket is served again afterwards).
    pub drain_hold: Duration,
    /// Part encoding and read knobs.
    pub part_options: PartOptions,
    /// Size-tiered compaction policy.
    pub policy: CompactionPolicy,
    /// Projection format of the namespaces this pod reads and writes
    /// (`v{fmt}/`). A pod at another format builds its own namespaces from
    /// record 0 next to the served ones: the blue/green rebuild (§6.1 U20).
    pub projection_format: u32,
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
            idle_namespace: Duration::from_secs(600),
            drain_hold: Duration::from_secs(600),
            part_options: PartOptions::default(),
            policy: CompactionPolicy::default(),
            projection_format: KEYED_PROJECTION_FORMAT,
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

/// Wall-clock milliseconds for `published_at_ms` and the publish interval;
/// the epoch under the simulator, which has no wall clock.
#[cfg(not(madsim))]
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(madsim)]
fn now_ms() -> u64 {
    0
}

/// blake3 of a record's stored bytes: its message text plus the LF.
fn stored_digest(text: &str) -> String {
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
    /// clears transient and rebuilding states.
    fn adopt(&self, published: Arc<PublishedKeyedManifest>) {
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
        });
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
    parts: HashMap<String, Bytes>,
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
        state.parts.insert(key, bytes);
        while state.bytes > self.capacity {
            let Some(oldest) = state.order.pop_front() else {
                break;
            };
            if let Some(evicted) = state.parts.remove(&oldest) {
                state.bytes = state.bytes.saturating_sub(evicted.len());
            }
        }
    }

    fn get(&self, key: &str) -> Option<Bytes> {
        lock(&self.state).parts.get(key).cloned()
    }
}

/// Reads parts from the write cache when present, else through the verified
/// object-store reader.
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
    ) -> BoxFuture<'a, Result<Box<dyn AsyncFileReader>, IndexError>> {
        async move {
            let object_key = format!("{}{}", self.prefix, part.key);
            if let Some(bytes) = self.written.get(&object_key) {
                let tail = usize::try_from(part.data_bytes)
                    .ok()
                    .and_then(|start| bytes.get(start..));
                let size_matches = u64::try_from(bytes.len()).ok() == Some(part.bytes);
                if size_matches && tail.is_some_and(|tail| digest(tail) == part.tail_hash) {
                    return Ok(Box::new(BytesReader(bytes)) as Box<dyn AsyncFileReader>);
                }
                return Err(IndexError::ObjectHashMismatch(object_key));
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

struct GcItem {
    namespace: Arc<Namespace>,
    key: String,
    due: Instant,
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
    source: KeyedSourceClient,
    cache: Option<EventIndexCache>,
    written: Arc<WrittenParts>,
    config: KeyedEngineConfig,
    namespaces: Mutex<HashMap<NamespaceId, Arc<Namespace>>>,
    buckets: Mutex<HashMap<String, BucketState>>,
    bucket_idle: Notify,
    waiters: AtomicUsize,
    gc: Mutex<Vec<GcItem>>,
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
    /// and reading sources through `source`. `cache` is a serving cache for
    /// verified part ranges.
    pub fn new(
        store: ObjectStore,
        source: KeyedSourceClient,
        cache: Option<EventIndexCache>,
        config: KeyedEngineConfig,
    ) -> Self {
        let written = Arc::new(WrittenParts {
            capacity: config.write_cache_bytes,
            state: Mutex::new(WrittenState::default()),
        });
        Self {
            inner: Arc::new(Inner {
                store,
                source,
                cache,
                written,
                config,
                namespaces: Mutex::new(HashMap::new()),
                buckets: Mutex::new(HashMap::new()),
                bucket_idle: Notify::new(),
                waiters: AtomicUsize::new(0),
                gc: Mutex::new(Vec::new()),
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

    /// Deletes due garbage now; returns the number of objects deleted.
    pub async fn collect_garbage(&self) -> usize {
        self.inner.collect_garbage().await
    }

    /// Runs garbage collection every `gc_tick` until `shutdown` turns true.
    pub async fn run_maintenance(&self, mut shutdown: watch::Receiver<bool>) {
        loop {
            tokio::select! {
                () = tokio::time::sleep(self.inner.config.gc_tick) => {}
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
        let namespace = KeyedNamespace::with_format(
            self.store.clone(),
            source.clone(),
            self.config.projection_format,
        );
        let mut store = namespace.opener();
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
        if let Some(published) = namespace.namespace.load().await? {
            let published = Arc::new(published);
            let remaining = published
                .manifest
                .published_at_ms
                .saturating_add(u64::try_from(self.config.gc_grace.as_millis()).unwrap_or(u64::MAX))
                .saturating_sub(now_ms());
            let due = Instant::now()
                .checked_add(Duration::from_millis(remaining))
                .unwrap_or_else(Instant::now);
            self.schedule_gc(namespace, published.manifest.obsoleted.clone(), due);
            namespace.adopt(published);
        }
        *loaded = true;
        Ok(())
    }

    async fn reload(
        &self,
        namespace: &Namespace,
    ) -> Result<Option<Arc<PublishedKeyedManifest>>, CycleError> {
        if let Some(published) = namespace.namespace.load().await.map_err(transient)? {
            namespace.adopt(Arc::new(published));
        }
        Ok(namespace.published())
    }

    async fn read(self: &Arc<Self>, request: KeyedReadRequest) -> KeyedReadOutcome {
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
        if let Err(error) = self.ensure_loaded(&namespace).await {
            tracing::warn!(%error, bucket, key = %request.source.key, "keyed namespace load failed");
            return KeyedReadOutcome::Unavailable("keyed state cannot be loaded".to_owned());
        }
        let mut receiver = namespace.view.subscribe();
        let view = Arc::clone(&receiver.borrow_and_update());
        if view.invalid {
            // Restart the rebuild if its worker stopped (a transient failure).
            self.request_work(&namespace, None, true);
            return match &view.status {
                Status::Failed(reason) => KeyedReadOutcome::Failed(reason.clone()),
                _ => KeyedReadOutcome::Unavailable("keyed state is being rebuilt".to_owned()),
            };
        }
        if view.through() > request.source_next {
            self.request_work(&namespace, None, true);
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
        self.request_work(
            &namespace,
            Some((wanted, request.source_next, deadline)),
            false,
        );
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
            tokio::select! {
                changed = receiver.changed() => {
                    if changed.is_err() {
                        return KeyedReadOutcome::Unavailable("keyed namespace closed".to_owned());
                    }
                }
                () = tokio::time::sleep_until(deadline) => {
                    let view = Arc::clone(&receiver.borrow());
                    if matches!(view.status, Status::Ready) && view.through() >= wanted {
                        return self.serve(&namespace, &view, &request.selection).await;
                    }
                    return KeyedReadOutcome::NotYet { through: view.through() };
                }
            }
        }
    }

    async fn serve(
        &self,
        namespace: &Namespace,
        view: &View,
        selection: &Selection,
    ) -> KeyedReadOutcome {
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
        };
        match page {
            Ok(page) => KeyedReadOutcome::Rows { through, page },
            Err(error) => {
                tracing::warn!(
                    %error,
                    bucket = %namespace.source.bucket,
                    key = %namespace.source.key,
                    "keyed-state read failed"
                );
                KeyedReadOutcome::Unavailable("keyed state cannot be read".to_owned())
            }
        }
    }

    /// Registers a want (or a re-validation) and starts the namespace's
    /// worker unless one runs (single flight).
    fn request_work(
        self: &Arc<Self>,
        namespace: &Arc<Namespace>,
        want: Option<(u64, u64, Instant)>,
        verify: bool,
    ) {
        let mut work = lock(&namespace.work);
        if let Some((record, next, until)) = want {
            work.want_record = work.want_record.max(record);
            work.want_next = work.want_next.max(next);
            work.want_until = Some(work.want_until.map_or(until, |current| current.max(until)));
            namespace.clear_unavailable();
        }
        work.verify |= verify;
        if work.running {
            return;
        }
        work.running = true;
        drop(work);
        let inner = Arc::clone(self);
        let namespace = Arc::clone(namespace);
        tokio::spawn(async move {
            inner.run_worker(namespace).await;
        });
    }

    async fn run_worker(self: Arc<Self>, namespace: Arc<Namespace>) {
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
            match self.cycle(&namespace, want_next, verify).await {
                Ok(true) => {}
                Ok(false) => tokio::time::sleep(NO_PROGRESS_BACKOFF).await,
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

    /// Waits for the publish interval, reloads `CURRENT` and ingests up to
    /// `want_next`. Returns whether `D` advanced (or a re-validation ran).
    async fn cycle(
        &self,
        namespace: &Arc<Namespace>,
        want_next: u64,
        verify: bool,
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
                .saturating_sub(now_ms())
                .min(interval);
            if wait > 0 {
                tokio::time::sleep(Duration::from_millis(wait)).await;
            }
        }
        let base = self.reload(namespace).await?;
        let through = base
            .as_ref()
            .map_or(0, |published| published.manifest.through_record);
        if !verify && through >= lock(&namespace.work).want_record {
            return Ok(true);
        }
        self.ingest(namespace, base, want_next.max(through)).await?;
        Ok(verify || namespace.through() > before)
    }

    async fn ingest(
        &self,
        namespace: &Arc<Namespace>,
        base: Option<Arc<PublishedKeyedManifest>>,
        target: u64,
    ) -> Result<(), CycleError> {
        let Folded::Discontinuity(reason) =
            self.fold(namespace, base.as_ref(), false, target).await?
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
        match self.fold(namespace, base.as_ref(), true, target).await? {
            Folded::Done => Ok(()),
            Folded::Discontinuity(reason) => Err(CycleError::Transient(reason)),
        }
    }

    /// Reads `[D−1, target)` (from 0 for a rebuild or `D = 0`), checks the
    /// continuity record, folds the rest into one run and publishes it.
    async fn fold(
        &self,
        namespace: &Arc<Namespace>,
        base: Option<&Arc<PublishedKeyedManifest>>,
        rebuild: bool,
        target: u64,
    ) -> Result<Folded, CycleError> {
        let (d, mut expected) = match base {
            Some(published) if !rebuild => (
                published.manifest.through_record,
                published.manifest.through_digest.clone(),
            ),
            _ => (0, None),
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
        let bucket = namespace.source.bucket.as_str();
        let key = namespace.source.key.as_str();
        'pages: loop {
            if expected.is_none() && cursor >= target {
                break;
            }
            if expected.is_none() && bytes >= self.config.max_ingest_bytes {
                truncated = true;
                break;
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
                if bytes >= self.config.max_ingest_bytes {
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
        if let Some(digest) = last_digest {
            let through = builder.next_record();
            let options = self.config.part_options;
            let built = tokio::task::spawn_blocking(move || builder.finish(&options))
                .await
                .map_err(transient)?
                .map_err(transient)?;
            self.store_parts(namespace, &built.parts).await?;
            new_keys = built
                .parts
                .iter()
                .map(|part| part.meta.key.clone())
                .collect();
            manifest = manifest
                .after_ingest(Some(built.meta), through, digest, now_ms())
                .map_err(transient)?;
        } else if !rebuild {
            return Ok(Folded::Done);
        } else {
            manifest.published_at_ms = now_ms();
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
        Ok(Folded::Done)
    }

    async fn store_parts(
        &self,
        namespace: &Namespace,
        parts: &[EncodedPart],
    ) -> Result<(), CycleError> {
        for part in parts {
            namespace
                .namespace
                .put_part(part)
                .await
                .map_err(transient)?;
            self.written.insert(
                format!("{}{}", namespace.namespace.prefix(), part.meta.key),
                Bytes::clone(&part.bytes),
            );
        }
        Ok(())
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
                let published = Arc::new(*published);
                self.schedule_gc(namespace, published.manifest.obsoleted.clone(), due);
                namespace.adopt(published);
                self.check_incarnation(namespace).await?;
                Ok(true)
            }
            PublishOutcome::Conflict { manifest_key } => {
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
            let output = compact(&namespace.opener, runs, range, &self.config.part_options)
                .await
                .map_err(transient)?;
            self.store_parts(namespace, &output.output.parts).await?;
            let new_keys: Vec<String> = output
                .output
                .parts
                .iter()
                .map(|part| part.meta.key.clone())
                .collect();
            let due = Instant::now()
                .checked_add(self.config.gc_grace)
                .unwrap_or_else(Instant::now);
            let mut attempt = base;
            let mut committed = false;
            for _ in 0..COMPACTION_COMMIT_ATTEMPTS {
                let Some(mut next) = attempt
                    .manifest
                    .after_compaction(
                        &output.inputs,
                        &output.output.meta,
                        output.into_oldest,
                        now_ms(),
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
                    committed = true;
                    break;
                }
                let Some(latest) = namespace.published() else {
                    break;
                };
                attempt = latest;
            }
            if !committed {
                // Abandoned: the inputs changed, or the CAS kept losing.
                self.schedule_gc(namespace, new_keys, due);
                return Ok(());
            }
        }
        Ok(())
    }

    fn schedule_gc(&self, namespace: &Arc<Namespace>, keys: Vec<String>, due: Instant) {
        if keys.is_empty() {
            return;
        }
        let mut gc = lock(&self.gc);
        gc.extend(keys.into_iter().map(|key| GcItem {
            namespace: Arc::clone(namespace),
            key,
            due,
        }));
    }

    /// Deletes due objects that the namespace's current manifest does not
    /// reference, and forgets idle namespaces.
    async fn collect_garbage(&self) -> usize {
        let now = Instant::now();
        let due: Vec<GcItem> = {
            let mut gc = lock(&self.gc);
            let (due, later): (Vec<GcItem>, Vec<GcItem>) =
                gc.drain(..).partition(|item| item.due <= now);
            *gc = later;
            due
        };
        let mut by_namespace: Vec<(Arc<Namespace>, Vec<String>)> = Vec::new();
        for item in due {
            match by_namespace
                .iter_mut()
                .find(|(namespace, _)| Arc::ptr_eq(namespace, &item.namespace))
            {
                Some((_, keys)) => keys.push(item.key),
                None => by_namespace.push((item.namespace, vec![item.key])),
            }
        }
        let mut deleted = 0_usize;
        let retry = now.checked_add(self.config.gc_tick).unwrap_or(now);
        for (namespace, keys) in by_namespace {
            if self.draining(&namespace.source.bucket) {
                continue;
            }
            let referenced: HashSet<String> = match namespace.namespace.load().await {
                Ok(Some(published)) => {
                    let mut referenced: HashSet<String> =
                        published.manifest.part_keys().map(str::to_owned).collect();
                    referenced.insert(published.manifest_key.clone());
                    namespace.adopt(Arc::new(published));
                    referenced
                }
                Ok(None) => HashSet::new(),
                Err(error) => {
                    tracing::warn!(%error, "keyed GC cannot load CURRENT; retrying later");
                    self.schedule_gc(&namespace, keys, retry);
                    continue;
                }
            };
            let mut failed = Vec::new();
            for key in keys {
                if referenced.contains(&key) {
                    continue;
                }
                match namespace.namespace.delete(&key).await {
                    Ok(()) => deleted = deleted.saturating_add(1),
                    Err(error) => {
                        tracing::warn!(%error, key, "keyed GC delete failed; retrying later");
                        failed.push(key);
                    }
                }
            }
            self.schedule_gc(&namespace, failed, retry);
        }
        self.forget_idle();
        deleted
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
