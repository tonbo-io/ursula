use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::Context;
use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::routing::post;
use axum::routing::put;
use chrono::DateTime;
use clap::Args;
use reqwest::Url;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;
use ursula_observability::serve::shutdown_signal;

use crate::EventIndex;
use crate::EventIndexCache;
use crate::EventIndexConfig;
use crate::Extractor;
use crate::ExtractorConfig;
use crate::FsObjectStore;
use crate::IndexCatalog;
use crate::IndexError;
use crate::IndexRegistration;
use crate::IndexStatus;
use crate::ObjectStore;
use crate::S3ObjectStore;
use crate::S3ObjectStoreConfig;
use crate::SourceClient;
use crate::catalog::StartPosition;
use crate::source::MAX_MESSAGE_BYTES;
use crate::source::ReadLimits;
use crate::source::SegmentRead;
use crate::source::SourceFormat;
use crate::source::SourceHead;
use crate::store::Coverage;
use crate::store::IndexBase;
use crate::store::MatchMode;
use crate::store::QueryCursor;
use crate::store::QueryRequest;
use crate::store::SkipCounts;
use crate::store::SourceBinding;
use crate::store::offset_token;
use crate::store::parse_offset_token;

#[derive(Debug, Args)]
pub struct IndexerArgs {
    #[arg(long)]
    stream_url: Option<Url>,
    #[command(flatten)]
    backend: BackendArgs,
    #[arg(long, default_value = "event-index")]
    s3_prefix: String,
    #[arg(long)]
    s3_region: Option<String>,
    #[arg(long)]
    s3_endpoint: Option<String>,
    #[arg(long)]
    cache_dir: PathBuf,
    #[arg(long, default_value_t = 2 * 1024 * 1024 * 1024_u64)]
    cache_max_bytes: u64,
    #[arg(long, default_value_t = 512 * 1024 * 1024_u64)]
    maintenance_cache_max_bytes: u64,
    #[arg(long, default_value = "127.0.0.1:4493")]
    listen: SocketAddr,
    /// Maximum messages (entries plus skips) in one committed segment, and
    /// so entries in one uncompacted part.
    #[arg(long, default_value_t = 4_096)]
    flush_entries: usize,
    #[arg(long, default_value_t = 16_384)]
    row_group_entries: usize,
    /// Source bytes read before a segment is committed. A shorter segment
    /// waits for `--tail-flush-interval-ms`, unless it follows one that
    /// stopped at `--flush-entries`.
    #[arg(long, default_value_t = 32 * 1024 * 1024_u64)]
    segment_bytes: u64,
    #[arg(long, default_value_t = 4)]
    worker_concurrency: usize,
    #[arg(long, default_value_t = 60_000)]
    segment_lease_ms: u64,
    #[arg(long, default_value = "ursula-indexer-local")]
    worker_id: String,
    #[arg(long, default_value_t = 250)]
    poll_interval_ms: u64,
    /// Maximum delay before publishing an incomplete tail segment. Full
    /// segments are still claimed immediately.
    #[arg(long, default_value_t = 5_000)]
    tail_flush_interval_ms: u64,
    #[arg(long = "compact-parts", default_value_t = 8)]
    compaction_fan_in: usize,
    #[arg(long, default_value_t = 1_000_000)]
    compaction_max_entries: u64,
    #[arg(long, default_value_t = 300_000)]
    maintenance_lease_ms: u64,
    #[arg(long, default_value_t = 3_600)]
    gc_interval_seconds: u64,
    #[arg(long, default_value_t = 86_400)]
    gc_grace_seconds: u64,
    #[arg(long, default_value_t = 8)]
    gc_retain_generations: u64,
    #[arg(long, default_value_t = 1_000)]
    maintenance_interval_ms: u64,
    /// Single-source mode: the top-level member holding the event time.
    #[arg(long, conflicts_with = "extract")]
    timestamp_field: Option<String>,
    /// Single-source mode: an extractor as JSON,
    /// `{"each":…,"time":[…],"end":[…],"unit":…}`.
    #[arg(long)]
    extract: Option<String>,
    /// Single-source mode: start a new index at the `retained` offset or at
    /// the `tail`. A recreated source is reindexed from its retained offset.
    #[arg(long, default_value = "retained")]
    start: String,
}

#[derive(Debug, Args)]
#[group(required = true, multiple = false)]
struct BackendArgs {
    #[arg(long)]
    object_dir: Option<PathBuf>,
    #[arg(long)]
    s3_bucket: Option<String>,
}

/// Where authoritative index objects live; opens the base store or one
/// namespaced store per registered source.
#[derive(Clone)]
enum StoreTarget {
    Fs {
        root: PathBuf,
    },
    S3 {
        bucket: String,
        root: String,
        region: Option<String>,
        endpoint: Option<String>,
    },
}

impl StoreTarget {
    fn from_args(args: &IndexerArgs) -> anyhow::Result<Self> {
        if let Some(object_dir) = &args.backend.object_dir {
            return Ok(Self::Fs {
                root: object_dir.clone(),
            });
        }
        let bucket = args
            .backend
            .s3_bucket
            .clone()
            .context("--s3-bucket is required without --object-dir")?;
        Ok(Self::S3 {
            bucket,
            root: args.s3_prefix.clone(),
            region: args.s3_region.clone(),
            endpoint: args.s3_endpoint.clone(),
        })
    }

    fn open(&self, suffix: &str) -> Result<ObjectStore, IndexError> {
        match self {
            Self::Fs { root } => {
                let root = if suffix.is_empty() {
                    root.clone()
                } else {
                    root.join(suffix)
                };
                Ok(FsObjectStore::new(root)?.into())
            }
            Self::S3 {
                bucket,
                root,
                region,
                endpoint,
            } => Ok(S3ObjectStore::new(S3ObjectStoreConfig {
                bucket: bucket.clone(),
                root: join_object_prefix(root, suffix),
                region: region.clone(),
                endpoint: endpoint.clone(),
            })?
            .into()),
        }
    }
}

fn join_object_prefix(root: &str, suffix: &str) -> String {
    let root = root.trim_matches('/');
    if root.is_empty() {
        suffix.to_owned()
    } else if suffix.is_empty() {
        root.to_owned()
    } else {
        format!("{root}/{suffix}")
    }
}

#[derive(Clone, Copy)]
struct MaintenanceConfig {
    interval: Duration,
    compaction_fan_in: usize,
    compaction_max_entries: u64,
    lease_ms: u64,
    gc_interval: Duration,
    gc_grace: Duration,
    gc_retain_generations: u64,
}

impl MaintenanceConfig {
    fn from_args(args: &IndexerArgs) -> Self {
        Self {
            interval: Duration::from_millis(args.maintenance_interval_ms),
            compaction_fan_in: args.compaction_fan_in,
            compaction_max_entries: args.compaction_max_entries,
            lease_ms: args.maintenance_lease_ms,
            gc_interval: Duration::from_secs(args.gc_interval_seconds),
            gc_grace: Duration::from_secs(args.gc_grace_seconds),
            gc_retain_generations: args.gc_retain_generations,
        }
    }
}

/// How one worker reads and claims source segments.
#[derive(Clone, Debug)]
struct WorkerParams {
    worker_id: String,
    lease_ms: u64,
    limits: ReadLimits,
}

impl WorkerParams {
    fn from_args(args: &IndexerArgs) -> Self {
        Self {
            worker_id: args.worker_id.clone(),
            lease_ms: args.segment_lease_ms,
            limits: ReadLimits {
                segment_bytes: args.segment_bytes,
                max_entries: args.flush_entries,
                max_message_bytes: MAX_MESSAGE_BYTES,
            },
        }
    }
}

#[derive(Clone)]
struct SingleAppState {
    index: Arc<Mutex<EventIndex>>,
}

#[derive(Clone)]
struct PoolIndexSettings {
    serving_cache: EventIndexCache,
    maintenance_cache: EventIndexCache,
    row_group_entries: usize,
}

struct PoolIndex {
    namespace: String,
    serving: Arc<Mutex<EventIndex>>,
    maintenance: Mutex<EventIndex>,
}

#[derive(Clone)]
struct PoolState {
    catalog: IndexCatalog,
    backend: StoreTarget,
    settings: PoolIndexSettings,
    indexes: Arc<RwLock<HashMap<String, Arc<PoolIndex>>>>,
    http: reqwest::Client,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterIndexRequest {
    stream_url: String,
    #[serde(default)]
    extract: Option<ExtractorConfig>,
    /// Legacy form of `extract`: one top-level member.
    #[serde(default)]
    timestamp_field: Option<String>,
    #[serde(default)]
    start: StartPosition,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventQuery {
    from: String,
    until: String,
    #[serde(default, rename = "match")]
    match_mode: Option<String>,
    after: Option<String>,
    through: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    1_000
}

#[derive(Debug, Serialize)]
struct StatusBody {
    status: IndexStatus,
    source: SourceBinding,
    coverage: Coverage,
    skipped: SkipCounts,
    parts: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    restarted_from_incarnation: Option<String>,
}

impl StatusBody {
    fn read(index: &EventIndex, restarted_from_incarnation: Option<String>) -> Self {
        Self {
            status: index.status().clone(),
            source: index.source().clone(),
            coverage: index.coverage(),
            skipped: index.skipped(),
            parts: index.part_count(),
            restarted_from_incarnation,
        }
    }
}

#[derive(Debug)]
struct ApiError(IndexError);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0 {
            IndexError::InvalidQuery
            | IndexError::InvalidExtractor(_)
            | IndexError::InvalidConfig(_) => StatusCode::BAD_REQUEST,
            IndexError::InvalidSourceResponse(_) => StatusCode::UNPROCESSABLE_ENTITY,
            IndexError::Blocked { .. }
            | IndexError::SourceGone
            | IndexError::CannotResume(_)
            | IndexError::RegistrationConflict(_)
            | IndexError::NamespaceRetired(_) => StatusCode::CONFLICT,
            IndexError::UnknownIndex(_) => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (
            status,
            Json(serde_json::json!({ "error": self.0.to_string() })),
        )
            .into_response()
    }
}

pub async fn run(args: IndexerArgs) -> anyhow::Result<()> {
    let _observability =
        ursula_observability::init(ursula_observability::InitOptions::new("ursula-indexer"));
    run_until(args, shutdown_signal()).await
}

/// Run the indexer until `shutdown` resolves. Used by `run` with the process
/// signal handler and by in-process tests.
pub async fn run_until(
    args: IndexerArgs,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    validate_args(&args)?;
    match args.stream_url.clone() {
        Some(stream_url) => run_single(args, stream_url, shutdown).await,
        None => run_pool(args, shutdown).await,
    }
}

fn validate_args(args: &IndexerArgs) -> anyhow::Result<()> {
    if args.compaction_fan_in < 2 {
        anyhow::bail!("--compact-parts must be at least 2");
    }
    let maximum_l0_entries = u64::try_from(args.flush_entries)
        .ok()
        .and_then(|entries| {
            u64::try_from(args.compaction_fan_in)
                .ok()
                .and_then(|fan_in| entries.checked_mul(fan_in))
        })
        .context("flush entries times compaction fan-in overflowed")?;
    if maximum_l0_entries > args.compaction_max_entries {
        anyhow::bail!("--compaction-max-entries must cover --flush-entries times --compact-parts");
    }
    if args.gc_interval_seconds == 0
        || args.gc_retain_generations == 0
        || args.maintenance_interval_ms == 0
    {
        anyhow::bail!(
            "maintenance interval, GC interval, and retained generations must be positive"
        );
    }
    if args.segment_bytes == 0
        || args.flush_entries == 0
        || args.worker_concurrency == 0
        || args.segment_lease_ms == 0
        || args.worker_id.is_empty()
    {
        anyhow::bail!(
            "segment bytes, flush entries, worker concurrency, lease duration, and worker id must be valid"
        );
    }
    let _start = parse_start(&args.start)?;
    Ok(())
}

fn parse_start(value: &str) -> anyhow::Result<StartPosition> {
    match value {
        "retained" => Ok(StartPosition::Retained),
        "tail" => Ok(StartPosition::Tail),
        _ => anyhow::bail!("--start must be `retained` or `tail`"),
    }
}

fn single_extractor(args: &IndexerArgs) -> anyhow::Result<Extractor> {
    if let Some(extract) = &args.extract {
        let config: ExtractorConfig =
            serde_json::from_str(extract).context("--extract is not a valid extractor")?;
        return Ok(Extractor::new(config)?);
    }
    Ok(Extractor::timestamp_field(
        args.timestamp_field.as_deref().unwrap_or("captured_at"),
    )?)
}

fn start_offset(start: StartPosition, head: &SourceHead) -> u64 {
    match start {
        StartPosition::Retained => head.retained_offset,
        StartPosition::Tail => head.next_offset,
    }
}

/// Compare incarnations for equality only. An unknown incarnation on either
/// side never triggers a restart.
fn incarnation_changed(known: Option<&str>, observed: Option<&str>) -> bool {
    matches!((known, observed), (Some(known), Some(observed)) if known != observed)
}

async fn run_single(
    args: IndexerArgs,
    stream_url: Url,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let extractor = single_extractor(&args)?;
    let start = parse_start(&args.start)?;
    let source = SourceClient::new(reqwest::Client::new(), stream_url.clone());
    let head = source
        .head()
        .await
        .context("HEAD the source stream")?
        .context("the source stream does not exist")?;
    head.readable_format().context("HEAD the source stream")?;
    let base = IndexBase {
        offset: start_offset(start, &head),
        incarnation: head.incarnation.clone(),
    };
    let mut config = EventIndexConfig::new(stream_url.to_string(), extractor);
    config.row_group_entries = args.row_group_entries;
    let store = StoreTarget::from_args(&args)?
        .open("")
        .context("open object store")?;
    let index = EventIndex::open(
        store.clone(),
        EventIndexCache::serving(&args.cache_dir, args.cache_max_bytes)?,
        config.clone(),
        base.clone(),
    )
    .await
    .context("open event index")?;
    let maintenance_index = EventIndex::open(
        store,
        EventIndexCache::maintenance(
            args.cache_dir.join("maintenance"),
            args.maintenance_cache_max_bytes,
        )?,
        config,
        base,
    )
    .await
    .context("open event index")?;
    let index = Arc::new(Mutex::new(index));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let sync_task = tokio::spawn(sync_loop(
        source,
        Arc::clone(&index),
        WorkerParams::from_args(&args),
        Duration::from_millis(args.poll_interval_ms),
        Duration::from_millis(args.tail_flush_interval_ms),
        shutdown_rx.clone(),
    ));
    let maintenance_task = tokio::spawn(maintenance_loop(
        maintenance_index,
        MaintenanceConfig::from_args(&args),
        shutdown_rx,
    ));

    tracing::info!(
        listen = %args.listen,
        stream_url = %stream_url,
        cache_dir = %args.cache_dir.display(),
        s3_bucket = args
            .backend
            .s3_bucket
            .as_deref()
            .unwrap_or("filesystem-dev-backend"),
        "event indexer starting"
    );
    serve(build_router(index), args.listen, shutdown_tx, shutdown).await?;
    sync_task.await.context("join source sync loop")??;
    maintenance_task
        .await
        .context("join event index maintenance loop")??;
    Ok(())
}

impl PoolState {
    async fn ensure_index(
        &self,
        registration: &IndexRegistration,
    ) -> Result<Arc<PoolIndex>, IndexError> {
        let namespace = registration.namespace()?;
        if let Some(index) = self.indexes.read().await.get(&registration.id)
            && index.namespace == namespace
        {
            return Ok(Arc::clone(index));
        }
        let mut config = EventIndexConfig::new(
            registration.stream_url.clone(),
            Extractor::new(registration.extract.clone())?,
        );
        config.row_group_entries = self.settings.row_group_entries;
        let base = IndexBase {
            offset: registration.indexed_from_offset,
            incarnation: registration.incarnation.clone(),
        };
        let store = self.backend.open(&format!("indexes/{namespace}"))?;
        let serving = EventIndex::open(
            store.clone(),
            self.settings.serving_cache.clone(),
            config.clone(),
            base.clone(),
        )
        .await?;
        let maintenance =
            EventIndex::open(store, self.settings.maintenance_cache.clone(), config, base).await?;
        let index = Arc::new(PoolIndex {
            namespace: namespace.clone(),
            serving: Arc::new(Mutex::new(serving)),
            maintenance: Mutex::new(maintenance),
        });
        let mut indexes = self.indexes.write().await;
        let entry = indexes
            .entry(registration.id.clone())
            .or_insert_with(|| Arc::clone(&index));
        if entry.namespace != namespace {
            *entry = Arc::clone(&index);
        }
        Ok(Arc::clone(entry))
    }
}

async fn run_pool(
    args: IndexerArgs,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let backend = StoreTarget::from_args(&args)?;
    let catalog = IndexCatalog::new(backend.open("").context("open object store")?);
    let state = PoolState {
        catalog,
        backend,
        settings: PoolIndexSettings {
            serving_cache: EventIndexCache::serving(
                args.cache_dir.join("serving"),
                args.cache_max_bytes,
            )?,
            maintenance_cache: EventIndexCache::maintenance(
                args.cache_dir.join("maintenance"),
                args.maintenance_cache_max_bytes,
            )?,
            row_group_entries: args.row_group_entries,
        },
        indexes: Arc::new(RwLock::new(HashMap::new())),
        http: reqwest::Client::new(),
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker = tokio::spawn(pool_worker_loop(
        state.clone(),
        WorkerParams::from_args(&args),
        args.worker_concurrency,
        Duration::from_millis(args.poll_interval_ms),
        Duration::from_millis(args.tail_flush_interval_ms),
        shutdown_rx.clone(),
    ));
    let maintenance = tokio::spawn(pool_maintenance_loop(
        state.clone(),
        args.worker_id.clone(),
        MaintenanceConfig::from_args(&args),
        shutdown_rx,
    ));
    tracing::info!(
        listen = %args.listen,
        worker_id = %args.worker_id,
        segment_bytes = args.segment_bytes,
        cache_dir = %args.cache_dir.display(),
        "dynamic event-index worker pool starting"
    );
    serve(pool_router(state), args.listen, shutdown_tx, shutdown).await?;
    worker.await.context("join event-index worker pool")??;
    maintenance
        .await
        .context("join event-index maintenance pool")??;
    Ok(())
}

/// Serve the HTTP app until `shutdown`, then broadcast shutdown to the
/// background loops.
async fn serve(
    app: Router,
    listen: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(listen).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.await;
            if shutdown_tx.send(true).is_err() {
                tracing::debug!("background loops already stopped");
            }
        })
        .await?;
    Ok(())
}

async fn pool_worker_loop(
    state: PoolState,
    params: WorkerParams,
    worker_concurrency: usize,
    poll_interval: Duration,
    tail_flush_interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut next_tail_flush = Instant::now();
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let allow_partial = Instant::now() >= next_tail_flush;
        match state.catalog.list().await {
            Ok(registrations) => {
                if !registrations.is_empty() {
                    let max_attempts = registrations
                        .len()
                        .checked_mul(worker_concurrency)
                        .and_then(|count| count.checked_mul(4))
                        .ok_or_else(|| anyhow::anyhow!("worker scheduling count overflowed"))?;
                    let mut pending = registrations.into_iter().collect::<VecDeque<_>>();
                    let mut tasks = JoinSet::new();
                    let mut attempts = 0_usize;
                    let mut partial_attempted = HashSet::new();
                    let mut entry_capped = HashSet::new();
                    while (!pending.is_empty() || !tasks.is_empty()) && attempts < max_attempts {
                        while tasks.len() < worker_concurrency && attempts < max_attempts {
                            let Some(registration) = pending.pop_front() else {
                                break;
                            };
                            attempts = attempts.saturating_add(1);
                            let task_state = state.clone();
                            let task_params = params.clone();
                            let task_allow_partial = entry_capped.remove(&registration.id)
                                || (allow_partial
                                    && partial_attempted.insert(registration.id.clone()));
                            tasks.spawn(async move {
                                let result = process_pool_source(
                                    &task_state,
                                    &registration,
                                    &task_params,
                                    task_allow_partial,
                                )
                                .await;
                                (registration, result)
                            });
                        }
                        match tasks.join_next().await {
                            Some(Ok((registration, Ok(Backlog::More)))) => {
                                pending.push_back(registration)
                            }
                            Some(Ok((registration, Ok(Backlog::EntryCapped)))) => {
                                entry_capped.insert(registration.id.clone());
                                pending.push_back(registration);
                            }
                            Some(Ok((_registration, Ok(Backlog::Idle)))) => {}
                            Some(Ok((registration, Err(error)))) => tracing::warn!(
                                index_id = %registration.id,
                                stream_url = %registration.stream_url,
                                %error,
                                "dynamic event-index source attempt failed; retrying"
                            ),
                            Some(Err(error)) => {
                                tracing::warn!(%error, "dynamic event-index worker task failed")
                            }
                            None => break,
                        }
                    }
                }
            }
            Err(error) => tracing::warn!(%error, "event-index catalog refresh failed; retrying"),
        }
        if allow_partial {
            next_tail_flush = Instant::now()
                .checked_add(tail_flush_interval)
                .unwrap_or_else(Instant::now);
        }
        if wait_or_shutdown(poll_interval, &mut shutdown).await {
            return Ok(());
        }
    }
}

/// What an indexing pass leaves to do right away.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Backlog {
    /// Nothing until the next poll.
    Idle,
    /// More source bytes are ready.
    More,
    /// The segment stopped at `--flush-entries` before the tail; claim the
    /// rest even if it is shorter than `--segment-bytes`, so a stream of
    /// small messages is not held to one segment per tail flush.
    EntryCapped,
}

/// One scheduling attempt for one registration.
async fn process_pool_source(
    state: &PoolState,
    registration: &IndexRegistration,
    params: &WorkerParams,
    allow_partial: bool,
) -> Result<Backlog, IndexError> {
    let stream_url = Url::parse(&registration.stream_url)
        .map_err(|_error| IndexError::InvalidConfig("registered stream URL is invalid"))?;
    let source = SourceClient::new(state.http.clone(), stream_url);
    let handles = state.ensure_index(registration).await?;
    let Some(head) = source.head().await? else {
        mark_source_gone(&handles.serving).await?;
        return Ok(Backlog::Idle);
    };
    if incarnation_changed(
        registration.incarnation.as_deref(),
        head.incarnation.as_deref(),
    ) {
        let restarted = state
            .catalog
            .restart(
                &registration.id,
                registration.incarnation.as_deref(),
                head.incarnation.clone(),
                // Every byte of a recreated stream was appended after the
                // registration, so the restart indexes it all, whatever
                // `start` says.
                head.retained_offset,
                wall_clock_millis()?,
            )
            .await?;
        if restarted.incarnation == registration.incarnation {
            // Equality-only comparison cannot order incarnations, so this
            // is either a stale HEAD or a recreate into an incarnation this
            // registration already retired; both wait for cleanup.
            tracing::warn!(
                index_id = %registration.id,
                incarnation = ?registration.incarnation,
                reported_incarnation = ?head.incarnation,
                "source HEAD reports an incarnation whose index namespace is retired; not restarting"
            );
        } else {
            tracing::info!(
                index_id = %registration.id,
                previous_incarnation = ?registration.incarnation,
                incarnation = ?restarted.incarnation,
                "source stream was recreated; restarted its event index"
            );
        }
        // The next pass picks up the restarted registration.
        return Ok(Backlog::Idle);
    }
    index_source(&handles.serving, &source, &head, params, allow_partial).await
}

/// Record a 404 from the source. An index this handle already saw gone needs
/// no S3 request.
async fn mark_source_gone(index: &Mutex<EventIndex>) -> Result<(), IndexError> {
    let mut index = index.lock().await;
    if matches!(index.status(), IndexStatus::SourceGone) {
        return Ok(());
    }
    index.set_source_gone(true).await
}

/// Claim, read and commit one segment of `source` into `index`.
async fn index_source(
    index: &Mutex<EventIndex>,
    source: &SourceClient,
    head: &SourceHead,
    params: &WorkerParams,
    allow_partial: bool,
) -> Result<Backlog, IndexError> {
    let format = head.readable_format()?;
    let now_ms = wall_clock_millis()?;
    let (claim, resync, resume, extractor) = {
        let mut index = index.lock().await;
        // Idle skips, without any S3 request. The cached durable offset
        // never exceeds the published one, so whenever the pending bytes
        // it implies are below what a claim needs, the claim would be
        // refused anyway. A blocked index (resumed perhaps on another pod),
        // or one whose last read found only an unterminated line, is
        // retried on tail-flush passes only.
        let floor_current = head.retained_offset <= index.floor_offset();
        let pending = head.next_offset.saturating_sub(index.durable_offset());
        let idle = match index.status() {
            IndexStatus::Ready => {
                (floor_current
                    && (pending == 0 || (!allow_partial && pending < params.limits.segment_bytes)))
                    || (!allow_partial && index.is_stalled())
            }
            IndexStatus::Blocked { .. } => !allow_partial,
            IndexStatus::SourceGone => false,
        };
        if idle {
            return Ok(Backlog::Idle);
        }
        index.refresh().await?;
        if matches!(index.status(), IndexStatus::SourceGone) {
            index.set_source_gone(false).await?;
        }
        // Follow retention even while blocked.
        if head.retained_offset > index.floor_offset() {
            index.advance_floor(head.retained_offset).await?;
        }
        if matches!(index.status(), IndexStatus::Blocked { .. }) {
            return Ok(Backlog::Idle);
        }
        let Some(claim) = index
            .claim_segment(
                head.next_offset,
                params.limits.segment_bytes,
                allow_partial,
                &params.worker_id,
                now_ms,
                params.lease_ms,
            )
            .await?
        else {
            return Ok(Backlog::Idle);
        };
        let resync =
            format == SourceFormat::Ndjson && index.resync_offset() == Some(claim.start_offset);
        (
            claim,
            resync,
            index.oversize_scan(),
            index.config().extractor.clone(),
        )
    };
    let read = source
        .read_segment(
            claim.start_offset,
            resync,
            resume,
            &extractor,
            params.limits,
        )
        .await;
    let mut index = index.lock().await;
    let segment = match read {
        Ok(SegmentRead::Segment {
            segment,
            oversize_scan,
        }) => {
            index.note_oversize_scan(oversize_scan);
            segment
        }
        Ok(SegmentRead::Retained { retained_offset }) => {
            index.advance_floor(retained_offset).await?;
            index.release_claim(&claim).await?;
            return Ok(Backlog::More);
        }
        Err(error) => {
            if let Err(release) = index.release_claim(&claim).await {
                tracing::warn!(%release, "failed to release an event-index claim");
            }
            return Err(error);
        }
    };
    let end = segment.end;
    if end == segment.start {
        index.note_stalled(end);
        index.release_claim(&claim).await?;
        return Ok(Backlog::Idle);
    }
    let entry_capped =
        segment.entries.len().saturating_add(segment.skips.len()) >= params.limits.max_entries;
    match index.finish_segment(&claim, segment).await {
        Ok(()) if end >= head.next_offset => Ok(Backlog::Idle),
        Ok(()) if entry_capped => Ok(Backlog::EntryCapped),
        Ok(()) => Ok(Backlog::More),
        Err(IndexError::EntryConflict { offset }) => {
            let reason = IndexError::EntryConflict { offset }.to_string();
            index.mark_blocked(offset, reason.clone()).await?;
            index.release_claim(&claim).await?;
            Err(IndexError::Blocked { offset, reason })
        }
        Err(error) => Err(error),
    }
}

/// One compaction-plus-GC maintenance pass over one index instance. Failures
/// are logged and retried on the next pass rather than propagated.
async fn maintenance_pass(
    index: &mut EventIndex,
    index_id: &str,
    config: &MaintenanceConfig,
    run_gc: bool,
) {
    if let Err(error) = index.refresh().await {
        tracing::warn!(index_id, %error, "event index maintenance refresh failed; retrying");
        return;
    }
    if index.needs_partition_compaction(config.compaction_fan_in, config.compaction_max_entries) {
        match index
            .compact_partition_once(config.compaction_fan_in, config.compaction_max_entries)
            .await
        {
            Ok(true) => tracing::info!(index_id, "compacted one event-time partition tier"),
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(index_id, %error, "event index compaction failed; retrying")
            }
        }
    }
    if run_gc {
        match index
            .garbage_collect(
                config.gc_retain_generations,
                config.gc_grace,
                gc_wall_clock_now(),
            )
            .await
        {
            Ok(report) => tracing::info!(
                index_id,
                deleted_parts = report.deleted_parts,
                deleted_layouts = report.deleted_layouts,
                deleted_manifests = report.deleted_manifests,
                deleted_claims = report.deleted_claims,
                "event index garbage collection completed"
            ),
            Err(error) => tracing::warn!(
                index_id,
                %error,
                "event index garbage collection failed; retrying later"
            ),
        }
    }
}

fn next_gc_deadline(interval: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(interval).unwrap_or(now)
}

async fn maintenance_loop(
    mut index: EventIndex,
    config: MaintenanceConfig,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut next_gc = next_gc_deadline(config.gc_interval);
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let run_gc = Instant::now() >= next_gc;
        maintenance_pass(&mut index, "single", &config, run_gc).await;
        if run_gc {
            next_gc = next_gc_deadline(config.gc_interval);
        }
        if wait_or_shutdown(config.interval, &mut shutdown).await {
            return Ok(());
        }
    }
}

async fn pool_maintenance_loop(
    state: PoolState,
    worker_id: String,
    config: MaintenanceConfig,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut next_gc = next_gc_deadline(config.gc_interval);
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let owns_maintenance = match wall_clock_millis() {
            Ok(now_ms) => match state
                .catalog
                .acquire_maintenance_lease(&worker_id, now_ms, config.lease_ms)
                .await
            {
                Ok(owns_maintenance) => owns_maintenance,
                Err(error) => {
                    tracing::warn!(%error, "event-index maintenance lease failed");
                    false
                }
            },
            Err(error) => {
                tracing::warn!(%error, "event-index maintenance clock failed");
                false
            }
        };
        if !owns_maintenance {
            if wait_or_shutdown(config.interval, &mut shutdown).await {
                return Ok(());
            }
            continue;
        }
        if let Err(error) = reconcile_pool_indexes(&state).await {
            tracing::warn!(%error, "event-index maintenance catalog refresh failed");
            if wait_or_shutdown(config.interval, &mut shutdown).await {
                return Ok(());
            }
            continue;
        }
        let indexes = state
            .indexes
            .read()
            .await
            .iter()
            .map(|(id, index)| (id.clone(), Arc::clone(index)))
            .collect::<Vec<_>>();
        let run_gc = Instant::now() >= next_gc;
        for (id, handles) in indexes {
            let mut index = handles.maintenance.lock().await;
            maintenance_pass(&mut index, &id, &config, run_gc).await;
        }
        if run_gc {
            cleanup_retired_indexes(&state, config.gc_grace).await;
            next_gc = next_gc_deadline(config.gc_interval);
        }
        if wait_or_shutdown(config.interval, &mut shutdown).await {
            return Ok(());
        }
    }
}

async fn cleanup_retired_indexes(state: &PoolState, grace: Duration) {
    let grace_ms = u64::try_from(grace.as_millis()).unwrap_or(u64::MAX);
    let cutoff_ms = wall_clock_millis()
        .map(|now| now.saturating_sub(grace_ms))
        .unwrap_or(0);
    let retired = match state.catalog.retired_before(cutoff_ms).await {
        Ok(retired) => retired,
        Err(error) => {
            tracing::warn!(%error, "failed to load retired event indexes");
            return;
        }
    };
    let live = match state.catalog.list().await {
        Ok(registrations) => registrations
            .iter()
            .filter_map(|registration| registration.namespace().ok())
            .collect::<HashSet<_>>(),
        Err(error) => {
            tracing::warn!(%error, "failed to load live event indexes");
            return;
        }
    };
    for retired in retired {
        // Never delete a namespace a live registration uses; only drop its
        // tombstone.
        if live.contains(&retired.namespace) {
            if let Err(error) = state.catalog.forget_retired(&retired.namespace).await {
                tracing::warn!(index_id = %retired.id, %error, "failed to drop the tombstone of a live event index");
            }
            continue;
        }
        let store = match state
            .backend
            .open(&format!("indexes/{}", retired.namespace))
        {
            Ok(store) => store,
            Err(error) => {
                tracing::warn!(index_id = %retired.id, %error, "failed to open retired event index namespace");
                continue;
            }
        };
        match store.delete_all().await {
            Ok(deleted_objects) => {
                if let Err(error) = state.catalog.forget_retired(&retired.namespace).await {
                    tracing::warn!(index_id = %retired.id, %error, "retired event index was deleted but its tombstone remains");
                } else {
                    tracing::info!(index_id = %retired.id, namespace = %retired.namespace, deleted_objects, "deleted retired event index namespace");
                }
            }
            Err(error) => {
                tracing::warn!(index_id = %retired.id, %error, "failed to delete retired event index namespace")
            }
        }
    }
}

/// Drop in-memory indexes whose registration is gone or now names another
/// namespace.
async fn reconcile_pool_indexes(state: &PoolState) -> Result<(), IndexError> {
    let mut namespaces = HashMap::new();
    for registration in state.catalog.list().await? {
        namespaces.insert(registration.id.clone(), registration.namespace()?);
    }
    state.indexes.write().await.retain(|id, index| {
        namespaces
            .get(id)
            .is_some_and(|namespace| *namespace == index.namespace)
    });
    Ok(())
}

fn wall_clock_millis() -> Result<u64, IndexError> {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .ok_or(IndexError::InvalidConfig(
            "system clock is before the Unix epoch",
        ))
}

/// Single-source mode: one pass, restarting in place if the source was
/// recreated.
async fn single_pass(
    source: &SourceClient,
    index: &Mutex<EventIndex>,
    params: &WorkerParams,
    allow_partial: bool,
) -> Result<Backlog, IndexError> {
    let Some(head) = source.head().await? else {
        mark_source_gone(index).await?;
        return Ok(Backlog::Idle);
    };
    {
        // The cached incarnation decides; `restart` refreshes and is a no-op
        // if another instance already restarted.
        let mut index = index.lock().await;
        let known = index.source().incarnation.clone();
        if incarnation_changed(known.as_deref(), head.incarnation.as_deref()) {
            // As in pool mode, a recreated stream is indexed from its
            // retained offset whatever `--start` says.
            index
                .restart(IndexBase {
                    offset: head.retained_offset,
                    incarnation: head.incarnation.clone(),
                })
                .await?;
            tracing::info!(
                previous_incarnation = ?known,
                incarnation = ?head.incarnation,
                "source stream was recreated; restarted the event index"
            );
        }
    }
    index_source(index, source, &head, params, allow_partial).await
}

async fn sync_loop(
    source: SourceClient,
    index: Arc<Mutex<EventIndex>>,
    params: WorkerParams,
    poll_interval: Duration,
    tail_flush_interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut next_tail_flush = Instant::now();
    let mut entry_capped = false;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let tail_flush = Instant::now() >= next_tail_flush;
        let follows_entry_cap = std::mem::take(&mut entry_capped);
        let allow_partial = tail_flush || follows_entry_cap;
        let result = single_pass(&source, &index, &params, allow_partial).await;
        if tail_flush {
            next_tail_flush = Instant::now()
                .checked_add(tail_flush_interval)
                .unwrap_or_else(Instant::now);
        }
        match result {
            Ok(Backlog::More) => continue,
            Ok(Backlog::EntryCapped) => {
                entry_capped = true;
                continue;
            }
            Ok(Backlog::Idle) => {}
            Err(error @ IndexError::Blocked { .. }) => {
                tracing::error!(%error, "event index source processing blocked");
            }
            Err(error) => {
                tracing::warn!(%error, "event index update failed transiently; retrying");
            }
        }
        if wait_or_shutdown(poll_interval, &mut shutdown).await {
            return Ok(());
        }
    }
}

async fn wait_or_shutdown(duration: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        () = tokio::time::sleep(duration) => false,
        changed = shutdown.changed() => {
            changed.is_err() || *shutdown.borrow()
        }
    }
}

#[cfg(not(madsim))]
fn gc_wall_clock_now() -> SystemTime {
    SystemTime::now()
}

#[cfg(madsim)]
fn gc_wall_clock_now() -> SystemTime {
    SystemTime::UNIX_EPOCH
}

fn build_router(index: Arc<Mutex<EventIndex>>) -> Router {
    Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/v1/events", get(query_events))
        .route("/v1/status", get(index_status))
        .route("/v1/status/resume", post(resume_index))
        .with_state(SingleAppState { index })
}

fn pool_router(state: PoolState) -> Router {
    Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(pool_readyz))
        .route("/v1/indexes", get(list_pool_indexes))
        .route(
            "/v1/indexes/{id}",
            put(register_pool_index).delete(unregister_pool_index),
        )
        .route("/v1/indexes/{id}/events", get(query_pool_events))
        .route("/v1/indexes/{id}/status", get(pool_index_status))
        .route("/v1/indexes/{id}/status/resume", post(resume_pool_index))
        .with_state(state)
}

async fn pool_readyz(State(state): State<PoolState>) -> StatusCode {
    match state.catalog.list().await {
        Ok(_) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

fn request_extractor(request: &RegisterIndexRequest) -> Result<ExtractorConfig, IndexError> {
    match (&request.extract, &request.timestamp_field) {
        (Some(_), Some(_)) => Err(IndexError::InvalidConfig(
            "pass either `extract` or the legacy `timestamp_field`, not both",
        )),
        (Some(extract), None) => Ok(extract.clone()),
        (None, Some(field)) => Ok(Extractor::timestamp_field(field)?.config().clone()),
        (None, None) => Ok(Extractor::timestamp_field("captured_at")?.config().clone()),
    }
}

async fn register_pool_index(
    State(state): State<PoolState>,
    Path(id): Path<String>,
    Json(request): Json<RegisterIndexRequest>,
) -> Result<Response, ApiError> {
    let stream_url = crate::validate_stream_url(&request.stream_url).map_err(ApiError)?;
    let extract = request_extractor(&request).map_err(ApiError)?;
    let canonical_stream_url = stream_url.to_string();
    let head = SourceClient::new(state.http.clone(), stream_url)
        .head()
        .await
        .map_err(ApiError)?
        .ok_or(ApiError(IndexError::InvalidSourceResponse(
            "the source stream does not exist",
        )))?;
    head.readable_format().map_err(ApiError)?;
    let registration = IndexRegistration {
        id,
        stream_url: canonical_stream_url,
        extract,
        start: request.start,
        indexed_from_offset: start_offset(request.start, &head),
        incarnation: head.incarnation.clone(),
        restarted_from_incarnation: None,
    };
    state
        .catalog
        .register(&registration)
        .await
        .map_err(ApiError)?;
    let registration = state
        .catalog
        .get(&registration.id)
        .await
        .map_err(ApiError)?;
    state.ensure_index(&registration).await.map_err(ApiError)?;
    Ok((StatusCode::CREATED, Json(registration)).into_response())
}

async fn unregister_pool_index(
    State(state): State<PoolState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .catalog
        .unregister(&id, wall_clock_millis().map_err(ApiError)?)
        .await
        .map_err(ApiError)?;
    state.indexes.write().await.remove(&id);
    Ok(StatusCode::NO_CONTENT)
}

async fn list_pool_indexes(
    State(state): State<PoolState>,
) -> Result<Json<Vec<IndexRegistration>>, ApiError> {
    Ok(Json(state.catalog.list().await.map_err(ApiError)?))
}

async fn pool_index(
    state: &PoolState,
    id: &str,
) -> Result<(Arc<Mutex<EventIndex>>, IndexRegistration), ApiError> {
    let registration = state.catalog.get(id).await.map_err(ApiError)?;
    let index = state.ensure_index(&registration).await.map_err(ApiError)?;
    Ok((Arc::clone(&index.serving), registration))
}

/// Refresh (or, for a resume request, clear the blocked status of) an index
/// and report its published status.
async fn status_response(
    index: &Mutex<EventIndex>,
    resume: bool,
    restarted_from_incarnation: Option<String>,
) -> Result<Json<StatusBody>, ApiError> {
    let mut index = index.lock().await;
    if resume {
        index.clear_blocked().await.map_err(ApiError)?;
    } else {
        index.refresh().await.map_err(ApiError)?;
    }
    Ok(Json(StatusBody::read(&index, restarted_from_incarnation)))
}

async fn pool_index_status(
    State(state): State<PoolState>,
    Path(id): Path<String>,
) -> Result<Json<StatusBody>, ApiError> {
    let (index, registration) = pool_index(&state, &id).await?;
    status_response(&index, false, registration.restarted_from_incarnation).await
}

async fn resume_pool_index(
    State(state): State<PoolState>,
    Path(id): Path<String>,
) -> Result<Json<StatusBody>, ApiError> {
    let (index, registration) = pool_index(&state, &id).await?;
    status_response(&index, true, registration.restarted_from_incarnation).await
}

async fn query_pool_events(
    State(state): State<PoolState>,
    Path(id): Path<String>,
    Query(query): Query<EventQuery>,
) -> Result<Response, ApiError> {
    let (index, _registration) = pool_index(&state, &id).await?;
    query_index(index, query).await
}

async fn livez() -> StatusCode {
    StatusCode::NO_CONTENT
}

/// Query readiness is independent of source health: a blocked or gone source
/// still serves its committed index.
async fn readyz(State(state): State<SingleAppState>) -> StatusCode {
    let mut index = state.index.lock().await;
    match index.refresh().await {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn resume_index(State(state): State<SingleAppState>) -> Result<Json<StatusBody>, ApiError> {
    status_response(&state.index, true, None).await
}

async fn index_status(State(state): State<SingleAppState>) -> Result<Json<StatusBody>, ApiError> {
    status_response(&state.index, false, None).await
}

async fn query_events(
    State(state): State<SingleAppState>,
    Query(query): Query<EventQuery>,
) -> Result<Response, ApiError> {
    query_index(state.index, query).await
}

fn query_request(query: &EventQuery) -> Result<QueryRequest, IndexError> {
    let from_ms = parse_query_timestamp(&query.from).ok_or(IndexError::InvalidQuery)?;
    let until_ms = parse_query_timestamp(&query.until).ok_or(IndexError::InvalidQuery)?;
    let match_mode = match query.match_mode.as_deref() {
        None | Some("start") => MatchMode::Start,
        Some("overlap") => MatchMode::Overlap,
        Some(_) => return Err(IndexError::InvalidQuery),
    };
    let after = query
        .after
        .as_deref()
        .map(|after| QueryCursor::decode(after).ok_or(IndexError::InvalidQuery))
        .transpose()?;
    let through = query
        .through
        .as_deref()
        .map(|through| parse_offset_token(through).ok_or(IndexError::InvalidQuery))
        .transpose()?;
    if query.limit > 10_000 {
        return Err(IndexError::InvalidQuery);
    }
    Ok(QueryRequest {
        from_ms,
        until_ms,
        match_mode,
        after,
        through,
        limit: query.limit,
    })
}

async fn query_index(
    index: Arc<Mutex<EventIndex>>,
    query: EventQuery,
) -> Result<Response, ApiError> {
    let request = query_request(&query).map_err(ApiError)?;
    let result = index.lock().await.query(request).await.map_err(ApiError)?;
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("indexed-from-offset", result.coverage.from),
        ("floor-offset", result.coverage.floor),
        ("durable-offset", result.coverage.durable),
        ("through-offset", result.coverage.through),
    ] {
        let value = HeaderValue::from_str(&offset_token(value))
            .map_err(|_error| ApiError(IndexError::InvalidQuery))?;
        headers.insert(name, value);
    }
    Ok((headers, Json(result)).into_response())
}

fn parse_query_timestamp(value: &str) -> Option<i64> {
    value.parse::<i64>().ok().or_else(|| {
        DateTime::parse_from_rfc3339(value)
            .ok()
            .map(|value| value.timestamp_millis())
    })
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::panic_in_result_fn,
        reason = "the test combines fallible setup with assertions"
    )]

    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;

    use axum::body::Body;
    use axum::body::to_bytes;
    use axum::http::Request;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use reqwest::Url;
    use tempfile::TempDir;
    use tokio::sync::Mutex;
    use tower::ServiceExt;

    use super::Backlog;
    use super::PoolIndexSettings;
    use super::PoolState;
    use super::StoreTarget;
    use super::WorkerParams;
    use super::build_router;
    use super::cleanup_retired_indexes;
    use super::pool_router;
    use super::process_pool_source;
    use super::reconcile_pool_indexes;
    use super::single_pass;
    use crate::EventEntry;
    use crate::EventIndex;
    use crate::EventIndexCache;
    use crate::EventIndexConfig;
    use crate::Extractor;
    use crate::FsObjectStore;
    use crate::IndexBase;
    use crate::IndexCatalog;
    use crate::IndexError;
    use crate::IndexRegistration;
    use crate::IndexStatus;
    use crate::QueryRequest;
    use crate::Segment;
    use crate::SourceClient;
    use crate::catalog::StartPosition;
    use crate::source::ReadLimits;

    fn pool_state(objects: &TempDir, cache: &TempDir) -> anyhow::Result<PoolState> {
        Ok(PoolState {
            catalog: IndexCatalog::new(FsObjectStore::new(objects.path())?),
            backend: StoreTarget::Fs {
                root: objects.path().to_path_buf(),
            },
            settings: PoolIndexSettings {
                serving_cache: EventIndexCache::serving(
                    cache.path().join("serving"),
                    16 * 1024 * 1024,
                )?,
                maintenance_cache: EventIndexCache::maintenance(
                    cache.path().join("maintenance"),
                    16 * 1024 * 1024,
                )?,
                row_group_entries: 2,
            },
            indexes: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            http: reqwest::Client::new(),
        })
    }

    fn params(worker_id: &str, segment_bytes: u64) -> WorkerParams {
        WorkerParams {
            worker_id: worker_id.to_owned(),
            lease_ms: 60_000,
            limits: ReadLimits {
                segment_bytes,
                max_entries: 1_000,
                max_message_bytes: 1_024,
            },
        }
    }

    /// A mock source whose body, incarnation and retained offset tests
    /// change between passes.
    #[derive(Clone, Default)]
    struct MockSource {
        body: Arc<StdMutex<Vec<u8>>>,
        incarnation: Arc<StdMutex<String>>,
        retained: Arc<StdMutex<u64>>,
        content_type: Arc<StdMutex<&'static str>>,
    }

    impl MockSource {
        fn set(&self, body: &str, incarnation: &str, retained: u64) {
            *self.body.lock().expect("lock") = body.as_bytes().to_vec();
            *self.incarnation.lock().expect("lock") = incarnation.to_owned();
            *self.retained.lock().expect("lock") = retained;
        }

        fn set_content_type(&self, content_type: &'static str) {
            *self.content_type.lock().expect("lock") = content_type;
        }

        async fn serve(self) -> anyhow::Result<(String, tokio::task::JoinHandle<()>)> {
            let app = axum::Router::new().route(
                "/stream",
                axum::routing::any(move |request: axum::extract::Request| {
                    let source = self.clone();
                    async move { source.respond(&request) }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let server = tokio::spawn(async move {
                if let Err(error) = axum::serve(listener, app).await {
                    tracing::warn!(%error, "mock source stopped");
                }
            });
            Ok((format!("http://{address}/stream"), server))
        }

        fn respond(&self, request: &axum::extract::Request) -> axum::response::Response {
            let body = self.body.lock().expect("lock").clone();
            let incarnation = self.incarnation.lock().expect("lock").clone();
            let retained = *self.retained.lock().expect("lock");
            let tail = u64::try_from(body.len()).expect("small body");
            let pad = |offset: u64| format!("{offset:020}");
            if request.method() == axum::http::Method::HEAD {
                let content_type = match *self.content_type.lock().expect("lock") {
                    "" => "application/json",
                    content_type => content_type,
                };
                return (StatusCode::OK, [
                    ("content-type", content_type.to_owned()),
                    ("stream-next-offset", pad(tail)),
                    ("stream-retained-offset", pad(retained)),
                    ("stream-incarnation", incarnation),
                ])
                    .into_response();
            }
            let offset = request
                .uri()
                .query()
                .and_then(|query| query.strip_prefix("offset="))
                .and_then(|offset| offset.parse::<u64>().ok())
                .unwrap_or(0);
            if offset < retained {
                return (StatusCode::GONE, [("stream-next-offset", pad(retained))]).into_response();
            }
            let start = usize::try_from(offset).expect("small offset");
            let chunk = body.get(start..).unwrap_or_default().to_vec();
            (
                StatusCode::OK,
                [
                    ("stream-next-offset", pad(tail)),
                    ("stream-up-to-date", "true".to_owned()),
                ],
                chunk,
            )
                .into_response()
        }
    }

    #[tokio::test]
    async fn pool_registers_and_queries_an_index() -> anyhow::Result<()> {
        let source = MockSource::default();
        source.set(
            concat!(
                "{\"captured_at\":\"2026-07-18T10:00:00Z\"}\n",
                "{\"other\":1}\n",
                "{\"captured_at\":\"2026-07-18T09:00:00Z\"}\n"
            ),
            "100",
            0,
        );
        let (stream_url, server) = source.clone().serve().await?;
        let objects = TempDir::new()?;
        let cache = TempDir::new()?;
        let state = pool_state(&objects, &cache)?;
        let app = pool_router(state.clone());
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/v1/indexes/session-42")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        "{{\"stream_url\":\"{stream_url}\",\"timestamp_field\":\"captured_at\"}}"
                    )))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = to_bytes(response.into_body(), 64 * 1024).await?;
        let body: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(body["indexed_from_offset"], "00000000000000000000");
        assert_eq!(body["incarnation"], "100");
        let registration = state.catalog.get("session-42").await?;

        // A 2-byte segment limit commits one message per pass.
        while process_pool_source(&state, &registration, &params("worker-a", 2), true).await?
            != Backlog::Idle
        {}
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/indexes/session-42/events?from=0&until=2000000000000&limit=10")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["durable-offset"], "00000000000000000090");
        let body = to_bytes(response.into_body(), 64 * 1024).await?;
        let body: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(body["entries"][0]["offset"], "00000000000000000051");
        assert_eq!(body["entries"][0]["len"], 39);
        assert_eq!(body["entries"][1]["offset"], "00000000000000000000");
        assert_eq!(body["skipped"]["missing"], 1);
        assert_eq!(body["coverage"]["complete"], true);
        assert_eq!(body["source"]["incarnation"], "100");
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn retention_past_unindexed_bytes_advances_the_floor_and_reports_incomplete()
    -> anyhow::Result<()> {
        // 18-byte messages. On JSON, retention lands on a message boundary;
        // on NDJSON it lands mid-line in the second message, whose 9-byte
        // tail is discarded and counted as trimmed too.
        for (ndjson, retained, trimmed, offsets) in
            [(false, 54, 36, vec![54]), (true, 27, 18, vec![36, 54])]
        {
            let source = MockSource::default();
            if ndjson {
                source.set_content_type("application/x-ndjson");
            }
            let message = "{\"captured_at\":7}\n";
            source.set(&message.repeat(4), "1", 0);
            let (stream_url, server) = source.clone().serve().await?;
            let objects = TempDir::new()?;
            let cache = TempDir::new()?;
            let state = pool_state(&objects, &cache)?;
            let registration = IndexRegistration {
                id: "floor".to_owned(),
                stream_url,
                extract: Extractor::timestamp_field("captured_at")?.config().clone(),
                start: StartPosition::Retained,
                indexed_from_offset: 0,
                incarnation: Some("1".to_owned()),
                restarted_from_incarnation: None,
            };
            state.catalog.register(&registration).await?;
            let registration = state.catalog.get("floor").await?;
            // Index the first message only.
            assert_eq!(
                process_pool_source(&state, &registration, &params("worker-a", 1), true).await?,
                Backlog::More
            );
            source.set(&message.repeat(4), "1", retained);
            while process_pool_source(&state, &registration, &params("worker-a", 64), true).await?
                != Backlog::Idle
            {}
            let handles = state.ensure_index(&registration).await?;
            let mut index = handles.serving.lock().await;
            assert_eq!(index.floor_offset(), retained);
            assert_eq!(index.durable_offset(), 72);
            assert_eq!(index.trimmed_bytes(), trimmed);
            assert!(!index.coverage().complete);
            let result = index.query(QueryRequest::window(0, 1_000, 10)).await?;
            let located = result
                .entries
                .iter()
                .map(|entry| entry.offset)
                .collect::<Vec<_>>();
            assert_eq!(located, offsets);
            drop(index);
            server.abort();
        }
        Ok(())
    }

    #[tokio::test]
    async fn single_source_router_serves_queries_and_stays_ready_while_blocked()
    -> anyhow::Result<()> {
        let objects = TempDir::new()?;
        let cache = TempDir::new()?;
        let mut config = EventIndexConfig::new(
            "https://example.test/single",
            Extractor::timestamp_field("captured_at")?,
        );
        config.row_group_entries = 2;
        let mut index = EventIndex::open(
            FsObjectStore::new(objects.path())?,
            EventIndexCache::serving(cache.path(), 16 * 1024 * 1024)?,
            config,
            IndexBase::default(),
        )
        .await?;
        let entry = |t_ms: i64, offset: u64| EventEntry {
            t_ms,
            t_end_ms: t_ms,
            offset,
            len: 10,
        };
        index
            .commit_segment(Segment {
                start: 0,
                end: 20,
                entries: vec![entry(200, 0), entry(100, 10)],
                skips: Vec::new(),
            })
            .await?;
        index
            .mark_blocked(20, "operator repair required".to_owned())
            .await?;
        let index = Arc::new(Mutex::new(index));
        let app = build_router(Arc::clone(&index));

        // A blocked index still answers readiness and queries.
        for uri in ["/livez", "/readyz"] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty())?)
                .await?;
            assert_eq!(response.status(), StatusCode::NO_CONTENT);
        }
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?from=0&until=1000&limit=10")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["durable-offset"], "00000000000000000020");
        assert_eq!(response.headers()["through-offset"], "00000000000000000020");
        let body = to_bytes(response.into_body(), 64 * 1024).await?;
        let body: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(body["entries"][0]["offset"], "00000000000000000010");
        assert_eq!(body["entries"][1]["offset"], "00000000000000000000");

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/status/resume")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(index.lock().await.status(), &IndexStatus::Ready);
        Ok(())
    }

    #[tokio::test]
    async fn single_source_mode_restarts_in_place_on_a_recreated_source() -> anyhow::Result<()> {
        let source = MockSource::default();
        let message = "{\"captured_at\":7}\n";
        source.set(&message.repeat(2), "1", 0);
        let (stream_url, server) = source.clone().serve().await?;
        let objects = TempDir::new()?;
        let cache = TempDir::new()?;
        let index = EventIndex::open(
            FsObjectStore::new(objects.path())?,
            EventIndexCache::serving(cache.path(), 16 * 1024 * 1024)?,
            EventIndexConfig::new(
                stream_url.clone(),
                Extractor::timestamp_field("captured_at")?,
            ),
            IndexBase {
                offset: 0,
                incarnation: Some("1".to_owned()),
            },
        )
        .await?;
        let index = Mutex::new(index);
        let client = SourceClient::new(reqwest::Client::new(), Url::parse(&stream_url)?);
        let worker = params("worker-a", 64);
        while single_pass(&client, &index, &worker, true).await? != Backlog::Idle {}
        assert_eq!(index.lock().await.durable_offset(), 36);

        source.set(message, "2", 0);
        while single_pass(&client, &index, &worker, true).await? != Backlog::Idle {}
        let mut guard = index.lock().await;
        assert_eq!(guard.source().incarnation.as_deref(), Some("2"));
        assert_eq!(guard.durable_offset(), 18);
        let result = guard.query(QueryRequest::window(0, 1_000, 10)).await?;
        assert_eq!(result.entries.len(), 1);
        drop(guard);

        // Recreated with a type the indexer cannot read: the restart still
        // happens, then indexing fails instead of serving stale locators.
        source.set_content_type("application/octet-stream");
        source.set(message, "3", 0);
        assert!(matches!(
            single_pass(&client, &index, &worker, true).await,
            Err(IndexError::InvalidSourceResponse(_))
        ));
        let guard = index.lock().await;
        assert_eq!(guard.source().incarnation.as_deref(), Some("3"));
        assert_eq!(guard.durable_offset(), 0);
        drop(guard);
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn a_tail_registration_indexes_a_recreated_stream_from_its_first_message()
    -> anyhow::Result<()> {
        let source = MockSource::default();
        let message = "{\"captured_at\":7}\n";
        source.set(&message.repeat(2), "1", 0);
        let (stream_url, server) = source.clone().serve().await?;
        let objects = TempDir::new()?;
        let cache = TempDir::new()?;
        let state = pool_state(&objects, &cache)?;
        state
            .catalog
            .register(&IndexRegistration {
                id: "tail".to_owned(),
                stream_url,
                extract: Extractor::timestamp_field("captured_at")?.config().clone(),
                start: StartPosition::Tail,
                indexed_from_offset: 36,
                incarnation: Some("1".to_owned()),
                restarted_from_incarnation: None,
            })
            .await?;

        // Recreated with an initial message, written before the indexer
        // notices the new incarnation.
        source.set(message, "2", 0);
        let registration = state.catalog.get("tail").await?;
        assert_eq!(
            process_pool_source(&state, &registration, &params("worker-a", 64), true).await?,
            Backlog::Idle
        );
        let registration = state.catalog.get("tail").await?;
        assert_eq!(registration.incarnation.as_deref(), Some("2"));
        assert_eq!(registration.indexed_from_offset, 0);
        while process_pool_source(&state, &registration, &params("worker-a", 64), true).await?
            != Backlog::Idle
        {}
        let handles = state.ensure_index(&registration).await?;
        let mut index = handles.serving.lock().await;
        assert_eq!(index.durable_offset(), 18);
        let result = index.query(QueryRequest::window(0, 1_000, 10)).await?;
        assert_eq!(result.entries.len(), 1);
        drop(index);
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn catalog_unregistration_is_reconciled_in_every_pool_pod() -> anyhow::Result<()> {
        let objects = TempDir::new()?;
        let cache = TempDir::new()?;
        let state = pool_state(&objects, &cache)?;
        let registration = IndexRegistration {
            id: "removed-stream".to_owned(),
            stream_url: "https://example.test/removed".to_owned(),
            extract: Extractor::timestamp_field("captured_at")?.config().clone(),
            start: StartPosition::Retained,
            indexed_from_offset: 0,
            incarnation: None,
            restarted_from_incarnation: None,
        };
        state.catalog.register(&registration).await?;
        state.ensure_index(&registration).await?;
        assert_eq!(state.indexes.read().await.len(), 1);
        let current = objects
            .path()
            .join("indexes")
            .join(registration.namespace()?)
            .join("CURRENT");
        assert!(current.exists());

        state.catalog.unregister(&registration.id, 0).await?;
        reconcile_pool_indexes(&state).await?;
        assert!(state.indexes.read().await.is_empty());
        cleanup_retired_indexes(&state, std::time::Duration::ZERO).await;
        assert!(!current.exists());
        assert!(state.catalog.retired_before(u64::MAX).await?.is_empty());
        Ok(())
    }
}
