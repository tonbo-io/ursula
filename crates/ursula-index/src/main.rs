use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
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
use clap::ArgGroup;
use clap::Parser;
use reqwest::Url;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;
use ursula_index::EventIndex;
use ursula_index::EventIndexCache;
use ursula_index::EventIndexConfig;
use ursula_index::FsObjectStore;
use ursula_index::IndexCatalog;
use ursula_index::IndexError;
use ursula_index::IndexRegistration;
use ursula_index::IndexStatus;
use ursula_index::ObjectStore;
use ursula_index::QueryCursor;
use ursula_index::S3ObjectStore;
use ursula_index::S3ObjectStoreConfig;
use ursula_index::SourceBatch;
use ursula_index::SourceClient;
use ursula_observability::serve::shutdown_signal;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "S3-backed client-event-time index for an Ursula JSON stream",
    group(ArgGroup::new("backend").required(true).args(["object_dir", "s3_bucket"]))
)]
struct Args {
    #[arg(long)]
    stream_url: Option<Url>,
    #[arg(long, conflicts_with = "s3_bucket")]
    object_dir: Option<PathBuf>,
    #[arg(long, conflicts_with = "object_dir")]
    s3_bucket: Option<String>,
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
    #[arg(long, default_value_t = 4_096)]
    flush_entries: usize,
    #[arg(long, default_value_t = 16_384)]
    row_group_entries: usize,
    #[arg(long, default_value_t = 4_096)]
    read_batch_records: usize,
    #[arg(long, default_value_t = 65_536)]
    segment_records: u64,
    #[arg(long, default_value_t = 4)]
    worker_concurrency: usize,
    #[arg(long, default_value_t = 60_000)]
    segment_lease_ms: u64,
    #[arg(long, default_value = "ursula-indexer-local")]
    worker_id: String,
    #[arg(long, default_value_t = 250)]
    poll_interval_ms: u64,
    #[arg(long = "compact-parts", default_value_t = 8)]
    compaction_fan_in: usize,
    #[arg(long, default_value_t = 1_000_000)]
    compaction_max_entries: u64,
    #[arg(long, default_value_t = 3_600)]
    gc_interval_seconds: u64,
    #[arg(long, default_value_t = 86_400)]
    gc_grace_seconds: u64,
    #[arg(long, default_value_t = 8)]
    gc_retain_generations: u64,
    #[arg(long, default_value_t = 1_000)]
    maintenance_interval_ms: u64,
    #[arg(long, default_value = "captured_at")]
    timestamp_field: String,
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
    fn from_args(args: &Args) -> anyhow::Result<Self> {
        if let Some(object_dir) = &args.object_dir {
            return Ok(Self::Fs {
                root: object_dir.clone(),
            });
        }
        let bucket = args
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
    gc_interval: Duration,
    gc_grace: Duration,
    gc_retain_generations: u64,
}

impl MaintenanceConfig {
    fn from_args(args: &Args) -> Self {
        Self {
            interval: Duration::from_millis(args.maintenance_interval_ms),
            compaction_fan_in: args.compaction_fan_in,
            compaction_max_entries: args.compaction_max_entries,
            gc_interval: Duration::from_secs(args.gc_interval_seconds),
            gc_grace: Duration::from_secs(args.gc_grace_seconds),
            gc_retain_generations: args.gc_retain_generations,
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
    flush_entries: usize,
    row_group_entries: usize,
}

struct PoolIndex {
    serving: Arc<Mutex<EventIndex>>,
    maintenance: Mutex<EventIndex>,
}

#[derive(Clone)]
struct PoolState {
    catalog: IndexCatalog,
    backend: StoreTarget,
    settings: PoolIndexSettings,
    indexes: Arc<RwLock<HashMap<String, Arc<PoolIndex>>>>,
}

#[derive(Debug, Deserialize)]
struct RegisterIndexRequest {
    stream_url: String,
    #[serde(default = "default_timestamp_field")]
    timestamp_field: String,
}

fn default_timestamp_field() -> String {
    "captured_at".to_owned()
}

#[derive(Debug, Deserialize)]
struct EventQuery {
    from: String,
    until: String,
    after_captured_at_ms: Option<i64>,
    after_record: Option<u64>,
    through_record: Option<u64>,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    1_000
}

#[derive(Debug, Serialize)]
struct StatusBody {
    status: IndexStatus,
    indexed_from_record: u64,
    indexed_through_record: u64,
    durable_through_record: u64,
    parts: usize,
}

impl StatusBody {
    fn read(index: &EventIndex) -> Self {
        Self {
            status: index.status().clone(),
            indexed_from_record: index.indexed_from_record(),
            indexed_through_record: index.indexed_through_record(),
            durable_through_record: index.durable_through_record(),
            parts: index.part_count(),
        }
    }
}

#[derive(Debug)]
struct ApiError(IndexError);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0 {
            IndexError::InvalidQuery
            | IndexError::InvalidTimestamp { .. }
            | IndexError::InvalidConfig(_) => StatusCode::BAD_REQUEST,
            IndexError::InvalidSourceResponse(_) | IndexError::MissingRecordCoordinates => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            IndexError::RetentionGap { .. }
            | IndexError::Blocked { .. }
            | IndexError::CannotResume(_)
            | IndexError::RegistrationConflict(_)
            | IndexError::IndexBaseMismatch { .. } => StatusCode::CONFLICT,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _observability =
        ursula_observability::init(ursula_observability::InitOptions::new("ursula-indexer"));
    let args = Args::parse();
    validate_args(&args)?;
    match args.stream_url.clone() {
        Some(stream_url) => run_single(args, stream_url).await,
        None => run_pool(args).await,
    }
}

fn validate_args(args: &Args) -> anyhow::Result<()> {
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
    if args.segment_records == 0
        || args.worker_concurrency == 0
        || args.segment_lease_ms == 0
        || usize::try_from(args.segment_records).is_err()
        || args.worker_id.is_empty()
    {
        anyhow::bail!("segment records, lease duration, and worker id must be valid");
    }
    Ok(())
}

async fn run_single(args: Args, stream_url: Url) -> anyhow::Result<()> {
    let config = EventIndexConfig {
        source_id: stream_url.to_string(),
        flush_entries: args.flush_entries,
        row_group_entries: args.row_group_entries,
        timestamp_field: args.timestamp_field.clone(),
    };
    let store = StoreTarget::from_args(&args)?
        .open("")
        .context("open object store")?;
    let index = EventIndex::open(
        store.clone(),
        EventIndexCache::serving(&args.cache_dir, args.cache_max_bytes)?,
        config.clone(),
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
    )
    .await
    .context("open event index")?;
    let index = Arc::new(Mutex::new(index));
    let source = SourceClient::new(stream_url.clone(), args.read_batch_records)
        .context("configure source client")?;
    source
        .probe()
        .await
        .context("source stream must be application/json with json-record-coordinates-v1")?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let sync_task = tokio::spawn(sync_loop(
        source,
        Arc::clone(&index),
        Duration::from_millis(args.poll_interval_ms),
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
        s3_bucket = args.s3_bucket.as_deref().unwrap_or("filesystem-dev-backend"),
        "event indexer starting"
    );
    serve(build_router(Arc::clone(&index)), args.listen, shutdown_tx).await?;
    sync_task.await.context("join source sync loop")??;
    maintenance_task
        .await
        .context("join event index maintenance loop")??;
    index
        .lock()
        .await
        .flush()
        .await
        .context("flush event index on shutdown")?;
    Ok(())
}

impl PoolState {
    async fn ensure_index(
        &self,
        registration: &IndexRegistration,
    ) -> Result<Arc<PoolIndex>, IndexError> {
        if let Some(index) = self.indexes.read().await.get(&registration.id).cloned() {
            return Ok(index);
        }
        let namespace = format!(
            "{}-{}",
            registration.id,
            blake3::hash(registration.stream_url.as_bytes()).to_hex()
        );
        let config = EventIndexConfig {
            source_id: registration.stream_url.clone(),
            flush_entries: self.settings.flush_entries,
            row_group_entries: self.settings.row_group_entries,
            timestamp_field: registration.timestamp_field.clone(),
        };
        let store = self.backend.open(&format!("indexes/{namespace}"))?;
        let serving = EventIndex::open_from_record(
            store.clone(),
            self.settings.serving_cache.clone(),
            config.clone(),
            registration.indexed_from_record,
        )
        .await?;
        let maintenance = EventIndex::open_from_record(
            store,
            self.settings.maintenance_cache.clone(),
            config,
            registration.indexed_from_record,
        )
        .await?;
        let index = Arc::new(PoolIndex {
            serving: Arc::new(Mutex::new(serving)),
            maintenance: Mutex::new(maintenance),
        });
        let mut indexes = self.indexes.write().await;
        Ok(indexes
            .entry(registration.id.clone())
            .or_insert_with(|| Arc::clone(&index))
            .clone())
    }
}

async fn run_pool(args: Args) -> anyhow::Result<()> {
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
            flush_entries: args.flush_entries,
            row_group_entries: args.row_group_entries,
        },
        indexes: Arc::new(RwLock::new(HashMap::new())),
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker = tokio::spawn(pool_worker_loop(
        state.clone(),
        args.worker_id.clone(),
        args.segment_records,
        args.worker_concurrency,
        args.segment_lease_ms,
        args.read_batch_records,
        Duration::from_millis(args.poll_interval_ms),
        shutdown_rx.clone(),
    ));
    let maintenance = tokio::spawn(pool_maintenance_loop(
        state.clone(),
        MaintenanceConfig::from_args(&args),
        shutdown_rx,
    ));
    tracing::info!(
        listen = %args.listen,
        worker_id = %args.worker_id,
        segment_records = args.segment_records,
        cache_dir = %args.cache_dir.display(),
        "dynamic event-index worker pool starting"
    );
    serve(pool_router(state), args.listen, shutdown_tx).await?;
    worker.await.context("join event-index worker pool")??;
    maintenance
        .await
        .context("join event-index maintenance pool")??;
    Ok(())
}

/// Serve the HTTP app until a shutdown signal, then broadcast shutdown to the
/// background loops.
async fn serve(
    app: Router,
    listen: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(listen).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            if shutdown_tx.send(true).is_err() {
                tracing::debug!("background loops already stopped");
            }
        })
        .await?;
    Ok(())
}

async fn pool_worker_loop(
    state: PoolState,
    worker_id: String,
    segment_records: u64,
    worker_concurrency: usize,
    lease_ms: u64,
    read_batch_records: usize,
    poll_interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
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
                    while (!pending.is_empty() || !tasks.is_empty()) && attempts < max_attempts {
                        while tasks.len() < worker_concurrency && attempts < max_attempts {
                            let Some(registration) = pending.pop_front() else {
                                break;
                            };
                            attempts = attempts.saturating_add(1);
                            let task_state = state.clone();
                            let task_worker_id = worker_id.clone();
                            tasks.spawn(async move {
                                let result = process_pool_source(
                                    &task_state,
                                    &registration,
                                    &task_worker_id,
                                    segment_records,
                                    lease_ms,
                                    read_batch_records,
                                )
                                .await;
                                (registration, result)
                            });
                        }
                        match tasks.join_next().await {
                            Some(Ok((registration, Ok(true)))) => pending.push_back(registration),
                            Some(Ok((_registration, Ok(false)))) => {}
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
        if wait_or_shutdown(poll_interval, &mut shutdown).await {
            return Ok(());
        }
    }
}

async fn process_pool_source(
    state: &PoolState,
    registration: &IndexRegistration,
    worker_id: &str,
    segment_records: u64,
    lease_ms: u64,
    read_batch_records: usize,
) -> Result<bool, IndexError> {
    let stream_url = Url::parse(&registration.stream_url)
        .map_err(|_error| IndexError::InvalidConfig("registered stream URL is invalid"))?;
    let source = SourceClient::new(stream_url, read_batch_records)?;
    let source_range = source.record_range().await?;
    let handles = state.ensure_index(registration).await?;
    let index = &handles.serving;
    let now_ms = wall_clock_millis()?;
    let read_batch_records = u64::try_from(read_batch_records)
        .map_err(|_error| IndexError::InvalidConfig("read batch record count is too large"))?;
    let task_records = segment_records.min(read_batch_records);
    let claim = {
        let mut index = index.lock().await;
        if source_range.first_record > index.durable_through_record() {
            index.mark_retention_gap(source_range.first_record).await?;
            return Ok(false);
        }
        index
            .claim_next_segment(
                source_range.next_record,
                task_records,
                worker_id,
                now_ms,
                lease_ms,
            )
            .await?
    };
    let Some(claim) = claim else {
        return Ok(false);
    };
    let maximum = usize::try_from(claim.end_record.saturating_sub(claim.start_record))
        .map_err(|_error| IndexError::InvalidConfig("claimed record range is too large"))?;
    match source.read_range(claim.start_record, maximum).await? {
        SourceBatch::Records(records) if records.is_empty() => {
            Err(IndexError::InvalidSourceResponse(
                "source returned no records for a non-empty claimed range",
            ))
        }
        SourceBatch::Records(records) => {
            let mut index = index.lock().await;
            match index.finish_segment(&claim, records).await {
                Err(error) if is_deterministic_data_error(&error) => {
                    let reason = error.to_string();
                    index
                        .mark_segment_blocked(claim.start_record, reason.clone())
                        .await?;
                    Err(IndexError::Blocked {
                        record: claim.start_record,
                        reason,
                    })
                }
                Ok(()) => Ok(true),
                Err(error) => Err(error),
            }
        }
        SourceBatch::RetentionGap {
            first_available_record,
        } => {
            index
                .lock()
                .await
                .mark_retention_gap(first_available_record)
                .await?;
            Ok(false)
        }
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
    if matches!(index.status(), IndexStatus::Ready)
        && index.needs_partition_compaction(config.compaction_fan_in, config.compaction_max_entries)
    {
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
    config: MaintenanceConfig,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut next_gc = next_gc_deadline(config.gc_interval);
    loop {
        if *shutdown.borrow() {
            return Ok(());
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
            next_gc = next_gc_deadline(config.gc_interval);
        }
        if wait_or_shutdown(config.interval, &mut shutdown).await {
            return Ok(());
        }
    }
}

async fn reconcile_pool_indexes(state: &PoolState) -> Result<(), IndexError> {
    let registrations = state.catalog.list().await?;
    let registered_ids = registrations
        .into_iter()
        .map(|registration| registration.id)
        .collect::<HashSet<_>>();
    state
        .indexes
        .write()
        .await
        .retain(|id, _index| registered_ids.contains(id));
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

async fn sync_loop(
    source: SourceClient,
    index: Arc<Mutex<EventIndex>>,
    poll_interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let (from_record, ready) = {
            let mut index = index.lock().await;
            index.refresh().await.context("refresh event index")?;
            (
                index.indexed_through_record(),
                matches!(index.status(), IndexStatus::Ready),
            )
        };
        if !ready {
            if wait_or_shutdown(poll_interval, &mut shutdown).await {
                return Ok(());
            }
            continue;
        }
        match source.read_from(from_record).await {
            Ok(SourceBatch::Records(records)) if records.is_empty() => {}
            Ok(SourceBatch::Records(records)) => {
                let update = async {
                    let mut index = index.lock().await;
                    for envelope in records {
                        let record = envelope.record;
                        if let Err(error) = index.ingest_envelope(envelope).await {
                            if !is_deterministic_data_error(&error) {
                                return Err(error);
                            }
                            let reason = error.to_string();
                            let blocked_record = index.indexed_through_record();
                            index.mark_blocked(blocked_record, reason.clone()).await?;
                            return Err(IndexError::Blocked {
                                record: blocked_record,
                                reason: format!("source record {record}: {reason}"),
                            });
                        }
                    }
                    Ok(())
                }
                .await;
                match update {
                    Ok(()) => continue,
                    Err(error) if matches!(error, IndexError::Blocked { .. }) => {
                        tracing::error!(%error, "event index source processing blocked");
                    }
                    Err(error) => {
                        tracing::warn!(%error, "event index update failed transiently; retrying");
                    }
                }
            }
            Ok(SourceBatch::RetentionGap {
                first_available_record,
            }) => {
                let result = index
                    .lock()
                    .await
                    .mark_retention_gap(first_available_record)
                    .await;
                if let Err(error) = result {
                    tracing::error!(%error, "event index cannot cover retained source history");
                }
            }
            Err(error) => {
                tracing::warn!(%error, from_record, "source read failed; retrying");
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

async fn register_pool_index(
    State(state): State<PoolState>,
    Path(id): Path<String>,
    Json(request): Json<RegisterIndexRequest>,
) -> Result<Response, ApiError> {
    let stream_url = ursula_index::validate_stream_url(&request.stream_url).map_err(ApiError)?;
    let canonical_stream_url = stream_url.to_string();
    let source_range = SourceClient::new(stream_url, 1)
        .map_err(ApiError)?
        .record_range()
        .await
        .map_err(ApiError)?;
    let registration = IndexRegistration {
        id,
        stream_url: canonical_stream_url,
        timestamp_field: request.timestamp_field,
        indexed_from_record: source_range.first_record,
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
    state.catalog.unregister(&id).await.map_err(ApiError)?;
    state.indexes.write().await.remove(&id);
    Ok(StatusCode::NO_CONTENT)
}

async fn list_pool_indexes(
    State(state): State<PoolState>,
) -> Result<Json<Vec<IndexRegistration>>, ApiError> {
    Ok(Json(state.catalog.list().await.map_err(ApiError)?))
}

async fn pool_index(state: &PoolState, id: &str) -> Result<Arc<Mutex<EventIndex>>, ApiError> {
    let registration = state.catalog.get(id).await.map_err(ApiError)?;
    let index = state.ensure_index(&registration).await.map_err(ApiError)?;
    Ok(Arc::clone(&index.serving))
}

/// Refresh (or, for a resume request, clear the blocked status of) an index
/// and report its published status.
async fn status_response(
    index: &Mutex<EventIndex>,
    resume: bool,
) -> Result<Json<StatusBody>, ApiError> {
    let mut index = index.lock().await;
    if resume {
        index.clear_blocked().await.map_err(ApiError)?;
    } else {
        index.refresh().await.map_err(ApiError)?;
    }
    Ok(Json(StatusBody::read(&index)))
}

async fn pool_index_status(
    State(state): State<PoolState>,
    Path(id): Path<String>,
) -> Result<Json<StatusBody>, ApiError> {
    let index = pool_index(&state, &id).await?;
    status_response(&index, false).await
}

async fn resume_pool_index(
    State(state): State<PoolState>,
    Path(id): Path<String>,
) -> Result<Json<StatusBody>, ApiError> {
    let index = pool_index(&state, &id).await?;
    status_response(&index, true).await
}

async fn query_pool_events(
    State(state): State<PoolState>,
    Path(id): Path<String>,
    Query(query): Query<EventQuery>,
) -> Result<Response, ApiError> {
    let index = pool_index(&state, &id).await?;
    query_index(index, query).await
}

async fn livez() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn readyz(State(state): State<SingleAppState>) -> StatusCode {
    let mut index = state.index.lock().await;
    match index.refresh().await {
        Ok(())
            if matches!(
                index.status(),
                IndexStatus::Ready | IndexStatus::Blocked { .. }
            ) =>
        {
            StatusCode::NO_CONTENT
        }
        Ok(()) | Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

fn is_deterministic_data_error(error: &IndexError) -> bool {
    matches!(
        error,
        IndexError::InvalidTimestamp { .. }
            | IndexError::UnexpectedRecord { .. }
            | IndexError::RecordConflict { .. }
    )
}

async fn resume_index(State(state): State<SingleAppState>) -> Result<Json<StatusBody>, ApiError> {
    status_response(&state.index, true).await
}

async fn index_status(State(state): State<SingleAppState>) -> Result<Json<StatusBody>, ApiError> {
    status_response(&state.index, false).await
}

async fn query_events(
    State(state): State<SingleAppState>,
    Query(query): Query<EventQuery>,
) -> Result<Response, ApiError> {
    query_index(state.index, query).await
}

async fn query_index(
    index: Arc<Mutex<EventIndex>>,
    query: EventQuery,
) -> Result<Response, ApiError> {
    let from_ms = parse_query_timestamp(&query.from).ok_or(ApiError(IndexError::InvalidQuery))?;
    let until_ms = parse_query_timestamp(&query.until).ok_or(ApiError(IndexError::InvalidQuery))?;
    let after = match (query.after_captured_at_ms, query.after_record) {
        (None, None) => None,
        (Some(captured_at_ms), Some(record)) => Some(QueryCursor {
            captured_at_ms,
            record,
        }),
        _ => return Err(ApiError(IndexError::InvalidQuery)),
    };
    if query.limit > 10_000 {
        return Err(ApiError(IndexError::InvalidQuery));
    }
    let result = index
        .lock()
        .await
        .query(from_ms, until_ms, after, query.through_record, query.limit)
        .await
        .map_err(ApiError)?;
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("indexed-from-record", result.indexed_from_record),
        ("indexed-through-record", result.indexed_through_record),
        ("durable-through-record", result.durable_through_record),
    ] {
        let value = HeaderValue::from_str(&value.to_string())
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

    use axum::body::Body;
    use axum::body::to_bytes;
    use axum::http::Request;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::get;
    use tempfile::TempDir;
    use tokio::sync::Mutex;
    use tower::ServiceExt;
    use ursula_index::EventEntry;
    use ursula_index::EventIndex;
    use ursula_index::EventIndexCache;
    use ursula_index::EventIndexConfig;
    use ursula_index::FsObjectStore;
    use ursula_index::IndexCatalog;
    use ursula_index::IndexError;
    use ursula_index::IndexRegistration;

    use super::PoolIndexSettings;
    use super::PoolState;
    use super::StoreTarget;
    use super::build_router;
    use super::is_deterministic_data_error;
    use super::pool_router;
    use super::process_pool_source;
    use super::reconcile_pool_indexes;

    async fn fs_index(
        objects: &TempDir,
        cache: &TempDir,
        source_id: &str,
    ) -> anyhow::Result<EventIndex> {
        Ok(EventIndex::open(
            FsObjectStore::new(objects.path())?,
            EventIndexCache::serving(cache.path(), 16 * 1024 * 1024)?,
            EventIndexConfig {
                source_id: source_id.to_owned(),
                flush_entries: 2,
                row_group_entries: 2,
                timestamp_field: "captured_at".to_owned(),
            },
        )
        .await?)
    }

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
                flush_entries: 2,
                row_group_entries: 2,
            },
            indexes: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        })
    }

    #[test]
    fn only_deterministic_source_data_errors_block_the_index() {
        assert!(is_deterministic_data_error(&IndexError::InvalidTimestamp {
            record: 7,
            field: "captured_at".to_owned(),
        }));
        assert!(is_deterministic_data_error(&IndexError::UnexpectedRecord {
            expected: 7,
            actual: 8,
        }));
        assert!(is_deterministic_data_error(&IndexError::RecordConflict {
            record: 7,
        }));
        assert!(!is_deterministic_data_error(&IndexError::PublishConflict));
        assert!(!is_deterministic_data_error(&IndexError::ObjectStore(
            "temporary outage".to_owned(),
        )));
    }

    #[tokio::test]
    async fn http_query_exposes_sorted_records_and_watermarks() -> anyhow::Result<()> {
        let objects = TempDir::new()?;
        let cache = TempDir::new()?;
        let mut index = fs_index(&objects, &cache, "http-test").await?;
        index
            .ingest(EventEntry {
                captured_at_ms: 200,
                record: 0,
            })
            .await?;
        index
            .ingest(EventEntry {
                captured_at_ms: 100,
                record: 1,
            })
            .await?;
        let index = Arc::new(Mutex::new(index));
        let app = build_router(Arc::clone(&index));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?from=0&until=1000&limit=10")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["indexed-through-record"], "2");
        assert_eq!(response.headers()["durable-through-record"], "2");
        let body = to_bytes(response.into_body(), 64 * 1024).await?;
        let body: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(body["records"][0]["record"], 1);
        assert_eq!(body["records"][1]["record"], 0);
        Ok(())
    }

    #[tokio::test]
    async fn health_endpoints_keep_query_readiness_independent_from_source_health()
    -> anyhow::Result<()> {
        let objects = TempDir::new()?;
        let cache = TempDir::new()?;
        let index = fs_index(&objects, &cache, "health-test").await?;
        let index = Arc::new(Mutex::new(index));
        let app = build_router(Arc::clone(&index));

        let response = app
            .clone()
            .oneshot(Request::builder().uri("/livez").body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let response = app
            .clone()
            .oneshot(Request::builder().uri("/readyz").body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        index
            .lock()
            .await
            .mark_blocked(0, "operator repair required".to_owned())
            .await?;
        let response = app
            .clone()
            .oneshot(Request::builder().uri("/readyz").body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/status/resume")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let response = app
            .oneshot(Request::builder().uri("/readyz").body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        Ok(())
    }

    #[tokio::test]
    async fn dynamic_pool_registers_and_processes_record_ranges_without_reconfiguration()
    -> anyhow::Result<()> {
        let source_app = axum::Router::new()
            .route(
                "/stream",
                get(|request: axum::extract::Request| async move {
                    let query = request.uri().query().unwrap_or_default();
                    let start = if query.contains("record=7") { 7 } else { 5 };
                    let body = if start == 5 {
                        concat!(
                            "{\"record\":5,\"value\":{\"captured_at\":\"2026-07-18T10:00:00Z\"}}\n",
                            "{\"record\":6,\"value\":{\"captured_at\":\"2026-07-18T09:00:00Z\"}}\n"
                        )
                    } else {
                        concat!(
                            "{\"record\":7,\"value\":{\"captured_at\":\"2026-07-18T11:00:00Z\"}}\n",
                            "{\"record\":8,\"value\":{\"captured_at\":\"2026-07-18T08:00:00Z\"}}\n"
                        )
                    };
                    (
                        StatusCode::OK,
                        [("stream-extensions", "json-record-coordinates-v1")],
                        body,
                    )
                        .into_response()
                }),
            )
            .route_layer(axum::middleware::from_fn(
                |request: axum::extract::Request, next: axum::middleware::Next| async move {
                    if request.method() == axum::http::Method::HEAD {
                        return (StatusCode::OK, [
                            ("content-type", "application/json"),
                            ("stream-extensions", "json-record-coordinates-v1"),
                            ("stream-record-first", "5"),
                            ("stream-record-next", "9"),
                        ])
                            .into_response();
                    }
                    next.run(request).await
                },
            ));
        let source_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let source_address = source_listener.local_addr()?;
        let source_server = tokio::spawn(axum::serve(source_listener, source_app).into_future());

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
                        "{{\"stream_url\":\"http://{source_address}/stream\",\"timestamp_field\":\"captured_at\"}}"
                    )))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = to_bytes(response.into_body(), 64 * 1024).await?;
        let body: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(body["indexed_from_record"], 5);
        let registration = state.catalog.get("session-42").await?;
        assert_eq!(registration.indexed_from_record, 5);

        process_pool_source(&state, &registration, "worker-a", 2, 60_000, 2).await?;
        process_pool_source(&state, &registration, "worker-b", 2, 60_000, 2).await?;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/indexes/session-42/events?from=0&until=2000000000000&limit=10")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["indexed-from-record"], "5");
        assert_eq!(response.headers()["durable-through-record"], "9");
        let body = to_bytes(response.into_body(), 64 * 1024).await?;
        let body: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(body["records"].as_array().map(Vec::len), Some(4));

        source_server.abort();
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
            timestamp_field: "captured_at".to_owned(),
            indexed_from_record: 0,
        };
        state.catalog.register(&registration).await?;
        state.ensure_index(&registration).await?;
        assert_eq!(state.indexes.read().await.len(), 1);

        state.catalog.unregister(&registration.id).await?;
        reconcile_pool_indexes(&state).await?;
        assert!(state.indexes.read().await.is_empty());
        Ok(())
    }
}
