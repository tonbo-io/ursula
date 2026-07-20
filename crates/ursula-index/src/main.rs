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
use ursula_index::EventIndexCache;
use ursula_index::EventIndexConfig;
use ursula_index::FsObjectStore;
use ursula_index::IndexCatalog;
use ursula_index::IndexError;
use ursula_index::IndexRegistration;
use ursula_index::IndexStatus;
use ursula_index::QueryCursor;
use ursula_index::S3ObjectStore;
use ursula_index::S3ObjectStoreConfig;
use ursula_index::ServerlessEventIndex;
use ursula_index::SourceBatch;
use ursula_index::SourceClient;

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

#[derive(Clone)]
struct SingleAppState {
    index: Arc<Mutex<ServerlessEventIndex>>,
}

#[derive(Clone)]
enum PoolBackend {
    Fs {
        object_root: PathBuf,
    },
    S3 {
        bucket: String,
        root: String,
        region: Option<String>,
        endpoint: Option<String>,
    },
}

#[derive(Clone)]
struct PoolIndexSettings {
    serving_cache: EventIndexCache,
    maintenance_cache: EventIndexCache,
    flush_entries: usize,
    row_group_entries: usize,
}

struct PoolIndex {
    serving: Arc<Mutex<ServerlessEventIndex>>,
    maintenance: Mutex<ServerlessEventIndex>,
}

#[derive(Clone)]
struct PoolState {
    catalog: IndexCatalog,
    backend: PoolBackend,
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
        timestamp_field: args.timestamp_field,
    };
    let maintenance_cache_dir = args.cache_dir.join("maintenance");
    let (index, maintenance_index) = if let Some(object_dir) = &args.object_dir {
        let store = FsObjectStore::new(object_dir).context("open filesystem object store")?;
        let index = ServerlessEventIndex::open_fs(
            store.clone(),
            &args.cache_dir,
            args.cache_max_bytes,
            config.clone(),
        )
        .await?;
        let maintenance = ServerlessEventIndex::open_fs(
            store,
            &maintenance_cache_dir,
            args.maintenance_cache_max_bytes,
            config,
        )
        .await?;
        Ok::<_, IndexError>((index, maintenance))
    } else {
        let bucket = args
            .s3_bucket
            .clone()
            .context("--s3-bucket is required without --object-dir")?;
        let store = S3ObjectStore::new(S3ObjectStoreConfig {
            bucket,
            root: args.s3_prefix.clone(),
            region: args.s3_region.clone(),
            endpoint: args.s3_endpoint.clone(),
        })
        .context("configure S3 object store")?;
        let index = ServerlessEventIndex::open_s3(
            store.clone(),
            &args.cache_dir,
            args.cache_max_bytes,
            config.clone(),
        )
        .await?;
        let maintenance = ServerlessEventIndex::open_s3(
            store,
            &maintenance_cache_dir,
            args.maintenance_cache_max_bytes,
            config,
        )
        .await?;
        Ok::<_, IndexError>((index, maintenance))
    }
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
        Duration::from_millis(args.maintenance_interval_ms),
        args.compaction_fan_in,
        args.compaction_max_entries,
        Duration::from_secs(args.gc_interval_seconds),
        Duration::from_secs(args.gc_grace_seconds),
        args.gc_retain_generations,
        shutdown_rx,
    ));

    let app = build_router(Arc::clone(&index));
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    tracing::info!(
        listen = %args.listen,
        stream_url = %stream_url,
        cache_dir = %args.cache_dir.display(),
        s3_bucket = args.s3_bucket.as_deref().unwrap_or("filesystem-dev-backend"),
        "event indexer starting"
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            if shutdown_tx.send(true).is_err() {
                tracing::debug!("source sync loop already stopped");
            }
        })
        .await?;
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
        let (serving, maintenance) = match &self.backend {
            PoolBackend::Fs { object_root } => {
                let store = FsObjectStore::new(object_root.join("indexes").join(&namespace))?;
                (
                    ServerlessEventIndex::open_fs_with_cache_from_record(
                        store.clone(),
                        self.settings.serving_cache.clone(),
                        config.clone(),
                        registration.indexed_from_record,
                    )
                    .await?,
                    ServerlessEventIndex::open_fs_with_cache_from_record(
                        store,
                        self.settings.maintenance_cache.clone(),
                        config,
                        registration.indexed_from_record,
                    )
                    .await?,
                )
            }
            PoolBackend::S3 {
                bucket,
                root,
                region,
                endpoint,
            } => {
                let store = S3ObjectStore::new(S3ObjectStoreConfig {
                    bucket: bucket.clone(),
                    root: join_object_prefix(root, &format!("indexes/{namespace}")),
                    region: region.clone(),
                    endpoint: endpoint.clone(),
                })?;
                (
                    ServerlessEventIndex::open_s3_with_cache_from_record(
                        store.clone(),
                        self.settings.serving_cache.clone(),
                        config.clone(),
                        registration.indexed_from_record,
                    )
                    .await?,
                    ServerlessEventIndex::open_s3_with_cache_from_record(
                        store,
                        self.settings.maintenance_cache.clone(),
                        config,
                        registration.indexed_from_record,
                    )
                    .await?,
                )
            }
        };
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

fn join_object_prefix(root: &str, suffix: &str) -> String {
    let root = root.trim_matches('/');
    if root.is_empty() {
        suffix.to_owned()
    } else {
        format!("{root}/{suffix}")
    }
}

async fn run_pool(args: Args) -> anyhow::Result<()> {
    let (backend, catalog) = if let Some(object_dir) = &args.object_dir {
        let store = FsObjectStore::new(object_dir).context("open filesystem object store")?;
        (
            PoolBackend::Fs {
                object_root: object_dir.clone(),
            },
            IndexCatalog::open_fs(store),
        )
    } else {
        let bucket = args
            .s3_bucket
            .clone()
            .context("--s3-bucket is required without --object-dir")?;
        let store = S3ObjectStore::new(S3ObjectStoreConfig {
            bucket: bucket.clone(),
            root: args.s3_prefix.clone(),
            region: args.s3_region.clone(),
            endpoint: args.s3_endpoint.clone(),
        })
        .context("configure S3 object store")?;
        (
            PoolBackend::S3 {
                bucket,
                root: args.s3_prefix.clone(),
                region: args.s3_region.clone(),
                endpoint: args.s3_endpoint.clone(),
            },
            IndexCatalog::open_s3(store),
        )
    };
    let state = PoolState {
        catalog,
        backend,
        settings: PoolIndexSettings {
            serving_cache: EventIndexCache::new(
                args.cache_dir.join("serving"),
                args.cache_max_bytes,
            )?,
            maintenance_cache: EventIndexCache::new(
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
        Duration::from_millis(args.maintenance_interval_ms),
        args.compaction_fan_in,
        args.compaction_max_entries,
        Duration::from_secs(args.gc_interval_seconds),
        Duration::from_secs(args.gc_grace_seconds),
        args.gc_retain_generations,
        shutdown_rx,
    ));
    let app = pool_router(state);
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    tracing::info!(
        listen = %args.listen,
        worker_id = %args.worker_id,
        segment_records = args.segment_records,
        cache_dir = %args.cache_dir.display(),
        "dynamic event-index worker pool starting"
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            if shutdown_tx.send(true).is_err() {
                tracing::debug!("event-index worker pool already stopped");
            }
        })
        .await?;
    worker.await.context("join event-index worker pool")??;
    maintenance
        .await
        .context("join event-index maintenance pool")??;
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

async fn pool_maintenance_loop(
    state: PoolState,
    interval: Duration,
    compaction_fan_in: usize,
    compaction_max_entries: u64,
    gc_interval: Duration,
    gc_grace: Duration,
    gc_retain_generations: u64,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let now = Instant::now();
    let mut next_gc = now.checked_add(gc_interval).unwrap_or(now);
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        if let Err(error) = reconcile_pool_indexes(&state).await {
            tracing::warn!(%error, "event-index maintenance catalog refresh failed");
            if wait_or_shutdown(interval, &mut shutdown).await {
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
            if let Err(error) = index.refresh().await {
                tracing::warn!(index_id = %id, %error, "event-index maintenance refresh failed");
                continue;
            }
            if matches!(index.status(), IndexStatus::Ready)
                && index.needs_partition_compaction(compaction_fan_in, compaction_max_entries)
                && let Err(error) = index
                    .compact_partition_once(compaction_fan_in, compaction_max_entries)
                    .await
            {
                tracing::warn!(index_id = %id, %error, "event-index compaction failed; retrying");
            }
            if run_gc
                && let Err(error) = index
                    .garbage_collect(gc_retain_generations, gc_grace, gc_wall_clock_now())
                    .await
            {
                tracing::warn!(index_id = %id, %error, "event-index garbage collection failed");
            }
        }
        if run_gc {
            let now = Instant::now();
            next_gc = now.checked_add(gc_interval).unwrap_or(now);
        }
        if wait_or_shutdown(interval, &mut shutdown).await {
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
    index: Arc<Mutex<ServerlessEventIndex>>,
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

async fn maintenance_loop(
    mut index: ServerlessEventIndex,
    interval: Duration,
    compaction_fan_in: usize,
    compaction_max_entries: u64,
    gc_interval: Duration,
    gc_grace: Duration,
    gc_retain_generations: u64,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let now = Instant::now();
    let mut next_gc = now.checked_add(gc_interval).unwrap_or(now);
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        match index.refresh().await {
            Ok(()) => {
                if matches!(index.status(), IndexStatus::Ready)
                    && index.needs_partition_compaction(compaction_fan_in, compaction_max_entries)
                {
                    match index
                        .compact_partition_once(compaction_fan_in, compaction_max_entries)
                        .await
                    {
                        Ok(true) => tracing::info!("compacted one event-time partition tier"),
                        Ok(false) => {}
                        Err(error) => {
                            tracing::warn!(%error, "event index compaction failed; retrying")
                        }
                    }
                }
                if Instant::now() >= next_gc {
                    match index
                        .garbage_collect(gc_retain_generations, gc_grace, gc_wall_clock_now())
                        .await
                    {
                        Ok(report) => tracing::info!(
                            deleted_parts = report.deleted_parts,
                            deleted_manifests = report.deleted_manifests,
                            deleted_claims = report.deleted_claims,
                            "event index garbage collection completed"
                        ),
                        Err(error) => tracing::warn!(
                            %error,
                            "event index garbage collection failed; retrying later"
                        ),
                    }
                    let now = Instant::now();
                    next_gc = now.checked_add(gc_interval).unwrap_or(now);
                }
            }
            Err(error) => {
                tracing::warn!(%error, "event index maintenance refresh failed; retrying")
            }
        }
        if wait_or_shutdown(interval, &mut shutdown).await {
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

fn build_router(index: Arc<Mutex<ServerlessEventIndex>>) -> Router {
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
    let stream_url = Url::parse(&request.stream_url)
        .map_err(|_error| ApiError(IndexError::InvalidConfig("stream URL is invalid")))?;
    if !matches!(stream_url.scheme(), "http" | "https")
        || stream_url.host_str().is_none()
        || !stream_url.username().is_empty()
        || stream_url.password().is_some()
        || stream_url.fragment().is_some()
    {
        return Err(ApiError(IndexError::InvalidConfig(
            "stream URL must be credential-free HTTP(S) without a fragment",
        )));
    }
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

async fn pool_registration(
    state: &PoolState,
    id: &str,
) -> Result<(IndexRegistration, Arc<Mutex<ServerlessEventIndex>>), ApiError> {
    let registration = state.catalog.get(id).await.map_err(ApiError)?;
    let index = state.ensure_index(&registration).await.map_err(ApiError)?;
    Ok((registration, Arc::clone(&index.serving)))
}

async fn pool_index_status(
    State(state): State<PoolState>,
    Path(id): Path<String>,
) -> Result<Json<StatusBody>, ApiError> {
    let (_registration, index) = pool_registration(&state, &id).await?;
    let mut index = index.lock().await;
    index.refresh().await.map_err(ApiError)?;
    Ok(Json(StatusBody {
        status: index.status().clone(),
        indexed_from_record: index.indexed_from_record(),
        indexed_through_record: index.indexed_through_record(),
        durable_through_record: index.durable_through_record(),
        parts: index.part_count(),
    }))
}

async fn resume_pool_index(
    State(state): State<PoolState>,
    Path(id): Path<String>,
) -> Result<Json<StatusBody>, ApiError> {
    let (_registration, index) = pool_registration(&state, &id).await?;
    let mut index = index.lock().await;
    index.clear_blocked().await.map_err(ApiError)?;
    Ok(Json(StatusBody {
        status: index.status().clone(),
        indexed_from_record: index.indexed_from_record(),
        indexed_through_record: index.indexed_through_record(),
        durable_through_record: index.durable_through_record(),
        parts: index.part_count(),
    }))
}

async fn query_pool_events(
    State(state): State<PoolState>,
    Path(id): Path<String>,
    Query(query): Query<EventQuery>,
) -> Result<Response, ApiError> {
    let (_registration, index) = pool_registration(&state, &id).await?;
    query_index(index, query).await
}

async fn livez() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn readyz(State(state): State<SingleAppState>) -> StatusCode {
    let mut index = state.index.lock().await;
    match index.refresh().await {
        Ok(()) if matches!(index.status(), IndexStatus::Ready) => StatusCode::NO_CONTENT,
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

async fn resume_index(State(state): State<SingleAppState>) -> Result<impl IntoResponse, ApiError> {
    let mut index = state.index.lock().await;
    index.clear_blocked().await.map_err(ApiError)?;
    Ok(Json(StatusBody {
        status: index.status().clone(),
        indexed_from_record: index.indexed_from_record(),
        indexed_through_record: index.indexed_through_record(),
        durable_through_record: index.durable_through_record(),
        parts: index.part_count(),
    }))
}

async fn index_status(State(state): State<SingleAppState>) -> Result<impl IntoResponse, ApiError> {
    let mut index = state.index.lock().await;
    index.refresh().await.map_err(ApiError)?;
    Ok(Json(StatusBody {
        status: index.status().clone(),
        indexed_from_record: index.indexed_from_record(),
        indexed_through_record: index.indexed_through_record(),
        durable_through_record: index.durable_through_record(),
        parts: index.part_count(),
    }))
}

async fn query_events(
    State(state): State<SingleAppState>,
    Query(query): Query<EventQuery>,
) -> Result<Response, ApiError> {
    query_index(state.index, query).await
}

async fn query_index(
    index: Arc<Mutex<ServerlessEventIndex>>,
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
    insert_u64_header(
        &mut headers,
        "indexed-from-record",
        result.indexed_from_record,
    )?;
    insert_u64_header(
        &mut headers,
        "indexed-through-record",
        result.indexed_through_record,
    )?;
    insert_u64_header(
        &mut headers,
        "durable-through-record",
        result.durable_through_record,
    )?;
    Ok((headers, Json(result)).into_response())
}

fn insert_u64_header(
    headers: &mut HeaderMap,
    name: &'static str,
    value: u64,
) -> Result<(), ApiError> {
    let value = HeaderValue::from_str(&value.to_string())
        .map_err(|_error| ApiError(IndexError::InvalidQuery))?;
    headers.insert(name, value);
    Ok(())
}

fn parse_query_timestamp(value: &str) -> Option<i64> {
    value.parse::<i64>().ok().or_else(|| {
        DateTime::parse_from_rfc3339(value)
            .ok()
            .map(|value| value.timestamp_millis())
    })
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to install Ctrl+C handler");
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                let _signal = signal.recv().await;
            }
            Err(error) => tracing::error!(%error, "failed to install SIGTERM handler"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
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
    use ursula_index::EventIndexCache;
    use ursula_index::EventIndexConfig;
    use ursula_index::FsObjectStore;
    use ursula_index::IndexCatalog;
    use ursula_index::IndexError;
    use ursula_index::IndexRegistration;
    use ursula_index::ServerlessEventIndex;

    use super::PoolBackend;
    use super::PoolIndexSettings;
    use super::PoolState;
    use super::build_router;
    use super::is_deterministic_data_error;
    use super::pool_router;
    use super::process_pool_source;
    use super::reconcile_pool_indexes;

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
        let mut index = ServerlessEventIndex::open_fs(
            FsObjectStore::new(objects.path())?,
            cache.path(),
            16 * 1024 * 1024,
            EventIndexConfig {
                source_id: "http-test".to_owned(),
                flush_entries: 2,
                row_group_entries: 2,
                timestamp_field: "captured_at".to_owned(),
            },
        )
        .await?;
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
        let app = build_router(Arc::new(Mutex::new(index)));
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
        let index = ServerlessEventIndex::open_fs(
            FsObjectStore::new(objects.path())?,
            cache.path(),
            16 * 1024 * 1024,
            EventIndexConfig {
                source_id: "health-test".to_owned(),
                flush_entries: 2,
                row_group_entries: 2,
                timestamp_field: "captured_at".to_owned(),
            },
        )
        .await?;
        let app = build_router(Arc::new(Mutex::new(index)));

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
        let state = PoolState {
            catalog: IndexCatalog::open_fs(FsObjectStore::new(objects.path())?),
            backend: PoolBackend::Fs {
                object_root: objects.path().to_path_buf(),
            },
            settings: PoolIndexSettings {
                serving_cache: EventIndexCache::new(
                    cache.path().join("serving"),
                    16 * 1024 * 1024,
                )?,
                maintenance_cache: EventIndexCache::new(
                    cache.path().join("maintenance"),
                    16 * 1024 * 1024,
                )?,
                flush_entries: 2,
                row_group_entries: 2,
            },
            indexes: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        };
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
        let state = PoolState {
            catalog: IndexCatalog::open_fs(FsObjectStore::new(objects.path())?),
            backend: PoolBackend::Fs {
                object_root: objects.path().to_path_buf(),
            },
            settings: PoolIndexSettings {
                serving_cache: EventIndexCache::new(
                    cache.path().join("serving"),
                    16 * 1024 * 1024,
                )?,
                maintenance_cache: EventIndexCache::new(
                    cache.path().join("maintenance"),
                    16 * 1024 * 1024,
                )?,
                flush_entries: 2,
                row_group_entries: 2,
            },
            indexes: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        };
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
