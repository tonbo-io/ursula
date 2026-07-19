use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::Json;
use axum::Router;
use axum::extract::Query;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use chrono::DateTime;
use clap::ArgGroup;
use clap::Parser;
use reqwest::Url;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;
use tokio::sync::watch;
use ursula_event_index::EventIndexConfig;
use ursula_event_index::FsObjectStore;
use ursula_event_index::IndexError;
use ursula_event_index::IndexStatus;
use ursula_event_index::QueryCursor;
use ursula_event_index::S3ObjectStore;
use ursula_event_index::S3ObjectStoreConfig;
use ursula_event_index::ServerlessEventIndex;
use ursula_event_index::SourceBatch;
use ursula_event_index::SourceClient;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "S3-backed client-event-time index for an Ursula JSON stream",
    group(ArgGroup::new("backend").required(true).args(["object_dir", "s3_bucket"]))
)]
struct Args {
    #[arg(long)]
    stream_url: Url,
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
    #[arg(long, default_value = "127.0.0.1:4493")]
    listen: SocketAddr,
    #[arg(long, default_value_t = 65_536)]
    flush_entries: usize,
    #[arg(long, default_value_t = 16_384)]
    row_group_entries: usize,
    #[arg(long, default_value_t = 4_096)]
    read_batch_records: usize,
    #[arg(long, default_value_t = 250)]
    poll_interval_ms: u64,
    #[arg(long, default_value_t = 8)]
    compact_parts: usize,
    #[arg(long, default_value = "captured_at")]
    timestamp_field: String,
}

#[derive(Clone)]
struct AppState {
    index: Arc<Mutex<ServerlessEventIndex>>,
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
    indexed_through_record: u64,
    durable_through_record: u64,
    parts: usize,
}

#[derive(Debug)]
struct ApiError(IndexError);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0 {
            IndexError::InvalidQuery | IndexError::InvalidTimestamp { .. } => {
                StatusCode::BAD_REQUEST
            }
            IndexError::RetentionGap { .. } | IndexError::Blocked { .. } => StatusCode::CONFLICT,
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
    let _observability = ursula_observability::init(ursula_observability::InitOptions::new(
        "ursula-event-indexer",
    ));
    let args = Args::parse();
    if args.compact_parts < 2 {
        anyhow::bail!("--compact-parts must be at least 2");
    }
    let config = EventIndexConfig {
        source_id: args.stream_url.to_string(),
        flush_entries: args.flush_entries,
        row_group_entries: args.row_group_entries,
        timestamp_field: args.timestamp_field,
    };
    let index = if let Some(object_dir) = &args.object_dir {
        ServerlessEventIndex::open_fs(
            FsObjectStore::new(object_dir).context("open filesystem object store")?,
            &args.cache_dir,
            args.cache_max_bytes,
            config,
        )
        .await
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
        ServerlessEventIndex::open_s3(store, &args.cache_dir, args.cache_max_bytes, config).await
    }
    .context("open event index")?;
    let index = Arc::new(Mutex::new(index));
    let source = SourceClient::new(args.stream_url.clone(), args.read_batch_records)
        .context("configure source client")?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let sync_task = tokio::spawn(sync_loop(
        source,
        Arc::clone(&index),
        Duration::from_millis(args.poll_interval_ms),
        args.compact_parts,
        shutdown_rx,
    ));

    let app = build_router(Arc::clone(&index));
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    tracing::info!(
        listen = %args.listen,
        stream_url = %args.stream_url,
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
    index
        .lock()
        .await
        .flush()
        .await
        .context("flush event index on shutdown")?;
    Ok(())
}

async fn sync_loop(
    source: SourceClient,
    index: Arc<Mutex<ServerlessEventIndex>>,
    poll_interval: Duration,
    compact_parts: usize,
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
                            let reason = error.to_string();
                            let blocked_record = index.indexed_through_record();
                            index.mark_blocked(blocked_record, reason.clone()).await?;
                            return Err(IndexError::Blocked {
                                record: blocked_record,
                                reason: format!("source record {record}: {reason}"),
                            });
                        }
                    }
                    if index.part_count() >= compact_parts {
                        index.compact_all().await?;
                    }
                    Ok(())
                }
                .await;
                match update {
                    Ok(()) => continue,
                    Err(error) => tracing::error!(%error, "event index source processing blocked"),
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
            Err(error) => tracing::warn!(%error, from_record, "source read failed; retrying"),
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

fn build_router(index: Arc<Mutex<ServerlessEventIndex>>) -> Router {
    Router::new()
        .route("/v1/events", get(query_events))
        .route("/v1/status", get(index_status))
        .with_state(AppState { index })
}

async fn index_status(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let mut index = state.index.lock().await;
    index.refresh().await.map_err(ApiError)?;
    Ok(Json(StatusBody {
        status: index.status().clone(),
        indexed_through_record: index.indexed_through_record(),
        durable_through_record: index.durable_through_record(),
        parts: index.part_count(),
    }))
}

async fn query_events(
    State(state): State<AppState>,
    Query(query): Query<EventQuery>,
) -> Result<impl IntoResponse, ApiError> {
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
    let result = state
        .index
        .lock()
        .await
        .query(from_ms, until_ms, after, query.through_record, query.limit)
        .await
        .map_err(ApiError)?;
    let mut headers = HeaderMap::new();
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
    Ok((headers, Json(result)))
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
    use tempfile::TempDir;
    use tokio::sync::Mutex;
    use tower::ServiceExt;
    use ursula_event_index::EventEntry;
    use ursula_event_index::EventIndexConfig;
    use ursula_event_index::FsObjectStore;
    use ursula_event_index::ServerlessEventIndex;

    use super::build_router;

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
}
