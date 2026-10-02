//! The indexer's internal keyed-state API (design §6.1 U17).
//!
//! - `GET /v1/keyed/{bucket}/{key}?incarnation=c&source_next=N[&key=k |
//!   start=k | after=k][&end=k][&limit=n][&min_through_record=r][&timeout_ms=t]`,
//!   where `{key}` is the stream's local name as one percent-encoded path
//!   component. Statuses and headers follow P3 (`extensions.md` §9.2); the
//!   node answers 404 and adds `Stream-Extensions`.
//! - `POST /v1/keyed/drain` with `{"bucket":"<id>"}`: blocks new work for the
//!   bucket and answers 200 once it is quiescent.
//! - `GET /__ursula/indexer/metrics`: the engine's metrics snapshot as JSON
//!   (U24).
//!
//! Row bodies are gzip-compressed when the caller accepts it.

use std::collections::HashMap;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::extract::RawQuery;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::header::CACHE_CONTROL;
use axum::http::header::CONTENT_TYPE;
use axum::http::header::RETRY_AFTER;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::routing::post;
use serde::Deserialize;
use tower_http::compression::CompressionLayer;

use super::batch::decode_key;
use super::batch::encode_key;
use super::engine::KeyedEngine;
use super::engine::KeyedReadOutcome;
use super::engine::KeyedReadRequest;
use super::engine::Selection;
use super::fold::KEYED_STATE_RESPONSE_BUDGET;
use super::fold::Lower;
use super::fold::RangeQuery;
use super::manifest::KeyedSource;
use super::metrics::INDEXER_METRICS_PATH;

/// Media type of keyed-state rows.
pub const KEYED_ROWS_CONTENT_TYPE: &str = "application/vnd.durable-stream-keyed-rows+ndjson";
/// `Stream-Keyed-Through`.
pub const HEADER_STREAM_KEYED_THROUGH: &str = "stream-keyed-through";
/// `Stream-Keyed-After`.
pub const HEADER_STREAM_KEYED_AFTER: &str = "stream-keyed-after";
const HEADER_STREAM_RECORD_NEXT: &str = "stream-record-next";
const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;
const DEFAULT_TIMEOUT_MS: u64 = 1000;
const MAX_TIMEOUT_MS: u64 = 60_000;

/// The keyed-mode router: `/v1/keyed/...`, the metrics snapshot, `/livez`
/// and `/readyz`.
pub fn router(engine: KeyedEngine) -> Router {
    Router::new()
        .route("/livez", get(|| async { StatusCode::OK }))
        .route("/readyz", get(|| async { StatusCode::OK }))
        .route("/v1/keyed/drain", post(drain))
        .route(INDEXER_METRICS_PATH, get(metrics))
        .route("/v1/keyed/{bucket}/{key}", get(read))
        .layer(CompressionLayer::new().gzip(true))
        .with_state(engine)
}

#[derive(Deserialize)]
struct DrainRequest {
    bucket: String,
}

async fn drain(State(engine): State<KeyedEngine>, Json(request): Json<DrainRequest>) -> Response {
    if request.bucket.is_empty() {
        return bad_request("bucket must not be empty");
    }
    engine.drain(&request.bucket).await;
    Json(serde_json::json!({ "bucket": request.bucket, "drained": true })).into_response()
}

async fn metrics(State(engine): State<KeyedEngine>) -> Response {
    Json(engine.metrics()).into_response()
}

fn bad_request(reason: &str) -> Response {
    (StatusCode::BAD_REQUEST, reason.to_owned()).into_response()
}

/// Parses the query, refusing repeated parameters.
fn parse_query(raw: Option<&str>) -> Result<HashMap<String, String>, String> {
    let mut params = HashMap::new();
    for (name, value) in url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes()) {
        if params
            .insert(name.clone().into_owned(), value.into_owned())
            .is_some()
        {
            return Err(format!("parameter `{name}` is repeated"));
        }
    }
    Ok(params)
}

fn parse_u64(params: &HashMap<String, String>, name: &str) -> Result<Option<u64>, String> {
    params
        .get(name)
        .map(|raw| {
            raw.parse::<u64>()
                .map_err(|_error| format!("`{name}` must be a non-negative integer"))
        })
        .transpose()
}

fn parse_key(params: &HashMap<String, String>, name: &str) -> Result<Option<Vec<u8>>, String> {
    params
        .get(name)
        .map(|raw| decode_key(raw).map_err(|error| format!("`{name}` is not a valid key: {error}")))
        .transpose()
}

fn parse_request(
    bucket: String,
    key: String,
    params: &HashMap<String, String>,
) -> Result<KeyedReadRequest, String> {
    let incarnation =
        parse_u64(params, "incarnation")?.ok_or_else(|| "`incarnation` is required".to_owned())?;
    let source_next =
        parse_u64(params, "source_next")?.ok_or_else(|| "`source_next` is required".to_owned())?;
    let selection = if let Some(point) = parse_key(params, "key")? {
        if ["start", "after", "end", "limit"]
            .iter()
            .any(|name| params.contains_key(*name))
        {
            return Err("`key` cannot be combined with start, after, end or limit".to_owned());
        }
        Selection::Point(point)
    } else {
        let lower = match (parse_key(params, "start")?, parse_key(params, "after")?) {
            (Some(_), Some(_)) => return Err("`start` and `after` are exclusive".to_owned()),
            (Some(start), None) => Lower::Start(start),
            (None, Some(after)) => Lower::After(after),
            (None, None) => Lower::First,
        };
        let limit = match params.get("limit") {
            None => DEFAULT_LIMIT,
            Some(raw) => match raw.parse::<usize>() {
                Ok(limit) if (1..=MAX_LIMIT).contains(&limit) => limit,
                _ => return Err("`limit` must be in 1..=1000".to_owned()),
            },
        };
        Selection::Range(RangeQuery {
            lower,
            end: parse_key(params, "end")?,
            limit,
            budget: Some(KEYED_STATE_RESPONSE_BUDGET),
        })
    };
    let min_through_record = parse_u64(params, "min_through_record")?;
    let timeout_ms = params
        .get("timeout_ms")
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(DEFAULT_TIMEOUT_MS)
        .clamp(1, MAX_TIMEOUT_MS);
    Ok(KeyedReadRequest {
        source: KeyedSource {
            bucket,
            key,
            incarnation,
        },
        source_next,
        selection,
        min_through_record,
        timeout: Duration::from_millis(timeout_ms),
    })
}

fn state_headers(through: u64) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(HEADER_STREAM_KEYED_THROUGH, HeaderValue::from(through));
    headers
}

async fn read(
    State(engine): State<KeyedEngine>,
    Path((bucket, key)): Path<(String, String)>,
    RawQuery(raw): RawQuery,
) -> Response {
    if bucket.is_empty() || key.is_empty() {
        return bad_request("bucket and stream must not be empty");
    }
    let request =
        match parse_query(raw.as_deref()).and_then(|params| parse_request(bucket, key, &params)) {
            Ok(request) => request,
            Err(reason) => return bad_request(&reason),
        };
    match engine.read(request).await {
        KeyedReadOutcome::Rows { through, page } => {
            let mut headers = state_headers(through);
            headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static(KEYED_ROWS_CONTENT_TYPE),
            );
            if let Some(after) = &page.after
                && let Ok(value) = HeaderValue::from_str(&encode_key(after))
            {
                headers.insert(HEADER_STREAM_KEYED_AFTER, value);
            }
            (StatusCode::OK, headers, page.body()).into_response()
        }
        KeyedReadOutcome::NotYet { through } => {
            (StatusCode::NO_CONTENT, state_headers(through)).into_response()
        }
        KeyedReadOutcome::BeyondTail { next_record } => {
            let mut headers = HeaderMap::new();
            headers.insert(HEADER_STREAM_RECORD_NEXT, HeaderValue::from(next_record));
            (
                StatusCode::BAD_REQUEST,
                headers,
                "min_through_record is beyond the source tail".to_owned(),
            )
                .into_response()
        }
        KeyedReadOutcome::Failed(reason) => {
            (StatusCode::INTERNAL_SERVER_ERROR, reason).into_response()
        }
        KeyedReadOutcome::Unavailable(reason) => {
            let mut headers = HeaderMap::new();
            headers.insert(RETRY_AFTER, HeaderValue::from_static("1"));
            (StatusCode::SERVICE_UNAVAILABLE, headers, reason).into_response()
        }
    }
}
