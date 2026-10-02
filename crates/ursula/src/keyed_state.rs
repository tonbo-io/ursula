//! `GET {stream_url}/keyed-state` (keyed-streams P3, design U7;
//! `extensions.md` §9.2): the node-side proxy in front of the indexer's
//! internal `/v1/keyed` API.
//!
//! The node validates the parameters, resolves the stream from its own state
//! (404 when absent, not keyed or not served; the beyond-tail 400), and
//! forwards the read with the stream incarnation (`created_at_ms`) and its
//! record tail `N` as `source_next`. The indexer owns `state(D)`, waiting,
//! and the 200/204/500/503 answers; the node maps them, adds
//! `Stream-Extensions: keyed-state-v1` and `Cache-Control: no-store`, and
//! lets the client router's compression layer gzip the rows. The read goes
//! to the first healthy indexer pod of the configured failover order
//! ([`crate::keyed_upstream`]).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::OriginalUri;
use axum::extract::Path;
use axum::extract::RawQuery;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Method;
use axum::http::StatusCode;
use axum::http::header::ALLOW;
use axum::http::header::CONTENT_TYPE;
use axum::http::header::RETRY_AFTER;
use axum::response::IntoResponse;
use axum::response::Response;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::HeadStreamResponse;
use ursula_shard::BucketStreamId;

use crate::DEFAULT_LONG_POLL_TIMEOUT_MS;
use crate::HEADER_STREAM_RECORD_NEXT;
use crate::HttpState;
use crate::MAX_LONG_POLL_TIMEOUT_MS;
use crate::StreamPath;
use crate::insert_extension_token;
use crate::keyed_upstream::UpstreamAnswer;
use crate::render::insert_cache_control;
use crate::render::insert_content_type;
use crate::render::insert_default_response_headers;
use crate::render::insert_u64_header;
use crate::request_target;
use crate::runtime_error_or_leader_redirect_async;

/// Extension token of the keyed-state read resource.
pub(crate) const KEYED_STATE_EXTENSION: &str = "keyed-state-v1";
/// Media type of a 200 body: one `{"key","record","value"}` row per line.
pub(crate) const KEYED_ROWS_CONTENT_TYPE: &str = "application/vnd.durable-stream-keyed-rows+ndjson";
pub(crate) const HEADER_STREAM_KEYED_THROUGH: &str = "stream-keyed-through";
pub(crate) const HEADER_STREAM_KEYED_AFTER: &str = "stream-keyed-after";

const DEFAULT_LIMIT: u64 = 100;
const MAX_LIMIT: u64 = 1_000;
/// Indexer budget for a request that does not wait: generous, because a cold
/// namespace may be built from the source log on first read.
const UPSTREAM_BASE_TIMEOUT: Duration = Duration::from_secs(30);
const RETRY_AFTER_SECS: &str = "1";

pub use crate::keyed_upstream::FailoverOptions;
pub use crate::keyed_upstream::KeyedStateUpstream;

/// `{base}/v1/keyed/{bucket}/{key}?…`, with `{key}` the stream's local name
/// (`stream` or `affinity/stream`) as one percent-encoded segment. `None`
/// when the base cannot carry a path.
fn request_url(
    base: &url::Url,
    stream_id: &BucketStreamId,
    incarnation: u64,
    source_next: u64,
    params: &KeyedStateParams,
) -> Option<url::Url> {
    let local_name = match &stream_id.affinity_key {
        Some(affinity) => format!("{affinity}/{}", stream_id.stream_id),
        None => stream_id.stream_id.clone(),
    };
    let mut url = base.clone();
    url.path_segments_mut().ok()?.pop_if_empty().extend([
        "v1",
        "keyed",
        &stream_id.bucket_id,
        &local_name,
    ]);
    url.set_query(None);
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("incarnation", &incarnation.to_string());
        query.append_pair("source_next", &source_next.to_string());
        match &params.selection {
            Selection::Point(key) => {
                query.append_pair("key", key);
            }
            Selection::Range { lower, end, limit } => {
                match lower {
                    Some(Lower::Start(key)) => {
                        query.append_pair("start", key);
                    }
                    Some(Lower::After(key)) => {
                        query.append_pair("after", key);
                    }
                    None => {}
                }
                if let Some(end) = end {
                    query.append_pair("end", end);
                }
                query.append_pair("limit", &limit.to_string());
            }
        }
        if let Some(wait) = params.wait {
            query.append_pair("min_through_record", &wait.min_through_record.to_string());
            query.append_pair("timeout_ms", &wait.timeout_ms.to_string());
        }
    }
    Some(url)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Lower {
    Start(String),
    After(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Selection {
    Point(String),
    Range {
        lower: Option<Lower>,
        end: Option<String>,
        limit: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Wait {
    min_through_record: u64,
    timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyedStateParams {
    selection: Selection,
    wait: Option<Wait>,
}

/// Parses the query of a keyed-state GET per `extensions.md` §9.2.1. Unknown
/// parameters are ignored, as on the other read surfaces; a repeated
/// parameter of any name is rejected.
fn parse_params(raw_query: Option<&str>) -> Result<KeyedStateParams, String> {
    let mut values: HashMap<String, String> = HashMap::new();
    for (name, value) in url::form_urlencoded::parse(raw_query.unwrap_or_default().as_bytes()) {
        if values
            .insert(name.to_string(), value.into_owned())
            .is_some()
        {
            return Err(format!("repeated parameter {name}"));
        }
    }
    let key_param = |name: &str| -> Result<Option<String>, String> {
        match values.get(name) {
            None => Ok(None),
            Some(value) => match ursula_index::keyed::decode_key(value) {
                Ok(_) => Ok(Some(value.clone())),
                Err(err) => Err(format!("invalid {name}: {err}")),
            },
        }
    };
    let key = key_param("key")?;
    let start = key_param("start")?;
    let after = key_param("after")?;
    let end = key_param("end")?;
    let limit = match values.get("limit") {
        None => None,
        Some(raw) => match raw.parse::<u64>() {
            Ok(limit) if (1..=MAX_LIMIT).contains(&limit) => Some(limit),
            _ => return Err(format!("limit must be an integer in 1..={MAX_LIMIT}")),
        },
    };
    let selection = match key {
        Some(key) => {
            if start.is_some() || after.is_some() || end.is_some() || limit.is_some() {
                return Err("key cannot be combined with start, after, end or limit".to_owned());
            }
            Selection::Point(key)
        }
        None => {
            let lower = match (start, after) {
                (Some(_), Some(_)) => {
                    return Err("start and after are mutually exclusive".to_owned());
                }
                (Some(start), None) => Some(Lower::Start(start)),
                (None, Some(after)) => Some(Lower::After(after)),
                (None, None) => None,
            };
            Selection::Range {
                lower,
                end,
                limit: limit.unwrap_or(DEFAULT_LIMIT),
            }
        }
    };
    let wait = match values.get("min_through_record") {
        None => None,
        Some(raw) => {
            let min_through_record = raw
                .parse::<u64>()
                .map_err(|_| "min_through_record must be a non-negative integer".to_owned())?;
            // The house long-poll rule: an unparseable value means the
            // default, and a parsed one is clamped.
            let timeout_ms = values
                .get("timeout_ms")
                .and_then(|raw| raw.parse::<u64>().ok())
                .unwrap_or(DEFAULT_LONG_POLL_TIMEOUT_MS)
                .clamp(1, MAX_LONG_POLL_TIMEOUT_MS);
            Some(Wait {
                min_through_record,
                timeout_ms,
            })
        }
    };
    Ok(KeyedStateParams { selection, wait })
}

/// Handler for every method on both forms of `{stream_url}/keyed-state`.
/// Routed with `any` so `HEAD` is never answered by running the GET (and its
/// wait); only `GET` is defined (P3.1).
/// Every response is counted in the keyed-state request metrics (U24).
pub(crate) async fn keyed_state(
    State(state): State<HttpState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<StreamPath>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let response = serve(&state, method, uri, path, raw_query).await;
    state.keyed_state_metrics.record(response.status());
    response
}

async fn serve(
    state: &HttpState,
    method: Method,
    uri: axum::http::Uri,
    path: StreamPath,
    raw_query: Option<String>,
) -> Response {
    let Some(upstream) = state.keyed_state_upstream.clone() else {
        return not_served();
    };
    let stream_id = path.into_stream_id();
    if method != Method::GET {
        let mut headers = HeaderMap::new();
        insert_default_response_headers(&mut headers);
        headers.insert(ALLOW, HeaderValue::from_static("GET"));
        let response = (
            StatusCode::METHOD_NOT_ALLOWED,
            headers,
            "keyed-state only supports GET",
        )
            .into_response();
        return advertise_if_keyed(state, stream_id, response).await;
    }
    let params = match parse_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(reason) => {
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            let response = (StatusCode::BAD_REQUEST, headers, reason).into_response();
            return advertise_if_keyed(state, stream_id, response).await;
        }
    };
    let head = match head(state, stream_id.clone()).await {
        Ok(head) => head,
        Err(err) => {
            return runtime_error_or_leader_redirect_async(state, err, &request_target(&uri)).await;
        }
    };
    if !ursula_shard::is_keyed_batch_content_type(&head.content_type) {
        return not_served();
    }
    let source_next = head
        .record_range
        .map(|range| range.next_record)
        .unwrap_or_default();
    if let Some(wait) = params.wait
        && wait.min_through_record > source_next
    {
        let mut headers = keyed_headers();
        insert_u64_header(&mut headers, HEADER_STREAM_RECORD_NEXT, source_next);
        return (
            StatusCode::BAD_REQUEST,
            headers,
            format!(
                "min_through_record {} exceeds the record tail {source_next}",
                wait.min_through_record
            ),
        )
            .into_response();
    }
    let Some(incarnation) = head.created_at_ms else {
        // Answered by a peer too old to report the incarnation.
        return unavailable("stream incarnation is not known yet");
    };
    forward(&upstream, &stream_id, incarnation, source_next, &params).await
}

async fn head(
    state: &HttpState,
    stream_id: BucketStreamId,
) -> Result<HeadStreamResponse, ursula_runtime::RuntimeError> {
    state
        .runtime
        .head_stream(HeadStreamRequest {
            stream_id,
            now_ms: state.unix_time_ms(),
        })
        .await
}

/// Adds `keyed-state-v1` to an early (405 or parameter 400) answer when the
/// stream resolves locally as keyed. Responses for other streams, or when the
/// stream cannot be resolved here, never carry the token (P3.10).
async fn advertise_if_keyed(
    state: &HttpState,
    stream_id: BucketStreamId,
    mut response: Response,
) -> Response {
    if head(state, stream_id)
        .await
        .is_ok_and(|head| ursula_shard::is_keyed_batch_content_type(&head.content_type))
    {
        insert_extension_token(response.headers_mut(), KEYED_STATE_EXTENSION);
    }
    response
}

/// 404 when the stream is absent, not keyed, or keyed state is not served
/// (no upstream configured). It never advertises `keyed-state-v1`.
pub(crate) fn not_served() -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    (
        StatusCode::NOT_FOUND,
        headers,
        "keyed-state is not served for this stream",
    )
        .into_response()
}

fn keyed_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_cache_control(&mut headers, "no-store");
    insert_extension_token(&mut headers, KEYED_STATE_EXTENSION);
    headers
}

fn unavailable(reason: &str) -> Response {
    let mut headers = keyed_headers();
    headers.insert(RETRY_AFTER, HeaderValue::from_static(RETRY_AFTER_SECS));
    (StatusCode::SERVICE_UNAVAILABLE, headers, reason.to_owned()).into_response()
}

async fn forward(
    upstream: &Arc<KeyedStateUpstream>,
    stream_id: &BucketStreamId,
    incarnation: u64,
    source_next: u64,
    params: &KeyedStateParams,
) -> Response {
    let answer = upstream
        .get(
            params.wait.map(|wait| wait.timeout_ms),
            UPSTREAM_BASE_TIMEOUT,
            |base, wait_ms| {
                let mut params = params.clone();
                if let (Some(wait), Some(timeout_ms)) = (params.wait.as_mut(), wait_ms) {
                    wait.timeout_ms = timeout_ms;
                }
                request_url(base, stream_id, incarnation, source_next, &params)
            },
        )
        .await;
    let Some(UpstreamAnswer {
        status,
        headers: upstream_headers,
        body,
    }) = answer
    else {
        tracing::warn!(
            bucket = %stream_id.bucket_id,
            stream = %stream_id.stream_id,
            "no keyed-state indexer pod answered"
        );
        return unavailable("keyed-state upstream is unavailable");
    };
    let through = upstream_headers
        .get(HEADER_STREAM_KEYED_THROUGH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let after = upstream_headers.get(HEADER_STREAM_KEYED_AFTER).cloned();
    let retry_after = upstream_headers.get(RETRY_AFTER).cloned();
    let mut headers = keyed_headers();
    match status.as_u16() {
        200 | 204 => {
            // P3.4: every 200 and 204 names its `D`; an answer without it
            // cannot be served as `state(D)`.
            let Some(through) = through else {
                tracing::warn!(
                    bucket = %stream_id.bucket_id,
                    stream = %stream_id.stream_id,
                    %status,
                    "keyed-state upstream answer lacks Stream-Keyed-Through"
                );
                return unavailable("keyed-state upstream answer is incomplete");
            };
            insert_u64_header(&mut headers, HEADER_STREAM_KEYED_THROUGH, through);
            if status == StatusCode::NO_CONTENT {
                return (StatusCode::NO_CONTENT, headers).into_response();
            }
            if let Some(after) = after {
                headers.insert(HEADER_STREAM_KEYED_AFTER, after);
            }
            insert_content_type(&mut headers, KEYED_ROWS_CONTENT_TYPE);
            (StatusCode::OK, headers, body).into_response()
        }
        500 | 400 => {
            headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            (status, headers, body).into_response()
        }
        503 => {
            headers.insert(
                RETRY_AFTER,
                retry_after.unwrap_or(HeaderValue::from_static(RETRY_AFTER_SECS)),
            );
            headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            (StatusCode::SERVICE_UNAVAILABLE, headers, body).into_response()
        }
        _ => {
            tracing::warn!(
                bucket = %stream_id.bucket_id,
                stream = %stream_id.stream_id,
                %status,
                "unexpected keyed-state upstream status"
            );
            unavailable("keyed-state upstream answered unexpectedly")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(lower: Option<Lower>, end: Option<&str>, limit: u64) -> Selection {
        Selection::Range {
            lower,
            end: end.map(str::to_owned),
            limit,
        }
    }

    #[test]
    fn parses_point_and_range_reads_with_defaults() {
        assert_eq!(parse_params(None).expect("empty query"), KeyedStateParams {
            selection: range(None, None, DEFAULT_LIMIT),
            wait: None,
        });
        assert_eq!(
            parse_params(Some("key=AQ")).expect("point").selection,
            Selection::Point("AQ".to_owned())
        );
        assert_eq!(
            parse_params(Some("start=AQ&end=Ag&limit=1000"))
                .expect("range")
                .selection,
            range(Some(Lower::Start("AQ".to_owned())), Some("Ag"), 1000)
        );
        assert_eq!(
            parse_params(Some("after=AQ&limit=1"))
                .expect("after")
                .selection,
            range(Some(Lower::After("AQ".to_owned())), None, 1)
        );
        // lo >= hi is not an error: it selects nothing.
        assert!(parse_params(Some("start=Ag&end=AQ")).is_ok());
        // Unknown parameters are ignored.
        assert!(parse_params(Some("other=1")).is_ok());
        // Percent-encoded base64url characters decode before the check.
        assert_eq!(
            parse_params(Some("key=A%51")).expect("encoded").selection,
            Selection::Point("AQ".to_owned())
        );
    }

    #[test]
    fn timeout_follows_the_house_long_poll_rule_and_needs_a_wait() {
        let wait = |query: &str| parse_params(Some(query)).expect("valid").wait;
        assert_eq!(wait("timeout_ms=5"), None);
        for (query, expected) in [
            ("min_through_record=3", 1_000),
            ("min_through_record=3&timeout_ms=250", 250),
            ("min_through_record=3&timeout_ms=0", 1),
            ("min_through_record=3&timeout_ms=999999", 60_000),
            ("min_through_record=3&timeout_ms=soon", 1_000),
            ("min_through_record=3&timeout_ms=-5", 1_000),
        ] {
            assert_eq!(
                wait(query),
                Some(Wait {
                    min_through_record: 3,
                    timeout_ms: expected,
                }),
                "{query}"
            );
        }
    }

    #[test]
    fn rejects_invalid_parameters() {
        let long_key = "A".repeat(5463);
        for query in [
            "key=AQ&key=AQ",
            "limit=1&limit=2",
            "timeout_ms=1&timeout_ms=2",
            "other=1&other=2",
            "key=AQ&start=AQ",
            "key=AQ&after=AQ",
            "key=AQ&end=AQ",
            "key=AQ&limit=5",
            "start=AQ&after=AQ",
            "key=",
            "key=AB",
            "key=AA%3D%3D",
            "key=%2B%2F",
            "key=AQ%3D",
            "start=not*base64",
            &format!("end={long_key}"),
            "limit=0",
            "limit=1001",
            "limit=",
            "limit=ten",
            "limit=-1",
            "min_through_record=",
            "min_through_record=-1",
            "min_through_record=x",
            "min_through_record=18446744073709551616",
        ] {
            assert!(parse_params(Some(query)).is_err(), "{query}");
        }
    }

    #[test]
    fn request_url_encodes_the_local_name_as_one_segment() {
        let base = url::Url::parse("http://indexer:9000/prefix/").expect("base");
        let params =
            parse_params(Some("start=AQ&min_through_record=4&timeout_ms=99999")).expect("params");
        let url = request_url(
            &base,
            &BucketStreamId::with_affinity("b", "run 1", "a%b"),
            7,
            9,
            &params,
        )
        .expect("url");
        assert_eq!(
            url.as_str(),
            "http://indexer:9000/prefix/v1/keyed/b/run%201%2Fa%25b?incarnation=7&source_next=9&start=AQ&limit=100&min_through_record=4&timeout_ms=60000"
        );
    }
}
