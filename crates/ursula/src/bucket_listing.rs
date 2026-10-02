//! Bucket listing across Raft groups (`extensions.md` §1.4, RT3).
//!
//! With subset voter placement no node hosts every group. The node serving
//! `GET /{bucket}/streams` lists its hosted groups from local replica state
//! and fetches each other group's share from one of that group's voters
//! through [`GROUP_SHARE_PATH`]. A group whose share cannot be fetched fails
//! the listing (retryable 503); it is never silently skipped.

use axum::extract::Path;
use axum::extract::RawQuery;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use ursula_runtime::BucketStreamListing;
use ursula_runtime::ListBucketStreamsRequest;
use ursula_runtime::ListBucketStreamsResponse;
use ursula_runtime::RuntimeError;
use ursula_shard::RaftGroupId;

use crate::HttpState;
use crate::parse_query;
use crate::runtime_error_response;

/// Internal client-plane route answering one group's listing share from
/// this node's local replica state.
pub(crate) const GROUP_SHARE_PATH: &str = "/__ursula/bucket-streams/{raft_group_id}/{bucket}";

/// Largest per-group share a peer may ask for (the public limit plus one).
const MAX_SHARE_LIMIT: usize = crate::BUCKET_LISTING_MAX_LIMIT + 1;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct GroupShare {
    /// `None` when the group does not know the bucket.
    streams: Option<Vec<BucketStreamListing>>,
}

/// Lists `bucket` across every group: hosted groups locally, the others from
/// one of their voters. `Ok(None)` when no group knows the bucket.
pub(crate) async fn list_across_groups(
    state: &HttpState,
    bucket: &str,
    prefix: &str,
    after: Option<&str>,
    limit: usize,
) -> Result<Option<ListBucketStreamsResponse>, Response> {
    let request = ListBucketStreamsRequest {
        bucket_id: bucket.to_owned(),
        prefix: prefix.to_owned(),
        after: after.map(str::to_owned),
        limit: limit.saturating_add(1),
        now_ms: state.unix_time_ms(),
    };
    let mut shares = Vec::new();
    for group_id in 0..state.runtime.raft_group_count() {
        let group = RaftGroupId(group_id);
        match state
            .runtime
            .list_bucket_streams(group, request.clone())
            .await
        {
            Ok(share) => shares.push(share),
            Err(RuntimeError::GroupNotHosted { .. }) => {
                shares.push(fetch_remote_share(state, group, &request).await?);
            }
            Err(err) => return Err(runtime_error_response(err)),
        }
    }
    Ok(ursula_runtime::merge_bucket_stream_listings(shares, limit))
}

/// Fetches one group's share from its voters, trying each in turn.
async fn fetch_remote_share(
    state: &HttpState,
    group: RaftGroupId,
    request: &ListBucketStreamsRequest,
) -> Result<Option<Vec<BucketStreamListing>>, Response> {
    let unavailable = |message: String| {
        tracing::warn!(raft_group = group.0, error = %message, "bucket listing share unavailable");
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "1")],
            message,
        )
            .into_response()
    };
    let Some(router) = state.client_write_router.as_ref() else {
        return Err(unavailable(format!(
            "raft group {} is not hosted here and no peer topology is configured",
            group.0
        )));
    };
    let bases = router.group_voter_bases(group);
    if bases.is_empty() {
        return Err(unavailable(format!(
            "raft group {} is not hosted here and has no reachable voter",
            group.0
        )));
    }
    let mut failures = Vec::new();
    for base in bases {
        let url = format!(
            "{base}/__ursula/bucket-streams/{}/{}",
            group.0, request.bucket_id
        );
        let mut query = vec![
            ("prefix", request.prefix.clone()),
            ("limit", request.limit.to_string()),
            ("now_ms", request.now_ms.to_string()),
        ];
        if let Some(after) = &request.after {
            query.push(("after", after.clone()));
        }
        let result = router.peer_client().get(&url).query(&query).send().await;
        match result {
            Ok(response) if response.status() == reqwest::StatusCode::OK => {
                match response.json::<GroupShare>().await {
                    Ok(share) => return Ok(share.streams),
                    Err(err) => failures.push(format!("{url}: {err}")),
                }
            }
            Ok(response) => failures.push(format!("{url}: {}", response.status())),
            Err(err) => failures.push(format!("{url}: {err}")),
        }
    }
    Err(unavailable(format!(
        "raft group {} listing share unavailable: {}",
        group.0,
        failures.join("; ")
    )))
}

/// `GET /__ursula/bucket-streams/{raft_group_id}/{bucket}?prefix=&after=&limit=&now_ms=`:
/// one group's share from this node's local replica state. Never forwards.
pub(crate) async fn group_share(
    State(state): State<HttpState>,
    Path((raft_group_id, bucket)): Path<(u32, String)>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    if let Err(message) = ursula_runtime::validate_bucket_id(&bucket) {
        return (StatusCode::BAD_REQUEST, message).into_response();
    }
    let query = match parse_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(response) => return *response,
    };
    let limit = match query.get("limit").map(|raw| raw.parse::<usize>()) {
        Some(Ok(limit)) if (1..=MAX_SHARE_LIMIT).contains(&limit) => limit,
        _ => return (StatusCode::BAD_REQUEST, "invalid limit").into_response(),
    };
    let now_ms = match query.get("now_ms").map(|raw| raw.parse::<u64>()) {
        Some(Ok(now_ms)) => now_ms,
        _ => return (StatusCode::BAD_REQUEST, "invalid now_ms").into_response(),
    };
    let request = ListBucketStreamsRequest {
        bucket_id: bucket,
        prefix: query.get("prefix").cloned().unwrap_or_default(),
        after: query.get("after").cloned(),
        limit,
        now_ms,
    };
    match state
        .runtime
        .list_bucket_streams(RaftGroupId(raft_group_id), request)
        .await
    {
        Ok(streams) => axum::Json(GroupShare { streams }).into_response(),
        Err(err) => runtime_error_response(err),
    }
}
