//! The front door's size limits (extensions Section 10): what HEAD
//! advertises, and the limit every `413` names.
//!
//! A node has one request limit, [`MAX_REQUEST_BYTES`]. A snapshot publish
//! with a cold store still accepts a body of up to [`MAX_COLD_SNAPSHOT_BYTES`]
//! in one request until snapshots go up in parts, and its refusal names the
//! snapshot limit.

use axum::body::Body;
use axum::http::HeaderMap;
use axum::http::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use ursula_headers::SizeLimit;
use ursula_runtime::MAX_COLD_SNAPSHOT_BYTES;

use crate::MAX_HTTP_BODY_BYTES;
use crate::render::insert_default_response_headers;
use crate::render::insert_header_str;

/// The largest request body any request may carry
/// (`Ursula-Max-Request-Bytes`).
pub(crate) const MAX_REQUEST_BYTES: u64 = MAX_HTTP_BODY_BYTES as u64;

/// The request limit, as a `413` names it.
pub(crate) const REQUEST_LIMIT: SizeLimit = SizeLimit::Request(MAX_REQUEST_BYTES);

/// `Ursula-Max-Snapshot-Bytes`: the largest snapshot a node accepts, which
/// only a node with a cold store advertises.
pub(crate) fn max_snapshot_bytes(cold_store: bool) -> Option<u64> {
    cold_store.then_some(MAX_COLD_SNAPSHOT_BYTES)
}

/// The limit a snapshot publish applies: the snapshot limit with a cold
/// store, the request limit without one.
pub(crate) fn snapshot_publish_limit(cold_store: bool) -> SizeLimit {
    max_snapshot_bytes(cold_store).map_or(REQUEST_LIMIT, SizeLimit::Snapshot)
}

/// A `413` that names `limit`.
pub(crate) fn size_refusal(limit: SizeLimit) -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_size_limit(&mut headers, limit);
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        headers,
        "request body is too large",
    )
        .into_response()
}

fn insert_size_limit(headers: &mut HeaderMap, limit: SizeLimit) {
    limit.render(|name, value| insert_header_str(headers, name, value));
}

/// Adds the request limit to any `413` that names no limit: the bare `413`
/// of axum's body limit, and any refusal a path added later forgets to
/// type. A `413` whose limit headers are malformed is left alone, since no
/// path of this crate renders one.
pub(crate) async fn name_size_refusals(request: Request<Body>, next: Next) -> Response {
    let mut response = next.run(request).await;
    if response.status() == StatusCode::PAYLOAD_TOO_LARGE {
        let named = SizeLimit::parse(
            SizeLimit::STATUS,
            response
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        );
        if matches!(named, Ok(None)) {
            insert_size_limit(response.headers_mut(), REQUEST_LIMIT);
        }
    }
    response
}
