//! The front door's size limits (extensions Section 10): what HEAD
//! advertises, and the limit every `413` names.
//!
//! Every `413` names its limit in a header and in its error body's code
//! (`RequestTooLarge` or `SnapshotTooLarge`).
//!
//! A node has one request limit, [`MAX_REQUEST_BYTES`], for every request.
//! A node with a cold store also accepts snapshots of up to
//! [`MAX_COLD_SNAPSHOT_BYTES`] uploaded in parts ([`crate::snapshot_parts`]),
//! and a refusal of an assembled snapshot names that limit,
//! [`SNAPSHOT_LIMIT`].

use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::Response;
use ursula_headers::SizeLimit;
use ursula_runtime::MAX_COLD_SNAPSHOT_BYTES;

use crate::render::error_answer;
use crate::render::insert_header_str;

/// The largest request body any request may carry
/// (`Ursula-Max-Request-Bytes`).
pub(crate) const MAX_REQUEST_BYTES: u64 = ursula_headers::MAX_REQUEST_BYTES;

/// The request limit, as a `413` names it.
pub(crate) const REQUEST_LIMIT: SizeLimit = SizeLimit::Request(MAX_REQUEST_BYTES);

/// The snapshot limit, as the `413` of an assembled snapshot names it.
pub(crate) const SNAPSHOT_LIMIT: SizeLimit = SizeLimit::Snapshot(MAX_COLD_SNAPSHOT_BYTES);

/// `Ursula-Max-Snapshot-Bytes`: the largest snapshot a node accepts in
/// parts, which only a node with a cold store advertises.
pub(crate) fn max_snapshot_bytes(cold_store: bool) -> Option<u64> {
    cold_store.then_some(MAX_COLD_SNAPSHOT_BYTES)
}

/// A `413` that names `limit`.
pub(crate) fn size_refusal(limit: SizeLimit) -> Response {
    let mut headers = HeaderMap::new();
    insert_size_limit(&mut headers, limit);
    let message = match limit {
        SizeLimit::Request(bytes) => {
            format!("the request body is above the request limit of {bytes} bytes")
        }
        SizeLimit::Snapshot(bytes) => {
            format!("the snapshot is above the snapshot limit of {bytes} bytes")
        }
    };
    error_answer(
        StatusCode::PAYLOAD_TOO_LARGE,
        limit.error_code(),
        message,
        headers,
    )
}

fn insert_size_limit(headers: &mut HeaderMap, limit: SizeLimit) {
    limit.render(|name, value| insert_header_str(headers, name, value));
}

/// Adds the request limit to a `413` that names no limit: the bare `413`
/// of axum's body limit, and any refusal a path added later forgets to
/// type. A `413` whose limit headers are malformed is left alone, since no
/// path of this crate renders one.
pub(crate) fn name_unnamed_size_refusal(response: &mut Response) {
    if response.status() != StatusCode::PAYLOAD_TOO_LARGE {
        return;
    }
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
