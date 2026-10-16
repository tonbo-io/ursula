//! Immutable reads (extensions Section 4, #581 section 5): the two resources
//! of one incarnation of a stream, named by its token and an offset in the
//! path, so that a shared cache can serve the bytes below the tail and the
//! published snapshots.
//!
//! - `GET {stream}/incarnation/{token}/chunk/{offset}`: the stream's bytes
//!   from `offset` to the end of the chunk of [`CHUNK_GRID_BYTES`] that holds
//!   it, or to the tail.
//! - `GET {stream}/incarnation/{token}/snapshot/{offset}`: the snapshot
//!   published at `offset`, with the byte ranges of a snapshot read.
//!
//! Both answer `400` for a query component or a token or offset that is not
//! in the form the server issues, and then evaluate like any read with the
//! path's token as the `Stream-Incarnation` precondition: `404`, `410` and
//! `416` before `412`. The request's own `Stream-Incarnation` header has no
//! effect: the path names the token. The node marks what may be cached and
//! keeps `Cache-Control: no-store`; a layer in front relaxes it.

use axum::extract::OriginalUri;
use axum::extract::Path;
use axum::extract::RawQuery;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::Method;
use axum::http::StatusCode;
use axum::response::Response;
use ursula_headers::ErrorCode;
use ursula_headers::HeaderError;
use ursula_headers::ImmutableResource;
use ursula_headers::ResourceKind;
use ursula_runtime::FollowerRead;
use ursula_runtime::ReadStreamRequest;
use ursula_shard::BucketStreamId;

use crate::BoxResponse;
use crate::HttpState;
use crate::READ_MAX_RESPONSE_BYTES;
use crate::SnapshotAnswer;
use crate::SnapshotBody;
use crate::SnapshotResource;
use crate::read_at_token;
use crate::read_precondition_failed;
use crate::render::chunk_response;
use crate::render::error_response;
use crate::request_target;
use crate::runtime_error_or_leader_redirect_async;
use crate::snapshot_answer;

/// The route of a chunk.
pub(crate) const CHUNK_ROUTE: &str = "/{bucket}/{stream}/incarnation/{token}/chunk/{offset}";
/// The route of a snapshot of one incarnation.
pub(crate) const SNAPSHOT_ROUTE: &str = "/{bucket}/{stream}/incarnation/{token}/snapshot/{offset}";

/// Ursula's chunk grid (extensions Section 4.2): chunk boundaries are the
/// multiples of 4 MiB in the stream's offset space, so readers that start
/// anywhere request the same URLs after one answer. Changing it never makes
/// an answer wrong, only caches miss once.
pub(crate) const CHUNK_GRID_BYTES: u64 = 4 * 1024 * 1024;
// A chunk fits one response, so an answer below the tail always reaches the
// end of its chunk.
const _: () = assert!(CHUNK_GRID_BYTES <= READ_MAX_RESPONSE_BYTES as u64);

/// The length of a chunk read from `offset`: to the end of the chunk that
/// holds it. The read plan ends it at the tail when the tail comes first.
pub(crate) fn chunk_len(offset: u64) -> usize {
    let into_chunk = offset.checked_rem(CHUNK_GRID_BYTES).unwrap_or(0);
    // At most the grid, which the assertion keeps within the response cap.
    usize::try_from(CHUNK_GRID_BYTES.saturating_sub(into_chunk)).unwrap_or(READ_MAX_RESPONSE_BYTES)
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct ImmutablePath {
    bucket: String,
    stream: String,
    token: String,
    offset: String,
}

impl ImmutablePath {
    /// The stream and the resource the path names, or the `400` of a query
    /// component or a malformed segment.
    fn parse(
        self,
        kind: ResourceKind,
        raw_query: Option<&str>,
    ) -> Result<(BucketStreamId, ImmutableResource), BoxResponse> {
        if raw_query.is_some() {
            return Err(Box::new(bad_request("immutable resources take no query")));
        }
        let resource = ImmutableResource::parse(kind, &self.token, &self.offset)
            .map_err(|err| Box::new(malformed(&err)))?;
        Ok((BucketStreamId::new(self.bucket, self.stream), resource))
    }
}

fn bad_request(message: &'static str) -> Response {
    error_response(
        StatusCode::BAD_REQUEST,
        ErrorCode::QueryNotAccepted,
        message,
    )
}

fn malformed(err: &HeaderError) -> Response {
    error_response(
        StatusCode::BAD_REQUEST,
        ErrorCode::InvalidResource,
        format!("malformed immutable resource: {err}"),
    )
}

#[tracing::instrument(
    name = "http.chunk_read",
    skip_all,
    fields(bucket = %path.bucket, stream = %path.stream, offset = %path.offset),
)]
pub(crate) async fn read_chunk(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<ImmutablePath>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let (stream_id, resource) = match path.parse(ResourceKind::Chunk, raw_query.as_deref()) {
        Ok(parsed) => parsed,
        Err(response) => return *response,
    };
    let token = resource.incarnation.get();
    let offset = resource.offset.get();
    let max_len = chunk_len(offset);
    // A follower serves only an answer it can mark, and asks the leader
    // otherwise, so an unmarked answer always means the tail lies within
    // the chunk (#581 section 5.3).
    let read = read_at_token(
        &state,
        ReadStreamRequest {
            stream_id,
            offset,
            max_len,
            now_ms: state.unix_time_ms(),
            leader_only: false,
            read_index: None,
            follower: FollowerRead::Immutable,
        },
        Some(token),
    )
    .await;
    match read {
        Ok(response) => {
            if let Some(refused) = read_precondition_failed(Some(token), response.incarnation) {
                return refused;
            }
            chunk_response(response, max_len, &headers)
        }
        Err(err) => {
            runtime_error_or_leader_redirect_async(&state, err, &request_target(&uri)).await
        }
    }
}

#[tracing::instrument(
    name = "http.immutable_snapshot_read",
    skip_all,
    fields(bucket = %path.bucket, stream = %path.stream, snapshot_offset = %path.offset),
)]
pub(crate) async fn read_immutable_snapshot(
    State(state): State<HttpState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<ImmutablePath>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let (stream_id, resource) = match path.parse(ResourceKind::Snapshot, raw_query.as_deref()) {
        Ok(parsed) => parsed,
        Err(response) => return *response,
    };
    snapshot_answer(
        &state,
        SnapshotAnswer {
            stream_id,
            snapshot_offset: resource.offset.get(),
            if_incarnation: Some(resource.incarnation.get()),
            resource: SnapshotResource::Immutable,
            body: SnapshotBody::of(&method),
        },
        &headers,
        &request_target(&uri),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::CHUNK_GRID_BYTES;
    use super::chunk_len;

    #[test]
    fn chunks_end_on_the_grid() {
        let grid = usize::try_from(CHUNK_GRID_BYTES).unwrap();
        assert_eq!(chunk_len(0), grid);
        assert_eq!(chunk_len(1), grid - 1);
        assert_eq!(chunk_len(CHUNK_GRID_BYTES - 1), 1);
        assert_eq!(chunk_len(CHUNK_GRID_BYTES), grid);
        assert_eq!(chunk_len(3 * CHUNK_GRID_BYTES + 5), grid - 5);
        assert_eq!(
            chunk_len(u64::MAX),
            grid - usize::try_from(u64::MAX % CHUNK_GRID_BYTES).unwrap()
        );
    }
}
