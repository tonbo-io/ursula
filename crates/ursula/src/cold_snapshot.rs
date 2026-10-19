//! Snapshot bodies in the cold tier (bounded-stream-state F16).
//!
//! Publish streams a large body into an object under the stream's external
//! prefix (the F5 staging path) while hashing it, then proposes
//! `PublishSnapshotExternal` with the object reference and digest, so
//! replicated state never holds the body. Reads and `/bootstrap` stream the
//! object back in bounded pieces, a read with a byte range only the pieces
//! it covers. Without a cold store, or for bodies under the staging
//! threshold, publish keeps the inline path. Every publish body is at most
//! the request limit: a larger snapshot goes up in parts
//! ([`crate::snapshot_parts`]).

use std::io;

use axum::body::Body;
use axum::body::Bytes;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::header::CONTENT_LENGTH;
use axum::response::IntoResponse;
use axum::response::Response;
use futures_util::StreamExt;
use futures_util::stream;
use ursula_headers::ErrorCode;
use ursula_runtime::BootstrapStreamResponse;
use ursula_runtime::ColdSnapshotBody;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::ExternalPayloadRef;
use ursula_runtime::ProposedUpload;
use ursula_runtime::SnapshotDigest;
use ursula_runtime::UploadFence;
use ursula_runtime::new_external_payload_path;
use ursula_shard::BucketStreamId;

use crate::HttpState;
use crate::MAX_HTTP_BODY_BYTES;
use crate::limits;
use crate::render;

/// Bytes read from the cold store per piece of a streamed snapshot body.
pub(crate) const COLD_SNAPSHOT_READ_PIECE_BYTES: u64 = 8 * 1024 * 1024;

/// A received snapshot body: inline bytes, or an object already staged.
pub(crate) enum SnapshotUpload {
    Inline(Bytes),
    Cold(ColdSnapshotBody),
}

fn body_read_error(err: impl std::fmt::Display) -> Response {
    render::error_response(
        StatusCode::BAD_REQUEST,
        ErrorCode::BodyUnreadable,
        format!("read snapshot body: {err}"),
    )
}

/// Receives a snapshot body of at most the request limit (`413` above it).
/// Bodies below the staging threshold (the smaller of
/// `runtime.external_payload_min_size` and the request limit) stay inline;
/// larger ones go to a new object under the stream's external prefix when
/// cold snapshots are enabled, whose upload is fenced to `fence`, the
/// leadership that admitted the publish (F14j).
pub(crate) async fn receive_snapshot_body(
    state: &HttpState,
    stream_id: &BucketStreamId,
    content_type: &str,
    body: Body,
    fence: UploadFence,
) -> Result<SnapshotUpload, Response> {
    // F16: publishes stage their body in the cold tier whenever a cold
    // store is configured.
    let cold = state.runtime.has_cold_store();
    let threshold = if cold {
        state
            .external_payload_min_bytes
            .clamp(1, MAX_HTTP_BODY_BYTES)
    } else {
        MAX_HTTP_BODY_BYTES
    };
    let mut frames = body.into_data_stream();
    let mut buffered = Vec::new();
    loop {
        match frames.next().await {
            None => return Ok(SnapshotUpload::Inline(Bytes::from(buffered))),
            Some(Err(err)) => return Err(body_read_error(err)),
            Some(Ok(frame)) => {
                buffered.extend_from_slice(&frame);
                if buffered.len() > MAX_HTTP_BODY_BYTES {
                    return Err(limits::size_refusal(limits::REQUEST_LIMIT));
                }
                if cold && buffered.len() >= threshold {
                    break;
                }
            }
        }
    }

    let Some(cold_store) = state.runtime.cold_store() else {
        return Err(render::fixed_error_response(
            StatusCode::NOT_IMPLEMENTED,
            ErrorCode::NoColdStore,
        ));
    };
    let path = new_external_payload_path(stream_id);
    let mut digest = SnapshotDigest::new(content_type);
    let (size, object_size) = stream_into_object(
        &cold_store,
        &path,
        Bytes::from(buffered),
        frames,
        Some(&mut digest),
        limits::MAX_REQUEST_BYTES,
    )
    .await
    .map_err(|err| match err {
        StreamIntoObjectError::TooLarge => limits::size_refusal(limits::REQUEST_LIMIT),
        StreamIntoObjectError::Body(err) => body_read_error(err),
        StreamIntoObjectError::ColdStore(err) => stage_error(err),
    })?;
    Ok(SnapshotUpload::Cold(ColdSnapshotBody {
        object: ExternalPayloadRef {
            s3_path: path,
            payload_len: size,
            object_size,
        },
        digest: digest.finalize(),
        upload: ProposedUpload::new(fence),
    }))
}

/// Why a body could not be streamed into a new cold object. `E` is the
/// error of the body's stream: a request body's, or a parts commit's copy.
#[derive(Debug, thiserror::Error)]
pub(crate) enum StreamIntoObjectError<E: std::error::Error + 'static = axum::Error> {
    /// The body is above its limit.
    #[error("the body is above its limit")]
    TooLarge,
    /// The body could not be read.
    #[error("read the body: {0}")]
    Body(#[source] E),
    /// The cold store failed.
    #[error("write the object: {0}")]
    ColdStore(#[source] io::Error),
}

/// Streams `first`, then `frames`, into a new object at `path`: at most
/// `limit` bytes in all, every byte fed to `digest` when one is given. The
/// one write loop of a staged snapshot body, a snapshot part and a parts
/// commit's copy. The upload is aborted on any failure, so no object is
/// left behind. Returns the bytes written and the object's size.
pub(crate) async fn stream_into_object<E: std::error::Error + 'static>(
    cold_store: &ColdStoreHandle,
    path: &str,
    first: Bytes,
    frames: impl futures_util::Stream<Item = Result<Bytes, E>>,
    mut digest: Option<&mut SnapshotDigest>,
    limit: u64,
) -> Result<(u64, u64), StreamIntoObjectError<E>> {
    let mut frames = std::pin::pin!(frames);
    let mut writer = cold_store
        .open_object_writer(path)
        .await
        .map_err(StreamIntoObjectError::ColdStore)?;
    let streamed: Result<u64, StreamIntoObjectError<E>> = async {
        let mut size = 0_u64;
        let mut next = (!first.is_empty()).then_some(first);
        loop {
            let frame = match next.take() {
                Some(frame) => frame,
                None => match frames.next().await {
                    None => return Ok(size),
                    Some(frame) => frame.map_err(StreamIntoObjectError::Body)?,
                },
            };
            size = size.saturating_add(u64::try_from(frame.len()).unwrap_or(u64::MAX));
            if size > limit {
                return Err(StreamIntoObjectError::TooLarge);
            }
            if let Some(digest) = digest.as_deref_mut() {
                digest.update(&frame);
            }
            writer
                .write(frame)
                .await
                .map_err(StreamIntoObjectError::ColdStore)?;
        }
    }
    .await;
    let size = match streamed {
        Ok(size) => size,
        Err(err) => {
            writer.abort().await;
            return Err(err);
        }
    };
    let object_size = writer
        .close()
        .await
        .map_err(StreamIntoObjectError::ColdStore)?;
    Ok((size, object_size))
}

fn stage_error(err: io::Error) -> Response {
    tracing::warn!(error = %err, "write snapshot object");
    render::error_response(
        StatusCode::BAD_GATEWAY,
        ErrorCode::ColdStoreFailed,
        "writing the snapshot object failed",
    )
}

/// Streams `len` bytes of a cold snapshot body from `start`, piece by
/// piece. The first piece is read before returning, so a missing object
/// fails the request instead of truncating a `200`.
async fn object_stream(
    cold_store: ColdStoreHandle,
    object: ExternalPayloadRef,
    start: u64,
    len: u64,
) -> io::Result<impl futures_util::Stream<Item = io::Result<Bytes>> + Send + 'static> {
    let end = start.saturating_add(len).min(object.payload_len);
    let first_len = piece_len(end.saturating_sub(start));
    let first = read_piece(
        &cold_store,
        &object.s3_path,
        object.payload_len,
        start,
        first_len,
    )
    .await?;
    let read = u64::try_from(first_len).unwrap_or(u64::MAX);
    let rest = stream::try_unfold(start.saturating_add(read), move |offset| {
        let cold_store = cold_store.clone();
        let object = object.clone();
        async move {
            let remaining = end.saturating_sub(offset);
            if remaining == 0 {
                return Ok(None);
            }
            let len = piece_len(remaining);
            let piece = read_piece(
                &cold_store,
                &object.s3_path,
                object.payload_len,
                offset,
                len,
            )
            .await?;
            // `len <= remaining`, so `offset + len <= end` and never saturates.
            Ok(Some((
                piece,
                offset.saturating_add(u64::try_from(len).unwrap_or(u64::MAX)),
            )))
        }
    });
    Ok(stream::once(async move { Ok(first) }).chain(rest))
}

/// The length of the next piece of a body with `remaining` bytes left:
/// at most [`COLD_SNAPSHOT_READ_PIECE_BYTES`].
pub(crate) fn piece_len(remaining: u64) -> usize {
    let piece = usize::try_from(COLD_SNAPSHOT_READ_PIECE_BYTES).unwrap_or(usize::MAX);
    usize::try_from(remaining).map_or(piece, |remaining| remaining.min(piece))
}

/// Reads `len` bytes, at most one piece (see [`piece_len`]), at `start` of
/// the object at `path`, whose body is `size` bytes. The one piece reader of
/// snapshot reads, `/bootstrap` and the copy of a parts commit.
pub(crate) async fn read_piece(
    cold_store: &ColdStoreHandle,
    path: &str,
    size: u64,
    start: u64,
    len: usize,
) -> io::Result<Bytes> {
    debug_assert!(
        u64::try_from(len).is_ok_and(|len| len <= COLD_SNAPSHOT_READ_PIECE_BYTES),
        "a piece is at most {COLD_SNAPSHOT_READ_PIECE_BYTES} bytes"
    );
    cold_store
        .read_whole_object_range(path, size, start, len)
        .await
        .map(Bytes::from)
}

fn cold_read_error(err: io::Error) -> Response {
    tracing::error!(error = %err, "read cold snapshot body");
    render::error_response(
        StatusCode::BAD_GATEWAY,
        ErrorCode::ColdStoreFailed,
        "reading the snapshot object failed",
    )
}

/// The response body of a snapshot read whose body lives in `object`: its
/// `len` bytes from `start`.
pub(crate) async fn cold_snapshot_body(
    state: &HttpState,
    object: ExternalPayloadRef,
    start: u64,
    len: u64,
) -> Result<Body, Response> {
    let Some(cold_store) = state.runtime.cold_store() else {
        return Err(cold_read_error(io::Error::other(
            "snapshot body is in the cold tier but no cold backend is configured",
        )));
    };
    object_stream(cold_store, object, start, len)
        .await
        .map(Body::from_stream)
        .map_err(cold_read_error)
}

/// A `/bootstrap` answer whose snapshot part streams `object` from the
/// cold store between the multipart prefix and the update parts.
pub(crate) async fn bootstrap_response(
    state: &HttpState,
    response: BootstrapStreamResponse,
    object: ExternalPayloadRef,
) -> Response {
    let Some(cold_store) = state.runtime.cold_store() else {
        return cold_read_error(io::Error::other(
            "snapshot body is in the cold tier but no cold backend is configured",
        ));
    };
    let (boundary, mut headers) = render::bootstrap_head(&response);
    let (prefix, suffix) = render::bootstrap_multipart_around_snapshot(&response, &boundary);
    // Two in-memory buffers cannot sum past `usize::MAX`, so this never saturates.
    let len = u64::try_from(prefix.len().saturating_add(suffix.len()))
        .expect("multipart len fits u64")
        .saturating_add(object.payload_len);
    let size = object.payload_len;
    let body = match object_stream(cold_store, object, 0, size).await {
        Ok(body) => body,
        Err(err) => return cold_read_error(err),
    };
    let parts = stream::once(async move { Ok::<_, io::Error>(Bytes::from(prefix)) })
        .chain(body)
        .chain(stream::once(async move { Ok(Bytes::from(suffix)) }));
    headers.insert(CONTENT_LENGTH, HeaderValue::from(len));
    (StatusCode::OK, headers, Body::from_stream(parts)).into_response()
}
