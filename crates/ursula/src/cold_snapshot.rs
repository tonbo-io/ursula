//! Snapshot bodies in the cold tier (bounded-stream-state F16).
//!
//! Publish streams a large body into an object under the stream's external
//! prefix (the F5 staging path) while hashing it, then proposes
//! `PublishSnapshotExternal` with the object reference and digest, so
//! replicated state never holds the body. Reads and `/bootstrap` stream the
//! object back in bounded pieces. Without a cold store, or for bodies under
//! the staging threshold, publish keeps the inline path and its 32 MiB cap.

use std::io;

use axum::body::Body;
use axum::body::Bytes;
use axum::http::HeaderValue;
use axum::http::Method;
use axum::http::StatusCode;
use axum::http::Uri;
use axum::http::header::CONTENT_LENGTH;
use axum::response::IntoResponse;
use axum::response::Response;
use futures_util::StreamExt;
use futures_util::stream;
use ursula_runtime::BootstrapStreamResponse;
use ursula_runtime::ColdSnapshotBody;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::ExternalPayloadRef;
use ursula_runtime::MAX_COLD_SNAPSHOT_BYTES;
use ursula_runtime::SnapshotDigest;
use ursula_runtime::new_external_payload_path;
use ursula_shard::BucketStreamId;

use crate::HttpState;
use crate::MAX_HTTP_BODY_BYTES;
use crate::render;

/// Bytes read from the cold store per piece of a streamed snapshot body.
const COLD_SNAPSHOT_READ_PIECE_BYTES: u64 = 8 * 1024 * 1024;

/// A received snapshot body: inline bytes, or an object already staged.
pub(crate) enum SnapshotUpload {
    Inline(Bytes),
    Cold(ColdSnapshotBody),
}

/// Whether a request publishes a snapshot (`PUT …/snapshot/{offset}`).
/// Admission lets these bodies exceed the inline
/// cap, up to [`MAX_COLD_SNAPSHOT_BYTES`], and charges at most the inline cap
/// against the in-flight budget, because a large body is streamed to the
/// cold store in bounded parts. The handler enforces the real limit.
pub(crate) fn is_snapshot_publish(method: &Method, uri: &Uri) -> bool {
    if *method != Method::PUT {
        return false;
    }
    let segments = uri
        .path()
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    match segments.as_slice() {
        [bucket, ..] if bucket.starts_with("__ursula") => false,
        [_, _, "snapshot", offset] => offset.parse::<u64>().is_ok(),
        _ => false,
    }
}

/// Admission's body limit for a write request.
pub(crate) fn max_admitted_body_bytes(method: &Method, uri: &Uri) -> u64 {
    if is_snapshot_publish(method, uri) {
        MAX_COLD_SNAPSHOT_BYTES
    } else {
        u64::try_from(MAX_HTTP_BODY_BYTES).expect("max body bytes fits u64")
    }
}

fn payload_too_large() -> Response {
    (StatusCode::PAYLOAD_TOO_LARGE, "snapshot body is too large").into_response()
}

fn body_read_error(err: impl std::fmt::Display) -> Response {
    (
        StatusCode::BAD_REQUEST,
        format!("read snapshot body: {err}"),
    )
        .into_response()
}

/// Receives a snapshot body. Bodies below the staging threshold (the
/// smaller of `runtime.external_payload_min_size` and the inline cap) stay
/// inline; larger ones go to a new object under the stream's external
/// prefix when cold snapshots are enabled, and are rejected with `413`
/// above the inline cap otherwise.
pub(crate) async fn receive_snapshot_body(
    state: &HttpState,
    stream_id: &BucketStreamId,
    content_type: &str,
    body: Body,
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
                if !cold && buffered.len() > MAX_HTTP_BODY_BYTES {
                    return Err(payload_too_large());
                }
                if cold && buffered.len() >= threshold {
                    break;
                }
            }
        }
    }

    let Some(cold_store) = state.runtime.cold_store() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "cold backend must be configured before staging snapshots",
        )
            .into_response());
    };
    let path = new_external_payload_path(stream_id);
    let mut writer = cold_store
        .open_object_writer(&path)
        .await
        .map_err(stage_error)?;
    let mut digest = SnapshotDigest::new(content_type);
    let mut size = u64::try_from(buffered.len()).expect("buffer len fits u64");
    digest.update(&buffered);
    let staged: Result<(), Response> = async {
        writer
            .write(Bytes::from(buffered))
            .await
            .map_err(stage_error)?;
        while let Some(frame) = frames.next().await {
            let frame = frame.map_err(body_read_error)?;
            size = size.saturating_add(u64::try_from(frame.len()).unwrap_or(u64::MAX));
            if size > MAX_COLD_SNAPSHOT_BYTES {
                return Err(payload_too_large());
            }
            digest.update(&frame);
            writer.write(frame).await.map_err(stage_error)?;
        }
        Ok(())
    }
    .await;
    if let Err(response) = staged {
        writer.abort().await;
        return Err(response);
    }
    let object_size = writer.close().await.map_err(stage_error)?;
    Ok(SnapshotUpload::Cold(ColdSnapshotBody {
        object: ExternalPayloadRef {
            s3_path: path,
            payload_len: size,
            object_size,
        },
        digest: digest.finalize(),
    }))
}

fn stage_error(err: io::Error) -> Response {
    (
        StatusCode::BAD_GATEWAY,
        format!("write snapshot object: {err}"),
    )
        .into_response()
}

/// Streams a cold snapshot body piece by piece. The first piece is read
/// before returning, so a missing object fails the request instead of
/// truncating a `200`.
async fn object_stream(
    cold_store: ColdStoreHandle,
    object: ExternalPayloadRef,
) -> io::Result<impl futures_util::Stream<Item = io::Result<Bytes>> + Send + 'static> {
    let size = object.payload_len;
    let first_len = size.min(COLD_SNAPSHOT_READ_PIECE_BYTES);
    let first = read_piece(&cold_store, &object, 0, first_len).await?;
    let rest = stream::try_unfold(first_len, move |offset| {
        let cold_store = cold_store.clone();
        let object = object.clone();
        async move {
            let remaining = size.saturating_sub(offset);
            if remaining == 0 {
                return Ok(None);
            }
            let len = remaining.min(COLD_SNAPSHOT_READ_PIECE_BYTES);
            let piece = read_piece(&cold_store, &object, offset, len).await?;
            // `len <= remaining`, so `offset + len <= size` and never saturates.
            Ok(Some((piece, offset.saturating_add(len))))
        }
    });
    Ok(stream::once(async move { Ok(first) }).chain(rest))
}

async fn read_piece(
    cold_store: &ColdStoreHandle,
    object: &ExternalPayloadRef,
    start: u64,
    len: u64,
) -> io::Result<Bytes> {
    let len =
        usize::try_from(len).map_err(|_overflow| io::Error::other("piece length exceeds usize"))?;
    cold_store
        .read_whole_object_range(&object.s3_path, object.payload_len, start, len)
        .await
        .map(Bytes::from)
}

fn cold_read_error(err: io::Error) -> Response {
    tracing::error!(error = %err, "read cold snapshot body");
    (
        StatusCode::BAD_GATEWAY,
        format!("read snapshot object: {err}"),
    )
        .into_response()
}

/// The response body of a snapshot read whose body lives in `object`.
pub(crate) async fn cold_snapshot_body(
    state: &HttpState,
    object: ExternalPayloadRef,
) -> Result<Body, Response> {
    let Some(cold_store) = state.runtime.cold_store() else {
        return Err(cold_read_error(io::Error::other(
            "snapshot body is in the cold tier but no cold backend is configured",
        )));
    };
    object_stream(cold_store, object)
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
    let body = match object_stream(cold_store, object).await {
        Ok(body) => body,
        Err(err) => return cold_read_error(err),
    };
    let parts = stream::once(async move { Ok::<_, io::Error>(Bytes::from(prefix)) })
        .chain(body)
        .chain(stream::once(async move { Ok(Bytes::from(suffix)) }));
    headers.insert(CONTENT_LENGTH, HeaderValue::from(len));
    (StatusCode::OK, headers, Body::from_stream(parts)).into_response()
}
