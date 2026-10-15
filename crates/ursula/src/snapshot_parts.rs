//! Snapshots uploaded in parts (#581 section 3.1, W4; extensions Section
//! 2.2).
//!
//! Every request body is at most the request limit, so a snapshot above it
//! goes up in parts, on a node with a cold store:
//!
//! - `POST {stream}/snapshot/{offset}/parts` stages one part as a new object
//!   under the stream's external prefix and answers `201` with its id, the
//!   object's file name. Nothing is proposed. A part's name ends in `.part`,
//!   which no object that replicated state references does, so a commit can
//!   name only parts, and deleting a part can never remove data.
//! - `PUT {stream}/snapshot/{offset}` with `Stream-Snapshot-Parts` commits:
//!   its body lists the part ids in order. The leader first answers a repeat
//!   of a published snapshot from the key `(offset, digest)` alone, before
//!   it reads the body. Otherwise it resolves each id under the same
//!   stream's prefix, copies the parts in order into one new object while
//!   hashing it, checks the digest, and proposes `PublishSnapshotExternal`
//!   exactly as a single-request publish of a large body does. After a
//!   definite outcome it deletes the parts, best effort.
//!
//! The server keeps no upload session. A part that no commit used, or whose
//! deletion failed, is swept as an orphan once it is older than the sweep's
//! grace (a day by default). Without a cold store, a part or a commit
//! answers `501`.

use std::collections::HashMap;
use std::io;

use axum::body::Body;
use axum::body::Bytes;
use axum::extract::OriginalUri;
use axum::extract::Path;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::Version;
use axum::response::IntoResponse;
use axum::response::Response;
use futures_util::StreamExt;
use ursula_headers::HeaderError;
use ursula_headers::PartId;
use ursula_headers::PartStaged;
use ursula_headers::PartsCommit;
use ursula_headers::PartsManifest;
use ursula_runtime::ColdSnapshotBody;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::ExternalPayloadRef;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::MAX_COLD_SNAPSHOT_BYTES;
use ursula_runtime::SnapshotDigest;
use ursula_runtime::cold_external_dir;
use ursula_runtime::is_snapshot_part_file_name;
use ursula_runtime::new_external_payload_path;
use ursula_runtime::new_snapshot_part_path;
use ursula_shard::BucketStreamId;

use crate::HttpState;
use crate::MAX_HTTP_BODY_BYTES;
use crate::PublishOutcome;
use crate::PublishTarget;
use crate::SnapshotPath;
use crate::admission::PendingAdmission;
use crate::cold_snapshot::COLD_SNAPSHOT_READ_PIECE_BYTES;
use crate::cold_snapshot::SnapshotUpload;
use crate::front_door;
use crate::front_door::HeadDecision;
use crate::limits;
use crate::render::insert_default_response_headers;
use crate::render::insert_header_str;
use crate::render::snapshot_published_response;

/// Why a part upload or a parts commit was refused before it proposed
/// anything.
#[derive(Debug, thiserror::Error)]
pub(crate) enum PartsRefusal {
    /// The node has no cold store to stage parts in (`501`).
    #[error("this node has no cold backend, so it takes no snapshot parts")]
    NoColdStore,
    /// A commit's headers or manifest are malformed (`400`).
    #[error("malformed parts commit: {0}")]
    Malformed(#[from] HeaderError),
    /// A manifest names a part that is not a staged part of this stream
    /// (`400`).
    #[error("part {part} is not a staged part of this stream")]
    UnknownPart { part: PartId },
    /// The assembled snapshot's digest is not `Stream-Snapshot-Digest`
    /// (`400`).
    #[error("the assembled snapshot's digest {assembled} differs from Stream-Snapshot-Digest")]
    DigestMismatch { assembled: String },
    /// The assembled snapshot is above the snapshot limit (`413` naming it).
    #[error("the assembled snapshot is above the snapshot limit")]
    SnapshotTooLarge,
    /// A part's body is above the request limit (`413` naming it).
    #[error("a part's body is above the request limit")]
    PartTooLarge,
    /// A part's body could not be read (`400`).
    #[error("read part body: {0}")]
    Body(#[source] axum::Error),
    /// The cold store failed (`502`).
    #[error("snapshot parts in the cold store: {0}")]
    ColdStore(#[source] io::Error),
}

impl IntoResponse for PartsRefusal {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::NoColdStore => StatusCode::NOT_IMPLEMENTED,
            Self::SnapshotTooLarge => return limits::size_refusal(limits::SNAPSHOT_LIMIT),
            Self::PartTooLarge => return limits::size_refusal(limits::REQUEST_LIMIT),
            Self::Malformed(_)
            | Self::UnknownPart { .. }
            | Self::DigestMismatch { .. }
            | Self::Body(_) => StatusCode::BAD_REQUEST,
            Self::ColdStore(err) => {
                tracing::warn!(error = %err, "snapshot parts in the cold store failed");
                StatusCode::BAD_GATEWAY
            }
        };
        let mut headers = HeaderMap::new();
        insert_default_response_headers(&mut headers);
        (status, headers, self.to_string()).into_response()
    }
}

/// The commit a publish carries: `None` for a publish whose body is the
/// snapshot itself. A commit on a node without a cold store is refused
/// with `501`, like a part.
pub(crate) fn parts_commit(
    state: &HttpState,
    headers: &HeaderMap,
) -> Result<Option<PartsCommit>, PartsRefusal> {
    let commit = PartsCommit::parse(
        headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes())),
    )?;
    if commit.is_some() && !state.runtime.has_cold_store() {
        return Err(PartsRefusal::NoColdStore);
    }
    Ok(commit)
}

/// The answer a commit gets from its key alone, before its body is read:
/// `204` when the stream's latest snapshot is the one it commits (A2 of the
/// #581 addendum on the VFS client asks), and the stream's own refusals
/// (`404`, `412`). `None` when
/// the commit must assemble its parts.
pub(crate) async fn published_answer(
    target: &PublishTarget,
    commit: &PartsCommit,
) -> Option<Response> {
    let state = &target.state;
    let head = match state
        .runtime
        .head_stream(HeadStreamRequest {
            stream_id: target.stream_id.clone(),
            now_ms: state.unix_time_ms(),
            linearizable: true,
            read_index: None,
        })
        .await
    {
        Ok(head) => head,
        Err(err) => {
            return Some(
                crate::runtime_error_or_leader_redirect_async(state, err, &target.request_target)
                    .await,
            );
        }
    };
    if let Some(refused) = crate::read_precondition_failed(target.if_incarnation, head.incarnation)
    {
        return Some(refused);
    }
    let digest = commit.digest.to_string();
    (head.snapshot_offset == Some(target.snapshot_offset)
        && head.snapshot_digest.as_deref() == Some(digest.as_str()))
    .then(|| snapshot_published_response(target.snapshot_offset, &digest, head.incarnation))
}

/// Commits a snapshot uploaded in parts: reads the manifest, assembles the
/// parts, and publishes the assembled object. The leader routed the commit
/// and found its key unpublished.
pub(crate) async fn commit(target: PublishTarget, commit: PartsCommit, body: Body) -> Response {
    let manifest = match axum::body::to_bytes(body, MAX_HTTP_BODY_BYTES).await {
        Ok(manifest) => manifest,
        Err(err) => return crate::unreadable_body_response(&err),
    };
    let manifest = match PartsManifest::parse(commit.parts, &manifest) {
        Ok(manifest) => manifest,
        Err(err) => return PartsRefusal::from(err).into_response(),
    };
    let Some(cold_store) = target.state.runtime.cold_store() else {
        return PartsRefusal::NoColdStore.into_response();
    };
    let parts = match resolve(&cold_store, &target.stream_id, &manifest).await {
        Ok(parts) => parts,
        Err(refusal) => return refusal.into_response(),
    };
    let assembled = match assemble(
        &cold_store,
        &target.stream_id,
        &target.content_type,
        &parts,
        &commit,
    )
    .await
    {
        Ok(assembled) => assembled,
        Err(refusal) => return refusal.into_response(),
    };
    let (response, outcome) = target.publish(SnapshotUpload::Cold(assembled)).await;
    if outcome == PublishOutcome::Definite {
        delete_parts(&cold_store, &parts).await;
    }
    response
}

/// One part a manifest names: its object and its size.
struct ResolvedPart {
    path: String,
    size: u64,
}

/// Resolves every id of `manifest` under the stream's own external prefix,
/// so that a manifest cannot name another stream's objects, and sums their
/// sizes against the snapshot limit before anything is copied.
async fn resolve(
    cold_store: &ColdStoreHandle,
    stream_id: &BucketStreamId,
    manifest: &PartsManifest,
) -> Result<Vec<ResolvedPart>, PartsRefusal> {
    let dir = cold_external_dir(stream_id);
    let mut sizes = HashMap::new();
    let mut parts = Vec::with_capacity(manifest.parts().len());
    let mut total = 0_u64;
    for part in manifest.parts() {
        if !is_snapshot_part_file_name(part.as_str()) {
            return Err(PartsRefusal::UnknownPart { part: part.clone() });
        }
        let path = format!("{dir}{part}");
        let size = match sizes.get(&path) {
            Some(size) => *size,
            None => {
                let size = match cold_store.object_size(&path).await {
                    Ok(size) => size,
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {
                        return Err(PartsRefusal::UnknownPart { part: part.clone() });
                    }
                    Err(err) => return Err(PartsRefusal::ColdStore(err)),
                };
                sizes.insert(path.clone(), size);
                size
            }
        };
        total = total
            .checked_add(size)
            .filter(|total| *total <= MAX_COLD_SNAPSHOT_BYTES)
            .ok_or(PartsRefusal::SnapshotTooLarge)?;
        parts.push(ResolvedPart { path, size });
    }
    Ok(parts)
}

/// Copies `parts` in order into one new object under the stream's external
/// prefix while computing the snapshot digest, and keeps the object only
/// when the digest is the one the commit names.
async fn assemble(
    cold_store: &ColdStoreHandle,
    stream_id: &BucketStreamId,
    content_type: &str,
    parts: &[ResolvedPart],
    commit: &PartsCommit,
) -> Result<ColdSnapshotBody, PartsRefusal> {
    let path = new_external_payload_path(stream_id);
    let mut writer = cold_store
        .open_object_writer(&path)
        .await
        .map_err(PartsRefusal::ColdStore)?;
    let mut digest = SnapshotDigest::new(content_type);
    let copied: Result<u64, PartsRefusal> = async {
        let mut size = 0_u64;
        for part in parts {
            let mut start = 0_u64;
            while start < part.size {
                let len = part
                    .size
                    .saturating_sub(start)
                    .min(COLD_SNAPSHOT_READ_PIECE_BYTES);
                let piece = read_part_piece(cold_store, part, start, len).await?;
                digest.update(&piece);
                writer
                    .write(Bytes::from(piece))
                    .await
                    .map_err(PartsRefusal::ColdStore)?;
                // `len <= part.size - start`, so neither sum saturates.
                start = start.saturating_add(len);
                size = size.saturating_add(len);
            }
        }
        let assembled = digest.finalize();
        if assembled != commit.digest.to_string() {
            return Err(PartsRefusal::DigestMismatch { assembled });
        }
        Ok(size)
    }
    .await;
    let size = match copied {
        Ok(size) => size,
        Err(refusal) => {
            writer.abort().await;
            return Err(refusal);
        }
    };
    let object_size = writer.close().await.map_err(PartsRefusal::ColdStore)?;
    Ok(ColdSnapshotBody {
        object: ExternalPayloadRef {
            s3_path: path,
            payload_len: size,
            object_size,
        },
        digest: digest.finalize(),
    })
}

async fn read_part_piece(
    cold_store: &ColdStoreHandle,
    part: &ResolvedPart,
    start: u64,
    len: u64,
) -> Result<Vec<u8>, PartsRefusal> {
    // A piece is at most 8 MiB.
    let len = usize::try_from(len).map_err(|_overflow| {
        PartsRefusal::ColdStore(io::Error::other("part piece length exceeds usize"))
    })?;
    cold_store
        .read_whole_object_range(&part.path, part.size, start, len)
        .await
        .map_err(PartsRefusal::ColdStore)
}

/// Deletes the parts of a commit whose outcome is definite, best effort: a
/// part left behind is swept as an orphan.
async fn delete_parts(cold_store: &ColdStoreHandle, parts: &[ResolvedPart]) {
    let mut deleted = std::collections::HashSet::new();
    for part in parts {
        if !deleted.insert(part.path.as_str()) {
            continue;
        }
        if let Err(err) = cold_store.delete_chunk(&part.path).await {
            tracing::warn!(
                path = %part.path,
                error = %err,
                "failed to delete a committed snapshot part; the orphan sweep reclaims it"
            );
        }
    }
}

/// `POST {stream}/snapshot/{offset}/parts`: stages one part of a snapshot
/// and answers `201` with its id. It is routed before its body like every
/// write, and the stream must exist (and hold the token a
/// `Stream-Incarnation` precondition names), so that the sweep of the
/// stream reclaims a part no commit uses. The offset in the path is checked
/// and otherwise unused: a part is bound to its stream, not to an offset.
#[tracing::instrument(
    name = "http.snapshot_part",
    skip_all,
    fields(bucket = %path.bucket, stream = %path.stream, snapshot_offset = %path.snapshot_offset),
)]
pub(crate) async fn stage_snapshot_part(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    version: Version,
    Path(path): Path<SnapshotPath>,
    headers: HeaderMap,
    admission: PendingAdmission,
    body: Body,
) -> Response {
    let (stream_id, snapshot_offset) = path.into_parts();
    if let Err(response) = crate::parse_snapshot_offset(&snapshot_offset) {
        return crate::answer_before_body(*response, version, &headers, body).await;
    }
    let if_incarnation = match crate::incarnation_precondition(&headers) {
        Ok(if_incarnation) => if_incarnation,
        Err(response) => {
            return crate::answer_before_body(*response, version, &headers, body).await;
        }
    };
    let Some(cold_store) = state.runtime.cold_store() else {
        return crate::answer_before_body(
            PartsRefusal::NoColdStore.into_response(),
            version,
            &headers,
            body,
        )
        .await;
    };
    let request_target = crate::request_target(&uri);
    let decision =
        front_door::route_before_body(&state, &stream_id, &request_target, None, None).await;
    if let HeadDecision::Answer(answer) = decision {
        return crate::answer_before_body(answer, version, &headers, body).await;
    }
    let head = state
        .runtime
        .head_stream(HeadStreamRequest {
            stream_id: stream_id.clone(),
            now_ms: state.unix_time_ms(),
            linearizable: false,
            read_index: None,
        })
        .await;
    let refused = match head {
        Ok(head) => crate::read_precondition_failed(if_incarnation, head.incarnation),
        Err(err) => {
            Some(crate::runtime_error_or_leader_redirect_async(&state, err, &request_target).await)
        }
    };
    if let Some(refused) = refused {
        return crate::answer_before_body(refused, version, &headers, body).await;
    }
    let _permit = match front_door::admit(admission.0) {
        HeadDecision::Answer(answer) => {
            return crate::answer_before_body(answer, version, &headers, body).await;
        }
        HeadDecision::Read(permit) => permit,
    };
    match stage(&cold_store, &stream_id, body).await {
        Ok(part) => {
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            PartStaged { part }.render(|name, value| insert_header_str(&mut headers, name, value));
            (StatusCode::CREATED, headers).into_response()
        }
        Err(refusal) => refusal.into_response(),
    }
}

/// Streams a part's body, at most the request limit, into a new part
/// object, and returns its id.
async fn stage(
    cold_store: &ColdStoreHandle,
    stream_id: &BucketStreamId,
    body: Body,
) -> Result<PartId, PartsRefusal> {
    let path = new_snapshot_part_path(stream_id);
    let id = path
        .rsplit('/')
        .next()
        .map(PartId::new)
        .transpose()?
        .ok_or_else(|| PartsRefusal::ColdStore(io::Error::other("a part path has no name")))?;
    let mut writer = cold_store
        .open_object_writer(&path)
        .await
        .map_err(PartsRefusal::ColdStore)?;
    let mut frames = body.into_data_stream();
    let streamed: Result<(), PartsRefusal> = async {
        let mut size = 0_u64;
        while let Some(frame) = frames.next().await {
            let frame = frame.map_err(PartsRefusal::Body)?;
            size = size.saturating_add(u64::try_from(frame.len()).unwrap_or(u64::MAX));
            if size > limits::MAX_REQUEST_BYTES {
                return Err(PartsRefusal::PartTooLarge);
            }
            writer.write(frame).await.map_err(PartsRefusal::ColdStore)?;
        }
        Ok(())
    }
    .await;
    if let Err(refusal) = streamed {
        writer.abort().await;
        return Err(refusal);
    }
    writer.close().await.map_err(PartsRefusal::ColdStore)?;
    Ok(id)
}
