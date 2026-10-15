//! Snapshots uploaded in parts (#581 section 3.1, W4; extensions Section
//! 2.2).
//!
//! Every request body is at most the request limit, so a snapshot above it
//! goes up in parts, on a node with a cold store:
//!
//! - `POST {stream}/snapshot/{offset}/parts` stages one part and answers
//!   `201` with its id. Nothing is proposed. The part's object lives in the
//!   group's part directory of the stream's bucket, named by its id and a
//!   hash of the stream and its token, so only a commit on the same
//!   incarnation of the same stream finds it. Its name ends in `.part`,
//!   which no object that replicated state references does, so deleting a
//!   part can never remove data.
//! - `PUT {stream}/snapshot/{offset}` with `Stream-Snapshot-Parts` commits:
//!   its body lists the distinct part ids in order. Before it reads the body
//!   the leader answers from the key `(offset, digest)` alone when the
//!   stream's latest snapshot decides it: `204` for the same snapshot, `409`
//!   naming the latest when another one is there or a later one supersedes
//!   it. Otherwise it resolves the ids, checks their sizes concurrently,
//!   copies the parts in order into one new object, a few pieces read ahead
//!   while it hashes and writes in order, checks the digest, and proposes
//!   `PublishSnapshotExternal` as a single-request publish of a large body
//!   does. Once the commit's answer is final for its key it deletes the
//!   parts; after a retryable answer it keeps them for the repeat.
//!
//! A node copies at most [`SNAPSHOT_ASSEMBLIES`] commits at once, and each
//! charges its copy buffers against the in-flight body budget. The server
//! keeps no upload session: the orphan sweep reclaims every part older than
//! its grace (a day by default), the parts of a deleted stream included.
//! Without a cold store, a part or a commit answers `501`.

use std::io;

use axum::body::Body;
use axum::body::Bytes;
use axum::extract::OriginalUri;
use axum::extract::Path;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::Version;
use axum::http::header::RETRY_AFTER;
use axum::response::IntoResponse;
use axum::response::Response;
use futures_util::StreamExt;
use futures_util::stream;
use ursula_headers::HeaderError;
use ursula_headers::MAX_MANIFEST_BYTES;
use ursula_headers::PartId;
use ursula_headers::PartStaged;
use ursula_headers::PartsCommit;
use ursula_headers::PartsManifest;
use ursula_headers::SnapshotPublishAnswer;
use ursula_headers::names;
use ursula_headers::value::SnapshotDigest as DigestValue;
use ursula_runtime::COLD_OBJECT_WRITE_CONCURRENCY;
use ursula_runtime::ColdSnapshotBody;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::ExternalPayloadRef;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::MAX_COLD_SNAPSHOT_BYTES;
use ursula_runtime::SnapshotDigest;
use ursula_runtime::is_snapshot_part_id;
use ursula_runtime::new_external_payload_path;
use ursula_runtime::new_snapshot_part_id;
use ursula_runtime::snapshot_part_path;
use ursula_shard::BucketStreamId;

use crate::HttpState;
use crate::PublishTarget;
use crate::SnapshotPath;
use crate::admission::PendingAdmission;
use crate::cold_snapshot::COLD_SNAPSHOT_READ_PIECE_BYTES;
use crate::cold_snapshot::SnapshotUpload;
use crate::cold_snapshot::StreamIntoObjectError;
use crate::cold_snapshot::read_piece;
use crate::cold_snapshot::stream_into_object;
use crate::front_door;
use crate::front_door::HeadDecision;
use crate::limits;
use crate::render::insert_default_response_headers;
use crate::render::insert_header_str;
use crate::render::snapshot_published_response;
use crate::render::snapshot_ref;

/// Parts commits a node copies at once. A commit past them answers `503`
/// with `Retry-After`.
pub(crate) const SNAPSHOT_ASSEMBLIES: usize = 2;

/// Pieces a commit reads ahead of the one it hashes and writes.
const PIECES_IN_FLIGHT: usize = 4;

/// Part sizes a commit checks at once.
const STATS_IN_FLIGHT: usize = 16;

/// What one commit's copy holds in memory at most, which it charges against
/// the in-flight body budget: the pieces read ahead, the one it writes, and
/// the writer's upload parts in flight.
pub(crate) const ASSEMBLY_BUFFER_BYTES: u64 = COLD_SNAPSHOT_READ_PIECE_BYTES
    * (PIECES_IN_FLIGHT as u64 + 1 + COLD_OBJECT_WRITE_CONCURRENCY as u64);

/// Why a part upload or a parts commit was refused before it proposed
/// anything.
#[derive(Debug, thiserror::Error)]
pub(crate) enum PartsRefusal {
    /// The node has no cold store to stage parts in (`501`).
    #[error("this node has no cold backend, so it takes no snapshot parts")]
    NoColdStore,
    /// A commit's headers or manifest are malformed, or the manifest repeats
    /// a part (`400`).
    #[error("malformed parts commit: {0}")]
    Malformed(#[from] HeaderError),
    /// A manifest is longer than any valid one (`400`).
    #[error("a parts manifest is at most {MAX_MANIFEST_BYTES} bytes")]
    ManifestTooLarge,
    /// A manifest names a part that is not staged for this incarnation of
    /// this stream, or that is gone (`400`).
    #[error("part {part} is not a staged part of this stream")]
    UnknownPart { part: PartId },
    /// The assembled snapshot's digest is not `Stream-Snapshot-Digest`
    /// (`400`).
    #[error("the assembled snapshot's digest {assembled} differs from Stream-Snapshot-Digest")]
    DigestMismatch { assembled: DigestValue },
    /// The assembled snapshot is above the snapshot limit (`413` naming it).
    #[error("the assembled snapshot is above the snapshot limit")]
    SnapshotTooLarge,
    /// A part's body is above the request limit (`413` naming it).
    #[error("a part's body is above the request limit")]
    PartTooLarge,
    /// A part's body could not be read (`400`).
    #[error("read part body: {0}")]
    Body(#[source] axum::Error),
    /// The node copies as many commits as it may (`503`).
    #[error("this node copies as many snapshot commits as it may; retry")]
    Busy,
    /// The cold store failed (`502`).
    #[error("snapshot parts in the cold store: {0}")]
    ColdStore(#[source] io::Error),
    /// A value the server produced is outside the protocol: a part id it
    /// named or a digest it computed (`500`).
    #[error("a value the server produced is outside the protocol: {0}")]
    Invariant(#[source] HeaderError),
}

impl IntoResponse for PartsRefusal {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        insert_default_response_headers(&mut headers);
        let status = match &self {
            Self::NoColdStore => StatusCode::NOT_IMPLEMENTED,
            Self::SnapshotTooLarge => return limits::size_refusal(limits::SNAPSHOT_LIMIT),
            Self::PartTooLarge => return limits::size_refusal(limits::REQUEST_LIMIT),
            Self::Malformed(_)
            | Self::ManifestTooLarge
            | Self::UnknownPart { .. }
            | Self::DigestMismatch { .. }
            | Self::Body(_) => StatusCode::BAD_REQUEST,
            Self::Busy => {
                headers.insert(RETRY_AFTER, HeaderValue::from_static("1"));
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::ColdStore(err) => {
                tracing::warn!(error = %err, "snapshot parts in the cold store failed");
                StatusCode::BAD_GATEWAY
            }
            Self::Invariant(err) => {
                tracing::error!(error = %err, "a snapshot part value is outside the protocol");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        (status, headers, self.to_string()).into_response()
    }
}

impl From<StreamIntoObjectError> for PartsRefusal {
    fn from(err: StreamIntoObjectError) -> Self {
        match err {
            StreamIntoObjectError::TooLarge => Self::PartTooLarge,
            StreamIntoObjectError::Body(err) => Self::Body(err),
            StreamIntoObjectError::ColdStore(err) => Self::ColdStore(err),
        }
    }
}

/// Whether a commit's answer is final for its key, so that no repeat needs
/// its parts again: the publish (`204`), or a refusal that a repeat gets
/// again (`400`, `404`, `409`, `410`, `412`). After any other answer, a
/// `503` above all, the client repeats the commit and its parts stay.
pub(crate) fn final_for_key(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::NO_CONTENT
            | StatusCode::BAD_REQUEST
            | StatusCode::NOT_FOUND
            | StatusCode::CONFLICT
            | StatusCode::GONE
            | StatusCode::PRECONDITION_FAILED
    )
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

/// What a commit's key decides before its body is read.
pub(crate) enum KeyCheck {
    /// The answer: `204` for the stream's latest snapshot, `409` naming the
    /// latest when another snapshot is at the offset or a later one
    /// supersedes it, or the stream's own refusal (`404`, `412`).
    Answered(Response),
    /// The key is open: the commit assembles its parts for the stream's
    /// incarnation `incarnation`.
    Open { incarnation: u64 },
}

/// Checks a commit's key against the stream's latest snapshot, on the
/// leader's linearizable `HEAD`, so a deposed leader never answers from a
/// stale state (A2 of the #581 addendum on the VFS client asks). A
/// superseded or conflicting commit copies nothing.
pub(crate) async fn key_check(target: &PublishTarget, commit: &PartsCommit) -> KeyCheck {
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
            return KeyCheck::Answered(
                crate::runtime_error_or_leader_redirect_async(state, err, &target.request_target)
                    .await,
            );
        }
    };
    if let Some(refused) = crate::read_precondition_failed(target.if_incarnation, head.incarnation)
    {
        return KeyCheck::Answered(refused);
    }
    let digest = commit.digest.to_string();
    let Some((latest_offset, latest_digest)) = head.snapshot_offset.zip(head.snapshot_digest)
    else {
        return KeyCheck::Open {
            incarnation: head.incarnation,
        };
    };
    if latest_offset == target.snapshot_offset && latest_digest == digest {
        return KeyCheck::Answered(snapshot_published_response(
            target.snapshot_offset,
            &digest,
            head.incarnation,
        ));
    }
    if latest_offset < target.snapshot_offset {
        return KeyCheck::Open {
            incarnation: head.incarnation,
        };
    }
    KeyCheck::Answered(match snapshot_ref(latest_offset, &latest_digest) {
        Ok(latest) => {
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            SnapshotPublishAnswer::Conflict { latest }
                .render(|name, value| insert_header_str(&mut headers, name, value));
            (
                StatusCode::CONFLICT,
                headers,
                "another snapshot is the latest at or after this offset",
            )
                .into_response()
        }
        Err(err) => PartsRefusal::Invariant(err).into_response(),
    })
}

/// Commits a snapshot uploaded in parts, staged for the stream's
/// incarnation `incarnation`: reads the manifest, assembles the parts, and
/// publishes the assembled object. The leader routed the commit, found its
/// key open, and admitted its copy buffers. The publish carries the
/// incarnation the parts were found for as its precondition, so a stream
/// recreated meanwhile is answered `412`.
pub(crate) async fn commit(
    mut target: PublishTarget,
    commit: PartsCommit,
    incarnation: u64,
    body: Body,
) -> Response {
    target.if_incarnation = Some(incarnation);
    let manifest = match axum::body::to_bytes(body, MAX_MANIFEST_BYTES).await {
        Ok(manifest) => manifest,
        Err(err) => {
            let over_limit = std::error::Error::source(&err)
                .is_some_and(|source| source.is::<http_body_util::LengthLimitError>());
            return if over_limit {
                PartsRefusal::ManifestTooLarge
            } else {
                PartsRefusal::Body(err)
            }
            .into_response();
        }
    };
    let manifest = match PartsManifest::parse(commit.parts, &manifest) {
        Ok(manifest) => manifest,
        Err(err) => return PartsRefusal::from(err).into_response(),
    };
    let Some(cold_store) = target.state.runtime.cold_store() else {
        return PartsRefusal::NoColdStore.into_response();
    };
    let Ok(copying) = target.state.snapshot_assemblies.clone().try_acquire_owned() else {
        return PartsRefusal::Busy.into_response();
    };
    let owner = PartOwner {
        stream_id: &target.stream_id,
        raft_group_id: target
            .state
            .runtime
            .locate(&target.stream_id)
            .raft_group_id
            .0,
        incarnation,
    };
    let assembled = match resolve(&cold_store, owner, &manifest, MAX_COLD_SNAPSHOT_BYTES).await {
        Ok(parts) => assemble(&cold_store, &target, &parts, &commit)
            .await
            .map(|assembled| (parts, assembled)),
        Err(refusal) => Err(refusal),
    };
    drop(copying);
    let (parts, assembled) = match assembled {
        Ok(assembled) => assembled,
        // A part went away during the copy: a concurrent identical commit
        // may have published the snapshot and deleted it.
        Err(PartsRefusal::UnknownPart { part }) => {
            return match key_check(&target, &commit).await {
                KeyCheck::Answered(answer) => answer,
                KeyCheck::Open { .. } => PartsRefusal::UnknownPart { part }.into_response(),
            };
        }
        Err(refusal) => return refusal.into_response(),
    };
    let response = target.publish(SnapshotUpload::Cold(assembled)).await;
    if final_for_key(response.status()) {
        delete_parts(&cold_store, &parts).await;
    }
    response
}

/// The incarnation of a stream that parts are staged for.
#[derive(Clone, Copy)]
struct PartOwner<'a> {
    stream_id: &'a BucketStreamId,
    raft_group_id: u32,
    incarnation: u64,
}

impl PartOwner<'_> {
    fn path(&self, id: &str) -> String {
        snapshot_part_path(self.stream_id, self.raft_group_id, self.incarnation, id)
    }
}

/// One part a manifest names: its id, object and size.
struct ResolvedPart {
    id: PartId,
    path: String,
    size: u64,
}

/// Resolves every id of `manifest` for `owner` alone, so that a manifest
/// cannot name another stream's part, and checks their sizes, a few at a
/// time, against `limit` before anything is copied.
async fn resolve(
    cold_store: &ColdStoreHandle,
    owner: PartOwner<'_>,
    manifest: &PartsManifest,
    limit: u64,
) -> Result<Vec<ResolvedPart>, PartsRefusal> {
    if let Some(part) = manifest
        .parts()
        .iter()
        .find(|part| !is_snapshot_part_id(part.as_str()))
    {
        return Err(PartsRefusal::UnknownPart { part: part.clone() });
    }
    // Owned inputs and futures: a stream of futures that borrow does not
    // prove `Send` for the handler.
    let lookups = manifest
        .parts()
        .iter()
        .map(|part| (part.clone(), owner.path(part.as_str())))
        .collect::<Vec<_>>();
    let parts = stream::iter(lookups)
        .map(|(id, path)| {
            let cold_store = cold_store.clone();
            async move {
                match cold_store.object_size(&path).await {
                    Ok(size) => Ok(ResolvedPart { id, path, size }),
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {
                        Err(PartsRefusal::UnknownPart { part: id })
                    }
                    Err(err) => Err(PartsRefusal::ColdStore(err)),
                }
            }
        })
        .buffered(STATS_IN_FLIGHT)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    parts.iter().try_fold(0_u64, |total, part| {
        total
            .checked_add(part.size)
            .filter(|total| *total <= limit)
            .ok_or(PartsRefusal::SnapshotTooLarge)
    })?;
    Ok(parts)
}

/// Copies `parts` in order into one new object under the stream's external
/// prefix while computing the snapshot digest, and keeps the object only
/// when the digest is the one the commit names. Up to [`PIECES_IN_FLIGHT`]
/// pieces are read ahead of the one hashed and written, which stay in
/// order. A part that is gone answers [`PartsRefusal::UnknownPart`].
async fn assemble(
    cold_store: &ColdStoreHandle,
    target: &PublishTarget,
    parts: &[ResolvedPart],
    commit: &PartsCommit,
) -> Result<ColdSnapshotBody, PartsRefusal> {
    let path = new_external_payload_path(&target.stream_id);
    let mut digest = SnapshotDigest::new(&target.content_type);
    let piece_bytes = usize::try_from(COLD_SNAPSHOT_READ_PIECE_BYTES).unwrap_or(usize::MAX);
    let pieces = parts
        .iter()
        .flat_map(|part| {
            (0..part.size).step_by(piece_bytes).map(move |start| {
                (
                    part.id.clone(),
                    part.path.clone(),
                    part.size,
                    start,
                    part.size
                        .saturating_sub(start)
                        .min(COLD_SNAPSHOT_READ_PIECE_BYTES),
                )
            })
        })
        .collect::<Vec<_>>();
    let reads = stream::iter(pieces)
        .map(|(id, path, size, start, len)| {
            let cold_store = cold_store.clone();
            async move {
                read_piece(&cold_store, &path, size, start, len)
                    .await
                    .map_err(|err| match err.kind() {
                        io::ErrorKind::NotFound => PartsRefusal::UnknownPart { part: id },
                        _ => PartsRefusal::ColdStore(err),
                    })
            }
        })
        .buffered(PIECES_IN_FLIGHT);
    let (size, object_size) = match stream_into_object(
        cold_store,
        &path,
        Bytes::new(),
        reads,
        Some(&mut digest),
        u64::MAX,
    )
    .await
    {
        Ok(written) => written,
        Err(StreamIntoObjectError::Body(refusal)) => return Err(refusal),
        Err(StreamIntoObjectError::ColdStore(err)) => return Err(PartsRefusal::ColdStore(err)),
        // `resolve` checked the sizes against the snapshot limit.
        Err(StreamIntoObjectError::TooLarge) => return Err(PartsRefusal::SnapshotTooLarge),
    };
    let assembled = DigestValue::parse(names::STREAM_SNAPSHOT_DIGEST, &digest.finalize())
        .map_err(PartsRefusal::Invariant)?;
    if assembled != commit.digest {
        delete_object(cold_store, &path).await;
        return Err(PartsRefusal::DigestMismatch { assembled });
    }
    Ok(ColdSnapshotBody {
        object: ExternalPayloadRef {
            s3_path: path,
            payload_len: size,
            object_size,
        },
        digest: assembled.to_string(),
    })
}

/// Deletes the parts of a commit whose answer is final for its key, best
/// effort: a part left behind is swept as an orphan.
async fn delete_parts(cold_store: &ColdStoreHandle, parts: &[ResolvedPart]) {
    for part in parts {
        delete_object(cold_store, &part.path).await;
    }
}

async fn delete_object(cold_store: &ColdStoreHandle, path: &str) {
    if let Err(err) = cold_store.delete_chunk(path).await {
        tracing::warn!(
            path = %path,
            error = %err,
            "failed to delete a snapshot part or copy; the orphan sweep reclaims it"
        );
    }
}

/// `POST {stream}/snapshot/{offset}/parts`: stages one part of a snapshot
/// and answers `201` with its id. It is routed before its body like every
/// write, and the stream must exist (and hold the token a
/// `Stream-Incarnation` precondition names): the part is staged for that
/// incarnation. It meets the in-flight body budget but no WAL or Raft-log
/// pressure check, since it touches neither until it is committed. The
/// offset in the path is checked and otherwise unused: a part is bound to
/// its stream, not to an offset.
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
    let incarnation = match head {
        Ok(head) => match crate::read_precondition_failed(if_incarnation, head.incarnation) {
            Some(refused) => {
                return crate::answer_before_body(refused, version, &headers, body).await;
            }
            None => head.incarnation,
        },
        Err(err) => {
            let refused =
                crate::runtime_error_or_leader_redirect_async(&state, err, &request_target).await;
            return crate::answer_before_body(refused, version, &headers, body).await;
        }
    };
    let _permit = match admission
        .0
        .map(crate::admission::DeferredAdmission::admit_budget)
    {
        None => None,
        Some(Ok(permit)) => Some(permit),
        Some(Err(refusal)) => {
            return crate::answer_before_body(refusal.into_response(), version, &headers, body)
                .await;
        }
    };
    let owner = PartOwner {
        stream_id: &stream_id,
        raft_group_id: state.runtime.locate(&stream_id).raft_group_id.0,
        incarnation,
    };
    match stage(&cold_store, owner, body).await {
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
/// object for `owner`, and returns its id.
async fn stage(
    cold_store: &ColdStoreHandle,
    owner: PartOwner<'_>,
    body: Body,
) -> Result<PartId, PartsRefusal> {
    let id = PartId::new(new_snapshot_part_id()).map_err(PartsRefusal::Invariant)?;
    stream_into_object(
        cold_store,
        &owner.path(id.as_str()),
        Bytes::new(),
        body.into_data_stream(),
        None,
        limits::MAX_REQUEST_BYTES,
    )
    .await?;
    Ok(id)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::response::IntoResponse;
    use ursula_headers::PartId;
    use ursula_headers::PartsManifest;
    use ursula_headers::SizeLimit;
    use ursula_runtime::ColdStore;
    use ursula_runtime::MAX_COLD_SNAPSHOT_BYTES;
    use ursula_runtime::new_snapshot_part_id;
    use ursula_shard::BucketStreamId;

    use super::PartOwner;
    use super::PartsRefusal;
    use super::resolve;

    /// The snapshot limit is checked from the parts' sizes, before anything
    /// is copied, and its refusal names the snapshot limit. A limit of a few
    /// bytes stands in for 1 GiB, which a test cannot stage in memory.
    #[tokio::test]
    async fn the_assembled_size_is_checked_from_the_parts_before_any_copy() {
        let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
        let stream_id = BucketStreamId::new("parts", "s");
        let owner = PartOwner {
            stream_id: &stream_id,
            raft_group_id: 0,
            incarnation: 1,
        };
        let mut ids = Vec::new();
        for _ in 0..3 {
            let id = new_snapshot_part_id();
            cold_store
                .write_chunk(&owner.path(&id), &[7; 10])
                .await
                .expect("stage part");
            ids.push(PartId::new(id).unwrap());
        }
        let manifest = PartsManifest::new(ids).unwrap();
        let resolved = resolve(&cold_store, owner, &manifest, 30)
            .await
            .expect("30 bytes fit");
        assert_eq!(resolved.iter().map(|part| part.size).sum::<u64>(), 30);
        let refused = resolve(&cold_store, owner, &manifest, 29).await;
        assert!(matches!(refused, Err(PartsRefusal::SnapshotTooLarge)));
        let response = PartsRefusal::SnapshotTooLarge.into_response();
        assert_eq!(
            SizeLimit::parse(
                413,
                response
                    .headers()
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_bytes()))
            ),
            Ok(Some(SizeLimit::Snapshot(MAX_COLD_SNAPSHOT_BYTES)))
        );
        // Another incarnation of the stream finds none of them.
        let other = PartOwner {
            incarnation: 2,
            ..owner
        };
        assert!(matches!(
            resolve(&cold_store, other, &manifest, 30).await,
            Err(PartsRefusal::UnknownPart { .. })
        ));
    }
}
