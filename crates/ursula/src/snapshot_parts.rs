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
//! its grace (a day by default), the parts of a deleted stream included. A
//! stream holds a bounded number of unused parts, and a part past the bound
//! answers `429` before its body ([`crate::staged_parts`]).
//! `DELETE {stream}/snapshot/parts` discards them. A node runs one commit or
//! discard per stream incarnation at a time, so two commits never spend the
//! same parts. Without a cold store, a part, a commit or a discard answers
//! `501`.

use std::collections::HashSet;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::Ordering;

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
use futures_util::stream;
use ursula_headers::ErrorCode;
use ursula_headers::HeaderError;
use ursula_headers::MAX_MANIFEST_BYTES;
use ursula_headers::PartId;
use ursula_headers::PartStaged;
use ursula_headers::PartsCommit;
use ursula_headers::PartsManifest;
use ursula_headers::SnapshotPublishAnswer;
use ursula_headers::StagedPartsLimit;
use ursula_headers::names;
use ursula_headers::value::SnapshotDigest as DigestValue;
use ursula_runtime::COLD_OBJECT_WRITE_CONCURRENCY;
use ursula_runtime::ColdSnapshotBody;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::ExternalPayloadRef;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::HeadStreamResponse;
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
use crate::admission::AdmissionPermit;
use crate::admission::BodyBudget;
use crate::admission::PendingAdmission;
use crate::admission::retry_after_json;
use crate::cold_snapshot::COLD_SNAPSHOT_READ_PIECE_BYTES;
use crate::cold_snapshot::SnapshotUpload;
use crate::cold_snapshot::StreamIntoObjectError;
use crate::cold_snapshot::piece_len;
use crate::cold_snapshot::read_piece;
use crate::cold_snapshot::stream_into_object;
use crate::front_door;
use crate::front_door::HeadDecision;
use crate::limits;
use crate::render::error_answer;
use crate::render::error_response;
use crate::render::fixed_error_response;
use crate::render::insert_default_response_headers;
use crate::render::insert_header_str;
use crate::render::insert_offset;
use crate::render::snapshot_published_response;
use crate::render::snapshot_ref;
use crate::staged_parts::StagedOwner;

/// Parts commits a node copies at once. A commit past them answers `503`
/// with `Retry-After` and the code `SnapshotCommitsBusy`.
pub(crate) const SNAPSHOT_ASSEMBLIES: usize = 2;

/// The most pieces a commit reads ahead of the one it hashes and writes.
const MAX_PIECES_IN_FLIGHT: u64 = 4;

/// The fewest: a small node still overlaps one read with the write.
const MIN_PIECES_IN_FLIGHT: u64 = 1;

/// Part sizes a commit checks at once.
const STATS_IN_FLIGHT: usize = 16;

/// What a copy holds besides the pieces it reads ahead: the piece it
/// writes and the writer's upload parts in flight.
const COPY_FIXED_PIECES: u64 = 1 + COLD_OBJECT_WRITE_CONCURRENCY as u64;

/// The charge of a copy at the fewest pieces in flight: what the in-flight
/// body budget must hold besides one request, which configuration checks
/// (`ursula_config::ServerConfig::MIN_INFLIGHT_BODY_SIZE_WITH_COLD`).
pub(crate) const MIN_COPY_BYTES: u64 =
    COLD_SNAPSHOT_READ_PIECE_BYTES * (MIN_PIECES_IN_FLIGHT + COPY_FIXED_PIECES);

const _: () = assert!(
    limits::MAX_REQUEST_BYTES + MIN_COPY_BYTES
        <= ursula_config::ServerConfig::MIN_INFLIGHT_BODY_SIZE_WITH_COLD,
    "the smallest budget configuration admits must hold a request and a copy"
);

/// A copy reads ahead from a share of `budget / (2 * SNAPSHOT_ASSEMBLIES)`.
const COPIES_SHARE_DIVISOR: u64 = 2 * SNAPSHOT_ASSEMBLIES as u64;

/// How a copy runs on a node whose in-flight body budget is `capacity`:
/// the pieces it reads ahead and what it charges the budget. Its read-ahead
/// comes from a share of `capacity / (2 * SNAPSHOT_ASSEMBLIES)`, and its
/// charge is never less than [`MIN_COPY_BYTES`], so on a small budget the
/// share does not bound it. What bounds the copies is their reservation: a
/// copy starts only while one request's worth of the budget stays free
/// beside it ([`BodyBudget::reserve_leaving`]).
fn copy_plan(capacity: u64) -> (usize, u64) {
    let share = capacity.checked_div(COPIES_SHARE_DIVISOR).unwrap_or(0);
    let pieces = share
        .checked_div(COLD_SNAPSHOT_READ_PIECE_BYTES)
        .unwrap_or(0)
        .saturating_sub(COPY_FIXED_PIECES)
        .clamp(MIN_PIECES_IN_FLIGHT, MAX_PIECES_IN_FLIGHT);
    // At most 7 pieces of 8 MiB.
    let charge =
        COLD_SNAPSHOT_READ_PIECE_BYTES.saturating_mul(pieces.saturating_add(COPY_FIXED_PIECES));
    (usize::try_from(pieces).unwrap_or(1), charge)
}

/// The commits a node runs: its copy slots, and the stream incarnations it
/// runs a commit or a discard of now. One runs per stream incarnation at a
/// time, so a repeat of a running commit is answered instead of copied
/// again, and two commits never spend the same parts.
#[derive(Clone)]
pub(crate) struct SnapshotCommits {
    slots: Arc<tokio::sync::Semaphore>,
    running: Arc<Mutex<HashSet<CommitKey>>>,
    /// Test pause point: how many commits have ended.
    #[cfg(test)]
    ended: Arc<tokio::sync::watch::Sender<u64>>,
}

impl Default for SnapshotCommits {
    fn default() -> Self {
        Self {
            slots: Arc::new(tokio::sync::Semaphore::new(SNAPSHOT_ASSEMBLIES)),
            running: Arc::default(),
            #[cfg(test)]
            ended: Arc::new(tokio::sync::watch::Sender::new(0)),
        }
    }
}

/// What a commit or a discard claims: the stream incarnation whose parts
/// it uses.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CommitKey {
    pub(crate) stream_id: BucketStreamId,
    pub(crate) incarnation: u64,
}

impl SnapshotCommits {
    /// Marks a commit or a discard of `key` as running, or `None` when one
    /// runs already.
    pub(crate) fn claim(&self, key: CommitKey) -> Option<RunningCommit> {
        let mut running = self.running.lock().unwrap_or_else(PoisonError::into_inner);
        running.insert(key.clone()).then(|| RunningCommit {
            running: Arc::clone(&self.running),
            key,
            #[cfg(test)]
            ended: Arc::clone(&self.ended),
        })
    }

    /// How many commits have ended, for a test to wait on.
    #[cfg(test)]
    pub(crate) fn ended(&self) -> tokio::sync::watch::Receiver<u64> {
        self.ended.subscribe()
    }
}

/// A running commit or discard, until it ends.
pub(crate) struct RunningCommit {
    running: Arc<Mutex<HashSet<CommitKey>>>,
    key: CommitKey,
    #[cfg(test)]
    ended: Arc<tokio::sync::watch::Sender<u64>>,
}

impl Drop for RunningCommit {
    fn drop(&mut self) {
        self.running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.key);
        #[cfg(test)]
        self.ended
            .send_modify(|ended| *ended = ended.saturating_add(1));
    }
}

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
    /// The node copies as many commits as it may (`503`,
    /// `SnapshotCommitsBusy`).
    #[error("this node copies as many snapshot commits as it may; retry")]
    Busy,
    /// The node runs a commit or a discard of this stream incarnation
    /// already (`503`, `SnapshotCommitInProgress`): a repeat answers once it
    /// ends.
    #[error("a commit or discard of this stream runs; repeat it")]
    InProgress,
    /// The stream holds as many staged, unused parts, or bytes of them, as
    /// it may (`429` naming the limit and the stream's `incarnation`,
    /// `STAGED_PARTS_LIMIT`).
    #[error("this stream holds as many staged snapshot parts as it may; commit or discard them")]
    StagedLimit {
        limit: StagedPartsLimit,
        incarnation: u64,
    },
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
        let (status, code) = match &self {
            Self::NoColdStore => (StatusCode::NOT_IMPLEMENTED, ErrorCode::NoColdStore),
            Self::SnapshotTooLarge => return limits::size_refusal(limits::SNAPSHOT_LIMIT),
            Self::PartTooLarge => return limits::size_refusal(limits::REQUEST_LIMIT),
            Self::Malformed(_) | Self::ManifestTooLarge => {
                (StatusCode::BAD_REQUEST, ErrorCode::InvalidPartsCommit)
            }
            Self::UnknownPart { .. } => (StatusCode::BAD_REQUEST, ErrorCode::UnknownPart),
            Self::DigestMismatch { .. } => {
                (StatusCode::BAD_REQUEST, ErrorCode::SnapshotDigestMismatch)
            }
            Self::Body(_) => (StatusCode::BAD_REQUEST, ErrorCode::BodyUnreadable),
            Self::Busy => return retry_after_json(ErrorCode::SnapshotCommitsBusy),
            Self::InProgress => return retry_after_json(ErrorCode::SnapshotCommitInProgress),
            Self::StagedLimit { limit, incarnation } => {
                return staged_limit_response(*limit, *incarnation);
            }
            Self::ColdStore(err) => {
                tracing::warn!(error = %err, "snapshot parts in the cold store failed");
                return fixed_error_response(StatusCode::BAD_GATEWAY, ErrorCode::ColdStoreFailed);
            }
            Self::Invariant(err) => {
                tracing::error!(error = %err, "a snapshot part value is outside the protocol");
                return fixed_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorCode::Internal,
                );
            }
        };
        error_response(status, code, self.to_string())
    }
}

/// `429` naming the staged-parts limit a part passed and the stream's
/// incarnation, with no `Retry-After`: a short wait does not release it, a
/// commit, a discard or the sweep does.
fn staged_limit_response(limit: StagedPartsLimit, incarnation: u64) -> Response {
    let mut headers = HeaderMap::new();
    crate::render::insert_incarnation(&mut headers, incarnation);
    limit.render(|name, value| insert_header_str(&mut headers, name, value));
    error_answer(
        StatusCode::TOO_MANY_REQUESTS,
        limit.error_code(),
        format!(
            "the stream holds as many staged snapshot parts as it may: {} {}",
            limit.header_name(),
            limit.value()
        ),
        headers,
    )
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
    let Some((latest_offset, latest_digest)) =
        head.snapshot_offset.zip(head.snapshot_digest.clone())
    else {
        return open_unless_out_of_range(target, &head);
    };
    if latest_offset == target.snapshot_offset && latest_digest == digest {
        return KeyCheck::Answered(snapshot_published_response(
            target.snapshot_offset,
            &digest,
            head.incarnation,
        ));
    }
    if latest_offset < target.snapshot_offset {
        return open_unless_out_of_range(target, &head);
    }
    KeyCheck::Answered(match snapshot_ref(latest_offset, &latest_digest) {
        Ok(latest) => {
            let mut headers = HeaderMap::new();
            SnapshotPublishAnswer::Conflict { latest }
                .render(|name, value| insert_header_str(&mut headers, name, value));
            error_answer(
                StatusCode::CONFLICT,
                ErrorCode::SnapshotConflict,
                "another snapshot is the latest at or after this offset",
                headers,
            )
        }
        Err(err) => PartsRefusal::Invariant(err).into_response(),
    })
}

/// The publish's own range refusals, from the same `HEAD`, so that a repeat
/// of one of these final answers gets it again with no part left: `410`
/// below the earliest retained offset, `409` beyond the tail. Each carries
/// `Stream-Next-Offset` as the publish's refusal does. Otherwise the key is
/// open.
fn open_unless_out_of_range(target: &PublishTarget, head: &HeadStreamResponse) -> KeyCheck {
    // The codes of the publish's own refusals (`StreamGone` and
    // `SnapshotConflict`).
    let (status, next_offset, code, message) = if target.snapshot_offset < head.retained_offset {
        (
            StatusCode::GONE,
            head.retained_offset,
            ErrorCode::StreamGone,
            "snapshot offset is older than the stream's retained offset",
        )
    } else if target.snapshot_offset > head.tail_offset {
        (
            StatusCode::CONFLICT,
            head.tail_offset,
            ErrorCode::SnapshotConflict,
            "snapshot offset is beyond the stream's tail",
        )
    } else {
        return KeyCheck::Open {
            incarnation: head.incarnation,
        };
    };
    let mut headers = HeaderMap::new();
    insert_offset(&mut headers, next_offset);
    KeyCheck::Answered(error_answer(status, code, message, headers))
}

/// Commits a snapshot uploaded in parts, staged for the stream's
/// incarnation `incarnation`: reads the manifest, then hands the commit to a
/// task the node owns, which holds a copy slot and runs the copy, the
/// publish and the deletion of the parts. The answer goes out as soon as
/// the publish answers, before the parts are deleted. A client that goes
/// away, or a gateway that times out, cancels none of it: the repeat finds
/// the snapshot published and answers `204` from its key, and a repeat, or
/// any other commit or discard of the stream incarnation, while the commit
/// runs answers `503` with `SnapshotCommitInProgress`.
///
/// The leader routed the commit and found its key open. `budget` is the
/// node's in-flight body budget, which the copy is charged against once it
/// holds its slot, until the copy ends. The publish carries the incarnation
/// the parts were found for as its precondition, so a stream recreated
/// meanwhile is not published to.
pub(crate) async fn commit(
    mut target: PublishTarget,
    commit: PartsCommit,
    incarnation: u64,
    budget: Option<BodyBudget>,
    manifest_permit: Option<AdmissionPermit>,
    body: Body,
) -> Response {
    let sent_token = target.if_incarnation.is_some();
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
    // The manifest is read: its share of the budget goes back.
    drop(manifest_permit);
    let manifest = match PartsManifest::parse(commit.parts, &manifest) {
        Ok(manifest) => manifest,
        Err(err) => return PartsRefusal::from(err).into_response(),
    };
    let Some(cold_store) = target.state.runtime.cold_store() else {
        return PartsRefusal::NoColdStore.into_response();
    };
    let commits = target.state.snapshot_commits.clone();
    let Some(running) = commits.claim(CommitKey {
        stream_id: target.stream_id.clone(),
        incarnation,
    }) else {
        target
            .state
            .http_metrics
            .snapshot_commits_in_progress
            .fetch_add(1, Ordering::Relaxed);
        return PartsRefusal::InProgress.into_response();
    };
    let Ok(slot) = commits.slots.clone().try_acquire_owned() else {
        target
            .state
            .http_metrics
            .snapshot_commits_busy
            .fetch_add(1, Ordering::Relaxed);
        return PartsRefusal::Busy.into_response();
    };
    let (answer, answered) = tokio::sync::oneshot::channel();
    // The node owns the commit from here, as it owns an admitted admin
    // mutation: dropping the request does not cancel it.
    let task = tokio::spawn(async move {
        let _running = running;
        let _slot = slot;
        let (response, spent) = run_commit(
            &cold_store,
            &target,
            &commit,
            &manifest,
            budget.as_ref(),
            sent_token,
        )
        .await;
        if answer.send(response).is_err() {
            tracing::debug!(
                stream = %target.stream_id,
                "the client of a snapshot commit went away; the commit ran to its end"
            );
        }
        if let Some(parts) = spent {
            let paths = parts.into_iter().map(|part| part.path).collect();
            target
                .state
                .staged_parts
                .delete_spent(
                    &cold_store,
                    &target.stream_id,
                    target
                        .state
                        .runtime
                        .locate(&target.stream_id)
                        .raft_group_id
                        .0,
                    paths,
                )
                .await;
        }
    });
    match answered.await {
        Ok(response) => response,
        Err(_closed) => {
            if let Err(err) = task.await {
                tracing::error!(error = %err, "a snapshot commit task failed");
            }
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::Internal,
                "the snapshot commit failed",
            )
        }
    }
}

/// Runs a commit: resolves its parts, copies them under its budget charge,
/// and publishes. Returns the answer and the parts to delete after it: the
/// parts of an answer final for the key, and of a digest mismatch, whose
/// copy consumed them. The parts of a refusal that copied nothing, or of a
/// retryable answer, stay for the repeat.
async fn run_commit(
    cold_store: &ColdStoreHandle,
    target: &PublishTarget,
    commit: &PartsCommit,
    manifest: &PartsManifest,
    budget: Option<&BodyBudget>,
    sent_token: bool,
) -> (Response, Option<Vec<ResolvedPart>>) {
    let owner = PartOwner {
        stream_id: &target.stream_id,
        raft_group_id: target
            .state
            .runtime
            .locate(&target.stream_id)
            .raft_group_id
            .0,
        incarnation: target.if_incarnation.unwrap_or(0),
    };
    let parts = match resolve(cold_store, owner, manifest, MAX_COLD_SNAPSHOT_BYTES).await {
        Ok(parts) => parts,
        // A part is gone: a commit of the same key may have published the
        // snapshot and deleted its parts after this one's key check.
        Err(PartsRefusal::UnknownPart { part }) => {
            return (recheck_key(target, commit, sent_token, part).await, None);
        }
        Err(refusal) => return (refusal.into_response(), None),
    };
    let (pieces, charge) = copy_plan(budget.map_or(u64::MAX, BodyBudget::capacity));
    // A copy never takes the room of the node's next request: on the 64 MiB
    // floor one copy runs at a time, beside a request of the full limit.
    let charged = match budget
        .map(|budget| budget.reserve_leaving(charge, limits::MAX_REQUEST_BYTES))
        .transpose()
    {
        Ok(charged) => charged,
        Err(_no_room) => {
            target
                .state
                .http_metrics
                .snapshot_commits_busy
                .fetch_add(1, Ordering::Relaxed);
            return (PartsRefusal::Busy.into_response(), None);
        }
    };
    let assembled = assemble(cold_store, target, &parts, commit, pieces).await;
    drop(charged);
    let assembled = match assembled {
        Ok(assembled) => assembled,
        // A part went away during the copy: a concurrent identical commit
        // may have published the snapshot and deleted it.
        Err(PartsRefusal::UnknownPart { part }) => {
            return (recheck_key(target, commit, sent_token, part).await, None);
        }
        Err(refusal @ PartsRefusal::DigestMismatch { .. }) => {
            return (refusal.into_response(), Some(parts));
        }
        Err(refusal) => return (refusal.into_response(), None),
    };
    let response = target.publish(SnapshotUpload::Cold(assembled)).await;
    let response = match parts.first() {
        Some(part) => untokened(response, sent_token, &part.id),
        None => response,
    };
    let spent = final_for_key(response.status()).then_some(parts);
    (response, spent)
}

/// The answer to a commit one of whose parts is gone, from its key again:
/// `204` when a commit of the same key published the snapshot, `409` or
/// `410` when another one decides it, `400` for the unknown `part`
/// otherwise.
async fn recheck_key(
    target: &PublishTarget,
    commit: &PartsCommit,
    sent_token: bool,
    part: PartId,
) -> Response {
    match key_check(target, commit).await {
        KeyCheck::Answered(answer) => untokened(answer, sent_token, &part),
        KeyCheck::Open { .. } => PartsRefusal::UnknownPart { part }.into_response(),
    }
}

/// A `412` for the token a commit was forced to (the incarnation its parts
/// were staged for), when its client named no token: the stream was
/// recreated meanwhile, and the client gets what a commit arriving after
/// the recreate gets, a `400` for an unknown part. A client that named the
/// token keeps its `412`.
fn untokened(response: Response, sent_token: bool, part: &PartId) -> Response {
    if response.status() == StatusCode::PRECONDITION_FAILED && !sent_token {
        return PartsRefusal::UnknownPart { part: part.clone() }.into_response();
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
/// when the digest is the one the commit names. Up to `pieces_in_flight`
/// pieces are read ahead of the one hashed and written, which stay in
/// order. A part that is gone answers [`PartsRefusal::UnknownPart`].
async fn assemble(
    cold_store: &ColdStoreHandle,
    target: &PublishTarget,
    parts: &[ResolvedPart],
    commit: &PartsCommit,
    pieces_in_flight: usize,
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
                    piece_len(part.size.saturating_sub(start)),
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
        .buffered(pieces_in_flight);
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
/// incarnation. A stream that holds as many staged parts as it may answers
/// `429` naming the limit ([`crate::staged_parts`]). It meets the in-flight
/// body budget but no WAL or Raft-log pressure check, since it touches
/// neither until it is committed. The
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
    // The anchor of the retirement rule: read before the HEAD that names
    // the incarnation.
    let anchor_ms = state.unix_time_ms();
    let head = state
        .runtime
        .head_stream(HeadStreamRequest {
            stream_id: stream_id.clone(),
            now_ms: anchor_ms,
            linearizable: true,
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
    let raft_group_id = state.runtime.locate(&stream_id).raft_group_id;
    // The count is valid for the term this node leads in. A node that lost
    // the lead since routing answers as the routing does now.
    let Some(term) = front_door::leader_term(&state, raft_group_id) else {
        let answer = front_door::not_leading(&state, &stream_id, &request_target).await;
        return crate::answer_before_body(answer, version, &headers, body).await;
    };
    // The stream's staged parts are an answer of the leader's own state,
    // decided before the pressure checks, so a listing holds no budget.
    let reservation = match state
        .staged_parts
        .reserve(
            &cold_store,
            StagedOwner {
                stream_id: &stream_id,
                raft_group_id: raft_group_id.0,
                incarnation,
                term,
            },
            part_reservation(&headers, &body),
            anchor_ms,
        )
        .await
    {
        Ok(reservation) => reservation,
        Err(refusal) => {
            if matches!(refusal, PartsRefusal::StagedLimit { .. }) {
                state
                    .http_metrics
                    .snapshot_parts_staged_limit
                    .fetch_add(1, Ordering::Relaxed);
            }
            return crate::answer_before_body(refusal.into_response(), version, &headers, body)
                .await;
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
        raft_group_id: raft_group_id.0,
        incarnation,
    };
    match stage(&cold_store, owner, body).await {
        Ok((part, size)) => {
            reservation.staged(size);
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            PartStaged { part }.render(|name, value| insert_header_str(&mut headers, name, value));
            (StatusCode::CREATED, headers).into_response()
        }
        Err(refusal) => refusal.into_response(),
    }
}

/// What a part reserves of its stream's staged bytes before its body is
/// read: its declared size, which ingress admission held to the request
/// limit, or the request limit when it declares none.
fn part_reservation(headers: &HeaderMap, body: &Body) -> u64 {
    crate::admission::declared_body_bytes(headers, body).min(limits::MAX_REQUEST_BYTES)
}

/// Streams a part's body, at most the request limit, into a new part
/// object for `owner`, and returns its id and size.
async fn stage(
    cold_store: &ColdStoreHandle,
    owner: PartOwner<'_>,
    body: Body,
) -> Result<(PartId, u64), PartsRefusal> {
    let id = PartId::new(new_snapshot_part_id()).map_err(PartsRefusal::Invariant)?;
    let (_written, size) = stream_into_object(
        cold_store,
        &owner.path(id.as_str()),
        Bytes::new(),
        body.into_data_stream(),
        None,
        limits::MAX_REQUEST_BYTES,
    )
    .await?;
    Ok((id, size))
}

/// `DELETE {stream}/snapshot/parts`: discards every staged part of the
/// stream's current incarnation, and the retired parts of earlier ones, and
/// answers `204` with `Stream-Incarnation`. It honors a `Stream-Incarnation`
/// precondition, is answered by the stream's leader (`307` elsewhere), and
/// waits for no commit: one that runs answers `503` with
/// `SnapshotCommitInProgress`. A part still uploading is not discarded.
pub(crate) async fn discard_snapshot_parts(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<crate::StreamPath>,
    headers: HeaderMap,
) -> Response {
    let stream_id = path.into_stream_id();
    let if_incarnation = match crate::incarnation_precondition(&headers) {
        Ok(if_incarnation) => if_incarnation,
        Err(response) => return *response,
    };
    let Some(cold_store) = state.runtime.cold_store() else {
        return PartsRefusal::NoColdStore.into_response();
    };
    let request_target = crate::request_target(&uri);
    if let HeadDecision::Answer(answer) =
        front_door::route_before_body(&state, &stream_id, &request_target, None, None).await
    {
        return answer;
    }
    let anchor_ms = state.unix_time_ms();
    let incarnation = match state
        .runtime
        .head_stream(HeadStreamRequest {
            stream_id: stream_id.clone(),
            now_ms: anchor_ms,
            linearizable: true,
            read_index: None,
        })
        .await
    {
        Ok(head) => match crate::read_precondition_failed(if_incarnation, head.incarnation) {
            Some(refused) => return refused,
            None => head.incarnation,
        },
        Err(err) => {
            return crate::runtime_error_or_leader_redirect_async(&state, err, &request_target)
                .await;
        }
    };
    let raft_group_id = state.runtime.locate(&stream_id).raft_group_id;
    let Some(term) = front_door::leader_term(&state, raft_group_id) else {
        return front_door::not_leading(&state, &stream_id, &request_target).await;
    };
    let Some(_running) = state.snapshot_commits.claim(CommitKey {
        stream_id: stream_id.clone(),
        incarnation,
    }) else {
        state
            .http_metrics
            .snapshot_commits_in_progress
            .fetch_add(1, Ordering::Relaxed);
        return PartsRefusal::InProgress.into_response();
    };
    let discarded = state
        .staged_parts
        .discard(
            &cold_store,
            StagedOwner {
                stream_id: &stream_id,
                raft_group_id: raft_group_id.0,
                incarnation,
                term,
            },
            anchor_ms,
        )
        .await;
    match discarded {
        Ok(listed) => {
            tracing::debug!(
                stream = %stream_id,
                discarded = listed.deleted,
                "discarded a stream's staged snapshot parts"
            );
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            crate::render::insert_incarnation(&mut headers, incarnation);
            (StatusCode::NO_CONTENT, headers).into_response()
        }
        Err(refusal) => refusal.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::response::IntoResponse;
    use ursula_headers::ErrorCode;
    use ursula_headers::PartId;
    use ursula_headers::PartsManifest;
    use ursula_headers::SizeLimit;
    use ursula_runtime::ColdStore;
    use ursula_runtime::MAX_COLD_SNAPSHOT_BYTES;
    use ursula_runtime::new_snapshot_part_id;
    use ursula_shard::BucketStreamId;

    use super::MIN_COPY_BYTES;
    use super::PartOwner;
    use super::PartsRefusal;
    use super::copy_plan;
    use super::resolve;

    const MIB: u64 = 1024 * 1024;

    /// The copy's read-ahead follows the budget: a 64 MiB node (the tiny and
    /// small presets) reads one piece ahead and charges 32 MiB, leaving room
    /// for one request; the default 256 MiB reads four ahead.
    #[test]
    fn the_read_ahead_follows_the_budget() {
        assert_eq!(copy_plan(64 * MIB), (1, MIN_COPY_BYTES));
        assert_eq!(MIN_COPY_BYTES, 32 * MIB);
        assert_eq!(copy_plan(128 * MIB), (1, 32 * MIB));
        assert_eq!(copy_plan(256 * MIB), (4, 56 * MIB));
        assert_eq!(copy_plan(u64::MAX), (4, 56 * MIB));
    }

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
        assert_eq!(
            crate::render::error_code_of(response).await,
            ErrorCode::SnapshotTooLarge
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
