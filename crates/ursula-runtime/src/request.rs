use std::sync::Arc;

use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;
use ursula_stream::ColdChunkRef;
use ursula_stream::ColdFlushPressure;
use ursula_stream::ExternalPayloadRef;
use ursula_stream::ProducerAck;
use ursula_stream::ProducerRequest;
use ursula_stream::StreamReadPlan;
use ursula_stream::StreamReadSegment;

use crate::cold_index::ColdIndexPageCache;
use crate::cold_index::ColdStoreColdIndexPageStore;
use crate::cold_store::ColdStoreHandle;
use crate::cold_store::DEFAULT_CONTENT_TYPE;
use crate::cold_uploads::ProposedUpload;
use crate::cold_uploads::UploadFence;
use crate::engine::GroupEngineError;
use crate::engine::in_memory::InMemoryGroupEngine;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStreamRequest {
    pub stream_id: BucketStreamId,
    pub content_type: String,
    pub content_type_explicit: bool,
    pub initial_payload: Bytes,
    pub close_after: bool,
    pub stream_seq: Option<String>,
    pub stream_ttl_seconds: Option<u64>,
    pub stream_expires_at_ms: Option<u64>,
    pub now_ms: u64,
    /// `Stream-Incarnation` request precondition (D12): apply only to this
    /// incarnation of the stream. `None` for a request without the header.
    pub if_incarnation: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStreamExternalRequest {
    pub stream_id: BucketStreamId,
    pub content_type: String,
    pub initial_payload: ExternalPayloadRef,
    /// JSON messages in the staged payload, counted over the normalized
    /// body while staging. Zero on any other content type.
    pub message_count: u64,
    pub close_after: bool,
    pub stream_seq: Option<String>,
    pub stream_ttl_seconds: Option<u64>,
    pub stream_expires_at_ms: Option<u64>,
    pub now_ms: u64,
    /// See [`CreateStreamRequest::if_incarnation`].
    pub if_incarnation: Option<u64>,
    /// The upload of `initial_payload`, fenced to the leadership that
    /// admitted the create (F14j).
    pub upload: Arc<ProposedUpload>,
}

impl CreateStreamExternalRequest {
    pub fn from_create_request(
        request: CreateStreamRequest,
        initial_payload: ExternalPayloadRef,
        message_count: u64,
        upload: Arc<ProposedUpload>,
    ) -> Self {
        Self {
            stream_id: request.stream_id,
            content_type: request.content_type,
            initial_payload,
            message_count,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            stream_ttl_seconds: request.stream_ttl_seconds,
            stream_expires_at_ms: request.stream_expires_at_ms,
            now_ms: request.now_ms,
            if_incarnation: request.if_incarnation,
            upload,
        }
    }
}

impl CreateStreamRequest {
    /// The canonical JSON messages of the initial payload, zero for any
    /// other content type. A payload that is not canonical counts zero, which
    /// apply refuses on a JSON stream.
    pub fn canonical_message_count(&self) -> u64 {
        ursula_stream::canonical_json_message_count(&self.content_type, &self.initial_payload)
            .unwrap_or_default()
    }

    pub fn new(stream_id: BucketStreamId, content_type: impl Into<String>) -> Self {
        Self {
            stream_id,
            content_type: content_type.into(),
            content_type_explicit: true,
            initial_payload: Bytes::new(),
            close_after: false,
            stream_seq: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            now_ms: 0,
            if_incarnation: None,
        }
    }
}

/// A stream's and its group's hot payload bytes right after a write applied.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteHotBacklog {
    pub stream_hot_bytes: u64,
    pub group_hot_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateStreamResponse {
    pub placement: ShardPlacement,
    pub next_offset: u64,
    pub closed: bool,
    pub already_exists: bool,
    pub group_commit_index: u64,
    /// The token of the stream incarnation the write applied to, rendered
    /// as `Stream-Incarnation` (D12). A token is never `0`; `0` means
    /// unknown and renders no header.
    pub incarnation: u64,
    /// Hot backlog after the write applied (bounded-stream-state F6a), so
    /// the runtime records its metric without a second state-machine round
    /// trip.
    pub hot_backlog: Option<WriteHotBacklog>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadStreamRequest {
    pub stream_id: BucketStreamId,
    pub now_ms: u64,
    /// `true` when the HEAD promises linearizability (D10), so the leader
    /// confirms a read index first: a client HEAD and the `offset=now`
    /// resolution of a `consistency=leader` read. `false` for the `offset=now`
    /// resolution of `consistency=local` catch-up reads, which needs only
    /// the leader's applied state. Live reads take no separate HEAD; see
    /// [`LiveReadOwner`].
    pub linearizable: bool,
    /// The read index this request was linearized at before it was queued
    /// (D10): `ShardRuntime` confirms the group's leadership and waits
    /// for the local apply outside the group actor, then sets it. The
    /// engine serves the read without a quorum round trip only while this
    /// replica leads and has applied that index. Callers leave it `None`;
    /// the runtime overwrites it.
    pub read_index: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadStreamResponse {
    pub placement: ShardPlacement,
    pub content_type: String,
    pub tail_offset: u64,
    pub cold_hot_start_offset: u64,
    pub closed: bool,
    pub stream_ttl_seconds: Option<u64>,
    pub stream_expires_at_ms: Option<u64>,
    pub snapshot_offset: Option<u64>,
    pub snapshot_digest: Option<String>,
    pub retained_offset: u64,
    /// The stream's public `Stream-Incarnation` token in the group's current
    /// history: it changes when the stream is deleted and recreated, and at
    /// every history event (#581 section 4.2).
    pub incarnation: u64,
    /// What the stream's current history began from, after a history event
    /// (`Stream-Previous-Incarnation`, `Stream-Inherited-Offset`, W5).
    pub inherited: Option<ursula_stream::InheritedReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadStreamRequest {
    pub stream_id: BucketStreamId,
    pub offset: u64,
    pub max_len: usize,
    pub now_ms: u64,
    /// Require the owning Raft leader's applied state instead of permitting a
    /// local follower read. Recovery paths use this after an acknowledged
    /// write; ordinary catch-up consumers keep the cheaper follower-local
    /// behavior.
    pub leader_only: bool,
    /// For a `leader_only` read, the index the runtime confirmed before
    /// queueing it; see [`HeadStreamRequest::read_index`]. Without
    /// `leader_only`, a live read sets it to its owner's confirmed index
    /// ([`LiveReadOwner::read_index`]): the engine then serves the read only
    /// while this replica leads and has applied that index, never forwards
    /// it, and otherwise answers the leader redirect or 503. `None` for
    /// every other read.
    pub read_index: Option<u64>,
    /// What a follower may answer this read with from its own applied
    /// state. Ignored with `leader_only`, which a follower always forwards.
    pub follower: FollowerRead,
}

/// What a follower may serve of a read that is not `leader_only` from its
/// own applied state, without asking the leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowerRead {
    /// Any prefix of what it applied, as a base-protocol catch-up read
    /// allows: offsets are immutable, so the bytes are correct, and a
    /// follower that lags may only return fewer of them.
    Prefix,
    /// Only an answer that the server marks immutable ([`immutable_read`]),
    /// for a chunk of the immutable-read resources (extensions Section 4,
    /// #581 section 5.3). The follower forwards any other answer to the
    /// leader, so an unmarked answer always means that the tail of an open
    /// stream lies within the chunk, never that a follower lags.
    Immutable,
}

/// Where a read ended, as seen by the stream state that served it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadEnd {
    /// Below the tail: more bytes follow. Closure is visible only at the
    /// tail, so a closed stream's read ends here too.
    BelowTail,
    /// At the tail of an open stream: a later append may extend it.
    OpenTail,
    /// At the tail of a closed stream: nothing ever follows.
    ClosedTail,
}

impl ReadEnd {
    /// The one reading of a read's `up_to_date` and `closed`.
    pub fn of(up_to_date: bool, closed: bool) -> Self {
        match (up_to_date, closed) {
            (false, _) => Self::BelowTail,
            (true, false) => Self::OpenTail,
            (true, true) => Self::ClosedTail,
        }
    }
}

/// Whether a read of `[offset, next_offset)` that asked for `requested_len`
/// bytes and ended at `end` is one that no later read of the same request
/// can contradict, under the same incarnation token (extensions Section
/// 4.3). It either reached what it asked for strictly below the tail, which
/// for a chunk means the end of the chunk, or it ended at the tail of a
/// closed stream. A read that ends at the tail of an open stream never is,
/// even when it filled its request: the stream can still be closed there.
pub fn immutable_read(offset: u64, next_offset: u64, requested_len: usize, end: ReadEnd) -> bool {
    match end {
        ReadEnd::ClosedTail => true,
        ReadEnd::OpenTail
            if ursula_stream::sim_controls::off(
                ursula_stream::sim_controls::Defence::ImmutableOpenTail,
            ) =>
        {
            filled(offset, next_offset, requested_len)
        }
        ReadEnd::OpenTail => false,
        ReadEnd::BelowTail => filled(offset, next_offset, requested_len),
    }
}

/// Whether a read of `[offset, next_offset)` returned all of the
/// `requested_len` bytes it asked for.
fn filled(offset: u64, next_offset: u64, requested_len: usize) -> bool {
    requested_len > 0
        && next_offset
            .checked_sub(offset)
            .is_some_and(|returned| u64::try_from(requested_len) == Ok(returned))
}

/// What a live read (SSE, long-poll) learned from its owner check (D10):
/// this replica confirmed it leads the stream's group at `read_index` and
/// read `head` from state at or after that index. The live read resolves
/// `offset=now`, existence, content type, incarnation and the 416 check
/// from `head`, and pins its later reads to `read_index`
/// ([`ReadStreamRequest::read_index`]), so no lookup of a live read is
/// answered by a replica that has not applied what the owner confirmed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveReadOwner {
    /// `None` for an engine without a Raft log (single-node, in-memory).
    pub read_index: Option<u64>,
    pub head: HeadStreamResponse,
}

impl ReadStreamRequest {
    pub(crate) fn same_wait_plan(&self, other: &Self) -> bool {
        self.stream_id == other.stream_id
            && self.offset == other.offset
            && self.max_len == other.max_len
            && self.leader_only == other.leader_only
            && self.read_index.is_some() == other.read_index.is_some()
            && self.follower == other.follower
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadStreamResponse {
    pub placement: ShardPlacement,
    pub offset: u64,
    pub next_offset: u64,
    pub content_type: String,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    pub up_to_date: bool,
    pub closed: bool,
    /// The token of the stream that served the read (D12). A token is
    /// never `0`; `0` means unknown and renders no header.
    pub incarnation: u64,
}

impl ReadStreamResponse {
    /// Where the read ended, as seen by the state that served it.
    pub fn end(&self) -> ReadEnd {
        ReadEnd::of(self.up_to_date, self.closed)
    }

    /// Whether the read is immutable for a request of `requested_len`
    /// bytes ([`immutable_read`]).
    pub fn is_immutable(&self, requested_len: usize) -> bool {
        immutable_read(self.offset, self.next_offset, requested_len, self.end())
    }
}

pub enum GroupReadStreamBody {
    Materialized(Vec<u8>),
    Planned {
        stream_id: BucketStreamId,
        plan: StreamReadPlan,
        cold_store: Option<ColdStoreHandle>,
        cold_index_cache: Option<Arc<ColdIndexPageCache<ColdStoreColdIndexPageStore>>>,
    },
    #[cfg(test)]
    Blocking {
        entered: Arc<crate::rt::sync::Notify>,
        materialized: Arc<crate::rt::sync::Notify>,
        release: Arc<crate::rt::sync::Notify>,
        payload: Vec<u8>,
    },
}

pub struct GroupReadStreamParts {
    pub placement: ShardPlacement,
    /// See [`ReadStreamResponse::incarnation`].
    pub incarnation: u64,
    pub offset: u64,
    pub next_offset: u64,
    pub content_type: String,
    pub up_to_date: bool,
    pub closed: bool,
    pub body: GroupReadStreamBody,
}

impl GroupReadStreamParts {
    pub fn from_response(response: ReadStreamResponse) -> Self {
        Self {
            placement: response.placement,
            incarnation: response.incarnation,
            offset: response.offset,
            next_offset: response.next_offset,
            content_type: response.content_type,
            up_to_date: response.up_to_date,
            closed: response.closed,
            body: GroupReadStreamBody::Materialized(response.payload),
        }
    }

    pub fn from_plan(
        placement: ShardPlacement,
        stream_id: BucketStreamId,
        plan: StreamReadPlan,
        cold_store: Option<ColdStoreHandle>,
        cold_index_cache: Option<Arc<ColdIndexPageCache<ColdStoreColdIndexPageStore>>>,
    ) -> Self {
        Self {
            placement,
            incarnation: plan.incarnation,
            offset: plan.offset,
            next_offset: plan.next_offset,
            content_type: plan.content_type.clone(),
            up_to_date: plan.up_to_date,
            closed: plan.closed,
            body: GroupReadStreamBody::Planned {
                stream_id,
                plan,
                cold_store,
                cold_index_cache,
            },
        }
    }

    pub async fn into_response(self) -> Result<ReadStreamResponse, GroupEngineError> {
        let payload = match &self.body {
            GroupReadStreamBody::Materialized(payload) => payload.clone(),
            GroupReadStreamBody::Planned {
                stream_id,
                plan,
                cold_store,
                cold_index_cache,
            } => {
                InMemoryGroupEngine::read_payload_from_plan(
                    cold_store.as_ref(),
                    cold_index_cache.as_ref(),
                    stream_id,
                    plan,
                )
                .await?
            }
            #[cfg(test)]
            GroupReadStreamBody::Blocking {
                entered,
                materialized,
                release,
                payload,
            } => {
                entered.notify_one();
                materialized.notify_one();
                release.notified().await;
                payload.clone()
            }
        };
        Ok(ReadStreamResponse {
            placement: self.placement,
            offset: self.offset,
            next_offset: self.next_offset,
            content_type: self.content_type,
            payload,
            up_to_date: self.up_to_date,
            closed: self.closed,
            incarnation: self.incarnation,
        })
    }

    /// Where the read ended, as seen by the state that served it.
    pub fn end(&self) -> ReadEnd {
        ReadEnd::of(self.up_to_date, self.closed)
    }

    /// Whether the read is immutable for a request of `requested_len`
    /// bytes ([`immutable_read`]).
    pub fn is_immutable(&self, requested_len: usize) -> bool {
        immutable_read(self.offset, self.next_offset, requested_len, self.end())
    }

    pub fn payload_is_empty(&self) -> bool {
        match &self.body {
            GroupReadStreamBody::Materialized(payload) => payload.is_empty(),
            GroupReadStreamBody::Planned { plan, .. } => {
                plan.segments.iter().all(|segment| match segment {
                    StreamReadSegment::Hot(payload) => payload.is_empty(),
                    StreamReadSegment::ColdIndex(segment) => segment.len == 0,
                    StreamReadSegment::Object(segment) => segment.len == 0,
                })
            }
            #[cfg(test)]
            GroupReadStreamBody::Blocking { payload, .. } => payload.is_empty(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishSnapshotRequest {
    pub stream_id: BucketStreamId,
    pub snapshot_offset: u64,
    pub content_type: String,
    /// Inline body; empty when `cold_body` is set.
    pub payload: Bytes,
    /// A body staged as a cold-tier object (bounded-state F16). Proposed as `PublishSnapshotExternal`.
    pub cold_body: Option<ColdSnapshotBody>,
    pub now_ms: u64,
    /// JSON streams: the incarnation whose byte before `snapshot_offset`
    /// the proposer read and found LF (see `StreamCommand::PublishSnapshot`).
    pub expected_incarnation: Option<u64>,
    /// `Stream-Incarnation` request precondition (D12): apply only to this
    /// incarnation of the stream. `None` for a request without the header.
    pub if_incarnation: Option<u64>,
}

/// A snapshot body staged in the cold tier with the digest computed while
/// staging it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdSnapshotBody {
    pub object: ExternalPayloadRef,
    pub digest: String,
    /// The upload of `object`, fenced to the leadership that admitted the
    /// publish (F14j).
    pub upload: Arc<ProposedUpload>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishSnapshotResponse {
    pub placement: ShardPlacement,
    pub snapshot_offset: u64,
    pub snapshot_digest: String,
    pub group_commit_index: u64,
    /// The token of the stream incarnation the write applied to, rendered
    /// as `Stream-Incarnation` (D12). A token is never `0`; `0` means
    /// unknown and renders no header.
    pub incarnation: u64,
    /// Hot backlog after the write applied (bounded-stream-state F6a), so
    /// the runtime records its metric without a second state-machine round
    /// trip.
    pub hot_backlog: Option<WriteHotBacklog>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvanceRetentionRequest {
    pub stream_id: BucketStreamId,
    pub retained_offset: u64,
    pub now_ms: u64,
    /// As on [`PublishSnapshotRequest::expected_incarnation`].
    pub expected_incarnation: Option<u64>,
    /// See [`PublishSnapshotRequest::if_incarnation`].
    pub if_incarnation: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvanceRetentionResponse {
    pub placement: ShardPlacement,
    pub retained_offset: u64,
    pub group_commit_index: u64,
    /// The token of the stream incarnation the write applied to, rendered
    /// as `Stream-Incarnation` (D12). A token is never `0`; `0` means
    /// unknown and renders no header.
    pub incarnation: u64,
    /// Hot backlog after the write applied (bounded-stream-state F6a), so
    /// the runtime records its metric without a second state-machine round
    /// trip.
    pub hot_backlog: Option<WriteHotBacklog>,
}

/// One tidy pass over a group (bounded-state F0, F20). Every replica first
/// samples its group's retirement clock
/// ([`crate::expiry_sweep::RetirementClock`]), which gives the usage seq
/// through which end records past their retention under
/// `deleted_data_max_age` are due. A
/// leader then runs the group sweep: while the group's TTL front is due at
/// [`crate::expiry_sweep::sweep_now_ms`] of `now_ms` or an end record is
/// due, it proposes `Sweep`, at most `max_expiry_sweeps` entries, and on an
/// engine with a log starts none after `expiry_sweep_budget`
/// ([`crate::expiry_sweep`]). It then proposes `TidyStream` for at most
/// `max_streams` streams with normalization debt at `now_ms`. Followers
/// propose nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TidyStreamsRequest {
    pub max_streams: usize,
    pub max_expiry_sweeps: usize,
    /// [`crate::expiry_sweep::GROUP_BUDGET`] for a scheduled sweep,
    /// [`crate::expiry_sweep::RERUN_BUDGET`] for one that drains a capped
    /// group.
    pub expiry_sweep_budget: std::time::Duration,
    /// The node's `raft.deleted_data_max_age`
    /// (`UrsulaConfig::effective_deleted_data_max_age`). An end record stays
    /// for its `RaftConfig::end_record_retention` before the sweep folds it.
    pub deleted_data_max_age: std::time::Duration,
    pub now_ms: u64,
}

/// One tidy pass (bounded-state F0) of a group, or the total over a node's
/// groups: what it committed, and the first error that ended a group's pass
/// early. The totals survive that error, so a sweep that stopped at its cap
/// runs again soon even when another step of the pass failed.
#[derive(Debug)]
pub struct TidyStreamsPass<E> {
    pub report: TidyStreamsResponse,
    pub error: Option<E>,
}

impl<E> TidyStreamsPass<E> {
    /// A pass that committed nothing and failed nowhere.
    pub fn empty() -> Self {
        Self {
            report: TidyStreamsResponse::default(),
            error: None,
        }
    }

    /// The totals, or the first error.
    pub fn into_result(self) -> Result<TidyStreamsResponse, E> {
        match self.error {
            Some(err) => Err(err),
            None => Ok(self.report),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TidyStreamsResponse {
    /// `TidyStream` commands committed by this pass.
    pub tidied: u64,
    /// Tidied streams that still report debt after their command.
    pub debt_remaining: u64,
    /// `Sweep` entries committed by this pass.
    pub expiry_sweeps: u64,
    /// Expired streams those entries removed.
    pub expiry_swept_streams: u64,
    /// End records those entries folded into their buckets' usage (F20).
    pub end_records_retired: u64,
    /// Groups whose sweep stopped at its entry cap, its time budget or an
    /// error while expired streams or due end records remained (0 or 1 for
    /// one group).
    pub expiry_due_groups: u64,
    /// Of those, the groups whose sweep stopped at its entry cap or its time
    /// budget, not at an error: the tidy driver sweeps them again a second
    /// later.
    pub expiry_capped_groups: u64,
}

impl TidyStreamsResponse {
    /// Adds another group's pass to this total.
    pub fn add(&mut self, other: &Self) {
        self.tidied = self.tidied.saturating_add(other.tidied);
        self.debt_remaining = self.debt_remaining.saturating_add(other.debt_remaining);
        self.expiry_sweeps = self.expiry_sweeps.saturating_add(other.expiry_sweeps);
        self.expiry_swept_streams = self
            .expiry_swept_streams
            .saturating_add(other.expiry_swept_streams);
        self.end_records_retired = self
            .end_records_retired
            .saturating_add(other.end_records_retired);
        self.expiry_due_groups = self
            .expiry_due_groups
            .saturating_add(other.expiry_due_groups);
        self.expiry_capped_groups = self
            .expiry_capped_groups
            .saturating_add(other.expiry_capped_groups);
    }
}

/// Result of one replicated `Sweep` entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SweepResponse {
    pub placement: ShardPlacement,
    /// Streams the entry removed, at most its limit.
    pub expired: u64,
    /// End records the entry folded, at most its limit.
    pub retired: u64,
    pub group_commit_index: u64,
}

/// Result of one replicated `TidyStream` command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TidyStreamResponse {
    pub placement: ShardPlacement,
    pub debt_remaining: bool,
    pub group_commit_index: u64,
}

/// A restore of one group from a backup: the import is a history event
/// (#581 section 4.4), keyed by the backup's digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportGroupStateRequest {
    pub snapshot: Box<ursula_stream::StreamSnapshot>,
    /// The new history's id, drawn by the proposer.
    pub history: std::num::NonZeroU64,
    /// The BLAKE3-256 digest of the backup.
    pub backup_digest: ursula_stream::BackupDigest,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ImportGroupStateResponse {
    pub placement: ShardPlacement,
    pub buckets: u64,
    pub streams: u64,
    pub group_commit_index: u64,
    /// The history the import started.
    pub history: std::num::NonZeroU64,
    /// The group already held this import; nothing changed.
    pub already: bool,
}

/// The answer of a history event (`NewHistory`): the group's genesis or
/// the event after an accepted loss.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NewHistoryResponse {
    pub placement: ShardPlacement,
    pub history: std::num::NonZeroU64,
    pub group_commit_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSnapshotRequest {
    pub stream_id: BucketStreamId,
    pub snapshot_offset: Option<u64>,
    pub now_ms: u64,
    /// See [`HeadStreamRequest::read_index`].
    pub read_index: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSnapshotResponse {
    pub placement: ShardPlacement,
    pub snapshot_offset: u64,
    pub next_offset: u64,
    pub content_type: String,
    pub snapshot_digest: String,
    /// Inline body; empty when `object` holds it.
    pub payload: Vec<u8>,
    /// Cold-tier object holding the body (F16). Callers stream
    /// it from the cold store.
    pub object: Option<ExternalPayloadRef>,
    pub up_to_date: bool,
    /// See [`ReadStreamResponse::incarnation`].
    pub incarnation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapStreamRequest {
    pub stream_id: BucketStreamId,
    pub now_ms: u64,
    /// See [`HeadStreamRequest::read_index`].
    pub read_index: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapUpdate {
    pub start_offset: u64,
    pub next_offset: u64,
    pub content_type: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapStreamResponse {
    pub placement: ShardPlacement,
    pub snapshot_offset: Option<u64>,
    pub snapshot_content_type: String,
    /// Inline snapshot body; empty when `snapshot_object` holds it.
    pub snapshot_payload: Vec<u8>,
    /// Cold-tier object holding the snapshot body (F16).
    pub snapshot_object: Option<ExternalPayloadRef>,
    pub updates: Vec<BootstrapUpdate>,
    pub next_offset: u64,
    pub up_to_date: bool,
    pub closed: bool,
    /// See [`ReadStreamResponse::incarnation`].
    pub incarnation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseStreamRequest {
    pub stream_id: BucketStreamId,
    pub stream_seq: Option<String>,
    pub producer: Option<ProducerRequest>,
    pub now_ms: u64,
    /// `Stream-Incarnation` request precondition (D12): apply only to this
    /// incarnation of the stream. `None` for a request without the header.
    pub if_incarnation: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloseStreamResponse {
    pub placement: ShardPlacement,
    pub next_offset: u64,
    pub group_commit_index: u64,
    /// The producer write's answer when the close carried producer headers.
    pub producer: Option<ProducerAck>,
    /// The token of the stream incarnation the write applied to, rendered
    /// as `Stream-Incarnation` (D12). A token is never `0`; `0` means
    /// unknown and renders no header.
    pub incarnation: u64,
}

impl CloseStreamResponse {
    /// Whether the write is a repeat of a producer write that applied
    /// before, so this request changed nothing.
    pub fn deduplicated(&self) -> bool {
        self.producer.as_ref().is_some_and(ProducerAck::is_repeat)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteStreamRequest {
    pub stream_id: BucketStreamId,
    /// The proposer's clock: a stream expired at it answers 404, as a
    /// missing stream does.
    pub now_ms: u64,
    /// `Stream-Incarnation` request precondition (D12): apply only to this
    /// incarnation of the stream. `None` for a request without the header.
    pub if_incarnation: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteStreamResponse {
    pub placement: ShardPlacement,
    pub group_commit_index: u64,
    /// The stream had expired at the delete's `now_ms`: the delete removed
    /// it, and the request answers 404, as for a missing stream.
    pub expired: bool,
    /// Hot backlog after the write applied (bounded-stream-state F6a), so
    /// the runtime records its metric without a second state-machine round
    /// trip.
    pub hot_backlog: Option<WriteHotBacklog>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckColdGcResponse {
    pub placement: ShardPlacement,
    pub removed: u64,
    pub group_commit_index: u64,
}

/// Result of a replicated `DeferColdGc` (bounded-state F14b).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeferColdGcResponse {
    pub placement: ShardPlacement,
    /// The entry's sequence number at the tail, or `None` when no pending
    /// entry had the requested one.
    pub new_seq: Option<u64>,
    pub group_commit_index: u64,
}

/// A bucket purge of one group (tenant offboarding).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgeBucketRequest {
    pub bucket_id: String,
    /// The admin request's clock, stamped once and sent to every group of
    /// one purge: the purged versions' `ended_at_ms` (F20).
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PurgeBucketResponse {
    pub placement: ShardPlacement,
    pub removed_streams: u64,
    /// Cold-GC entries this group still holds for the bucket.
    pub pending_cold_gc_entries: u64,
    pub group_commit_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushColdRequest {
    pub stream_id: BucketStreamId,
    pub chunk: ColdChunkRef,
    /// Cold generation the chunk was planned from
    /// ([`ursula_stream::ColdFlushCandidate::cold_generation`]); see
    /// `StreamCommand::FlushCold`.
    pub cold_generation: u64,
    /// The upload of `chunk`'s object, fenced to the leadership the flush
    /// was planned under (F14j). The slices of one pack share it.
    pub upload: Arc<ProposedUpload>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlushColdResponse {
    pub placement: ShardPlacement,
    pub hot_start_offset: u64,
    pub group_commit_index: u64,
    /// Hot backlog after the write applied (bounded-stream-state F6a), so
    /// the runtime records its metric without a second state-machine round
    /// trip.
    pub hot_backlog: Option<WriteHotBacklog>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactColdRequest {
    pub stream_id: BucketStreamId,
    pub old_chunks: Vec<ColdChunkRef>,
    pub replacement: ColdChunkRef,
    pub gc_not_before_ms: u64,
    /// The upload of `replacement`'s object, fenced to the leadership the
    /// compaction was planned under (F14j).
    pub upload: Arc<ProposedUpload>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactColdResponse {
    pub placement: ShardPlacement,
    pub compacted_chunks: u64,
    pub compacted_bytes: u64,
    pub group_commit_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TouchStreamAccessResponse {
    pub placement: ShardPlacement,
    pub changed: bool,
    pub expired: bool,
    pub group_commit_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanColdFlushRequest {
    pub stream_id: BucketStreamId,
    pub min_hot_bytes: usize,
    pub max_flush_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanGroupColdFlushRequest {
    pub min_hot_bytes: usize,
    pub max_flush_bytes: usize,
    /// Maximum aggregate payload bytes returned by one group planning pass.
    pub max_batch_bytes: usize,
    /// Node-level flush pressure: the group drains, largest streams first,
    /// its proportional share of the node's excess hot bytes
    /// (bounded-stream-state F10).
    pub pressure: Option<ColdFlushPressure>,
    /// Maximum hot age (`flush_max_hot_age`, bounded-stream-state F10): a
    /// stream's hot tail older than this is flushed whole, even below the
    /// group threshold. `None` disables it.
    pub max_hot_age: Option<std::time::Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdHotBacklog {
    pub stream_id: BucketStreamId,
    pub stream_hot_bytes: u64,
    pub group_hot_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ColdWriteAdmission {
    pub max_hot_bytes_per_group: Option<u64>,
}

impl ColdWriteAdmission {
    pub(crate) fn is_enabled(self) -> bool {
        self.max_hot_bytes_per_group.is_some()
    }
}

/// A tokened producer write's head, for an answer before its body is read
/// (#581 section 2.5, W7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerEarlyAnswerRequest {
    pub stream_id: BucketStreamId,
    pub producer: ProducerRequest,
    /// The write's `Stream-Incarnation`. Only a write that names the token
    /// of the applied state is answered early.
    pub incarnation: u64,
    pub now_ms: u64,
}

/// A write's head, routed before its body is read (#581 section 6.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteWriteRequest {
    pub stream_id: BucketStreamId,
    /// The key of a tokened producer write, whose answer the group's leader
    /// may decide from the head alone (section 2.5, W7).
    pub early_answer: Option<EarlyAnswerQuery>,
}

impl RouteWriteRequest {
    /// The early-answer lookup this request asks for, if any.
    pub fn early_answer_request(&self) -> Option<ProducerEarlyAnswerRequest> {
        self.early_answer
            .as_ref()
            .map(|query| ProducerEarlyAnswerRequest {
                stream_id: self.stream_id.clone(),
                producer: query.producer.clone(),
                incarnation: query.incarnation,
                now_ms: query.now_ms,
            })
    }
}

/// The key and token of a producer write that asks for an early answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EarlyAnswerQuery {
    pub producer: ProducerRequest,
    /// The write's `Stream-Incarnation`.
    pub incarnation: u64,
    pub now_ms: u64,
}

/// Where a write goes, as this replica of its group sees it before the
/// write's body is read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteRoute {
    /// This replica leads the group: the write is read and proposed here,
    /// unless its head alone decides its answer.
    Leader {
        early_answer: Option<ursula_stream::ProducerEarlyAnswer>,
        /// The leadership a cold upload of the write is fenced to (F14j).
        fence: UploadFence,
    },
    /// Another node leads the group in `term`.
    Follower { leader_id: u64, term: u64 },
    /// No leader of the group is known.
    NoLeader,
}

/// A [`WriteRoute`] and the group it is about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedWrite {
    pub raft_group_id: RaftGroupId,
    pub route: WriteRoute,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRequest {
    pub stream_id: BucketStreamId,
    pub content_type: String,
    pub payload: Bytes,
    pub close_after: bool,
    pub stream_seq: Option<String>,
    pub producer: Option<ProducerRequest>,
    pub now_ms: u64,
    /// `Stream-Incarnation` request precondition (D12): apply only to this
    /// incarnation of the stream. `None` for a request without the header.
    pub if_incarnation: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendExternalRequest {
    pub stream_id: BucketStreamId,
    pub content_type: String,
    pub payload: ExternalPayloadRef,
    /// JSON messages in the staged payload, counted over the normalized
    /// body while staging. Zero on any other content type.
    pub message_count: u64,
    pub close_after: bool,
    pub stream_seq: Option<String>,
    pub producer: Option<ProducerRequest>,
    pub now_ms: u64,
    /// See [`AppendRequest::if_incarnation`].
    pub if_incarnation: Option<u64>,
    /// The upload of `payload`, fenced to the leadership that admitted the
    /// append (F14j).
    pub upload: Arc<ProposedUpload>,
}

impl AppendExternalRequest {
    pub fn from_append_request(
        request: AppendRequest,
        payload: ExternalPayloadRef,
        message_count: u64,
        upload: Arc<ProposedUpload>,
    ) -> Self {
        Self {
            stream_id: request.stream_id,
            content_type: request.content_type,
            payload,
            message_count,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            now_ms: request.now_ms,
            if_incarnation: request.if_incarnation,
            upload,
        }
    }
}

impl AppendRequest {
    /// The canonical JSON messages of the payload, as
    /// [`CreateStreamRequest::canonical_message_count`].
    pub fn canonical_message_count(&self) -> u64 {
        ursula_stream::canonical_json_message_count(&self.content_type, &self.payload)
            .unwrap_or_default()
    }

    pub fn new(stream_id: BucketStreamId, payload_len: u64) -> Self {
        Self {
            stream_id,
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            payload: Bytes::from(vec![
                0;
                usize::try_from(payload_len)
                    .expect("payload_len fits usize")
            ]),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            if_incarnation: None,
        }
    }

    pub fn from_bytes(stream_id: BucketStreamId, payload: impl Into<Bytes>) -> Self {
        Self {
            stream_id,
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            payload: payload.into(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            if_incarnation: None,
        }
    }

    pub fn payload_len(&self) -> u64 {
        u64::try_from(self.payload.len()).expect("payload len fits u64")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendResponse {
    pub placement: ShardPlacement,
    pub start_offset: u64,
    pub next_offset: u64,
    pub group_commit_index: u64,
    pub closed: bool,
    /// The producer write's answer when the append carried producer headers.
    /// For a repeat, `start_offset` and `next_offset` are the original
    /// write's range, or the tail when its range was not kept.
    pub producer: Option<ProducerAck>,
    pub stream_hot_bytes: u64,
    pub group_hot_bytes: u64,
    /// The token of the stream incarnation the write applied to, rendered
    /// as `Stream-Incarnation` (D12). A token is never `0`; `0` means
    /// unknown and renders no header.
    pub incarnation: u64,
}

impl AppendResponse {
    /// Whether the write is a repeat of a producer write that applied
    /// before, so this request changed nothing.
    pub fn deduplicated(&self) -> bool {
        self.producer.as_ref().is_some_and(ProducerAck::is_repeat)
    }
}

#[cfg(test)]
mod immutable_read_tests {
    use super::ReadEnd;
    use super::immutable_read;

    #[test]
    fn where_a_read_ends_decides_whether_it_is_immutable() {
        assert_eq!(ReadEnd::of(false, false), ReadEnd::BelowTail);
        assert_eq!(ReadEnd::of(false, true), ReadEnd::BelowTail);
        assert_eq!(ReadEnd::of(true, false), ReadEnd::OpenTail);
        assert_eq!(ReadEnd::of(true, true), ReadEnd::ClosedTail);

        // Filled its request below the tail.
        assert!(immutable_read(0, 10, 10, ReadEnd::BelowTail));
        assert!(immutable_read(5, 10, 5, ReadEnd::BelowTail));
        // Short of its request below the tail: a lagging replica's answer.
        assert!(!immutable_read(0, 4, 10, ReadEnd::BelowTail));
        // Nothing asked for, nothing proven.
        assert!(!immutable_read(10, 10, 0, ReadEnd::BelowTail));
        // Filled its request and ended at an open tail: the stream can still
        // be closed there.
        assert!(!immutable_read(0, 10, 10, ReadEnd::OpenTail));
        assert!(!immutable_read(0, 4, 10, ReadEnd::OpenTail));
        // The tail of a closed stream, full, short or empty.
        assert!(immutable_read(0, 10, 10, ReadEnd::ClosedTail));
        assert!(immutable_read(0, 4, 10, ReadEnd::ClosedTail));
        assert!(immutable_read(10, 10, 10, ReadEnd::ClosedTail));
        // A range that does not start at its offset is never filled.
        assert!(!immutable_read(10, 4, 10, ReadEnd::BelowTail));
    }
}
