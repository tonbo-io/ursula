use std::sync::Arc;

use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;
use ursula_shard::ShardPlacement;
use ursula_stream::ColdChunkRef;
use ursula_stream::ColdFlushPressure;
use ursula_stream::ExternalPayloadRef;
use ursula_stream::ProducerRequest;
use ursula_stream::StreamReadPlan;
use ursula_stream::StreamReadSegment;

use crate::cold_index::ColdIndexPageCache;
use crate::cold_index::ColdStoreColdIndexPageStore;
use crate::cold_store::ColdStoreHandle;
use crate::cold_store::DEFAULT_CONTENT_TYPE;
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
    pub producer: Option<ProducerRequest>,
    pub stream_ttl_seconds: Option<u64>,
    pub stream_expires_at_ms: Option<u64>,
    pub now_ms: u64,
    /// `Stream-Incarnation` request precondition (D12): apply only to this
    /// incarnation of the stream. `None` for a request without the header.
    pub if_incarnation: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateStreamExternalRequest {
    pub stream_id: BucketStreamId,
    pub content_type: String,
    pub initial_payload: ExternalPayloadRef,
    pub record_ends: Vec<u64>,
    pub close_after: bool,
    pub stream_seq: Option<String>,
    pub producer: Option<ProducerRequest>,
    pub stream_ttl_seconds: Option<u64>,
    pub stream_expires_at_ms: Option<u64>,
    pub now_ms: u64,
    /// See [`CreateStreamRequest::if_incarnation`].
    #[serde(default)]
    pub if_incarnation: Option<u64>,
}

impl CreateStreamExternalRequest {
    pub fn from_create_request(
        request: CreateStreamRequest,
        initial_payload: ExternalPayloadRef,
        record_ends: Vec<u64>,
    ) -> Self {
        Self {
            stream_id: request.stream_id,
            content_type: request.content_type,
            initial_payload,
            record_ends,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            stream_ttl_seconds: request.stream_ttl_seconds,
            stream_expires_at_ms: request.stream_expires_at_ms,
            now_ms: request.now_ms,
            if_incarnation: request.if_incarnation,
        }
    }
}

impl CreateStreamRequest {
    pub fn canonical_record_ends(&self) -> Vec<u64> {
        ursula_stream::canonical_json_record_ends(&self.content_type, &self.initial_payload)
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
            producer: None,
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
    /// The stream incarnation (`created_at_ms`) the write applied to,
    /// rendered as `Stream-Incarnation` (D12). Every epoch-2 leader sets
    /// it, and a stream's `created_at_ms` is never `0`; `0` means unknown
    /// and renders no header.
    #[serde(default)]
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
    /// The stream incarnation's `created_at_ms`, unique per group (C7). HEAD renders it as the public
    /// `Stream-Incarnation` header, an opaque token that changes when the
    /// stream is deleted and recreated.
    pub created_at_ms: u64,
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
    /// The incarnation (`created_at_ms`) of the stream that served the
    /// read (D12). Every epoch-2 leader sets it, and a stream's
    /// `created_at_ms` is never `0`; `0` means unknown and renders no
    /// header.
    #[serde(default)]
    pub incarnation: u64,
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

/// A local read plan or an owned remote read, materialized outside the group actor.
pub enum GroupReadStreamParts {
    Prepared(PreparedGroupReadStreamParts),
    Deferred(crate::engine::GroupReadStreamFuture<'static>),
}

impl GroupReadStreamParts {
    pub fn deferred(
        future: impl std::future::Future<Output = Result<ReadStreamResponse, GroupEngineError>>
        + Send
        + 'static,
    ) -> Self {
        Self::Deferred(Box::pin(future))
    }
    pub fn from_response(response: ReadStreamResponse) -> Self {
        Self::Prepared(PreparedGroupReadStreamParts::from_response(response))
    }
    pub fn from_plan(
        placement: ShardPlacement,
        stream_id: BucketStreamId,
        plan: StreamReadPlan,
        cold_store: Option<ColdStoreHandle>,
        cold_index_cache: Option<Arc<ColdIndexPageCache<ColdStoreColdIndexPageStore>>>,
    ) -> Self {
        Self::Prepared(PreparedGroupReadStreamParts::from_plan(
            placement,
            stream_id,
            plan,
            cold_store,
            cold_index_cache,
        ))
    }
    pub async fn into_response(self) -> Result<ReadStreamResponse, GroupEngineError> {
        match self {
            Self::Prepared(parts) => parts.into_response().await,
            Self::Deferred(future) => future.await,
        }
    }
    pub fn payload_is_empty(&self) -> bool {
        matches!(self, Self::Prepared(parts) if parts.payload_is_empty())
    }
    pub fn is_open_tail(&self) -> bool {
        matches!(self, Self::Prepared(parts) if parts.up_to_date && !parts.closed)
    }
    pub fn tail_incarnation(&self) -> Option<u64> {
        match self {
            Self::Prepared(parts)
                if parts.payload_is_empty() && parts.up_to_date && !parts.closed =>
            {
                Some(parts.incarnation)
            }
            _ => None,
        }
    }
    pub fn mark_not_up_to_date(&mut self) {
        if let Self::Prepared(parts) = self {
            parts.up_to_date = false;
        }
    }
}

pub struct PreparedGroupReadStreamParts {
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

impl PreparedGroupReadStreamParts {
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
                // Publish readiness only after the broadcast release waiter
                // is registered; notify_waiters does not retain a permit.
                let released = release.notified();
                tokio::pin!(released);
                released.as_mut().enable();
                entered.notify_one();
                materialized.notify_one();
                released.await;
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishSnapshotResponse {
    pub placement: ShardPlacement,
    pub snapshot_offset: u64,
    pub snapshot_digest: String,
    pub group_commit_index: u64,
    /// The stream incarnation (`created_at_ms`) the write applied to,
    /// rendered as `Stream-Incarnation` (D12). Every epoch-2 leader sets
    /// it, and a stream's `created_at_ms` is never `0`; `0` means unknown
    /// and renders no header.
    #[serde(default)]
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
    /// The stream incarnation (`created_at_ms`) the write applied to,
    /// rendered as `Stream-Incarnation` (D12). Every epoch-2 leader sets
    /// it, and a stream's `created_at_ms` is never `0`; `0` means unknown
    /// and renders no header.
    #[serde(default)]
    pub incarnation: u64,
    /// Hot backlog after the write applied (bounded-stream-state F6a), so
    /// the runtime records its metric without a second state-machine round
    /// trip.
    pub hot_backlog: Option<WriteHotBacklog>,
}

/// One leader-side `TidyStream` pass over a group (bounded-state F0): the
/// engine proposes `TidyStream` for at most `max_streams` streams with
/// normalization debt at `now_ms`. Followers propose nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TidyStreamsRequest {
    pub max_streams: usize,
    pub now_ms: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TidyStreamsResponse {
    /// `TidyStream` commands committed by this pass.
    pub tidied: u64,
    /// Tidied streams that still report debt after their command.
    pub debt_remaining: u64,
}

/// Result of one replicated `TidyStream` command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TidyStreamResponse {
    pub placement: ShardPlacement,
    pub debt_remaining: bool,
    pub group_commit_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportGroupStateRequest {
    pub snapshot: Box<ursula_stream::StreamSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ImportGroupStateResponse {
    pub placement: ShardPlacement,
    pub buckets: u64,
    pub streams: u64,
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
    pub deduplicated: bool,
    /// The stream incarnation (`created_at_ms`) the write applied to,
    /// rendered as `Stream-Incarnation` (D12). Every epoch-2 leader sets
    /// it, and a stream's `created_at_ms` is never `0`; `0` means unknown
    /// and renders no header.
    #[serde(default)]
    pub incarnation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteStreamRequest {
    pub stream_id: BucketStreamId,
    /// `Stream-Incarnation` request precondition (D12): apply only to this
    /// incarnation of the stream. `None` for a request without the header.
    pub if_incarnation: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteStreamResponse {
    pub placement: ShardPlacement,
    pub group_commit_index: u64,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendExternalRequest {
    pub stream_id: BucketStreamId,
    pub content_type: String,
    pub payload: ExternalPayloadRef,
    pub record_ends: Vec<u64>,
    pub close_after: bool,
    pub stream_seq: Option<String>,
    pub producer: Option<ProducerRequest>,
    pub now_ms: u64,
    /// See [`AppendRequest::if_incarnation`].
    #[serde(default)]
    pub if_incarnation: Option<u64>,
}

impl AppendExternalRequest {
    pub fn from_append_request(
        request: AppendRequest,
        payload: ExternalPayloadRef,
        record_ends: Vec<u64>,
    ) -> Self {
        Self {
            stream_id: request.stream_id,
            content_type: request.content_type,
            payload,
            record_ends,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            now_ms: request.now_ms,
            if_incarnation: request.if_incarnation,
        }
    }
}

impl AppendRequest {
    pub fn canonical_record_ends(&self) -> Vec<u64> {
        ursula_stream::canonical_json_record_ends(&self.content_type, &self.payload)
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
    pub stream_append_count: u64,
    pub group_commit_index: u64,
    pub closed: bool,
    pub deduplicated: bool,
    pub producer: Option<ProducerRequest>,
    pub stream_hot_bytes: u64,
    pub group_hot_bytes: u64,
    /// A duplicate beyond the stream's receipt window (bounded-state F3):
    /// deduplicated without byte ranges. `start_offset` and
    /// `next_offset` then carry no information about the original append.
    pub receipt_evicted: bool,
    /// The stream incarnation (`created_at_ms`) the write applied to,
    /// rendered as `Stream-Incarnation` (D12). Every epoch-2 leader sets
    /// it, and a stream's `created_at_ms` is never `0`; `0` means unknown
    /// and renders no header.
    #[serde(default)]
    pub incarnation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamAppendCount {
    pub stream_id: BucketStreamId,
    pub append_count: u64,
}
