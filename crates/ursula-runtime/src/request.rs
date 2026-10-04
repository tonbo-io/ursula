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
use ursula_stream::StreamErrorCode;
use ursula_stream::StreamIntegritySnapshot;
use ursula_stream::StreamReadPlan;
use ursula_stream::StreamReadSegment;
use ursula_stream::StreamRecordRange;

use crate::cold_index::ColdIndexPageCache;
use crate::cold_index::ColdStoreColdIndexPageStore;
use crate::cold_store::ColdStoreHandle;
use crate::cold_store::DEFAULT_CONTENT_TYPE;
use crate::engine::GroupEngineError;
use crate::engine::in_memory::InMemoryGroupEngine;
use crate::error::RuntimeError;

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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateStreamExternalRequest {
    pub stream_id: BucketStreamId,
    pub content_type: String,
    pub initial_payload: ExternalPayloadRef,
    #[serde(default)]
    pub record_ends: Vec<u64>,
    pub close_after: bool,
    pub stream_seq: Option<String>,
    pub producer: Option<ProducerRequest>,
    pub stream_ttl_seconds: Option<u64>,
    pub stream_expires_at_ms: Option<u64>,
    pub now_ms: u64,
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
    pub record_range: Option<StreamRecordRange>,
    /// Hot backlog after the write applied (bounded-stream-state F6a), so
    /// the runtime records its metric without a second state-machine round
    /// trip. `None` from an older leader.
    #[serde(default)]
    pub hot_backlog: Option<WriteHotBacklog>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadStreamRequest {
    pub stream_id: BucketStreamId,
    pub now_ms: u64,
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
    pub integrity: StreamIntegritySnapshot,
    pub record_range: Option<StreamRecordRange>,
    /// The stream incarnation's `created_at_ms`, unique per group from
    /// feature level 1 (C7). HEAD renders it as the public
    /// `Stream-Incarnation` header, an opaque token that changes when the
    /// stream is deleted and recreated. `default` keeps HEAD responses
    /// forwarded by older followers decodable.
    #[serde(default)]
    pub created_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadStreamRequest {
    pub stream_id: BucketStreamId,
    pub offset: u64,
    pub max_len: usize,
    pub now_ms: u64,
    pub record: Option<u64>,
    pub max_records: Option<u64>,
    /// Require the owning Raft leader's applied state instead of permitting a
    /// local follower read. Recovery paths use this after an acknowledged
    /// write; ordinary catch-up consumers keep the cheaper follower-local
    /// behavior.
    pub leader_only: bool,
    /// Server-side continuation anchor (F1): the exact start of `record`
    /// taken from this reader's own previous response. Used only when its
    /// incarnation matches and it validates; otherwise the read resolves
    /// from the record index. Never parsed from client input.
    pub record_anchor: Option<RecordAnchor>,
}

/// Where record `record` of stream incarnation `incarnation` (its
/// `created_at_ms`) starts, as a previous read returned it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordAnchor {
    pub incarnation: u64,
    pub record: u64,
    pub offset: u64,
}

impl ReadStreamRequest {
    pub(crate) fn same_wait_plan(&self, other: &Self) -> bool {
        self.stream_id == other.stream_id
            && self.offset == other.offset
            && self.max_len == other.max_len
            && self.record == other.record
            && self.max_records == other.max_records
            && self.leader_only == other.leader_only
            && self.record_anchor == other.record_anchor
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
    pub retained_record_range: Option<StreamRecordRange>,
    pub record_range: Option<StreamRecordRange>,
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
    pub offset: u64,
    pub next_offset: u64,
    pub content_type: String,
    pub up_to_date: bool,
    pub closed: bool,
    pub retained_record_range: Option<StreamRecordRange>,
    pub record_range: Option<StreamRecordRange>,
    pub body: GroupReadStreamBody,
}

impl GroupReadStreamParts {
    pub fn from_response(response: ReadStreamResponse) -> Self {
        Self {
            placement: response.placement,
            offset: response.offset,
            next_offset: response.next_offset,
            content_type: response.content_type,
            up_to_date: response.up_to_date,
            closed: response.closed,
            retained_record_range: response.retained_record_range,
            record_range: response.record_range,
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
            offset: plan.offset,
            next_offset: plan.next_offset,
            content_type: plan.content_type.clone(),
            up_to_date: plan.up_to_date,
            closed: plan.closed,
            retained_record_range: plan.retained_record_range,
            record_range: plan.record_range,
            body: GroupReadStreamBody::Planned {
                stream_id,
                plan,
                cold_store,
                cold_index_cache,
            },
        }
    }

    /// Followers never let a trimmed record read claim `up_to_date`; they
    /// forward or hold reads that reach the tail (F1, design §5.2).
    pub fn forbid_trimmed_up_to_date(&mut self) {
        if let GroupReadStreamBody::Planned { plan, .. } = &mut self.body
            && let Some(trim) = plan.record_trim.as_mut()
        {
            trim.claim_up_to_date = false;
        }
    }

    pub async fn into_response(mut self) -> Result<ReadStreamResponse, GroupEngineError> {
        let record_trim = match &self.body {
            GroupReadStreamBody::Planned { plan, .. } => plan.record_trim.as_deref().cloned(),
            _ => None,
        };
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
        let payload = match record_trim {
            Some(trim) => {
                // F1: cut the bracketed window to the requested records.
                let trimmed = ursula_stream::trim_record_window(&payload, self.offset, &trim)
                    .map_err(|err| {
                        tracing::error!(
                            offset = self.offset,
                            error = %err,
                            "record read found bytes that disagree with the record index"
                        );
                        crate::metrics::record_coordinate_corruption();
                        GroupEngineError::stream(StreamErrorCode::InvalidColdFlush, err.to_string())
                    })?;
                self.offset = trimmed.offset;
                self.next_offset = trimmed.next_offset;
                self.record_range = Some(trimmed.record_range);
                self.up_to_date = trimmed.up_to_date;
                let mut payload = payload;
                payload.truncate(trimmed.end);
                payload.drain(..trimmed.start);
                payload
            }
            None => payload,
        };
        Ok(ReadStreamResponse {
            placement: self.placement,
            offset: self.offset,
            next_offset: self.next_offset,
            content_type: self.content_type,
            payload,
            up_to_date: self.up_to_date,
            closed: self.closed,
            retained_record_range: self.retained_record_range,
            record_range: self.record_range,
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
    /// A body staged as a cold-tier object (feature level 5, bounded-state
    /// F16). Proposed as `PublishSnapshotExternal`.
    pub cold_body: Option<ColdSnapshotBody>,
    pub now_ms: u64,
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
    pub record_range: Option<StreamRecordRange>,
    /// Hot backlog after the write applied (bounded-stream-state F6a), so
    /// the runtime records its metric without a second state-machine round
    /// trip. `None` from an older leader.
    #[serde(default)]
    pub hot_backlog: Option<WriteHotBacklog>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvanceRetentionRequest {
    pub stream_id: BucketStreamId,
    pub retained_offset: u64,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvanceRetentionResponse {
    pub placement: ShardPlacement,
    pub retained_offset: u64,
    pub group_commit_index: u64,
    pub record_range: Option<StreamRecordRange>,
    /// Hot backlog after the write applied (bounded-stream-state F6a), so
    /// the runtime records its metric without a second state-machine round
    /// trip. `None` from an older leader.
    #[serde(default)]
    pub hot_backlog: Option<WriteHotBacklog>,
}

/// Raises one group's replicated feature level (C0) to
/// `max(current, level)`. The caller replicates the same request to every
/// group, and must only send levels every replica supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetFeatureLevelRequest {
    pub level: u32,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetFeatureLevelResponse {
    pub placement: ShardPlacement,
    /// The group's level after apply.
    pub level: u32,
    /// The group's level before apply.
    pub previous_level: u32,
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
    /// Cold-tier object holding the body (feature level 5). Callers stream
    /// it from the cold store.
    pub object: Option<ExternalPayloadRef>,
    pub up_to_date: bool,
    pub record_range: Option<StreamRecordRange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapStreamRequest {
    pub stream_id: BucketStreamId,
    pub now_ms: u64,
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
    /// Cold-tier object holding the snapshot body (feature level 5).
    pub snapshot_object: Option<ExternalPayloadRef>,
    pub updates: Vec<BootstrapUpdate>,
    pub next_offset: u64,
    pub up_to_date: bool,
    pub closed: bool,
    pub record_range: Option<StreamRecordRange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseStreamRequest {
    pub stream_id: BucketStreamId,
    pub stream_seq: Option<String>,
    pub producer: Option<ProducerRequest>,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloseStreamResponse {
    pub placement: ShardPlacement,
    pub next_offset: u64,
    pub group_commit_index: u64,
    pub deduplicated: bool,
    pub record_range: Option<StreamRecordRange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteStreamRequest {
    pub stream_id: BucketStreamId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteStreamResponse {
    pub placement: ShardPlacement,
    pub group_commit_index: u64,
    /// Hot backlog after the write applied (bounded-stream-state F6a), so
    /// the runtime records its metric without a second state-machine round
    /// trip. `None` from an older leader.
    #[serde(default)]
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
    /// Rolling-upgrade compatibility for <=0.4.5 voters, which did not return
    /// a bucket-specific cold-GC count. Missing is mapped to maximally pending,
    /// never to zero, so a mixed-version cluster cannot forge absence proof.
    /// Remove after the minimum supported rolling source is >=0.4.6.
    #[serde(default = "unknown_pending_cold_gc_entries")]
    pub pending_cold_gc_entries: u64,
    pub group_commit_index: u64,
}

const fn unknown_pending_cold_gc_entries() -> u64 {
    u64::MAX
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushColdRequest {
    pub stream_id: BucketStreamId,
    pub chunk: ColdChunkRef,
    /// Cold generation the chunk was planned from
    /// ([`ursula_stream::ColdFlushCandidate::cold_generation`]); see
    /// `StreamCommand::FlushCold`. `None` skips the incarnation check.
    pub cold_generation: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlushColdResponse {
    pub placement: ShardPlacement,
    pub hot_start_offset: u64,
    pub group_commit_index: u64,
    /// Hot backlog after the write applied (bounded-stream-state F6a), so
    /// the runtime records its metric without a second state-machine round
    /// trip. `None` from an older leader.
    #[serde(default)]
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
    pub record_match: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendExternalRequest {
    pub stream_id: BucketStreamId,
    pub content_type: String,
    pub payload: ExternalPayloadRef,
    #[serde(default)]
    pub record_ends: Vec<u64>,
    pub close_after: bool,
    pub stream_seq: Option<String>,
    pub producer: Option<ProducerRequest>,
    pub now_ms: u64,
    pub record_match: Option<u64>,
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
            record_match: request.record_match,
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
            record_match: None,
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
            record_match: None,
        }
    }

    pub fn payload_len(&self) -> u64 {
        u64::try_from(self.payload.len()).expect("payload len fits u64")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendBatchRequest {
    pub stream_id: BucketStreamId,
    pub content_type: String,
    pub payloads: Vec<Bytes>,
    pub producer: Option<ProducerRequest>,
    pub now_ms: u64,
}

impl AppendBatchRequest {
    pub fn new<P>(stream_id: BucketStreamId, payloads: Vec<P>) -> Self
    where P: Into<Bytes> {
        Self {
            stream_id,
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            payloads: payloads.into_iter().map(Into::into).collect(),
            producer: None,
            now_ms: 0,
        }
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
    pub record_range: Option<StreamRecordRange>,
    #[serde(default)]
    pub stream_hot_bytes: u64,
    #[serde(default)]
    pub group_hot_bytes: u64,
    /// A duplicate beyond the stream's receipt window (bounded-state F3):
    /// deduplicated without byte or record ranges. `start_offset` and
    /// `next_offset` then carry no information about the original append.
    #[serde(default)]
    pub receipt_evicted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendBatchResponse {
    pub placement: ShardPlacement,
    pub items: Vec<Result<AppendResponse, RuntimeError>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendTransactionRequest {
    pub operations: Vec<AppendRequest>,
}

impl AppendTransactionRequest {
    pub fn payload_bytes(&self) -> u64 {
        self.operations.iter().fold(0_u64, |total, operation| {
            total.saturating_add(operation.payload_len())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendTransactionResponse {
    pub placement: ShardPlacement,
    pub items: Vec<AppendResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamAppendCount {
    pub stream_id: BucketStreamId,
    pub append_count: u64,
}

#[cfg(test)]
mod compatibility_tests {
    use super::PurgeBucketResponse;

    #[test]
    fn legacy_purge_response_is_never_interpreted_as_absence_proof() {
        let response: PurgeBucketResponse = serde_json::from_value(serde_json::json!({
            "placement": {"core_id": 0, "shard_id": 0, "raft_group_id": 7},
            "removed_streams": 0,
            "group_commit_index": 8
        }))
        .expect("decode <=0.4.5 purge response");

        assert_eq!(response.pending_cold_gc_entries, u64::MAX);
    }
}
