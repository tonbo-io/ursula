//! Deterministic stream state machine driving a single Raft group.
//!
//! The state machine lives in this module root; its behavior is split across
//! cohesive submodules to keep each surface readable:
//!
//! - [`query`]: read paths — heads, accessors, read plans, snapshots, bootstrap.
//! - [`append`]: append paths and idempotent producer bookkeeping.
//! - [`lifecycle`]: bucket/stream create, close, delete, fork refs, attrs, TTL expiry.
//! - [`cold`]: cold-tier flush planning, GC, retention compaction, snapshot publishing.
//! - [`persist`]: snapshot / restore / integrity serialization.
//! - [`hot_buffer`], [`cold_state`], [`ttl`]: internal per-stream data structures.
//!
//! The root keeps the [`StreamStateMachine`] type, its core slot/TTL accessors,
//! the [`StreamStateMachine::apply`] command dispatcher, and cross-cutting helpers.

use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use slotmap::Key;
use slotmap::new_key_type;
use ursula_shard::BucketStreamId;

use self::cold_gc::ColdGcQueue;
use self::cold_state::StreamColdState;
use self::hot_buffer::HotBuffer;
use self::registry::StreamRegistry;
use self::ttl::TtlEntry;
use self::ttl::TtlIndex;
use crate::command::StreamCommand;
use crate::integrity::StreamIntegrity;
use crate::model::AppendExternalInput;
use crate::model::AppendStreamInput;
use crate::model::COLD_INDEX_PAGE_SPAN_BYTES;
use crate::model::ColdChunkRef;
use crate::model::ColdFlushCandidate;
use crate::model::ColdGcEntry;
use crate::model::ColdGcTarget;
use crate::model::ExternalPayloadRef;
use crate::model::HotPayloadSegment;
use crate::model::MAX_STREAM_ATTRS_BYTES;
use crate::model::ObjectPayloadRef;
use crate::model::ProducerAppendRecord;
use crate::model::ProducerRequest;
use crate::model::ProducerSnapshot;
use crate::model::ProducerState;
use crate::model::StreamAttrs;
use crate::model::StreamBatchAppend;
use crate::model::StreamBatchAppendItem;
use crate::model::StreamBootstrapPlan;
use crate::model::StreamMessageRecord;
use crate::model::StreamMetadata;
use crate::model::StreamRead;
use crate::model::StreamReadColdIndexSegment;
use crate::model::StreamReadObjectSegment;
use crate::model::StreamReadPlan;
use crate::model::StreamReadSegment;
use crate::model::StreamStatus;
use crate::model::StreamVisibleSnapshot;
use crate::response::StreamErrorCode;
use crate::response::StreamErrorContext;
use crate::response::StreamResponse;
use crate::snapshot::StreamSnapshot;
use crate::snapshot::StreamSnapshotEntry;
use crate::snapshot::StreamSnapshotError;
use crate::validate::validate_bucket_id;
use crate::validate::validate_stream_id;

mod append;
mod cold;
mod cold_gc;
mod cold_state;
mod hot_buffer;
mod lifecycle;
mod persist;
mod query;
mod registry;
mod ttl;

const TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE: usize = 256;

new_key_type! {
    struct StreamKey;
}

#[derive(Debug, Clone, Default)]
pub struct StreamStateMachine {
    buckets: HashSet<String>,
    registry: StreamRegistry,
    cold_gc: ColdGcQueue,
}

#[derive(Debug, Clone)]
struct StreamSlot {
    metadata: StreamMetadata,
    attrs: Option<StreamAttrs>,
    hot_buffer: HotBuffer,
    cold: StreamColdState,
    message_records: Vec<StreamMessageRecord>,
    integrity: StreamIntegrity,
    visible_snapshot: Option<StreamVisibleSnapshot>,
    producers: HashMap<String, ProducerState>,
}

impl StreamStateMachine {
    pub fn new() -> Self {
        Self::default()
    }

    fn stream_slot(&self, stream_id: &BucketStreamId) -> Option<&StreamSlot> {
        self.registry.slot(stream_id)
    }

    fn stream_slot_mut(&mut self, stream_id: &BucketStreamId) -> Option<&mut StreamSlot> {
        self.registry.slot_mut(stream_id)
    }

    fn stream_metadata(&self, stream_id: &BucketStreamId) -> Option<&StreamMetadata> {
        self.registry.metadata(stream_id)
    }

    fn stream_metadata_mut(&mut self, stream_id: &BucketStreamId) -> Option<&mut StreamMetadata> {
        self.registry.metadata_mut(stream_id)
    }

    fn insert_stream_slot(&mut self, slot: StreamSlot) -> Option<StreamKey> {
        self.registry.insert(slot)
    }

    fn refresh_ttl_entry(&mut self, stream_id: &BucketStreamId) {
        self.registry.refresh_ttl(stream_id);
    }

    pub fn apply(&mut self, command: StreamCommand) -> StreamResponse {
        match command {
            StreamCommand::CreateBucket { bucket_id } => self.create_bucket(bucket_id),
            StreamCommand::DeleteBucket { bucket_id } => self.delete_bucket(&bucket_id),
            StreamCommand::CreateStream {
                stream_id,
                content_type,
                initial_payload,
                close_after,
                stream_seq,
                producer,
                stream_ttl_seconds,
                stream_expires_at_ms,
                forked_from,
                fork_offset,
                attrs,
                now_ms,
            } => {
                let response = self.create_stream(CreateStreamInput {
                    stream_id,
                    content_type,
                    initial_payload,
                    close_after,
                    stream_seq,
                    producer,
                    stream_ttl_seconds,
                    stream_expires_at_ms,
                    forked_from,
                    fork_offset,
                    attrs,
                    now_ms,
                });
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::CreateExternal {
                stream_id,
                content_type,
                initial_payload,
                close_after,
                stream_seq,
                producer,
                stream_ttl_seconds,
                stream_expires_at_ms,
                forked_from,
                fork_offset,
                attrs,
                now_ms,
            } => {
                let response = self.create_external_stream(CreateExternalStreamInput {
                    stream_id,
                    content_type,
                    initial_payload,
                    close_after,
                    stream_seq,
                    producer,
                    stream_ttl_seconds,
                    stream_expires_at_ms,
                    forked_from,
                    fork_offset,
                    attrs,
                    now_ms,
                });
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::Append {
                stream_id,
                content_type,
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => {
                let response = self.append_borrowed(AppendStreamInput {
                    stream_id,
                    content_type: content_type.as_deref(),
                    payload: &payload,
                    close_after,
                    stream_seq,
                    producer,
                    now_ms,
                });
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::AppendExternal {
                stream_id,
                content_type,
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => {
                let response = self.append_external(AppendExternalInput {
                    stream_id,
                    content_type: content_type.as_deref(),
                    payload,
                    close_after,
                    stream_seq,
                    producer,
                    now_ms,
                });
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::AppendBatch {
                stream_id,
                content_type,
                payloads,
                producer,
                now_ms,
            } => {
                let response = match self.append_batch_borrowed(
                    stream_id,
                    content_type.as_deref(),
                    &payloads.iter().map(Vec::as_slice).collect::<Vec<_>>(),
                    producer,
                    now_ms,
                ) {
                    Ok(batch) => batch
                        .items
                        .last()
                        .map(|item| StreamResponse::Appended {
                            offset: item.offset,
                            next_offset: item.next_offset,
                            closed: item.closed,
                            deduplicated: item.deduplicated,
                            producer: None,
                        })
                        .unwrap_or_else(|| {
                            StreamResponse::error(
                                StreamErrorCode::EmptyAppend,
                                "append batch must contain at least one payload",
                            )
                        }),
                    Err(response) => response,
                };
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::PublishSnapshot {
                stream_id,
                snapshot_offset,
                content_type,
                payload,
                now_ms,
            } => {
                let response = self.publish_snapshot(
                    stream_id,
                    snapshot_offset,
                    content_type,
                    payload,
                    now_ms,
                );
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::TouchStreamAccess {
                stream_id,
                now_ms,
                renew_ttl,
            } => {
                let response = self.touch_stream_access(&stream_id, now_ms, renew_ttl);
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::UpdateStreamAttrs {
                stream_id,
                attrs,
                now_ms,
            } => {
                let response = self.update_stream_attrs(&stream_id, attrs, now_ms);
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::AddForkRef { stream_id, now_ms } => {
                let response = self.add_fork_ref(&stream_id, now_ms);
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::ReleaseForkRef { stream_id } => self.release_fork_ref(&stream_id),
            StreamCommand::FlushCold { stream_id, chunk } => self.flush_cold(stream_id, chunk),
            StreamCommand::Close {
                stream_id,
                stream_seq,
                producer,
                now_ms,
            } => {
                let response = self.close(stream_id, stream_seq, producer, now_ms);
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::DeleteStream { stream_id } => self.delete_stream(&stream_id),
            StreamCommand::AckColdGc { up_to_seq } => self.ack_cold_gc(up_to_seq),
        }
    }
}

#[derive(Debug)]
struct CreateStreamInput {
    stream_id: BucketStreamId,
    content_type: String,
    initial_payload: Vec<u8>,
    close_after: bool,
    stream_seq: Option<String>,
    producer: Option<ProducerRequest>,
    stream_ttl_seconds: Option<u64>,
    stream_expires_at_ms: Option<u64>,
    forked_from: Option<BucketStreamId>,
    fork_offset: Option<u64>,
    attrs: Option<StreamAttrs>,
    now_ms: u64,
}

#[derive(Debug)]
struct CreateExternalStreamInput {
    stream_id: BucketStreamId,
    content_type: String,
    initial_payload: ExternalPayloadRef,
    close_after: bool,
    stream_seq: Option<String>,
    producer: Option<ProducerRequest>,
    stream_ttl_seconds: Option<u64>,
    stream_expires_at_ms: Option<u64>,
    forked_from: Option<BucketStreamId>,
    fork_offset: Option<u64>,
    attrs: Option<StreamAttrs>,
    now_ms: u64,
}

impl CreateStreamInput {
    fn initial_len(&self) -> u64 {
        u64::try_from(self.initial_payload.len()).expect("payload len fits u64")
    }
}

fn is_soft_deleted(stream: &StreamMetadata) -> bool {
    stream.status == StreamStatus::SoftDeleted
}

fn normalize_stream_attrs(attrs: Option<StreamAttrs>) -> Option<StreamAttrs> {
    attrs.filter(|attrs| !attrs.is_empty())
}

fn stream_expiry_at_ms(stream: &StreamMetadata) -> Option<u64> {
    if let Some(expires_at_ms) = stream.stream_expires_at_ms {
        return Some(expires_at_ms);
    }
    stream.stream_ttl_seconds.map(|ttl_seconds| {
        stream
            .last_ttl_touch_at_ms
            .saturating_add(ttl_seconds.saturating_mul(1000))
    })
}

fn stream_is_expired(stream: &StreamMetadata, now_ms: u64) -> bool {
    stream_expiry_at_ms(stream).is_some_and(|expires_at_ms| now_ms >= expires_at_ms)
}

fn renew_stream_ttl(stream: &mut StreamMetadata, now_ms: u64) {
    if stream.stream_ttl_seconds.is_some() && stream.stream_expires_at_ms.is_none() {
        stream.last_ttl_touch_at_ms = now_ms;
    }
}

fn validate_producer_request(producer: Option<&ProducerRequest>) -> Result<(), StreamResponse> {
    let Some(producer) = producer else {
        return Ok(());
    };
    if producer.producer_id.trim().is_empty() {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidProducer,
            "producer id must not be empty",
        ));
    }
    const MAX_JS_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
    if producer.producer_epoch > MAX_JS_SAFE_INTEGER {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidProducer,
            format!(
                "producer epoch {} exceeds maximum {}",
                producer.producer_epoch, MAX_JS_SAFE_INTEGER
            ),
        ));
    }
    if producer.producer_seq > MAX_JS_SAFE_INTEGER {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidProducer,
            format!(
                "producer sequence {} exceeds maximum {}",
                producer.producer_seq, MAX_JS_SAFE_INTEGER
            ),
        ));
    }
    Ok(())
}

fn validate_external_payload_ref(payload: &ExternalPayloadRef) -> Result<(), StreamResponse> {
    if payload.s3_path.trim().is_empty() {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidColdFlush,
            "external payload S3 path must not be empty",
        ));
    }
    if payload.payload_len == 0 {
        return Err(StreamResponse::error(
            StreamErrorCode::EmptyAppend,
            "external payload length must be greater than zero",
        ));
    }
    if payload.object_size < payload.payload_len {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidColdFlush,
            "external payload object size must cover payload length",
        ));
    }
    Ok(())
}

fn compare_stream_ids(left: &BucketStreamId, right: &BucketStreamId) -> std::cmp::Ordering {
    left.bucket_id
        .cmp(&right.bucket_id)
        .then_with(|| left.stream_id.cmp(&right.stream_id))
}

#[cfg(test)]
mod tests;
