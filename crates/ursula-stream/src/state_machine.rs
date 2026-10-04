//! Deterministic stream state machine driving a single Raft group.
//!
//! The state machine lives in this module root; its behavior is split across
//! cohesive submodules to keep each surface readable:
//!
//! - [`query`]: read paths — heads, accessors, read plans, snapshots, bootstrap.
//! - [`append`]: append paths and idempotent producer bookkeeping.
//! - [`lifecycle`]: bucket/stream create, close, delete, and TTL expiry.
//! - [`cold`]: cold-tier flush candidates, GC, retention compaction, snapshot publishing.
//! - [`flush_planner`]: leader-side flush passes over a derived hot-stream index.
//! - [`persist`]: snapshot / restore serialization.
//! - [`producers`]: F3 receipt window, idle-producer expiry and `TidyStream`.
//! - [`external_locators`]: F5 state-held external payload locators and
//!   `OffloadColdRefs`, plus the offload pass's query.
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
pub use self::gauges::GroupStateGauges;
use self::hot_buffer::HotBuffer;
use self::registry::StreamRegistry;
use self::ttl::TtlEntry;
use self::ttl::TtlIndex;
use crate::command::StreamCommand;
use crate::json_records::canonical_json_record_ends;
use crate::json_records::is_json_record_content_type;
use crate::json_records::record_ends_valid;
use crate::model::AppendExternalInput;
use crate::model::AppendStreamInput;
use crate::model::BOOTSTRAP_MAX_UPDATE_BYTES;
use crate::model::BucketUsage;
use crate::model::BucketUsageSnapshot;
use crate::model::COLD_INDEX_PAGE_SPAN_BYTES;
use crate::model::ColdChunkRef;
use crate::model::ColdFlushCandidate;
use crate::model::ColdGcEntry;
use crate::model::ColdGcPlanEntry;
use crate::model::ColdGcTarget;
use crate::model::ExternalPayloadRef;
use crate::model::HotPayloadSegment;
use crate::model::ObjectPayloadRef;
use crate::model::ProducerReceipt;
use crate::model::ProducerRequest;
use crate::model::ProducerSnapshot;
use crate::model::ProducerState;
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
mod cold_refs;
mod cold_state;
mod external_locators;
mod flush_planner;
mod gauges;
mod hot_buffer;

pub use self::cold::RETENTION_COLD_GC_GRACE_MS;
pub use self::cold_refs::SHARED_REF_COMPACTION_THRESHOLD;
pub use self::cold_refs::SHARED_REF_IDLE_MS;
pub use self::cold_refs::SharedRefCandidate;
pub use self::cold_refs::SharedRefCompactionRequest;
pub use self::cold_refs::SharedRefIdleTracker;
pub use self::cold_refs::is_legacy_cross_bucket_pack;
pub use self::cold_refs::plan_shared_ref_run;
pub use self::external_locators::MAX_STAGED_EXTERNAL_REFS;
pub use self::external_locators::STAGED_EXTERNAL_REF_MAX_AGE_MS;
pub use self::external_locators::StagedExternalRefCandidate;
pub use self::flush_planner::ColdFlushHotAge;
pub use self::flush_planner::ColdFlushPass;
pub use self::flush_planner::ColdFlushPassRequest;
pub use self::flush_planner::ColdFlushPlanStats;
pub use self::flush_planner::ColdFlushPressure;
mod lifecycle;
mod persist;
mod producers;
mod query;
mod registry;
mod ttl;

const TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE: usize = 256;
/// Size of the self-described derived write unit exported by Ursula.
///
/// Raw committed bytes and records remain available beside it. Keeping the
/// unit in the usage contract, rather than its field name, lets consumers
/// validate the interpretation before using the derived counter.
pub const COMMITTED_WRITE_UNIT_BYTES: u64 = 10 * 1024;

new_key_type! {
    struct StreamKey;
}

#[derive(Debug, Clone, Default)]
pub struct StreamStateMachine {
    buckets: HashSet<String>,
    /// Permanent tenant-erasure fences. A purged bucket name can never be
    /// reused, including after snapshot restore, so bytes cannot reappear
    /// behind an already-issued physical absence proof.
    erased_buckets: HashSet<String>,
    registry: StreamRegistry,
    /// Group-wide hot payload gauge. Kept incrementally so append admission
    /// and responses do not scan every stream in the group. Admission and
    /// flush thresholds count it alone (F6c): the hot window keeps no
    /// per-message bookkeeping.
    hot_payload_bytes: u64,
    cold_gc: ColdGcQueue,
    /// Live logical references to group-scoped shared cold objects. This is
    /// derived from per-stream cold refs when snapshots are restored.
    shared_cold_object_refs: HashMap<String, u64>,
    /// Every bucket whose bytes have ever occupied a still-live shared object.
    /// Keep owners after an individual bucket releases its references so the
    /// eventual physical-delete work is attributed to every erasure proof.
    shared_cold_object_owners: HashMap<String, HashSet<String>>,
    /// Per-bucket committed usage for this group; see [`BucketUsage`] for the
    /// monotonic-versus-gauge split. Mutated only by the accounting helpers
    /// below so every counter change stays deterministic and auditable.
    bucket_usage: HashMap<String, BucketUsage>,
    /// Derived flush-planner state (bounded-stream-state F10): the streams
    /// that hold hot bytes and the leader-local rotation cursor. Neither is
    /// replicated nor part of snapshots; restore rebuilds the index.
    flush_planner: flush_planner::FlushPlannerState,
    /// Largest stream `created_at_ms` this group assigned (C7, F14a/F14g).
    /// Every create assigns `max(now_ms, last_created_at_ms + 1)`, so stream
    /// incarnations are unique per group even under a frozen or skewed
    /// clock.
    last_created_at_ms: u64,
}

#[derive(Debug, Clone)]
struct StreamSlot {
    metadata: StreamMetadata,
    hot_buffer: HotBuffer,
    cold: StreamColdState,
    retained_offset: u64,
    visible_snapshot: Option<StreamVisibleSnapshot>,
    producers: HashMap<String, ProducerState>,
    /// Derived F3 receipt window over `producers`; rebuilt on restore.
    receipt_window: producers::ReceiptWindow,
    /// Runtime append count for this incarnation (F9). Kept by the group
    /// engine, not in [`StreamSnapshot`]; living in the slot makes it die with
    /// the stream on every removal path (delete, TTL expiry, bucket purge).
    append_count: u64,
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

    fn retain_shared_cold_object(&mut self, path: &str, bucket_id: &str) {
        let refs = self
            .shared_cold_object_refs
            .entry(path.to_owned())
            .or_default();
        *refs = refs.saturating_add(1);
        self.shared_cold_object_owners
            .entry(path.to_owned())
            .or_default()
            .insert(bucket_id.to_owned());
    }

    fn release_shared_cold_objects(
        &mut self,
        bucket_id: &str,
        paths: impl IntoIterator<Item = String>,
        not_before_ms: u64,
    ) {
        let mut reclaim = Vec::new();
        for path in paths {
            let Some(refs) = self.shared_cold_object_refs.get_mut(&path) else {
                continue;
            };
            *refs = refs.saturating_sub(1);
            if *refs == 0 {
                self.shared_cold_object_refs.remove(&path);
                let mut owners = self
                    .shared_cold_object_owners
                    .remove(&path)
                    .unwrap_or_default()
                    .into_iter()
                    .collect::<Vec<_>>();
                owners.sort();
                reclaim.push((path, owners));
            }
        }
        for (path, mut owners) in reclaim {
            if owners.is_empty() {
                owners.push(bucket_id.to_owned());
            }
            for owner in owners {
                self.cold_gc.enqueue_after(
                    owner,
                    ColdGcTarget::Paths(vec![path.clone()]),
                    not_before_ms,
                );
            }
        }
    }

    fn stream_metadata_mut(&mut self, stream_id: &BucketStreamId) -> Option<&mut StreamMetadata> {
        self.registry.metadata_mut(stream_id)
    }

    fn insert_stream_slot(&mut self, slot: StreamSlot) -> Option<StreamKey> {
        let hot_payload_bytes = u64::try_from(slot.hot_buffer.len()).expect("payload len fits u64");
        let stream_id = (!slot.hot_buffer.is_empty()).then(|| slot.metadata.stream_id.clone());
        let key = self.registry.insert(slot)?;
        self.hot_payload_bytes = self.hot_payload_bytes.saturating_add(hot_payload_bytes);
        if let Some(stream_id) = stream_id {
            self.flush_planner.mark_hot(&stream_id);
        }
        Some(key)
    }

    /// Re-derives one stream's membership in the flush planner's hot index
    /// after its hot buffer changed.
    fn sync_hot_index(&mut self, stream_id: &BucketStreamId) {
        let hot = self
            .registry
            .slot(stream_id)
            .is_some_and(|slot| !slot.hot_buffer.is_empty());
        if hot {
            self.flush_planner.mark_hot(stream_id);
        } else {
            self.flush_planner.unmark_hot(stream_id);
        }
    }

    fn add_hot_payload_bytes(&mut self, bytes: u64) {
        self.hot_payload_bytes = self.hot_payload_bytes.saturating_add(bytes);
    }

    fn remove_hot_payload_bytes(&mut self, bytes: u64) {
        self.hot_payload_bytes = self.hot_payload_bytes.saturating_sub(bytes);
    }

    /// Records committed by one accepted append. JSON streams provide exact
    /// canonical boundaries; a byte stream counts one message record per
    /// non-empty append.
    fn appended_record_count(record_ends: &[u64], payload_len: u64) -> u64 {
        if !record_ends.is_empty() {
            record_ends.len() as u64
        } else if payload_len > 0 {
            1
        } else {
            0
        }
    }

    fn usage_mut(&mut self, bucket_id: &str) -> &mut BucketUsage {
        self.bucket_usage.entry(bucket_id.to_owned()).or_default()
    }

    /// One accepted (non-deduplicated) append: monotonic counters grow and
    /// the retained gauge grows by the same bytes.
    fn usage_on_append(&mut self, bucket_id: &str, payload_bytes: u64, records: u64) {
        let usage = self.usage_mut(bucket_id);
        usage.committed_append_bytes = usage.committed_append_bytes.saturating_add(payload_bytes);
        usage.committed_records = usage.committed_records.saturating_add(records);
        usage.committed_write_units = usage
            .committed_write_units
            .saturating_add(payload_bytes.div_ceil(COMMITTED_WRITE_UNIT_BYTES).max(1));
        usage.retained_bytes = usage.retained_bytes.saturating_add(payload_bytes);
    }

    /// A newly created stream, including any initial payload it was created
    /// with.
    fn usage_on_stream_created(&mut self, bucket_id: &str, initial_bytes: u64, records: u64) {
        let usage = self.usage_mut(bucket_id);
        usage.stream_count = usage.stream_count.saturating_add(1);
        usage.committed_append_bytes = usage.committed_append_bytes.saturating_add(initial_bytes);
        usage.committed_records = usage.committed_records.saturating_add(records);
        usage.committed_write_units = usage
            .committed_write_units
            .saturating_add(initial_bytes.div_ceil(COMMITTED_WRITE_UNIT_BYTES).max(1));
        usage.retained_bytes = usage.retained_bytes.saturating_add(initial_bytes);
    }

    /// Destructive retention reclaimed `reclaimed_bytes` of logical prefix.
    fn usage_on_retention(&mut self, bucket_id: &str, reclaimed_bytes: u64) {
        let usage = self.usage_mut(bucket_id);
        usage.retained_bytes = usage.retained_bytes.saturating_sub(reclaimed_bytes);
    }

    /// A stream left the registry (delete or TTL expiry); its remaining
    /// retained bytes leave the gauge with it.
    fn usage_on_stream_removed(&mut self, bucket_id: &str, retained_bytes: u64) {
        let usage = self.usage_mut(bucket_id);
        usage.stream_count = usage.stream_count.saturating_sub(1);
        usage.retained_bytes = usage.retained_bytes.saturating_sub(retained_bytes);
    }

    /// Largest `created_at_ms` assigned by this group (C7).
    pub fn last_created_at_ms(&self) -> u64 {
        self.last_created_at_ms
    }

    fn max_live_created_at_ms(&self, floor: u64) -> u64 {
        self.registry
            .slots()
            .map(|slot| slot.metadata.created_at_ms)
            .fold(floor, u64::max)
    }

    /// The `created_at_ms` of a new stream incarnation (C7):
    /// `max(now_ms, last_created_at_ms + 1)`, unique and strictly increasing
    /// per group. The create records it with [`Self::record_created_at_ms`]
    /// once the stream is inserted.
    fn next_created_at_ms(&self, now_ms: u64) -> u64 {
        now_ms.max(self.last_created_at_ms.saturating_add(1))
    }

    fn record_created_at_ms(&mut self, created_at_ms: u64) {
        self.last_created_at_ms = self.last_created_at_ms.max(created_at_ms);
    }

    /// Current per-bucket usage for this group, sorted for deterministic
    /// output.
    pub fn bucket_usage_report(&self) -> Vec<BucketUsageSnapshot> {
        let mut report = self
            .bucket_usage
            .iter()
            .map(|(bucket_id, usage)| BucketUsageSnapshot {
                bucket_id: bucket_id.clone(),
                usage: *usage,
            })
            .collect::<Vec<_>>();
        report.sort_by(|left, right| left.bucket_id.cmp(&right.bucket_id));
        report
    }

    fn refresh_ttl_entry(&mut self, stream_id: &BucketStreamId) {
        self.registry.refresh_ttl(stream_id);
    }

    pub fn apply(&mut self, command: StreamCommand) -> StreamResponse {
        match command {
            StreamCommand::CreateBucket { bucket_id } => self.create_bucket(bucket_id),
            StreamCommand::CreateStream {
                stream_id,
                content_type,
                initial_payload,
                close_after,
                stream_seq,
                producer,
                stream_ttl_seconds,
                stream_expires_at_ms,
                now_ms,
            } => {
                let response = match canonical_json_record_ends(&content_type, &initial_payload) {
                    Ok(record_ends) => self.create_stream(CreateStreamInput {
                        stream_id,
                        content_type,
                        initial_payload: initial_payload.into(),
                        record_ends,
                        close_after,
                        stream_seq,
                        producer,
                        stream_ttl_seconds,
                        stream_expires_at_ms,
                        now_ms,
                    }),
                    Err(_) => StreamResponse::error(
                        StreamErrorCode::InvalidRecordBoundaries,
                        "application/json initial payload must use canonical newline boundaries",
                    ),
                };
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::CreateExternal {
                stream_id,
                content_type,
                initial_payload,
                record_ends,
                close_after,
                stream_seq,
                producer,
                stream_ttl_seconds,
                stream_expires_at_ms,
                now_ms,
            } => {
                let response = self.create_external_stream(CreateExternalStreamInput {
                    stream_id,
                    content_type,
                    initial_payload,
                    record_ends,
                    close_after,
                    stream_seq,
                    producer,
                    stream_ttl_seconds,
                    stream_expires_at_ms,
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
                record_ends,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => {
                let response = self.append_external(AppendExternalInput {
                    stream_id,
                    content_type: content_type.as_deref(),
                    payload,
                    record_ends,
                    close_after,
                    stream_seq,
                    producer,
                    now_ms,
                });
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::PublishSnapshot {
                stream_id,
                snapshot_offset,
                content_type,
                payload,
                now_ms,
                expected_incarnation,
            } => {
                let response = self.publish_snapshot(
                    stream_id,
                    snapshot_offset,
                    content_type,
                    cold::SnapshotBody::Inline(payload.into()),
                    now_ms,
                    expected_incarnation,
                );
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::PublishSnapshotExternal {
                stream_id,
                snapshot_offset,
                content_type,
                object,
                digest,
                now_ms,
                expected_incarnation,
            } => {
                let response = self.publish_snapshot(
                    stream_id,
                    snapshot_offset,
                    content_type,
                    cold::SnapshotBody::Object { object, digest },
                    now_ms,
                    expected_incarnation,
                );
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::AdvanceRetention {
                stream_id,
                retained_offset,
                now_ms,
                expected_incarnation,
            } => {
                let response = self.advance_retention(
                    stream_id,
                    retained_offset,
                    now_ms,
                    expected_incarnation,
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
            StreamCommand::FlushCold {
                stream_id,
                chunk,
                cold_generation,
            } => self.flush_cold(stream_id, chunk, cold_generation),
            StreamCommand::CompactCold {
                stream_id,
                old_chunks,
                replacement,
                gc_not_before_ms,
            } => self.compact_cold(stream_id, old_chunks, replacement, gc_not_before_ms),
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
            StreamCommand::PurgeBucket { bucket_id } => self.purge_bucket(&bucket_id),
            StreamCommand::AckColdGc { up_to_seq } => self.ack_cold_gc(up_to_seq),
            StreamCommand::DeferColdGc { seq, not_before_ms } => {
                self.defer_cold_gc(seq, not_before_ms)
            }
            StreamCommand::ImportSnapshot { snapshot } => self.import_snapshot(*snapshot),
            StreamCommand::TidyStream { stream_id, now_ms } => self.tidy_stream(&stream_id, now_ms),
            StreamCommand::OffloadColdRefs { stream_id, refs } => {
                self.offload_cold_refs(&stream_id, &refs)
            }
        }
    }
}

#[derive(Debug)]
struct CreateStreamInput {
    stream_id: BucketStreamId,
    content_type: String,
    initial_payload: Vec<u8>,
    record_ends: Vec<u64>,
    close_after: bool,
    stream_seq: Option<String>,
    producer: Option<ProducerRequest>,
    stream_ttl_seconds: Option<u64>,
    stream_expires_at_ms: Option<u64>,
    now_ms: u64,
}

#[derive(Debug)]
struct CreateExternalStreamInput {
    stream_id: BucketStreamId,
    content_type: String,
    initial_payload: ExternalPayloadRef,
    record_ends: Vec<u64>,
    close_after: bool,
    stream_seq: Option<String>,
    producer: Option<ProducerRequest>,
    stream_ttl_seconds: Option<u64>,
    stream_expires_at_ms: Option<u64>,
    now_ms: u64,
}

impl CreateStreamInput {
    fn initial_len(&self) -> u64 {
        u64::try_from(self.initial_payload.len()).expect("payload len fits u64")
    }
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

fn stream_ttl_renewal_due(stream: &StreamMetadata, now_ms: u64) -> bool {
    let Some(ttl_seconds) = stream.stream_ttl_seconds else {
        return false;
    };
    if stream.stream_expires_at_ms.is_some() {
        return false;
    }
    let ttl_ms = ttl_seconds.saturating_mul(1000);
    let renewal_interval_ms = ttl_ms.div_ceil(4).max(1);
    now_ms.saturating_sub(stream.last_ttl_touch_at_ms) >= renewal_interval_ms
}

/// Renewal never moves expiry earlier: `now_ms` comes from whichever node
/// proposed the command, and a forwarding node with a slow clock must not
/// shorten a stream's life.
fn renew_stream_ttl(stream: &mut StreamMetadata, now_ms: u64) {
    if stream.stream_ttl_seconds.is_some() && stream.stream_expires_at_ms.is_none() {
        stream.last_ttl_touch_at_ms = stream.last_ttl_touch_at_ms.max(now_ms);
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

/// Checks the message ends of an external create or append (computed by
/// the proposer from the staged payload): canonical JSON ends for a JSON
/// stream, none for any other.
fn validate_record_ends(
    content_type: &str,
    payload_len: u64,
    record_ends: &[u64],
) -> Result<(), StreamResponse> {
    if record_ends_valid(
        is_json_record_content_type(content_type),
        payload_len,
        record_ends,
    ) {
        return Ok(());
    }
    Err(StreamResponse::error(
        StreamErrorCode::InvalidRecordBoundaries,
        "message ends do not match the canonical JSON payload",
    ))
}

fn compare_stream_ids(left: &BucketStreamId, right: &BucketStreamId) -> std::cmp::Ordering {
    left.bucket_id
        .cmp(&right.bucket_id)
        .then_with(|| left.stream_id.cmp(&right.stream_id))
}

fn snapshot_digest(content_type: &str, payload: &[u8]) -> String {
    let mut digest = crate::model::SnapshotDigest::new(content_type);
    digest.update(payload);
    digest.finalize()
}

#[cfg(test)]
mod cold_snapshot_tests;
#[cfg(test)]
mod derived_boundaries_tests;
#[cfg(test)]
mod derived_cold_tests;
#[cfg(test)]
mod external_locators_tests;
#[cfg(test)]
mod hygiene_tests;
#[cfg(test)]
mod producer_window_tests;
#[cfg(test)]
mod tests;
