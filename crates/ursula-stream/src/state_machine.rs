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
//! - [`persist`]: snapshot / restore serialization, and the import that
//!   restores a backup as a history event.
//! - [`history`]: history events (`NewHistory`), the token every response
//!   renders, and what a stream inherited from the history before (W5).
//! - [`usage_ledger`]: per-version usage, counted at the bucket counter's
//!   call sites, the bucket totals derived from it, and the node-local
//!   index of live versions by seq (F20).
//! - [`usage_export`]: the export's pure read: one page of a group's
//!   per-stream usage, and the cursor rule that continues, relists or
//!   refuses as behind (F20).
//! - [`producer_record`]: the producer record and the evaluation of a producer
//!   write (#581 section 2.3).
//! - [`producers`]: F3 producer bounds (idle expiry, the producer cap, the
//!   epoch floor) and `TidyStream`.
//! - [`external_locators`]: F5 state-held external payload locators and
//!   `OffloadColdRefs`, plus the offload pass's query.
//! - [`hot_buffer`], [`cold_state`], [`ttl`]: internal per-stream data structures.
//!
//! The root keeps the [`StreamStateMachine`] type, its core slot/TTL accessors,
//! the [`StreamStateMachine::apply`] command dispatcher, and cross-cutting helpers.

use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BTreeMap;
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
use crate::history::HistoryEvent;
use crate::history::InheritedHistory;
use crate::json_records::NonCanonicalJsonPayload;
use crate::json_records::canonical_json_message_count;
use crate::json_records::is_json_record_content_type;
use crate::json_records::message_count_valid;
use crate::model::AppendExternalInput;
use crate::model::AppendStreamInput;
use crate::model::BOOTSTRAP_MAX_UPDATE_BYTES;
use crate::model::COLD_INDEX_PAGE_SPAN_BYTES;
use crate::model::ClosedEpoch;
use crate::model::ColdChunkRef;
use crate::model::ColdFlushCandidate;
use crate::model::ColdGcEntry;
use crate::model::ColdGcPlanEntry;
use crate::model::ColdGcTarget;
use crate::model::ContentFault;
use crate::model::ExternalPayloadRef;
use crate::model::HotPayloadSegment;
use crate::model::IncarnationCheck;
use crate::model::ObjectPayloadRef;
use crate::model::Opened;
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
use crate::model::WriteDigest;
use crate::model::WriteKind;
use crate::model::WriteReceipt;
use crate::response::ClosedBy;
use crate::response::ProducerAck;
use crate::response::ProducerDelivery;
use crate::response::ProducerEarlyAnswer;
use crate::response::ProducerRefusal;
use crate::response::StreamErrorCode;
use crate::response::StreamErrorContext;
use crate::response::StreamResponse;
use crate::snapshot::StreamSnapshot;
use crate::snapshot::StreamSnapshotEntry;
use crate::snapshot::StreamSnapshotError;
use crate::usage::VersionUsage;
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
mod history;
mod hot_buffer;

pub use self::cold::RETENTION_COLD_GC_GRACE_MS;
pub use self::cold_refs::SHARED_REF_COMPACTION_THRESHOLD;
pub use self::cold_refs::SHARED_REF_IDLE_MS;
pub use self::cold_refs::SharedRefCandidate;
pub use self::cold_refs::SharedRefCompactionRequest;
pub use self::cold_refs::SharedRefIdleTracker;
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
mod producer_record;
mod producers;
mod query;
mod registry;
mod ttl;
mod usage_export;
mod usage_ledger;

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
    /// The usage ledger (F15, F20): the group's genesis, its usage seq, its
    /// end records and each bucket's retired usage. Bucket totals derive
    /// from it and the slots ([`Self::bucket_usage_report`]).
    usage: usage_ledger::UsageLedger,
    /// Derived flush-planner state (bounded-stream-state F10): the streams
    /// that hold hot bytes and the leader-local rotation cursor. Neither is
    /// replicated nor part of snapshots; restore rebuilds the index.
    flush_planner: flush_planner::FlushPlannerState,
    /// Largest stream `created_at_ms` this group assigned (C7, F14a/F14g).
    /// Every create assigns `max(now_ms, last_created_at_ms + 1)`, so stream
    /// incarnations are unique per group even under a frozen or skewed
    /// clock.
    last_created_at_ms: u64,
    /// The group's latest history event (#581 section 4.2); `None` until
    /// its genesis applies, while the group serves nothing.
    history: Option<HistoryEvent>,
    /// What this state machine removed since it was built or restored
    /// (#669). Neither replicated nor part of snapshots: the Raft state
    /// machine reads it around each entry to learn that the entry removed a
    /// stream, so that the replica scrubs the stream's copies from its
    /// snapshot and log in bounded time. Restore starts it at zero.
    removed: RemovedState,
}

/// Streams and end records a group's state machine removed and the bytes
/// they held in a group snapshot (#669): cumulative since the state machine
/// was built or restored. A deletion, a bucket purge, a TTL expiry and a
/// create over an expired stream all remove a stream through one path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RemovedState {
    /// Streams removed.
    pub streams: u64,
    /// End records folded into their buckets' retired usage (F20).
    pub end_records: u64,
    /// Of those, end records folded early, because a removal found the
    /// group holding [`MAX_END_RECORDS`].
    pub end_records_early: u64,
    /// What those streams and end records added to a group snapshot: a
    /// stream's hot bytes, any inline snapshot body, and
    /// [`crate::COMMAND_LOG_OVERHEAD_BYTES`] each, and an end record's
    /// encoded size.
    pub bytes: u64,
}

impl RemovedState {
    /// What was removed between `earlier` and this tally.
    pub fn since(self, earlier: Self) -> Self {
        Self {
            streams: self.streams.saturating_sub(earlier.streams),
            end_records: self.end_records.saturating_sub(earlier.end_records),
            end_records_early: self
                .end_records_early
                .saturating_sub(earlier.end_records_early),
            bytes: self.bytes.saturating_sub(earlier.bytes),
        }
    }
}

/// The most end records a group holds (F20, bounded-state I2). A removal
/// that would hold more first folds the lowest-seq end record into its
/// bucket's retired usage. Part of the apply semantics of format epoch 7.
pub const MAX_END_RECORDS: usize = 65_536;

#[derive(Debug, Clone)]
struct StreamSlot {
    metadata: StreamMetadata,
    hot_buffer: HotBuffer,
    cold: StreamColdState,
    retained_offset: u64,
    visible_snapshot: Option<StreamVisibleSnapshot>,
    producers: HashMap<String, ProducerState>,
    /// The highest epoch of any producer record the stream retired: its
    /// epoch floor (#581 section 2.3). `None` when it never retired one.
    retired_epoch_watermark: Option<u64>,
    /// The group's history id when the stream was created or last changed
    /// its bytes or closure (W5). While it differs from the group's, no
    /// byte changed since the history event.
    history: u64,
    /// What the stream's current history began from: set by the first
    /// write after a history event (W5).
    inherited: Option<InheritedHistory>,
    /// The version's usage and its seq (F20), its applied appends (F9)
    /// included.
    usage: VersionUsage,
}

impl StreamStateMachine {
    /// A group before its genesis: it applies nothing but the genesis
    /// history event, and creates no stream before it.
    pub fn new() -> Self {
        Self::default()
    }

    /// A group whose genesis history event `history` applied as its first
    /// entry, the state both engines start a group in (#581 section 4.3).
    /// For callers that drive the machine without an engine: tests,
    /// benchmarks and the state probe.
    pub fn with_genesis(history: std::num::NonZeroU64) -> Self {
        let mut machine = Self::new();
        let started = machine.apply_entry(1, StreamCommand::NewHistory {
            history,
            now_ms: 0,
            previous: 0,
            kept: 0,
            leader: crate::HistoryLeader {
                term: 0,
                node_id: 0,
            },
        });
        debug_assert!(
            matches!(started, StreamResponse::HistoryStarted { .. }),
            "a genesis event always applies: {started:?}"
        );
        machine
    }

    /// What this state machine removed since it was built or restored
    /// ([`RemovedState`], #669).
    pub fn removed_state(&self) -> RemovedState {
        self.removed
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

    fn retain_shared_cold_object(&mut self, path: &str) {
        let refs = self
            .shared_cold_object_refs
            .entry(path.to_owned())
            .or_default();
        *refs = refs.saturating_add(1);
    }

    /// Drops one reference to each shared object in `paths`, and enqueues
    /// GC for the releasing bucket once an object is unreferenced. Shared
    /// packs live under `{bucket}/_packs/{group:08x}/`, so the releasing
    /// bucket is the object's only possible owner.
    fn release_shared_cold_objects(
        &mut self,
        bucket_id: &str,
        paths: impl IntoIterator<Item = String>,
        not_before_ms: u64,
    ) {
        for path in paths {
            let Some(refs) = self.shared_cold_object_refs.get_mut(&path) else {
                continue;
            };
            *refs = refs.saturating_sub(1);
            if *refs == 0 {
                self.shared_cold_object_refs.remove(&path);
                debug_assert!(
                    path.strip_prefix(bucket_id)
                        .is_some_and(|rest| rest.starts_with('/')),
                    "shared object '{path}' is outside bucket '{bucket_id}'"
                );
                self.cold_gc.enqueue_after(
                    bucket_id.to_owned(),
                    ColdGcTarget::Paths(vec![path]),
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
        let seq = slot.usage.seq;
        let key = self.registry.insert(slot)?;
        self.usage.index_live(seq, key);
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

    /// Records committed by one accepted append. JSON streams provide the
    /// exact count of canonical messages; a byte stream counts one message
    /// record per non-empty append.
    fn appended_record_count(json_messages: u64, payload_len: u64) -> u64 {
        if json_messages > 0 {
            json_messages
        } else if payload_len > 0 {
            1
        } else {
            0
        }
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
    /// A `created_at_ms` at or above 2^52 is refused
    /// ([`crate::MAX_CREATED_AT_MS`]), so a token never drops a bit of it.
    fn next_created_at_ms(&self, now_ms: u64) -> Result<u64, StreamResponse> {
        let created_at_ms = now_ms.max(self.last_created_at_ms.saturating_add(1));
        if created_at_ms > crate::history::MAX_CREATED_AT_MS {
            return Err(StreamResponse::error(
                StreamErrorCode::CreatedAtOutOfRange,
                format!(
                    "stream creation time {created_at_ms} ms is at or above 2^52 ms; the \
                     proposer's clock is far ahead"
                ),
            ));
        }
        Ok(created_at_ms)
    }

    fn record_created_at_ms(&mut self, created_at_ms: u64) {
        self.last_created_at_ms = self.last_created_at_ms.max(created_at_ms);
    }

    /// Per bucket, the summed body length of each live stream's latest
    /// visible snapshot, as uploaded: the inline payload's length, or
    /// `payload_len` of the cold object that holds the body. A bucket
    /// without a published snapshot is absent.
    ///
    /// Derived from the slots on every call (O(streams)), never kept in
    /// replicated or persisted state, so it cannot drift from the streams
    /// it describes and changes no format. A replaced body during its GC
    /// grace, staged snapshot parts and Raft group snapshots do not count.
    pub fn snapshot_bytes_by_bucket(&self) -> BTreeMap<String, u64> {
        let mut bytes = BTreeMap::<String, u64>::new();
        for slot in self.registry.slots() {
            let Some(body_len) = usage_ledger::visible_snapshot_bytes(slot) else {
                continue;
            };
            let bucket_id = &slot.metadata.stream_id.bucket_id;
            match bytes.get_mut(bucket_id.as_str()) {
                Some(total) => *total = total.saturating_add(body_len),
                None => {
                    bytes.insert(bucket_id.clone(), body_len);
                }
            }
        }
        bytes
    }

    fn refresh_ttl_entry(&mut self, stream_id: &BucketStreamId) {
        self.registry.refresh_ttl(stream_id);
    }

    /// Applies one command for a caller without a log (tests, benchmarks,
    /// probes): the entry's index is 0. See [`Self::apply_entry`].
    pub fn apply(&mut self, command: StreamCommand) -> StreamResponse {
        self.apply_entry(0, command)
    }

    /// Applies the command of the log entry at `index`: a command of a
    /// request without a `Stream-Incarnation` precondition, or a wrapped
    /// [`StreamCommand::IfIncarnation`]. A history event records `index`
    /// (#581 section 4.3), as a command carries its `now_ms`.
    pub fn apply_entry(&mut self, index: u64, command: StreamCommand) -> StreamResponse {
        self.apply_checked(index, command, IncarnationCheck::Absent)
    }

    /// Applies one command of the log entry at `index`. `check` says
    /// whether the request's `Stream-Incarnation` precondition matched; the
    /// epoch floor binds tokened producer writes.
    pub fn apply_checked(
        &mut self,
        index: u64,
        command: StreamCommand,
        check: IncarnationCheck,
    ) -> StreamResponse {
        match command {
            StreamCommand::CreateBucket { bucket_id } => self.create_bucket(bucket_id),
            StreamCommand::CreateStream {
                stream_id,
                content_type,
                initial_payload,
                close_after,
                stream_seq,
                stream_ttl_seconds,
                stream_expires_at_ms,
                now_ms,
            } => match canonical_json_message_count(&content_type, &initial_payload) {
                Ok(message_count) => self.create_stream(CreateStreamInput {
                    stream_id,
                    content_type,
                    initial_payload: initial_payload.into(),
                    message_count,
                    close_after,
                    stream_seq,
                    stream_ttl_seconds,
                    stream_expires_at_ms,
                    now_ms,
                }),
                Err(NonCanonicalJsonPayload) => StreamResponse::error(
                    StreamErrorCode::InvalidRecordBoundaries,
                    "application/json initial payload must use canonical newline boundaries",
                ),
            },
            StreamCommand::CreateExternal {
                stream_id,
                content_type,
                initial_payload,
                message_count,
                close_after,
                stream_seq,
                stream_ttl_seconds,
                stream_expires_at_ms,
                now_ms,
            } => self.create_external_stream(CreateExternalStreamInput {
                stream_id,
                content_type,
                initial_payload,
                message_count,
                close_after,
                stream_seq,
                stream_ttl_seconds,
                stream_expires_at_ms,
                now_ms,
            }),
            StreamCommand::Append {
                stream_id,
                content_type,
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => self.append_borrowed(AppendStreamInput {
                stream_id,
                content_type: content_type.as_deref(),
                payload: &payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
                kind: WriteKind::Append,
                check,
            }),
            StreamCommand::AppendExternal {
                stream_id,
                content_type,
                payload,
                message_count,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => self.append_external(AppendExternalInput {
                stream_id,
                content_type: content_type.as_deref(),
                payload,
                message_count,
                close_after,
                stream_seq,
                producer,
                now_ms,
                check,
            }),
            StreamCommand::PublishSnapshot {
                stream_id,
                snapshot_offset,
                content_type,
                payload,
                now_ms,
                expected_incarnation,
            } => self.publish_snapshot(
                stream_id,
                snapshot_offset,
                content_type,
                cold::SnapshotBody::Inline(payload.into()),
                now_ms,
                expected_incarnation,
            ),
            StreamCommand::PublishSnapshotExternal {
                stream_id,
                snapshot_offset,
                content_type,
                object,
                digest,
                now_ms,
                expected_incarnation,
            } => self.publish_snapshot(
                stream_id,
                snapshot_offset,
                content_type,
                cold::SnapshotBody::Object { object, digest },
                now_ms,
                expected_incarnation,
            ),
            StreamCommand::AdvanceRetention {
                stream_id,
                retained_offset,
                now_ms,
                expected_incarnation,
            } => self.advance_retention(stream_id, retained_offset, now_ms, expected_incarnation),
            StreamCommand::TouchStreamAccess {
                stream_id,
                now_ms,
                renew_ttl,
            } => self.touch_stream_access(&stream_id, now_ms, renew_ttl),
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
            } => self.close(stream_id, stream_seq, producer, now_ms, check),
            StreamCommand::DeleteStream { stream_id, now_ms } => {
                self.delete_stream(&stream_id, now_ms)
            }
            StreamCommand::PurgeBucket { bucket_id, now_ms } => {
                self.purge_bucket(&bucket_id, now_ms)
            }
            StreamCommand::AckColdGc { up_to_seq } => self.ack_cold_gc(up_to_seq),
            StreamCommand::DeferColdGc { seq, not_before_ms } => {
                self.defer_cold_gc(seq, not_before_ms)
            }
            StreamCommand::ImportSnapshot {
                snapshot,
                history,
                backup_digest,
                now_ms,
            } => self.import_snapshot(index, *snapshot, history, backup_digest, now_ms),
            StreamCommand::NewHistory {
                history,
                now_ms,
                previous,
                kept,
                leader,
            } => self.new_history(index, history, previous, kept, leader, now_ms),
            StreamCommand::TidyStream { stream_id, now_ms } => self.tidy_stream(&stream_id, now_ms),
            StreamCommand::Sweep {
                now_ms,
                limit,
                retire_through,
            } => self.sweep(now_ms, limit, retire_through),
            StreamCommand::OffloadColdRefs { stream_id, refs } => {
                self.offload_cold_refs(&stream_id, &refs)
            }
            StreamCommand::IfIncarnation {
                incarnation,
                command,
            } => match self.incarnation_precondition(&command, incarnation) {
                Some(refusal) => refusal,
                None => self.apply_checked(index, *command, IncarnationCheck::Matched),
            },
        }
    }
}

#[derive(Debug)]
struct CreateStreamInput {
    stream_id: BucketStreamId,
    content_type: String,
    initial_payload: Vec<u8>,
    message_count: u64,
    close_after: bool,
    stream_seq: Option<String>,
    stream_ttl_seconds: Option<u64>,
    stream_expires_at_ms: Option<u64>,
    now_ms: u64,
}

#[derive(Debug)]
struct CreateExternalStreamInput {
    stream_id: BucketStreamId,
    content_type: String,
    initial_payload: ExternalPayloadRef,
    message_count: u64,
    close_after: bool,
    stream_seq: Option<String>,
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

/// The content fault the proposer recorded on a producer write, if any.
fn producer_content_fault(
    producer: &ProducerRequest,
) -> Result<Option<ContentFault>, StreamResponse> {
    match ursula_proto::ContentFaultV1::try_from(producer.content_fault) {
        Ok(ursula_proto::ContentFaultV1::ContentFaultNone) => Ok(None),
        Ok(ursula_proto::ContentFaultV1::ContentFaultMissingContentType) => {
            Ok(Some(ContentFault::MissingContentType))
        }
        Ok(ursula_proto::ContentFaultV1::ContentFaultDigestMismatch) => {
            Ok(Some(ContentFault::DigestMismatch))
        }
        Ok(ursula_proto::ContentFaultV1::ContentFaultMalformedJson) => {
            Ok(Some(ContentFault::MalformedJson))
        }
        Err(_unknown) => Err(StreamResponse::error(
            StreamErrorCode::InvalidProducer,
            format!("unknown producer content fault {}", producer.content_fault),
        )),
    }
}

/// The `400` a write that would apply gets for its content fault.
fn content_fault_response(fault: ContentFault) -> StreamResponse {
    match fault {
        ContentFault::MissingContentType => StreamResponse::error(
            StreamErrorCode::MissingContentType,
            "append with a body must include content type",
        ),
        ContentFault::DigestMismatch => StreamResponse::error(
            StreamErrorCode::ProducerDigestMismatch,
            "producer-digest does not match the request body",
        ),
        ContentFault::MalformedJson => StreamResponse::error(
            StreamErrorCode::InvalidRecordBoundaries,
            "application/json body is not valid JSON message text",
        ),
    }
}

/// The key of a producer write, validated: a non-empty id, an epoch and a
/// seq within the base protocol's limit, and a 32-byte digest when present.
fn producer_write_key(
    producer: &ProducerRequest,
) -> Result<producer_record::WriteKey, StreamResponse> {
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
    // `ursula-headers` already parsed an HTTP digest into 32 bytes, so this
    // refusal cannot be reached from HTTP. The replicated command carries the
    // digest as bytes, though, and apply checks every command it is given.
    let digest = match producer.digest.as_deref() {
        None => None,
        Some(bytes) => Some(WriteDigest::try_from(bytes).map_err(|_wrong_length| {
            StreamResponse::error(
                StreamErrorCode::InvalidProducer,
                format!("producer digest must be 32 bytes, got {}", bytes.len()),
            )
        })?),
    };
    Ok(producer_record::WriteKey {
        epoch: producer.producer_epoch,
        seq: producer.producer_seq,
        digest,
    })
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

/// Checks the message count of an external create or append (counted by
/// the proposer over the staged payload): at least one and at most one per
/// two payload bytes for a JSON stream, none for any other.
fn validate_message_count(
    content_type: &str,
    payload_len: u64,
    message_count: u64,
) -> Result<(), StreamResponse> {
    if message_count_valid(
        is_json_record_content_type(content_type),
        payload_len,
        message_count,
    ) {
        return Ok(());
    }
    Err(StreamResponse::error(
        StreamErrorCode::InvalidRecordBoundaries,
        "message count does not match the canonical JSON payload",
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
mod history_tests;
#[cfg(test)]
mod hygiene_tests;
#[cfg(test)]
mod producer_record_tests;
#[cfg(test)]
mod sweep_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod usage_export_tests;
#[cfg(test)]
mod usage_ledger_tests;
#[cfg(test)]
mod usage_oracle_tests;
#[cfg(test)]
mod usage_snapshot_tests;
