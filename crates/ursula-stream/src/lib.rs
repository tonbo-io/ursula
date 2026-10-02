//! Durable Streams state machine for Ursula.
//!
//! Module map:
//!
//! - [`command`]: replicated command variants applied to the state machine.
//! - [`feature`]: replicated group feature levels and the apply-time gate.
//! - [`response`]: result variants and error codes returned per command.
//! - [`model`]: persistent data types (metadata, segments, producer state, plans).
//! - [`record_index`]: retained record-ordinal to offset boundaries: exact for
//!   unflushed records, sparse 1 MiB marks for sealed cold records (F1).
//! - [`snapshot`]: snapshot wire format and restoration errors.
//! - [`state_machine`]: the deterministic [`StreamStateMachine`] that drives a Raft group,
//!   plus [`GroupStateGauges`], the per-group bounded-state gauges
//!   (`docs/architecture/bounded-stream-state.md` §7.5), and the leader-side
//!   cold-reference queries: shared-ref compaction discovery
//!   ([`SharedRefCandidate`], F2), orphan-sweep references (F14h) and
//!   staged external-ref offload discovery ([`StagedExternalRefCandidate`],
//!   F5).
//! - [`validate`]: bucket/stream id validation used by HTTP and Raft entry points.

mod command;
mod feature;
mod integrity;
mod model;
mod record_index;
mod response;
mod snapshot;
mod state_machine;
mod validate;

pub use command::StreamCommand;
pub use feature::FEATURE_LEVEL_BASELINE;
pub use feature::FEATURE_LEVEL_EXTERNAL_LOCATORS;
pub use feature::FEATURE_LEVEL_KEYED_STREAMS;
pub use feature::FEATURE_LEVEL_SPARSE_MARKS;
pub use feature::MAX_SUPPORTED_FEATURE_LEVEL;
pub use feature::check_feature_level;
pub use integrity::StreamIntegritySnapshot;
pub use model::AppendStreamInput;
pub use model::BOOTSTRAP_MAX_UPDATE_BYTES;
pub use model::BucketQuota;
pub use model::BucketQuotaSnapshot;
pub use model::BucketStreamListing;
pub use model::BucketUsage;
pub use model::BucketUsageSnapshot;
pub use model::COLD_INDEX_PAGE_SPAN_BYTES;
pub use model::ColdChunkRef;
pub use model::ColdFlushCandidate;
pub use model::ColdGcEntry;
pub use model::ColdGcPlanEntry;
pub use model::ColdGcTarget;
pub use model::ExternalPayloadRef;
pub use model::HotPayloadSegment;
pub use model::ObjectPayloadRef;
pub use model::ProducerAppendRecord;
pub use model::ProducerReceipt;
pub use model::ProducerRequest;
pub use model::ProducerSnapshot;
pub use model::StreamAttrs;
pub use model::StreamBatchAppend;
pub use model::StreamBatchAppendItem;
pub use model::StreamBootstrapPlan;
pub use model::StreamMessageRecord;
pub use model::StreamMetadata;
pub use model::StreamRead;
pub use model::StreamReadColdIndexSegment;
pub use model::StreamReadColdSegment;
pub use model::StreamReadObjectSegment;
pub use model::StreamReadPlan;
pub use model::StreamReadSegment;
pub use model::StreamStatus;
pub use model::StreamVisibleSnapshot;
pub use model::bucket_local_stream_path;
pub use record_index::MARK_BLOCK_BYTES;
pub use record_index::MARK_BLOCK_SHIFT;
pub use record_index::OffsetLocation;
pub(crate) use record_index::PreparedRecordAppend;
pub use record_index::RecordBracket;
pub use record_index::RecordCorruption;
pub use record_index::RecordIndexError;
pub use record_index::RecordMark;
pub use record_index::RecordOffset;
pub use record_index::RecordTrim;
pub use record_index::SEAL_BUDGET_RECORDS;
pub use record_index::StreamRecordIndex;
pub use record_index::StreamRecordRange;
pub use record_index::TrimmedRecords;
pub use record_index::canonical_json_record_ends;
pub use record_index::is_json_record_content_type;
pub use record_index::mark_block_end;
pub use record_index::trim_record_window;
pub use response::StreamErrorCode;
pub use response::StreamErrorContext;
pub use response::StreamResponse;
pub use snapshot::SharedColdObjectOwnersSnapshot;
pub use snapshot::StreamSnapshot;
pub use snapshot::StreamSnapshotEntry;
pub use snapshot::StreamSnapshotError;
pub use state_machine::COMMITTED_WRITE_UNIT_BYTES;
pub use state_machine::ColdFlushHotAge;
pub use state_machine::ColdFlushPass;
pub use state_machine::ColdFlushPassRequest;
pub use state_machine::ColdFlushPlanStats;
pub use state_machine::ColdFlushPressure;
pub use state_machine::GroupStateGauges;
pub use state_machine::RecordPlanError;
pub use state_machine::RecordReadAnchor;
pub use state_machine::RecordReadRequest;
pub use state_machine::MAX_STAGED_EXTERNAL_REFS;
pub use state_machine::SHARED_REF_COMPACTION_THRESHOLD;
pub use state_machine::SHARED_REF_IDLE_MS;
pub use state_machine::STAGED_EXTERNAL_REF_MAX_AGE_MS;
pub use state_machine::SharedRefCandidate;
pub use state_machine::SharedRefCompactionRequest;
pub use state_machine::SharedRefIdleTracker;
pub use state_machine::StagedExternalRefCandidate;
pub use state_machine::StreamStateMachine;
pub use state_machine::is_legacy_cross_bucket_pack;
pub use state_machine::plan_shared_ref_run;
pub use validate::validate_bucket_id;
pub use validate::validate_stream_id;
