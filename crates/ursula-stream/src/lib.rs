//! Durable Streams state machine for Ursula.
//!
//! Module map:
//!
//! - [`command`]: replicated command variants applied to the state machine.
//! - [`format`]: the version of every persisted or replicated artifact, the
//!   format epoch, and the rule for changing them.
//! - [`response`]: result variants and error codes returned per command.
//! - [`model`]: persistent data types (metadata, segments, producer state, plans).
//! - [`json_records`]: canonical JSON message text: LF-terminated messages,
//!   whose ends feed the `committed_records` usage counter.
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
mod format;
mod json_records;
mod model;
mod response;
mod snapshot;
mod state_machine;
mod validate;

pub use command::COMMAND_LOG_OVERHEAD_BYTES;
pub use command::StreamCommand;
pub use format::BACKUP_FORMAT_VERSION;
pub use format::COLD_INDEX_PAGE_VERSION;
pub use format::FORMAT_EPOCH;
pub use format::GROUP_SNAPSHOT_VERSION;
pub use format::RAFT_GRPC_PROTOCOL_VERSION;
pub use format::RAFT_WAL_VERSION;
pub use format::SNAPSHOT_REFERENCE_VERSION;
pub use format::STREAM_SNAPSHOT_VERSION;
pub use format::UPGRADE_GUIDE_URL;
pub use format::other_release_refusal;
pub use json_records::NonCanonicalJsonPayload;
pub use json_records::canonical_json_record_ends;
pub use model::AppendStreamInput;
pub use model::BOOTSTRAP_MAX_UPDATE_BYTES;
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
pub use model::MAX_COLD_SNAPSHOT_BYTES;
pub use model::ObjectPayloadRef;
pub use model::ProducerReceipt;
pub use model::ProducerRequest;
pub use model::ProducerSnapshot;
pub use model::SnapshotDigest;
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
pub use response::StreamErrorCode;
pub use response::StreamErrorContext;
pub use response::StreamResponse;
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
pub use state_machine::MAX_STAGED_EXTERNAL_REFS;
pub use state_machine::RETENTION_COLD_GC_GRACE_MS;
pub use state_machine::SHARED_REF_COMPACTION_THRESHOLD;
pub use state_machine::SHARED_REF_IDLE_MS;
pub use state_machine::STAGED_EXTERNAL_REF_MAX_AGE_MS;
pub use state_machine::SharedRefCandidate;
pub use state_machine::SharedRefCompactionRequest;
pub use state_machine::SharedRefIdleTracker;
pub use state_machine::StagedExternalRefCandidate;
pub use state_machine::StreamStateMachine;
pub use state_machine::plan_shared_ref_run;
pub use validate::validate_bucket_id;
pub use validate::validate_stream_id;
