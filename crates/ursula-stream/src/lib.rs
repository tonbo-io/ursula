//! Durable Streams state machine for Ursula.
//!
//! Module map:
//!
//! - [`command`]: replicated command variants applied to the state machine.
//! - [`history`]: history identity: the group's history, the
//!   `Stream-Incarnation` token derived from it, and what a stream inherited
//!   from the history before (#581 section 4).
//! - [`format`]: the format epoch every persisted format and the Raft RPC
//!   protocol share, the backup format, and their policy.
//! - [`response`]: result variants and error codes returned per command.
//! - [`model`]: persistent data types (metadata, segments, producer state, plans).
//! - [`msgpack`]: Ursula's one MessagePack codec, compact: a struct is an
//!   array of its fields in declaration order and a variant keeps its name.
//! - [`json_records`]: canonical JSON message text: LF-terminated messages,
//!   whose count feeds the `committed_records` usage counter.
//! - [`sim_controls`]: defences a simulation's negative control turns off
//!   (`cfg(madsim)` only; constant in every other build).
//! - [`snapshot`]: snapshot wire format and restoration errors.
//! - [`state_machine`]: the deterministic [`StreamStateMachine`] that drives a Raft group,
//!   plus [`GroupStateGauges`], the per-group bounded-state gauges
//!   (`docs/architecture/bounded-stream-state.md`, Gauges), and the leader-side
//!   cold-reference queries: shared-ref compaction discovery
//!   ([`SharedRefCandidate`], F2), orphan-sweep references (F14h) and
//!   staged external-ref offload discovery ([`StagedExternalRefCandidate`],
//!   F5).
//! - [`usage`]: per-stream usage (F20): each version's committed counters
//!   and applied appends, counted at the bucket counter's call sites, the
//!   end record a removal leaves, and the types of the paged export read.
//! - [`validate`]: bucket/stream id validation used by HTTP and Raft entry points.

mod command;
mod format;
mod history;
mod json_records;
mod model;
pub mod msgpack;
mod response;
pub mod sim_controls;
mod snapshot;
mod state_machine;
mod usage;
mod validate;

pub use command::COMMAND_LOG_OVERHEAD_BYTES;
pub use command::StreamCommand;
pub use format::BACKUP_FORMAT_VERSION;
pub use format::FORMAT_EPOCH;
pub use format::backup_format_refusal;
pub use format::format_epoch_refusal;
pub use history::BackupDigest;
pub use history::HistoryEvent;
pub use history::HistoryLeader;
pub use history::INCARNATION_TAG;
pub use history::InheritedHistory;
pub use history::InheritedReport;
pub use history::MAX_CREATED_AT_MS;
pub use history::draw_history_id;
pub use history::history_key;
pub use history::incarnation_token;
pub use json_records::NonCanonicalJsonPayload;
pub use json_records::canonical_json_message_count;
pub use model::BOOTSTRAP_MAX_UPDATE_BYTES;
pub use model::BucketUsage;
pub use model::BucketUsageSnapshot;
pub use model::COLD_INDEX_PAGE_SPAN_BYTES;
pub use model::ClosedEpoch;
pub use model::ColdChunkRef;
pub use model::ColdFlushCandidate;
pub use model::ColdGcEntry;
pub use model::ColdGcPlanEntry;
pub use model::ColdGcTarget;
pub use model::ContentFault;
pub use model::ExternalPayloadRef;
pub use model::HotPayloadSegment;
pub use model::IncarnationCheck;
pub use model::ObjectPayloadRef;
pub use model::Opened;
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
pub use model::WriteDigest;
pub use model::WriteReceipt;
pub use response::ClosedBy;
pub use response::ProducerAck;
pub use response::ProducerDelivery;
pub use response::ProducerEarlyAnswer;
pub use response::ProducerRefusal;
pub use response::StreamErrorCode;
pub use response::StreamErrorContext;
pub use response::StreamResponse;
pub use snapshot::BackupSnapshotError;
pub use snapshot::StreamSnapshot;
pub use snapshot::StreamSnapshotEntry;
pub use snapshot::StreamSnapshotError;
pub use snapshot::decode_backup_snapshot;
pub use state_machine::COMMITTED_WRITE_UNIT_BYTES;
pub use state_machine::ColdFlushHotAge;
pub use state_machine::ColdFlushPass;
pub use state_machine::ColdFlushPassRequest;
pub use state_machine::ColdFlushPlanStats;
pub use state_machine::ColdFlushPressure;
pub use state_machine::GroupStateGauges;
pub use state_machine::MAX_END_RECORDS;
pub use state_machine::MAX_STAGED_EXTERNAL_REFS;
pub use state_machine::RETENTION_COLD_GC_GRACE_MS;
pub use state_machine::RemovedState;
pub use state_machine::SHARED_REF_COMPACTION_THRESHOLD;
pub use state_machine::SHARED_REF_IDLE_MS;
pub use state_machine::STAGED_EXTERNAL_REF_MAX_AGE_MS;
pub use state_machine::SharedRefCandidate;
pub use state_machine::SharedRefCompactionRequest;
pub use state_machine::SharedRefIdleTracker;
pub use state_machine::StagedExternalRefCandidate;
pub use state_machine::StreamStateMachine;
pub use state_machine::plan_shared_ref_run;
pub use usage::BucketRetired;
pub use usage::BucketUsageRow;
pub use usage::CommittedUsage;
pub use usage::EndRecord;
pub use usage::GroupUsageView;
pub use usage::LastWrite;
pub use usage::Lifecycle;
pub use usage::RelistReason;
pub use usage::StreamEnd;
pub use usage::StreamUsageRead;
pub use usage::UsageCounter;
pub use usage::UsageCursor;
pub use usage::UsageEntry;
pub use usage::UsageListing;
pub use usage::UsageLogId;
pub use usage::UsagePage;
pub use usage::UsagePosition;
pub use usage::UsageSnapshot;
pub use usage::UsageSnapshotError;
pub use usage::VersionCounters;
pub use usage::VersionUsage;
pub use validate::validate_bucket_id;
pub use validate::validate_stream_id;
