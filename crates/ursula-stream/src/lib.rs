//! Durable Streams state machine for Ursula.
//!
//! Module map:
//!
//! - [`command`]: replicated command variants applied to the state machine.
//! - [`response`]: result variants and error codes returned per command.
//! - [`model`]: persistent data types (metadata, segments, producer state, plans).
//! - [`record_index`]: exact retained record-ordinal to offset boundaries.
//! - [`snapshot`]: snapshot wire format and restoration errors.
//! - [`state_machine`]: the deterministic [`StreamStateMachine`] that drives a Raft group.
//! - [`validate`]: bucket/stream id validation used by HTTP and Raft entry points.

mod command;
mod integrity;
mod model;
mod record_index;
mod response;
mod snapshot;
mod state_machine;
mod validate;

pub use command::StreamCommand;
pub use integrity::StreamIntegritySnapshot;
pub use model::AppendStreamInput;
pub use model::COLD_INDEX_PAGE_SPAN_BYTES;
pub use model::ColdChunkRef;
pub use model::ColdFlushCandidate;
pub use model::ColdGcEntry;
pub use model::ColdGcTarget;
pub use model::ExternalPayloadRef;
pub use model::HotPayloadSegment;
pub use model::ObjectPayloadRef;
pub use model::ProducerAppendRecord;
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
pub(crate) use record_index::PreparedRecordAppend;
pub use record_index::RecordIndexError;
pub use record_index::StreamRecordIndex;
pub use record_index::StreamRecordRange;
pub use record_index::canonical_json_record_ends;
pub use record_index::is_json_record_content_type;
pub use response::StreamErrorCode;
pub use response::StreamErrorContext;
pub use response::StreamResponse;
pub use snapshot::StreamSnapshot;
pub use snapshot::StreamSnapshotEntry;
pub use snapshot::StreamSnapshotError;
pub use state_machine::StreamStateMachine;
pub use validate::validate_bucket_id;
pub use validate::validate_stream_id;
