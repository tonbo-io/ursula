//! Durable Streams state machine for Ursula.
//!
//! Module map:
//!
//! - [`command`]: replicated command variants applied to the state machine.
//! - [`response`]: result variants and error codes returned per command.
//! - [`model`]: persistent data types (metadata, segments, producer state, plans).
//! - [`snapshot`]: snapshot wire format and restoration errors.
//! - [`state_machine`]: the deterministic [`StreamStateMachine`] that drives a Raft group.
//! - [`validate`]: bucket/stream id validation used by HTTP and Raft entry points.

mod command;
mod model;
mod response;
mod snapshot;
mod state_machine;
mod validate;

pub use command::StreamCommand;
pub use model::{
    AppendStreamInput, ColdChunkRef, ColdFlushCandidate, ExternalPayloadRef, HotPayloadSegment,
    ObjectPayloadRef, ProducerAppendRecord, ProducerRequest, ProducerSnapshot, StreamBatchAppend,
    StreamBatchAppendItem, StreamBootstrapPlan, StreamMessageRecord, StreamMetadata, StreamRead,
    StreamReadColdSegment, StreamReadObjectSegment, StreamReadPlan, StreamReadSegment,
    StreamStatus, StreamVisibleSnapshot,
};
pub use response::{StreamErrorCode, StreamResponse};
pub use snapshot::{StreamSnapshot, StreamSnapshotEntry, StreamSnapshotError};
pub use state_machine::StreamStateMachine;
pub use validate::{validate_bucket_id, validate_stream_id};
