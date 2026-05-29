//! Per-core actor runtime for Ursula.
//!
//! Module map:
//!
//! - [`cold_store`]: opendal-backed cold tier handle and object path helpers.
//! - [`request`]: HTTP/gRPC request and response value types for each engine op.
//! - [`command`]: the replicated [`GroupWriteCommand`] and `From` conversions from
//!   request values into the wire command consumed by `GroupEngine`.
//! - [`error`]: runtime-level error type [`RuntimeError`].
//! - [`engine`]: the `GroupEngine` trait, factory, metrics, and the boxed-future
//!   type aliases that form the replaceable per-group engine boundary, plus the
//!   in-memory and WAL implementations under [`engine::in_memory`] and
//!   [`engine::wal`].
//! - [`runtime`]: `ShardRuntime`, `RuntimeConfig`, and per-core worker spawn.
//! - [`core_worker`]: single-thread actor that owns groups for one core.
//! - [`group_actor`]: per-group mailbox actor running inside a core worker.
//! - [`metrics`]: runtime metrics shared across cores; lock-free counters.

mod admission;
mod cold_store;
mod command;
mod core_worker;
mod engine;
mod error;
mod group_actor;
mod metrics;
mod request;
mod rt;
mod runtime;
mod snapshot_store;

pub use admission::RaftUncommittedAdmission;
pub use cold_store::{
    ColdReadCacheConfig, ColdStore, ColdStoreEvent, ColdStoreFault, ColdStoreFaultContext,
    ColdStoreFaultEffect, ColdStoreHandle, ColdStoreInfo, ColdStoreOperation, new_cold_chunk_path,
    new_external_payload_path,
};
pub use command::{GroupSnapshot, GroupWriteCommand};
pub use engine::in_memory::{InMemoryGroupEngine, InMemoryGroupEngineFactory};
pub use engine::wal::{WalGroupEngine, WalGroupEngineFactory};
pub use engine::{
    GroupAckColdGcFuture, GroupAppendBatchFuture, GroupAppendBatchResponse, GroupAppendFuture,
    GroupBootstrapStreamFuture, GroupCloseStreamFuture, GroupColdHotBacklogFuture,
    GroupCreateStreamFuture, GroupDeleteSnapshotFuture, GroupDeleteStreamFuture, GroupEngine,
    GroupEngineCreateFuture, GroupEngineError, GroupEngineFactory, GroupEngineMetrics,
    GroupFlushColdFuture, GroupForkRefFuture, GroupHeadStreamFuture, GroupInstallSnapshotFuture,
    GroupLeaderHint, GroupPlanColdFlushFuture, GroupPlanColdGcFuture,
    GroupPlanNextColdFlushBatchFuture, GroupPlanNextColdFlushFuture, GroupPublishSnapshotFuture,
    GroupReadSnapshotFuture, GroupReadStreamFuture, GroupReadStreamPartsFuture,
    GroupRequireLiveReadOwnerFuture, GroupShutdownFuture, GroupSnapshotFuture,
    GroupTouchStreamAccessFuture, GroupWriteBatchFuture, GroupWriteResponse,
};
pub use error::RuntimeError;
pub use metrics::{RuntimeMailboxSnapshot, RuntimeMetrics, RuntimeMetricsSnapshot};
pub use request::{
    AckColdGcResponse, AppendBatchRequest, AppendBatchResponse, AppendExternalRequest,
    AppendRequest, AppendResponse, BootstrapStreamRequest, BootstrapStreamResponse,
    BootstrapUpdate, CloseStreamRequest, CloseStreamResponse, ColdHotBacklog, ColdWriteAdmission,
    CreateStreamExternalRequest, CreateStreamRequest, CreateStreamResponse, DeleteSnapshotRequest,
    DeleteStreamRequest, DeleteStreamResponse, FlushColdRequest, FlushColdResponse,
    ForkRefResponse, GroupReadStreamBody, GroupReadStreamParts, HeadStreamRequest,
    HeadStreamResponse, PlanColdFlushRequest, PlanGroupColdFlushRequest, PublishSnapshotRequest,
    PublishSnapshotResponse, ReadSnapshotRequest, ReadSnapshotResponse, ReadStreamRequest,
    ReadStreamResponse, StreamAppendCount, TouchStreamAccessResponse,
};
pub use runtime::{RuntimeConfig, RuntimeThreading, ShardRuntime};
pub use snapshot_store::{
    InlineSnapshotStore, SharedSnapshotStore, SnapshotKey, SnapshotLocation, SnapshotPointer,
    SnapshotStore, SnapshotStoreError, SnapshotStoreFuture, default_snapshot_store,
    snapshot_store_from_env,
};
#[cfg(not(madsim))]
pub use snapshot_store::{LocalSnapshotStore, S3SnapshotStore};

pub use ursula_stream::{
    ColdChunkRef, ColdFlushCandidate, ColdGcEntry, ColdGcTarget, ExternalPayloadRef,
    ProducerRequest, StreamErrorCode, StreamIntegritySnapshot,
};

#[cfg(test)]
mod tests;
