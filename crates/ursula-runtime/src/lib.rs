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
pub mod cold_index;
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
pub use cold_index::ColdIndexPage;
pub use cold_index::ColdIndexPageCache;
pub use cold_index::ColdIndexPageKey;
pub use cold_index::ColdIndexPageStore;
pub use cold_index::ColdStoreColdIndexPageStore;
pub use cold_index::InMemoryColdIndexPageStore;
pub use cold_index::cold_index_prefix;
pub use cold_index::write_cold_chunk_index_pages;
pub use cold_index::write_external_segment_index_pages;
pub use cold_store::ColdReadCacheConfig;
pub use cold_store::ColdStore;
pub use cold_store::ColdStoreEvent;
pub use cold_store::ColdStoreFault;
pub use cold_store::ColdStoreFaultContext;
pub use cold_store::ColdStoreFaultEffect;
pub use cold_store::ColdStoreHandle;
pub use cold_store::ColdStoreInfo;
pub use cold_store::ColdStoreOperation;
pub use cold_store::new_cold_chunk_path;
pub use cold_store::new_external_payload_path;
pub use command::GroupSnapshot;
pub use command::GroupWriteCommand;
pub use engine::GroupAckColdGcFuture;
pub use engine::GroupAppendBatchFuture;
pub use engine::GroupAppendBatchResponse;
pub use engine::GroupAppendFuture;
pub use engine::GroupBootstrapStreamFuture;
pub use engine::GroupCloseStreamFuture;
pub use engine::GroupColdHotBacklogFuture;
pub use engine::GroupCreateStreamFuture;
pub use engine::GroupDeleteSnapshotFuture;
pub use engine::GroupDeleteStreamFuture;
pub use engine::GroupEngine;
pub use engine::GroupEngineCreateFuture;
pub use engine::GroupEngineError;
pub use engine::GroupEngineFactory;
pub use engine::GroupEngineMetrics;
pub use engine::GroupFlushColdFuture;
pub use engine::GroupForkRefFuture;
pub use engine::GroupHeadStreamFuture;
pub use engine::GroupInfraError;
pub use engine::GroupInstallSnapshotFuture;
pub use engine::GroupLeaderHint;
pub use engine::GroupPlanColdFlushFuture;
pub use engine::GroupPlanColdGcFuture;
pub use engine::GroupPlanNextColdFlushBatchFuture;
pub use engine::GroupPlanNextColdFlushFuture;
pub use engine::GroupPublishSnapshotFuture;
pub use engine::GroupReadSnapshotFuture;
pub use engine::GroupReadStreamFuture;
pub use engine::GroupReadStreamPartsFuture;
pub use engine::GroupRequireLiveReadOwnerFuture;
pub use engine::GroupShutdownFuture;
pub use engine::GroupSnapshotFuture;
pub use engine::GroupTouchStreamAccessFuture;
pub use engine::GroupWriteBatchFuture;
pub use engine::GroupWriteResponse;
pub use engine::in_memory::InMemoryGroupEngine;
pub use engine::in_memory::InMemoryGroupEngineFactory;
pub use engine::wal::WalGroupEngine;
pub use engine::wal::WalGroupEngineFactory;
pub use error::ErrorStatus;
pub use error::RuntimeError;
pub use metrics::RuntimeMailboxSnapshot;
pub use metrics::RuntimeMetrics;
pub use metrics::RuntimeMetricsSnapshot;
pub use request::AckColdGcResponse;
pub use request::AppendBatchRequest;
pub use request::AppendBatchResponse;
pub use request::AppendExternalRequest;
pub use request::AppendRequest;
pub use request::AppendResponse;
pub use request::BootstrapStreamRequest;
pub use request::BootstrapStreamResponse;
pub use request::BootstrapUpdate;
pub use request::CloseStreamRequest;
pub use request::CloseStreamResponse;
pub use request::ColdHotBacklog;
pub use request::ColdWriteAdmission;
pub use request::CreateStreamExternalRequest;
pub use request::CreateStreamRequest;
pub use request::CreateStreamResponse;
pub use request::DeleteSnapshotRequest;
pub use request::DeleteStreamRequest;
pub use request::DeleteStreamResponse;
pub use request::FlushColdRequest;
pub use request::FlushColdResponse;
pub use request::ForkRefResponse;
pub use request::GroupReadStreamBody;
pub use request::GroupReadStreamParts;
pub use request::HeadStreamRequest;
pub use request::HeadStreamResponse;
pub use request::PlanColdFlushRequest;
pub use request::PlanGroupColdFlushRequest;
pub use request::PublishSnapshotRequest;
pub use request::PublishSnapshotResponse;
pub use request::ReadSnapshotRequest;
pub use request::ReadSnapshotResponse;
pub use request::ReadStreamRequest;
pub use request::ReadStreamResponse;
pub use request::StreamAppendCount;
pub use request::TouchStreamAccessResponse;
pub use runtime::RuntimeConfig;
pub use runtime::RuntimeThreading;
pub use runtime::ShardRuntime;
pub use snapshot_store::InlineSnapshotStore;
#[cfg(not(madsim))]
pub use snapshot_store::LocalSnapshotStore;
#[cfg(not(madsim))]
pub use snapshot_store::S3SnapshotStore;
pub use snapshot_store::SharedSnapshotStore;
pub use snapshot_store::SnapshotKey;
pub use snapshot_store::SnapshotLocation;
pub use snapshot_store::SnapshotPointer;
pub use snapshot_store::SnapshotStore;
pub use snapshot_store::SnapshotStoreError;
pub use snapshot_store::SnapshotStoreFuture;
pub use snapshot_store::default_snapshot_store;
pub use snapshot_store::snapshot_store_from_env;
pub use ursula_stream::ColdChunkRef;
pub use ursula_stream::ColdFlushCandidate;
pub use ursula_stream::ColdGcEntry;
pub use ursula_stream::ColdGcTarget;
pub use ursula_stream::ExternalPayloadRef;
pub use ursula_stream::ProducerRequest;
pub use ursula_stream::StreamErrorCode;
pub use ursula_stream::StreamErrorContext;
pub use ursula_stream::StreamIntegritySnapshot;

#[cfg(test)]
mod tests;
