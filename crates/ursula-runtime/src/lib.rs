//! Per-core actor runtime for Ursula.
//!
//! Module map:
//!
//! - [`cold_store`]: opendal-backed cold tier handle and object path helpers.
//! - [`cold_index`]: cold-index pages (binary format, stores, cache), their
//!   writes with the clip rule and rollback, and leader-side page repair.
//! - [`cold_references`]: the cold objects a stream snapshot references and
//!   whether a cold store holds them, checked before a backup restore.
//! - [`cold_refs`]: value types of the shared pack-reference compaction driver
//!   (F2), the cold orphan sweep (F14h) and the external-locator offload
//!   (F5), and object-name age parsing.
//! - [`cold_worker`]: background flush, compaction, GC, page-repair,
//!   orphan-sweep and external-locator offload loops.
//! - [`request`]: HTTP/gRPC request and response value types for each engine op.
//! - [`command`]: the replicated [`GroupWriteCommand`] envelope around the
//!   canonical [`ursula_stream::StreamCommand`], plus `From` conversions from
//!   request values into that command.
//! - [`error`]: runtime-level error type [`RuntimeError`].
//! - [`engine`]: the `GroupEngine` trait, factory, metrics, and the boxed-future
//!   type aliases that form the replaceable per-group engine boundary, plus the
//!   in-memory implementation under [`engine::in_memory`].
//! - [`runtime`]: `ShardRuntime`, `RuntimeConfig`, and per-core worker spawn.
//! - [`core_worker`]: single-thread actor that owns groups for one core.
//! - [`group_actor`]: per-group mailbox actor running inside a core worker.
//! - [`ops`]: declarative manifest of the uniform runtime operations; expands
//!   into the per-operation actor and client plumbing.
//! - [`metrics`]: runtime metrics shared across cores; lock-free counters.
//! - [`read_index`]: the per-group ReadIndex barrier that linearizes reads
//!   before they are queued to the group actor (D10).
//! - [`tidy_worker`]: leader-side `TidyStream` driver (bounded-state F0).

mod admission;
pub mod cold_index;
mod cold_references;
mod cold_refs;
mod cold_store;
pub mod cold_worker;
mod command;
mod core_worker;
mod engine;

mod error;
pub mod format_marker;
mod group_actor;
mod metrics;
mod ops;
mod read_index;
mod request;
mod retention_gc;
mod rt;
mod runtime;
mod s3_sse;
mod snapshot_store;
pub mod tidy_worker;
mod trace;

pub use admission::RaftUncommittedAdmission;
pub use cold_index::ColdIndexPage;
pub use cold_index::ColdIndexPageCache;
pub use cold_index::ColdIndexPageKey;
pub use cold_index::ColdIndexPageStore;
pub use cold_index::ColdIndexRepairInput;
pub use cold_index::ColdIndexRepairReport;
pub use cold_index::ColdStoreColdIndexPageStore;
pub use cold_index::InMemoryColdIndexPageStore;
pub use cold_index::RepairColdIndexRequest;
pub use cold_index::RepairColdIndexResponse;
pub use cold_index::clipped_entries;
pub use cold_index::cold_index_generation_dir;
pub use cold_index::cold_index_prefix;
pub use cold_index::load_cold_chunks_from_pages;
pub use cold_index::repair_cold_index_streams;
pub use cold_index::replace_cold_chunk_index_pages_with_rollback_in_generation;
pub use cold_index::rollback_cold_index_pages;
pub use cold_index::select_cold_chunk_compaction;
pub use cold_index::write_cold_chunk_index_pages_in_generation;
pub use cold_index::write_cold_chunk_index_pages_with_rollback_in_generation;
pub use cold_index::write_proven_external_index_pages;
pub use cold_references::ColdReferenceError;
pub use cold_references::ColdReferenceReport;
pub use cold_references::check_cold_references;
pub use cold_refs::ColdOrphanSweepPlan;
pub use cold_refs::ColdOrphanSweepReport;
pub use cold_refs::ColdOrphanSweepRequest;
pub use cold_refs::ColdOrphanSweepStream;
pub use cold_refs::OffloadColdRefsRequest;
pub use cold_refs::OffloadColdRefsResponse;
pub use cold_refs::OffloadStreamColdRefsResponse;
pub use cold_refs::SharedRefCompactionConfig;
pub use cold_refs::SharedRefCompactionReport;
pub use cold_refs::cold_object_written_unix_ms;
pub use cold_store::COLD_OBJECT_WRITE_PART_BYTES;
pub use cold_store::ColdObjectWriter;
pub use cold_store::ColdReadCacheParams;
pub use cold_store::ColdStore;
pub use cold_store::ColdStoreEvent;
pub use cold_store::ColdStoreFault;
pub use cold_store::ColdStoreFaultContext;
pub use cold_store::ColdStoreFaultEffect;
pub use cold_store::ColdStoreHandle;
pub use cold_store::ColdStoreInfo;
pub use cold_store::ColdStoreOperation;
pub use cold_store::cold_bucket_prefix;
pub use cold_store::cold_chunk_dir;
pub use cold_store::cold_external_dir;
pub use cold_store::cold_pack_dir;
pub use cold_store::new_cold_chunk_path_in_generation;
pub use cold_store::new_cold_pack_path;
pub use cold_store::new_external_payload_path;
pub use cold_worker::spawn_cold_compaction_worker_if_configured;
pub use cold_worker::spawn_cold_flush_worker_if_configured;
pub use cold_worker::spawn_cold_gc_worker_if_configured;
pub use cold_worker::spawn_cold_index_repair_worker;
pub use cold_worker::spawn_cold_orphan_sweep_worker;
pub use cold_worker::spawn_cold_ref_offload_worker;
pub use command::GroupSnapshot;
pub use command::GroupWriteCommand;
pub use engine::GroupAckColdGcFuture;
pub use engine::GroupAdvanceRetentionFuture;
pub use engine::GroupAppendBatchFuture;
pub use engine::GroupAppendFuture;
pub use engine::GroupBootstrapStreamFuture;
pub use engine::GroupBucketUsageFuture;
pub use engine::GroupCloseStreamFuture;
pub use engine::GroupColdHotBacklogFuture;
pub use engine::GroupCompactColdFuture;
pub use engine::GroupCreateStreamFuture;
pub use engine::GroupDeferColdGcFuture;
pub use engine::GroupDeleteStreamFuture;
pub use engine::GroupEngine;
pub use engine::GroupEngineCreateFuture;
pub use engine::GroupEngineError;
pub use engine::GroupEngineFactory;
pub use engine::GroupEngineMetrics;
pub use engine::GroupFlushColdFuture;
pub use engine::GroupHeadStreamFuture;
pub use engine::GroupImportGroupStateFuture;
pub use engine::GroupInfraError;
pub use engine::GroupInstallSnapshotFuture;
pub use engine::GroupLeaderHint;
pub use engine::GroupLeaderReadFuture;
pub use engine::GroupOffloadColdRefsFuture;
pub use engine::GroupOpenLiveReadFuture;
pub use engine::GroupPlanColdFlushFuture;
pub use engine::GroupPlanColdGcFuture;
pub use engine::GroupPlanColdOrphanSweepFuture;
pub use engine::GroupPlanNextColdFlushBatchFuture;
pub use engine::GroupPlanSharedRefCompactionFuture;
pub use engine::GroupPublishSnapshotFuture;
pub use engine::GroupPurgeBucketFuture;
pub use engine::GroupReadRoute;
pub use engine::GroupReadSnapshotFuture;
pub use engine::GroupReadStreamFuture;
pub use engine::GroupReadStreamPartsFuture;
pub use engine::GroupRepairColdIndexFuture;
pub use engine::GroupRouteHeadStreamFuture;
pub use engine::GroupRouteReadStreamFuture;
pub use engine::GroupShutdownFuture;
pub use engine::GroupSnapshotFuture;
pub use engine::GroupStateGaugesFuture;
pub use engine::GroupTidyStreamFuture;
pub use engine::GroupTidyStreamsFuture;
pub use engine::GroupTouchStreamAccessFuture;
pub use engine::GroupWriteResponse;
pub use engine::in_memory::InMemoryGroupEngine;
pub use engine::in_memory::InMemoryGroupEngineFactory;
pub use engine::in_memory::next_repair_cursor;
pub use engine::in_memory::repair_cold_index_response;
pub use error::ErrorStatus;
pub use error::RuntimeError;
pub use metrics::RuntimeMailboxSnapshot;
pub use metrics::RuntimeMetrics;
pub use metrics::RuntimeMetricsSnapshot;
pub use read_index::LinearizableReadBarrier;
pub use read_index::ReadIndexFuture;
pub use request::AckColdGcResponse;
pub use request::AdvanceRetentionRequest;
pub use request::AdvanceRetentionResponse;
pub use request::AppendExternalRequest;
pub use request::AppendRequest;
pub use request::AppendResponse;
pub use request::BootstrapStreamRequest;
pub use request::BootstrapStreamResponse;
pub use request::BootstrapUpdate;
pub use request::CloseStreamRequest;
pub use request::CloseStreamResponse;
pub use request::ColdHotBacklog;
pub use request::ColdSnapshotBody;
pub use request::ColdWriteAdmission;
pub use request::CompactColdRequest;
pub use request::CompactColdResponse;
pub use request::CreateStreamExternalRequest;
pub use request::CreateStreamRequest;
pub use request::CreateStreamResponse;
pub use request::DeferColdGcResponse;
pub use request::DeleteStreamRequest;
pub use request::DeleteStreamResponse;
pub use request::FlushColdRequest;
pub use request::FlushColdResponse;
pub use request::GroupReadStreamBody;
pub use request::GroupReadStreamParts;
pub use request::HeadStreamRequest;
pub use request::HeadStreamResponse;
pub use request::ImportGroupStateRequest;
pub use request::ImportGroupStateResponse;
pub use request::LiveReadOwner;
pub use request::PlanColdFlushRequest;
pub use request::PlanGroupColdFlushRequest;
pub use request::PublishSnapshotRequest;
pub use request::PublishSnapshotResponse;
pub use request::PurgeBucketResponse;
pub use request::ReadSnapshotRequest;
pub use request::ReadSnapshotResponse;
pub use request::ReadStreamRequest;
pub use request::ReadStreamResponse;
pub use request::StreamAppendCount;
pub use request::TidyStreamResponse;
pub use request::TidyStreamsRequest;
pub use request::TidyStreamsResponse;
pub use request::TouchStreamAccessResponse;
pub use request::WriteHotBacklog;
pub use retention_gc::RetentionGcReport;
pub use retention_gc::RetentionGcTarget;
pub use retention_gc::RetentionGcTracker;
pub use retention_gc::collect_retained_cold_objects;
pub use runtime::COLD_ORPHAN_SWEEP_GRACE_MS;
pub use runtime::ColdIndexRepairStep;
pub use runtime::EXCLUSIVE_FLUSH_MIN_BYTES;
pub use runtime::PurgeBucketReport;
pub use runtime::RuntimeConfig;
pub use runtime::RuntimeThreading;
pub use runtime::ShardRuntime;
pub use snapshot_store::InlineSnapshotStore;
#[cfg(not(madsim))]
pub use snapshot_store::S3SnapshotStore;
pub use snapshot_store::SharedSnapshotStore;
pub use snapshot_store::SnapshotBytesIterator;
pub use snapshot_store::SnapshotCompression;
pub use snapshot_store::SnapshotKey;
pub use snapshot_store::SnapshotLocation;
pub use snapshot_store::SnapshotPointer;
pub use snapshot_store::SnapshotReferenceConfig;
pub use snapshot_store::SnapshotStore;
pub use snapshot_store::SnapshotStoreError;
pub use snapshot_store::SnapshotStoreFuture;
pub use snapshot_store::decode_snapshot_envelope;
pub use snapshot_store::default_snapshot_store;
pub use snapshot_store::encode_binary_envelope;
pub use snapshot_store::resolved_snapshot_backend;
pub use snapshot_store::snapshot_store_from_config;
pub use ursula_config::config::ColdConfig;
pub use ursula_stream::COMMITTED_WRITE_UNIT_BYTES;
pub use ursula_stream::ColdChunkRef;
pub use ursula_stream::ColdFlushCandidate;
pub use ursula_stream::ColdFlushPressure;
pub use ursula_stream::ColdGcEntry;
pub use ursula_stream::ColdGcPlanEntry;
pub use ursula_stream::ColdGcTarget;
pub use ursula_stream::ExternalPayloadRef;
pub use ursula_stream::FORMAT_EPOCH;
pub use ursula_stream::MAX_COLD_SNAPSHOT_BYTES;
pub use ursula_stream::ProducerRequest;
pub use ursula_stream::SnapshotDigest;
pub use ursula_stream::StreamErrorCode;
pub use ursula_stream::StreamErrorContext;
pub use ursula_stream::StreamSnapshot;
pub use ursula_stream::validate_bucket_id;

#[cfg(test)]
mod cold_drivers_tests;
#[cfg(all(test, not(madsim)))]
mod cold_gc_hygiene_tests;
#[cfg(test)]
mod cold_page_repair_tests;
#[cfg(test)]
mod external_offload_tests;
#[cfg(test)]
mod incarnation_gc_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tidy_driver_tests;

pub use metrics::WalJournalSample;
pub use metrics::WalMemorySample;
pub use metrics::WalReadSample;
pub use metrics::WalStorageSample;
