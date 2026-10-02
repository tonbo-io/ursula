//! Rebuildable client-event-time index for Ursula JSON record streams.
//!
//! Module map:
//!
//! - [`cache`]: disposable local caches for whole parts and verified Parquet
//!   page ranges.
//! - [`catalog`]: dynamic index registrations shared by a worker pool.
//! - [`clock`]: the injectable wall clock of the keyed engine and the
//!   in-memory store (simulation, design U21).
//! - [`index`]: the S3-authoritative ingest/flush/query/compact/GC engine.
//! - [`keyed`]: the `keyed-batch-v1` record format (validator and parser),
//!   the reference fold of keyed state, and the keyed projection engine's
//!   data plane (part v2, k-way merge, runs and compaction, manifest v6),
//!   and the keyed engine service (ingest, publish, compaction, GC, the
//!   internal `/v1/keyed` API).
//! - [`manifest`]: the conditionally published manifest state model.
//! - [`memory_store`]: the in-memory conditional object store with
//!   deterministic fault hooks (latency, failures, CAS conflicts, ambiguous
//!   writes) for the indexer simulation.
//! - [`object_store`]: conditional object operations for S3 and local tests,
//!   with request counting by S3 class and fault injection for crash tests.
//! - [`part`]: immutable sorted Parquet parts.
//! - `rt` (private): the task and timer seam, madsim under `cfg(madsim)`.
//! - [`service`]: command arguments and the long-running indexer service entrypoint.
//! - [`source`]: HTTP client for the upstream record stream.
//! - [`store`]: shared event, query, status, configuration, and error types.

mod cache;
mod catalog;
pub mod clock;
mod index;
pub mod keyed;
mod manifest;
pub mod memory_store;
mod object_store;
mod part;
mod rt;
pub mod service;
mod source;
mod store;

pub use cache::EventIndexCache;
pub use catalog::IndexCatalog;
pub use catalog::IndexRegistration;
pub use catalog::validate_stream_url;
pub use index::EventIndex;
pub use manifest::CompletedRecordRange;
pub use manifest::GarbageCollectionReport;
pub use manifest::RecordSegmentLease;
pub use memory_store::MemoryObjectStore;
pub use object_store::FsObjectStore;
pub use object_store::ObjectFaults;
pub use object_store::ObjectOp;
pub use object_store::ObjectRequestCounters;
pub use object_store::ObjectRequestCounts;
pub use object_store::ObjectStore;
pub use object_store::ObservedStore;
pub use object_store::S3ObjectStore;
pub use object_store::S3ObjectStoreConfig;
pub use source::SourceBatch;
pub use source::SourceClient;
pub use source::SourceRecordRange;
pub use store::EventEntry;
pub use store::EventIndexConfig;
pub use store::IndexError;
pub use store::IndexStatus;
pub use store::QueryCursor;
pub use store::QueryResult;
pub use store::SourceEnvelope;
