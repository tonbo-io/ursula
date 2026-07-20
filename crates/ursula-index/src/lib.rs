//! Rebuildable client-event-time index for Ursula JSON record streams.
//!
//! Module map:
//!
//! - [`cache`]: disposable local caches for whole parts and verified Parquet
//!   page ranges.
//! - [`catalog`]: dynamic index registrations shared by a worker pool.
//! - [`index`]: the S3-authoritative ingest/flush/query/compact/GC engine.
//! - [`manifest`]: the conditionally published manifest state model.
//! - [`object_store`]: conditional object operations for S3 and local tests.
//! - [`part`]: immutable sorted Parquet parts.
//! - [`source`]: HTTP client for the upstream record stream.
//! - [`store`]: shared event, query, status, configuration, and error types.

mod cache;
mod catalog;
mod index;
mod manifest;
mod object_store;
mod part;
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
pub use object_store::FsObjectStore;
pub use object_store::ObjectStore;
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
