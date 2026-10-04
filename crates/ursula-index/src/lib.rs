//! Rebuildable event-time index for Ursula JSON and NDJSON streams
//! (experimental).
//!
//! Module map:
//!
//! - [`cache`]: disposable local caches for whole parts and verified Parquet
//!   page ranges.
//! - [`catalog`]: dynamic index registrations shared by a worker pool.
//! - [`extract`]: event-time extraction with JSON pointers, wildcards and
//!   units.
//! - [`index`]: the S3-authoritative claim/commit/query/compact/GC engine.
//! - [`manifest`]: the conditionally published manifest state model.
//! - [`object_store`]: conditional object operations for S3 and local tests.
//! - [`part`]: immutable sorted Parquet parts.
//! - [`service`]: command arguments and the long-running indexer service entrypoint.
//! - [`source`]: base-protocol HTTP client and message framing for the source stream.
//! - [`store`]: shared entry, query, status, configuration, and error types.

mod cache;
mod catalog;
mod extract;
mod index;
mod manifest;
mod object_store;
mod part;
pub mod service;
mod source;
mod store;

pub use cache::EventIndexCache;
pub use catalog::IndexCatalog;
pub use catalog::IndexRegistration;
pub use catalog::RetiredNamespace;
pub use catalog::StartPosition;
pub use catalog::validate_stream_url;
pub use extract::Extraction;
pub use extract::Extractor;
pub use extract::ExtractorConfig;
pub use extract::TimeUnit;
pub use index::EventIndex;
pub use manifest::GarbageCollectionReport;
pub use manifest::SegmentLease;
pub use object_store::FsObjectStore;
pub use object_store::ObjectStore;
pub use object_store::S3ObjectStore;
pub use object_store::S3ObjectStoreConfig;
pub use source::MAX_MESSAGE_BYTES;
pub use source::ReadLimits;
pub use source::SegmentRead;
pub use source::SourceClient;
pub use source::SourceFormat;
pub use source::SourceHead;
pub use source::SourceRead;
pub use store::Coverage;
pub use store::EventEntry;
pub use store::EventIndexConfig;
pub use store::IndexBase;
pub use store::IndexError;
pub use store::IndexStatus;
pub use store::MatchMode;
pub use store::QueryCursor;
pub use store::QueryRequest;
pub use store::QueryResult;
pub use store::Segment;
pub use store::Skip;
pub use store::SkipCounts;
pub use store::SkipKind;
pub use store::SourceBinding;
