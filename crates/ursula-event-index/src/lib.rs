//! Rebuildable client-event-time index for Ursula JSON record streams.
//!
//! Module map:
//!
//! - [`part`]: immutable sorted Parquet parts.
//! - [`object_store`]: conditional object operations for S3 and local tests.
//! - [`serverless`]: S3-authoritative index with a disposable local cache.
//! - [`store`]: crash-consistent manifest, checkpoint, query, and compaction.

mod object_store;
mod part;
mod serverless;
mod source;
mod store;

pub use object_store::FsObjectStore;
pub use object_store::S3ObjectStore;
pub use object_store::S3ObjectStoreConfig;
pub use serverless::ServerlessEventIndex;
pub use source::SourceBatch;
pub use source::SourceClient;
pub use store::EventEntry;
pub use store::EventIndexConfig;
pub use store::IndexError;
pub use store::IndexStatus;
pub use store::LocalEventIndex;
pub use store::QueryCursor;
pub use store::QueryResult;
pub use store::SourceEnvelope;
