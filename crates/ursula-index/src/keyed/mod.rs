//! Keyed streams: the `keyed-batch-v1` record format and its fold.
//!
//! A keyed stream is an `application/json; profile=keyed-batch-v1` stream
//! whose every message is a writer-declared batch of key operations (design
//! `docs/architecture/keyed-streams-pi-durable.md` §4.1 and §5.2). Its
//! keyed state `state(D)` is the fold of records `0 .. D`.
//!
//! - [`batch`]: zero-copy validator and parser of one message, shared by the
//!   node's write path and the indexer's ingest.
//! - [`fold`]: the reference model of `state(D)` and of keyed-state range
//!   reads, against which engines and vectors are checked.

pub mod batch;
pub mod fold;

pub use batch::InvalidMessage;
pub use batch::KEYED_BATCH_PROFILE;
pub use batch::KeyError;
pub use batch::KeyedBatch;
pub use batch::KeyedBatchError;
pub use batch::KeyedBatchErrorReason;
pub use batch::KeyedOp;
pub use batch::MAX_KEY_CHARS;
pub use batch::MAX_KEY_OCTETS;
pub use batch::decode_key;
pub use batch::encode_key;
pub use batch::parse_batch;
pub use batch::validate_batch;
pub use batch::validate_messages;
pub use fold::KEYED_STATE_RESPONSE_BUDGET;
pub use fold::KeyedState;
pub use fold::Lower;
pub use fold::RangePage;
pub use fold::RangeQuery;
pub use fold::Row;
pub use fold::row_line;
