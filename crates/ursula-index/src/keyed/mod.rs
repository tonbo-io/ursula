//! Keyed streams: the `keyed-batch-v1` record format, its fold, and the
//! keyed projection engine's data plane.
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
//! - [`part`]: part v2, the immutable key-sorted Parquet file of a run, with
//!   range tombstones and the verified-read layout in its footer (U13).
//! - [`merge`]: the streaming k-way merge across runs and merged range reads
//!   (U14).
//! - [`run`]: folding records into a run, compaction, and the size-tiered
//!   compaction policy (§9.6).
//! - [`manifest`]: manifest v6, the namespace layout, and publication by
//!   CAS on `CURRENT` (U15).
//! - [`source`]: the [`SourceClient`] trait of the source log and its HTTP
//!   client: P7 pages, incarnation checks (U18, U21).
//! - [`engine`]: on-demand ingest with the continuity check, publication,
//!   compaction, delta GC, the in-process orphan sweep and drain (U16, U20
//!   sweep); its source, object store and clock are injectable, so it runs
//!   under the deterministic simulator (U21).
//! - [`admission`]: the process-wide byte budget, ingest and compaction
//!   slots and the bounded admission queue of the engine.
//! - [`http`]: the internal `/v1/keyed` API (U17) and the metrics endpoint.
//! - [`metrics`]: the engine's counters and gauges (U24).
//! - [`tools`]: namespace maintenance: verify, rebuild, sweep, dump (U20).
//!
//! Engine data flow: a [`RunBuilder`] folds `(record, batch)` pairs into a
//! [`BuiltRun`]; its parts are stored with [`KeyedNamespace::put_part`]; the
//! manifest from [`KeyedManifest::after_ingest`] is published with
//! [`KeyedNamespace::publish`]; reads go through [`read_range`] over the
//! manifest's runs and a [`PartOpener`].

pub mod admission;
pub mod batch;
pub mod engine;
pub mod fold;
pub mod http;
pub mod manifest;
pub mod merge;
pub mod metrics;
pub mod part;
pub mod run;
pub mod source;
pub mod tools;

pub use admission::AdmissionMetrics;
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
pub use engine::KeyedEngine;
pub use engine::KeyedEngineConfig;
pub use engine::KeyedReadOutcome;
pub use engine::KeyedReadRequest;
pub use engine::Selection;
pub use fold::KEYED_STATE_RESPONSE_BUDGET;
pub use fold::KeyedState;
pub use fold::Lower;
pub use fold::RangePage;
pub use fold::RangeQuery;
pub use fold::Row;
pub use fold::row_line;
pub use manifest::KEYED_MANIFEST_VERSION;
pub use manifest::KEYED_PROJECTION_FORMAT;
pub use manifest::KeyedManifest;
pub use manifest::KeyedNamespace;
pub use manifest::KeyedPartMeta;
pub use manifest::KeyedRunMeta;
pub use manifest::KeyedSource;
pub use manifest::PublishOutcome;
pub use manifest::PublishedKeyedManifest;
pub use manifest::record_digest;
pub use merge::KeyedPage;
pub use merge::KeyedRow;
pub use merge::get;
pub use merge::read_range;
pub use part::EncodedPart;
pub use part::KeyedEntry;
pub use part::MemoryParts;
pub use part::PartOpener;
pub use part::PartOptions;
pub use part::RangeTombstone;
pub use part::StorePartOpener;
pub use run::BuiltRun;
pub use run::CompactionOutput;
pub use run::CompactionPolicy;
pub use run::RunBuilder;
pub use run::compact;
pub use run::plan_compaction;
pub use source::IncarnationState;
pub use source::KeyedSourceClient;
pub use source::SourceClient;
pub use source::SourceError;
pub use source::SourcePage;
pub use source::read_response;
