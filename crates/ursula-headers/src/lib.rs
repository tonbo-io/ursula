//! Typed HTTP headers of the Ursula protocol (version 2).
//!
//! The server renders its headers with this crate and its clients (the
//! gateway, `clients/sqlite-vfs`) parse them with it, so the two sides share
//! one implementation and cannot drift. It covers:
//!
//! - the write-key headers of a producer request and every typed answer to
//!   it (extensions Section 5);
//! - HEAD metadata: the server's identity (`Ursula-Protocol`,
//!   `Ursula-Cluster-Id`), sent on every HEAD answer, `404` included, and a
//!   stream's metadata (`Stream-Incarnation`, `Stream-Next-Offset`,
//!   `Stream-Retained-Offset`, `Stream-Closed`, the latest snapshot, the
//!   request and snapshot limits, and what the current history inherited
//!   after a history event) (Sections 2.4 and 10);
//! - snapshot headers: the answers of a snapshot read and a snapshot publish,
//!   and the headers of a snapshot uploaded in parts (Section 2.2);
//! - the typed `413`, which names the limit it applied (Section 10).
//!
//! Field syntax is strict. Offsets are `u64` rendered as 20 zero-padded
//! decimal digits and parsed only in that form. Tokens, limits and counts
//! are canonical decimal `u64`: no sign, no inner whitespace, no leading
//! zero. Digests are 64 lowercase hex digits.
//!
//! The crate uses the standard library only, so a standalone client
//! workspace can depend on it by path. Headers come in as `(name, value)`
//! pairs, with names compared case-insensitively and values as raw bytes,
//! which is what every HTTP library's header map iterates as. They go out
//! through a callback that receives a lowercase name and a value.
//!
//! Module map:
//!
//! - [`names`]: the lowercase header names.
//! - [`value`]: field syntax: [`Digest`], producer integers, offsets, tokens
//!   and canonical decimals.
//! - [`request`]: [`ProducerRequestHeaders`], the key, digest and token a
//!   write carries.
//! - [`answer`]: [`ProducerAnswer`], every typed answer to a producer write.
//! - [`head`]: [`ServerIdentity`] on every HEAD and [`StreamHead`] on a
//!   stream's successful HEAD.
//! - [`snapshot`]: [`SnapshotRead`], [`SnapshotPublishAnswer`], and the parts
//!   upload: [`PartStaged`] and [`PartsCommit`].
//! - [`limit`]: [`SizeLimit`], the limit a `413` applied.
//! - [`error`]: the one parse error, [`HeaderError`].

pub mod answer;
pub mod error;
pub mod head;
pub mod limit;
pub mod names;
pub mod request;
pub mod snapshot;
pub mod value;

mod fields;

pub use answer::ClosedBy;
pub use answer::ClosedEpoch;
pub use answer::Delivery;
pub use answer::Outcome;
pub use answer::ProducerAnswer;
pub use answer::Receipt;
pub use error::HeaderError;
pub use head::CLUSTER_ID_MAX_BYTES;
pub use head::ClusterId;
pub use head::Inherited;
pub use head::PROTOCOL_VERSION;
pub use head::ServerIdentity;
pub use head::StreamHead;
pub use limit::SizeLimit;
pub use request::ProducerRequestHeaders;
pub use request::ProducerWrite;
pub use snapshot::MAX_SNAPSHOT_PARTS;
pub use snapshot::PART_ID_MAX_BYTES;
pub use snapshot::PartId;
pub use snapshot::PartStaged;
pub use snapshot::PartsCommit;
pub use snapshot::SnapshotPublishAnswer;
pub use snapshot::SnapshotRead;
pub use snapshot::SnapshotRef;
pub use value::Digest;
pub use value::MAX_PRODUCER_INTEGER;
pub use value::OFFSET_WIDTH;
pub use value::PRODUCER_ID_MAX_BYTES;
