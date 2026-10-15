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
//!   and a snapshot uploaded in parts, with the grammar of the commit body
//!   (Section 2.2);
//! - byte ranges on a snapshot read: the one `Range` it honors and the
//!   `Content-Range` of its `206` or `416` (Section 2.5);
//! - the typed `413`, which names the limit it applied (Section 10);
//! - the routing answers a node sends before it reads the body (Section
//!   5.7);
//! - the `412` of the incarnation precondition and the `409` of a closed
//!   stream;
//! - the JSON body of every error answer, `{"error":"<code>"}`, with its
//!   stable code (Section 11).
//!
//! Not yet typed: the `400` of a `Producer-Digest` mismatch, the answer of a
//! retention request (`Stream-Retained-Offset`), and the immutable-read
//! headers `Stream-Immutable` and `Stream-Read-Origin`. The immutable-reads
//! PR (#567) adds the last two; the write-keys server PR adds the first two.
//!
//! Every value is valid by construction: the value types check their range
//! when built, and the structs whose fields constrain each other keep them
//! private behind checked constructors. So every value renders headers that
//! parse back to it, and rendering cannot fail. Every rendered value is
//! visible ASCII, with inner spaces or tabs only in `Producer-Id`.
//!
//! Field syntax is strict. Offsets ([`Offset`]) are `u64` rendered as 20
//! zero-padded decimal digits and parsed only in that form. Tokens
//! ([`Incarnation`]), limits, counts and every integer of an answer are
//! canonical decimal: no sign, no inner whitespace, no leading zero. Only a
//! request's `Producer-Epoch` and `Producer-Seq`, which base-protocol
//! clients choose, may carry leading zeros. Digests are 64 lowercase hex
//! digits. Flags are exactly `true`.
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
//! - [`value`]: field syntax and the value types: [`Offset`],
//!   [`Incarnation`], [`Epoch`], [`Seq`], [`ProducerId`], [`BodyDigest`]
//!   and [`SnapshotDigest`].
//! - [`request`]: [`ProducerRequestHeaders`], the key, digest and
//!   [`IncarnationPrecondition`] a write carries.
//! - [`answer`]: [`ProducerAnswer`], every typed answer to a producer write.
//! - [`head`]: [`ServerIdentity`] on every HEAD and [`StreamHead`] on a
//!   stream's successful HEAD.
//! - [`snapshot`]: [`SnapshotRead`], [`SnapshotPublishAnswer`], and the parts
//!   upload: [`PartStaged`], [`PartsCommit`] and [`PartsManifest`].
//! - [`range`]: [`ByteRange`], the range a snapshot read honors, and
//!   [`ContentRange`], its answer.
//! - [`limit`]: [`SizeLimit`], the limit a `413` applied, and
//!   [`lowered_request_limit`], the request limit an intermediary forwards.
//! - [`routing`]: [`RoutingAnswer`], the `307` or `503` of a node that does
//!   not lead the stream's group.
//! - [`refusal`]: [`IncarnationMismatch`] (`412`) and
//!   [`StreamClosedConflict`] (`409`).
//! - [`error_body`]: [`ErrorBody`], the body of every error answer, and its
//!   [`ErrorCode`].
//! - [`error`]: the one error, [`HeaderError`].

pub mod answer;
pub mod error;
pub mod error_body;
pub mod head;
pub mod limit;
pub mod names;
pub mod range;
pub mod refusal;
pub mod request;
pub mod routing;
pub mod snapshot;
pub mod value;

mod fields;

#[cfg(test)]
mod proptests;

pub use answer::ClosedBy;
pub use answer::ClosedEpoch;
pub use answer::CurrentEpoch;
pub use answer::Delivery;
pub use answer::EpochClosure;
pub use answer::Outcome;
pub use answer::ProducerAnswer;
pub use answer::Receipt;
pub use error::HeaderError;
pub use error_body::ErrorBody;
pub use error_body::ErrorCode;
pub use head::CLUSTER_ID_MAX_BYTES;
pub use head::ClusterId;
pub use head::Inherited;
pub use head::PROTOCOL_VERSION;
pub use head::ServerIdentity;
pub use head::StreamHead;
pub use limit::SizeLimit;
pub use limit::lowered_request_limit;
pub use range::ACCEPT_RANGES_BYTES;
pub use range::ByteRange;
pub use range::ByteSpan;
pub use range::ContentRange;
pub use refusal::IncarnationMismatch;
pub use refusal::StreamClosedConflict;
pub use request::IncarnationPrecondition;
pub use request::ProducerRequestHeaders;
pub use request::ProducerWrite;
pub use routing::NodeId;
pub use routing::RaftGroup;
pub use routing::RaftTerm;
pub use routing::RoutingAnswer;
pub use snapshot::MAX_MANIFEST_BYTES;
pub use snapshot::MAX_SNAPSHOT_PARTS;
pub use snapshot::PART_ID_MAX_BYTES;
pub use snapshot::PartCount;
pub use snapshot::PartId;
pub use snapshot::PartStaged;
pub use snapshot::PartsCommit;
pub use snapshot::PartsManifest;
pub use snapshot::SnapshotPublishAnswer;
pub use snapshot::SnapshotRead;
pub use snapshot::SnapshotRef;
pub use value::BodyDigest;
pub use value::Epoch;
pub use value::Incarnation;
pub use value::MAX_INCARNATION;
pub use value::MAX_PRODUCER_INTEGER;
pub use value::OFFSET_WIDTH;
pub use value::Offset;
pub use value::PRODUCER_ID_MAX_BYTES;
pub use value::ProducerId;
pub use value::Seq;
pub use value::SnapshotDigest;
