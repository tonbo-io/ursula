//! Lowercase names of the headers this crate parses and renders.

/// The producer's identity. Request.
pub const PRODUCER_ID: &str = "producer-id";
/// The producer's epoch on a request; the producer's current epoch, or the
/// stream's epoch floor, on an answer.
pub const PRODUCER_EPOCH: &str = "producer-epoch";
/// The write's sequence number on a request; the epoch's highest applied
/// sequence number on a `200` or `204`.
pub const PRODUCER_SEQ: &str = "producer-seq";
/// BLAKE3-256 of the request body, 64 lowercase hex digits. Request, optional.
pub const PRODUCER_DIGEST: &str = "producer-digest";
/// The typed outcome of a producer write: an RFC 9651 token with an optional
/// `by` parameter.
pub const PRODUCER_OUTCOME: &str = "producer-outcome";
/// The epoch a claim closed, or the closed epoch an `epoch-closed` names.
pub const PRODUCER_CLOSED_EPOCH: &str = "producer-closed-epoch";
/// The last applied sequence number of [`PRODUCER_CLOSED_EPOCH`].
pub const PRODUCER_CLOSED_SEQ: &str = "producer-closed-seq";
/// The sequence number the producer's epoch expects next.
pub const PRODUCER_EXPECTED_SEQ: &str = "producer-expected-seq";
/// The sequence number a `409` refused.
pub const PRODUCER_RECEIVED_SEQ: &str = "producer-received-seq";
/// The stream incarnation token, a write's precondition and a receipt's
/// token.
pub const STREAM_INCARNATION: &str = "stream-incarnation";
/// Where a receipt's bytes start.
pub const STREAM_OFFSET: &str = "stream-offset";
/// Where a receipt's bytes end.
pub const STREAM_NEXT_OFFSET: &str = "stream-next-offset";
/// `true` when the receipt's write closed the stream.
pub const STREAM_CLOSED: &str = "stream-closed";
