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
/// The stream's last accepted `Stream-Seq`, on the `409` of a `Stream-Seq`
/// that is not above it.
pub const STREAM_LAST_SEQ: &str = "stream-last-seq";

/// The stream incarnation token: a request's precondition, and the token
/// that served a response.
pub const STREAM_INCARNATION: &str = "stream-incarnation";
/// The token the stream had before its current history began. HEAD, after a
/// history event.
pub const STREAM_PREVIOUS_INCARNATION: &str = "stream-previous-incarnation";
/// The offset below which the bytes under the current and the previous token
/// are the same. HEAD, after a history event.
pub const STREAM_INHERITED_OFFSET: &str = "stream-inherited-offset";
/// The first offset a response is about: where a receipt's bytes start, and
/// the first byte of an immutable chunk's body, equal to the offset in its
/// path.
pub const STREAM_OFFSET: &str = "stream-offset";
/// Where a receipt's bytes end; the stream's tail on HEAD and on a `409` of
/// a closed stream; the snapshot's offset on a snapshot read, where the
/// reader continues.
pub const STREAM_NEXT_OFFSET: &str = "stream-next-offset";
/// `true` when the stream is closed, or when a receipt's write closed it.
pub const STREAM_CLOSED: &str = "stream-closed";
/// `true` when a read reached the stream's tail.
pub const STREAM_UP_TO_DATE: &str = "stream-up-to-date";
/// The earliest offset the stream retains.
pub const STREAM_RETAINED_OFFSET: &str = "stream-retained-offset";
/// The offset of a snapshot: the latest on HEAD and on a `409`, the served
/// or published one otherwise.
pub const STREAM_SNAPSHOT_OFFSET: &str = "stream-snapshot-offset";
/// The digest of the snapshot at [`STREAM_SNAPSHOT_OFFSET`]; on a parts
/// commit, the digest of the whole snapshot.
pub const STREAM_SNAPSHOT_DIGEST: &str = "stream-snapshot-digest";
/// `true` on an answer of an immutable resource that no later successful
/// answer for the same URL contradicts (extensions Section 4.3).
pub const STREAM_IMMUTABLE: &str = "stream-immutable";
/// The origin that serves a stream's immutable resources at the same paths.
/// HEAD, sent by a deployment, never by an Ursula node.
pub const STREAM_READ_ORIGIN: &str = "stream-read-origin";
/// The id of a staged snapshot part. `201` of a part upload.
pub const STREAM_SNAPSHOT_PART: &str = "stream-snapshot-part";
/// The number of parts a snapshot commit lists. Request.
pub const STREAM_SNAPSHOT_PARTS: &str = "stream-snapshot-parts";

/// The version of the Ursula protocol contract. Every HEAD.
pub const URSULA_PROTOCOL: &str = "ursula-protocol";
/// The deployment that serves the route. Every HEAD.
pub const URSULA_CLUSTER_ID: &str = "ursula-cluster-id";
/// The largest request body any request to the stream may carry. HEAD, and a
/// `413` that applied it.
pub const URSULA_MAX_REQUEST_BYTES: &str = "ursula-max-request-bytes";
/// The largest snapshot the server accepts in parts. HEAD with a cold store,
/// and a `413` that applied it.
pub const URSULA_MAX_SNAPSHOT_BYTES: &str = "ursula-max-snapshot-bytes";

/// The node that leads the stream's Raft group. `307` of a node that does
/// not lead it; the same name as 0.7.
pub const X_URSULA_RAFT_LEADER_ID: &str = "x-ursula-raft-leader-id";
/// The stream's Raft group. `307` of a node that does not lead it, and the
/// `503` of a node that knows no leader.
pub const X_URSULA_RAFT_GROUP: &str = "x-ursula-raft-group";
/// The term of the leader that [`X_URSULA_RAFT_LEADER_ID`] names. `307`.
pub const X_URSULA_RAFT_TERM: &str = "x-ursula-raft-term";

/// The byte range a snapshot read asks for. Request.
pub const RANGE: &str = "range";
/// A `Range` condition. A snapshot read that carries it answers the whole
/// body. Request.
pub const IF_RANGE: &str = "if-range";
/// The bytes a `206` holds, or the body's size on a `416`.
pub const CONTENT_RANGE: &str = "content-range";
/// `bytes` on every snapshot answer: snapshot reads honor `Range`.
pub const ACCEPT_RANGES: &str = "accept-ranges";
