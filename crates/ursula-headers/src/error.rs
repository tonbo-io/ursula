//! The one error of parsing Ursula's headers, the parts manifest and error
//! bodies, and of constructing a value that would not round-trip.

use std::error::Error;
use std::fmt;

use crate::names;
use crate::snapshot::PART_ID_MAX_BYTES;
use crate::snapshot::PartCount;
use crate::value::Epoch;
use crate::value::OFFSET_WIDTH;
use crate::value::Offset;
use crate::value::PRODUCER_ID_MAX_BYTES;
use crate::value::Seq;

/// A header that is malformed, missing where the rest requires it, or
/// repeated. A server answers each in a request with `400 Bad Request`; a
/// client treats one in a response as a protocol violation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeaderError {
    /// The header appears more than once.
    Duplicate { name: &'static str },
    /// The value holds bytes outside visible ASCII.
    NotText { name: &'static str },
    /// The other headers, or the status, require this one.
    Missing { name: &'static str },
    /// Some, but not all, of `Producer-Id`, `Producer-Epoch` and
    /// `Producer-Seq`.
    PartialProducer,
    /// `Producer-Id` is empty.
    EmptyProducerId,
    /// `Producer-Id` is longer than [`PRODUCER_ID_MAX_BYTES`].
    ProducerIdTooLong { len: usize },
    /// `Producer-Id` holds bytes outside visible ASCII, spaces and tabs, or
    /// starts or ends with whitespace.
    InvalidProducerId,
    /// Not a decimal integer: empty, signed, spaced or not digits.
    InvalidInteger { name: &'static str },
    /// A decimal integer above `u64::MAX`.
    IntegerTooLarge { name: &'static str },
    /// An integer outside the range of its header: a producer integer or a
    /// token above 2^53 - 1, a token of `0`, a part count outside 1 to
    /// [`MAX_SNAPSHOT_PARTS`](crate::snapshot::MAX_SNAPSHOT_PARTS), a Raft group above `u32::MAX`.
    OutOfRange { name: &'static str, value: u64 },
    /// An integer of an answer, or a token, limit or count, with a leading
    /// zero.
    NonCanonicalInteger { name: &'static str },
    /// An offset that is not exactly [`OFFSET_WIDTH`] decimal digits.
    InvalidOffset { name: &'static str },
    /// A digest that is not 64 lowercase hex digits.
    InvalidDigest { name: &'static str },
    /// `Producer-Digest` without the producer headers.
    DigestWithoutProducer,
    /// `Producer-Outcome` names no known outcome, or `epoch-closed` lacks its
    /// `by` parameter.
    InvalidOutcome,
    /// A flag (`Stream-Closed`, `Stream-Up-To-Date`) is present and not
    /// `true`.
    InvalidFlag { name: &'static str },
    /// A `400` names another expected sequence number than `0`.
    UnexpectedExpectedSeq { expected: Seq },
    /// `Producer-Outcome` does not fit the answer's status.
    OutcomeStatusMismatch { status: u16 },
    /// `Ursula-Cluster-Id` is not 1 to 64 bytes of `[A-Za-z0-9._-]`.
    InvalidClusterId,
    /// `Stream-Snapshot-Part` is not 1 to [`PART_ID_MAX_BYTES`] visible
    /// ASCII characters.
    InvalidPartId,
    /// A `413` names both limits; it must name the one it applied.
    AmbiguousLimit,
    /// A receipt starts after it ends.
    ReceiptRangeInverted { start: Offset, next: Offset },
    /// A claim closed an epoch that is not below the producer's current
    /// `epoch`.
    ClosedEpochNotEarlier { epoch: Epoch, closed: Epoch },
    /// An `epoch-closed;by=history` closed another epoch than the current
    /// one.
    HistoryClosedOtherEpoch { epoch: Epoch, closed: Epoch },
    /// HEAD's `Stream-Retained-Offset` is above `Stream-Next-Offset`.
    RetainedPastNext { retained: Offset, next: Offset },
    /// HEAD's `Stream-Inherited-Offset` is above `Stream-Next-Offset`.
    InheritedPastNext { inherited: Offset, next: Offset },
    /// HEAD's `Stream-Previous-Incarnation` is the current token.
    PreviousIncarnationIsCurrent,
    /// A snapshot read's `Stream-Next-Offset` is not its snapshot's offset.
    SnapshotNextOffsetMismatch { snapshot: Offset, next: Offset },
    /// A parts manifest does not end with LF.
    ManifestUnterminated,
    /// Line `line` (from 1) of a parts manifest is not a part id.
    InvalidManifestLine { line: usize },
    /// Line `line` (from 1) of a parts manifest repeats an earlier id.
    DuplicateManifestPart { line: usize },
    /// A parts manifest lists another number of parts than its commit's
    /// `Stream-Snapshot-Parts`.
    ManifestPartCount { declared: PartCount },
    /// A decimal that must fit in a `u32` does not.
    U32TooLarge { name: &'static str },
    /// A producer header that the answer's status and outcome do not use,
    /// such as `Producer-Seq` on a `403`.
    UnexpectedHeader { name: &'static str, status: u16 },
    /// The latest snapshot's offset is above the stream's tail.
    SnapshotPastNext { snapshot: Offset, next: Offset },
    /// `Content-Range` is not `bytes first-last/total` with `first <= last <
    /// total` on a `206`, or `bytes */total` on a `416`.
    InvalidContentRange,
    /// An error answer's body is not a JSON object whose `error` member
    /// holds the strings `code` and `message`.
    InvalidErrorBody,
    /// `Stream-Last-Seq` is not 1 to 256 bytes of visible ASCII with inner
    /// spaces or tabs.
    InvalidStreamSeq,
}

impl fmt::Display for HeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Duplicate { name } => write!(f, "{name} must appear at most once"),
            Self::NotText { name } => write!(f, "{name} must be visible ASCII"),
            Self::Missing { name } => write!(f, "{name} is required here"),
            Self::PartialProducer => write!(
                f,
                "{}, {} and {} must be provided together",
                names::PRODUCER_ID,
                names::PRODUCER_EPOCH,
                names::PRODUCER_SEQ
            ),
            Self::EmptyProducerId => write!(f, "{} must not be empty", names::PRODUCER_ID),
            Self::ProducerIdTooLong { len } => write!(
                f,
                "{} must be at most {PRODUCER_ID_MAX_BYTES} bytes, got {len}",
                names::PRODUCER_ID
            ),
            Self::InvalidInteger { name } => write!(f, "{name} must be a non-negative integer"),
            Self::InvalidProducerId => write!(
                f,
                "{} must be visible ASCII with no surrounding whitespace",
                names::PRODUCER_ID
            ),
            Self::IntegerTooLarge { name } => write!(f, "{name} does not fit in 64 bits"),
            Self::OutOfRange { name, value } => write!(f, "{name} is out of range: {value}"),
            Self::NonCanonicalInteger { name } => {
                write!(f, "{name} must not have a leading zero")
            }
            Self::InvalidOffset { name } => {
                write!(
                    f,
                    "{name} must be an offset of {OFFSET_WIDTH} decimal digits"
                )
            }
            Self::InvalidDigest { name } => write!(f, "{name} must be 64 lowercase hex digits"),
            Self::DigestWithoutProducer => write!(
                f,
                "{} requires the producer headers",
                names::PRODUCER_DIGEST
            ),
            Self::InvalidOutcome => write!(f, "{} is not a known outcome", names::PRODUCER_OUTCOME),
            Self::InvalidFlag { name } => write!(f, "{name} must be `true`"),
            Self::UnexpectedExpectedSeq { expected } => {
                write!(f, "a 400 producer answer expects seq 0, not {expected}")
            }
            Self::OutcomeStatusMismatch { status } => write!(
                f,
                "{} does not fit status {status}",
                names::PRODUCER_OUTCOME
            ),
            Self::U32TooLarge { name } => write!(f, "{name} does not fit in a u32"),
            Self::UnexpectedHeader { name, status } => {
                write!(f, "{name} does not belong on a {status} producer answer")
            }
            Self::SnapshotPastNext { snapshot, next } => write!(
                f,
                "{} {snapshot} is above {} {next}",
                names::STREAM_SNAPSHOT_OFFSET,
                names::STREAM_NEXT_OFFSET
            ),
            Self::InvalidClusterId => write!(
                f,
                "{} must be 1 to 64 characters from A-Z, a-z, 0-9, `.`, `_` and `-`",
                names::URSULA_CLUSTER_ID
            ),
            Self::InvalidPartId => write!(
                f,
                "{} must be 1 to {PART_ID_MAX_BYTES} visible ASCII characters",
                names::STREAM_SNAPSHOT_PART
            ),
            Self::AmbiguousLimit => write!(
                f,
                "a 413 must name one limit, not both {} and {}",
                names::URSULA_MAX_REQUEST_BYTES,
                names::URSULA_MAX_SNAPSHOT_BYTES
            ),
            Self::ReceiptRangeInverted { start, next } => write!(
                f,
                "{} {start} is above {} {next}",
                names::STREAM_OFFSET,
                names::STREAM_NEXT_OFFSET
            ),
            Self::ClosedEpochNotEarlier { epoch, closed } => write!(
                f,
                "{} {closed} is not below {} {epoch}",
                names::PRODUCER_CLOSED_EPOCH,
                names::PRODUCER_EPOCH
            ),
            Self::HistoryClosedOtherEpoch { epoch, closed } => write!(
                f,
                "a history event closes the current epoch {epoch}, not {closed}"
            ),
            Self::RetainedPastNext { retained, next } => write!(
                f,
                "{} {retained} is above {} {next}",
                names::STREAM_RETAINED_OFFSET,
                names::STREAM_NEXT_OFFSET
            ),
            Self::InheritedPastNext { inherited, next } => write!(
                f,
                "{} {inherited} is above {} {next}",
                names::STREAM_INHERITED_OFFSET,
                names::STREAM_NEXT_OFFSET
            ),
            Self::PreviousIncarnationIsCurrent => write!(
                f,
                "{} must differ from {}",
                names::STREAM_PREVIOUS_INCARNATION,
                names::STREAM_INCARNATION
            ),
            Self::SnapshotNextOffsetMismatch { snapshot, next } => write!(
                f,
                "a snapshot read's {} {next} must equal its {} {snapshot}",
                names::STREAM_NEXT_OFFSET,
                names::STREAM_SNAPSHOT_OFFSET
            ),
            Self::ManifestUnterminated => write!(f, "a parts manifest must end with LF"),
            Self::InvalidManifestLine { line } => write!(
                f,
                "line {line} of a parts manifest is not a part id of 1 to {PART_ID_MAX_BYTES} \
                 visible ASCII characters"
            ),
            Self::InvalidContentRange => write!(
                f,
                "{} must be `bytes first-last/total` within the body on a 206, or \
                 `bytes */total` on a 416",
                names::CONTENT_RANGE
            ),
            Self::InvalidStreamSeq => write!(
                f,
                "{} must be 1 to 256 bytes of visible ASCII",
                names::STREAM_LAST_SEQ
            ),
            Self::InvalidErrorBody => write!(
                f,
                r#"an error body must be {{"error":{{"code":"<code>","message":"<message>"}}}}"#
            ),
            Self::DuplicateManifestPart { line } => {
                write!(
                    f,
                    "line {line} of a parts manifest repeats an earlier part id"
                )
            }
            Self::ManifestPartCount { declared } => write!(
                f,
                "a parts manifest must list exactly {declared} parts, as {} says",
                names::STREAM_SNAPSHOT_PARTS
            ),
        }
    }
}

impl Error for HeaderError {}
