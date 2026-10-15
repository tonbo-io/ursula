//! The one error of parsing Ursula's headers.

use std::error::Error;
use std::fmt;

use crate::names;
use crate::value::MAX_PRODUCER_INTEGER;
use crate::value::OFFSET_WIDTH;
use crate::value::PRODUCER_ID_MAX_BYTES;

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
    /// Not a decimal integer: empty, signed, spaced or not digits.
    InvalidInteger { name: &'static str },
    /// A producer integer above [`MAX_PRODUCER_INTEGER`], or another integer
    /// above `u64::MAX`.
    IntegerTooLarge { name: &'static str },
    /// A token, limit or count with a leading zero.
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
    UnexpectedExpectedSeq { expected: u64 },
    /// `Producer-Outcome` does not fit the answer's status.
    OutcomeStatusMismatch { status: u16 },
    /// `Ursula-Cluster-Id` is not 1 to 64 bytes of `[A-Za-z0-9._-]`.
    InvalidClusterId,
    /// `Stream-Snapshot-Part` is not 1 to 64 visible ASCII characters.
    InvalidPartId,
    /// `Stream-Snapshot-Parts` is `0` or above the server's part limit.
    PartCountOutOfRange { count: u64 },
    /// A `413` names both limits; it must name the one it applied.
    AmbiguousLimit,
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
            Self::IntegerTooLarge { name } => write!(
                f,
                "{name} is out of range (producer integers are <= {MAX_PRODUCER_INTEGER})"
            ),
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
            Self::InvalidClusterId => write!(
                f,
                "{} must be 1 to 64 characters from A-Z, a-z, 0-9, `.`, `_` and `-`",
                names::URSULA_CLUSTER_ID
            ),
            Self::InvalidPartId => write!(
                f,
                "{} must be 1 to 64 visible ASCII characters",
                names::STREAM_SNAPSHOT_PART
            ),
            Self::PartCountOutOfRange { count } => write!(
                f,
                "{} must be between 1 and {}, got {count}",
                names::STREAM_SNAPSHOT_PARTS,
                crate::snapshot::MAX_SNAPSHOT_PARTS
            ),
            Self::AmbiguousLimit => write!(
                f,
                "a 413 must name one limit, not both {} and {}",
                names::URSULA_MAX_REQUEST_BYTES,
                names::URSULA_MAX_SNAPSHOT_BYTES
            ),
        }
    }
}

impl Error for HeaderError {}
