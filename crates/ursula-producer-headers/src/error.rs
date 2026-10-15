//! The one error of parsing producer headers.

use std::error::Error;
use std::fmt;

use crate::names;
use crate::value::MAX_PRODUCER_INTEGER;
use crate::value::PRODUCER_ID_MAX_BYTES;

/// A header that is malformed, missing where the rest requires it, or
/// repeated. A server answers each with `400 Bad Request`; a client treats
/// one in an answer as a protocol violation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeaderError {
    /// The header appears more than once.
    Duplicate { name: &'static str },
    /// The value holds bytes outside visible ASCII.
    NotText { name: &'static str },
    /// The other headers of the request or answer require this one.
    Missing { name: &'static str },
    /// Some, but not all, of `Producer-Id`, `Producer-Epoch` and
    /// `Producer-Seq`.
    PartialProducer,
    /// `Producer-Id` is empty.
    EmptyProducerId,
    /// `Producer-Id` is longer than [`PRODUCER_ID_MAX_BYTES`].
    ProducerIdTooLong { len: usize },
    /// Not a decimal integer.
    InvalidInteger { name: &'static str },
    /// A producer integer above [`MAX_PRODUCER_INTEGER`], or an offset or
    /// token above `u64::MAX`.
    IntegerTooLarge { name: &'static str },
    /// A token with a leading zero.
    NonCanonicalInteger { name: &'static str },
    /// `Producer-Digest` is not 64 lowercase hex digits.
    InvalidDigest,
    /// `Producer-Digest` without the producer headers.
    DigestWithoutProducer,
    /// `Producer-Outcome` names no known outcome, or `epoch-closed` lacks its
    /// `by` parameter.
    InvalidOutcome,
    /// `Stream-Closed` is present and not `true`.
    InvalidClosed,
    /// A `400` names another expected sequence number than `0`.
    UnexpectedExpectedSeq { expected: u64 },
    /// `Producer-Outcome` does not fit the answer's status.
    OutcomeStatusMismatch { status: u16 },
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
            Self::IntegerTooLarge { name } => {
                write!(
                    f,
                    "{name} is too large (producer integers are <= {MAX_PRODUCER_INTEGER})"
                )
            }
            Self::NonCanonicalInteger { name } => {
                write!(f, "{name} must not have a leading zero")
            }
            Self::InvalidDigest => write!(
                f,
                "{} must be 64 lowercase hex digits",
                names::PRODUCER_DIGEST
            ),
            Self::DigestWithoutProducer => write!(
                f,
                "{} requires the producer headers",
                names::PRODUCER_DIGEST
            ),
            Self::InvalidOutcome => write!(f, "{} is not a known outcome", names::PRODUCER_OUTCOME),
            Self::InvalidClosed => write!(f, "{} must be `true`", names::STREAM_CLOSED),
            Self::UnexpectedExpectedSeq { expected } => {
                write!(f, "a 400 producer answer expects seq 0, not {expected}")
            }
            Self::OutcomeStatusMismatch { status } => write!(
                f,
                "{} does not fit status {status}",
                names::PRODUCER_OUTCOME
            ),
        }
    }
}

impl Error for HeaderError {}
