//! Field syntax shared by every header, and the value types that carry it:
//! offsets, incarnation tokens, producer ids and integers, and digests.
//!
//! Every value type here is valid by construction: a value that exists
//! renders to text that parses back to the same value. Raw integers convert
//! in explicitly, through `from_u64` where every `u64` is valid and through
//! `TryFrom<u64>` where some are not, and out through `get`.

use std::fmt;

use crate::error::HeaderError;
use crate::names;

/// The largest producer epoch or sequence number, 2^53 - 1: the base
/// protocol's limit, so that every value is exact in a JavaScript number.
pub const MAX_PRODUCER_INTEGER: u64 = 9_007_199_254_740_991;

/// The largest incarnation token, 2^53 - 1: tokens are exact in a
/// JavaScript number.
pub const MAX_INCARNATION: u64 = 9_007_199_254_740_991;

/// The longest `Producer-Id`, in bytes: the server's limit.
pub const PRODUCER_ID_MAX_BYTES: usize = 256;

/// The width Ursula pads offsets to, so that offsets compare as strings.
/// `u64::MAX` has 20 digits, so every offset fits.
pub const OFFSET_WIDTH: usize = 20;

/// ASCII digits only: no sign, no whitespace, not empty.
fn digits(name: &'static str, text: &str) -> Result<u64, HeaderError> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(HeaderError::InvalidInteger { name });
    }
    text.parse::<u64>()
        .map_err(|_overflow| HeaderError::IntegerTooLarge { name })
}

/// A canonical decimal integer, as Ursula renders tokens, limits, counts and
/// the integers of its answers: digits without a leading zero, `0` itself
/// excepted.
pub fn parse_decimal(name: &'static str, text: &str) -> Result<u64, HeaderError> {
    let value = digits(name, text)?;
    if text.len() > 1 && text.starts_with('0') {
        return Err(HeaderError::NonCanonicalInteger { name });
    }
    Ok(value)
}

/// A position in a stream. Offsets order: a later byte has a greater offset.
///
/// Rendered as exactly [`OFFSET_WIDTH`] zero-padded decimal digits and
/// parsed only in that form. Every `u64` is an offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Offset(u64);

impl Offset {
    pub const ZERO: Self = Self(0);

    pub const fn from_u64(offset: u64) -> Self {
        Self(offset)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    /// Parses the value of header `name`: exactly [`OFFSET_WIDTH`] decimal
    /// digits.
    pub fn parse(name: &'static str, text: &str) -> Result<Self, HeaderError> {
        if text.len() != OFFSET_WIDTH {
            return Err(HeaderError::InvalidOffset { name });
        }
        digits(name, text).map(Self).map_err(|err| match err {
            HeaderError::InvalidInteger { name } => HeaderError::InvalidOffset { name },
            other => other,
        })
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:0OFFSET_WIDTH$}", self.0)
    }
}

/// A `Stream-Incarnation` token: 1 to [`MAX_INCARNATION`], canonical
/// decimal. Tokens are compared for equality only, so the type has no
/// order. Every token a 0.8 server issues is at least 2^52.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Incarnation(u64);

impl Incarnation {
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Parses the value of header `name`: canonical decimal, not `0`, at
    /// most [`MAX_INCARNATION`].
    pub fn parse(name: &'static str, text: &str) -> Result<Self, HeaderError> {
        Self::checked(name, parse_decimal(name, text)?)
    }

    fn checked(name: &'static str, value: u64) -> Result<Self, HeaderError> {
        if value == 0 || value > MAX_INCARNATION {
            return Err(HeaderError::OutOfRange { name, value });
        }
        Ok(Self(value))
    }
}

impl TryFrom<u64> for Incarnation {
    type Error = HeaderError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::checked(names::STREAM_INCARNATION, value)
    }
}

impl fmt::Display for Incarnation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Defines a producer integer: 0 to [`MAX_PRODUCER_INTEGER`].
macro_rules! producer_integer {
    ($(#[$doc:meta])* $type:ident, $name:expr) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $type(u64);

        impl $type {
            pub const ZERO: Self = Self(0);
            pub const MAX: Self = Self(MAX_PRODUCER_INTEGER);

            pub const fn get(self) -> u64 {
                self.0
            }

            /// Parses the value of header `name` as Ursula renders it:
            /// canonical decimal, at most [`MAX_PRODUCER_INTEGER`].
            pub fn parse(name: &'static str, text: &str) -> Result<Self, HeaderError> {
                Self::checked(name, parse_decimal(name, text)?)
            }

            /// Parses a request's value of header `name`: decimal digits, at
            /// most [`MAX_PRODUCER_INTEGER`]. Leading zeros are accepted,
            /// since base-protocol clients choose these values.
            pub(crate) fn parse_request(
                name: &'static str,
                text: &str,
            ) -> Result<Self, HeaderError> {
                Self::checked(name, digits(name, text)?)
            }

            fn checked(name: &'static str, value: u64) -> Result<Self, HeaderError> {
                if value > MAX_PRODUCER_INTEGER {
                    return Err(HeaderError::OutOfRange { name, value });
                }
                Ok(Self(value))
            }
        }

        impl TryFrom<u64> for $type {
            type Error = HeaderError;

            fn try_from(value: u64) -> Result<Self, Self::Error> {
                Self::checked($name, value)
            }
        }

        impl fmt::Display for $type {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

producer_integer!(
    /// A producer epoch (`Producer-Epoch`, `Producer-Closed-Epoch`).
    Epoch,
    names::PRODUCER_EPOCH
);

producer_integer!(
    /// A producer sequence number (`Producer-Seq`, `Producer-Closed-Seq`,
    /// `Producer-Expected-Seq`, `Producer-Received-Seq`).
    Seq,
    names::PRODUCER_SEQ
);

/// A `Producer-Id`: 1 to [`PRODUCER_ID_MAX_BYTES`] bytes of visible ASCII,
/// spaces and tabs, starting and ending with visible ASCII. Field
/// whitespace around a header value is not part of it, so an id with
/// surrounding whitespace could not round-trip.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProducerId(String);

impl ProducerId {
    pub fn new(id: impl Into<String>) -> Result<Self, HeaderError> {
        let id = id.into();
        let bytes = id.as_bytes();
        let (Some(first), Some(last)) = (bytes.first(), bytes.last()) else {
            return Err(HeaderError::EmptyProducerId);
        };
        if bytes.len() > PRODUCER_ID_MAX_BYTES {
            return Err(HeaderError::ProducerIdTooLong { len: bytes.len() });
        }
        let visible = |byte: &u8| (0x21..=0x7e).contains(byte);
        if !visible(first)
            || !visible(last)
            || !bytes
                .iter()
                .all(|byte| visible(byte) || matches!(byte, b' ' | b'\t'))
        {
            return Err(HeaderError::InvalidProducerId);
        }
        Ok(Self(id))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProducerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The longest `Stream-Seq` a server accepts, in bytes.
pub const STREAM_SEQ_MAX_BYTES: usize = 256;

/// A `Stream-Seq` value: 1 to [`STREAM_SEQ_MAX_BYTES`] bytes of visible
/// ASCII, with inner spaces or tabs, compared as bytes (base protocol).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamSeq(String);

impl StreamSeq {
    pub fn new(seq: impl Into<String>) -> Result<Self, HeaderError> {
        let seq = seq.into();
        let bytes = seq.as_bytes();
        let visible = |byte: &u8| (0x21..=0x7e).contains(byte);
        let valid = matches!((bytes.first(), bytes.last()), (Some(first), Some(last)) if visible(first) && visible(last))
            && bytes.len() <= STREAM_SEQ_MAX_BYTES
            && bytes
                .iter()
                .all(|byte| visible(byte) || matches!(byte, b' ' | b'\t'));
        if !valid {
            return Err(HeaderError::InvalidStreamSeq);
        }
        Ok(Self(seq))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StreamSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// 32 bytes rendered as 64 lowercase hex digits.
fn parse_hex32(name: &'static str, text: &str) -> Result<[u8; 32], HeaderError> {
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => byte.checked_sub(b'0'),
            b'a'..=b'f' => byte.checked_sub(b'a').and_then(|n| n.checked_add(10)),
            _ => None,
        }
    }
    let bytes = text.as_bytes();
    if bytes.len() != 64 {
        return Err(HeaderError::InvalidDigest { name });
    }
    let mut out = [0_u8; 32];
    for (pair, slot) in bytes.chunks_exact(2).zip(out.iter_mut()) {
        let [high, low] = pair else {
            return Err(HeaderError::InvalidDigest { name });
        };
        let (Some(high), Some(low)) = (nibble(*high), nibble(*low)) else {
            return Err(HeaderError::InvalidDigest { name });
        };
        *slot = (high << 4) | low;
    }
    Ok(out)
}

/// Defines a BLAKE3-256 digest type rendered as 64 lowercase hex digits.
macro_rules! digest {
    ($(#[$doc:meta])* $type:ident) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $type([u8; 32]);

        impl $type {
            pub const fn from_bytes(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }

            pub const fn as_bytes(&self) -> &[u8; 32] {
                &self.0
            }

            /// Parses the value of header `name`: 64 lowercase hex digits.
            pub fn parse(name: &'static str, text: &str) -> Result<Self, HeaderError> {
                parse_hex32(name, text).map(Self)
            }
        }

        impl fmt::Display for $type {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                for byte in self.0 {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
        }

        impl fmt::Debug for $type {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({self})", stringify!($type))
            }
        }
    };
}

digest!(
    /// `Producer-Digest`: BLAKE3-256 of a write's request body.
    BodyDigest
);

digest!(
    /// `Stream-Snapshot-Digest`: BLAKE3-256 over the length of the
    /// snapshot's content type, the content type and the snapshot body.
    SnapshotDigest
);

#[cfg(test)]
mod tests {
    use super::BodyDigest;
    use super::Epoch;
    use super::Incarnation;
    use super::MAX_PRODUCER_INTEGER;
    use super::Offset;
    use super::ProducerId;
    use super::Seq;
    use super::parse_decimal;
    use crate::error::HeaderError;

    const NAME: &str = "x";

    #[test]
    fn offsets_round_trip_at_twenty_digits() {
        for offset in [0, 1, 12, 33_554_432, u64::MAX] {
            let text = Offset::from_u64(offset).to_string();
            assert_eq!(text.len(), 20);
            assert_eq!(Offset::parse(NAME, &text), Ok(Offset::from_u64(offset)));
        }
        assert_eq!(Offset::from_u64(7).to_string(), "00000000000000000007");
        assert!(Offset::from_u64(7) < Offset::from_u64(8));
    }

    #[test]
    fn offsets_reject_every_other_form() {
        for invalid in [
            "",
            "0",
            "7",
            "0000000000000000007",
            "000000000000000000007",
            "+0000000000000000007",
            "-0000000000000000007",
            " 0000000000000000007",
            "0000000000 000000007",
            "0000000000000000000x",
        ] {
            assert_eq!(
                Offset::parse(NAME, invalid),
                Err(HeaderError::InvalidOffset { name: NAME }),
                "{invalid:?}"
            );
        }
        assert_eq!(
            Offset::parse(NAME, "18446744073709551616"),
            Err(HeaderError::IntegerTooLarge { name: NAME })
        );
    }

    #[test]
    fn incarnations_are_non_zero_and_js_exact() {
        for token in [1, 4_503_599_627_370_496, 9_007_199_254_740_991] {
            let incarnation = Incarnation::try_from(token).unwrap();
            assert_eq!(incarnation.get(), token);
            assert_eq!(
                Incarnation::parse(NAME, &incarnation.to_string()),
                Ok(incarnation)
            );
        }
        for value in [0, 9_007_199_254_740_992, u64::MAX] {
            assert_eq!(
                Incarnation::parse(NAME, &value.to_string()),
                Err(HeaderError::OutOfRange { name: NAME, value })
            );
            assert_eq!(
                Incarnation::try_from(value),
                Err(HeaderError::OutOfRange {
                    name: "stream-incarnation",
                    value
                })
            );
        }
    }

    #[test]
    fn decimals_reject_signs_whitespace_and_leading_zeros() {
        for (invalid, expected) in [
            ("", HeaderError::InvalidInteger { name: NAME }),
            ("+1", HeaderError::InvalidInteger { name: NAME }),
            ("-1", HeaderError::InvalidInteger { name: NAME }),
            (" 1", HeaderError::InvalidInteger { name: NAME }),
            ("1 2", HeaderError::InvalidInteger { name: NAME }),
            ("1.0", HeaderError::InvalidInteger { name: NAME }),
            ("0x1", HeaderError::InvalidInteger { name: NAME }),
            ("01", HeaderError::NonCanonicalInteger { name: NAME }),
            ("00", HeaderError::NonCanonicalInteger { name: NAME }),
            ("18446744073709551616", HeaderError::IntegerTooLarge {
                name: NAME,
            }),
        ] {
            assert_eq!(
                Incarnation::parse(NAME, invalid),
                Err(expected.clone()),
                "{invalid:?}"
            );
            assert_eq!(Epoch::parse(NAME, invalid), Err(expected.clone()));
            assert_eq!(parse_decimal(NAME, invalid), Err(expected), "{invalid:?}");
        }
    }

    #[test]
    fn producer_integers_stop_at_two_to_the_fifty_three() {
        assert_eq!(Seq::try_from(MAX_PRODUCER_INTEGER), Ok(Seq::MAX));
        assert_eq!(
            Seq::try_from(MAX_PRODUCER_INTEGER + 1),
            Err(HeaderError::OutOfRange {
                name: "producer-seq",
                value: MAX_PRODUCER_INTEGER + 1
            })
        );
        assert_eq!(Epoch::parse(NAME, "9007199254740991"), Ok(Epoch::MAX));
        assert_eq!(
            Epoch::parse(NAME, "9007199254740992"),
            Err(HeaderError::OutOfRange {
                name: NAME,
                value: 9_007_199_254_740_992
            })
        );
        // Requests accept leading zeros; answers do not.
        assert_eq!(Epoch::parse_request(NAME, "007").map(Epoch::get), Ok(7));
        assert_eq!(
            Epoch::parse(NAME, "007"),
            Err(HeaderError::NonCanonicalInteger { name: NAME })
        );
        assert_eq!(
            Epoch::parse_request(NAME, "+7"),
            Err(HeaderError::InvalidInteger { name: NAME })
        );
    }

    #[test]
    fn producer_ids_are_trimmed_visible_ascii() {
        for valid in ["w", "sqlite-ursula-vfs/2", "a b\tc", &"x".repeat(256)] {
            assert_eq!(ProducerId::new(valid).unwrap().as_str(), valid);
        }
        assert_eq!(ProducerId::new(""), Err(HeaderError::EmptyProducerId));
        assert_eq!(
            ProducerId::new("x".repeat(257)),
            Err(HeaderError::ProducerIdTooLong { len: 257 })
        );
        for invalid in [" w", "w ", "\tw", " ", "a\r\nx", "é", "a\u{7f}"] {
            assert_eq!(
                ProducerId::new(invalid),
                Err(HeaderError::InvalidProducerId),
                "{invalid:?}"
            );
        }
    }

    #[test]
    fn digests_round_trip_through_lowercase_hex() {
        let mut bytes = [0_u8; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::try_from(index * 7).unwrap();
        }
        let digest = BodyDigest::from_bytes(bytes);
        let hex = digest.to_string();
        assert_eq!(hex.len(), 64);
        assert_eq!(BodyDigest::parse(NAME, &hex), Ok(digest));
        for invalid in [
            hex.to_uppercase(),
            hex.get(1..).unwrap().to_owned(),
            format!("{hex}0"),
            format!(" {}", hex.get(1..).unwrap()),
            "g".repeat(64),
        ] {
            assert_eq!(
                BodyDigest::parse(NAME, &invalid),
                Err(HeaderError::InvalidDigest { name: NAME }),
                "{invalid:?}"
            );
        }
    }
}
