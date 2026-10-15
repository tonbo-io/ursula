//! Field syntax shared by every header: digests, integers, offsets and
//! tokens.

use std::fmt;

use crate::error::HeaderError;

/// The largest producer epoch or sequence number, 2^53 - 1: the base
/// protocol's limit, so that every value is exact in a JavaScript number.
pub const MAX_PRODUCER_INTEGER: u64 = 9_007_199_254_740_991;

/// The longest `Producer-Id`, in bytes.
pub const PRODUCER_ID_MAX_BYTES: usize = 256;

/// The width Ursula pads offsets to, so that offsets compare as strings.
/// `u64::MAX` has 20 digits, so every offset fits.
pub const OFFSET_WIDTH: usize = 20;

/// A BLAKE3-256 digest in 64 lowercase hex digits: a write body's
/// `Producer-Digest`, or a snapshot's `Stream-Snapshot-Digest`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Digest([u8; 32]);

impl Digest {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Parses the value of header `name`: 64 lowercase hex digits.
    pub fn parse(name: &'static str, text: &str) -> Result<Self, HeaderError> {
        let bytes = text.as_bytes();
        if bytes.len() != 64
            || !bytes
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
        {
            return Err(HeaderError::InvalidDigest { name });
        }
        let mut digest = [0_u8; 32];
        for (pair, out) in bytes.chunks_exact(2).zip(digest.iter_mut()) {
            let pair = std::str::from_utf8(pair)
                .map_err(|_not_utf8| HeaderError::InvalidDigest { name })?;
            *out = u8::from_str_radix(pair, 16)
                .map_err(|_not_hex| HeaderError::InvalidDigest { name })?;
        }
        Ok(Self(digest))
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Digest({self})")
    }
}

/// ASCII digits only: no sign, no whitespace, not empty.
fn digits(name: &'static str, text: &str) -> Result<u64, HeaderError> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(HeaderError::InvalidInteger { name });
    }
    text.parse::<u64>()
        .map_err(|_overflow| HeaderError::IntegerTooLarge { name })
}

/// A canonical decimal integer, as Ursula renders tokens, limits and
/// counts: digits without a leading zero, `0` itself excepted.
pub fn parse_decimal(name: &'static str, text: &str) -> Result<u64, HeaderError> {
    let value = digits(name, text)?;
    if text.len() > 1 && text.starts_with('0') {
        return Err(HeaderError::NonCanonicalInteger { name });
    }
    Ok(value)
}

/// A producer epoch or sequence number: decimal digits, at most
/// [`MAX_PRODUCER_INTEGER`]. Leading zeros are accepted, since base-protocol
/// clients choose these values; Ursula renders them canonically.
pub fn parse_producer_integer(name: &'static str, text: &str) -> Result<u64, HeaderError> {
    let value = digits(name, text)?;
    if value > MAX_PRODUCER_INTEGER {
        return Err(HeaderError::IntegerTooLarge { name });
    }
    Ok(value)
}

/// A stream offset as Ursula renders it: exactly [`OFFSET_WIDTH`] decimal
/// digits, zero-padded.
pub fn parse_offset(name: &'static str, text: &str) -> Result<u64, HeaderError> {
    if text.len() != OFFSET_WIDTH {
        return Err(HeaderError::InvalidOffset { name });
    }
    digits(name, text).map_err(|err| match err {
        HeaderError::InvalidInteger { name } => HeaderError::InvalidOffset { name },
        other => other,
    })
}

/// An offset as Ursula renders it: padded to [`OFFSET_WIDTH`] digits.
pub fn render_offset(offset: u64) -> String {
    format!("{offset:0OFFSET_WIDTH$}")
}

/// A `Stream-Incarnation` token (or `Stream-Previous-Incarnation`): canonical
/// decimal. Clients compare tokens only for equality.
pub fn parse_token(name: &'static str, text: &str) -> Result<u64, HeaderError> {
    parse_decimal(name, text)
}

/// A token, limit or count as Ursula renders it: canonical decimal.
pub fn render_decimal(value: u64) -> String {
    value.to_string()
}

#[cfg(test)]
mod tests {
    use super::Digest;
    use super::parse_decimal;
    use super::parse_offset;
    use super::parse_producer_integer;
    use super::parse_token;
    use super::render_decimal;
    use super::render_offset;
    use crate::error::HeaderError;

    const NAME: &str = "x";

    #[test]
    fn offsets_round_trip_at_twenty_digits() {
        for offset in [0, 1, 12, 33_554_432, u64::MAX] {
            let text = render_offset(offset);
            assert_eq!(text.len(), 20);
            assert_eq!(parse_offset(NAME, &text), Ok(offset));
        }
        assert_eq!(render_offset(7), "00000000000000000007");
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
                parse_offset(NAME, invalid),
                Err(HeaderError::InvalidOffset { name: NAME }),
                "{invalid:?}"
            );
        }
        assert_eq!(
            parse_offset(NAME, "18446744073709551616"),
            Err(HeaderError::IntegerTooLarge { name: NAME })
        );
    }

    #[test]
    fn tokens_round_trip_in_canonical_decimal() {
        for token in [0, 1, 4_503_599_627_370_496, 9_007_199_254_740_991, u64::MAX] {
            assert_eq!(parse_token(NAME, &render_decimal(token)), Ok(token));
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
                parse_token(NAME, invalid),
                Err(expected.clone()),
                "{invalid:?}"
            );
            assert_eq!(parse_decimal(NAME, invalid), Err(expected), "{invalid:?}");
        }
    }

    #[test]
    fn producer_integers_stop_at_two_to_the_fifty_three() {
        assert_eq!(
            parse_producer_integer(NAME, "9007199254740991"),
            Ok(9_007_199_254_740_991)
        );
        assert_eq!(
            parse_producer_integer(NAME, "9007199254740992"),
            Err(HeaderError::IntegerTooLarge { name: NAME })
        );
        assert_eq!(parse_producer_integer(NAME, "007"), Ok(7));
        assert_eq!(
            parse_producer_integer(NAME, "+7"),
            Err(HeaderError::InvalidInteger { name: NAME })
        );
    }

    #[test]
    fn digests_round_trip_through_lowercase_hex() {
        let mut bytes = [0_u8; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::try_from(index * 7).unwrap();
        }
        let digest = Digest::from_bytes(bytes);
        let hex = digest.to_string();
        assert_eq!(hex.len(), 64);
        assert_eq!(Digest::parse(NAME, &hex), Ok(digest));
        for invalid in [
            hex.to_uppercase(),
            hex.get(1..).unwrap().to_owned(),
            format!("{hex}0"),
            format!(" {}", hex.get(1..).unwrap()),
            "g".repeat(64),
        ] {
            assert_eq!(
                Digest::parse(NAME, &invalid),
                Err(HeaderError::InvalidDigest { name: NAME }),
                "{invalid:?}"
            );
        }
    }
}
