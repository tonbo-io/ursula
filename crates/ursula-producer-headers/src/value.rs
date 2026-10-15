//! Field syntax shared by requests and answers.

use std::fmt;

use crate::error::HeaderError;

/// The largest producer epoch or sequence number, 2^53 - 1: the base
/// protocol's limit, so that every value is exact in a JavaScript number.
pub const MAX_PRODUCER_INTEGER: u64 = 9_007_199_254_740_991;

/// The longest `Producer-Id`, in bytes.
pub const PRODUCER_ID_MAX_BYTES: usize = 256;

/// The width Ursula pads offsets to, so that offsets compare as strings.
const OFFSET_WIDTH: usize = 20;

/// BLAKE3-256 of a write's body (`Producer-Digest`).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Digest([u8; 32]);

impl Digest {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Parses 64 lowercase hex digits.
    pub fn parse_hex(text: &str) -> Result<Self, HeaderError> {
        let bytes = text.as_bytes();
        if bytes.len() != 64
            || !bytes
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
        {
            return Err(HeaderError::InvalidDigest);
        }
        let mut digest = [0_u8; 32];
        for (pair, out) in bytes.chunks_exact(2).zip(digest.iter_mut()) {
            let pair = std::str::from_utf8(pair).map_err(|_not_utf8| HeaderError::InvalidDigest)?;
            *out = u8::from_str_radix(pair, 16).map_err(|_not_hex| HeaderError::InvalidDigest)?;
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

/// A decimal integer: ASCII digits only, no sign.
fn decimal(name: &'static str, text: &str) -> Result<u64, HeaderError> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(HeaderError::InvalidInteger { name });
    }
    text.parse::<u64>()
        .map_err(|_overflow| HeaderError::IntegerTooLarge { name })
}

/// A producer epoch or sequence number: decimal, at most
/// [`MAX_PRODUCER_INTEGER`].
pub fn parse_producer_integer(name: &'static str, text: &str) -> Result<u64, HeaderError> {
    let value = decimal(name, text)?;
    if value > MAX_PRODUCER_INTEGER {
        return Err(HeaderError::IntegerTooLarge { name });
    }
    Ok(value)
}

/// A stream offset: decimal, any width. Ursula pads offsets with zeros.
pub fn parse_offset(name: &'static str, text: &str) -> Result<u64, HeaderError> {
    decimal(name, text)
}

/// An offset as Ursula renders it: padded to 20 digits.
pub fn render_offset(offset: u64) -> String {
    format!("{offset:0OFFSET_WIDTH$}")
}

/// A `Stream-Incarnation` token: decimal without a leading zero.
pub fn parse_token(name: &'static str, text: &str) -> Result<u64, HeaderError> {
    let value = decimal(name, text)?;
    if text.len() > 1 && text.starts_with('0') {
        return Err(HeaderError::NonCanonicalInteger { name });
    }
    Ok(value)
}
