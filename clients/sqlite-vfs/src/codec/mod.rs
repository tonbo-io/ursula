//! The 0.8 byte formats. All integers are little-endian.
//!
//! The formats are fixed within 0.8.x and carry no extension fields. Unknown kinds, versions and
//! magics are refused by design, never skipped, and a format change needs a new minor version.
//!
//! Module map:
//!
//! - [`frame`]: `USQ2` stream frames (claims, commits, parts of a commit) and commit records.
//! - [`segment`]: `USL1` local log segments, a header and a chain of records.
//! - [`marker`]: the page-1 marker a generation's db file holds on disk in place of SQLite's
//!   magic.
//! - [`snapshot`]: `USS3` snapshot bodies, streamed between readers and writers.
//! - [`digest`]: the producer digest of an append body and the server's snapshot digest.
//! - [`stream`]: `USQ2` frames read out of a stream range, read by read.

pub mod digest;
pub mod frame;
pub mod marker;
pub mod segment;
pub mod snapshot;
pub mod stream;
#[cfg(test)]
mod vectors;

/// A SQLite page: the VFS requires 4 KiB pages.
pub const PAGE: usize = 4096;

/// Reads little-endian fields off the front of a byte slice. Every read returns `None` when the
/// slice is too short, leaving the caller to say what that means.
struct Take<'a>(&'a [u8]);

impl<'a> Take<'a> {
    fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, rest) = self.0.split_first_chunk::<N>()?;
        self.0 = rest;
        Some(*head)
    }

    fn u8(&mut self) -> Option<u8> {
        let (&b, rest) = self.0.split_first()?;
        self.0 = rest;
        Some(b)
    }

    fn u16(&mut self) -> Option<u16> {
        self.array().map(u16::from_le_bytes)
    }

    fn u32(&mut self) -> Option<u32> {
        self.array().map(u32::from_le_bytes)
    }

    fn u64(&mut self) -> Option<u64> {
        self.array().map(u64::from_le_bytes)
    }

    fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        let (head, rest) = self.0.split_at_checked(n)?;
        self.0 = rest;
        Some(head)
    }

    fn rest_len(&self) -> usize {
        self.0.len()
    }

    fn rest_slice(&self) -> &'a [u8] {
        self.0
    }

    fn rest(self) -> &'a [u8] {
        self.0
    }
}
