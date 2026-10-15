//! `USS3` snapshot bodies, published at `PUT {stream}/snapshot/{W}`: the database's page images at
//! a frame boundary of the stream, and what a replay of the rest needs.
//!
//! ```text
//! "USS3" | u64 W | u64 epoch | [16] lineage | u64 position | u32 pages | u32 crc32c(image)
//!        | u32 crc32c(header) | zstd(image)
//! ```
//!
//! `W` is the stream offset the image reflects: every frame before it applied, none after.
//! `epoch` is the highest producer epoch claimed before `W` (retention may trim the claims
//! themselves). `(lineage, position)` is the last commit included. `image` is `pages` x 4 KiB, page
//! 1 first, byte-identical to a file built by replaying the stream up to `W`. The header carries
//! its own checksum, so it can be verified without the body. The zstd frame declares its content
//! size and carries a content checksum.
//!
//! Bodies stream between files: [`write_body`] reads the image twice (once for its checksum, once
//! to compress it) and [`read_image`] decompresses into a writer, so neither holds the image in
//! memory.

use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use super::PAGE;
use super::Take;
use crate::ids::CommitId;
use crate::ids::Epoch;
use crate::ids::Lineage;
use crate::ids::Position;

const MAGIC: &[u8; 4] = b"USS3";
/// The header's length.
pub const HEADER: usize = 56;
const ZSTD_LEVEL: i32 = 3;
/// Enough of a zstd frame to read its declared content size (`ZSTD_FRAMEHEADERSIZE_MAX`).
const ZSTD_FRAME_HEADER_MAX: usize = 18;
const CHUNK: usize = 64 * 1024;

/// What the snapshot thread knows of a snapshot before it reads the image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotFields {
    /// `W`, the stream offset the image reflects.
    pub offset: u64,
    /// The highest epoch claimed before `offset`.
    pub epoch: Epoch,
    /// The last commit included.
    pub last: CommitId,
}

/// A body's header: the fields, the image's page count and its checksum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotHeader {
    pub fields: SnapshotFields,
    pub pages: u32,
    pub crc: u32,
}

impl SnapshotHeader {
    /// The image's length in bytes.
    pub fn image_len(&self) -> u64 {
        u64::from(self.pages).saturating_mul(PAGE as u64)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// A body of another minor version's format (an incompatible change bumps the magic).
    #[error(
        "snapshot magic \"{}\": this extension reads \"USS3\" snapshots, so the snapshot was \
         taken by an extension of another minor version",
        .found.escape_ascii()
    )]
    Magic { found: Vec<u8> },
    #[error("snapshot header cut short: {len} bytes")]
    HeaderTruncated { len: usize },
    #[error("snapshot header checksum {found:#010x}, expected {expected:#010x}")]
    HeaderChecksum { expected: u32, found: u32 },
    #[error("an image of {len} bytes is not whole pages")]
    PartialPage { len: u64 },
    #[error("an image of {len} bytes is too large")]
    TooLarge { len: u64 },
    #[error("the image changed while it was compressed")]
    ImageChanged,
    #[error("{pages} pages, but the compressed image declares {found:?} bytes")]
    PageCount { pages: u32, found: Option<u64> },
    #[error("an image of {found} bytes, {expected} expected")]
    ImageLength { expected: u64, found: u64 },
    #[error("image checksum {found:#010x}, expected {expected:#010x}")]
    Checksum { expected: u32, found: u32 },
    #[error("read the image: {0}")]
    Read(#[source] io::Error),
    #[error("write the body: {0}")]
    Write(#[source] io::Error),
    #[error("zstd: {0}")]
    Zstd(#[source] io::Error),
}

/// Copies at most `limit` bytes from `from` to `to`, returning their length and crc32c.
fn copy_crc(
    from: &mut impl Read,
    to: &mut impl Write,
    limit: u64,
    read: fn(io::Error) -> SnapshotError,
) -> Result<(u64, u32), SnapshotError> {
    let mut from = from.take(limit);
    let mut buf = vec![0u8; CHUNK];
    let (mut len, mut crc) = (0u64, 0u32);
    loop {
        let n = match from.read(&mut buf) {
            Ok(0) => return Ok((len, crc)),
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(read(e)),
        };
        let chunk = buf.get(..n).unwrap_or_default();
        crc = crc32c::crc32c_append(crc, chunk);
        len = len.saturating_add(n as u64);
        to.write_all(chunk).map_err(SnapshotError::Write)?;
    }
}

/// Writes the body of the image read from `image` (from its current position) to `out`. The image
/// is read twice, so it must not change meanwhile: a change is an error. The header goes out
/// before the second read verifies the image, so `out` must be a temp file that is published only
/// after this returns `Ok`.
pub fn write_body(
    fields: &SnapshotFields,
    image: &mut (impl Read + Seek),
    out: &mut impl Write,
) -> Result<SnapshotHeader, SnapshotError> {
    let start = image.stream_position().map_err(SnapshotError::Read)?;
    let (len, crc) = copy_crc(image, &mut io::sink(), u64::MAX, SnapshotError::Read)?;
    if !len.is_multiple_of(PAGE as u64) {
        return Err(SnapshotError::PartialPage { len });
    }
    let pages = len
        .checked_div(PAGE as u64)
        .and_then(|n| u32::try_from(n).ok())
        .ok_or(SnapshotError::TooLarge { len })?;
    let header = SnapshotHeader {
        fields: *fields,
        pages,
        crc,
    };
    image
        .seek(SeekFrom::Start(start))
        .map_err(SnapshotError::Read)?;
    out.write_all(&encode_header(&header))
        .map_err(SnapshotError::Write)?;
    let mut z =
        zstd::stream::write::Encoder::new(&mut *out, ZSTD_LEVEL).map_err(SnapshotError::Zstd)?;
    z.include_contentsize(true).map_err(SnapshotError::Zstd)?;
    z.include_checksum(true).map_err(SnapshotError::Zstd)?;
    z.set_pledged_src_size(Some(len))
        .map_err(SnapshotError::Zstd)?;
    let again = copy_crc(image, &mut z, len, SnapshotError::Read)?;
    if again != (len, crc) {
        return Err(SnapshotError::ImageChanged);
    }
    // One byte more means the image grew between the two reads.
    let mut extra = [0u8; 1];
    loop {
        match image.read(&mut extra) {
            Ok(0) => break,
            Ok(_) => return Err(SnapshotError::ImageChanged),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(SnapshotError::Read(e)),
        }
    }
    z.finish().map_err(SnapshotError::Zstd)?;
    Ok(header)
}

fn encode_header(h: &SnapshotHeader) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&h.fields.offset.to_le_bytes());
    out.extend_from_slice(&h.fields.epoch.0.to_le_bytes());
    out.extend_from_slice(&h.fields.last.lineage.0);
    out.extend_from_slice(&h.fields.last.position.0.to_le_bytes());
    out.extend_from_slice(&h.pages.to_le_bytes());
    out.extend_from_slice(&h.crc.to_le_bytes());
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// The body's header and where its compressed image starts.
pub fn read_header(body: &[u8]) -> Result<(SnapshotHeader, usize), SnapshotError> {
    let truncated = || SnapshotError::HeaderTruncated { len: body.len() };
    let mut t = Take(body);
    let Some(magic) = t.array::<4>() else {
        return Err(if MAGIC.starts_with(body) {
            truncated()
        } else {
            SnapshotError::Magic {
                found: body.to_vec(),
            }
        });
    };
    if &magic != MAGIC {
        return Err(SnapshotError::Magic {
            found: magic.to_vec(),
        });
    }
    let mut take = || {
        Some(SnapshotHeader {
            fields: SnapshotFields {
                offset: t.u64()?,
                epoch: Epoch(t.u64()?),
                last: CommitId {
                    lineage: Lineage(t.array()?),
                    position: Position(t.u64()?),
                },
            },
            pages: t.u32()?,
            crc: t.u32()?,
        })
    };
    let header = take().ok_or_else(truncated)?;
    let found = t.u32().ok_or_else(truncated)?;
    let expected = crc32c::crc32c(body.get(..HEADER.saturating_sub(4)).unwrap_or_default());
    if found != expected {
        return Err(SnapshotError::HeaderChecksum { expected, found });
    }
    Ok((header, HEADER))
}

/// Decompresses the image of a body whose header is `header` from `compressed` into `out`. The
/// compressed image must declare the header's page count before anything is decompressed, so a
/// damaged count fails before any allocation or write, and the image must match its checksum.
pub fn read_image(
    header: &SnapshotHeader,
    compressed: &mut impl Read,
    out: &mut impl Write,
) -> Result<(), SnapshotError> {
    let mut head = [0u8; ZSTD_FRAME_HEADER_MAX];
    let mut filled = 0;
    while let Some(rest) = head.get_mut(filled..).filter(|r| !r.is_empty()) {
        match compressed.read(rest) {
            Ok(0) => break,
            Ok(n) => filled = filled.saturating_add(n),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(SnapshotError::Read(e)),
        }
    }
    let head = head.get(..filled).unwrap_or_default();
    let expected = header.image_len();
    let found = zstd::zstd_safe::get_frame_content_size(head).ok().flatten();
    if found != Some(expected) {
        return Err(SnapshotError::PageCount {
            pages: header.pages,
            found,
        });
    }
    let mut z =
        zstd::stream::read::Decoder::new(head.chain(compressed)).map_err(SnapshotError::Zstd)?;
    // One byte past the image is read to tell a longer image from an exact one.
    let (len, crc) = copy_crc(&mut z, out, expected.saturating_add(1), SnapshotError::Zstd)?;
    if len != expected {
        return Err(SnapshotError::ImageLength {
            expected,
            found: len,
        });
    }
    if crc != header.crc {
        return Err(SnapshotError::Checksum {
            expected: header.crc,
            found: crc,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
