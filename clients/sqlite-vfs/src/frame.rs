//! Stream frames. A byte stream has no message boundaries on read, so every append is one
//! self-delimiting frame:
//!
//! ```text
//! "USQ1" | u32 len | u32 crc32c(payload) | payload = zstd(record)        (integers little-endian)
//! record = 0u8 | u32 db size after the commit | u32 n | n × (u32 pgno | page image)
//!        | 1u8 | u64 producer epoch | 16-byte random nonce                  (an owner's claim)
//! ```
use std::collections::BTreeMap;

pub const PAGE: usize = 4096;
const MAGIC: &[u8; 4] = b"USQ1";
const HEADER: usize = 12;
const ZSTD_LEVEL: i32 = 3;

#[derive(Debug, PartialEq, Eq)]
pub enum Record {
    Commit {
        size: u32,
        pages: Vec<(u32, Vec<u8>)>,
    },
    Claim {
        epoch: u64,
        /// Random per claim: tells this owner's claim from another's at the same epoch.
        nonce: [u8; 16],
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum Decoded {
    /// A whole frame of `len` bytes.
    Frame { record: Record, len: usize },
    /// The buffer ends inside a frame (or is empty).
    Partial,
}

/// A frame that cannot be encoded or decoded.
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    /// Not a frame this extension reads: a frame of another minor version's format (an
    /// incompatible change bumps the magic), or not a frame at all (bytes not written by the
    /// extension, or a read that does not start at a frame, as a claim's scan can meet).
    #[error(
        "frame magic \"{}\": this extension reads \"USQ1\" frames, so these bytes are a frame \
         of another minor version's extension, were not written by the extension, or are not the \
         start of a frame",
        .found.escape_ascii()
    )]
    Magic { found: Vec<u8> },
    #[error("frame checksum mismatch")]
    Checksum,
    #[error("zstd: {0}")]
    Zstd(#[source] std::io::Error),
    #[error("zstd compress: {0}")]
    Compress(#[source] std::io::Error),
    #[error("commit record: {bytes} bytes for {pages} pages")]
    CommitLength { bytes: usize, pages: usize },
    #[error("commit record: page {pgno} outside db size {size}")]
    PageOutside { pgno: u32, size: u32 },
    #[error("unknown record")]
    UnknownRecord,
    #[error("a frame of {0} bytes or pages is too large")]
    TooLarge(usize),
    #[error("page {pgno}: an image of {len} bytes")]
    PageImage { pgno: u32, len: usize },
}

/// A commit record's kind, db size and page count.
const COMMIT_HEADER: usize = 9;
/// A commit record's page entry: the page number and the image.
const ENTRY: usize = 4 + PAGE;

fn wrap(record: &[u8]) -> Result<Vec<u8>, FrameError> {
    let payload = zstd::bulk::compress(record, ZSTD_LEVEL).map_err(FrameError::Compress)?;
    let len =
        u32::try_from(payload.len()).map_err(|_too_long| FrameError::TooLarge(payload.len()))?;
    let mut out = Vec::with_capacity(payload.len().saturating_add(HEADER));
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// A commit frame and the uncompressed record size.
pub fn encode_commit(
    size: u32,
    pages: &BTreeMap<u32, Vec<u8>>,
) -> Result<(Vec<u8>, usize), FrameError> {
    let n = u32::try_from(pages.len()).map_err(|_too_many| FrameError::TooLarge(pages.len()))?;
    let mut record = Vec::with_capacity(
        pages
            .len()
            .saturating_mul(ENTRY)
            .saturating_add(COMMIT_HEADER),
    );
    record.push(0);
    record.extend_from_slice(&size.to_le_bytes());
    record.extend_from_slice(&n.to_le_bytes());
    for (&pgno, image) in pages {
        if image.len() != PAGE {
            return Err(FrameError::PageImage {
                pgno,
                len: image.len(),
            });
        }
        record.extend_from_slice(&pgno.to_le_bytes());
        record.extend_from_slice(image);
    }
    Ok((wrap(&record)?, record.len()))
}

pub fn encode_claim(epoch: u64, nonce: &[u8; 16]) -> Result<Vec<u8>, FrameError> {
    let mut record = vec![1];
    record.extend_from_slice(&epoch.to_le_bytes());
    record.extend_from_slice(nonce);
    wrap(&record)
}

/// The little-endian u32 at the start of `b`, and the rest.
fn take_u32(b: &[u8]) -> Option<(u32, &[u8])> {
    let (n, rest) = b.split_first_chunk::<4>()?;
    Some((u32::from_le_bytes(*n), rest))
}

/// Decodes the frame at the start of `buf`.
pub fn decode(buf: &[u8]) -> Result<Decoded, FrameError> {
    let Some((magic, rest)) = buf.split_first_chunk::<4>() else {
        // Shorter than the magic: what there is must be its start.
        return if MAGIC.iter().zip(buf).all(|(m, b)| m == b) {
            Ok(Decoded::Partial)
        } else {
            Err(FrameError::Magic {
                found: buf.to_vec(),
            })
        };
    };
    if magic != MAGIC {
        return Err(FrameError::Magic {
            found: magic.to_vec(),
        });
    }
    let Some((len, rest)) = take_u32(rest) else {
        return Ok(Decoded::Partial);
    };
    let Some((crc, rest)) = take_u32(rest) else {
        return Ok(Decoded::Partial);
    };
    let len = len as usize;
    let Some(payload) = rest.get(..len) else {
        return Ok(Decoded::Partial);
    };
    if crc32c::crc32c(payload) != crc {
        return Err(FrameError::Checksum);
    }
    let record = zstd::stream::decode_all(payload).map_err(FrameError::Zstd)?;
    Ok(Decoded::Frame {
        record: record_of(&record)?,
        // The payload is within `buf`: no overflow.
        len: HEADER.saturating_add(len),
    })
}

fn record_of(record: &[u8]) -> Result<Record, FrameError> {
    match record.split_first() {
        Some((0, rest)) => {
            let Some((size, rest)) = take_u32(rest) else {
                return Err(FrameError::UnknownRecord);
            };
            let Some((n, body)) = take_u32(rest) else {
                return Err(FrameError::UnknownRecord);
            };
            let n = n as usize;
            if n.checked_mul(ENTRY) != Some(body.len()) {
                return Err(FrameError::CommitLength {
                    bytes: body.len(),
                    pages: n,
                });
            }
            let mut pages = Vec::with_capacity(n);
            for entry in body.chunks_exact(ENTRY) {
                let Some((pgno, image)) = take_u32(entry) else {
                    return Err(FrameError::UnknownRecord);
                };
                if pgno == 0 || pgno > size {
                    return Err(FrameError::PageOutside { pgno, size });
                }
                pages.push((pgno, image.to_vec()));
            }
            Ok(Record::Commit { size, pages })
        }
        Some((1, rest)) => {
            let Some((epoch, nonce)) = rest.split_first_chunk::<8>() else {
                return Err(FrameError::UnknownRecord);
            };
            let Ok(nonce) = <[u8; 16]>::try_from(nonce) else {
                return Err(FrameError::UnknownRecord);
            };
            Ok(Record::Claim {
                epoch: u64::from_le_bytes(*epoch),
                nonce,
            })
        }
        _ => Err(FrameError::UnknownRecord),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::Decoded;
    use super::FrameError;
    use super::PAGE;
    use super::Record;
    use super::decode;
    use super::encode_claim;
    use super::encode_commit;

    // Reads cut frames anywhere: every prefix of a frame is Partial, the whole frame decodes, and a
    // flipped payload byte is a checksum error rather than a different record.
    #[test]
    fn frames_decode_only_whole_and_intact() {
        let pages = BTreeMap::from([(1, vec![7u8; PAGE]), (3, vec![9u8; PAGE])]);
        let (mut buf, _) = encode_commit(3, &pages).unwrap();
        let first = buf.len();
        buf.extend(encode_claim(5, &[3; 16]).unwrap());
        for cut in 0..first {
            assert_eq!(
                decode(&buf[..cut]).unwrap(),
                Decoded::Partial,
                "cut at {cut}"
            );
        }
        let commit = Record::Commit {
            size: 3,
            pages: pages.into_iter().collect(),
        };
        assert_eq!(decode(&buf).unwrap(), Decoded::Frame {
            record: commit,
            len: first
        });
        assert_eq!(decode(&buf[first..]).unwrap(), Decoded::Frame {
            record: Record::Claim {
                epoch: 5,
                nonce: [3; 16]
            },
            len: buf.len() - first
        });
        buf[first - 1] ^= 1;
        assert!(matches!(decode(&buf), Err(FrameError::Checksum)));
    }

    // A frame of another format fails closed and names what it found (§7, versions).
    #[test]
    fn another_format_is_refused_by_its_magic() {
        let pages = BTreeMap::from([(1, vec![7u8; PAGE])]);
        let (mut buf, _) = encode_commit(1, &pages).unwrap();
        buf[3] = b'2';
        let err = decode(&buf).unwrap_err();
        assert!(matches!(&err, FrameError::Magic { found } if found == b"USQ2"));
        assert!(
            err.to_string()
                .starts_with("frame magic \"USQ2\": this extension reads \"USQ1\"")
        );
        assert!(matches!(decode(b"UX"), Err(FrameError::Magic { found }) if found == b"UX"));
    }
}
