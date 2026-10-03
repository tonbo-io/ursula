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

fn wrap(record: &[u8]) -> Vec<u8> {
    let payload = zstd::bulk::compress(record, ZSTD_LEVEL).expect("zstd compress");
    let mut out = Vec::with_capacity(HEADER + payload.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

/// A commit frame and the uncompressed record size.
pub fn encode_commit(size: u32, pages: &BTreeMap<u32, Vec<u8>>) -> (Vec<u8>, usize) {
    let mut record = Vec::with_capacity(9 + pages.len() * (4 + PAGE));
    record.push(0);
    record.extend_from_slice(&size.to_le_bytes());
    record.extend_from_slice(&(pages.len() as u32).to_le_bytes());
    for (pgno, data) in pages {
        record.extend_from_slice(&pgno.to_le_bytes());
        record.extend_from_slice(data);
    }
    (wrap(&record), record.len())
}

pub fn encode_claim(epoch: u64, nonce: &[u8; 16]) -> Vec<u8> {
    let mut record = vec![1];
    record.extend_from_slice(&epoch.to_le_bytes());
    record.extend_from_slice(nonce);
    wrap(&record)
}

fn le32(b: &[u8]) -> u32 {
    u32::from_le_bytes(b[..4].try_into().unwrap())
}

/// Decodes the frame at the start of `buf`.
pub fn decode(buf: &[u8]) -> Result<Decoded, String> {
    if buf.len() < HEADER {
        if !MAGIC.starts_with(&buf[..buf.len().min(4)]) {
            return Err("bad frame magic".into());
        }
        return Ok(Decoded::Partial);
    }
    if &buf[..4] != MAGIC {
        return Err("bad frame magic".into());
    }
    let len = le32(&buf[4..]) as usize;
    let Some(payload) = buf.get(HEADER..HEADER + len) else {
        return Ok(Decoded::Partial);
    };
    if crc32c::crc32c(payload) != le32(&buf[8..]) {
        return Err("frame checksum mismatch".into());
    }
    let record = zstd::stream::decode_all(payload).map_err(|e| format!("zstd: {e}"))?;
    let record = match record.first() {
        Some(0) if record.len() >= 9 => {
            let (size, n) = (le32(&record[1..]), le32(&record[5..]) as usize);
            let body = &record[9..];
            if body.len() != n * (4 + PAGE) {
                return Err(format!("commit record: {} bytes for {n} pages", body.len()));
            }
            let mut pages = Vec::with_capacity(n);
            for chunk in body.chunks_exact(4 + PAGE) {
                let pgno = le32(chunk);
                if pgno == 0 || pgno > size {
                    return Err(format!("commit record: page {pgno} outside db size {size}"));
                }
                pages.push((pgno, chunk[4..].to_vec()));
            }
            Record::Commit { size, pages }
        }
        Some(1) if record.len() == 25 => Record::Claim {
            epoch: u64::from_le_bytes(record[1..9].try_into().unwrap()),
            nonce: record[9..25].try_into().unwrap(),
        },
        _ => return Err("unknown record".into()),
    };
    Ok(Decoded::Frame {
        record,
        len: HEADER + len,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Reads cut frames anywhere: every prefix of a frame is Partial, the whole frame decodes, and a
    // flipped payload byte is a checksum error rather than a different record.
    #[test]
    fn frames_decode_only_whole_and_intact() {
        let pages = BTreeMap::from([(1, vec![7u8; PAGE]), (3, vec![9u8; PAGE])]);
        let (mut buf, _) = encode_commit(3, &pages);
        let first = buf.len();
        buf.extend(encode_claim(5, &[3; 16]));
        for cut in 0..first {
            assert_eq!(decode(&buf[..cut]), Ok(Decoded::Partial), "cut at {cut}");
        }
        let commit = Record::Commit {
            size: 3,
            pages: pages.into_iter().collect(),
        };
        assert_eq!(
            decode(&buf),
            Ok(Decoded::Frame {
                record: commit,
                len: first
            })
        );
        assert_eq!(
            decode(&buf[first..]),
            Ok(Decoded::Frame {
                record: Record::Claim {
                    epoch: 5,
                    nonce: [3; 16]
                },
                len: buf.len() - first
            })
        );
        buf[first - 1] ^= 1;
        assert_eq!(decode(&buf), Err("frame checksum mismatch".into()));
    }
}
