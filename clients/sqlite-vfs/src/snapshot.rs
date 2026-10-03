//! Snapshot bodies, published at `PUT {stream}/snapshot/{offset}`: the database's page images at a
//! frame boundary of the stream, plus what a replay of the rest needs.
//!
//! ```text
//! "USS1" | u64 offset | u64 epoch | u32 pages | u32 crc32c(image) | zstd(image)   (little-endian)
//! ```
//!
//! `offset` is the frame boundary the image reflects (every frame before it applied, none after);
//! `epoch` is the highest producer epoch claimed before it (the claims themselves may be trimmed by
//! retention); `image` is pages × 4 KiB, page 1 first, byte-identical to a file built by replaying
//! the stream up to `offset`, so later page-image frames apply on top of it.
use crate::frame::PAGE;

const MAGIC: &[u8; 4] = b"USS1";
const HEADER: usize = 28;
const ZSTD_LEVEL: i32 = 3;

#[derive(Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub offset: u64,
    pub epoch: u64,
    pub image: Vec<u8>,
}

pub fn encode(offset: u64, epoch: u64, image: &[u8]) -> Vec<u8> {
    assert_eq!(image.len() % PAGE, 0, "snapshot image of whole pages");
    let payload = zstd::bulk::compress(image, ZSTD_LEVEL).expect("zstd compress");
    let mut out = Vec::with_capacity(HEADER + payload.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(&epoch.to_le_bytes());
    out.extend_from_slice(&((image.len() / PAGE) as u32).to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(image).to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

pub fn decode(body: &[u8]) -> Result<Snapshot, String> {
    if body.len() < HEADER || &body[..4] != MAGIC {
        return Err("snapshot: bad header".into());
    }
    let u64_at = |i: usize| u64::from_le_bytes(body[i..i + 8].try_into().unwrap());
    let u32_at = |i: usize| u32::from_le_bytes(body[i..i + 4].try_into().unwrap());
    let (offset, epoch, pages, crc) = (u64_at(4), u64_at(12), u32_at(20) as usize, u32_at(24));
    let image = zstd::bulk::decompress(&body[HEADER..], pages * PAGE)
        .map_err(|e| format!("snapshot: zstd: {e}"))?;
    if image.len() != pages * PAGE || crc32c::crc32c(&image) != crc {
        return Err("snapshot: image does not match its size and checksum".into());
    }
    Ok(Snapshot {
        offset,
        epoch,
        image,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // A snapshot carries its offset and epoch through, and a damaged body is refused rather than
    // installed.
    #[test]
    fn snapshots_round_trip_and_damage_is_refused() {
        let image: Vec<u8> = (0..3 * PAGE).map(|i| (i * 7 % 251) as u8).collect();
        let body = encode(4242, 9, &image);
        assert_eq!(
            decode(&body),
            Ok(Snapshot {
                offset: 4242,
                epoch: 9,
                image: image.clone()
            })
        );
        let mut bad = body.clone();
        bad[HEADER - 1] ^= 1; // the checksum
        assert!(decode(&bad).is_err());
        assert!(decode(&body[..body.len() - 1]).is_err());
    }
}
