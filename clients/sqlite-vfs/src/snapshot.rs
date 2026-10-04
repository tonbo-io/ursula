//! Snapshot bodies, published at `PUT {stream}/snapshot/{offset}`: the database's page images at a
//! frame boundary of the stream, plus what a replay of the rest needs.
//!
//! ```text
//! "USS2" | u8 n | n bytes offset | u64 epoch | u32 pages | u32 crc32c(image) | zstd(image)
//! ```
//!
//! (integers little-endian). `offset` is the frame boundary the image reflects, as the server
//! wrote it (opaque, at most 255 bytes; "USS1" held it as a u64): every frame before it applied,
//! none after;
//! `epoch` is the highest producer epoch claimed before it (the claims themselves may be trimmed by
//! retention); `image` is pages × 4 KiB, page 1 first, byte-identical to a file built by replaying
//! the stream up to `offset`, so later page-image frames apply on top of it.
use crate::frame::PAGE;

const MAGIC: &[u8; 4] = b"USS2";
/// The header without the offset's bytes.
const HEADER: usize = 21;
const ZSTD_LEVEL: i32 = 3;

#[derive(Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub offset: String,
    pub epoch: u64,
    pub image: Vec<u8>,
}

pub fn encode(offset: &str, epoch: u64, image: &[u8]) -> Vec<u8> {
    assert_eq!(image.len() % PAGE, 0, "snapshot image of whole pages");
    let n = u8::try_from(offset.len()).expect("an offset of at most 255 bytes");
    let payload = zstd::bulk::compress(image, ZSTD_LEVEL).expect("zstd compress");
    let mut out = Vec::with_capacity(HEADER + offset.len() + payload.len());
    out.extend_from_slice(MAGIC);
    out.push(n);
    out.extend_from_slice(offset.as_bytes());
    out.extend_from_slice(&epoch.to_le_bytes());
    out.extend_from_slice(&((image.len() / PAGE) as u32).to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(image).to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

pub fn decode(body: &[u8]) -> Result<Snapshot, String> {
    let n = body.get(4).map_or(0, |&n| n as usize);
    if body.len() < HEADER + n || &body[..4] != MAGIC {
        return Err("snapshot: bad header".into());
    }
    let offset = std::str::from_utf8(&body[5..5 + n])
        .map_err(|_| "snapshot: bad header".to_owned())?
        .to_owned();
    let body = &body[5 + n..];
    let u64_at = |i: usize| u64::from_le_bytes(body[i..i + 8].try_into().unwrap());
    let u32_at = |i: usize| u32::from_le_bytes(body[i..i + 4].try_into().unwrap());
    let (epoch, pages, crc) = (u64_at(0), u32_at(8) as usize, u32_at(12));
    // The header is not checksummed: size the buffer only once the zstd frame's own content size
    // agrees with it, and allocate fallibly, so a damaged body is an error rather than an abort.
    let payload = &body[HEADER - 5..];
    let len = pages * PAGE;
    match zstd::zstd_safe::get_frame_content_size(payload) {
        Ok(Some(n)) if n == len as u64 => {}
        _ => return Err("snapshot: page count does not match the compressed image".into()),
    }
    let mut image = Vec::new();
    image
        .try_reserve_exact(len)
        .map_err(|e| format!("snapshot: {len} bytes: {e}"))?;
    image.resize(len, 0); // within the reserved capacity
    let n = zstd::bulk::decompress_to_buffer(payload, &mut image[..])
        .map_err(|e| format!("snapshot: zstd: {e}"))?;
    image.truncate(n);
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
        let body = encode("00000000000000004242", 9, &image);
        assert_eq!(
            decode(&body),
            Ok(Snapshot {
                offset: "00000000000000004242".into(),
                epoch: 9,
                image: image.clone()
            })
        );
        let mut bad = body.clone();
        bad[HEADER + 20 - 1] ^= 1; // the checksum
        assert!(decode(&bad).is_err());
        assert!(decode(&body[..body.len() - 1]).is_err());
    }
}
