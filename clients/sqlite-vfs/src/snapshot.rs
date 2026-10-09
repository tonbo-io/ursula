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

/// A snapshot body that cannot be encoded or decoded.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("bad header")]
    Header,
    /// A body of another minor version's format (an incompatible change bumps the magic).
    #[error(
        "snapshot magic \"{}\": this extension reads \"USS2\" snapshots, so the snapshot was \
         taken by an extension of another minor version",
        .found.escape_ascii()
    )]
    Magic { found: [u8; 4] },
    #[error("the offset is not UTF-8: {0}")]
    OffsetUtf8(#[source] std::str::Utf8Error),
    #[error("page count does not match the compressed image")]
    PageCount,
    #[error("{len} bytes: {source}")]
    Alloc {
        len: usize,
        #[source]
        source: std::collections::TryReserveError,
    },
    #[error("zstd: {0}")]
    Zstd(#[source] std::io::Error),
    #[error("image does not match its size and checksum")]
    Checksum,
    #[error("an image of {0} bytes is not whole pages")]
    PartialPage(usize),
    #[error("an offset of {0} bytes (at most 255)")]
    OffsetTooLong(usize),
    #[error("zstd compress: {0}")]
    Compress(#[source] std::io::Error),
    #[error("an image of {0} bytes is too large")]
    TooLarge(usize),
}

pub fn encode(offset: &str, epoch: u64, image: &[u8]) -> Result<Vec<u8>, SnapshotError> {
    if !image.len().is_multiple_of(PAGE) {
        return Err(SnapshotError::PartialPage(image.len()));
    }
    let n = u8::try_from(offset.len())
        .map_err(|_too_long| SnapshotError::OffsetTooLong(offset.len()))?;
    let pages = u32::try_from(image.len() / PAGE)
        .map_err(|_too_many| SnapshotError::TooLarge(image.len()))?;
    let payload = zstd::bulk::compress(image, ZSTD_LEVEL).map_err(SnapshotError::Compress)?;
    let mut out = Vec::with_capacity(
        payload
            .len()
            .saturating_add(HEADER)
            .saturating_add(offset.len()),
    );
    out.extend_from_slice(MAGIC);
    out.push(n);
    out.extend_from_slice(offset.as_bytes());
    out.extend_from_slice(&epoch.to_le_bytes());
    out.extend_from_slice(&pages.to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(image).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

pub fn decode(body: &[u8]) -> Result<Snapshot, SnapshotError> {
    let Some((magic, rest)) = body.split_first_chunk::<4>() else {
        return Err(SnapshotError::Header);
    };
    if magic != MAGIC {
        return Err(SnapshotError::Magic { found: *magic });
    }
    let Some((&n, rest)) = rest.split_first() else {
        return Err(SnapshotError::Header);
    };
    let Some((offset, rest)) = rest.split_at_checked(usize::from(n)) else {
        return Err(SnapshotError::Header);
    };
    let Some((epoch, rest)) = rest.split_first_chunk::<8>() else {
        return Err(SnapshotError::Header);
    };
    let Some((pages, rest)) = rest.split_first_chunk::<4>() else {
        return Err(SnapshotError::Header);
    };
    let Some((crc, payload)) = rest.split_first_chunk::<4>() else {
        return Err(SnapshotError::Header);
    };
    let offset = std::str::from_utf8(offset)
        .map_err(SnapshotError::OffsetUtf8)?
        .to_owned();
    let epoch = u64::from_le_bytes(*epoch);
    let crc = u32::from_le_bytes(*crc);
    // The header is not checksummed: size the buffer only once the zstd frame's own content size
    // agrees with it, and allocate fallibly, so a damaged body is an error rather than an abort.
    let Some(len) = (u32::from_le_bytes(*pages) as usize).checked_mul(PAGE) else {
        return Err(SnapshotError::PageCount);
    };
    match zstd::zstd_safe::get_frame_content_size(payload) {
        Ok(Some(n)) if n == len as u64 => {}
        _ => return Err(SnapshotError::PageCount),
    }
    let mut image = Vec::new();
    image
        .try_reserve_exact(len)
        .map_err(|source| SnapshotError::Alloc { len, source })?;
    image.resize(len, 0); // within the reserved capacity
    let n = zstd::bulk::decompress_to_buffer(payload, image.as_mut_slice())
        .map_err(SnapshotError::Zstd)?;
    image.truncate(n);
    if image.len() != len || crc32c::crc32c(&image) != crc {
        return Err(SnapshotError::Checksum);
    }
    Ok(Snapshot {
        offset,
        epoch,
        image,
    })
}

#[cfg(test)]
mod tests {
    use super::HEADER;
    use super::PAGE;
    use super::Snapshot;
    use super::SnapshotError;
    use super::decode;
    use super::encode;

    // A snapshot carries its offset and epoch through, and a damaged body is refused rather than
    // installed.
    #[test]
    fn snapshots_round_trip_and_damage_is_refused() {
        let image: Vec<u8> = (0..3 * PAGE).map(|i| (i * 7 % 251) as u8).collect();
        let body = encode("00000000000000004242", 9, &image).unwrap();
        assert_eq!(decode(&body).unwrap(), Snapshot {
            offset: "00000000000000004242".into(),
            epoch: 9,
            image: image.clone()
        });
        let mut bad = body.clone();
        bad[HEADER + 20 - 1] ^= 1; // the checksum
        assert!(matches!(decode(&bad), Err(SnapshotError::Checksum)));
        assert!(matches!(
            decode(&body[..body.len() - 1]),
            Err(SnapshotError::Zstd(_))
        ));
        assert!(matches!(
            encode("o", 1, &image[1..]),
            Err(SnapshotError::PartialPage(_))
        ));
    }

    // A body of another format fails closed and names what it found (§7, versions), before
    // anything of its layout is read.
    #[test]
    fn another_format_is_refused_by_its_magic() {
        let mut body = encode("o", 1, &[0u8; PAGE]).unwrap();
        body[3] = b'1';
        let err = decode(&body).unwrap_err();
        assert!(matches!(err, SnapshotError::Magic { found } if &found == b"USS1"));
        assert!(
            err.to_string()
                .starts_with("snapshot magic \"USS1\": this extension reads \"USS2\"")
        );
    }
}
