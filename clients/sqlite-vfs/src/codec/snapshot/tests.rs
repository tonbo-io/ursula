#![expect(
    clippy::arithmetic_side_effects,
    reason = "tests compute cuts, bits and positions from small generated sizes"
)]

use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

use proptest::collection::vec;
use proptest::prelude::any;
use proptest::prop_assert_eq;
use proptest::proptest;
use ursula_headers::Epoch;
use ursula_headers::MAX_PRODUCER_INTEGER;
use ursula_headers::Offset;

use super::HEADER;
use super::PAGE;
use super::SnapshotError;
use super::SnapshotFields;
use super::SnapshotHeader;
use super::read_header;
use super::read_image;
use super::write_body;
use crate::ids::CommitId;
use crate::ids::Lineage;
use crate::ids::Position;

fn epoch(e: u64) -> Epoch {
    Epoch::try_from(e).unwrap()
}

fn fields() -> SnapshotFields {
    SnapshotFields {
        offset: Offset::from_u64(4242),
        epoch: epoch(9),
        last: CommitId {
            lineage: Lineage([5; 16]),
            position: Position(77),
        },
    }
}

fn image(pages: usize) -> Vec<u8> {
    (0..pages * PAGE).map(|i| (i * 7 % 251) as u8).collect()
}

fn body(fields: &SnapshotFields, image: &[u8]) -> (Vec<u8>, SnapshotHeader) {
    let mut out = Vec::new();
    let header = write_body(fields, &mut Cursor::new(image), &mut out).unwrap();
    (out, header)
}

fn read(body: &[u8]) -> Result<(SnapshotHeader, Vec<u8>), SnapshotError> {
    let (header, at) = read_header(body)?;
    let mut out = Vec::new();
    read_image(&header, &mut &body[at..], &mut out)?;
    Ok((header, out))
}

proptest! {
    // A body carries its fields and image through, whatever they are.
    #[test]
    fn bodies_round_trip(
        offset in any::<u64>(),
        epoch_raw in 0..=MAX_PRODUCER_INTEGER,
        lineage in any::<[u8; 16]>(),
        position in any::<u64>(),
        seeds in vec(any::<u8>(), 0..4),
    ) {
        let f = SnapshotFields {
            offset: Offset::from_u64(offset),
            epoch: epoch(epoch_raw),
            last: CommitId { lineage: Lineage(lineage), position: Position(position) },
        };
        let image: Vec<u8> = seeds.iter().flat_map(|&s| [s; PAGE]).collect();
        let (body, header) = body(&f, &image);
        prop_assert_eq!(header.fields, f);
        prop_assert_eq!(header.pages as usize, seeds.len());
        prop_assert_eq!(read(&body).unwrap(), (header, image));
    }
}

#[test]
fn the_image_is_read_from_its_current_position() {
    let mut file = vec![1u8; 10];
    file.extend(image(2));
    let mut cursor = Cursor::new(file);
    cursor.seek(SeekFrom::Start(10)).unwrap();
    let mut out = Vec::new();
    let header = write_body(&fields(), &mut cursor, &mut out).unwrap();
    assert_eq!(header.pages, 2);
    assert_eq!(read(&out).unwrap().1, image(2));
}

// A damaged page count fails before anything is decompressed or written.
#[test]
fn a_damaged_page_count_fails_without_allocating() {
    let (mut body, _) = body(&fields(), &image(3));
    body[44..48].copy_from_slice(&u32::MAX.to_le_bytes());
    let crc = crc32c::crc32c(&body[..52]);
    body[52..56].copy_from_slice(&crc.to_le_bytes());
    let (header, at) = read_header(&body).unwrap();
    assert_eq!(header.pages, u32::MAX);
    let mut out = Vec::new();
    assert!(matches!(
        read_image(&header, &mut &body[at..], &mut out),
        Err(SnapshotError::PageCount { pages: u32::MAX, found: Some(n) }) if n == 3 * PAGE as u64
    ));
    assert!(out.is_empty());
}

#[test]
fn a_checksum_mismatch_is_an_error() {
    let (mut body, header) = body(&fields(), &image(3));
    // The image checksum, with the header checksum fixed up to match.
    body[48] ^= 1;
    let crc = crc32c::crc32c(&body[..52]);
    body[52..56].copy_from_slice(&crc.to_le_bytes());
    assert!(matches!(
        read(&body),
        Err(SnapshotError::Checksum { expected, found }) if found == header.crc && expected != found
    ));
}

#[test]
fn a_cut_or_damaged_image_is_an_error() {
    let (body, _) = body(&fields(), &image(3));
    assert!(matches!(
        read(&body[..body.len() - 1]),
        Err(SnapshotError::Zstd(_) | SnapshotError::ImageLength { .. })
    ));
    assert!(matches!(
        read(&body[..HEADER + 2]),
        Err(SnapshotError::PageCount { found: None, .. })
    ));
    assert!(matches!(
        read(&body[..HEADER - 1]),
        Err(SnapshotError::HeaderTruncated { len }) if len == HEADER - 1
    ));
}

#[test]
fn partial_pages_are_refused() {
    let mut out = Vec::new();
    assert!(matches!(
        write_body(&fields(), &mut Cursor::new(&image(1)[1..]), &mut out),
        Err(SnapshotError::PartialPage { len }) if len == PAGE as u64 - 1
    ));
}

/// An image that changes between `write_body`'s two reads: it grows, or one byte changes.
struct Growing {
    inner: Cursor<Vec<u8>>,
    rewound: bool,
    grow: bool,
}

impl Read for Growing {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Seek for Growing {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        if matches!(pos, SeekFrom::Start(_)) && !self.rewound {
            self.rewound = true;
            let mut more = self.inner.get_ref().clone();
            if self.grow {
                more.extend(image(1));
            } else {
                more[100] ^= 1;
            }
            *self.inner.get_mut() = more;
        }
        self.inner.seek(pos)
    }
}

#[test]
fn an_image_that_changes_between_reads_is_refused() {
    for grow in [true, false] {
        let mut out = Vec::new();
        let mut changing = Growing {
            inner: Cursor::new(image(1)),
            rewound: false,
            grow,
        };
        assert!(matches!(
            write_body(&fields(), &mut changing, &mut out),
            Err(SnapshotError::ImageChanged)
        ));
    }
}

// An image compressed with a window above 4 MiB is refused.
#[test]
fn an_image_with_a_larger_window_is_refused() {
    let image = vec![0u8; 2048 * PAGE];
    let mut z = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
    z.window_log(23).unwrap();
    z.include_contentsize(true).unwrap();
    z.set_pledged_src_size(Some(image.len() as u64)).unwrap();
    std::io::Write::write_all(&mut z, &image).unwrap();
    let compressed = z.finish().unwrap();
    let header = SnapshotHeader {
        fields: fields(),
        pages: 2048,
        crc: crc32c::crc32c(&image),
    };
    let mut out = Vec::new();
    assert!(matches!(
        read_image(&header, &mut &compressed[..], &mut out),
        Err(SnapshotError::Zstd(_))
    ));
    assert!(out.is_empty());
}

// A body of another format fails closed and names what it found.
#[test]
fn another_format_is_refused_by_its_magic() {
    let (mut body, _) = body(&fields(), &image(1));
    body[3] = b'2';
    let err = read_header(&body).unwrap_err();
    assert!(matches!(&err, SnapshotError::Magic { found } if found == b"USS2"));
    assert!(
        err.to_string()
            .starts_with("snapshot magic \"USS2\": this extension reads \"USS3\"")
    );
}

// The header verifies without the body: every damaged header bit is refused.
#[test]
fn every_header_bit_flip_is_refused() {
    let (body, _) = body(&fields(), &image(1));
    for bit in 0..HEADER * 8 {
        let mut bad = body[..HEADER].to_vec();
        bad[bit / 8] ^= 1 << (bit % 8);
        assert!(read_header(&bad).is_err(), "bit {bit}");
    }
    read_header(&body[..HEADER]).unwrap();
}

#[test]
fn an_epoch_out_of_range_is_refused() {
    let (mut body, _) = body(&fields(), &image(1));
    body[12..20].copy_from_slice(&(MAX_PRODUCER_INTEGER + 1).to_le_bytes());
    let crc = crc32c::crc32c(&body[..52]);
    body[52..56].copy_from_slice(&crc.to_le_bytes());
    assert!(matches!(read_header(&body), Err(SnapshotError::Epoch(_))));
}
