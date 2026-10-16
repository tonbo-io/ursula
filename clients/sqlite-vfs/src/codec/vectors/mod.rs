//! Golden byte vectors of the 0.8 formats.
//!
//! THE FORMATS ARE FROZEN FOR 0.8.x. Each vector is canonical: its test decodes it and encodes
//! the result again to the identical bytes. A failure here means a change to bytes that 0.8.x
//! extensions write to the stream or to disk, which every 0.8.x extension must keep reading. Fix
//! the code, never the vector. A format change needs a new minor version with new magics.
//!
//! - [`IMPORT_ACTIVATION`]: the activation of every import's claim.
//! - [`CLAIM`], [`COMMIT`] and [`PARTS`]: `USQ2` frames, the last a run of two PART frames that
//!   [`COMMIT`] cuts into.
//! - [`SEGMENT`]: a `USL1` segment header and one record of each kind, and [`FRESH_HEADER`]: a
//!   fresh build's header, with no `supersedes` and no claim.
//! - [`SNAPSHOT_HEADER`]: a `USS3` snapshot body's 56-byte header.
//! - [`MARKER`] and [`EMPTY_PAGE_ONE_BLAKE3`]: the page-1 marker, and page 1 of an empty
//!   generation.
//! - [`MANIFEST`]: a salvage's `manifest.json`.

use std::io::Cursor;

use ursula_headers::Epoch;
use ursula_headers::Incarnation;
use ursula_headers::Offset;

use super::PAGE;
use super::frame;
use super::frame::CommitRecord;
use super::frame::Decoded;
use super::frame::Frame;
use super::marker;
use super::segment;
use super::segment::RecordBody;
use super::segment::SegmentHeader;
use super::segment::Tail;
use super::snapshot;
use super::snapshot::SnapshotFields;
use super::snapshot::SnapshotHeader;
use crate::ids::Activation;
use crate::ids::CommitId;
use crate::ids::GenId;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::salvage::Acknowledged;
use crate::salvage::Manifest;
use crate::salvage::Reason;
use crate::salvage::State;

/// The activation of every import's claim (`frame::IMPORT_ACTIVATION`), which reads of the stream
/// refuse to build from: `ursula-import-v1`.
const IMPORT_ACTIVATION: &str = "757273756c612d696d706f72742d7631";

/// A CLAIM at epoch 3 of activation `22..22`.
const CLAIM: &str = "\
    5553513201180000002e2aed0a\
    0300000000000000 22222222222222222222222222222222";

/// The COMMIT of [`record`] at `(11..11, 5)`. The compressed record is zstd's output, which other
/// zstd versions may write differently: decoders read it, so the frame is frozen as it stands.
const COMMIT: &str = "\
    555351320234000000de0e7652\
    11111111111111111111111111111111 0500000000000000\
    28b52ffd640c0f75000028010000005a0200fcc779f1eb888f1b3777";

/// [`COMMIT`] cut into a run of two PART frames of 14 bytes of its compressed record each.
const PARTS: [&str; 2] = [
    "\
    55535132032e0000009e4d1796\
    11111111111111111111111111111111 0500000000000000 00000000 02000000\
    28b52ffd640c0f75000028010000",
    "\
    55535132032e0000007946d9ea\
    11111111111111111111111111111111 0500000000000000 01000000 02000000\
    005a0200fcc779f1eb888f1b3777",
];

/// The `USL1` segment of [`header`], then a COMMIT record holding [`COMMIT`] at position 1, a
/// CLAIM at 2, an ACK at 3 and a BASE at 4. Every carried field is distinct and non-zero, so a
/// field moved or re-encoded fails.
const SEGMENT: &str = "\
    55534c31 01000000 11111111111111111111111111111111 0100000000000000\
    01 33333333333333333333333333333333 0200 6331 0400 2f622f73\
    0100000000001000 0700000000000000 0900000000000000 9210000000000000 0903000000000000\
    01 0300000000000000 22222222222222222222222222222222 a17d8c27\
    41000000 01 0100000000000000\
    555351320234000000de0e765211111111111111111111111111111111050000000000000028b52ffd640c0f75\
    000028010000005a0200fcc779f1eb888f1b3777 d73b805b\
    18000000 02 0200000000000000 0300000000000000 22222222222222222222222222222222 02da6cc8\
    18000000 03 0300000000000000 0100000000000000 6400000000000000 3200000000000000 5bc160c9\
    08000000 04 0400000000000000 0100000000000000 d9fc9a71";

/// The `USL1` header of [`fresh_header`], a fresh build's first segment: no `supersedes`, an
/// empty cluster id, no claim.
const FRESH_HEADER: &str = "\
    55534c31 01000000 11111111111111111111111111111111 0300000000000000\
    00 0000 0400 2f622f73\
    0200000000001000 0200000000000000 0200000000000000 6300000000000000 0000000000000000\
    00 37ffc760";

/// The header of a snapshot at offset 4242, epoch 3, last commit `(11..11, 5)`, of one page whose
/// bytes are all 0x5a.
const SNAPSHOT_HEADER: &str = "\
    55535333 9210000000000000 0300000000000000 11111111111111111111111111111111\
    0500000000000000 01000000 498cf137 1615e289";

/// The page-1 marker of lineage `11..11`.
const MARKER: &str = "557273756c613038 1111111111111111";

/// BLAKE3 of [`marker::empty_page_one`] for lineage `11..11`.
const EMPTY_PAGE_ONE_BLAKE3: &str =
    "13e797b6a8d26ae5d2c9fd4a89be0d15a8d1e40fd939f3354511cf6b200ef203";

/// A complete salvage's manifest.
const MANIFEST: &str = concat!(
    r#"{"format":1,"state":"complete","reason":"history_changed","#,
    r#""time":"2026-10-16T12:34:56Z","path":"/b/s","cluster_id":"c1","#,
    r#""lineage":"11111111111111111111111111111111","#,
    r#""incarnation":"4503599627370497","stream_incarnation":"4503599627370498","#,
    r#""acknowledged_position":3,"acknowledged_offset":100,"unacknowledged":1,"plain":true}"#
);

fn unhex(text: &str) -> Vec<u8> {
    let digits: Vec<u8> = text
        .bytes()
        .filter(|b| !b.is_ascii_whitespace())
        .map(|b| match b {
            b'0'..=b'9' => b.wrapping_sub(b'0'),
            b'a'..=b'f' => b.wrapping_sub(b'a').wrapping_add(10),
            _ => panic!("not a hex digit: {b}"),
        })
        .collect();
    digits
        .chunks_exact(2)
        .map(|pair| (pair[0] << 4) | pair[1])
        .collect()
}

fn lineage() -> Lineage {
    Lineage([0x11; 16])
}

fn activation() -> Activation {
    Activation([0x22; 16])
}

fn epoch() -> Epoch {
    Epoch::try_from(3).unwrap()
}

fn commit_id() -> CommitId {
    CommitId {
        lineage: lineage(),
        position: Position(5),
    }
}

/// Page 1 of a one-page database, every byte 0x5a.
fn record() -> CommitRecord {
    CommitRecord {
        db_pages_after: 1,
        pages: vec![(1, Box::new([0x5a; PAGE]))],
    }
}

fn header() -> SegmentHeader {
    SegmentHeader {
        lineage: lineage(),
        first_position: Position(1),
        supersedes: Some(GenId([0x33; 16])),
        cluster_id: "c1".to_owned(),
        stream_path: "/b/s".to_owned(),
        incarnation: Incarnation::try_from(4_503_599_627_370_497).unwrap(),
        base: Position(7),
        acked: Position(9),
        acked_offset: Offset::from_u64(4242),
        log_bytes: 777,
        claim: Some((epoch(), activation())),
    }
}

/// The first segment of a fresh build: no generation superseded, no claim yet.
fn fresh_header() -> SegmentHeader {
    SegmentHeader {
        lineage: lineage(),
        first_position: Position(3),
        supersedes: None,
        cluster_id: String::new(),
        stream_path: "/b/s".to_owned(),
        incarnation: Incarnation::try_from(4_503_599_627_370_498).unwrap(),
        base: Position(2),
        acked: Position(2),
        acked_offset: Offset::from_u64(99),
        log_bytes: 0,
        claim: None,
    }
}

fn bodies() -> Vec<RecordBody> {
    vec![
        RecordBody::Commit {
            frame: unhex(COMMIT),
        },
        RecordBody::Claim {
            epoch: epoch(),
            activation: activation(),
        },
        RecordBody::Ack {
            acked: Position(1),
            acked_offset: Offset::from_u64(100),
            log_bytes: 50,
        },
        RecordBody::Base {
            position: Position(1),
        },
    ]
}

fn decoded(bytes: &[u8]) -> Frame {
    match frame::decode(bytes, u64::MAX).unwrap() {
        Decoded::Frame { frame, len } => {
            assert_eq!(len, bytes.len());
            frame
        }
        Decoded::Partial => panic!("a partial frame"),
    }
}

#[test]
fn the_claim_frame_is_frozen() {
    let bytes = unhex(CLAIM);
    let frame = decoded(&bytes);
    assert_eq!(frame, Frame::Claim {
        epoch: epoch(),
        activation: activation(),
    });
    assert_eq!(frame::encode(&frame).unwrap(), bytes);
    assert_eq!(frame::encode_claim(epoch(), activation()), bytes);
}

#[test]
fn the_import_activation_is_frozen() {
    assert_eq!(
        frame::IMPORT_ACTIVATION.0.to_vec(),
        unhex(IMPORT_ACTIVATION)
    );
    let claim = frame::encode_claim(epoch(), frame::IMPORT_ACTIVATION);
    assert!(frame::is_import_claim(&decoded(&claim)));
    assert!(!frame::is_import_claim(&decoded(&unhex(CLAIM))));
}

#[test]
fn the_commit_frame_is_frozen() {
    let bytes = unhex(COMMIT);
    let frame = decoded(&bytes);
    let Frame::Commit { id, compressed } = &frame else {
        panic!("not a commit: {frame:?}");
    };
    assert_eq!(*id, commit_id());
    assert_eq!(frame::decompress(compressed).unwrap(), record());
    assert_eq!(frame::encode(&frame).unwrap(), bytes);
}

#[test]
fn the_part_frames_are_frozen() {
    let commit = unhex(COMMIT);
    let mut run = frame::PartRun::default();
    let mut joined = None;
    for (index, text) in (0u32..).zip(PARTS) {
        let bytes = unhex(text);
        let frame = decoded(&bytes);
        let Frame::Part {
            id,
            index: at,
            count,
            chunk,
        } = &frame
        else {
            panic!("not a part: {frame:?}");
        };
        assert_eq!((*id, *at, *count, chunk.len()), (commit_id(), index, 2, 14));
        assert_eq!(frame::encode(&frame).unwrap(), bytes);
        assert_eq!(frame::part_frame(&commit, index, 2, 14).unwrap(), bytes);
        assert_eq!(frame::part_index(&bytes), Some(index));
        joined = run.push(frame).unwrap();
    }
    assert_eq!(frame::part_count(&commit, 14).unwrap(), 2);
    let joined = joined.expect("the run completes");
    assert_eq!(frame::encode(&joined).unwrap(), commit);
}

#[test]
fn the_log_segment_is_frozen() {
    let bytes = unhex(SEGMENT);
    let scan = segment::scan(&bytes).unwrap();
    assert_eq!(scan.header, header());
    assert_eq!(scan.tail, Tail::Clean);
    let positions: Vec<Position> = scan.records.iter().map(|r| r.position).collect();
    assert_eq!(positions, [1, 2, 3, 4].map(Position));
    let read: Vec<RecordBody> = scan.records.iter().map(|r| r.body.clone()).collect();
    assert_eq!(read, bodies());
    let (mut again, mut chain) = segment::encode_header(&scan.header).unwrap();
    assert_eq!(chain, scan.seed);
    for r in &scan.records {
        let (record, next) = segment::encode_record(chain, r.position, &r.body).unwrap();
        again.extend_from_slice(&record);
        chain = next;
    }
    assert_eq!(again, bytes);
}

#[test]
fn a_fresh_builds_segment_header_is_frozen() {
    let bytes = unhex(FRESH_HEADER);
    let scan = segment::scan(&bytes).unwrap();
    assert_eq!(scan.header, fresh_header());
    assert_eq!(scan.header_len, bytes.len());
    assert_eq!(scan.records, []);
    assert_eq!(scan.tail, Tail::Clean);
    let (again, seed) = segment::encode_header(&scan.header).unwrap();
    assert_eq!(seed, scan.seed);
    assert_eq!(again, bytes);
}

#[test]
fn the_snapshot_header_is_frozen() {
    let bytes = unhex(SNAPSHOT_HEADER);
    let (header, start) = snapshot::read_header(&bytes).unwrap();
    assert_eq!(start, snapshot::HEADER);
    assert_eq!(header, SnapshotHeader {
        fields: SnapshotFields {
            offset: Offset::from_u64(4242),
            epoch: epoch(),
            last: commit_id(),
        },
        pages: 1,
        crc: crc32c::crc32c(&[0x5a; PAGE]),
    });
    let mut body = Vec::new();
    let written = snapshot::write_body(
        &header.fields,
        &mut Cursor::new(vec![0x5a; PAGE]),
        &mut body,
    )
    .unwrap();
    assert_eq!(written, header);
    assert_eq!(body.get(..snapshot::HEADER), Some(bytes.as_slice()));
}

#[test]
fn the_page_one_marker_is_frozen() {
    let bytes = unhex(MARKER);
    assert!(marker::is_marked(&bytes, lineage()));
    assert_eq!(marker::mark(lineage()).as_slice(), bytes.as_slice());
    let mut read = bytes.clone();
    marker::on_read(&mut read, 0, lineage()).unwrap();
    assert_eq!(read, marker::SQLITE_MAGIC);
    assert_eq!(
        marker::on_write(&read, 0, lineage()).unwrap(),
        Some(bytes.clone())
    );
    let page = marker::empty_page_one(lineage());
    assert_eq!(page.get(..marker::LEN), Some(bytes.as_slice()));
    assert_eq!(
        blake3::hash(page.as_slice()).to_hex().as_str(),
        EMPTY_PAGE_ONE_BLAKE3
    );
}

#[test]
fn the_salvage_manifest_is_frozen() {
    let manifest = Manifest::parse(MANIFEST).unwrap();
    assert_eq!(manifest, Manifest {
        reason: Reason::HistoryChanged,
        state: State::Complete,
        time: "2026-10-16T12:34:56Z".to_owned(),
        path: "/b/s".to_owned(),
        cluster_id: Some("c1".to_owned()),
        lineage: Some(lineage()),
        incarnation: Some(Incarnation::try_from(4_503_599_627_370_497).unwrap()),
        stream_incarnation: Some(Incarnation::try_from(4_503_599_627_370_498).unwrap()),
        acknowledged: Some(Acknowledged {
            position: Position(3),
            offset: Offset::from_u64(100),
            unacknowledged: 1,
        }),
        plain: true,
    });
    assert_eq!(manifest.render(), MANIFEST);
}
