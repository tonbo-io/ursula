#![expect(
    clippy::arithmetic_side_effects,
    reason = "tests compute cuts, bits and positions from small generated sizes"
)]

use std::io::Write;

use proptest::collection::btree_map;
use proptest::collection::vec;
use proptest::prelude::Strategy;
use proptest::prelude::any;
use proptest::prop_assert;
use proptest::prop_assert_eq;
use proptest::prop_oneof;
use proptest::proptest;
use ursula_headers::Epoch;
use ursula_headers::MAX_PRODUCER_INTEGER;

use super::CommitRecord;
use super::Decoded;
use super::Frame;
use super::FrameError;
use super::PAGE;
use super::PartRun;
use super::RecordReader;
use super::decompress;
use super::encode;
use super::encode_claim;
use super::encode_commit;
use crate::ids::Activation;
use crate::ids::CommitId;
use crate::ids::Lineage;
use crate::ids::Position;

fn epoch(e: u64) -> Epoch {
    Epoch::try_from(e).unwrap()
}

/// The frame limit the tests decode under.
const MAX_BODY: u64 = 1 << 24;

fn decode(buf: &[u8]) -> Result<Decoded, FrameError> {
    super::decode(buf, MAX_BODY)
}

fn id() -> impl Strategy<Value = CommitId> {
    (any::<[u8; 16]>(), any::<u64>()).prop_map(|(l, p)| CommitId {
        lineage: Lineage(l),
        position: Position(p),
    })
}

/// A page that compresses well but differs from page to page.
fn page(fill: u8, head: [u8; 8]) -> Box<[u8; PAGE]> {
    let mut p = Box::new([fill; PAGE]);
    p[..8].copy_from_slice(&head);
    p
}

fn record() -> impl Strategy<Value = CommitRecord> {
    btree_map(1u32..200, (any::<u8>(), any::<[u8; 8]>()), 0..6).prop_flat_map(|pages| {
        let top = pages.keys().last().copied().unwrap_or(1);
        (top..top + 50).prop_map(move |db_pages_after| CommitRecord {
            db_pages_after,
            pages: pages
                .iter()
                .map(|(&pgno, &(fill, head))| (pgno, page(fill, head)))
                .collect(),
        })
    })
}

fn claim() -> impl Strategy<Value = Frame> {
    (0..=MAX_PRODUCER_INTEGER, any::<[u8; 16]>()).prop_map(|(e, a)| Frame::Claim {
        epoch: epoch(e),
        activation: Activation(a),
    })
}

fn commit() -> impl Strategy<Value = (Frame, CommitRecord)> {
    (id(), record()).prop_map(|(id, record)| {
        let Decoded::Frame { frame, .. } = decode(&encode_commit(id, &record).unwrap()).unwrap()
        else {
            panic!("a whole frame decodes")
        };
        (frame, record)
    })
}

fn part() -> impl Strategy<Value = Frame> {
    (id(), 2u32..10, vec(any::<u8>(), 1..40))
        .prop_flat_map(|(id, count, chunk)| {
            (0..count).prop_map(move |index| Frame::Part {
                id,
                index,
                count,
                chunk: chunk.clone(),
            })
        })
        .boxed()
}

fn any_frame() -> impl Strategy<Value = Frame> {
    prop_oneof![claim(), commit().prop_map(|(f, _)| f), part()]
}

/// The COMMIT `frame` cut into `count` parts at the given proportions.
fn split(frame: &Frame, cuts: &[usize]) -> Vec<Frame> {
    let Frame::Commit { id, compressed } = frame else {
        panic!("a commit")
    };
    let mut at: Vec<usize> = cuts
        .iter()
        .map(|c| 1 + c % (compressed.len() - 1))
        .collect();
    at.sort_unstable();
    at.dedup();
    let mut bounds = vec![0];
    bounds.extend(at);
    bounds.push(compressed.len());
    let count = (bounds.len() - 1) as u32;
    bounds
        .windows(2)
        .enumerate()
        .map(|(i, w)| Frame::Part {
            id: *id,
            index: i as u32,
            count,
            chunk: compressed[w[0]..w[1]].to_vec(),
        })
        .collect()
}

fn decoded(bytes: &[u8]) -> Frame {
    match decode(bytes).unwrap() {
        Decoded::Frame { frame, len } => {
            assert_eq!(len, bytes.len());
            frame
        }
        Decoded::Partial => panic!("a whole frame is not partial"),
    }
}

proptest! {
    // Every frame decodes to itself, and a commit's record comes back exactly.
    #[test]
    fn frames_round_trip(frame in any_frame()) {
        prop_assert_eq!(decoded(&encode(&frame).unwrap()), frame);
    }

    #[test]
    fn commit_records_round_trip((frame, record) in commit()) {
        let Frame::Commit { compressed, .. } = frame else { panic!("a commit") };
        prop_assert_eq!(decompress(&compressed).unwrap(), record);
    }

    // Reads cut frames anywhere: every strict prefix is Partial, also with a frame following.
    #[test]
    fn every_strict_prefix_is_partial(frame in any_frame(), next in any_frame()) {
        let bytes = encode(&frame).unwrap();
        for cut in 0..bytes.len() {
            prop_assert_eq!(decode(&bytes[..cut]).unwrap(), Decoded::Partial, "cut at {}", cut);
        }
        let mut two = bytes.clone();
        two.extend(encode(&next).unwrap());
        prop_assert_eq!(decode(&two).unwrap(), Decoded::Frame { frame, len: bytes.len() });
    }

    // No single damaged bit makes another valid frame: the checksum covers the kind and the body,
    // and a length no frame of the kind can have is refused.
    #[test]
    fn a_flipped_bit_is_never_another_frame(frame in any_frame()) {
        let bytes = encode(&frame).unwrap();
        for bit in 0..bytes.len() * 8 {
            let mut bad = bytes.clone();
            bad[bit / 8] ^= 1 << (bit % 8);
            prop_assert!(
                matches!(decode(&bad), Err(_) | Ok(Decoded::Partial)),
                "bit {} gave {:?}", bit, decode(&bad)
            );
        }
    }

    // A complete run reassembles into the original commit, whatever the cuts.
    #[test]
    fn a_complete_run_is_the_commit((frame, _) in commit(), cuts in vec(any::<usize>(), 1..6)) {
        let parts = split(&frame, &cuts);
        prop_assert!(parts.len() >= 2);
        let mut run = PartRun::default();
        let (last, init) = parts.split_last().unwrap();
        for p in init {
            prop_assert_eq!(run.push(decoded(&encode(p).unwrap())).unwrap(), None);
        }
        prop_assert_eq!(run.push(decoded(&encode(last).unwrap())).unwrap(), Some(frame));
    }

    // Any other frame inside a run voids it: the frame passes through, and the run's remaining
    // parts complete nothing.
    #[test]
    fn a_foreign_frame_voids_the_run(
        (frame, _) in commit(),
        cuts in vec(any::<usize>(), 1..6),
        foreign in any_frame(),
        at in any::<usize>(),
    ) {
        let parts = split(&frame, &cuts);
        let at = 1 + at % (parts.len() - 1);
        let mut run = PartRun::default();
        for p in &parts[..at] {
            prop_assert_eq!(run.push(p.clone()).unwrap(), None);
        }
        let continues = matches!(
            &foreign,
            Frame::Part { id, index, count, .. }
                if Some(id) == match &frame { Frame::Commit { id, .. } => Some(id), _ => None }
                    && *index as usize == at && *count as usize == parts.len()
        );
        proptest::prop_assume!(!continues);
        let out = run.push(foreign.clone()).unwrap();
        match &foreign {
            Frame::Part { .. } => prop_assert_eq!(out, None),
            other => prop_assert_eq!(out.as_ref(), Some(other)),
        }
        for p in &parts[at..] {
            prop_assert_eq!(run.push(p.clone()).unwrap(), None);
        }
    }

    // Parts of the run's own commit that do not continue it void it too: a skipped index, a
    // repeated index or another count. A restart at 0 starts the run again.
    #[test]
    fn a_part_of_the_same_commit_out_of_turn_voids_the_run(
        (frame, _) in commit(),
        cuts in vec(any::<usize>(), 2..6),
        at in any::<usize>(),
        how in 0u8..3,
    ) {
        let parts = split(&frame, &cuts);
        let count = parts.len() as u32;
        proptest::prop_assume!(count >= 3);
        let at = 1 + at % (parts.len() - 1);
        let Frame::Part { id, chunk, .. } = parts[at].clone() else { panic!("a part") };
        let foreign = match how {
            0 if at + 1 < parts.len() => Frame::Part { id, index: at as u32 + 1, count, chunk },
            1 if at >= 2 => parts[at - 1].clone(),
            _ => Frame::Part { id, index: at as u32, count: count + 1, chunk },
        };
        let mut run = PartRun::default();
        for p in &parts[..at] {
            prop_assert_eq!(run.push(p.clone()).unwrap(), None);
        }
        prop_assert_eq!(run.push(foreign).unwrap(), None);
        for p in &parts[at..] {
            prop_assert_eq!(run.push(p.clone()).unwrap(), None);
        }
        // A restart at 0 mid-run, then the whole run, completes the commit.
        let mut run = PartRun::default();
        for p in &parts[..at] {
            prop_assert_eq!(run.push(p.clone()).unwrap(), None);
        }
        let (last, init) = parts.split_last().unwrap();
        for p in init {
            prop_assert_eq!(run.push(p.clone()).unwrap(), None);
        }
        prop_assert_eq!(run.push(last.clone()).unwrap(), Some(frame));
    }
}

#[test]
fn a_run_restarted_whole_completes() {
    let id = CommitId {
        lineage: Lineage([4; 16]),
        position: Position(9),
    };
    let part = |index, count| Frame::Part {
        id,
        index,
        count,
        chunk: vec![index as u8],
    };
    let mut run = PartRun::default();
    assert_eq!(run.push(part(0, 3)).unwrap(), None);
    assert_eq!(run.push(part(1, 3)).unwrap(), None);
    // Resent whole, cut again under a smaller limit.
    assert_eq!(run.push(part(0, 2)).unwrap(), None);
    assert_eq!(
        run.push(part(1, 2)).unwrap(),
        Some(Frame::Commit {
            id,
            compressed: vec![0, 1]
        })
    );
    assert!(matches!(
        run.push(part(2, 2)),
        Err(FrameError::PartIndex { index: 2, count: 2 })
    ));
}

#[test]
fn claims_have_a_fixed_layout() {
    let bytes = encode_claim(epoch(7), Activation([3; 16]));
    assert_eq!(bytes.len(), 13 + 24);
    assert_eq!(&bytes[..5], b"USQ2\x01");
    assert_eq!(decoded(&bytes), Frame::Claim {
        epoch: epoch(7),
        activation: Activation([3; 16])
    });
}

#[test]
fn records_out_of_order_or_outside_the_database_are_refused() {
    let id = CommitId {
        lineage: Lineage([1; 16]),
        position: Position(1),
    };
    let rec = |db_pages_after, pgnos: &[u32]| CommitRecord {
        db_pages_after,
        pages: pgnos.iter().map(|&p| (p, page(0, [0; 8]))).collect(),
    };
    assert!(matches!(
        encode_commit(id, &rec(3, &[2, 1])),
        Err(FrameError::PageOrder {
            pgno: 1,
            previous: 2
        })
    ));
    assert!(matches!(
        encode_commit(id, &rec(3, &[2, 2])),
        Err(FrameError::PageOrder {
            pgno: 2,
            previous: 2
        })
    ));
    assert!(matches!(
        encode_commit(id, &rec(3, &[0])),
        Err(FrameError::PageOutside {
            pgno: 0,
            db_pages_after: 3
        })
    ));
    assert!(matches!(
        encode_commit(id, &rec(3, &[4])),
        Err(FrameError::PageOutside {
            pgno: 4,
            db_pages_after: 3
        })
    ));
}

/// `raw` compressed without a declared content size.
fn compress_unsized(raw: &[u8]) -> Vec<u8> {
    let mut z = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
    z.write_all(raw).unwrap();
    z.finish().unwrap()
}

fn record_head(db_pages_after: u32, pages: u32) -> Vec<u8> {
    let mut raw = db_pages_after.to_le_bytes().to_vec();
    raw.extend(pages.to_le_bytes());
    raw
}

#[test]
fn a_record_whose_size_disagrees_is_refused() {
    // The declared content size is not the size the record header implies.
    let wrong = zstd::bulk::compress(&record_head(1_000_000, 1_000_000), 3).unwrap();
    assert!(matches!(
        decompress(&wrong),
        Err(FrameError::ContentSize {
            pages: 1_000_000,
            declared: 8,
            ..
        })
    ));
    // Without a declared size: fewer pages than the header says, or bytes after the last page.
    let mut one = record_head(2, 2);
    one.extend(1u32.to_le_bytes());
    one.extend([0u8; PAGE]);
    assert!(matches!(
        decompress(&compress_unsized(&one)),
        Err(FrameError::RecordShort { pages: 2, .. })
    ));
    let mut long = record_head(1, 1);
    long.extend(1u32.to_le_bytes());
    long.extend([0u8; PAGE + 1]);
    assert!(matches!(
        decompress(&compress_unsized(&long)),
        Err(FrameError::RecordLong { pages: 1, .. })
    ));
    assert!(matches!(
        decompress(&compress_unsized(&[0u8; 5])),
        Err(FrameError::RecordShort { pages: 0, .. })
    ));
    assert!(matches!(
        decompress(b"not zstd"),
        Err(FrameError::Decompress(_))
    ));
}

// A tiny frame that declares a huge record is read as a stream: nothing is sized from the
// declaration, and it fails as soon as its bytes run out.
#[test]
fn a_huge_declared_size_fails_fast_with_bounded_memory() {
    let pages = u32::MAX;
    let declared = 8 + u64::from(pages) * (4 + PAGE as u64);
    let mut z = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
    z.include_contentsize(true).unwrap();
    z.set_pledged_src_size(Some(declared)).unwrap();
    z.write_all(&record_head(pages, pages)).unwrap();
    z.write_all(&1u32.to_le_bytes()).unwrap();
    z.flush().unwrap();
    let tiny = z.get_ref().clone();
    assert!(tiny.len() < 64);
    assert_eq!(
        zstd::zstd_safe::get_frame_content_size(&tiny).unwrap(),
        Some(declared)
    );
    let mut reader = RecordReader::new(&tiny).unwrap();
    assert_eq!(reader.header().pages, pages);
    let mut image = [0u8; PAGE];
    assert!(matches!(
        reader.next_page(&mut image),
        Err(FrameError::RecordShort { pages: u32::MAX, expected }) if expected == declared
    ));
    // The reader holds no record buffer: pages land in the caller's one-page buffer.
    assert!(std::mem::size_of::<RecordReader<'static>>() < 256);
}

// A frame that asks for a window above 4 MiB is refused before it is decoded.
#[test]
fn a_frame_with_a_larger_window_is_refused() {
    let mut z = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
    z.window_log(23).unwrap();
    z.write_all(&record_head(0, 0)).unwrap();
    let wide = z.finish().unwrap();
    assert!(matches!(decompress(&wide), Err(FrameError::Decompress(_))));
    // The same record under the accepted window decodes.
    assert_eq!(
        decompress(&compress_unsized(&record_head(0, 0))).unwrap(),
        CommitRecord {
            db_pages_after: 0,
            pages: Vec::new()
        }
    );
}

// The zstd frame carries a content checksum, so damage the frame checksum missed still fails.
#[test]
fn records_carry_a_content_checksum() {
    let id = CommitId {
        lineage: Lineage([1; 16]),
        position: Position(1),
    };
    let record = CommitRecord {
        db_pages_after: 1,
        pages: vec![(1, page(3, [1; 8]))],
    };
    let Frame::Commit { mut compressed, .. } = decoded(&encode_commit(id, &record).unwrap()) else {
        panic!("a commit")
    };
    // The frame header descriptor's checksum flag.
    assert_eq!(compressed[4] & 0b100, 0b100);
    let last = compressed.len() - 1;
    compressed[last] ^= 1;
    assert!(matches!(
        decompress(&compressed),
        Err(FrameError::Decompress(_))
    ));
}

// A body above the reader's limit is an error as soon as its length is read, never Partial.
#[test]
fn a_body_above_the_limit_is_an_error() {
    let claim = encode_claim(epoch(1), Activation([0; 16]));
    assert!(matches!(
        super::decode(&claim, 10),
        Err(FrameError::BodyTooLarge { len: 24, max: 10 })
    ));
    let mut head = b"USQ2\x02".to_vec();
    head.extend(u32::MAX.to_le_bytes());
    assert!(matches!(
        decode(&head),
        Err(FrameError::BodyTooLarge { len, max: MAX_BODY }) if len == u64::from(u32::MAX)
    ));
}

// A frame of another format fails closed and names what it found.
#[test]
fn another_format_is_refused_by_its_magic() {
    let mut bytes = encode_claim(epoch(1), Activation([0; 16]));
    bytes[3] = b'1';
    let err = decode(&bytes).unwrap_err();
    assert!(matches!(&err, FrameError::Magic { found } if found == b"USQ1"));
    assert!(
        err.to_string()
            .starts_with("frame magic \"USQ1\": this extension reads \"USQ2\"")
    );
    assert!(matches!(decode(b"UX"), Err(FrameError::Magic { found }) if found == b"UX"));
    let mut kind = encode_claim(epoch(1), Activation([0; 16]));
    kind[4] = 9;
    assert!(matches!(
        decode(&kind),
        Err(FrameError::UnknownKind { kind: 9 })
    ));
}

// An epoch outside the protocol's range is refused, though the frame checksum verifies.
#[test]
fn a_claim_epoch_out_of_range_is_refused() {
    let mut body = (MAX_PRODUCER_INTEGER + 1).to_le_bytes().to_vec();
    body.extend([0u8; 16]);
    let frame = super::assemble(1, 24, &body);
    assert!(matches!(decode(&frame), Err(FrameError::Epoch(_))));
}

#[test]
fn part_frames_cut_a_commit_and_join_back_to_it() {
    let id = CommitId {
        lineage: Lineage([7; 16]),
        position: Position(9),
    };
    let compressed: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
    let commit = encode(&Frame::Commit { id, compressed }).unwrap();
    let count = super::part_count(&commit, 300).unwrap();
    assert_eq!(count, 4);
    let mut run = PartRun::default();
    let mut joined = None;
    for index in 0..count {
        let part = super::part_frame(&commit, index, count, 300).unwrap();
        assert_eq!(super::part_index(&part), Some(index));
        let last = if index + 1 == count { 100 } else { 300 };
        assert_eq!(part.len(), super::PART_OVERHEAD + last);
        joined = run.push(decoded(&part)).unwrap();
    }
    assert_eq!(encode(&joined.unwrap()).unwrap(), commit);
    // A count that does not cover the record exactly, an empty chunk, an index past the run.
    assert!(matches!(
        super::part_frame(&commit, 0, 3, 300),
        Err(FrameError::PartChunk { count: 3, .. })
    ));
    assert!(matches!(
        super::part_count(&commit, 0),
        Err(FrameError::PartChunk { chunk: 0, .. })
    ));
    assert!(matches!(
        super::part_frame(&commit, 4, 4, 300),
        Err(FrameError::PartIndex { index: 4, count: 4 })
    ));
    // Only a COMMIT frame cuts.
    let claim = encode_claim(epoch(1), Activation([1; 16]));
    assert!(matches!(
        super::part_count(&claim, 300),
        Err(FrameError::NotCommit { kind: 1 })
    ));
    assert_eq!(super::part_index(&commit), None);
    assert_eq!(super::part_index(&claim), None);
}

// A run whose parts before the last are as long as part 0 cannot fit a COMMIT frame (a body of at
// most 2^32 - 1 bytes) once `(count - 1) x len(part 0) + 1` passes it: refused at part 0, before
// anything is joined. A run that fits starts as usual.
#[test]
fn a_run_no_commit_frame_holds_is_refused_at_its_first_part() {
    let id = CommitId {
        lineage: Lineage([5; 16]),
        position: Position(3),
    };
    let chunk = vec![7u8; 4 << 20];
    let part = |count| Frame::Part {
        id,
        index: 0,
        count,
        chunk: chunk.clone(),
    };
    let mut run = PartRun::default();
    // 1023 x 4 MiB + 1 fits under 2^32 - 25.
    assert_eq!(run.push(part(1024)).unwrap(), None);
    // 1024 x 4 MiB + 1 does not.
    assert!(matches!(
        run.push(part(1025)),
        Err(FrameError::RunTooLong { count: 1025, len, .. }) if len == (1024u64 << 22) + 1
    ));
    // A short part 0 of a long run fits.
    let short = Frame::Part {
        id,
        index: 0,
        count: 1 << 20,
        chunk: vec![1],
    };
    assert_eq!(run.push(short).unwrap(), None);
}
