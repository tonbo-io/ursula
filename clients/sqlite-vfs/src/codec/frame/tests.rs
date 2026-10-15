#![expect(
    clippy::arithmetic_side_effects,
    reason = "tests compute cuts, bits and positions from small generated sizes"
)]

use proptest::collection::btree_map;
use proptest::collection::vec;
use proptest::prelude::Strategy;
use proptest::prelude::any;
use proptest::prop_assert;
use proptest::prop_assert_eq;
use proptest::prop_oneof;
use proptest::proptest;

use super::CommitRecord;
use super::Decoded;
use super::Frame;
use super::FrameError;
use super::PAGE;
use super::PartRun;
use super::decode;
use super::decompress;
use super::encode;
use super::encode_claim;
use super::encode_commit;
use crate::ids::Activation;
use crate::ids::CommitId;
use crate::ids::Epoch;
use crate::ids::Lineage;
use crate::ids::Position;

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
    (any::<u64>(), any::<[u8; 16]>()).prop_map(|(e, a)| Frame::Claim {
        epoch: Epoch(e),
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
    let bytes = encode_claim(Epoch(7), Activation([3; 16]));
    assert_eq!(bytes.len(), 13 + 24);
    assert_eq!(&bytes[..5], b"USQ2\x01");
    assert_eq!(decoded(&bytes), Frame::Claim {
        epoch: Epoch(7),
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

#[test]
fn a_record_of_the_wrong_size_is_refused_before_allocating() {
    // Not a whole record by its declared size.
    let short = zstd::bulk::compress(&[0u8; 9], 3).unwrap();
    assert!(matches!(
        decompress(&short),
        Err(FrameError::ContentSize { found: Some(9) })
    ));
    // A whole record by size whose count disagrees.
    let mut raw = vec![0u8; 8 + 4 + PAGE];
    raw[0] = 1;
    raw[4] = 2;
    let wrong = zstd::bulk::compress(&raw, 3).unwrap();
    assert!(matches!(
        decompress(&wrong),
        Err(FrameError::PageCount { pages: 2, .. })
    ));
    // Not zstd at all.
    assert!(matches!(
        decompress(b"not zstd"),
        Err(FrameError::ContentSize { found: None })
    ));
}

// A frame of another format fails closed and names what it found.
#[test]
fn another_format_is_refused_by_its_magic() {
    let mut bytes = encode_claim(Epoch(1), Activation([0; 16]));
    bytes[3] = b'1';
    let err = decode(&bytes).unwrap_err();
    assert!(matches!(&err, FrameError::Magic { found } if found == b"USQ1"));
    assert!(
        err.to_string()
            .starts_with("frame magic \"USQ1\": this extension reads \"USQ2\"")
    );
    assert!(matches!(decode(b"UX"), Err(FrameError::Magic { found }) if found == b"UX"));
    let mut kind = encode_claim(Epoch(1), Activation([0; 16]));
    kind[4] = 9;
    assert!(matches!(
        decode(&kind),
        Err(FrameError::UnknownKind { kind: 9 })
    ));
}
