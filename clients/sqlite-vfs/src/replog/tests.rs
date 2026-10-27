#![expect(
    clippy::arithmetic_side_effects,
    reason = "tests compute cuts and positions from small generated sizes"
)]

mod oracle;

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use proptest::prelude::Just;
use proptest::prelude::ProptestConfig;
use proptest::prelude::Strategy;
use proptest::prelude::any;
use proptest::prop_assert;
use proptest::prop_assert_eq;
use proptest::prop_oneof;
use proptest::proptest;
use proptest::test_runner::TestCaseError;
use ursula_headers::Epoch;
use ursula_headers::Incarnation;
use ursula_headers::Offset;

use super::Cut;
use super::Fold;
use super::Log;
use super::LogError;
use super::Logged;
use crate::codec::frame::Frame;
use crate::codec::segment::BUFFER;
use crate::codec::segment::Keep;
use crate::codec::segment::RecordBody;
use crate::codec::segment::SegmentError;
use crate::codec::segment::SegmentHeader;
use crate::codec::segment::encode_header;
use crate::codec::segment::encode_record;
use crate::codec::segment::scan;
use crate::ids::Activation;
use crate::ids::CommitId;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::testdir::TempDir;

fn epoch(n: u64) -> Epoch {
    Epoch::try_from(n).unwrap()
}

fn header() -> SegmentHeader {
    SegmentHeader {
        lineage: Lineage([7; 16]),
        first_position: Position(1),
        supersedes: None,
        cluster_id: "cluster".into(),
        stream_path: "/v1/stream/db".into(),
        incarnation: Incarnation::try_from(1 << 52).unwrap(),
        base: Position(0),
        acked: Position(0),
        acked_offset: Offset::from_u64(0),
        log_bytes: 0,
        claim: None,
    }
}

fn commit(n: u8, len: usize) -> RecordBody {
    RecordBody::Commit {
        frame: vec![n; len.max(1)],
    }
}

fn bodies(log: &[RecordBody]) -> Vec<(Position, RecordBody)> {
    log.iter()
        .cloned()
        .zip(1..)
        .map(|(b, p)| (Position(p), b))
        .collect()
}

fn records(r: &super::Recovered) -> Vec<(Position, RecordBody)> {
    r.records
        .iter()
        .map(|r| (r.position, r.body.clone()))
        .collect()
}

#[test]
fn a_log_reopens_with_its_records_and_state() {
    let dir = TempDir::new("log-reopen");
    let log_dir = dir.path().join("log");
    let mut log = Log::create(&log_dir, header()).unwrap();
    let written = vec![
        RecordBody::Claim {
            epoch: epoch(3),
            activation: Activation([1; 16]),
        },
        commit(1, 100),
        commit(2, 100),
        RecordBody::Ack {
            acked: Position(2),
            acked_offset: Offset::from_u64(77),
            log_bytes: 200,
        },
        commit(3, 100),
        RecordBody::Base {
            position: Position(3),
        },
    ];
    for (b, p) in written.iter().zip(1..) {
        assert_eq!(log.append(b).unwrap(), Position(p));
    }
    log.sync().unwrap();
    drop(log);
    let (log, r) = Log::open(&log_dir).unwrap();
    assert_eq!(log.next_position(), Position(7));
    assert_eq!(r.cut, None);
    assert_eq!(
        (r.base, r.acked, r.acked_offset, r.log_bytes),
        (Position(3), Position(2), Offset::from_u64(77), 200)
    );
    assert_eq!(r.claim, Some((epoch(3), Activation([1; 16]))));
    assert_eq!(r.claims, vec![(epoch(3), Activation([1; 16]))]);
    // Records after min(base, acked) = 2.
    assert_eq!(records(&r), bodies(&written)[2..].to_vec());
    let pending: Vec<Position> = r.pending().into_iter().map(|(p, _)| p).collect();
    assert_eq!(pending, vec![Position(3), Position(5)]);
    let after_base: Vec<Position> = r.after_base().map(|r| r.position).collect();
    assert_eq!(after_base, vec![Position(4), Position(5), Position(6)]);
}

#[test]
fn an_injected_write_failure_cuts_the_segment_back() {
    let dir = TempDir::new("log-fail");
    let log_dir = dir.path().join("log");
    let mut log = Log::create(&log_dir, header()).unwrap();
    log.append(&commit(1, 50)).unwrap();
    let len = fs::metadata(&log.segment_paths()[0]).unwrap().len();
    log.fail_next_append = Some(20);
    let err = log.append(&commit(2, 50)).unwrap_err();
    assert!(matches!(err, LogError::CutBack { .. }));
    assert_eq!(fs::metadata(&log.segment_paths()[0]).unwrap().len(), len);
    // The next append takes the position the failed one did not.
    assert_eq!(log.append(&commit(3, 50)).unwrap(), Position(2));
    drop(log);
    let (_, r) = Log::open(&log_dir).unwrap();
    assert_eq!(records(&r), bodies(&[commit(1, 50), commit(3, 50)]));
}

#[test]
fn a_gap_under_valid_chains_is_an_error() {
    let dir = TempDir::new("log-gap");
    let log_dir = dir.path().join("log");
    let mut log = Log::create(&log_dir, header()).unwrap();
    log.append(&commit(1, 10)).unwrap();
    drop(log);
    // A record at position 3 chained onto position 1.
    let path = super::segment_path(&log_dir, Position(1));
    let bytes = fs::read(&path).unwrap();
    let scanned = crate::codec::segment::scan(&bytes).unwrap();
    let chain = scanned.records.last().unwrap().chain;
    let (rec, _) =
        crate::codec::segment::encode_record(chain, Position(3), &commit(3, 10)).unwrap();
    let mut bytes = bytes;
    bytes.extend_from_slice(&rec);
    fs::write(&path, bytes).unwrap();
    let err = Log::open(&log_dir).unwrap_err();
    assert!(matches!(err, LogError::Gap {
        expected: Position(2),
        found: Position(3),
        ..
    }));
}

/// Opens the log, checks the cut, and checks that it takes appends again.
fn reopen_cut(log_dir: &std::path::Path, at: u64, kept: &[RecordBody]) {
    let path = super::segment_path(log_dir, Position(1));
    let (mut log, r) = Log::open(log_dir).unwrap();
    assert_eq!(
        r.cut,
        Some(super::Cut {
            segment: path.clone(),
            at
        })
    );
    assert_eq!(records(&r), bodies(kept));
    assert_eq!(fs::metadata(&path).unwrap().len(), at);
    let next = log.append(&commit(9, 5)).unwrap();
    assert_eq!(next, Position(kept.len() as u64 + 1));
    drop(log);
    let (_, again) = Log::open(log_dir).unwrap();
    assert_eq!(again.cut, None);
}

/// The log with two commits, and the byte offset where the second starts.
fn two_commits(log_dir: &std::path::Path, first: RecordBody) -> u64 {
    let mut log = Log::create(log_dir, header()).unwrap();
    log.append(&first).unwrap();
    let second = fs::metadata(super::segment_path(log_dir, Position(1)))
        .unwrap()
        .len();
    log.append(&commit(2, 40)).unwrap();
    second
}

#[test]
fn a_torn_tail_is_cut() {
    let dir = TempDir::new("log-torn");
    let log_dir = dir.path().join("log");
    let second = two_commits(&log_dir, commit(1, 40));
    let path = super::segment_path(&log_dir, Position(1));
    let bytes = fs::read(&path).unwrap();
    fs::write(&path, &bytes[..bytes.len() - 3]).unwrap();
    reopen_cut(&log_dir, second, &[commit(1, 40)]);
}

#[test]
fn a_record_lost_before_a_kept_one_is_cut_there() {
    // After a power loss the filesystem kept the second record's block and lost part of the
    // first's.
    let dir = TempDir::new("log-damaged");
    let log_dir = dir.path().join("log");
    two_commits(&log_dir, commit(1, 40));
    let path = super::segment_path(&log_dir, Position(1));
    let mut bytes = fs::read(&path).unwrap();
    let header_len = crate::codec::segment::scan(&bytes).unwrap().header_len;
    bytes[header_len + 20] ^= 1;
    fs::write(&path, &bytes).unwrap();
    reopen_cut(&log_dir, header_len as u64, &[]);
}

#[test]
fn a_planted_look_alike_record_does_not_refuse_the_open() {
    // A database user stored bytes that verify as a later record (zstd keeps incompressible
    // data raw), and the commit holding them was torn after them.
    let dir = TempDir::new("log-planted");
    let log_dir = dir.path().join("log");
    let seed = 0x1234_5678u32;
    let (look_alike, _) =
        crate::codec::segment::encode_record(seed, Position(2), &commit(7, 16)).unwrap();
    let mut planted = vec![0xAB; 8];
    planted.extend_from_slice(&seed.to_le_bytes());
    planted.extend_from_slice(&look_alike);
    planted.extend_from_slice(&[0xCD; 32]);
    let mut log = Log::create(&log_dir, header()).unwrap();
    let path = super::segment_path(&log_dir, Position(1));
    let header_len = fs::metadata(&path).unwrap().len();
    log.append(&RecordBody::Commit { frame: planted }).unwrap();
    drop(log);
    let bytes = fs::read(&path).unwrap();
    fs::write(&path, &bytes[..bytes.len() - 20]).unwrap();
    reopen_cut(&log_dir, header_len, &[]);
}

#[test]
fn a_later_segment_of_another_version_is_refused_not_cut() {
    let dir = TempDir::new("log-version");
    let log_dir = dir.path().join("log");
    let mut log = Log::create(&log_dir, header()).unwrap();
    log.set_roll_bytes(100);
    log.append(&commit(1, 200)).unwrap();
    log.append(&commit(2, 200)).unwrap();
    let segments = log.segment_paths();
    assert_eq!(segments.len(), 2);
    drop(log);
    // Version 2 where version 1 is: a header of another format, not a torn one.
    let mut bytes = fs::read(&segments[1]).unwrap();
    bytes[4] = 2;
    fs::write(&segments[1], &bytes).unwrap();
    assert!(matches!(
        Log::open(&log_dir).unwrap_err(),
        LogError::Segment {
            source: crate::codec::segment::SegmentError::Version { found: 2 },
            ..
        }
    ));
    assert!(segments[1].exists());
}

/// A log of three segments of one commit each.
fn three_segments(log_dir: &std::path::Path) -> (Log, Vec<std::path::PathBuf>) {
    let mut log = Log::create(log_dir, header()).unwrap();
    log.set_roll_bytes(100);
    for n in 0..3 {
        log.append(&commit(n, 200)).unwrap();
    }
    let segments = log.segment_paths();
    assert_eq!(segments.len(), 3);
    (log, segments)
}

#[test]
fn a_later_segment_never_written_after_its_roll_is_cut() {
    let dir = TempDir::new("log-zero-magic");
    let log_dir = dir.path().join("log");
    let (log, segments) = three_segments(&log_dir);
    drop(log);
    // The roll's header block was allocated but its bytes never reached the disk.
    let len = fs::metadata(&segments[2]).unwrap().len() as usize;
    fs::write(&segments[2], vec![0u8; len]).unwrap();
    let (log, r) = Log::open(&log_dir).unwrap();
    assert_eq!(
        r.cut,
        Some(super::Cut {
            segment: segments[2].clone(),
            at: 0
        })
    );
    assert_eq!(log.segment_paths(), segments[..2].to_vec());
    assert_eq!(log.next_position(), Position(3));
    drop(log);
    // A foreign magic is refused, not cut.
    let (log, segments) = {
        let dir = dir.path().join("foreign");
        let (log, segments) = three_segments(&dir);
        drop(log);
        let mut bytes = fs::read(&segments[2]).unwrap();
        bytes[..4].copy_from_slice(b"XXXX");
        fs::write(&segments[2], bytes).unwrap();
        (Log::open(&dir), segments)
    };
    assert!(matches!(log.unwrap_err(), LogError::Segment {
        source: crate::codec::segment::SegmentError::Magic { .. },
        ..
    }));
    assert!(segments[2].exists());
}

#[test]
fn a_failed_unlink_in_a_trim_keeps_the_log_contiguous() {
    let dir = TempDir::new("log-trim-fail");
    let log_dir = dir.path().join("log");
    let (mut log, _) = three_segments(&log_dir);
    log.append(&RecordBody::Ack {
        acked: Position(3),
        acked_offset: Offset::from_u64(30),
        log_bytes: 0,
    })
    .unwrap();
    log.append(&RecordBody::Base {
        position: Position(3),
    })
    .unwrap();
    let segments = log.segment_paths();
    assert_eq!(segments.len(), 5);
    log.fail_next_unlink = true;
    assert!(matches!(log.trim().unwrap_err(), LogError::Io {
        op: "remove",
        ..
    }));
    assert_eq!(log.segment_paths(), segments);
    assert!(segments.iter().all(|s| s.exists()));
    // The unlink succeeds but its fsync fails: the segment stays listed though it is gone, and
    // the next trim takes it as removed.
    log.fail_next_trim_fsync = true;
    assert!(matches!(log.trim().unwrap_err(), LogError::Io {
        op: "fsync",
        ..
    }));
    assert_eq!(log.segment_paths(), segments);
    assert!(!segments[0].exists());
    // Positions 1 to 3 lie at or below min(base, acked) = 3: their segments go.
    assert_eq!(log.trim().unwrap(), 3);
    assert_eq!(log.segment_paths(), segments[3..].to_vec());
    drop(log);
    let (log, r) = Log::open(&log_dir).unwrap();
    assert_eq!(log.next_position(), Position(6));
    assert_eq!(r.cut, None);
}

#[test]
fn a_log_missing_records_it_still_needs_is_refused() {
    let dir = TempDir::new("log-prefix");
    let log_dir = dir.path().join("log");
    let mut log = Log::create(&log_dir, header()).unwrap();
    log.set_roll_bytes(100);
    for n in 0..4 {
        log.append(&commit(n, 200)).unwrap();
    }
    let segments = log.segment_paths();
    drop(log);
    // Base and acked are 0: every record is needed, but the oldest segment is gone. The last
    // segment's torn tail is left as it is: the refusal comes before any cut.
    fs::remove_file(&segments[0]).unwrap();
    let last = segments.last().unwrap();
    let mut bytes = fs::read(last).unwrap();
    bytes.extend_from_slice(&[1, 2, 3]);
    fs::write(last, &bytes).unwrap();
    assert!(matches!(
        Log::open(&log_dir).unwrap_err(),
        LogError::MissingPrefix {
            needed: Position(1),
            ..
        }
    ));
    assert_eq!(fs::read(last).unwrap(), bytes);
}

#[test]
fn an_unreadable_first_header_is_an_error() {
    let dir = TempDir::new("log-header");
    let log_dir = dir.path().join("log");
    drop(Log::create(&log_dir, header()).unwrap());
    let path = super::segment_path(&log_dir, Position(1));
    let mut bytes = fs::read(&path).unwrap();
    bytes[10] ^= 1;
    fs::write(&path, bytes).unwrap();
    assert!(matches!(
        Log::open(&log_dir).unwrap_err(),
        LogError::Segment { .. }
    ));
    let empty = dir.path().join("empty");
    fs::create_dir(&empty).unwrap();
    assert!(matches!(
        Log::open(&empty).unwrap_err(),
        LogError::Empty { .. }
    ));
}

#[test]
fn a_roll_carries_the_state_and_trim_deletes_what_base_and_ack_cover() {
    let dir = TempDir::new("log-roll");
    let log_dir = dir.path().join("log");
    let mut log = Log::create(&log_dir, header()).unwrap();
    log.set_roll_bytes(1000);
    log.append(&RecordBody::Claim {
        epoch: epoch(5),
        activation: Activation([5; 16]),
    })
    .unwrap();
    for n in 0..20u8 {
        log.append(&commit(n, 300)).unwrap();
    }
    // Positions 1..=21: segments start at 1, then every few records.
    let segments = log.segment_paths();
    assert!(segments.len() > 3, "{segments:?}");
    log.append(&RecordBody::Ack {
        acked: Position(15),
        acked_offset: Offset::from_u64(1500),
        log_bytes: 9,
    })
    .unwrap();
    // Nothing goes while BASE is 0.
    assert_eq!(log.trim().unwrap(), 0);
    log.append(&RecordBody::Base {
        position: Position(10),
    })
    .unwrap();
    let removed = log.trim().unwrap();
    assert!(removed > 0);
    let left = log.segment_paths();
    let first_left: u64 = left[0]
        .file_stem()
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    // The first segment left holds position 11, the first after BASE.
    assert!(first_left <= 11);
    drop(log);
    let (log, r) = Log::open(&log_dir).unwrap();
    assert_eq!(log.next_position(), Position(24));
    // The claim was in a deleted segment: the headers carried it over.
    assert_eq!(r.claim, Some((epoch(5), Activation([5; 16]))));
    assert_eq!((r.base, r.acked), (Position(10), Position(15)));
    let positions: Vec<u64> = r.records.iter().map(|r| r.position.0).collect();
    assert_eq!(positions, (11..24).collect::<Vec<_>>());
}

#[test]
fn a_torn_roll_deletes_the_new_segment() {
    let dir = TempDir::new("log-torn-roll");
    let log_dir = dir.path().join("log");
    let mut log = Log::create(&log_dir, header()).unwrap();
    log.set_roll_bytes(200);
    log.append(&commit(1, 300)).unwrap();
    log.append(&commit(2, 300)).unwrap();
    let segments = log.segment_paths();
    assert_eq!(segments.len(), 2);
    drop(log);
    // The new segment's header was cut short by a crash.
    let bytes = fs::read(&segments[1]).unwrap();
    fs::write(&segments[1], &bytes[..30]).unwrap();
    let (log, r) = Log::open(&log_dir).unwrap();
    assert!(matches!(&r.cut, Some(c) if c.at == 0 && c.segment == segments[1]));
    assert_eq!(log.segment_paths(), segments[..1].to_vec());
    assert!(!segments[1].exists());
    assert_eq!(records(&r), bodies(&[commit(1, 300)]));
}

fn body() -> impl Strategy<Value = RecordBody> {
    prop_oneof![
        (0u8..=255, 1usize..48).prop_map(|(n, len)| commit(n, len)),
        (0u64..10, 0u8..=255).prop_map(|(e, a)| RecordBody::Claim {
            epoch: epoch(e),
            activation: Activation([a; 16]),
        }),
        (0u64..10, 0u64..1000).prop_map(|(p, o)| RecordBody::Ack {
            acked: Position(p),
            acked_offset: Offset::from_u64(o),
            log_bytes: o,
        }),
    ]
}

proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(16))]

    /// A crash may leave any prefix of what was written: open keeps exactly the records that
    /// end at or before the cut, and the log takes appends again from there.
    #[test]
    fn crash_cuts_at_every_byte_keep_the_whole_records(
        written in proptest::collection::vec(body(), 1..6),
    ) {
        let dir = TempDir::new("log-cut");
        let log_dir = dir.path().join("log");
        let mut log = Log::create(&log_dir, header()).unwrap();
        let path = super::segment_path(&log_dir, Position(1));
        let header_len = fs::metadata(&path).unwrap().len();
        let mut ends = Vec::new();
        for b in &written {
            log.append(b).unwrap();
            ends.push(fs::metadata(&path).unwrap().len());
        }
        drop(log);
        let full = fs::read(&path).unwrap();
        for cut in header_len..=full.len() as u64 {
            fs::write(&path, &full[..cut as usize]).unwrap();
            let (mut log, r) = Log::open(&log_dir).unwrap();
            let whole = ends.iter().filter(|&&e| e <= cut).count();
            // Every record is kept (base and acked stay 0 unless an ACK moved acked).
            let got: Vec<RecordBody> = r.records.iter().map(|r| r.body.clone()).collect();
            let want: Vec<RecordBody> = written[..whole]
                .iter()
                .cloned()
                .zip(1u64..)
                .filter(|(_, p)| *p > r.acked.0.min(r.base.0))
                .map(|(b, _)| b)
                .collect();
            assert_eq!(got, want, "cut at {cut}");
            assert_eq!(r.cut.is_some(), cut != header_len && !ends.contains(&cut));
            assert_eq!(log.next_position(), Position(whole as u64 + 1));
            assert_eq!(fs::metadata(&path).unwrap().len(), ends.iter().rev().find(|&&e| e <= cut).copied().unwrap_or(header_len));
            // Appends continue the chain.
            log.append(&commit(9, 5)).unwrap();
            drop(log);
            let (_, again) = Log::open(&log_dir).unwrap();
            assert!(again.cut.is_none());
            fs::write(&path, &full).unwrap();
        }
    }
}

// A roll that fails leaves a partial segment behind; the next roll replaces it, so one failure
// (`ENOSPC`, say) does not refuse every later append, and the log reopens whole.
#[test]
fn a_failed_roll_leaves_nothing_that_refuses_the_next() {
    let dir = TempDir::new("log-roll-fail");
    let log_dir = dir.path().join("log");
    let mut log = Log::create(&log_dir, header()).unwrap();
    log.set_roll_bytes(100);
    log.append(&commit(1, 200)).unwrap();
    log.fail_next_roll = true;
    let err = log.append(&commit(2, 50)).unwrap_err();
    assert!(matches!(err, LogError::Io { op: "write", .. }), "{err}");
    assert_eq!(log.segment_paths().len(), 1);
    assert_eq!(
        fs::read_dir(&log_dir).unwrap().count(),
        2,
        "the partial segment is left"
    );
    assert_eq!(log.append(&commit(3, 50)).unwrap(), Position(2));
    assert_eq!(log.segment_paths().len(), 2);
    drop(log);
    let (_, r) = Log::open(&log_dir).unwrap();
    let positions: Vec<Position> = r.records.iter().map(|r| r.position).collect();
    assert_eq!(positions, vec![Position(1), Position(2)]);
}

fn token(n: u64) -> Incarnation {
    Incarnation::try_from((1 << 52) | n).unwrap()
}

#[test]
fn a_rekey_rolls_to_a_segment_naming_the_new_token_with_its_claim() {
    let dir = TempDir::new("log-rekey");
    let log_dir = dir.path().join("log");
    let mut log = Log::create(&log_dir, header()).unwrap();
    log.append(&commit(1, 100)).unwrap();
    log.append(&RecordBody::Ack {
        acked: Position(1),
        acked_offset: Offset::from_u64(140),
        log_bytes: 100,
    })
    .unwrap();
    let position = log.rekey(token(2), epoch(4), Activation([4; 16])).unwrap();
    assert_eq!(position, Position(3));
    let segments = log.segment_paths();
    assert_eq!(segments.len(), 2);
    drop(log);
    let (log, r) = Log::open(&log_dir).unwrap();
    assert_eq!(log.next_position(), Position(4));
    // The latest segment names the new token and carries the state over.
    assert_eq!(r.header.incarnation, token(2));
    assert_eq!(r.header.first_position, Position(3));
    assert_eq!(
        (r.acked, r.acked_offset),
        (Position(1), Offset::from_u64(140))
    );
    assert_eq!(r.claim, Some((epoch(4), Activation([4; 16]))));
    let first = crate::codec::segment::scan(&fs::read(&segments[0]).unwrap()).unwrap();
    assert_eq!(first.header.incarnation, header().incarnation);
}

#[test]
fn a_rekey_of_an_empty_segment_replaces_its_header() {
    let dir = TempDir::new("log-rekey-empty");
    let log_dir = dir.path().join("log");
    let mut log = Log::create(&log_dir, header()).unwrap();
    // A stale temp file of an earlier crash goes.
    let temp = log.segment_paths()[0].with_extension("usl.tmp");
    fs::write(&temp, b"torn").unwrap();
    let position = log.rekey(token(2), epoch(1), Activation([1; 16])).unwrap();
    assert_eq!(position, Position(1));
    assert_eq!(log.segment_paths().len(), 1);
    assert!(!temp.exists());
    drop(log);
    let (log, r) = Log::open(&log_dir).unwrap();
    assert_eq!(log.next_position(), Position(2));
    assert_eq!(r.header.incarnation, token(2));
    assert_eq!(r.claims, vec![(epoch(1), Activation([1; 16]))]);
}

#[test]
fn a_crash_after_the_roll_before_the_claim_leaves_the_new_token() {
    let dir = TempDir::new("log-rekey-crash");
    let log_dir = dir.path().join("log");
    let mut log = Log::create(&log_dir, header()).unwrap();
    log.append(&RecordBody::Claim {
        epoch: epoch(1),
        activation: Activation([1; 16]),
    })
    .unwrap();
    log.append(&commit(1, 100)).unwrap();
    // The roll of a re-key, and a crash before its CLAIM record.
    log.roll_to(token(2)).unwrap();
    drop(log);
    let (mut log, r) = Log::open(&log_dir).unwrap();
    assert_eq!(r.cut, None);
    assert_eq!(r.header.incarnation, token(2));
    assert_eq!(log.next_position(), Position(3));
    // The claim the crash left out, and nothing lost before it.
    assert_eq!(r.claim, Some((epoch(1), Activation([1; 16]))));
    assert_eq!(r.pending().len(), 1);
    // A re-key after the reopen goes into the empty segment.
    log.rekey(token(3), epoch(2), Activation([2; 16])).unwrap();
    assert_eq!(log.segment_paths().len(), 2);
    drop(log);
    let (_, r) = Log::open(&log_dir).unwrap();
    assert_eq!(r.header.incarnation, token(3));
    assert_eq!(r.claim, Some((epoch(2), Activation([2; 16]))));
}

#[test]
fn a_crash_before_the_rename_keeps_the_old_header() {
    let dir = TempDir::new("log-rekey-temp");
    let log_dir = dir.path().join("log");
    let log = Log::create(&log_dir, header()).unwrap();
    // The temp header was written, the rename never happened.
    let path = log.segment_paths()[0].clone();
    drop(log);
    let mut h = header();
    h.incarnation = token(2);
    super::write_segment(&path.with_extension("usl.tmp"), &h).unwrap();
    let (log, r) = Log::open(&log_dir).unwrap();
    assert_eq!(r.header.incarnation, header().incarnation);
    assert_eq!(log.segment_paths(), vec![path]);
}

#[test]
fn trim_deletes_segments_across_a_rekey() {
    let dir = TempDir::new("log-rekey-trim");
    let log_dir = dir.path().join("log");
    let mut log = Log::create(&log_dir, header()).unwrap();
    for n in 1..=3 {
        log.append(&commit(n, 100)).unwrap();
    }
    log.rekey(token(2), epoch(2), Activation([2; 16])).unwrap();
    log.append(&commit(5, 100)).unwrap();
    log.append(&RecordBody::Ack {
        acked: Position(5),
        acked_offset: Offset::from_u64(500),
        log_bytes: 400,
    })
    .unwrap();
    log.append(&RecordBody::Base {
        position: Position(5),
    })
    .unwrap();
    assert_eq!(log.trim().unwrap(), 1);
    drop(log);
    let (log, r) = Log::open(&log_dir).unwrap();
    assert_eq!(log.segment_paths().len(), 1);
    assert_eq!(r.header.incarnation, token(2));
    assert_eq!(r.claim, Some((epoch(2), Activation([2; 16]))));
}

/// A COMMIT record of a stream frame from another lineage, as a catch-up logs it.
fn caught_up(n: u8) -> RecordBody {
    let frame = crate::codec::frame::encode(&crate::codec::frame::Frame::Commit {
        id: crate::ids::CommitId {
            lineage: Lineage([9; 16]),
            position: Position(u64::from(n)),
        },
        compressed: vec![n; 40],
    })
    .unwrap();
    RecordBody::Commit { frame }
}

/// Stream frames a catch-up logged with no ACK after them were never applied (the catch-up
/// applies them once that ACK is fsynced): the open cuts them like a torn tail, across segments,
/// and the log goes on from the first of them. Frames an ACK follows, and the generation's own
/// pending commits, stay.
#[test]
fn caught_up_frames_no_ack_follows_are_cut() {
    let dir = TempDir::new("log-cut-caught-up");
    let log_dir = dir.path().join("log");
    let mut log = Log::create(&log_dir, header()).unwrap();
    log.set_roll_bytes(200);
    let ack = |acked: u64| RecordBody::Ack {
        acked: Position(acked),
        acked_offset: Offset::from_u64(acked * 10),
        log_bytes: 0,
    };
    for body in [
        caught_up(1),
        ack(1),
        caught_up(2),
        caught_up(3),
        caught_up(4),
    ] {
        log.append(&body).unwrap();
    }
    log.sync().unwrap();
    let segments = log.segment_paths().len();
    assert!(segments > 1, "the unacknowledged run crosses a roll");
    drop(log);
    let (mut log, r) = Log::open(&log_dir).unwrap();
    assert!(r.cut.is_some());
    assert_eq!(records(&r), vec![
        (Position(1), caught_up(1)),
        (Position(2), ack(1))
    ]);
    assert!(r.any_commit);
    assert_eq!(log.next_position(), Position(3));
    assert!(log.segment_paths().len() < segments);
    // The log goes on from the cut, and reopens contiguous.
    log.append(&commit(5, 10)).unwrap();
    log.sync().unwrap();
    drop(log);
    let (log, r) = Log::open(&log_dir).unwrap();
    assert_eq!(r.cut, None);
    assert_eq!(log.next_position(), Position(4));
    assert_eq!(r.pending().len(), 1);
}

/// The generation's own commit after the ACK is pending, never cut.
#[test]
fn own_commits_after_the_ack_are_kept() {
    let dir = TempDir::new("log-keep-own");
    let log_dir = dir.path().join("log");
    let mut log = Log::create(&log_dir, header()).unwrap();
    log.append(&commit(1, 10)).unwrap();
    log.sync().unwrap();
    drop(log);
    let (_, r) = Log::open(&log_dir).unwrap();
    assert_eq!(r.cut, None);
    assert_eq!(r.pending().len(), 1);
}

/// The generation's own pending commits stay when caught-up frames no ACK follows come after
/// them: only the trailing run of caught-up frames goes.
#[test]
fn own_pending_commits_before_a_trailing_caught_up_run_stay() {
    let dir = TempDir::new("log-cut-after-own");
    let log_dir = dir.path().join("log");
    let mut log = Log::create(&log_dir, header()).unwrap();
    for body in [commit(1, 10), commit(2, 10), caught_up(3), caught_up(4)] {
        log.append(&body).unwrap();
    }
    log.sync().unwrap();
    drop(log);
    let (log, r) = Log::open(&log_dir).unwrap();
    assert!(r.cut.is_some());
    assert_eq!(records(&r), vec![
        (Position(1), commit(1, 10)),
        (Position(2), commit(2, 10))
    ]);
    assert_eq!(r.pending().len(), 2);
    assert_eq!(log.next_position(), Position(3));
}

/// A cut across an empty segment a re-key rolled to brings back the state carried to the cut
/// point: the later segments the log rolls to name the token before the re-key.
#[test]
fn a_cut_across_a_re_key_segment_restores_the_token_before_it() {
    let dir = TempDir::new("log-cut-rekey");
    let log_dir = dir.path().join("log");
    let mut log = Log::create(&log_dir, header()).unwrap();
    for body in [
        commit(1, 10),
        RecordBody::Ack {
            acked: Position(1),
            acked_offset: Offset::from_u64(10),
            log_bytes: 0,
        },
        caught_up(2),
    ] {
        log.append(&body).unwrap();
    }
    log.roll_to(token(2)).unwrap();
    drop(log);
    let (mut log, r) = Log::open(&log_dir).unwrap();
    assert!(r.cut.is_some());
    assert_eq!(r.header.incarnation, header().incarnation);
    assert_eq!(log.segment_paths().len(), 1);
    // A roll after the cut carries the restored token.
    log.set_roll_bytes(1);
    log.append(&commit(3, 10)).unwrap();
    log.append(&commit(4, 10)).unwrap();
    log.sync().unwrap();
    assert!(log.segment_paths().len() > 1);
    drop(log);
    let (_, r) = Log::open(&log_dir).unwrap();
    assert_eq!(r.header.incarnation, header().incarnation);
    assert_eq!(r.pending().len(), 2);
}

/// The `supersedes` of every segment header of the log in `dir`, in order.
fn supersedes_of(dir: &std::path::Path) -> Vec<Option<crate::ids::GenId>> {
    let mut segments: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    segments.sort();
    segments
        .iter()
        .map(|s| {
            crate::codec::segment::scan(&fs::read(s).unwrap())
                .unwrap()
                .header
                .supersedes
        })
        .collect()
}

// Every segment names the generation directory its generation superseded: a roll, a re-key's
// included, carries it over, after a reopen too, so a trim never removes it. A copy names the
// directory it copied in every segment.
#[test]
fn every_segment_names_what_its_generation_supersedes() {
    let dir = TempDir::new("log-supersedes");
    let log_dir = dir.path().join("log");
    let old = crate::ids::GenId::parse_hex("0123456789abcdef0123456789abcdef").unwrap();
    let mut log = Log::create(&log_dir, SegmentHeader {
        supersedes: Some(old),
        ..header()
    })
    .unwrap();
    log.set_roll_bytes(256);
    for n in 0..8 {
        log.append(&commit(n, 200)).unwrap();
    }
    drop(log);
    let (mut log, _) = Log::open(&log_dir).unwrap();
    log.set_roll_bytes(256);
    log.append(&commit(9, 200)).unwrap();
    log.append(&commit(10, 200)).unwrap();
    let token = Incarnation::try_from((1 << 52) | 2).unwrap();
    log.rekey(token, epoch(2), Activation([2; 16])).unwrap();
    let acked = Position(log.next_position().0 - 1);
    log.append(&RecordBody::Ack {
        acked,
        acked_offset: Offset::from_u64(1),
        log_bytes: 0,
    })
    .unwrap();
    log.append(&RecordBody::Base { position: acked }).unwrap();
    log.append(&commit(11, 200)).unwrap();
    log.sync().unwrap();
    let all = supersedes_of(&log_dir);
    assert!(all.len() > 3, "{} segments", all.len());
    assert!(all.iter().all(|s| *s == Some(old)), "{all:?}");
    assert!(log.trim().unwrap() > 0);
    assert!(supersedes_of(&log_dir).iter().all(|s| *s == Some(old)));
    let copy_dir = dir.path().join("copy");
    let source = crate::ids::GenId::parse_hex("fedcba9876543210fedcba9876543210").unwrap();
    super::copy(&log_dir, &copy_dir, source).unwrap();
    let copied = supersedes_of(&copy_dir);
    assert_eq!(copied.len(), supersedes_of(&log_dir).len());
    assert!(copied.iter().all(|s| *s == Some(source)), "{copied:?}");
    let (_, original) = Log::open(&log_dir).unwrap();
    let (_, recopied) = Log::open(&copy_dir).unwrap();
    assert_eq!(records(&original), records(&recopied));
}

/// Keeps a frame's length.
#[derive(Debug, Default)]
struct Length(u32);

impl Keep for Length {
    type Kept = u32;

    fn start(&mut self, len: u32) {
        self.0 = len;
    }

    fn piece(&mut self, _bytes: &[u8]) {}

    fn finish(&mut self) -> u32 {
        self.0
    }
}

/// A host's fold over [`Log::read`]: the position and frame length of each of the generation's own
/// COMMIT records above the latest ACK, dropped as later ACKs come, so it keeps no frame and 16
/// bytes per pending commit.
#[derive(Debug, Default)]
struct Unacknowledged {
    commits: Vec<(Position, u64)>,
    /// The highest position an ACK dropped.
    dropped: Option<Position>,
    /// At the end of the log: whether a lower ACK brought back a commit an earlier one dropped,
    /// which the fold no longer has.
    lowered: bool,
    /// Where the read cut, as [`Fold::end`] says.
    cut: Option<Cut>,
}

impl Fold for Unacknowledged {
    type Frames = Length;

    fn record(&mut self, position: Position, body: RecordBody<Logged<u32>>, state: &SegmentHeader) {
        if let RecordBody::Commit { frame } = &body
            && !super::foreign(frame.id, state.lineage)
        {
            self.commits.push((position, u64::from(frame.kept)));
        }
        self.drop_through(state.acked);
    }

    fn cut(&mut self, from: Position) {
        self.commits.retain(|(p, _)| *p < from);
    }

    fn end(&mut self, state: &SegmentHeader, cut: Option<&Cut>) {
        self.drop_through(state.acked);
        self.lowered = self.dropped.is_some_and(|d| d > state.acked);
        self.cut = cut.cloned();
    }
}

impl Unacknowledged {
    fn drop_through(&mut self, acked: Position) {
        for (p, _) in self.commits.iter().filter(|(p, _)| *p <= acked) {
            self.dropped = self.dropped.max(Some(*p));
        }
        self.commits.retain(|(p, _)| *p > acked);
    }

    /// The pending commits once the read ended, `None` when a lower ACK brought back a commit an
    /// earlier one dropped.
    fn pending(&self) -> Option<&[(Position, u64)]> {
        (!self.lowered).then_some(self.commits.as_slice())
    }
}

/// What a test writes to a log.
#[derive(Debug, Clone)]
enum Op {
    /// A commit of the generation's own: a COMMIT frame of its lineage, or bytes no frame starts
    /// with.
    Own {
        frame: bool,
        fill: u8,
        len: usize,
    },
    /// A stream frame a catch-up logged.
    CaughtUp {
        fill: u8,
        len: usize,
    },
    Claim(u64),
    /// An ACK of the record `back` before the last one, never below an earlier one.
    Ack {
        back: u64,
    },
    /// An ACK of any position.
    AnyAck(u64),
    /// A BASE at the record `back` before the last one.
    Base {
        back: u64,
    },
    Rekey(u64),
    Trim,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (any::<bool>(), any::<u8>(), 1usize..300)
            .prop_map(|(frame, fill, len)| Op::Own { frame, fill, len }),
        // Larger than the reader's buffer.
        1 => (any::<bool>(), any::<u8>(), BUFFER..BUFFER + 4000)
            .prop_map(|(frame, fill, len)| Op::Own { frame, fill, len }),
        3 => (any::<u8>(), 1usize..300).prop_map(|(fill, len)| Op::CaughtUp { fill, len }),
        1 => (0u64..10).prop_map(Op::Claim),
        3 => (0u64..3).prop_map(|back| Op::Ack { back }),
        1 => (0u64..24).prop_map(Op::AnyAck),
        2 => (0u64..3).prop_map(|back| Op::Base { back }),
        1 => (2u64..6).prop_map(Op::Rekey),
        1 => Just(Op::Trim),
    ]
}

/// A COMMIT frame of `lineage`.
fn commit_frame(lineage: Lineage, position: u64, fill: u8, len: usize) -> Vec<u8> {
    crate::codec::frame::encode(&Frame::Commit {
        id: CommitId {
            lineage,
            position: Position(position),
        },
        compressed: vec![fill; len],
    })
    .unwrap()
}

/// Writes `ops` to a new log in `dir`, rolled past `roll` bytes. Returns whether an ACK went below
/// an earlier one.
fn write_log(dir: &Path, roll: u64, ops: &[Op]) -> bool {
    let mut log = Log::create(dir, header()).unwrap();
    log.set_roll_bytes(roll);
    let mut acked = 0;
    let mut highest = 0;
    let mut went_back = false;
    for op in ops {
        let last = log.next_position().0 - 1;
        let body = match op {
            Op::Own {
                frame: true,
                fill,
                len,
            } => RecordBody::Commit {
                frame: commit_frame(header().lineage, last + 1, *fill, *len),
            },
            Op::Own {
                frame: false,
                fill,
                len,
            } => commit(*fill, *len),
            Op::CaughtUp { fill, len } => RecordBody::Commit {
                frame: commit_frame(Lineage([9; 16]), last + 1, *fill, *len),
            },
            Op::Claim(e) => RecordBody::Claim {
                epoch: epoch(*e),
                activation: Activation([*e as u8; 16]),
            },
            Op::Ack { back } => {
                acked = last.saturating_sub(*back).max(acked);
                went_back |= acked < highest;
                highest = highest.max(acked);
                RecordBody::Ack {
                    acked: Position(acked),
                    acked_offset: Offset::from_u64(acked * 10),
                    log_bytes: 0,
                }
            }
            Op::AnyAck(n) => {
                went_back |= *n < highest;
                highest = highest.max(*n);
                RecordBody::Ack {
                    acked: Position(*n),
                    acked_offset: Offset::from_u64(*n),
                    log_bytes: 1,
                }
            }
            Op::Base { back } => RecordBody::Base {
                position: Position(last.saturating_sub(*back)),
            },
            Op::Rekey(n) => {
                log.rekey(token(*n), epoch(*n), Activation([*n as u8; 16]))
                    .unwrap();
                continue;
            }
            Op::Trim => {
                log.trim().unwrap();
                continue;
            }
        };
        log.append(&body).unwrap();
    }
    log.sync().unwrap();
    went_back
}

/// What a crash or a damage may leave of the log's segment files.
#[derive(Debug, Clone)]
enum Damage {
    None,
    /// A segment cut short.
    Cut {
        segment: usize,
        at: usize,
    },
    /// A damaged bit in a segment.
    Flip {
        segment: usize,
        bit: usize,
    },
    /// The first segment gone.
    DropFirst,
    /// A segment between the first and the last gone: the next one does not follow.
    DropMiddle {
        segment: usize,
    },
    /// A later segment written again with a valid header and chains: of another lineage, or with
    /// a record after the last position there is (a header that does not follow, and a record the
    /// reader refuses after it), or both.
    Rewrite {
        segment: usize,
        lineage: bool,
        overflow: bool,
    },
    /// A later segment zero-filled: a roll whose header never reached the disk.
    ZeroLater {
        segment: usize,
    },
    /// Bytes after the last segment's records.
    Garbage(Vec<u8>),
}

fn damage() -> impl Strategy<Value = Damage> {
    prop_oneof![
        2 => Just(Damage::None),
        3 => (any::<usize>(), any::<usize>()).prop_map(|(segment, at)| Damage::Cut { segment, at }),
        2 => (any::<usize>(), any::<usize>()).prop_map(|(segment, bit)| Damage::Flip { segment, bit }),
        1 => Just(Damage::DropFirst),
        1 => any::<usize>().prop_map(|segment| Damage::DropMiddle { segment }),
        2 => (
            any::<usize>(),
            prop_oneof![Just((true, false)), Just((true, true)), Just((false, true))],
        )
            .prop_map(|(segment, (lineage, overflow))| Damage::Rewrite {
                segment,
                lineage,
                overflow,
            }),
        1 => any::<usize>().prop_map(|segment| Damage::ZeroLater { segment }),
        1 => proptest::collection::vec(any::<u8>(), 1..40).prop_map(Damage::Garbage),
    ]
}

/// The segment files of the log in `dir`, by name.
fn segment_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut files: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    files.sort();
    files
}

fn apply(dir: &Path, damage: &Damage) {
    let files = segment_files(dir);
    let n = files.len();
    match damage {
        Damage::None => {}
        Damage::Cut { segment, at } => {
            let path = &files[segment % n];
            let bytes = fs::read(path).unwrap();
            fs::write(path, &bytes[..at % (bytes.len() + 1)]).unwrap();
        }
        Damage::Flip { segment, bit } => {
            let path = &files[segment % n];
            let mut bytes = fs::read(path).unwrap();
            let bit = bit % (bytes.len() * 8);
            bytes[bit / 8] ^= 1 << (bit % 8);
            fs::write(path, bytes).unwrap();
        }
        Damage::DropFirst if n > 1 => fs::remove_file(&files[0]).unwrap(),
        Damage::DropFirst => {}
        Damage::DropMiddle { segment } if n > 2 => {
            fs::remove_file(&files[1 + segment % (n - 2)]).unwrap();
        }
        Damage::DropMiddle { .. } => {}
        Damage::Rewrite {
            segment,
            lineage,
            overflow,
        } if n > 1 => {
            let path = &files[1 + segment % (n - 1)];
            let scanned = scan(&fs::read(path).unwrap()).unwrap();
            let mut h = scanned.header;
            if *lineage {
                h.lineage = Lineage([8; 16]);
            }
            let records: Vec<(Position, RecordBody)> = if *overflow {
                h.first_position = Position(u64::MAX);
                let base = |at| RecordBody::Base {
                    position: Position(at),
                };
                vec![(Position(u64::MAX), base(1)), (Position(1), base(2))]
            } else {
                scanned
                    .records
                    .into_iter()
                    .map(|r| (r.position, r.body))
                    .collect()
            };
            let (mut bytes, mut chain) = encode_header(&h).unwrap();
            for (position, body) in &records {
                let (record, next) = encode_record(chain, *position, body).unwrap();
                bytes.extend(record);
                chain = next;
            }
            fs::write(path, bytes).unwrap();
        }
        Damage::Rewrite { .. } => {}
        Damage::ZeroLater { segment } if n > 1 => {
            let path = &files[1 + segment % (n - 1)];
            let len = fs::metadata(path).unwrap().len() as usize;
            fs::write(path, vec![0u8; len]).unwrap();
        }
        Damage::ZeroLater { .. } => {}
        Damage::Garbage(bytes) => {
            let path = &files[n - 1];
            let mut all = fs::read(path).unwrap();
            all.extend_from_slice(bytes);
            fs::write(path, all).unwrap();
        }
    }
}

/// Every file in `dir`, by name, with its bytes.
fn snapshot(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    fs::read_dir(dir)
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            (
                e.file_name().into_string().unwrap(),
                fs::read(e.path()).unwrap(),
            )
        })
        .collect()
}

fn restore(dir: &Path, files: &BTreeMap<String, Vec<u8>>) {
    fs::remove_dir_all(dir).unwrap();
    fs::create_dir(dir).unwrap();
    for (name, bytes) in files {
        fs::write(dir.join(name), bytes).unwrap();
    }
}

/// Checks that the open built on [`Log::read`] opens the log in `dir` as the open before it did,
/// with the same log and the same records, or the same error, and the same files after. Checks
/// too that the read changes nothing, and that a host's fold over the read keeps the open's
/// pending commits, unless `acks_go_back` and the fold found an ACK below an earlier one.
fn opens_as_before(dir: &Path, acks_go_back: bool) -> Result<(), TestCaseError> {
    let files = snapshot(dir);
    let before = oracle::open(dir);
    let files_before = snapshot(dir);
    restore(dir, &files);
    let now = Log::open(dir);
    prop_assert_eq!(snapshot(dir), files_before);
    match (&before, &now) {
        (Ok((old, old_recovered)), Ok((new, new_recovered))) => {
            prop_assert_eq!(old_recovered, new_recovered);
            prop_assert_eq!(&old.segments, &new.segments);
            prop_assert_eq!(&old.state, &new.state);
            prop_assert_eq!(
                (old.len, old.header_len, old.chain, old.next),
                (new.len, new.header_len, new.chain, new.next)
            );
        }
        (Err(old), Err(new)) => prop_assert_eq!(format!("{old:?}"), format!("{new:?}")),
        (old, new) => {
            return Err(TestCaseError::fail(format!(
                "before: {:?}, now: {:?}",
                old.as_ref().err(),
                new.as_ref().err()
            )));
        }
    }
    drop(now);
    restore(dir, &files);
    let mut unacknowledged = Unacknowledged::default();
    let read = Log::read(dir, &mut unacknowledged);
    prop_assert_eq!(snapshot(dir), files);
    match (read, &before) {
        (Ok(_), Ok((_, recovered))) => {
            prop_assert_eq!(&unacknowledged.cut, &recovered.cut);
            let pending: Vec<(Position, u64)> = recovered
                .pending()
                .iter()
                .map(|(p, frame)| (*p, frame.len() as u64))
                .collect();
            match unacknowledged.pending() {
                Some(kept) => prop_assert_eq!(kept, pending.as_slice()),
                // Only an ACK below an earlier one brings back a commit the fold dropped.
                None => prop_assert!(acks_go_back, "the fold lost its pending commits"),
            }
        }
        (Err(read), Err(open)) => prop_assert_eq!(format!("{read:?}"), format!("{open:?}")),
        (read, open) => {
            return Err(TestCaseError::fail(format!(
                "read: {:?}, open: {:?}",
                read.err(),
                open.as_ref().err()
            )));
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // Over own and caught-up commits, records larger than the reader's buffer, ACKs, BASEs,
    // CLAIMs, rolls, re-keys and trims, then a cut, a damaged bit, a lost, zero-filled or
    // rewritten segment or bytes after the end: the open built on the streaming read opens the
    // log exactly as the whole-segment open did, the read changes nothing, and a fold that keeps
    // only positions and lengths keeps the open's pending commits.
    #[test]
    fn the_read_opens_logs_as_the_whole_segment_open_did(
        ops in proptest::collection::vec(op(), 0..14),
        roll in 200u64..3000,
        harm in damage(),
    ) {
        let dir = TempDir::new("log-read-equivalence");
        let log_dir = dir.path().join("log");
        let acks_go_back = write_log(&log_dir, roll, &ops);
        apply(&log_dir, &harm);
        opens_as_before(&log_dir, acks_go_back)?;
    }
}

// The fold keeps what the open's pending commits are, and reads a frame larger than the reader's
// buffer without keeping it: own commits after the latest ACK, not the caught-up frames an ACK
// follows or the trailing ones the read cuts.
#[test]
fn a_hosts_fold_keeps_the_pending_commits_without_their_frames() {
    let dir = TempDir::new("log-read-fold");
    let log_dir = dir.path().join("log");
    let acks_go_back = write_log(&log_dir, 1000, &[
        Op::Own {
            frame: true,
            fill: 1,
            len: 10,
        },
        Op::Ack { back: 0 },
        Op::CaughtUp { fill: 2, len: 10 },
        Op::Ack { back: 0 },
        Op::Own {
            frame: true,
            fill: 3,
            len: BUFFER * 2,
        },
        Op::Own {
            frame: false,
            fill: 4,
            len: 10,
        },
        Op::CaughtUp { fill: 5, len: 10 },
    ]);
    assert!(!acks_go_back);
    let mut fold = Unacknowledged::default();
    Log::read(&log_dir, &mut fold).unwrap();
    assert_eq!(fold.cut.as_ref().map(|c| c.at > 0), Some(true));
    let frame = commit_frame(header().lineage, 5, 3, BUFFER * 2).len() as u64;
    assert_eq!(
        fold.pending(),
        Some([(Position(5), frame), (Position(6), 10)].as_slice())
    );
    opens_as_before(&log_dir, acks_go_back).unwrap();
}

// A later segment that does not follow (one gone between, another lineage, or a first position
// that is not the next) fails the open, and a record its reader refuses fails it first, as a scan
// of the whole segment did.
#[test]
fn a_later_segment_that_does_not_follow_fails_after_its_records() {
    let ops: Vec<Op> = (0..12u8)
        .map(|fill| Op::Own {
            frame: true,
            fill,
            len: 100,
        })
        .collect();
    // The error a case fails with.
    enum Refusal {
        Gap,
        Lineage,
        Overflow,
    }
    let cases = [
        (Damage::DropMiddle { segment: 0 }, Refusal::Gap),
        (
            Damage::Rewrite {
                segment: 0,
                lineage: true,
                overflow: false,
            },
            Refusal::Lineage,
        ),
        (
            Damage::Rewrite {
                segment: 0,
                lineage: true,
                overflow: true,
            },
            Refusal::Overflow,
        ),
        (
            Damage::Rewrite {
                segment: 0,
                lineage: false,
                overflow: true,
            },
            Refusal::Overflow,
        ),
    ];
    for (harm, refusal) in cases {
        let dir = TempDir::new("log-later-segment");
        let log_dir = dir.path().join("log");
        let acks_go_back = write_log(&log_dir, 300, &ops);
        assert!(segment_files(&log_dir).len() > 2);
        apply(&log_dir, &harm);
        let err = Log::open(&log_dir).unwrap_err();
        let refused = match refusal {
            Refusal::Gap => matches!(err, LogError::Gap { at: 0, .. }),
            Refusal::Lineage => matches!(err, LogError::Lineage { .. }),
            Refusal::Overflow => matches!(err, LogError::Segment {
                source: SegmentError::PositionOverflow { .. },
                ..
            }),
        };
        assert!(refused, "{harm:?}: {err:?}");
        opens_as_before(&log_dir, acks_go_back).unwrap();
    }
}
