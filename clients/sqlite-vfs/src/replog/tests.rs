#![expect(
    clippy::arithmetic_side_effects,
    reason = "tests compute cuts and positions from small generated sizes"
)]

use std::fs;

use proptest::prelude::Strategy;
use proptest::prop_oneof;
use proptest::proptest;
use ursula_headers::Epoch;
use ursula_headers::Incarnation;
use ursula_headers::Offset;

use super::Log;
use super::LogError;
use crate::codec::segment::RecordBody;
use crate::codec::segment::SegmentHeader;
use crate::ids::Activation;
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
    assert!(matches!(err, LogError::Io {
        op: "append to",
        ..
    }));
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
    // Base and acked are 0: every record is needed, but the oldest segment is gone.
    fs::remove_file(&segments[0]).unwrap();
    assert!(matches!(
        Log::open(&log_dir).unwrap_err(),
        LogError::MissingPrefix {
            needed: Position(1),
            ..
        }
    ));
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
