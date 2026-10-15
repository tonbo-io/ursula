#![expect(
    clippy::arithmetic_side_effects,
    reason = "tests compute page counts, offsets and cuts from small generated sizes"
)]

use std::collections::BTreeMap;
use std::fs;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

use proptest::prelude::Strategy;
use proptest::prop_oneof;
use proptest::proptest;
use ursula_headers::Epoch;
use ursula_headers::Incarnation;
use ursula_headers::Offset;

use super::ForeignFrame;
use super::catch_up;
use super::materialize;
use crate::capture::db_len;
use crate::capture::page_offset;
use crate::codec::PAGE;
use crate::codec::frame::CommitRecord;
use crate::codec::frame::Frame;
use crate::codec::frame::encode_commit;
use crate::codec::segment::RecordBody;
use crate::codec::segment::SegmentHeader;
use crate::generation::Generation;
use crate::generation::Root;
use crate::generation::canonical_path;
use crate::ids::Activation;
use crate::ids::CommitId;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::replog::Log;
use crate::testdir::TempDir;

fn epoch(n: u64) -> Epoch {
    Epoch::try_from(n).unwrap()
}

const LINEAGE: Lineage = Lineage([3; 16]);

fn header() -> SegmentHeader {
    SegmentHeader {
        lineage: LINEAGE,
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

/// A fresh published generation with an empty db and log.
fn generation(dir: &TempDir) -> (Root, Generation) {
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let root = Root::lock(dir.path(), &p).unwrap();
    let g = root.build(header()).unwrap().publish().unwrap();
    (root, g)
}

fn image(pgno: u32, fill: u8) -> Box<[u8; PAGE]> {
    let mut p = Box::new([fill; PAGE]);
    p[..4].copy_from_slice(&pgno.to_be_bytes());
    p
}

/// The database after a commit: its size and, per page, the image's fill and the commit that
/// wrote it (1-based). Pages without an entry are zero.
#[derive(Debug, Clone, Default)]
struct State {
    size: u32,
    pages: BTreeMap<u32, (u8, usize)>,
}

impl State {
    fn bytes(&self) -> Vec<u8> {
        let mut out = vec![0u8; usize::try_from(db_len(self.size)).unwrap()];
        for (&pgno, &(fill, _)) in &self.pages {
            let at = usize::try_from(page_offset(pgno)).unwrap();
            out[at..at + PAGE].copy_from_slice(image(pgno, fill).as_slice());
        }
        out
    }
}

fn read(path: &std::path::Path) -> Vec<u8> {
    fs::read(path).unwrap()
}

#[derive(Debug, Clone)]
enum Op {
    Commit {
        size: u32,
        pages: Vec<(u32, u8)>,
    },
    /// A checkpoint copying some pages of the state after a later commit into the db, or, when
    /// `complete`, all of them and cutting the db to that state's size.
    Checkpoint {
        pick: usize,
        subset: u64,
        complete: bool,
    },
    /// F2: the db brought to the last commit and fsynced, then a BASE.
    Base,
    Ack,
    /// F3: the log fsynced and the segments below `min(base, acked)` deleted.
    Trim,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (1u32..40, proptest::collection::vec((1u32..40, 1u8..=255), 0..24))
            .prop_map(|(size, pages)| Op::Commit { size, pages }),
        2 => (0usize..100, proptest::num::u64::ANY, proptest::bool::weighted(0.3))
            .prop_map(|(pick, subset, complete)| Op::Checkpoint { pick, subset, complete }),
        1 => proptest::strategy::Just(Op::Base),
        1 => proptest::strategy::Just(Op::Ack),
        1 => proptest::strategy::Just(Op::Trim),
    ]
}

/// How a crash leaves the bytes written after the log's last fsync, in its last segment.
#[derive(Debug, Clone, Copy)]
enum Tear {
    /// Cut at this fraction of them.
    Cut(u16),
    /// A byte at this fraction lost (flipped), the bytes after it kept.
    Hole(u16),
}

fn tear() -> impl Strategy<Value = Tear> {
    prop_oneof![
        proptest::num::u16::ANY.prop_map(Tear::Cut),
        proptest::num::u16::ANY.prop_map(Tear::Hole),
    ]
}

fn last_segment(log: &Log) -> PathBuf {
    log.segment_paths().last().unwrap().clone()
}

fn header_len(segment: &std::path::Path) -> u64 {
    crate::codec::segment::scan(&fs::read(segment).unwrap())
        .unwrap()
        .header_len as u64
}

/// Runs `ops` against a real generation whose log rolls past `roll` bytes, tears the log as
/// `tear` says, crashes a first materialization after `crash` page writes, materializes, and
/// checks the db against the model.
fn run(ops: &[Op], roll: u64, tear: Tear, crash: Option<u64>) {
    let dir = TempDir::new("materialize");
    let (_root, g) = generation(&dir);
    let mut log = Log::open(&g.log_dir()).unwrap().0;
    log.set_roll_bytes(roll);
    let db = File::options().read(true).write(true).open(g.db()).unwrap();
    let len = |p: &std::path::Path| fs::metadata(p).unwrap().len();
    let mut states = vec![State::default()];
    // Where each commit ends: its segment and the segment's length after it.
    let mut ends: Vec<(PathBuf, u64)> = Vec::new();
    let mut base = 0usize;
    // The last fsync: the segment and its length then.
    let mut synced = {
        let s = last_segment(&log);
        let l = len(&s);
        (s, l)
    };
    let sync = |log: &mut Log| {
        log.sync().unwrap();
        let s = last_segment(log);
        let l = len(&s);
        (s, l)
    };
    for op in ops {
        match op {
            Op::Commit { size, pages } => {
                let n = states.len();
                let mut state = states[n - 1].clone();
                if *size < state.size {
                    state.pages.retain(|&p, _| p <= *size);
                }
                state.size = *size;
                let pages: BTreeMap<u32, u8> =
                    pages.iter().copied().filter(|&(p, _)| p <= *size).collect();
                for (&p, &fill) in &pages {
                    state.pages.insert(p, (fill, n));
                }
                let record = CommitRecord {
                    db_pages_after: *size,
                    pages: pages.iter().map(|(&p, &f)| (p, image(p, f))).collect(),
                };
                let id = CommitId {
                    lineage: LINEAGE,
                    position: log.next_position(),
                };
                let frame = encode_commit(id, &record).unwrap();
                log.append(&RecordBody::Commit { frame }).unwrap();
                states.push(state);
                let s = last_segment(&log);
                let l = len(&s);
                ends.push((s, l));
            }
            Op::Checkpoint {
                pick,
                subset,
                complete,
            } => {
                let later = states.len() - 1 - base;
                if later == 0 {
                    continue;
                }
                // F1: the log is durable before the first page reaches the db.
                synced = sync(&mut log);
                let j = base + 1 + pick % later;
                for (&pgno, &(fill, writer)) in &states[j].pages {
                    if writer > base && (*complete || subset & (1 << (pgno % 64)) != 0) {
                        db.write_all_at(image(pgno, fill).as_slice(), page_offset(pgno))
                            .unwrap();
                    }
                }
                if *complete {
                    db.set_len(db_len(states[j].size)).unwrap();
                }
            }
            Op::Base => {
                synced = sync(&mut log);
                let last = states.last().unwrap();
                db.set_len(0).unwrap();
                db.write_all_at(&last.bytes(), 0).unwrap();
                db.sync_all().unwrap();
                let position = Position(log.next_position().0 - 1);
                log.append(&RecordBody::Base { position }).unwrap();
                base = states.len() - 1;
            }
            Op::Ack => {
                let acked = Position(log.next_position().0 - 1);
                log.append(&RecordBody::Ack {
                    acked,
                    acked_offset: Offset::from_u64(acked.0 * 10),
                    log_bytes: 0,
                })
                .unwrap();
            }
            Op::Trim => {
                log.trim().unwrap();
                synced = sync(&mut log);
            }
        }
    }
    let last = last_segment(&log);
    drop(log);
    // A crash keeps every segment before the last (a roll fsyncs them), and in the last one the
    // bytes up to its last fsync and any of what came after.
    let durable = if synced.0 == last {
        synced.1
    } else {
        header_len(&last)
    };
    let end = len(&last);
    let at = |f: u16| durable + (end - durable) * u64::from(f) / u64::from(u16::MAX);
    let broken = match tear {
        Tear::Cut(f) => {
            let cut = at(f);
            File::options()
                .write(true)
                .open(&last)
                .unwrap()
                .set_len(cut)
                .unwrap();
            cut
        }
        Tear::Hole(f) => {
            let hole = at(f);
            if hole < end {
                let mut bytes = fs::read(&last).unwrap();
                bytes[hole as usize] ^= 0x5a;
                fs::write(&last, bytes).unwrap();
                hole
            } else {
                end
            }
        }
    };
    // The commits whose bytes all survive: those in earlier segments, and those ending at or
    // before the break in the last.
    let kept = ends
        .iter()
        .filter(|(s, e)| *s < last || (*s == last && *e <= broken))
        .count();
    let want = states[kept].bytes();
    // Stale WAL files are deleted unread.
    fs::write(g.wal(), b"stale").unwrap();
    fs::write(g.shm(), b"stale").unwrap();
    if let Some(n) = crash {
        super::CRASH_AFTER_PAGES.set(Some(n));
        let crashed = materialize(&g);
        super::CRASH_AFTER_PAGES.set(None);
        drop(crashed);
    }
    let m = materialize(&g).unwrap();
    assert!(!g.wal().exists() && !g.shm().exists());
    assert_eq!(
        read(&g.db()),
        want,
        "kept {kept} commits of {}",
        states.len() - 1
    );
    drop(m);
    // Again: nothing changes.
    let m = materialize(&g).unwrap();
    assert_eq!(read(&g.db()), want);
    // The BASE covers every commit, so the second open applied nothing.
    let after: Vec<&RecordBody> = m.recovered.after_base().map(|r| &r.body).collect();
    assert!(
        after
            .iter()
            .all(|b| !matches!(b, RecordBody::Commit { .. })),
        "{after:?}"
    );
}

proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(64))]

    #[test]
    fn materialization_yields_the_state_at_the_last_valid_record(
        ops in proptest::collection::vec(op(), 0..32),
        roll in 300u64..4_000,
        tear in tear(),
        crash in proptest::option::of(0u64..60),
    ) {
        run(&ops, roll, tear, crash);
    }
}

#[test]
fn a_batch_larger_than_the_bound_is_written_in_pieces() {
    let size = 2500;
    let pages: Vec<(u32, u8)> = (1..=size).map(|p| (p, (p % 250) as u8 + 1)).collect();
    run(
        &[Op::Commit { size, pages }, Op::Commit {
            size: 3,
            pages: vec![(2, 9)],
        }],
        super::super::replog::SEGMENT_BYTES,
        Tear::Cut(u16::MAX),
        Some(100),
    );
}

#[test]
fn catching_up_logs_applies_and_survives_a_reopen() {
    let dir = TempDir::new("catch-up");
    let (_root, g) = generation(&dir);
    let mut m = materialize(&g).unwrap();
    let foreign = Lineage([9; 16]);
    let commit = |position, size, pages: &[(u32, u8)]| {
        let record = CommitRecord {
            db_pages_after: size,
            pages: pages.iter().map(|&(p, f)| (p, image(p, f))).collect(),
        };
        let id = CommitId {
            lineage: foreign,
            position: Position(position),
        };
        let crate::codec::frame::Decoded::Frame { frame, .. } =
            crate::codec::frame::decode(&encode_commit(id, &record).unwrap(), u64::MAX).unwrap()
        else {
            panic!("partial");
        };
        frame
    };
    let frames = vec![
        ForeignFrame {
            frame: Frame::Claim {
                epoch: epoch(4),
                activation: Activation([4; 16]),
            },
            end_offset: Offset::from_u64(37),
        },
        ForeignFrame {
            frame: commit(1, 2, &[(1, 1), (2, 2)]),
            end_offset: Offset::from_u64(100),
        },
        ForeignFrame {
            frame: commit(2, 3, &[(3, 3)]),
            end_offset: Offset::from_u64(180),
        },
    ];
    catch_up(&mut m, Position(0), &frames).unwrap();
    let want = {
        let mut s = State {
            size: 3,
            ..State::default()
        };
        for p in 1..=3 {
            s.pages.insert(p, (p as u8, 1));
        }
        s.bytes()
    };
    assert_eq!(read(&g.db()), want);
    drop(m);
    let m = materialize(&g).unwrap();
    assert_eq!(read(&g.db()), want);
    let r = &m.recovered;
    // Two COMMIT records, the ACK of the second, the BASE.
    assert_eq!(
        (r.acked, r.acked_offset, r.base),
        (Position(2), Offset::from_u64(180), Position(3))
    );
    assert!(r.pending().is_empty());
    let part = ForeignFrame {
        frame: Frame::Part {
            id: CommitId {
                lineage: foreign,
                position: Position(3),
            },
            index: 0,
            count: 2,
            chunk: vec![1],
        },
        end_offset: Offset::from_u64(200),
    };
    let mut m = m;
    assert!(matches!(
        catch_up(&mut m, Position(0), &[part]),
        Err(super::LocalError::Part { end_offset }) if end_offset == Offset::from_u64(200)
    ));
}

#[test]
fn catching_up_is_refused_while_our_commits_are_pending() {
    let dir = TempDir::new("catch-up-pending");
    let (_root, g) = generation(&dir);
    let mut log = Log::open(&g.log_dir()).unwrap().0;
    let record = CommitRecord {
        db_pages_after: 1,
        pages: vec![(1, image(1, 1))],
    };
    let id = CommitId {
        lineage: LINEAGE,
        position: Position(1),
    };
    let frame = encode_commit(id, &record).unwrap();
    log.append(&RecordBody::Commit { frame }).unwrap();
    // A stream frame a catch-up logged, with no ACK after it, is not pending.
    let foreign = CommitId {
        lineage: Lineage([9; 16]),
        position: Position(7),
    };
    let frame = encode_commit(foreign, &record).unwrap();
    log.append(&RecordBody::Commit { frame }).unwrap();
    drop(log);
    let mut m = materialize(&g).unwrap();
    let pending: Vec<Position> = m.recovered.pending().into_iter().map(|(p, _)| p).collect();
    assert_eq!(pending, vec![Position(1)]);
    let frames = [ForeignFrame {
        frame: Frame::Claim {
            epoch: epoch(2),
            activation: Activation([2; 16]),
        },
        end_offset: Offset::from_u64(37),
    }];
    assert!(matches!(
        catch_up(&mut m, Position(0), &frames),
        Err(super::LocalError::OwnPending { first: Position(1) })
    ));
    // Once the caller knows commit 1 acknowledged, the frames go after it.
    catch_up(&mut m, Position(1), &frames).unwrap();
}
