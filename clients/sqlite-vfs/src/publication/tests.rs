#![expect(
    clippy::arithmetic_side_effects,
    reason = "tests number positions from small counters"
)]

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use libsqlite3_sys as ffi;
use proptest::prelude::Just;
use proptest::prelude::Strategy;
use proptest::prop_oneof;
use proptest::proptest;

use super::Call;
use super::HandleState;
use super::Verdict;
use super::phase_two_reply;
use crate::ids::Position;

/// What a trace decided: commits by the trace line of their commit frame's page, with the line of
/// the call that decided them, and the WAL writes refused.
#[derive(Debug, Default, PartialEq, Eq)]
struct Outcome {
    confirmed: Vec<(u32, u32)>,
    unconfirmed: Vec<(u32, u32)>,
    unsupported: Vec<u32>,
    /// PHASETWO calls that decided nothing.
    ignored_phase_two: Vec<u32>,
}

/// Replays a transcribed trace: each connection's main handle has its own state, and a commit
/// frame's page logs the commit at the next position, as the VFS logs before forwarding it.
fn replay(trace: &str) -> Outcome {
    let mut handles: BTreeMap<&str, HandleState> = BTreeMap::new();
    let mut logged_at: BTreeMap<Position, u32> = BTreeMap::new();
    let mut next = 1u64;
    let mut out = Outcome::default();
    for line in trace.lines().filter(|l| !l.starts_with('#')) {
        let mut words = line.split_whitespace();
        let (Some(num), Some(conn), Some(event), None) =
            (words.next(), words.next(), words.next(), words.next())
        else {
            panic!("malformed trace line {line:?}");
        };
        let num: u32 = num.parse().unwrap();
        let state = handles.entry(conn).or_default();
        let calls: &[Call] = match event {
            "lock-write" => &[Call::LockWrite],
            "unlock-write" => &[Call::UnlockWrite],
            "wal-write" | "commit" => &[Call::WalWrite],
            "phasetwo" => &[Call::PhaseTwo],
            "close" => &[Call::Close],
            "other" => &[Call::Other],
            other => panic!("unknown event {other:?} at line {num}"),
        };
        let mut verdicts: Vec<Verdict> = calls.iter().map(|&c| state.on(c)).collect();
        if event == "commit" && verdicts.last() != Some(&Verdict::Unsupported) {
            let p = Position(next);
            next += 1;
            logged_at.insert(p, num);
            verdicts.push(state.on(Call::Logged(p)));
        }
        for v in verdicts {
            match v {
                Verdict::None if event == "phasetwo" => out.ignored_phase_two.push(num),
                Verdict::None => {}
                Verdict::Confirmed(p) => out.confirmed.push((logged_at[&p], num)),
                Verdict::Unconfirmed(p) => out.unconfirmed.push((logged_at[&p], num)),
                Verdict::Unsupported => out.unsupported.push(num),
            }
        }
    }
    out
}

/// Every commit frame's page in the trace.
fn commits(trace: &str) -> Vec<u32> {
    trace
        .lines()
        .filter(|l| l.ends_with(" commit"))
        .map(|l| l.split_whitespace().next().unwrap().parse().unwrap())
        .collect()
}

/// Asserts that every commit of the trace but those in `unconfirmed` is confirmed by the PHASETWO
/// right after its WRITE unlock, and that nothing was refused.
fn all_confirmed_but(trace: &str, unconfirmed: &[(u32, u32)]) -> Outcome {
    let out = replay(trace);
    let left: BTreeSet<u32> = unconfirmed.iter().map(|&(c, _)| c).collect();
    let want: Vec<u32> = commits(trace)
        .into_iter()
        .filter(|c| !left.contains(c))
        .collect();
    let got: Vec<u32> = out.confirmed.iter().map(|&(c, _)| c).collect();
    assert_eq!(got, want);
    assert_eq!(out.unconfirmed, unconfirmed);
    assert!(out.unsupported.is_empty());
    out
}

#[test]
fn basic_commits_are_each_confirmed() {
    let out = all_confirmed_but(include_str!("vectors/basic.trace"), &[]);
    assert_eq!(out.confirmed.len(), 7);
    // The setup's first WAL commit, decided by the PHASETWO at line 54.
    assert_eq!(out.confirmed.first(), Some(&(51, 54)));
}

#[test]
fn spilled_transactions_are_confirmed() {
    let out = all_confirmed_but(include_str!("vectors/spill.trace"), &[]);
    assert!(!out.confirmed.is_empty());
}

#[test]
fn commits_of_two_connections_are_confirmed_on_their_own_handles() {
    let out = all_confirmed_but(include_str!("vectors/multi.trace"), &[]);
    assert_eq!(out.confirmed.len(), 7);
}

#[test]
fn checkpoints_between_commits_confirm_each_commit() {
    all_confirmed_but(include_str!("vectors/checkpoint.trace"), &[]);
}

#[test]
fn a_published_commit_without_phase_two_is_unconfirmed() {
    // AF1: a transaction writing the WAL database and an attached rollback-journal one whose sync
    // fails. SQLite published the WAL commit (`attachfail.out`: the row is visible) but reports the
    // COMMIT failed and never sends PHASETWO: a read-mark unlock follows the WRITE unlock.
    all_confirmed_but(include_str!("vectors/attachfail.trace"), &[(93, 107)]);
}

#[test]
fn an_unpublished_commit_is_unconfirmed() {
    // Growing the wal-index fails after the commit frame was written: SQLite never published it
    // (`fault-shm_map_new.out`: the row is not visible), releases the WRITE lock and sends no
    // PHASETWO.
    all_confirmed_but(include_str!("vectors/fault-shm_map_new.trace"), &[(
        9085, 9089,
    )]);
}

#[test]
fn a_commit_whose_frame_write_failed_under_a_reader_is_unconfirmed() {
    // P2: the commit frame's page write fails while a SELECT of the same connection is mid-step.
    all_confirmed_but(include_str!("vectors/pendingreader.trace"), &[(86, 89)]);
}

#[test]
fn the_phase_two_handler_never_fails() {
    // The prototype failed PHASETWO of the commit at line 85: SQLite reported IOERR for a commit
    // that is published (`fault-phasetwo_err.out`). The verdict confirms it, and the reply the
    // handler returns is never an error.
    let out = all_confirmed_but(include_str!("vectors/fault-phasetwo_err.trace"), &[]);
    assert!(out.confirmed.contains(&(85, 88)));
    assert_eq!(phase_two_reply(ffi::SQLITE_IOERR), ffi::SQLITE_NOTFOUND);
    assert_eq!(phase_two_reply(ffi::SQLITE_NOTFOUND), ffi::SQLITE_NOTFOUND);
    assert_eq!(phase_two_reply(ffi::SQLITE_OK), ffi::SQLITE_OK);
}

#[test]
fn exclusive_locking_mode_is_refused() {
    // `locking_mode=EXCLUSIVE` keeps the wal-index in heap memory: X1 and X2 write the WAL with no
    // WRITE shm lock. Their writes are refused, so nothing is logged, and their PHASETWO calls
    // decide nothing.
    let trace = include_str!("vectors/exclusive.trace");
    let out = replay(trace);
    assert_eq!(out.confirmed, vec![(51, 54)]);
    assert!(out.unconfirmed.is_empty());
    assert_eq!(out.unsupported, vec![63, 66, 70, 73]);
    assert_eq!(out.ignored_phase_two, vec![19, 68, 75]);
}

#[test]
fn a_phase_two_without_a_logged_commit_is_ignored() {
    // `basic` S6 and S7: write transactions that change no page still get PHASETWO.
    let out = replay(include_str!("vectors/basic.trace"));
    assert!(out.ignored_phase_two.contains(&130));
    assert!(out.ignored_phase_two.contains(&136));
    let mut s = HandleState::Idle;
    assert_eq!(s.on(Call::PhaseTwo), Verdict::None);
    assert_eq!(s.on(Call::LockWrite), Verdict::None);
    assert_eq!(s.on(Call::UnlockWrite), Verdict::None);
    assert_eq!(s.on(Call::PhaseTwo), Verdict::None);
}

#[test]
fn a_close_after_a_logged_commit_leaves_it_unconfirmed() {
    let mut s = HandleState::Idle;
    s.on(Call::LockWrite);
    s.on(Call::Logged(Position(4)));
    assert_eq!(s.undecided(), Some(Position(4)));
    assert_eq!(s.on(Call::Close), Verdict::Unconfirmed(Position(4)));
    assert_eq!(s.undecided(), None);
}

#[test]
fn a_new_write_lock_before_phase_two_leaves_the_commit_unconfirmed() {
    let mut s = HandleState::Idle;
    s.on(Call::LockWrite);
    s.on(Call::Logged(Position(1)));
    assert_eq!(s.on(Call::UnlockWrite), Verdict::None);
    assert_eq!(s.undecided(), Some(Position(1)));
    assert_eq!(s.on(Call::LockWrite), Verdict::Unconfirmed(Position(1)));
    assert_eq!(s, HandleState::Writing);
}

/// A call, with `Logged` positions assigned in order by the harness.
fn call() -> impl Strategy<Value = Call> {
    prop_oneof![
        Just(Call::LockWrite),
        Just(Call::UnlockWrite),
        Just(Call::WalWrite),
        Just(Call::Logged(Position(0))),
        Just(Call::PhaseTwo),
        Just(Call::Close),
        Just(Call::Other),
    ]
}

proptest! {
    #[test]
    fn a_commit_is_confirmed_only_by_phase_two_right_after_its_write_unlock(
        calls in proptest::collection::vec(call(), 0..200),
    ) {
        let mut s = HandleState::Idle;
        let mut next = 1u64;
        let mut history: Vec<Call> = Vec::new();
        let mut decided = BTreeSet::new();
        // The position logged since the last WRITE lock, if any.
        let mut logged_under_lock: Option<Position> = None;
        for c in calls {
            let c = match c {
                Call::Logged(_) => {
                    let p = Position(next);
                    next += 1;
                    Call::Logged(p)
                }
                other => other,
            };
            let before = s;
            let v = s.on(c);
            match v {
                Verdict::Confirmed(p) => {
                    assert_eq!(c, Call::PhaseTwo);
                    assert_eq!(history.last(), Some(&Call::UnlockWrite));
                    assert_eq!(logged_under_lock, Some(p));
                    assert!(decided.insert(p), "{p:?} decided twice");
                }
                Verdict::Unconfirmed(p) => {
                    assert_ne!(c, Call::PhaseTwo);
                    assert!(decided.insert(p), "{p:?} decided twice");
                }
                Verdict::Unsupported => {
                    assert_eq!(c, Call::WalWrite);
                    assert_eq!(before, HandleState::Idle);
                }
                Verdict::None => {
                    // A logged commit whose WRITE unlock is followed by anything but PHASETWO is
                    // never left undecided.
                    assert!(!matches!(before, HandleState::AwaitingPhaseTwo(_)));
                }
            }
            match c {
                Call::LockWrite if matches!(s, HandleState::Writing) => logged_under_lock = None,
                Call::Logged(p) if matches!(s, HandleState::Logged(_)) => {
                    logged_under_lock = Some(p);
                }
                _ => {}
            }
            history.push(c);
        }
    }
}
