//! Publication tracking (design §4.2): whether SQLite published a logged commit locally.
//!
//! A logged commit is final, shipped and replayed whatever happens next. What is left to learn is
//! whether SQLite published it, since nothing built without it may be logged after it. SQLite
//! sends `SQLITE_FCNTL_COMMIT_PHASETWO` on the committing connection's main db handle right after
//! it releases the WAL write lock of a committed transaction, and only then. So a commit is
//! confirmed per handle, and only when PHASETWO is the very next call on that handle after the
//! WRITE shm unlock. Any other call there first (a read-mark unlock, a file lock, a new write
//! lock, a close) leaves the commit unconfirmed, and the attachment resets.
//!
//! [`HandleState`] is that rule as a pure state machine over a main db handle's calls, with the
//! WAL writes of its connection and the position its commit was logged at. A WAL write without
//! the WRITE shm lock (EXCLUSIVE locking mode keeps the wal-index in heap memory and takes no shm
//! lock) is refused. [`phase_two_reply`] is what the PHASETWO handler returns: never an error,
//! since SQLite reports a published commit as failed when it gets one.

use std::ffi::c_int;

use libsqlite3_sys as ffi;

use crate::ids::Position;

/// A call on a main db handle, or a WAL write of its connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Call {
    /// `xShmLock` took the WRITE lock (a failed attempt is [`Call::Other`]).
    LockWrite,
    /// `xShmLock` released the WRITE lock.
    UnlockWrite,
    /// A WAL write of the handle's connection.
    WalWrite,
    /// The connection's commit was logged at this position (its commit point).
    Logged(Position),
    /// `SQLITE_FCNTL_COMMIT_PHASETWO`.
    PhaseTwo,
    /// `xClose`.
    Close,
    /// Any other call on the handle.
    Other,
}

/// Where a main db handle stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum HandleState {
    /// No WRITE shm lock.
    #[default]
    Idle,
    /// The WRITE shm lock is held, nothing logged.
    Writing,
    /// The WRITE shm lock is held and the commit was logged.
    Logged(Position),
    /// The WRITE shm lock was released after a logged commit: the next call decides.
    AwaitingPhaseTwo(Position),
}

/// What a call decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    None,
    /// SQLite published the commit logged at this position.
    Confirmed(Position),
    /// The handle's transaction ended otherwise: the attachment resets (`local_damage`) and the
    /// reopen replays the log, which commits it.
    Unconfirmed(Position),
    /// A WAL write without the WRITE shm lock (EXCLUSIVE locking mode): it fails alone as
    /// `unsupported`.
    Unsupported,
}

impl HandleState {
    /// Takes the handle's next call.
    pub(crate) fn on(&mut self, call: Call) -> Verdict {
        match (*self, call) {
            (HandleState::AwaitingPhaseTwo(p), Call::PhaseTwo) => {
                *self = HandleState::Idle;
                Verdict::Confirmed(p)
            }
            (HandleState::AwaitingPhaseTwo(p), next) => {
                // The transaction ended otherwise. The call still counts: a new WRITE lock starts
                // a transaction.
                *self = HandleState::Idle;
                if next == Call::LockWrite {
                    *self = HandleState::Writing;
                }
                Verdict::Unconfirmed(p)
            }
            (HandleState::Idle, Call::LockWrite) => {
                *self = HandleState::Writing;
                Verdict::None
            }
            (HandleState::Idle, Call::WalWrite) => Verdict::Unsupported,
            (HandleState::Writing, Call::Logged(p)) | (HandleState::Logged(_), Call::Logged(p)) => {
                *self = HandleState::Logged(p);
                Verdict::None
            }
            (HandleState::Writing, Call::UnlockWrite | Call::Close) => {
                *self = HandleState::Idle;
                Verdict::None
            }
            (HandleState::Logged(p), Call::UnlockWrite) => {
                *self = HandleState::AwaitingPhaseTwo(p);
                Verdict::None
            }
            (HandleState::Logged(p), Call::Close) => {
                *self = HandleState::Idle;
                Verdict::Unconfirmed(p)
            }
            // Idle: no transaction to decide. Writing or Logged: the transaction goes on (its WAL
            // writes, shm maps, read marks, and a PHASETWO under the lock, which SQLite never
            // sends, decides nothing).
            (HandleState::Idle | HandleState::Writing | HandleState::Logged(_), _) => Verdict::None,
        }
    }

    /// The position of the handle's logged commit while it is undecided: another connection
    /// reaching its commit point waits until it is decided.
    pub(crate) fn undecided(&self) -> Option<Position> {
        match *self {
            HandleState::Logged(p) | HandleState::AwaitingPhaseTwo(p) => Some(p),
            HandleState::Idle | HandleState::Writing => None,
        }
    }
}

/// What the PHASETWO handler returns, given what the call forwarded to the "unix" VFS returned:
/// `SQLITE_OK` or `SQLITE_NOTFOUND`, never an error.
pub(crate) fn phase_two_reply(forwarded: c_int) -> c_int {
    if forwarded == ffi::SQLITE_OK {
        ffi::SQLITE_OK
    } else {
        ffi::SQLITE_NOTFOUND
    }
}

#[cfg(test)]
mod tests;
