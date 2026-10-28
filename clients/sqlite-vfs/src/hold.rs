//! The hold (design: Hosts): a host that snapshots or moves the process, or the VM it runs in,
//! takes `<host dir>/hold` exclusively, and write transactions wait before they start, so the
//! logs drain before it freezes the workload. It is advisory: what slips past it shows in the
//! host's inspection after the freeze, which alone decides.
//!
//! The extension's side, [`Hold::wait`], runs in `xShmLock` before a write transaction's WAL write
//! lock request is forwarded (`vfs`), when the thread holds no binding mutex and no WAL write lock.
//! It tries a shared `flock` on the file and releases it at once. While the host holds the file,
//! it tries again every 5 ms, for at most 60 s since the request began. Then it logs
//! `write_hold_expired` and sets a latch, under which a request that finds the hold taken goes on
//! at once, until a try finds it free. Any other error goes on, logged once
//! (`write_hold_failed`). The file is opened at the process's first request that may wait, and the
//! outcome, a file or none, is kept for the life of the process. SQLite sees a write transaction
//! that took longer, never `SQLITE_BUSY`.
//!
//! The host's side, [`take`], is what `ursula-vfs-host hold` runs: an exclusive `flock` on the
//! file, held until it is dropped or the process exits.

use std::fs::File;
use std::fs::TryLockError;
use std::path::Path;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use crate::hostdir;
use crate::log;
use crate::log::Level;

/// The longest a write transaction waits at the hold: the valve.
const LIMIT: Duration = Duration::from_secs(60);
/// How often a waiting write transaction tries the hold again.
const POLL: Duration = Duration::from_millis(5);

/// The host's exclusive lock on `<host dir>/hold`, released when dropped.
#[derive(Debug)]
pub struct Held {
    _file: File,
}

/// The hold could not be taken.
#[derive(Debug, thiserror::Error)]
pub enum HoldError {
    #[error("open {}: {source}", .path.display())]
    Open {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// A FIFO, a socket, a device or a directory in place of the lock file.
    #[error("{} is not a regular file", .path.display())]
    NotAFile { path: PathBuf },
    #[error("lock {}: {source}", .path.display())]
    Lock {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// Takes the hold of the host directory `dir` (the compiled-in one when `None`, never the
/// extension's test hook): an exclusive `flock` on its `hold`, which the host created. Blocks
/// until no other process holds the file.
pub fn take(dir: Option<&Path>) -> Result<Held, HoldError> {
    let host = dir.unwrap_or(Path::new(hostdir::DEFAULT));
    let path = host.join(hostdir::HOLD);
    let file = hostdir::open_hold(host).map_err(|source| HoldError::Open {
        path: path.clone(),
        source,
    })?;
    match file.metadata() {
        Ok(meta) if meta.is_file() => {}
        Ok(_) => return Err(HoldError::NotAFile { path }),
        Err(source) => return Err(HoldError::Open { path, source }),
    }
    file.lock()
        .map_err(|source| HoldError::Lock { path, source })?;
    Ok(Held { _file: file })
}

/// What a write transaction's wait at the hold ended with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Waited {
    /// No host held the file, or it released it.
    Free,
    /// The valve fired: 60 s passed, or had passed before and the hold is still taken.
    Expired,
    /// The file could not be tried.
    Failed,
}

/// The hold as this process waits at it: the host's lock file, opened read-only.
#[derive(Debug)]
pub(crate) struct Hold {
    file: File,
    path: PathBuf,
    limit: Duration,
    poll: Duration,
    /// The valve fired, and no try since found the hold free.
    expired: AtomicBool,
    /// `write_hold_failed` was logged.
    failed: AtomicBool,
}

impl Hold {
    pub(crate) fn new(file: File, path: PathBuf, limit: Duration, poll: Duration) -> Hold {
        Hold {
            file,
            path,
            limit,
            poll,
            expired: AtomicBool::new(false),
            failed: AtomicBool::new(false),
        }
    }

    /// Waits while the host holds the file, at most until the valve fires.
    pub(crate) fn wait(&self) -> Waited {
        let started = Instant::now();
        loop {
            match self.file.try_lock_shared() {
                Ok(()) => {
                    if let Err(e) = self.file.unlock() {
                        self.fail(&e);
                    }
                    self.expired.store(false, Ordering::Relaxed);
                    return Waited::Free;
                }
                Err(TryLockError::WouldBlock) => {
                    if self.expired.load(Ordering::Relaxed) {
                        return Waited::Expired;
                    }
                    let waited = started.elapsed();
                    if waited >= self.limit {
                        if !self.expired.swap(true, Ordering::Relaxed) {
                            log::emit(Level::Warn, "write_hold_expired", &[
                                ("path", &self.path.display()),
                                ("waited_ms", &waited.as_millis()),
                            ]);
                        }
                        return Waited::Expired;
                    }
                    std::thread::sleep(self.poll);
                }
                Err(TryLockError::Error(e)) => {
                    self.fail(&e);
                    return Waited::Failed;
                }
            }
        }
    }

    /// Whether the valve's latch is set.
    #[cfg(test)]
    pub(crate) fn expired(&self) -> bool {
        self.expired.load(Ordering::Relaxed)
    }

    fn fail(&self, e: &std::io::Error) {
        if !self.failed.swap(true, Ordering::Relaxed) {
            log::emit(Level::Warn, "write_hold_failed", &[
                ("path", &self.path.display()),
                ("error", e),
            ]);
        }
    }
}

/// This process's hold: `<host dir>/hold` when the host directory resolves and the file is there,
/// opened at the first call and kept for the life of the process. A file that cannot be opened,
/// other than one that does not exist, or is not a regular file, is none, logged
/// (`write_hold_failed`). A unit test's thread waits at the hold it set ([`set_for_tests`]).
pub(crate) fn process_hold() -> Option<&'static Hold> {
    #[cfg(test)]
    if let Some(hold) = TEST_HOLD.get() {
        return Some(hold);
    }
    static HOLD: OnceLock<Option<Hold>> = OnceLock::new();
    HOLD.get_or_init(open_hold).as_ref()
}

#[cfg(test)]
thread_local! {
    static TEST_HOLD: std::cell::Cell<Option<&'static Hold>> = const { std::cell::Cell::new(None) };
}

/// Sets the hold this thread's write transactions wait at, in place of the process's.
#[cfg(test)]
pub(crate) fn set_for_tests(hold: Option<&'static Hold>) {
    TEST_HOLD.set(hold);
}

fn open_hold() -> Option<Hold> {
    let host = hostdir::host_dir()?;
    let path = host.join(hostdir::HOLD);
    let refused = |error: &dyn std::fmt::Display| {
        log::emit(Level::Warn, "write_hold_failed", &[
            ("path", &path.display()),
            ("error", error),
        ]);
    };
    let file = match hostdir::open_hold(&host) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            refused(&e);
            return None;
        }
    };
    match file.metadata() {
        Ok(meta) if meta.is_file() => {}
        Ok(_) => {
            refused(&"not a regular file");
            return None;
        }
        Err(e) => {
            refused(&e);
            return None;
        }
    }
    Some(Hold::new(file, path, LIMIT, POLL))
}
