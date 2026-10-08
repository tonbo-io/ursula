//! Attached databases: their replication state ([`Db`]) and the process-wide registry.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ffi::c_int;
use std::fs::{self};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

use libsqlite3_sys as ffi;

use crate::config::fail_post_ack;
use crate::config::snapshot_min_bytes;
use crate::error::Error;
use crate::frame::PAGE;
use crate::host::OK;
use crate::snapshotter::Snapper;

pub(crate) struct CommitStat {
    pub(crate) bytes: usize,
    pub(crate) raw: usize,
    pub(crate) pages: usize,
    pub(crate) attempts: u32,
    pub(crate) append: Duration,
    pub(crate) vfs: Duration,
}

pub(crate) struct Db {
    pub(crate) url: String,
    /// The stream's incarnation at attach (`Head::incarnation`): a re-claim or a snapshot checks
    /// the stream is still it.
    pub(crate) incarnation: String,
    /// `producer_id(incarnation)`.
    pub(crate) producer: String,
    pub(crate) sidecar: String,
    /// What the sidecar records besides offset, epoch, format, log count and the WAL claim (see
    /// `stamp`).
    pub(crate) stamp: String,
    pub(crate) path: String,
    pub(crate) epoch: u64,
    /// Producer sequence of the last acknowledged append (the claim is 0).
    pub(crate) seq: u64,
    /// Stream offset after the last acknowledged frame.
    pub(crate) offset: String,
    /// Frame bytes since the latest snapshot, counted locally (see `snapshot_due`).
    pub(crate) log: u64,
    /// Why the database is poisoned (re-attach to recover); [`Db::fenced`] tells the fenced ones.
    pub(crate) poisoned: Option<Error>,
    /// WAL writes of that transaction not yet on the local WAL, by offset (WAL writes never
    /// partially overlap: the header at 0, frame headers at frame offsets, page data at frame
    /// offset + 24).
    pub(crate) overlay: BTreeMap<i64, Vec<u8>>,
    /// The transaction's commit is acknowledged: later writes go straight to the local WAL, and
    /// the sidecar advances once the transaction ends.
    pub(crate) committed: bool,
    /// WAL frame number (1-based) of the acknowledged commit frame: the transaction is published
    /// locally once the wal-index header's mxFrame reaches it.
    pub(crate) commit_frame_no: u32,
    /// Open WAL handles, and the main db handle holding an EXCLUSIVE file lock (0: none):
    /// together the closing connection's checkpoint, the one main-db write that takes no
    /// checkpoint shm lock.
    pub(crate) wal_open: usize,
    pub(crate) exclusive: usize,
    /// The main db handle holding the WAL write lock (0: none): the write transaction in progress
    /// (between taking and releasing that lock); see `sync_db`.
    pub(crate) writer: usize,
    pub(crate) acked: u64,
    pub(crate) fault_fired: bool,
    pub(crate) stats: Vec<CommitStat>,
    pub(crate) checkpoint_started: Option<Instant>,
    pub(crate) checkpoints: Vec<Duration>,
    /// Database size in pages at `offset`.
    pub(crate) pages: u32,
    /// Offset of the latest snapshot known readable (published and read back by this owner, or
    /// found at attach); `START` for none.
    pub(crate) snapshot: String,
    /// Retention this owner advanced the stream to (`START`: none).
    pub(crate) retained: String,
    pub(crate) snapper: Arc<Snapper>,
    /// A snapshot is pinning the state at `offset`: no commit is acknowledged until it closes.
    pub(crate) window: bool,
    /// A due snapshot is waiting to open its window: the next commit waits at its commit point
    /// (up to `WINDOW_WAIT`) until it has, so a writer committing back to back cannot starve it.
    pub(crate) window_wanted: bool,
    pub(crate) snapshot_stats: Vec<SnapshotStat>,
    /// Stream offset of the local state attach started from (`START`: rebuilt from nothing), and of
    /// the snapshot it installed (`START`: none).
    pub(crate) attached_from: String,
    pub(crate) installed: String,
}

pub(crate) struct SnapshotStat {
    pub(crate) offset: String,
    pub(crate) bytes: usize,
    pub(crate) raw: usize,
    pub(crate) copy: Duration,
    pub(crate) total: Duration,
}

impl Db {
    /// The log since the latest snapshot outgrew the database (and the configured minimum). The log
    /// is counted in frame bytes, not taken from offsets (opaque): what attach replayed after the
    /// snapshot it started from (plus, from trusted local files, the sidecar's count), and every
    /// frame acknowledged since; a snapshot taken subtracts what it covers.
    pub(crate) fn snapshot_due(&self) -> bool {
        self.log > (self.pages as u64 * PAGE as u64).max(snapshot_min_bytes())
    }

    pub(crate) fn overlay_end(&self) -> i64 {
        self.overlay
            .iter()
            .next_back()
            .map(|(o, d)| o + d.len() as i64)
            .unwrap_or(0)
    }

    /// Another owner, a writer outside the protocol, or another incarnation of the stream holds
    /// the stream (the poison's class, [`Error::is_fenced`]).
    pub(crate) fn fenced(&self) -> bool {
        self.poisoned.as_ref().is_some_and(Error::is_fenced)
    }

    pub(crate) fn poison(&mut self, why: Error) -> c_int {
        eprintln!(
            "sqlite-ursula-vfs: {}: {why}; database poisoned (re-attach to recover)",
            self.url
        );
        self.poisoned = Some(why);
        self.overlay.clear();
        self.committed = false;
        ffi::SQLITE_IOERR_WRITE
    }

    /// A local operation of an acknowledged transaction (the rest of its WAL writes, -shm growth)
    /// failed: SQLite rolls the transaction back locally, so the file no longer
    /// reflects the stream offset; poison it (re-attach replays the commit from the stream).
    pub(crate) fn post_ack(&mut self, what: &'static str, rc: c_int) -> c_int {
        if rc != OK && self.committed {
            self.poison(Error::PostAck { what, code: rc });
        }
        rc
    }

    /// Test hook `URSULA_VFS_FAIL_POST_ACK=<n>`: fail the local WAL write of the n-th acknowledged
    /// commit.
    pub(crate) fn fault(&mut self) -> bool {
        if !self.fault_fired && fail_post_ack() == Some(self.acked) {
            self.fault_fired = true;
            return true;
        }
        false
    }

    /// Whether a write to the main db file is a checkpoint: under the checkpoint lock, or the
    /// closing connection's checkpoint (EXCLUSIVE file lock with the WAL still open). Anything else
    /// (a rollback journal mode, journal_mode=MEMORY/OFF) would bypass replication.
    pub(crate) fn db_write_allowed(&self) -> bool {
        self.checkpoint_started.is_some() || (self.exclusive != 0 && self.wal_open > 0)
    }
}

#[derive(Default)]
pub(crate) struct Registry {
    pub(crate) dbs: HashMap<String, Arc<Mutex<Db>>>,
    /// Open main-db handles per path (attached or not): attach requires none.
    pub(crate) open: HashMap<String, usize>,
    /// Paths being attached: `x_open` refuses their main db meanwhile (an open would read under
    /// attach's writes, and its close would drop the lock attach holds against other processes:
    /// `lock_unused`). Each keeps its binding in `dbs` (if any) until the attach ends.
    pub(crate) attaching: HashSet<String>,
    /// Paths whose last attach failed, with the reason (`ursula_status`, and the refusal `x_open`
    /// logs: see `refused`).
    pub(crate) failed: HashMap<String, Arc<Error>>,
    /// Host locks held for the process lifetime.
    pub(crate) locks: HashMap<String, fs::File>,
    /// Snapshot thread per attached database.
    pub(crate) snappers: HashMap<String, SnapshotThread>,
}

pub(crate) type SnapshotThread = (Arc<Snapper>, std::thread::JoinHandle<()>);

pub(crate) fn registry() -> MutexGuard<'static, Registry> {
    static R: OnceLock<Mutex<Registry>> = OnceLock::new();
    R.get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

pub(crate) fn lookup(path: &str) -> Option<Arc<Mutex<Db>>> {
    registry().dbs.get(path).cloned()
}

/// The attachment of `path`, for `ursula_status` and `ursula_stats`.
pub(crate) fn attached(path: &str) -> Result<Arc<Mutex<Db>>, Error> {
    let reg = registry();
    if let Some(db) = reg.dbs.get(path) {
        return Ok(db.clone());
    }
    Err(Error::NotAttached {
        path: path.to_owned(),
        last_failure: reg.failed.get(path).cloned(),
    })
}

/// Why `x_open` refuses the main db `path` that has no binding: a sidecar next to it makes it the
/// cache of a stream, which passed through to "unix" would take commits that never reach the stream
/// (in a process that never attached it, or after its attach failed: the files may hold anything
/// between their old state and the stream's). A path without a sidecar passes through. No WAL open
/// can follow a refusal: SQLite opens `<db>-wal` only through a connection whose main db it opened.
pub(crate) fn refused(reg: &Registry, path: &str) -> Option<Refusal> {
    if !fs::exists(format!("{path}-ursula")).unwrap_or(true) {
        return None;
    }
    Some(match reg.failed.get(path) {
        Some(why) => Refusal::AttachFailed(why.clone()),
        None => Refusal::NotAttached,
    })
}

/// Why `x_open` refuses a main db (`refused`), for its log line.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Refusal {
    #[error("its last attach failed ({0}); attach it again")]
    AttachFailed(Arc<Error>),
    #[error("it is the cache of an Ursula stream (it has a sidecar); attach it first")]
    NotAttached,
}

pub(crate) fn lock(db: &Mutex<Db>) -> MutexGuard<'_, Db> {
    db.lock().unwrap_or_else(|e| e.into_inner())
}
