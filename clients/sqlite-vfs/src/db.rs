//! Attached databases: their replication state ([`Db`]) and the process-wide registry.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ffi::c_int;
use std::fmt::Display;
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
use crate::host::OK;
use crate::log;
use crate::log::Level;
use crate::snapshotter::Snapper;
use crate::wal::db_len;

pub(crate) struct CommitStat {
    pub(crate) bytes: usize,
    pub(crate) raw: usize,
    pub(crate) pages: usize,
    pub(crate) attempts: u32,
    pub(crate) append: Duration,
    pub(crate) vfs: Duration,
}

/// How `ursula_attach` binds a file to its stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    /// Claims the stream, which fences every earlier owner, and replicates every commit.
    Owner,
    /// Claims nothing and never writes the stream (no claim, commit, snapshot or retention move),
    /// so it fences no owner and a layer that refuses writes does not stop it: the file holds the
    /// stream as of the attach, and every write fails with SQLITE_READONLY (`x_write`).
    ReadOnly,
}

/// The mode argument of `ursula_attach`: `'owner'` (the default, also for an absent or NULL
/// argument) or `'read_only'`.
impl TryFrom<&str> for Mode {
    type Error = Error;

    fn try_from(mode: &str) -> Result<Mode, Error> {
        match mode {
            "" | "owner" => Ok(Mode::Owner),
            "read_only" => Ok(Mode::ReadOnly),
            other => Err(Error::AttachMode {
                mode: other.to_owned(),
            }),
        }
    }
}

pub(crate) struct Db {
    pub(crate) url: String,
    pub(crate) mode: Mode,
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
    /// The owner's epoch; for a read-only attachment, the highest one claimed up to `offset`.
    pub(crate) epoch: u64,
    /// Producer sequence of the last acknowledged append (the claim is 0).
    pub(crate) seq: u64,
    /// Stream offset after the last acknowledged frame.
    pub(crate) offset: String,
    /// Frame bytes since the latest snapshot, counted locally (see `snapshot_due`).
    pub(crate) log: u64,
    /// Why the database is poisoned (re-attach to recover), see `set_poisoned`.
    pub(crate) poisoned: Option<Poisoned>,
    /// A layer in front of the stream refused the latest write with 402 Payment Required, with its
    /// explanation (see `payment_refused`): cleared by the next acknowledged commit.
    pub(crate) payment_required: Option<String>,
    /// WAL writes of that transaction not yet on the local WAL, by offset (WAL writes never
    /// partially overlap: the header at 0, frame headers at frame offsets, page data at frame
    /// offset + 24).
    pub(crate) overlay: BTreeMap<i64, Vec<u8>>,
    /// The crc32c of every WAL page this process wrote in the WAL's current generation, by file
    /// offset. A page read back with other bytes was lost on its way to the disk (see
    /// `overlay_read`).
    pub(crate) wal_written: BTreeMap<i64, u32>,
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
    /// How long the attach took.
    pub(crate) attach_ms: u64,
    /// Append attempts beyond the first, over every acknowledged commit.
    pub(crate) append_retries: u64,
    /// When this owner last saw a snapshot published (its own, or a newer one it found).
    pub(crate) snapshot_published_at: Option<Instant>,
    /// Snapshot attempts that failed since the last one that did not, and the last failure.
    pub(crate) snapshot_failures: u32,
    pub(crate) snapshot_error: Option<String>,
}

/// Why an attached database is poisoned: the first reason, and what any reason so far showed.
pub(crate) struct Poisoned {
    /// The first reason, which a later one never replaces.
    pub(crate) first: Error,
    /// A reason so far was a fence ([`Error::is_fenced`]), the first or a later one: once set, it
    /// stays.
    pub(crate) fenced: bool,
    /// A reason so far says the local files may hold other pages than this process wrote
    /// ([`Error::is_local_damage`]): once set, it stays.
    pub(crate) damaged: bool,
}

pub(crate) struct SnapshotStat {
    pub(crate) offset: String,
    pub(crate) bytes: usize,
    pub(crate) raw: usize,
    pub(crate) copy: Duration,
    pub(crate) total: Duration,
}

impl Db {
    /// The log since the latest snapshot outgrew the database (and the configured minimum,
    /// `snapshot_due_bytes`). The log is counted in frame bytes, not taken from offsets (opaque):
    /// what attach replayed after the snapshot it started from (plus, from trusted local files, the
    /// sidecar's count), and every frame acknowledged since; a snapshot taken subtracts what it
    /// covers.
    pub(crate) fn snapshot_due(&self) -> bool {
        self.log > self.snapshot_due_bytes()
    }

    /// The log size past which a snapshot is due: the database's size, or the configured minimum
    /// (`URSULA_VFS_SNAPSHOT_MIN_BYTES`) when larger.
    pub(crate) fn snapshot_due_bytes(&self) -> u64 {
        db_len(self.pages).max(snapshot_min_bytes())
    }

    pub(crate) fn overlay_end(&self) -> i64 {
        self.overlay
            .iter()
            .next_back()
            .map(|(&o, d)| o.saturating_add(i64::try_from(d.len()).unwrap_or(i64::MAX)))
            .unwrap_or(0)
    }

    /// Another owner, a writer outside the protocol, or another incarnation of the stream holds
    /// the stream: a fence was observed since the database was poisoned (`Poisoned::fenced`).
    pub(crate) fn fenced(&self) -> bool {
        self.poisoned.as_ref().is_some_and(|p| p.fenced)
    }

    /// The local files may hold other pages than this process wrote (`Poisoned::damaged`): nothing
    /// read from them may reach the stream.
    pub(crate) fn damaged(&self) -> bool {
        self.poisoned.as_ref().is_some_and(|p| p.damaged)
    }

    /// Poisons the database (`set_poisoned`) and drops the write transaction's overlay: the write
    /// that fails with the code returned rolls the transaction back.
    pub(crate) fn poison(&mut self, why: Error) -> c_int {
        self.set_poisoned(why, None);
        self.overlay.clear();
        self.committed = false;
        ffi::SQLITE_IOERR_WRITE
    }

    /// Records why the database is poisoned (`during`: what failed, for the log). The first reason
    /// stays: a later one follows from it (the rest of a fenced transaction's writes, a lost page
    /// read again) or changes nothing (every commit is refused already). But a fence and a lost
    /// write are kept whenever they come (`Poisoned`), so [`Db::fenced`] and [`Db::damaged`] never
    /// flip back. Logged when it poisons the database, and when it fences a poisoned one.
    pub(crate) fn set_poisoned(&mut self, why: Error, during: Option<&str>) {
        let fenced = why.is_fenced();
        let damaged = why.is_local_damage();
        if self.poisoned.as_ref().is_none_or(|p| fenced && !p.fenced) {
            let mut fields: Vec<(&str, &dyn Display)> = vec![
                ("file", &self.path),
                ("stream", &self.url),
                ("fenced", &fenced),
            ];
            if let Some(during) = &during {
                fields.push(("during", during));
            }
            fields.push(("reason", &why));
            log::emit(Level::Warn, "poisoned", &fields);
        }
        match &mut self.poisoned {
            Some(p) => {
                p.fenced |= fenced;
                p.damaged |= damaged;
            }
            None => {
                self.poisoned = Some(Poisoned {
                    first: why,
                    fenced,
                    damaged,
                });
            }
        }
    }

    /// A layer in front of the stream refused the write transaction's commit with 402 Payment
    /// Required, and nothing of it reached the stream (`Append::PaymentRequired`): the write fails
    /// with SQLITE_IOERR_AUTH and SQLite rolls the transaction back, as on a poison, but the
    /// database is not poisoned. Its epoch, producer sequence and offset still describe the
    /// stream, so the next commit is sent as usual, and is acknowledged once the layer accepts
    /// writes again: no attach is needed.
    pub(crate) fn refuse(&mut self, body: String) -> c_int {
        self.payment_refused(body, "commit");
        self.overlay.clear();
        self.committed = false;
        ffi::SQLITE_IOERR_AUTH
    }

    /// Records a write refused with 402 Payment Required (`during`: which, for the log), logged
    /// when no refusal is recorded yet.
    pub(crate) fn payment_refused(&mut self, body: String, during: &str) {
        if self.payment_required.is_none() {
            log::emit(Level::Warn, "payment_required", &[
                ("file", &self.path),
                ("stream", &self.url),
                ("during", &during),
                ("reason", &body),
            ]);
        }
        self.payment_required = Some(body);
    }

    /// A commit was acknowledged: a layer that refused writes with 402 accepts them again.
    pub(crate) fn writes_accepted(&mut self) {
        if self.payment_required.take().is_some() {
            log::emit(Level::Info, "writes_resumed", &[
                ("file", &self.path),
                ("stream", &self.url),
            ]);
        }
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

#[cfg(test)]
impl Db {
    /// An attachment of nothing: the state alone, for tests of its bookkeeping.
    pub(crate) fn for_tests() -> Db {
        Db {
            url: "http://h/b/s".into(),
            mode: Mode::Owner,
            incarnation: "i1".into(),
            producer: "sqlite-ursula-vfs/i1".into(),
            sidecar: "/data/app.db-ursula".into(),
            stamp: String::new(),
            path: "/data/app.db".into(),
            epoch: 2,
            seq: 0,
            offset: crate::client::START.into(),
            log: 0,
            poisoned: None,
            payment_required: None,
            overlay: BTreeMap::new(),
            wal_written: BTreeMap::new(),
            committed: false,
            commit_frame_no: 0,
            wal_open: 0,
            exclusive: 0,
            writer: 0,
            acked: 0,
            fault_fired: false,
            stats: Vec::new(),
            checkpoint_started: None,
            checkpoints: Vec::new(),
            pages: 0,
            snapshot: crate::client::START.into(),
            retained: crate::client::START.into(),
            snapper: Arc::new(Snapper::default()),
            window: false,
            window_wanted: false,
            snapshot_stats: Vec::new(),
            attached_from: crate::client::START.into(),
            installed: crate::client::START.into(),
            attach_ms: 0,
            append_retries: 0,
            snapshot_published_at: None,
            snapshot_failures: 0,
            snapshot_error: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use libsqlite3_sys as ffi;

    use super::Db;
    use super::Mode;
    use crate::error::Error;
    use crate::error::Fence;

    // A commit refused with 402 fails its transaction alone: the overlay goes, as on a poison, but
    // the database is not poisoned and keeps its epoch, sequence and offset, so the next commit
    // goes out as usual. The refusal shows until a commit is acknowledged.
    #[test]
    fn a_refused_commit_fails_alone() {
        let mut d = Db::for_tests();
        d.seq = 4;
        d.offset = "00000000000000000009".into();
        d.overlay.insert(0, vec![1u8; 32]);
        d.committed = true;
        assert_eq!(d.refuse("quota exhausted".into()), ffi::SQLITE_IOERR_AUTH);
        assert!(d.overlay.is_empty() && !d.committed);
        assert!(d.poisoned.is_none() && !d.fenced());
        assert_eq!(d.payment_required.as_deref(), Some("quota exhausted"));
        assert_eq!(
            (d.epoch, d.seq, d.offset.as_str()),
            (2, 4, "00000000000000000009")
        );
        d.refuse(String::new());
        assert_eq!(d.payment_required.as_deref(), Some(""));
        d.writes_accepted();
        assert!(d.payment_required.is_none());
        // A fence after a refusal still poisons.
        d.refuse("quota exhausted".into());
        d.poison(Error::Fenced(Fence::Superseded {
            epoch: 2,
            current: Some(3),
        }));
        assert!(d.fenced());
    }

    #[test]
    fn attach_modes() {
        assert_eq!(Mode::try_from("").unwrap(), Mode::Owner);
        assert_eq!(Mode::try_from("owner").unwrap(), Mode::Owner);
        assert_eq!(Mode::try_from("read_only").unwrap(), Mode::ReadOnly);
        assert!(matches!(
            Mode::try_from("readonly"),
            Err(Error::AttachMode { mode }) if mode == "readonly"
        ));
    }

    // The first poison's reason stays, whatever fails after it (the rest of the transaction, a
    // lost page read again, the snapshot thread's own fence). A fence and a lost write stick
    // whenever they come: `fenced` and `damaged` never flip back, in either order.
    #[test]
    fn a_later_poison_does_not_replace_a_fence() {
        let mut d = Db::for_tests();
        d.poison(Error::Fenced(Fence::Superseded {
            epoch: 2,
            current: Some(3),
        }));
        assert!(d.fenced() && !d.damaged());
        d.poison(Error::LostWrite { offset: 56 });
        d.poison(Error::DbWriteOutsideCheckpoint);
        d.set_poisoned(
            Error::NotPublished {
                mx_frame: 1,
                frame: 2,
            },
            Some("snapshot at 7 not published"),
        );
        assert!(d.fenced() && d.damaged());
        assert!(matches!(
            d.poisoned.as_ref().map(|p| &p.first),
            Some(Error::Fenced(Fence::Superseded {
                epoch: 2,
                current: Some(3)
            }))
        ));
        // A fence after another reason: `fenced` too, and the first reason stays.
        let mut d = Db::for_tests();
        d.poison(Error::LostWrite { offset: 56 });
        assert!(!d.fenced() && d.damaged());
        d.poison(Error::Fenced(Fence::Reclaim));
        d.poison(Error::ProducerExpiredAgain);
        assert!(d.fenced() && d.damaged());
        assert!(matches!(
            d.poisoned.as_ref().map(|p| &p.first),
            Some(Error::LostWrite { offset: 56 })
        ));
    }
}
