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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Instant;

use libsqlite3_sys as ffi;

use crate::capture::db_len;
use crate::config::fail_post_ack;
use crate::config::snapshot_min_bytes;
use crate::error::Action;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::error::WriteRefusal;
use crate::host::OK;
use crate::log;
use crate::log::Level;
use crate::snapshotter::Snapper;
use crate::snapshotter::SnapshotFailing;
use crate::stats::Stats;

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
    /// Set once a fence poisons the database (`Poisoned::fenced`), and never cleared: read without
    /// the mutex by every read transaction's start (`vfs::x_shm_lock`), which must not wait for a
    /// commit holding the mutex across its append. The registry hands it to each main db handle
    /// (`Registry::fences`).
    pub(crate) fence: Arc<AtomicBool>,
    /// Why the latest write was refused before it reached the stream, which applied nothing of it:
    /// by a layer in front of the stream (`writes_refused`), or by the extension itself, which
    /// never sent it (`refuse_unsent`). `ursula_status` reports it (`writes_refused`,
    /// `refused_kind`, `refused_reason`). Cleared by the next acknowledged commit. Never set while
    /// `poisoned`: a poisoned database needs a re-attach whatever the refusing layer does
    /// (`set_poisoned`).
    pub(crate) refused: Option<WriteRefusal>,
    /// The latest refusal that a layer in front of the stream answered and that refuses other
    /// writes too (`WriteRefusal::refuses_other_writes`), cleared with `refused`: what that layer
    /// does now. A write the extension refused itself, or a 413 for one frame alone, leaves it.
    /// `payment_required` follows it, and so does whether a snapshot may be taken
    /// (`snapshot_wanted`).
    pub(crate) layer_refused: Option<WriteRefusal>,
    /// The epoch and producer sequence of the latest commit refused before it reached the stream
    /// (`refuse`), until a commit is acknowledged. The next commit goes out with them, so a
    /// duplicate's answer to it that none of its own attempts explains may be the refused
    /// commit's receipt: it may have been applied after all (`append_commit`).
    pub(crate) refused_at: Option<(u64, u64)>,
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
    /// What `ursula_stats` drains, bounded whether or not anything drains it.
    pub(crate) stats: Stats,
    pub(crate) checkpoint_started: Option<Instant>,
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
    /// Snapshot attempts that failed in a row since one published a snapshot or found one at its
    /// offset, and the wait before the next (`None` while none has).
    pub(crate) snapshot_failing: Option<SnapshotFailing>,
    /// The compressed size of the latest snapshot body built, published or not.
    pub(crate) snapshot_bytes: Option<usize>,
    /// The largest snapshot body this owner published, or installed at attach
    /// (`attach::Applier::installed_bytes`), in bytes: the publish of a larger body that fails may
    /// have failed for its size, which a probe of the stream decides
    /// (`snapshotter::FailureKind::may_be_size`).
    pub(crate) snapshot_published_bytes: usize,
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

impl Poisoned {
    /// The kind `ursula_status` reports for the poison: `fenced` once a fence was seen, whatever
    /// the first reason was, and otherwise the first reason's (`Error::poison_kind`).
    pub(crate) fn kind(&self) -> ErrorKind {
        if self.fenced {
            ErrorKind::Fenced
        } else {
            self.first.poison_kind()
        }
    }

    /// What a caller does about the poison: the kind's action (a poison's kind always has one).
    pub(crate) fn action(&self) -> Action {
        self.kind().action().unwrap_or(Action::Reattach)
    }
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

    /// A snapshot is due and may be taken now: never while the database is poisoned, nor while
    /// writes are refused before they reach the stream by a layer that refuses other writes too
    /// (`layer_refused`): its publish would be refused too. The next acknowledged commit clears the
    /// refusal and requests one if it is still due. A commit frame too large for one request,
    /// never sent or refused with a 413 in front, holds no snapshot back: an owner that stops
    /// committing after one still publishes, and retention still moves. Nor does it lift the hold
    /// of a layer that refused before it.
    pub(crate) fn snapshot_wanted(&self) -> bool {
        self.snapshot_due() && self.poisoned.is_none() && self.layer_refused.is_none()
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
    /// flip back. Logged when it poisons the database, and when it fences a poisoned one. It drops
    /// a refusal recorded before (`refused`): the database now needs a re-attach whatever the
    /// refusing layer does, so `ursula_status` reports the poison alone.
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
        if fenced {
            self.fence.store(true, Ordering::Release);
        }
        self.refused = None;
        self.layer_refused = None;
    }

    /// The write transaction's commit was refused before it reached the stream, and so was every
    /// attempt of it (`Append::Refused`): the write fails with SQLITE_IOERR_AUTH and SQLite rolls
    /// the transaction back, as on a poison, but the database is not poisoned. Its epoch, producer
    /// sequence and offset still describe the stream, so the next commit is sent as usual, with the
    /// sequence the refused one left free (`refused_at`), and is acknowledged once the refusals
    /// stop: no attach is needed.
    pub(crate) fn refuse(&mut self, refusal: WriteRefusal) -> c_int {
        self.writes_refused(refusal, "commit");
        self.refused_at = Some((self.epoch, self.seq.saturating_add(1)));
        self.commit_failed_alone()
    }

    /// The write transaction's commit was refused by the extension itself and never sent
    /// (`Append::Unsent`: its frame is over the frame limit). It fails alone as a refused commit
    /// does, but no exchange took place. So a layer's refusal recorded before it stays
    /// (`layer_refused`), and so does the sequence check (`refused_at`), which it neither sets nor
    /// clears: the stream cannot hold a frame that was never sent.
    pub(crate) fn refuse_unsent(&mut self, refusal: WriteRefusal) -> c_int {
        if self.poisoned.is_none() {
            self.log_refusal(&refusal, "commit");
            self.refused = Some(refusal);
        }
        self.commit_failed_alone()
    }

    /// Drops the write transaction's overlay: the write that fails with the code returned,
    /// SQLITE_IOERR_AUTH, rolls the transaction back without poisoning the database.
    fn commit_failed_alone(&mut self) -> c_int {
        self.overlay.clear();
        self.committed = false;
        ffi::SQLITE_IOERR_AUTH
    }

    /// Records a write that a layer in front of the stream refused before it reached the stream
    /// (`during`: which, for the log), as the latest refusal, and as the layer's when it refuses
    /// other writes too. A 413 for one frame alone leaves the refusal before it, and the snapshots
    /// that one holds back, as they were. A poisoned database records none (a snapshot publish
    /// under way can be refused after the poison): see `set_poisoned`.
    pub(crate) fn writes_refused(&mut self, refusal: WriteRefusal, during: &str) {
        if self.poisoned.is_some() {
            return;
        }
        self.log_refusal(&refusal, during);
        if refusal.refuses_other_writes() {
            self.layer_refused = Some(refusal.clone());
        }
        self.refused = Some(refusal);
    }

    /// Logs a refusal (`during`: of which write) when no refusal of its kind is recorded yet: a
    /// 402 as `payment_required` with the layer's explanation (as since 0.7.1), any other as
    /// `writes_refused` with its kind.
    fn log_refusal(&self, refusal: &WriteRefusal, during: &str) {
        if self
            .refused
            .as_ref()
            .is_some_and(|last| last.kind() == refusal.kind())
        {
            return;
        }
        match refusal {
            WriteRefusal::PaymentRequired { body } => {
                log::emit(Level::Warn, "payment_required", &[
                    ("file", &self.path),
                    ("stream", &self.url),
                    ("during", &during),
                    ("reason", body),
                ]);
            }
            other => log::emit(Level::Warn, "writes_refused", &[
                ("file", &self.path),
                ("stream", &self.url),
                ("during", &during),
                ("kind", &other.kind()),
                ("reason", other),
            ]),
        }
    }

    /// A commit was acknowledged: whatever refused writes before accepts them again, and the
    /// sequence a refused commit left free is taken.
    pub(crate) fn writes_accepted(&mut self) {
        self.refused_at = None;
        self.layer_refused = None;
        if self.refused.take().is_some() {
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
    /// The fence flag of each binding in `dbs` (`Db::fence`), bound and unbound with it, which
    /// `x_open` hands to the main db handles it opens without taking the database's mutex.
    pub(crate) fences: HashMap<String, Arc<AtomicBool>>,
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

/// Why `x_open` refuses the main db `path` that has no binding. After a failed attach of it in
/// this process, always: the application meant it to be replicated, and passed through to "unix"
/// its commits would never reach the stream (and its files may hold anything between their old
/// state and the stream's). An attach refused for the file as it stands records none
/// (`attach::refuses_the_file`). Otherwise a sidecar next to it makes it the cache of a stream (in
/// a process that never attached it), and a path without one passes through. No WAL open can
/// follow a refusal: SQLite opens `<db>-wal` only through a connection whose main db it opened.
pub(crate) fn refused(reg: &Registry, path: &str) -> Option<Refusal> {
    if let Some(why) = reg.failed.get(path) {
        return Some(Refusal::AttachFailed(why.clone()));
    }
    if !fs::exists(format!("{path}-ursula")).unwrap_or(true) {
        return None;
    }
    Some(Refusal::NotAttached)
}

/// Why `x_open` refuses a main db (`refused`), for its log line.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Refusal {
    #[error(
        "its last attach failed ({}: {0}); it opens through the extension once an attach succeeds",
        .0.kind().as_str()
    )]
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
            fence: Arc::new(AtomicBool::new(false)),
            refused: None,
            layer_refused: None,
            refused_at: None,
            overlay: BTreeMap::new(),
            wal_written: BTreeMap::new(),
            committed: false,
            commit_frame_no: 0,
            wal_open: 0,
            exclusive: 0,
            writer: 0,
            acked: 0,
            fault_fired: false,
            stats: Stats::default(),
            checkpoint_started: None,
            pages: 0,
            snapshot: crate::client::START.into(),
            retained: crate::client::START.into(),
            snapper: Arc::new(Snapper::default()),
            window: false,
            window_wanted: false,
            attached_from: crate::client::START.into(),
            installed: crate::client::START.into(),
            attach_ms: 0,
            append_retries: 0,
            snapshot_published_at: None,
            snapshot_failing: None,
            snapshot_bytes: None,
            snapshot_published_bytes: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use libsqlite3_sys as ffi;

    use super::Db;
    use super::Mode;
    use super::Refusal;
    use super::Registry;
    use super::refused;
    use crate::error::Error;
    use crate::error::Fence;
    use crate::error::FrameLimit;
    use crate::error::WriteRefusal;

    // A commit refused before it reached the stream (a 402, or any refusal that applied nothing)
    // fails its transaction alone: the overlay goes, as on a poison, but the database is not
    // poisoned and keeps its epoch, sequence and offset, so the next commit goes out as usual. The
    // latest refusal shows until a commit is acknowledged, or until a poison, which needs a
    // re-attach whatever the refusing layer does.
    #[test]
    fn a_refused_commit_fails_alone() {
        let payment = |body: &str| WriteRefusal::PaymentRequired { body: body.into() };
        let pressure = WriteRefusal::NotApplied {
            status: 503,
            reason: "RaftLogPressure".into(),
        };
        let mut d = Db::for_tests();
        d.seq = 4;
        d.offset = "00000000000000000009".into();
        d.overlay.insert(0, vec![1u8; 32]);
        d.committed = true;
        assert_eq!(d.refuse(payment("quota exhausted")), ffi::SQLITE_IOERR_AUTH);
        assert!(d.overlay.is_empty() && !d.committed);
        assert!(d.poisoned.is_none() && !d.fenced());
        assert_eq!(d.refused, Some(payment("quota exhausted")));
        assert_eq!(
            (d.epoch, d.seq, d.offset.as_str()),
            (2, 4, "00000000000000000009")
        );
        // The next commit goes out at the epoch and sequence the refused one left free.
        assert_eq!(d.refused_at, Some((2, 5)));
        d.refuse(payment(""));
        assert_eq!(d.refused, Some(payment("")));
        d.writes_accepted();
        assert!(d.refused.is_none() && d.refused_at.is_none());
        // Any other refusal that applied nothing: the same, with its own reason.
        d.overlay.insert(0, vec![1u8; 32]);
        assert_eq!(d.refuse(pressure.clone()), ffi::SQLITE_IOERR_AUTH);
        assert!(d.overlay.is_empty() && d.poisoned.is_none());
        assert_eq!((d.seq, d.refused.as_ref()), (4, Some(&pressure)));
        d.refuse(payment("quota exhausted"));
        assert_eq!(d.refused, Some(payment("quota exhausted")));
        // A fence after a refusal still poisons, and the poison clears the refusal.
        d.poison(Error::Fenced(Fence::Superseded {
            epoch: 2,
            current: Some(3),
        }));
        assert!(d.fenced());
        assert!(d.refused.is_none());
        // A refusal after a poison (a snapshot publish under way) is not recorded.
        d.writes_refused(payment("quota exhausted"), "snapshot publish");
        d.writes_refused(pressure, "commit");
        assert!(d.refused.is_none() && d.fenced());
    }

    // A due snapshot waits while writes are refused by a layer that refuses other writes too (its
    // publish would be refused too), and while the database is poisoned. An acknowledged commit lets
    // it go again. A commit frame too large to send holds it back for no reason, so it does not, nor
    // does a 413 in front.
    #[test]
    fn no_snapshot_while_writes_are_refused() {
        let mut d = Db::for_tests();
        assert!(!d.snapshot_wanted(), "not due");
        d.log = u64::MAX;
        assert!(d.snapshot_due() && d.snapshot_wanted());
        for refusal in [
            WriteRefusal::NotApplied {
                status: 503,
                reason: "RaftLogPressure".into(),
            },
            WriteRefusal::Unauthorized {
                body: String::new(),
            },
            WriteRefusal::Forbidden {
                body: String::new(),
            },
            WriteRefusal::RateLimited {
                body: String::new(),
            },
            WriteRefusal::PaymentRequired {
                body: String::new(),
            },
        ] {
            let kind = refusal.kind();
            d.refuse(refusal);
            assert!(!d.snapshot_wanted(), "{kind}");
            d.writes_accepted();
            assert!(d.snapshot_wanted(), "{kind}");
        }
        d.refuse_unsent(WriteRefusal::frame_over_limit(40 << 20, 32 << 20));
        assert!(d.refused.is_some() && d.snapshot_wanted(), "too_large");
        let in_front = WriteRefusal::TooLarge {
            bytes: 40 << 20,
            over: FrameLimit::InFront {
                token: "RequestBodyTooLarge".into(),
            },
        };
        d.refuse(in_front.clone());
        assert!(d.layer_refused.is_none() && d.snapshot_wanted(), "413");
        // A 413 for one frame leaves a refusal of other writes before it, which still holds the
        // snapshot back.
        d.refuse(WriteRefusal::NotApplied {
            status: 503,
            reason: "RaftLogPressure".into(),
        });
        d.refuse(in_front.clone());
        assert_eq!(d.refused, Some(in_front));
        assert!(!d.snapshot_wanted(), "503, then a 413");
        d.poison(Error::ProducerExpiredAgain);
        assert!(!d.snapshot_wanted());
    }

    // A transaction the extension refuses itself, never sent, leaves what a layer refused before
    // it: the layer still refuses writes, so `payment_required` stays and snapshots still wait.
    // The latest refusal, the one `ursula_status` reports, is the transaction's. It neither sets
    // nor clears the sequence check a refused commit needs (`refused_at`): the stream cannot hold
    // a frame that was never sent. An acknowledged commit clears both refusals.
    #[test]
    fn a_transaction_refused_unsent_leaves_what_the_layer_refused() {
        let mut d = Db::for_tests();
        d.log = u64::MAX;
        let payment = WriteRefusal::PaymentRequired {
            body: "quota exhausted".into(),
        };
        let too_large = WriteRefusal::frame_over_limit(40 << 20, 32 << 20);

        d.refuse(payment.clone());
        let at = d.refused_at;
        assert!(at.is_some());
        assert_eq!(d.refuse_unsent(too_large.clone()), ffi::SQLITE_IOERR_AUTH);
        assert_eq!(d.refused, Some(too_large.clone()));
        assert_eq!(d.layer_refused, Some(payment));
        assert_eq!(d.refused_at, at);
        assert!(!d.snapshot_wanted());
        d.writes_accepted();
        assert!(d.refused.is_none() && d.layer_refused.is_none() && d.refused_at.is_none());

        // Alone, it records no layer refusal and no sequence check.
        d.refuse_unsent(too_large.clone());
        assert_eq!(d.refused, Some(too_large));
        assert!(d.layer_refused.is_none() && d.refused_at.is_none());
        assert!(d.snapshot_wanted());
    }

    // After a failed attach, `x_open` refuses the path whether or not a sidecar exists (the
    // attach may have failed before writing one), naming the failure's kind; a path never
    // attached opens unless it has a sidecar.
    #[test]
    fn a_failed_attach_refuses_opens_without_a_sidecar_too() {
        let dir = std::env::temp_dir().join(format!("ursula-refused-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("app.db").to_string_lossy().into_owned();
        let mut reg = Registry::default();
        assert!(refused(&reg, &path).is_none());
        reg.failed.insert(
            path.clone(),
            Arc::new(Error::Status {
                op: "head",
                url: "http://h/s2/kv1".into(),
                status: 400,
                body: "bucket_id must be 4 to 64 bytes, got 2 bytes".into(),
            }),
        );
        let why = refused(&reg, &path).unwrap();
        assert!(matches!(why, Refusal::AttachFailed(_)));
        assert!(
            why.to_string()
                .starts_with("its last attach failed (invalid_name: head ")
        );
        reg.failed.clear();
        std::fs::write(format!("{path}-ursula"), "x").unwrap();
        assert!(matches!(refused(&reg, &path), Some(Refusal::NotAttached)));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // A fence raises the flag every read transaction's start checks without the database's mutex,
    // and nothing lowers it. Any other poison leaves it down.
    #[test]
    fn a_fence_raises_the_read_flag() {
        let mut d = Db::for_tests();
        d.poison(Error::ProducerExpiredAgain);
        assert!(!d.fence.load(Ordering::Acquire));
        d.poison(Error::Fenced(Fence::Superseded {
            epoch: 2,
            current: Some(3),
        }));
        assert!(d.fence.load(Ordering::Acquire));
        d.poison(Error::LostWrite { offset: 56 });
        assert!(d.fence.load(Ordering::Acquire) && d.fenced() && d.damaged());
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
