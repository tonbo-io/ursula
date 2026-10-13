//! The "ursula" VFS: the file methods that route an attached database's WAL writes through
//! the overlay and append each commit to the stream at its commit point.
//!
//! SQLite calls these methods with pointers it owns, under the VFS contract every unsafe block here
//! relies on: each `file` is one this VFS opened and has not closed, laid out as [`File`] in the
//! `szOsFile` bytes SQLite allocated for it (`x_open` fills it in), and buffers and out-parameters
//! are valid for the sizes SQLite passes with them.
#![expect(
    unsafe_code,
    reason = "a SQLite VFS implements C callbacks over raw pointers SQLite owns"
)]

use std::cell::Cell;
use std::collections::BTreeMap;
use std::ffi::CStr;
use std::ffi::c_int;
use std::ffi::c_void;
use std::ptr::null;
use std::ptr::null_mut;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use libsqlite3_sys as ffi;

use crate::claim::reclaim;
use crate::client::Append;
use crate::client::append;
use crate::client::recreated;
use crate::client::stream_seq;
use crate::config::abort_after_ack;
use crate::config::max_frame_bytes;
use crate::db::Db;
use crate::db::DbFileStamp;
use crate::db::Mode;
use crate::db::lock;
use crate::db::lookup;
use crate::db::refused;
use crate::db::registry;
use crate::error::Error;
use crate::error::Fence;
use crate::error::Foreign;
use crate::error::WriteRefusal;
use crate::frame;
use crate::frame::FrameError;
use crate::frame::PAGE;
use crate::guard::contain;
use crate::host::OK;
use crate::host::WalIndexHeader;
use crate::host::unix;
use crate::host::wal_index_header;
use crate::local::sidecar_line;
use crate::local::write_sidecar;
use crate::log;
use crate::log::Level;
use crate::stats::CommitStat;
use crate::wal::FRAME;
use crate::wal::FRAME_HDR;
use crate::wal::FRAME_HDR_LEN;
use crate::wal::FRAME_LEN;
use crate::wal::PAGE_BYTES;
use crate::wal::WAL_HDR;
use crate::wal::WalClaim;
use crate::wal::be32;
use crate::wal::page_at;

/// `WAL_WRITE_LOCK` and `WAL_CKPT_LOCK`: the shm lock slots SQLite takes for a write transaction and
/// a checkpoint.
const WAL_WRITE_LOCK: c_int = 0;
const WAL_CKPT_LOCK: c_int = 1;

/// `WAL_READ_LOCK(0)`: the first of the read-mark lock slots, one of which every read transaction
/// takes shared.
const WAL_READ_LOCK_0: c_int = 3;

/// The longest a commit waits at its commit point for a due snapshot to open its window.
const WINDOW_WAIT: Duration = Duration::from_secs(1);

/// A file of this VFS: its own header and state, then the "unix" file it wraps.
#[repr(C)]
pub(crate) struct File {
    base: ffi::sqlite3_file,
    /// Main db handle of any path (`path` set, for the open count), and the main db or WAL handle
    /// of an attached database (`db` set). Null otherwise.
    ext: *mut Ext,
    /// The underlying "unix" file (szOsFile bytes from here).
    pub(crate) inner: ffi::sqlite3_file,
}

struct Ext {
    path: Option<String>,
    db: Option<Arc<Mutex<Db>>>,
    /// The fence flag of a main db handle's attachment (`Db::fence`), read without its mutex.
    fence: Option<Arc<AtomicBool>>,
    wal: bool,
    /// This main db handle holds an EXCLUSIVE file lock.
    exclusive: Cell<bool>,
}

/// The address of the "unix" file inside `f`, a file of this VFS.
fn inner(f: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    f.cast::<File>()
        .wrapping_byte_add(std::mem::offset_of!(File, inner))
        .cast()
}

/// The "unix" file inside `f` and its io methods (`None` when it has none).
///
/// # Safety
///
/// `f` is an open file of this VFS.
unsafe fn unix_file(
    f: *mut ffi::sqlite3_file,
) -> (
    *mut ffi::sqlite3_file,
    Option<&'static ffi::sqlite3_io_methods>,
) {
    let i = inner(f);
    // SAFETY: `i` is the open "unix" file inside `f` (the caller's contract).
    let methods = unsafe { (*i).pMethods };
    // SAFETY: an open file's methods are its VFS's io methods, a static table.
    (i, unsafe { methods.as_ref() })
}

/// Calls method `$m` of the "unix" file inside `$f`, an open file of this VFS, with the arguments
/// SQLite passed; `SQLITE_IOERR` when "unix" has no such method.
macro_rules! fwd {
    ($f:expr, $m:ident $(, $a:expr)*) => {{
        // SAFETY: `$f` is an open file of this VFS (the module contract).
        match unsafe { unix_file($f) } {
            (i, Some(ffi::sqlite3_io_methods { $m: Some(method), .. })) => {
                // SAFETY: SQLite's call on this file, forwarded with its arguments to the "unix"
                // file inside it.
                unsafe { method(i $(, $a)*) }
            }
            _ => ffi::SQLITE_IOERR,
        }
    }};
}

/// This VFS's state for `f`: `None` for a file it does not track.
///
/// # Safety
///
/// `f` is an open file of this VFS, and the reference does not outlive the method call.
unsafe fn ext<'a>(f: *mut ffi::sqlite3_file) -> Option<&'a Ext> {
    // SAFETY: `f` points to a `File` (the caller's contract).
    let ext = unsafe { (*f.cast::<File>()).ext };
    // SAFETY: a non-null `ext` is the `Box` `x_open` leaked for this file, freed only by
    // `take_ext`, once the file is closed or its open failed.
    unsafe { ext.as_ref() }
}

/// The attached database of a WAL handle.
///
/// # Safety
///
/// `f` is an open file of this VFS.
unsafe fn wal_db(f: *mut ffi::sqlite3_file) -> Option<Arc<Mutex<Db>>> {
    // SAFETY: the caller's contract.
    unsafe { ext(f) }
        .filter(|e| e.wal)
        .and_then(|e| e.db.clone())
}

/// The attached database of a main db handle.
///
/// # Safety
///
/// `f` is an open file of this VFS.
unsafe fn main_db(f: *mut ffi::sqlite3_file) -> Option<Arc<Mutex<Db>>> {
    // SAFETY: the caller's contract.
    unsafe { ext(f) }
        .filter(|e| !e.wal)
        .and_then(|e| e.db.clone())
}

/// The wal-index header, read through the "unix" file inside the main db file `f` (not through
/// this VFS's `xShmMap`, which takes the database's mutex on a failure).
///
/// # Safety
///
/// `f` is an open main db file of this VFS.
unsafe fn header(f: *mut ffi::sqlite3_file) -> Option<WalIndexHeader> {
    // SAFETY: the "unix" file inside an open file of this VFS is open, with "unix"'s methods.
    unsafe { wal_index_header(inner(f)) }
}

impl Db {
    /// A commit the wal-index `header` shows that no connection of this VFS made (`WalSeen::foreign`)
    /// came from a connection outside the extension, which the stream never got. Checked when a
    /// connection of this VFS takes the WAL write lock, when it releases one it committed nothing
    /// under (a recovery rebuilt the header from the WAL file), and before the snapshot thread
    /// copies pages (`during` says which). It poisons the database as local damage: no snapshot
    /// publishes the foreign pages, the next WAL write fails, and the next attach rebuilds the
    /// local files (`Db::distrust_local`).
    pub(crate) fn check_foreign(&mut self, header: Option<WalIndexHeader>, during: &str) {
        let foreign = match header.and_then(|found| Some((self.wal_seen.foreign(found)?, found))) {
            Some((seen, found)) => Some(Foreign::Frames {
                seen,
                found: found.mx_frame,
            }),
            None => self
                .untouched
                .filter(|&stamp| DbFileStamp::of(&self.path) != Some(stamp))
                .map(|_changed| Foreign::DbFile),
        };
        if let Some(foreign) = foreign {
            self.set_poisoned(Error::ForeignWrite(foreign), Some(during));
        }
    }

    /// Records a write that reached the local WAL (see `wal_written`): the WAL header starts a new
    /// generation, a page is remembered by its crc32c.
    fn record_wal_write(&mut self, off: i64, data: &[u8]) {
        if off == 0 {
            self.wal_written.clear();
        }
        if data.len() == PAGE && page_at(off) {
            self.wal_written.insert(off, crc32c::crc32c(data));
        }
    }

    /// Fsyncs the db file through the main db handle of the connection asking (the one holding the
    /// WAL write lock, or a closing one's EXCLUSIVE lock): before the local WAL starts a new
    /// generation or is truncated to nothing, so a disk image that shows the new WAL holds every
    /// page checkpointed from the old one (see `WalClaim`). Rare: once per WAL wrap. A failure
    /// poisons.
    ///
    /// `writer` and `exclusive` name open main db files: each is set while its handle holds the
    /// lock and cleared when it releases the lock or closes.
    fn sync_db(&mut self, before: &'static str) -> c_int {
        let h = [self.writer, self.exclusive].into_iter().find(|&h| h != 0);
        let rc = match h {
            Some(h) => {
                let file = std::ptr::with_exposed_provenance_mut::<ffi::sqlite3_file>(h);
                fwd!(file, xSync, ffi::SQLITE_SYNC_NORMAL)
            }
            None => ffi::SQLITE_IOERR_FSYNC,
        };
        if rc != OK {
            self.poison(Error::Fsync { before, code: rc });
        }
        rc
    }

    /// The write transaction ended (WAL write lock released): drop what never committed; after an
    /// acknowledged commit that SQLite published, advance the sidecar (no fsync: see the crate
    /// docs).
    ///
    /// Runs before the real lock is released (see `x_shm_lock`), on the main db handle `file`,
    /// whose wal-index header tells whether SQLite published the commit.
    ///
    /// # Safety
    ///
    /// `file` is this database's main db file, open and holding the WAL write lock.
    unsafe fn end_write_transaction(&mut self, file: *mut ffi::sqlite3_file) {
        self.writer = 0;
        self.overlay.clear();
        if !std::mem::take(&mut self.committed) {
            return;
        }
        // A snapshot waiting for the acknowledged commit to be published (it runs once this
        // returns and the mutex is released, seeing the outcome).
        self.snapper.window_cv.notify_all();
        // SAFETY: `file` is an open main db file of this VFS (the caller's contract).
        let (mx_frame, salts) = match unsafe { header(file) } {
            Some(h) => (h.mx_frame, h.salts),
            None => (0, [0; 8]),
        };
        if mx_frame < self.commit_frame_no {
            // Already poisoned when the snapshot thread's fence failed the rest of the
            // transaction's WAL writes: that reason stays (`set_poisoned`).
            self.poison(Error::NotPublished {
                mx_frame,
                frame: self.commit_frame_no,
            });
            return;
        }
        let wal = WalClaim {
            salts: u64::from_be_bytes(salts),
            frame: self.commit_frame_no,
        };
        let line = sidecar_line(&self.offset, self.epoch, self.log, &self.stamp, wal);
        if let Err(e) = write_sidecar(&self.sidecar, &line) {
            self.poison(e);
            return;
        }
        // After a snapshot outage, the stream takes writes again: the next attempt need not wait
        // (once a run, see `SnapshotFailing::commit_acknowledged`).
        if let Some(failing) = self.snapshot_failing.as_mut() {
            failing.commit_acknowledged(Instant::now());
        }
        if self.snapshot_due() {
            self.snapper.request();
        }
    }

    /// Appends a write transaction's commit frame `body` at the next producer sequence (taking an
    /// expired producer back first, see `reclaim`) and settles the outcome. Acknowledged: the owner
    /// moves to that sequence and the stream offset after the frame, and the append's attempts are
    /// returned. Otherwise the database is poisoned, or the commit refused alone when nothing of it
    /// reached the stream (`refuse`), and the code the write fails with is returned. A duplicate's
    /// answer that none of the commit's attempts explains, at the sequence a refused commit left
    /// free (`refused_at`), poisons: the stream holds the refused commit, applied after all, or
    /// this one, sent again by a layer in front of the stream, and the local files hold neither.
    fn append_commit(&mut self, body: &[u8]) -> Result<u32, c_int> {
        let mut outcome = append(
            &self.url,
            &self.incarnation,
            &self.producer,
            body,
            self.epoch,
            self.seq.saturating_add(1),
        );
        // An unknown producer: the server expired it (`reclaim`).
        if let Append::ProducerExpired = outcome {
            outcome = match reclaim(self) {
                // The re-claim moved the epoch up and the sequence back to the claim's.
                Ok(()) => append(
                    &self.url,
                    &self.incarnation,
                    &self.producer,
                    body,
                    self.epoch,
                    self.seq.saturating_add(1),
                ),
                // Refused before it reached the stream: nothing of the re-claim was applied.
                Err(Error::Refused { refusal, .. }) => Append::Refused(refusal),
                Err(e) => return Err(self.poison(e)),
            };
        }
        let seq = self.seq.saturating_add(1);
        let (next, attempts) = match outcome {
            // A duplicate's answer that no attempt of this commit explains, at the sequence a
            // refused commit left free: the stream may hold that commit, which a layer in front of
            // it refused after the stream applied it (design doc §2). Acknowledged, the next
            // commits would build on a local file without it while the stream holds it, with no
            // error. This commit may be what the stream holds instead (a gateway that followed a
            // redirect sent after the leader proposed it), so this can poison an owner whose
            // commit is in the stream, which costs an attach, never data.
            Append::UnexplainedDuplicate {
                next: Some(receipt),
                ..
            } if self.refused_at == Some((self.epoch, seq)) => {
                return Err(self.poison(Error::RefusedCommitApplied {
                    epoch: self.epoch,
                    seq,
                    offset: self.offset.clone(),
                    receipt,
                }));
            }
            // A duplicate's answer after an attempt of this commit whose outcome is unknown is
            // taken for that attempt's (`client::append`). At the sequence a refused commit left
            // free it may be the refused commit's instead, which nothing here tells apart (design
            // doc §2): acknowledged, with a warning that leaves a trace of the unchecked refusal.
            Append::Acked {
                next: Some(n),
                attempts,
                duplicate,
            } if n > self.offset => {
                if duplicate && self.refused_at == Some((self.epoch, seq)) {
                    log::emit(Level::Warn, "refused_commit_unchecked", &[
                        ("file", &self.path),
                        ("stream", &self.url),
                        ("epoch", &self.epoch),
                        ("seq", &seq),
                        ("offset", &self.offset),
                        ("receipt", &n),
                    ]);
                }
                (n, attempts)
            }
            // Elsewhere no commit at this sequence was refused, so an unexplained duplicate is this
            // commit, applied by an exchange whose answer said otherwise (see
            // `Append::UnexplainedDuplicate`): acknowledged like any other.
            Append::UnexplainedDuplicate {
                next: Some(n),
                attempts,
            } if n > self.offset => (n, attempts),
            Append::Acked { next: Some(n), .. }
            | Append::UnexplainedDuplicate { next: Some(n), .. } => {
                return Err(self.poison(Error::AckNotPast {
                    offset: self.offset.clone(),
                    next: n,
                }));
            }
            // A duplicate answered without its receipt: the server evicted it (more than its
            // receipt window ago). Never our own retry: this owner is the only writer of its
            // producer at its epoch (verified claim) with one append in flight, the newest, and the
            // server never evicts a producer's newest receipt. So another writer appended at this
            // epoch past `seq`.
            Append::Acked { next: None, .. } | Append::UnexplainedDuplicate { next: None, .. } => {
                return Err(self.poison(Error::Fenced(Fence::DuplicateWithoutReceipt {
                    offset: self.offset.clone(),
                    producer: self.producer.clone(),
                    epoch: self.epoch,
                    seq,
                })));
            }
            Append::Fenced { current } => {
                return Err(self.poison(Error::Fenced(Fence::Superseded {
                    epoch: self.epoch,
                    current,
                })));
            }
            // The stream at the path is another incarnation: this commit reached nothing.
            Append::Recreated => {
                return Err(self.poison(recreated(&self.url, &self.incarnation)));
            }
            // Not this owner's own retry (that is a duplicate, answered before `Stream-Seq` is
            // checked), nor an older owner's (fenced by epoch): a writer outside this protocol.
            Append::SeqConflict { body } => {
                return Err(self.poison(Error::Fenced(Fence::SeqConflict {
                    offset: self.offset.clone(),
                    stream_seq: stream_seq((self.epoch, seq)),
                    body: body.trim().to_owned(),
                })));
            }
            Append::ProducerExpired => return Err(self.poison(Error::ProducerExpiredAgain)),
            // Nothing of the transaction reached the stream: it fails alone (see `Db::refuse`).
            Append::Refused(refusal) => return Err(self.refuse(refusal)),
            // Never sent: it fails alone too, but no layer refused it (see `Db::refuse_unsent`).
            Append::Unsent(refusal) => return Err(self.refuse_unsent(refusal)),
            Append::Failed(e) => return Err(self.poison(e)),
        };
        self.writes_accepted();
        self.seq = seq;
        self.offset = next;
        Ok(attempts)
    }
}

unsafe fn x_open(
    _vfs: *mut ffi::sqlite3_vfs,
    zname: ffi::sqlite3_filename,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    let f = file.cast::<File>();
    let name = if zname.is_null() {
        None
    } else {
        // SAFETY: a non-null name is NUL-terminated and outlives the file (xOpen's contract).
        unsafe { CStr::from_ptr(zname) }.to_str().ok()
    };
    // Rollback-journal writes of an attached database would bypass replication (attach gives
    // an empty file its WAL-format page 1, so no commit ever needs a journal).
    if flags & ffi::SQLITE_OPEN_MAIN_JOURNAL != 0
        && let Some(db) = name.and_then(|n| n.strip_suffix("-journal"))
        && lookup(db).is_some()
    {
        log::emit(Level::Warn, "journal_refused", &[("file", &db)]);
        return ffi::SQLITE_CANTOPEN;
    }
    // Counted before the open, so attach (which refuses while any is counted) and an open
    // cannot pass each other; refused while `attach` runs on the path, and without a binding
    // after its last attach failed or while it has a sidecar (`refused`).
    let main = match name {
        Some(name) if flags & ffi::SQLITE_OPEN_MAIN_DB != 0 => {
            let mut reg = registry();
            if reg.attaching.contains(name) {
                return ffi::SQLITE_BUSY;
            }
            let db = reg.dbs.get(name).cloned();
            if db.is_none()
                && let Some(why) = refused(&reg, name)
            {
                log::emit(Level::Warn, "open_refused", &[
                    ("file", &name),
                    ("reason", &why),
                ]);
                return ffi::SQLITE_CANTOPEN;
            }
            let fence = reg.fences.get(name).cloned();
            let open = reg.open.entry(name.to_owned()).or_default();
            *open = open.saturating_add(1);
            Some((db, fence))
        }
        _ => None,
    };
    // Released if the open fails, or a panic unwinds through it. Once open, the file holds the
    // count until it is closed (`c_close` releases it).
    let mut counted = Counted(name.filter(|_| main.is_some()).map(str::to_owned));
    let u = unix();
    // SAFETY: `UNIX` is null or SQLite's "unix" VFS, which lives for the process.
    let open = unsafe { u.as_ref() }.and_then(|u| u.xOpen);
    // SAFETY: the "unix" file lies within SQLite's allocation for this file (`szOsFile` counts
    // it), and the other arguments are SQLite's.
    let rc = open.map_or(ffi::SQLITE_CANTOPEN, |open| unsafe {
        open(u, zname, inner(file), flags, out)
    });
    if rc != OK {
        return rc;
    }
    let ext = match (name, main) {
        (Some(name), Some((db, fence))) => Some(Ext {
            path: Some(name.to_owned()),
            db,
            fence,
            wal: false,
            exclusive: Cell::new(false),
        }),
        (Some(name), None) if flags & ffi::SQLITE_OPEN_WAL != 0 => {
            name.strip_suffix("-wal").and_then(lookup).map(|db| Ext {
                path: None,
                db: Some(db),
                fence: None,
                wal: true,
                exclusive: Cell::new(false),
            })
        }
        _ => None,
    };
    // Counted once the file is open, below: `closing` uncounts it when the file is closed.
    let wal_of = ext.as_ref().filter(|e| e.wal).and_then(|e| e.db.clone());
    if let Some(ext) = ext {
        // SAFETY: as above.
        unsafe { (*f).ext = Box::into_raw(Box::new(ext)) };
    }
    // SAFETY: as above. From here the file is open, and SQLite calls its methods.
    unsafe { (*f).base.pMethods = &raw const METHODS };
    if let Some(db) = wal_of {
        let mut d = lock(&db);
        d.wal_open = d.wal_open.saturating_add(1);
    }
    counted.0 = None;
    OK
}

/// Sets up `file` as not open: no methods (until they are set it is not open, so a failure leaves
/// nothing for SQLite to close), no state, and a "unix" file without methods, which its own xOpen
/// sets once it succeeds (so a panic in between can tell whether there is one to close).
///
/// # Safety
///
/// `file` is SQLite's allocation for a new file of this VFS (`szOsFile` bytes, laid out as `File`).
unsafe fn not_open(file: *mut ffi::sqlite3_file) {
    let f = file.cast::<File>();
    // SAFETY: the caller's contract.
    unsafe { (*f).base.pMethods = null() };
    // SAFETY: as above.
    unsafe { (*f).ext = null_mut() };
    // SAFETY: as above.
    unsafe { (*f).inner.pMethods = null() };
}

/// An open of `path` counted in the registry (`x_open`), released when dropped: when an open fails,
/// also when a panic unwinds through `x_open`, and when `c_close` returns, however its body ended.
/// So the file is not taken for open in this process for good (attach refuses a file that is
/// open).
struct Counted(Option<String>);

impl Drop for Counted {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            uncount_open(&path);
        }
    }
}

/// Takes `file`'s state out of it, for `c_close`, and for an open that failed (`failed_open`).
///
/// # Safety
///
/// `file` is a file of this VFS whose state is taken once: an open file that SQLite is closing (it
/// closes a file once), or one that `not_open` set up and whose open failed, whose `ext` is null or
/// the `Box` `x_open` made.
unsafe fn take_ext(file: *mut ffi::sqlite3_file) -> Option<Box<Ext>> {
    let f = file.cast::<File>();
    // SAFETY: the caller's contract; the state is taken once, here.
    let ext = unsafe { std::mem::replace(&mut (*f).ext, null_mut()) };
    if ext.is_null() {
        None
    } else {
        // SAFETY: a non-null `ext` is the `Box` `x_open` leaked for this file, freed only here.
        Some(unsafe { Box::from_raw(ext) })
    }
}

/// The database's bookkeeping of a handle that closes: the WAL handles open, and the main db
/// handle that holds an EXCLUSIVE lock.
fn closing(ext: &Ext) {
    if let Some(db) = &ext.db {
        let mut db = lock(db);
        if ext.wal {
            db.wal_open = db.wal_open.saturating_sub(1);
        } else if ext.exclusive.get() {
            db.exclusive = 0;
        }
    }
}

/// Closes the "unix" file under `file`, whose state `c_close` took and accounted for.
unsafe fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    fwd!(file, xClose)
}

fn uncount_open(path: &str) {
    let mut reg = registry();
    if let Some(n) = reg.open.get_mut(path) {
        *n = n.saturating_sub(1);
        if *n == 0 {
            reg.open.remove(path);
        }
    }
}

/// The first page of `buf` (read at `off`) whose bytes differ from what this process wrote there
/// (`written`, by offset), outside the transaction in progress (`overlay`).
fn lost_page(
    written: &BTreeMap<i64, u32>,
    overlay: &BTreeMap<i64, Vec<u8>>,
    buf: &[u8],
    off: i64,
) -> Option<i64> {
    let end = off.saturating_add(i64::try_from(buf.len()).unwrap_or(i64::MAX));
    written
        .range(off..end)
        .filter(|&(&p, _)| p.saturating_add(PAGE_BYTES) <= end && !overlay.contains_key(&p))
        .find_map(|(&p, &crc)| {
            let at = usize::try_from(p.saturating_sub(off)).ok()?;
            let page = buf.get(at..at.saturating_add(PAGE))?;
            (crc32c::crc32c(page) != crc).then_some(p)
        })
}

/// `[from, to)` as indexes (`None` for a negative bound).
fn span(from: i64, to: i64) -> Option<std::ops::Range<usize>> {
    Some(usize::try_from(from).ok()?..usize::try_from(to).ok()?)
}

/// Reads `buf` at `off` through the overlay.
///
/// # Safety
///
/// `file` is an open WAL file of this VFS.
unsafe fn overlay_read(
    file: *mut ffi::sqlite3_file,
    db: &mut Db,
    buf: &mut [u8],
    off: i64,
) -> c_int {
    let Ok(amt) = c_int::try_from(buf.len()) else {
        return ffi::SQLITE_IOERR_READ;
    };
    let rc = fwd!(file, xRead, buf.as_mut_ptr().cast::<c_void>(), amt, off);
    if rc != OK && rc != ffi::SQLITE_IOERR_SHORT_READ {
        return rc;
    }
    // A page this process wrote that reads back other bytes was lost on its way to the disk (a
    // write-back error, after which the kernel dropped the dirty page): SQLite does not check WAL
    // frames on a read, so commits built on it would append the damage to the stream.
    if let Some(offset) = lost_page(&db.wal_written, &db.overlay, buf, off) {
        db.poison(Error::LostWrite { offset });
        return ffi::SQLITE_IOERR_READ;
    }
    if db.overlay.is_empty() {
        return rc;
    }
    let end = off.saturating_add(i64::from(amt));
    let start = off.saturating_sub(FRAME).max(0);
    if start < end {
        for (&o, d) in db.overlay.range(start..end) {
            let d_end = o.saturating_add(i64::try_from(d.len()).unwrap_or(i64::MAX));
            let (s, e) = (o.max(off), d_end.min(end));
            if s < e {
                // `[s, e)` lies within both the read and the overlay write.
                let dst =
                    span(s.saturating_sub(off), e.saturating_sub(off)).and_then(|r| buf.get_mut(r));
                let src = span(s.saturating_sub(o), e.saturating_sub(o)).and_then(|r| d.get(r));
                match (dst, src) {
                    (Some(dst), Some(src)) if dst.len() == src.len() => dst.copy_from_slice(src),
                    _ => return ffi::SQLITE_IOERR_READ,
                }
            }
        }
    }
    if rc == OK {
        return OK;
    }
    let mut size: ffi::sqlite3_int64 = 0;
    fwd!(file, xFileSize, &raw mut size);
    if end <= size.max(db.overlay_end()) {
        OK
    } else {
        ffi::SQLITE_IOERR_SHORT_READ
    }
}

unsafe fn x_read(
    file: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    amt: c_int,
    off: ffi::sqlite3_int64,
) -> c_int {
    // SAFETY: the module contract.
    let Some(db) = (unsafe { wal_db(file) }) else {
        return fwd!(file, xRead, buf, amt, off);
    };
    let Ok(len) = usize::try_from(amt) else {
        return ffi::SQLITE_IOERR_READ;
    };
    // SAFETY: SQLite passes `amt` writable bytes at `buf`.
    let buf = unsafe { std::slice::from_raw_parts_mut(buf.cast::<u8>(), len) };
    // SAFETY: `file` is a WAL file of this VFS.
    unsafe { overlay_read(file, &mut lock(&db), buf, off) }
}

unsafe fn x_write(
    file: *mut ffi::sqlite3_file,
    buf: *const c_void,
    amt: c_int,
    off: ffi::sqlite3_int64,
) -> c_int {
    // SAFETY: the module contract.
    let Some(db) = (unsafe { wal_db(file) }) else {
        // SAFETY: the module contract.
        if let Some(db) = unsafe { main_db(file) } {
            let mut db = lock(&db);
            if !db.db_write_allowed() {
                return db.poison(Error::DbWriteOutsideCheckpoint);
            }
        }
        return fwd!(file, xWrite, buf, amt, off);
    };
    let mut db = lock(&db);
    // A read-only attachment writes neither the stream nor its WAL: its write transactions fail at
    // their first WAL write. SQLITE_READONLY is not an error on which SQLite rolls a transaction
    // back by itself: a failed COMMIT (or autocommit statement) ends it, but a spill (a statement
    // of an explicit transaction outgrowing the page cache) fails before COMMIT and leaves the
    // transaction open until the application rolls it back.
    if db.mode == Mode::ReadOnly {
        return ffi::SQLITE_READONLY;
    }
    if db.poisoned.is_some() {
        return ffi::SQLITE_IOERR_WRITE;
    }
    if db.committed {
        // The rest of an acknowledged transaction (checksum rewrites, padding): the local WAL.
        let rc = fwd!(file, xWrite, buf, amt, off);
        if rc == OK
            && let Ok(len) = usize::try_from(amt)
        {
            // SAFETY: SQLite passes `amt` readable bytes at `buf`.
            let data = unsafe { std::slice::from_raw_parts(buf.cast::<u8>(), len) };
            db.record_wal_write(off, data);
        }
        return db.post_ack("local WAL write", rc);
    }
    if db.writer == 0 {
        return db.poison(Error::WalWriteOutsideTransaction);
    }
    let Ok(len) = usize::try_from(amt) else {
        return ffi::SQLITE_IOERR_WRITE;
    };
    // SAFETY: SQLite passes `amt` readable bytes at `buf`.
    let data = unsafe { std::slice::from_raw_parts(buf.cast::<u8>(), len) };
    if off == 0
        && let Some(page_size) = be32(data, 8)
        && page_size as usize != PAGE
    {
        return db.poison(Error::PageSize(page_size));
    }
    db.overlay.insert(off, data.to_vec());
    // The commit point: the page data of a frame whose header carries "db size after commit".
    if data.len() == PAGE && page_at(off) {
        let mut h = [0u8; FRAME_HDR_LEN];
        let header = off.saturating_sub(FRAME_HDR);
        // SAFETY: `file` is a WAL file of this VFS.
        let rc = unsafe { overlay_read(file, &mut db, &mut h, header) };
        if rc != OK {
            return rc;
        }
        let size = be32(&h, 4).unwrap_or(0);
        if size != 0 {
            let snapper = db.snapper.clone();
            // Nothing is acknowledged yet (`!committed`), so a wanted window opens now.
            if db.window_wanted {
                snapper.window_cv.notify_all();
            }
            let now = Instant::now();
            let deadline = now.checked_add(WINDOW_WAIT).unwrap_or(now);
            while db.window || db.window_wanted {
                let now = Instant::now();
                if !db.window && now >= deadline {
                    break;
                }
                let wait = if db.window {
                    Duration::from_secs(1)
                } else {
                    deadline.saturating_duration_since(now)
                };
                db = snapper
                    .window_cv
                    .wait_timeout(db, wait)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
            // The snapshot thread may have fenced this owner while it waited.
            if db.poisoned.is_some() {
                return ffi::SQLITE_IOERR_WRITE;
            }
            // SAFETY: `file` is a WAL file of this VFS.
            return unsafe { commit(file, &mut db, size, header) };
        }
    }
    OK
}

/// The transaction's final page set: the last image per pgno over every frame it wrote up to the
/// commit frame (spilled frames included; frames rewritten in place hold their newest image).
///
/// # Safety
///
/// `file` is the open WAL file of `db`.
unsafe fn final_pages(
    file: *mut ffi::sqlite3_file,
    db: &mut Db,
    size: u32,
    commit_frame: i64,
) -> Result<BTreeMap<u32, Vec<u8>>, c_int> {
    let mut pages = BTreeMap::new();
    let frames = db.overlay.keys().copied().filter(|&o| {
        o >= WAL_HDR && o <= commit_frame && o.saturating_sub(WAL_HDR).checked_rem(FRAME) == Some(0)
    });
    for o in frames.collect::<Vec<_>>() {
        let (h, d) = (
            db.overlay.get(&o),
            db.overlay.get(&o.saturating_add(FRAME_HDR)),
        );
        let (pgno, data) = match (h, d) {
            (Some(h), Some(d)) if h.len() == FRAME_HDR_LEN && d.len() == PAGE => {
                (be32(h, 0), d.clone())
            }
            _ => {
                let mut buf = vec![0u8; FRAME_LEN];
                // SAFETY: the caller's contract.
                let rc = unsafe { overlay_read(file, db, &mut buf, o) };
                if rc != OK {
                    return Err(rc);
                }
                let page = buf.get(FRAME_HDR_LEN..).unwrap_or_default().to_vec();
                (be32(&buf, 0), page)
            }
        };
        if let Some(pgno) = pgno
            && pgno >= 1
            && pgno <= size
        {
            pages.insert(pgno, data);
        }
    }
    Ok(pages)
}

/// What a commit whose frame could not be encoded does, with the code its write fails with. One
/// too large for a frame at all is never sent, so it fails alone through `Db::refuse_unsent`, as
/// one over the frame limit does (`Append::Unsent` in `Db::append_commit`). Its size is the
/// payload's bytes, or for more pages than a frame can count their number: either is over 4 GiB.
/// Any other failure poisons the database.
fn unencodable(db: &mut Db, e: FrameError) -> c_int {
    match e {
        FrameError::TooLarge(size) => {
            db.refuse_unsent(WriteRefusal::frame_over_limit(size, max_frame_bytes()))
        }
        other => db.poison(Error::EncodeFrame(other)),
    }
}

/// The commit point: appends the transaction to the stream and, once acknowledged, writes it to
/// the local WAL.
///
/// # Safety
///
/// `file` is the open WAL file of `db`.
unsafe fn commit(file: *mut ffi::sqlite3_file, db: &mut Db, size: u32, commit_frame: i64) -> c_int {
    let started = Instant::now();
    // The transaction starts a new WAL generation (its header is in the overlay): every page
    // checkpointed from the previous one must be on disk before the header can be (`WalClaim`).
    if db.overlay.contains_key(&0) {
        let rc = db.sync_db("a new WAL generation");
        if rc != OK {
            return rc;
        }
    }
    // SAFETY: the caller's contract.
    let pages = match unsafe { final_pages(file, db, size, commit_frame) } {
        Ok(p) => p,
        Err(rc) => {
            db.overlay.clear();
            return rc;
        }
    };
    // A connection leaving WAL (`journal_mode=MEMORY`/`DELETE`) reopens the kept WAL
    // (`x_file_control`) and commits page 1 in rollback format (bytes 18/19 = 1) through it: every
    // copy rebuilt from the stream would then need a rollback journal to be written.
    if pages
        .get(&1)
        .is_some_and(|p| p.get(18..20) != Some(&[2, 2][..]))
    {
        return db.poison(Error::LeavesWal);
    }
    let (body, raw) = match frame::encode_commit(size, &pages) {
        Ok(encoded) => encoded,
        Err(e) => return unencodable(db, e),
    };
    let t = Instant::now();
    let attempts = match db.append_commit(&body) {
        Ok(attempts) => attempts,
        Err(rc) => return rc,
    };
    let append_time = t.elapsed();
    db.log = db.log.saturating_add(body.len() as u64);
    db.pages = size;
    db.acked = db.acked.saturating_add(1);
    db.append_retries = db
        .append_retries
        .saturating_add(u64::from(attempts.saturating_sub(1)));
    if abort_after_ack() == Some(db.acked) {
        log::emit(Level::Error, "test_hook_abort", &[
            ("hook", &"URSULA_VFS_ABORT_AFTER_ACK"),
            ("file", &db.path),
            ("acked", &db.acked),
        ]);
        std::process::abort();
    }
    // The commit frame's number (1-based); past `u32` (never) no publish check can pass.
    db.commit_frame_no = commit_frame
        .saturating_sub(WAL_HDR)
        .checked_div(FRAME)
        .and_then(|n| u32::try_from(n).ok())
        .map_or(u32::MAX, |n| n.saturating_add(1));
    for (o, d) in std::mem::take(&mut db.overlay) {
        let rc = if db.fault() {
            ffi::SQLITE_IOERR_WRITE
        } else if let Ok(amt) = c_int::try_from(d.len()) {
            fwd!(file, xWrite, d.as_ptr().cast::<c_void>(), amt, o)
        } else {
            ffi::SQLITE_IOERR_WRITE
        };
        if rc != OK {
            db.poison(Error::LocalWalWrite(rc));
            return rc;
        }
        db.record_wal_write(o, &d);
    }
    db.committed = true;
    db.stats.commit(CommitStat {
        bytes: body.len(),
        raw,
        pages: pages.len(),
        attempts,
        append: append_time,
        vfs: started.elapsed(),
    });
    OK
}

unsafe fn x_truncate(file: *mut ffi::sqlite3_file, size: ffi::sqlite3_int64) -> c_int {
    // SAFETY: the module contract.
    if let Some(db) = unsafe { main_db(file) } {
        let mut db = lock(&db);
        if !db.db_write_allowed() {
            return db.poison(Error::DbTruncateOutsideCheckpoint);
        }
    }
    // SAFETY: the module contract.
    let Some(db) = (unsafe { wal_db(file) }) else {
        return fwd!(file, xTruncate, size);
    };
    let mut db = lock(&db);
    // Truncated to nothing (only after a complete checkpoint, under the write lock or the
    // closing connection's EXCLUSIVE lock): the db file alone then holds the state at
    // `offset`, so it is fsynced and the sidecar says so before the WAL loses its frames
    // (`WalClaim`). A truncate to a non-zero size (`journal_size_limit`, in the commit that
    // started a new generation) cuts only the previous generation's tail, whose pages
    // `commit` already fsynced.
    if size == 0 {
        // No frames left in the WAL file (`WalSeen`).
        db.wal_seen.truncated();
        let rc = db.sync_db("a WAL truncate");
        if rc != OK {
            return rc;
        }
        let line = sidecar_line(&db.offset, db.epoch, db.log, &db.stamp, WalClaim::NONE);
        if db.poisoned.is_none()
            && let Err(e) = write_sidecar(&db.sidecar, &line)
        {
            return db.poison(e);
        }
    }
    db.overlay.retain(|&o, _| o < size);
    db.wal_written
        .retain(|&o, _| o.saturating_add(PAGE_BYTES) <= size);
    let rc = fwd!(file, xTruncate, size);
    db.post_ack("local WAL truncate", rc)
}

/// A no-op for the db file and WAL of an attached database, whatever `PRAGMA synchronous` says:
/// they are a cache of the stream, rebuilt after a reboot (see the crate docs), so an fsync would
/// only add latency to commits and checkpoints.
unsafe fn x_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    // SAFETY: the module contract.
    if unsafe { ext(file) }.is_some_and(|e| e.db.is_some()) {
        OK
    } else {
        fwd!(file, xSync, flags)
    }
}

unsafe fn x_file_size(file: *mut ffi::sqlite3_file, out: *mut ffi::sqlite3_int64) -> c_int {
    let rc = fwd!(file, xFileSize, out);
    // SAFETY: the module contract.
    let db = unsafe { wal_db(file) };
    if rc == OK
        && let Some(db) = db
    {
        let overlay_end = lock(&db).overlay_end();
        // SAFETY: SQLite passes a valid `sqlite3_int64` at `out`, which the "unix" file just set.
        let size = unsafe { *out };
        // SAFETY: as above.
        unsafe { *out = size.max(overlay_end) };
    }
    rc
}

/// Tracks which main db handle holds an EXCLUSIVE file lock (see `Db::db_write_allowed`).
///
/// # Safety
///
/// `file` is an open file of this VFS.
unsafe fn track_exclusive(file: *mut ffi::sqlite3_file, exclusive: bool) {
    // SAFETY: the caller's contract.
    let Some(ext) = (unsafe { ext(file) }) else {
        return;
    };
    if let (false, Some(db)) = (ext.wal, &ext.db)
        && ext.exclusive.get() != exclusive
    {
        ext.exclusive.set(exclusive);
        lock(db).exclusive = if exclusive {
            file.expose_provenance()
        } else {
            0
        };
    }
}

unsafe fn x_lock(file: *mut ffi::sqlite3_file, l: c_int) -> c_int {
    let rc = fwd!(file, xLock, l);
    if rc == OK && l == ffi::SQLITE_LOCK_EXCLUSIVE {
        // SAFETY: the module contract.
        unsafe { track_exclusive(file, true) };
    }
    rc
}

unsafe fn x_unlock(file: *mut ffi::sqlite3_file, l: c_int) -> c_int {
    let rc = fwd!(file, xUnlock, l);
    if l < ffi::SQLITE_LOCK_EXCLUSIVE {
        // SAFETY: the module contract.
        unsafe { track_exclusive(file, false) };
    }
    rc
}

unsafe fn x_check_reserved_lock(file: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    fwd!(file, xCheckReservedLock, out)
}

/// An attached database's WAL persists past its last connection (checkpointed, not deleted), so
/// the sidecar's claim on it (`WalClaim`) still holds after a clean close. A connection leaving
/// WAL then reopens it and commits the format change through it, which `commit` refuses.
unsafe fn x_file_control(file: *mut ffi::sqlite3_file, op: c_int, arg: *mut c_void) -> c_int {
    // SAFETY: the module contract.
    if op == ffi::SQLITE_FCNTL_PERSIST_WAL && unsafe { main_db(file) }.is_some() {
        // SAFETY: PERSIST_WAL passes a pointer to one `int`, which it writes the setting to.
        unsafe { *arg.cast::<c_int>() = 1 };
        return OK;
    }
    fwd!(file, xFileControl, op, arg)
}

unsafe fn x_sector_size(file: *mut ffi::sqlite3_file) -> c_int {
    fwd!(file, xSectorSize)
}

unsafe fn x_device_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
    fwd!(file, xDeviceCharacteristics)
}

unsafe fn x_shm_map(
    file: *mut ffi::sqlite3_file,
    pg: c_int,
    pgsz: c_int,
    extend: c_int,
    pp: *mut *mut c_void,
) -> c_int {
    let rc = fwd!(file, xShmMap, pg, pgsz, extend, pp);
    if rc == OK {
        return rc;
    }
    // SAFETY: the module contract.
    match unsafe { main_db(file) } {
        Some(db) => lock(&db).post_ack("-shm map", rc),
        None => rc,
    }
}

/// Tracks the write transaction (WAL write lock) and checkpoints (checkpoint lock) of an attached
/// database; SQLite takes both through the main db handle. The end of a write transaction
/// (publication check, sidecar) runs under the database's mutex *before* the real lock is released:
/// once it is released, another thread's connection may start its own write transaction, whose
/// state the bookkeeping would otherwise see (or whose commit the sidecar would cover before its
/// rewrites reach the WAL). Lock order: the real shm lock may be taken without the mutex, and the
/// mutex may be held while releasing it, which never blocks.
unsafe fn x_shm_lock(file: *mut ffi::sqlite3_file, offset: c_int, n: c_int, flags: c_int) -> c_int {
    // Every read transaction on a WAL database takes one of the read-mark locks shared when it
    // starts, even one whose pages its connection has cached. A fenced database refuses it, so no
    // new read transaction serves the state the newer owner moved past: reads fail like writes
    // until the file is attached again (`ursula_status` says `fenced`). A read transaction open
    // already keeps its snapshot until it ends. The flag is read without the database's mutex,
    // which a commit holds across its append (`Db::fence`).
    if flags == ffi::SQLITE_SHM_LOCK | ffi::SQLITE_SHM_SHARED
        && offset >= WAL_READ_LOCK_0
        // SAFETY: the module contract.
        && unsafe { ext(file) }
            .and_then(|e| e.fence.as_deref())
            .is_some_and(|fence| fence.load(Ordering::Acquire))
    {
        return ffi::SQLITE_IOERR_READ;
    }
    let tracked = flags & ffi::SQLITE_SHM_EXCLUSIVE != 0
        && n == 1
        && (offset == WAL_WRITE_LOCK || offset == WAL_CKPT_LOCK);
    let db = if tracked {
        // SAFETY: the module contract.
        unsafe { main_db(file) }
    } else {
        None
    };
    let Some(db) = db else {
        return fwd!(file, xShmLock, offset, n, flags);
    };
    let locking = flags & ffi::SQLITE_SHM_LOCK != 0;
    if !locking {
        // Releases: the bookkeeping and the real unlock happen in one critical section of the
        // db mutex. Released first, another thread's connection could take the lock and start
        // its own write transaction or checkpoint, whose state (`committed`,
        // `checkpoint_started`) this bookkeeping would then clear or misjudge.
        let mut db = lock(&db);
        if offset == WAL_WRITE_LOCK {
            let committed = db.committed;
            // SAFETY: `file` is the main db handle releasing the WAL write lock it holds.
            unsafe { db.end_write_transaction(file) };
            if committed {
                // A frame of this VFS exists now, which a checkpoint writes into the db file.
                db.untouched = None;
            }
            // SAFETY: as above.
            if let Some(h) = unsafe { header(file) } {
                // Without a commit of its own, the lock changed the header only by a recovery,
                // which rebuilt it from the WAL file: a commit outside the extension there shows.
                if !committed {
                    db.check_foreign(Some(h), "WAL write lock release");
                }
                db.wal_seen.released(h, committed);
            }
            return fwd!(file, xShmLock, offset, n, flags);
        }
        if let Some(t) = db.checkpoint_started.take() {
            db.stats.checkpoint(t.elapsed());
        }
        let rc = fwd!(file, xShmLock, offset, n, flags);
        // A snapshot waiting for this checkpoint to finish before it opens its window.
        db.snapper.window_cv.notify_all();
        return rc;
    }
    let rc = fwd!(file, xShmLock, offset, n, flags);
    let mut db = lock(&db);
    match offset {
        WAL_WRITE_LOCK if rc == OK => {
            db.writer = file.expose_provenance();
            db.committed = false;
            db.overlay.clear();
            // SAFETY: `file` is the main db handle that now holds the WAL write lock.
            let h = unsafe { header(file) };
            db.check_foreign(h, "WAL write lock");
        }
        WAL_CKPT_LOCK if rc == OK => db.checkpoint_started = Some(Instant::now()),
        _ => {}
    }
    rc
}

unsafe fn x_shm_barrier(file: *mut ffi::sqlite3_file) {
    // SAFETY: the module contract.
    let (i, methods) = unsafe { unix_file(file) };
    if let Some(barrier) = methods.and_then(|m| m.xShmBarrier) {
        // SAFETY: SQLite's call on this file, forwarded to the "unix" file inside it.
        unsafe { barrier(i) };
    }
}

unsafe fn x_shm_unmap(file: *mut ffi::sqlite3_file, delete: c_int) -> c_int {
    fwd!(file, xShmUnmap, delete)
}

unsafe fn x_fetch(
    file: *mut ffi::sqlite3_file,
    off: ffi::sqlite3_int64,
    amt: c_int,
    pp: *mut *mut c_void,
) -> c_int {
    fwd!(file, xFetch, off, amt, pp)
}

unsafe fn x_unfetch(
    file: *mut ffi::sqlite3_file,
    off: ffi::sqlite3_int64,
    p: *mut c_void,
) -> c_int {
    fwd!(file, xUnfetch, off, p)
}

/// A file method's C entry point: runs the method named without `c_` through [`contain`], so a
/// panic in it answers `$on_panic` instead of unwinding into SQLite (which aborts the host process)
/// and poisons the file's database, whose state it may have left half changed. `xOpen`, `xClose`
/// and `xFetch` have entry points of their own.
macro_rules! io_entry {
    ($entry:ident => $method:ident($file:ident $(, $arg:ident: $ty:ty)*) -> $ret:ty, $on_panic:expr) => {
        unsafe extern "C" fn $entry($file: *mut ffi::sqlite3_file $(, $arg: $ty)*) -> $ret {
            contain(
                stringify!($method),
                || {
                    // SAFETY: `$file` is the open file SQLite called the method on.
                    unsafe { poison_after_panic($file, stringify!($method)) };
                    $on_panic
                },
                || {
                    // SAFETY: SQLite's call of the method, forwarded with its arguments.
                    unsafe { $method($file $(, $arg)*) }
                },
            )
        }
    };
}

/// After a panic in a method of `f`: poisons the database `f` belongs to, if any, since the panic
/// may have left its state half changed. Not for `xClose`: `c_close` takes the file's state and
/// accounts for it before its body runs, so the database's state is whole.
/// Not `Db::poison`: the overlay belongs to the connection that holds the WAL write lock, maybe
/// another than `f`'s, and its reads of pages it spilled must still find them. Its next write
/// fails (`x_write` refuses a poisoned database), which rolls its transaction back.
///
/// # Safety
///
/// `f` is an open file of this VFS.
unsafe fn poison_after_panic(f: *mut ffi::sqlite3_file, entry: &'static str) {
    // SAFETY: the caller's contract.
    if let Some(db) = unsafe { ext(f) }.and_then(|e| e.db.clone()) {
        lock(&db).set_poisoned(Error::Panicked { entry }, None);
    }
}

/// `xOpen`'s C entry point (see `io_entry`): nothing is open yet, so a panic has nothing to poison.
/// The file is set up as not open before the body runs, so whatever the body did before a panic,
/// the fallback finds initialized fields.
pub(crate) unsafe extern "C" fn c_open(
    vfs: *mut ffi::sqlite3_vfs,
    zname: ffi::sqlite3_filename,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    // SAFETY: SQLite passes `szOsFile` writable bytes for the new file, laid out as `File`.
    unsafe { not_open(file) };
    contain(
        "x_open",
        || {
            // SAFETY: `not_open` set `file` up above, and `x_open`, if it started, stopped midway.
            unsafe { failed_open(file) }
        },
        || {
            // SAFETY: SQLite's call of `xOpen`, forwarded with its arguments.
            unsafe { x_open(vfs, zname, file, flags, out) }
        },
    )
}

/// What an open that panicked leaves: no methods, since SQLite closes a file only when it has them.
/// So the "unix" file, if its own open succeeded before the panic, is closed here, and the state
/// `x_open` made for the file freed. Its open count was released as the panic unwound through
/// `x_open` (`Counted`).
///
/// # Safety
///
/// `file` is SQLite's allocation for a new file of this VFS, which `not_open` set up before `x_open`
/// ran on it: its `ext` is null or the `Box` `x_open` made, and its "unix" file has methods only if
/// that file's own open succeeded.
unsafe fn failed_open(file: *mut ffi::sqlite3_file) -> c_int {
    let _closed = fwd!(file, xClose);
    // SAFETY: the caller's contract, which `take_ext` asks for.
    drop(unsafe { take_ext(file) });
    // SAFETY: as above.
    unsafe { (*file).pMethods = null() };
    ffi::SQLITE_CANTOPEN
}

/// `xClose`'s C entry point (see `io_entry`). The file's state is taken and accounted for
/// (`closing`) before the body runs, and its open count released when this returns, however the
/// body ended: a file that stays counted could never be attached again in this process. So a panic
/// poisons nothing (the database's state is whole), and the fallback closes the "unix" file, which
/// is all the body does.
unsafe extern "C" fn c_close(file: *mut ffi::sqlite3_file) -> c_int {
    // SAFETY: SQLite's call of `xClose` on an open file of this VFS.
    let ext = unsafe { take_ext(file) };
    let _counted = Counted(ext.as_ref().and_then(|e| e.path.clone()));
    if let Some(e) = &ext {
        closing(e);
    }
    contain(
        "x_close",
        || {
            let _closed = fwd!(file, xClose);
            ffi::SQLITE_IOERR_CLOSE
        },
        || {
            // SAFETY: as above, with the state taken.
            unsafe { x_close(file) }
        },
    )
}
io_entry!(c_read => x_read(file, buf: *mut c_void, amt: c_int, off: ffi::sqlite3_int64) -> c_int, ffi::SQLITE_IOERR_READ);
io_entry!(c_write => x_write(file, buf: *const c_void, amt: c_int, off: ffi::sqlite3_int64) -> c_int, ffi::SQLITE_IOERR_WRITE);
io_entry!(c_truncate => x_truncate(file, size: ffi::sqlite3_int64) -> c_int, ffi::SQLITE_IOERR_TRUNCATE);
io_entry!(c_sync => x_sync(file, flags: c_int) -> c_int, ffi::SQLITE_IOERR_FSYNC);
io_entry!(c_file_size => x_file_size(file, out: *mut ffi::sqlite3_int64) -> c_int, ffi::SQLITE_IOERR_FSTAT);
io_entry!(c_lock => x_lock(file, l: c_int) -> c_int, ffi::SQLITE_IOERR_LOCK);
io_entry!(c_unlock => x_unlock(file, l: c_int) -> c_int, ffi::SQLITE_IOERR_UNLOCK);
io_entry!(c_check_reserved_lock => x_check_reserved_lock(file, out: *mut c_int) -> c_int, ffi::SQLITE_IOERR_CHECKRESERVEDLOCK);
io_entry!(c_file_control => x_file_control(file, op: c_int, arg: *mut c_void) -> c_int, ffi::SQLITE_IOERR);
// SQLite's default sector size and no device characteristics: neither method can report an error.
io_entry!(c_sector_size => x_sector_size(file) -> c_int, 4096);
io_entry!(c_device_characteristics => x_device_characteristics(file) -> c_int, 0);
io_entry!(c_shm_map => x_shm_map(file, pg: c_int, pgsz: c_int, extend: c_int, pp: *mut *mut c_void) -> c_int, ffi::SQLITE_IOERR_SHMMAP);
io_entry!(c_shm_lock => x_shm_lock(file, offset: c_int, n: c_int, flags: c_int) -> c_int, ffi::SQLITE_IOERR_SHMLOCK);
io_entry!(c_shm_barrier => x_shm_barrier(file) -> (), ());
io_entry!(c_shm_unmap => x_shm_unmap(file, delete: c_int) -> c_int, ffi::SQLITE_IOERR);
io_entry!(c_unfetch => x_unfetch(file, off: ffi::sqlite3_int64, p: *mut c_void) -> c_int, ffi::SQLITE_IOERR);

/// `xFetch`'s C entry point (see `io_entry`): after a panic it also leaves no mapping in `*pp`.
unsafe extern "C" fn c_fetch(
    file: *mut ffi::sqlite3_file,
    off: ffi::sqlite3_int64,
    amt: c_int,
    pp: *mut *mut c_void,
) -> c_int {
    contain(
        "x_fetch",
        || {
            // SAFETY: `file` is the open file SQLite called the method on.
            unsafe { poison_after_panic(file, "x_fetch") };
            if !pp.is_null() {
                // SAFETY: SQLite passes `pp` writable for the mapping.
                unsafe { *pp = null_mut() };
            }
            ffi::SQLITE_IOERR
        },
        || {
            // SAFETY: SQLite's call of `xFetch`, forwarded with its arguments.
            unsafe { x_fetch(file, off, amt, pp) }
        },
    )
}

static METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 3,
    xClose: Some(c_close),
    xRead: Some(c_read),
    xWrite: Some(c_write),
    xTruncate: Some(c_truncate),
    xSync: Some(c_sync),
    xFileSize: Some(c_file_size),
    xLock: Some(c_lock),
    xUnlock: Some(c_unlock),
    xCheckReservedLock: Some(c_check_reserved_lock),
    xFileControl: Some(c_file_control),
    xSectorSize: Some(c_sector_size),
    xDeviceCharacteristics: Some(c_device_characteristics),
    xShmMap: Some(c_shm_map),
    xShmLock: Some(c_shm_lock),
    xShmBarrier: Some(c_shm_barrier),
    xShmUnmap: Some(c_shm_unmap),
    xFetch: Some(c_fetch),
    xUnfetch: Some(c_unfetch),
};

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::ffi::c_int;
    use std::ffi::c_void;
    use std::ptr::NonNull;
    use std::ptr::null;
    use std::ptr::null_mut;
    use std::sync::Arc;
    use std::sync::Mutex;

    use libsqlite3_sys as ffi;

    use super::Ext;
    use super::File;
    use super::METHODS;
    use super::c_open;
    use super::failed_open;
    use super::lost_page;
    use super::unencodable;
    use crate::client::mock::LAST;
    use crate::client::mock::VOUCH;
    use crate::client::mock::answer;
    use crate::client::mock::answer_with_body;
    use crate::client::mock::marker;
    use crate::client::mock::request_header;
    use crate::client::mock::serve;
    use crate::client::mock::vouched;
    use crate::config::max_frame_bytes;
    use crate::config::set_panic_in_for_tests;
    use crate::db::Db;
    use crate::db::lock;
    use crate::db::registry;
    use crate::error::Error;
    use crate::error::FrameLimit;
    use crate::error::WriteRefusal;
    use crate::frame::FrameError;
    use crate::frame::PAGE;
    use crate::wal::FIRST_PAGE;
    use crate::wal::FRAME;

    /// The server's answer under pressure: refused at ingress, marked as applying nothing, through
    /// a gateway that vouches for the marker.
    fn pressure(token: &str, last: &str) -> String {
        unavailable(&vouched(token), last)
    }

    /// A 503 with header lines `headers`, then `last`.
    fn unavailable(headers: &str, last: &str) -> String {
        answer(&format!("503 Service Unavailable\r\n{headers}\r\n{last}"))
    }

    /// An owner at sequence 4 and offset 9 of the stream `url` serves.
    fn owner(url: String) -> Db {
        let mut d = Db::for_tests();
        d.url = url;
        d.seq = 4;
        d.offset = "00000000000000000009".into();
        d
    }

    /// The owner's epoch, producer sequence and offset.
    fn at(d: &Db) -> (u64, u64, &str) {
        (d.epoch, d.seq, d.offset.as_str())
    }

    // A commit every attempt of which was refused before it reached the stream fails alone with
    // SQLITE_IOERR_AUTH (`Db::refuse`): the owner is not poisoned and keeps its epoch, sequence and
    // offset, so the next commit goes out with the same producer sequence and is acknowledged once
    // the refusals stop. A 401, 402 or 429 proves it by its status, any other answer by a marker
    // with a gateway's vouch. One attempt whose outcome is unknown among the refusals poisons
    // instead: the append may be in the stream, and the next commit would be answered as its
    // duplicate. So does a 403 that does not say it applied nothing (a filtering proxy may have
    // sent it after the stream applied the append).
    #[test]
    fn a_commit_refused_before_the_stream_fails_alone_but_an_unknown_outcome_poisons() {
        let (url, server) = serve(vec![
            // The server under pressure until the budget runs out.
            pressure("RaftLogPressure", "retry-after: 0"),
            pressure("RaftLogPressure", LAST),
            // The token refused until the budget runs out.
            answer(&format!("401 Unauthorized\r\n{LAST}")),
            // An authorizer's denial, marked: at once.
            answer(&format!("403 Forbidden\r\n{}", vouched("Forbidden"))),
            // A quota in front of the stream: at once.
            answer_with_body("402 Payment Required", "quota exhausted"),
            // Rate limited until the budget runs out.
            answer(&format!("429 Too Many Requests\r\n{LAST}")),
            // A commit over a layer's request limit, marked and vouched for: at once, as too large.
            answer(&format!(
                "413 Payload Too Large\r\n{}",
                vouched("RequestBodyTooLarge")
            )),
            // Accepted again.
            answer("200 OK\r\nstream-next-offset: 00000000000000000012"),
            // No answer, then refusals until the budget runs out.
            String::new(),
            pressure("RaftLogPressure", LAST),
            // Another owner: a denial without the marker.
            answer("403 Forbidden"),
        ]);
        let mut d = owner(url.clone());
        assert_eq!(d.append_commit(b"frame"), Err(ffi::SQLITE_IOERR_AUTH));
        assert!(d.poisoned.is_none());
        assert_eq!(
            d.refused,
            Some(WriteRefusal::NotApplied {
                status: 503,
                reason: "RaftLogPressure".into()
            })
        );
        assert_eq!(at(&d), (2, 4, "00000000000000000009"));
        assert_eq!(d.append_commit(b"frame"), Err(ffi::SQLITE_IOERR_AUTH));
        assert!(matches!(d.refused, Some(WriteRefusal::Unauthorized { .. })));
        assert_eq!(d.append_commit(b"frame"), Err(ffi::SQLITE_IOERR_AUTH));
        assert!(matches!(d.refused, Some(WriteRefusal::Forbidden { .. })));
        assert_eq!(d.append_commit(b"frame"), Err(ffi::SQLITE_IOERR_AUTH));
        assert!(matches!(
            d.refused,
            Some(WriteRefusal::PaymentRequired { .. })
        ));
        assert_eq!(d.append_commit(b"frame"), Err(ffi::SQLITE_IOERR_AUTH));
        assert!(matches!(d.refused, Some(WriteRefusal::RateLimited { .. })));
        assert_eq!(d.append_commit(b"frame"), Err(ffi::SQLITE_IOERR_AUTH));
        assert_eq!(
            d.refused,
            Some(WriteRefusal::TooLarge {
                bytes: 5,
                over: FrameLimit::InFront {
                    token: "RequestBodyTooLarge".into()
                }
            })
        );
        assert!(d.poisoned.is_none());
        assert_eq!(at(&d), (2, 4, "00000000000000000009"));
        assert_eq!(d.append_commit(b"frame"), Ok(1));
        assert_eq!(at(&d), (2, 5, "00000000000000000012"));
        assert!(d.refused.is_none());
        assert_eq!(d.append_commit(b"frame"), Err(ffi::SQLITE_IOERR_WRITE));
        assert!(matches!(
            d.poisoned.as_ref().map(|p| &p.first),
            Some(Error::AppendUnknown { attempts: 2, .. })
        ));
        assert!(!d.fenced() && d.refused.is_none());
        let mut e = owner(url);
        assert_eq!(e.append_commit(b"frame"), Err(ffi::SQLITE_IOERR_WRITE));
        assert!(matches!(
            e.poisoned.as_ref().map(|p| &p.first),
            Some(Error::Unauthorized { status: 403, .. })
        ));
        assert!(!e.fenced() && e.refused.is_none());
        let requests = server.join().unwrap();
        let seqs: Vec<&str> = requests
            .iter()
            .map(|r| request_header(r, "producer-seq").unwrap())
            .collect();
        assert_eq!(seqs, [
            "5", "5", "5", "5", "5", "5", "5", "5", "6", "6", "5"
        ]);
    }

    // A marker counts only with a gateway's vouch (`client::applied_nothing`): a node sends it
    // alone, and a gateway before 0.7.3 relays it alone on an answer it got by following a
    // redirect that a node can send after it proposed the commit. The vouch alone proves nothing
    // either. So a commit refused that way until the budget runs out poisons as an unknown
    // outcome, a 403 or a 413 so marked poisons as a failure, and one attempt marked alone among
    // vouched refusals poisons too. Each time the owner keeps no refusal.
    #[test]
    fn a_marker_without_the_vouch_proves_nothing_and_the_commit_poisons() {
        let (url, server) = serve(vec![
            // The marker alone until the budget runs out.
            unavailable(&marker("RaftLogPressure"), "retry-after: 0"),
            unavailable(&marker("RaftLogPressure"), LAST),
            // The vouch alone until the budget runs out.
            unavailable(VOUCH, LAST),
            // A denial and a commit over a request limit, marked alone.
            answer(&format!("403 Forbidden\r\n{}", marker("Forbidden"))),
            answer(&format!(
                "413 Payload Too Large\r\n{}",
                marker("RequestBodyTooLarge")
            )),
            // Vouched refusals around one marked alone.
            pressure("RaftLogPressure", "retry-after: 0"),
            unavailable(&marker("RaftLogPressure"), "retry-after: 0"),
            pressure("RaftLogPressure", LAST),
        ]);
        let poison = |url: &str| {
            let mut d = owner(url.to_owned());
            assert_eq!(d.append_commit(b"frame"), Err(ffi::SQLITE_IOERR_WRITE));
            assert!(!d.fenced() && d.refused.is_none());
            assert_eq!(at(&d), (2, 4, "00000000000000000009"));
            d.poisoned.map(|p| p.first)
        };
        assert!(matches!(
            poison(&url),
            Some(Error::AppendUnknown { attempts: 2, .. })
        ));
        assert!(matches!(
            poison(&url),
            Some(Error::AppendUnknown { attempts: 1, .. })
        ));
        assert!(matches!(
            poison(&url),
            Some(Error::Unauthorized { status: 403, .. })
        ));
        assert!(matches!(
            poison(&url),
            Some(Error::AppendRejected { status: 413, .. })
        ));
        assert!(matches!(
            poison(&url),
            Some(Error::AppendUnknown { attempts: 3, .. })
        ));
        assert_eq!(server.join().unwrap().len(), 8);
    }

    // A commit frame too large to encode at all is refused unsent, as one over the frame limit is:
    // it leaves a layer's refusal and the sequence check as they were. Any other failure to encode
    // one poisons.
    #[test]
    fn an_unencodable_frame_too_large_is_refused_and_any_other_poisons() {
        let mut d = owner("http://h/b/s".into());
        // A layer refused a snapshot publish: a layer's refusal without the sequence check.
        let payment = WriteRefusal::PaymentRequired {
            body: "quota".into(),
        };
        d.writes_refused(payment.clone(), "snapshot publish");
        assert_eq!(
            unencodable(&mut d, FrameError::TooLarge(5 << 30)),
            ffi::SQLITE_IOERR_AUTH
        );
        assert_eq!(
            d.refused,
            Some(WriteRefusal::frame_over_limit(5 << 30, max_frame_bytes()))
        );
        assert_eq!(d.layer_refused, Some(payment));
        assert!(d.refused_at.is_none() && d.poisoned.is_none());
        assert_eq!(
            unencodable(&mut d, FrameError::UnknownRecord),
            ffi::SQLITE_IOERR_WRITE
        );
        assert!(matches!(
            d.poisoned.as_ref().map(|p| &p.first),
            Some(Error::EncodeFrame(FrameError::UnknownRecord))
        ));
    }

    // A transaction the extension refused itself was never sent, so the stream cannot hold it: a
    // duplicate's answer to the next commit is that commit's own, applied by an exchange whose
    // answer said otherwise, and is acknowledged. A commit that a layer refused before it still
    // poisons the commit after them, which goes out with the sequence the refused one left free.
    #[test]
    fn a_transaction_refused_unsent_leaves_the_duplicate_check_as_it_was() {
        let duplicate = || answer("204 No Content\r\nstream-next-offset: 00000000000000000012");
        let (url, server) = serve(vec![
            duplicate(),
            pressure("RaftLogPressure", LAST),
            duplicate(),
        ]);
        let over = vec![0_u8; max_frame_bytes() + 1];

        let mut alone = owner(url.clone());
        assert_eq!(alone.append_commit(&over), Err(ffi::SQLITE_IOERR_AUTH));
        assert!(matches!(alone.refused, Some(WriteRefusal::TooLarge { .. })));
        assert!(alone.refused_at.is_none() && alone.poisoned.is_none());
        assert_eq!(alone.append_commit(b"frame"), Ok(1));
        assert_eq!(at(&alone), (2, 5, "00000000000000000012"));
        assert!(alone.refused.is_none());

        let mut after = owner(url);
        assert_eq!(after.append_commit(b"frame"), Err(ffi::SQLITE_IOERR_AUTH));
        assert_eq!(after.append_commit(&over), Err(ffi::SQLITE_IOERR_AUTH));
        assert_eq!(after.refused_at, Some((2, 5)));
        assert_eq!(after.append_commit(b"frame"), Err(ffi::SQLITE_IOERR_WRITE));
        assert!(matches!(
            after.poisoned.as_ref().map(|p| &p.first),
            Some(Error::RefusedCommitApplied {
                epoch: 2,
                seq: 5,
                ..
            })
        ));
        assert_eq!(server.join().unwrap().len(), 3);
    }

    // A layer that refused a commit after the stream applied it breaks the rule of design doc §2:
    // the next commit, sent with the same sequence, is answered as the refused commit's duplicate.
    // Acknowledged, the stream would hold the refused transaction and the local file the next one,
    // with no error: it poisons instead, also when the refused commit was the first after the
    // claim. A fresh append's 200 after the refusal is acknowledged, and so is a duplicate's answer
    // at a sequence no refused commit used (this commit, applied by an exchange that a gateway made
    // and answered for itself): one refused at an older epoch, or a refused snapshot publish,
    // leaves no sequence. After an attempt whose outcome is unknown a duplicate's answer is taken
    // for that attempt's, which hides a refused commit applied after all (the gap design doc §2
    // names).
    #[test]
    fn the_commit_after_a_refused_one_that_was_applied_poisons() {
        let duplicate = || answer("204 No Content\r\nstream-next-offset: 00000000000000000012");
        let (url, server) = serve(vec![
            pressure("RaftLogPressure", LAST),
            duplicate(),
            pressure("RaftLogPressure", LAST),
            answer("200 OK\r\nstream-next-offset: 00000000000000000012"),
            duplicate(),
            pressure("RaftLogPressure", LAST),
            String::new(),
            duplicate(),
            duplicate(),
            duplicate(),
            pressure("RaftLogPressure", LAST),
            duplicate(),
        ]);
        let mut d = owner(url.clone());
        assert_eq!(d.append_commit(b"frame"), Err(ffi::SQLITE_IOERR_AUTH));
        assert_eq!(d.refused_at, Some((2, 5)));
        assert_eq!(d.append_commit(b"frame"), Err(ffi::SQLITE_IOERR_WRITE));
        assert!(matches!(
            d.poisoned.as_ref().map(|p| &p.first),
            Some(Error::RefusedCommitApplied { epoch: 2, seq: 5, offset, receipt })
                if offset == "00000000000000000009" && receipt == "00000000000000000012"
        ));
        assert!(!d.fenced() && !d.damaged() && d.refused.is_none());
        assert_eq!(at(&d), (2, 4, "00000000000000000009"));
        let mut accepted = owner(url.clone());
        assert_eq!(
            accepted.append_commit(b"frame"),
            Err(ffi::SQLITE_IOERR_AUTH)
        );
        assert_eq!(accepted.append_commit(b"frame"), Ok(1));
        assert_eq!(at(&accepted), (2, 5, "00000000000000000012"));
        assert!(accepted.refused.is_none() && accepted.refused_at.is_none());
        let mut unrefused = owner(url.clone());
        assert_eq!(unrefused.append_commit(b"frame"), Ok(1));
        assert_eq!(at(&unrefused), (2, 5, "00000000000000000012"));
        let mut lost = owner(url.clone());
        assert_eq!(lost.append_commit(b"frame"), Err(ffi::SQLITE_IOERR_AUTH));
        assert_eq!(lost.append_commit(b"frame"), Ok(2));
        assert!(lost.poisoned.is_none());
        // A commit refused at epoch 1, before a re-claim moved the owner to epoch 2, left no
        // sequence at this epoch.
        let mut reclaimed = owner(url.clone());
        reclaimed.refused_at = Some((1, 5));
        assert_eq!(reclaimed.append_commit(b"frame"), Ok(1));
        assert!(reclaimed.poisoned.is_none() && reclaimed.refused_at.is_none());
        // A refused snapshot publish records a refusal but no commit's sequence.
        let mut snapshot = owner(url.clone());
        snapshot.writes_refused(
            WriteRefusal::PaymentRequired {
                body: "quota".into(),
            },
            "snapshot publish",
        );
        assert!(snapshot.refused.is_some() && snapshot.refused_at.is_none());
        assert_eq!(snapshot.append_commit(b"frame"), Ok(1));
        assert!(snapshot.poisoned.is_none() && snapshot.refused.is_none());
        // The first commit after the claim (sequence 1), refused, then answered for at the next.
        let mut first = owner(url);
        first.seq = 0;
        assert_eq!(first.append_commit(b"frame"), Err(ffi::SQLITE_IOERR_AUTH));
        assert_eq!(first.refused_at, Some((2, 1)));
        assert_eq!(first.append_commit(b"frame"), Err(ffi::SQLITE_IOERR_WRITE));
        assert!(matches!(
            first.poisoned.as_ref().map(|p| &p.first),
            Some(Error::RefusedCommitApplied {
                epoch: 2,
                seq: 1,
                ..
            })
        ));
        let requests = server.join().unwrap();
        let seqs: Vec<&str> = requests
            .iter()
            .map(|r| request_header(r, "producer-seq").unwrap())
            .collect();
        assert_eq!(seqs, [
            "5", "5", "5", "5", "5", "5", "5", "5", "5", "5", "1", "1"
        ]);
    }

    // The server expired the owner's producer: the owner takes the stream back before the commit
    // (a read at its offset, then a claim one epoch up). A re-claim refused before it reached the
    // stream claimed nothing, so the commit fails alone too, and the owner keeps its epoch.
    #[test]
    fn a_re_claim_refused_before_the_stream_fails_the_commit_alone() {
        let (url, server) = serve(vec![
            answer("409 Conflict\r\nproducer-expected-seq: 0"),
            answer("200 OK\r\nstream-next-offset: 00000000000000000009"),
            pressure("WalDiskPressure", LAST),
        ]);
        let mut d = owner(url);
        assert_eq!(d.append_commit(b"frame"), Err(ffi::SQLITE_IOERR_AUTH));
        assert!(d.poisoned.is_none());
        assert_eq!(
            d.refused,
            Some(WriteRefusal::NotApplied {
                status: 503,
                reason: "WalDiskPressure".into()
            })
        );
        assert_eq!(at(&d), (2, 4, "00000000000000000009"));
        let requests = server.join().unwrap();
        let sent: Vec<(&str, Option<&str>, Option<&str>)> = requests
            .iter()
            .map(|r| {
                (
                    r.split(' ').next().unwrap(),
                    request_header(r, "producer-epoch"),
                    request_header(r, "producer-seq"),
                )
            })
            .collect();
        assert_eq!(sent, [
            ("POST", Some("2"), Some("5")),
            ("GET", None, None),
            ("POST", Some("3"), Some("0"))
        ]);
    }

    // A page read back is checked against what this process wrote at its offset, wherever it lies
    // in the read (one page, or a whole frame with its header), unless the transaction in progress
    // rewrote it (its overlay wins). Pages never written here are not checked.
    #[test]
    fn pages_read_back_are_checked_against_what_was_written() {
        let (first, second) = (FIRST_PAGE, FIRST_PAGE + FRAME);
        let written = BTreeMap::from([
            (first, crc32c::crc32c(&[1u8; PAGE])),
            (second, crc32c::crc32c(&[2u8; PAGE])),
        ]);
        let none = BTreeMap::new();
        assert_eq!(lost_page(&written, &none, &[1u8; PAGE], first), None);
        assert_eq!(lost_page(&written, &none, &[0u8; PAGE], first), Some(first));
        // A frame read whole, header first: the page is checked at its offset.
        let mut frame = vec![9u8; 24];
        frame.extend([0u8; PAGE]);
        assert_eq!(
            lost_page(&written, &none, &frame, second - 24),
            Some(second)
        );
        frame.truncate(24);
        frame.extend([2u8; PAGE]);
        assert_eq!(lost_page(&written, &none, &frame, second - 24), None);
        // Rewritten by the transaction in progress, or never written here: not checked.
        let overlay = BTreeMap::from([(first, vec![7u8; PAGE])]);
        assert_eq!(lost_page(&written, &overlay, &[0u8; PAGE], first), None);
        assert_eq!(
            lost_page(&written, &none, &[0u8; PAGE], second + FRAME),
            None
        );
        // A read that covers part of a page does not check it.
        assert_eq!(lost_page(&written, &none, &[0u8; 100], first), None);
    }

    // Every file method's C entry point contains a panic (one on purpose here: the entry point
    // panics before its body touches the file). It answers the method's own error code (the sector
    // size and device characteristics, which cannot report one, answer 4096 and 0) and poisons the
    // file's database with the method's name, except `xClose`. That one takes the file's state and
    // accounts for it before its body: the WAL handle is uncounted, the state freed, and nothing
    // poisoned. The calls go through the methods table SQLite gets, so each method is wired to its
    // entry point. `xOpen`, on a file of its own as SQLite allocates one, answers SQLITE_CANTOPEN
    // and leaves the file without methods.
    #[test]
    fn every_file_entry_point_contains_a_panic() {
        let db = Arc::new(Mutex::new(Db::for_tests()));
        let mut file = wal_file_of(&db);
        let f = (&raw mut file).cast::<ffi::sqlite3_file>();
        let mut buf = [0u8; 16];
        let mut size: ffi::sqlite3_int64 = 0;
        let mut flag: c_int = 0;
        let mut region: *mut c_void = null_mut();
        let m = &METHODS;
        macro_rules! contained {
            ($entry:literal, $call:expr, $code:expr) => {{
                set_panic_in_for_tests(Some($entry));
                // SAFETY: the entry point panics before its body touches `f`, whose own state
                // (`ext`) is valid.
                let answer = unsafe { $call };
                assert_eq!(answer, $code, $entry);
                let mut d = lock(&db);
                let poisoned = d.poisoned.take();
                assert!(
                    matches!(&poisoned, Some(p) if matches!(p.first, Error::Panicked { entry } if entry == $entry)),
                    $entry
                );
            }};
        }
        contained!(
            "x_read",
            m.xRead.unwrap()(f, buf.as_mut_ptr().cast(), 16, 0),
            ffi::SQLITE_IOERR_READ
        );
        contained!(
            "x_write",
            m.xWrite.unwrap()(f, buf.as_ptr().cast(), 16, 0),
            ffi::SQLITE_IOERR_WRITE
        );
        contained!(
            "x_truncate",
            m.xTruncate.unwrap()(f, 0),
            ffi::SQLITE_IOERR_TRUNCATE
        );
        contained!("x_sync", m.xSync.unwrap()(f, 0), ffi::SQLITE_IOERR_FSYNC);
        contained!(
            "x_file_size",
            m.xFileSize.unwrap()(f, &raw mut size),
            ffi::SQLITE_IOERR_FSTAT
        );
        contained!("x_lock", m.xLock.unwrap()(f, 1), ffi::SQLITE_IOERR_LOCK);
        contained!(
            "x_unlock",
            m.xUnlock.unwrap()(f, 0),
            ffi::SQLITE_IOERR_UNLOCK
        );
        contained!(
            "x_check_reserved_lock",
            m.xCheckReservedLock.unwrap()(f, &raw mut flag),
            ffi::SQLITE_IOERR_CHECKRESERVEDLOCK
        );
        contained!(
            "x_file_control",
            m.xFileControl.unwrap()(f, 0, null_mut()),
            ffi::SQLITE_IOERR
        );
        contained!("x_sector_size", m.xSectorSize.unwrap()(f), 4096);
        contained!(
            "x_device_characteristics",
            m.xDeviceCharacteristics.unwrap()(f),
            0
        );
        contained!(
            "x_shm_map",
            m.xShmMap.unwrap()(f, 0, 32768, 0, &raw mut region),
            ffi::SQLITE_IOERR_SHMMAP
        );
        contained!(
            "x_shm_lock",
            m.xShmLock.unwrap()(f, 0, 1, 0),
            ffi::SQLITE_IOERR_SHMLOCK
        );
        contained!("x_shm_barrier", m.xShmBarrier.unwrap()(f), ());
        contained!("x_shm_unmap", m.xShmUnmap.unwrap()(f, 0), ffi::SQLITE_IOERR);
        region = NonNull::dangling().as_ptr();
        contained!(
            "x_fetch",
            m.xFetch.unwrap()(f, 0, 16, &raw mut region),
            ffi::SQLITE_IOERR
        );
        assert!(region.is_null(), "a failed xFetch leaves no mapping");
        contained!(
            "x_unfetch",
            m.xUnfetch.unwrap()(f, 0, null_mut()),
            ffi::SQLITE_IOERR
        );
        // The WAL handle is counted open, as `x_open` counts it.
        lock(&db).wal_open = 1;
        set_panic_in_for_tests(Some("x_close"));
        // SAFETY: as in `contained`; `c_close` takes and frees the state `wal_file_of` made, and
        // the "unix" file has no methods to close.
        let closed = unsafe { m.xClose.unwrap()(f) };
        set_panic_in_for_tests(None);
        assert_eq!(closed, ffi::SQLITE_IOERR_CLOSE);
        let d = lock(&db);
        assert!(
            d.poisoned.is_none(),
            "xClose poisons nothing: its bookkeeping is done before the body"
        );
        assert_eq!(d.wal_open, 0, "the WAL handle is uncounted");
        drop(d);
        assert!(file.ext.is_null());
        assert_eq!(Arc::strong_count(&db), 1, "the file's state is freed");

        // SQLite's allocation for a new file holds whatever was there before: stale methods here,
        // which `c_open` clears (`not_open`) before anything can panic. So the fallback leaves the
        // file without methods and closes no "unix" file, which never opened.
        let mut fresh = File {
            base: ffi::sqlite3_file {
                pMethods: &raw const METHODS,
            },
            ext: null_mut(),
            inner: ffi::sqlite3_file {
                pMethods: &raw const COUNTING,
            },
        };
        UNIX_CLOSES.set(0);
        set_panic_in_for_tests(Some("x_open"));
        let mut out: c_int = 0;
        // SAFETY: a `File` as SQLite allocates one; `c_open` sets it up as not open, then panics
        // before its body reads any argument.
        let opened =
            unsafe { c_open(null_mut(), null(), (&raw mut fresh).cast(), 0, &raw mut out) };
        set_panic_in_for_tests(None);
        assert_eq!(opened, ffi::SQLITE_CANTOPEN);
        assert!(
            fresh.base.pMethods.is_null(),
            "a failed xOpen leaves no methods"
        );
        assert_eq!(
            UNIX_CLOSES.get(),
            0,
            "no \"unix\" file opened, so none is closed"
        );
    }

    // A panic in `xClose` of a main db handle still releases the file's open count, and the
    // EXCLUSIVE lock the handle held: a file that stays counted could never be attached again in
    // this process, and a database whose closed handle still held the lock would take another
    // handle's main-db writes for its own.
    #[test]
    fn a_panic_in_close_releases_the_open_count_and_the_exclusive_lock() {
        let path = format!("/a-panic-in-close-{}", std::process::id());
        registry().open.insert(path.clone(), 1);
        let db = Arc::new(Mutex::new(Db::for_tests()));
        let mut file = File {
            base: ffi::sqlite3_file {
                pMethods: &raw const METHODS,
            },
            ext: Box::into_raw(Box::new(Ext {
                path: Some(path.clone()),
                db: Some(db.clone()),
                fence: None,
                wal: false,
                exclusive: Cell::new(true),
            })),
            inner: ffi::sqlite3_file { pMethods: null() },
        };
        lock(&db).exclusive = (&raw const file).expose_provenance();
        set_panic_in_for_tests(Some("x_close"));
        // SAFETY: `c_close` takes and frees the state above, and panics before its body.
        let closed = unsafe { METHODS.xClose.unwrap()((&raw mut file).cast()) };
        set_panic_in_for_tests(None);
        assert_eq!(closed, ffi::SQLITE_IOERR_CLOSE);
        assert!(!registry().open.contains_key(&path));
        assert_eq!(lock(&db).exclusive, 0, "the EXCLUSIVE lock is released");
        assert!(lock(&db).poisoned.is_none());
        assert!(file.ext.is_null());
        assert_eq!(Arc::strong_count(&db), 1, "the file's state is freed");
    }

    thread_local! {
        /// The closes of a "unix" file with `COUNTING` methods on this thread.
        static UNIX_CLOSES: Cell<usize> = const { Cell::new(0) };
    }

    /// `xClose` of a stand-in "unix" file: counts the close.
    extern "C" fn counting_close(_file: *mut ffi::sqlite3_file) -> c_int {
        UNIX_CLOSES.set(UNIX_CLOSES.get().saturating_add(1));
        ffi::SQLITE_OK
    }

    /// The methods of a stand-in "unix" file whose own open succeeded: `xClose` alone.
    static COUNTING: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
        iVersion: 1,
        xClose: Some(counting_close),
        xRead: None,
        xWrite: None,
        xTruncate: None,
        xSync: None,
        xFileSize: None,
        xLock: None,
        xUnlock: None,
        xCheckReservedLock: None,
        xFileControl: None,
        xSectorSize: None,
        xDeviceCharacteristics: None,
        xShmMap: None,
        xShmLock: None,
        xShmBarrier: None,
        xShmUnmap: None,
        xFetch: None,
        xUnfetch: None,
    };

    // A file's "unix" file is closed when the file closes, and also when the close panics: the
    // fallback forwards the close the body would have made.
    #[test]
    fn a_close_closes_the_unix_file_even_after_a_panic() {
        UNIX_CLOSES.set(0);
        // An open file of this VFS without state, over a stand-in "unix" file. Each is closed once.
        let over_counting = || File {
            base: ffi::sqlite3_file {
                pMethods: &raw const METHODS,
            },
            ext: null_mut(),
            inner: ffi::sqlite3_file {
                pMethods: &raw const COUNTING,
            },
        };
        let mut closed_normally = over_counting();
        // SAFETY: an open file of this VFS, closed once.
        let closed = unsafe { METHODS.xClose.unwrap()((&raw mut closed_normally).cast()) };
        assert_eq!((closed, UNIX_CLOSES.get()), (ffi::SQLITE_OK, 1));
        let mut panicking = over_counting();
        set_panic_in_for_tests(Some("x_close"));
        // SAFETY: another open file of this VFS, closed once; `c_close` panics before its body.
        let closed = unsafe { METHODS.xClose.unwrap()((&raw mut panicking).cast()) };
        set_panic_in_for_tests(None);
        assert_eq!(
            (closed, UNIX_CLOSES.get()),
            (ffi::SQLITE_IOERR_CLOSE, 2),
            "each of the two files closed its \"unix\" file"
        );
    }

    // An open that panicked closes the "unix" file whose own open succeeded, frees the state
    // `x_open` made, and leaves no methods, so SQLite never closes the file.
    #[test]
    fn a_failed_open_closes_the_unix_file_and_frees_its_state() {
        UNIX_CLOSES.set(0);
        let db = Arc::new(Mutex::new(Db::for_tests()));
        let mut file = wal_file_of(&db);
        file.inner.pMethods = &raw const COUNTING;
        // SAFETY: a file set up as `x_open` leaves it once the "unix" file is open: its `ext` is
        // the `Box` `wal_file_of` made.
        let answer = unsafe { failed_open((&raw mut file).cast()) };
        assert_eq!(answer, ffi::SQLITE_CANTOPEN);
        assert_eq!(UNIX_CLOSES.get(), 1);
        assert!(file.ext.is_null());
        assert!(file.base.pMethods.is_null());
        assert_eq!(Arc::strong_count(&db), 1, "the file's state is freed");
        // Without a "unix" file that opened, nothing is closed.
        let mut fresh = unopened();
        // SAFETY: a file as `not_open` sets it up.
        let answer = unsafe { failed_open((&raw mut fresh).cast()) };
        assert_eq!((answer, UNIX_CLOSES.get()), (ffi::SQLITE_CANTOPEN, 1));
    }

    // A panic poisons the database without dropping the overlay, which belongs to the connection
    // that holds the WAL write lock (maybe another than the one that panicked): its reads of
    // pages it spilled still find them, and its next write fails, which rolls its transaction
    // back.
    #[test]
    fn a_panic_poisons_without_dropping_the_writers_overlay() {
        let db = Arc::new(Mutex::new(Db::for_tests()));
        lock(&db).overlay.insert(4096, vec![7u8; 16]);
        let mut file = wal_file_of(&db);
        let f = (&raw mut file).cast::<ffi::sqlite3_file>();
        let mut buf = [0u8; 16];
        set_panic_in_for_tests(Some("x_read"));
        // SAFETY: `c_read` panics before its body touches `f`.
        let read = unsafe { METHODS.xRead.unwrap()(f, buf.as_mut_ptr().cast(), 16, 0) };
        set_panic_in_for_tests(None);
        assert_eq!(read, ffi::SQLITE_IOERR_READ);
        let d = lock(&db);
        assert!(d.poisoned.is_some());
        assert_eq!(d.overlay.get(&4096), Some(&vec![7u8; 16]));
        drop(d);
        // SAFETY: a WAL write to a poisoned database is refused before it reaches the file.
        let write = unsafe { METHODS.xWrite.unwrap()(f, buf.as_ptr().cast(), 16, 32) };
        assert_eq!(write, ffi::SQLITE_IOERR_WRITE);
        // SAFETY: the `Box` `wal_file_of` made, which nothing freed.
        drop(unsafe { Box::from_raw(file.ext) });
    }

    /// A file as SQLite allocates one for `xOpen`, and as `not_open` sets it up: no methods, no
    /// state, and a "unix" file without methods.
    fn unopened() -> File {
        File {
            base: ffi::sqlite3_file { pMethods: null() },
            ext: null_mut(),
            inner: ffi::sqlite3_file { pMethods: null() },
        }
    }

    /// An open WAL handle of `db` for the entry points: this VFS's state alone, with no "unix" file
    /// under it (`inner` has no methods). The caller frees `ext`.
    fn wal_file_of(db: &Arc<Mutex<Db>>) -> File {
        File {
            base: ffi::sqlite3_file {
                pMethods: &raw const METHODS,
            },
            ext: Box::into_raw(Box::new(Ext {
                path: None,
                db: Some(db.clone()),
                fence: None,
                wal: true,
                exclusive: Cell::new(false),
            })),
            inner: ffi::sqlite3_file { pMethods: null() },
        }
    }

    // An open that fails (here the "unix" VFS is missing, as in every unit test) releases the open
    // count it took: a count left behind would refuse every later attach of the file in this
    // process.
    #[test]
    fn a_failed_open_releases_its_open_count() {
        let path = format!("/a-failed-open-{}", std::process::id());
        let name = std::ffi::CString::new(path.clone()).unwrap();
        let mut file = unopened();
        let mut out: c_int = 0;
        // SAFETY: a `File` to open, and a name that outlives the call.
        let opened = unsafe {
            c_open(
                null_mut(),
                name.as_ptr(),
                (&raw mut file).cast(),
                ffi::SQLITE_OPEN_MAIN_DB | ffi::SQLITE_OPEN_READWRITE,
                &raw mut out,
            )
        };
        assert_eq!(opened, ffi::SQLITE_CANTOPEN);
        assert!(file.base.pMethods.is_null());
        assert!(!registry().open.contains_key(&path));
    }
}
