//! The "ursula" VFS: the file methods that route an attached database's WAL writes through
//! the overlay and append each commit to the stream at its commit point.

use std::collections::BTreeMap;
use std::ffi::CStr;
use std::ffi::c_int;
use std::ffi::c_void;
use std::ptr::null;
use std::ptr::null_mut;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use libsqlite3_sys as ffi;

use crate::claim::reclaim;
use crate::client::Append;
use crate::client::append;
use crate::client::recreated_error;
use crate::client::stream_seq;
use crate::config::abort_after_ack;
use crate::db::CommitStat;
use crate::db::Db;
use crate::db::lock;
use crate::db::lookup;
use crate::db::refused;
use crate::db::registry;
use crate::frame;
use crate::frame::PAGE;
use crate::host::OK;
use crate::host::unix;
use crate::local::sidecar_line;
use crate::local::write_sidecar;
use crate::wal::FRAME;
use crate::wal::FRAME_HDR;
use crate::wal::WAL_HDR;
use crate::wal::WalClaim;
use crate::wal::be32;

/// `WAL_WRITE_LOCK` and `WAL_CKPT_LOCK`: the shm lock slots SQLite takes for a write transaction and
/// a checkpoint.
const WAL_WRITE_LOCK: c_int = 0;

const WAL_CKPT_LOCK: c_int = 1;

/// The longest a commit waits at its commit point for a due snapshot to open its window.
pub(crate) const WINDOW_WAIT: Duration = Duration::from_secs(1);

/// Calls method `$m` of the underlying "unix" file.
macro_rules! fwd {
    ($f:expr, $m:ident $(, $a:expr)*) => {{
        let i = inner($f);
        ((*(*i).pMethods).$m.unwrap())(i $(, $a)*)
    }};
}

impl Db {
    /// Fsyncs the db file through the main db handle of the connection asking (the one holding the
    /// WAL write lock, or a closing one's EXCLUSIVE lock): before the local WAL starts a new
    /// generation or is truncated to nothing, so a disk image that shows the new WAL holds every
    /// page checkpointed from the old one (see `WalClaim`). Rare: once per WAL wrap. A failure
    /// poisons.
    pub(crate) unsafe fn sync_db(&mut self, before: &str) -> c_int {
        let h = [self.writer, self.exclusive].into_iter().find(|&h| h != 0);
        let rc = match h {
            Some(h) => unsafe { fwd!(h as *mut ffi::sqlite3_file, xSync, ffi::SQLITE_SYNC_NORMAL) },
            None => ffi::SQLITE_IOERR_FSYNC,
        };
        if rc != OK {
            self.poison(format!(
                "fsync of the db file before {before} failed ({rc})"
            ));
        }
        rc
    }

    /// The write transaction ended (WAL write lock released): drop what never committed; after an
    /// acknowledged commit that SQLite published, advance the sidecar (no fsync: see the crate
    /// docs).
    ///
    /// Runs before the real lock is released (see `x_shm_lock`), on the main db handle `file`,
    /// whose wal-index header tells whether SQLite published the commit.
    unsafe fn end_write_transaction(&mut self, file: *mut ffi::sqlite3_file) {
        self.writer = 0;
        self.overlay.clear();
        if !std::mem::take(&mut self.committed) {
            return;
        }
        // A snapshot waiting for the acknowledged commit to be published (it runs once this
        // returns and the mutex is released, seeing the outcome).
        self.snapper.window_cv.notify_all();
        // wal-index header (first copy): mxFrame is the u32 at byte 16, native endian; the salts
        // at byte 32 are the WAL header's bytes 16..24.
        let mut p: *mut c_void = null_mut();
        let rc = unsafe { fwd!(file, xShmMap, 0, 32 * 1024, 0, &mut p) };
        let (mx_frame, salts) = if rc == OK && !p.is_null() {
            let p = p as *const u8;
            unsafe {
                (
                    std::ptr::read_volatile(p.add(16) as *const u32),
                    std::ptr::read_volatile(p.add(32) as *const [u8; 8]),
                )
            }
        } else {
            (0, [0; 8])
        };
        if mx_frame < self.commit_frame_no {
            // Already poisoned (the snapshot thread's fence fails the rest of the transaction's
            // WAL writes): keep that reason.
            if self.poisoned.is_none() {
                self.poison(format!(
                    "commit acknowledged but not published locally (mxFrame {mx_frame} < frame {})",
                    self.commit_frame_no
                ));
            }
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
        if self.snapshot_due() {
            self.snapper.request();
        }
    }
}

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
    wal: bool,
    /// This main db handle holds an EXCLUSIVE file lock.
    exclusive: bool,
}

pub(crate) unsafe fn inner(f: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    unsafe { &mut (*(f as *mut File)).inner }
}

/// The attached database of a WAL handle.
unsafe fn wal_db(f: *mut ffi::sqlite3_file) -> Option<Arc<Mutex<Db>>> {
    unsafe {
        (*(f as *mut File))
            .ext
            .as_ref()
            .filter(|e| e.wal)
            .and_then(|e| e.db.clone())
    }
}

/// The attached database of a main db handle.
unsafe fn main_db(f: *mut ffi::sqlite3_file) -> Option<Arc<Mutex<Db>>> {
    unsafe {
        (*(f as *mut File))
            .ext
            .as_ref()
            .filter(|e| !e.wal)
            .and_then(|e| e.db.clone())
    }
}

pub(crate) unsafe extern "C" fn x_open(
    _vfs: *mut ffi::sqlite3_vfs,
    zname: ffi::sqlite3_filename,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    unsafe {
        let f = file as *mut File;
        (*f).base.pMethods = null();
        (*f).ext = null_mut();
        let name = if zname.is_null() {
            None
        } else {
            CStr::from_ptr(zname).to_str().ok()
        };
        // Rollback-journal writes of an attached database would bypass replication (attach gives
        // an empty file its WAL-format page 1, so no commit ever needs a journal).
        if flags & ffi::SQLITE_OPEN_MAIN_JOURNAL != 0
            && let Some(db) = name.and_then(|n| n.strip_suffix("-journal"))
            && lookup(db).is_some()
        {
            eprintln!(
                "sqlite-ursula-vfs: {db}: rollback journal refused (journal_mode must be WAL)"
            );
            return ffi::SQLITE_CANTOPEN;
        }
        // Counted before the open, so attach (which refuses while any is counted) and an open
        // cannot pass each other; refused while `attach` runs on the path, and when it has a
        // sidecar but no binding (`refused`).
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
                    eprintln!("sqlite-ursula-vfs: {name}: open refused: {why}");
                    return ffi::SQLITE_CANTOPEN;
                }
                *reg.open.entry(name.to_owned()).or_default() += 1;
                Some(db)
            }
            _ => None,
        };
        let u = unix();
        let rc = ((*u).xOpen.unwrap())(u, zname, inner(file), flags, out);
        if rc != OK {
            if main.is_some() {
                uncount_open(name.unwrap());
            }
            return rc;
        }
        if let Some(name) = name {
            if let Some(db) = main {
                (*f).ext = Box::into_raw(Box::new(Ext {
                    path: Some(name.to_owned()),
                    db,
                    wal: false,
                    exclusive: false,
                }));
            } else if flags & ffi::SQLITE_OPEN_WAL != 0
                && let Some(db) = name.strip_suffix("-wal").and_then(lookup)
            {
                lock(&db).wal_open += 1;
                (*f).ext = Box::into_raw(Box::new(Ext {
                    path: None,
                    db: Some(db),
                    wal: true,
                    exclusive: false,
                }));
            }
        }
        (*f).base.pMethods = &METHODS;
        OK
    }
}

unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let f = file as *mut File;
        let ext = if (*f).ext.is_null() {
            None
        } else {
            Some(Box::from_raw(std::mem::replace(&mut (*f).ext, null_mut())))
        };
        if let Some(db) = ext.as_ref().and_then(|e| e.db.as_ref()) {
            let mut db = lock(db);
            if ext.as_ref().is_some_and(|e| e.wal) {
                db.wal_open -= 1;
            } else if ext.as_ref().is_some_and(|e| e.exclusive) {
                db.exclusive = 0;
            }
        }
        let rc = fwd!(file, xClose);
        if let Some(path) = ext.and_then(|e| e.path) {
            uncount_open(&path);
        }
        rc
    }
}

fn uncount_open(path: &str) {
    let mut reg = registry();
    if let Some(n) = reg.open.get_mut(path) {
        *n -= 1;
        if *n == 0 {
            reg.open.remove(path);
        }
    }
}

/// Reads `buf` at `off` through the overlay.
unsafe fn overlay_read(file: *mut ffi::sqlite3_file, db: &Db, buf: &mut [u8], off: i64) -> c_int {
    unsafe {
        let rc = fwd!(
            file,
            xRead,
            buf.as_mut_ptr() as *mut c_void,
            buf.len() as c_int,
            off
        );
        if rc != OK && rc != ffi::SQLITE_IOERR_SHORT_READ {
            return rc;
        }
        if db.overlay.is_empty() {
            return rc;
        }
        let end = off + buf.len() as i64;
        for (&o, d) in db.overlay.range((off - FRAME).max(0)..end) {
            let (s, e) = (o.max(off), (o + d.len() as i64).min(end));
            if s < e {
                buf[(s - off) as usize..(e - off) as usize]
                    .copy_from_slice(&d[(s - o) as usize..(e - o) as usize]);
            }
        }
        if rc == OK {
            return OK;
        }
        let mut size: ffi::sqlite3_int64 = 0;
        fwd!(file, xFileSize, &mut size);
        if end <= size.max(db.overlay_end()) {
            OK
        } else {
            ffi::SQLITE_IOERR_SHORT_READ
        }
    }
}

unsafe extern "C" fn x_read(
    file: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    amt: c_int,
    off: ffi::sqlite3_int64,
) -> c_int {
    unsafe {
        match wal_db(file) {
            None => fwd!(file, xRead, buf, amt, off),
            Some(db) => overlay_read(
                file,
                &lock(&db),
                std::slice::from_raw_parts_mut(buf as *mut u8, amt as usize),
                off,
            ),
        }
    }
}

pub(crate) unsafe extern "C" fn x_write(
    file: *mut ffi::sqlite3_file,
    buf: *const c_void,
    amt: c_int,
    off: ffi::sqlite3_int64,
) -> c_int {
    unsafe {
        let Some(db) = wal_db(file) else {
            if let Some(db) = main_db(file) {
                let mut db = lock(&db);
                if !db.db_write_allowed() {
                    return db.poison(
                        "db file write outside a checkpoint (journal_mode must stay WAL)".into(),
                    );
                }
            }
            return fwd!(file, xWrite, buf, amt, off);
        };
        let mut db = lock(&db);
        if db.poisoned.is_some() {
            return ffi::SQLITE_IOERR_WRITE;
        }
        if db.committed {
            // The rest of an acknowledged transaction (checksum rewrites, padding): the local WAL.
            let rc = fwd!(file, xWrite, buf, amt, off);
            return db.post_ack("local WAL write", rc);
        }
        if db.writer == 0 {
            return db.poison("WAL write outside a write-locked transaction (locking_mode=EXCLUSIVE is not supported)".into());
        }
        let data = std::slice::from_raw_parts(buf as *const u8, amt as usize);
        if off == 0 && data.len() >= 12 && be32(&data[8..12]) as usize != PAGE {
            return db.poison(format!(
                "page size {} (only {PAGE} is supported)",
                be32(&data[8..12])
            ));
        }
        db.overlay.insert(off, data.to_vec());
        // The commit point: the page data of a frame whose header carries "db size after commit".
        if data.len() == PAGE
            && off >= WAL_HDR + FRAME_HDR
            && (off - WAL_HDR - FRAME_HDR) % FRAME == 0
        {
            let mut h = [0u8; FRAME_HDR as usize];
            let rc = overlay_read(file, &db, &mut h, off - FRAME_HDR);
            if rc != OK {
                return rc;
            }
            let size = be32(&h[4..8]);
            if size != 0 {
                let snapper = db.snapper.clone();
                // Nothing is acknowledged yet (`!committed`), so a wanted window opens now.
                if db.window_wanted {
                    snapper.window_cv.notify_all();
                }
                let deadline = Instant::now() + WINDOW_WAIT;
                while db.window || db.window_wanted {
                    let now = Instant::now();
                    if !db.window && now >= deadline {
                        break;
                    }
                    let wait = if db.window {
                        Duration::from_secs(1)
                    } else {
                        deadline - now
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
                return commit(file, &mut db, size, off - FRAME_HDR);
            }
        }
        OK
    }
}

/// The transaction's final page set: the last image per pgno over every frame it wrote up to the
/// commit frame (spilled frames included; frames rewritten in place hold their newest image).
unsafe fn final_pages(
    file: *mut ffi::sqlite3_file,
    db: &Db,
    size: u32,
    commit_frame: i64,
) -> Result<BTreeMap<u32, Vec<u8>>, c_int> {
    let mut pages = BTreeMap::new();
    let frames = db
        .overlay
        .keys()
        .copied()
        .filter(|&o| o >= WAL_HDR && o <= commit_frame && (o - WAL_HDR) % FRAME == 0);
    for o in frames {
        let (h, d) = (db.overlay.get(&o), db.overlay.get(&(o + FRAME_HDR)));
        let (pgno, data) = match (h, d) {
            (Some(h), Some(d)) if h.len() == FRAME_HDR as usize && d.len() == PAGE => {
                (be32(h), d.clone())
            }
            _ => {
                let mut buf = vec![0u8; FRAME as usize];
                let rc = unsafe { overlay_read(file, db, &mut buf, o) };
                if rc != OK {
                    return Err(rc);
                }
                (be32(&buf), buf.split_off(FRAME_HDR as usize))
            }
        };
        if pgno >= 1 && pgno <= size {
            pages.insert(pgno, data);
        }
    }
    Ok(pages)
}

pub(crate) unsafe fn commit(
    file: *mut ffi::sqlite3_file,
    db: &mut Db,
    size: u32,
    commit_frame: i64,
) -> c_int {
    let started = Instant::now();
    // The transaction starts a new WAL generation (its header is in the overlay): every page
    // checkpointed from the previous one must be on disk before the header can be (`WalClaim`).
    if db.overlay.contains_key(&0) {
        let rc = unsafe { db.sync_db("a new WAL generation") };
        if rc != OK {
            return rc;
        }
    }
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
    if pages.get(&1).is_some_and(|p| p[18] != 2 || p[19] != 2) {
        return db.poison("a commit leaves WAL format (journal_mode must stay WAL)".into());
    }
    let (body, raw) = frame::encode_commit(size, &pages);
    let t = Instant::now();
    let mut outcome = append(
        &db.url,
        &db.incarnation,
        &db.producer,
        &body,
        db.epoch,
        db.seq + 1,
    );
    // An unknown producer: the server expired it (`reclaim`).
    if let Append::ProducerExpired = outcome {
        if let Err(e) = reclaim(db) {
            return db.poison(e);
        }
        outcome = append(
            &db.url,
            &db.incarnation,
            &db.producer,
            &body,
            db.epoch,
            db.seq + 1,
        );
    }
    let seq = db.seq + 1;
    let append_time = t.elapsed();
    let (next, attempts) = match outcome {
        Append::Acked {
            next: Some(n),
            attempts,
        } if n > db.offset => (n, attempts),
        Append::Acked { next: Some(n), .. } => {
            return db.poison(format!(
                "append after {} acknowledged with next offset {n}, not past it",
                db.offset
            ));
        }
        // A duplicate answered without its receipt: the server evicted it (more than its receipt
        // window ago). Never our own retry: this owner is the only writer of its producer at its
        // epoch (verified claim) with one append in flight, the newest, and the server never
        // evicts a producer's newest receipt. So another writer appended at this epoch past `seq`.
        Append::Acked { next: None, .. } => {
            db.fenced = true;
            return db.poison(format!(
                "fenced: append at {} answered as a duplicate without a receipt: another writer \
                 (a foreign one using this Producer-Id?) holds producer {} at epoch {} past seq \
                 {seq}",
                db.offset, db.producer, db.epoch
            ));
        }
        Append::Fenced { current } => {
            db.fenced = true;
            return db.poison(format!(
                "fenced: epoch {} superseded by {current:?} (403)",
                db.epoch
            ));
        }
        // The stream at the path is another incarnation: this commit reached nothing.
        Append::Recreated => {
            db.fenced = true;
            return db.poison(recreated_error(&db.url, &db.incarnation));
        }
        // Not this owner's own retry (that is a duplicate, answered before `Stream-Seq` is
        // checked), nor an older owner's (fenced by epoch): a writer outside this protocol.
        Append::SeqConflict(e) => {
            db.fenced = true;
            return db.poison(format!(
                "fenced: append after {} refused its Stream-Seq {}: another writer appended with \
                 a higher one ({})",
                db.offset,
                stream_seq((db.epoch, seq)),
                e.trim()
            ));
        }
        Append::ProducerExpired => {
            return db.poison("producer expired again right after a re-claim".into());
        }
        Append::Failed(e) => return db.poison(e),
    };
    db.seq = seq;
    db.offset = next;
    db.log += body.len() as u64;
    db.pages = size;
    db.acked += 1;
    if abort_after_ack() == Some(db.acked) {
        eprintln!(
            "sqlite-ursula-vfs: URSULA_VFS_ABORT_AFTER_ACK={}: aborting after the ack",
            db.acked
        );
        std::process::abort();
    }
    db.commit_frame_no = ((commit_frame - WAL_HDR) / FRAME + 1) as u32;
    for (o, d) in std::mem::take(&mut db.overlay) {
        let rc = if db.fault() {
            ffi::SQLITE_IOERR_WRITE
        } else {
            unsafe {
                fwd!(
                    file,
                    xWrite,
                    d.as_ptr() as *const c_void,
                    d.len() as c_int,
                    o
                )
            }
        };
        if rc != OK {
            db.poison(format!("local WAL write: {rc}"));
            return rc;
        }
    }
    db.committed = true;
    if db.stats.len() < 1_000_000 {
        db.stats.push(CommitStat {
            bytes: body.len(),
            raw,
            pages: pages.len(),
            attempts,
            append: append_time,
            vfs: started.elapsed(),
        });
    }
    OK
}

unsafe extern "C" fn x_truncate(file: *mut ffi::sqlite3_file, size: ffi::sqlite3_int64) -> c_int {
    unsafe {
        if let Some(db) = main_db(file) {
            let mut db = lock(&db);
            if !db.db_write_allowed() {
                return db.poison(
                    "db file truncate outside a checkpoint (journal_mode must stay WAL)".into(),
                );
            }
        }
        let Some(db) = wal_db(file) else {
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
        let rc = fwd!(file, xTruncate, size);
        db.post_ack("local WAL truncate", rc)
    }
}

/// A no-op for the db file and WAL of an attached database, whatever `PRAGMA synchronous` says:
/// they are a cache of the stream, rebuilt after a reboot (see the crate docs), so an fsync would
/// only add latency to commits and checkpoints.
unsafe extern "C" fn x_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    unsafe {
        let attached = (*(file as *mut File))
            .ext
            .as_ref()
            .is_some_and(|e| e.db.is_some());
        if attached {
            OK
        } else {
            fwd!(file, xSync, flags)
        }
    }
}

unsafe extern "C" fn x_file_size(
    file: *mut ffi::sqlite3_file,
    out: *mut ffi::sqlite3_int64,
) -> c_int {
    unsafe {
        let rc = fwd!(file, xFileSize, out);
        if rc == OK
            && let Some(db) = wal_db(file)
        {
            *out = (*out).max(lock(&db).overlay_end());
        }
        rc
    }
}

/// Tracks which main db handle holds an EXCLUSIVE file lock (see `Db::db_write_allowed`).
unsafe fn track_exclusive(file: *mut ffi::sqlite3_file, exclusive: bool) {
    unsafe {
        let Some(ext) = (*(file as *mut File)).ext.as_mut() else {
            return;
        };
        if let (false, Some(db)) = (ext.wal, &ext.db)
            && ext.exclusive != exclusive
        {
            ext.exclusive = exclusive;
            lock(db).exclusive = if exclusive { file as usize } else { 0 };
        }
    }
}

unsafe extern "C" fn x_lock(file: *mut ffi::sqlite3_file, l: c_int) -> c_int {
    unsafe {
        let rc = fwd!(file, xLock, l);
        if rc == OK && l == ffi::SQLITE_LOCK_EXCLUSIVE {
            track_exclusive(file, true);
        }
        rc
    }
}

unsafe extern "C" fn x_unlock(file: *mut ffi::sqlite3_file, l: c_int) -> c_int {
    unsafe {
        let rc = fwd!(file, xUnlock, l);
        if l < ffi::SQLITE_LOCK_EXCLUSIVE {
            track_exclusive(file, false);
        }
        rc
    }
}

unsafe extern "C" fn x_check_reserved_lock(file: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    unsafe { fwd!(file, xCheckReservedLock, out) }
}

/// An attached database's WAL persists past its last connection (checkpointed, not deleted), so
/// the sidecar's claim on it (`WalClaim`) still holds after a clean close. A connection leaving
/// WAL then reopens it and commits the format change through it, which `commit` refuses.
pub(crate) unsafe extern "C" fn x_file_control(
    file: *mut ffi::sqlite3_file,
    op: c_int,
    arg: *mut c_void,
) -> c_int {
    unsafe {
        if op == ffi::SQLITE_FCNTL_PERSIST_WAL && main_db(file).is_some() {
            *(arg as *mut c_int) = 1;
            return OK;
        }
        fwd!(file, xFileControl, op, arg)
    }
}

unsafe extern "C" fn x_sector_size(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe { fwd!(file, xSectorSize) }
}

unsafe extern "C" fn x_device_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe { fwd!(file, xDeviceCharacteristics) }
}

unsafe extern "C" fn x_shm_map(
    file: *mut ffi::sqlite3_file,
    pg: c_int,
    pgsz: c_int,
    extend: c_int,
    pp: *mut *mut c_void,
) -> c_int {
    unsafe {
        let rc = fwd!(file, xShmMap, pg, pgsz, extend, pp);
        if rc != OK
            && let Some(db) = main_db(file)
        {
            return lock(&db).post_ack("-shm map", rc);
        }
        rc
    }
}

/// Tracks the write transaction (WAL write lock) and checkpoints (checkpoint lock) of an attached
/// database; SQLite takes both through the main db handle. The end of a write transaction
/// (publication check, sidecar) runs under the database's mutex *before* the real lock is released:
/// once it is released, another thread's connection may start its own write transaction, whose
/// state the bookkeeping would otherwise see (or whose commit the sidecar would cover before its
/// rewrites reach the WAL). Lock order: the real shm lock may be taken without the mutex, and the
/// mutex may be held while releasing it, which never blocks.
unsafe extern "C" fn x_shm_lock(
    file: *mut ffi::sqlite3_file,
    offset: c_int,
    n: c_int,
    flags: c_int,
) -> c_int {
    unsafe {
        let tracked = flags & ffi::SQLITE_SHM_EXCLUSIVE != 0
            && n == 1
            && (offset == WAL_WRITE_LOCK || offset == WAL_CKPT_LOCK);
        let db = if tracked { main_db(file) } else { None };
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
                db.end_write_transaction(file);
                return fwd!(file, xShmLock, offset, n, flags);
            }
            if let Some(t) = db.checkpoint_started.take()
                && db.checkpoints.len() < 1_000_000
            {
                db.checkpoints.push(t.elapsed());
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
                db.writer = file as usize;
                db.committed = false;
                db.overlay.clear();
            }
            WAL_CKPT_LOCK if rc == OK => db.checkpoint_started = Some(Instant::now()),
            _ => {}
        }
        rc
    }
}

unsafe extern "C" fn x_shm_barrier(file: *mut ffi::sqlite3_file) {
    unsafe { fwd!(file, xShmBarrier) }
}

unsafe extern "C" fn x_shm_unmap(file: *mut ffi::sqlite3_file, delete: c_int) -> c_int {
    unsafe { fwd!(file, xShmUnmap, delete) }
}

unsafe extern "C" fn x_fetch(
    file: *mut ffi::sqlite3_file,
    off: ffi::sqlite3_int64,
    amt: c_int,
    pp: *mut *mut c_void,
) -> c_int {
    unsafe { fwd!(file, xFetch, off, amt, pp) }
}

unsafe extern "C" fn x_unfetch(
    file: *mut ffi::sqlite3_file,
    off: ffi::sqlite3_int64,
    p: *mut c_void,
) -> c_int {
    unsafe { fwd!(file, xUnfetch, off, p) }
}

static METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 3,
    xClose: Some(x_close),
    xRead: Some(x_read),
    xWrite: Some(x_write),
    xTruncate: Some(x_truncate),
    xSync: Some(x_sync),
    xFileSize: Some(x_file_size),
    xLock: Some(x_lock),
    xUnlock: Some(x_unlock),
    xCheckReservedLock: Some(x_check_reserved_lock),
    xFileControl: Some(x_file_control),
    xSectorSize: Some(x_sector_size),
    xDeviceCharacteristics: Some(x_device_characteristics),
    xShmMap: Some(x_shm_map),
    xShmLock: Some(x_shm_lock),
    xShmBarrier: Some(x_shm_barrier),
    xShmUnmap: Some(x_shm_unmap),
    xFetch: Some(x_fetch),
    xUnfetch: Some(x_unfetch),
};
