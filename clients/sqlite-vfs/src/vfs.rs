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
use std::time::Duration;
use std::time::Instant;

use libsqlite3_sys as ffi;

use crate::claim::reclaim;
use crate::client::Append;
use crate::client::append;
use crate::client::recreated;
use crate::client::stream_seq;
use crate::config::abort_after_ack;
use crate::db::CommitStat;
use crate::db::Db;
use crate::db::lock;
use crate::db::lookup;
use crate::db::refused;
use crate::db::registry;
use crate::error::Error;
use crate::error::Fence;
use crate::frame;
use crate::frame::PAGE;
use crate::host::OK;
use crate::host::unix;
use crate::local::sidecar_line;
use crate::local::write_sidecar;
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

/// The size of a wal-index region (the first holds the header).
const WAL_INDEX_REGION: c_int = 32 * 1024;

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
    // SAFETY: a non-null `ext` is the `Box` `x_open` leaked for this file, freed only by `x_close`.
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

impl Db {
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
        // wal-index header (first copy): mxFrame is the u32 at byte 16, native endian; the salts
        // at byte 32 are the WAL header's bytes 16..24.
        let mut p: *mut c_void = null_mut();
        let rc = fwd!(file, xShmMap, 0, WAL_INDEX_REGION, 0, &raw mut p);
        let (mx_frame, salts) = if rc == OK && !p.is_null() {
            let p = p.cast::<u8>();
            // SAFETY: region 0 of the wal-index is mapped (32 KiB, page aligned): the u32 at
            // byte 16 lies in it, aligned.
            let mx_frame = unsafe { p.wrapping_add(16).cast::<u32>().read_volatile() };
            // SAFETY: as above, for the 8 bytes at byte 32.
            let salts = unsafe { p.wrapping_add(32).cast::<[u8; 8]>().read_volatile() };
            (mx_frame, salts)
        } else {
            (0, [0; 8])
        };
        if mx_frame < self.commit_frame_no {
            // Already poisoned (the snapshot thread's fence fails the rest of the transaction's
            // WAL writes): keep that reason.
            if self.poisoned.is_none() {
                self.poison(Error::NotPublished {
                    mx_frame,
                    frame: self.commit_frame_no,
                });
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

pub(crate) unsafe extern "C" fn x_open(
    _vfs: *mut ffi::sqlite3_vfs,
    zname: ffi::sqlite3_filename,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    let f = file.cast::<File>();
    // SAFETY: SQLite passes `szOsFile` writable bytes for the new file, laid out as `File`. Until
    // its methods are set it is not open, so a failure leaves nothing for SQLite to close.
    unsafe { (*f).base.pMethods = null() };
    // SAFETY: as above.
    unsafe { (*f).ext = null_mut() };
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
        eprintln!("sqlite-ursula-vfs: {db}: rollback journal refused (journal_mode must be WAL)");
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
            let open = reg.open.entry(name.to_owned()).or_default();
            *open = open.saturating_add(1);
            Some(db)
        }
        _ => None,
    };
    let u = unix();
    // SAFETY: `UNIX` is null or SQLite's "unix" VFS, which lives for the process.
    let open = unsafe { u.as_ref() }.and_then(|u| u.xOpen);
    // SAFETY: the "unix" file lies within SQLite's allocation for this file (`szOsFile` counts
    // it), and the other arguments are SQLite's.
    let rc = open.map_or(ffi::SQLITE_CANTOPEN, |open| unsafe {
        open(u, zname, inner(file), flags, out)
    });
    if rc != OK {
        if let Some(name) = name.filter(|_| main.is_some()) {
            uncount_open(name);
        }
        return rc;
    }
    let ext = match (name, main) {
        (Some(name), Some(db)) => Some(Ext {
            path: Some(name.to_owned()),
            db,
            wal: false,
            exclusive: Cell::new(false),
        }),
        (Some(name), None) if flags & ffi::SQLITE_OPEN_WAL != 0 => {
            name.strip_suffix("-wal").and_then(lookup).map(|db| {
                let mut d = lock(&db);
                d.wal_open = d.wal_open.saturating_add(1);
                drop(d);
                Ext {
                    path: None,
                    db: Some(db),
                    wal: true,
                    exclusive: Cell::new(false),
                }
            })
        }
        _ => None,
    };
    if let Some(ext) = ext {
        // SAFETY: as above.
        unsafe { (*f).ext = Box::into_raw(Box::new(ext)) };
    }
    // SAFETY: as above. From here the file is open, and SQLite calls its methods.
    unsafe { (*f).base.pMethods = &raw const METHODS };
    OK
}

unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    let f = file.cast::<File>();
    // SAFETY: SQLite closes an open file of this VFS once; its state is taken here.
    let ext = unsafe { std::mem::replace(&mut (*f).ext, null_mut()) };
    let ext = if ext.is_null() {
        None
    } else {
        // SAFETY: a non-null `ext` is the `Box` `x_open` leaked for this file, freed only here.
        Some(unsafe { Box::from_raw(ext) })
    };
    if let Some(e) = &ext
        && let Some(db) = &e.db
    {
        let mut db = lock(db);
        if e.wal {
            db.wal_open = db.wal_open.saturating_sub(1);
        } else if e.exclusive.get() {
            db.exclusive = 0;
        }
    }
    let rc = fwd!(file, xClose);
    if let Some(path) = ext.and_then(|e| e.path) {
        uncount_open(&path);
    }
    rc
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

unsafe extern "C" fn x_read(
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

unsafe extern "C" fn x_write(
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
        Err(e) => return db.poison(Error::EncodeFrame(e)),
    };
    let t = Instant::now();
    let mut outcome = append(
        &db.url,
        &db.incarnation,
        &db.producer,
        &body,
        db.epoch,
        db.seq.saturating_add(1),
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
            db.seq.saturating_add(1),
        );
    }
    let seq = db.seq.saturating_add(1);
    let append_time = t.elapsed();
    let (next, attempts) = match outcome {
        Append::Acked {
            next: Some(n),
            attempts,
        } if n > db.offset => (n, attempts),
        Append::Acked { next: Some(n), .. } => {
            return db.poison(Error::AckNotPast {
                offset: db.offset.clone(),
                next: n,
            });
        }
        // A duplicate answered without its receipt: the server evicted it (more than its receipt
        // window ago). Never our own retry: this owner is the only writer of its producer at its
        // epoch (verified claim) with one append in flight, the newest, and the server never
        // evicts a producer's newest receipt. So another writer appended at this epoch past `seq`.
        Append::Acked { next: None, .. } => {
            return db.poison(Error::Fenced(Fence::DuplicateWithoutReceipt {
                offset: db.offset.clone(),
                producer: db.producer.clone(),
                epoch: db.epoch,
                seq,
            }));
        }
        Append::Fenced { current } => {
            return db.poison(Error::Fenced(Fence::Superseded {
                epoch: db.epoch,
                current,
            }));
        }
        // The stream at the path is another incarnation: this commit reached nothing.
        Append::Recreated => {
            return db.poison(recreated(&db.url, &db.incarnation));
        }
        // Not this owner's own retry (that is a duplicate, answered before `Stream-Seq` is
        // checked), nor an older owner's (fenced by epoch): a writer outside this protocol.
        Append::SeqConflict { body } => {
            return db.poison(Error::Fenced(Fence::SeqConflict {
                offset: db.offset.clone(),
                stream_seq: stream_seq((db.epoch, seq)),
                body: body.trim().to_owned(),
            }));
        }
        Append::ProducerExpired => {
            return db.poison(Error::ProducerExpiredAgain);
        }
        Append::Failed(e) => return db.poison(e),
    };
    db.seq = seq;
    db.offset = next;
    db.log = db.log.saturating_add(body.len() as u64);
    db.pages = size;
    db.acked = db.acked.saturating_add(1);
    if abort_after_ack() == Some(db.acked) {
        eprintln!(
            "sqlite-ursula-vfs: URSULA_VFS_ABORT_AFTER_ACK={}: aborting after the ack",
            db.acked
        );
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
unsafe extern "C" fn x_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    // SAFETY: the module contract.
    if unsafe { ext(file) }.is_some_and(|e| e.db.is_some()) {
        OK
    } else {
        fwd!(file, xSync, flags)
    }
}

unsafe extern "C" fn x_file_size(
    file: *mut ffi::sqlite3_file,
    out: *mut ffi::sqlite3_int64,
) -> c_int {
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

unsafe extern "C" fn x_lock(file: *mut ffi::sqlite3_file, l: c_int) -> c_int {
    let rc = fwd!(file, xLock, l);
    if rc == OK && l == ffi::SQLITE_LOCK_EXCLUSIVE {
        // SAFETY: the module contract.
        unsafe { track_exclusive(file, true) };
    }
    rc
}

unsafe extern "C" fn x_unlock(file: *mut ffi::sqlite3_file, l: c_int) -> c_int {
    let rc = fwd!(file, xUnlock, l);
    if l < ffi::SQLITE_LOCK_EXCLUSIVE {
        // SAFETY: the module contract.
        unsafe { track_exclusive(file, false) };
    }
    rc
}

unsafe extern "C" fn x_check_reserved_lock(file: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    fwd!(file, xCheckReservedLock, out)
}

/// An attached database's WAL persists past its last connection (checkpointed, not deleted), so
/// the sidecar's claim on it (`WalClaim`) still holds after a clean close. A connection leaving
/// WAL then reopens it and commits the format change through it, which `commit` refuses.
unsafe extern "C" fn x_file_control(
    file: *mut ffi::sqlite3_file,
    op: c_int,
    arg: *mut c_void,
) -> c_int {
    // SAFETY: the module contract.
    if op == ffi::SQLITE_FCNTL_PERSIST_WAL && unsafe { main_db(file) }.is_some() {
        // SAFETY: PERSIST_WAL passes a pointer to one `int`, which it writes the setting to.
        unsafe { *arg.cast::<c_int>() = 1 };
        return OK;
    }
    fwd!(file, xFileControl, op, arg)
}

unsafe extern "C" fn x_sector_size(file: *mut ffi::sqlite3_file) -> c_int {
    fwd!(file, xSectorSize)
}

unsafe extern "C" fn x_device_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
    fwd!(file, xDeviceCharacteristics)
}

unsafe extern "C" fn x_shm_map(
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
unsafe extern "C" fn x_shm_lock(
    file: *mut ffi::sqlite3_file,
    offset: c_int,
    n: c_int,
    flags: c_int,
) -> c_int {
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
            // SAFETY: `file` is the main db handle releasing the WAL write lock it holds.
            unsafe { db.end_write_transaction(file) };
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
            db.writer = file.expose_provenance();
            db.committed = false;
            db.overlay.clear();
        }
        WAL_CKPT_LOCK if rc == OK => db.checkpoint_started = Some(Instant::now()),
        _ => {}
    }
    rc
}

unsafe extern "C" fn x_shm_barrier(file: *mut ffi::sqlite3_file) {
    // SAFETY: the module contract.
    let (i, methods) = unsafe { unix_file(file) };
    if let Some(barrier) = methods.and_then(|m| m.xShmBarrier) {
        // SAFETY: SQLite's call on this file, forwarded to the "unix" file inside it.
        unsafe { barrier(i) };
    }
}

unsafe extern "C" fn x_shm_unmap(file: *mut ffi::sqlite3_file, delete: c_int) -> c_int {
    fwd!(file, xShmUnmap, delete)
}

unsafe extern "C" fn x_fetch(
    file: *mut ffi::sqlite3_file,
    off: ffi::sqlite3_int64,
    amt: c_int,
    pp: *mut *mut c_void,
) -> c_int {
    fwd!(file, xFetch, off, amt, pp)
}

unsafe extern "C" fn x_unfetch(
    file: *mut ffi::sqlite3_file,
    off: ffi::sqlite3_int64,
    p: *mut c_void,
) -> c_int {
    fwd!(file, xUnfetch, off, p)
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::lost_page;
    use crate::frame::PAGE;
    use crate::wal::FIRST_PAGE;
    use crate::wal::FRAME;

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
}
