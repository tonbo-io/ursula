//! SQLite loadable extension: a shim VFS ("ursula", registered as the default) over "unix" that
//! replicates every WAL commit of an attached database to an Ursula JSON stream *before* any of the
//! transaction's frames reach the local `-wal` file.
//!
//! * `SELECT ursula_attach(path, stream_url)` catches the file up from the stream (checkpointing any
//!   local WAL first, then writing every record's page images into the db file) and attaches it; the
//!   next connection that opens `path` (through the default VFS) is replicated. Returns the tail.
//! * WAL writes of an attached database are buffered in memory (reads and the file size see the
//!   buffer as an overlay). The write of the commit frame's page data (the frame whose header has a
//!   non-zero "db size after commit") is the commit point: the transaction's final page images become
//!   one record `{"size":N,"pages":[[pgno,"<base64>"],...]}`, appended with
//!   `Stream-Record-Match: <tail>`. On 2xx the buffer is written to the real WAL and synced, and the
//!   sidecar `<db>-ursula` (next record applied locally) is advanced; on 412 or an unknown outcome
//!   the buffer is dropped, the write fails with SQLITE_IOERR_WRITE (SQLite rolls the transaction
//!   back) and the database is poisoned: every later WAL write fails until it is re-attached.
//! * `SELECT ursula_stats(path)` drains per-commit stats as JSON (bench).
//!
//! Test hook: `URSULA_VFS_ABORT_AFTER_ACK=<n>` aborts the process right after the n-th acknowledged
//! append of an attachment, before the local WAL write.
#![allow(non_snake_case, clippy::missing_safety_doc)]

use base64::Engine as _;
use libsqlite3_sys as ffi;
use std::collections::{BTreeMap, HashMap};
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::os::unix::fs::FileExt;
use std::ptr::{null, null_mut};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

const PAGE: usize = 4096;
const WAL_HDR: i64 = 32;
const FRAME_HDR: i64 = 24;
const FRAME: i64 = FRAME_HDR + PAGE as i64;
const OK: c_int = ffi::SQLITE_OK;
const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

static API: AtomicPtr<ffi::sqlite3_api_routines> = AtomicPtr::new(null_mut());
static UNIX: AtomicPtr<ffi::sqlite3_vfs> = AtomicPtr::new(null_mut());
static VFS: AtomicPtr<ffi::sqlite3_vfs> = AtomicPtr::new(null_mut());

fn api() -> &'static ffi::sqlite3_api_routines {
    unsafe { &*API.load(Ordering::Acquire) }
}
fn unix() -> *mut ffi::sqlite3_vfs {
    UNIX.load(Ordering::Acquire)
}

// ---------------------------------------------------------------------------------------------
// Attached databases

struct Stat {
    bytes: usize,
    pages: usize,
    append: Duration,
    vfs: Duration,
}

struct Db {
    url: String,
    /// Stream tail as known by this attachment (= next record ordinal).
    next: u64,
    poisoned: bool,
    sidecar: String,
    acked: u64,
    stats: Vec<Stat>,
}

fn registry() -> &'static Mutex<HashMap<String, Arc<Mutex<Db>>>> {
    static R: OnceLock<Mutex<HashMap<String, Arc<Mutex<Db>>>>> = OnceLock::new();
    R.get_or_init(Default::default)
}

fn lookup(path: &str) -> Option<Arc<Mutex<Db>>> {
    registry().lock().unwrap().get(path).cloned()
}

fn abort_after_ack() -> Option<u64> {
    static V: OnceLock<Option<u64>> = OnceLock::new();
    *V.get_or_init(|| std::env::var("URSULA_VFS_ABORT_AFTER_ACK").ok().and_then(|v| v.parse().ok()))
}

fn local_sync() -> bool {
    static V: OnceLock<bool> = OnceLock::new();
    *V.get_or_init(|| std::env::var("URSULA_VFS_LOCAL_SYNC").map(|v| v != "0").unwrap_or(true))
}

// ---------------------------------------------------------------------------------------------
// HTTP (blocking, plain HTTP only)

fn agent() -> &'static ureq::Agent {
    static A: OnceLock<ureq::Agent> = OnceLock::new();
    A.get_or_init(|| {
        ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(30)))
            .http_status_as_error(false)
            .build()
            .into()
    })
}

enum Append {
    Acked,
    Fenced,
    Failed(String),
}

fn append(url: &str, tail: u64, body: &str) -> Append {
    let sent = agent()
        .post(url)
        .header("content-type", "application/json")
        .header("stream-record-match", tail.to_string())
        .send(body);
    match sent {
        Ok(mut r) => {
            let status = r.status().as_u16();
            let text = r.body_mut().read_to_string().unwrap_or_default();
            match status {
                200..=299 => Append::Acked,
                412 => Append::Fenced,
                _ => Append::Failed(format!("append: {status} {text}")),
            }
        }
        Err(e) => Append::Failed(format!("append: {e}")),
    }
}

fn create_stream(url: &str) -> Result<(), String> {
    let mut r = agent()
        .put(url)
        .header("content-type", "application/json")
        .send_empty()
        .map_err(|e| format!("create {url}: {e}"))?;
    let status = r.status().as_u16();
    let text = r.body_mut().read_to_string().unwrap_or_default();
    if (200..300).contains(&status) || status == 409 {
        Ok(())
    } else {
        Err(format!("create {url}: {status} {text}"))
    }
}

fn read_records(url: &str, from: u64) -> Result<Vec<String>, String> {
    let mut r = agent()
        .get(format!("{url}?record={from}&max_records=256"))
        .call()
        .map_err(|e| format!("read {url} from {from}: {e}"))?;
    let status = r.status().as_u16();
    let text = r
        .body_mut()
        .with_config()
        .limit(1 << 30)
        .read_to_string()
        .map_err(|e| format!("read body: {e}"))?;
    if status == 204 {
        return Ok(Vec::new());
    }
    if status != 200 {
        return Err(format!("read {url} from {from}: {status} {text}"));
    }
    let t = text.trim();
    if t.starts_with('[') {
        let v: Vec<serde_json::Value> = serde_json::from_str(t).map_err(|e| e.to_string())?;
        return Ok(v.into_iter().map(|v| v.to_string()).collect());
    }
    Ok(t.lines().filter(|l| !l.is_empty()).map(str::to_owned).collect())
}

// ---------------------------------------------------------------------------------------------
// Attach / catch-up

unsafe fn full_pathname(path: &str) -> Result<String, String> {
    let u = unix();
    let c = CString::new(path).map_err(|e| e.to_string())?;
    let mut buf = vec![0u8; (*u).mxPathname as usize + 1];
    let rc = ((*u).xFullPathname.unwrap())(u, c.as_ptr(), buf.len() as c_int, buf.as_mut_ptr() as *mut c_char);
    if rc != OK {
        return Err(format!("xFullPathname({path}): {rc}"));
    }
    Ok(CStr::from_ptr(buf.as_ptr() as *const c_char).to_string_lossy().into_owned())
}

/// Checkpoint (TRUNCATE) any local WAL into the db file, through a private "unix" connection.
unsafe fn checkpoint_local(path: &str) -> Result<(), String> {
    if fs::metadata(format!("{path}-wal")).is_err() {
        return Ok(());
    }
    let a = api();
    let c = CString::new(path).unwrap();
    let mut db: *mut ffi::sqlite3 = null_mut();
    let rc = (a.open_v2.unwrap())(c.as_ptr(), &mut db, ffi::SQLITE_OPEN_READWRITE, c"unix".as_ptr());
    let result = if rc != OK {
        Err(format!("checkpoint open {path}: {rc}"))
    } else {
        let rc = (a.exec.unwrap())(db, c"PRAGMA wal_checkpoint(TRUNCATE)".as_ptr(), None, null_mut(), null_mut());
        if rc != OK {
            Err(format!("checkpoint {path}: {rc}"))
        } else {
            Ok(())
        }
    };
    (a.close.unwrap())(db);
    result
}

/// Writes one record's page images into the db file; returns the db size (pages) after it.
fn apply_record(file: &fs::File, record: &str) -> Result<u64, String> {
    let v: serde_json::Value = serde_json::from_str(record).map_err(|e| format!("record: {e}"))?;
    let size = v["size"].as_u64().ok_or("record without size")?;
    for p in v["pages"].as_array().ok_or("record without pages")? {
        let pgno = p[0].as_u64().ok_or("bad pgno")?;
        let data = B64.decode(p[1].as_str().ok_or("bad page")?).map_err(|e| e.to_string())?;
        if data.len() != PAGE || pgno == 0 {
            return Err(format!("bad page {pgno} ({} bytes)", data.len()));
        }
        file.write_all_at(&data, (pgno - 1) * PAGE as u64).map_err(|e| e.to_string())?;
    }
    let len = file.metadata().map_err(|e| e.to_string())?.len();
    if len > size * PAGE as u64 {
        file.set_len(size * PAGE as u64).map_err(|e| e.to_string())?;
    }
    Ok(size)
}

unsafe fn attach(path: &str, url: &str) -> Result<u64, String> {
    let path = full_pathname(path)?;
    let url = url.trim_end_matches('/').to_owned();
    create_stream(&url)?;
    let sidecar = format!("{path}-ursula");
    let db_len = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    let watermark = if db_len == 0 {
        0
    } else {
        fs::read_to_string(&sidecar).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(0)
    };
    let mut next = watermark;
    let mut file: Option<fs::File> = None;
    let mut size = 0;
    loop {
        let records = read_records(&url, next)?;
        if records.is_empty() {
            break;
        }
        if file.is_none() {
            checkpoint_local(&path)?;
            file = Some(
                OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&path).map_err(|e| e.to_string())?,
            );
        }
        for r in &records {
            size = apply_record(file.as_ref().unwrap(), r)?;
            next += 1;
        }
    }
    if let Some(f) = file {
        f.set_len(size * PAGE as u64).map_err(|e| e.to_string())?;
        f.sync_all().map_err(|e| e.to_string())?;
        fs::write(&sidecar, next.to_string()).map_err(|e| e.to_string())?;
    }
    let db = Db { url, next, poisoned: false, sidecar, acked: 0, stats: Vec::new() };
    registry().lock().unwrap().insert(path, Arc::new(Mutex::new(db)));
    Ok(next)
}

unsafe fn stats(path: &str) -> Result<String, String> {
    let path = full_pathname(path)?;
    let db = lookup(&path).ok_or_else(|| format!("{path} is not attached"))?;
    let mut db = db.lock().unwrap();
    let mut out = String::from("[");
    for (i, s) in db.stats.drain(..).enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(
            out,
            "{{\"bytes\":{},\"pages\":{},\"append_us\":{},\"vfs_us\":{}}}",
            s.bytes,
            s.pages,
            s.append.as_micros(),
            s.vfs.as_micros()
        );
    }
    out.push(']');
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// SQL functions

unsafe fn arg(argv: *mut *mut ffi::sqlite3_value, i: usize) -> String {
    let p = (api().value_text.unwrap())(*argv.add(i));
    if p.is_null() {
        String::new()
    } else {
        CStr::from_ptr(p as *const c_char).to_string_lossy().into_owned()
    }
}

unsafe fn result_error(ctx: *mut ffi::sqlite3_context, msg: &str) {
    let c = CString::new(msg.replace('\0', " ")).unwrap();
    (api().result_error.unwrap())(ctx, c.as_ptr(), -1);
}

unsafe extern "C" fn fn_attach(ctx: *mut ffi::sqlite3_context, _argc: c_int, argv: *mut *mut ffi::sqlite3_value) {
    match attach(&arg(argv, 0), &arg(argv, 1)) {
        Ok(n) => (api().result_int64.unwrap())(ctx, n as i64),
        Err(e) => result_error(ctx, &format!("ursula_attach: {e}")),
    }
}

unsafe extern "C" fn fn_stats(ctx: *mut ffi::sqlite3_context, _argc: c_int, argv: *mut *mut ffi::sqlite3_value) {
    match stats(&arg(argv, 0)) {
        Ok(s) => {
            let c = CString::new(s).unwrap();
            (api().result_text.unwrap())(ctx, c.as_ptr(), -1, ffi::SQLITE_TRANSIENT());
        }
        Err(e) => result_error(ctx, &format!("ursula_stats: {e}")),
    }
}

// ---------------------------------------------------------------------------------------------
// VFS

#[repr(C)]
struct File {
    base: ffi::sqlite3_file,
    /// Non-null only for the WAL of an attached database.
    wal: *mut WalState,
    /// The underlying "unix" file (szOsFile bytes from here).
    inner: ffi::sqlite3_file,
}

struct WalState {
    db: Arc<Mutex<Db>>,
    /// Writes of the open write transaction, by offset (WAL writes never partially overlap: the
    /// header at 0, frame headers at frame offsets, page data at frame offset + 24).
    pending: BTreeMap<i64, Vec<u8>>,
}

impl WalState {
    fn pending_end(&self) -> i64 {
        self.pending.iter().next_back().map(|(o, d)| o + d.len() as i64).unwrap_or(0)
    }
}

unsafe fn inner(f: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    &mut (*(f as *mut File)).inner
}

macro_rules! fwd {
    ($f:expr, $m:ident $(, $a:expr)*) => {{
        let i = inner($f);
        ((*(*i).pMethods).$m.unwrap())(i $(, $a)*)
    }};
}

unsafe fn wal<'a>(f: *mut ffi::sqlite3_file) -> Option<&'a mut WalState> {
    (*(f as *mut File)).wal.as_mut()
}

unsafe extern "C" fn x_open(
    _vfs: *mut ffi::sqlite3_vfs,
    zname: ffi::sqlite3_filename,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    let f = file as *mut File;
    (*f).base.pMethods = null();
    (*f).wal = null_mut();
    let name = if zname.is_null() { None } else { CStr::from_ptr(zname).to_str().ok() };
    // Rollback-journal writes of an attached database would bypass replication; the only one allowed
    // is on an empty file (the very first transaction, `PRAGMA journal_mode=WAL` writing page 1).
    if flags & ffi::SQLITE_OPEN_MAIN_JOURNAL != 0 {
        if let Some(db) = name.and_then(|n| n.strip_suffix("-journal")) {
            if lookup(db).is_some() && fs::metadata(db).map(|m| m.len() > 0).unwrap_or(false) {
                eprintln!("sqlite-ursula-vfs: {db}: rollback journal refused (journal_mode must be WAL)");
                return ffi::SQLITE_CANTOPEN;
            }
        }
    }
    let u = unix();
    let rc = ((*u).xOpen.unwrap())(u, zname, inner(file), flags, out);
    if rc != OK {
        return rc;
    }
    if flags & ffi::SQLITE_OPEN_WAL != 0 {
        if let Some(db) = name.and_then(|n| n.strip_suffix("-wal")).and_then(lookup) {
            (*f).wal = Box::into_raw(Box::new(WalState { db, pending: BTreeMap::new() }));
        }
    }
    (*f).base.pMethods = &METHODS;
    OK
}

unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    let rc = fwd!(file, xClose);
    let f = file as *mut File;
    if !(*f).wal.is_null() {
        drop(Box::from_raw((*f).wal));
        (*f).wal = null_mut();
    }
    rc
}

/// Read through the pending-write overlay.
unsafe fn overlay_read(file: *mut ffi::sqlite3_file, ws: &WalState, buf: &mut [u8], off: i64) -> c_int {
    let rc = fwd!(file, xRead, buf.as_mut_ptr() as *mut c_void, buf.len() as c_int, off);
    if rc != OK && rc != ffi::SQLITE_IOERR_SHORT_READ {
        return rc;
    }
    if ws.pending.is_empty() {
        return rc;
    }
    let end = off + buf.len() as i64;
    for (&o, d) in ws.pending.range((off - 65_536 - FRAME_HDR).max(0)..end) {
        let (s, e) = (o.max(off), (o + d.len() as i64).min(end));
        if s < e {
            buf[(s - off) as usize..(e - off) as usize].copy_from_slice(&d[(s - o) as usize..(e - o) as usize]);
        }
    }
    if rc == OK {
        return OK;
    }
    let mut size: ffi::sqlite3_int64 = 0;
    fwd!(file, xFileSize, &mut size);
    if end <= size.max(ws.pending_end()) {
        OK
    } else {
        ffi::SQLITE_IOERR_SHORT_READ
    }
}

unsafe extern "C" fn x_read(file: *mut ffi::sqlite3_file, buf: *mut c_void, amt: c_int, off: ffi::sqlite3_int64) -> c_int {
    match wal(file) {
        None => fwd!(file, xRead, buf, amt, off),
        Some(ws) => overlay_read(file, ws, std::slice::from_raw_parts_mut(buf as *mut u8, amt as usize), off),
    }
}

fn be32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

fn poison(ws: &mut WalState, db: &mut Db, why: &str) -> c_int {
    eprintln!("sqlite-ursula-vfs: {}: {why}; database poisoned (re-attach to recover)", db.url);
    db.poisoned = true;
    ws.pending.clear();
    ffi::SQLITE_IOERR_WRITE
}

unsafe extern "C" fn x_write(file: *mut ffi::sqlite3_file, buf: *const c_void, amt: c_int, off: ffi::sqlite3_int64) -> c_int {
    let Some(ws) = wal(file) else {
        return fwd!(file, xWrite, buf, amt, off);
    };
    let data = std::slice::from_raw_parts(buf as *const u8, amt as usize);
    {
        let db = ws.db.clone();
        let mut db = db.lock().unwrap();
        if db.poisoned {
            return ffi::SQLITE_IOERR_WRITE;
        }
        if off == 0 && data.len() >= 12 && be32(&data[8..12]) as usize != PAGE {
            return poison(ws, &mut db, &format!("page size {} (only {PAGE} is supported)", be32(&data[8..12])));
        }
    }
    ws.pending.insert(off, data.to_vec());
    // The commit point: the page data of a frame whose header carries "db size after commit".
    if data.len() == PAGE && off >= WAL_HDR + FRAME_HDR && (off - WAL_HDR - FRAME_HDR) % FRAME == 0 {
        let mut h = [0u8; FRAME_HDR as usize];
        let rc = overlay_read(file, ws, &mut h, off - FRAME_HDR);
        if rc != OK {
            return rc;
        }
        let size = be32(&h[4..8]);
        if size != 0 {
            return commit(file, ws, size, off - FRAME_HDR);
        }
    }
    OK
}

unsafe fn commit(file: *mut ffi::sqlite3_file, ws: &mut WalState, size: u32, commit_frame: i64) -> c_int {
    let started = Instant::now();
    // The transaction's final page set: last image per pgno over every frame it wrote up to the
    // commit frame (cache-spill frames included; frames rewritten in place are read back through the
    // overlay). Frames past the commit frame are leftovers of a rolled-back transaction that spilled.
    let frames: Vec<i64> =
        ws.pending.keys().copied().filter(|&o| o >= WAL_HDR && o <= commit_frame && (o - WAL_HDR) % FRAME == 0).collect();
    let mut pages: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
    let mut buf = vec![0u8; FRAME as usize];
    for o in frames {
        let rc = overlay_read(file, ws, &mut buf, o);
        if rc != OK {
            ws.pending.clear();
            return rc;
        }
        let pgno = be32(&buf[0..4]);
        if pgno >= 1 && pgno <= size {
            pages.insert(pgno, buf[FRAME_HDR as usize..].to_vec());
        }
    }
    let mut body = String::with_capacity(pages.len() * (PAGE * 4 / 3 + 16) + 32);
    let _ = write!(body, "{{\"size\":{size},\"pages\":[");
    for (i, (pgno, data)) in pages.iter().enumerate() {
        if i > 0 {
            body.push(',');
        }
        let _ = write!(body, "[{pgno},\"");
        B64.encode_string(data, &mut body);
        body.push_str("\"]");
    }
    body.push_str("]}");

    let db = ws.db.clone();
    let mut db = db.lock().unwrap();
    let t = Instant::now();
    let outcome = append(&db.url, db.next, &body);
    let append_time = t.elapsed();
    match outcome {
        Append::Acked => {}
        Append::Fenced => {
            let why = format!("append fenced at record {} (412)", db.next);
            return poison(ws, &mut db, &why);
        }
        Append::Failed(e) => return poison(ws, &mut db, &e),
    }
    db.next += 1;
    db.acked += 1;
    if abort_after_ack() == Some(db.acked) {
        eprintln!("sqlite-ursula-vfs: URSULA_VFS_ABORT_AFTER_ACK={}: aborting after the ack", db.acked);
        std::process::abort();
    }
    let pending = std::mem::take(&mut ws.pending);
    for (o, d) in &pending {
        let rc = fwd!(file, xWrite, d.as_ptr() as *const c_void, d.len() as c_int, *o);
        if rc != OK {
            db.poisoned = true;
            return rc;
        }
    }
    if local_sync() {
        let rc = fwd!(file, xSync, ffi::SQLITE_SYNC_NORMAL);
        if rc != OK {
            db.poisoned = true;
            return rc;
        }
    }
    let _ = fs::write(&db.sidecar, db.next.to_string());
    if db.stats.len() < 1_000_000 {
        db.stats.push(Stat { bytes: body.len(), pages: pages.len(), append: append_time, vfs: started.elapsed() });
    }
    OK
}

unsafe extern "C" fn x_truncate(file: *mut ffi::sqlite3_file, size: ffi::sqlite3_int64) -> c_int {
    if let Some(ws) = wal(file) {
        ws.pending.retain(|&o, _| o < size);
    }
    fwd!(file, xTruncate, size)
}

unsafe extern "C" fn x_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    fwd!(file, xSync, flags)
}

unsafe extern "C" fn x_file_size(file: *mut ffi::sqlite3_file, out: *mut ffi::sqlite3_int64) -> c_int {
    let rc = fwd!(file, xFileSize, out);
    if rc == OK {
        if let Some(ws) = wal(file) {
            *out = (*out).max(ws.pending_end());
        }
    }
    rc
}

unsafe extern "C" fn x_lock(file: *mut ffi::sqlite3_file, l: c_int) -> c_int {
    fwd!(file, xLock, l)
}
unsafe extern "C" fn x_unlock(file: *mut ffi::sqlite3_file, l: c_int) -> c_int {
    fwd!(file, xUnlock, l)
}
unsafe extern "C" fn x_check_reserved_lock(file: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    fwd!(file, xCheckReservedLock, out)
}
unsafe extern "C" fn x_file_control(file: *mut ffi::sqlite3_file, op: c_int, arg: *mut c_void) -> c_int {
    fwd!(file, xFileControl, op, arg)
}
unsafe extern "C" fn x_sector_size(file: *mut ffi::sqlite3_file) -> c_int {
    fwd!(file, xSectorSize)
}
unsafe extern "C" fn x_device_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
    fwd!(file, xDeviceCharacteristics)
}
unsafe extern "C" fn x_shm_map(file: *mut ffi::sqlite3_file, pg: c_int, pgsz: c_int, extend: c_int, pp: *mut *mut c_void) -> c_int {
    fwd!(file, xShmMap, pg, pgsz, extend, pp)
}
unsafe extern "C" fn x_shm_lock(file: *mut ffi::sqlite3_file, offset: c_int, n: c_int, flags: c_int) -> c_int {
    fwd!(file, xShmLock, offset, n, flags)
}
unsafe extern "C" fn x_shm_barrier(file: *mut ffi::sqlite3_file) {
    fwd!(file, xShmBarrier)
}
unsafe extern "C" fn x_shm_unmap(file: *mut ffi::sqlite3_file, delete: c_int) -> c_int {
    fwd!(file, xShmUnmap, delete)
}
unsafe extern "C" fn x_fetch(file: *mut ffi::sqlite3_file, off: ffi::sqlite3_int64, amt: c_int, pp: *mut *mut c_void) -> c_int {
    fwd!(file, xFetch, off, amt, pp)
}
unsafe extern "C" fn x_unfetch(file: *mut ffi::sqlite3_file, off: ffi::sqlite3_int64, p: *mut c_void) -> c_int {
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

// ---------------------------------------------------------------------------------------------
// Entry point

#[no_mangle]
pub unsafe extern "C" fn sqlite3_extension_init(
    db: *mut ffi::sqlite3,
    _err: *mut *mut c_char,
    p_api: *const ffi::sqlite3_api_routines,
) -> c_int {
    API.store(p_api as *mut _, Ordering::Release);
    let a = api();
    if VFS.load(Ordering::Acquire).is_null() {
        let u = (a.vfs_find.unwrap())(c"unix".as_ptr());
        if u.is_null() {
            return ffi::SQLITE_ERROR;
        }
        UNIX.store(u, Ordering::Release);
        let mut v: ffi::sqlite3_vfs = *u;
        v.zName = c"ursula".as_ptr();
        v.szOsFile = (std::mem::offset_of!(File, inner) + (*u).szOsFile as usize) as c_int;
        v.pNext = null_mut();
        v.xOpen = Some(x_open);
        let v = Box::into_raw(Box::new(v));
        let rc = (a.vfs_register.unwrap())(v, 1);
        if rc != OK {
            return rc;
        }
        VFS.store(v, Ordering::Release);
    }
    let create = a.create_function_v2.unwrap();
    let rc = create(db, c"ursula_attach".as_ptr(), 2, ffi::SQLITE_UTF8, null_mut(), Some(fn_attach), None, None, None);
    if rc != OK {
        return rc;
    }
    let rc = create(db, c"ursula_stats".as_ptr(), 1, ffi::SQLITE_UTF8, null_mut(), Some(fn_stats), None, None, None);
    if rc != OK {
        return rc;
    }
    ffi::SQLITE_OK_LOAD_PERMANENTLY
}
