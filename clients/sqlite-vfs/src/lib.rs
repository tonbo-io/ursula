//! SQLite loadable extension: a shim VFS ("ursula", registered as the default) over "unix" that
//! replicates every WAL commit of an attached database to an Ursula stream *before* any of the
//! transaction's frames reach the local `-wal` file. The stream is the source of truth; the local
//! db file and WAL are a cache of it.
//!
//! * `SELECT ursula_attach(path, stream_url)` takes the host lock `<db>-ursula.lock` (held for the
//!   process lifetime), catches the file up from the stream, claims the stream for this owner (a
//!   new producer epoch, which fences every earlier owner), catches up to the claim and attaches the
//!   file. It requires that no connection to the file is open. Returns the stream offset applied.
//! * Stream: `application/octet-stream`, one self-delimiting frame per append (see [`frame`]):
//!   a commit carries the db size after it and the transaction's final page images; a claim carries
//!   the owner's producer epoch. Appends use the idempotent producer (`Producer-Id` per db,
//!   `Producer-Epoch` per owner, `Producer-Seq` per append): an append whose outcome is unknown is
//!   retried with the same sequence until the server answers (a duplicate is acknowledged without
//!   being applied twice); 403 means another owner claimed the stream.
//! * WAL writes of an attached database go to a per-database overlay (reads and the file size see
//!   it). The write of the commit frame's page data (the frame whose header carries a non-zero
//!   "db size after commit") is the commit point: the transaction's final page images become one
//!   frame, appended. On an ack the overlay is written to the local WAL and every later write of
//!   that transaction (checksum rewrites, padding) goes straight to it; when the write transaction
//!   ends (the WAL write lock is released) the WAL is synced and only then does the sidecar
//!   `<db>-ursula` (stream offset and epoch reflected locally) advance. On a rejection or an
//!   exhausted retry budget the overlay is dropped, the write fails with SQLITE_IOERR_WRITE (SQLite
//!   rolls the transaction back) and the database is poisoned until it is re-attached. The overlay
//!   belongs to the write transaction: it is cleared whenever the WAL write lock is taken or
//!   released, so a rolled-back transaction's spilled frames never shadow a later one's.
//! * `SELECT ursula_status(path)` returns `{"offset","epoch","poisoned","fenced","reason"}`;
//!   `SELECT ursula_stats(path)` drains per-commit and per-checkpoint numbers (bench).
//!
//! Test hook: `URSULA_VFS_ABORT_AFTER_ACK=<n>` aborts the process right after the n-th acknowledged
//! commit of an attachment, before the local WAL write. `URSULA_VFS_RETRY_MS` bounds the retries of
//! an append with an unknown outcome (default 30000).
#![allow(non_snake_case, clippy::missing_safety_doc)]

pub mod frame;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ffi::CStr;
use std::ffi::CString;
use std::ffi::c_char;
use std::ffi::c_int;
use std::ffi::c_void;
use std::fmt::Write as _;
use std::fs::OpenOptions;
use std::fs::{self};
use std::os::unix::fs::FileExt;
use std::ptr::null;
use std::ptr::null_mut;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::OnceLock;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use frame::Decoded;
use frame::Record;
use libsqlite3_sys as ffi;

const PAGE: usize = frame::PAGE;
const WAL_HDR: i64 = 32;
const FRAME_HDR: i64 = 24;
const FRAME: i64 = FRAME_HDR + PAGE as i64;
const OK: c_int = ffi::SQLITE_OK;
/// `WAL_WRITE_LOCK` and `WAL_CKPT_LOCK`: the shm lock slots SQLite takes for a write transaction and
/// a checkpoint.
const WAL_WRITE_LOCK: c_int = 0;
const WAL_CKPT_LOCK: c_int = 1;
const PRODUCER_ID: &str = "sqlite-ursula-vfs";
const CONTENT_TYPE: &str = "application/octet-stream";

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

struct CommitStat {
    bytes: usize,
    raw: usize,
    pages: usize,
    attempts: u32,
    append: Duration,
    vfs: Duration,
}

struct Db {
    url: String,
    sidecar: String,
    wal: String,
    epoch: u64,
    /// Producer sequence of the last acknowledged append (the claim is 0).
    seq: u64,
    /// Stream offset after the last acknowledged frame.
    offset: u64,
    poisoned: Option<String>,
    fenced: bool,
    /// The write transaction in progress (between taking and releasing the WAL write lock).
    write_locked: bool,
    /// WAL writes of that transaction not yet on the local WAL, by offset (WAL writes never
    /// partially overlap: the header at 0, frame headers at frame offsets, page data at frame
    /// offset + 24).
    overlay: BTreeMap<i64, Vec<u8>>,
    /// The transaction's commit is acknowledged: later writes go straight to the local WAL, and
    /// the sidecar advances once the transaction ends.
    committed: bool,
    acked: u64,
    stats: Vec<CommitStat>,
    checkpoint_started: Option<Instant>,
    checkpoints: Vec<Duration>,
}

impl Db {
    fn overlay_end(&self) -> i64 {
        self.overlay
            .iter()
            .next_back()
            .map(|(o, d)| o + d.len() as i64)
            .unwrap_or(0)
    }

    fn poison(&mut self, why: String) -> c_int {
        eprintln!(
            "sqlite-ursula-vfs: {}: {why}; database poisoned (re-attach to recover)",
            self.url
        );
        self.poisoned = Some(why);
        self.overlay.clear();
        ffi::SQLITE_IOERR_WRITE
    }

    /// The write transaction ended (WAL write lock released): drop what never committed; after an
    /// acknowledged commit, sync the local WAL and only then advance the sidecar.
    fn end_write_transaction(&mut self) {
        self.write_locked = false;
        self.overlay.clear();
        if !std::mem::take(&mut self.committed) {
            return;
        }
        let synced = fs::File::open(&self.wal).and_then(|f| f.sync_data());
        if let Err(e) = synced {
            self.poison(format!("sync {}: {e}", self.wal));
            return;
        }
        if let Err(e) = write_sidecar(&self.sidecar, self.offset, self.epoch) {
            self.poison(e);
        }
    }
}

#[derive(Default)]
struct Registry {
    dbs: HashMap<String, Arc<Mutex<Db>>>,
    /// Open main-db handles per path (attached or not): attach requires none.
    open: HashMap<String, usize>,
    /// Host locks held for the process lifetime.
    locks: HashMap<String, fs::File>,
}

fn registry() -> MutexGuard<'static, Registry> {
    static R: OnceLock<Mutex<Registry>> = OnceLock::new();
    R.get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn lookup(path: &str) -> Option<Arc<Mutex<Db>>> {
    registry().dbs.get(path).cloned()
}

fn lock(db: &Mutex<Db>) -> MutexGuard<'_, Db> {
    db.lock().unwrap_or_else(|e| e.into_inner())
}

fn abort_after_ack() -> Option<u64> {
    static V: OnceLock<Option<u64>> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_ABORT_AFTER_ACK")
            .ok()
            .and_then(|v| v.parse().ok())
    })
}

fn retry_budget() -> Duration {
    static V: OnceLock<Duration> = OnceLock::new();
    *V.get_or_init(|| {
        Duration::from_millis(
            std::env::var("URSULA_VFS_RETRY_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30_000),
        )
    })
}

fn write_sidecar(path: &str, offset: u64, epoch: u64) -> Result<(), String> {
    fs::write(path, format!("{offset} {epoch}\n")).map_err(|e| format!("sidecar {path}: {e}"))
}

/// `(offset, epoch)` reflected by the local file.
fn read_sidecar(path: &str) -> Result<(u64, u64), String> {
    let text = fs::read_to_string(path).map_err(|e| format!("sidecar {path}: {e}"))?;
    let mut it = text.split_whitespace().map(|v| v.parse::<u64>());
    match (it.next(), it.next()) {
        (Some(Ok(offset)), Some(Ok(epoch))) => Ok((offset, epoch)),
        _ => Err(format!("sidecar {path}: malformed {text:?}")),
    }
}

// ---------------------------------------------------------------------------------------------
// HTTP (blocking, plain HTTP only)

fn agent() -> &'static ureq::Agent {
    static A: OnceLock<ureq::Agent> = OnceLock::new();
    A.get_or_init(|| {
        ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(10)))
            .http_status_as_error(false)
            .build()
            .into()
    })
}

fn header_u64(r: &ureq::http::Response<ureq::Body>, name: &str) -> Option<u64> {
    r.headers().get(name)?.to_str().ok()?.trim().parse().ok()
}

enum Append {
    /// Applied (or a duplicate of an applied append); the stream offset after it when known.
    Acked { next: Option<u64>, attempts: u32 },
    /// 403: a newer epoch owns the stream.
    Fenced { current: Option<u64> },
    /// A definite rejection, or no answer within the retry budget.
    Failed(String),
}

/// One idempotent append: retried with the same producer sequence until the outcome is known.
fn append(url: &str, body: &[u8], epoch: u64, seq: u64) -> Append {
    let deadline = Instant::now() + retry_budget();
    let mut backoff = Duration::from_millis(20);
    let mut attempts = 0;
    loop {
        attempts += 1;
        let sent = agent()
            .post(url)
            .header("content-type", CONTENT_TYPE)
            .header("producer-id", PRODUCER_ID)
            .header("producer-epoch", epoch.to_string())
            .header("producer-seq", seq.to_string())
            .send(body);
        let unknown = match sent {
            Ok(mut r) => {
                let status = r.status().as_u16();
                match status {
                    200..=299 => {
                        return Append::Acked {
                            next: header_u64(&r, "stream-next-offset"),
                            attempts,
                        };
                    }
                    403 => {
                        return Append::Fenced {
                            current: header_u64(&r, "producer-epoch"),
                        };
                    }
                    400..=499 => {
                        let text = r.body_mut().read_to_string().unwrap_or_default();
                        return Append::Failed(format!("append rejected: {status} {text}"));
                    }
                    _ => format!(
                        "append: {status} {}",
                        r.body_mut().read_to_string().unwrap_or_default()
                    ),
                }
            }
            Err(e) => format!("append: {e}"),
        };
        if Instant::now() + backoff > deadline {
            return Append::Failed(format!(
                "{unknown} (outcome unknown after {attempts} attempts)"
            ));
        }
        std::thread::sleep(backoff);
        backoff = (backoff * 2).min(Duration::from_secs(1));
    }
}

fn create_stream(url: &str) -> Result<(), String> {
    let mut r = agent()
        .put(url)
        .header("content-type", CONTENT_TYPE)
        .send_empty()
        .map_err(|e| format!("create {url}: {e}"))?;
    let status = r.status().as_u16();
    let text = r.body_mut().read_to_string().unwrap_or_default();
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(format!("create {url}: {status} {text}"))
    }
}

/// One read from `offset`: the bytes and the offset after them (empty at the tail).
fn read_from(url: &str, offset: u64) -> Result<(Vec<u8>, u64), String> {
    let mut r = agent()
        .get(format!("{url}?offset={offset}"))
        .call()
        .map_err(|e| format!("read {url} at {offset}: {e}"))?;
    let status = r.status().as_u16();
    if status == 204 {
        return Ok((Vec::new(), offset));
    }
    let next = header_u64(&r, "stream-next-offset");
    let body = r
        .body_mut()
        .with_config()
        .limit(1 << 30)
        .read_to_vec()
        .map_err(|e| format!("read body: {e}"))?;
    if status != 200 {
        return Err(format!(
            "read {url} at {offset}: {status} {}",
            String::from_utf8_lossy(&body)
        ));
    }
    let next = next.unwrap_or(offset + body.len() as u64);
    if next != offset + body.len() as u64 {
        return Err(format!(
            "read {url} at {offset}: {} bytes but next offset {next}",
            body.len()
        ));
    }
    Ok((body, next))
}

// ---------------------------------------------------------------------------------------------
// Attach / catch-up

unsafe fn full_pathname(path: &str) -> Result<String, String> {
    unsafe {
        let u = unix();
        let c = CString::new(path).map_err(|e| e.to_string())?;
        let mut buf = vec![0u8; (*u).mxPathname as usize + 1];
        let rc = ((*u).xFullPathname.unwrap())(
            u,
            c.as_ptr(),
            buf.len() as c_int,
            buf.as_mut_ptr() as *mut c_char,
        );
        if rc != OK {
            return Err(format!("xFullPathname({path}): {rc}"));
        }
        Ok(CStr::from_ptr(buf.as_ptr() as *const c_char)
            .to_string_lossy()
            .into_owned())
    }
}

/// Recovery precondition and step: checkpoints (TRUNCATE) the local WAL into the db file through a
/// private "unix" connection and fails unless every frame was checkpointed and that connection was
/// the last one on the file (closing the last connection deletes the WAL; any other connection, in
/// any process, keeps it), so nothing holds an old WAL or page cache while pages are rewritten.
unsafe fn checkpoint_local(path: &str) -> Result<(), String> {
    unsafe {
        let a = api();
        let c = CString::new(path).unwrap();
        let mut db: *mut ffi::sqlite3 = null_mut();
        let rc = (a.open_v2.unwrap())(
            c.as_ptr(),
            &mut db,
            ffi::SQLITE_OPEN_READWRITE,
            c"unix".as_ptr(),
        );
        let result = if rc != OK {
            Err(format!("checkpoint open {path}: {rc}"))
        } else {
            let mut stmt: *mut ffi::sqlite3_stmt = null_mut();
            let rc = (a.prepare_v2.unwrap())(
                db,
                c"PRAGMA wal_checkpoint(TRUNCATE)".as_ptr(),
                -1,
                &mut stmt,
                null_mut(),
            );
            let r = if rc != OK {
                Err(format!("checkpoint {path}: prepare {rc}"))
            } else if (a.step.unwrap())(stmt) != ffi::SQLITE_ROW {
                let msg = CStr::from_ptr((a.errmsg.unwrap())(db))
                    .to_string_lossy()
                    .into_owned();
                Err(format!("checkpoint {path}: {msg}"))
            } else {
                let col = |i| (a.column_int64.unwrap())(stmt, i);
                let (busy, log, done) = (col(0), col(1), col(2));
                if busy != 0 || log != done {
                    Err(format!(
                        "checkpoint {path}: busy {busy}, {done} of {log} frames checkpointed; another connection is open"
                    ))
                } else {
                    Ok(())
                }
            };
            (a.finalize.unwrap())(stmt);
            r
        };
        (a.close.unwrap())(db);
        result?;
    }
    if fs::metadata(format!("{path}-wal")).is_ok() {
        return Err(format!(
            "{path} is open by another connection (its WAL outlived the recovery connection)"
        ));
    }
    Ok(())
}

/// Applies records to the db file, deduplicating page writes per batch.
struct Applier {
    path: String,
    file: Option<fs::File>,
    /// Final image per page of the current batch (pages past a later shrink removed).
    pages: BTreeMap<u32, Vec<u8>>,
    /// Smallest db size within the batch, and the size after it.
    min_size: Option<u32>,
    size: Option<u32>,
    /// Highest claimed epoch seen.
    epoch: u64,
}

impl Applier {
    fn apply(&mut self, record: Record) {
        match record {
            Record::Claim { epoch } => self.epoch = self.epoch.max(epoch),
            Record::Commit { size, pages } => {
                if size < self.size.unwrap_or(u32::MAX) {
                    self.pages.retain(|&p, _| p <= size);
                }
                self.min_size = Some(self.min_size.map_or(size, |m| m.min(size)));
                self.size = Some(size);
                for (pgno, data) in pages {
                    self.pages.insert(pgno, data);
                }
            }
        }
    }

    /// Writes the batch: truncate to its smallest size, write the final page images, set the size.
    unsafe fn flush(&mut self) -> Result<(), String> {
        let (Some(min), Some(size)) = (self.min_size.take(), self.size) else {
            return Ok(());
        };
        if self.file.is_none() {
            if fs::metadata(&self.path).is_ok_and(|m| m.len() > 0) {
                unsafe { checkpoint_local(&self.path)? };
            }
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&self.path);
            self.file = Some(f.map_err(|e| format!("open {}: {e}", self.path))?);
        }
        let f = self.file.as_ref().unwrap();
        let len = f.metadata().map_err(|e| e.to_string())?.len();
        if len > min as u64 * PAGE as u64 {
            f.set_len(min as u64 * PAGE as u64)
                .map_err(|e| e.to_string())?;
        }
        for (pgno, data) in std::mem::take(&mut self.pages) {
            f.write_all_at(&data, (pgno as u64 - 1) * PAGE as u64)
                .map_err(|e| e.to_string())?;
        }
        f.set_len(size as u64 * PAGE as u64)
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn finish(self) -> Result<(), String> {
        if let Some(f) = self.file {
            f.sync_all()
                .map_err(|e| format!("sync {}: {e}", self.path))?;
        }
        Ok(())
    }
}

/// Reads and applies frames from `offset` until the tail (`until == None`) or `until`; returns the
/// offset after the last applied frame.
unsafe fn catch_up(
    url: &str,
    mut offset: u64,
    until: Option<u64>,
    applier: &mut Applier,
) -> Result<u64, String> {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        if until.is_some_and(|u| offset >= u) {
            break;
        }
        let (bytes, _) = read_from(url, offset + buf.len() as u64)?;
        if bytes.is_empty() {
            if !buf.is_empty() || until.is_some() {
                return Err(format!(
                    "stream {url} ends at {} inside a frame or before {until:?}",
                    offset + buf.len() as u64
                ));
            }
            break;
        }
        buf.extend_from_slice(&bytes);
        let mut used = 0;
        while let Decoded::Frame { record, len } = frame::decode(&buf[used..])
            .map_err(|e| format!("{url} at {}: {e}", offset + used as u64))?
        {
            applier.apply(record);
            used += len;
        }
        buf.drain(..used);
        offset += used as u64;
        unsafe { applier.flush()? };
    }
    Ok(offset)
}

/// Claims the stream with an epoch above every earlier owner's; returns it and the claim's end.
fn claim(url: &str, mut epoch: u64) -> Result<(u64, u64), String> {
    for _ in 0..16 {
        match append(url, &frame::encode_claim(epoch), epoch, 0) {
            Append::Acked {
                next: Some(next), ..
            } => return Ok((epoch, next)),
            Append::Acked { next: None, .. } => {
                return Err(format!("claim {url}: no Stream-Next-Offset"));
            }
            Append::Fenced { current } => epoch = current.unwrap_or(epoch).max(epoch) + 1,
            Append::Failed(e) => return Err(format!("claim {url}: {e}")),
        }
    }
    Err(format!("claim {url}: lost 16 claim races"))
}

unsafe fn attach(path: &str, url: &str) -> Result<u64, String> {
    let path = unsafe { full_pathname(path)? };
    let url = url.trim_end_matches('/').to_owned();
    {
        let mut reg = registry();
        if reg.open.get(&path).copied().unwrap_or(0) > 0 {
            return Err(format!(
                "{path} has open connections; close them before attaching"
            ));
        }
        if !reg.locks.contains_key(&path) {
            let lock_path = format!("{path}-ursula.lock");
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock_path);
            let f = f.map_err(|e| format!("open {lock_path}: {e}"))?;
            f.try_lock()
                .map_err(|e| format!("{path} is attached by another process ({lock_path}: {e})"))?;
            reg.locks.insert(path.clone(), f);
        }
        reg.dbs.remove(&path);
    }
    create_stream(&url)?;
    let sidecar = format!("{path}-ursula");
    let db_len = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    let (from, epoch) = if db_len == 0 {
        // Nothing local: a WAL next to an empty db file holds nothing committed.
        let _ = fs::remove_file(format!("{path}-wal"));
        let _ = fs::remove_file(format!("{path}-shm"));
        (0, 0)
    } else {
        // A file with content but no sidecar was never attached: its pages are not in the stream.
        read_sidecar(&sidecar).map_err(|e| {
            format!("{path} has content but no valid sidecar ({e}); refusing to attach")
        })?
    };
    let mut applier = Applier {
        path: path.clone(),
        file: None,
        pages: BTreeMap::new(),
        min_size: None,
        size: None,
        epoch,
    };
    let offset = unsafe { catch_up(&url, from, None, &mut applier)? };
    let (epoch, claimed) = claim(&url, applier.epoch + 1)?;
    let offset = unsafe { catch_up(&url, offset, Some(claimed), &mut applier)? };
    applier.finish()?;
    write_sidecar(&sidecar, offset, epoch)?;
    let db = Db {
        url,
        sidecar,
        wal: format!("{path}-wal"),
        epoch,
        seq: 0,
        offset,
        poisoned: None,
        fenced: false,
        write_locked: false,
        overlay: BTreeMap::new(),
        committed: false,
        acked: 0,
        stats: Vec::new(),
        checkpoint_started: None,
        checkpoints: Vec::new(),
    };
    registry().dbs.insert(path, Arc::new(Mutex::new(db)));
    Ok(offset)
}

fn json_str(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

unsafe fn status(path: &str) -> Result<String, String> {
    let path = unsafe { full_pathname(path)? };
    let db = lookup(&path).ok_or_else(|| format!("{path} is not attached"))?;
    let db = lock(&db);
    Ok(format!(
        "{{\"offset\":{},\"epoch\":{},\"poisoned\":{},\"fenced\":{},\"reason\":{}}}",
        db.offset,
        db.epoch,
        db.poisoned.is_some(),
        db.fenced,
        db.poisoned.as_deref().map_or("null".to_owned(), json_str)
    ))
}

unsafe fn stats(path: &str) -> Result<String, String> {
    let path = unsafe { full_pathname(path)? };
    let db = lookup(&path).ok_or_else(|| format!("{path} is not attached"))?;
    let mut db = lock(&db);
    let mut out = String::from("{\"commits\":[");
    for (i, s) in db.stats.drain(..).enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(
            out,
            "{{\"bytes\":{},\"raw\":{},\"pages\":{},\"attempts\":{},\"append_us\":{},\"vfs_us\":{}}}",
            s.bytes,
            s.raw,
            s.pages,
            s.attempts,
            s.append.as_micros(),
            s.vfs.as_micros()
        );
    }
    out.push_str("],\"checkpoints_us\":[");
    for (i, d) in db.checkpoints.drain(..).enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(out, "{}", d.as_micros());
    }
    out.push_str("]}");
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// SQL functions

unsafe fn arg(argv: *mut *mut ffi::sqlite3_value, i: usize) -> String {
    unsafe {
        let p = (api().value_text.unwrap())(*argv.add(i));
        if p.is_null() {
            String::new()
        } else {
            CStr::from_ptr(p as *const c_char)
                .to_string_lossy()
                .into_owned()
        }
    }
}

unsafe fn result_error(ctx: *mut ffi::sqlite3_context, msg: &str) {
    let c = CString::new(msg.replace('\0', " ")).unwrap();
    unsafe { (api().result_error.unwrap())(ctx, c.as_ptr(), -1) };
}

unsafe fn result_text(ctx: *mut ffi::sqlite3_context, s: String) {
    let c = CString::new(s).unwrap();
    unsafe { (api().result_text.unwrap())(ctx, c.as_ptr(), -1, ffi::SQLITE_TRANSIENT()) };
}

unsafe extern "C" fn fn_attach(
    ctx: *mut ffi::sqlite3_context,
    _argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    unsafe {
        match attach(&arg(argv, 0), &arg(argv, 1)) {
            Ok(n) => (api().result_int64.unwrap())(ctx, n as i64),
            Err(e) => result_error(ctx, &format!("ursula_attach: {e}")),
        }
    }
}

unsafe extern "C" fn fn_status(
    ctx: *mut ffi::sqlite3_context,
    _argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    unsafe {
        match status(&arg(argv, 0)) {
            Ok(s) => result_text(ctx, s),
            Err(e) => result_error(ctx, &format!("ursula_status: {e}")),
        }
    }
}

unsafe extern "C" fn fn_stats(
    ctx: *mut ffi::sqlite3_context,
    _argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    unsafe {
        match stats(&arg(argv, 0)) {
            Ok(s) => result_text(ctx, s),
            Err(e) => result_error(ctx, &format!("ursula_stats: {e}")),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// VFS

#[repr(C)]
struct File {
    base: ffi::sqlite3_file,
    /// Main db handle of any path (`path` set, for the open count), and the main db or WAL handle
    /// of an attached database (`db` set). Null otherwise.
    ext: *mut Ext,
    /// The underlying "unix" file (szOsFile bytes from here).
    inner: ffi::sqlite3_file,
}

struct Ext {
    path: Option<String>,
    db: Option<Arc<Mutex<Db>>>,
    wal: bool,
}

unsafe fn inner(f: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    unsafe { &mut (*(f as *mut File)).inner }
}

macro_rules! fwd {
    ($f:expr, $m:ident $(, $a:expr)*) => {{
        let i = inner($f);
        ((*(*i).pMethods).$m.unwrap())(i $(, $a)*)
    }};
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

unsafe extern "C" fn x_open(
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
        // Rollback-journal writes of an attached database would bypass replication; the only one
        // allowed is on an empty file (the very first `PRAGMA journal_mode=WAL` writing page 1).
        if flags & ffi::SQLITE_OPEN_MAIN_JOURNAL != 0
            && let Some(db) = name.and_then(|n| n.strip_suffix("-journal"))
            && lookup(db).is_some()
            && fs::metadata(db).map(|m| m.len() > 0).unwrap_or(false)
        {
            eprintln!(
                "sqlite-ursula-vfs: {db}: rollback journal refused (journal_mode must be WAL)"
            );
            return ffi::SQLITE_CANTOPEN;
        }
        let u = unix();
        let rc = ((*u).xOpen.unwrap())(u, zname, inner(file), flags, out);
        if rc != OK {
            return rc;
        }
        if let Some(name) = name {
            if flags & ffi::SQLITE_OPEN_MAIN_DB != 0 {
                let mut reg = registry();
                *reg.open.entry(name.to_owned()).or_default() += 1;
                let db = reg.dbs.get(name).cloned();
                (*f).ext = Box::into_raw(Box::new(Ext {
                    path: Some(name.to_owned()),
                    db,
                    wal: false,
                }));
            } else if flags & ffi::SQLITE_OPEN_WAL != 0
                && let Some(db) = name.strip_suffix("-wal").and_then(lookup)
            {
                (*f).ext = Box::into_raw(Box::new(Ext {
                    path: None,
                    db: Some(db),
                    wal: true,
                }));
            }
        }
        (*f).base.pMethods = &METHODS;
        OK
    }
}

unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let rc = fwd!(file, xClose);
        let f = file as *mut File;
        if !(*f).ext.is_null() {
            let ext = Box::from_raw((*f).ext);
            (*f).ext = null_mut();
            if let Some(path) = ext.path {
                let mut reg = registry();
                if let Some(n) = reg.open.get_mut(&path) {
                    *n -= 1;
                    if *n == 0 {
                        reg.open.remove(&path);
                    }
                }
            }
        }
        rc
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

fn be32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

unsafe extern "C" fn x_write(
    file: *mut ffi::sqlite3_file,
    buf: *const c_void,
    amt: c_int,
    off: ffi::sqlite3_int64,
) -> c_int {
    unsafe {
        let Some(db) = wal_db(file) else {
            return fwd!(file, xWrite, buf, amt, off);
        };
        let mut db = lock(&db);
        if db.poisoned.is_some() {
            return ffi::SQLITE_IOERR_WRITE;
        }
        if db.committed {
            // The rest of an acknowledged transaction (checksum rewrites, padding): the local WAL.
            return fwd!(file, xWrite, buf, amt, off);
        }
        if !db.write_locked {
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

unsafe fn commit(file: *mut ffi::sqlite3_file, db: &mut Db, size: u32, commit_frame: i64) -> c_int {
    let started = Instant::now();
    let pages = match unsafe { final_pages(file, db, size, commit_frame) } {
        Ok(p) => p,
        Err(rc) => {
            db.overlay.clear();
            return rc;
        }
    };
    let (body, raw) = frame::encode_commit(size, &pages);
    let seq = db.seq + 1;
    let t = Instant::now();
    let outcome = append(&db.url, &body, db.epoch, seq);
    let append_time = t.elapsed();
    let expected = db.offset + body.len() as u64;
    let attempts = match outcome {
        Append::Acked { next, attempts } if next.is_none_or(|n| n == expected) => attempts,
        Append::Acked { next, .. } => {
            return db.poison(format!(
                "append at {} acknowledged with next offset {next:?}, expected {expected}",
                db.offset
            ));
        }
        Append::Fenced { current } => {
            db.fenced = true;
            return db.poison(format!(
                "fenced: epoch {} superseded by {current:?} (403)",
                db.epoch
            ));
        }
        Append::Failed(e) => return db.poison(e),
    };
    db.seq = seq;
    db.offset = expected;
    db.acked += 1;
    if abort_after_ack() == Some(db.acked) {
        eprintln!(
            "sqlite-ursula-vfs: URSULA_VFS_ABORT_AFTER_ACK={}: aborting after the ack",
            db.acked
        );
        std::process::abort();
    }
    for (o, d) in std::mem::take(&mut db.overlay) {
        let rc = unsafe {
            fwd!(
                file,
                xWrite,
                d.as_ptr() as *const c_void,
                d.len() as c_int,
                o
            )
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
        if let Some(db) = wal_db(file) {
            lock(&db).overlay.retain(|&o, _| o < size);
        }
        fwd!(file, xTruncate, size)
    }
}

unsafe extern "C" fn x_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    unsafe { fwd!(file, xSync, flags) }
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

unsafe extern "C" fn x_lock(file: *mut ffi::sqlite3_file, l: c_int) -> c_int {
    unsafe { fwd!(file, xLock, l) }
}
unsafe extern "C" fn x_unlock(file: *mut ffi::sqlite3_file, l: c_int) -> c_int {
    unsafe { fwd!(file, xUnlock, l) }
}
unsafe extern "C" fn x_check_reserved_lock(file: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    unsafe { fwd!(file, xCheckReservedLock, out) }
}
unsafe extern "C" fn x_file_control(
    file: *mut ffi::sqlite3_file,
    op: c_int,
    arg: *mut c_void,
) -> c_int {
    unsafe { fwd!(file, xFileControl, op, arg) }
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
    unsafe { fwd!(file, xShmMap, pg, pgsz, extend, pp) }
}

/// Tracks the write transaction (WAL write lock) and checkpoints (checkpoint lock) of an attached
/// database; SQLite takes both through the main db handle.
unsafe extern "C" fn x_shm_lock(
    file: *mut ffi::sqlite3_file,
    offset: c_int,
    n: c_int,
    flags: c_int,
) -> c_int {
    unsafe {
        let rc = fwd!(file, xShmLock, offset, n, flags);
        if flags & ffi::SQLITE_SHM_EXCLUSIVE == 0
            || n != 1
            || (offset != WAL_WRITE_LOCK && offset != WAL_CKPT_LOCK)
        {
            return rc;
        }
        let Some(db) = main_db(file) else {
            return rc;
        };
        let mut db = lock(&db);
        let locking = flags & ffi::SQLITE_SHM_LOCK != 0;
        match (offset, locking) {
            (WAL_WRITE_LOCK, true) if rc == OK => {
                db.write_locked = true;
                db.committed = false;
                db.overlay.clear();
            }
            (WAL_WRITE_LOCK, false) => db.end_write_transaction(),
            (WAL_CKPT_LOCK, true) if rc == OK => db.checkpoint_started = Some(Instant::now()),
            (WAL_CKPT_LOCK, false) => {
                if let Some(t) = db.checkpoint_started.take()
                    && db.checkpoints.len() < 1_000_000
                {
                    db.checkpoints.push(t.elapsed());
                }
            }
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

// ---------------------------------------------------------------------------------------------
// Entry point

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_extension_init(
    db: *mut ffi::sqlite3,
    _err: *mut *mut c_char,
    p_api: *const ffi::sqlite3_api_routines,
) -> c_int {
    unsafe {
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
        type SqlFn =
            unsafe extern "C" fn(*mut ffi::sqlite3_context, c_int, *mut *mut ffi::sqlite3_value);
        let fns: [(&CStr, c_int, SqlFn); 3] = [
            (c"ursula_attach", 2, fn_attach),
            (c"ursula_status", 1, fn_status),
            (c"ursula_stats", 1, fn_stats),
        ];
        for (name, n, f) in fns {
            let rc = create(
                db,
                name.as_ptr(),
                n,
                ffi::SQLITE_UTF8,
                null_mut(),
                Some(f),
                None,
                None,
                None,
            );
            if rc != OK {
                return rc;
            }
        }
        ffi::SQLITE_OK_LOAD_PERMANENTLY
    }
}
