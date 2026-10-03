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
//! * Snapshots and retention (see [`snapshot`]): once the log since the latest snapshot exceeds the
//!   database size (and `URSULA_VFS_SNAPSHOT_MIN_BYTES`, default 8 MiB), a background thread per
//!   attached database checkpoints the local WAL through a private connection, pins the result with
//!   a read transaction (no commit may land in between; otherwise it tries again later), copies the
//!   db file's pages, publishes them at the stream offset they reflect, reads the snapshot back and
//!   only then advances the stream's retention to the *previous* snapshot's offset. Attach installs
//!   the latest snapshot when the local file is missing or behind it, then replays the tail.
//! * `SELECT ursula_status(path)` returns
//!   `{"offset","epoch","poisoned","fenced","reason","snapshot","retained"}`;
//!   `SELECT ursula_stats(path)` drains per-commit, per-checkpoint and per-snapshot numbers (bench).
//!
//! Test hook: `URSULA_VFS_ABORT_AFTER_ACK=<n>` aborts the process right after the n-th acknowledged
//! commit of an attachment, before the local WAL write. `URSULA_VFS_RETRY_MS` bounds the retries of
//! an append with an unknown outcome (default 30000). `URSULA_VFS_SNAPSHOT_MIN_BYTES` sets the
//! smallest log (bytes since the latest snapshot) that triggers a snapshot.
#![allow(non_snake_case, clippy::missing_safety_doc)]

pub mod frame;
pub mod snapshot;

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
use std::sync::Condvar;
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

/// Calls method `$m` of the underlying "unix" file.
macro_rules! fwd {
    ($f:expr, $m:ident $(, $a:expr)*) => {{
        let i = inner($f);
        ((*(*i).pMethods).$m.unwrap())(i $(, $a)*)
    }};
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
    path: String,
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
    /// WAL frame number (1-based) of the acknowledged commit frame: the transaction is published
    /// locally once the wal-index header's mxFrame reaches it.
    commit_frame_no: u32,
    /// Open WAL handles, and whether a main db handle holds an EXCLUSIVE file lock: together the
    /// closing connection's checkpoint, the one main-db write that takes no checkpoint shm lock.
    wal_handles: Vec<Handle>,
    /// The underlying "unix" files of the open main db handles: the db file is synced through
    /// them (see `sync_db_file`).
    db_handles: Vec<Handle>,
    exclusive: bool,
    acked: u64,
    fault_fired: bool,
    stats: Vec<CommitStat>,
    checkpoint_started: Option<Instant>,
    checkpoints: Vec<Duration>,
    /// Database size in pages at `offset`.
    pages: u32,
    /// Offset of the latest snapshot known readable (published and read back by this owner, or
    /// found at attach); 0 for none.
    snapshot: u64,
    /// Retention this owner advanced the stream to.
    retained: u64,
    snapper: Arc<Snapper>,
    /// A snapshot is pinning the state at `offset`: no commit is acknowledged until it closes.
    window: bool,
    snapshot_stats: Vec<SnapshotStat>,
}

struct SnapshotStat {
    offset: u64,
    bytes: usize,
    raw: usize,
    copy: Duration,
    total: Duration,
}

impl Db {
    /// The log since the latest snapshot outgrew the database (and the configured minimum).
    fn snapshot_due(&self) -> bool {
        let log = self.offset.saturating_sub(self.snapshot);
        log > (self.pages as u64 * PAGE as u64).max(snapshot_min_bytes())
    }

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
        self.committed = false;
        ffi::SQLITE_IOERR_WRITE
    }

    /// A local operation of an acknowledged transaction (the rest of its WAL writes, its sync,
    /// -shm growth) failed: SQLite rolls the transaction back locally, so the file no longer
    /// reflects the stream offset; poison it (re-attach replays the commit from the stream).
    fn post_ack(&mut self, what: &str, rc: c_int) -> c_int {
        if rc != OK && self.committed {
            self.poison(format!(
                "{what} failed ({rc}) after the commit was acknowledged"
            ));
        }
        rc
    }

    /// Test hook `URSULA_VFS_FAIL_POST_ACK=<n>`: fail the first local WAL write or sync after the
    /// n-th acknowledged commit.
    fn fault(&mut self) -> bool {
        if self.committed && !self.fault_fired && fail_post_ack() == Some(self.acked) {
            self.fault_fired = true;
            return true;
        }
        false
    }

    /// Whether a write to the main db file is a checkpoint: under the checkpoint lock, or the
    /// closing connection's checkpoint (EXCLUSIVE file lock with the WAL still open). Anything else
    /// (a rollback journal mode, journal_mode=MEMORY/OFF) would bypass replication.
    fn db_write_allowed(&self) -> bool {
        self.checkpoint_started.is_some() || (self.exclusive && !self.wal_handles.is_empty())
    }

    /// With synchronous=OFF a checkpoint does not sync the db file; sync it before the WAL frames
    /// it copied are overwritten (a WAL restart) or truncated away.
    unsafe fn sync_db_file(&self) -> Result<(), String> {
        unsafe { sync_handle(&self.db_handles, &self.path) }
    }

    /// The write transaction ended (WAL write lock released): drop what never committed; after an
    /// acknowledged commit, sync the local WAL and only then advance the sidecar.
    ///
    /// Runs before the real lock is released (see `x_shm_lock`), on the main db handle `file`,
    /// whose wal-index header tells whether SQLite published the commit.
    unsafe fn end_write_transaction(&mut self, file: *mut ffi::sqlite3_file) {
        self.write_locked = false;
        self.overlay.clear();
        if !std::mem::take(&mut self.committed) {
            return;
        }
        // A snapshot waiting for the acknowledged commit to be published (it runs once this
        // returns and the mutex is released, seeing the outcome).
        self.snapper.window_cv.notify_all();
        // wal-index header (first copy): mxFrame is the u32 at byte 16, native endian.
        let mut p: *mut c_void = null_mut();
        let rc = unsafe { fwd!(file, xShmMap, 0, 32 * 1024, 0, &mut p) };
        let mx_frame = if rc == OK && !p.is_null() {
            unsafe { std::ptr::read_volatile((p as *const u8).add(16) as *const u32) }
        } else {
            0
        };
        if mx_frame < self.commit_frame_no {
            self.poison(format!(
                "commit acknowledged but not published locally (mxFrame {mx_frame} < frame {})",
                self.commit_frame_no
            ));
            return;
        }
        if let Err(e) = unsafe { sync_handle(&self.wal_handles, &self.wal) } {
            self.poison(e);
            return;
        }
        if let Err(e) = write_sidecar(&self.sidecar, self.offset, self.epoch) {
            self.poison(e);
            return;
        }
        if self.snapshot_due() {
            self.snapper.request();
        }
    }
}

/// An underlying "unix" file of an open handle of an attached database (owned by SQLite; tracked
/// from `x_open` to `x_close`, and only used under the database's mutex).
#[derive(Clone, Copy, PartialEq, Eq)]
struct Handle(*mut ffi::sqlite3_file);

// SAFETY: the pointer is only dereferenced under the database's mutex while the handle is open,
// and a unix file may be synced from any thread.
unsafe impl Send for Handle {}

/// Syncs a file through one of SQLite's own open handles on it. Never through a descriptor of our
/// own: closing any descriptor of a file drops every POSIX (fcntl) lock this process holds on it,
/// SQLite's included, which would let another process take conflicting locks. fsync covers the
/// file, whichever descriptor issues it.
unsafe fn sync_handle(handles: &[Handle], what: &str) -> Result<(), String> {
    let Some(&Handle(f)) = handles.first() else {
        return Err(format!("sync {what}: no open handle"));
    };
    let rc = unsafe { ((*(*f).pMethods).xSync.unwrap())(f, ffi::SQLITE_SYNC_NORMAL) };
    if rc != OK {
        return Err(format!("sync {what}: {rc}"));
    }
    Ok(())
}

#[derive(Default)]
struct Registry {
    dbs: HashMap<String, Arc<Mutex<Db>>>,
    /// Open main-db handles per path (attached or not): attach requires none.
    open: HashMap<String, usize>,
    /// Host locks held for the process lifetime.
    locks: HashMap<String, fs::File>,
    /// Snapshot thread per attached database.
    snappers: HashMap<String, (Arc<Snapper>, std::thread::JoinHandle<()>)>,
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

fn snapshot_min_bytes() -> u64 {
    static V: OnceLock<u64> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_SNAPSHOT_MIN_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8 << 20)
    })
}

fn fail_post_ack() -> Option<u64> {
    static V: OnceLock<Option<u64>> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_FAIL_POST_ACK")
            .ok()
            .and_then(|v| v.parse().ok())
    })
}

/// Replaces the sidecar atomically and durably: temp file, fsync, rename, fsync of the directory.
fn write_sidecar(path: &str, offset: u64, epoch: u64) -> Result<(), String> {
    let err = |e: std::io::Error| format!("sidecar {path}: {e}");
    let tmp = format!("{path}.tmp");
    let mut f = fs::File::create(&tmp).map_err(err)?;
    std::io::Write::write_all(&mut f, format!("{offset} {epoch}\n").as_bytes()).map_err(err)?;
    f.sync_all().map_err(err)?;
    fs::rename(&tmp, path).map_err(err)?;
    sync_dir(path).map_err(err)
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
    /// 409 expecting sequence 0: the server expired this idle producer (7 days).
    ProducerExpired,
    /// A definite rejection, or no answer within the retry budget.
    Failed(String),
}

/// One idempotent append: retried with the same producer sequence until the outcome is known.
///
/// A duplicate answer (204) is taken as proof of *our* earlier attempt only for commits (seq >= 1):
/// they are sent after a verified claim (see `claim_once`), which makes this owner the only writer
/// at its epoch, so whatever holds (epoch, seq) is ours. A claim's answer is verified separately.
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
                    409 if seq > 0 && header_u64(&r, "producer-expected-seq") == Some(0) => {
                        return Append::ProducerExpired;
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

/// A failed stream operation: `Gone` when the data lies below the stream's retention (or a
/// snapshot was superseded), which a re-attach answers by installing the latest snapshot.
enum Fail {
    Gone(String),
    Other(String),
}

impl From<String> for Fail {
    fn from(e: String) -> Self {
        Fail::Other(e)
    }
}

impl From<Fail> for String {
    fn from(f: Fail) -> Self {
        match f {
            Fail::Gone(e) => format!("gone: {e}"),
            Fail::Other(e) => e,
        }
    }
}

/// One read from `offset`: the bytes and the offset after them (empty at the tail). Reads the
/// leader's applied state: a follower may lag behind an acknowledged append (a claim, a commit).
fn read_from(url: &str, offset: u64) -> Result<(Vec<u8>, u64), Fail> {
    let mut r = agent()
        .get(format!("{url}?offset={offset}&consistency=leader"))
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
    if status == 410 {
        return Err(Fail::Gone(format!(
            "read {url} at {offset}: below the stream's retention"
        )));
    }
    if status != 200 {
        return Err(Fail::Other(format!(
            "read {url} at {offset}: {status} {}",
            String::from_utf8_lossy(&body)
        )));
    }
    let next = next.unwrap_or(offset + body.len() as u64);
    if next != offset + body.len() as u64 {
        return Err(Fail::Other(format!(
            "read {url} at {offset}: {} bytes but next offset {next}",
            body.len()
        )));
    }
    Ok((body, next))
}

/// Snapshot transfers move whole databases: a longer timeout than appends.
fn bulk_agent() -> &'static ureq::Agent {
    static A: OnceLock<ureq::Agent> = OnceLock::new();
    A.get_or_init(|| {
        ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(600)))
            .http_status_as_error(false)
            .build()
            .into()
    })
}

struct Head {
    retained: u64,
    snapshot: Option<u64>,
}

fn head(url: &str) -> Result<Head, String> {
    let r = agent()
        .head(url)
        .call()
        .map_err(|e| format!("head {url}: {e}"))?;
    let status = r.status().as_u16();
    if status != 200 {
        return Err(format!("head {url}: {status}"));
    }
    Ok(Head {
        retained: header_u64(&r, "stream-retained-offset").unwrap_or(0),
        snapshot: header_u64(&r, "stream-snapshot-offset"),
    })
}

/// The snapshot at `offset`; `None` when it does not exist (superseded, or not yet visible here).
fn get_snapshot(url: &str, offset: u64) -> Result<Option<Vec<u8>>, String> {
    let mut r = bulk_agent()
        .get(format!("{url}/snapshot/{offset}"))
        .call()
        .map_err(|e| format!("get snapshot {offset}: {e}"))?;
    let status = r.status().as_u16();
    let body = r
        .body_mut()
        .with_config()
        .limit(2 << 30)
        .read_to_vec()
        .map_err(|e| format!("get snapshot {offset}: body: {e}"))?;
    match status {
        200 => Ok(Some(body)),
        404 | 410 => Ok(None),
        _ => Err(format!(
            "get snapshot {offset}: {status} {}",
            String::from_utf8_lossy(&body)
        )),
    }
}

/// `PUT` with retries while the outcome is unknown (transport errors, 5xx): both publishing a
/// snapshot and advancing retention are idempotent. Returns the status and response.
fn put_idempotent(
    url: &str,
    body: &[u8],
) -> Result<(u16, ureq::http::Response<ureq::Body>), String> {
    let deadline = Instant::now() + retry_budget();
    let mut backoff = Duration::from_millis(50);
    loop {
        let unknown = match bulk_agent()
            .put(url)
            .header("content-type", CONTENT_TYPE)
            .send(body)
        {
            Ok(r) if r.status().as_u16() < 500 => return Ok((r.status().as_u16(), r)),
            Ok(mut r) => format!(
                "{} {}",
                r.status(),
                r.body_mut().read_to_string().unwrap_or_default()
            ),
            Err(e) => e.to_string(),
        };
        if Instant::now() + backoff > deadline {
            return Err(format!("put {url}: {unknown}"));
        }
        std::thread::sleep(backoff);
        backoff = (backoff * 2).min(Duration::from_secs(1));
    }
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

/// A private connection on the "unix" VFS, outside this VFS's bookkeeping: recovery's checkpoint,
/// and the snapshot thread's checkpoint, read transaction and page copy.
struct Private {
    db: *mut ffi::sqlite3,
}

impl Private {
    unsafe fn open(path: &str) -> Result<Self, String> {
        unsafe {
            let c = CString::new(path).map_err(|e| e.to_string())?;
            let mut db: *mut ffi::sqlite3 = null_mut();
            let rc = (api().open_v2.unwrap())(
                c.as_ptr(),
                &mut db,
                ffi::SQLITE_OPEN_READWRITE,
                c"unix".as_ptr(),
            );
            let conn = Private { db };
            if rc != OK {
                return Err(format!("open {path}: {}", conn.errmsg()));
            }
            Ok(conn)
        }
    }

    unsafe fn errmsg(&self) -> String {
        if self.db.is_null() {
            return "out of memory".into();
        }
        unsafe {
            CStr::from_ptr((api().errmsg.unwrap())(self.db))
                .to_string_lossy()
                .into_owned()
        }
    }

    /// Runs `sql` and returns the integer columns of its first row (empty without a row).
    unsafe fn query(&self, sql: &CStr) -> Result<Vec<i64>, String> {
        unsafe {
            let a = api();
            let mut stmt: *mut ffi::sqlite3_stmt = null_mut();
            if (a.prepare_v2.unwrap())(self.db, sql.as_ptr(), -1, &mut stmt, null_mut()) != OK {
                return Err(format!("{sql:?}: {}", self.errmsg()));
            }
            let r = match (a.step.unwrap())(stmt) {
                ffi::SQLITE_ROW => Ok((0..(a.column_count.unwrap())(stmt))
                    .map(|i| (a.column_int64.unwrap())(stmt, i))
                    .collect()),
                ffi::SQLITE_DONE => Ok(Vec::new()),
                _ => Err(format!("{sql:?}: {}", self.errmsg())),
            };
            (a.finalize.unwrap())(stmt);
            r
        }
    }

    /// `PRAGMA wal_checkpoint(mode)`: whether every WAL frame is now in the db file.
    unsafe fn checkpoint(&self, sql: &CStr) -> Result<bool, String> {
        let row = unsafe { self.query(sql)? };
        match row[..] {
            [busy, log, done] => Ok(busy == 0 && log == done),
            _ => Err(format!("{sql:?}: no result row")),
        }
    }

    /// Pages `1..=n` of the db file, read through the connection's own file handle (closing a
    /// descriptor of our own would drop the process's POSIX locks on the file).
    unsafe fn read_pages(&self, n: u32) -> Result<Vec<u8>, String> {
        unsafe {
            let mut f: *mut ffi::sqlite3_file = null_mut();
            let rc = (api().file_control.unwrap())(
                self.db,
                c"main".as_ptr(),
                ffi::SQLITE_FCNTL_FILE_POINTER,
                &mut f as *mut *mut ffi::sqlite3_file as *mut c_void,
            );
            if rc != OK || f.is_null() || (*f).pMethods.is_null() {
                return Err(format!("db file handle: {rc}"));
            }
            const CHUNK: usize = 256 * PAGE;
            let mut image = vec![0u8; n as usize * PAGE];
            for (i, chunk) in image.chunks_mut(CHUNK).enumerate() {
                let rc = ((*(*f).pMethods).xRead.unwrap())(
                    f,
                    chunk.as_mut_ptr() as *mut c_void,
                    chunk.len() as c_int,
                    (i * CHUNK) as i64,
                );
                if rc != OK {
                    return Err(format!(
                        "read db file pages: {rc} (file shorter than {n} pages?)"
                    ));
                }
            }
            Ok(image)
        }
    }
}

impl Drop for Private {
    fn drop(&mut self) {
        unsafe { (api().close.unwrap())(self.db) };
    }
}

/// Recovery precondition and step: checkpoints (TRUNCATE) the local WAL into the db file through a
/// private "unix" connection and fails unless every frame was checkpointed and that connection was
/// the last one on the file (closing the last connection deletes the WAL; any other connection, in
/// any process, keeps it), so nothing holds an old WAL or page cache while pages are rewritten.
unsafe fn checkpoint_local(path: &str) -> Result<(), String> {
    let complete = unsafe { Private::open(path)?.checkpoint(c"PRAGMA wal_checkpoint(TRUNCATE)") }
        .map_err(|e| format!("checkpoint {path}: {e}"))?;
    if !complete {
        return Err(format!(
            "checkpoint {path}: incomplete; another connection is open"
        ));
    }
    if fs::metadata(format!("{path}-wal")).is_ok() {
        return Err(format!(
            "{path} is open by another connection (its WAL outlived the recovery connection)"
        ));
    }
    Ok(())
}

fn sync_dir(path: &str) -> std::io::Result<()> {
    let dir = std::path::Path::new(path)
        .parent()
        .unwrap_or(std::path::Path::new("."));
    fs::File::open(dir).and_then(|d| d.sync_all())
}

/// Applies records to the db file, deduplicating page writes per batch.
///
/// Runs only inside `ursula_attach`, which refuses while any connection of this process has the
/// file open and has stopped the previous attachment's snapshot thread; `checkpoint_local` proves
/// no other process holds it either. So its own descriptors on the db file (and the temp file and
/// rename of `install`) cannot drop anyone's POSIX locks when they close. Everywhere else the
/// extension touches the db file, -wal or -shm only through SQLite's handles (`sync_handle`, the
/// private connections of `Private`).
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
            Record::Claim { epoch, .. } => self.epoch = self.epoch.max(epoch),
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

    /// The db file, opened once; a file with content is recovered first (`checkpoint_local`).
    unsafe fn file(&mut self) -> Result<&fs::File, String> {
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
        Ok(self.file.as_ref().unwrap())
    }

    /// Writes the batch: truncate to its smallest size, write the final page images, set the size.
    unsafe fn flush(&mut self) -> Result<(), String> {
        let (Some(min), Some(size)) = (self.min_size.take(), self.size) else {
            return Ok(());
        };
        let pages = std::mem::take(&mut self.pages);
        let f = unsafe { self.file()? };
        let len = f.metadata().map_err(|e| e.to_string())?.len();
        if len > min as u64 * PAGE as u64 {
            f.set_len(min as u64 * PAGE as u64)
                .map_err(|e| e.to_string())?;
        }
        for (pgno, data) in pages {
            f.write_all_at(&data, (pgno as u64 - 1) * PAGE as u64)
                .map_err(|e| e.to_string())?;
        }
        f.set_len(size as u64 * PAGE as u64)
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Replaces the db file with a snapshot's image (temp file, fsync, rename, fsync of the
    /// directory) and continues the batch from it. A crash before the sidecar records the new
    /// offset leaves the old one, from which a re-attach installs the snapshot again.
    unsafe fn install(&mut self, snap: snapshot::Snapshot) -> Result<(), String> {
        unsafe { self.file()? };
        self.file = None;
        let err = |e: std::io::Error| format!("install snapshot into {}: {e}", self.path);
        let tmp = format!("{}-ursula.snap", self.path);
        let mut f = fs::File::create(&tmp).map_err(err)?;
        std::io::Write::write_all(&mut f, &snap.image).map_err(err)?;
        f.sync_all().map_err(err)?;
        drop(f);
        fs::rename(&tmp, &self.path).map_err(err)?;
        sync_dir(&self.path).map_err(err)?;
        let f = OpenOptions::new().read(true).write(true).open(&self.path);
        self.file = Some(f.map_err(err)?);
        self.pages.clear();
        self.min_size = None;
        self.size = Some((snap.image.len() / PAGE) as u32);
        self.epoch = self.epoch.max(snap.epoch);
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

/// Reads and applies frames from `pos` until the tail (`until == None`) or `until`, advancing `pos`
/// past every applied frame.
unsafe fn catch_up(
    url: &str,
    pos: &mut u64,
    until: Option<u64>,
    applier: &mut Applier,
) -> Result<(), Fail> {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        if until.is_some_and(|u| *pos >= u) {
            break;
        }
        let (bytes, _) = read_from(url, *pos + buf.len() as u64)?;
        if bytes.is_empty() {
            if !buf.is_empty() || until.is_some() {
                return Err(Fail::Other(format!(
                    "stream {url} ends at {} inside a frame or before {until:?}",
                    *pos + buf.len() as u64
                )));
            }
            break;
        }
        buf.extend_from_slice(&bytes);
        let mut used = 0;
        while let Decoded::Frame { record, len } = frame::decode(&buf[used..])
            .map_err(|e| format!("{url} at {}: {e}", *pos + used as u64))?
        {
            applier.apply(record);
            used += len;
        }
        buf.drain(..used);
        *pos += used as u64;
        unsafe { applier.flush()? };
    }
    Ok(())
}

fn nonce() -> Result<[u8; 16], String> {
    let mut n = [0u8; 16];
    fs::File::open("/dev/urandom")
        .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut n))
        .map_err(|e| format!("/dev/urandom: {e}"))?;
    Ok(n)
}

fn first_claim_epoch() -> Option<u64> {
    static V: OnceLock<Option<u64>> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_FIRST_CLAIM_EPOCH")
            .ok()
            .and_then(|v| v.parse().ok())
    })
}

enum Claimed {
    /// Our claim spans `start..next`: this owner alone writes at `epoch` from here on.
    Won {
        start: u64,
        next: u64,
    },
    /// Another owner's claim (or anything else) holds the answered position: a concurrent
    /// claim at the same epoch was answered as a duplicate of theirs.
    Lost,
    Fenced(Option<u64>),
}

/// Appends a claim at (`epoch`, seq 0) and verifies that the frame ending at the answered offset
/// is ours. A 2xx alone proves nothing: two owners claiming the same epoch both get one, the
/// second as a duplicate of the first's receipt. The nonce makes our claim's bytes unique.
fn claim_once(url: &str, epoch: u64) -> Result<Claimed, String> {
    let frame = frame::encode_claim(epoch, &nonce()?);
    match append(url, &frame, epoch, 0) {
        Append::Acked {
            next: Some(next), ..
        } => {
            let start = next.checked_sub(frame.len() as u64);
            let ours = match start {
                Some(start) => read_from(url, start)?.0.get(..frame.len()) == Some(&frame[..]),
                None => false,
            };
            Ok(match start {
                Some(start) if ours => Claimed::Won { start, next },
                _ => Claimed::Lost,
            })
        }
        Append::Acked { next: None, .. } => Err(format!("claim {url}: no Stream-Next-Offset")),
        Append::Fenced { current } => Ok(Claimed::Fenced(current)),
        Append::ProducerExpired => unreachable!("a claim has sequence 0"),
        Append::Failed(e) => Err(format!("claim {url}: {e}")),
    }
}

/// Claims the stream with an epoch above every earlier owner's; returns it and the claim's end.
fn claim(url: &str, epoch: u64) -> Result<(u64, u64), String> {
    // Test hook URSULA_VFS_FIRST_CLAIM_EPOCH: the process's first claim uses this epoch.
    static HOOKED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    let mut epoch = match first_claim_epoch() {
        Some(e) if !HOOKED.swap(true, Ordering::Relaxed) => e,
        _ => epoch,
    };
    for _ in 0..16 {
        match claim_once(url, epoch)? {
            Claimed::Won { next, .. } => return Ok((epoch, next)),
            Claimed::Lost => epoch += 1,
            Claimed::Fenced(current) => epoch = current.unwrap_or(epoch).max(epoch) + 1,
        }
    }
    Err(format!("claim {url}: lost 16 claim races"))
}

/// The server expired this owner's idle producer (7 days without a write) and forgot its epoch.
/// Taking the stream back is safe only if nobody wrote since this owner's last frame: the stream
/// must end at our offset, and our new claim (one epoch up, fencing any later owner's older
/// epochs) must be ours (verified) and land exactly there. Otherwise another owner wrote or
/// claimed, and this one is fenced.
fn reclaim(db: &mut Db) -> Result<(), String> {
    let (bytes, _) = read_from(&db.url, db.offset)?;
    if !bytes.is_empty() {
        db.fenced = true;
        return Err(format!(
            "fenced: producer expired and the stream moved past {}",
            db.offset
        ));
    }
    let epoch = db.epoch + 1;
    match claim_once(&db.url, epoch)? {
        Claimed::Won { start, next } if start == db.offset => {
            db.epoch = epoch;
            db.seq = 0;
            db.offset = next;
            Ok(())
        }
        _ => {
            db.fenced = true;
            Err("fenced: another owner wrote or claimed while re-claiming".into())
        }
    }
}

/// Gives an empty file its WAL-format page 1 through a private "unix" connection, so no
/// connection ever commits through a rollback journal (the main-db write guard refuses that).
unsafe fn init_wal_format(path: &str) -> Result<(), String> {
    unsafe {
        let a = api();
        let c = CString::new(path).unwrap();
        let mut db: *mut ffi::sqlite3 = null_mut();
        let flags = ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE;
        let mut rc = (a.open_v2.unwrap())(c.as_ptr(), &mut db, flags, c"unix".as_ptr());
        if rc == OK {
            rc = (a.exec.unwrap())(
                db,
                c"PRAGMA journal_mode=WAL".as_ptr(),
                None,
                null_mut(),
                null_mut(),
            );
        }
        (a.close.unwrap())(db);
        if rc != OK {
            return Err(format!("journal_mode=WAL on {path}: {rc}"));
        }
    }
    Ok(())
}

/// Brings the db file from `pos` to the stream's tail and claims the stream: installs the latest
/// snapshot when the file is behind it (or below the stream's retention), replays the frames after
/// it, claims, and replays up to the claim. Returns the epoch claimed and the latest snapshot's
/// offset (0 for none).
unsafe fn sync(url: &str, pos: &mut u64, applier: &mut Applier) -> Result<(u64, u64), Fail> {
    let head = head(url)?;
    if let Some(s) = head.snapshot
        && *pos < s
    {
        let Some(body) = get_snapshot(url, s)? else {
            return Err(Fail::Gone(format!("snapshot {s} superseded")));
        };
        let snap = snapshot::decode(&body)?;
        if snap.offset != s {
            return Err(Fail::Other(format!(
                "snapshot at {s} reflects offset {}",
                snap.offset
            )));
        }
        unsafe { applier.install(snap)? };
        *pos = s;
    } else if *pos < head.retained {
        return Err(Fail::Gone(format!(
            "{pos} is below the retention {} and no newer snapshot is visible",
            head.retained
        )));
    }
    unsafe { catch_up(url, pos, None, applier)? };
    let (epoch, claimed) = claim(url, applier.epoch + 1)?;
    unsafe { catch_up(url, pos, Some(claimed), applier)? };
    Ok((epoch, head.snapshot.unwrap_or(0)))
}

unsafe fn attach(path: &str, url: &str) -> Result<u64, String> {
    let path = unsafe { full_pathname(path)? };
    let url = url.trim_end_matches('/').to_owned();
    let previous = {
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
        reg.snappers.remove(&path)
    };
    // The previous attachment's snapshot thread may hold a private connection on the file.
    if let Some((snapper, thread)) = previous {
        snapper.stop();
        let _ = thread.join();
    }
    create_stream(&url)?;
    let sidecar = format!("{path}-ursula");
    let db_len = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    let (from, epoch) = if db_len == 0 {
        // Nothing local: a WAL next to an empty db file holds nothing committed. The sidecar is
        // written before anything lands in the file, so an attach that fails halfway leaves a
        // file the next one resumes (replaying page images from an older offset is idempotent).
        let _ = fs::remove_file(format!("{path}-wal"));
        let _ = fs::remove_file(format!("{path}-shm"));
        write_sidecar(&sidecar, 0, 0)?;
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
    let mut pos = from;
    let mut tries = 0;
    // `Gone`: retention moved past the file (or the snapshot read was superseded) under a HEAD
    // that did not show it yet; the next round installs the newer snapshot.
    let (epoch, snapshot) = loop {
        match unsafe { sync(&url, &mut pos, &mut applier) } {
            Ok(r) => break r,
            Err(Fail::Gone(e)) if tries < 10 => {
                tries += 1;
                eprintln!("sqlite-ursula-vfs: {url}: attach: {e}; retrying");
                std::thread::sleep(Duration::from_millis(50 * tries));
            }
            Err(e) => return Err(e.into()),
        }
    };
    let offset = pos;
    applier.finish()?;
    if fs::metadata(&path).map(|m| m.len()).unwrap_or(0) == 0 {
        unsafe { init_wal_format(&path)? };
    }
    write_sidecar(&sidecar, offset, epoch)?;
    let pages = (fs::metadata(&path).map(|m| m.len()).unwrap_or(0) / PAGE as u64) as u32;
    let snapper = Arc::new(Snapper::default());
    let db = Arc::new(Mutex::new(Db {
        url,
        sidecar,
        path: path.clone(),
        wal: format!("{path}-wal"),
        epoch,
        seq: 0,
        offset,
        poisoned: None,
        fenced: false,
        write_locked: false,
        overlay: BTreeMap::new(),
        committed: false,
        commit_frame_no: 0,
        wal_handles: Vec::new(),
        db_handles: Vec::new(),
        exclusive: false,
        acked: 0,
        fault_fired: false,
        stats: Vec::new(),
        checkpoint_started: None,
        checkpoints: Vec::new(),
        pages,
        snapshot,
        retained: 0,
        snapper: snapper.clone(),
        window: false,
        snapshot_stats: Vec::new(),
    }));
    let thread = {
        let (db, snapper) = (db.clone(), snapper.clone());
        std::thread::Builder::new()
            .name("ursula-snapshot".into())
            .spawn(move || snapshot_loop(&db, &snapper))
            .map_err(|e| format!("spawn the snapshot thread: {e}"))?
    };
    let mut reg = registry();
    reg.dbs.insert(path.clone(), db);
    reg.snappers.insert(path, (snapper, thread));
    Ok(offset)
}

// ---------------------------------------------------------------------------------------------
// Snapshots

/// Wakes an attached database's snapshot thread.
#[derive(Default)]
struct Snapper {
    /// (requested, stopped)
    state: Mutex<(bool, bool)>,
    cv: Condvar,
    /// With the database's mutex: a commit waits for the snapshot window to close, the snapshot
    /// for an acknowledged commit to be published.
    window_cv: Condvar,
}

/// Closes the snapshot window when dropped.
struct Window<'a>(&'a Mutex<Db>);

impl Drop for Window<'_> {
    fn drop(&mut self) {
        let mut d = lock(self.0);
        d.window = false;
        d.snapper.window_cv.notify_all();
    }
}

impl Snapper {
    fn request(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).0 = true;
        self.cv.notify_one();
    }

    fn stop(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).1 = true;
        self.cv.notify_one();
    }

    /// Waits for a request (true) or the stop (false).
    fn wait(&self) -> bool {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if s.1 {
                return false;
            }
            if std::mem::take(&mut s.0) {
                return true;
            }
            s = self.cv.wait(s).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Sleeps for `d` unless stopped first (false).
    fn pause(&self, d: Duration) -> bool {
        let s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let (s, _) = self
            .cv
            .wait_timeout_while(s, d, |s| !s.1)
            .unwrap_or_else(|e| e.into_inner());
        !s.1
    }
}

fn snapshot_loop(db: &Mutex<Db>, snapper: &Snapper) {
    let mut backoff = Duration::from_millis(100);
    while snapper.wait() {
        let outcome = unsafe { snapshot_once(db) };
        if let Ok(true) = outcome {
            backoff = Duration::from_millis(100);
            continue;
        }
        if let Err(e) = outcome {
            eprintln!(
                "sqlite-ursula-vfs: {}: snapshot: {e}; retrying later",
                lock(db).url
            );
        }
        if !snapper.pause(backoff) {
            return;
        }
        backoff = (backoff * 2).min(Duration::from_secs(30));
        if lock(db).snapshot_due() {
            snapper.request();
        }
    }
}

/// One snapshot attempt (see the crate docs). `Ok(false)`: not possible right now (a reader pins
/// WAL frames the checkpoint needs), try again later.
///
/// The image is exactly the stream's state at `offset`. The window opens once every acknowledged
/// commit is published locally (`!committed`) and keeps any further commit from being
/// acknowledged (it waits at its commit point) until the read transaction has started, so the
/// checkpoint moves every WAL frame up to `offset` into the db file and the read transaction sees
/// the db file alone at `offset`. While the read transaction lasts no checkpoint can write newer
/// frames into the db file (a reader at mark 0 blocks backfill, one at a later mark caps it) and no
/// closing connection can checkpoint (that needs an EXCLUSIVE lock), so the pages copied are those
/// of `offset`. Commits wait only for the checkpoint and the start of the read transaction.
unsafe fn snapshot_once(db: &Mutex<Db>) -> Result<bool, String> {
    let started = Instant::now();
    let path = lock(db).path.clone();
    let conn = unsafe { Private::open(&path)? };
    let (url, offset, epoch, pages) = {
        let mut d = lock(db);
        loop {
            if !d.snapshot_due() || d.poisoned.is_some() {
                return Ok(true);
            }
            if !d.committed {
                break;
            }
            let snapper = d.snapper.clone();
            d = snapper
                .window_cv
                .wait_timeout(d, Duration::from_millis(100))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        d.window = true;
        (d.url.clone(), d.offset, d.epoch, d.pages)
    };
    let window = Window(db);
    if !unsafe { conn.checkpoint(c"PRAGMA wal_checkpoint(PASSIVE)")? } {
        return Ok(false);
    }
    unsafe {
        conn.query(c"BEGIN")?;
        conn.query(c"SELECT count(*) FROM sqlite_schema")?;
    }
    drop(window);
    let copy_started = Instant::now();
    let image = unsafe { conn.read_pages(pages)? };
    drop(conn); // ends the read transaction
    let copy = copy_started.elapsed();
    let body = snapshot::encode(offset, epoch, &image);
    match put_idempotent(&format!("{url}/snapshot/{offset}"), &body)? {
        (200..=299, _) => {}
        (409 | 410, _) => {
            // A newer snapshot exists (another owner's, or this file's before a re-attach).
            let newer = head(&url)?.snapshot.unwrap_or(0);
            let mut d = lock(db);
            d.snapshot = d.snapshot.max(newer);
            return Ok(true);
        }
        (status, mut r) => {
            return Err(format!(
                "publish at {offset}: {status} {}",
                r.body_mut().read_to_string().unwrap_or_default()
            ));
        }
    }
    // Nothing relies on the snapshot before it reads back intact (a follower may not show it yet).
    let mut verified = false;
    for i in 1..=20 {
        if get_snapshot(&url, offset)?.as_deref() == Some(&body[..]) {
            verified = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(25 * i));
    }
    if !verified {
        return Err(format!("the snapshot at {offset} does not read back"));
    }
    let (previous, retained) = {
        let mut d = lock(db);
        let previous = d.snapshot;
        d.snapshot = d.snapshot.max(offset);
        if d.snapshot_stats.len() < 100_000 {
            d.snapshot_stats.push(SnapshotStat {
                offset,
                bytes: body.len(),
                raw: image.len(),
                copy,
                total: started.elapsed(),
            });
        }
        (previous, d.retained)
    };
    // Retention trails one snapshot behind: a host that read the previous snapshot (or whose file
    // is past it) still finds the frames after it, and the newer snapshot has read back.
    if previous > retained && previous < offset {
        match put_idempotent(&format!("{url}/retention/{previous}"), &[])? {
            (200..=299, r) => {
                let effective = header_u64(&r, "stream-retained-offset").unwrap_or(previous);
                let mut d = lock(db);
                d.retained = d.retained.max(effective);
            }
            // Already past it (another owner, or this file before a re-attach).
            (409 | 410, _) => {}
            (status, mut r) => {
                return Err(format!(
                    "retention to {previous}: {status} {}",
                    r.body_mut().read_to_string().unwrap_or_default()
                ));
            }
        }
    }
    Ok(true)
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
        "{{\"offset\":{},\"epoch\":{},\"poisoned\":{},\"fenced\":{},\"reason\":{},\"snapshot\":{},\"retained\":{}}}",
        db.offset,
        db.epoch,
        db.poisoned.is_some(),
        db.fenced,
        db.poisoned.as_deref().map_or("null".to_owned(), json_str),
        db.snapshot,
        db.retained
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
    out.push_str("],\"snapshots\":[");
    for (i, s) in db.snapshot_stats.drain(..).enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(
            out,
            "{{\"offset\":{},\"bytes\":{},\"raw\":{},\"copy_us\":{},\"total_us\":{}}}",
            s.offset,
            s.bytes,
            s.raw,
            s.copy.as_micros(),
            s.total.as_micros()
        );
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
    /// This main db handle holds an EXCLUSIVE file lock.
    exclusive: bool,
}

unsafe fn inner(f: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
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
                drop(reg);
                if let Some(db) = &db {
                    lock(db).db_handles.push(Handle(inner(file)));
                }
                (*f).ext = Box::into_raw(Box::new(Ext {
                    path: Some(name.to_owned()),
                    db,
                    wal: false,
                    exclusive: false,
                }));
            } else if flags & ffi::SQLITE_OPEN_WAL != 0
                && let Some(db) = name.strip_suffix("-wal").and_then(lookup)
            {
                lock(&db).wal_handles.push(Handle(inner(file)));
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
        // Untracked before the underlying file closes: nothing syncs through it afterwards.
        let f = file as *mut File;
        let ext = if (*f).ext.is_null() {
            None
        } else {
            Some(Box::from_raw(std::mem::replace(&mut (*f).ext, null_mut())))
        };
        if let Some(db) = ext.as_ref().and_then(|e| e.db.as_ref()) {
            let mut db = lock(db);
            let h = Handle(inner(file));
            if ext.as_ref().is_some_and(|e| e.wal) {
                db.wal_handles.retain(|&x| x != h);
            } else {
                db.db_handles.retain(|&x| x != h);
                if ext.as_ref().is_some_and(|e| e.exclusive) {
                    db.exclusive = false;
                }
            }
        }
        let rc = fwd!(file, xClose);
        if let Some(path) = ext.and_then(|e| e.path) {
            let mut reg = registry();
            if let Some(n) = reg.open.get_mut(&path) {
                *n -= 1;
                if *n == 0 {
                    reg.open.remove(&path);
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
            let rc = if db.fault() {
                ffi::SQLITE_IOERR_WRITE
            } else {
                fwd!(file, xWrite, buf, amt, off)
            };
            return db.post_ack("local WAL write", rc);
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
                let snapper = db.snapper.clone();
                while db.window {
                    db = snapper
                        .window_cv
                        .wait(db)
                        .unwrap_or_else(|e| e.into_inner());
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
    let t = Instant::now();
    let mut outcome = append(&db.url, &body, db.epoch, db.seq + 1);
    if let Append::ProducerExpired = outcome {
        if let Err(e) = reclaim(db) {
            return db.poison(e);
        }
        outcome = append(&db.url, &body, db.epoch, db.seq + 1);
    }
    let seq = db.seq + 1;
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
        Append::ProducerExpired => {
            return db.poison("producer expired again right after a re-claim".into());
        }
        Append::Failed(e) => return db.poison(e),
    };
    db.seq = seq;
    db.offset = expected;
    db.pages = size;
    db.acked += 1;
    if abort_after_ack() == Some(db.acked) {
        eprintln!(
            "sqlite-ursula-vfs: URSULA_VFS_ABORT_AFTER_ACK={}: aborting after the ack",
            db.acked
        );
        std::process::abort();
    }
    // A WAL header write restarts the WAL over frames a checkpoint copied into the db file.
    if db.overlay.contains_key(&0)
        && let Err(e) = unsafe { db.sync_db_file() }
    {
        return db.poison(e);
    }
    db.commit_frame_no = ((commit_frame - WAL_HDR) / FRAME + 1) as u32;
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
        db.overlay.retain(|&o, _| o < size);
        // Truncating drops frames a checkpoint copied into the db file (see `sync_db_file`).
        if let Err(e) = unsafe { db.sync_db_file() } {
            return db.poison(e);
        }
        let rc = fwd!(file, xTruncate, size);
        db.post_ack("local WAL truncate", rc)
    }
}

unsafe extern "C" fn x_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    unsafe {
        let Some(db) = wal_db(file) else {
            return fwd!(file, xSync, flags);
        };
        let mut db = lock(&db);
        let rc = if db.fault() {
            ffi::SQLITE_IOERR_FSYNC
        } else {
            fwd!(file, xSync, flags)
        };
        db.post_ack("local WAL sync", rc)
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
            lock(db).exclusive = exclusive;
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
/// database; SQLite takes both through the main db handle. The end of a write transaction (sync,
/// publication check, sidecar) runs under the database's mutex *before* the real lock is released:
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
        if offset == WAL_WRITE_LOCK && !locking {
            let mut db = lock(&db);
            db.end_write_transaction(file);
            return fwd!(file, xShmLock, offset, n, flags);
        }
        let rc = fwd!(file, xShmLock, offset, n, flags);
        let mut db = lock(&db);
        match (offset, locking) {
            (WAL_WRITE_LOCK, true) if rc == OK => {
                db.write_locked = true;
                db.committed = false;
                db.overlay.clear();
            }
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
