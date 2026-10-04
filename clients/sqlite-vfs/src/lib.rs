//! SQLite loadable extension: a shim VFS ("ursula", registered as the default) over "unix" that
//! replicates every WAL commit of an attached database to an Ursula stream *before* any of the
//! transaction's frames reach the local `-wal` file. The stream is the source of truth; the local
//! db file and WAL are a cache of it.
//!
//! * `SELECT ursula_attach(path, stream_url)` takes the host lock `<db>-ursula.lock` (held for the
//!   process lifetime), catches the file up from the stream, claims the stream for this owner (a
//!   new producer epoch, which fences every earlier owner), catches up to the claim and attaches the
//!   file. It requires that no connection to the file is open. Returns the stream offset applied.
//!   Offsets are the server's strings (`Stream-Next-Offset`), opaque: kept as they came, compared
//!   only lexicographically (the protocol orders them that way), never computed; `-1` is the
//!   protocol's "beginning of the stream", and stands for "none" where an offset may be absent.
//!   A db file with a sidecar (`<db>-ursula`: it is the cache of a stream) opens only while an
//!   attach of it has succeeded in this process: before that, after a failed attach, or in a
//!   process that never attached it, opening it fails (SQLITE_CANTOPEN), so it is never written
//!   unreplicated. A file without a sidecar passes through to "unix".
//! * Stream: `application/octet-stream`, one self-delimiting frame per append (see [`frame`]):
//!   a commit carries the db size after it and the transaction's final page images; a claim carries
//!   the owner's producer epoch. Appends use the idempotent producer (`Producer-Id`
//!   `sqlite-ursula-vfs/<Stream-Incarnation>` per stream incarnation, `Producer-Epoch` per owner,
//!   `Producer-Seq` per append; commits also carry `Stream-Seq`, see `stream_seq`): an append
//!   whose outcome is unknown is retried with the same sequence until the server answers (a
//!   duplicate is acknowledged without being applied twice);
//!   403 means another owner claimed the stream. An owner of a deleted stream is an unknown
//!   producer in the stream recreated at its path (unless an attach racing the recreate claimed
//!   there under the old id; see the design doc, §6): its next append is answered as an expired
//!   producer's, and the re-claim that follows finds the incarnation changed and fences it.
//! * WAL writes of an attached database go to a per-database overlay (reads and the file size see
//!   it). The write of the commit frame's page data (the frame whose header carries a non-zero
//!   "db size after commit") is the commit point: the transaction's final page images become one
//!   frame, appended. On an ack the overlay is written to the local WAL and every later write of
//!   that transaction (checksum rewrites, padding) goes straight to it; when the write transaction
//!   ends (the WAL write lock is released) the sidecar `<db>-ursula` (stream offset and epoch
//!   reflected locally) advances. On a rejection or an exhausted retry budget the overlay is
//!   dropped, the write fails with SQLITE_IOERR_WRITE (SQLite rolls the transaction back) and the
//!   database is poisoned until it is re-attached. The overlay belongs to the write transaction: it
//!   is cleared whenever the WAL write lock is taken or released, so a rolled-back transaction's
//!   spilled frames never shadow a later one's.
//! * Durability is the stream's acknowledgement alone: the local files (db, -wal, -shm, sidecar)
//!   are a cache, not fsynced on the commit path (`xSync` of an attached database is a no-op,
//!   whatever `PRAGMA synchronous` says). The sidecar records the kernel's boot id, the stream's
//!   incarnation, the db file's inode and what it needs of the local WAL (`WalClaim`); attach
//!   trusts the local files only when all four check out (written since this boot, from this
//!   incarnation of the stream, into this file, and the WAL still holds the claimed frames) and
//!   otherwise discards them and rebuilds from the latest snapshot and the tail (from a stream
//!   deleted and recreated at the path, whatever its length). The db file alone is fsynced,
//!   rarely, so a crash-consistent image of the files (a disk snapshot) is either
//!   verifiably complete or rejected: before the WAL starts a new generation or is truncated to
//!   nothing, and at attach before a sidecar that relies on it. Attach never opens local files
//!   through SQLite before replaying onto them: it folds the WAL into the db file itself.
//! * Snapshots and retention (see [`snapshot`]): once the log since the latest snapshot (frame
//!   bytes counted locally: replayed at attach, appended since, carried in the sidecar) exceeds the
//!   database size (and `URSULA_VFS_SNAPSHOT_MIN_BYTES`, default 8 MiB), a background thread per
//!   attached database checkpoints the local WAL through a private connection, pins the result with
//!   a read transaction (no commit may land in between; otherwise it tries again later), copies the
//!   db file's pages, publishes them at the stream offset they reflect, reads the snapshot back and
//!   only then advances the stream's retention to the *previous* snapshot's offset. Attach installs
//!   the latest snapshot when the local file is missing or behind it, then replays the tail.
//! * `SELECT ursula_status(path)` returns
//!   `{"offset","epoch","poisoned","fenced","reason","snapshot","retained","local","installed"}`
//!   (offsets as strings; `local`: the stream offset of the local state attach started from, `-1`
//!   when it rebuilt the file; `installed`: the offset of the snapshot attach installed, `-1` for
//!   none; `snapshot`, `retained`: `-1` for none);
//!   `SELECT ursula_stats(path)` drains per-commit, per-checkpoint and per-snapshot numbers (bench).
//!
//! Test hook: `URSULA_VFS_ABORT_AFTER_ACK=<n>` aborts the process right after the n-th acknowledged
//! commit of an attachment, before the local WAL write. `URSULA_VFS_RETRY_MS` bounds every retry
//! loop: appends with an unknown outcome, idempotent PUTs, and reads answered 429/503 (default
//! 30000). `URSULA_VFS_SNAPSHOT_MIN_BYTES` sets the smallest log (bytes since the latest snapshot)
//! that triggers a snapshot.
#![allow(non_snake_case, clippy::missing_safety_doc)]

pub mod frame;
pub mod snapshot;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
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
/// The longest a commit waits at its commit point for a due snapshot to open its window.
const WINDOW_WAIT: Duration = Duration::from_secs(1);
/// `Producer-Id` prefix; the stream incarnation follows (`producer_id`).
const PRODUCER: &str = "sqlite-ursula-vfs";
const CONTENT_TYPE: &str = "application/octet-stream";
/// The protocol's offset for the beginning of a stream, and "none" for an offset that may be absent
/// (a snapshot, retention, the local state): it sorts before every offset the server mints.
const START: &str = "-1";
/// The sidecar format (`sidecar_line`): 2 records offsets as the server's strings.
const SIDECAR_VERSION: u32 = 2;

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
    /// The stream's incarnation at attach (`Head::incarnation`): a re-claim or a snapshot checks
    /// the stream is still it.
    incarnation: String,
    /// `producer_id(incarnation)`.
    producer: String,
    sidecar: String,
    /// What the sidecar records besides offset, epoch, format, log count and the WAL claim (see
    /// `stamp`).
    stamp: String,
    path: String,
    epoch: u64,
    /// Producer sequence of the last acknowledged append (the claim is 0).
    seq: u64,
    /// Stream offset after the last acknowledged frame.
    offset: String,
    /// Frame bytes since the latest snapshot, counted locally (see `snapshot_due`).
    log: u64,
    poisoned: Option<String>,
    fenced: bool,
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
    /// Open WAL handles, and the main db handle holding an EXCLUSIVE file lock (0: none):
    /// together the closing connection's checkpoint, the one main-db write that takes no
    /// checkpoint shm lock.
    wal_open: usize,
    exclusive: usize,
    /// The main db handle holding the WAL write lock (0: none): the write transaction in progress
    /// (between taking and releasing that lock); see `sync_db`.
    writer: usize,
    acked: u64,
    fault_fired: bool,
    stats: Vec<CommitStat>,
    checkpoint_started: Option<Instant>,
    checkpoints: Vec<Duration>,
    /// Database size in pages at `offset`.
    pages: u32,
    /// Offset of the latest snapshot known readable (published and read back by this owner, or
    /// found at attach); `START` for none.
    snapshot: String,
    /// Retention this owner advanced the stream to (`START`: none).
    retained: String,
    snapper: Arc<Snapper>,
    /// A snapshot is pinning the state at `offset`: no commit is acknowledged until it closes.
    window: bool,
    /// A due snapshot is waiting to open its window: the next commit waits at its commit point
    /// (up to `WINDOW_WAIT`) until it has, so a writer committing back to back cannot starve it.
    window_wanted: bool,
    snapshot_stats: Vec<SnapshotStat>,
    /// Stream offset of the local state attach started from (`START`: rebuilt from nothing), and of
    /// the snapshot it installed (`START`: none).
    attached_from: String,
    installed: String,
}

struct SnapshotStat {
    offset: String,
    bytes: usize,
    raw: usize,
    copy: Duration,
    total: Duration,
}

impl Db {
    /// The log since the latest snapshot outgrew the database (and the configured minimum). The log
    /// is counted in frame bytes, not taken from offsets (opaque): what attach replayed after the
    /// snapshot it started from (plus, from trusted local files, the sidecar's count), and every
    /// frame acknowledged since; a snapshot taken subtracts what it covers.
    fn snapshot_due(&self) -> bool {
        self.log > (self.pages as u64 * PAGE as u64).max(snapshot_min_bytes())
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

    /// A local operation of an acknowledged transaction (the rest of its WAL writes, -shm growth)
    /// failed: SQLite rolls the transaction back locally, so the file no longer
    /// reflects the stream offset; poison it (re-attach replays the commit from the stream).
    fn post_ack(&mut self, what: &str, rc: c_int) -> c_int {
        if rc != OK && self.committed {
            self.poison(format!(
                "{what} failed ({rc}) after the commit was acknowledged"
            ));
        }
        rc
    }

    /// Test hook `URSULA_VFS_FAIL_POST_ACK=<n>`: fail the local WAL write of the n-th acknowledged
    /// commit.
    fn fault(&mut self) -> bool {
        if !self.fault_fired && fail_post_ack() == Some(self.acked) {
            self.fault_fired = true;
            return true;
        }
        false
    }

    /// Whether a write to the main db file is a checkpoint: under the checkpoint lock, or the
    /// closing connection's checkpoint (EXCLUSIVE file lock with the WAL still open). Anything else
    /// (a rollback journal mode, journal_mode=MEMORY/OFF) would bypass replication.
    fn db_write_allowed(&self) -> bool {
        self.checkpoint_started.is_some() || (self.exclusive != 0 && self.wal_open > 0)
    }

    /// Fsyncs the db file through the main db handle of the connection asking (the one holding the
    /// WAL write lock, or a closing one's EXCLUSIVE lock): before the local WAL starts a new
    /// generation or is truncated to nothing, so a disk image that shows the new WAL holds every
    /// page checkpointed from the old one (see `WalClaim`). Rare: once per WAL wrap. A failure
    /// poisons.
    unsafe fn sync_db(&mut self, before: &str) -> c_int {
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

#[derive(Default)]
struct Registry {
    dbs: HashMap<String, Arc<Mutex<Db>>>,
    /// Open main-db handles per path (attached or not): attach requires none.
    open: HashMap<String, usize>,
    /// Paths being attached: `x_open` refuses their main db meanwhile (an open would read under
    /// attach's writes, and its close would drop the lock attach holds against other processes:
    /// `lock_unused`). Each keeps its binding in `dbs` (if any) until the attach ends.
    attaching: HashSet<String>,
    /// Paths whose last attach failed, with the reason (`ursula_status`, and the refusal `x_open`
    /// logs: see `refused`).
    failed: HashMap<String, String>,
    /// Host locks held for the process lifetime.
    locks: HashMap<String, fs::File>,
    /// Snapshot thread per attached database.
    snappers: HashMap<String, SnapshotThread>,
}

type SnapshotThread = (Arc<Snapper>, std::thread::JoinHandle<()>);

fn registry() -> MutexGuard<'static, Registry> {
    static R: OnceLock<Mutex<Registry>> = OnceLock::new();
    R.get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn lookup(path: &str) -> Option<Arc<Mutex<Db>>> {
    registry().dbs.get(path).cloned()
}

/// The attachment of `path`, for `ursula_status` and `ursula_stats`.
fn attached(path: &str) -> Result<Arc<Mutex<Db>>, String> {
    let reg = registry();
    if let Some(db) = reg.dbs.get(path) {
        return Ok(db.clone());
    }
    Err(match reg.failed.get(path) {
        Some(why) => format!("{path} is not attached: its last attach failed ({why})"),
        None => format!("{path} is not attached"),
    })
}

/// Why `x_open` refuses the main db `path` that has no binding: a sidecar next to it makes it the
/// cache of a stream, which passed through to "unix" would take commits that never reach the stream
/// (in a process that never attached it, or after its attach failed: the files may hold anything
/// between their old state and the stream's). A path without a sidecar passes through. No WAL open
/// can follow a refusal: SQLite opens `<db>-wal` only through a connection whose main db it opened.
fn refused(reg: &Registry, path: &str) -> Option<String> {
    if !fs::exists(format!("{path}-ursula")).unwrap_or(true) {
        return None;
    }
    Some(match reg.failed.get(path) {
        Some(why) => format!("its last attach failed ({why}); attach it again"),
        None => "it is the cache of an Ursula stream (it has a sidecar); attach it first".into(),
    })
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

/// This kernel's boot id; `None` when unknown, which never matches a recorded one. Within one boot
/// every completed write stays visible (the page cache survives any process crash), so local files
/// written since this boot are exactly what this host wrote; across a reboot or power loss they may
/// be anything. Read at every attach, never cached: a process restored after a reboot (CRIU) must
/// see the new boot. No override, not even for tests (they rewrite the sidecar instead): a fixed id
/// would make every reboot look like the same boot.
fn boot_id() -> Option<String> {
    read_boot_id()
        .map(|id| id.trim().to_owned())
        .filter(|id| !id.is_empty() && !id.contains(char::is_whitespace))
}

#[cfg(target_os = "linux")]
fn read_boot_id() -> Option<String> {
    fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()
}

/// `kern.bootsessionuuid` (not `kern.uuid`, the kernel binary's, which never changes).
#[cfg(target_os = "macos")]
fn read_boot_id() -> Option<String> {
    let mut buf = [0u8; 64];
    let mut len = buf.len();
    let name = c"kern.bootsessionuuid";
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            buf.as_mut_ptr().cast(),
            &mut len,
            null_mut(),
            0,
        )
    };
    if rc != 0 {
        return None;
    }
    let id = CStr::from_bytes_until_nul(&buf).ok()?;
    id.to_str().ok().map(str::to_owned)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn read_boot_id() -> Option<String> {
    None
}

/// The stream's identity in a sidecar: its URL path (the host may differ: a gateway, another node).
fn stream_key(url: &str) -> &str {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    rest.find('/').map_or("", |i| &rest[i..])
}

/// The db file's identity, when it exists: its inode. Not the device: an overlay root filesystem
/// (a container's writable layer) gets a new device number at every mount, and a db on another
/// volume has its own sidecar next to it anyway.
fn file_id(path: &str) -> Option<String> {
    use std::os::unix::fs::MetadataExt;
    fs::metadata(path).ok().map(|m| m.ino().to_string())
}

/// What a sidecar records besides offset, epoch, format (`v=`), log count (`log=`) and the WAL
/// claim: the boot it was written in (`boot`, from `boot_id`), the stream and its incarnation
/// (`Head::incarnation`), and the db file it describes (see `trusted`).
fn stamp(path: &str, url: &str, boot: Option<&str>, incarnation: &str) -> String {
    let mut s = format!(
        " boot={} stream={}",
        boot.unwrap_or("unknown"),
        stream_key(url)
    );
    if let Some(id) = file_id(path) {
        let _ = write!(s, " file={id}");
    }
    let _ = write!(s, " incarnation={incarnation}");
    s
}

/// What a sidecar needs of the local WAL, so attach can check it against the files instead of
/// inferring it: the WAL's generation (the salts of its header, which SQLite changes at every
/// restart) and the frame number of the last acknowledged commit frame. Frame 0: the WAL holds no
/// commit; the db file alone holds the state, fsynced before the sidecar was written.
///
/// Why that suffices for any crash-consistent image of the files (a process crash, or a disk
/// snapshot restored without a reboot): SQLite starts a WAL generation only once every frame of
/// the previous one is in the db file, and the db file is fsynced before the new generation's
/// header can reach the disk (`commit`), so an image that shows the generation holds every page
/// from before it; the frames up to the claimed one hold every later commit up to the sidecar's
/// offset. Any page of the image that is newer (backfilled, or frames past the claim) belongs to a
/// commit after the offset, which attach replays.
#[derive(Clone, Copy)]
struct WalClaim {
    salts: u64,
    frame: u32,
}

impl WalClaim {
    const NONE: WalClaim = WalClaim { salts: 0, frame: 0 };

    /// The local WAL, as SQLite's recovery will read it, holds what the claim needs: this
    /// generation with frames reaching the claimed one, or for `NONE` no commit at all (frames left
    /// over from before the db file was synced would roll pages back).
    fn covered(self, path: &str) -> bool {
        let (salts, last) =
            wal_recover(&format!("{path}-wal")).map_or((0, 0), |w| (w.salts, w.last));
        match self.frame {
            0 => last == 0,
            frame => salts == self.salts && last >= frame,
        }
    }
}

/// What SQLite's recovery (`walIndexRecover`) finds in a WAL file. The valid frames are read in
/// order while their salts match the header's and the cumulative checksum holds.
struct WalScan {
    /// The generation: the header's salts.
    salts: u64,
    /// The number of the last commit frame among the valid ones (0: none).
    last: u32,
    /// The db size (pages) after that commit.
    size: u32,
    /// The page number of each frame up to `last`, in order.
    pages: Vec<u32>,
}

/// `None` without a valid header (missing, short, torn, or not 4 KiB pages).
fn wal_recover(path: &str) -> Option<WalScan> {
    use std::io::Read;
    let mut r = std::io::BufReader::new(fs::File::open(path).ok()?);
    let mut hdr = [0u8; WAL_HDR as usize];
    r.read_exact(&mut hdr).ok()?;
    let magic = be32(&hdr);
    if magic & !1 != 0x377f_0682 || be32(&hdr[4..]) != 3_007_000 || be32(&hdr[8..]) as usize != PAGE
    {
        return None;
    }
    // The checksum reads 32-bit words big-endian when the magic's low bit is set.
    let word = |b: &[u8]| {
        let w = [b[0], b[1], b[2], b[3]];
        if magic & 1 == 1 {
            u32::from_be_bytes(w)
        } else {
            u32::from_le_bytes(w)
        }
    };
    let sum = |mut s: (u32, u32), b: &[u8]| {
        for c in b.chunks_exact(8) {
            s.0 = s.0.wrapping_add(word(c)).wrapping_add(s.1);
            s.1 = s.1.wrapping_add(word(&c[4..])).wrapping_add(s.0);
        }
        s
    };
    let mut s = sum((0, 0), &hdr[..24]);
    if s != (be32(&hdr[24..]), be32(&hdr[28..])) {
        return None;
    }
    let (mut last, mut size, mut pages) = (0, 0, Vec::new());
    let mut f = vec![0u8; FRAME as usize];
    while r.read_exact(&mut f).is_ok() {
        if be32(&f) == 0 || f[8..16] != hdr[16..24] {
            break;
        }
        s = sum(sum(s, &f[..8]), &f[FRAME_HDR as usize..]);
        if s != (be32(&f[16..]), be32(&f[20..])) {
            break;
        }
        pages.push(be32(&f));
        if be32(&f[4..]) != 0 {
            (last, size) = (pages.len() as u32, be32(&f[4..]));
        }
    }
    pages.truncate(last as usize);
    let salts: [u8; 8] = hdr[16..24].try_into().unwrap();
    Some(WalScan {
        salts: u64::from_be_bytes(salts),
        last,
        size,
        pages,
    })
}

/// Folds the local WAL into the db file `f` the way a complete checkpoint would (the valid frames
/// up to the last commit, a later frame winning, pages past that commit's db size dropped, the file
/// cut to it), without opening the file through SQLite: trust (`WalClaim`) only says every page
/// holds the state at the sidecar's offset or a later commit's, not that SQLite can read the file
/// (a disk image may hold a torn page 1 that replay rewrites). The caller holds `f` locked against
/// other processes (`lock_unused`).
fn fold_wal(path: &str, f: &fs::File) -> Result<(), String> {
    let wal = format!("{path}-wal");
    let Some(scan) = wal_recover(&wal).filter(|w| w.last > 0) else {
        return Ok(());
    };
    let err = |e: std::io::Error| format!("fold {wal} into {path}: {e}");
    let w = fs::File::open(&wal).map_err(err)?;
    // Frame index per page, a later frame winning.
    let latest: BTreeMap<u32, i64> = scan.pages.iter().copied().zip(0..).collect();
    let mut page = vec![0u8; PAGE];
    for (pgno, i) in latest.range(1..=scan.size) {
        let at = (WAL_HDR + i * FRAME + FRAME_HDR) as u64;
        w.read_exact_at(&mut page, at).map_err(err)?;
        f.write_all_at(&page, (*pgno as u64 - 1) * PAGE as u64)
            .map_err(err)?;
    }
    f.set_len(scan.size as u64 * PAGE as u64).map_err(err)
}

/// A sidecar's content: the offset the local files reflect (the server's string), the epoch, the
/// format version, the log since the latest snapshot (`Db::log`), the `stamp`, and the WAL claim.
fn sidecar_line(offset: &str, epoch: u64, log: u64, stamp: &str, wal: WalClaim) -> String {
    format!(
        "{offset} {epoch} v={SIDECAR_VERSION} log={log}{stamp} wal={:016x}:{}\n",
        wal.salts, wal.frame
    )
}

/// Replaces the sidecar (`sidecar_line`) atomically against a process crash: temp file, rename. No
/// fsync: `Sidecar::trusted` checks it against the files (a sidecar ahead of its WAL is rebuilt;
/// one behind it replays from its offset).
fn write_sidecar(path: &str, line: &str) -> Result<(), String> {
    let err = |e: std::io::Error| format!("sidecar {path}: {e}");
    let tmp = format!("{path}.tmp");
    fs::write(&tmp, line).map_err(err)?;
    fs::rename(&tmp, path).map_err(err)
}

struct Sidecar {
    /// The offset the local file reflects (as the server wrote it; a version 1 sidecar's is a
    /// decimal number, which the server still reads).
    offset: String,
    epoch: u64,
    /// The format (`SIDECAR_VERSION`; 1 without a `v=` token).
    version: u32,
    /// `Db::log` (0 when absent).
    log: u64,
    boot: Option<String>,
    stream: Option<String>,
    incarnation: Option<String>,
    file: Option<String>,
    wal: Option<WalClaim>,
}

impl Sidecar {
    /// The local files hold at least the state at the sidecar's offset: written since this boot,
    /// from this incarnation of the stream (a stream deleted and recreated at the same path is
    /// another one, whatever its length), into this db file (one replaced behind our back would get
    /// the old one's WAL applied to it), and the local WAL still holds what the sidecar claims of
    /// it (a disk image restored without a reboot keeps boot and inode but may have lost any
    /// unsynced write). A sidecar of an older version (an older format, no boot, no incarnation,
    /// or no WAL claim) is not trusted, and nothing is when the current boot (`boot`, from
    /// `boot_id`) is unknown.
    fn trusted(&self, path: &str, boot: Option<&str>, incarnation: &str) -> bool {
        self.version == SIDECAR_VERSION
            && boot.is_some_and(|b| self.boot.as_deref() == Some(b))
            && self.incarnation.as_deref() == Some(incarnation)
            && self.file.is_some()
            && self.file == file_id(path)
            && self.wal.is_some_and(|w| w.covered(path))
    }
}

/// `Ok(None)`: the sidecar exists but does not parse (torn by a power loss, or garbage). `Err`: it
/// cannot be read (missing included).
fn read_sidecar(path: &str) -> Result<Option<Sidecar>, String> {
    let text = match fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::InvalidData => return Ok(None),
        Err(e) => return Err(format!("sidecar {path}: {e}")),
    };
    let mut it = text.split_whitespace();
    let offset = it.next().and_then(offset_token);
    let epoch = it.next().and_then(|v| v.parse::<u64>().ok());
    let (Some(offset), Some(epoch)) = (offset, epoch) else {
        return Ok(None);
    };
    let mut s = Sidecar {
        offset,
        epoch,
        version: 1,
        log: 0,
        boot: None,
        stream: None,
        incarnation: None,
        file: None,
        wal: None,
    };
    for token in it {
        match token.split_once('=') {
            Some(("v", v)) => match v.parse() {
                Ok(v) => s.version = v,
                Err(_) => return Ok(None),
            },
            Some(("log", v)) => match v.parse() {
                Ok(v) => s.log = v,
                Err(_) => return Ok(None),
            },
            Some(("boot", v)) => s.boot = Some(v.to_owned()),
            Some(("stream", v)) => s.stream = Some(v.to_owned()),
            Some(("incarnation", v)) => s.incarnation = Some(v.to_owned()),
            Some(("file", v)) => s.file = Some(v.to_owned()),
            Some(("wal", v)) => {
                let claim = v.split_once(':').and_then(|(salts, frame)| {
                    Some(WalClaim {
                        salts: u64::from_str_radix(salts, 16).ok()?,
                        frame: frame.parse().ok()?,
                    })
                });
                let Some(claim) = claim else {
                    return Ok(None);
                };
                s.wal = Some(claim);
            }
            _ => return Ok(None),
        }
    }
    Ok(Some(s))
}

/// Empties a database's local files (a cache of the stream) so attach rebuilds them: never while
/// another process has the file open, and never the host lock (held). The db file is truncated, not
/// unlinked, and returned still locked (`lock_unused`) for the rebuild to write: a connection that
/// opened the path meanwhile gets SQLITE_BUSY until the lock drops (a busy timeout retries) and
/// then reads the rebuilt file, never a deleted
/// inode. Every attach then removes `-journal`, and the fresh path `-wal` and `-shm`, and rewrites
/// the sidecar: until then the sidecar marks whatever is left untrusted, so a crash midway discards
/// again. A snapshot temp file an older version may have left is removed too.
fn discard_local(path: &str) -> Result<fs::File, String> {
    let f = open_locked(path)?;
    remove_if_exists(&format!("{path}-ursula.snap"))?;
    f.set_len(0).map_err(|e| format!("truncate {path}: {e}"))?;
    Ok(f)
}

/// Fails loudly: a stale WAL or journal left next to a rebuilt db file would be applied to it.
fn remove_if_exists(f: &str) -> Result<(), String> {
    match fs::remove_file(f) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(format!("remove {f}: {e}")),
        _ => Ok(()),
    }
}

fn abort_in_replay() -> Option<u64> {
    static V: OnceLock<Option<u64>> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_ABORT_IN_REPLAY")
            .ok()
            .and_then(|v| v.parse().ok())
    })
}

/// Keeps other processes off the db file `f` while attach rewrites it. Attach never opens local
/// files through SQLite before that (a trusted file may hold a torn page 1, see `fold_wal`), so it
/// does this without parsing pages: every SQLite connection on a WAL-format file (any process, any
/// unix-based VFS) takes a POSIX read lock in the db file's lock-byte range at its first read and
/// keeps it until it closes. Attach takes a write lock on that range (failing while any connection
/// holds it) and holds it until it is done writing the file: a connection reading after a mere
/// probe would recover the stale WAL, keep it open, and checkpoint it over the replayed pages at
/// its close, silently rolling the file back. Meanwhile a connection's first read fails with
/// SQLITE_BUSY. The file keeps its inode throughout (`discard_local` truncates, `install` writes
/// in place), so a connection that opened the path meanwhile reads the rewritten file once the
/// lock drops, not a deleted one, whose checkpoint would copy the shared `-wal`'s frames into the
/// dead inode and mark them checkpointed in the shared `-shm`.
///
/// The lock lives as long as `f` and any other descriptor of this process on the file: closing any
/// of them drops it, so the caller keeps `f` open and opens no other one meanwhile. No connection
/// of this process is open or can open (attach refuses otherwise, and `x_open` refuses the main db
/// while it attaches), so closing `f` drops only this lock. Another *attached* process is excluded
/// by the host lock already held.
fn lock_unused(path: &str, f: &fs::File) -> Result<(), String> {
    use std::os::fd::AsRawFd;
    let mut l: libc::flock = unsafe { std::mem::zeroed() };
    l.l_type = libc::F_WRLCK as _;
    l.l_whence = libc::SEEK_SET as _;
    l.l_start = 0x4000_0000; // PENDING_BYTE; RESERVED and the SHARED range follow (512 bytes)
    l.l_len = 512;
    if unsafe { libc::fcntl(f.as_raw_fd(), libc::F_SETLK, &l) } == 0 {
        return Ok(());
    }
    let e = std::io::Error::last_os_error();
    if !matches!(e.raw_os_error(), Some(libc::EAGAIN | libc::EACCES)) {
        return Err(format!("lock {path}: {e}"));
    }
    // Name the holder, if it still holds the range.
    let held = unsafe { libc::fcntl(f.as_raw_fd(), libc::F_GETLK, &mut l) } == 0
        && l.l_type != libc::F_UNLCK as _;
    let pid = if held {
        l.l_pid.to_string()
    } else {
        "unknown".into()
    };
    Err(format!(
        "{path} is open by another process (pid {pid}); close it before attaching"
    ))
}

/// Opens the db file and locks it (`lock_unused`); the lock lasts while the descriptor is open.
fn open_locked(path: &str) -> Result<fs::File, String> {
    let f = OpenOptions::new().read(true).write(true).open(path);
    let f = f.map_err(|e| format!("open {path}: {e}"))?;
    lock_unused(path, &f)?;
    Ok(f)
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

/// An offset as the server wrote it: opaque, kept verbatim and compared only lexicographically
/// (never parsed or computed with). `None` unless it is usable in a URL path or query and a
/// sidecar as it is: 1 to 64 unreserved URL characters.
fn offset_token(v: &str) -> Option<String> {
    let ok = |b: u8| b.is_ascii_alphanumeric() || b"-._~".contains(&b);
    (!v.is_empty() && v.len() <= 64 && v.bytes().all(ok)).then(|| v.to_owned())
}

fn header_offset(r: &ureq::http::Response<ureq::Body>, name: &str) -> Option<String> {
    offset_token(r.headers().get(name)?.to_str().ok()?.trim())
}

/// Waits before the next retry: no sooner than `retry_after`, and at least `backoff`, which then
/// doubles (up to 1 s). Returns false, without waiting, when the wait would end past `deadline`
/// (or overflow, for an absurd Retry-After).
fn pause(retry_after: Option<Duration>, backoff: &mut Duration, deadline: Instant) -> bool {
    let wait = retry_after.map_or(*backoff, |after| after.max(*backoff));
    if Instant::now()
        .checked_add(wait)
        .is_none_or(|end| end > deadline)
    {
        return false;
    }
    std::thread::sleep(wait);
    *backoff = (*backoff * 2).min(Duration::from_secs(1));
    true
}

/// Sends a read, retrying 429 (rate limiting) and 503 (overload, or a `consistency=leader` read,
/// HEAD or snapshot read the leader could not confirm with a quorum in time) the way `append`
/// does: no sooner than Retry-After (seconds), with backoff, within `retry_budget()`. Returns the
/// first other answer, or the last 429/503 once the budget is spent or `stopped` (a re-attach is
/// waiting for the snapshot thread).
fn read_retrying(
    send: impl Fn() -> Result<ureq::http::Response<ureq::Body>, ureq::Error>,
    stopped: &dyn Fn() -> bool,
) -> Result<ureq::http::Response<ureq::Body>, ureq::Error> {
    let deadline = Instant::now() + retry_budget();
    let mut backoff = Duration::from_millis(20);
    loop {
        let r = send()?;
        if !matches!(r.status().as_u16(), 429 | 503) {
            return Ok(r);
        }
        let retry_after = header_u64(&r, "retry-after").map(Duration::from_secs);
        if stopped() || !pause(retry_after, &mut backoff, deadline) {
            return Ok(r);
        }
    }
}

enum Append {
    /// Applied (or a duplicate of an applied append); the stream offset after it when known.
    Acked { next: Option<String>, attempts: u32 },
    /// 403: a newer epoch owns the stream.
    Fenced { current: Option<u64> },
    /// 409 expecting sequence 0: the server does not know this producer (expired after 7 idle days,
    /// or the stream was deleted and recreated: `producer_id`); `reclaim` tells them apart.
    ProducerExpired,
    /// 409 refusing the commit's `Stream-Seq` (not above the stream's last one): another writer
    /// appended with a higher one (see `stream_seq`).
    SeqConflict(String),
    /// A definite rejection, or no answer within the retry budget.
    Failed(String),
}

/// The `Producer-Id` of every owner of one stream incarnation: owners of the same incarnation fence
/// each other by epoch; to the stream recreated at the path, an owner of the deleted one is an
/// unknown producer (see `commit`).
fn producer_id(incarnation: &str) -> String {
    format!("{PRODUCER}/{incarnation}")
}

/// One idempotent append by `producer`: retried with the same producer sequence until the outcome
/// is known.
///
/// A duplicate answer is taken as proof of *our* earlier attempt only for commits (seq >= 1), and
/// only with its receipt (`Stream-Next-Offset`, checked by `commit`): they are sent after a
/// verified claim (see `claim_once`), which makes this owner the only writer of its incarnation's
/// producer at its epoch, so whatever holds (epoch, seq) there is ours. A claim's answer is
/// verified separately. Commits also carry their `Stream-Seq` (see `stream_seq`).
fn append(url: &str, producer: &str, body: &[u8], epoch: u64, seq: u64) -> Append {
    let deadline = Instant::now() + retry_budget();
    let mut backoff = Duration::from_millis(20);
    let mut attempts = 0;
    loop {
        attempts += 1;
        let mut req = agent()
            .post(url)
            .header("content-type", CONTENT_TYPE)
            .header("producer-id", producer)
            .header("producer-epoch", epoch.to_string())
            .header("producer-seq", seq.to_string());
        if seq > 0 {
            req = req.header("stream-seq", stream_seq((epoch, seq)));
        }
        let sent = req.send(body);
        let mut retry_after = None;
        let unknown = match sent {
            Ok(mut r) => {
                let status = r.status().as_u16();
                // Rate limiting (429, e.g. ursulagw) and overload (503) are transient: retried
                // with the same producer sequence, no sooner than Retry-After (seconds).
                retry_after = header_u64(&r, "retry-after").map(Duration::from_secs);
                match status {
                    200..=299 => {
                        return Append::Acked {
                            next: header_offset(&r, "stream-next-offset"),
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
                    // Neither a producer sequence conflict nor a closed stream: the `Stream-Seq`.
                    409 if seq > 0
                        && !r.headers().contains_key("producer-expected-seq")
                        && !r.headers().contains_key("stream-closed") =>
                    {
                        let text = r.body_mut().read_to_string().unwrap_or_default();
                        return Append::SeqConflict(text);
                    }
                    429 => format!(
                        "append: 429 {}",
                        r.body_mut().read_to_string().unwrap_or_default()
                    ),
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
        if !pause(retry_after, &mut backoff, deadline) {
            return Append::Failed(format!(
                "{unknown} (outcome unknown after {attempts} attempts)"
            ));
        }
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

/// One read from `offset`: the bytes and the server's offset after them (empty at the tail). Reads
/// the leader's applied state: a follower may lag behind an acknowledged append (a claim, a
/// commit). A 200 without a usable `Stream-Next-Offset` is an error: offsets are never computed.
fn read_from(url: &str, offset: &str) -> Result<(Vec<u8>, String), Fail> {
    let mut r = read_retrying(
        || {
            agent()
                .get(format!("{url}?offset={offset}&consistency=leader"))
                .call()
        },
        &|| false,
    )
    .map_err(|e| format!("read {url} at {offset}: {e}"))?;
    let status = r.status().as_u16();
    let next = header_offset(&r, "stream-next-offset");
    if status == 204 {
        return Ok((Vec::new(), next.unwrap_or_else(|| offset.to_owned())));
    }
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
    if status == 416 {
        return Err(Fail::Other(format!(
            "read {url} at {offset}: beyond the end of the stream that acknowledged it (the \
             server lost acknowledged data?); the local files are kept (delete them to rebuild)"
        )));
    }
    if status != 200 {
        return Err(Fail::Other(format!(
            "read {url} at {offset}: {status} {}",
            String::from_utf8_lossy(&body)
        )));
    }
    let Some(next) = next else {
        return Err(Fail::Other(format!(
            "read {url} at {offset}: no Stream-Next-Offset"
        )));
    };
    Ok((body, next))
}

/// A read loop's step: `next`, answered by a read at `at` that returned bytes, must sort past `at`.
/// Checked in the loops only, where both are server offsets (or `START`): an older sidecar's
/// unpadded offset, read once to probe for a 416, compares meaninglessly.
fn advanced(url: &str, at: &str, len: usize, next: &str) -> Result<(), String> {
    if next <= at {
        return Err(format!(
            "read {url} at {at}: {len} bytes but next offset {next}"
        ));
    }
    Ok(())
}

/// Snapshot transfers move whole databases: a longer timeout than appends.
fn bulk_agent() -> &'static ureq::Agent {
    static A: OnceLock<ureq::Agent> = OnceLock::new();
    A.get_or_init(|| {
        ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(120)))
            .http_status_as_error(false)
            .build()
            .into()
    })
}

struct Head {
    /// `START` when absent.
    retained: String,
    /// `None` when absent or `-1` (no snapshot).
    snapshot: Option<String>,
    /// `Stream-Incarnation`: opaque, changes when the stream is deleted and recreated; compared
    /// for equality only. `None` when absent (or unusable in a sidecar or `Producer-Id`): attach
    /// refuses.
    incarnation: Option<String>,
}

/// `stopped` ends the retries of a 429/503 early (see `read_retrying`).
fn head(url: &str, stopped: &dyn Fn() -> bool) -> Result<Head, String> {
    let r = read_retrying(|| agent().head(url).call(), stopped)
        .map_err(|e| format!("head {url}: {e}"))?;
    let status = r.status().as_u16();
    if status != 200 {
        return Err(format!("head {url}: {status}"));
    }
    Ok(Head {
        retained: header_offset(&r, "stream-retained-offset").unwrap_or_else(|| START.into()),
        snapshot: header_offset(&r, "stream-snapshot-offset").filter(|s| s != START),
        incarnation: r
            .headers()
            .get("stream-incarnation")
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty() && !v.contains(char::is_whitespace))
            .map(str::to_owned),
    })
}

/// The snapshot at `offset`; `None` when it does not exist (superseded, or not yet visible here).
/// `stopped` ends the retries of a 429/503 early (see `read_retrying`).
fn get_snapshot(
    url: &str,
    offset: &str,
    stopped: &dyn Fn() -> bool,
) -> Result<Option<Vec<u8>>, String> {
    let mut r = read_retrying(
        || bulk_agent().get(format!("{url}/snapshot/{offset}")).call(),
        stopped,
    )
    .map_err(|e| format!("get snapshot {offset}: {e}"))?;
    let status = r.status().as_u16();
    // A body cut short: the snapshot was superseded and its cold object deleted (after its grace)
    // while it was streaming, or the connection dropped. Either way, start again from `HEAD`.
    let Ok(body) = r.body_mut().with_config().limit(2 << 30).read_to_vec() else {
        return Ok(None);
    };
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
/// snapshot and advancing retention are idempotent. Returns the status and response. Gives up
/// once `stopped` (a re-attach is waiting for the snapshot thread).
fn put_idempotent(
    url: &str,
    body: &[u8],
    stopped: &dyn Fn() -> bool,
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
        if Instant::now() + backoff > deadline || stopped() {
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

/// Outcome of a checkpoint on a [`Private`] connection.
enum Checkpoint {
    /// Every WAL frame is in the db file.
    Done,
    /// A reader pins WAL frames the checkpoint needs.
    Pinned,
    /// Another connection holds the checkpoint (or a needed) lock.
    Busy,
}

/// A private connection on the "unix" VFS, outside this VFS's bookkeeping: the snapshot thread's
/// checkpoint, read transaction and page copy. `synchronous=OFF`: the local files are a cache (see
/// the crate docs), so its checkpoints never fsync.
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
            conn.query(c"PRAGMA synchronous=OFF")?;
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

    /// `PRAGMA wal_checkpoint(mode)`.
    unsafe fn checkpoint(&self, sql: &CStr) -> Result<Checkpoint, String> {
        let row = unsafe { self.query(sql)? };
        match row[..] {
            [0, log, done] if log == done => Ok(Checkpoint::Done),
            [0, ..] => Ok(Checkpoint::Pinned),
            [_, ..] => Ok(Checkpoint::Busy),
            _ => Err(format!("{sql:?}: no result row")),
        }
    }

    /// Keeps the WAL when this connection closes last (see `x_file_control`).
    unsafe fn persist_wal(&self) -> Result<(), String> {
        let mut on: c_int = 1;
        let rc = unsafe {
            (api().file_control.unwrap())(
                self.db,
                c"main".as_ptr(),
                ffi::SQLITE_FCNTL_PERSIST_WAL,
                &mut on as *mut c_int as *mut c_void,
            )
        };
        if rc != OK {
            return Err(format!("persist WAL: {rc}"));
        }
        Ok(())
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

/// Applies records to the db file, deduplicating page writes per batch.
///
/// Runs only inside `ursula_attach`, which refuses while any connection of this process has the
/// file open (or opens one) and has stopped the previous attachment's snapshot thread, and
/// `lock_unused` keeps other processes off the file from its first write until the Applier drops.
/// So its own descriptor on the db file cannot drop anyone else's POSIX locks when it closes.
/// Outside `ursula_attach`, the extension touches the db file, -wal or -shm only through SQLite's
/// handles (the private connections of `Private`).
struct Applier {
    path: String,
    /// The sidecar, its stamp, and the offset, epoch and log the replay starts from: rewritten once
    /// the WAL is folded (`file`).
    sidecar: String,
    stamp: String,
    from: (String, u64, u64),
    /// Offset of the snapshot installed (`START`: none).
    installed: String,
    /// Frame bytes since the latest snapshot (`Db::log`): `from`'s, plus every frame replayed;
    /// reset by an install.
    log: u64,
    /// Page images written (for the `URSULA_VFS_ABORT_IN_REPLAY` test hook).
    written: u64,
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

    /// The db file, opened once and locked (`lock_unused`, held on `self.file` until the Applier
    /// drops; `discard_local` hands over its locked file). Before its first write, a file with
    /// content (trusted, never opened through SQLite here) gets its WAL folded in (`fold_wal`);
    /// then the db file is fsynced, the sidecar keeps its offset but claims no WAL frame, and the
    /// WAL is deleted, all before replay writes a page. So no stale WAL sits next to pages replay
    /// moves past it (once a crash midway has dropped the lock, a plain SQLite connection opening
    /// the file would checkpoint it over them when it closes), and a recovery that dies midway
    /// leaves files the next attach trusts and replays again from the same offset (folded pages
    /// hold the state at the WAL's last commit, replayed ones later commits). A crash between that
    /// sidecar and the delete leaves a WAL with commits next to a claim of none: a rebuild.
    unsafe fn file(&mut self) -> Result<&fs::File, String> {
        if self.file.is_none() {
            let existing = fs::metadata(&self.path).is_ok_and(|m| m.len() > 0);
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&self.path);
            let f = f.map_err(|e| format!("open {}: {e}", self.path))?;
            lock_unused(&self.path, &f)?;
            if existing {
                fold_wal(&self.path, &f)?;
                f.sync_all()
                    .map_err(|e| format!("fsync {}: {e}", self.path))?;
                let (offset, epoch, log) = &self.from;
                let line = sidecar_line(offset, *epoch, *log, &self.stamp, WalClaim::NONE);
                write_sidecar(&self.sidecar, &line)?;
                remove_if_exists(&format!("{}-wal", self.path))?;
                remove_if_exists(&format!("{}-shm", self.path))?;
            }
            self.file = Some(f);
        }
        Ok(self.file.as_ref().unwrap())
    }

    /// Writes the batch: truncate to its smallest size, write the final page images, set the size.
    unsafe fn flush(&mut self) -> Result<(), String> {
        let (Some(min), Some(size)) = (self.min_size.take(), self.size) else {
            return Ok(());
        };
        let pages = std::mem::take(&mut self.pages);
        let mut written = self.written;
        let f = unsafe { self.file()? };
        let len = f.metadata().map_err(|e| e.to_string())?.len();
        if len > min as u64 * PAGE as u64 {
            f.set_len(min as u64 * PAGE as u64)
                .map_err(|e| e.to_string())?;
        }
        for (pgno, data) in pages {
            f.write_all_at(&data, (pgno as u64 - 1) * PAGE as u64)
                .map_err(|e| e.to_string())?;
            written += 1;
            if abort_in_replay() == Some(written) {
                eprintln!("sqlite-ursula-vfs: URSULA_VFS_ABORT_IN_REPLAY: aborting mid-replay");
                std::process::abort();
            }
        }
        f.set_len(size as u64 * PAGE as u64)
            .map_err(|e| e.to_string())?;
        self.written = written;
        Ok(())
    }

    /// Writes a snapshot's image over the db file in place (same inode, under the lock: see
    /// `lock_unused`) and continues the batch from it. The old file is not folded, and its WAL is
    /// deleted first so none sits next to the new image. A crash midway leaves a mix of the old
    /// file's pages and the image's (a state past the sidecar's offset): if the sidecar claims no
    /// WAL frame the next attach trusts it and installs the snapshot again (every page holds the
    /// state at its offset or a later one), otherwise the deleted WAL is behind it: a rebuild.
    unsafe fn install(&mut self, snap: snapshot::Snapshot) -> Result<(), String> {
        let err = |e: std::io::Error| format!("install snapshot into {}: {e}", self.path);
        let f = match self.file.take() {
            Some(f) => f,
            None => {
                let f = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&self.path)
                    .map_err(err)?;
                lock_unused(&self.path, &f)?;
                f
            }
        };
        remove_if_exists(&format!("{}-wal", self.path))?;
        remove_if_exists(&format!("{}-shm", self.path))?;
        f.write_all_at(&snap.image, 0).map_err(err)?;
        f.set_len(snap.image.len() as u64).map_err(err)?;
        self.file = Some(f);
        self.pages.clear();
        self.min_size = None;
        self.size = Some((snap.image.len() / PAGE) as u32);
        self.epoch = self.epoch.max(snap.epoch);
        self.installed = snap.offset;
        self.log = 0;
        Ok(())
    }
}

/// Reads and applies frames from `pos` (a frame boundary) until the tail (`until == None`) or
/// until `pos` reaches `until`. Offsets are opaque, so `pos` only ever takes a read's
/// `Stream-Next-Offset`, once no partial frame is buffered: a frame boundary at or before
/// everything applied (replay from there is idempotent).
unsafe fn catch_up(
    url: &str,
    pos: &mut String,
    until: Option<&str>,
    applier: &mut Applier,
) -> Result<(), Fail> {
    let (mut buf, mut at) = (Vec::new(), pos.clone());
    loop {
        if buf.is_empty() && until.is_some_and(|u| pos.as_str() >= u) {
            break;
        }
        let (bytes, next) = read_from(url, &at)?;
        if bytes.is_empty() {
            if !buf.is_empty() || until.is_some() {
                return Err(Fail::Other(format!(
                    "stream {url} ends at {at} inside a frame or before {until:?}"
                )));
            }
            break;
        }
        advanced(url, &at, bytes.len(), &next)?;
        buf.extend_from_slice(&bytes);
        at = next;
        let mut used = 0;
        while let Decoded::Frame { record, len } =
            frame::decode(&buf[used..]).map_err(|e| format!("{url}: a frame after {pos}: {e}"))?
        {
            applier.apply(record);
            used += len;
        }
        buf.drain(..used);
        applier.log += used as u64;
        unsafe { applier.flush()? };
        if buf.is_empty() {
            pos.clone_from(&at);
        }
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
    /// Our claim ends at `next`: this owner alone writes at `epoch` from here on. `first`: it is
    /// the first frame after the position the claim was checked from.
    Won {
        first: bool,
        next: String,
    },
    /// Another owner's claim (or anything else) holds the answered position: a concurrent
    /// claim at the same epoch was answered as a duplicate of theirs.
    Lost,
    Fenced(Option<u64>),
}

/// Appends a claim at (`epoch`, seq 0) and verifies our claim among the frames read back (see
/// `find_claim`). A 2xx alone proves nothing: two owners claiming the same epoch both get one, the
/// second as a duplicate of the first's receipt. The nonce makes our claim's bytes unique.
///
/// Offsets are opaque, so the claim's start is not computed from its end: `from` is a frame
/// boundary at or before the stream's tail before the claim (the tail catch-up recorded, or the
/// owner's own offset), and the frames from there to the answered offset are read and decoded.
fn claim_once(url: &str, producer: &str, epoch: u64, from: &str) -> Result<Claimed, String> {
    let nonce = nonce()?;
    let frame = frame::encode_claim(epoch, &nonce);
    let next = match append(url, producer, &frame, epoch, 0) {
        Append::Acked {
            next: Some(next), ..
        } => next,
        Append::Acked { next: None, .. } => {
            return Err(format!("claim {url}: no Stream-Next-Offset"));
        }
        Append::Fenced { current } => return Ok(Claimed::Fenced(current)),
        Append::ProducerExpired | Append::SeqConflict(_) => {
            unreachable!("a claim has sequence 0 and no Stream-Seq")
        }
        Append::Failed(e) => return Err(format!("claim {url}: {e}")),
    };
    let (mut buf, mut at) = (Vec::new(), from.to_owned());
    while at.as_str() < next.as_str() {
        let (bytes, n) = read_from(url, &at).map_err(String::from)?;
        if bytes.is_empty() {
            return Err(format!(
                "claim {url}: the stream ends at {at}, before the claim's end {next}"
            ));
        }
        advanced(url, &at, bytes.len(), &n)?;
        buf.extend_from_slice(&bytes);
        at = n;
    }
    let exact = at == next;
    let found = find_claim(&buf, exact, epoch, &nonce).map_err(|e| format!("claim {url}: {e}"))?;
    Ok(match found {
        Some(first) => Claimed::Won { first, next },
        None => Claimed::Lost,
    })
}

/// Our claim (`epoch`, `nonce`) among the frames in `buf`, read from a frame boundary before it up
/// to the answered offset (`exact`) or past it (another owner appended meanwhile, and the answered
/// offset's place in the bytes is unknown): `Some(first)` when it is the frame ending at the
/// answered offset (`first`: no frame precedes it in `buf`), `None` (lost) otherwise. Past the
/// answered offset, our claim being there at all is enough: the server applies one append per
/// (producer, epoch, seq 0) and answers every other with that append's receipt, so our claim is in
/// the stream only if the answer was its own end.
fn find_claim(
    buf: &[u8],
    exact: bool,
    epoch: u64,
    nonce: &[u8; 16],
) -> Result<Option<bool>, String> {
    let (mut used, mut found, mut last_ours) = (0, None, false);
    while let Decoded::Frame { record, len } = frame::decode(&buf[used..])? {
        last_ours = record
            == Record::Claim {
                epoch,
                nonce: *nonce,
            };
        if last_ours {
            found = Some(used == 0);
        }
        used += len;
    }
    if exact && used != buf.len() {
        return Err("the answered offset is not a frame boundary".into());
    }
    Ok(found.filter(|_| !exact || last_ours))
}

/// Claims the stream with an epoch above every earlier owner's; returns it and the claim's end.
/// `from`: a frame boundary at or before the tail (see `claim_once`).
fn claim(url: &str, producer: &str, epoch: u64, from: &str) -> Result<(u64, String), String> {
    // Test hook URSULA_VFS_FIRST_CLAIM_EPOCH: the process's first claim uses this epoch.
    static HOOKED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    let mut epoch = match first_claim_epoch() {
        Some(e) if !HOOKED.swap(true, Ordering::Relaxed) => e,
        _ => epoch,
    };
    for _ in 0..16 {
        match claim_once(url, producer, epoch, from)? {
            Claimed::Won { next, .. } => return Ok((epoch, next)),
            Claimed::Lost => epoch += 1,
            Claimed::Fenced(current) => epoch = current.unwrap_or(epoch).max(epoch) + 1,
        }
    }
    Err(format!("claim {url}: lost 16 claim races"))
}

/// The server does not know this owner's producer: it expired it (7 days without a write) and
/// forgot its epoch, or the stream was deleted and recreated, which knows no producer of the
/// deleted one (`producer_id`). Taking the stream back is safe only if it is still the incarnation
/// this owner attached to and nobody wrote since this owner's last frame: the stream must end at
/// our offset, and our new claim (one epoch up, fencing any later owner's older epochs) must be
/// ours (verified) and be the first frame after it, the incarnation unchanged after it. Otherwise
/// another owner wrote or claimed, or the stream is another one, and this one is fenced.
fn reclaim(db: &mut Db) -> Result<(), String> {
    fn same(db: &mut Db) -> Result<(), String> {
        match recreated(&db.url, &db.incarnation, &|| false)? {
            Some(e) => {
                db.fenced = true;
                Err(format!("fenced: {e}"))
            }
            None => Ok(()),
        }
    }
    same(db)?;
    let (bytes, _) = read_from(&db.url, &db.offset)?;
    if !bytes.is_empty() {
        db.fenced = true;
        return Err(format!(
            "fenced: producer expired and the stream moved past {}",
            db.offset
        ));
    }
    let epoch = db.epoch + 1;
    match claim_once(&db.url, &db.producer, epoch, &db.offset)? {
        Claimed::Won { first: true, next } => {
            same(db)?;
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
                c"PRAGMA synchronous=OFF; PRAGMA journal_mode=WAL".as_ptr(),
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

/// `Some(why)` unless the stream is still the incarnation `expected`: state built from one
/// incarnation must never reach another. `HEAD` is a leader read and incarnations never repeat
/// (unique per group), so a match also covers everything done on the stream
/// since the last check that matched.
fn recreated(
    url: &str,
    expected: &str,
    stopped: &dyn Fn() -> bool,
) -> Result<Option<String>, String> {
    let now = head(url, stopped)?.incarnation;
    Ok((now.as_deref() != Some(expected)).then(|| {
        format!(
            "{url} was deleted and recreated (incarnation {expected} is now {})",
            now.as_deref().unwrap_or("unknown")
        )
    }))
}

/// Brings the db file from `pos` to the stream's tail and claims the stream: installs the latest
/// snapshot when the file is behind it (or below the stream's retention), replays the frames after
/// it, claims, and replays up to the claim, all from the stream's `incarnation` (checked by the
/// `HEAD` before and after: a stream deleted and recreated meanwhile fails the attach, and the
/// sidecar, still stamped with the old incarnation, makes the next one rebuild; a newer owner's
/// claim replayed after ours fails it too). Returns the epoch claimed and the latest snapshot's
/// offset (`START` for none).
unsafe fn sync(
    url: &str,
    incarnation: &str,
    pos: &mut String,
    applier: &mut Applier,
) -> Result<(u64, String), Fail> {
    let head = head(url, &|| false)?;
    if head.incarnation.as_deref() != Some(incarnation) {
        return Err(Fail::Other(format!(
            "{url} was deleted and recreated during attach; attach again"
        )));
    }
    if let Some(s) = head.snapshot.as_deref()
        && pos.as_str() < s
    {
        let Some(body) = get_snapshot(url, s, &|| false)? else {
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
        s.clone_into(pos);
    } else if *pos != START && *pos < head.retained {
        return Err(Fail::Gone(format!(
            "{pos} is below the retention {} and no newer snapshot is visible",
            head.retained
        )));
    }
    // From the beginning (`START`), a stream trimmed with no snapshot visible answers 410: `Gone`.
    unsafe { catch_up(url, pos, None, applier)? };
    // `pos` is now the tail as catch-up found it: the claim is checked from there.
    let producer = producer_id(incarnation);
    let (epoch, claimed) = claim(url, &producer, applier.epoch + 1, pos)?;
    unsafe { catch_up(url, pos, Some(claimed.as_str()), applier)? };
    // The replay may run past our claim into a higher one: another owner claimed meanwhile
    // (normally after ours, as the server refuses a lower epoch from this producer; a stray claim
    // under a deleted incarnation's producer is not epoch-fenced and may precede it). Either way
    // this owner is fenced, and a snapshot it took would record an epoch below the highest
    // claimed before it.
    if applier.epoch > epoch {
        return Err(Fail::Other(format!(
            "fenced: another owner claimed epoch {} during attach; attach again",
            applier.epoch
        )));
    }
    if let Some(e) = recreated(url, incarnation, &|| false)? {
        return Err(Fail::Other(format!("{e} during attach; attach again")));
    }
    Ok((epoch, head.snapshot.unwrap_or_else(|| START.into())))
}

unsafe fn attach(path: &str, url: &str) -> Result<String, String> {
    let path = unsafe { full_pathname(path)? };
    let url = url.trim_end_matches('/').to_owned();
    let previous = {
        let mut reg = registry();
        if reg.open.get(&path).copied().unwrap_or(0) > 0 {
            return Err(format!(
                "{path} has open connections; close them before attaching"
            ));
        }
        if reg.attaching.contains(&path) {
            return Err(format!("{path} is being attached by another thread"));
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
        // The path keeps its binding (refused to `x_open` meanwhile) until the outcome replaces
        // it below. No panic gets past `fn_attach` (extern "C" aborts), so this always ends there.
        reg.attaching.insert(path.clone());
        reg.snappers.remove(&path)
    };
    // The previous attachment's snapshot thread may hold a private connection on the file.
    if let Some((snapper, thread)) = previous {
        snapper.stop();
        let _ = thread.join();
    }
    let outcome = unsafe { attach_files(&path, &url) };
    let mut reg = registry();
    reg.attaching.remove(&path);
    match outcome {
        Ok((offset, db, snapper)) => {
            reg.failed.remove(&path);
            reg.dbs.insert(path.clone(), db);
            reg.snappers.insert(path, snapper);
            Ok(offset)
        }
        Err(e) => {
            // The binding no longer describes the files (they may hold anything between its state
            // and the stream's, the stream a newer claim): without it, `x_open` refuses the path
            // while it has a sidecar (`refused`) until an attach succeeds.
            reg.dbs.remove(&path);
            reg.failed.insert(path, e.clone());
            Err(e)
        }
    }
}

/// Decides whether the local files can be trusted (or discards them), brings them to the stream's
/// tail and claims it (`sync`), then builds the new attachment for `attach` to bind.
unsafe fn attach_files(
    path: &str,
    url: &str,
) -> Result<(String, Arc<Mutex<Db>>, SnapshotThread), String> {
    create_stream(url)?;
    let sidecar = format!("{path}-ursula");
    let boot = boot_id();
    let boot = boot.as_deref();
    let incarnation = head(url, &|| false)?.incarnation.ok_or_else(|| {
        format!("{url} reports no Stream-Incarnation (an older server?); refusing to attach")
    })?;
    let (mut local, mut emptied) = (None, None);
    if fs::metadata(path).is_ok_and(|m| m.len() > 0) {
        // A file with content but no sidecar was never attached: its pages are not in the stream.
        let s = read_sidecar(&sidecar).map_err(|e| {
            format!("{path} has content but no readable sidecar ({e}); refusing to attach")
        })?;
        if let Some(k) = s.as_ref().and_then(|s| s.stream.as_deref())
            && k != stream_key(url)
        {
            return Err(format!(
                "{path} is a cache of stream {k}, not {}; delete it to attach it there",
                stream_key(url)
            ));
        }
        match s {
            Some(s) if s.trusted(path, boot, &incarnation) => local = Some(s),
            // Written before a reboot (a power loss may have left any prefix of any write), by an
            // older version, torn, from another incarnation of the stream, for another db file,
            // or a disk image whose WAL lost frames the sidecar counts on: the stream has
            // everything committed.
            s => {
                let why: String = match s
                    .as_ref()
                    .map(|s| (s.incarnation.as_deref(), s.version, s.offset.as_str()))
                {
                    // The stream at the path is another one: nothing of the old one is wanted,
                    // whatever the new one's length.
                    Some((Some(old), _, _)) if old != incarnation => format!(
                        "a cache of stream incarnation {old}, but {url} is now incarnation \
                         {incarnation}: deleted and recreated"
                    ),
                    // The same incarnation, or an older version's sidecar (incarnation unknown:
                    // possibly the same stream), unless the stream lost acknowledged data: a
                    // sidecar offset never exceeds an acknowledged one, so a read there answering
                    // 416 (beyond the end) refuses, as for trusted files, instead of rebuilding an
                    // older database. `Gone` (below retention) is fine: the rebuild starts from a
                    // snapshot.
                    Some((old, version, offset)) => {
                        if offset != START
                            && let Err(Fail::Other(e)) = read_from(url, offset)
                        {
                            return Err(e);
                        }
                        if old.is_none() {
                            "an older version's sidecar, without the stream incarnation".into()
                        } else if version != SIDECAR_VERSION {
                            format!("a sidecar of format {version}, not {SIDECAR_VERSION}")
                        } else {
                            "another boot, a replaced db file, or a WAL behind the sidecar".into()
                        }
                    }
                    None => "a torn sidecar".into(),
                };
                emptied = Some(discard_local(path)?);
                eprintln!(
                    "sqlite-ursula-vfs: {path}: local files untrusted ({why}); discarded them, \
                     rebuilding from the stream"
                );
            }
        }
    }
    // The only rollback journal an attached file can have is `init_wal_format`'s, left by a crash
    // mid-switch: the file is an empty database with or without it, but SQLite's first open would
    // roll it back, truncating whatever attach writes after it.
    remove_if_exists(&format!("{path}-journal"))?;
    let initial = stamp(path, url, boot, &incarnation);
    let (from, epoch, log) = match &local {
        Some(s) => (s.offset.clone(), s.epoch, s.log),
        None => {
            // Nothing local: a WAL next to an empty db file holds nothing committed. The sidecar
            // is written before anything lands in the file, so a file an attach leaves
            // half-written is known as this stream's cache (resumed when the sidecar names it,
            // otherwise discarded and rebuilt) instead of being refused as never attached.
            remove_if_exists(&format!("{path}-wal"))?;
            remove_if_exists(&format!("{path}-shm"))?;
            write_sidecar(
                &sidecar,
                &sidecar_line(START, 0, 0, &initial, WalClaim::NONE),
            )?;
            (START.to_owned(), 0, 0)
        }
    };
    let mut applier = Applier {
        path: path.to_owned(),
        sidecar: sidecar.clone(),
        stamp: initial,
        from: (from.clone(), epoch, log),
        installed: START.into(),
        log,
        written: 0,
        file: emptied,
        pages: BTreeMap::new(),
        min_size: None,
        size: None,
        epoch,
    };
    let mut pos = from.clone();
    let mut tries = 0;
    // `Gone`: retention moved past the file (or the snapshot read was superseded) under a HEAD
    // that did not show it yet; the next round installs the newer snapshot.
    let (epoch, snapshot) = loop {
        match unsafe { sync(url, &incarnation, &mut pos, &mut applier) } {
            Ok(r) => break r,
            Err(Fail::Gone(e)) if tries < 10 => {
                tries += 1;
                eprintln!("sqlite-ursula-vfs: {url}: attach: {e}; retrying");
                std::thread::sleep(Duration::from_millis(50 * tries));
            }
            Err(e) => return Err(e.into()),
        }
    };
    let (installed, log) = (std::mem::take(&mut applier.installed), applier.log);
    // Untouched trusted files keep their claim; otherwise the WAL is gone (`Applier::file`,
    // `install`, or never there) and the db file alone holds the state.
    let wal = local.and_then(|s| s.wal.filter(|_| applier.file.is_none()));
    drop(applier);
    if fs::metadata(path).map(|m| m.len()).unwrap_or(0) == 0 {
        unsafe { init_wal_format(path)? };
    }
    // The db file alone must be on disk before the sidecar says so (no connection is open, so
    // this descriptor's close drops no lock).
    let wal = match wal {
        Some(wal) => wal,
        None => {
            fs::File::open(path)
                .and_then(|f| f.sync_all())
                .map_err(|e| format!("fsync {path}: {e}"))?;
            WalClaim::NONE
        }
    };
    // Stamped again: the db file may not have existed before (`file_id`).
    let stamp = stamp(path, url, boot, &incarnation);
    write_sidecar(&sidecar, &sidecar_line(&pos, epoch, log, &stamp, wal))?;
    let pages = (fs::metadata(path).map(|m| m.len()).unwrap_or(0) / PAGE as u64) as u32;
    let snapper = Arc::new(Snapper::default());
    let db = Arc::new(Mutex::new(Db {
        url: url.to_owned(),
        producer: producer_id(&incarnation),
        incarnation,
        sidecar,
        stamp,
        path: path.to_owned(),
        epoch,
        seq: 0,
        offset: pos.clone(),
        log,
        poisoned: None,
        fenced: false,
        overlay: BTreeMap::new(),
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
        pages,
        snapshot,
        retained: START.into(),
        snapper: snapper.clone(),
        window: false,
        window_wanted: false,
        snapshot_stats: Vec::new(),
        attached_from: from,
        installed,
    }));
    let thread = {
        let (db, snapper) = (db.clone(), snapper.clone());
        std::thread::Builder::new()
            .name("ursula-snapshot".into())
            .spawn(move || snapshot_loop(&db, &snapper))
            .map_err(|e| format!("spawn the snapshot thread: {e}"))?
    };
    Ok((pos, db, (snapper, thread)))
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

    fn stopped(&self) -> bool {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).1
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
    // A pinned or busy checkpoint clears up quickly; a failing server may not.
    let (mut busy, mut failing) = (Duration::from_millis(10), Duration::from_millis(100));
    while snapper.wait() {
        let (backoff, cap) = match unsafe { snapshot_once(db, snapper) } {
            Ok(true) => {
                (busy, failing) = (Duration::from_millis(10), Duration::from_millis(100));
                continue;
            }
            Ok(false) => (&mut busy, Duration::from_secs(1)),
            Err(e) => {
                eprintln!(
                    "sqlite-ursula-vfs: {}: snapshot: {e}; retrying later",
                    lock(db).url
                );
                (&mut failing, Duration::from_secs(30))
            }
        };
        if !snapper.pause(*backoff) {
            return;
        }
        *backoff = (*backoff * 2).min(cap);
        if lock(db).snapshot_due() {
            snapper.request();
        }
    }
}

/// One snapshot attempt (see the crate docs). `Ok(false)`: not possible right now (a reader pins
/// WAL frames the checkpoint needs, or another checkpoint kept it busy), try again shortly.
///
/// The image is exactly the stream's state at `offset`. The window opens once every acknowledged
/// commit is published locally (`!committed`); until then the next commit waits for it at its
/// commit point (`window_wanted`, bounded by `WINDOW_WAIT`), so a writer committing back to back
/// cannot starve it: a due snapshot opens its window within one transaction. It keeps any further commit from being
/// acknowledged (it waits at its commit point) until the read transaction has started, so the
/// checkpoint moves every WAL frame up to `offset` into the db file and the read transaction sees
/// the db file alone at `offset`. While the read transaction lasts no checkpoint can write newer
/// frames into the db file (a reader at mark 0 blocks backfill, one at a later mark caps it) and no
/// closing connection can checkpoint (that needs an EXCLUSIVE lock), so the pages copied are those
/// of `offset`. Commits wait only for the checkpoint and the start of the read transaction.
unsafe fn snapshot_once(db: &Mutex<Db>, snapper: &Snapper) -> Result<bool, String> {
    let stopped = || snapper.stopped();
    let started = Instant::now();
    let path = lock(db).path.clone();
    let conn = unsafe { Private::open(&path)? };
    // Closing as the last connection keeps the WAL (as the attached connections do).
    unsafe { conn.persist_wal()? };
    // Backfill outside the window, so the checkpoint inside it (which commits wait for) only
    // covers the frames committed in between.
    unsafe { conn.checkpoint(c"PRAGMA wal_checkpoint(PASSIVE)")? };
    let (url, incarnation, offset, epoch, pages, log) = {
        let mut d = lock(db);
        loop {
            if !d.snapshot_due() || d.poisoned.is_some() || stopped() {
                d.window_wanted = false;
                snapper.window_cv.notify_all();
                return Ok(true);
            }
            // Not while a commit is acknowledged but unpublished, nor while another connection
            // (typically the writer's auto-checkpoint, right after its commit) holds the
            // checkpoint lock, which would make the checkpoint below busy.
            if !d.committed && d.checkpoint_started.is_none() {
                break;
            }
            // The next commit waits for the window (see `window_wanted`).
            d.window_wanted = true;
            d = snapper
                .window_cv
                .wait_timeout(d, Duration::from_millis(100))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        d.window_wanted = false;
        d.window = true;
        (
            d.url.clone(),
            d.incarnation.clone(),
            d.offset.clone(),
            d.epoch,
            d.pages,
            d.log,
        )
    };
    let window = Window(db);
    // A checkpoint started by another connection after the window opened makes this one busy
    // (it gets no busy handler): retry briefly, it only covers frames up to `offset` too.
    let mut tries = 0;
    loop {
        match unsafe { conn.checkpoint(c"PRAGMA wal_checkpoint(PASSIVE)")? } {
            Checkpoint::Done => break,
            Checkpoint::Busy if tries < 50 => {
                tries += 1;
                std::thread::sleep(Duration::from_millis(2));
            }
            _ => return Ok(false),
        }
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
    let body = snapshot::encode(&offset, epoch, &image);
    // This state is a prefix of the incarnation it was attached to, not of a stream recreated at
    // the same path since: never publish it there (the snapshot endpoint knows no producer), and
    // stop this owner's commits now rather than at its next append (which the recreated stream
    // answers as an unknown producer's, and `reclaim` fences).
    let fence = |what: String, e: String| {
        // Not `poison()`: the overlay belongs to a write transaction that may be in flight; it
        // clears it itself when it ends, and `x_write` refuses its commit (`poisoned`).
        let why = format!("fenced: {what}: {e}");
        let mut d = lock(db);
        eprintln!(
            "sqlite-ursula-vfs: {}: {why}; database poisoned (re-attach to recover)",
            d.url
        );
        d.fenced = true;
        d.poisoned = Some(why);
    };
    if let Some(e) = recreated(&url, &incarnation, &stopped)? {
        fence(format!("snapshot at {offset} not published"), e);
        return Ok(true);
    }
    match put_idempotent(&format!("{url}/snapshot/{offset}"), &body, &stopped)? {
        (200..=299, _) => {}
        (409 | 410, _) => {
            // A snapshot at or past `offset` exists (another owner's, or this file's before a
            // re-attach): it covers the log counted up to the window.
            let newer = head(&url, &stopped)?.snapshot;
            let mut d = lock(db);
            if let Some(newer) = newer.filter(|n| *n > d.snapshot) {
                d.snapshot = newer;
            }
            d.log = d.log.saturating_sub(log);
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
        if stopped() {
            return Ok(true);
        }
        if get_snapshot(&url, &offset, &stopped)?.as_deref() == Some(&body[..]) {
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
        let previous = d.snapshot.clone();
        if offset > d.snapshot {
            d.snapshot.clone_from(&offset);
        }
        d.log = d.log.saturating_sub(log);
        if d.snapshot_stats.len() < 100_000 {
            d.snapshot_stats.push(SnapshotStat {
                offset: offset.clone(),
                bytes: body.len(),
                raw: image.len(),
                copy,
                total: started.elapsed(),
            });
        }
        (previous, d.retained.clone())
    };
    // Retention trails one snapshot behind: a host that read the previous snapshot (or whose file
    // is past it) still finds the frames after it, and the newer snapshot has read back.
    if previous > retained && previous < offset {
        // Checked again: the publish and its read-back are a window for a recreate too.
        if let Some(e) = recreated(&url, &incarnation, &stopped)? {
            fence(format!("retention not moved to {previous}"), e);
            return Ok(true);
        }
        match put_idempotent(&format!("{url}/retention/{previous}"), &[], &stopped)? {
            (200..=299, r) => {
                let effective = header_offset(&r, "stream-retained-offset").unwrap_or(previous);
                let mut d = lock(db);
                if effective > d.retained {
                    d.retained = effective;
                }
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
    let db = attached(&path)?;
    let db = lock(&db);
    Ok(format!(
        "{{\"offset\":{},\"epoch\":{},\"poisoned\":{},\"fenced\":{},\"reason\":{},\"snapshot\":{},\"retained\":{},\"local\":{},\"installed\":{}}}",
        json_str(&db.offset),
        db.epoch,
        db.poisoned.is_some(),
        db.fenced,
        db.poisoned.as_deref().map_or("null".to_owned(), json_str),
        json_str(&db.snapshot),
        json_str(&db.retained),
        json_str(&db.attached_from),
        json_str(&db.installed)
    ))
}

unsafe fn stats(path: &str) -> Result<String, String> {
    let path = unsafe { full_pathname(path)? };
    let db = attached(&path)?;
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
            json_str(&s.offset),
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
            Ok(offset) => result_text(ctx, offset),
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

/// A commit's `Stream-Seq`: the owner's (epoch, producer sequence), each zero-padded to 20 digits,
/// so it sorts like the pair. Owners of one incarnation claim ever higher epochs and number commits
/// upwards within one, so every commit's is above every earlier commit's: the server's check
/// (refused unless above the stream's last `Stream-Seq`) then fences a writer outside this protocol
/// that appended with a higher one since this owner's last commit (one with a lower or equal
/// `Stream-Seq` is itself refused). Writers without `Stream-Seq` go unnoticed: offsets are opaque,
/// so the VFS does not check where its frame landed.
fn stream_seq((epoch, seq): (u64, u64)) -> String {
    format!("{epoch:020}{seq:020}")
}

unsafe fn commit(file: *mut ffi::sqlite3_file, db: &mut Db, size: u32, commit_frame: i64) -> c_int {
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
    let mut outcome = append(&db.url, &db.producer, &body, db.epoch, db.seq + 1);
    // An unknown producer: expired, or this owner's stream was deleted and the one recreated at
    // its path never knew it (`producer_id`); `reclaim` tells them apart.
    if let Append::ProducerExpired = outcome {
        if let Err(e) = reclaim(db) {
            return db.poison(e);
        }
        outcome = append(&db.url, &db.producer, &body, db.epoch, db.seq + 1);
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
unsafe extern "C" fn x_file_control(
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

#[cfg(test)]
mod tests {
    use std::io::Read as _;
    use std::io::Write as _;
    use std::net::TcpListener;

    use super::*;

    // A leader read the server could not confirm with a quorum in time answers 503 (Retry-After),
    // and a gateway may rate-limit with 429: catch-up reads and HEAD retry both, as appends do,
    // so attach, the claim check and reclaim never fail on them.
    #[test]
    fn reads_retry_transient_unavailability() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/b/s", listener.local_addr().unwrap());
        let answer = |head: &str, body: &str| {
            format!(
                "HTTP/1.1 {head}\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            )
        };
        let unavailable = answer(
            "503 Service Unavailable\r\nretry-after: 0",
            "leader unknown",
        );
        let answers = [
            unavailable.clone(),
            answer("429 Too Many Requests", ""),
            answer("200 OK\r\nstream-next-offset: 7", "abc"),
            unavailable.clone(),
            answer("200 OK\r\nstream-retained-offset: 2", ""),
            // A Retry-After too large to wait for (or even add to now) ends the retries at once.
            answer(
                "503 Service Unavailable\r\nretry-after: 18446744073709551615",
                "leader unknown",
            ),
            // So does the stop flag (a re-attach waiting for the snapshot thread).
            unavailable,
        ];
        let server = std::thread::spawn(move || {
            answers
                .into_iter()
                .map(|answer| {
                    let (mut conn, _) = listener.accept().unwrap();
                    let mut request = Vec::new();
                    let mut byte = [0u8; 1];
                    while !request.ends_with(b"\r\n\r\n") {
                        conn.read_exact(&mut byte).unwrap();
                        request.push(byte[0]);
                    }
                    conn.write_all(answer.as_bytes()).unwrap();
                    let request = String::from_utf8_lossy(&request);
                    request.split(" HTTP/").next().unwrap().to_owned()
                })
                .collect::<Vec<_>>()
        });
        let (bytes, next) = read_from(&url, "4").map_err(String::from).unwrap();
        assert_eq!((&bytes[..], next.as_str()), (&b"abc"[..], "7"));
        assert_eq!(head(&url, &|| false).unwrap().retained, "2");
        let refused = get_snapshot(&url, "9", &|| false).unwrap_err();
        assert!(refused.contains("503"), "{refused}");
        let refused = head(&url, &|| true).err().unwrap();
        assert!(refused.contains("503"), "{refused}");
        let read = "GET /b/s?offset=4&consistency=leader";
        assert_eq!(server.join().unwrap(), [
            read,
            read,
            read,
            "HEAD /b/s",
            "HEAD /b/s",
            "GET /b/s/snapshot/9",
            "HEAD /b/s"
        ]);
    }

    // A claim is ours only if it is the frame ending at the answered offset (the bytes read from a
    // frame boundary before it end there), or, when the read ran past that offset (another owner
    // appended meanwhile), if it is among the frames at all. A same-epoch claim with another nonce
    // (the answer was a duplicate of theirs) is lost; offsets are never computed.
    #[test]
    fn claims_are_found_by_their_nonce() {
        let (ours, theirs) = ([1u8; 16], [2u8; 16]);
        let commit = frame::encode_commit(1, &BTreeMap::from([(1, vec![0u8; PAGE])])).0;
        let (mine, other) = (
            frame::encode_claim(7, &ours),
            frame::encode_claim(7, &theirs),
        );
        let find = |frames: &[&[u8]], exact: bool| find_claim(&frames.concat(), exact, 7, &ours);
        assert_eq!(find(&[&mine[..]], true), Ok(Some(true)));
        assert_eq!(find(&[&commit[..], &mine[..]], true), Ok(Some(false)));
        assert_eq!(find(&[&commit[..], &other[..]], true), Ok(None));
        assert_eq!(find(&[&frame::encode_claim(6, &ours)[..]], true), Ok(None));
        // Read past the answered offset: ours, then another owner's claim and a partial frame.
        let past: [&[u8]; 3] = [&mine[..], &other[..], &commit[..9]];
        assert_eq!(find(&past, false), Ok(Some(true)));
        assert_eq!(find(&[&other[..], &commit[..]], false), Ok(None));
        // Read exactly to it, but not on a frame boundary.
        assert!(find(&past, true).is_err());
    }

    // Attach trusts local files only when the sidecar was written in this boot, from this stream
    // incarnation, for this db file, and the WAL holds what it claims; a legacy, torn, other-boot
    // or other-incarnation sidecar is discarded, and a missing one is an error (the file may be a
    // database that was never attached).
    #[test]
    fn sidecar_trust() {
        let dir = std::env::temp_dir().join(format!("ursula-sidecar-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let db = dir.join("db").to_str().unwrap().to_owned();
        let sidecar = format!("{db}-ursula");
        fs::write(&db, b"x").unwrap();
        let trust = |line: &[u8], boot: Option<&str>| {
            fs::write(&sidecar, line).unwrap();
            read_sidecar(&sidecar)
                .unwrap()
                .map(|s| s.trusted(&db, boot, "i1"))
        };
        let b1 = Some("b1");
        let none = " wal=0000000000000000:0\n";
        let offset = "00000000000000000007";
        let mine = sidecar_line(
            offset,
            2,
            5,
            &stamp(&db, "http://h:1/b/s", b1, "i1"),
            WalClaim::NONE,
        )
        .replace(none, "");
        let here = format!("{mine}{none}");
        assert!(here.starts_with("00000000000000000007 2 v=2 log=5 boot=b1 stream=/b/s file="));
        assert!(here.contains(" incarnation=i1 wal="));
        assert_eq!(trust(here.as_bytes(), b1), Some(true));
        let s = read_sidecar(&sidecar).unwrap().unwrap();
        assert_eq!((s.offset.as_str(), s.epoch, s.log), (offset, 2, 5));
        // The stream deleted and recreated (another incarnation), a sidecar from before
        // incarnations were recorded, or one of format 1 (a numeric offset; still parsed, for the
        // incarnation and the offset's read check before discarding).
        assert!(!s.trusted(&db, b1, "i2"));
        let legacy = here.replace(" incarnation=i1", "");
        assert_eq!(trust(legacy.as_bytes(), b1), Some(false));
        let v1 = here.replace(" v=2 log=5", "").replace(offset, "7");
        assert_eq!(trust(v1.as_bytes(), b1), Some(false));
        let s = read_sidecar(&sidecar).unwrap().unwrap();
        assert_eq!((s.version, s.offset.as_str()), (1, "7"));
        assert_eq!(s.incarnation.as_deref(), Some("i1"));
        assert_eq!(trust(here.as_bytes(), Some("b2")), Some(false));
        assert_eq!(trust(here.as_bytes(), None), Some(false));
        let unknown = here.replace(" boot=b1", " boot=unknown");
        assert_eq!(
            unknown,
            sidecar_line(
                offset,
                2,
                5,
                &stamp(&db, "http://h:1/b/s", None, "i1"),
                WalClaim::NONE
            )
        );
        assert_eq!(trust(unknown.as_bytes(), None), Some(false));
        // A claim on WAL frames that are not there (no WAL here), or no claim at all.
        let behind = format!("{mine} wal=00000000000000ff:3\n");
        assert_eq!(trust(behind.as_bytes(), b1), Some(false));
        assert_eq!(trust(format!("{mine}\n").as_bytes(), b1), Some(false));
        // Another db file (replaced by a rename) is not trusted.
        let other_file = format!("{offset} 2 v=2 boot=b1 stream=/b/s file=0 incarnation=i1");
        assert_eq!(
            trust(format!("{other_file}{none}").as_bytes(), b1),
            Some(false)
        );
        assert_eq!(trust(b"7 2\n", b1), Some(false));
        for torn in [
            "",
            "7",
            "7 2 boot",
            "7 2 x=1",
            "7 2 wal=12",
            "7 2 wal=zz:1",
            "7 2 v=x",
            "7 2 log=-1",
            "7/ 2",
        ] {
            assert_eq!(trust(torn.as_bytes(), b1), None);
        }
        assert_eq!(trust(b"\xff\xfe 7 2", b1), None);
        // A WAL of generation 0xaa with two commit frames of page 1 (images of 1s, then 2s): a
        // claim holds only for this generation (an older WAL with as many frames proves nothing),
        // `:0` only without a commit; folding it leaves the last image, cut to its db size.
        let sum = |mut s: (u32, u32), b: &[u8]| {
            for c in b.chunks_exact(8) {
                s.0 = s.0.wrapping_add(be32(c)).wrapping_add(s.1);
                s.1 = s.1.wrapping_add(be32(&c[4..])).wrapping_add(s.0);
            }
            s
        };
        let mut wal = Vec::new();
        for v in [0x377f_0683u32, 3_007_000, PAGE as u32, 0, 0, 0xaa] {
            wal.extend(v.to_be_bytes());
        }
        let mut s = sum((0, 0), &wal);
        wal.extend(s.0.to_be_bytes());
        wal.extend(s.1.to_be_bytes());
        for image in [1u8, 2] {
            let page = vec![image; PAGE];
            let mut h = Vec::new();
            for v in [1u32, 1, 0, 0xaa] {
                h.extend(v.to_be_bytes());
            }
            s = sum(sum(s, &h[..8]), &page);
            h.extend(s.0.to_be_bytes());
            h.extend(s.1.to_be_bytes());
            wal.extend(h);
            wal.extend(page);
        }
        fs::write(format!("{db}-wal"), &wal).unwrap();
        for (claim, trusted) in [("00000000000000aa:2", true), ("00000000000000bb:2", false)] {
            let line = format!("{mine} wal={claim}\n");
            assert_eq!(trust(line.as_bytes(), b1), Some(trusted), "{claim}");
        }
        assert_eq!(trust(here.as_bytes(), b1), Some(false));
        fs::write(&db, vec![9u8; 2 * PAGE]).unwrap();
        let f = OpenOptions::new().read(true).write(true).open(&db).unwrap();
        fold_wal(&db, &f).unwrap();
        assert_eq!(fs::read(&db).unwrap(), vec![2u8; PAGE]);
        fs::remove_file(&sidecar).unwrap();
        assert!(read_sidecar(&sidecar).is_err());
        fs::remove_dir_all(&dir).unwrap();
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(boot_id().is_some(), "the kernel's boot id is unreadable");
    }
}
