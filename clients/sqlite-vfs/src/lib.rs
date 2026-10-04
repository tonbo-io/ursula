//! SQLite loadable extension: a shim VFS ("ursula", registered as the default) over "unix" that
//! replicates every WAL commit of an attached database to an Ursula stream *before* any of the
//! transaction's frames reach the local `-wal` file. The stream is the source of truth; the local
//! db file and WAL are a cache of it.
//!
//! * `SELECT ursula_attach(path, stream_url)` takes the host lock `<db>-ursula.lock` (held for the
//!   process lifetime), catches the file up from the stream, claims the stream for this owner (a
//!   new producer epoch, which fences every earlier owner), catches up to the claim and attaches the
//!   file. It requires that no connection to the file is open. Returns the stream offset applied.
//!   A failed attach leaves a file that was attached in this process refusing opens
//!   (SQLITE_CANTOPEN) until an attach succeeds, so it is never written unreplicated.
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
//!   otherwise discards them and rebuilds from the latest snapshot and the tail. The db file alone is fsynced, rarely, so a crash-consistent image of the files (a disk
//!   snapshot) is either verifiably complete or rejected: before the WAL starts a new generation
//!   or is truncated to nothing, and at attach before a sidecar that relies on it. Attach never
//!   opens local files through SQLite before replaying onto them: it folds the WAL into the db
//!   file itself.
//! * Snapshots and retention (see [`snapshot`]): once the log since the latest snapshot exceeds the
//!   database size (and `URSULA_VFS_SNAPSHOT_MIN_BYTES`, default 8 MiB), a background thread per
//!   attached database checkpoints the local WAL through a private connection, pins the result with
//!   a read transaction (no commit may land in between; otherwise it tries again later), copies the
//!   db file's pages, publishes them at the stream offset they reflect, reads the snapshot back and
//!   only then advances the stream's retention to the *previous* snapshot's offset. Attach installs
//!   the latest snapshot when the local file is missing or behind it, then replays the tail.
//! * `SELECT ursula_status(path)` returns
//!   `{"offset","epoch","poisoned","fenced","reason","snapshot","retained","local","installed"}`
//!   (`local`: the stream offset of the local state attach started from, 0 when it rebuilt the
//!   file; `installed`: the offset of the snapshot attach installed, 0 for none);
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
    /// The stream's incarnation at attach (`Head::incarnation`): a re-claim or a snapshot checks
    /// the stream is still it.
    incarnation: Option<String>,
    sidecar: String,
    /// What the sidecar records besides offset, epoch and the WAL claim (see `stamp`).
    stamp: String,
    path: String,
    epoch: u64,
    /// Producer sequence of the last acknowledged append (the claim is 0).
    seq: u64,
    /// Stream offset after the last acknowledged frame.
    offset: u64,
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
    /// found at attach); 0 for none.
    snapshot: u64,
    /// Retention this owner advanced the stream to.
    retained: u64,
    snapper: Arc<Snapper>,
    /// A snapshot is pinning the state at `offset`: no commit is acknowledged until it closes.
    window: bool,
    /// A due snapshot is waiting to open its window: the next commit waits at its commit point
    /// (up to `WINDOW_WAIT`) until it has, so a writer committing back to back cannot starve it.
    window_wanted: bool,
    snapshot_stats: Vec<SnapshotStat>,
    /// Stream offset of the local state attach started from (0: rebuilt from nothing), and of the
    /// snapshot it installed (0: none).
    attached_from: u64,
    installed: u64,
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
            self.poison(format!(
                "commit acknowledged but not published locally (mxFrame {mx_frame} < frame {})",
                self.commit_frame_no
            ));
            return;
        }
        let wal = WalClaim {
            salts: u64::from_be_bytes(salts),
            frame: self.commit_frame_no,
        };
        if let Err(e) = write_sidecar(&self.sidecar, self.offset, self.epoch, &self.stamp, wal) {
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
    /// Paths being attached: `x_open` refuses their main db meanwhile. Each keeps its binding
    /// in `dbs` (if any) until the attach ends.
    attaching: HashSet<String>,
    /// Paths whose attach failed after they had been attached in this process, with the reason:
    /// `x_open` refuses their main db and WAL until an attach succeeds (see `attach`).
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

/// What a sidecar records besides offset, epoch and the WAL claim: the boot it was written in
/// (`boot`, from `boot_id`), the stream and its incarnation (`Head::incarnation`), and the db file
/// it describes (see `trusted`).
fn stamp(path: &str, url: &str, boot: Option<&str>, incarnation: Option<&str>) -> String {
    let mut s = format!(
        " boot={} stream={}",
        boot.unwrap_or("unknown"),
        stream_key(url)
    );
    if let Some(id) = file_id(path) {
        let _ = write!(s, " file={id}");
    }
    if let Some(i) = incarnation {
        let _ = write!(s, " incarnation={i}");
    }
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

/// Replaces the sidecar atomically against a process crash: temp file, rename. No fsync:
/// `Sidecar::trusted` checks it against the files (a sidecar ahead of its WAL is rebuilt; one
/// behind it replays from its offset).
fn write_sidecar(
    path: &str,
    offset: u64,
    epoch: u64,
    stamp: &str,
    wal: WalClaim,
) -> Result<(), String> {
    let line = format!(
        "{offset} {epoch}{stamp} wal={:016x}:{}\n",
        wal.salts, wal.frame
    );
    let err = |e: std::io::Error| format!("sidecar {path}: {e}");
    let tmp = format!("{path}.tmp");
    fs::write(&tmp, line).map_err(err)?;
    fs::rename(&tmp, path).map_err(err)
}

struct Sidecar {
    /// The offset the local file reflects.
    offset: u64,
    epoch: u64,
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
    /// unsynced write). A sidecar of an older version (no boot, no incarnation, or no WAL claim)
    /// is not trusted, and nothing is when the current boot (`boot`, from `boot_id`) or
    /// incarnation is unknown.
    fn trusted(&self, path: &str, boot: Option<&str>, incarnation: Option<&str>) -> bool {
        boot.is_some_and(|b| self.boot.as_deref() == Some(b))
            && incarnation.is_some_and(|i| self.incarnation.as_deref() == Some(i))
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
    let mut num = || it.next().and_then(|v| v.parse::<u64>().ok());
    let (Some(offset), Some(epoch)) = (num(), num()) else {
        return Ok(None);
    };
    let mut s = Sidecar {
        offset,
        epoch,
        boot: None,
        stream: None,
        incarnation: None,
        file: None,
        wal: None,
    };
    for token in it {
        match token.split_once('=') {
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
/// snapshot was superseded), which a re-attach answers by installing the latest snapshot;
/// `Recreated` when the stream was deleted and recreated under an attach, which starts over from
/// the trust decision.
enum Fail {
    Gone(String),
    Recreated(String),
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
            Fail::Recreated(e) | Fail::Other(e) => e,
        }
    }
}

/// One read from `offset`: the bytes and the offset after them (empty at the tail). Reads the
/// leader's applied state: a follower may lag behind an acknowledged append (a claim, a commit).
fn read_from(url: &str, offset: u64) -> Result<(Vec<u8>, u64), Fail> {
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
    if status == 416 {
        return Err(Fail::Other(format!(
            "read {url} at {offset}: beyond the stream's end (was the stream deleted and \
             recreated? delete the local file to rebuild it)"
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
            .timeout_global(Some(Duration::from_secs(120)))
            .http_status_as_error(false)
            .build()
            .into()
    })
}

struct Head {
    retained: u64,
    snapshot: Option<u64>,
    /// `Stream-Incarnation`: opaque, changes when the stream is deleted and recreated; compared
    /// for equality only. `None` when absent (or unusable in a sidecar): nothing local is trusted.
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
        retained: header_u64(&r, "stream-retained-offset").unwrap_or(0),
        snapshot: header_u64(&r, "stream-snapshot-offset"),
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
    offset: u64,
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
    /// The sidecar, its stamp, and the offset and epoch the replay starts from: rewritten once the
    /// WAL is folded (`file`).
    sidecar: String,
    stamp: String,
    from: (u64, u64),
    /// Offset of the snapshot installed (0: none).
    installed: u64,
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
                let (offset, epoch) = self.from;
                write_sidecar(&self.sidecar, offset, epoch, &self.stamp, WalClaim::NONE)?;
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
/// epochs) must be ours (verified) and land exactly there, in the incarnation this owner attached
/// to (a stream deleted and recreated forgets its producers too). Otherwise another owner wrote or
/// claimed, or the stream is another one, and this one is fenced.
fn reclaim(db: &mut Db) -> Result<(), String> {
    fn same(db: &mut Db) -> Result<(), String> {
        match same_incarnation(&db.url, db.incarnation.as_deref(), &|| false) {
            Err(Fail::Recreated(e)) => {
                db.fenced = true;
                Err(format!("fenced: {e}"))
            }
            r => r.map_err(String::from),
        }
    }
    same(db)?;
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

/// `Recreated` unless the stream is still the incarnation `expected`: state built from one
/// incarnation must never reach another. `HEAD` is a leader read and incarnations never repeat, so
/// a match also covers everything done on the stream since the last check that matched.
fn same_incarnation(
    url: &str,
    expected: Option<&str>,
    stopped: &dyn Fn() -> bool,
) -> Result<(), Fail> {
    let now = head(url, stopped)?.incarnation;
    if now.as_deref() == expected {
        return Ok(());
    }
    Err(Fail::Recreated(format!(
        "{url} was deleted and recreated (incarnation {} is now {})",
        expected.unwrap_or("unknown"),
        now.as_deref().unwrap_or("unknown")
    )))
}

/// Brings the db file from `pos` to the stream's tail and claims the stream: installs the latest
/// snapshot when the file is behind it (or below the stream's retention), replays the frames after
/// it, claims, and replays up to the claim, all from the stream's `incarnation` (checked by the
/// `HEAD` before and after). Returns the epoch claimed and the latest snapshot's offset (0 for
/// none).
unsafe fn sync(
    url: &str,
    incarnation: Option<&str>,
    pos: &mut u64,
    applier: &mut Applier,
) -> Result<(u64, u64), Fail> {
    let head = head(url, &|| false)?;
    if head.incarnation.as_deref() != incarnation {
        return Err(Fail::Recreated(format!("{url} was deleted and recreated")));
    }
    if let Some(s) = head.snapshot
        && *pos < s
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
    same_incarnation(url, incarnation, &|| false)?;
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
            // A path attached before is no longer described by its binding (its files may hold
            // anything between that state and the stream's, its stream a newer claim): it stays
            // refused until an attach succeeds, never passed through to "unix" unreplicated. A path
            // never attached in this process keeps passing through.
            if reg.dbs.remove(&path).is_some() || reg.failed.contains_key(&path) {
                reg.failed.insert(path, e.clone());
            }
            Err(e)
        }
    }
}

/// What `recover` leaves for attach to bind.
struct Recovered {
    offset: u64,
    epoch: u64,
    snapshot: u64,
    /// Stream offset of the local state recovery started from (0: rebuilt from nothing), and of
    /// the snapshot it installed (0: none).
    from: u64,
    installed: u64,
    /// The trusted files' WAL claim, when recovery did not write the db file.
    wal: Option<WalClaim>,
    incarnation: Option<String>,
}

/// Recovers the files and claims the stream (see `recover`), then binds a new attachment to them.
unsafe fn attach_files(
    path: &str,
    url: &str,
) -> Result<(u64, Arc<Mutex<Db>>, SnapshotThread), String> {
    create_stream(url)?;
    let sidecar = format!("{path}-ursula");
    let boot = boot_id();
    let mut rounds = 0;
    let r = loop {
        match unsafe { recover(path, url, &sidecar, boot.as_deref()) } {
            Err(Fail::Recreated(e)) if rounds < 3 => {
                rounds += 1;
                eprintln!("sqlite-ursula-vfs: {path}: attach: {e}; starting over");
            }
            r => break r?,
        }
    };
    if fs::metadata(path).map(|m| m.len()).unwrap_or(0) == 0 {
        unsafe { init_wal_format(path)? };
    }
    // Untouched trusted files keep their claim; otherwise the WAL is gone (`Applier::file`,
    // `install`, or never there) and the db file alone holds the state, which must be on disk
    // before the sidecar says so (no connection is open, so this descriptor's close drops no lock).
    let wal = match r.wal {
        Some(wal) => wal,
        None => {
            fs::File::open(path)
                .and_then(|f| f.sync_all())
                .map_err(|e| format!("fsync {path}: {e}"))?;
            WalClaim::NONE
        }
    };
    let stamp = stamp(path, url, boot.as_deref(), r.incarnation.as_deref());
    write_sidecar(&sidecar, r.offset, r.epoch, &stamp, wal)?;
    let pages = (fs::metadata(path).map(|m| m.len()).unwrap_or(0) / PAGE as u64) as u32;
    let snapper = Arc::new(Snapper::default());
    let db = Arc::new(Mutex::new(Db {
        url: url.to_owned(),
        incarnation: r.incarnation,
        sidecar,
        stamp,
        path: path.to_owned(),
        epoch: r.epoch,
        seq: 0,
        offset: r.offset,
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
        snapshot: r.snapshot,
        retained: 0,
        snapper: snapper.clone(),
        window: false,
        window_wanted: false,
        snapshot_stats: Vec::new(),
        attached_from: r.from,
        installed: r.installed,
    }));
    let thread = {
        let (db, snapper) = (db.clone(), snapper.clone());
        std::thread::Builder::new()
            .name("ursula-snapshot".into())
            .spawn(move || snapshot_loop(&db, &snapper))
            .map_err(|e| format!("spawn the snapshot thread: {e}"))?
    };
    Ok((r.offset, db, (snapper, thread)))
}

/// Decides whether the local files can be trusted (or discards them), then brings them to the
/// stream's tail and claims it (`sync`). `Recreated`: the stream was deleted and recreated after
/// the trust decision; attach starts over, and the sidecar (stamped with the old incarnation)
/// makes the next round discard whatever this one wrote.
unsafe fn recover(
    path: &str,
    url: &str,
    sidecar: &str,
    boot: Option<&str>,
) -> Result<Recovered, Fail> {
    let incarnation = head(url, &|| false)?.incarnation;
    let (mut local, mut emptied) = (None, None);
    if fs::metadata(path).is_ok_and(|m| m.len() > 0) {
        // A file with content but no sidecar was never attached: its pages are not in the stream.
        let s = read_sidecar(sidecar).map_err(|e| {
            format!("{path} has content but no readable sidecar ({e}); refusing to attach")
        })?;
        if let Some(k) = s.as_ref().and_then(|s| s.stream.as_deref())
            && k != stream_key(url)
        {
            return Err(Fail::Other(format!(
                "{path} is a cache of stream {k}, not {}; delete it to attach it there",
                stream_key(url)
            )));
        }
        match s {
            Some(s) if s.trusted(path, boot, incarnation.as_deref()) => local = Some(s),
            // Written before a reboot (a power loss may have left any prefix of any write), by an
            // older version, torn, from another incarnation of the stream, for another db file,
            // or a disk image whose WAL lost frames the sidecar counts on: the stream has
            // everything committed.
            s => {
                // Unless it lost acknowledged data (deleted and recreated shorter): a sidecar
                // offset never exceeds an acknowledged one, so a read there answering 416 (beyond
                // the end) refuses, as for trusted files, instead of rebuilding an empty database.
                // `Gone` (below retention) is fine: the rebuild starts from a snapshot.
                if let Some(s) = s.filter(|s| s.offset > 0)
                    && let Err(Fail::Other(e)) = read_from(url, s.offset)
                {
                    return Err(Fail::Other(e));
                }
                emptied = Some(discard_local(path)?);
                eprintln!(
                    "sqlite-ursula-vfs: {path}: local files untrusted (another boot or stream \
                     incarnation, torn, replaced, or behind their sidecar); discarded them, \
                     rebuilding from the stream"
                );
            }
        }
    }
    // The only rollback journal an attached file can have is `init_wal_format`'s, left by a crash
    // mid-switch: the file is an empty database with or without it, but SQLite's first open would
    // roll it back, truncating whatever attach writes after it.
    remove_if_exists(&format!("{path}-journal"))?;
    let stamp = stamp(path, url, boot, incarnation.as_deref());
    let (from, epoch) = match &local {
        Some(s) => (s.offset, s.epoch),
        None => {
            // Nothing local: a WAL next to an empty db file holds nothing committed. The sidecar
            // is written before anything lands in the file, so a file an attach leaves
            // half-written is known as this stream's cache (resumed when the sidecar names it,
            // otherwise discarded and rebuilt) instead of being refused as never attached.
            remove_if_exists(&format!("{path}-wal"))?;
            remove_if_exists(&format!("{path}-shm"))?;
            write_sidecar(sidecar, 0, 0, &stamp, WalClaim::NONE)?;
            (0, 0)
        }
    };
    let mut applier = Applier {
        path: path.to_owned(),
        sidecar: sidecar.to_owned(),
        stamp,
        from: (from, epoch),
        installed: 0,
        written: 0,
        file: emptied,
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
        match unsafe { sync(url, incarnation.as_deref(), &mut pos, &mut applier) } {
            Ok(r) => break r,
            Err(Fail::Gone(e)) if tries < 10 => {
                tries += 1;
                eprintln!("sqlite-ursula-vfs: {url}: attach: {e}; retrying");
                std::thread::sleep(Duration::from_millis(50 * tries));
            }
            Err(e) => return Err(e),
        }
    };
    let rewritten = applier.file.is_some();
    Ok(Recovered {
        offset: pos,
        epoch,
        snapshot,
        from,
        installed: applier.installed,
        wal: local.and_then(|s| s.wal.filter(|_| !rewritten)),
        incarnation,
    })
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
    let (url, incarnation, offset, epoch, pages) = {
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
            d.offset,
            d.epoch,
            d.pages,
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
    let body = snapshot::encode(offset, epoch, &image);
    // This state is a prefix of the incarnation it was attached to, not of a stream recreated at
    // the same path since (whose next append from this owner fails: its producers are new).
    match same_incarnation(&url, incarnation.as_deref(), &stopped) {
        Err(Fail::Recreated(e)) => {
            eprintln!("sqlite-ursula-vfs: snapshot at {offset} not published: {e}");
            return Ok(true);
        }
        r => r.map_err(String::from)?,
    }
    match put_idempotent(&format!("{url}/snapshot/{offset}"), &body, &stopped)? {
        (200..=299, _) => {}
        (409 | 410, _) => {
            // A newer snapshot exists (another owner's, or this file's before a re-attach).
            let newer = head(&url, &stopped)?.snapshot.unwrap_or(0);
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
        if stopped() {
            return Ok(true);
        }
        if get_snapshot(&url, offset, &stopped)?.as_deref() == Some(&body[..]) {
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
        match put_idempotent(&format!("{url}/retention/{previous}"), &[], &stopped)? {
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
    let db = attached(&path)?;
    let db = lock(&db);
    Ok(format!(
        "{{\"offset\":{},\"epoch\":{},\"poisoned\":{},\"fenced\":{},\"reason\":{},\"snapshot\":{},\"retained\":{},\"local\":{},\"installed\":{}}}",
        db.offset,
        db.epoch,
        db.poisoned.is_some(),
        db.fenced,
        db.poisoned.as_deref().map_or("null".to_owned(), json_str),
        db.snapshot,
        db.retained,
        db.attached_from,
        db.installed
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
        if flags & ffi::SQLITE_OPEN_WAL != 0
            && let Some(db) = name.and_then(|n| n.strip_suffix("-wal"))
            && let Some(why) = registry().failed.get(db).cloned()
        {
            return refuse_failed(db, &why);
        }
        // Counted before the open, so attach (which refuses while any is counted) and an open
        // cannot pass each other; refused while `attach` runs on the path, and after a failed one
        // (`Registry::failed`).
        let main = match name {
            Some(name) if flags & ffi::SQLITE_OPEN_MAIN_DB != 0 => {
                let mut reg = registry();
                if reg.attaching.contains(name) {
                    return ffi::SQLITE_BUSY;
                }
                if let Some(why) = reg.failed.get(name) {
                    return refuse_failed(name, why);
                }
                *reg.open.entry(name.to_owned()).or_default() += 1;
                Some(reg.dbs.get(name).cloned())
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

/// An open of a path whose attach failed after it had been attached (`Registry::failed`): its
/// writes would bypass replication if it were passed through to "unix", so it fails until an attach
/// succeeds.
fn refuse_failed(path: &str, why: &str) -> c_int {
    eprintln!(
        "sqlite-ursula-vfs: {path}: open refused: its last attach failed ({why}); attach it again"
    );
    ffi::SQLITE_CANTOPEN
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
            if db.poisoned.is_none()
                && let Err(e) =
                    write_sidecar(&db.sidecar, db.offset, db.epoch, &db.stamp, WalClaim::NONE)
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
        let (bytes, next) = read_from(&url, 4).map_err(String::from).unwrap();
        assert_eq!((&bytes[..], next), (&b"abc"[..], 7));
        assert_eq!(head(&url, &|| false).unwrap().retained, 2);
        let refused = get_snapshot(&url, 9, &|| false).unwrap_err();
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
                .map(|s| s.trusted(&db, boot, Some("i1")))
        };
        let b1 = Some("b1");
        let none = " wal=0000000000000000:0\n";
        let mine = format!("7 2{}", stamp(&db, "http://h:1/b/s", b1, Some("i1")));
        let here = format!("{mine}{none}");
        assert!(here.starts_with("7 2 boot=b1 stream=/b/s file="));
        assert!(here.contains(" incarnation=i1 wal="));
        assert_eq!(trust(here.as_bytes(), b1), Some(true));
        // The stream deleted and recreated (another incarnation), an unknown current one, or a
        // sidecar from before incarnations were recorded.
        let s = read_sidecar(&sidecar).unwrap().unwrap();
        assert!(!s.trusted(&db, b1, Some("i2")));
        assert!(!s.trusted(&db, b1, None));
        let legacy = here.replace(" incarnation=i1", "");
        assert_eq!(trust(legacy.as_bytes(), b1), Some(false));
        assert_eq!(trust(here.as_bytes(), Some("b2")), Some(false));
        assert_eq!(trust(here.as_bytes(), None), Some(false));
        let unknown = format!(
            "7 2{}{none}",
            stamp(&db, "http://h:1/b/s", None, Some("i1"))
        );
        assert!(unknown.starts_with("7 2 boot=unknown "));
        assert_eq!(trust(unknown.as_bytes(), None), Some(false));
        // A claim on WAL frames that are not there (no WAL here), or no claim at all.
        let behind = format!("{mine} wal=00000000000000ff:3\n");
        assert_eq!(trust(behind.as_bytes(), b1), Some(false));
        assert_eq!(trust(format!("{mine}\n").as_bytes(), b1), Some(false));
        // Another db file (replaced by a rename) is not trusted.
        let other_file = "7 2 boot=b1 stream=/b/s file=0 incarnation=i1";
        assert_eq!(
            trust(format!("{other_file}{none}").as_bytes(), b1),
            Some(false)
        );
        assert_eq!(trust(b"7 2\n", b1), Some(false));
        for torn in ["", "7", "7 2 boot", "7 2 x=1", "7 2 wal=12", "7 2 wal=zz:1"] {
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
