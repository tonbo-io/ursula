//! The "ursula" VFS: the file methods of a bound database (design: Capture, Publication, Fsyncs,
//! Backpressure).
//!
//! WAL writes go straight to the WAL file. Each write transaction is admitted at its first WAL
//! write (`backlog_full` fails it with `SQLITE_IOERR_AUTH`, nothing written), and every WAL write
//! is fed to its [`WriteTxn`]. At the commit point, the write of the commit frame's page, a commit
//! of more bytes of pages than the commit limit (`URSULA_VFS_MAX_COMMIT_BYTES`) fails first, alone,
//! as `too_large` with `SQLITE_IOERR_AUTH`. Then the commit waits while the snapshot thread's
//! window is open, and at most a second for a wanted one (design: Snapshots), waits while another
//! connection's logged commit is undecided (at most 1 s, then the binding resets `local_damage`),
//! reads the transaction's final frames back from the WAL and checks their crc32c, encodes one
//! `USQ2` COMMIT frame, appends it to the log (one write, no fsync), hands it to the driver and
//! only then forwards the commit page. A failure before the log write fails the transaction alone.
//!
//! The snapshot thread's private connection opens through this VFS too (`opening_private`), so its
//! checkpoints take F1; it counts as a connection of the binding only beside one of the host's.
//!
//! A logged commit is final. SQLite publishes it when it sends `SQLITE_FCNTL_COMMIT_PHASETWO` on
//! the committing connection's main db handle as the very next call after the WAL write lock's
//! release ([`publication`]); any other call there first leaves it unconfirmed, and the binding
//! resets `local_damage`, so the reopen replays it from the log. The PHASETWO handler always
//! returns `SQLITE_OK` or `SQLITE_NOTFOUND`.
//!
//! Fsyncs: none per commit (`xSync` of a bound file is a no-op). F1: at `SQLITE_FCNTL_CKPT_START`
//! the log is fdatasynced before a checkpoint copies its first page. F2: before a new WAL
//! generation or a WAL truncate to nothing the db file is fsynced, then a BASE record names the
//! last confirmed commit. A failed fsync resets `local_damage`.
//!
//! Page 1 (design: Marker): on disk bytes 0..16 of a bound db file hold the generation's marker
//! ([`marker`]). `xRead` gives SQLite its magic in their place, `xWrite` stores the marker in place
//! of the magic, and `xFetch` maps nothing over them, so SQLite reads them through `xRead`. A read
//! that finds anything but the marker resets `local_damage` (`SQLITE_IOERR_READ`). The WAL of a
//! bound db is `db-uwal`: `xOpen` opens it in place of `db-wal`, and the VFS's own `xAccess` and
//! `xDelete` map SQLite's probes and deletes of `db-wal` to it. A generation is published with
//! page 1 written and marked (the page 1 of an empty database when it holds no commit), so its db
//! file is never empty or unmarked: unbound, it reads as no database (`SQLITE_NOTADB`), through
//! this VFS or any other, and SQLite outside the extension never takes it for a new one.
//!
//! After a reset, a new read transaction fails at its read-mark lock (`SQLITE_IOERR_READ`) and
//! every write fails (`SQLITE_IOERR_WRITE`), db-file writes included, so no checkpoint moves pages
//! of a commit that may be discarded. A panic in a file method resets `internal`.
//!
//! While a host holds the host directory's `hold` file, a write transaction of the host's
//! connection to an owner binding waits in `xShmLock` before its WAL write lock request goes on,
//! at most 60 s, holding only its read mark ([`hold`], design: Hosts).
//!
//! In a child forked after its parent started the extension (design: Forks, [`fork`]) every
//! method on a file of a bound database fails without running, with the code it answers after a
//! panic, and touches nothing: the binding's state, its log and its "unix" inode may be held by a
//! thread that stayed in the parent. Closing such a file leaves its state and its "unix" file as
//! they are. The child opens no file of a bound database (`SQLITE_CANTOPEN`), and `xAccess` and
//! `xDelete` refuse their names. Every other file passes through. Whether a file is a bound
//! database's is its own state for a file the child inherited, and the registry's answer for a name
//! ([`Part`]). `xShmMap` lists the wal-index regions of main db handles, which the child gets as
//! private copies.
//!
//! A reader binding's db file opens read-only whatever the connection asked (design: Readers), so
//! SQLite refuses its writes with `SQLITE_READONLY`, and a write or truncate that reaches one of
//! its files anyway is refused the same way: a reader logs no commit.
//!
//! SQLite calls these methods with pointers it owns, under the VFS contract every unsafe block here
//! relies on: each `file` is one this VFS opened and has not closed, laid out as [`File`] in the
//! `szOsFile` bytes SQLite allocated for it (`x_open` fills it in), and buffers and out-parameters
//! are valid for the sizes SQLite passes with them.
//!
//! [`hold`]: crate::hold
//! [`fork`]: crate::fork
//! [`publication`]: crate::publication
//! [`marker`]: crate::codec::marker
#![expect(
    unsafe_code,
    reason = "a SQLite VFS implements C callbacks over raw pointers SQLite owns"
)]

use std::cell::Cell;
use std::collections::BTreeMap;
use std::ffi::CStr;
use std::ffi::CString;
use std::ffi::c_char;
use std::ffi::c_int;
use std::ffi::c_void;
use std::ptr::null;
use std::ptr::null_mut;
use std::sync::Arc;
use std::sync::MutexGuard;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use libsqlite3_sys as ffi;

use crate::capture::CaptureError;
use crate::capture::CommitPoint;
use crate::capture::PAGE_BYTES;
use crate::capture::WalWrite;
use crate::capture::WriteTxn;
use crate::capture::classify;
use crate::capture::page_at;
use crate::codec::PAGE;
use crate::codec::frame::encode_commit;
use crate::codec::marker;
use crate::codec::segment::RecordBody;
use crate::config::Step;
use crate::config::abort_at;
use crate::config::fail_sync;
use crate::db::Binding;
use crate::db::Commit;
use crate::db::LogRefused;
use crate::db::Refusal;
use crate::db::Window;
use crate::db::lookup;
use crate::driver::Connected;
use crate::driver::Holder;
use crate::driver::runtime::driver;
use crate::error::WriteRefusal;
use crate::fork;
use crate::guard::contain;
use crate::hold;
use crate::host::OK;
use crate::host::unix;
use crate::ids::CommitId;
use crate::log;
use crate::log::Level;
use crate::publication::Call;
use crate::publication::HandleState;
use crate::publication::Verdict;
use crate::publication::phase_two_reply;
use crate::replicator::Pending;
use crate::replicator::ResetKind;
use crate::replog::Log;
use crate::replog::LogError;
use crate::stats::CommitStat;

/// `WAL_WRITE_LOCK` and `WAL_CKPT_LOCK`: the shm lock slots SQLite takes for a write transaction
/// and a checkpoint.
const WAL_WRITE_LOCK: c_int = 0;
const WAL_CKPT_LOCK: c_int = 1;

/// `WAL_READ_LOCK(0)`: the first of the read-mark lock slots, one of which every read transaction
/// takes shared.
const WAL_READ_LOCK_0: c_int = 3;

/// `WAL_NREADER`: the read-mark lock slots, `WAL_READ_LOCK_0` and the four after it.
const WAL_NREADER: c_int = 5;

/// The longest a commit waits at its commit point for another connection's logged commit to be
/// decided (design: Publication).
const UNDECIDED_WAIT: Duration = Duration::from_secs(1);

/// The longest a commit waits at its commit point for a wanted snapshot window to open.
const WINDOW_WAIT: Duration = Duration::from_secs(1);

/// How often a commit waiting on a snapshot window looks at it again: the window's close wakes
/// it, this only bounds a missed wake-up.
const WINDOW_POLL: Duration = Duration::from_millis(100);

thread_local! {
    /// The extension's private connection is opening on this thread (`opening_private`).
    static PRIVATE: Cell<bool> = const { Cell::new(false) };
    /// The WAL write locks of bound databases taken on this thread and not yet released: a write
    /// transaction that holds one never waits at the hold for another (design: Hosts). SQLite may
    /// move a connection to another thread between taking a write lock and releasing it, so the
    /// handle that took one keeps this counter (`Role::Main`'s `writing`) and releases it there,
    /// on whatever thread. Read with `try_with`: SQLite may run on a thread whose thread-locals
    /// are being destroyed, and a write there never waits.
    static WRITE_LOCKS: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));
}

/// Runs `open`, the open of the extension's private connection (`host::Private`), so that it
/// counts as a connection of its binding only while the binding has one already: it never takes
/// a retiring binding back, so it never keeps a binding no host connection holds.
pub(crate) fn opening_private<R>(open: impl FnOnce() -> R) -> R {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            PRIVATE.set(false);
        }
    }
    PRIVATE.set(true);
    let _reset = Reset;
    open()
}

/// A file of this VFS: its own header and state, then the "unix" file it wraps.
#[repr(C)]
pub(crate) struct File {
    base: ffi::sqlite3_file,
    /// The main db or WAL handle of a bound database. Null otherwise.
    ext: *mut Ext,
    /// Test hook `URSULA_VFS_FAIL_SYNC`: this file's `xSync` fails.
    fail_sync: bool,
    /// The name the "unix" file of a bound db's WAL was opened under (`db-uwal`), which "unix"
    /// keeps as its path until the file closes. Null otherwise.
    wal_name: *mut WalName,
    /// The underlying "unix" file (szOsFile bytes from here).
    pub(crate) inner: ffi::sqlite3_file,
}

struct Ext {
    binding: Arc<Binding>,
    role: Role,
}

/// A WAL name as SQLite lays file names out for its VFSs: four NULs before it (where
/// `sqlite3_filename_database` stops looking back) and two after it (no URI parameters).
struct WalName(Box<[u8]>);

impl WalName {
    fn new(name: &str) -> WalName {
        let mut bytes = vec![0u8; 4];
        bytes.extend_from_slice(name.as_bytes());
        bytes.extend_from_slice(&[0, 0]);
        WalName(bytes.into_boxed_slice())
    }

    /// The NUL-terminated name.
    fn as_ptr(&self) -> *const c_char {
        self.0.get(4..).unwrap_or_default().as_ptr().cast()
    }
}

/// A file SQLite opens for a database: the db file itself, its WAL (`<db>-wal`) or its rollback
/// journal (`<db>-journal`). The one rule from a file's name to its database's, for `x_open`,
/// [`uwal`] and a forked child's refusals.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Part {
    Db,
    Wal,
    Journal,
}

impl Part {
    const ALL: [Part; 3] = [Part::Db, Part::Wal, Part::Journal];

    /// The part `xOpen`'s `flags` open: `None` for a file of no database's (a temp file, a
    /// statement journal, a super-journal).
    fn opened(flags: c_int) -> Option<Part> {
        if flags & ffi::SQLITE_OPEN_MAIN_DB != 0 {
            Some(Part::Db)
        } else if flags & ffi::SQLITE_OPEN_WAL != 0 {
            Some(Part::Wal)
        } else if flags & ffi::SQLITE_OPEN_MAIN_JOURNAL != 0 {
            Some(Part::Journal)
        } else {
            None
        }
    }

    /// The db file `name` is this part of, when it is this part's name.
    fn db_of(self, name: &str) -> Option<&str> {
        match self {
            Part::Db => Some(name),
            Part::Wal => name.strip_suffix("-wal"),
            Part::Journal => name.strip_suffix("-journal"),
        }
    }
}

/// The WAL's name for `name`, a name SQLite gives a WAL (`<db>-wal`), when `bound(<db>)`: the bound
/// db's WAL is `<db>-uwal`. `None` for any other name, which passes unchanged.
fn uwal(name: &str, bound: impl FnOnce(&str) -> bool) -> Option<String> {
    Part::Wal
        .db_of(name)
        .filter(|db| bound(db))
        .map(|db| format!("{db}-uwal"))
}

enum Role {
    /// A main db handle: whose connection it is, where its commit stands, whether it holds an
    /// EXCLUSIVE file lock, the read-mark lock slots it holds shared (bit `i` for
    /// `WAL_READ_LOCK(i)`), and while it holds the WAL write lock, the `WRITE_LOCKS` counter of
    /// the thread that took it.
    Main {
        holder: Holder,
        state: Cell<HandleState>,
        exclusive: Cell<bool>,
        marks: Cell<u8>,
        writing: Cell<Option<Arc<AtomicU32>>>,
    },
    Wal,
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

/// This VFS's state for `f`: `None` for a file of no bound database.
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

/// Whether `f` is a file of a bound database in a forked child (design: Forks): one it inherited,
/// since the child opens none. The first such refusal says so on stderr ([`fork::refused`]).
///
/// # Safety
///
/// `f` is an open file of this VFS.
unsafe fn inherited(f: *mut ffi::sqlite3_file) -> bool {
    // SAFETY: the caller's contract.
    let inherited = fork::forked() && unsafe { ext(f) }.is_some();
    if inherited {
        fork::refused();
    }
    inherited
}

/// Whether `name` is a part of a bound database ([`Part`]).
fn of_bound(name: &str) -> bool {
    Part::ALL
        .into_iter()
        .filter_map(|part| part.db_of(name))
        .any(|db| lookup(db).is_some())
}

/// In a forked child (design: Forks): whether `zname` names a part of a bound database, whose
/// open, access check and delete it refuses. The first such refusal says so on stderr
/// ([`fork::refused`]).
///
/// # Safety
///
/// `zname` is null or NUL-terminated.
unsafe fn inherited_name(zname: *const c_char) -> bool {
    if !fork::forked() || zname.is_null() {
        return false;
    }
    // SAFETY: the caller's contract.
    let refused = unsafe { CStr::from_ptr(zname) }
        .to_str()
        .is_ok_and(of_bound);
    if refused {
        fork::refused();
    }
    refused
}

/// fsyncs the "unix" file inside `file`, bypassing this VFS's `xSync`, which is a no-op for a
/// bound database: the snapshot thread's db fsync before its BASE (design: Snapshots).
///
/// # Safety
///
/// `file` is an open file of this VFS.
pub(crate) unsafe fn sync_unix(file: *mut ffi::sqlite3_file) -> c_int {
    fwd!(file, xSync, ffi::SQLITE_SYNC_NORMAL)
}

/// The open main db handle at address `addr`, which a binding recorded (`Commit::writer`,
/// `Commit::exclusive`) while that handle holds its lock.
fn handle_at(addr: usize) -> *mut ffi::sqlite3_file {
    std::ptr::with_exposed_provenance_mut::<ffi::sqlite3_file>(addr)
}

/// A main db handle's call, fed to its [`HandleState`], with the verdict carried out under the
/// binding's state. Most calls decide nothing and change nothing, and take no lock.
///
/// # Safety
///
/// `f` is an open file of this VFS.
unsafe fn observe(f: *mut ffi::sqlite3_file, call: Call) -> Verdict {
    // SAFETY: the caller's contract.
    let Some(ext) = (unsafe { ext(f) }) else {
        return Verdict::None;
    };
    let Role::Main { state, .. } = &ext.role else {
        return Verdict::None;
    };
    let before = state.get();
    let mut after = before;
    let verdict = after.on(call);
    if verdict == Verdict::None && after.undecided() == before.undecided() {
        state.set(after);
        return verdict;
    }
    let mut c = ext.binding.lock();
    state.set(after);
    settle(&ext.binding, &mut c, f.expose_provenance(), after, verdict);
    verdict
}

/// Carries out a verdict on handle `addr`, now at `after`, under the binding's state.
fn settle(b: &Binding, c: &mut Commit, addr: usize, after: HandleState, verdict: Verdict) {
    match after.undecided() {
        Some(p) => {
            c.undecided.insert(addr, p);
        }
        None => {
            c.undecided.remove(&addr);
        }
    }
    match verdict {
        Verdict::Confirmed(p) => {
            c.confirmed = c.confirmed.max(p);
            abort_at(Step::PhaseTwo);
        }
        Verdict::Unconfirmed(p) => b.reset(
            ResetKind::LocalDamage,
            &format_args!(
                "the commit logged at {} was not confirmed as published",
                p.0
            ),
        ),
        Verdict::None | Verdict::Unsupported => {}
    }
    b.notify_decided();
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
    // A forked child opens no file of a bound database (design: Forks).
    // SAFETY: a non-null name is NUL-terminated (xOpen's contract).
    if unsafe { inherited_name(zname) } {
        return ffi::SQLITE_CANTOPEN;
    }
    let bound = name.zip(Part::opened(flags)).and_then(|(name, part)| {
        let db = part.db_of(name)?;
        Some((part, db, lookup(db)?))
    });
    let mut connected = None;
    let ext = match bound {
        // A rollback journal of a bound database would bypass the log: its db file has WAL format
        // from the start, so no commit needs one.
        Some((Part::Journal, db, _)) => {
            log::emit(Level::Warn, "journal_refused", &[("file", &db)]);
            return ffi::SQLITE_CANTOPEN;
        }
        Some((Part::Db, name, binding)) => {
            let holder = if PRIVATE.get() {
                Holder::Private
            } else {
                Holder::Host
            };
            connected = binding.connect(holder);
            if connected.is_none() {
                // The private connection is refused beside no connection of the host: the
                // snapshot attempt skips, nothing to report.
                if holder == Holder::Host {
                    log::emit(Level::Warn, "open_refused", &[
                        ("file", &name),
                        ("reason", &"its binding retired: ursula_open it again"),
                    ]);
                }
                return ffi::SQLITE_CANTOPEN;
            }
            Some(Ext {
                binding,
                role: Role::Main {
                    holder,
                    state: Cell::new(HandleState::Idle),
                    exclusive: Cell::new(false),
                    marks: Cell::new(0),
                    writing: Cell::new(None),
                },
            })
        }
        Some((Part::Wal, _, binding)) => Some(Ext {
            binding,
            role: Role::Wal,
        }),
        None => None,
    };
    // Released when the open fails below, also when a panic unwinds through it.
    let mut counted = Counted(
        ext.as_ref()
            .zip(connected)
            .map(|(e, connected)| (Arc::clone(&e.binding), connected)),
    );
    // A reader binding's db file opens read-only, so SQLite refuses every write on it with
    // `SQLITE_READONLY` before one reaches the file (design: Readers).
    let flags = match &ext {
        Some(e) if e.is_main() && e.binding.handle.role == crate::driver::Role::Reader => {
            (flags & !(ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE))
                | ffi::SQLITE_OPEN_READONLY
        }
        _ => flags,
    };
    // A bound db's WAL opens as `db-uwal`, under a name the file owns until it closes ("unix"
    // keeps it as its path). Without URI parameters: the WAL takes none of its own.
    let (zname, flags) = match (&ext, name) {
        (
            Some(Ext {
                role: Role::Wal, ..
            }),
            Some(name),
        ) => {
            let Some(uwal) = uwal(name, |_bound| true) else {
                return ffi::SQLITE_CANTOPEN;
            };
            let mapped = Box::into_raw(Box::new(WalName::new(&uwal)));
            // SAFETY: as above; freed by `take_wal_name` when the open fails or the file closes.
            unsafe { (*f).wal_name = mapped };
            // SAFETY: `mapped` was just leaked above and lives until `take_wal_name`.
            let ptr = unsafe { (*mapped).as_ptr() };
            (ptr, flags & !ffi::SQLITE_OPEN_URI)
        }
        _ => (zname, flags),
    };
    let u = unix();
    // SAFETY: `UNIX` is null or SQLite's "unix" VFS, which lives for the process.
    let open = unsafe { u.as_ref() }.and_then(|u| u.xOpen);
    // SAFETY: the "unix" file lies within SQLite's allocation for this file (`szOsFile` counts
    // it), and the other arguments are SQLite's, or the WAL name the file owns.
    let rc = open.map_or(ffi::SQLITE_CANTOPEN, |open| unsafe {
        open(u, zname, inner(file), flags, out)
    });
    if rc != OK {
        // SAFETY: the open failed, so SQLite never closes the file: its name is freed here.
        drop(unsafe { take_wal_name(file) });
        return rc;
    }
    if ext.is_none() {
        let failing = fail_sync().zip(name).is_some_and(|(s, n)| n.ends_with(s));
        // SAFETY: as above.
        unsafe { (*f).fail_sync = failing };
    }
    if let Some(ext) = ext {
        if !ext.is_main() {
            let mut c = ext.binding.lock();
            c.wal_open = c.wal_open.saturating_add(1);
        }
        // SAFETY: as above.
        unsafe { (*f).ext = Box::into_raw(Box::new(ext)) };
    }
    // SAFETY: as above. From here the file is open, and SQLite calls its methods.
    unsafe { (*f).base.pMethods = &raw const METHODS };
    counted.0 = None;
    OK
}

impl Ext {
    fn is_main(&self) -> bool {
        matches!(self.role, Role::Main { .. })
    }
}

/// A main db handle counted by [`Binding::connect`] while `x_open` runs, undone when dropped
/// before the open succeeded (a failed open, or a panic unwinding through it): the binding goes
/// back to where it stood, a hand-out the handle took up waiting again.
struct Counted(Option<(Arc<Binding>, Connected)>);

impl Drop for Counted {
    fn drop(&mut self) {
        if let Some((binding, connected)) = self.0.take() {
            binding.unconnect(connected);
        }
    }
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
    unsafe { (*f).fail_sync = false };
    // SAFETY: as above.
    unsafe { (*f).wal_name = null_mut() };
    // SAFETY: as above.
    unsafe { (*f).inner.pMethods = null() };
}

/// Takes the WAL name out of `file`, once its "unix" file is closed or its open failed.
///
/// # Safety
///
/// `file` is a file of this VFS that `not_open` set up, whose `wal_name` is null or the `Box`
/// `x_open` made, taken once.
unsafe fn take_wal_name(file: *mut ffi::sqlite3_file) -> Option<Box<WalName>> {
    let f = file.cast::<File>();
    // SAFETY: the caller's contract; the name is taken once, here.
    let name = unsafe { std::mem::replace(&mut (*f).wal_name, null_mut()) };
    if name.is_null() {
        None
    } else {
        // SAFETY: a non-null `wal_name` is the `Box` `x_open` leaked for this file.
        Some(unsafe { Box::from_raw(name) })
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

/// The binding's bookkeeping of a handle that closes, at address `addr`: a main db handle's
/// connection ends its transaction (an undecided commit becomes unconfirmed) and releases its
/// EXCLUSIVE lock, and after the last one the binding retires; a WAL handle is uncounted.
fn closing(ext: &Ext, addr: usize) {
    uncount_write(ext);
    let b = &ext.binding;
    match &ext.role {
        Role::Main {
            holder,
            state,
            exclusive,
            ..
        } => {
            {
                let mut c = b.lock();
                let mut after = state.get();
                let verdict = after.on(Call::Close);
                state.set(after);
                settle(b, &mut c, addr, after, verdict);
                if exclusive.get() {
                    c.exclusive = 0;
                }
                if c.writer == addr {
                    c.writer = 0;
                    c.txn = None;
                }
            }
            b.disconnect(*holder);
        }
        Role::Wal => {
            let mut c = b.lock();
            c.wal_open = c.wal_open.saturating_sub(1);
        }
    }
}

/// The first page of `buf` (read at `off`) whose bytes differ from what this process wrote there
/// (`written`, by offset).
fn lost_page(written: &BTreeMap<i64, u32>, buf: &[u8], off: i64) -> Option<i64> {
    let end = off.saturating_add(i64::try_from(buf.len()).unwrap_or(i64::MAX));
    written
        .range(off..end)
        .filter(|&(&p, _)| p.saturating_add(PAGE_BYTES) <= end)
        .find_map(|(&p, &crc)| {
            let at = usize::try_from(p.saturating_sub(off)).ok()?;
            let page = buf.get(at..at.saturating_add(PAGE))?;
            (crc32c::crc32c(page) != crc).then_some(p)
        })
}

unsafe fn x_read(
    file: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    amt: c_int,
    off: ffi::sqlite3_int64,
) -> c_int {
    let rc = fwd!(file, xRead, buf, amt, off);
    // SAFETY: the module contract.
    let Some(ext) = (unsafe { ext(file) }) else {
        return rc;
    };
    if rc != OK && rc != ffi::SQLITE_IOERR_SHORT_READ {
        return rc;
    }
    let Ok(len) = usize::try_from(amt) else {
        return ffi::SQLITE_IOERR_READ;
    };
    if ext.is_main() {
        let Ok(at) = u64::try_from(off) else {
            return rc;
        };
        if at >= marker::LEN as u64 {
            return rc;
        }
        // SAFETY: SQLite passes `amt` writable bytes at `buf`, which the read just filled.
        let data = unsafe { std::slice::from_raw_parts_mut(buf.cast::<u8>(), len) };
        let lineage = ext.binding.handle.generation.lineage;
        if let Err(e) = marker::on_read(data, at, lineage) {
            ext.binding.reset(ResetKind::LocalDamage, &e);
            return ffi::SQLITE_IOERR_READ;
        }
        return rc;
    }
    // SAFETY: SQLite passes `amt` bytes at `buf`, which the read just filled.
    let data = unsafe { std::slice::from_raw_parts(buf.cast::<u8>(), len) };
    // A page this process wrote that reads back other bytes was lost on its way to the disk (a
    // write-back error, after which the kernel dropped the dirty page): SQLite does not check WAL
    // frames on a read, so commits built on it would log the damage.
    let lost = lost_page(&ext.binding.wal_written(), data, off);
    if let Some(at) = lost {
        ext.binding.reset(
            ResetKind::LocalDamage,
            &format_args!("the WAL page at offset {at} reads back other bytes than were written"),
        );
        return ffi::SQLITE_IOERR_READ;
    }
    rc
}

/// Fails the write transaction alone, nothing of it logged: `SQLITE_IOERR_AUTH`, and the refusal
/// stays in status until a commit is logged, and names the error until the next refusal.
fn refuse(b: &Binding, c: &mut Commit, refusal: WriteRefusal) -> c_int {
    if c.standing_refusal() != Some(&refusal) {
        log::emit(Level::Warn, "write_refused", &[
            ("file", &b.path),
            ("kind", &refusal.kind().as_str()),
            ("reason", &refusal),
        ]);
    }
    c.refusal = Some(Refusal {
        what: refusal,
        standing: true,
    });
    ffi::SQLITE_IOERR_AUTH
}

unsafe fn x_write(
    file: *mut ffi::sqlite3_file,
    buf: *const c_void,
    amt: c_int,
    off: ffi::sqlite3_int64,
) -> c_int {
    // SAFETY: the module contract.
    let Some(ext) = (unsafe { ext(file) }) else {
        return fwd!(file, xWrite, buf, amt, off);
    };
    let b = &ext.binding;
    if b.reset_kind().is_some() {
        return ffi::SQLITE_IOERR_WRITE;
    }
    if b.handle.role == crate::driver::Role::Reader {
        return ffi::SQLITE_READONLY;
    }
    let Ok(len) = usize::try_from(amt) else {
        return ffi::SQLITE_IOERR_WRITE;
    };
    // SAFETY: SQLite passes `amt` readable bytes at `buf`.
    let data = unsafe { std::slice::from_raw_parts(buf.cast::<u8>(), len) };
    if ext.is_main() {
        let mut c = b.lock();
        if !c.db_write_allowed() {
            return refuse(
                b,
                &mut c,
                WriteRefusal::Unsupported(
                    "a db file write outside a checkpoint: the connection left WAL mode".into(),
                ),
            );
        }
        // Page 1 goes to disk with the marker in place of SQLite's magic.
        let at = u64::try_from(off).unwrap_or(u64::MAX);
        let marked = match marker::on_write(data, at, b.handle.generation.lineage) {
            Ok(marked) => marked,
            Err(e) => {
                b.reset(ResetKind::LocalDamage, &e);
                return ffi::SQLITE_IOERR_WRITE;
            }
        };
        let rc = match &marked {
            Some(bytes) => fwd!(file, xWrite, bytes.as_ptr().cast::<c_void>(), amt, off),
            None => fwd!(file, xWrite, buf, amt, off),
        };
        if let Some((_, first @ false)) = &mut c.checkpoint {
            *first = true;
            abort_at(Step::CheckpointPage);
        }
        return rc;
    }
    // SAFETY: `file` is the binding's open WAL handle.
    unsafe { wal_write(file, b, data, off) }
}

/// A WAL write of a bound database (see the module docs).
///
/// # Safety
///
/// `file` is the open WAL handle of `b`.
unsafe fn wal_write(file: *mut ffi::sqlite3_file, b: &Binding, data: &[u8], off: i64) -> c_int {
    let mut c = b.lock();
    if c.writer == 0 {
        // EXCLUSIVE locking mode keeps the wal-index in heap memory and takes no shm lock.
        return refuse(
            b,
            &mut c,
            WriteRefusal::Unsupported(
                "a WAL write without the WAL write lock (EXCLUSIVE locking mode)".into(),
            ),
        );
    }
    let writer = c.writer;
    // SAFETY: the writer is the open main db handle of this write's connection.
    if let Some(Role::Main { state, .. }) = unsafe { ext(handle_at(writer)) }.map(|e| &e.role) {
        let mut after = state.get();
        let verdict = after.on(Call::WalWrite);
        state.set(after);
        if verdict == Verdict::Unsupported {
            return refuse(
                b,
                &mut c,
                WriteRefusal::Unsupported("a WAL write outside a write transaction".into()),
            );
        }
        settle(b, &mut c, writer, after, verdict);
    }
    if c.txn.is_none() {
        // Admission (design: Backpressure): at the first WAL write, never at the write lock, so
        // wal-index recovery is never refused.
        if let Err(full) = b.handle.admit(0) {
            return refuse(b, &mut c, WriteRefusal::BacklogFull {
                pending: full.pending,
                max: full.max,
            });
        }
        abort_at(Step::Admission);
        c.txn = Some(WriteTxn::new());
    }
    let w = classify(off, data);
    let point = match c.txn.as_mut().map(|txn| txn.on_write(&w)) {
        Some(Ok(point)) => point,
        Some(Err(e)) => return refuse(b, &mut c, WriteRefusal::Unsupported(e.to_string())),
        None => None,
    };
    if let WalWrite::Header { .. } = w {
        // SAFETY: the binding's writer holds the WAL write lock.
        let rc = unsafe { base(b, &mut c, "a new WAL generation") };
        if rc != OK {
            return rc;
        }
        b.wal_written().clear();
    }
    if let Some(point) = point {
        // SAFETY: the caller's contract.
        return unsafe { commit(file, b, c, point, data, off) };
    }
    forward(file, b, data, off)
}

/// Forwards a WAL write and records the crc32c of a page written.
fn forward(file: *mut ffi::sqlite3_file, b: &Binding, data: &[u8], off: i64) -> c_int {
    let Ok(amt) = c_int::try_from(data.len()) else {
        return ffi::SQLITE_IOERR_WRITE;
    };
    let rc = fwd!(file, xWrite, data.as_ptr().cast::<c_void>(), amt, off);
    if rc == OK && data.len() == PAGE && page_at(off) {
        b.wal_written().insert(off, crc32c::crc32c(data));
    }
    rc
}

/// F2 (design: Fsyncs): fsyncs the db file through the main db handle holding the WAL write lock
/// (or a closing connection's EXCLUSIVE lock), then appends a BASE record naming the last confirmed
/// commit, before the WAL starts a new generation or is truncated to nothing. A failure resets
/// `local_damage`.
///
/// # Safety
///
/// `c.writer` and `c.exclusive` name open main db handles of `b`, or are 0.
unsafe fn base(b: &Binding, c: &mut Commit, before: &'static str) -> c_int {
    abort_at(Step::BaseStart);
    let rc = match [c.writer, c.exclusive].into_iter().find(|&h| h != 0) {
        Some(h) => fwd!(handle_at(h), xSync, ffi::SQLITE_SYNC_NORMAL),
        None => ffi::SQLITE_IOERR_FSYNC,
    };
    if rc != OK {
        b.reset(
            ResetKind::LocalDamage,
            &format_args!("the db file's fsync before {before} failed ({rc})"),
        );
        return rc;
    }
    abort_at(Step::BaseSynced);
    if c.confirmed > c.based {
        let position = c.confirmed;
        let appended = match b.log_unless_reset() {
            Ok(mut log) => log.append(&RecordBody::Base { position }).map(drop),
            Err(LogRefused::Reset(_)) => return ffi::SQLITE_IOERR_WRITE,
            Err(LogRefused::Poisoned(_)) => Err(LogError::Broken {
                dir: b.handle.generation.log_dir(),
            }),
        };
        if let Err(e) = appended {
            b.reset(
                ResetKind::LocalDamage,
                &format_args!("the BASE record: {e}"),
            );
            return ffi::SQLITE_IOERR_WRITE;
        }
        c.based = position;
    }
    abort_at(Step::Based);
    OK
}

/// The binding's log, for the commit point to append to: refused once the binding reset
/// ([`Binding::log_unless_reset`]).
fn commit_log(b: &Binding) -> Result<MutexGuard<'_, Log>, c_int> {
    b.log_unless_reset().map_err(|refused| {
        if let LogRefused::Poisoned(e) = refused {
            b.reset(ResetKind::Internal, &e);
        }
        ffi::SQLITE_IOERR_WRITE
    })
}

/// The commit point (design: Capture): the commit limit, capture, encode, log, hand to the driver,
/// forward.
///
/// # Safety
///
/// `file` is the open WAL handle of `b`, and `c.writer` its writer's main db handle.
unsafe fn commit(
    file: *mut ffi::sqlite3_file,
    b: &Binding,
    mut c: MutexGuard<'_, Commit>,
    point: CommitPoint,
    page: &[u8],
    off: i64,
) -> c_int {
    let started = Instant::now();
    // The commit limit, before anything is waited for, read back or logged: a commit above it
    // fails alone and the replicator never sees it.
    let size = c
        .txn
        .as_ref()
        .map(|txn| txn.check_size(point, b.handle.max_commit_bytes));
    if let Some(Err(e)) = size {
        return refuse(b, &mut c, WriteRefusal::TooLarge {
            bytes: e.bytes,
            max: e.max,
        });
    }
    // A snapshot's window (design: Snapshots): the commit waits while it is open, so the snapshot's
    // read sees exactly its `k`, and at most a second for a wanted one to open, so a writer
    // committing back to back cannot starve it.
    let deadline = started.checked_add(WINDOW_WAIT).unwrap_or(started);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let wait = match c.window {
            Window::Open => WINDOW_POLL,
            Window::Wanted if !left.is_zero() => left.min(WINDOW_POLL),
            Window::Wanted | Window::Closed => break,
        };
        c = b.wait_decided(c, wait);
        if b.reset_kind().is_some() {
            return ffi::SQLITE_IOERR_WRITE;
        }
    }
    // Nothing is logged on top of another connection's undecided commit.
    let deadline = Instant::now()
        .checked_add(UNDECIDED_WAIT)
        .unwrap_or(started);
    while c.undecided.keys().any(|&h| h != c.writer) {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            b.reset(
                ResetKind::LocalDamage,
                &"another connection's logged commit stayed undecided for a second",
            );
            return ffi::SQLITE_IOERR_WRITE;
        }
        c = b.wait_decided(c, left);
        if b.reset_kind().is_some() {
            return ffi::SQLITE_IOERR_WRITE;
        }
    }
    abort_at(Step::Capture);
    let read_at = |at: i64, image: &mut [u8; PAGE]| -> std::io::Result<()> {
        let amt = c_int::try_from(PAGE).unwrap_or(c_int::MAX);
        let rc = fwd!(file, xRead, image.as_mut_ptr().cast::<c_void>(), amt, at);
        if rc == OK {
            Ok(())
        } else {
            Err(std::io::Error::other(format!("xRead failed ({rc})")))
        }
    };
    let captured = c.txn.as_ref().map(|txn| txn.capture(point, page, read_at));
    let record = match captured {
        Some(Ok(record)) => record,
        Some(Err(e @ (CaptureError::LostWrite { .. } | CaptureError::ReadBack { .. }))) => {
            b.reset(ResetKind::LocalDamage, &e);
            return ffi::SQLITE_IOERR_WRITE;
        }
        Some(Err(e)) => return refuse(b, &mut c, WriteRefusal::Unsupported(e.to_string())),
        None => return ffi::SQLITE_IOERR_WRITE,
    };
    let raw = record.pages.len().saturating_mul(PAGE);
    let pages = record.pages.len();
    let mut log = match commit_log(b) {
        Ok(log) => log,
        Err(rc) => return rc,
    };
    let id = CommitId {
        lineage: b.handle.generation.lineage,
        position: log.next_position(),
    };
    let frame = match encode_commit(id, &record) {
        Ok(frame) => frame,
        Err(e) => {
            drop(log);
            b.reset(ResetKind::Internal, &format_args!("encoding a commit: {e}"));
            return ffi::SQLITE_IOERR_WRITE;
        }
    };
    // The pages are in the frame now: free them before the log write copies it.
    drop(record);
    let bytes = frame.len();
    abort_at(Step::Encode);
    let logging = Instant::now();
    let body = RecordBody::Commit { frame };
    let appended = log.append(&body);
    if let Ok(position) = appended {
        // Under the log's lock: `synced()` targets it from now on, and admission counts it.
        b.handle.record_commit(position);
        b.handle
            .add_pending(u64::try_from(bytes).unwrap_or(u64::MAX));
    }
    drop(log);
    let log_time = logging.elapsed();
    let position = match appended {
        Ok(position) => position,
        // The record was cut back: the transaction fails alone, nothing logged. Anything else (a
        // failed roll, a failed cut) may have left the log changed: the binding resets.
        Err(e @ LogError::CutBack { .. }) => {
            log::emit(Level::Warn, "log_write_failed", &[
                ("file", &b.path),
                ("error", &e),
            ]);
            return ffi::SQLITE_IOERR_WRITE;
        }
        Err(e) => {
            b.reset(ResetKind::LocalDamage, &e);
            return ffi::SQLITE_IOERR_WRITE;
        }
    };
    abort_at(Step::Logged);
    let RecordBody::Commit { frame } = body else {
        return ffi::SQLITE_IOERR_WRITE;
    };
    let pending = Pending {
        id: CommitId {
            lineage: id.lineage,
            position,
        },
        frame: Arc::from(frame),
    };
    if driver().logged(b.id, pending).is_err() {
        b.reset(ResetKind::Internal, &"the driver thread is gone");
        return ffi::SQLITE_IOERR_WRITE;
    }
    c.lift_refusal();
    let writer = c.writer;
    // SAFETY: the writer is the open main db handle of this transaction's connection.
    if let Some(Role::Main { state, .. }) = unsafe { ext(handle_at(writer)) }.map(|e| &e.role) {
        let mut after = state.get();
        let verdict = after.on(Call::Logged(position));
        state.set(after);
        settle(b, &mut c, writer, after, verdict);
    }
    let rc = forward(file, b, page, off);
    abort_at(Step::Forward);
    c.stats.commit(CommitStat {
        bytes,
        raw,
        pages,
        log: log_time,
        vfs: started.elapsed(),
    });
    rc
}

unsafe fn x_truncate(file: *mut ffi::sqlite3_file, size: ffi::sqlite3_int64) -> c_int {
    // SAFETY: the module contract.
    let Some(ext) = (unsafe { ext(file) }) else {
        return fwd!(file, xTruncate, size);
    };
    let b = &ext.binding;
    if b.reset_kind().is_some() {
        return ffi::SQLITE_IOERR_TRUNCATE;
    }
    if b.handle.role == crate::driver::Role::Reader {
        return ffi::SQLITE_READONLY;
    }
    let mut c = b.lock();
    if ext.is_main() {
        if !c.db_write_allowed() {
            return refuse(
                b,
                &mut c,
                WriteRefusal::Unsupported(
                    "a db file truncate outside a checkpoint: the connection left WAL mode".into(),
                ),
            );
        }
        return fwd!(file, xTruncate, size);
    }
    // Truncated to nothing (after a complete checkpoint, under the write lock or the closing
    // connection's EXCLUSIVE lock): F2 first. A truncate to a non-zero size cuts only frames
    // already checkpointed into the db past a restart, which F2 covered.
    if size == 0 {
        // SAFETY: the binding's writer or closing connection holds its lock.
        let rc = unsafe { base(b, &mut c, "a WAL truncate") };
        if rc != OK {
            return rc;
        }
    }
    b.wal_written()
        .retain(|&o, _| o.saturating_add(PAGE_BYTES) <= size);
    fwd!(file, xTruncate, size)
}

/// A no-op for the db file and WAL of a bound database, whatever `PRAGMA synchronous` says: the log
/// holds every commit, and F1 and F2 are the only fsyncs the local files need (design: Fsyncs).
unsafe fn x_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    // SAFETY: the module contract.
    if unsafe { ext(file) }.is_some() {
        return OK;
    }
    // SAFETY: the module contract.
    if unsafe { (*file.cast::<File>()).fail_sync } {
        return ffi::SQLITE_IOERR_FSYNC;
    }
    fwd!(file, xSync, flags)
}

unsafe fn x_file_size(file: *mut ffi::sqlite3_file, out: *mut ffi::sqlite3_int64) -> c_int {
    fwd!(file, xFileSize, out)
}

/// Tracks which main db handle holds an EXCLUSIVE file lock (see `Commit::db_write_allowed`).
///
/// # Safety
///
/// `file` is an open file of this VFS.
unsafe fn track_exclusive(file: *mut ffi::sqlite3_file, held: bool) {
    // SAFETY: the caller's contract.
    let Some(ext) = (unsafe { ext(file) }) else {
        return;
    };
    if let Role::Main { exclusive, .. } = &ext.role
        && exclusive.get() != held
    {
        exclusive.set(held);
        ext.binding.lock().exclusive = if held { file.expose_provenance() } else { 0 };
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

/// PHASETWO decides the connection's logged commit, CKPT_START is F1, and a bound database's WAL
/// persists past its last connection (checkpointed, not deleted). Every other call on a main db
/// handle is an `Other` call for publication.
unsafe fn x_file_control(file: *mut ffi::sqlite3_file, op: c_int, arg: *mut c_void) -> c_int {
    // SAFETY: the module contract.
    let main = unsafe { ext(file) }.filter(|e| e.is_main());
    let Some(ext) = main else {
        return fwd!(file, xFileControl, op, arg);
    };
    match op {
        ffi::SQLITE_FCNTL_COMMIT_PHASETWO => {
            // SAFETY: the module contract.
            unsafe { observe(file, Call::PhaseTwo) };
            phase_two_reply(fwd!(file, xFileControl, op, arg))
        }
        ffi::SQLITE_FCNTL_CKPT_START => {
            // SAFETY: the module contract.
            unsafe { observe(file, Call::Other) };
            checkpoint_start(&ext.binding);
            fwd!(file, xFileControl, op, arg)
        }
        ffi::SQLITE_FCNTL_PERSIST_WAL => {
            // SAFETY: the module contract.
            unsafe { observe(file, Call::Other) };
            // SAFETY: PERSIST_WAL passes a pointer to one `int`, which it writes the setting to.
            unsafe { *arg.cast::<c_int>() = 1 };
            OK
        }
        _ => {
            // SAFETY: the module contract.
            unsafe { observe(file, Call::Other) };
            fwd!(file, xFileControl, op, arg)
        }
    }
}

/// F1 (design: Fsyncs): the log is fdatasynced before a checkpoint copies its first page, so every
/// page reaching the db file comes from a durable record. SQLite ignores what CKPT_START returns:
/// a failure resets `local_damage`, which refuses the checkpoint's db writes.
fn checkpoint_start(b: &Binding) {
    abort_at(Step::CheckpointStart);
    let synced = match b.handle.log() {
        Ok(mut log) => log.sync().map_err(|e| e.to_string()),
        Err(e) => Err(e.to_string()),
    };
    if let Err(e) = synced {
        b.reset(
            ResetKind::LocalDamage,
            &format_args!("the log's fdatasync before a checkpoint: {e}"),
        );
        return;
    }
    abort_at(Step::CheckpointSynced);
}

unsafe fn x_sector_size(file: *mut ffi::sqlite3_file) -> c_int {
    fwd!(file, xSectorSize)
}

unsafe fn x_device_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
    fwd!(file, xDeviceCharacteristics)
}

/// Maps a wal-index region through "unix", which maps it shared with every process that has the
/// file. A main db handle's region of a bound database is listed for a forked child, which makes
/// its copy private (design: Forks, [`fork`]).
unsafe fn x_shm_map(
    file: *mut ffi::sqlite3_file,
    pg: c_int,
    pgsz: c_int,
    extend: c_int,
    pp: *mut *mut c_void,
) -> c_int {
    let rc = fwd!(file, xShmMap, pg, pgsz, extend, pp);
    // A read-only wal-index maps too, answering `SQLITE_READONLY`.
    let mapped = rc == OK || (rc & 0xff) == ffi::SQLITE_READONLY;
    // SAFETY: the module contract.
    let main = unsafe { ext(file) }.is_some_and(Ext::is_main);
    if mapped && main && !pp.is_null() {
        // SAFETY: SQLite passes `pp` readable, and "unix" set it (null when it mapped nothing).
        let region = unsafe { *pp };
        if !region.is_null()
            && let Ok(len) = usize::try_from(pgsz)
        {
            fork::mapped(file.expose_provenance(), region, len);
        }
    }
    rc
}

/// Whether a WAL write lock request on `ext`, a main db handle, waits at the hold first (design:
/// Hosts): the host's connection to an owner binding, holding a read mark, so a write transaction
/// (SQLite takes the write lock only with a read mark held), on a thread that took no other bound
/// database's write lock still held (`WRITE_LOCKS`). A checkpoint of any mode and a wal-index recovery hold no read
/// mark when they ask for it, and the private connection and readers never wait.
fn waits_at_hold(ext: &Ext) -> bool {
    matches!(
        ext.role,
        Role::Main {
            holder: Holder::Host,
            ref marks,
            ..
        } if marks.get() != 0
    ) && ext.binding.handle.role == crate::driver::Role::Owner
        && WRITE_LOCKS
            .try_with(|count| count.load(Ordering::Relaxed))
            .is_ok_and(|held| held == 0)
}

/// Counts the WAL write lock `ext`, a main db handle, was granted on this thread's `WRITE_LOCKS`,
/// and keeps that counter in the handle until the lock is released. A thread whose thread-locals
/// are being destroyed counts nothing: it never waits at the hold again.
fn count_write(ext: &Ext) {
    if let Role::Main { writing, .. } = &ext.role {
        let count = WRITE_LOCKS.try_with(Arc::clone).ok();
        if let Some(count) = &count {
            count.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(stale) = writing.replace(count) {
            stale.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// Uncounts the WAL write lock `ext`, a main db handle, released (or held when it closes), on the
/// counter of the thread that took it, whatever thread this is.
fn uncount_write(ext: &Ext) {
    if let Role::Main { writing, .. } = &ext.role
        && let Some(count) = writing.take()
    {
        count.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The read-mark lock slots among `n` slots from `offset`, as `Role::Main`'s `marks` has them.
fn read_marks(offset: c_int, n: c_int) -> u8 {
    (0..WAL_NREADER)
        .filter(|i| {
            let slot = WAL_READ_LOCK_0.saturating_add(*i);
            slot >= offset && slot < offset.saturating_add(n)
        })
        .filter_map(|i| u32::try_from(i).ok().and_then(|i| 1u8.checked_shl(i)))
        .fold(0, |mask, bit| mask | bit)
}

/// Feeds the WAL write lock to publication and tracks the write transaction and checkpoints of a
/// bound database; SQLite takes both locks through the main db handle. The end of a write
/// transaction runs under the binding's mutex *before* the real lock is released: once it is
/// released, another connection may start its own write transaction, whose state the bookkeeping
/// would otherwise clear. Lock order: the real shm lock may be taken without the mutex, and the
/// mutex may be held while releasing it, which never blocks. A write transaction's write lock
/// request first waits at the hold (`waits_at_hold`), with no mutex and no WAL write lock held.
unsafe fn x_shm_lock(file: *mut ffi::sqlite3_file, offset: c_int, n: c_int, flags: c_int) -> c_int {
    // SAFETY: the module contract.
    let main = unsafe { ext(file) }.filter(|e| e.is_main());
    let Some(ext) = main else {
        return fwd!(file, xShmLock, offset, n, flags);
    };
    let b = &ext.binding;
    let tracked = flags & ffi::SQLITE_SHM_EXCLUSIVE != 0
        && n == 1
        && (offset == WAL_WRITE_LOCK || offset == WAL_CKPT_LOCK);
    let locking = flags & ffi::SQLITE_SHM_LOCK != 0;
    if tracked && offset == WAL_WRITE_LOCK {
        if locking {
            if waits_at_hold(ext)
                && let Some(hold) = hold::process_hold()
            {
                hold.wait();
            }
            let rc = fwd!(file, xShmLock, offset, n, flags);
            if rc == OK {
                count_write(ext);
            }
            let call = if rc == OK {
                Call::LockWrite
            } else {
                Call::Other
            };
            // SAFETY: the module contract.
            unsafe { observe(file, call) };
            if rc == OK {
                let mut c = b.lock();
                c.writer = file.expose_provenance();
                c.txn = None;
            }
            return rc;
        }
        let mut c = b.lock();
        if let Role::Main { state, .. } = &ext.role {
            let mut after = state.get();
            let verdict = after.on(Call::UnlockWrite);
            state.set(after);
            settle(b, &mut c, file.expose_provenance(), after, verdict);
        }
        c.writer = 0;
        c.txn = None;
        uncount_write(ext);
        return fwd!(file, xShmLock, offset, n, flags);
    }
    // SAFETY: the module contract.
    unsafe { observe(file, Call::Other) };
    // Every read transaction on a WAL database takes one of the read-mark locks shared when it
    // starts, even one whose pages its connection has cached: after a reset it fails, so no new
    // read transaction serves state that may be discarded. One open already keeps its snapshot.
    if locking
        && flags & ffi::SQLITE_SHM_SHARED != 0
        && offset >= WAL_READ_LOCK_0
        && b.reset_kind().is_some()
    {
        return ffi::SQLITE_IOERR_READ;
    }
    if !tracked {
        let rc = fwd!(file, xShmLock, offset, n, flags);
        if flags & ffi::SQLITE_SHM_SHARED != 0
            && let Role::Main { marks, .. } = &ext.role
        {
            let slots = read_marks(offset, n);
            if !locking {
                marks.set(marks.get() & !slots);
            } else if rc == OK {
                marks.set(marks.get() | slots);
            }
        }
        return rc;
    }
    if locking {
        let rc = fwd!(file, xShmLock, offset, n, flags);
        if rc == OK {
            b.lock().checkpoint = Some((Instant::now(), false));
        }
        return rc;
    }
    let mut c = b.lock();
    if let Some((started, _)) = c.checkpoint.take() {
        c.stats.checkpoint(started.elapsed());
    }
    // A snapshot waiting for no other checkpointer looks again.
    b.notify_decided();
    fwd!(file, xShmLock, offset, n, flags)
}

unsafe fn x_shm_barrier(file: *mut ffi::sqlite3_file) {
    // SAFETY: the module contract.
    let (i, methods) = unsafe { unix_file(file) };
    if let Some(barrier) = methods.and_then(|m| m.xShmBarrier) {
        // SAFETY: SQLite's call on this file, forwarded to the "unix" file inside it.
        unsafe { barrier(i) };
    }
}

/// Unmaps the wal-index through "unix", after the handle's regions left the forked child's list,
/// so each listed region is mapped ([`fork`]).
unsafe fn x_shm_unmap(file: *mut ffi::sqlite3_file, delete: c_int) -> c_int {
    fork::unmapping(file.expose_provenance());
    fwd!(file, xShmUnmap, delete)
}

/// Maps nothing over bytes 0..16 of a bound db file, which hold the marker on disk: SQLite reads
/// such a page through `xRead`, which translates them.
unsafe fn x_fetch(
    file: *mut ffi::sqlite3_file,
    off: ffi::sqlite3_int64,
    amt: c_int,
    pp: *mut *mut c_void,
) -> c_int {
    // SAFETY: the module contract.
    let main = unsafe { ext(file) }.is_some_and(Ext::is_main);
    if main && off < marker::LEN as i64 {
        if !pp.is_null() {
            // SAFETY: SQLite passes `pp` writable for the mapping.
            unsafe { *pp = null_mut() };
        }
        return OK;
    }
    fwd!(file, xFetch, off, amt, pp)
}

unsafe fn x_unfetch(
    file: *mut ffi::sqlite3_file,
    off: ffi::sqlite3_int64,
    p: *mut c_void,
) -> c_int {
    fwd!(file, xUnfetch, off, p)
}

/// After a panic in a method of `f`: resets the binding `f` belongs to `internal`, since the panic
/// may have left its state half changed.
///
/// # Safety
///
/// `f` is an open file of this VFS.
unsafe fn reset_after_panic(f: *mut ffi::sqlite3_file, entry: &'static str) {
    // SAFETY: the caller's contract.
    if let Some(ext) = unsafe { ext(f) } {
        ext.binding
            .reset(ResetKind::Internal, &format_args!("{entry} panicked"));
    }
}

/// A file method's C entry point: feeds the call to publication first (`$call`, for a main db
/// handle; the methods that classify their own calls pass `None`), then runs the method named
/// without `c_` through [`contain`], so a panic in it answers `$fails` instead of unwinding into
/// SQLite (which aborts the host process) and resets the file's binding. On a file a forked child
/// inherited it answers `$fails` without running (design: Forks). `xOpen`, `xClose` and `xFetch`
/// have entry points of their own.
macro_rules! io_entry {
    ($entry:ident => $method:ident($file:ident $(, $arg:ident: $ty:ty)*) -> $ret:ty, $call:expr, $fails:expr) => {
        unsafe extern "C" fn $entry($file: *mut ffi::sqlite3_file $(, $arg: $ty)*) -> $ret {
            let fails = || $fails;
            // SAFETY: `$file` is the open file SQLite called the method on.
            if unsafe { inherited($file) } {
                return fails();
            }
            contain(
                stringify!($method),
                || {
                    // SAFETY: `$file` is the open file SQLite called the method on.
                    unsafe { reset_after_panic($file, stringify!($method)) };
                    fails()
                },
                || {
                    let call: Option<Call> = $call;
                    if let Some(call) = call {
                        // SAFETY: `$file` is the open file SQLite called the method on.
                        unsafe { observe($file, call) };
                    }
                    // SAFETY: SQLite's call of the method, forwarded with its arguments.
                    unsafe { $method($file $(, $arg)*) }
                },
            )
        }
    };
}

/// `xOpen`'s C entry point (see `io_entry`): nothing is open yet, so a panic has nothing to reset.
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
/// `x_open` made for the file freed.
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
    // SAFETY: the caller's contract, which `take_wal_name` asks for; the "unix" file is closed.
    drop(unsafe { take_wal_name(file) });
    // SAFETY: as above.
    unsafe { (*file).pMethods = null() };
    ffi::SQLITE_CANTOPEN
}

/// `xClose`'s C entry point (see `io_entry`). The file's state is taken and accounted for
/// (`closing`, which decides an undecided commit of the handle) before the body runs, so a panic
/// in the body changes nothing of the binding, and the fallback closes the "unix" file, which is
/// all the body does. A file a forked child inherited closes without either (design: Forks).
unsafe extern "C" fn c_close(file: *mut ffi::sqlite3_file) -> c_int {
    // SAFETY: SQLite's call of `xClose` on an open file of this VFS.
    let ext = match unsafe { take_ext(file) } {
        Some(e) if fork::forked() => {
            // A file a forked child inherited (design: Forks): its state is left as it is, and so
            // is its "unix" file, whose close takes locks of the inode that a thread of the parent
            // may have held at the fork. The child keeps its copy of the descriptor.
            let _left: &mut Ext = Box::leak(e);
            // SAFETY: SQLite closes a file once, and nothing reads the "unix" file's name again.
            drop(unsafe { take_wal_name(file) });
            return OK;
        }
        ext => ext,
    };
    if let Some(e) = &ext {
        let addr = file.expose_provenance();
        // Its wal-index regions, if it still maps any, leave the forked child's list before "unix"
        // unmaps them ([`fork`]).
        fork::unmapping(addr);
        contain(
            "x_close_bookkeeping",
            || e.binding.reset(ResetKind::Internal, &"xClose panicked"),
            || closing(e, addr),
        );
    }
    let rc = contain(
        "x_close",
        || {
            let _closed = fwd!(file, xClose);
            ffi::SQLITE_IOERR_CLOSE
        },
        || fwd!(file, xClose),
    );
    // SAFETY: SQLite closes a file once, and its "unix" file, which kept the name, is closed.
    drop(unsafe { take_wal_name(file) });
    rc
}

/// The name "unix" gets for `zname`: a bound db's WAL (`<db>-wal`) is `<db>-uwal`. `None` when it
/// passes unchanged.
///
/// # Safety
///
/// `zname` is null or NUL-terminated.
unsafe fn mapped_name(zname: *const c_char) -> Option<CString> {
    if zname.is_null() {
        return None;
    }
    // SAFETY: the caller's contract.
    let name = unsafe { CStr::from_ptr(zname) }.to_str().ok()?;
    uwal(name, |db| lookup(db).is_some()).and_then(|n| CString::new(n).ok())
}

/// `xAccess` of the "ursula" VFS over the "unix" VFS `u`: SQLite's probe of a bound db's `db-wal`
/// looks at its `db-uwal`. A forked child's probe of a bound db's file fails.
///
/// # Safety
///
/// SQLite's `xAccess` contract: `zname` is NUL-terminated and `out` writable. `u` is null or a VFS
/// that outlives the call.
unsafe fn x_access(
    u: *mut ffi::sqlite3_vfs,
    zname: *const c_char,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    // SAFETY: the caller's contract.
    if unsafe { inherited_name(zname) } {
        return ffi::SQLITE_IOERR_ACCESS;
    }
    // SAFETY: the caller's contract.
    let Some(access) = unsafe { u.as_ref() }.and_then(|u| u.xAccess) else {
        return ffi::SQLITE_IOERR_ACCESS;
    };
    // SAFETY: the caller's contract.
    let mapped = unsafe { mapped_name(zname) };
    let name = mapped.as_ref().map_or(zname, |m| m.as_ptr());
    // SAFETY: SQLite's call, forwarded to "unix" with a NUL-terminated name that outlives it.
    unsafe { access(u, name, flags, out) }
}

/// `xDelete` of the "ursula" VFS over the "unix" VFS `u`: SQLite's delete of a bound db's `db-wal`
/// deletes its `db-uwal`. A forked child's delete of a bound db's file fails.
///
/// # Safety
///
/// SQLite's `xDelete` contract: `zname` is NUL-terminated. `u` is null or a VFS that outlives the
/// call.
unsafe fn x_delete(u: *mut ffi::sqlite3_vfs, zname: *const c_char, sync_dir: c_int) -> c_int {
    // SAFETY: the caller's contract.
    if unsafe { inherited_name(zname) } {
        return ffi::SQLITE_IOERR_DELETE;
    }
    // SAFETY: the caller's contract.
    let Some(delete) = unsafe { u.as_ref() }.and_then(|u| u.xDelete) else {
        return ffi::SQLITE_IOERR_DELETE;
    };
    // SAFETY: the caller's contract.
    let mapped = unsafe { mapped_name(zname) };
    let name = mapped.as_ref().map_or(zname, |m| m.as_ptr());
    // SAFETY: SQLite's call, forwarded to "unix" with a NUL-terminated name that outlives it.
    unsafe { delete(u, name, sync_dir) }
}

/// `xAccess`'s C entry point: a panic answers `SQLITE_IOERR_ACCESS`.
pub(crate) unsafe extern "C" fn c_access(
    _vfs: *mut ffi::sqlite3_vfs,
    zname: *const c_char,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    contain(
        "x_access",
        || ffi::SQLITE_IOERR_ACCESS,
        || {
            // SAFETY: SQLite's call of `xAccess`, forwarded with its arguments over `UNIX`, null or
            // SQLite's "unix" VFS, which lives for the process.
            unsafe { x_access(unix(), zname, flags, out) }
        },
    )
}

/// `xDelete`'s C entry point: a panic answers `SQLITE_IOERR_DELETE`.
pub(crate) unsafe extern "C" fn c_delete(
    _vfs: *mut ffi::sqlite3_vfs,
    zname: *const c_char,
    sync_dir: c_int,
) -> c_int {
    contain(
        "x_delete",
        || ffi::SQLITE_IOERR_DELETE,
        || {
            // SAFETY: SQLite's call of `xDelete`, forwarded with its arguments over `UNIX`, null or
            // SQLite's "unix" VFS, which lives for the process.
            unsafe { x_delete(unix(), zname, sync_dir) }
        },
    )
}

/// What `xFileControl` answers when it fails without running (a panic, or a file a forked child
/// inherited): never an error for PHASETWO, since SQLite reports a published commit as failed
/// when it gets one.
fn file_control_failed(op: c_int) -> c_int {
    if op == ffi::SQLITE_FCNTL_COMMIT_PHASETWO {
        ffi::SQLITE_NOTFOUND
    } else {
        ffi::SQLITE_IOERR
    }
}

io_entry!(c_read => x_read(file, buf: *mut c_void, amt: c_int, off: ffi::sqlite3_int64) -> c_int, Some(Call::Other), ffi::SQLITE_IOERR_READ);
io_entry!(c_write => x_write(file, buf: *const c_void, amt: c_int, off: ffi::sqlite3_int64) -> c_int, Some(Call::Other), ffi::SQLITE_IOERR_WRITE);
io_entry!(c_truncate => x_truncate(file, size: ffi::sqlite3_int64) -> c_int, Some(Call::Other), ffi::SQLITE_IOERR_TRUNCATE);
io_entry!(c_sync => x_sync(file, flags: c_int) -> c_int, Some(Call::Other), ffi::SQLITE_IOERR_FSYNC);
io_entry!(c_file_size => x_file_size(file, out: *mut ffi::sqlite3_int64) -> c_int, Some(Call::Other), ffi::SQLITE_IOERR_FSTAT);
io_entry!(c_lock => x_lock(file, l: c_int) -> c_int, Some(Call::Other), ffi::SQLITE_IOERR_LOCK);
io_entry!(c_unlock => x_unlock(file, l: c_int) -> c_int, Some(Call::Other), ffi::SQLITE_IOERR_UNLOCK);
io_entry!(c_check_reserved_lock => x_check_reserved_lock(file, out: *mut c_int) -> c_int, Some(Call::Other), ffi::SQLITE_IOERR_CHECKRESERVEDLOCK);
io_entry!(c_file_control => x_file_control(file, op: c_int, arg: *mut c_void) -> c_int, None, file_control_failed(op));
// SQLite's default sector size and no device characteristics: neither method can report an error.
io_entry!(c_sector_size => x_sector_size(file) -> c_int, Some(Call::Other), 4096);
io_entry!(c_device_characteristics => x_device_characteristics(file) -> c_int, Some(Call::Other), 0);
io_entry!(c_shm_map => x_shm_map(file, pg: c_int, pgsz: c_int, extend: c_int, pp: *mut *mut c_void) -> c_int, Some(Call::Other), ffi::SQLITE_IOERR_SHMMAP);
io_entry!(c_shm_lock => x_shm_lock(file, offset: c_int, n: c_int, flags: c_int) -> c_int, None, ffi::SQLITE_IOERR_SHMLOCK);
io_entry!(c_shm_barrier => x_shm_barrier(file) -> (), Some(Call::Other), ());
io_entry!(c_shm_unmap => x_shm_unmap(file, delete: c_int) -> c_int, Some(Call::Other), ffi::SQLITE_IOERR);
io_entry!(c_unfetch => x_unfetch(file, off: ffi::sqlite3_int64, p: *mut c_void) -> c_int, Some(Call::Other), ffi::SQLITE_IOERR);

/// `xFetch`'s C entry point (see `io_entry`): when it fails without running, after a panic or on
/// a file a forked child inherited, it also leaves no mapping in `*pp`.
unsafe extern "C" fn c_fetch(
    file: *mut ffi::sqlite3_file,
    off: ffi::sqlite3_int64,
    amt: c_int,
    pp: *mut *mut c_void,
) -> c_int {
    let unmapped = || {
        if !pp.is_null() {
            // SAFETY: SQLite passes `pp` writable for the mapping.
            unsafe { *pp = null_mut() };
        }
        ffi::SQLITE_IOERR
    };
    // SAFETY: `file` is the open file SQLite called the method on.
    if unsafe { inherited(file) } {
        return unmapped();
    }
    contain(
        "x_fetch",
        || {
            // SAFETY: `file` is the open file SQLite called the method on.
            unsafe { reset_after_panic(file, "x_fetch") };
            unmapped()
        },
        || {
            // SAFETY: `file` is the open file SQLite called the method on.
            unsafe { observe(file, Call::Other) };
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
mod tests;
