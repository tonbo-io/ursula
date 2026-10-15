use std::cell::Cell;
use std::collections::BTreeMap;
use std::ffi::c_int;
use std::ffi::c_void;
use std::ptr::NonNull;
use std::ptr::null;
use std::ptr::null_mut;
use std::sync::Arc;

use libsqlite3_sys as ffi;

use super::Ext;
use super::File;
use super::METHODS;
use super::Role;
use super::c_open;
use super::failed_open;
use super::lost_page;
use crate::capture::FIRST_PAGE;
use crate::capture::FRAME;
use crate::codec::PAGE;
use crate::config::set_panic_in_for_tests;
use crate::db::Binding;
use crate::db::for_tests;
use crate::publication::HandleState;
use crate::replicator::ResetKind;
use crate::testdir::TempDir;

// A page read back is checked against what this process wrote at its offset, wherever it lies in
// the read (one page, or a whole frame with its header). Pages never written here are not checked,
// nor is a page the read covers only in part.
#[test]
fn pages_read_back_are_checked_against_what_was_written() {
    let (first, second) = (FIRST_PAGE, FIRST_PAGE + FRAME);
    let written = BTreeMap::from([
        (first, crc32c::crc32c(&[1u8; PAGE])),
        (second, crc32c::crc32c(&[2u8; PAGE])),
    ]);
    assert_eq!(lost_page(&written, &[1u8; PAGE], first), None);
    assert_eq!(lost_page(&written, &[0u8; PAGE], first), Some(first));
    let mut frame = vec![9u8; 24];
    frame.extend([0u8; PAGE]);
    assert_eq!(lost_page(&written, &frame, second - 24), Some(second));
    frame.truncate(24);
    frame.extend([2u8; PAGE]);
    assert_eq!(lost_page(&written, &frame, second - 24), None);
    assert_eq!(lost_page(&written, &[0u8; PAGE], second + FRAME), None);
    assert_eq!(lost_page(&written, &[0u8; 100], first), None);
}

/// A file of `b` with this VFS's state, over a stand-in "unix" file with `inner` methods. The
/// caller frees `ext` unless the file is closed.
fn file_of(b: &Arc<Binding>, main: bool, inner: *const ffi::sqlite3_io_methods) -> File {
    let role = if main {
        Role::Main {
            state: Cell::new(HandleState::Idle),
            exclusive: Cell::new(false),
        }
    } else {
        Role::Wal
    };
    File {
        base: ffi::sqlite3_file {
            pMethods: &raw const METHODS,
        },
        ext: Box::into_raw(Box::new(Ext {
            binding: Arc::clone(b),
            role,
        })),
        fail_sync: false,
        inner: ffi::sqlite3_file { pMethods: inner },
    }
}

/// A file as SQLite allocates one for `xOpen`, and as `not_open` sets it up.
fn unopened() -> File {
    File {
        base: ffi::sqlite3_file { pMethods: null() },
        ext: null_mut(),
        fail_sync: false,
        inner: ffi::sqlite3_file { pMethods: null() },
    }
}

// Every file method's C entry point contains a panic (one on purpose here, before the body
// touches the file): it answers the method's own error code (the sector size and device
// characteristics, which cannot report one, answer 4096 and 0), and resets the binding `internal`.
// PHASETWO is never answered with an error, panic or not. `xClose` takes the file's state first,
// so its panic resets nothing and the state is freed. `xOpen` answers SQLITE_CANTOPEN and leaves
// the file without methods.
#[test]
fn every_file_entry_point_contains_a_panic() {
    let dir = TempDir::new("vfs-panic");
    let b = for_tests(dir.path());
    let mut file = file_of(&b, false, null());
    let f = (&raw mut file).cast::<ffi::sqlite3_file>();
    let mut buf = [0u8; 16];
    let mut size: ffi::sqlite3_int64 = 0;
    let mut flag: c_int = 0;
    let mut region: *mut c_void = null_mut();
    let m = &METHODS;
    macro_rules! contained {
        ($entry:literal, $call:expr, $code:expr) => {{
            set_panic_in_for_tests(Some($entry));
            // SAFETY: the entry point panics before its body touches `f`, whose own state is
            // valid.
            let answer = unsafe { $call };
            set_panic_in_for_tests(None);
            assert_eq!(answer, $code, $entry);
            assert_eq!(b.reset_kind(), Some(ResetKind::Internal), $entry);
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
    contained!(
        "x_file_control",
        m.xFileControl.unwrap()(f, ffi::SQLITE_FCNTL_COMMIT_PHASETWO, null_mut()),
        ffi::SQLITE_NOTFOUND
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
    b.lock().wal_open = 1;
    set_panic_in_for_tests(Some("x_close"));
    // SAFETY: `c_close` takes and frees the state `file_of` made; the "unix" file has no methods.
    let closed = unsafe { m.xClose.unwrap()(f) };
    set_panic_in_for_tests(None);
    assert_eq!(closed, ffi::SQLITE_IOERR_CLOSE);
    assert_eq!(b.lock().wal_open, 0, "the WAL handle is uncounted");
    assert!(file.ext.is_null());
    assert_eq!(Arc::strong_count(&b), 1, "the file's state is freed");

    let mut fresh = File {
        base: ffi::sqlite3_file {
            pMethods: &raw const METHODS,
        },
        ext: null_mut(),
        fail_sync: true,
        inner: ffi::sqlite3_file {
            pMethods: &raw const STAND_IN,
        },
    };
    CLOSES.set(0);
    set_panic_in_for_tests(Some("x_open"));
    let mut out: c_int = 0;
    // SAFETY: a `File` as SQLite allocates one; `c_open` sets it up as not open, then panics
    // before its body reads any argument.
    let opened = unsafe { c_open(null_mut(), null(), (&raw mut fresh).cast(), 0, &raw mut out) };
    set_panic_in_for_tests(None);
    assert_eq!(opened, ffi::SQLITE_CANTOPEN);
    assert!(
        fresh.base.pMethods.is_null(),
        "a failed xOpen leaves no methods"
    );
    assert!(!fresh.fail_sync);
    assert_eq!(
        CLOSES.get(),
        0,
        "no \"unix\" file opened, so none is closed"
    );
}

thread_local! {
    /// The closes of a stand-in "unix" file on this thread.
    static CLOSES: Cell<usize> = const { Cell::new(0) };
}

extern "C" fn stand_in_close(_file: *mut ffi::sqlite3_file) -> c_int {
    CLOSES.set(CLOSES.get().saturating_add(1));
    ffi::SQLITE_OK
}

extern "C" fn stand_in_ok(_file: *mut ffi::sqlite3_file, _a: c_int, _b: c_int, _c: c_int) -> c_int {
    ffi::SQLITE_OK
}

extern "C" fn stand_in_write(
    _file: *mut ffi::sqlite3_file,
    _buf: *const c_void,
    _amt: c_int,
    _off: ffi::sqlite3_int64,
) -> c_int {
    ffi::SQLITE_OK
}

/// A stand-in "unix" file whose own open succeeded: it closes, writes and takes shm locks.
static STAND_IN: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 2,
    xClose: Some(stand_in_close),
    xRead: None,
    xWrite: Some(stand_in_write),
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
    xShmLock: Some(stand_in_ok),
    xShmBarrier: None,
    xShmUnmap: None,
    xFetch: None,
    xUnfetch: None,
};

// A file's "unix" file is closed when the file closes, and also when the close panics.
#[test]
fn a_close_closes_the_unix_file_even_after_a_panic() {
    CLOSES.set(0);
    let over_stand_in = || File {
        base: ffi::sqlite3_file {
            pMethods: &raw const METHODS,
        },
        ext: null_mut(),
        fail_sync: false,
        inner: ffi::sqlite3_file {
            pMethods: &raw const STAND_IN,
        },
    };
    let mut normal = over_stand_in();
    // SAFETY: an open file of this VFS, closed once.
    let closed = unsafe { METHODS.xClose.unwrap()((&raw mut normal).cast()) };
    assert_eq!((closed, CLOSES.get()), (ffi::SQLITE_OK, 1));
    let mut panicking = over_stand_in();
    set_panic_in_for_tests(Some("x_close"));
    // SAFETY: another open file of this VFS, closed once.
    let closed = unsafe { METHODS.xClose.unwrap()((&raw mut panicking).cast()) };
    set_panic_in_for_tests(None);
    assert_eq!((closed, CLOSES.get()), (ffi::SQLITE_IOERR_CLOSE, 2));
}

// An open that panicked closes the "unix" file whose own open succeeded, frees the state `x_open`
// made, and leaves no methods.
#[test]
fn a_failed_open_closes_the_unix_file_and_frees_its_state() {
    let dir = TempDir::new("vfs-failed-open");
    let b = for_tests(dir.path());
    CLOSES.set(0);
    let mut file = file_of(&b, false, &raw const STAND_IN);
    // SAFETY: a file set up as `x_open` leaves it once the "unix" file is open.
    let answer = unsafe { failed_open((&raw mut file).cast()) };
    assert_eq!(answer, ffi::SQLITE_CANTOPEN);
    assert_eq!(CLOSES.get(), 1);
    assert!(file.ext.is_null() && file.base.pMethods.is_null());
    assert_eq!(Arc::strong_count(&b), 1, "the file's state is freed");
    let mut fresh = unopened();
    // SAFETY: a file as `not_open` sets it up.
    let answer = unsafe { failed_open((&raw mut fresh).cast()) };
    assert_eq!((answer, CLOSES.get()), (ffi::SQLITE_CANTOPEN, 1));
}

// A main db inside a generation directory opens only while bound: unbound, it is refused before
// the "unix" VFS sees it.
#[test]
fn an_unbound_generation_db_is_refused() {
    let name = std::ffi::CString::new(format!("/tmp/root/gen-{}/db", "ab".repeat(16))).unwrap();
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
}

/// `xShmLock` of an open file of this VFS over the stand-in.
fn shm_lock(f: *mut ffi::sqlite3_file, offset: c_int, flags: c_int) -> c_int {
    // SAFETY: the tests pass open files of this VFS over the stand-in.
    unsafe { METHODS.xShmLock.unwrap()(f, offset, 1, flags) }
}

/// `xWrite` of `data` at `off` to an open file of this VFS over the stand-in.
fn write(f: *mut ffi::sqlite3_file, data: &[u8], off: i64) -> c_int {
    let amt = c_int::try_from(data.len()).unwrap();
    // SAFETY: as in `shm_lock`; `data` is `amt` readable bytes.
    unsafe { METHODS.xWrite.unwrap()(f, data.as_ptr().cast(), amt, off) }
}

// After a reset, a new read transaction fails at its read-mark lock and every write fails, the db
// file's included; locks other than read marks still pass.
#[test]
fn a_reset_refuses_new_reads_and_every_write() {
    let dir = TempDir::new("vfs-reset");
    let b = for_tests(dir.path());
    let mut main = file_of(&b, true, &raw const STAND_IN);
    let mut wal = file_of(&b, false, &raw const STAND_IN);
    let (m, w) = (
        (&raw mut main).cast::<ffi::sqlite3_file>(),
        (&raw mut wal).cast::<ffi::sqlite3_file>(),
    );
    let read_mark = ffi::SQLITE_SHM_LOCK | ffi::SQLITE_SHM_SHARED;
    let page = [0u8; PAGE];
    assert_eq!(shm_lock(m, 3, read_mark), ffi::SQLITE_OK);
    b.reset(ResetKind::Fenced, &"test");
    assert_eq!(shm_lock(m, 3, read_mark), ffi::SQLITE_IOERR_READ);
    assert_eq!(shm_lock(m, 2, read_mark), ffi::SQLITE_OK, "not a read mark");
    assert_eq!(write(m, &page, 0), ffi::SQLITE_IOERR_WRITE);
    assert_eq!(write(w, &page, 56), ffi::SQLITE_IOERR_WRITE);
    b.reset(ResetKind::LocalDamage, &"later");
    assert_eq!(
        b.reset_kind(),
        Some(ResetKind::Fenced),
        "the first reset stays"
    );
    // SAFETY: the `Box`es `file_of` made, which nothing freed.
    drop(unsafe { Box::from_raw(main.ext) });
    // SAFETY: as above.
    drop(unsafe { Box::from_raw(wal.ext) });
}

// A WAL write without the WAL write lock (EXCLUSIVE locking mode takes no shm lock) fails alone as
// `unsupported`, nothing written, and the binding stays live. One under the lock is admitted.
#[test]
fn a_wal_write_without_the_write_lock_is_unsupported() {
    let dir = TempDir::new("vfs-exclusive");
    let b = for_tests(dir.path());
    let mut main = file_of(&b, true, &raw const STAND_IN);
    let mut wal = file_of(&b, false, &raw const STAND_IN);
    let (m, w) = (
        (&raw mut main).cast::<ffi::sqlite3_file>(),
        (&raw mut wal).cast::<ffi::sqlite3_file>(),
    );
    let mut header = [0u8; 32];
    header[8..12].copy_from_slice(&4096u32.to_be_bytes());
    assert_eq!(write(w, &header, 0), ffi::SQLITE_IOERR_AUTH);
    assert!(b.reset_kind().is_none());
    assert!(
        b.lock()
            .refused
            .as_ref()
            .is_some_and(|r| r.kind().as_str() == "unsupported")
    );
    let write_lock = ffi::SQLITE_SHM_LOCK | ffi::SQLITE_SHM_EXCLUSIVE;
    assert_eq!(shm_lock(m, 0, write_lock), ffi::SQLITE_OK);
    // The header write needs F2's db fsync, which the stand-in cannot do.
    assert_eq!(write(w, &header, 0), ffi::SQLITE_IOERR);
    assert_eq!(
        b.reset_kind(),
        Some(ResetKind::LocalDamage),
        "a failed F2 fsync resets"
    );
    // SAFETY: the `Box`es `file_of` made, which nothing freed.
    drop(unsafe { Box::from_raw(main.ext) });
    // SAFETY: as above.
    drop(unsafe { Box::from_raw(wal.ext) });
}
