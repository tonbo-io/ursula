use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ffi::CStr;
use std::ffi::CString;
use std::ffi::c_char;
use std::ffi::c_int;
use std::ffi::c_void;
use std::ptr::NonNull;
use std::ptr::null;
use std::ptr::null_mut;
use std::sync::Arc;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use libsqlite3_sys as ffi;

use super::Ext;
use super::File;
use super::METHODS;
use super::Role;
use super::WalName;
use super::c_access;
use super::c_delete;
use super::c_open;
use super::commit_log;
use super::failed_open;
use super::lost_page;
use super::take_ext;
use super::uwal;
use super::x_access;
use super::x_delete;
use crate::capture::FIRST_PAGE;
use crate::capture::FRAME;
use crate::capture::WAL_HDR;
use crate::codec::PAGE;
use crate::codec::marker::SQLITE_MAGIC;
use crate::codec::marker::mark;
use crate::config::set_panic_in_for_tests;
use crate::db::Binding;
use crate::db::failure_state_at;
use crate::db::for_tests;
use crate::db::for_tests_with_limit;
use crate::db::reader_for_tests;
use crate::db::registered_for_tests;
use crate::db::registry_locked_for_tests;
use crate::driver::Holder;
use crate::hold::Hold;
use crate::hold::set_for_tests as set_hold;
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
    file_held_by(b, main.then_some(Holder::Host), inner)
}

/// [`file_of`]: a main db handle of `holder`'s connection, or a WAL handle with none.
fn file_held_by(
    b: &Arc<Binding>,
    holder: Option<Holder>,
    inner: *const ffi::sqlite3_io_methods,
) -> File {
    let role = match holder {
        Some(holder) => Role::Main {
            holder,
            state: Cell::new(HandleState::Idle),
            exclusive: Cell::new(false),
            marks: Cell::new(0),
            writing: Cell::new(None),
        },
        None => Role::Wal,
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
        wal_name: null_mut(),
        inner: ffi::sqlite3_file { pMethods: inner },
    }
}

/// A file as SQLite allocates one for `xOpen`, and as `not_open` sets it up.
fn unopened() -> File {
    File {
        base: ffi::sqlite3_file { pMethods: null() },
        ext: null_mut(),
        fail_sync: false,
        wal_name: null_mut(),
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
        wal_name: null_mut(),
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
        wal_name: null_mut(),
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

thread_local! {
    /// The bytes of a stand-in "unix" file on this thread (`DISK_FILE`).
    static DISK: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
    /// The calls of the stand-in's `xFetch` on this thread.
    static FETCHES: Cell<usize> = const { Cell::new(0) };
}

extern "C" fn disk_read(
    _file: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    amt: c_int,
    off: ffi::sqlite3_int64,
) -> c_int {
    let (amt, off) = (usize::try_from(amt).unwrap(), usize::try_from(off).unwrap());
    // SAFETY: the VFS passes `amt` writable bytes at `buf`.
    let out = unsafe { std::slice::from_raw_parts_mut(buf.cast::<u8>(), amt) };
    DISK.with_borrow(|disk| {
        let have = disk.len().saturating_sub(off).min(amt);
        out[..have].copy_from_slice(&disk[off..off.saturating_add(have)]);
        out[have..].fill(0);
        if have < amt {
            ffi::SQLITE_IOERR_SHORT_READ
        } else {
            ffi::SQLITE_OK
        }
    })
}

extern "C" fn disk_write(
    _file: *mut ffi::sqlite3_file,
    buf: *const c_void,
    amt: c_int,
    off: ffi::sqlite3_int64,
) -> c_int {
    let (amt, off) = (usize::try_from(amt).unwrap(), usize::try_from(off).unwrap());
    // SAFETY: the VFS passes `amt` readable bytes at `buf`.
    let data = unsafe { std::slice::from_raw_parts(buf.cast::<u8>(), amt) };
    DISK.with_borrow_mut(|disk| {
        let end = off.saturating_add(amt);
        if disk.len() < end {
            disk.resize(end, 0);
        }
        disk[off..end].copy_from_slice(data);
    });
    ffi::SQLITE_OK
}

extern "C" fn disk_fetch(
    _file: *mut ffi::sqlite3_file,
    _off: ffi::sqlite3_int64,
    _amt: c_int,
    pp: *mut *mut c_void,
) -> c_int {
    FETCHES.set(FETCHES.get().saturating_add(1));
    // SAFETY: the VFS passes `pp` writable.
    unsafe { *pp = NonNull::dangling().as_ptr() };
    ffi::SQLITE_OK
}

/// A stand-in "unix" file over [`DISK`]: it reads, writes and maps.
static DISK_FILE: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 3,
    xClose: Some(stand_in_close),
    xRead: Some(disk_read),
    xWrite: Some(disk_write),
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
    xFetch: Some(disk_fetch),
    xUnfetch: None,
};

/// `xRead` of `amt` bytes at `off` from an open file of this VFS: its answer and the bytes.
fn read(f: *mut ffi::sqlite3_file, amt: usize, off: i64) -> (c_int, Vec<u8>) {
    let mut buf = vec![0u8; amt];
    // SAFETY: the tests pass open files of this VFS; `buf` is `amt` writable bytes.
    let rc = unsafe {
        METHODS.xRead.unwrap()(
            f,
            buf.as_mut_ptr().cast(),
            c_int::try_from(amt).unwrap(),
            off,
        )
    };
    (rc, buf)
}

// Page 1 of a bound db reaches the disk with the generation's marker in place of SQLite's magic,
// and reads back as the magic, whatever range a read or write covers. A db write that does not
// carry the magic resets `local_damage`, nothing written.
#[test]
fn page_one_lands_on_disk_marked_and_reads_back_as_magic() {
    let dir = TempDir::new("vfs-marker");
    let b = for_tests(dir.path());
    let lineage = b.handle.generation.lineage;
    let mut main = file_of(&b, true, &raw const DISK_FILE);
    let m = (&raw mut main).cast::<ffi::sqlite3_file>();
    DISK.with_borrow_mut(Vec::clear);
    // A checkpoint writes the db file.
    b.lock().checkpoint = Some((std::time::Instant::now(), false));
    let mut page = vec![7u8; PAGE];
    page[..16].copy_from_slice(&SQLITE_MAGIC);
    assert_eq!(write(m, &page, 0), ffi::SQLITE_OK);
    let disk = DISK.with_borrow(Clone::clone);
    assert_eq!(disk[..16], mark(lineage));
    assert_eq!(disk[16..], page[16..]);
    assert_eq!(read(m, PAGE, 0), (ffi::SQLITE_OK, page.clone()));
    // SQLite reads the first 100 bytes at open, and any range is translated.
    assert_eq!(read(m, 100, 0).1, page[..100]);
    assert_eq!(read(m, 10, 4).1, page[4..14]);
    assert_eq!(read(m, 8, 16).1, page[16..24]);
    // A write of part of the header.
    assert_eq!(write(m, &SQLITE_MAGIC[6..12], 6), ffi::SQLITE_OK);
    assert_eq!(DISK.with_borrow(|d| d[..16].to_vec()), mark(lineage));
    // Pages past page 1 pass unchanged.
    assert_eq!(write(m, &[9u8; PAGE], PAGE as i64), ffi::SQLITE_OK);
    assert_eq!(read(m, PAGE, PAGE as i64).1, vec![9u8; PAGE]);
    assert!(b.reset_kind().is_none());
    // A page 1 without SQLite's magic is no page 1 SQLite wrote.
    assert_eq!(write(m, &[1u8; PAGE], 0), ffi::SQLITE_IOERR_WRITE);
    assert_eq!(DISK.with_borrow(|d| d[..16].to_vec()), mark(lineage));
    assert_eq!(b.reset_kind(), Some(ResetKind::LocalDamage));
    // SAFETY: the `Box` `file_of` made, which nothing freed.
    drop(unsafe { Box::from_raw(main.ext) });
}

// A read that finds another marker on disk (another generation's db file, or SQLite's own magic
// written outside the extension) resets `local_damage` with SQLITE_IOERR_READ (266).
#[test]
fn a_foreign_marker_resets_local_damage() {
    let dir = TempDir::new("vfs-foreign");
    let b = for_tests(dir.path());
    let mut main = file_of(&b, true, &raw const DISK_FILE);
    let m = (&raw mut main).cast::<ffi::sqlite3_file>();
    let mut page = vec![0u8; PAGE];
    page[..16].copy_from_slice(&SQLITE_MAGIC);
    DISK.with_borrow_mut(|d| *d = page.clone());
    let (rc, buf) = read(m, PAGE, 0);
    assert_eq!(rc, ffi::SQLITE_IOERR_READ);
    assert_eq!(rc, 266);
    assert_eq!(buf, page, "the bytes read are left as found");
    assert_eq!(b.reset_kind(), Some(ResetKind::LocalDamage));
    // SAFETY: the `Box` `file_of` made, which nothing freed.
    drop(unsafe { Box::from_raw(main.ext) });
}

// `xFetch` maps nothing over bytes 0..16 of a bound db (SQLite then reads the page through
// `xRead`), and passes every other range, and every unbound file, to "unix".
#[test]
fn fetch_maps_nothing_over_page_one() {
    let dir = TempDir::new("vfs-fetch");
    let b = for_tests(dir.path());
    let mut main = file_of(&b, true, &raw const DISK_FILE);
    let m = (&raw mut main).cast::<ffi::sqlite3_file>();
    let fetch = |f: *mut ffi::sqlite3_file, off: i64| {
        let mut p: *mut c_void = null_mut();
        // SAFETY: an open file of this VFS, and a writable out-pointer.
        let rc = unsafe { METHODS.xFetch.unwrap()(f, off, 4096, &raw mut p) };
        (rc, p.is_null())
    };
    FETCHES.set(0);
    assert_eq!(fetch(m, 0), (ffi::SQLITE_OK, true));
    assert_eq!(fetch(m, 15), (ffi::SQLITE_OK, true));
    assert_eq!(FETCHES.get(), 0);
    assert_eq!(fetch(m, 4096), (ffi::SQLITE_OK, false));
    assert_eq!(FETCHES.get(), 1);
    let mut plain = File {
        base: ffi::sqlite3_file {
            pMethods: &raw const METHODS,
        },
        ext: null_mut(),
        fail_sync: false,
        wal_name: null_mut(),
        inner: ffi::sqlite3_file {
            pMethods: &raw const DISK_FILE,
        },
    };
    assert_eq!(
        fetch((&raw mut plain).cast(), 0),
        (ffi::SQLITE_OK, false),
        "an unbound file maps as \"unix\" maps it"
    );
    assert_eq!(FETCHES.get(), 2);
    // SAFETY: the `Box` `file_of` made, which nothing freed.
    drop(unsafe { Box::from_raw(main.ext) });
}

// The WAL of a bound db is `db-uwal`; `xAccess` and `xDelete` map SQLite's `db-wal` to it only
// for a bound db, and leave every other name as it is.
#[test]
fn the_wal_of_a_bound_db_is_db_uwal() {
    let bound = |db: &str| db == "/c/gen-1/db";
    assert_eq!(
        uwal("/c/gen-1/db-wal", bound).as_deref(),
        Some("/c/gen-1/db-uwal")
    );
    assert_eq!(uwal("/c/gen-2/db-wal", bound), None, "not bound");
    assert_eq!(uwal("/c/gen-1/db-journal", bound), None);
    assert_eq!(uwal("/c/gen-1/db", bound), None);
    assert_eq!(uwal("/c/gen-1/db-shm", bound), None);
    let dir = TempDir::new("vfs-uwal");
    let b = for_tests(dir.path());
    assert_eq!(
        b.handle.generation.wal(),
        b.handle.generation.dir.join("db-uwal")
    );
    // The name "unix" keeps: NUL-terminated, laid out as SQLite lays out its file names.
    let name = WalName::new("/c/gen-1/db-uwal");
    assert_eq!(&name.0[..4], &[0; 4]);
    assert_eq!(&name.0[name.0.len() - 2..], &[0, 0]);
    // SAFETY: `as_ptr` points at the NUL-terminated name inside `name`.
    let c = unsafe { std::ffi::CStr::from_ptr(name.as_ptr()) };
    assert_eq!(c.to_str().unwrap(), "/c/gen-1/db-uwal");
}

thread_local! {
    /// The names the stand-in "unix" VFS's `xAccess` and `xDelete` got, in order.
    static NAMES: RefCell<Vec<(&'static str, String)>> = const { RefCell::new(Vec::new()) };
}

fn named(call: &'static str, zname: *const c_char) {
    // SAFETY: the VFS methods under test pass a NUL-terminated name.
    let name = unsafe { CStr::from_ptr(zname) }
        .to_str()
        .unwrap()
        .to_owned();
    NAMES.with_borrow_mut(|n| n.push((call, name)));
}

unsafe extern "C" fn stand_in_access(
    _vfs: *mut ffi::sqlite3_vfs,
    zname: *const c_char,
    _flags: c_int,
    out: *mut c_int,
) -> c_int {
    named("access", zname);
    // SAFETY: the method under test passes its caller's writable `out`.
    unsafe { *out = 1 };
    ffi::SQLITE_OK
}

unsafe extern "C" fn stand_in_delete(
    _vfs: *mut ffi::sqlite3_vfs,
    zname: *const c_char,
    _sync_dir: c_int,
) -> c_int {
    named("delete", zname);
    ffi::SQLITE_OK
}

// The VFS's own `xAccess` and `xDelete` give "unix" `db-uwal` for SQLite's `db-wal` of a db in the
// registry, and every other name as it is: another db's WAL, the db itself, and the same WAL once
// its db left the registry. Without a "unix" VFS they answer their I/O error codes.
#[test]
fn access_and_delete_map_the_wal_of_a_registered_db_only() {
    let dir = TempDir::new("vfs-access");
    let b = for_tests(dir.path());
    // SAFETY: every field of `sqlite3_vfs` is an integer, a raw pointer or an `Option` of a
    // function pointer, for which all zero bytes are a valid value.
    let mut unix: ffi::sqlite3_vfs = unsafe { std::mem::zeroed() };
    unix.xAccess = Some(stand_in_access);
    unix.xDelete = Some(stand_in_delete);
    let u = &raw mut unix;
    let probe = |name: &str| {
        let c = CString::new(name).unwrap();
        let mut out: c_int = 0;
        // SAFETY: `c` is NUL-terminated, `out` writable, and `u` outlives the calls.
        let access = unsafe { x_access(u, c.as_ptr(), ffi::SQLITE_ACCESS_EXISTS, &raw mut out) };
        // SAFETY: as above.
        let delete = unsafe { x_delete(u, c.as_ptr(), 0) };
        assert_eq!(
            (access, delete, out),
            (ffi::SQLITE_OK, ffi::SQLITE_OK, 1),
            "{name}"
        );
    };
    let wal = format!("{}-wal", b.path);
    let uwal_name = format!("{}-uwal", b.path);
    let other = dir.path().join("other").join("db-wal");
    let other = other.to_str().unwrap();
    let registered = registered_for_tests(&b);
    for name in [wal.as_str(), other, b.path.as_str()] {
        probe(name);
    }
    drop(registered);
    probe(&wal);
    let names = NAMES.with_borrow_mut(std::mem::take);
    let expected: Vec<(&str, String)> = [&uwal_name, other, &b.path, &wal]
        .into_iter()
        .flat_map(|n| [("access", n.to_owned()), ("delete", n.to_owned())])
        .collect();
    assert_eq!(names, expected);
    // The registered entry points read the process's "unix" VFS, which no test registers.
    let c = CString::new(wal).unwrap();
    let mut out: c_int = 0;
    // SAFETY: the entry points' contracts: a NUL-terminated name and a writable `out`.
    let access = unsafe { c_access(null_mut(), c.as_ptr(), 0, &raw mut out) };
    // SAFETY: as above.
    let delete = unsafe { c_delete(null_mut(), c.as_ptr(), 0) };
    assert_eq!(access, ffi::SQLITE_IOERR_ACCESS);
    assert_eq!(delete, ffi::SQLITE_IOERR_DELETE);
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

// The commit point checks the reset under the log's lock (design: Materialize): a commit waiting
// for the lock while the binding resets is refused once it takes it, and appends nothing, so no
// commit lands in a log an open may be copying. A live binding's commit gets the log.
#[test]
fn a_commit_after_a_reset_is_refused_under_the_log_lock() {
    let dir = TempDir::new("vfs-commit-reset");
    let b = for_tests(dir.path());
    let next = commit_log(&b).map(|log| log.next_position()).unwrap();
    let held = b.handle.log().unwrap();
    let waiting = {
        let b = Arc::clone(&b);
        std::thread::spawn(move || commit_log(&b).map(|log| log.next_position()))
    };
    // The commit is blocked on the lock (or not there yet: refused all the same).
    std::thread::sleep(std::time::Duration::from_millis(50));
    b.reset(ResetKind::Fenced, &"test");
    drop(held);
    assert_eq!(waiting.join().unwrap(), Err(ffi::SQLITE_IOERR_WRITE));
    assert_eq!(b.handle.log().unwrap().next_position(), next);
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
            .standing_refusal()
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

// A commit of more bytes of pages than the commit limit fails alone as `too_large` at the commit
// point, before anything is read back (the stand-in has no `xRead`) or logged: no position is
// taken, nothing is pending and the binding stays live.
#[test]
fn a_commit_above_the_limit_fails_alone_with_nothing_logged() {
    let dir = TempDir::new("vfs-too-large");
    // Two pages.
    let b = for_tests_with_limit(dir.path(), 8192);
    let mut main = file_of(&b, true, &raw const STAND_IN);
    let mut wal = file_of(&b, false, &raw const STAND_IN);
    let (m, w) = (
        (&raw mut main).cast::<ffi::sqlite3_file>(),
        (&raw mut wal).cast::<ffi::sqlite3_file>(),
    );
    let next = commit_log(&b).map(|log| log.next_position()).unwrap();
    let write_lock = ffi::SQLITE_SHM_LOCK | ffi::SQLITE_SHM_EXCLUSIVE;
    assert_eq!(shm_lock(m, 0, write_lock), ffi::SQLITE_OK);
    // Frames of pages 1, 2 and 3, the last the commit frame of a 3-page database, written after
    // the WAL header (whose F2 fsync the stand-in cannot do).
    let frame = |pgno: u32, commit_size: u32| {
        let mut h = [0u8; 24];
        h[0..4].copy_from_slice(&pgno.to_be_bytes());
        h[4..8].copy_from_slice(&commit_size.to_be_bytes());
        h
    };
    // Frame `n`'s header and page offsets.
    let at = |n: i64| {
        let header = FRAME
            .checked_mul(n)
            .and_then(|f| f.checked_add(WAL_HDR))
            .unwrap();
        (header, header.checked_add(24).unwrap())
    };
    for (n, pgno) in [(0, 1), (1, 2)] {
        let (header, page) = at(n);
        assert_eq!(write(w, &frame(pgno, 0), header), ffi::SQLITE_OK);
        assert_eq!(write(w, &[1u8; PAGE], page), ffi::SQLITE_OK);
    }
    let (header, page) = at(2);
    assert_eq!(write(w, &frame(3, 3), header), ffi::SQLITE_OK);
    assert_eq!(write(w, &[3u8; PAGE], page), ffi::SQLITE_IOERR_AUTH);
    assert!(b.reset_kind().is_none());
    assert_eq!(
        b.lock().standing_refusal().map(|r| r.kind().as_str()),
        Some("too_large")
    );
    // A commit another connection logs before the refused one's host classifies its error lifts
    // status's refusal, not the one that names the SQLITE_IOERR_AUTH.
    let registered = registered_for_tests(&b);
    b.lock().lift_refusal();
    assert_eq!(b.lock().standing_refusal(), None);
    assert_eq!(
        failure_state_at(&b.path)
            .and_then(|state| state.refused)
            .map(|r| r.kind().as_str()),
        Some("too_large")
    );
    drop(registered);
    assert_eq!(b.handle.pending_bytes(), 0);
    assert_eq!(b.handle.log().unwrap().next_position(), next);
    // SAFETY: the `Box`es `file_of` made, which nothing freed.
    drop(unsafe { Box::from_raw(main.ext) });
    // SAFETY: as above.
    drop(unsafe { Box::from_raw(wal.ext) });
}

/// A host's hold for this thread's write transactions: the lock file in `dir`, which the returned
/// file holds exclusively (as `ursula-vfs-host hold` does) until it is unlocked, waited at for at
/// most `limit`.
fn held_hold(dir: &std::path::Path, limit: Duration) -> (&'static Hold, std::fs::File) {
    let path = dir.join("hold");
    std::fs::write(&path, b"").unwrap();
    let host = std::fs::File::open(&path).unwrap();
    host.lock().unwrap();
    let ours = std::fs::File::open(&path).unwrap();
    let hold: &'static Hold = Box::leak(Box::new(Hold::new(
        ours,
        path,
        limit,
        Duration::from_millis(5),
    )));
    set_hold(Some(hold));
    (hold, host)
}

const READ_MARK: c_int = ffi::SQLITE_SHM_LOCK | ffi::SQLITE_SHM_SHARED;
const READ_UNMARK: c_int = ffi::SQLITE_SHM_UNLOCK | ffi::SQLITE_SHM_SHARED;
const LOCK_EXCLUSIVE: c_int = ffi::SQLITE_SHM_LOCK | ffi::SQLITE_SHM_EXCLUSIVE;
const UNLOCK_EXCLUSIVE: c_int = ffi::SQLITE_SHM_UNLOCK | ffi::SQLITE_SHM_EXCLUSIVE;

/// The time `xShmLock` of the WAL write lock on `f` took, which must succeed.
fn write_lock_takes(f: *mut ffi::sqlite3_file) -> Duration {
    let started = Instant::now();
    assert_eq!(shm_lock(f, 0, LOCK_EXCLUSIVE), ffi::SQLITE_OK);
    started.elapsed()
}

/// Releases `host`'s exclusive lock after `after`, on a thread of its own.
fn release_after(host: std::fs::File, after: Duration) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        std::thread::sleep(after);
        host.unlock().unwrap();
    })
}

// A write transaction (the WAL write lock asked for with a read mark held) waits while the host
// holds the hold, and goes on once it is released, with SQLITE_OK: SQLite sees a transaction that
// took longer (design: Hosts).
#[test]
fn a_write_transaction_waits_while_the_host_holds_the_hold() {
    let dir = TempDir::new("vfs-hold-wait");
    let b = for_tests(dir.path());
    let (hold, host) = held_hold(dir.path(), Duration::from_secs(60));
    let mut main = file_of(&b, true, &raw const STAND_IN);
    let m = (&raw mut main).cast::<ffi::sqlite3_file>();
    assert_eq!(shm_lock(m, 3, READ_MARK), ffi::SQLITE_OK);
    let release = release_after(host, Duration::from_millis(200));
    let waited = write_lock_takes(m);
    release.join().unwrap();
    assert!(waited >= Duration::from_millis(150), "{waited:?}");
    assert!(waited < Duration::from_secs(30), "{waited:?}");
    assert!(!hold.expired());
    assert_eq!(shm_lock(m, 0, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);
    // Free: the next one does not wait.
    assert!(write_lock_takes(m) < Duration::from_secs(30));
    assert_eq!(shm_lock(m, 0, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);
    set_hold(None);
    // SAFETY: the `Box` `file_of` made, which nothing freed.
    drop(unsafe { Box::from_raw(main.ext) });
}

// The valve: a write transaction waits at most the limit (60 s in the extension), then goes on and
// sets a latch, under which the next one that finds the hold taken goes on at once. The first try
// that finds the hold free clears the latch, so a later hold makes writers wait again.
#[test]
fn the_valve_lets_a_write_go_on_and_latches_until_the_hold_is_free() {
    let dir = TempDir::new("vfs-hold-valve");
    let b = for_tests(dir.path());
    let limit = Duration::from_millis(300);
    let (hold, host) = held_hold(dir.path(), limit);
    let mut main = file_of(&b, true, &raw const STAND_IN);
    let m = (&raw mut main).cast::<ffi::sqlite3_file>();
    assert_eq!(shm_lock(m, 3, READ_MARK), ffi::SQLITE_OK);

    assert!(write_lock_takes(m) >= limit);
    assert!(hold.expired());
    assert_eq!(shm_lock(m, 0, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);
    // Latched: at once, though the hold is still taken.
    assert!(write_lock_takes(m) < limit);
    assert!(hold.expired());
    assert_eq!(shm_lock(m, 0, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);

    host.unlock().unwrap();
    assert!(write_lock_takes(m) < limit);
    assert!(!hold.expired(), "a free hold clears the latch");
    assert_eq!(shm_lock(m, 0, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);
    host.lock().unwrap();
    assert!(
        write_lock_takes(m) >= limit,
        "the next hold is waited at again"
    );
    assert_eq!(shm_lock(m, 0, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);
    set_hold(None);
    // SAFETY: the `Box` `file_of` made, which nothing freed.
    drop(unsafe { Box::from_raw(main.ext) });
}

// Only a write transaction of the host's connection to an owner binding waits: the private
// connection and a reader's handle do not, and neither does a write lock asked for with no read
// mark held, as a wal-index recovery and a checkpoint of any mode (FULL, RESTART and TRUNCATE take
// the write lock after the checkpoint lock) ask for it, or after the read mark was released.
#[test]
fn only_a_write_transaction_of_the_host_waits_at_the_hold() {
    let dir = TempDir::new("vfs-hold-exempt");
    let reader_dir = TempDir::new("vfs-hold-exempt-reader");
    let b = for_tests(dir.path());
    let r = reader_for_tests(reader_dir.path());
    // A wrongly held request would wait the whole minute.
    let (hold, _host) = held_hold(dir.path(), Duration::from_secs(60));
    let quick = Duration::from_secs(30);
    let mut private = file_held_by(&b, Some(Holder::Private), &raw const STAND_IN);
    let mut reader = file_of(&r, true, &raw const STAND_IN);
    let mut main = file_of(&b, true, &raw const STAND_IN);
    let (p, rd, m) = (
        (&raw mut private).cast::<ffi::sqlite3_file>(),
        (&raw mut reader).cast::<ffi::sqlite3_file>(),
        (&raw mut main).cast::<ffi::sqlite3_file>(),
    );
    for f in [p, rd] {
        assert_eq!(shm_lock(f, 3, READ_MARK), ffi::SQLITE_OK);
        assert!(write_lock_takes(f) < quick);
        assert_eq!(shm_lock(f, 0, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);
        assert_eq!(shm_lock(f, 3, READ_UNMARK), ffi::SQLITE_OK);
    }
    // A wal-index recovery: no read mark.
    assert!(write_lock_takes(m) < quick);
    assert_eq!(shm_lock(m, 0, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);
    // A checkpoint that takes the write lock.
    assert_eq!(shm_lock(m, 1, LOCK_EXCLUSIVE), ffi::SQLITE_OK);
    assert!(write_lock_takes(m) < quick);
    assert_eq!(shm_lock(m, 0, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);
    assert_eq!(shm_lock(m, 1, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);
    // A read mark taken and released, and one only locked exclusively to be set, as a read
    // transaction's start does: neither is held.
    assert_eq!(shm_lock(m, 4, READ_MARK), ffi::SQLITE_OK);
    assert_eq!(shm_lock(m, 4, READ_UNMARK), ffi::SQLITE_OK);
    assert_eq!(shm_lock(m, 5, LOCK_EXCLUSIVE), ffi::SQLITE_OK);
    assert_eq!(shm_lock(m, 5, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);
    assert!(write_lock_takes(m) < quick);
    assert_eq!(shm_lock(m, 0, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);
    assert!(!hold.expired());
    set_hold(None);
    // SAFETY: the `Box`es `file_of` and `file_held_by` made, which nothing freed.
    drop(unsafe { Box::from_raw(private.ext) });
    // SAFETY: as above.
    drop(unsafe { Box::from_raw(reader.ext) });
    // SAFETY: as above.
    drop(unsafe { Box::from_raw(main.ext) });
}

// A transaction that writes two attached databases never waits while it holds one's write lock:
// on a thread that holds a bound database's WAL write lock, the next database's write lock does
// not wait. Once the first is released, it does.
#[test]
fn a_thread_holding_a_bound_write_lock_does_not_wait_for_another() {
    let (first_dir, second_dir) = (
        TempDir::new("vfs-hold-two-a"),
        TempDir::new("vfs-hold-two-b"),
    );
    let (a, b) = (for_tests(first_dir.path()), for_tests(second_dir.path()));
    let mut first = file_of(&a, true, &raw const STAND_IN);
    let mut second = file_of(&b, true, &raw const STAND_IN);
    let (f1, f2) = (
        (&raw mut first).cast::<ffi::sqlite3_file>(),
        (&raw mut second).cast::<ffi::sqlite3_file>(),
    );
    assert_eq!(shm_lock(f1, 3, READ_MARK), ffi::SQLITE_OK);
    assert_eq!(shm_lock(f2, 3, READ_MARK), ffi::SQLITE_OK);
    // The first write lock, before the host takes the hold.
    assert!(write_lock_takes(f1) < Duration::from_secs(30));
    let (hold, host) = held_hold(first_dir.path(), Duration::from_secs(60));
    assert!(write_lock_takes(f2) < Duration::from_secs(30));
    assert_eq!(shm_lock(f2, 0, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);
    assert_eq!(shm_lock(f1, 0, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);
    let release = release_after(host, Duration::from_millis(200));
    assert!(write_lock_takes(f2) >= Duration::from_millis(150));
    release.join().unwrap();
    assert_eq!(shm_lock(f2, 0, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);
    assert!(!hold.expired());
    set_hold(None);
    // SAFETY: the `Box`es `file_of` made, which nothing freed.
    drop(unsafe { Box::from_raw(first.ext) });
    // SAFETY: as above.
    drop(unsafe { Box::from_raw(second.ext) });
}

// SQLite may move a connection to another thread between taking a write lock and releasing it: the
// lock counts for the thread that took it until it is released, on whatever thread. A thread whose
// write lock another thread released waits at its next write again, and the releasing thread keeps
// the count of the write lock it holds itself, so it does not wait for another while it holds it.
#[test]
fn a_write_lock_released_on_another_thread_counts_for_the_thread_that_took_it() {
    let dirs = [
        TempDir::new("vfs-hold-moved-a"),
        TempDir::new("vfs-hold-moved-b"),
        TempDir::new("vfs-hold-moved-c"),
    ];
    let [a, b, c] = dirs.each_ref().map(|d| for_tests(d.path()));
    let mut files = [&a, &b, &c].map(|x| file_of(x, true, &raw const STAND_IN));
    let [f1, f2, f3] = files
        .each_mut()
        .map(|f| (&raw mut *f).cast::<ffi::sqlite3_file>());
    for f in [f1, f2, f3] {
        assert_eq!(shm_lock(f, 3, READ_MARK), ffi::SQLITE_OK);
    }
    let quick = Duration::from_secs(30);
    // This thread takes the first write lock before the host takes the hold.
    assert!(write_lock_takes(f1) < quick);
    let (hold, host) = held_hold(dirs[0].path(), Duration::from_secs(60));
    let [a1, a2, a3] = [f1, f2, f3].map(|f| f.expose_provenance());
    std::thread::scope(|s| {
        s.spawn(|| {
            let [g1, g2, g3] =
                [a1, a2, a3].map(std::ptr::with_exposed_provenance_mut::<ffi::sqlite3_file>);
            // The other thread holds a write lock of its own, then releases the first.
            assert!(write_lock_takes(g2) < quick);
            assert_eq!(shm_lock(g1, 0, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);
            set_hold(Some(hold));
            assert!(write_lock_takes(g3) < quick, "it still holds the second");
            assert_eq!(shm_lock(g3, 0, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);
            assert_eq!(shm_lock(g2, 0, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);
            set_hold(None);
        });
    });
    // This thread holds no write lock now, so its next write waits.
    let release = release_after(host, Duration::from_millis(200));
    assert!(write_lock_takes(f2) >= Duration::from_millis(150));
    release.join().unwrap();
    assert_eq!(shm_lock(f2, 0, UNLOCK_EXCLUSIVE), ffi::SQLITE_OK);
    assert!(!hold.expired());
    set_hold(None);
    for f in files {
        // SAFETY: the `Box`es `file_of` made, which nothing freed.
        drop(unsafe { Box::from_raw(f.ext) });
    }
}

/// Writes `line` to the process's stderr with one `write`, through no lock a thread of a forked
/// child's parent may have held at the fork.
fn say_raw(line: &str) {
    // SAFETY: `line` is `line.len()` readable bytes.
    let _written = unsafe { libc::write(2, line.as_ptr().cast(), line.len()) };
}

/// Ends a forked child with the outcome of `checks`: 0 when every check held, 1 otherwise (each
/// failed one named on stderr), 2 after a panic. Nothing of the parent's test harness runs on.
fn exit_with(checks: impl FnOnce() -> Vec<(&'static str, bool)>) -> ! {
    let code = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(checks)) {
        Ok(checks) => {
            let failed: Vec<&str> = checks
                .iter()
                .filter(|(_, held)| !held)
                .map(|(name, _)| *name)
                .collect();
            for name in &failed {
                say_raw(&format!("forked child: {name} failed\n"));
            }
            c_int::from(!failed.is_empty())
        }
        Err(_panicked) => 2,
    };
    // SAFETY: `_exit` ends the process at once, running no destructor and no exit handler.
    unsafe { libc::_exit(code) }
}

/// The checks a child forked from the test below runs on what it inherited: `main` and `wal`, the
/// files of the bound database `b`, and `plain`, a file of no bound database, all over the
/// stand-in "unix" file. A thread of the parent held `b`'s state at the fork, so anything that
/// takes it hangs the child.
fn forked_child_checks(
    b: &Arc<Binding>,
    main: *mut ffi::sqlite3_file,
    wal: *mut ffi::sqlite3_file,
    plain: *mut ffi::sqlite3_file,
) -> Vec<(&'static str, bool)> {
    let m = &METHODS;
    let mut checks = vec![
        ("forked", crate::fork::forked()),
        (
            "the registry is whole and free",
            crate::db::lookup(&b.path).is_some(),
        ),
    ];
    let mut buf = [0u8; 16];
    let mut size: ffi::sqlite3_int64 = 0;
    let mut flag: c_int = 0;
    for f in [main, wal] {
        let mut region: *mut c_void = NonNull::dangling().as_ptr();
        macro_rules! call {
            ($method:ident($($arg:expr),*)) => {{
                // SAFETY: an open file of this VFS over the stand-in, with arguments valid for
                // the call.
                unsafe { m.$method.unwrap()(f $(, $arg)*) }
            }};
        }
        checks.extend([
            (
                "xRead",
                call!(xRead(buf.as_mut_ptr().cast(), 16, 0)) == ffi::SQLITE_IOERR_READ,
            ),
            (
                "xWrite",
                call!(xWrite(buf.as_ptr().cast(), 16, 0)) == ffi::SQLITE_IOERR_WRITE,
            ),
            (
                "xTruncate",
                call!(xTruncate(0)) == ffi::SQLITE_IOERR_TRUNCATE,
            ),
            ("xSync", call!(xSync(0)) == ffi::SQLITE_IOERR_FSYNC),
            (
                "xFileSize",
                call!(xFileSize(&raw mut size)) == ffi::SQLITE_IOERR_FSTAT,
            ),
            ("xLock", call!(xLock(1)) == ffi::SQLITE_IOERR_LOCK),
            ("xUnlock", call!(xUnlock(0)) == ffi::SQLITE_IOERR_UNLOCK),
            (
                "xCheckReservedLock",
                call!(xCheckReservedLock(&raw mut flag)) == ffi::SQLITE_IOERR_CHECKRESERVEDLOCK,
            ),
            (
                "xFileControl",
                call!(xFileControl(0, null_mut())) == ffi::SQLITE_IOERR,
            ),
            (
                "xFileControl PHASETWO",
                call!(xFileControl(ffi::SQLITE_FCNTL_COMMIT_PHASETWO, null_mut()))
                    == ffi::SQLITE_NOTFOUND,
            ),
            ("xSectorSize", call!(xSectorSize()) == 4096),
            (
                "xDeviceCharacteristics",
                call!(xDeviceCharacteristics()) == 0,
            ),
            (
                "xShmMap",
                call!(xShmMap(0, 32768, 0, &raw mut region)) == ffi::SQLITE_IOERR_SHMMAP,
            ),
            (
                "xShmLock",
                call!(xShmLock(3, 1, READ_MARK)) == ffi::SQLITE_IOERR_SHMLOCK,
            ),
            ("xShmUnmap", call!(xShmUnmap(1)) == ffi::SQLITE_IOERR),
            (
                "xFetch",
                call!(xFetch(0, 16, &raw mut region)) == ffi::SQLITE_IOERR,
            ),
            ("xFetch leaves no mapping", region.is_null()),
            (
                "xUnfetch",
                call!(xUnfetch(0, null_mut())) == ffi::SQLITE_IOERR,
            ),
        ]);
        call!(xShmBarrier());
    }
    // Closing an inherited file leaves its state and its "unix" file as they are.
    CLOSES.set(0);
    let held = Arc::strong_count(b);
    for f in [main, wal] {
        // SAFETY: an open file of this VFS, closed once.
        let closed = unsafe { m.xClose.unwrap()(f) };
        checks.push(("xClose", closed == ffi::SQLITE_OK));
        // SAFETY: `f` is a `File`, whose state the close took.
        checks.push((
            "xClose takes the state",
            unsafe { (*f.cast::<File>()).ext }.is_null(),
        ));
    }
    checks.push(("xClose closes no \"unix\" file", CLOSES.get() == 0));
    checks.push(("xClose leaves the state", Arc::strong_count(b) == held));
    // The bound database's names are refused, another's pass through.
    // SAFETY: every field of `sqlite3_vfs` is an integer, a raw pointer or an `Option` of a
    // function pointer, for which all zero bytes are a valid value.
    let mut unix: ffi::sqlite3_vfs = unsafe { std::mem::zeroed() };
    unix.xAccess = Some(stand_in_access);
    unix.xDelete = Some(stand_in_delete);
    let u = &raw mut unix;
    let other = format!("{}-other", b.path);
    for name in [
        b.path.clone(),
        format!("{}-wal", b.path),
        format!("{}-journal", b.path),
    ] {
        let c = CString::new(name).unwrap();
        let mut out: c_int = 0;
        // SAFETY: `c` is NUL-terminated, `out` writable, and `u` outlives the calls.
        let access = unsafe { x_access(u, c.as_ptr(), ffi::SQLITE_ACCESS_EXISTS, &raw mut out) };
        // SAFETY: as above.
        let delete = unsafe { x_delete(u, c.as_ptr(), 0) };
        checks.push((
            "xAccess of a bound name",
            access == ffi::SQLITE_IOERR_ACCESS,
        ));
        checks.push((
            "xDelete of a bound name",
            delete == ffi::SQLITE_IOERR_DELETE,
        ));
    }
    let c = CString::new(other.clone()).unwrap();
    // SAFETY: as above.
    let delete = unsafe { x_delete(u, c.as_ptr(), 0) };
    checks.push(("xDelete of another name", delete == ffi::SQLITE_OK));
    let names = NAMES.with_borrow_mut(std::mem::take);
    checks.push((
        "only another name reaches \"unix\"",
        names == [("delete", other.clone())],
    ));
    // A file of no bound database passes through.
    checks.push((
        "a plain file's write",
        write(plain, &[1u8; 16], 0) == ffi::SQLITE_OK,
    ));
    // SAFETY: an open file of this VFS, closed once.
    let closed = unsafe { m.xClose.unwrap()(plain) };
    checks.push((
        "a plain file's close",
        closed == ffi::SQLITE_OK && CLOSES.get() == 1,
    ));
    // No part of the bound database opens, and another file opens over "unix": here a stand-in
    // VFS, set in the child alone.
    // SAFETY: as above.
    let mut unix: ffi::sqlite3_vfs = unsafe { std::mem::zeroed() };
    unix.xOpen = Some(stand_in_open);
    let was = crate::host::UNIX.swap(&raw mut unix, Ordering::AcqRel);
    for (name, flags, opens) in [
        (b.path.clone(), ffi::SQLITE_OPEN_MAIN_DB, false),
        (format!("{}-wal", b.path), ffi::SQLITE_OPEN_WAL, false),
        (
            format!("{}-journal", b.path),
            ffi::SQLITE_OPEN_MAIN_JOURNAL,
            false,
        ),
        (other.clone(), ffi::SQLITE_OPEN_MAIN_DB, true),
    ] {
        let c = CString::new(name).unwrap();
        let mut f = unopened();
        let mut out: c_int = 0;
        let flags = flags | ffi::SQLITE_OPEN_READWRITE;
        // SAFETY: a `File` as SQLite allocates one, a NUL-terminated name, a writable `out`, and
        // a "unix" VFS that outlives the call.
        let opened = unsafe {
            c_open(
                null_mut(),
                c.as_ptr(),
                (&raw mut f).cast(),
                flags,
                &raw mut out,
            )
        };
        if opens {
            checks.push(("another file opens", opened == ffi::SQLITE_OK));
            // SAFETY: the file just opened, closed once.
            let closed = unsafe { m.xClose.unwrap()((&raw mut f).cast()) };
            checks.push((
                "another file closes",
                closed == ffi::SQLITE_OK && CLOSES.get() == 2,
            ));
        } else {
            checks.push((
                "no part of the bound database opens",
                opened == ffi::SQLITE_CANTOPEN && f.base.pMethods.is_null(),
            ));
        }
    }
    crate::host::UNIX.store(was, Ordering::Release);
    let names = NAMES.with_borrow_mut(std::mem::take);
    checks.push((
        "only another file's open reaches \"unix\"",
        names == [("open", other)],
    ));
    checks
}

unsafe extern "C" fn stand_in_open(
    _vfs: *mut ffi::sqlite3_vfs,
    zname: *const c_char,
    file: *mut ffi::sqlite3_file,
    _flags: c_int,
    _out: *mut c_int,
) -> c_int {
    named("open", zname);
    // SAFETY: the open passes the "unix" file inside its own, writable.
    unsafe { (*file).pMethods = &raw const STAND_IN };
    ffi::SQLITE_OK
}

// A child forked after its parent started the extension (design: Forks) inherits the parent's
// bindings without the threads that serve them, and a lock such a thread held at the fork stays
// held there: here a thread of the parent holds the binding's state across the fork. In the child
// every method on a file of the bound database answers its own error code without running (no
// "unix" method runs, and nothing waits on the binding), a close leaves the file's state and its
// "unix" file as they are, and the database's names are refused: `xOpen` of its db file, its WAL
// and its journal, `xAccess` and `xDelete`. A file of no bound database and other names pass
// through. Another thread held the registry when this one forked: the fork waited for it, so the
// child finds it whole and free. The parent is not forked, and its files work on.
#[test]
fn a_forked_child_fails_every_io_on_a_bound_file_and_touches_nothing_the_parent_holds() {
    let dir = TempDir::new("vfs-fork");
    let b = for_tests(dir.path());
    let registered = registered_for_tests(&b);
    crate::fork::watch();
    crate::db::start_process();
    let mut main = file_of(&b, true, &raw const STAND_IN);
    let mut wal = file_of(&b, false, &raw const STAND_IN);
    let mut plain = File {
        base: ffi::sqlite3_file {
            pMethods: &raw const METHODS,
        },
        ext: null_mut(),
        fail_sync: false,
        wal_name: null_mut(),
        inner: ffi::sqlite3_file {
            pMethods: &raw const STAND_IN,
        },
    };
    let [m, w, p] =
        [&mut main, &mut wal, &mut plain].map(|f| (&raw mut *f).cast::<ffi::sqlite3_file>());

    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let state_holder = {
        let b = Arc::clone(&b);
        std::thread::spawn(move || {
            let state = b.lock();
            held_tx.send(()).unwrap();
            done_rx.recv().unwrap();
            drop(state);
        })
    };
    held_rx.recv().unwrap();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let registry_holder = std::thread::spawn(move || {
        let locked = registry_locked_for_tests();
        locked_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(200));
        drop(locked);
    });
    locked_rx.recv().unwrap();

    let forking = Instant::now();
    // SAFETY: the child runs only `forked_child_checks` and ends with `_exit`.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork: {}", std::io::Error::last_os_error());
    if pid == 0 {
        // A child that hangs is ended by SIGALRM, which the parent sees.
        // SAFETY: a plain libc call.
        unsafe { libc::alarm(20) };
        exit_with(|| forked_child_checks(&b, m, w, p));
    }
    let waited = forking.elapsed();
    done_tx.send(()).unwrap();
    state_holder.join().unwrap();
    registry_holder.join().unwrap();
    let mut status: c_int = 0;
    // SAFETY: waits for the child forked above.
    assert_eq!(unsafe { libc::waitpid(pid, &raw mut status, 0) }, pid);
    assert!(
        libc::WIFEXITED(status),
        "the forked child hung or died (signal {})",
        libc::WTERMSIG(status)
    );
    assert_eq!(
        libc::WEXITSTATUS(status),
        0,
        "a check of the forked child failed: its stderr names it"
    );
    assert!(
        waited >= Duration::from_millis(100),
        "the fork waited for the registry: {waited:?}"
    );

    assert!(!crate::fork::forked());
    assert_eq!(shm_lock(m, 3, READ_MARK), ffi::SQLITE_OK);
    assert_eq!(write(p, &[2u8; 16], 0), ffi::SQLITE_OK);
    drop(registered);
    for f in [m, w] {
        // SAFETY: the `Box`es `file_of` made, which nothing freed in this process.
        drop(unsafe { take_ext(f) });
    }
}

/// The wal-index region the stand-in "unix" of [`SHM_STAND_IN`] maps.
static SHM_REGION: AtomicPtr<c_void> = AtomicPtr::new(null_mut());

unsafe extern "C" fn stand_in_shm_map(
    _file: *mut ffi::sqlite3_file,
    _pg: c_int,
    _pgsz: c_int,
    _extend: c_int,
    pp: *mut *mut c_void,
) -> c_int {
    // SAFETY: the method under test passes SQLite's writable `pp`.
    unsafe { *pp = SHM_REGION.load(Ordering::Acquire) };
    ffi::SQLITE_OK
}

unsafe extern "C" fn stand_in_shm_unmap(_file: *mut ffi::sqlite3_file, _delete: c_int) -> c_int {
    ffi::SQLITE_OK
}

/// A stand-in "unix" file that maps its wal-index at [`SHM_REGION`].
static SHM_STAND_IN: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 2,
    xClose: Some(stand_in_close),
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
    xShmMap: Some(stand_in_shm_map),
    xShmLock: None,
    xShmBarrier: None,
    xShmUnmap: Some(stand_in_shm_unmap),
    xFetch: None,
    xUnfetch: None,
};

/// `len` bytes mapped shared, as "unix" maps a wal-index region, each `fill`.
fn shared_region(len: usize, fill: u8) -> *mut c_void {
    // SAFETY: a new anonymous mapping, at an address the kernel picks.
    let region = unsafe {
        libc::mmap(
            null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_ANON,
            -1,
            0,
        )
    };
    assert_ne!(
        region,
        libc::MAP_FAILED,
        "{}",
        std::io::Error::last_os_error()
    );
    // SAFETY: `region` is `len` writable bytes.
    unsafe { std::ptr::write_bytes(region.cast::<u8>(), fill, len) };
    region
}

/// Whether every one of the `len` bytes at `region` is `byte`.
fn all(region: *mut c_void, len: usize, byte: u8) -> bool {
    // SAFETY: `region` is `len` readable bytes.
    unsafe { std::slice::from_raw_parts(region.cast::<u8>(), len) }
        .iter()
        .all(|b| *b == byte)
}

/// Forks a child that, as SQLite does through the pointers it cached, overwrites the `len` bytes
/// at each of `regions` (each `was` before), and waits for it.
fn fork_writing(regions: &[(*mut c_void, u8)], len: usize) {
    // SAFETY: the child runs only the closure below and ends with `_exit`.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork: {}", std::io::Error::last_os_error());
    if pid == 0 {
        // SAFETY: a plain libc call.
        unsafe { libc::alarm(20) };
        exit_with(|| {
            let mut checks = vec![("forked", crate::fork::forked())];
            for &(region, was) in regions {
                checks.push(("the child's copy is the parent's", all(region, len, was)));
                // SAFETY: `region` is `len` writable bytes in the child.
                unsafe { std::ptr::write_bytes(region.cast::<u8>(), 0xEE, len) };
            }
            checks
        });
    }
    let mut status: c_int = 0;
    // SAFETY: waits for the child forked above.
    assert_eq!(unsafe { libc::waitpid(pid, &raw mut status, 0) }, pid);
    assert!(libc::WIFEXITED(status), "the forked child died");
    assert_eq!(
        libc::WEXITSTATUS(status),
        0,
        "a check of the forked child failed"
    );
}

// A write transaction open at a fork leaves the child with SQLite's pointers into the wal-index
// regions "unix" mapped shared, and its rollback or close writes through them, calling no VFS
// method (`sqlite3WalUndo` clears the hash entries above the index's last frame). So the child
// gets a private copy, with the same bytes at the same address, of every region a main db handle
// of a bound database mapped: what it writes there never reaches the parent (design: Forks). A
// region of a file of no bound database stays shared, and so does a region whose handle unmapped
// its wal-index before the fork.
#[test]
fn a_forked_child_writes_only_its_own_copy_of_a_bound_wal_index() {
    let dir = TempDir::new("vfs-fork-shm");
    let b = for_tests(dir.path());
    let registered = registered_for_tests(&b);
    crate::fork::watch();
    crate::db::start_process();
    let len = 32768usize;
    let (bound, plain_region) = (shared_region(len, 0x11), shared_region(len, 0x22));
    let mut main = file_of(&b, true, &raw const SHM_STAND_IN);
    let mut plain = File {
        base: ffi::sqlite3_file {
            pMethods: &raw const METHODS,
        },
        ext: null_mut(),
        fail_sync: false,
        wal_name: null_mut(),
        inner: ffi::sqlite3_file {
            pMethods: &raw const SHM_STAND_IN,
        },
    };
    let [m, p] = [&mut main, &mut plain].map(|f| (&raw mut *f).cast::<ffi::sqlite3_file>());
    for (f, region) in [(m, bound), (p, plain_region)] {
        SHM_REGION.store(region, Ordering::Release);
        let mut got: *mut c_void = null_mut();
        // SAFETY: an open file of this VFS over the stand-in, and a writable `got`.
        let mapped = unsafe { METHODS.xShmMap.unwrap()(f, 0, 32768, 1, &raw mut got) };
        assert_eq!((mapped, got), (ffi::SQLITE_OK, region));
    }

    fork_writing(&[(bound, 0x11), (plain_region, 0x22)], len);
    assert!(all(bound, len, 0x11), "the child wrote its own copy");
    assert!(
        all(plain_region, len, 0xEE),
        "a plain file's region stays shared"
    );

    // SAFETY: an open file of this VFS over the stand-in.
    assert_eq!(unsafe { METHODS.xShmUnmap.unwrap()(m, 0) }, ffi::SQLITE_OK);
    fork_writing(&[(bound, 0x11)], len);
    assert!(
        all(bound, len, 0xEE),
        "an unmapped handle's region is the child's no longer"
    );

    drop(registered);
    // SAFETY: the `Box` `file_of` made, which nothing freed.
    drop(unsafe { take_ext(m) });
    for region in [bound, plain_region] {
        // SAFETY: a mapping `shared_region` made, which nothing lists any more.
        assert_eq!(unsafe { libc::munmap(region, len) }, 0);
    }
}
