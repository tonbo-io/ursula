//! The fork guard (design: Forks).
//!
//! The extension starts a process with the first `ursula_open`, `ursula_events` or `ursula_import`
//! ([`db::start_process`]), before it makes anything that lives on in the process:
//! the driver and its threads, HTTP agents with their pooled connections, the resolver's cache, a
//! token. A child forked after that has all of it in its memory but none of the threads that serve
//! it, and a lock one of them held at the fork stays held in the child forever. So in such a child
//! every SQL function and every method on a file of a bound database fails `forked` without
//! running, closing such a file touches nothing the parent holds, and files of no bound database
//! pass through to "unix".
//!
//! The fork handlers are registered when the extension loads ([`watch`]). Before every fork the
//! thread that forks takes the registry and the table of shared wal-index regions ([`prepare`]),
//! so no other thread holds either at the fork, the child gets both whole, and a start falls
//! wholly before the fork or after it. In the child, every wal-index region a main db handle of a
//! bound database mapped is replaced with a private copy at the same address ([`private`]): SQLite
//! reaches those regions through pointers it cached and calls no VFS method for it, so without the
//! copy a rollback or a close of a write transaction the child inherited would clear entries of
//! the parent's index.
#![expect(
    unsafe_code,
    reason = "fork handlers are C callbacks registered with pthread_atfork, and a forked child \
              remaps wal-index regions"
)]

use std::collections::BTreeMap;
use std::ffi::c_void;
use std::ptr::null_mut;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::Once;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::db;
use crate::log;
use crate::log::Level;

/// Set in a child forked after the process started the extension, and in every process forked
/// from one.
static FORKED: AtomicBool = AtomicBool::new(false);

/// Whether this process is a child forked after its parent started the extension.
pub(crate) fn forked() -> bool {
    FORKED.load(Ordering::Acquire)
}

/// The `forked` line was written in this process ([`refused`]).
static SAID: AtomicBool = AtomicBool::new(false);

/// The line [`refused`] writes, in the format of [`crate::log`].
const FORKED_LINE: &[u8] = b"sqlite-ursula-vfs level=error event=forked reason=\"the process \
forked after the extension started: open databases after the fork\"\n";

/// In a forked child, the first refused open of, or I/O on, a file of a bound database: one
/// `forked` line on stderr, so the host's I/O error is attributed. It is written with one
/// `write(2)`, since [`log::emit`] takes the lock of `std::io::stderr`, which a thread of the
/// parent may have held at the fork.
pub(crate) fn refused() {
    if !SAID.swap(true, Ordering::AcqRel) {
        // SAFETY: `FORKED_LINE` is `FORKED_LINE.len()` readable bytes.
        let _written = unsafe { libc::write(2, FORKED_LINE.as_ptr().cast(), FORKED_LINE.len()) };
    }
}

/// A page-aligned range of a wal-index mapping.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Range {
    start: usize,
    len: usize,
}

/// The wal-index regions "unix" mapped for the main db handles of bound databases, by handle
/// address: a forked child makes them private. A handle's ranges are dropped before "unix" may
/// unmap them, so each one is mapped while it is listed.
type Shared = BTreeMap<usize, Vec<Range>>;

static SHARED: Mutex<Shared> = Mutex::new(BTreeMap::new());

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// This process's page size.
fn page_size() -> usize {
    static P: OnceLock<usize> = OnceLock::new();
    *P.get_or_init(|| {
        // SAFETY: a plain libc call.
        let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        usize::try_from(size)
            .ok()
            .filter(|size| size.is_power_of_two())
            .unwrap_or(4096)
    })
}

/// The pages `len` bytes at `region` lie on.
fn pages_of(region: *mut c_void, len: usize) -> Option<Range> {
    let page = page_size();
    let addr = region.expose_provenance();
    let start = addr.checked_sub(addr.checked_rem(page)?)?;
    let end = addr.checked_add(len)?.checked_next_multiple_of(page)?;
    Some(Range {
        start,
        len: end.checked_sub(start)?,
    })
}

/// `xShmMap` of the main db handle at address `handle`, a bound database's, mapped `len` bytes of
/// its wal-index at `region`, shared with every process that maps it.
pub(crate) fn mapped(handle: usize, region: *mut c_void, len: usize) {
    let Some(range) = pages_of(region, len) else {
        return;
    };
    let mut shared = lock(&SHARED);
    let ranges = shared.entry(handle).or_default();
    if !ranges.contains(&range) {
        ranges.push(range);
    }
}

/// The main db handle at address `handle` is about to unmap its wal-index, or to close.
pub(crate) fn unmapping(handle: usize) {
    lock(&SHARED).remove(&handle);
}

/// Registers the fork handlers, once per process: when the extension loads, before anything takes
/// the registry. A failed registration is logged, and forks then go unguarded.
pub(crate) fn watch() {
    static WATCHED: Once = Once::new();
    WATCHED.call_once(|| {
        // SAFETY: the handlers are `extern "C"` functions that live for the process, take no
        // argument and never unwind (they neither panic nor call anything that does).
        let rc = unsafe { libc::pthread_atfork(Some(prepare), Some(parent), Some(child)) };
        if rc != 0 {
            log::emit(Level::Error, "fork_guard_failed", &[(
                "error",
                &std::io::Error::from_raw_os_error(rc),
            )]);
        }
    });
}

/// What the thread that forks holds from [`prepare`] to [`parent`] or [`child`]: the registry,
/// then the shared regions.
struct Held {
    registry: db::Held,
    shared: MutexGuard<'static, Shared>,
}

/// `Held`'s locks across the fork.
struct Slot(Option<Held>);

// SAFETY: a `Held` is made by `prepare` and dropped by `parent` or `child`, which run on the thread
// that forks, so each lock is released by the thread that took it. It sits in `HELD` only so that
// the handlers, which take no argument, can reach it, and only the thread holding the registry
// touches `HELD`.
unsafe impl Send for Slot {}

static HELD: Mutex<Slot> = Mutex::new(Slot(None));

/// Before a fork, on the thread that forks: takes the registry and the shared regions, so no other
/// thread holds either at the fork.
extern "C" fn prepare() {
    let held = Held {
        registry: db::hold_for_fork(),
        shared: lock(&SHARED),
    };
    lock(&HELD).0 = Some(held);
}

/// After a fork, in the parent: lets both go.
extern "C" fn parent() {
    drop(lock(&HELD).0.take());
}

/// After a fork, in the child, its only thread: makes every shared region private, once each,
/// marks the child forked if the process had started, then lets both go. In a child of a forked
/// process the listed regions are private already (or inaccessible, when [`private`] could not
/// copy one), so it makes none of them private again.
extern "C" fn child() {
    let held = lock(&HELD).0.take();
    if let Some(held) = &held {
        if !forked() {
            let listed = || held.shared.values().flatten();
            for (i, range) in listed().enumerate() {
                // The handles of one database map the same regions: each is copied once, without
                // allocating in the child.
                if listed().take(i).any(|earlier| earlier == range) {
                    continue;
                }
                // SAFETY: a listed range is mapped, and this is the child's only thread.
                unsafe { private(*range) };
            }
        }
        if held.registry.started() {
            FORKED.store(true, Ordering::Release);
        }
    }
    drop(held);
}

/// Replaces the mapping of `r` with a private copy of its bytes at the same address. When it
/// cannot, it takes all access to `r` away instead: the child faults on it rather than writing
/// the parent's wal-index.
///
/// # Safety
///
/// `r` is mapped, and no other thread runs.
unsafe fn private(r: Range) {
    let at = std::ptr::with_exposed_provenance_mut::<c_void>(r.start);
    let rw = libc::PROT_READ | libc::PROT_WRITE;
    let anon = libc::MAP_PRIVATE | libc::MAP_ANON;
    // SAFETY: a new anonymous mapping, at an address the kernel picks.
    let copy = unsafe { libc::mmap(null_mut(), r.len, rw, anon, -1, 0) };
    if copy == libc::MAP_FAILED {
        // SAFETY: `r` is mapped (the caller's contract).
        let _protected = unsafe { libc::mprotect(at, r.len, libc::PROT_NONE) };
        return;
    }
    // SAFETY: `r` is mapped and readable, `copy` is `r.len` writable bytes, and they are apart.
    unsafe { std::ptr::copy_nonoverlapping(at.cast::<u8>(), copy.cast::<u8>(), r.len) };
    // SAFETY: replaces the pages of `r`, which nothing else in this thread uses meanwhile.
    let fixed = unsafe { libc::mmap(at, r.len, rw, anon | libc::MAP_FIXED, -1, 0) };
    if fixed == at {
        // SAFETY: `copy` holds `r.len` bytes, and `at` is `r.len` writable bytes apart from it.
        unsafe { std::ptr::copy_nonoverlapping(copy.cast::<u8>(), at.cast::<u8>(), r.len) };
    } else {
        // SAFETY: `r` is still mapped: a failed `MAP_FIXED` leaves the old mapping.
        let _protected = unsafe { libc::mprotect(at, r.len, libc::PROT_NONE) };
    }
    // SAFETY: `copy` is the mapping made above, unmapped once.
    let _unmapped = unsafe { libc::munmap(copy, r.len) };
}
