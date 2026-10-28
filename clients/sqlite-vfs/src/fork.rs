//! The fork guard (design: Forks).
//!
//! A child forked after the process driver started has the parent's bindings in its memory but
//! none of the threads that serve them: the driver, its workers and the snapshot threads stay in
//! the parent, and a lock one of them held at the fork stays held in the child forever. Using
//! those bindings in the child would stall `synced()` or append to the parent's log. So the
//! driver's start registers fork handlers ([`watch`]), and in such a child every SQL function and
//! every method on a file of a bound database fails `forked` without running, closing such a file
//! touches nothing the parent holds, and files of no bound database pass through to "unix".
//!
//! Whether a file is bound is the registry's answer, and the registry is a lock other threads
//! take: the thread that forks holds it across the fork ([`crate::db::hold_for_fork`]), so the
//! child gets it whole and unlocked.
#![expect(
    unsafe_code,
    reason = "fork handlers are C callbacks registered with pthread_atfork"
)]

use std::sync::Once;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::db;
use crate::log;
use crate::log::Level;

/// Set in a child forked after [`watch`] ran, and in every process forked from one.
static FORKED: AtomicBool = AtomicBool::new(false);

/// Whether this process is a child forked after the extension started its driver.
pub(crate) fn forked() -> bool {
    FORKED.load(Ordering::Acquire)
}

/// Registers the fork handlers, once per process: called as the process driver starts, before any
/// binding exists. A failed registration is logged, and forks then go unguarded.
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

/// Before a fork, on the thread that forks: takes the registry, so the child gets it whole.
extern "C" fn prepare() {
    db::hold_for_fork();
}

/// After a fork, in the parent: lets the registry go.
extern "C" fn parent() {
    db::release_after_fork();
}

/// After a fork, in the child: marks it forked, then lets the registry go.
extern "C" fn child() {
    FORKED.store(true, Ordering::Release);
    db::release_after_fork();
}
