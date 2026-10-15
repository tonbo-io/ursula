//! The loadable extension's entry point and its SQL functions.
//!
//! No panic may unwind out of these `extern "C"` functions (it would abort the host process): each
//! runs its body through `guard::contain`, which answers a panic with the function's error.
#![expect(
    unsafe_code,
    reason = "a SQLite loadable extension exports a C entry point and C callbacks"
)]

use std::ffi::CStr;
use std::ffi::CString;
use std::ffi::c_char;
use std::ffi::c_int;
use std::ptr::null_mut;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;
use std::time::Duration;

use libsqlite3_sys as ffi;

use crate::auth::set_token;
use crate::db;
use crate::driver::OpenId;
use crate::driver::SyncError;
use crate::driver::runtime::driver;
use crate::error::Error;
use crate::guard::contain;
use crate::host::API;
use crate::host::OK;
use crate::host::UNIX;
use crate::host::api;
use crate::status;
use crate::vfs::File;
use crate::vfs::c_access;
use crate::vfs::c_delete;
use crate::vfs::c_open;

/// The "ursula" VFS, registered once per process (null before).
static VFS: AtomicPtr<ffi::sqlite3_vfs> = AtomicPtr::new(null_mut());

/// The text of a SQL function's argument `i` (empty for NULL or a missing argument).
///
/// # Safety
///
/// `argv` and `argc` are what SQLite passed to the function.
unsafe fn arg(argc: c_int, argv: *mut *mut ffi::sqlite3_value, i: usize) -> String {
    let Some(value_text) = api().and_then(|a| a.value_text) else {
        return String::new();
    };
    if argv.is_null() {
        return String::new();
    }
    let n = usize::try_from(argc).unwrap_or_default();
    // SAFETY: SQLite passes `argc` argument values at `argv` (the caller's contract).
    let args = unsafe { std::slice::from_raw_parts(argv, n) };
    let Some(&value) = args.get(i) else {
        return String::new();
    };
    // SAFETY: `value` is one of the function's arguments, valid for the call.
    let p = unsafe { value_text(value) };
    if p.is_null() {
        return String::new();
    }
    // SAFETY: SQLite returns NUL-terminated UTF-8 text, valid until the argument changes; it is
    // copied at once.
    unsafe { CStr::from_ptr(p.cast()) }
        .to_string_lossy()
        .into_owned()
}

/// `s` as a C string, NUL bytes replaced (SQLite would take the text up to the first one).
fn c_text(s: &str) -> CString {
    CString::new(s.replace('\0', " ")).unwrap_or_default()
}

/// Returns `outcome` from a SQL function: its text, or the error as `name: kind: error`, the
/// kind ([`ErrorKind`](crate::error::ErrorKind)) a stable token in a fixed place, which callers branch on instead of the
/// text.
///
/// # Safety
///
/// `ctx` is the context SQLite passed to the function.
unsafe fn result(ctx: *mut ffi::sqlite3_context, name: &str, outcome: Result<String, &Error>) {
    let Some(a) = api() else {
        return;
    };
    match outcome {
        Ok(text) => {
            let c = c_text(&text);
            if let Some(result_text) = a.result_text {
                // SAFETY: `ctx` is the function's context, and SQLITE_TRANSIENT makes SQLite copy
                // the NUL-terminated text before `c` drops.
                unsafe { result_text(ctx, c.as_ptr(), -1, ffi::SQLITE_TRANSIENT()) };
            }
        }
        Err(e) => {
            let c = c_text(&format!("{name}: {}: {e}", e.kind().as_str()));
            if let Some(result_error) = a.result_error {
                // SAFETY: `ctx` is the function's context, and SQLite copies the NUL-terminated
                // message.
                unsafe { result_error(ctx, c.as_ptr(), -1) };
            }
        }
    }
}

/// Argument `i` as an integer: `None` for NULL or a missing argument.
///
/// # Safety
///
/// `argv` and `argc` are what SQLite passed to the function.
unsafe fn arg_int(argc: c_int, argv: *mut *mut ffi::sqlite3_value, i: usize) -> Option<i64> {
    let a = api()?;
    let (value_type, value_int64) = (a.value_type?, a.value_int64?);
    if argv.is_null() {
        return None;
    }
    let n = usize::try_from(argc).unwrap_or_default();
    // SAFETY: SQLite passes `argc` argument values at `argv` (the caller's contract).
    let args = unsafe { std::slice::from_raw_parts(argv, n) };
    let &value = args.get(i)?;
    // SAFETY: `value` is one of the function's arguments, valid for the call.
    if unsafe { value_type(value) } == ffi::SQLITE_NULL {
        return None;
    }
    // SAFETY: as above.
    Some(unsafe { value_int64(value) })
}

/// Argument `i` as a non-negative integer (`name` for the error), 0 for NULL.
///
/// # Safety
///
/// As [`arg_int`].
unsafe fn arg_count(
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
    i: usize,
    name: &'static str,
) -> Result<u64, Error> {
    // SAFETY: the caller's contract.
    let n = unsafe { arg_int(argc, argv, i) }.unwrap_or(0);
    u64::try_from(n).map_err(|_negative| Error::InvalidArgument {
        name,
        why: format!("{n} is negative"),
    })
}

/// `ursula_open(url, dir, options_json, timeout_ms)` (design §8.6): `{done, path, name, state}`,
/// or `{done: false, request}` while the open goes on. `dir` NULL or `''` is the default cache
/// root; `timeout_ms` 0 returns at once.
unsafe fn fn_open(ctx: *mut ffi::sqlite3_context, argc: c_int, argv: *mut *mut ffi::sqlite3_value) {
    // SAFETY: SQLite calls the function with its arguments.
    let url = unsafe { arg(argc, argv, 0) };
    // SAFETY: as above.
    let dir = unsafe { arg(argc, argv, 1) };
    // SAFETY: as above.
    let options = unsafe { arg(argc, argv, 2) };
    // SAFETY: as above.
    let outcome = unsafe { arg_count(argc, argv, 3, "timeout_ms") }.and_then(|timeout| {
        let dir = (!dir.is_empty()).then_some(dir.as_str());
        db::open(&url, dir, &options, timeout).map(|opened| status::opened_json(&opened))
    });
    // SAFETY: SQLite calls the function with its context.
    unsafe { result(ctx, "ursula_open", outcome.as_ref().map(Clone::clone)) };
}

/// `ursula_synced(path, timeout_ms)` (design §8.2): `{synced, ticket, target, acknowledged}`; with
/// `synced` false the ticket resolves later, when a `synced` event of its path reaches `through` it
/// (`ursula_events`). A reset fails it.
unsafe fn fn_synced(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite calls the function with its arguments.
    let path = unsafe { arg(argc, argv, 0) };
    // SAFETY: as above.
    let outcome = unsafe { arg_count(argc, argv, 1, "timeout_ms") }.and_then(|timeout| {
        let b = db::bound(&path)?;
        if let Some(kind) = b.reset_kind() {
            return Err(Error::Reset(kind));
        }
        // The last commit logged at the call (design §8.2): every commit a reader can see.
        let target = Some(b.handle.last_commit());
        // With timeout 0 the outcome comes from `ursula_events` alone, and nothing is kept for it.
        let ticket = driver().sync(b.id, target, timeout > 0)?;
        if timeout == 0 {
            return Ok(status::synced_json(
                false,
                ticket.0,
                target,
                b.handle.acked(),
            ));
        }
        match driver().wait(ticket, Duration::from_millis(timeout)) {
            None => Ok(status::synced_json(
                false,
                ticket.0,
                target,
                b.handle.acked(),
            )),
            Some(Ok(())) => Ok(status::synced_json(
                true,
                ticket.0,
                target,
                b.handle.acked(),
            )),
            // The binding reset while the call went out: the reset, not its retirement.
            Some(Err(SyncError::NotBound)) => Err(b.reset_kind().map_or_else(
                || Error::NotBound {
                    path: b.path.clone(),
                },
                Error::Reset,
            )),
            Some(Err(e)) => Err(e.into()),
        }
    });
    // SAFETY: SQLite calls the function with its context.
    unsafe { result(ctx, "ursula_synced", outcome.as_ref().map(Clone::clone)) };
}

/// `ursula_events(cursor, timeout_ms)` (design §8.6): `{cursor, events}`, the events from `cursor`
/// on, waiting at most `timeout_ms` for one.
unsafe fn fn_events(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite calls the function with its arguments.
    let outcome = unsafe { arg_count(argc, argv, 0, "cursor") }.and_then(|cursor| {
        // SAFETY: as above.
        let timeout = unsafe { arg_count(argc, argv, 1, "timeout_ms") }?;
        let (events, next) = driver().events(cursor, Duration::from_millis(timeout));
        Ok(status::events_json(&events, next))
    });
    // SAFETY: SQLite calls the function with its context.
    unsafe { result(ctx, "ursula_events", outcome.as_ref().map(Clone::clone)) };
}

/// `ursula_cancel(request)`: gives up an open in progress, and returns what it ended with
/// (`{kind, action, message}`: the last condition it met, or `cancelled` when it met none), or
/// `null` when it had bound already.
unsafe fn fn_cancel(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite calls the function with its arguments.
    let outcome = unsafe { arg_count(argc, argv, 0, "request") }
        .and_then(|request| db::cancel(OpenId(request)))
        .map(|ended| ended.map_or_else(|| "null".to_owned(), |e| status::error_json(&e)));
    // SAFETY: SQLite calls the function with its context.
    unsafe { result(ctx, "ursula_cancel", outcome.as_ref().map(Clone::clone)) };
}

unsafe fn fn_status(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite calls the function with its arguments.
    let path = unsafe { arg(argc, argv, 0) };
    let outcome = db::bound(&path).and_then(|b| status::status_json(&b));
    // SAFETY: SQLite calls the function with its context.
    unsafe { result(ctx, "ursula_status", outcome.as_ref().map(Clone::clone)) };
}

unsafe fn fn_stats(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite calls the function with its arguments.
    let path = unsafe { arg(argc, argv, 0) };
    let outcome = db::bound(&path).map(|b| {
        let drained = b.lock().stats.drain();
        status::stats_json(&drained)
    });
    // SAFETY: SQLite calls the function with its context.
    unsafe { result(ctx, "ursula_stats", outcome.as_ref().map(Clone::clone)) };
}

/// `ursula_set_token(path, token)`: the bearer token requests carry from now on; NULL or `''` goes
/// back to `URSULA_VFS_TOKEN_FILE`. M1 has one token per process, so a path (which must name a
/// bound database) sets it as NULL does; M3's token providers make it per database.
unsafe fn fn_set_token(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite calls the function with its arguments.
    let path = unsafe { arg(argc, argv, 0) };
    // SAFETY: as above.
    let token = unsafe { arg(argc, argv, 1) };
    let outcome = if path.is_empty() {
        Ok(())
    } else {
        db::bound(&path).map(drop)
    }
    .map(|()| {
        set_token(Some(&token));
        String::new()
    });
    // SAFETY: SQLite calls the function with its context.
    unsafe { result(ctx, "ursula_set_token", outcome.as_ref().map(Clone::clone)) };
}

/// A SQL function's C entry point: runs the function named without `c_` through [`contain`], so a
/// panic in it ends the call with an error instead of unwinding into SQLite, which aborts the host
/// process.
macro_rules! sql_entry {
    ($entry:ident => $body:ident, $name:literal) => {
        unsafe extern "C" fn $entry(
            ctx: *mut ffi::sqlite3_context,
            argc: c_int,
            argv: *mut *mut ffi::sqlite3_value,
        ) {
            contain(
                $name,
                || {
                    // SAFETY: SQLite calls the function with its context.
                    unsafe { result(ctx, $name, Err(&Error::Panicked { entry: $name })) }
                },
                || {
                    // SAFETY: SQLite's call of the function, forwarded with its arguments.
                    unsafe { $body(ctx, argc, argv) }
                },
            )
        }
    };
}

sql_entry!(c_ursula_open => fn_open, "ursula_open");
sql_entry!(c_synced => fn_synced, "ursula_synced");
sql_entry!(c_events => fn_events, "ursula_events");
sql_entry!(c_cancel => fn_cancel, "ursula_cancel");
sql_entry!(c_status => fn_status, "ursula_status");
sql_entry!(c_stats => fn_stats, "ursula_stats");
sql_entry!(c_set_token => fn_set_token, "ursula_set_token");

/// Refuses a host SQLite the extension does not support (`gate`), then registers the "ursula" VFS
/// over "unix" as the default (once per process) and the SQL functions on `db`, direct-only: a
/// schema object of an untrusted database (a trigger, a view) cannot call them to open a stream or
/// replace the process's token.
///
/// # Safety
///
/// Called by SQLite when it loads the extension: `db` is the loading connection and `p_api` the
/// host's API routines, which outlive the process.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_extension_init(
    db: *mut ffi::sqlite3,
    err: *mut *mut c_char,
    p_api: *const ffi::sqlite3_api_routines,
) -> c_int {
    contain(
        "sqlite3_extension_init",
        || {
            // SAFETY: SQLite's arguments, as above.
            unsafe { set_init_error(err, p_api) };
            ffi::SQLITE_ERROR
        },
        || {
            // SAFETY: SQLite's call of the entry point, forwarded with its arguments.
            unsafe { init(db, err, p_api) }
        },
    )
}

/// After a panic in [`init`]: the load's error message, which the host shows, allocated with the
/// host's `sqlite3_mprintf` (SQLite frees it). Nothing when the host gave no routines or no place
/// for the message.
///
/// # Safety
///
/// As [`sqlite3_extension_init`]: `err` is null or writable, `p_api` null or the host's routines.
unsafe fn set_init_error(err: *mut *mut c_char, p_api: *const ffi::sqlite3_api_routines) {
    // SAFETY: the caller's contract.
    let Some(mprintf) = unsafe { p_api.as_ref() }.and_then(|api| api.mprintf) else {
        return;
    };
    if err.is_null() {
        return;
    }
    // SAFETY: a format without conversions, so `mprintf` reads no further argument.
    let message =
        unsafe { mprintf(c"sqlite3_extension_init panicked inside the extension".as_ptr()) };
    // SAFETY: the caller's contract: `err` is writable.
    unsafe { *err = message };
}

/// [`sqlite3_extension_init`]'s body.
///
/// # Safety
///
/// As [`sqlite3_extension_init`].
unsafe fn init(
    db: *mut ffi::sqlite3,
    err: *mut *mut c_char,
    p_api: *const ffi::sqlite3_api_routines,
) -> c_int {
    // SAFETY: the caller's contract: `p_api` is null or the host's routines.
    let Some(a) = (unsafe { p_api.as_ref() }) else {
        return ffi::SQLITE_ERROR;
    };
    if let Err(refusal) = gate(a) {
        // SAFETY: the caller's contract.
        unsafe { set_error(err, a, &refusal.to_string()) };
        return ffi::SQLITE_ERROR;
    }
    API.store(p_api.cast_mut(), Ordering::Release);
    let Some(a) = api() else {
        return ffi::SQLITE_ERROR;
    };
    if VFS.load(Ordering::Acquire).is_null() {
        let rc = register_vfs(a);
        if rc != OK {
            return rc;
        }
    }
    let Some(create) = a.create_function_v2 else {
        return ffi::SQLITE_ERROR;
    };
    type SqlFn =
        unsafe extern "C" fn(*mut ffi::sqlite3_context, c_int, *mut *mut ffi::sqlite3_value);
    let fns: [(&CStr, c_int, SqlFn); 7] = [
        (c"ursula_open", 4, c_ursula_open),
        (c"ursula_synced", 2, c_synced),
        (c"ursula_events", 2, c_events),
        (c"ursula_cancel", 1, c_cancel),
        (c"ursula_status", 1, c_status),
        (c"ursula_stats", 1, c_stats),
        (c"ursula_set_token", 2, c_set_token),
    ];
    for (name, n, f) in fns {
        // SAFETY: `db` is the loading connection, the name is NUL-terminated and static, and the
        // function takes no user data and no destructor.
        let rc = unsafe {
            create(
                db,
                name.as_ptr(),
                n,
                ffi::SQLITE_UTF8 | ffi::SQLITE_DIRECTONLY,
                null_mut(),
                Some(f),
                None,
                None,
                None,
            )
        };
        if rc != OK {
            return rc;
        }
    }
    ffi::SQLITE_OK_LOAD_PERMANENTLY
}

/// The oldest SQLite the extension loads into: 3.50.4, Node 22.19.0's (the oldest Node the
/// package supports, on which CI runs the PHASETWO conformance), as `sqlite3_libversion_number`
/// spells it. Publication relies on when SQLite sends `SQLITE_FCNTL_COMMIT_PHASETWO` (design §4.2).
pub(crate) const MIN_SQLITE: c_int = 3_050_004;

/// Why the extension refuses to load into the host's SQLite.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum GateRefusal {
    #[error(
        "sqlite-ursula-vfs needs SQLite {} or newer, the host runs {}",
        version(MIN_SQLITE),
        version(*.0)
    )]
    TooOld(c_int),
    #[error("sqlite-ursula-vfs needs WAL mode, which the host's SQLite omits (SQLITE_OMIT_WAL)")]
    NoWal,
    #[error("the host's SQLite lacks {0}")]
    MissingRoutine(&'static str),
}

/// `3050004` as `3.50.4`.
fn version(n: c_int) -> String {
    let (major, minor, patch) = (n / 1_000_000, n / 1_000 % 1_000, n % 1_000);
    format!("{major}.{minor}.{patch}")
}

/// Whether the host's SQLite is one the extension supports: new enough, with WAL.
fn gate(a: &ffi::sqlite3_api_routines) -> Result<(), GateRefusal> {
    let libversion = a
        .libversion_number
        .ok_or(GateRefusal::MissingRoutine("sqlite3_libversion_number"))?;
    let used = a
        .compileoption_used
        .ok_or(GateRefusal::MissingRoutine("sqlite3_compileoption_used"))?;
    // SAFETY: the host's routine, which takes no argument.
    let n = unsafe { libversion() };
    if n < MIN_SQLITE {
        return Err(GateRefusal::TooOld(n));
    }
    // SAFETY: the option name is NUL-terminated.
    if unsafe { used(c"OMIT_WAL".as_ptr()) } != 0 {
        return Err(GateRefusal::NoWal);
    }
    Ok(())
}

/// The load's error message, allocated with the host's `sqlite3_mprintf` (SQLite frees it).
///
/// # Safety
///
/// `err` is null or writable.
unsafe fn set_error(err: *mut *mut c_char, a: &ffi::sqlite3_api_routines, message: &str) {
    let Some(mprintf) = a.mprintf else {
        return;
    };
    if err.is_null() {
        return;
    }
    let message = c_text(message);
    // SAFETY: "%s" reads one NUL-terminated string, the one passed.
    let copy = unsafe { mprintf(c"%s".as_ptr(), message.as_ptr()) };
    // SAFETY: the caller's contract: `err` is writable.
    unsafe { *err = copy };
}

/// Registers the "ursula" VFS: a copy of "unix" whose files carry this VFS's state in front of the
/// "unix" file (`File`), made the default. `xOpen`, `xAccess` and `xDelete` are its own (a bound
/// db's WAL is `db-uwal`); every other method is "unix"'s.
fn register_vfs(a: &ffi::sqlite3_api_routines) -> c_int {
    let (Some(vfs_find), Some(vfs_register)) = (a.vfs_find, a.vfs_register) else {
        return ffi::SQLITE_ERROR;
    };
    // SAFETY: the name is NUL-terminated.
    let u = unsafe { vfs_find(c"unix".as_ptr()) };
    // SAFETY: a non-null result is SQLite's registered "unix" VFS, which lives for the process.
    let Some(unix) = (unsafe { u.as_ref() }) else {
        return ffi::SQLITE_ERROR;
    };
    let Some(size) = c_int::try_from(std::mem::offset_of!(File, inner))
        .ok()
        .and_then(|inner| inner.checked_add(unix.szOsFile))
    else {
        return ffi::SQLITE_ERROR;
    };
    UNIX.store(u, Ordering::Release);
    let v = Box::into_raw(Box::new(ffi::sqlite3_vfs {
        zName: c"ursula".as_ptr(),
        szOsFile: size,
        pNext: null_mut(),
        xOpen: Some(c_open),
        xDelete: Some(c_delete),
        xAccess: Some(c_access),
        ..*unix
    }));
    // SAFETY: `v` is a complete VFS that is never freed (SQLite keeps it registered for the
    // process); 1 makes it the default.
    let rc = unsafe { vfs_register(v, 1) };
    if rc == OK {
        VFS.store(v, Ordering::Release);
    }
    rc
}

#[cfg(test)]
mod tests {
    use super::MIN_SQLITE;
    use super::version;

    // The floor is Node 22.19.0's SQLite.
    #[test]
    fn the_floor_is_3_50_4() {
        assert_eq!(version(MIN_SQLITE), "3.50.4");
        assert_eq!(version(3_051_002), "3.51.2");
    }
}
