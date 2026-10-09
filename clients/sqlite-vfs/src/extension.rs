//! The loadable extension's entry point and its SQL functions.
//!
//! No panic may unwind out of these `extern "C"` functions (it would abort the host process): the
//! code they reach is free of panicking constructs, which the crate's lints enforce.
#![expect(
    unsafe_code,
    reason = "a SQLite loadable extension exports a C entry point and C callbacks"
)]

use std::ffi::CStr;
use std::ffi::CString;
use std::ffi::c_char;
use std::ffi::c_int;
use std::ptr::null_mut;
use std::sync::Arc;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;

use libsqlite3_sys as ffi;

use crate::attach::attach;
use crate::auth::set_token;
use crate::db::Mode;
use crate::error::Error;
use crate::host::API;
use crate::host::OK;
use crate::host::UNIX;
use crate::host::api;
use crate::status::stats;
use crate::status::status;
use crate::vfs::File;
use crate::vfs::x_open;

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

/// Returns `outcome` from a SQL function: its text, or the error as `name: error`.
///
/// # Safety
///
/// `ctx` is the context SQLite passed to the function.
unsafe fn result<E: std::fmt::Display>(
    ctx: *mut ffi::sqlite3_context,
    name: &str,
    outcome: Result<String, E>,
) {
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
            let c = c_text(&format!("{name}: {e}"));
            if let Some(result_error) = a.result_error {
                // SAFETY: `ctx` is the function's context, and SQLite copies the NUL-terminated
                // message.
                unsafe { result_error(ctx, c.as_ptr(), -1) };
            }
        }
    }
}

/// Sets the code a failed SQL function reports (`result` set its message; the code is
/// SQLITE_ERROR otherwise).
///
/// # Safety
///
/// `ctx` is the context SQLite passed to the function, whose error `result` set.
unsafe fn result_error_code(ctx: *mut ffi::sqlite3_context, code: c_int) {
    if let Some(result_error_code) = api().and_then(|a| a.result_error_code) {
        // SAFETY: `ctx` is the function's context (the caller's contract).
        unsafe { result_error_code(ctx, code) };
    }
}

/// `ursula_attach(path, url[, mode])`: the mode is `'owner'` (the default) or `'read_only'` (see
/// `Mode`). An attach refused with 402 in front of the stream fails with SQLITE_AUTH, which
/// callers tell from other failures (a read-only attach can read the database meanwhile).
unsafe extern "C" fn fn_attach(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite calls the function with its arguments.
    let path = unsafe { arg(argc, argv, 0) };
    // SAFETY: as above.
    let url = unsafe { arg(argc, argv, 1) };
    // SAFETY: as above (empty without a third argument).
    let mode = unsafe { arg(argc, argv, 2) };
    let outcome = Mode::try_from(mode.as_str())
        .map_err(Arc::new)
        .and_then(|mode| attach(&path, &url, mode));
    let refused = matches!(&outcome, Err(e) if e.is_payment_required());
    // SAFETY: SQLite calls the function with its context.
    unsafe { result(ctx, "ursula_attach", outcome) };
    if refused {
        // SAFETY: as above; `result` set the error.
        unsafe { result_error_code(ctx, ffi::SQLITE_AUTH) };
    }
}

unsafe extern "C" fn fn_status(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite calls the function with its arguments.
    let path = unsafe { arg(argc, argv, 0) };
    // SAFETY: SQLite calls the function with its context.
    unsafe { result(ctx, "ursula_status", status(&path)) };
}

unsafe extern "C" fn fn_stats(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite calls the function with its arguments.
    let path = unsafe { arg(argc, argv, 0) };
    // SAFETY: SQLite calls the function with its context.
    unsafe { result(ctx, "ursula_stats", stats(&path)) };
}

/// `ursula_set_token(token)`: the bearer token every request carries from now on; NULL or `''` goes
/// back to `URSULA_VFS_TOKEN_FILE`.
unsafe extern "C" fn fn_set_token(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite calls the function with its arguments.
    let token = unsafe { arg(argc, argv, 0) };
    set_token(Some(&token));
    // SAFETY: SQLite calls the function with its context.
    unsafe { result(ctx, "ursula_set_token", Ok::<_, Error>(String::new())) };
}

/// Registers the "ursula" VFS over "unix" as the default (once per process) and the SQL functions
/// on `db`, direct-only: a schema object of an untrusted database (a trigger, a view) cannot call
/// them to attach a file or replace the process's token.
///
/// # Safety
///
/// Called by SQLite when it loads the extension: `db` is the loading connection and `p_api` the
/// host's API routines, which outlive the process.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_extension_init(
    db: *mut ffi::sqlite3,
    _err: *mut *mut c_char,
    p_api: *const ffi::sqlite3_api_routines,
) -> c_int {
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
    let fns: [(&CStr, c_int, SqlFn); 5] = [
        (c"ursula_attach", 2, fn_attach),
        (c"ursula_attach", 3, fn_attach),
        (c"ursula_status", 1, fn_status),
        (c"ursula_stats", 1, fn_stats),
        (c"ursula_set_token", 1, fn_set_token),
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

/// Registers the "ursula" VFS: a copy of "unix" whose files carry this VFS's state in front of the
/// "unix" file (`File`), made the default.
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
        xOpen: Some(x_open),
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
