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
use std::sync::Arc;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;

use libsqlite3_sys as ffi;

use crate::attach::attach;
use crate::auth::set_token;
use crate::db::Mode;
use crate::error::Error;
use crate::error::WriteRefusal;
use crate::guard::contain;
use crate::host::API;
use crate::host::OK;
use crate::host::UNIX;
use crate::host::api;
use crate::status::stats;
use crate::status::status;
use crate::vfs::File;
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
/// kind ([`ErrorKind`]) a stable token in a fixed place, which callers branch on instead of the
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

/// The code `ursula_attach` fails with when `e` refused it before anything reached the stream
/// (nothing was claimed or created), which callers tell from other failures (a read-only attach
/// can read the database meanwhile): SQLITE_AUTH for a 402 (as since 0.7.1), SQLITE_IOERR_AUTH
/// (the code of a refused commit) for every other refusal. `None` for any other error.
fn refused_code(e: &Error) -> Option<c_int> {
    e.refusal().map(|refusal| match refusal {
        WriteRefusal::PaymentRequired { .. } => ffi::SQLITE_AUTH,
        _ => ffi::SQLITE_IOERR_AUTH,
    })
}

/// `ursula_attach(path, url[, mode])`: the mode is `'owner'` (the default) or `'read_only'` (see
/// `Mode`). An attach refused before anything reached the stream fails with `refused_code`.
unsafe fn fn_attach(
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
    let refused = outcome.as_ref().err().and_then(|e| refused_code(e));
    // SAFETY: SQLite calls the function with its context.
    unsafe {
        result(
            ctx,
            "ursula_attach",
            outcome.as_ref().cloned().map_err(|e| &**e),
        )
    };
    if let Some(code) = refused {
        // SAFETY: as above; `result` set the error.
        unsafe { result_error_code(ctx, code) };
    }
}

unsafe fn fn_status(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite calls the function with its arguments.
    let path = unsafe { arg(argc, argv, 0) };
    // SAFETY: SQLite calls the function with its context.
    unsafe { result(ctx, "ursula_status", status(&path).as_ref().cloned()) };
}

unsafe fn fn_stats(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite calls the function with its arguments.
    let path = unsafe { arg(argc, argv, 0) };
    // SAFETY: SQLite calls the function with its context.
    unsafe { result(ctx, "ursula_stats", stats(&path).as_ref().cloned()) };
}

/// `ursula_set_token(token)`: the bearer token every request carries from now on; NULL or `''` goes
/// back to `URSULA_VFS_TOKEN_FILE`.
unsafe fn fn_set_token(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite calls the function with its arguments.
    let token = unsafe { arg(argc, argv, 0) };
    set_token(Some(&token));
    // SAFETY: SQLite calls the function with its context.
    unsafe { result(ctx, "ursula_set_token", Ok(String::new())) };
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

sql_entry!(c_attach => fn_attach, "ursula_attach");
sql_entry!(c_status => fn_status, "ursula_status");
sql_entry!(c_stats => fn_stats, "ursula_stats");
sql_entry!(c_set_token => fn_set_token, "ursula_set_token");

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
        (c"ursula_attach", 2, c_attach),
        (c"ursula_attach", 3, c_attach),
        (c"ursula_status", 1, c_status),
        (c"ursula_stats", 1, c_stats),
        (c"ursula_set_token", 1, c_set_token),
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
        xOpen: Some(c_open),
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
    use libsqlite3_sys as ffi;

    use super::refused_code;
    use crate::error::Error;
    use crate::error::WriteRefusal;

    // An attach refused before anything reached the stream fails with a code of its own:
    // SQLITE_AUTH for a 402 (as since 0.7.1), SQLITE_IOERR_AUTH (a refused commit's) for any other
    // refusal. Every other failure keeps SQLITE_ERROR, a denied read included.
    #[test]
    fn refused_attaches_fail_with_codes_of_their_own() {
        let url = || "http://h/b/s".to_owned();
        let refused = |refusal| Error::Refused {
            op: "claim",
            url: url(),
            refusal,
        };
        let payment = WriteRefusal::PaymentRequired {
            body: String::new(),
        };
        assert_eq!(refused_code(&refused(payment)), Some(ffi::SQLITE_AUTH));
        for refusal in [
            WriteRefusal::Unauthorized {
                body: String::new(),
            },
            WriteRefusal::Forbidden {
                body: String::new(),
            },
            WriteRefusal::RateLimited {
                body: String::new(),
            },
            WriteRefusal::NotApplied {
                status: 503,
                reason: "RaftLogPressure".into(),
            },
        ] {
            assert_eq!(
                refused_code(&refused(refusal)),
                Some(ffi::SQLITE_IOERR_AUTH)
            );
        }
        let denied_read = Error::Unauthorized {
            op: "head",
            url: url(),
            status: 403,
            body: String::new(),
        };
        assert_eq!(refused_code(&denied_read), None);
        assert_eq!(refused_code(&Error::ClaimRaces { url: url() }), None);
    }
}
