//! The loadable extension's entry point and its SQL functions.

use std::ffi::CStr;
use std::ffi::CString;
use std::ffi::c_char;
use std::ffi::c_int;
use std::ptr::null_mut;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;

use libsqlite3_sys as ffi;

use crate::attach::attach;
use crate::host::API;
use crate::host::OK;
use crate::host::UNIX;
use crate::host::api;
use crate::status::stats;
use crate::status::status;
use crate::vfs::File;
use crate::vfs::x_open;

pub(crate) static VFS: AtomicPtr<ffi::sqlite3_vfs> = AtomicPtr::new(null_mut());

pub(crate) unsafe fn arg(argv: *mut *mut ffi::sqlite3_value, i: usize) -> String {
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

/// `s` as a C string, NUL bytes replaced (SQLite takes the text up to the first one).
fn c_text(s: &str) -> CString {
    CString::new(s.replace('\0', " ")).unwrap_or_default()
}

unsafe fn result_error(ctx: *mut ffi::sqlite3_context, msg: &str) {
    let c = c_text(msg);
    unsafe { (api().result_error.unwrap())(ctx, c.as_ptr(), -1) };
}

unsafe fn result_text(ctx: *mut ffi::sqlite3_context, s: &str) {
    let c = c_text(s);
    unsafe { (api().result_text.unwrap())(ctx, c.as_ptr(), -1, ffi::SQLITE_TRANSIENT()) };
}

pub(crate) unsafe extern "C" fn fn_attach(
    ctx: *mut ffi::sqlite3_context,
    _argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    unsafe {
        match attach(&arg(argv, 0), &arg(argv, 1)) {
            Ok(offset) => result_text(ctx, &offset),
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
            Ok(s) => result_text(ctx, &s),
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
            Ok(s) => result_text(ctx, &s),
            Err(e) => result_error(ctx, &format!("ursula_stats: {e}")),
        }
    }
}

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
