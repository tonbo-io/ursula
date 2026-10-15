//! The host's SQLite, reached through the `sqlite3_api_routines` it passed at load: the
//! "unix" VFS underneath, path resolution, and a private connection outside this VFS's
//! bookkeeping.
//!
//! Everything here is a safe wrapper over the host's C API: the rest of the crate never touches it
//! directly.
#![expect(
    unsafe_code,
    reason = "a SQLite loadable extension calls the host's C API through raw pointers"
)]

use std::ffi::CStr;
use std::ffi::CString;
use std::ffi::c_int;
use std::ptr::null_mut;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;

use libsqlite3_sys as ffi;

use crate::error::ErrorKind;

pub(crate) const OK: c_int = ffi::SQLITE_OK;

/// The routines the host passed to `sqlite3_extension_init`; null before.
pub(crate) static API: AtomicPtr<ffi::sqlite3_api_routines> = AtomicPtr::new(null_mut());

/// SQLite's "unix" VFS, which the "ursula" VFS wraps; null before `sqlite3_extension_init`.
pub(crate) static UNIX: AtomicPtr<ffi::sqlite3_vfs> = AtomicPtr::new(null_mut());

/// The host's routines, once the extension is loaded.
pub(crate) fn api() -> Option<&'static ffi::sqlite3_api_routines> {
    let p = API.load(Ordering::Acquire);
    // SAFETY: `API` is null or the routines the host passed at load, a table SQLite keeps in a
    // static for the life of the process and never writes to.
    unsafe { p.as_ref() }
}

/// Routine `$name` of the host's API, or [`HostError::MissingRoutine`].
macro_rules! routine {
    ($name:ident) => {
        $crate::host::api()
            .and_then(|a| a.$name)
            .ok_or($crate::host::HostError::MissingRoutine(stringify!($name)))
    };
}

/// A call into the host's SQLite failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum HostError {
    #[error("the host's SQLite lacks {0}")]
    MissingRoutine(&'static str),
    #[error("{path:?} holds a NUL byte")]
    NulInPath {
        path: String,
        #[source]
        source: std::ffi::NulError,
    },
    #[error("resolving {path:?}: SQLite error {code}")]
    FullPathname { path: String, code: c_int },
    #[error("opening {path}: {message}")]
    SqliteOpen { path: String, message: String },
    #[error("{sql:?}: {message}")]
    SqliteQuery { sql: &'static CStr, message: String },
}

impl HostError {
    pub(crate) fn kind(&self) -> ErrorKind {
        match self {
            HostError::NulInPath { .. } | HostError::FullPathname { .. } => {
                ErrorKind::InvalidArgument
            }
            HostError::SqliteOpen { .. } | HostError::SqliteQuery { .. } => ErrorKind::LocalSetup,
            HostError::MissingRoutine(_) => ErrorKind::Internal,
        }
    }
}

/// SQLite's "unix" VFS, once the extension is loaded (null before).
pub(crate) fn unix() -> *mut ffi::sqlite3_vfs {
    UNIX.load(Ordering::Acquire)
}

/// `path` as a C string.
fn c_path(path: &str) -> Result<CString, HostError> {
    CString::new(path).map_err(|source| HostError::NulInPath {
        path: path.to_owned(),
        source,
    })
}

/// `path` made absolute and canonical the way SQLite names the file (the registry's key).
pub(crate) fn full_pathname(path: &str) -> Result<String, HostError> {
    let c = c_path(path)?;
    let u = unix();
    // SAFETY: `UNIX` is null or SQLite's registered "unix" VFS, which lives as long as the process.
    let vfs = unsafe { u.as_ref() }.ok_or(HostError::MissingRoutine("unix VFS"))?;
    let full = vfs
        .xFullPathname
        .ok_or(HostError::MissingRoutine("xFullPathname"))?;
    let n_out = vfs.mxPathname.saturating_add(1);
    let len = usize::try_from(n_out).map_err(|_negative| HostError::FullPathname {
        path: path.to_owned(),
        code: ffi::SQLITE_CANTOPEN,
    })?;
    let mut buf = vec![0u8; len];
    // SAFETY: `c` is NUL-terminated and `buf` holds the `n_out` writable bytes the method may fill.
    let rc = unsafe { full(u, c.as_ptr(), n_out, buf.as_mut_ptr().cast()) };
    // SQLite reports a successful canonicalization through a symlink with an extended SQLITE_OK
    // code (for example macOS /var -> /private/var).
    if rc != OK && rc != ffi::SQLITE_OK_SYMLINK {
        return Err(HostError::FullPathname {
            path: path.to_owned(),
            code: rc,
        });
    }
    let name =
        CStr::from_bytes_until_nul(&buf).map_err(|_unterminated| HostError::FullPathname {
            path: path.to_owned(),
            code: ffi::SQLITE_CANTOPEN,
        })?;
    Ok(name.to_string_lossy().into_owned())
}

/// A private connection on the "unix" VFS, outside this VFS's bookkeeping: giving a bound
/// database's empty db file its WAL-format page 1 before any connection opens it.
pub(crate) struct Private {
    /// From `sqlite3_open_v2` (possibly a failed open's handle, which still needs closing).
    db: *mut ffi::sqlite3,
}

impl Private {
    /// Opens `path` read-write, creating it when `create` is set.
    pub(crate) fn open(path: &str, create: bool) -> Result<Self, HostError> {
        let c = c_path(path)?;
        let open_v2 = routine!(open_v2)?;
        let mut flags = ffi::SQLITE_OPEN_READWRITE;
        if create {
            flags |= ffi::SQLITE_OPEN_CREATE;
        }
        let mut db = null_mut();
        // SAFETY: the path and the VFS name are NUL-terminated, and `db` is a valid out-pointer.
        let rc = unsafe { open_v2(c.as_ptr(), &raw mut db, flags, c"unix".as_ptr()) };
        let conn = Private { db };
        if rc != OK {
            return Err(HostError::SqliteOpen {
                path: path.to_owned(),
                message: conn.errmsg(),
            });
        }
        Ok(conn)
    }

    fn errmsg(&self) -> String {
        if self.db.is_null() {
            return "out of memory".into();
        }
        let Ok(errmsg) = routine!(errmsg) else {
            return "no error message".into();
        };
        // SAFETY: `db` is a handle from `sqlite3_open_v2`, open or failed, which reports its error.
        let p = unsafe { errmsg(self.db) };
        if p.is_null() {
            return "no error message".into();
        }
        // SAFETY: SQLite returns a NUL-terminated message, valid until the next call on `db`; it
        // is copied before any.
        unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
    }

    /// Runs `sql` to its end.
    pub(crate) fn exec(&self, sql: &'static CStr) -> Result<(), HostError> {
        let exec = routine!(exec)?;
        // SAFETY: `db` is open and `sql` NUL-terminated; no callback, no error out-pointer.
        let rc = unsafe { exec(self.db, sql.as_ptr(), None, null_mut(), null_mut()) };
        if rc != OK {
            return Err(HostError::SqliteQuery {
                sql,
                message: self.errmsg(),
            });
        }
        Ok(())
    }
}

impl Drop for Private {
    fn drop(&mut self) {
        if let Ok(close) = routine!(close) {
            // SAFETY: `db` came from `sqlite3_open_v2` (null is a no-op), every statement on it is
            // finalized, and it is closed once, here.
            unsafe { close(self.db) };
        }
    }
}

/// Gives an empty db file its WAL-format page 1 through a private "unix" connection, so no
/// connection ever commits through a rollback journal, which the VFS refuses. Its WAL and `-shm`
/// go when it closes. Not a commit: the first commit writes page 1 again (every schema change
/// does), so the stream holds it.
pub(crate) fn init_wal_format(path: &str) -> Result<(), HostError> {
    Private::open(path, true)?.exec(c"PRAGMA journal_mode=WAL")
}
