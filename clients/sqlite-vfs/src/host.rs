//! The host's SQLite, reached through the `sqlite3_api_routines` it passed at load: the
//! "unix" VFS underneath, path resolution, and the extension's private connections.
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

use crate::codec::PAGE;
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
    #[error("{sql:?} returned no row")]
    NoResultRow { sql: &'static CStr },
    #[error("the db file handle: SQLite error {code}")]
    FileHandle { code: c_int },
    #[error("reading {pages} pages of the db file: SQLite error {code}")]
    ReadPages { code: c_int, pages: u32 },
    #[error("copying the db file's pages: {0}")]
    Copy(#[source] std::io::Error),
    #[error("fsync of the db file: SQLite error {code}")]
    FileSync { code: c_int },
}

impl HostError {
    pub(crate) fn kind(&self) -> ErrorKind {
        match self {
            HostError::NulInPath { .. } | HostError::FullPathname { .. } => {
                ErrorKind::InvalidArgument
            }
            HostError::SqliteOpen { .. }
            | HostError::SqliteQuery { .. }
            | HostError::NoResultRow { .. }
            | HostError::FileHandle { .. }
            | HostError::ReadPages { .. }
            | HostError::Copy(_)
            | HostError::FileSync { .. } => ErrorKind::LocalSetup,
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

/// The result of `PRAGMA wal_checkpoint`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Checkpoint {
    /// Every frame of the WAL is in the db file.
    Done,
    /// Another connection held the checkpoint lock.
    Busy,
    /// A reader kept some frames out of the db file.
    Pinned,
}

/// A private connection of the extension's own, in the extension's process. [`Private::open`]
/// goes through the "ursula" VFS, so its checkpoints take F1 (design: Fsyncs) and it counts as a
/// connection of the binding: the snapshot thread's checkpoint, read transaction, page copy and
/// db fsync.
pub(crate) struct Private {
    /// From `sqlite3_open_v2` (possibly a failed open's handle, which still needs closing).
    db: *mut ffi::sqlite3,
}

impl Private {
    /// Opens the bound database at `path` read-write through the "ursula" VFS, as a connection
    /// of its binding that never takes a retiring binding back (`vfs::opening_private`).
    pub(crate) fn open(path: &str) -> Result<Self, HostError> {
        crate::vfs::opening_private(|| Self::open_on(path, false, c"ursula"))
    }

    /// Opens `path` read-write on `vfs`, creating it when `create` is set.
    fn open_on(path: &str, create: bool, vfs: &CStr) -> Result<Self, HostError> {
        let c = c_path(path)?;
        let open_v2 = routine!(open_v2)?;
        let mut flags = ffi::SQLITE_OPEN_READWRITE;
        if create {
            flags |= ffi::SQLITE_OPEN_CREATE;
        }
        let mut db = null_mut();
        // SAFETY: the path and the VFS name are NUL-terminated, and `db` is a valid out-pointer.
        let rc = unsafe { open_v2(c.as_ptr(), &raw mut db, flags, vfs.as_ptr()) };
        let conn = Private { db };
        if rc != OK {
            return Err(HostError::SqliteOpen {
                path: path.to_owned(),
                message: conn.errmsg(),
            });
        }
        Ok(conn)
    }

    /// Runs `sql` and returns the integer columns of its first row (empty without a row).
    pub(crate) fn query(&self, sql: &'static CStr) -> Result<Vec<i64>, HostError> {
        let prepare = routine!(prepare_v2)?;
        let step = routine!(step)?;
        let column_count = routine!(column_count)?;
        let column_int64 = routine!(column_int64)?;
        let finalize = routine!(finalize)?;
        let mut stmt = null_mut();
        // SAFETY: `db` is open, `sql` is NUL-terminated (length -1: up to the NUL), and `stmt` is
        // a valid out-pointer; the tail pointer may be null.
        let rc = unsafe { prepare(self.db, sql.as_ptr(), -1, &raw mut stmt, null_mut()) };
        if rc != OK {
            return Err(HostError::SqliteQuery {
                sql,
                message: self.errmsg(),
            });
        }
        // SAFETY: `stmt` was just prepared on `db`.
        let r = match unsafe { step(stmt) } {
            ffi::SQLITE_ROW => {
                // SAFETY: `stmt` holds a row.
                let n = unsafe { column_count(stmt) };
                // SAFETY: every index below the row's column count is a column of it.
                Ok((0..n).map(|i| unsafe { column_int64(stmt, i) }).collect())
            }
            ffi::SQLITE_DONE => Ok(Vec::new()),
            _ => Err(HostError::SqliteQuery {
                sql,
                message: self.errmsg(),
            }),
        };
        // SAFETY: `stmt` is finalized once, here, after its error message was read.
        unsafe { finalize(stmt) };
        r
    }

    /// `PRAGMA wal_checkpoint(PASSIVE)`.
    pub(crate) fn checkpoint(&self) -> Result<Checkpoint, HostError> {
        let sql = c"PRAGMA wal_checkpoint(PASSIVE)";
        match *self.query(sql)?.as_slice() {
            [0, log, done] if log == done => Ok(Checkpoint::Done),
            [0, ..] => Ok(Checkpoint::Pinned),
            [_, ..] => Ok(Checkpoint::Busy),
            [] => Err(HostError::NoResultRow { sql }),
        }
    }

    /// The connection's open db file and its io methods (the "ursula" VFS's). Live while the
    /// connection is: closing a descriptor of our own instead would drop the process's POSIX
    /// locks on the file.
    fn db_file(
        &self,
    ) -> Result<(*mut ffi::sqlite3_file, &'static ffi::sqlite3_io_methods), HostError> {
        let file_control = routine!(file_control)?;
        let mut f: *mut ffi::sqlite3_file = null_mut();
        // SAFETY: `db` is open, "main" is NUL-terminated, and FILE_POINTER writes the
        // `sqlite3_file*` of the main database at the pointer.
        let rc = unsafe {
            file_control(
                self.db,
                c"main".as_ptr(),
                ffi::SQLITE_FCNTL_FILE_POINTER,
                (&raw mut f).cast(),
            )
        };
        // SAFETY: when non-null, `f` is a file of the connection, live while `db` is.
        let opened = unsafe { f.as_ref() };
        // SAFETY: an open file's methods are its VFS's io methods, a static table (null while the
        // file is closed).
        let methods = opened.and_then(|o| unsafe { o.pMethods.as_ref() });
        match methods {
            Some(methods) if rc == OK => Ok((f, methods)),
            _ => Err(HostError::FileHandle { code: rc }),
        }
    }

    /// Copies pages `1..=n` of the db file to `out`, 256 pages at a time, through the
    /// connection's own file handle.
    pub(crate) fn copy_pages(
        &self,
        n: u32,
        out: &mut impl std::io::Write,
    ) -> Result<(), HostError> {
        /// The most read at once: 256 pages.
        const CHUNK_PAGES: u32 = 256;
        let (f, methods) = self.db_file()?;
        let read = methods.xRead.ok_or(HostError::MissingRoutine("xRead"))?;
        let mut buf = vec![0u8; PAGE.saturating_mul(CHUNK_PAGES as usize)];
        let mut done: u32 = 0;
        while done < n {
            let pages = (n.saturating_sub(done)).min(CHUNK_PAGES);
            let len = PAGE.saturating_mul(pages as usize);
            let chunk = buf.get_mut(..len).unwrap_or_default();
            let amt = c_int::try_from(len).unwrap_or(c_int::MAX);
            let at = i64::from(done).saturating_mul(PAGE as i64);
            // SAFETY: `chunk` is `amt` writable bytes, and `f` is the open file `read` belongs to.
            let rc = unsafe { read(f, chunk.as_mut_ptr().cast(), amt, at) };
            if rc != OK {
                return Err(HostError::ReadPages { code: rc, pages: n });
            }
            out.write_all(chunk).map_err(HostError::Copy)?;
            done = done.saturating_add(pages);
        }
        Ok(())
    }

    /// fsyncs the db file through the connection's own handle, bypassing the "ursula" VFS's
    /// `xSync`, which is a no-op for a bound database (design: Fsyncs).
    pub(crate) fn sync_db(&self) -> Result<(), HostError> {
        let (f, _) = self.db_file()?;
        // SAFETY: `f` is the connection's open db file, a file of the "ursula" VFS.
        let rc = unsafe { crate::vfs::sync_unix(f) };
        if rc != OK {
            return Err(HostError::FileSync { code: rc });
        }
        Ok(())
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
