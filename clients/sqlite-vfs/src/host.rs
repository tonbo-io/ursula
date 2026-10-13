//! The host's SQLite, reached through the `sqlite3_api_routines` it passed at load: the
//! "unix" VFS underneath, path resolution, and private connections outside this VFS's bookkeeping.
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

use crate::error::Error;
use crate::frame::PAGE;

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

/// Routine `$name` of the host's API, or [`Error::MissingRoutine`].
macro_rules! routine {
    ($name:ident) => {
        $crate::host::api()
            .and_then(|a| a.$name)
            .ok_or($crate::error::Error::MissingRoutine(stringify!($name)))
    };
}

/// SQLite's "unix" VFS, once the extension is loaded (null before).
pub(crate) fn unix() -> *mut ffi::sqlite3_vfs {
    UNIX.load(Ordering::Acquire)
}

/// `path` as a C string.
fn c_path(path: &str) -> Result<CString, Error> {
    CString::new(path).map_err(|source| Error::NulInPath {
        path: path.to_owned(),
        source,
    })
}

/// `path` made absolute and canonical the way SQLite names the file (the registry's key).
pub(crate) fn full_pathname(path: &str) -> Result<String, Error> {
    let c = c_path(path)?;
    let u = unix();
    // SAFETY: `UNIX` is null or SQLite's registered "unix" VFS, which lives as long as the process.
    let vfs = unsafe { u.as_ref() }.ok_or(Error::MissingRoutine("unix VFS"))?;
    let full = vfs
        .xFullPathname
        .ok_or(Error::MissingRoutine("xFullPathname"))?;
    let n_out = vfs.mxPathname.saturating_add(1);
    let len = usize::try_from(n_out).map_err(|_negative| Error::FullPathname {
        path: path.to_owned(),
        code: ffi::SQLITE_CANTOPEN,
    })?;
    let mut buf = vec![0u8; len];
    // SAFETY: `c` is NUL-terminated and `buf` holds the `n_out` writable bytes the method may fill.
    let rc = unsafe { full(u, c.as_ptr(), n_out, buf.as_mut_ptr().cast()) };
    // SQLite reports a successful canonicalization through a symlink with an extended SQLITE_OK
    // code (for example macOS /var -> /private/var).
    if rc != OK && rc != ffi::SQLITE_OK_SYMLINK {
        return Err(Error::FullPathname {
            path: path.to_owned(),
            code: rc,
        });
    }
    let name = CStr::from_bytes_until_nul(&buf).map_err(|_unterminated| Error::FullPathname {
        path: path.to_owned(),
        code: ffi::SQLITE_CANTOPEN,
    })?;
    Ok(name.to_string_lossy().into_owned())
}

/// Outcome of a checkpoint on a [`Private`] connection.
pub(crate) enum Checkpoint {
    /// Every WAL frame is in the db file.
    Done,
    /// A reader pins WAL frames the checkpoint needs.
    Pinned,
    /// Another connection holds the checkpoint (or a needed) lock.
    Busy,
}

/// A private connection on the "unix" VFS, outside this VFS's bookkeeping: attach giving an empty
/// file its WAL format, and the snapshot thread's checkpoint, read transaction, page copy and
/// fsyncs. `synchronous=OFF`: the local files are a cache (see the crate docs), so its checkpoints
/// never fsync.
pub(crate) struct Private {
    /// From `sqlite3_open_v2` (possibly a failed open's handle, which still needs closing).
    db: *mut ffi::sqlite3,
}

impl Private {
    /// Opens `path` read-write, creating it when `create` is set.
    pub(crate) fn open(path: &str, create: bool) -> Result<Self, Error> {
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
            return Err(Error::SqliteOpen {
                path: path.to_owned(),
                message: conn.errmsg(),
            });
        }
        conn.query(c"PRAGMA synchronous=OFF")?;
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

    /// Runs `sql` and returns the integer columns of its first row (empty without a row).
    pub(crate) fn query(&self, sql: &'static CStr) -> Result<Vec<i64>, Error> {
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
            return Err(Error::SqliteQuery {
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
            _ => Err(Error::SqliteQuery {
                sql,
                message: self.errmsg(),
            }),
        };
        // SAFETY: `stmt` is finalized once, here, after its error message was read.
        unsafe { finalize(stmt) };
        r
    }

    /// `PRAGMA wal_checkpoint(mode)`.
    pub(crate) fn checkpoint(&self, sql: &'static CStr) -> Result<Checkpoint, Error> {
        match *self.query(sql)?.as_slice() {
            [0, log, done] if log == done => Ok(Checkpoint::Done),
            [0, ..] => Ok(Checkpoint::Pinned),
            [_, ..] => Ok(Checkpoint::Busy),
            [] => Err(Error::NoResultRow { sql }),
        }
    }

    /// Keeps the WAL when this connection closes last (see `x_file_control`).
    pub(crate) fn persist_wal(&self) -> Result<(), Error> {
        let file_control = routine!(file_control)?;
        let mut on: c_int = 1;
        // SAFETY: `db` is open, "main" is NUL-terminated, and PERSIST_WAL reads and writes one
        // `int` at the pointer.
        let rc = unsafe {
            file_control(
                self.db,
                c"main".as_ptr(),
                ffi::SQLITE_FCNTL_PERSIST_WAL,
                (&raw mut on).cast(),
            )
        };
        if rc != OK {
            return Err(Error::PersistWal(rc));
        }
        Ok(())
    }

    /// One of the connection's own open files and its io methods, through `op`: `FILE_POINTER`
    /// for the db file, `JOURNAL_POINTER` for the WAL (open once a read or checkpoint ran). Live
    /// while the connection is (closing a descriptor of our own instead would drop the process's
    /// POSIX locks on the file).
    fn file(
        &self,
        op: c_int,
        file: &'static str,
    ) -> Result<(*mut ffi::sqlite3_file, &'static ffi::sqlite3_io_methods), Error> {
        let file_control = routine!(file_control)?;
        let mut f: *mut ffi::sqlite3_file = null_mut();
        // SAFETY: `db` is open, "main" is NUL-terminated, and FILE_POINTER and JOURNAL_POINTER
        // write a `sqlite3_file*` of the main database at the pointer.
        let rc = unsafe { file_control(self.db, c"main".as_ptr(), op, (&raw mut f).cast()) };
        // SAFETY: when non-null, `f` is a file of the connection, live while `db` is.
        let opened = unsafe { f.as_ref() };
        // SAFETY: an open file's methods are its VFS's io methods, a static table (null while the
        // file is closed).
        let methods = opened.and_then(|o| unsafe { o.pMethods.as_ref() });
        match methods {
            Some(methods) if rc == OK => Ok((f, methods)),
            _ => Err(Error::FileHandle { file, code: rc }),
        }
    }

    /// Pages `1..=n` of the db file, read through the connection's own file handle (see `file`).
    pub(crate) fn read_pages(&self, n: u32) -> Result<Vec<u8>, Error> {
        /// The most read at once: 256 pages.
        const CHUNK: usize = 256 * PAGE;
        let (f, methods) = self.file(ffi::SQLITE_FCNTL_FILE_POINTER, "db file")?;
        let read = methods.xRead.ok_or(Error::MissingRoutine("xRead"))?;
        let too_large = || Error::ReadPages {
            code: ffi::SQLITE_TOOBIG,
            pages: n,
        };
        let len = usize::try_from(n)
            .ok()
            .and_then(|n| n.checked_mul(PAGE))
            .ok_or_else(too_large)?;
        let mut image = vec![0u8; len];
        let mut at: i64 = 0;
        for chunk in image.chunks_mut(CHUNK) {
            let amt = c_int::try_from(chunk.len()).map_err(|_too_long| too_large())?;
            // SAFETY: `chunk` is `amt` writable bytes, and `f` is the open file `read` belongs to.
            let rc = unsafe { read(f, chunk.as_mut_ptr().cast(), amt, at) };
            if rc != OK {
                return Err(Error::ReadPages { code: rc, pages: n });
            }
            at = at.saturating_add(i64::from(amt));
        }
        Ok(image)
    }

    /// Fsyncs the WAL, then the db file, through the connection's own handles (see `file`). On
    /// Linux an fsync reports a write-back error of the file that no fsync has reported yet,
    /// whichever descriptor wrote the page: [`Error::FileSync`] when a write-back may have been
    /// lost.
    pub(crate) fn sync_files(&self) -> Result<(), Error> {
        let files = [
            (ffi::SQLITE_FCNTL_JOURNAL_POINTER, "WAL"),
            (ffi::SQLITE_FCNTL_FILE_POINTER, "db file"),
        ];
        for (op, file) in files {
            let (f, methods) = self.file(op, file)?;
            let sync = methods.xSync.ok_or(Error::MissingRoutine("xSync"))?;
            // SAFETY: `f` is the open file `sync` belongs to.
            let rc = unsafe { sync(f, ffi::SQLITE_SYNC_NORMAL) };
            if rc != OK {
                return Err(Error::FileSync { file, code: rc });
            }
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

/// Gives an empty file its WAL-format page 1 through a private "unix" connection, so no
/// connection ever commits through a rollback journal (the main-db write guard refuses that).
pub(crate) fn init_wal_format(path: &str) -> Result<(), Error> {
    Private::open(path, true)?.query(c"PRAGMA journal_mode=WAL")?;
    Ok(())
}
