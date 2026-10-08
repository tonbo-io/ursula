//! The host's SQLite, reached through the `sqlite3_api_routines` it passed at load: the
//! "unix" VFS underneath, path resolution, and private connections outside this VFS's bookkeeping.

use std::ffi::CStr;
use std::ffi::CString;
use std::ffi::c_char;
use std::ffi::c_int;
use std::ffi::c_void;
use std::ptr::null_mut;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;

use libsqlite3_sys as ffi;

use crate::frame::PAGE;

pub(crate) const OK: c_int = ffi::SQLITE_OK;

pub(crate) static API: AtomicPtr<ffi::sqlite3_api_routines> = AtomicPtr::new(null_mut());

pub(crate) static UNIX: AtomicPtr<ffi::sqlite3_vfs> = AtomicPtr::new(null_mut());

pub(crate) fn api() -> &'static ffi::sqlite3_api_routines {
    unsafe { &*API.load(Ordering::Acquire) }
}

pub(crate) fn unix() -> *mut ffi::sqlite3_vfs {
    UNIX.load(Ordering::Acquire)
}

pub(crate) unsafe fn full_pathname(path: &str) -> Result<String, String> {
    unsafe {
        let u = unix();
        let c = CString::new(path).map_err(|e| e.to_string())?;
        let mut buf = vec![0u8; (*u).mxPathname as usize + 1];
        let rc = ((*u).xFullPathname.unwrap())(
            u,
            c.as_ptr(),
            buf.len() as c_int,
            buf.as_mut_ptr() as *mut c_char,
        );
        if rc != OK {
            return Err(format!("xFullPathname({path}): {rc}"));
        }
        Ok(CStr::from_ptr(buf.as_ptr() as *const c_char)
            .to_string_lossy()
            .into_owned())
    }
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

/// A private connection on the "unix" VFS, outside this VFS's bookkeeping: the snapshot thread's
/// checkpoint, read transaction and page copy. `synchronous=OFF`: the local files are a cache (see
/// the crate docs), so its checkpoints never fsync.
pub(crate) struct Private {
    db: *mut ffi::sqlite3,
}

impl Private {
    pub(crate) unsafe fn open(path: &str) -> Result<Self, String> {
        unsafe {
            let c = CString::new(path).map_err(|e| e.to_string())?;
            let mut db: *mut ffi::sqlite3 = null_mut();
            let rc = (api().open_v2.unwrap())(
                c.as_ptr(),
                &mut db,
                ffi::SQLITE_OPEN_READWRITE,
                c"unix".as_ptr(),
            );
            let conn = Private { db };
            if rc != OK {
                return Err(format!("open {path}: {}", conn.errmsg()));
            }
            conn.query(c"PRAGMA synchronous=OFF")?;
            Ok(conn)
        }
    }

    unsafe fn errmsg(&self) -> String {
        if self.db.is_null() {
            return "out of memory".into();
        }
        unsafe {
            CStr::from_ptr((api().errmsg.unwrap())(self.db))
                .to_string_lossy()
                .into_owned()
        }
    }

    /// Runs `sql` and returns the integer columns of its first row (empty without a row).
    pub(crate) unsafe fn query(&self, sql: &CStr) -> Result<Vec<i64>, String> {
        unsafe {
            let a = api();
            let mut stmt: *mut ffi::sqlite3_stmt = null_mut();
            if (a.prepare_v2.unwrap())(self.db, sql.as_ptr(), -1, &mut stmt, null_mut()) != OK {
                return Err(format!("{sql:?}: {}", self.errmsg()));
            }
            let r = match (a.step.unwrap())(stmt) {
                ffi::SQLITE_ROW => Ok((0..(a.column_count.unwrap())(stmt))
                    .map(|i| (a.column_int64.unwrap())(stmt, i))
                    .collect()),
                ffi::SQLITE_DONE => Ok(Vec::new()),
                _ => Err(format!("{sql:?}: {}", self.errmsg())),
            };
            (a.finalize.unwrap())(stmt);
            r
        }
    }

    /// `PRAGMA wal_checkpoint(mode)`.
    pub(crate) unsafe fn checkpoint(&self, sql: &CStr) -> Result<Checkpoint, String> {
        let row = unsafe { self.query(sql)? };
        match row[..] {
            [0, log, done] if log == done => Ok(Checkpoint::Done),
            [0, ..] => Ok(Checkpoint::Pinned),
            [_, ..] => Ok(Checkpoint::Busy),
            _ => Err(format!("{sql:?}: no result row")),
        }
    }

    /// Keeps the WAL when this connection closes last (see `x_file_control`).
    pub(crate) unsafe fn persist_wal(&self) -> Result<(), String> {
        let mut on: c_int = 1;
        let rc = unsafe {
            (api().file_control.unwrap())(
                self.db,
                c"main".as_ptr(),
                ffi::SQLITE_FCNTL_PERSIST_WAL,
                &mut on as *mut c_int as *mut c_void,
            )
        };
        if rc != OK {
            return Err(format!("persist WAL: {rc}"));
        }
        Ok(())
    }

    /// Pages `1..=n` of the db file, read through the connection's own file handle (closing a
    /// descriptor of our own would drop the process's POSIX locks on the file).
    pub(crate) unsafe fn read_pages(&self, n: u32) -> Result<Vec<u8>, String> {
        unsafe {
            let mut f: *mut ffi::sqlite3_file = null_mut();
            let rc = (api().file_control.unwrap())(
                self.db,
                c"main".as_ptr(),
                ffi::SQLITE_FCNTL_FILE_POINTER,
                &mut f as *mut *mut ffi::sqlite3_file as *mut c_void,
            );
            if rc != OK || f.is_null() || (*f).pMethods.is_null() {
                return Err(format!("db file handle: {rc}"));
            }
            const CHUNK: usize = 256 * PAGE;
            let mut image = vec![0u8; n as usize * PAGE];
            for (i, chunk) in image.chunks_mut(CHUNK).enumerate() {
                let rc = ((*(*f).pMethods).xRead.unwrap())(
                    f,
                    chunk.as_mut_ptr() as *mut c_void,
                    chunk.len() as c_int,
                    (i * CHUNK) as i64,
                );
                if rc != OK {
                    return Err(format!(
                        "read db file pages: {rc} (file shorter than {n} pages?)"
                    ));
                }
            }
            Ok(image)
        }
    }
}

impl Drop for Private {
    fn drop(&mut self) {
        unsafe { (api().close.unwrap())(self.db) };
    }
}

/// Gives an empty file its WAL-format page 1 through a private "unix" connection, so no
/// connection ever commits through a rollback journal (the main-db write guard refuses that).
pub(crate) unsafe fn init_wal_format(path: &str) -> Result<(), String> {
    unsafe {
        let a = api();
        let c = CString::new(path).unwrap();
        let mut db: *mut ffi::sqlite3 = null_mut();
        let flags = ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE;
        let mut rc = (a.open_v2.unwrap())(c.as_ptr(), &mut db, flags, c"unix".as_ptr());
        if rc == OK {
            rc = (a.exec.unwrap())(
                db,
                c"PRAGMA synchronous=OFF; PRAGMA journal_mode=WAL".as_ptr(),
                None,
                null_mut(),
                null_mut(),
            );
        }
        (a.close.unwrap())(db);
        if rc != OK {
            return Err(format!("journal_mode=WAL on {path}: {rc}"));
        }
    }
    Ok(())
}
