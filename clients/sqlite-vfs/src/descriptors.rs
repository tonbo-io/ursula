//! The scan for descriptors of this process on a database's files outside the extension, which
//! `ursula_open` refuses ([`open_in_process`]).

use std::ffi::OsStr;
use std::fs::{self};
use std::os::fd::AsRawFd;
use std::path::Path;
use std::path::PathBuf;

/// The directory listing this process's open descriptors, each by number.
#[cfg(target_os = "linux")]
const OPEN_FDS: &str = "/proc/self/fd";
#[cfg(not(target_os = "linux"))]
const OPEN_FDS: &str = "/dev/fd";

/// Which file of a database a descriptor is open on ([`open_in_process`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DbFile {
    Db,
    Wal,
    Shm,
    Journal,
}

impl std::fmt::Display for DbFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            DbFile::Db => "the db file",
            DbFile::Wal => "its -wal",
            DbFile::Shm => "its -shm",
            DbFile::Journal => "its -journal",
        })
    }
}

/// A descriptor of this process open on the db file `path`, its `-wal`, `-shm` or `-journal`, in
/// any access mode, if any. `ursula_open` calls it once a binding's generation is ready and before
/// any connection of this VFS opens it, so such a descriptor shows a SQLite connection outside the extension (one opened before the extension was
/// loaded, when "unix" was the default VFS, or through another VFS, a read-only one included) or
/// another open of the file. A connection outside the extension shares the "unix" VFS's locks and
/// wal-index node of the file with this VFS's connections in the process: its commits never reach
/// the stream, and even a read-only one keeps the wal-index of a WAL that materialization deletes.
/// The "unix"
/// VFS also keeps the descriptors of connections closed meanwhile (this VFS's included) open while
/// such a connection holds a lock on the file, so those show it too.
///
/// A descriptor counts when the path the kernel records for it (the `/proc/self/fd` link, or
/// `F_GETPATH` on macOS, the case ignored there) is one of those four files' (`db_files`), and its
/// device and inode are that file's. A descriptor of a file with another path is never stat'ed (a
/// stat on a hung network mount can block). So one opened through another hard link of the file,
/// another mount of it (a bind mount), or a file unlinked since, does not count. `None` as well
/// when the descriptors cannot be listed (no `/proc` or `/dev/fd`) or their paths cannot be read
/// (other than Linux and macOS).
pub(crate) fn open_in_process(path: &str) -> Option<(i32, DbFile)> {
    let files = db_files(path)?;
    fs::read_dir(OPEN_FDS)
        .ok()?
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().to_str()?.parse::<i32>().ok())
        .find_map(|fd| {
            // A descriptor closed since the listing (or the listing's own) fails one of these.
            let found = fd_path(fd)?;
            let (which, file) = files.iter().find(|(_, file)| same_path(&found, file))?;
            (fd_identity(fd)? == path_identity(file)?).then_some((fd, *which))
        })
}

/// The paths the kernel can record for a descriptor on the db file `path`, its `-wal`, `-shm` or
/// `-journal`: each name in the db file's directory with every symbolic link resolved (the kernel
/// records the resolved path: `/var` is `/private/var` on macOS), the same names under the
/// directory as the kernel spells it (a macOS firmlink: a directory reached through
/// `/System/Volumes/Data/...` is recorded without that prefix), and, when the db file is a link
/// itself, the same names next to its target (where SQLite puts them when its full pathname
/// resolves links). `None` when the directory cannot be resolved.
fn db_files(path: &str) -> Option<Vec<(DbFile, PathBuf)>> {
    let path = Path::new(path);
    let (parent, name) = (path.parent()?, path.file_name()?);
    let dir = fs::canonicalize(parent).ok()?;
    let spelled = fs::File::open(&dir)
        .ok()
        .and_then(|d| fd_path(d.as_raw_fd()))
        .map(PathBuf::from);
    let target = fs::canonicalize(path).ok();
    let mut bases = vec![dir.join(name)];
    for base in [spelled.map(|d| d.join(name)), target]
        .into_iter()
        .flatten()
    {
        if !bases.contains(&base) {
            bases.push(base);
        }
    }
    let suffixes = [
        (DbFile::Db, ""),
        (DbFile::Wal, "-wal"),
        (DbFile::Shm, "-shm"),
        (DbFile::Journal, "-journal"),
    ];
    Some(
        bases
            .iter()
            .flat_map(|base| {
                suffixes.iter().map(move |&(which, suffix)| {
                    let mut name = base.clone().into_os_string();
                    name.push(suffix);
                    (which, PathBuf::from(name))
                })
            })
            .collect(),
    )
}

/// A file's device and inode, as `stat` and `fstat` report them (one type on both sides).
type Identity = (libc::dev_t, libc::ino_t);

/// The device and inode of the file at `path`, `None` when it cannot be stat'ed.
#[expect(unsafe_code, reason = "stat into a libc struct has no safe std API")]
fn path_identity(path: &Path) -> Option<Identity> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: `stat` is a C struct of integers, for which all zeros is a valid value.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is NUL-terminated and `stat` writes the struct at the pointer.
    if unsafe { libc::stat(c.as_ptr(), &raw mut st) } != 0 {
        return None;
    }
    Some((st.st_dev, st.st_ino))
}

/// The device and inode of the file descriptor `fd` is open on, `None` when it is not open.
#[expect(
    unsafe_code,
    reason = "fstat of a descriptor number this process lists has no safe std API"
)]
fn fd_identity(fd: i32) -> Option<Identity> {
    // SAFETY: `stat` is a C struct of integers, for which all zeros is a valid value.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: `fstat` only reads the descriptor number (failing with EBADF when it is closed) and
    // writes the struct at the pointer.
    if unsafe { libc::fstat(fd, &raw mut st) } != 0 {
        return None;
    }
    Some((st.st_dev, st.st_ino))
}

/// The path of the file descriptor `fd` is open on, from the kernel's record of it (no filesystem
/// call): the `/proc/self/fd/<fd>` link on Linux, which reads ` (deleted)` after the path of a file
/// since unlinked.
#[cfg(target_os = "linux")]
fn fd_path(fd: i32) -> Option<std::ffi::OsString> {
    Some(
        fs::read_link(format!("{OPEN_FDS}/{fd}"))
            .ok()?
            .into_os_string(),
    )
}

/// As above, through `fcntl(F_GETPATH)` on macOS.
#[cfg(target_os = "macos")]
#[expect(
    unsafe_code,
    reason = "fcntl(F_GETPATH) into a buffer has no safe std API"
)]
fn fd_path(fd: i32) -> Option<std::ffi::OsString> {
    use std::os::unix::ffi::OsStrExt;
    let mut buf = [0u8; libc::PATH_MAX as usize];
    // SAFETY: F_GETPATH writes a NUL-terminated path of at most PATH_MAX bytes into `buf`.
    if unsafe { libc::fcntl(fd, libc::F_GETPATH, buf.as_mut_ptr()) } != 0 {
        return None;
    }
    let path = std::ffi::CStr::from_bytes_until_nul(&buf).ok()?;
    Some(std::ffi::OsStr::from_bytes(path.to_bytes()).to_owned())
}

/// Elsewhere no descriptor's path is known, and none is found.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn fd_path(_fd: i32) -> Option<std::ffi::OsString> {
    None
}

/// Whether a descriptor's path is `file`'s: equal, and on macOS equal but for ASCII case (its
/// volumes are usually case-insensitive, and the kernel reports a name as stored). A path that only
/// differs in case is compared by device and inode next, so it never matches another file.
fn same_path(found: &OsStr, file: &Path) -> bool {
    let (found, file) = (
        found.as_encoded_bytes(),
        file.as_os_str().as_encoded_bytes(),
    );
    if cfg!(target_os = "macos") {
        found.eq_ignore_ascii_case(file)
    } else {
        found == file
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::OpenOptions;

    use super::DbFile;
    use super::open_in_process;

    // Any descriptor of this process on the db file, its -wal, -shm or -journal is found, in any
    // access mode (a read-only SQLite connection maps the wal-index too), by the path the kernel
    // records for it, links resolved, and its inode. One on another file of the same name is not,
    // nor any once closed.
    #[test]
    fn any_descriptor_on_the_db_files_is_found() {
        let dir = std::env::temp_dir().join(format!("ursula-fds-{}", std::process::id()));
        let other = dir.join("other");
        fs::create_dir_all(&other).unwrap();
        let path = dir.join("app.db").to_string_lossy().into_owned();
        assert_eq!(open_in_process(&path), None, "no file");
        fs::write(&path, b"").unwrap();
        assert_eq!(open_in_process(&path), None);
        let reader = fs::File::open(&path).unwrap();
        assert!(matches!(open_in_process(&path), Some((_, DbFile::Db))));
        // Through a link to its directory: the kernel records the resolved path.
        let link = dir.join("link");
        std::os::unix::fs::symlink(&dir, &link).unwrap();
        let linked = link.join("app.db").to_string_lossy().into_owned();
        assert!(matches!(open_in_process(&linked), Some((_, DbFile::Db))));
        // Through the data volume's own spelling on macOS, which the kernel does not record.
        #[cfg(target_os = "macos")]
        {
            let real = fs::canonicalize(&path).unwrap();
            let data = format!("/System/Volumes/Data{}", real.display());
            if fs::exists(&data).unwrap_or(false) {
                assert!(
                    matches!(open_in_process(&data), Some((_, DbFile::Db))),
                    "{data}"
                );
            }
        }
        drop(reader);
        assert_eq!(open_in_process(&path), None);
        for (suffix, which) in [
            ("-wal", DbFile::Wal),
            ("-shm", DbFile::Shm),
            ("-journal", DbFile::Journal),
        ] {
            let file = format!("{path}{suffix}");
            fs::write(&file, b"").unwrap();
            let open = fs::File::open(&file).unwrap();
            assert_eq!(open_in_process(&path).map(|(_, w)| w), Some(which));
            drop(open);
            assert_eq!(open_in_process(&path), None, "{suffix}");
        }
        let same_name = other.join("app.db");
        fs::write(&same_name, b"").unwrap();
        let elsewhere = OpenOptions::new().write(true).open(&same_name).unwrap();
        assert_eq!(
            open_in_process(&path),
            None,
            "another file of the same name"
        );
        drop(elsewhere);
        fs::remove_dir_all(&dir).unwrap();
    }
}
