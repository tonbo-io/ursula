//! I/O seam of the per-core journal.
//!
//! Every file operation the journal performs goes through [`JournalDisk`].
//! Production builds use the operating system ([`OsDisk`]); `cfg(madsim)`
//! builds use the simulated disk in `sim_disk`, so deterministic simulation
//! runs the production journal over a disk that can lose unsynced pages.
//! The implementation is chosen at compile time through [`Disk`], as the
//! `rt` shim does for the async runtime.

use std::fmt::Debug;
#[cfg(not(madsim))]
use std::fs;
#[cfg(not(madsim))]
use std::fs::File;
#[cfg(not(madsim))]
use std::fs::OpenOptions;
use std::io;
#[cfg(not(madsim))]
use std::io::Read;
#[cfg(not(madsim))]
use std::io::Seek;
#[cfg(not(madsim))]
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

#[cfg(not(madsim))]
use fs4::fs_std::FileExt;

/// The disk the journal runs on in this build.
#[cfg(not(madsim))]
pub(crate) type Disk = OsDisk;
/// The disk the journal runs on in this build.
#[cfg(madsim)]
pub(crate) type Disk = super::sim_disk::SimDisk;

/// An open journal file of [`Disk`].
pub(crate) type DiskFile = <Disk as JournalDisk>::File;
/// A held journal lock of [`Disk`].
pub(crate) type DiskLock = <Disk as JournalDisk>::Lock;

/// The file operations of the journal.
pub trait JournalDisk {
    /// An open file.
    type File: JournalFile;
    /// An exclusive lock, released when dropped.
    type Lock: Debug + Send + Sync + 'static;

    /// Creates `path` and every missing parent directory.
    fn create_dir_all(path: &Path) -> io::Result<()>;
    /// Opens `path` for appending, creating it when missing. Reads start at
    /// the beginning of the file.
    fn open_append(path: &Path) -> io::Result<Self::File>;
    /// Opens an existing `path` for reading from the beginning.
    fn open_read(path: &Path) -> io::Result<Self::File>;
    /// Whether `path` exists.
    fn exists(path: &Path) -> bool;
    /// The paths of the entries of the directory at `path`, in name order.
    fn read_dir(path: &Path) -> io::Result<Vec<PathBuf>>;
    /// Truncates the file at `path` to `len` bytes and `fsync`s its data.
    fn truncate(path: &Path, len: u64) -> io::Result<()>;
    /// Atomically replaces `to` with `from`. The new name is durable only
    /// after [`JournalDisk::sync_dir`] of its parent.
    fn rename(from: &Path, to: &Path) -> io::Result<()>;
    /// Removes the file at `path`.
    fn remove_file(path: &Path) -> io::Result<()>;
    /// `fsync`s the directory at `path`, making its entries durable.
    fn sync_dir(path: &Path) -> io::Result<()>;
    /// Tries to take the exclusive lock at `path` without blocking.
    fn try_lock(path: &Path) -> io::Result<LockAttempt<Self::Lock>>;
    /// The id of the current boot of the host that holds `path`, or `None`
    /// when the platform does not report one. It changes on every host
    /// restart and never on a process restart.
    fn boot_id(path: &Path) -> Option<String>;
}

/// An open file of a [`JournalDisk`].
pub trait JournalFile: Debug + Send {
    /// The current length of the file.
    fn file_len(&self) -> io::Result<u64>;
    /// Reads exactly `buf.len()` bytes at the read position and advances it.
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()>;
    /// Appends `buf` at the end of the file. The bytes are durable only after
    /// [`JournalFile::sync_data`].
    fn append(&mut self, buf: &[u8]) -> io::Result<()>;
    /// `fsync`s the file data and the length needed to read it back.
    fn sync_data(&mut self) -> io::Result<()>;
}

/// The outcome of [`JournalDisk::try_lock`].
#[derive(Debug)]
pub enum LockAttempt<L> {
    /// The lock is now held by the caller.
    Acquired(L),
    /// Another owner holds the lock; `owner` is what it recorded, if anything.
    Held { owner: Option<String> },
}

/// Creates `path` and `fsync`s the parent of every directory it creates. A
/// file's own `fsync` makes the file's entry durable only once its directory
/// is, so a new directory's entry needs its parent `fsync`ed too, or a power
/// loss can drop the whole directory with every acknowledged write in it.
pub(crate) fn create_dir_all_durable(path: &Path) -> io::Result<()> {
    let created_parents = path
        .ancestors()
        .take_while(|dir| !dir.as_os_str().is_empty() && !Disk::exists(dir))
        .filter_map(Path::parent)
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .collect::<Vec<_>>();
    Disk::create_dir_all(path)?;
    for parent in created_parents.iter().rev() {
        Disk::sync_dir(parent)?;
    }
    Ok(())
}

/// The kernel's id of the current boot. Inside a container it is the host's.
#[cfg(all(not(madsim), target_os = "linux"))]
const LINUX_BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";

/// The operating-system disk: `std::fs` plus an advisory `flock` for the
/// journal lock.
#[cfg(not(madsim))]
#[derive(Debug, Clone, Copy)]
pub struct OsDisk;

/// An advisory exclusive lock on a lock file, released on drop.
#[cfg(not(madsim))]
#[derive(Debug)]
pub struct OsJournalLock {
    file: File,
    path: PathBuf,
}

#[cfg(not(madsim))]
impl Drop for OsJournalLock {
    fn drop(&mut self) {
        if let Err(err) = FileExt::unlock(&self.file) {
            tracing::warn!(path = %self.path.display(), %err, "failed to unlock OpenRaft WAL");
        }
    }
}

#[cfg(not(madsim))]
impl JournalDisk for OsDisk {
    type File = File;
    type Lock = OsJournalLock;

    fn create_dir_all(path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    fn open_append(path: &Path) -> io::Result<File> {
        OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
    }

    fn open_read(path: &Path) -> io::Result<File> {
        File::open(path)
    }

    fn exists(path: &Path) -> bool {
        path.exists()
    }

    fn read_dir(path: &Path) -> io::Result<Vec<PathBuf>> {
        let mut entries = fs::read_dir(path)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<io::Result<Vec<_>>>()?;
        entries.sort();
        Ok(entries)
    }

    fn truncate(path: &Path, len: u64) -> io::Result<()> {
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(len)?;
        file.sync_data()
    }

    fn rename(from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn remove_file(path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn sync_dir(path: &Path) -> io::Result<()> {
        File::open(path)?.sync_all()
    }

    fn try_lock(path: &Path) -> io::Result<LockAttempt<OsJournalLock>> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        if !file.try_lock_exclusive()? {
            let mut owner = String::new();
            file.rewind()?;
            if let Err(err) = file.read_to_string(&mut owner) {
                tracing::debug!(%err, "read journal lock owner");
            }
            let owner = owner.trim();
            return Ok(LockAttempt::Held {
                owner: (!owner.is_empty()).then(|| owner.to_owned()),
            });
        }
        file.set_len(0)?;
        file.rewind()?;
        write!(file, "pid={}", std::process::id())?;
        file.sync_data()?;
        Ok(LockAttempt::Acquired(OsJournalLock {
            file,
            path: path.to_owned(),
        }))
    }

    #[cfg(target_os = "linux")]
    fn boot_id(_path: &Path) -> Option<String> {
        match fs::read_to_string(LINUX_BOOT_ID_PATH) {
            Ok(boot_id) => Some(boot_id.trim().to_owned()).filter(|boot_id| !boot_id.is_empty()),
            Err(err) => {
                tracing::warn!(path = LINUX_BOOT_ID_PATH, %err, "read the kernel boot id");
                None
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn boot_id(_path: &Path) -> Option<String> {
        None
    }
}

#[cfg(not(madsim))]
impl JournalFile for File {
    fn file_len(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        Read::read_exact(self, buf)
    }

    fn append(&mut self, buf: &[u8]) -> io::Result<()> {
        self.write_all(buf)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        File::sync_data(self)
    }
}
