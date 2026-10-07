//! Simulated disk for deterministic simulation (`cfg(madsim)`).
//!
//! [`SimDisk`] implements the journal's I/O seam over an in-memory, path-keyed
//! store scoped to the current madsim runtime: every engine restart inside one
//! simulation sees the same files, and a new runtime starts with an empty disk.
//!
//! The disk keeps two views. The running process sees file contents as written
//! (the page cache) and directory entries as created, renamed or removed. A
//! power loss keeps only what was made durable: file pages through
//! [`JournalFile::sync_data`] and directory entries through
//! [`JournalDisk::sync_dir`] of their parent.
//!
//! - A process crash keeps everything ([`SimDisk::process_crash`]).
//! - [`SimDisk::power_loss`] keeps or drops each unsynced 4 KiB page with
//!   madsim's deterministic RNG, so a later page can survive while an earlier
//!   one is lost (writeback reordering, which leaves zero-filled holes), and
//!   reverts directory operations that were not `fsync`ed.
//! - [`SimDisk::inject_fault`] fails the next write, `fsync` or removal on a
//!   path. A failed `fsync` marks the pages clean without persisting them, as
//!   Linux does, so a later successful `fsync` does not make them durable.
//!
//! The disk also stands in for the host's boot id
//! ([`JournalDisk::boot_id`]): a power loss under a prefix starts a new boot
//! for every path below it, and a process crash does not.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Weak;

use madsim::net::NetSim;

use super::disk::JournalDisk;
use super::disk::JournalFile;
use super::disk::LockAttempt;

/// Page size the simulated disk keeps or drops as a unit on power loss.
pub const SIM_DISK_PAGE_SIZE: usize = 4096;
const PAGE_SIZE: usize = SIM_DISK_PAGE_SIZE;
const SIM_ROOT: &str = "/ursula-sim";

thread_local! {
    /// madsim runs a whole simulation on one thread, so a thread-local disk
    /// is the disk of the current runtime. It is replaced when a new runtime
    /// starts on this thread.
    static DISK: RefCell<Option<RuntimeDisk>> = const { RefCell::new(None) };
}

/// The simulated disk of the current madsim runtime.
#[derive(Debug, Clone, Copy)]
pub struct SimDisk;

/// A fault [`SimDisk::inject_fault`] arms for the next matching operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SimDiskFault {
    /// The next append to the file fails and writes nothing.
    Write,
    /// The next `fsync` of the file or directory fails.
    Sync,
    /// The next removal of the file fails and removes nothing.
    Remove,
}

/// What [`SimDisk::power_loss`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SimPowerLoss {
    /// Files with unsynced data at the time of the power loss.
    pub files: usize,
    /// Unsynced pages that reached the disk anyway.
    pub kept_pages: usize,
    /// Unsynced pages that were lost.
    pub dropped_pages: usize,
    /// Directory entries reverted to their last `fsync`ed state.
    pub reverted_entries: usize,
}

/// Failure of a simulated disk operation, carried inside the `io::Error`.
#[derive(Debug, thiserror::Error)]
pub enum SimDiskError {
    #[error("the simulated disk is only available inside a madsim runtime")]
    NoRuntime,
    #[error("simulated path '{}' does not exist", .path.display())]
    NotFound { path: PathBuf },
    #[error("read past the end of simulated file '{}'", .path.display())]
    ShortRead { path: PathBuf },
    #[error("simulated path '{}' is a directory", .path.display())]
    IsDirectory { path: PathBuf },
    #[error("simulated path '{}' is not a directory", .path.display())]
    NotDirectory { path: PathBuf },
    #[error("injected {fault:?} fault on simulated path '{}'", .path.display())]
    Injected { path: PathBuf, fault: SimDiskFault },
    #[error(
        "simulated node under '{}' still holds the journal lock '{}'; stop it first",
        .prefix.display(),
        .lock.display()
    )]
    NodeRunning { prefix: PathBuf, lock: PathBuf },
}

impl SimDiskError {
    fn into_io(self) -> io::Error {
        let kind = match &self {
            Self::NoRuntime => io::ErrorKind::Unsupported,
            Self::NotFound { .. } => io::ErrorKind::NotFound,
            Self::ShortRead { .. } => io::ErrorKind::UnexpectedEof,
            Self::IsDirectory { .. } | Self::NotDirectory { .. } => io::ErrorKind::InvalidInput,
            Self::Injected { .. } => io::ErrorKind::Other,
            Self::NodeRunning { .. } => io::ErrorKind::ResourceBusy,
        };
        io::Error::new(kind, self)
    }
}

/// An open simulated file. It keeps referring to the same file after a rename.
#[derive(Debug)]
pub struct SimFile {
    path: PathBuf,
    inode: u64,
    read_pos: usize,
}

/// A held simulated journal lock, released on drop.
#[derive(Debug)]
pub struct SimJournalLock {
    path: PathBuf,
}

impl Drop for SimJournalLock {
    fn drop(&mut self) {
        // Only the runtime that granted the lock may release it.
        let released = DISK.with(|cell| {
            let current = current_runtime()?;
            let mut slot = cell.borrow_mut();
            let disk = slot
                .as_mut()
                .filter(|disk| Weak::ptr_eq(&disk.runtime, &current))?;
            Some(disk.state.locks.remove(&self.path))
        });
        if released != Some(true) {
            tracing::trace!(path = %self.path.display(), "simulated journal lock outlived its runtime");
        }
    }
}

struct RuntimeDisk {
    runtime: Weak<NetSim>,
    state: DiskState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Entry {
    Dir,
    File(u64),
}

#[derive(Default)]
struct DiskState {
    /// Directory entries as the running process sees them.
    visible: BTreeMap<PathBuf, Entry>,
    /// Directory entries that survive a power loss.
    durable: BTreeMap<PathBuf, Entry>,
    inodes: BTreeMap<u64, Inode>,
    next_inode: u64,
    locks: BTreeSet<PathBuf>,
    faults: BTreeSet<(PathBuf, SimDiskFault)>,
    next_root: u64,
    /// The prefix of every power loss so far, in order.
    power_losses: Vec<PathBuf>,
}

#[derive(Default)]
struct Inode {
    /// Contents as the running process reads them.
    data: Vec<u8>,
    /// Contents that survive a power loss.
    durable: Vec<u8>,
    /// Pages written since the last `fsync`.
    dirty: BTreeSet<usize>,
}

fn current_runtime() -> Option<Weak<NetSim>> {
    madsim::runtime::Handle::try_current().ok()?;
    Some(Arc::downgrade(&NetSim::current()))
}

fn with_disk<T>(f: impl FnOnce(&mut DiskState) -> T) -> Result<T, SimDiskError> {
    let runtime = current_runtime().ok_or(SimDiskError::NoRuntime)?;
    DISK.with(|cell| {
        let mut slot = cell.borrow_mut();
        let disk = match slot.take() {
            Some(disk) if Weak::ptr_eq(&disk.runtime, &runtime) => disk,
            _ => RuntimeDisk {
                runtime,
                state: DiskState::default(),
            },
        };
        let disk = slot.insert(disk);
        Ok(f(&mut disk.state))
    })
}

fn io_with_disk<T>(f: impl FnOnce(&mut DiskState) -> Result<T, SimDiskError>) -> io::Result<T> {
    with_disk(f)
        .and_then(|result| result)
        .map_err(SimDiskError::into_io)
}

fn is_root(path: &Path) -> bool {
    path.parent().is_none() || path.as_os_str().is_empty()
}

fn page_range(start: usize, end: usize) -> std::ops::RangeInclusive<usize> {
    let first = start.checked_div(PAGE_SIZE).unwrap_or_default();
    let last = end
        .saturating_sub(1)
        .checked_div(PAGE_SIZE)
        .unwrap_or_default();
    first..=last
}

impl Inode {
    fn mark_dirty(&mut self, start: usize, end: usize) {
        if start < end {
            self.dirty.extend(page_range(start, end));
        }
    }

    fn append(&mut self, buf: &[u8]) {
        let start = self.data.len();
        self.data.extend_from_slice(buf);
        self.mark_dirty(start, self.data.len());
    }

    fn set_len(&mut self, len: usize) {
        let old = self.data.len();
        self.data.resize(len, 0);
        self.mark_dirty(old.min(len), old.max(len));
    }

    fn page_bounds(&self, page: usize) -> Option<(usize, usize)> {
        let start = page.checked_mul(PAGE_SIZE)?;
        let end = start.saturating_add(PAGE_SIZE).min(self.data.len());
        (start < end).then_some((start, end))
    }

    fn copy_page(&self, page: usize, image: &mut Vec<u8>) {
        let Some((start, end)) = self.page_bounds(page) else {
            return;
        };
        if image.len() < end {
            image.resize(end, 0);
        }
        if let (Some(target), Some(source)) = (image.get_mut(start..end), self.data.get(start..end))
        {
            target.copy_from_slice(source);
        }
    }

    fn sync(&mut self) {
        let mut durable = std::mem::take(&mut self.durable);
        durable.truncate(self.data.len());
        for page in std::mem::take(&mut self.dirty) {
            self.copy_page(page, &mut durable);
        }
        durable.resize(self.data.len(), 0);
        self.durable = durable;
    }

    /// Keeps or drops every unsynced page, then makes the result both the
    /// durable and the visible contents.
    fn lose_power(&mut self, report: &mut SimPowerLoss) {
        if self.dirty.is_empty() && self.data == self.durable {
            return;
        }
        report.files = report.files.saturating_add(1);
        let mut image = self.durable.clone();
        if self.data.len() < image.len() && madsim::rand::random::<bool>() {
            image.truncate(self.data.len());
        }
        for page in std::mem::take(&mut self.dirty) {
            if self.page_bounds(page).is_none() {
                continue;
            }
            if madsim::rand::random::<bool>() {
                self.copy_page(page, &mut image);
                report.kept_pages = report.kept_pages.saturating_add(1);
            } else {
                report.dropped_pages = report.dropped_pages.saturating_add(1);
            }
        }
        self.durable.clone_from(&image);
        self.data = image;
    }
}

impl DiskState {
    /// Resolves `path` in the visible tree; every ancestor must be a directory.
    fn resolve(&self, path: &Path) -> Option<Entry> {
        if is_root(path) {
            return Some(Entry::Dir);
        }
        let parent_is_dir = path
            .parent()
            .is_some_and(|parent| self.resolve(parent) == Some(Entry::Dir));
        if !parent_is_dir {
            return None;
        }
        self.visible.get(path).copied()
    }

    fn resolve_file(&self, path: &Path) -> Result<u64, SimDiskError> {
        match self.resolve(path) {
            Some(Entry::File(inode)) => Ok(inode),
            Some(Entry::Dir) => Err(SimDiskError::IsDirectory {
                path: path.to_owned(),
            }),
            None => Err(SimDiskError::NotFound {
                path: path.to_owned(),
            }),
        }
    }

    fn require_parent_dir(&self, path: &Path) -> Result<(), SimDiskError> {
        let Some(parent) = path.parent() else {
            return Err(SimDiskError::IsDirectory {
                path: path.to_owned(),
            });
        };
        match self.resolve(parent) {
            Some(Entry::Dir) => Ok(()),
            Some(Entry::File(_)) => Err(SimDiskError::NotDirectory {
                path: parent.to_owned(),
            }),
            None => Err(SimDiskError::NotFound {
                path: parent.to_owned(),
            }),
        }
    }

    fn inode(&self, inode: u64) -> Option<&Inode> {
        self.inodes.get(&inode)
    }

    fn inode_mut(&mut self, inode: u64) -> &mut Inode {
        self.inodes.entry(inode).or_default()
    }

    fn take_fault(&mut self, path: &Path, fault: SimDiskFault) -> Result<(), SimDiskError> {
        if self.faults.remove(&(path.to_owned(), fault)) {
            return Err(SimDiskError::Injected {
                path: path.to_owned(),
                fault,
            });
        }
        Ok(())
    }

    fn create_dir_all(&mut self, path: &Path) -> Result<(), SimDiskError> {
        let mut missing = Vec::new();
        for dir in path.ancestors() {
            match self.resolve(dir) {
                Some(Entry::Dir) => break,
                Some(Entry::File(_)) => {
                    return Err(SimDiskError::NotDirectory {
                        path: dir.to_owned(),
                    });
                }
                None => missing.push(dir.to_owned()),
            }
        }
        for dir in missing.into_iter().rev() {
            self.visible.insert(dir, Entry::Dir);
        }
        Ok(())
    }

    fn open(&mut self, path: &Path, create: bool) -> Result<SimFile, SimDiskError> {
        let inode = match self.resolve_file(path) {
            Ok(inode) => inode,
            Err(SimDiskError::NotFound { .. }) if create => {
                self.require_parent_dir(path)?;
                let inode = self.next_inode;
                self.next_inode = self.next_inode.saturating_add(1);
                self.inodes.insert(inode, Inode::default());
                self.visible.insert(path.to_owned(), Entry::File(inode));
                inode
            }
            Err(err) => return Err(err),
        };
        Ok(SimFile {
            path: path.to_owned(),
            inode,
            read_pos: 0,
        })
    }

    fn sync_dir(&mut self, dir: &Path) -> Result<(), SimDiskError> {
        if self.resolve(dir) != Some(Entry::Dir) {
            return Err(SimDiskError::NotFound {
                path: dir.to_owned(),
            });
        }
        self.take_fault(dir, SimDiskFault::Sync)?;
        let is_child = |path: &Path| path.parent() == Some(dir);
        self.durable.retain(|path, _| !is_child(path));
        for (path, entry) in &self.visible {
            if is_child(path) {
                self.durable.insert(path.clone(), *entry);
            }
        }
        Ok(())
    }

    fn power_loss(&mut self, prefix: &Path) -> Result<SimPowerLoss, SimDiskError> {
        if let Some(lock) = self.locks.iter().find(|lock| lock.starts_with(prefix)) {
            return Err(SimDiskError::NodeRunning {
                prefix: prefix.to_owned(),
                lock: lock.clone(),
            });
        }
        let mut report = SimPowerLoss::default();
        let under_prefix = |path: &Path| path.starts_with(prefix);
        let inodes = self
            .visible
            .iter()
            .chain(self.durable.iter())
            .filter(|(path, _)| under_prefix(path))
            .filter_map(|(_, entry)| match entry {
                Entry::File(inode) => Some(*inode),
                Entry::Dir => None,
            })
            .collect::<BTreeSet<_>>();
        for inode in inodes {
            self.inode_mut(inode).lose_power(&mut report);
        }

        let visible = self
            .visible
            .iter()
            .filter(|(path, _)| under_prefix(path))
            .map(|(path, entry)| (path.clone(), *entry))
            .collect::<BTreeMap<_, _>>();
        let durable = self
            .durable
            .iter()
            .filter(|(path, _)| under_prefix(path))
            .map(|(path, entry)| (path.clone(), *entry))
            .collect::<BTreeMap<_, _>>();
        report.reverted_entries = visible
            .keys()
            .chain(durable.keys())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|path| visible.get(*path) != durable.get(*path))
            .count();
        self.visible.retain(|path, _| !under_prefix(path));
        self.visible.extend(durable);
        // An entry whose directory lost its own entry is gone with it; a
        // directory created again under the same name starts empty.
        let orphans = self
            .visible
            .keys()
            .filter(|path| under_prefix(path))
            .filter(|path| {
                path.parent()
                    .is_some_and(|parent| self.resolve(parent) != Some(Entry::Dir))
            })
            .cloned()
            .collect::<Vec<_>>();
        for orphan in orphans {
            self.visible.retain(|path, _| !path.starts_with(&orphan));
            self.durable.retain(|path, _| !path.starts_with(&orphan));
        }
        self.faults.retain(|(path, _)| !under_prefix(path));
        self.power_losses.push(prefix.to_owned());
        Ok(report)
    }

    /// The simulated boot of the host holding `path`: one more for every
    /// power loss that covered it.
    fn boot_id(&self, path: &Path) -> String {
        let boots = self
            .power_losses
            .iter()
            .filter(|prefix| path.starts_with(prefix))
            .count();
        format!("sim-boot-{boots}")
    }
}

impl SimDisk {
    /// Simulates a crash of the process that owns `prefix`: the page cache
    /// survives, so nothing is lost. The node must already be stopped.
    pub fn process_crash(prefix: &Path) -> io::Result<()> {
        io_with_disk(
            |state| match state.locks.iter().find(|lock| lock.starts_with(prefix)) {
                Some(lock) => Err(SimDiskError::NodeRunning {
                    prefix: prefix.to_owned(),
                    lock: lock.clone(),
                }),
                None => Ok(()),
            },
        )
    }

    /// Simulates a power loss of the disk under `prefix`; the node must
    /// already be stopped. Unsynced pages are kept or dropped page by page and
    /// directory entries revert to their last `fsync`ed state.
    pub fn power_loss(prefix: &Path) -> io::Result<SimPowerLoss> {
        io_with_disk(|state| state.power_loss(prefix))
    }

    /// Fails the next write or `fsync` of `path` with an I/O error.
    pub fn inject_fault(path: &Path, fault: SimDiskFault) -> io::Result<()> {
        with_disk(|state| {
            state.faults.insert((path.to_owned(), fault));
        })
        .map_err(SimDiskError::into_io)
    }

    /// Disarms every fault not yet fired on a path under `prefix`.
    pub fn clear_faults(prefix: &Path) -> io::Result<()> {
        with_disk(|state| state.faults.retain(|(path, _)| !path.starts_with(prefix)))
            .map_err(SimDiskError::into_io)
    }

    /// The contents of the file at `path` as the running process reads them.
    pub fn read(path: &Path) -> io::Result<Vec<u8>> {
        io_with_disk(|state| {
            let inode = state.resolve_file(path)?;
            Ok(state
                .inode(inode)
                .map(|inode| inode.data.clone())
                .unwrap_or_default())
        })
    }

    /// Creates a new, empty directory for one simulated node or cluster and
    /// makes it durable, as a provisioned data directory is before the node
    /// first starts. Names are unique within the current runtime.
    pub fn provision_dir(name: &str) -> io::Result<PathBuf> {
        io_with_disk(|state| {
            let ordinal = state.next_root;
            state.next_root = state.next_root.saturating_add(1);
            let root = PathBuf::from(SIM_ROOT).join(format!("{name}-{ordinal}"));
            state.create_dir_all(&root)?;
            for dir in root.ancestors().skip(1) {
                state.sync_dir(dir)?;
            }
            Ok(root)
        })
    }
}

impl JournalDisk for SimDisk {
    type File = SimFile;
    type Lock = SimJournalLock;

    fn create_dir_all(path: &Path) -> io::Result<()> {
        io_with_disk(|state| state.create_dir_all(path))
    }

    fn open_append(path: &Path) -> io::Result<SimFile> {
        io_with_disk(|state| state.open(path, true))
    }

    fn open_read(path: &Path) -> io::Result<SimFile> {
        io_with_disk(|state| state.open(path, false))
    }

    fn exists(path: &Path) -> bool {
        with_disk(|state| state.resolve(path).is_some()).unwrap_or(false)
    }

    fn read_dir(path: &Path) -> io::Result<Vec<PathBuf>> {
        io_with_disk(|state| match state.resolve(path) {
            Some(Entry::Dir) => Ok(state
                .visible
                .keys()
                .filter(|entry| entry.parent() == Some(path))
                .cloned()
                .collect()),
            Some(Entry::File(_)) => Err(SimDiskError::NotDirectory {
                path: path.to_owned(),
            }),
            None => Err(SimDiskError::NotFound {
                path: path.to_owned(),
            }),
        })
    }

    fn truncate(path: &Path, len: u64) -> io::Result<()> {
        io_with_disk(|state| {
            let inode = state.resolve_file(path)?;
            state.take_fault(path, SimDiskFault::Sync)?;
            let inode = state.inode_mut(inode);
            inode.set_len(usize::try_from(len).unwrap_or(usize::MAX));
            inode.sync();
            Ok(())
        })
    }

    fn rename(from: &Path, to: &Path) -> io::Result<()> {
        io_with_disk(|state| {
            let inode = state.resolve_file(from)?;
            state.require_parent_dir(to)?;
            if state.resolve(to) == Some(Entry::Dir) {
                return Err(SimDiskError::IsDirectory {
                    path: to.to_owned(),
                });
            }
            state.visible.remove(from);
            state.visible.insert(to.to_owned(), Entry::File(inode));
            Ok(())
        })
    }

    fn remove_file(path: &Path) -> io::Result<()> {
        io_with_disk(|state| {
            state.resolve_file(path)?;
            state.take_fault(path, SimDiskFault::Remove)?;
            state.visible.remove(path);
            Ok(())
        })
    }

    fn sync_dir(path: &Path) -> io::Result<()> {
        io_with_disk(|state| state.sync_dir(path))
    }

    fn try_lock(path: &Path) -> io::Result<LockAttempt<SimJournalLock>> {
        io_with_disk(|state| {
            state.require_parent_dir(path)?;
            if !state.locks.insert(path.to_owned()) {
                return Ok(LockAttempt::Held {
                    owner: Some("simulated node".to_owned()),
                });
            }
            Ok(LockAttempt::Acquired(SimJournalLock {
                path: path.to_owned(),
            }))
        })
    }

    fn boot_id(path: &Path) -> Option<String> {
        with_disk(|state| state.boot_id(path)).ok()
    }
}

impl JournalFile for SimFile {
    fn file_len(&self) -> io::Result<u64> {
        io_with_disk(|state| Ok(state.inode(self.inode).map_or(0, |inode| inode.data.len())))
            .map(|len| u64::try_from(len).unwrap_or(u64::MAX))
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let start = self.read_pos;
        let end = start.saturating_add(buf.len());
        io_with_disk(|state| {
            let source = state
                .inode(self.inode)
                .and_then(|inode| inode.data.get(start..end))
                .ok_or_else(|| SimDiskError::ShortRead {
                    path: self.path.clone(),
                })?;
            buf.copy_from_slice(source);
            Ok(())
        })?;
        self.read_pos = end;
        Ok(())
    }

    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.read_pos = usize::try_from(offset).unwrap_or(usize::MAX);
        self.read_exact(buf)
    }

    fn append(&mut self, buf: &[u8]) -> io::Result<()> {
        io_with_disk(|state| {
            state.take_fault(&self.path, SimDiskFault::Write)?;
            state.inode_mut(self.inode).append(buf);
            Ok(())
        })
    }

    fn sync_data(&mut self) -> io::Result<()> {
        io_with_disk(|state| {
            if let Err(err) = state.take_fault(&self.path, SimDiskFault::Sync) {
                // Linux reports the error once and marks the pages clean.
                state.inode_mut(self.inode).dirty.clear();
                return Err(err);
            }
            state.inode_mut(self.inode).sync();
            Ok(())
        })
    }
}
