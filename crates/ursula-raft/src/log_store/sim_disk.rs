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
//!   [`SimDisk::power_loss_losing_unsynced`] drops every unsynced page, for
//!   a schedule that needs the loss to happen.
//! - [`SimDisk::write_back`] makes every page under a prefix durable, as a
//!   host's page cache writes pages back on its own, for a schedule that
//!   must keep what it wrote so far across a later power loss.
//! - [`SimDisk::inject_fault`] fails the next matching file operation. A
//!   partial write exposes half the bytes before failing, and a rename fault
//!   matches its destination. A failed `fsync` marks the pages clean without
//!   persisting them, as Linux does, so a later successful `fsync` does not make
//!   them durable.
//!
//! The disk also stands in for the host's boot id
//! ([`JournalDisk::boot_id`]): a power loss under a prefix starts a new boot
//! for every path below it, and a process crash does not. It stands in for
//! the device a path is mounted from too ([`JournalDisk::device`]): each
//! move of a volume ([`SimDisk::move_volume`]) attaches it as a new device.
//!
//! [`SimDisk::move_volume`] takes a volume from the process running on it,
//! as storage that revokes an attachment does, and attaches it again under
//! another root on the same host. The new attachment holds what was
//! durable, as after a power loss. The old process keeps its page cache:
//! its writes through files it opened land there, its syncs fail, and so
//! does every change of a directory entry, so nothing it does reaches the
//! volume again.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Weak;
use std::time::SystemTime;

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
    /// The next append writes half its bytes, then reports an I/O error.
    PartialWrite,
    /// The next `fsync` of the file or directory fails.
    Sync,
    /// The next removal of the file fails and removes nothing.
    Remove,
    /// The next rename to this destination fails without changing either name.
    Rename,
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
    #[error("simulated path '{}' is on a volume that moved away", .path.display())]
    Detached { path: PathBuf },
}

impl SimDiskError {
    fn into_io(self) -> io::Error {
        let kind = match &self {
            Self::NoRuntime => io::ErrorKind::Unsupported,
            Self::NotFound { .. } => io::ErrorKind::NotFound,
            Self::ShortRead { .. } => io::ErrorKind::UnexpectedEof,
            Self::IsDirectory { .. } | Self::NotDirectory { .. } => io::ErrorKind::InvalidInput,
            Self::Injected { .. } | Self::Detached { .. } => io::ErrorKind::Other,
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
    /// Tells this lock apart from a later one at the same path, taken
    /// after a detach dropped this one.
    token: u64,
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
            let held = disk.state.locks.get(&self.path) == Some(&self.token);
            if held {
                disk.state.locks.remove(&self.path);
            }
            Some(held)
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
    /// Each held lock, with its token.
    locks: BTreeMap<PathBuf, u64>,
    next_lock: u64,
    /// The prefixes whose volume moved away from the process on them.
    detached: BTreeSet<PathBuf>,
    faults: BTreeSet<(PathBuf, SimDiskFault)>,
    next_root: u64,
    /// The prefix of every power loss so far, in order.
    power_losses: Vec<PathBuf>,
    /// The prefix of every attachment of a moved volume so far, in order.
    attachments: Vec<PathBuf>,
}

struct Inode {
    /// Contents as the running process reads them.
    data: Vec<u8>,
    /// Contents that survive a power loss.
    durable: Vec<u8>,
    /// Pages written since the last `fsync`.
    dirty: BTreeSet<usize>,
    /// When the contents last changed, on the simulation's wall clock.
    modified: SystemTime,
}

impl Default for Inode {
    fn default() -> Self {
        Self {
            data: Vec::new(),
            durable: Vec::new(),
            dirty: BTreeSet::new(),
            modified: sim_wall_clock(),
        }
    }
}

/// The simulation's wall clock, which madsim advances with virtual time.
fn sim_wall_clock() -> SystemTime {
    madsim::time::TimeHandle::try_current().map_or(SystemTime::UNIX_EPOCH, |time| time.now_time())
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
    /// Another file with the same contents, page cache and durable alike.
    fn copy(&self) -> Self {
        Self {
            data: self.data.clone(),
            durable: self.durable.clone(),
            dirty: self.dirty.clone(),
            modified: self.modified,
        }
    }

    fn mark_dirty(&mut self, start: usize, end: usize) {
        if start < end {
            self.dirty.extend(page_range(start, end));
        }
        self.modified = sim_wall_clock();
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
    /// durable and the visible contents. `reaches_disk` decides, in order,
    /// whether an unsynced truncation and then each unsynced page landed.
    fn lose_power(&mut self, reaches_disk: &mut impl FnMut() -> bool, report: &mut SimPowerLoss) {
        if self.dirty.is_empty() && self.data == self.durable {
            return;
        }
        report.files = report.files.saturating_add(1);
        let mut image = self.durable.clone();
        if self.data.len() < image.len() && reaches_disk() {
            image.truncate(self.data.len());
        }
        for page in std::mem::take(&mut self.dirty) {
            if self.page_bounds(page).is_none() {
                continue;
            }
            if reaches_disk() {
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

    /// Refuses a sync or a change of a directory entry under a prefix whose
    /// volume moved away ([`SimDisk::move_volume`]).
    fn attached(&self, path: &Path) -> Result<(), SimDiskError> {
        if self.detached.iter().any(|prefix| path.starts_with(prefix)) {
            return Err(SimDiskError::Detached {
                path: path.to_owned(),
            });
        }
        Ok(())
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
                self.attached(path)?;
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
        self.attached(dir)?;
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

    /// Refuses `prefix` while a running node holds a lock under it.
    fn stopped(&self, prefix: &Path) -> Result<(), SimDiskError> {
        match self.locks.keys().find(|lock| lock.starts_with(prefix)) {
            Some(lock) => Err(SimDiskError::NodeRunning {
                prefix: prefix.to_owned(),
                lock: lock.clone(),
            }),
            None => Ok(()),
        }
    }

    fn power_loss(
        &mut self,
        prefix: &Path,
        reaches_disk: &mut impl FnMut() -> bool,
    ) -> Result<SimPowerLoss, SimDiskError> {
        self.stopped(prefix)?;
        let report = self.lose_unsynced(prefix, reaches_disk);
        self.power_losses.push(prefix.to_owned());
        Ok(report)
    }

    /// Keeps or drops what was not synced under `prefix`, as `reaches_disk`
    /// decides, and reverts its directory entries to their synced state.
    fn lose_unsynced(
        &mut self,
        prefix: &Path,
        reaches_disk: &mut impl FnMut() -> bool,
    ) -> SimPowerLoss {
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
            self.inode_mut(inode).lose_power(reaches_disk, &mut report);
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
        report
    }

    /// Makes every page of the files under `prefix` durable.
    fn write_back(&mut self, prefix: &Path) {
        let inodes = self
            .visible
            .iter()
            .filter(|(path, _)| path.starts_with(prefix))
            .filter_map(|(_, entry)| match entry {
                Entry::File(inode) => Some(*inode),
                Entry::Dir => None,
            })
            .collect::<BTreeSet<_>>();
        for inode in inodes {
            self.inode_mut(inode).sync();
        }
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

    /// The simulated device of the volume holding `path`: one more for
    /// every attachment of a moved volume that covered it.
    fn device(&self, path: &Path) -> u64 {
        let attachments = self
            .attachments
            .iter()
            .filter(|prefix| path.starts_with(prefix))
            .count();
        u64::try_from(attachments).unwrap_or(u64::MAX)
    }
}

impl SimDisk {
    /// Simulates a crash of the process that owns `prefix`: the page cache
    /// survives, so nothing is lost. The node must already be stopped.
    pub fn process_crash(prefix: &Path) -> io::Result<()> {
        io_with_disk(|state| state.stopped(prefix))
    }

    /// Moves the volume under `prefix` away from the process running on it,
    /// as storage that revokes an attachment does, and attaches it again on
    /// the same host as another device under a new root named after `name`,
    /// which it answers. The new root holds what was durable under
    /// `prefix`, as after a power loss, and keeps its boot. The process on
    /// `prefix` may run on: its writes through the files it opened land in
    /// its page cache, and its syncs and directory changes fail.
    pub fn move_volume(prefix: &Path, name: &str) -> io::Result<PathBuf> {
        let root = Self::provision_dir(name)?;
        io_with_disk(|state| {
            state.detached.insert(prefix.to_owned());
            let moved = |path: &Path| {
                path.strip_prefix(prefix)
                    .ok()
                    .map(|relative| root.join(relative))
            };
            let inodes = state
                .visible
                .iter()
                .chain(state.durable.iter())
                .filter(|(path, _)| path.starts_with(prefix))
                .filter_map(|(_, entry)| match entry {
                    Entry::File(inode) => Some(*inode),
                    Entry::Dir => None,
                })
                .collect::<BTreeSet<_>>();
            let mut copies = BTreeMap::new();
            for inode in inodes {
                let copy = state.next_inode;
                state.next_inode = state.next_inode.saturating_add(1);
                let contents = state.inode(inode).map(Inode::copy).unwrap_or_default();
                state.inodes.insert(copy, contents);
                copies.insert(inode, copy);
            }
            let copy_entry = |entry: &Entry| match entry {
                Entry::Dir => Entry::Dir,
                Entry::File(inode) => Entry::File(copies.get(inode).copied().unwrap_or(*inode)),
            };
            let copy_tree = |tree: &BTreeMap<PathBuf, Entry>| {
                tree.iter()
                    .filter(|(path, _)| path.as_path() != prefix)
                    .filter_map(|(path, entry)| Some((moved(path)?, copy_entry(entry))))
                    .collect::<Vec<_>>()
            };
            let visible = copy_tree(&state.visible);
            let durable = copy_tree(&state.durable);
            state.visible.extend(visible);
            state.durable.extend(durable);
            let boots = state
                .power_losses
                .iter()
                .filter(|loss| prefix.starts_with(loss))
                .count();
            let devices = state
                .attachments
                .iter()
                .filter(|attachment| prefix.starts_with(attachment))
                .count();
            state
                .power_losses
                .extend(std::iter::repeat_n(root.clone(), boots));
            state
                .attachments
                .extend(std::iter::repeat_n(root.clone(), devices.saturating_add(1)));
            state.lose_unsynced(&root, &mut || false);
            Ok(())
        })?;
        Ok(root)
    }

    /// Simulates a power loss of the disk under `prefix`; the node must
    /// already be stopped. Unsynced pages are kept or dropped page by page and
    /// directory entries revert to their last `fsync`ed state.
    pub fn power_loss(prefix: &Path) -> io::Result<SimPowerLoss> {
        io_with_disk(|state| state.power_loss(prefix, &mut madsim::rand::random::<bool>))
    }

    /// Simulates a power loss of the disk under `prefix` in which nothing
    /// written since the last `fsync` reaches the disk: every unsynced page
    /// and unsynced truncation is lost, and directory entries revert to their
    /// last `fsync`ed state. The node must already be stopped.
    pub fn power_loss_losing_unsynced(prefix: &Path) -> io::Result<SimPowerLoss> {
        io_with_disk(|state| state.power_loss(prefix, &mut || false))
    }

    /// Writes every unsynced page of the files under `prefix` back to the
    /// disk, as a host's page cache does on its own seconds after a write,
    /// whatever the process `fsync`s. A later power loss keeps those pages.
    /// Directory entries keep their last `fsync`ed state. A schedule that
    /// must keep what it wrote so far across a power loss says so with this,
    /// instead of relying on a segment rotation to `fsync` it.
    pub fn write_back(prefix: &Path) -> io::Result<()> {
        with_disk(|state| state.write_back(prefix)).map_err(SimDiskError::into_io)
    }

    /// Arms a one-shot fault on `path`; rename faults match the destination.
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

    fn open_overwrite(path: &Path) -> io::Result<SimFile> {
        io_with_disk(|state| state.open(path, true))
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
            state.attached(path)?;
            state.take_fault(path, SimDiskFault::Sync)?;
            let inode = state.inode_mut(inode);
            inode.set_len(usize::try_from(len).unwrap_or(usize::MAX));
            inode.sync();
            Ok(())
        })
    }

    fn rename(from: &Path, to: &Path) -> io::Result<()> {
        io_with_disk(|state| {
            state.attached(to)?;
            state.take_fault(to, SimDiskFault::Rename)?;
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
            state.attached(path)?;
            state.take_fault(path, SimDiskFault::Remove)?;
            state.visible.remove(path);
            Ok(())
        })
    }

    fn sync_dir(path: &Path) -> io::Result<()> {
        io_with_disk(|state| state.sync_dir(path))
    }

    fn modified(path: &Path) -> io::Result<SystemTime> {
        io_with_disk(|state| {
            let inode = state.resolve_file(path)?;
            Ok(state
                .inode(inode)
                .map_or(SystemTime::UNIX_EPOCH, |inode| inode.modified))
        })
    }

    fn wall_clock() -> SystemTime {
        sim_wall_clock()
    }

    fn try_lock(path: &Path) -> io::Result<LockAttempt<SimJournalLock>> {
        io_with_disk(|state| {
            state.require_parent_dir(path)?;
            state.attached(path)?;
            if state.locks.contains_key(path) {
                return Ok(LockAttempt::Held {
                    owner: Some("simulated node".to_owned()),
                });
            }
            let token = state.next_lock;
            state.next_lock = state.next_lock.saturating_add(1);
            state.locks.insert(path.to_owned(), token);
            Ok(LockAttempt::Acquired(SimJournalLock {
                path: path.to_owned(),
                token,
            }))
        })
    }

    fn boot_id(path: &Path) -> Option<String> {
        with_disk(|state| state.boot_id(path)).ok()
    }

    fn device(path: &Path) -> io::Result<u64> {
        with_disk(|state| state.device(path)).map_err(SimDiskError::into_io)
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

    /// Lands in the page cache, also after the volume moved away.
    fn append(&mut self, buf: &[u8]) -> io::Result<()> {
        io_with_disk(|state| {
            state.take_fault(&self.path, SimDiskFault::Write)?;
            if let Err(error) = state.take_fault(&self.path, SimDiskFault::PartialWrite) {
                let prefix = buf.get(..buf.len() / 2).unwrap_or_default();
                state.inode_mut(self.inode).append(prefix);
                return Err(error);
            }
            state.inode_mut(self.inode).append(buf);
            Ok(())
        })
    }

    fn sync_data(&mut self) -> io::Result<()> {
        io_with_disk(|state| {
            state.attached(&self.path)?;
            if let Err(err) = state.take_fault(&self.path, SimDiskFault::Sync) {
                // Linux reports the error once and marks the pages clean.
                state.inode_mut(self.inode).dirty.clear();
                return Err(err);
            }
            state.inode_mut(self.inode).sync();
            Ok(())
        })
    }

    /// The write lands in the page cache, also after the volume moved
    /// away, and the sync fails then.
    fn overwrite_start(&mut self, buf: &[u8]) -> io::Result<()> {
        io_with_disk(|state| {
            state.take_fault(&self.path, SimDiskFault::Write)?;
            let inode = state.inode_mut(self.inode);
            if inode.data.len() < buf.len() {
                inode.data.resize(buf.len(), 0);
            }
            if let Some(start) = inode.data.get_mut(..buf.len()) {
                start.copy_from_slice(buf);
            }
            inode.mark_dirty(0, buf.len());
            state.attached(&self.path)?;
            if let Err(err) = state.take_fault(&self.path, SimDiskFault::Sync) {
                state.inode_mut(self.inode).dirty.clear();
                return Err(err);
            }
            state.inode_mut(self.inode).sync();
            Ok(())
        })
    }
}
