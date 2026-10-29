//! A host's read of every root and every open root lock (design: Hosts): whether the disk holds a
//! commit the stream has not acknowledged, for a host that snapshots or moves the process, or the
//! VM it runs in. [`inspect`] is the one entry point, and `ursula-vfs-host inspect` prints its
//! [`Inspection`] as one line of JSON.
//!
//! A pass reads, and never writes or locks anything:
//!
//! 1. The host directory: the one the host names, or the compiled-in one (`hostdir::DEFAULT`),
//!    never the extension's test hook. It must be a directory, not a link. When it is the
//!    compiled-in directory (the same device and inode) it must be root's, mode 1777, in a
//!    root-owned directory of mode 0755, with every ancestor a directory and no link among them.
//!    Every name in it must be one the host directory's grammar knows (`hostdir::entry`), and
//!    every record a record of this line (`v` 1) naming an absolute path under its own key.
//! 2. The directories: the recorded ones and those the host names (`scans`), one per real path. A
//!    missing path, or one that is not a directory, holds no roots. In a present one every
//!    subdirectory with a root's name (`generation::is_root_name`) is a root. A root with no
//!    `gen-<id32>` directory holds no commit and is not listed.
//! 3. Each root: its current generation by the open's own rule (`Root::classify`), and that
//!    generation's log by the open's reading rule (`Log::read`) with a fold that keeps a position
//!    and a length per pending commit (`Unacknowledged`). Its bytes are what
//!    `Recovered::pending` holds: the frame lengths of the generation's own COMMIT records above
//!    the latest ACK. The commits its salvages list as unacknowledged, and those of a superseded
//!    generation the next open salvages, are reported beside them, outside the bytes.
//! 4. The lock check: every open file of a live process (`/proc/<pid>/fd`, or the descriptors of
//!    its threads once its first thread exited) that may be a root's `lock` must be the lock of
//!    a root this pass read, whatever path the process opened it by. A file named `lock` may be
//!    a root's when its directory has a root's name, or when the process holds an exclusive
//!    `flock` on it (`/proc/<pid>/fdinfo`), since a root's directory may have been renamed. A
//!    reader directory's lock is not one (a reader's name in a directory with a root's name):
//!    readers hold no commit. Any other is an unread lock, and so is a process `--pid` names that
//!    is alive while `/proc` hides it.
//!
//! A pass that cannot do one of these is incomplete, and says why with one reason, the first that
//! applies of `host_dir`, `unrecognized`, `unread_lock`, `unreadable` and `budget`.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use crate::generation;
use crate::generation::GenerationError;
use crate::generation::Root;
use crate::hostdir;
use crate::hostdir::Entry;
use crate::hostdir::Existing;
use crate::hostdir::Malformed;
use crate::json;
use crate::replog::Log;
use crate::replog::LogError;
use crate::replog::Unacknowledged;
use crate::salvage;
use crate::salvage::SalvageError;

/// How often [`inspect`] reads again while it waits for a clean result.
const AGAIN: Duration = Duration::from_millis(50);
/// What a lock path that names an open file of a deleted one ends in.
const DELETED: &[u8] = b" (deleted)";
/// The version of the inspection's JSON.
const VERSION: u64 = 1;

/// What [`inspect`] reads.
#[derive(Debug, Clone, Default)]
pub struct Inspect {
    /// The host directory: the compiled-in one (`hostdir::DEFAULT`) when `None`. The extension's
    /// test hook is never read: a hook set in the host's environment would read another
    /// directory than the extension's.
    pub dir: Option<PathBuf>,
    /// Directories to read besides the recorded ones, such as the default cache roots.
    pub scans: Vec<PathBuf>,
    /// The processes whose open files the lock check reads: every process when `None`, which
    /// only root can read.
    pub pids: Option<Vec<u32>>,
    /// Read again every 50 ms until the result is clean or incomplete, or this has passed. A pass
    /// after the first reads again only the roots whose files changed.
    pub until_clean: Option<Duration>,
    /// A pass that runs longer stops, incomplete (`budget`).
    pub budget: Option<Duration>,
}

/// Why an inspection is incomplete, in precedence order: an inspection reports the first that
/// applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Reason {
    /// The host directory is missing, not as it must be, or could not be read.
    HostDir,
    /// The host directory holds a name or a record this line does not know.
    Unrecognized,
    /// A live process holds a root's lock this pass did not read, or the process table could not
    /// be read.
    UnreadLock,
    /// A root, or a directory that may hold roots, could not be read.
    Unreadable,
    /// The pass ran out of time.
    Budget,
}

impl Reason {
    fn as_str(self) -> &'static str {
        match self {
            Reason::HostDir => "host_dir",
            Reason::Unrecognized => "unrecognized",
            Reason::UnreadLock => "unread_lock",
            Reason::Unreadable => "unreadable",
            Reason::Budget => "budget",
        }
    }
}

/// What made a pass incomplete as a whole.
#[derive(Debug, thiserror::Error)]
pub(crate) enum PassError {
    #[error("the host directory {}: {} {why}", .dir.display(), .path.display())]
    HostDirShape {
        dir: PathBuf,
        path: PathBuf,
        why: Shape,
    },
    #[error("the host directory {}: {op} {}: {source}", .dir.display(), .path.display())]
    HostDirIo {
        dir: PathBuf,
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("the host directory {}: it moved while it was listed", .dir.display())]
    HostDirMoved { dir: PathBuf },
    #[error("the process table: {op} {}: {source}", .path.display())]
    ProcessTable {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("the process table: {} names no effective uid", .path.display())]
    NoUid { path: PathBuf },
    #[error("the process table: process {pid} is alive and /proc does not show it")]
    Hidden { pid: u32 },
    #[error(
        "the process table: uid {euid} cannot read every process's open files, so the check of every process needs root"
    )]
    NotRoot { euid: u32 },
    #[error("the pass ran past its budget of {} ms", .budget.as_millis())]
    Budget { budget: Duration },
}

impl PassError {
    fn reason(&self) -> Reason {
        match self {
            PassError::HostDirShape { .. }
            | PassError::HostDirIo { .. }
            | PassError::HostDirMoved { .. } => Reason::HostDir,
            PassError::ProcessTable { .. }
            | PassError::NoUid { .. }
            | PassError::Hidden { .. }
            | PassError::NotRoot { .. } => Reason::UnreadLock,
            PassError::Budget { .. } => Reason::Budget,
        }
    }
}

/// How a path of the host directory is not what it must be.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Shape {
    #[error("is not a directory")]
    NotADirectory,
    #[error("is owned by uid {uid}, not {want}")]
    Owner { uid: u32, want: u32 },
    #[error("has mode {mode:o}, not {want:o}")]
    Mode { mode: u32, want: u32 },
}

/// Why a name in the host directory is not a record this line reads.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RecordError {
    #[error("is not a regular file")]
    NotAFile,
    #[error("is not a record: {0}")]
    Malformed(#[from] Malformed),
    #[error("is a record of format {}, not {}", .found.map_or_else(|| "unknown".to_owned(), |v| v.to_string()), hostdir::VERSION)]
    Version { found: Option<u64> },
    #[error("names {dir:?}, which is not an absolute path")]
    NotAbsolute { dir: String },
    #[error("names {dir:?}, whose record has another name")]
    Misnamed { dir: String },
}

/// Why a root could not be read.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RootError {
    #[error(transparent)]
    Generation(#[from] GenerationError),
    #[error(transparent)]
    Log(#[from] LogError),
    #[error(transparent)]
    Salvage(#[from] SalvageError),
    #[error("stat {}: {source}", .path.display())]
    Lock {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("log {}: it changed between two reads", .dir.display())]
    Changed { dir: PathBuf },
}

/// Where a directory to read came from.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Source {
    /// The record of this name.
    Record {
        name: String,
    },
    Scan,
}

/// A directory a pass read for roots.
#[derive(Debug)]
struct DirReport {
    dir: PathBuf,
    from: Source,
    state: DirState,
}

#[derive(Debug)]
enum DirState {
    Present,
    /// Nothing at the path, or something that is not a directory: no roots.
    Absent,
    /// It may hold roots, and could not be read.
    Unlistable(DirError),
}

/// Why a directory that may hold roots could not be read.
#[derive(Debug, thiserror::Error)]
#[error("{op} {}: {source}", .path.display())]
pub(crate) struct DirError {
    op: &'static str,
    path: PathBuf,
    #[source]
    source: std::io::Error,
}

/// A file's identity: its device and inode.
pub(crate) type FileId = (u64, u64);

/// What a pass found in one root, with the identity of its `lock` when it has one.
#[derive(Debug, Clone)]
enum RootRead {
    /// No `gen-<id32>` directory: it holds no commit and is not listed.
    NoGeneration { lock: Option<FileId> },
    /// A root the pass lists.
    Listed {
        lock: Option<FileId>,
        state: Arc<RootState>,
    },
}

/// What a root's read found: its stream and what it holds, or why it could not be read.
#[derive(Debug)]
pub(crate) enum RootState {
    Read {
        /// The stream path its segment header names.
        stream: String,
        cluster: String,
        /// The frame bytes of its pending commits.
        bytes: u64,
        commits: u64,
        /// The commits its salvages, and the superseded generations the next open salvages,
        /// list as unacknowledged.
        salvaged: u64,
    },
    Unreadable {
        error: Arc<RootError>,
        /// Its files changed while it was read, as a drain's trim changes them: a pass of
        /// `--until-clean` reads it again instead of ending.
        changed: bool,
    },
}

impl RootRead {
    /// Whether it is unreadable while its files changed: a later pass reads it again.
    fn changed(&self) -> bool {
        matches!(self, RootRead::Listed { state, .. }
            if matches!(**state, RootState::Unreadable { changed: true, .. }))
    }
}

/// A root a pass lists.
#[derive(Debug)]
struct RootReport {
    id: String,
    root: PathBuf,
    state: Arc<RootState>,
    /// Whether a live process holds its lock open, `None` when the lock check did not run.
    held: Option<bool>,
}

/// An open root lock of a live process that is the lock of no root a pass read.
#[derive(Debug)]
struct UnreadLock {
    pid: u32,
    path: PathBuf,
}

/// A name in the host directory a pass does not know.
#[derive(Debug)]
struct Unrecognized {
    name: String,
    /// Why a record's name holds no record of this line, `None` for an unknown name.
    error: Option<RecordError>,
}

/// The result of [`inspect`]: whether the inspection is complete, whether it is clean (complete,
/// with no unacknowledged byte), and what it read, as [`Inspection::to_json`] prints it.
#[derive(Debug)]
pub struct Inspection {
    dir: PathBuf,
    /// What it listed in the host directory, `None` when it could not list it, which makes it
    /// incomplete (`host_dir`).
    listing: Option<HostListing>,
    dirs: Vec<DirReport>,
    roots: Vec<RootReport>,
    unread_locks: Vec<UnreadLock>,
    unrecognized: Vec<Unrecognized>,
    errors: Vec<PassError>,
}

/// The host directory the strict checks apply to (design: Hosts), and the owner they want: the
/// compiled-in directory and root, or a test's directory and its owner.
#[derive(Debug, Clone, Copy)]
struct Strict<'a> {
    path: &'a Path,
    uid: u32,
}

/// Root's uid.
const ROOT_UID: u32 = 0;

impl Strict<'static> {
    /// The compiled-in host directory, owned by root.
    fn compiled_in() -> Self {
        Strict {
            path: Path::new(hostdir::DEFAULT),
            uid: ROOT_UID,
        }
    }
}

/// Reads every root of the host directory's records and of `request.scans`, and checks every open
/// root lock (design: Hosts). Never writes or locks anything.
pub fn inspect(request: &Inspect) -> Inspection {
    inspect_with(request, Strict::compiled_in())
}

/// [`inspect`] with the directory the strict checks apply to given.
fn inspect_with(request: &Inspect, strict: Strict<'_>) -> Inspection {
    let mut cache = Cache::default();
    let until = request
        .until_clean
        .and_then(|wait| Instant::now().checked_add(wait));
    loop {
        let seen = pass(request, strict, &mut cache);
        let Some(until) = until else {
            return seen;
        };
        let now = Instant::now();
        if seen.clean() || (!seen.complete() && !seen.settling()) || now >= until {
            return seen;
        }
        std::thread::sleep(AGAIN.min(until.saturating_duration_since(now)));
    }
}

/// A root a host's drain read ([`crate::drain`]): its name, the path the walk listed it at, and
/// what it holds.
#[derive(Debug)]
pub(crate) struct Listed {
    pub(crate) id: String,
    pub(crate) root: PathBuf,
    pub(crate) state: Arc<RootState>,
}

/// What narrowed a host's drain's walk ([`walk_for_drain`]): the first part of the disk it could
/// not read.
#[derive(Debug, thiserror::Error)]
pub(crate) enum WalkError {
    /// The host directory, or the deadline (`budget`).
    #[error(transparent)]
    Pass(#[from] PassError),
    #[error("the host directory's {name} {error}")]
    Record {
        name: String,
        #[source]
        error: RecordError,
    },
    #[error("the host directory holds {name}, which this line does not read")]
    Unknown { name: String },
    #[error(transparent)]
    Dir(DirError),
}

/// A host's drain's walk ([`crate::drain`], design: Hosts): the inspection's own walk and per-root
/// read, of the records of the host directory `host` (the compiled-in one checked strictly) and of
/// `scans`, until `at`. No lock check runs. Each root it lists goes to `listed`, as it is read.
/// Returns what narrowed the walk, the first part of the disk it could not read, if any: the host
/// directory, a name in it this line does not read, a directory, or the deadline (`budget`).
pub(crate) fn walk_for_drain(
    host: Option<&Path>,
    scans: &[PathBuf],
    at: Option<Instant>,
    budget: Duration,
    listed: &mut dyn FnMut(Listed),
) -> Option<WalkError> {
    let deadline = Deadline { at, budget };
    let mut unrecognized = Vec::new();
    let (wanted, listed_host) = wanted(host, scans, Strict::compiled_in(), &mut unrecognized);
    let error = listed_host.err();
    let mut dirs = Vec::new();
    let walked = walk(wanted, deadline, &mut dirs, |id, root| {
        if let RootRead::Listed { state, .. } = read_root(&root, None, deadline)? {
            listed(Listed { id, root, state });
        }
        Ok(())
    });
    let unlistable = dirs.into_iter().find_map(|d| match d.state {
        DirState::Unlistable(e) => Some(WalkError::Dir(e)),
        DirState::Present | DirState::Absent => None,
    });
    let unknown = unrecognized.into_iter().next().map(|u| match u.error {
        Some(error) => WalkError::Record {
            name: u.name,
            error,
        },
        None => WalkError::Unknown { name: u.name },
    });
    error
        .map(WalkError::Pass)
        .or(unknown)
        .or(unlistable)
        .or_else(|| {
            walked
                .err()
                .map(|Stopped| WalkError::Pass(PassError::Budget { budget }))
        })
}

/// What a host's drain's second read of a root found ([`read_for_drain`]).
#[derive(Debug)]
pub(crate) enum ReadAgain {
    Read(Arc<RootState>),
    /// The root holds no generation: no commit.
    NoGeneration,
    /// The deadline cut the read.
    Cut,
}

/// A host's drain's read of the root `root` again, by the walk's read, until `at`.
pub(crate) fn read_for_drain(root: &Path, at: Option<Instant>) -> ReadAgain {
    let deadline = Deadline {
        at,
        budget: Duration::MAX,
    };
    match read_root(root, None, deadline) {
        Ok(RootRead::Listed { state, .. }) => ReadAgain::Read(state),
        Ok(RootRead::NoGeneration { .. }) => ReadAgain::NoGeneration,
        Err(Stopped) => ReadAgain::Cut,
    }
}

impl Inspection {
    /// Whether the inspection read everything it must: the host directory and every record in it,
    /// every root, and the open files of every process, within its budget, and found nothing it
    /// does not know.
    pub fn complete(&self) -> bool {
        self.reason().is_none()
    }

    /// Whether it is complete and found no unacknowledged byte.
    pub fn clean(&self) -> bool {
        self.complete() && self.unacknowledged_bytes() == 0
    }

    /// The frame bytes of the commits logged and not acknowledged, over the roots it read.
    pub fn unacknowledged_bytes(&self) -> u64 {
        self.roots
            .iter()
            .filter_map(|r| match &*r.state {
                RootState::Read { bytes, .. } => Some(*bytes),
                RootState::Unreadable { .. } => None,
            })
            .fold(0, u64::saturating_add)
    }

    /// The host directory it read: the one the host named, or the compiled-in one.
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    /// What the pass listed in the host directory, `None` when it could not list it. A pass
    /// without it is incomplete, so a complete one always has it.
    pub(crate) fn listing(&self) -> Option<&HostListing> {
        self.listing.as_ref()
    }

    /// The records whose directory the pass found absent (`dirs[]` with `from: "record"` and
    /// `present: false`): nothing at the path, or something that is not a directory. Each comes
    /// with its record's name, in name order.
    pub(crate) fn absent_records(&self) -> impl Iterator<Item = (&str, &Path)> {
        self.dirs.iter().filter_map(|d| match (&d.from, &d.state) {
            (Source::Record { name }, DirState::Absent) => Some((name.as_str(), d.dir.as_path())),
            _ => None,
        })
    }

    /// The reason it is incomplete as JSON names it (`incomplete`), `None` when it is complete.
    pub(crate) fn incomplete(&self) -> Option<&'static str> {
        self.reason().map(Reason::as_str)
    }

    /// The message of the highest-precedence pass-level failure (`error`).
    pub(crate) fn error(&self) -> Option<String> {
        self.errors
            .iter()
            .min_by_key(|e| e.reason())
            .map(ToString::to_string)
    }

    /// The first reason, in precedence order, that the inspection is incomplete.
    fn reason(&self) -> Option<Reason> {
        let unreadable = self
            .roots
            .iter()
            .any(|r| matches!(*r.state, RootState::Unreadable { .. }))
            || self
                .dirs
                .iter()
                .any(|d| matches!(d.state, DirState::Unlistable(_)));
        let found = [
            self.listing.is_none().then_some(Reason::HostDir),
            (!self.unrecognized.is_empty()).then_some(Reason::Unrecognized),
            (!self.unread_locks.is_empty()).then_some(Reason::UnreadLock),
            unreadable.then_some(Reason::Unreadable),
        ];
        self.errors
            .iter()
            .map(PassError::reason)
            .chain(found.into_iter().flatten())
            .min()
    }

    /// Whether it is incomplete only for roots whose files changed while they were read: an
    /// `--until-clean` pass before the freeze reads them again, as the drain it waits for trims
    /// and rolls their logs. Any other reason, the budget included, ends the wait at once.
    fn settling(&self) -> bool {
        self.reason() == Some(Reason::Unreadable)
            && self.errors.is_empty()
            && self
                .dirs
                .iter()
                .all(|d| !matches!(d.state, DirState::Unlistable(_)))
            && self.roots.iter().all(|r| match &*r.state {
                RootState::Read { .. } => true,
                RootState::Unreadable { changed, .. } => *changed,
            })
    }

    /// The inspection as one line of JSON (design: Hosts). The format is frozen within a release
    /// line and may only grow.
    pub fn to_json(&self) -> String {
        let path = |p: &Path| json::str(&p.to_string_lossy());
        let dirs: Vec<String> = self.dirs.iter().map(|d| dir_json(d, &path)).collect();
        let roots: Vec<String> = self.roots.iter().map(|r| root_json(r, &path)).collect();
        let locks: Vec<String> = self
            .unread_locks
            .iter()
            .map(|l| json::object(&[("pid", l.pid.to_string()), ("path", path(&l.path))]))
            .collect();
        let unrecognized: Vec<String> = self
            .unrecognized
            .iter()
            .map(|u| {
                json::object(&[
                    ("name", json::str(&u.name)),
                    (
                        "error",
                        u.error
                            .as_ref()
                            .map_or_else(json::null, |e| json::str(&e.to_string())),
                    ),
                ])
            })
            .collect();
        let reason = self.reason();
        let error = self.error().map_or_else(json::null, |e| json::str(&e));
        json::object(&[
            ("v", VERSION.to_string()),
            ("dir", path(&self.dir)),
            ("complete", reason.is_none().to_string()),
            ("clean", self.clean().to_string()),
            (
                "incomplete",
                reason.map_or_else(json::null, |r| json::str(r.as_str())),
            ),
            (
                "unacknowledged_bytes",
                self.unacknowledged_bytes().to_string(),
            ),
            ("dirs", json::array(&dirs)),
            ("roots", json::array(&roots)),
            ("unread_locks", json::array(&locks)),
            ("unrecognized", json::array(&unrecognized)),
            ("error", error),
        ])
    }
}

fn dir_json(d: &DirReport, path: &dyn Fn(&Path) -> String) -> String {
    let from = match d.from {
        Source::Record { .. } => "record",
        Source::Scan => "scan",
    };
    let (present, error) = match &d.state {
        DirState::Present => (true, json::null()),
        DirState::Absent => (false, json::null()),
        DirState::Unlistable(e) => (true, json::str(&e.to_string())),
    };
    json::object(&[
        ("dir", path(&d.dir)),
        ("from", json::str(from)),
        ("present", present.to_string()),
        ("error", error),
    ])
}

fn root_json(r: &RootReport, path: &dyn Fn(&Path) -> String) -> String {
    let null = json::null;
    let (stream, cluster, state, bytes, commits, salvaged, error) = match &*r.state {
        RootState::Read {
            stream,
            cluster,
            bytes,
            commits,
            salvaged,
        } => (
            json::str(stream),
            json::str(cluster),
            if *bytes == 0 { "clean" } else { "pending" },
            bytes.to_string(),
            commits.to_string(),
            salvaged.to_string(),
            null(),
        ),
        RootState::Unreadable { error, .. } => (
            null(),
            null(),
            "unreadable",
            null(),
            null(),
            null(),
            json::str(&error.to_string()),
        ),
    };
    json::object(&[
        ("id", json::str(&r.id)),
        ("root", path(&r.root)),
        ("stream", stream),
        ("cluster", cluster),
        ("state", json::str(state)),
        ("held", r.held.map_or_else(null, |held| held.to_string())),
        ("unacknowledged_bytes", bytes),
        ("commits", commits),
        ("salvaged", salvaged),
        ("error", error),
    ])
}

/// The roots an earlier pass of one [`inspect`] read, by path, with what their files looked like.
#[derive(Debug, Default)]
struct Cache {
    roots: HashMap<PathBuf, (Fingerprint, RootRead)>,
}

/// What a root's files look like: every name in the root, in its `salvage/` and in the `log/` of
/// each of its generations, with its inode, size and modification time. A root whose fingerprint
/// is unchanged is not read again.
type Fingerprint = Vec<(PathBuf, u64, u64, i64, i64)>;

/// The pass's deadline, once its budget is spent.
#[derive(Debug, Clone, Copy)]
struct Deadline {
    at: Option<Instant>,
    budget: Duration,
}

impl Deadline {
    fn passed(self) -> bool {
        self.at.is_some_and(|at| Instant::now() >= at)
    }
}

/// The pass ran past its deadline.
#[derive(Debug)]
struct Stopped;

/// One pass of [`inspect`].
fn pass(request: &Inspect, strict: Strict<'_>, cache: &mut Cache) -> Inspection {
    let deadline = Deadline {
        at: request.budget.and_then(|b| Instant::now().checked_add(b)),
        budget: request.budget.unwrap_or(Duration::MAX),
    };
    let dir = request
        .dir
        .clone()
        .unwrap_or_else(|| strict.path.to_owned());
    let mut inspection = Inspection {
        dir: dir.clone(),
        listing: None,
        dirs: Vec::new(),
        roots: Vec::new(),
        unread_locks: Vec::new(),
        unrecognized: Vec::new(),
        errors: Vec::new(),
    };
    let (wanted, listed_host) = wanted(
        Some(&dir),
        &request.scans,
        strict,
        &mut inspection.unrecognized,
    );
    match listed_host {
        Ok(listing) => inspection.listing = listing,
        Err(e) => inspection.errors.push(e),
    }
    // Every root lock the pass read, with the listed roots it is the lock of: one lock may be
    // reached through two paths.
    let mut locks: HashMap<FileId, Vec<usize>> = HashMap::new();
    let mut fresh: HashMap<PathBuf, (Fingerprint, RootRead)> = HashMap::new();
    let roots = &mut inspection.roots;
    let walked = walk(wanted, deadline, &mut inspection.dirs, |id, root| {
        let print = fingerprint(&root, deadline)?;
        let read = match (&print, cache.roots.remove(&root)) {
            (Some(print), Some((before, read))) if *print == before => read,
            _ => read_root(&root, print.as_ref(), deadline)?,
        };
        if let Some(print) = print
            && !read.changed()
        {
            fresh.insert(root.clone(), (print, read.clone()));
        }
        match read {
            RootRead::NoGeneration { lock } => {
                if let Some(lock) = lock {
                    locks.entry(lock).or_default();
                }
            }
            RootRead::Listed { lock, state } => {
                if let Some(lock) = lock {
                    locks.entry(lock).or_default().push(roots.len());
                }
                roots.push(RootReport {
                    id,
                    root,
                    state,
                    held: None,
                });
            }
        }
        Ok(())
    });
    let mut stopped = walked.is_err();
    cache.roots = fresh;
    if !stopped {
        match open_locks(request.pids.as_deref(), deadline) {
            Ok(Some(open)) => {
                for r in &mut inspection.roots {
                    r.held = Some(false);
                }
                let mut unread = HashSet::new();
                for lock in open {
                    match locks.get(&lock.id) {
                        Some(listed) => {
                            for i in listed {
                                if let Some(r) = inspection.roots.get_mut(*i) {
                                    r.held = Some(true);
                                }
                            }
                        }
                        None => {
                            if unread.insert((lock.pid, lock.path.clone())) {
                                inspection.unread_locks.push(UnreadLock {
                                    pid: lock.pid,
                                    path: lock.path,
                                });
                            }
                        }
                    }
                }
            }
            Ok(None) => stopped = true,
            Err(e) => inspection.errors.push(e),
        }
    }
    if stopped {
        inspection.errors.push(PassError::Budget {
            budget: deadline.budget,
        });
    }
    inspection
}

/// The directories a pass reads for roots: those the records of the host directory `host` name,
/// in record name order, then `scans` in their order, with what it listed in the host directory
/// besides its records (`None` when no `host` is given). Names and records of the host directory
/// it does not know go to `unrecognized`, and a host directory that could not be read is the
/// error.
fn wanted(
    host: Option<&Path>,
    scans: &[PathBuf],
    strict: Strict<'_>,
    unrecognized: &mut Vec<Unrecognized>,
) -> (
    Vec<(PathBuf, Source)>,
    Result<Option<HostListing>, PassError>,
) {
    let mut wanted = Vec::new();
    let mut listed = Ok(None);
    if let Some(host) = host {
        match records(host, strict, unrecognized) {
            Ok(listing) => {
                wanted.extend(
                    listing
                        .recorded
                        .into_iter()
                        .map(|(name, d)| (d, Source::Record { name })),
                );
                listed = Ok(Some(listing.host));
            }
            Err(e) => listed = Err(e),
        }
    }
    wanted.extend(scans.iter().map(|d| (d.clone(), Source::Scan)));
    (wanted, listed)
}

/// The walk of a pass (steps 2 and 3 up to each root's read): every directory of `wanted` once per
/// real path, reported in `dirs`, and in each present one every root, by name, handed to `each`
/// with its name and path. [`Stopped`] once `deadline` passed, or once `each` stopped.
fn walk(
    wanted: Vec<(PathBuf, Source)>,
    deadline: Deadline,
    dirs: &mut Vec<DirReport>,
    mut each: impl FnMut(String, PathBuf) -> Result<(), Stopped>,
) -> Result<(), Stopped> {
    let mut seen = HashSet::new();
    for (path, from) in wanted {
        // One entry per real path, or per path when nothing is there.
        let (state, roots) = match locate(&path) {
            Located::Real(real) => {
                if !seen.insert(real.clone()) {
                    continue;
                }
                list_roots(&real, deadline)?
            }
            Located::Nothing => {
                if !seen.insert(path.clone()) {
                    continue;
                }
                (DirState::Absent, Vec::new())
            }
            Located::Unresolvable(source) => (
                DirState::Unlistable(DirError {
                    op: "resolve",
                    path: path.clone(),
                    source,
                }),
                Vec::new(),
            ),
        };
        dirs.push(DirReport {
            dir: path,
            from,
            state,
        });
        for (id, root) in roots {
            if deadline.passed() {
                return Err(Stopped);
            }
            each(id, root)?;
        }
    }
    Ok(())
}

/// Whether `e` says nothing is at a path: missing, or a component that is not a directory.
fn not_there(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
    )
}

/// Where a path of a directory to read leads.
#[derive(Debug)]
enum Located {
    /// Its real path.
    Real(PathBuf),
    /// Nothing is there.
    Nothing,
    /// It could not be resolved.
    Unresolvable(std::io::Error),
}

fn locate(path: &Path) -> Located {
    match fs::canonicalize(path) {
        Ok(real) => Located::Real(real),
        Err(e) if not_there(&e) => Located::Nothing,
        Err(e) => Located::Unresolvable(e),
    }
}

/// Whether a directory is at the real path `real`: false when nothing is there, or another file.
fn directory(real: &Path) -> std::io::Result<bool> {
    match fs::metadata(real) {
        Ok(meta) => Ok(meta.is_dir()),
        Err(e) if not_there(&e) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Whether a pass would find the directory at `path` absent (`present: false` with no error):
/// nothing there, or something that is not a directory.
pub(crate) fn absent(path: &Path) -> bool {
    match locate(path) {
        Located::Real(real) => matches!(directory(&real), Ok(false)),
        Located::Nothing => true,
        Located::Unresolvable(_) => false,
    }
}

/// What a pass listed in the host directory besides its records.
#[derive(Debug)]
pub(crate) struct HostListing {
    /// The host directory's identity (device and inode), which the listing and the records are
    /// of.
    pub(crate) id: FileId,
    /// Its temp files (`.tmp-<pid>-<16 hex>`) by name, in name order, which a pass otherwise
    /// ignores.
    pub(crate) tmps: Vec<String>,
}

/// What a pass listed in the host directory.
#[derive(Debug)]
struct Listing {
    host: HostListing,
    /// The directories its records name, with each record's name, in name order.
    recorded: Vec<(String, PathBuf)>,
}

/// Checks the host directory `dir` (design: Hosts) and reads its records: the directories they
/// name, in name order. Names it does not know, and records it does not read, go to
/// `unrecognized`.
fn records(
    dir: &Path,
    strict: Strict<'_>,
    unrecognized: &mut Vec<Unrecognized>,
) -> Result<Listing, PassError> {
    let checked = check_host_dir(dir, strict)?;
    let io = |op: &'static str, path: &Path| {
        let (dir, path) = (dir.to_owned(), path.to_owned());
        move |source| PassError::HostDirIo {
            dir,
            op,
            path,
            source,
        }
    };
    let handle = hostdir::open_dir(dir).map_err(io("open", dir))?;
    let opened = handle.metadata().map_err(io("stat", dir))?;
    let mut names = Vec::new();
    for entry in fs::read_dir(dir).map_err(io("list", dir))? {
        names.push(entry.map_err(io("list", dir))?.file_name());
    }
    // The listing, the records read through the handle and the checks are of one directory: the
    // one the path still names once it is listed.
    let listed = fs::symlink_metadata(dir).map_err(io("lstat", dir))?;
    let id = |m: &fs::Metadata| (m.dev(), m.ino());
    if id(&opened) != checked || id(&listed) != checked {
        return Err(PassError::HostDirMoved {
            dir: dir.to_owned(),
        });
    }
    names.sort();
    let mut recorded = Vec::new();
    let mut tmps = Vec::new();
    for name in names {
        let Some(text) = name.to_str() else {
            unrecognized.push(Unrecognized {
                name: name.to_string_lossy().into_owned(),
                error: None,
            });
            continue;
        };
        match hostdir::entry(text) {
            Entry::Tmp => tmps.push(text.to_owned()),
            Entry::Host => {}
            Entry::Unrecognized => unrecognized.push(Unrecognized {
                name: text.to_owned(),
                error: None,
            }),
            Entry::Record => {
                let read = match read_record(&handle, text) {
                    Ok(read) => read,
                    // A host deleted it since the listing.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(io("read", &dir.join(text))(e)),
                };
                match read {
                    Ok(cache) => recorded.push((text.to_owned(), cache)),
                    Err(error) => unrecognized.push(Unrecognized {
                        name: text.to_owned(),
                        error: Some(error),
                    }),
                }
            }
        }
    }
    Ok(Listing {
        host: HostListing { id: checked, tmps },
        recorded,
    })
}

/// Reads the record `name` through the open host directory `dir`, never through a link: the
/// directory it holds by [`record`], with a link read as [`Existing::NotAFile`]. Errors are the
/// read's own.
pub(crate) fn read_record(
    dir: &fs::File,
    name: &str,
) -> std::io::Result<Result<PathBuf, RecordError>> {
    let existing = match hostdir::read(dir, name) {
        // A link, which is never followed.
        Err(e) if e.raw_os_error() == Some(libc::ELOOP) => Existing::NotAFile,
        read => read?,
    };
    Ok(record(name, existing))
}

/// The directory the record `name` holds, `existing`: a record of this line (`v` 1) naming an
/// absolute path, under that path's own name.
fn record(name: &str, existing: Existing) -> Result<PathBuf, RecordError> {
    let bytes = match existing {
        Existing::File(bytes) => bytes,
        Existing::NotAFile => return Err(RecordError::NotAFile),
    };
    let record = hostdir::parse_record(&bytes)?;
    if record.version != Some(hostdir::VERSION) {
        return Err(RecordError::Version {
            found: record.version,
        });
    }
    if !Path::new(&record.dir).is_absolute() {
        return Err(RecordError::NotAbsolute { dir: record.dir });
    }
    if hostdir::record_name(&record.dir) != name {
        return Err(RecordError::Misnamed { dir: record.dir });
    }
    Ok(PathBuf::from(record.dir))
}

/// The mode of the compiled-in host directory.
const HOST_DIR_MODE: u32 = 0o1777;
/// The mode of the compiled-in host directory's parent.
const PARENT_MODE: u32 = 0o755;

/// Checks that the host directory `dir` is as it must be (design: Hosts), and returns its
/// identity. `dir` must be a directory and not a link. When it is the directory `strict` names
/// (the same device and inode, however `dir` spells it), that directory, its parent and every
/// ancestor must also be as the strict checks want, along the path `strict` spells.
fn check_host_dir(dir: &Path, strict: Strict<'_>) -> Result<FileId, PassError> {
    let shape = |path: &Path, why| PassError::HostDirShape {
        dir: dir.to_owned(),
        path: path.to_owned(),
        why,
    };
    let lstat = |path: &Path| -> Result<fs::Metadata, PassError> {
        let meta = fs::symlink_metadata(path).map_err(|source| PassError::HostDirIo {
            dir: dir.to_owned(),
            op: "lstat",
            path: path.to_owned(),
            source,
        })?;
        if meta.file_type().is_dir() {
            Ok(meta)
        } else {
            Err(shape(path, Shape::NotADirectory))
        }
    };
    let owned = |path: &Path, meta: &fs::Metadata, want: u32| {
        if meta.uid() != strict.uid {
            return Err(shape(path, Shape::Owner {
                uid: meta.uid(),
                want: strict.uid,
            }));
        }
        let mode = meta.mode() & 0o7777;
        if mode != want {
            return Err(shape(path, Shape::Mode { mode, want }));
        }
        Ok(())
    };
    let meta = lstat(dir)?;
    let id = (meta.dev(), meta.ino());
    let at_strict = match fs::symlink_metadata(strict.path) {
        Ok(m) => (m.dev(), m.ino()) == id,
        Err(e) if not_there(&e) => false,
        Err(source) => {
            return Err(PassError::HostDirIo {
                dir: dir.to_owned(),
                op: "lstat",
                path: strict.path.to_owned(),
                source,
            });
        }
    };
    if !at_strict {
        return Ok(id);
    }
    owned(strict.path, &meta, HOST_DIR_MODE)?;
    let mut ancestors = strict.path.ancestors().skip(1);
    if let Some(parent) = ancestors.next() {
        let meta = lstat(parent)?;
        owned(parent, &meta, PARENT_MODE)?;
    }
    for ancestor in ancestors {
        lstat(ancestor)?;
    }
    Ok(id)
}

/// Whether `e`, from a `stat` of a directory's entry, says the entry is no root: nothing there,
/// or a link that dangles or loops.
fn no_root(e: &std::io::Error) -> bool {
    not_there(e) || e.raw_os_error() == Some(libc::ELOOP)
}

/// The roots in the directory whose real path is `dir`: its subdirectories with a root's name,
/// by name, or [`Stopped`] when the pass ran past `deadline` while listing it.
fn list_roots(
    dir: &Path,
    deadline: Deadline,
) -> Result<(DirState, Vec<(String, PathBuf)>), Stopped> {
    let unlistable = |op, path: &Path, source| {
        Ok((
            DirState::Unlistable(DirError {
                op,
                path: path.to_owned(),
                source,
            }),
            Vec::new(),
        ))
    };
    match directory(dir) {
        Ok(true) => {}
        Ok(false) => return Ok((DirState::Absent, Vec::new())),
        Err(e) => return unlistable("stat", dir, e),
    }
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if not_there(&e) => return Ok((DirState::Absent, Vec::new())),
        Err(e) => return unlistable("list", dir, e),
    };
    let mut roots = Vec::new();
    for entry in entries {
        if deadline.passed() {
            return Err(Stopped);
        }
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) => return unlistable("list", dir, e),
        };
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        // Another entry belongs to the application.
        if !generation::is_root_name(&name) {
            continue;
        }
        // A root may be reached through a link, as the open's path does. An entry with a root's
        // name that cannot be looked at may be a root: its commits are not read.
        let path = entry.path();
        match fs::metadata(&path) {
            Ok(meta) if meta.is_dir() => roots.push((name, path)),
            Ok(_) => {}
            Err(e) if no_root(&e) => {}
            Err(e) => return unlistable("stat", &path, e),
        }
    }
    roots.sort();
    Ok((DirState::Present, roots))
}

/// What the root `root` holds, `before` its fingerprint ahead of the read, or [`Stopped`] when
/// the pass ran past `deadline` while reading it.
fn read_root(
    root: &Path,
    before: Option<&Fingerprint>,
    deadline: Deadline,
) -> Result<RootRead, Stopped> {
    let lock_file = root.join(generation::LOCK);
    let (lock, found) = match fs::metadata(&lock_file) {
        Ok(meta) => (Some((meta.dev(), meta.ino())), pending(root, deadline)),
        Err(e) if not_there(&e) => (None, pending(root, deadline)),
        Err(source) => (
            None,
            Err(RootError::Lock {
                path: lock_file,
                source,
            }),
        ),
    };
    let state = match found {
        Ok(None) => return Ok(RootRead::NoGeneration { lock }),
        Ok(Some(state)) => state,
        Err(RootError::Log(LogError::Stopped { .. })) => return Err(Stopped),
        Err(error) => {
            // A fingerprint that cannot be taken before the read nor after it is no change: the
            // root stays unreadable, and a wait for it ends at once.
            let changed = match (before, fingerprint(root, deadline)?) {
                (Some(before), Some(after)) => *before != after,
                (Some(_), None) | (None, Some(_)) => true,
                (None, None) => false,
            };
            RootState::Unreadable {
                error: Arc::new(error),
                changed,
            }
        }
    };
    Ok(RootRead::Listed {
        lock,
        state: Arc::new(state),
    })
}

/// The pending commits of the root `root`'s current generation, `None` for a root with no
/// generation.
fn pending(root: &Path, deadline: Deadline) -> Result<Option<RootState>, RootError> {
    let classified = Root::classify(root)?;
    let superseded = classified.superseded_dirs();
    let generation = match classified.current()? {
        None => return Ok(None),
        Some(Ok(generation)) => generation,
        Some(Err(unreadable)) => return Err(RootError::Generation(unreadable.error)),
    };
    let log = generation.log_dir();
    let mut fold = Unacknowledged::until(deadline.at);
    Log::read(&log, &mut fold)?;
    if fold.pending().is_none()
        && let Some(acked) = fold.state().map(|s| s.acked)
    {
        // An ACK went below an earlier one: read again, keeping every commit above the latest.
        fold = Unacknowledged::above(acked, deadline.at);
        Log::read(&log, &mut fold)?;
    }
    let (Some(state), Some(commits)) = (fold.state(), fold.pending()) else {
        return Err(RootError::Changed { dir: log });
    };
    let bytes = commits
        .iter()
        .fold(0u64, |sum, (_, len)| sum.saturating_add(*len));
    let unacknowledged = |m: &salvage::Manifest| m.acknowledged.map_or(0, |a| a.unacknowledged);
    let mut salvaged = salvage::list(root)?
        .iter()
        .map(|s| unacknowledged(&s.manifest))
        .fold(0u64, u64::saturating_add);
    // A superseded generation with a manifest is a salvage the next open moves into `salvage/`
    // ([`Root::finish_leftovers`]), as a crash between the publish and the move leaves it.
    for dir in superseded {
        if let Some(manifest) = salvage::read_manifest(&dir)? {
            salvaged = salvaged.saturating_add(unacknowledged(&manifest));
        }
    }
    Ok(Some(RootState::Read {
        stream: state.stream_path.clone(),
        cluster: state.cluster_id.clone(),
        bytes,
        commits: u64::try_from(commits.len()).unwrap_or(u64::MAX),
        salvaged,
    }))
}

/// The root `root`'s fingerprint, `None` when a part of it cannot be listed (it is read again),
/// or [`Stopped`] when the pass ran past `deadline` while taking it.
fn fingerprint(root: &Path, deadline: Deadline) -> Result<Option<Fingerprint>, Stopped> {
    let mut print = Vec::new();
    let Some(dirs) = add_names(root, deadline, &mut print)? else {
        return Ok(None);
    };
    for dir in dirs {
        let name = dir.file_name().and_then(|n| n.to_str()).unwrap_or_default();
        let (salvages, logged) = (name == salvage::DIR, name.starts_with("gen-"));
        let listed = if salvages {
            Some(dir)
        } else if logged {
            Some(dir.join("log")).filter(|log| log.exists())
        } else {
            None
        };
        if let Some(listed) = listed
            && add_names(&listed, deadline, &mut print)?.is_none()
        {
            return Ok(None);
        }
    }
    print.sort();
    Ok(Some(print))
}

/// Adds every name in `dir` to `print`, with its inode, size and modification time, and returns
/// its subdirectories: `None` when it cannot be listed, [`Stopped`] past `deadline`.
fn add_names(
    dir: &Path,
    deadline: Deadline,
    print: &mut Fingerprint,
) -> Result<Option<Vec<PathBuf>>, Stopped> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(None);
    };
    let mut dirs = Vec::new();
    for entry in entries {
        if deadline.passed() {
            return Err(Stopped);
        }
        let Ok(entry) = entry else {
            return Ok(None);
        };
        let path = entry.path();
        let Ok(meta) = fs::symlink_metadata(&path) else {
            return Ok(None);
        };
        if meta.is_dir() {
            dirs.push(path.clone());
        }
        print.push((
            path,
            meta.ino(),
            meta.len(),
            meta.mtime(),
            meta.mtime_nsec(),
        ));
    }
    Ok(Some(dirs))
}

/// An open file of a live process that may be a root's lock.
#[derive(Debug)]
struct OpenLock {
    pid: u32,
    /// The path the process opened it by, as its `/proc` entry names it.
    path: PathBuf,
    id: FileId,
}

/// The `/proc` directory of the processes.
const PROC: &str = "/proc";

/// A process-table failure: `op` on `path`.
fn table(op: &'static str, path: &Path) -> impl FnOnce(std::io::Error) -> PassError {
    let path = path.to_owned();
    move |source| PassError::ProcessTable { op, path, source }
}

/// Every open file of a live process among `pids` (every process when `None`) that may be a
/// root's lock ([`lock_path`]), or `None` when the pass ran past `deadline`. A process that exits
/// meanwhile is skipped. A process table, or a process's descriptors, that cannot be read fail,
/// and so do a live process `/proc` hides (`hidepid`) and the check of every process when this
/// one is not root's.
///
/// The descriptors of a process are those its first thread lists, or those of each of its
/// threads once the first exited. A thread that unshared its descriptor table
/// (`unshare(CLONE_FILES)`) while the first thread runs is not read.
fn open_locks(
    pids: Option<&[u32]>,
    deadline: Deadline,
) -> Result<Option<Vec<OpenLock>>, PassError> {
    let proc = Path::new(PROC);
    let pids: Vec<u32> = match pids {
        Some(pids) => pids.to_vec(),
        None => {
            // Another user sees some processes as absent (`hidepid`) and cannot read the open
            // files of others.
            let euid = effective_uid()?;
            if euid != ROOT_UID {
                return Err(PassError::NotRoot { euid });
            }
            let mut all = Vec::new();
            for entry in fs::read_dir(proc).map_err(table("list", proc))? {
                let entry = entry.map_err(table("list", proc))?;
                if let Some(pid) = entry.file_name().to_str().and_then(|n| n.parse().ok()) {
                    all.push(pid);
                }
            }
            all
        }
    };
    if !pids.is_empty() {
        // Without a `/proc` this process can list its own descriptors in (none mounted, or not
        // Linux), every process would look exited.
        let own = proc.join("self").join("fd");
        fs::read_dir(&own).map_err(table("list", &own))?;
    }
    let mut locks = Vec::new();
    for pid in pids {
        if deadline.passed() {
            return Ok(None);
        }
        let process = proc.join(pid.to_string());
        let listed = match descriptors(&process, pid, deadline, &mut locks)? {
            Fds::Listed(listed) => listed,
            Fds::Stopped => return Ok(None),
            // Hidden from this process (`hidepid`): its open files are not read.
            Fds::Gone if alive(pid) => return Err(PassError::Hidden { pid }),
            // Exited, or never there.
            Fds::Gone => continue,
        };
        if listed > 0 {
            continue;
        }
        // A process whose first thread exited lists no descriptor, while its other threads
        // still hold them.
        let tasks = process.join("task");
        let entries = match fs::read_dir(&tasks) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(table("list", &tasks)(e)),
        };
        for entry in entries {
            let task = match entry {
                Ok(entry) => entry.path(),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
                Err(e) => return Err(table("list", &tasks)(e)),
            };
            if matches!(descriptors(&task, pid, deadline, &mut locks)?, Fds::Stopped) {
                return Ok(None);
            }
        }
    }
    Ok(Some(locks))
}

/// What [`descriptors`] listed of a process or a thread.
#[derive(Debug, Clone, Copy)]
enum Fds {
    /// Its `/proc` directory is not there: it exited, or `/proc` hides it.
    Gone,
    /// It lists this many descriptors.
    Listed(usize),
    /// The pass ran past its deadline.
    Stopped,
}

/// Adds every open file of the process `pid`, or of one of its threads, whose `/proc` directory
/// is `dir` and that may be a root's lock ([`lock_path`]) to `locks`.
fn descriptors(
    dir: &Path,
    pid: u32,
    deadline: Deadline,
    locks: &mut Vec<OpenLock>,
) -> Result<Fds, PassError> {
    let fds = dir.join("fd");
    let entries = match fs::read_dir(&fds) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Fds::Gone),
        Err(e) => return Err(table("list", &fds)(e)),
    };
    let mut listed = 0usize;
    for entry in entries {
        if deadline.passed() {
            return Ok(Fds::Stopped);
        }
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
            Err(e) => return Err(table("list", &fds)(e)),
        };
        listed = listed.saturating_add(1);
        let fd = entry.path();
        let target = match fs::read_link(&fd) {
            Ok(target) => target,
            // Closed since the listing, or the process exited.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(table("readlink", &fd)(e)),
        };
        let candidate = match lock_path(target.as_os_str().as_bytes()) {
            LockPath::No => false,
            LockPath::Root => true,
            LockPath::Unnamed => {
                let info = dir.join("fdinfo").join(entry.file_name());
                match fs::read_to_string(&info) {
                    Ok(text) => holds_write_lock(&text),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
                    Err(e) => return Err(table("read", &info)(e)),
                }
            }
        };
        if !candidate {
            continue;
        }
        match fs::metadata(&fd) {
            Ok(meta) => locks.push(OpenLock {
                pid,
                path: target,
                id: (meta.dev(), meta.ino()),
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(table("stat", &fd)(e)),
        }
    }
    Ok(Fds::Listed(listed))
}

/// `pid` as a process id `kill` can probe. `None` for 0, which names the caller's process group,
/// and for an id above `pid_t`'s maximum, which no process has.
pub(crate) fn process_id(pid: u32) -> Option<libc::pid_t> {
    libc::pid_t::try_from(pid).ok().filter(|pid| *pid > 0)
}

/// Whether the process `pid` is alive, as `kill(pid, 0)` tells: it is unless no process has that
/// id (`ESRCH`). A process another user owns is alive (`EPERM`). An id `kill` cannot probe
/// ([`process_id`]) is no process.
#[expect(
    unsafe_code,
    reason = "probing a process with signal 0 has no safe std API"
)]
pub(crate) fn alive(pid: u32) -> bool {
    let Some(pid) = process_id(pid) else {
        return false;
    };
    // SAFETY: `kill` with signal 0 sends nothing and touches no memory of ours.
    let probed = unsafe { libc::kill(pid, 0) };
    probed == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

/// What the path a `/proc/<pid>/fd` entry names says of the file being a root's lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockPath {
    /// Not a file named `lock`, or the lock of a reader directory: a reader's name (`reader-<n>`,
    /// or `reader-trash-<32 hex>` while a cleanup deletes it) in a directory with a root's name,
    /// as a reader directory sits in its root. Readers hold no commit.
    No,
    /// `<label>-<32 hex>/lock`, with or without ` (deleted)`: a root's lock by its name. A root's
    /// name may also be a reader's (`reader-trash-<32 hex>` is the root of a stream whose label
    /// is `reader-trash`), so it is a root's unless its directory has a root's name.
    Root,
    /// Another file named `lock`: a root's when the process holds an exclusive lock on it
    /// ([`holds_write_lock`]), since a root's directory may have been renamed.
    Unnamed,
}

/// What `target`, the path a `/proc/<pid>/fd` entry names, says of the file being a root's lock.
fn lock_path(target: &[u8]) -> LockPath {
    let target = target.strip_suffix(DELETED).unwrap_or(target);
    let Some(dir) = target
        .strip_suffix(generation::LOCK.as_bytes())
        .and_then(|t| t.strip_suffix(b"/"))
    else {
        return LockPath::No;
    };
    let mut names = dir.rsplit(|&b| b == b'/');
    let name = names.next().unwrap_or_default();
    let parent = names.next().unwrap_or_default();
    let has =
        |bytes: &[u8], grammar: fn(&str) -> bool| std::str::from_utf8(bytes).is_ok_and(grammar);
    if has(name, generation::is_reader_name) && has(parent, generation::is_root_name) {
        LockPath::No
    } else if has(name, generation::is_root_name) {
        LockPath::Root
    } else {
        LockPath::Unnamed
    }
}

/// Whether `fdinfo`, a `/proc/<pid>/fdinfo/<n>`, shows an exclusive `flock` held through that
/// open file (a `lock:` line of an `FLOCK` for writing), as every release since 0.8 holds a
/// root's lock. A record lock (`fcntl`), as dpkg holds `/var/lib/dpkg/lock`, is not one.
fn holds_write_lock(fdinfo: &str) -> bool {
    fdinfo
        .lines()
        .filter_map(|line| line.strip_prefix("lock:"))
        .any(|lock| {
            // `<n>: <kind> <mode> <access> <pid> <dev>:<inode> <start> <end>`
            let mut words = lock.split_whitespace().skip(1);
            matches!(
                (words.next(), words.next(), words.next()),
                (Some("FLOCK"), Some(_), Some("WRITE"))
            )
        })
}

/// This process's effective uid, from `/proc/self/status`.
pub(crate) fn effective_uid() -> Result<u32, PassError> {
    let path = Path::new(PROC).join("self").join("status");
    let text = fs::read_to_string(&path).map_err(table("read", &path))?;
    text.lines()
        .find_map(|line| line.strip_prefix("Uid:"))
        .and_then(|ids| ids.split_whitespace().nth(1))
        .and_then(|euid| euid.parse().ok())
        .ok_or(PassError::NoUid { path })
}

#[cfg(test)]
mod tests;
