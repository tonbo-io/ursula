//! A host's read of every root and every open root lock (design: Hosts): whether the disk holds a
//! commit the stream has not acknowledged, for a host that snapshots or moves the process, or the
//! VM it runs in. [`inspect`] is the one entry point, and `ursula-vfs-host inspect` prints its
//! [`Inspection`] as one line of JSON.
//!
//! A pass reads, and never writes or locks anything:
//!
//! 1. The host directory. It must be a directory, not a link. At the compiled-in path
//!    (`hostdir::DEFAULT`) it must be root's, mode 1777, in a root-owned directory of mode 0755,
//!    with every ancestor a directory and no link among them. Every name in it must be one the
//!    host directory's grammar knows (`hostdir::entry`), and every record a record of this line
//!    (`v` 1) naming an absolute path under its own key.
//! 2. The directories: the recorded ones and those the host names (`scans`), one per real path. A
//!    missing path, or one that is not a directory, holds no roots. In a present one every
//!    subdirectory with a root's name (`generation::is_root_name`) is a root. A root with no
//!    `gen-<id32>` directory holds no commit and is not listed.
//! 3. Each root: its current generation by the open's own rule (`Root::classify`), and that
//!    generation's log by the open's reading rule (`Log::read`) with a fold that keeps a position
//!    and a length per pending commit (`Unacknowledged`). Its bytes are what
//!    `Recovered::pending` holds: the frame lengths of the generation's own COMMIT records above
//!    the latest ACK. The commits its salvages list as unacknowledged are reported beside them,
//!    outside the bytes.
//! 4. The lock check: every open file of a live process (`/proc/<pid>/fd`) that is a root's
//!    `lock` must be the lock of a root this pass read, whatever path the process opened it by.
//!    Any other is an unread lock.
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
    /// The host directory: the extension's own (`hostdir::host_dir`) when `None`.
    pub dir: Option<PathBuf>,
    /// Directories to read besides the recorded ones, such as the default cache roots.
    pub scans: Vec<PathBuf>,
    /// The processes whose open files the lock check reads: every process when `None`.
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
enum PassError {
    #[error(
        "no host directory: {} does not exist and none was given",
        hostdir::DEFAULT
    )]
    NoHostDir,
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
    #[error("the process table: {op} {}: {source}", .path.display())]
    ProcessTable {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("the pass ran past its budget of {} ms", .budget.as_millis())]
    Budget { budget: Duration },
}

impl PassError {
    fn reason(&self) -> Reason {
        match self {
            PassError::NoHostDir | PassError::HostDirShape { .. } | PassError::HostDirIo { .. } => {
                Reason::HostDir
            }
            PassError::ProcessTable { .. } => Reason::UnreadLock,
            PassError::Budget { .. } => Reason::Budget,
        }
    }
}

/// How a path of the host directory is not what it must be.
#[derive(Debug, thiserror::Error)]
enum Shape {
    #[error("is not a directory")]
    NotADirectory,
    #[error("is owned by uid {uid}, not root")]
    Owner { uid: u32 },
    #[error("has mode {mode:o}, not {want:o}")]
    Mode { mode: u32, want: u32 },
}

/// Why a name in the host directory is not a record this line reads.
#[derive(Debug, thiserror::Error)]
enum RecordError {
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
enum RootError {
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    Record,
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
    Unlistable(std::io::Error),
}

/// A file's identity: its device and inode.
type FileId = (u64, u64);

/// What a pass found in one root.
#[derive(Debug)]
struct RootRead {
    /// Its `lock`, if it has one.
    lock: Option<FileId>,
    /// `None` for a root with no generation, which holds no commit and is not listed.
    state: Option<RootState>,
}

#[derive(Debug)]
enum RootState {
    Read {
        stream: String,
        cluster: String,
        /// The frame bytes of its pending commits.
        bytes: u64,
        commits: u64,
        /// The commits its salvages list as unacknowledged.
        salvaged: u64,
    },
    Unreadable(RootError),
}

/// A root a pass lists.
#[derive(Debug)]
struct RootReport {
    id: String,
    root: PathBuf,
    read: Arc<RootRead>,
    held: bool,
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
    dir: Option<PathBuf>,
    dirs: Vec<DirReport>,
    roots: Vec<RootReport>,
    unread_locks: Vec<UnreadLock>,
    unrecognized: Vec<Unrecognized>,
    errors: Vec<PassError>,
}

/// Reads every root of the host directory's records and of `request.scans`, and checks every open
/// root lock (design: Hosts). Never writes or locks anything.
pub fn inspect(request: &Inspect) -> Inspection {
    let mut cache = Cache::default();
    let until = request
        .until_clean
        .and_then(|wait| Instant::now().checked_add(wait));
    loop {
        let seen = pass(request, &mut cache);
        let Some(until) = until else {
            return seen;
        };
        let now = Instant::now();
        if !seen.complete() || seen.clean() || now >= until {
            return seen;
        }
        std::thread::sleep(AGAIN.min(until.saturating_duration_since(now)));
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
            .filter_map(|r| match &r.read.state {
                Some(RootState::Read { bytes, .. }) => Some(*bytes),
                _ => None,
            })
            .fold(0, u64::saturating_add)
    }

    /// The first reason, in precedence order, that the inspection is incomplete.
    fn reason(&self) -> Option<Reason> {
        let unreadable = self
            .roots
            .iter()
            .any(|r| matches!(r.read.state, Some(RootState::Unreadable(_))))
            || self
                .dirs
                .iter()
                .any(|d| matches!(d.state, DirState::Unlistable(_)));
        let found = [
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
        let error = self
            .errors
            .iter()
            .min_by_key(|e| e.reason())
            .map_or_else(json::null, |e| json::str(&e.to_string()));
        json::object(&[
            ("v", VERSION.to_string()),
            ("dir", self.dir.as_deref().map_or_else(json::null, path)),
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
        Source::Record => "record",
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
    let (stream, cluster, state, bytes, commits, salvaged, error) = match &r.read.state {
        Some(RootState::Read {
            stream,
            cluster,
            bytes,
            commits,
            salvaged,
        }) => (
            json::str(stream),
            json::str(cluster),
            if *bytes == 0 { "clean" } else { "pending" },
            bytes.to_string(),
            commits.to_string(),
            salvaged.to_string(),
            null(),
        ),
        Some(RootState::Unreadable(e)) => (
            null(),
            null(),
            "unreadable",
            null(),
            null(),
            null(),
            json::str(&e.to_string()),
        ),
        // Never listed.
        None => (
            null(),
            null(),
            "clean",
            "0".into(),
            "0".into(),
            "0".into(),
            null(),
        ),
    };
    json::object(&[
        ("id", json::str(&r.id)),
        ("root", path(&r.root)),
        ("stream", stream),
        ("cluster", cluster),
        ("state", json::str(state)),
        ("held", r.held.to_string()),
        ("unacknowledged_bytes", bytes),
        ("commits", commits),
        ("salvaged", salvaged),
        ("error", error),
    ])
}

/// The roots an earlier pass of one [`inspect`] read, by path, with what their files looked like.
#[derive(Debug, Default)]
struct Cache {
    roots: HashMap<PathBuf, (Fingerprint, Arc<RootRead>)>,
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

/// One pass of [`inspect`].
fn pass(request: &Inspect, cache: &mut Cache) -> Inspection {
    let budget = request.budget.unwrap_or(Duration::MAX);
    let deadline = Deadline {
        at: request.budget.and_then(|b| Instant::now().checked_add(b)),
        budget,
    };
    let dir = request.dir.clone().or_else(hostdir::host_dir);
    let mut inspection = Inspection {
        dir: dir.clone(),
        dirs: Vec::new(),
        roots: Vec::new(),
        unread_locks: Vec::new(),
        unrecognized: Vec::new(),
        errors: Vec::new(),
    };
    let mut wanted = Vec::new();
    match &dir {
        Some(dir) => match records(dir, &mut inspection.unrecognized) {
            Ok(recorded) => wanted.extend(recorded.into_iter().map(|d| (d, Source::Record))),
            Err(e) => inspection.errors.push(e),
        },
        None => inspection.errors.push(PassError::NoHostDir),
    }
    wanted.extend(request.scans.iter().map(|d| (d.clone(), Source::Scan)));
    let mut locks: HashMap<FileId, Option<usize>> = HashMap::new();
    let mut fresh: HashMap<PathBuf, (Fingerprint, Arc<RootRead>)> = HashMap::new();
    let mut seen = HashSet::new();
    let mut stopped = false;
    'dirs: for (path, from) in wanted {
        // One entry per real path, or per path when nothing is there.
        let (state, roots) = match fs::canonicalize(&path) {
            Ok(real) => {
                if !seen.insert(real.clone()) {
                    continue;
                }
                list_roots(&real)
            }
            Err(e) if not_there(&e) => {
                if !seen.insert(path.clone()) {
                    continue;
                }
                (DirState::Absent, Vec::new())
            }
            Err(e) => (DirState::Unlistable(e), Vec::new()),
        };
        inspection.dirs.push(DirReport {
            dir: path,
            from,
            state,
        });
        for (id, root) in roots {
            if deadline.passed() {
                stopped = true;
                break 'dirs;
            }
            let print = fingerprint(&root);
            let read = match (&print, cache.roots.remove(&root)) {
                (Some(print), Some((before, read))) if *print == before => read,
                _ => match read_root(&root, deadline) {
                    Some(read) => Arc::new(read),
                    None => {
                        stopped = true;
                        break 'dirs;
                    }
                },
            };
            if let Some(print) = print {
                fresh.insert(root.clone(), (print, Arc::clone(&read)));
            }
            let listed = read.state.is_some().then_some(inspection.roots.len());
            if let Some(lock) = read.lock {
                locks.insert(lock, listed);
            }
            if listed.is_some() {
                inspection.roots.push(RootReport {
                    id,
                    root,
                    read,
                    held: false,
                });
            }
        }
    }
    cache.roots = fresh;
    if !stopped {
        match open_locks(request.pids.as_deref(), deadline) {
            Ok(Some(open)) => {
                let mut unread = HashSet::new();
                for lock in open {
                    match locks.get(&lock.id) {
                        Some(Some(i)) => {
                            if let Some(r) = inspection.roots.get_mut(*i) {
                                r.held = true;
                            }
                        }
                        Some(None) => {}
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

/// Whether `e` says nothing is at a path: missing, or a component that is not a directory.
fn not_there(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
    )
}

/// Checks the host directory `dir` (design: Hosts) and reads its records: the directories they
/// name, in name order. Names it does not know, and records it does not read, go to
/// `unrecognized`.
fn records(dir: &Path, unrecognized: &mut Vec<Unrecognized>) -> Result<Vec<PathBuf>, PassError> {
    check_host_dir(dir)?;
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
    let mut names = Vec::new();
    for entry in fs::read_dir(dir).map_err(io("list", dir))? {
        names.push(entry.map_err(io("list", dir))?.file_name());
    }
    names.sort();
    let mut recorded = Vec::new();
    for name in names {
        let Some(text) = name.to_str() else {
            unrecognized.push(Unrecognized {
                name: name.to_string_lossy().into_owned(),
                error: None,
            });
            continue;
        };
        match hostdir::entry(text) {
            Entry::Tmp | Entry::Host => {}
            Entry::Unrecognized => unrecognized.push(Unrecognized {
                name: text.to_owned(),
                error: None,
            }),
            Entry::Record => {
                let existing = match hostdir::read(&handle, text) {
                    Ok(existing) => existing,
                    // A host deleted it since the listing.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    // A link, which is never followed.
                    Err(e) if e.raw_os_error() == Some(libc::ELOOP) => Existing::NotAFile,
                    Err(e) => return Err(io("read", &dir.join(text))(e)),
                };
                match record(text, existing) {
                    Ok(cache) => recorded.push(cache),
                    Err(error) => unrecognized.push(Unrecognized {
                        name: text.to_owned(),
                        error: Some(error),
                    }),
                }
            }
        }
    }
    Ok(recorded)
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

/// Root's uid.
const ROOT_UID: u32 = 0;
/// The mode of the compiled-in host directory.
const HOST_DIR_MODE: u32 = 0o1777;
/// The mode of the compiled-in host directory's parent.
const PARENT_MODE: u32 = 0o755;

/// Checks that the host directory `dir` is as it must be (design: Hosts). Another directory than
/// the compiled-in one, such as a test's, must only be a directory and not a link.
fn check_host_dir(dir: &Path) -> Result<(), PassError> {
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
        if meta.uid() != ROOT_UID {
            return Err(shape(path, Shape::Owner { uid: meta.uid() }));
        }
        let mode = meta.mode() & 0o7777;
        if mode != want {
            return Err(shape(path, Shape::Mode { mode, want }));
        }
        Ok(())
    };
    let meta = lstat(dir)?;
    if dir != Path::new(hostdir::DEFAULT) {
        return Ok(());
    }
    owned(dir, &meta, HOST_DIR_MODE)?;
    let mut ancestors = dir.ancestors().skip(1);
    if let Some(parent) = ancestors.next() {
        let meta = lstat(parent)?;
        owned(parent, &meta, PARENT_MODE)?;
    }
    for ancestor in ancestors {
        lstat(ancestor)?;
    }
    Ok(())
}

/// The roots in the directory whose real path is `dir`: its subdirectories with a root's name,
/// by name.
fn list_roots(dir: &Path) -> (DirState, Vec<(String, PathBuf)>) {
    match fs::metadata(dir) {
        Ok(meta) if meta.is_dir() => {}
        Ok(_) => return (DirState::Absent, Vec::new()),
        Err(e) if not_there(&e) => return (DirState::Absent, Vec::new()),
        Err(e) => return (DirState::Unlistable(e), Vec::new()),
    }
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if not_there(&e) => return (DirState::Absent, Vec::new()),
        Err(e) => return (DirState::Unlistable(e), Vec::new()),
    };
    let mut roots = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) => return (DirState::Unlistable(e), Vec::new()),
        };
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        // Another entry belongs to the application. A root may be reached through a link, as
        // the open's path does.
        if generation::is_root_name(&name) && fs::metadata(entry.path()).is_ok_and(|m| m.is_dir()) {
            roots.push((name, entry.path()));
        }
    }
    roots.sort();
    (DirState::Present, roots)
}

/// What the root `root` holds, or `None` when the pass ran past `deadline` while reading it.
fn read_root(root: &Path, deadline: Deadline) -> Option<RootRead> {
    let lock_path = root.join(generation::LOCK);
    let lock = match fs::metadata(&lock_path) {
        Ok(meta) => Some((meta.dev(), meta.ino())),
        Err(e) if not_there(&e) => None,
        Err(source) => {
            return Some(RootRead {
                lock: None,
                state: Some(RootState::Unreadable(RootError::Lock {
                    path: lock_path,
                    source,
                })),
            });
        }
    };
    let state = match pending(root, deadline) {
        Ok(state) => state,
        Err(RootError::Log(LogError::Stopped { .. })) => return None,
        Err(e) => Some(RootState::Unreadable(e)),
    };
    Some(RootRead { lock, state })
}

/// The pending commits of the root `root`'s current generation, `None` for a root with no
/// generation.
fn pending(root: &Path, deadline: Deadline) -> Result<Option<RootState>, RootError> {
    let generation = match Root::classify(root)?.current()? {
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
    let salvaged = salvage::list(root)?
        .iter()
        .filter_map(|s| s.manifest.acknowledged.map(|a| a.unacknowledged))
        .fold(0u64, u64::saturating_add);
    Ok(Some(RootState::Read {
        stream: state.stream_path.clone(),
        cluster: state.cluster_id.clone(),
        bytes,
        commits: u64::try_from(commits.len()).unwrap_or(u64::MAX),
        salvaged,
    }))
}

/// The root `root`'s fingerprint, `None` when a part of it cannot be listed (it is read again).
fn fingerprint(root: &Path) -> Option<Fingerprint> {
    let mut print = Vec::new();
    let mut add = |dir: &Path| -> Option<Vec<PathBuf>> {
        let mut dirs = Vec::new();
        for entry in fs::read_dir(dir).ok()? {
            let path = entry.ok()?.path();
            let meta = fs::symlink_metadata(&path).ok()?;
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
        Some(dirs)
    };
    for dir in add(root)? {
        let name = dir.file_name().and_then(|n| n.to_str()).unwrap_or_default();
        if name == salvage::DIR {
            add(&dir)?;
        } else if name.starts_with("gen-") {
            let log = dir.join("log");
            if log.exists() {
                add(&log)?;
            }
        }
    }
    print.sort();
    Some(print)
}

/// An open file of a live process that is a root's lock.
#[derive(Debug)]
struct OpenLock {
    pid: u32,
    /// The path the process opened it by, as its `/proc` entry names it.
    path: PathBuf,
    id: FileId,
}

/// The `/proc` directory of the processes.
const PROC: &str = "/proc";

/// Every open file of a live process among `pids` (every process when `None`) that is a root's
/// lock ([`is_lock_path`]), or `None` when the pass ran past `deadline`. A process that exits
/// meanwhile is skipped. A process table, or a process's descriptors, that cannot be read fail.
fn open_locks(
    pids: Option<&[u32]>,
    deadline: Deadline,
) -> Result<Option<Vec<OpenLock>>, PassError> {
    let table = |op: &'static str, path: &Path| {
        let path = path.to_owned();
        move |source| PassError::ProcessTable { op, path, source }
    };
    let proc = Path::new(PROC);
    let pids: Vec<u32> = match pids {
        Some(pids) => pids.to_vec(),
        None => {
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
    let mut locks = Vec::new();
    for pid in pids {
        if deadline.passed() {
            return Ok(None);
        }
        let fds = proc.join(pid.to_string()).join("fd");
        let entries = match fs::read_dir(&fds) {
            Ok(entries) => entries,
            // Exited, or never there.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(table("list", &fds)(e)),
        };
        for entry in entries {
            let fd = match entry {
                Ok(entry) => entry.path(),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
                Err(e) => return Err(table("list", &fds)(e)),
            };
            let target = match fs::read_link(&fd) {
                Ok(target) => target,
                // Closed since the listing, or the process exited.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(table("readlink", &fd)(e)),
            };
            if !is_lock_path(target.as_os_str().as_bytes()) {
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
    }
    Ok(Some(locks))
}

/// Whether `target`, the path a `/proc/<pid>/fd` entry names, is a root's lock: it ends in
/// `/<label>-<32 hex>/lock`, with or without ` (deleted)`. A reader's `reader-<n>/lock` is not:
/// readers hold no commit.
fn is_lock_path(target: &[u8]) -> bool {
    let target = target.strip_suffix(DELETED).unwrap_or(target);
    let Some(dir) = target
        .strip_suffix(generation::LOCK.as_bytes())
        .and_then(|t| t.strip_suffix(b"/"))
    else {
        return false;
    };
    let name = dir.rsplit(|&b| b == b'/').next().unwrap_or_default();
    std::str::from_utf8(name).is_ok_and(generation::is_root_name)
}

#[cfg(test)]
mod tests;
