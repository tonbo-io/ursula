//! Local generations (design §5.1, §6.3): the directory a stream's files live in, its lock, and
//! the generations inside it.
//!
//! ```text
//! <dir>/<label>-<hash32>/     the root: BLAKE3 of the canonical stream path, first 32 hex digits
//!   lock                      flock, held with the binding
//!   gen-<id32>/               the current generation: db, db-uwal, db-shm, log/
//!   gen-<id32>.tmp/           a generation being built
//!   gen-<id32>.trash/         a superseded generation being deleted
//! ```
//!
//! A generation directory is named by a random id ([`GenId`]), not by its lineage: the lineage is
//! read from its log's segment header, and a copy of a generation keeps it in a directory of its
//! own. The db file holds the page-1 marker ([`crate::codec::marker`]) and the WAL is `db-uwal`, a
//! name SQLite never probes, so SQLite outside the extension cannot open a generation's files.
//!
//! The database is its canonical stream path ([`canonical_path`]): scheme, host, port and userinfo
//! are transport, so every host alias of a deployment shares one root. A generation is built in
//! `gen-<id32>.tmp`, fsynced, and published by one rename, and its first log segment names the
//! generation directory it supersedes by id, so a crash that leaves two is finished at the next
//! open ([`Root::finish_leftovers`]). A superseded generation is renamed to `gen-<id32>.trash`
//! before it is deleted, so a crash midway never leaves a half-deleted `gen-` directory. A
//! generation is published with page 1 written and marked, the page 1 of an empty database when
//! nothing was applied ([`marker::empty_page_one`]), so no published db file is ever empty or
//! unmarked.

use std::fs::File;
use std::fs::{self};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::path::PathBuf;

use crate::codec::marker;
use crate::codec::segment::SegmentError;
use crate::codec::segment::SegmentHeader;
use crate::codec::segment::scan;
use crate::ids::GenId;
use crate::ids::Lineage;
use crate::replog::Log;
use crate::replog::LogError;
use crate::replog::sync_dir;

/// The lock file in a root.
const LOCK: &str = "lock";
const GEN: &str = "gen-";
const TMP: &str = ".tmp";
const TRASH: &str = ".trash";

/// A stream URL's path spelled one way: every segment percent-decoded and encoded again one
/// fixed way (a segment that does not decode to UTF-8 stays as it is), `.` and `..` resolved, no
/// trailing `/`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct CanonicalPath(String);

impl CanonicalPath {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PathError {
    #[error("{url:?} is not an http:// or https:// URL")]
    Scheme { url: String },
    #[error("{url:?} has a query or a fragment, which name no stream")]
    QueryOrFragment { url: String },
    #[error("{url:?} names no stream: its path is empty")]
    NoPath { url: String },
}

/// The canonical path of the stream `url` names. The one implementation: file names and the log's
/// segment headers both use it.
pub(crate) fn canonical_path(url: &str) -> Result<CanonicalPath, PathError> {
    let scheme_error = || PathError::Scheme {
        url: url.to_owned(),
    };
    let (scheme, rest) = url.split_once("://").ok_or_else(scheme_error)?;
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return Err(scheme_error());
    }
    if rest.contains(['?', '#']) {
        return Err(PathError::QueryOrFragment {
            url: url.to_owned(),
        });
    }
    let path = rest.find('/').and_then(|i| rest.get(i..)).unwrap_or("");
    let mut segments: Vec<String> = Vec::new();
    let trimmed = path.trim_end_matches('/');
    for segment in trimmed.split('/').skip(1) {
        let text = decode(segment);
        match text.as_deref() {
            Some(".") => {}
            Some("..") => {
                segments.pop();
            }
            Some(text) => segments.push(encode(text)),
            None => segments.push(segment.to_owned()),
        }
    }
    if segments.iter().all(String::is_empty) {
        return Err(PathError::NoPath {
            url: url.to_owned(),
        });
    }
    Ok(CanonicalPath(format!("/{}", segments.join("/"))))
}

/// The percent-decoded text of a path segment, `None` when an escape is malformed or the bytes
/// are not UTF-8.
fn decode(segment: &str) -> Option<String> {
    let mut out = Vec::with_capacity(segment.len());
    let mut bytes = segment.bytes();
    while let Some(b) = bytes.next() {
        if b == b'%' {
            let hex = |c: Option<u8>| c.and_then(|c| char::from(c).to_digit(16));
            let (Some(hi), Some(lo)) = (hex(bytes.next()), hex(bytes.next())) else {
                return None;
            };
            out.push(u8::try_from(hi.checked_mul(16)?.checked_add(lo)?).ok()?);
        } else {
            out.push(b);
        }
    }
    String::from_utf8(out).ok()
}

/// `text` percent-encoded as JavaScript's `encodeURIComponent` does.
fn encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for b in text.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&b) {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// The root directory's name: a readable label from the path's last two segments, and the first
/// 32 hex digits of the BLAKE3 of the canonical path.
pub(crate) fn root_name(p: &CanonicalPath) -> String {
    let hash = blake3::hash(p.as_str().as_bytes()).to_hex();
    let segments: Vec<&str> = p.as_str().split('/').filter(|s| !s.is_empty()).collect();
    let tail = segments
        .get(segments.len().saturating_sub(2)..)
        .unwrap_or_default();
    let joined = tail
        .iter()
        .map(|s| decode(s).unwrap_or_else(|| (*s).to_owned()))
        .collect::<Vec<_>>()
        .join("-")
        .to_lowercase();
    // Runs of characters other than [a-z0-9_-] become one `_`.
    let mut label = String::new();
    let mut in_run = false;
    for c in joined.chars() {
        if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-' {
            label.push(c);
            in_run = false;
        } else if !in_run {
            label.push('_');
            in_run = true;
        }
    }
    if label.starts_with('-') {
        label.replace_range(..1, "_");
    }
    label.truncate(64);
    if label.is_empty() {
        label.push_str("stream");
    }
    format!("{label}-{}", hash.as_str().get(..32).unwrap_or_default())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum GenerationError {
    #[error("{}: another process holds the stream's local files", .root.display())]
    InUse { root: PathBuf },
    #[error("{op} {}: {source}", .path.display())]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Log(#[from] LogError),
    #[error("generation {}: its first log segment: {source}", .dir.display())]
    Header {
        dir: PathBuf,
        #[source]
        source: SegmentError,
    },
    #[error("generation {}: no log segment", .dir.display())]
    NoSegment { dir: PathBuf },
    #[error(
        "{}: generations {}, none superseding the others",
        .root.display(),
        .ids.iter().map(GenId::hex).collect::<Vec<_>>().join(", ")
    )]
    Ambiguous { root: PathBuf, ids: Vec<GenId> },
    #[error("{}: a generation of this id exists", .path.display())]
    Exists { path: PathBuf },
    #[error("a generation id from {}: {source}", crate::ids::URANDOM)]
    Random {
        #[source]
        source: std::io::Error,
    },
}

fn io(op: &'static str, path: &Path) -> impl FnOnce(std::io::Error) -> GenerationError {
    let path = path.to_owned();
    move |source| GenerationError::Io { op, path, source }
}

/// A generation: its directory and the directory's id, its lineage (from its log), its db, WAL
/// and log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Generation {
    pub(crate) dir: PathBuf,
    pub(crate) id: GenId,
    pub(crate) lineage: Lineage,
}

impl Generation {
    pub(crate) fn db(&self) -> PathBuf {
        self.dir.join("db")
    }

    /// The WAL: `db-uwal`, a name SQLite never probes. The VFS opens it in place of `db-wal` for a
    /// bound db, and maps SQLite's own `xAccess` and `xDelete` of `db-wal` to it.
    pub(crate) fn wal(&self) -> PathBuf {
        self.dir.join("db-uwal")
    }

    /// Files SQLite outside the extension would write next to the db, which no generation keeps:
    /// a WAL under SQLite's own name and a rollback journal.
    pub(crate) fn strays(&self) -> [PathBuf; 2] {
        [self.dir.join("db-wal"), self.dir.join("db-journal")]
    }

    pub(crate) fn shm(&self) -> PathBuf {
        self.dir.join("db-shm")
    }

    pub(crate) fn log_dir(&self) -> PathBuf {
        self.dir.join("log")
    }

    /// The header of the generation's first log segment.
    #[cfg(test)]
    fn first_header(&self) -> Result<SegmentHeader, GenerationError> {
        first_header(&self.dir)
    }
}

/// The header of the first log segment of the generation in `dir`.
fn first_header(dir: &Path) -> Result<SegmentHeader, GenerationError> {
    let log = dir.join("log");
    let mut segments: Vec<PathBuf> = fs::read_dir(&log)
        .map_err(io("list", &log))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "usl"))
        .collect();
    segments.sort();
    let first = segments.first().ok_or_else(|| GenerationError::NoSegment {
        dir: dir.to_owned(),
    })?;
    let bytes = fs::read(first).map_err(io("read", first))?;
    scan(&bytes)
        .map(|s| s.header)
        .map_err(|source| GenerationError::Header {
            dir: dir.to_owned(),
            source,
        })
}

/// A generation directory in a root, its log not read yet.
#[derive(Debug)]
struct Dir {
    path: PathBuf,
    id: GenId,
}

impl Dir {
    /// The generation, its lineage read from its first log segment's header.
    fn generation(self) -> Result<Generation, GenerationError> {
        let lineage = first_header(&self.path)?.lineage;
        Ok(Generation {
            dir: self.path,
            id: self.id,
            lineage,
        })
    }
}

/// A stream's root directory, locked.
#[derive(Debug)]
pub(crate) struct Root {
    dir: PathBuf,
    /// Holds the flock until the root drops.
    _lock: File,
}

impl Root {
    /// Creates `<dir>/<root_name>/` when missing and takes its `lock` (`InUse` if another process,
    /// or another open file of this one, holds it).
    pub(crate) fn lock(dir: &Path, p: &CanonicalPath) -> Result<Root, GenerationError> {
        let root = dir.join(root_name(p));
        fs::create_dir_all(&root).map_err(io("create", &root))?;
        let path = root.join(LOCK);
        let lock = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(io("open", &path))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(GenerationError::InUse { root });
            }
            Err(std::fs::TryLockError::Error(source)) => {
                return Err(GenerationError::Io {
                    op: "lock",
                    path,
                    source,
                });
            }
        }
        Ok(Root {
            dir: root,
            _lock: lock,
        })
    }

    /// The generation directories, temporary ones apart, by id.
    fn generations(&self) -> Result<Vec<Dir>, GenerationError> {
        let mut gens = Vec::new();
        for entry in fs::read_dir(&self.dir).map_err(io("list", &self.dir))? {
            let entry = entry.map_err(io("list", &self.dir))?;
            let name = entry.file_name();
            let id = name
                .to_str()
                .and_then(|n| n.strip_prefix(GEN))
                .and_then(GenId::parse_hex);
            if let Some(id) = id {
                gens.push(Dir {
                    path: entry.path(),
                    id,
                });
            }
        }
        gens.sort_by_key(|g| g.id);
        Ok(gens)
    }

    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        &self.dir
    }

    /// Finishes what a crash left: deletes every `gen-*.tmp` and every generation another one's
    /// first segment says it supersedes.
    pub(crate) fn finish_leftovers(&self) -> Result<(), GenerationError> {
        let mut removed = false;
        for entry in fs::read_dir(&self.dir).map_err(io("list", &self.dir))? {
            let entry = entry.map_err(io("list", &self.dir))?;
            let leftover = entry
                .file_name()
                .to_str()
                .is_some_and(|n| n.starts_with(GEN) && (n.ends_with(TMP) || n.ends_with(TRASH)));
            if leftover {
                let path = entry.path();
                fs::remove_dir_all(&path).map_err(io("remove", &path))?;
                removed = true;
            }
        }
        if removed {
            sync_dir(&self.dir).map_err(io("fsync", &self.dir))?;
        }
        let gens = self.generations()?;
        if gens.len() > 1 {
            // A generation whose header cannot be read is finished when a readable one says it
            // supersedes it; otherwise its error stands.
            let mut superseded = Vec::new();
            let mut unreadable = Vec::new();
            for g in &gens {
                match first_header(&g.path) {
                    Ok(h) => superseded.extend(h.supersedes),
                    Err(e) => unreadable.push((g.id, e)),
                }
            }
            if let Some((_, e)) = unreadable
                .into_iter()
                .find(|(id, _)| !superseded.contains(id))
            {
                return Err(e);
            }
            for g in gens.iter().filter(|g| superseded.contains(&g.id)) {
                self.discard(g)?;
            }
        }
        Ok(())
    }

    /// Deletes a superseded generation: renamed to `gen-<id32>.trash` and the root fsynced
    /// first, so a crash midway leaves only trash, then deleted and the root fsynced again.
    fn discard(&self, g: &Dir) -> Result<(), GenerationError> {
        let trash = self.dir.join(format!("{GEN}{}{TRASH}", g.id.hex()));
        fs::rename(&g.path, &trash).map_err(io("rename", &g.path))?;
        sync_dir(&self.dir).map_err(io("fsync", &self.dir))?;
        fs::remove_dir_all(&trash).map_err(io("remove", &trash))?;
        sync_dir(&self.dir).map_err(io("fsync", &self.dir))
    }

    /// The current generation, if any. Two left after [`Root::finish_leftovers`] are an error.
    pub(crate) fn current(&self) -> Result<Option<Generation>, GenerationError> {
        self.current_dir()?.map(Dir::generation).transpose()
    }

    /// The current generation directory, if any, its log not read.
    fn current_dir(&self) -> Result<Option<Dir>, GenerationError> {
        let mut gens = self.generations()?;
        match gens.len() {
            0 | 1 => Ok(gens.pop()),
            _ => Err(GenerationError::Ambiguous {
                root: self.dir.clone(),
                ids: gens.iter().map(|g| g.id).collect(),
            }),
        }
    }

    /// Starts building the generation of `h.lineage` in a fresh `gen-<id32>.tmp`: its log, with
    /// `h` naming the current generation directory as the one it supersedes, and an empty db.
    pub(crate) fn build(&self, mut h: SegmentHeader) -> Result<Building, GenerationError> {
        let supersedes = self.current_dir()?;
        h.supersedes = supersedes.as_ref().map(|g| g.id);
        let id = GenId::random().map_err(|source| GenerationError::Random { source })?;
        let name = format!("{GEN}{}", id.hex());
        let target = self.dir.join(&name);
        let tmp = self.dir.join(format!("{name}{TMP}"));
        if target.exists() {
            return Err(GenerationError::Exists { path: target });
        }
        fs::create_dir(&tmp).map_err(io("create", &tmp))?;
        let generation = Generation {
            dir: tmp.clone(),
            id,
            lineage: h.lineage,
        };
        let log = Log::create(&generation.log_dir(), h)?;
        let db_path = generation.db();
        let db = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&db_path)
            .map_err(io("create", &db_path))?;
        Ok(Building {
            root: self.dir.clone(),
            tmp,
            target,
            id,
            lineage: generation.lineage,
            log,
            db,
            supersedes,
        })
    }
}

/// A step of [`Building::publish`].
#[derive(Debug, Clone, Copy)]
enum Step {
    FsyncDb,
    FsyncLog,
    FsyncDir,
    Rename,
    FsyncRoot,
    TrashSuperseded,
    RemoveTrash,
}

/// A generation being built.
#[derive(Debug)]
pub(crate) struct Building {
    root: PathBuf,
    tmp: PathBuf,
    target: PathBuf,
    id: GenId,
    lineage: Lineage,
    pub(crate) log: Log,
    pub(crate) db: File,
    supersedes: Option<Dir>,
}

impl Building {
    /// Publishes the generation: writes the page 1 of an empty database into a db file nothing was
    /// applied to (design D7), fsyncs the db, the log and the directory, renames it into place,
    /// fsyncs the root, deletes the generation it supersedes and fsyncs the root again.
    pub(crate) fn publish(self) -> Result<Generation, GenerationError> {
        self.publish_steps(usize::MAX)
            .map(|(generation, _)| generation)
    }

    /// Runs the first `steps` steps of [`Building::publish`] (all of them for `usize::MAX`), as a
    /// crash after them would leave the files. Returns the generation and how many steps ran.
    fn publish_steps(mut self, steps: usize) -> Result<(Generation, usize), GenerationError> {
        let generation = Generation {
            dir: self.target.clone(),
            id: self.id,
            lineage: self.lineage,
        };
        let mut plan = vec![
            Step::FsyncDb,
            Step::FsyncLog,
            Step::FsyncDir,
            Step::Rename,
            Step::FsyncRoot,
        ];
        if self.supersedes.is_some() {
            plan.extend([
                Step::TrashSuperseded,
                Step::FsyncRoot,
                Step::RemoveTrash,
                Step::FsyncRoot,
            ]);
        }
        for (ran, step) in plan.iter().enumerate() {
            if ran == steps {
                return Ok((generation, ran));
            }
            match step {
                Step::FsyncDb => {
                    let db_path = self.db_path();
                    let len = self.db.metadata().map_err(io("stat", &db_path))?.len();
                    if len == 0 {
                        let page = marker::empty_page_one(self.lineage);
                        self.db
                            .write_all_at(page.as_slice(), 0)
                            .map_err(io("write", &db_path))?;
                    }
                    self.db.sync_all().map_err(io("fsync", &db_path))?;
                }
                Step::FsyncLog => self.log.sync()?,
                Step::FsyncDir => sync_dir(&self.tmp).map_err(io("fsync", &self.tmp))?,
                Step::Rename => {
                    fs::rename(&self.tmp, &self.target).map_err(io("rename", &self.tmp))?;
                }
                Step::FsyncRoot => sync_dir(&self.root).map_err(io("fsync", &self.root))?,
                Step::TrashSuperseded => {
                    if let Some(old) = &self.supersedes {
                        let trash = self.trash(old);
                        fs::rename(&old.path, &trash).map_err(io("rename", &old.path))?;
                    }
                }
                Step::RemoveTrash => {
                    if let Some(old) = &self.supersedes {
                        let trash = self.trash(old);
                        fs::remove_dir_all(&trash).map_err(io("remove", &trash))?;
                    }
                }
            }
        }
        Ok((generation, plan.len()))
    }

    /// Test hook: publish as far as `steps` steps, as a crash after them leaves the files.
    #[cfg(test)]
    pub(crate) fn crash_after(self, steps: usize) -> Result<usize, GenerationError> {
        self.publish_steps(steps).map(|(_, done)| done)
    }

    fn trash(&self, old: &Dir) -> PathBuf {
        self.root.join(format!("{GEN}{}{TRASH}", old.id.hex()))
    }

    /// Deletes the generation being built, its `.tmp` and everything in it.
    pub(crate) fn discard(self) -> Result<(), GenerationError> {
        let Self {
            root, tmp, log, db, ..
        } = self;
        drop((log, db));
        fs::remove_dir_all(&tmp).map_err(io("remove", &tmp))?;
        sync_dir(&root).map_err(io("fsync", &root))
    }

    /// The db file being built.
    pub(crate) fn db_path(&self) -> PathBuf {
        self.tmp.join("db")
    }
}

#[cfg(test)]
mod tests;
