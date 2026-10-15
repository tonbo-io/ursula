//! Local generations (design §5.1, §6.3): the directory a stream's files live in, its lock, and
//! the generations inside it.
//!
//! ```text
//! <dir>/<label>-<hash32>/     the root: BLAKE3 of the canonical stream path, first 32 hex digits
//!   lock                      flock, held with the binding
//!   gen-<id32>/               the current generation: db, db-uwal, db-shm, log/
//!   gen-<id32>.tmp/           a generation being built
//!   gen-<id32>.trash/         a superseded generation being deleted
//!   salvage/<UTC>-<reason>/   generations moved aside, with manifest.json ([`crate::salvage`])
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
//!
//! The superseded generation's fate ([`Fate`]) is written before the publishing rename: it is
//! trashed, or salvaged, its manifest written into it first and the directory moved into
//! `salvage/` after the rename. A crash at any step leaves either the old generation current
//! (its manifest deleted at the next open) or the new one current and the old one on its way into
//! `salvage/` (finished at the next open).
//!
//! A reset binding's connections may keep reading their generation after the binding retired. They
//! hold it ([`Root::hold`]) until the last of them closes, and the root with it, so another process
//! gets `in_use`. An open in this process shares the root and copies a held generation
//! ([`Root::copy`]): db then log into a `.tmp`, published like a build, so the old connections keep
//! their files and never share a WAL or wal-index with the copy (design §5.6).

use std::fs::File;
use std::fs::{self};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::Weak;

use crate::codec::marker;
use crate::codec::segment::SegmentError;
use crate::codec::segment::SegmentHeader;
use crate::codec::segment::scan;
use crate::config::Step as AbortStep;
use crate::config::abort_at;
use crate::ids::GenId;
use crate::ids::Lineage;
use crate::replog::Log;
use crate::replog::LogError;
use crate::replog::sync_dir;
use crate::replog::{self};
use crate::salvage;
use crate::salvage::Manifest;
use crate::salvage::SalvageError;

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

/// The root directory of stream `p` under the cache root `dir`.
pub(crate) fn root_dir(dir: &Path, p: &CanonicalPath) -> PathBuf {
    dir.join(root_name(p))
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
    #[error(transparent)]
    Salvage(#[from] SalvageError),
}

/// A generation directory whose log's first segment header cannot be read: no segment, or a
/// header cut short, not verifying, or of another format. The open salvages it as `replaced`.
#[derive(Debug)]
pub(crate) struct Unreadable {
    pub(crate) dir: PathBuf,
    pub(crate) error: GenerationError,
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
    pub(crate) fn first_header(&self) -> Result<SegmentHeader, GenerationError> {
        first_header(&self.dir)
    }
}

/// The header of the first log segment of the generation in `dir`.
fn first_header(dir: &Path) -> Result<SegmentHeader, GenerationError> {
    let log = dir.join("log");
    let entries = match fs::read_dir(&log) {
        Ok(entries) => entries,
        // No `log/` at all: a generation with no segment.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(GenerationError::NoSegment {
                dir: dir.to_owned(),
            });
        }
        Err(e) => return Err(io("list", &log)(e)),
    };
    let mut segments: Vec<PathBuf> = entries
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
///
/// One `Root` serves every binding and open of the stream in the process, so its flock is held
/// while any of them lives: another process gets `in_use`, and an open beside a reset binding's
/// connections shares the lock. The generations those connections still use are held
/// ([`Root::hold`]), and an open copies a held generation ([`Root::copy`]) instead of
/// materializing it under them.
#[derive(Debug)]
pub(crate) struct Root {
    dir: PathBuf,
    /// Holds the flock until the root drops.
    _lock: File,
    /// The generations bindings of this process hold, by id.
    holds: Mutex<Vec<Weak<GenId>>>,
}

/// A binding's hold on its generation, released when dropped: while one lives, [`Root::held`]
/// says so.
#[derive(Debug)]
pub(crate) struct Hold {
    _id: Arc<GenId>,
}

impl Root {
    /// Creates `<dir>/<root_name>/` when missing and takes its `lock` (`InUse` if another process,
    /// or another open file of this one, holds it).
    pub(crate) fn lock(dir: &Path, p: &CanonicalPath) -> Result<Root, GenerationError> {
        let root = root_dir(dir, p);
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
            holds: Mutex::new(Vec::new()),
        })
    }

    fn holds(&self) -> std::sync::MutexGuard<'_, Vec<Weak<GenId>>> {
        self.holds.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A hold on generation `g`, for the binding that serves it.
    pub(crate) fn hold(&self, g: &Generation) -> Hold {
        let mut holds = self.holds();
        holds.retain(|held| held.strong_count() > 0);
        if let Some(id) = holds
            .iter()
            .filter_map(Weak::upgrade)
            .find(|id| **id == g.id)
        {
            return Hold { _id: id };
        }
        let id = Arc::new(g.id);
        holds.push(Arc::downgrade(&id));
        Hold { _id: id }
    }

    /// Whether a binding of this process still holds generation `g`: a reset binding whose
    /// connections are open (the only one that takes a hold).
    pub(crate) fn held(&self, g: &Generation) -> bool {
        self.holds()
            .iter()
            .filter_map(Weak::upgrade)
            .any(|id| *id == g.id)
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

    /// The root directory.
    pub(crate) fn path(&self) -> &Path {
        &self.dir
    }

    /// Finishes what a crash left: deletes every `gen-*.tmp` and `gen-*.trash`, salvages every
    /// generation another one's first segment says it supersedes that holds a salvage manifest
    /// and deletes the others, deletes the manifest of a generation nothing supersedes (its
    /// publish never happened), and completes every salvage still marked `moving`.
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
        let mut superseded = Vec::new();
        if gens.len() > 1 {
            // A generation whose header cannot be read is finished when a readable one says it
            // supersedes it; otherwise its error stands.
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
        }
        for g in &gens {
            salvage::clear_tmp(&g.path)?;
            match (superseded.contains(&g.id), salvage::read_manifest(&g.path)?) {
                (true, Some(manifest)) => {
                    let moved = salvage::move_into(&self.dir, &g.path, &manifest)?;
                    salvage::complete(&moved, manifest)?;
                }
                (true, None) => self.discard(g)?,
                (false, Some(_)) => salvage::unmark(&g.path)?,
                (false, None) => {}
            }
        }
        salvage::finish(&self.dir)?;
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
    #[cfg(test)]
    pub(crate) fn current(&self) -> Result<Option<Generation>, GenerationError> {
        self.current_dir()?.map(Dir::generation).transpose()
    }

    /// The current generation, if any, or its directory when its log's first segment header
    /// cannot be read. Two left after [`Root::finish_leftovers`] are an error.
    pub(crate) fn current_or_unreadable(
        &self,
    ) -> Result<Option<Result<Generation, Unreadable>>, GenerationError> {
        let Some(dir) = self.current_dir()? else {
            return Ok(None);
        };
        let path = dir.path.clone();
        match dir.generation() {
            Ok(g) => Ok(Some(Ok(g))),
            Err(error @ (GenerationError::Header { .. } | GenerationError::NoSegment { .. })) => {
                Ok(Some(Err(Unreadable { dir: path, error })))
            }
            Err(e) => Err(e),
        }
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

impl Root {
    /// Starts a copy of the current generation `from` in a fresh `gen-<id32>.tmp`, for an open
    /// beside the connections of a reset binding that still hold it (design §5.6): its db file
    /// first, then its log ([`replog::copy`]), so every page the copied db holds comes from a
    /// record the copied log holds (F1). The copy keeps the lineage, so its pending commits keep
    /// their ids and the page-1 marker stays valid, and its first log segment names `from` as the
    /// generation it supersedes. Its WAL and wal-index stay behind: the WAL's commits are in the
    /// log, which materializing the copy re-applies. The copy is published like a build
    /// ([`Building::publish`], `from` trashed), and a crash before the rename leaves only a
    /// `.tmp` that the next open deletes. A failed copy deletes its `.tmp`. On a filesystem that
    /// cannot clone files, the copy reads and writes the whole db file.
    pub(crate) fn copy(&self, from: &Generation) -> Result<Building, GenerationError> {
        let id = GenId::random().map_err(|source| GenerationError::Random { source })?;
        let name = format!("{GEN}{}", id.hex());
        let target = self.dir.join(&name);
        let tmp = self.dir.join(format!("{name}{TMP}"));
        if target.exists() {
            return Err(GenerationError::Exists { path: target });
        }
        fs::create_dir(&tmp).map_err(io("create", &tmp))?;
        let files = || -> Result<(Log, File), GenerationError> {
            let generation = Generation {
                dir: tmp.clone(),
                id,
                lineage: from.lineage,
            };
            let db_path = generation.db();
            fs::copy(from.db(), &db_path).map_err(io("copy", &from.db()))?;
            abort_at(AbortStep::CopyDb);
            replog::copy(&from.log_dir(), &generation.log_dir(), from.id)?;
            abort_at(AbortStep::CopyLog);
            let (log, _) = Log::open(&generation.log_dir())?;
            let db = File::options()
                .read(true)
                .write(true)
                .open(&db_path)
                .map_err(io("open", &db_path))?;
            Ok((log, db))
        };
        let (log, db) = match files() {
            Ok(files) => files,
            Err(e) => {
                if let Err(left) = fs::remove_dir_all(&tmp) {
                    crate::log::emit(crate::log::Level::Warn, "copy_leftover", &[
                        ("dir", &tmp.display()),
                        ("error", &left),
                    ]);
                }
                return Err(e);
            }
        };
        Ok(Building {
            root: self.dir.clone(),
            tmp,
            target,
            id,
            lineage: from.lineage,
            log,
            db,
            supersedes: Some(Dir {
                path: from.dir.clone(),
                id: from.id,
            }),
        })
    }
}

/// What becomes of the generation a publish supersedes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Fate {
    /// Renamed to `gen-<id32>.trash` and deleted.
    Trash,
    /// Moved into `salvage/` with this manifest (design §6.2, D3).
    Salvage(Manifest),
}

/// A step of [`Building::publish`].
#[derive(Debug, Clone, Copy)]
enum Step {
    FsyncDb,
    FsyncLog,
    FsyncDir,
    /// The superseded generation's manifest written (`state: "moving"`) and fsynced into it.
    MarkSalvage,
    Rename,
    FsyncRoot,
    TrashSuperseded,
    RemoveTrash,
    /// The superseded generation moved into `salvage/`.
    MoveSalvage,
    /// Its magic restored and its manifest `complete`.
    CompleteSalvage,
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
    /// applied to (design D7), fsyncs the db, the log and the directory, writes the salvage
    /// manifest into the generation it supersedes when `fate` salvages it, renames the new one into
    /// place, fsyncs the root, then deletes the superseded one or moves it into `salvage/`.
    pub(crate) fn publish(self, fate: Fate) -> Result<Generation, GenerationError> {
        self.publish_steps(fate, usize::MAX)
            .map(|(generation, _)| generation)
    }

    /// Runs the first `steps` steps of [`Building::publish`] (all of them for `usize::MAX`), as a
    /// crash after them would leave the files. Returns the generation and how many steps ran.
    fn publish_steps(
        mut self,
        fate: Fate,
        steps: usize,
    ) -> Result<(Generation, usize), GenerationError> {
        let generation = Generation {
            dir: self.target.clone(),
            id: self.id,
            lineage: self.lineage,
        };
        let salvaging = match (&self.supersedes, fate) {
            (Some(_), Fate::Salvage(manifest)) => Some(manifest),
            (None, _) | (Some(_), Fate::Trash) => None,
        };
        let mut plan = vec![Step::FsyncDb, Step::FsyncLog, Step::FsyncDir];
        if salvaging.is_some() {
            plan.push(Step::MarkSalvage);
        }
        plan.extend([Step::Rename, Step::FsyncRoot]);
        match (&self.supersedes, &salvaging) {
            (None, _) => {}
            (Some(_), Some(_)) => plan.extend([Step::MoveSalvage, Step::CompleteSalvage]),
            (Some(_), None) => plan.extend([
                Step::TrashSuperseded,
                Step::FsyncRoot,
                Step::RemoveTrash,
                Step::FsyncRoot,
            ]),
        }
        let mut salvaged = None;
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
                Step::MarkSalvage => {
                    if let (Some(old), Some(manifest)) = (&self.supersedes, &salvaging) {
                        salvage::write_manifest(&old.path, manifest)?;
                        abort_at(AbortStep::SalvageMarked);
                    }
                }
                Step::Rename => {
                    fs::rename(&self.tmp, &self.target).map_err(io("rename", &self.tmp))?;
                }
                Step::FsyncRoot => {
                    sync_dir(&self.root).map_err(io("fsync", &self.root))?;
                    if matches!(plan.get(ran.wrapping_sub(1)), Some(Step::Rename)) {
                        abort_at(AbortStep::PublishRenamed);
                    }
                }
                Step::MoveSalvage => {
                    if let (Some(old), Some(manifest)) = (&self.supersedes, &salvaging) {
                        salvaged = Some(salvage::move_into(&self.root, &old.path, manifest)?);
                        abort_at(AbortStep::SalvageMoved);
                    }
                }
                Step::CompleteSalvage => {
                    if let (Some(dir), Some(manifest)) = (&salvaged, &salvaging) {
                        salvage::complete(dir, manifest.clone())?;
                    }
                }
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
    pub(crate) fn crash_after(self, fate: Fate, steps: usize) -> Result<usize, GenerationError> {
        self.publish_steps(fate, steps).map(|(_, done)| done)
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
