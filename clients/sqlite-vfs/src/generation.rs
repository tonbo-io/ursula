//! Local generations (design §5.1, §6.3): the directory a stream's files live in, its lock, and
//! the generations inside it.
//!
//! ```text
//! <dir>/<label>-<hash32>/     the root: BLAKE3 of the canonical stream path, first 32 hex digits
//!   lock                      flock, held with the binding
//!   gen-<lineage>/            the current generation: db, db-uwal, db-shm, log/
//!   gen-<lineage>.tmp/        a generation being built
//! ```
//!
//! The database is its canonical stream path ([`canonical_path`]): scheme, host, port and userinfo
//! are transport, so every host alias of a deployment shares one root. A generation is built in
//! `gen-<lineage>.tmp`, fsynced, and published by one rename, and its first log segment names the
//! generation it supersedes, so a crash that leaves two is finished at the next open
//! ([`Root::finish_leftovers`]).

use std::fs::File;
use std::fs::{self};
use std::path::Path;
use std::path::PathBuf;

use crate::codec::segment::SegmentError;
use crate::codec::segment::SegmentHeader;
use crate::codec::segment::scan;
use crate::ids::Lineage;
use crate::replog::Log;
use crate::replog::LogError;
use crate::replog::sync_dir;

/// The lock file in a root.
const LOCK: &str = "lock";
const GEN: &str = "gen-";
const TMP: &str = ".tmp";

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
    #[error("{}: generations {lineages:?}, none superseding the others", .root.display())]
    Ambiguous {
        root: PathBuf,
        lineages: Vec<Lineage>,
    },
    #[error("{}: a generation of this lineage exists", .path.display())]
    Exists { path: PathBuf },
}

fn io(op: &'static str, path: &Path) -> impl FnOnce(std::io::Error) -> GenerationError {
    let path = path.to_owned();
    move |source| GenerationError::Io { op, path, source }
}

fn hex(lineage: Lineage) -> String {
    lineage.0.iter().map(|b| format!("{b:02x}")).collect()
}

fn parse_hex(s: &str) -> Option<Lineage> {
    if s.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, o) in out.iter_mut().enumerate() {
        let at = i.checked_mul(2)?;
        *o = u8::from_str_radix(s.get(at..at.checked_add(2)?)?, 16).ok()?;
    }
    Some(Lineage(out))
}

/// A generation: one local lineage, its db, WAL and log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Generation {
    pub(crate) dir: PathBuf,
    pub(crate) lineage: Lineage,
}

impl Generation {
    pub(crate) fn db(&self) -> PathBuf {
        self.dir.join("db")
    }

    /// The WAL, under a name SQLite never probes.
    pub(crate) fn wal(&self) -> PathBuf {
        self.dir.join("db-uwal")
    }

    pub(crate) fn shm(&self) -> PathBuf {
        self.dir.join("db-shm")
    }

    pub(crate) fn log_dir(&self) -> PathBuf {
        self.dir.join("log")
    }

    /// The header of the generation's first log segment.
    fn first_header(&self) -> Result<SegmentHeader, GenerationError> {
        let log = self.log_dir();
        let mut segments: Vec<PathBuf> = fs::read_dir(&log)
            .map_err(io("list", &log))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|e| e == "usl"))
            .collect();
        segments.sort();
        let first = segments.first().ok_or_else(|| GenerationError::NoSegment {
            dir: self.dir.clone(),
        })?;
        let bytes = fs::read(first).map_err(io("read", first))?;
        scan(&bytes)
            .map(|s| s.header)
            .map_err(|source| GenerationError::Header {
                dir: self.dir.clone(),
                source,
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

    pub(crate) fn path(&self) -> &Path {
        &self.dir
    }

    /// The generations, temporary ones apart.
    fn generations(&self) -> Result<Vec<Generation>, GenerationError> {
        let mut gens = Vec::new();
        for entry in fs::read_dir(&self.dir).map_err(io("list", &self.dir))? {
            let entry = entry.map_err(io("list", &self.dir))?;
            let name = entry.file_name();
            let lineage = name
                .to_str()
                .and_then(|n| n.strip_prefix(GEN))
                .and_then(parse_hex);
            if let Some(lineage) = lineage {
                gens.push(Generation {
                    dir: entry.path(),
                    lineage,
                });
            }
        }
        gens.sort_by_key(|g| g.lineage);
        Ok(gens)
    }

    /// Finishes what a crash left: deletes every `gen-*.tmp` and every generation another one's
    /// first segment says it supersedes.
    pub(crate) fn finish_leftovers(&self) -> Result<(), GenerationError> {
        let mut removed = false;
        for entry in fs::read_dir(&self.dir).map_err(io("list", &self.dir))? {
            let entry = entry.map_err(io("list", &self.dir))?;
            let temporary = entry
                .file_name()
                .to_str()
                .is_some_and(|n| n.starts_with(GEN) && n.ends_with(TMP));
            if temporary {
                let path = entry.path();
                fs::remove_dir_all(&path).map_err(io("remove", &path))?;
                removed = true;
            }
        }
        let gens = self.generations()?;
        if gens.len() > 1 {
            let mut superseded = Vec::new();
            for g in &gens {
                if let Some(old) = g.first_header()?.supersedes {
                    superseded.push(old);
                }
            }
            for g in gens.iter().filter(|g| superseded.contains(&g.lineage)) {
                fs::remove_dir_all(&g.dir).map_err(io("remove", &g.dir))?;
                removed = true;
            }
        }
        if removed {
            sync_dir(&self.dir).map_err(io("fsync", &self.dir))?;
        }
        Ok(())
    }

    /// The current generation, if any. Two left after [`Root::finish_leftovers`] are an error.
    pub(crate) fn current(&self) -> Result<Option<Generation>, GenerationError> {
        let mut gens = self.generations()?;
        match gens.len() {
            0 | 1 => Ok(gens.pop()),
            _ => Err(GenerationError::Ambiguous {
                root: self.dir.clone(),
                lineages: gens.iter().map(|g| g.lineage).collect(),
            }),
        }
    }

    /// Starts building the generation of `h.lineage` in `gen-<lineage>.tmp`: its log, with `h`
    /// naming the current generation as the one it supersedes, and an empty db.
    pub(crate) fn build(&self, mut h: SegmentHeader) -> Result<Building, GenerationError> {
        let supersedes = self.current()?;
        h.supersedes = supersedes.as_ref().map(|g| g.lineage);
        let name = format!("{GEN}{}", hex(h.lineage));
        let target = self.dir.join(&name);
        let tmp = self.dir.join(format!("{name}{TMP}"));
        if target.exists() {
            return Err(GenerationError::Exists { path: target });
        }
        fs::create_dir(&tmp).map_err(io("create", &tmp))?;
        let generation = Generation {
            dir: tmp.clone(),
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
    RemoveSuperseded,
}

/// A generation being built.
#[derive(Debug)]
pub(crate) struct Building {
    root: PathBuf,
    tmp: PathBuf,
    target: PathBuf,
    lineage: Lineage,
    pub(crate) log: Log,
    pub(crate) db: File,
    supersedes: Option<Generation>,
}

impl Building {
    /// Publishes the generation: fsyncs the db, the log and the directory, renames it into place,
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
            plan.extend([Step::RemoveSuperseded, Step::FsyncRoot]);
        }
        for (ran, step) in plan.iter().enumerate() {
            if ran == steps {
                return Ok((generation, ran));
            }
            match step {
                Step::FsyncDb => {
                    let db_path = self.db_path();
                    self.db.sync_all().map_err(io("fsync", &db_path))?;
                }
                Step::FsyncLog => self.log.sync()?,
                Step::FsyncDir => sync_dir(&self.tmp).map_err(io("fsync", &self.tmp))?,
                Step::Rename => {
                    fs::rename(&self.tmp, &self.target).map_err(io("rename", &self.tmp))?;
                }
                Step::FsyncRoot => sync_dir(&self.root).map_err(io("fsync", &self.root))?,
                Step::RemoveSuperseded => {
                    if let Some(old) = &self.supersedes {
                        fs::remove_dir_all(&old.dir).map_err(io("remove", &old.dir))?;
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

    /// The db file being built.
    pub(crate) fn db_path(&self) -> PathBuf {
        self.tmp.join("db")
    }
}

#[cfg(test)]
mod tests;
