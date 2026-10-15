//! Salvage (design §6.2, D3): a generation that a history change, a replaced db file or a
//! recreate would otherwise discard is moved aside, whole, into the root's `salvage/`, and never
//! read or deleted by the extension again.
//!
//! ```text
//! <root>/salvage/<YYYYMMDDTHHMMSSZ>-<reason>[-<k>]/
//!   manifest.json     what the generation was: path, cluster, tokens, acknowledged position
//!   db                a plain SQLite database: SQLite's magic in bytes 0..16 (a replaced db as found)
//!   log/              the generation's log, kept
//! ```
//!
//! A salvage is made by one rename of the generation directory, so it needs no free space. Its db
//! already holds every commit of its valid log (the open materialized it), so restoring SQLite's
//! magic in place of the page-1 marker makes it a database any SQLite opens. A db file found
//! replaced is left as it was found.
//!
//! The manifest goes into the generation directory before the publish that supersedes it, with
//! `state: "moving"`, and says `complete` once the magic is back. A crash at any step is finished
//! at the next open: a superseded generation with a manifest is moved here, a salvage still
//! `moving` is completed, and a manifest in a generation nothing supersedes (the publish never
//! happened) is deleted ([`crate::generation::Root::finish_leftovers`]).

use std::fs::File;
use std::fs::{self};
use std::io::Write as _;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use ursula_headers::Incarnation;
use ursula_headers::Offset;

use crate::codec::marker;
use crate::codec::marker::SQLITE_MAGIC;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::json;
use crate::json::SyntaxError;
use crate::json::Value;
use crate::log;
use crate::log::Level;
use crate::replog::sync_dir;

/// The directory of a root that holds salvaged generations.
pub(crate) const DIR: &str = "salvage";
/// The manifest's file name, in a generation being salvaged and in a salvage.
pub(crate) const MANIFEST: &str = "manifest.json";
/// The manifest format this version writes.
const FORMAT: u64 = 1;

/// Why a generation was salvaged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reason {
    /// The stream's token changed and its history does not inherit the generation, or may have
    /// lost commits its log holds after the latest ACK (design §6.2).
    HistoryChanged,
    /// The db file is not the generation's (page 1 holds another marker, SQLite's magic, or
    /// nothing), or the log's first segment header is unreadable.
    Replaced,
    /// The stream was missing and the open created it again (`recreate`).
    Recreated,
}

impl Reason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Reason::HistoryChanged => "history_changed",
            Reason::Replaced => "replaced",
            Reason::Recreated => "recreated",
        }
    }

    fn parse(text: &str) -> Option<Reason> {
        [Reason::HistoryChanged, Reason::Replaced, Reason::Recreated]
            .into_iter()
            .find(|r| r.as_str() == text)
    }
}

/// Whether a salvage is finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    /// Written into the generation before its publish; the move or the magic may not have
    /// happened yet.
    Moving,
    /// Moved and plain.
    Complete,
}

impl State {
    fn as_str(self) -> &'static str {
        match self {
            State::Moving => "moving",
            State::Complete => "complete",
        }
    }
}

/// What the generation's log said when it was set aside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Acknowledged {
    pub(crate) position: Position,
    pub(crate) offset: Offset,
    /// The generation's own commits after the acknowledged position.
    pub(crate) unacknowledged: u64,
}

/// A salvage's `manifest.json`: one flat JSON object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Manifest {
    pub(crate) reason: Reason,
    pub(crate) state: State,
    /// When the salvage was decided, RFC 3339 in UTC to the second.
    pub(crate) time: String,
    /// The canonical stream path.
    pub(crate) path: String,
    /// The cluster the generation was built on, when its log's header was readable.
    pub(crate) cluster_id: Option<String>,
    pub(crate) lineage: Option<Lineage>,
    /// The token the generation's log names, when the log was read.
    pub(crate) incarnation: Option<Incarnation>,
    /// The stream's token when the salvage was decided, when it was known.
    pub(crate) stream_incarnation: Option<Incarnation>,
    /// `None` for a replaced db, whose log is not read.
    pub(crate) acknowledged: Option<Acknowledged>,
    /// Whether the db opens as a plain SQLite database once the salvage completes.
    pub(crate) plain: bool,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ManifestError {
    #[error(transparent)]
    Syntax(#[from] SyntaxError),
    #[error("format {found}, not {FORMAT}")]
    Format { found: u64 },
    #[error("{field} is missing or not what it must be")]
    Field { field: &'static str },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SalvageError {
    #[error("{op} {}: {source}", .path.display())]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{}: {source}", .path.display())]
    Manifest {
        path: PathBuf,
        #[source]
        source: ManifestError,
    },
}

fn io(op: &'static str, path: &Path) -> impl FnOnce(std::io::Error) -> SalvageError {
    let path = path.to_owned();
    move |source| SalvageError::Io { op, path, source }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn parse_lineage(text: &str) -> Option<Lineage> {
    if text.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, o) in out.iter_mut().enumerate() {
        let at = i.checked_mul(2)?;
        *o = u8::from_str_radix(text.get(at..at.checked_add(2)?)?, 16).ok()?;
    }
    Some(Lineage(out))
}

impl Manifest {
    /// The manifest as its file holds it.
    pub(crate) fn render(&self) -> String {
        let text = |s: Option<String>| s.map_or_else(json::null, |s| json::str(&s));
        let number = |n: Option<u64>| n.map_or_else(json::null, |n| n.to_string());
        json::object(&[
            ("format", FORMAT.to_string()),
            ("state", json::str(self.state.as_str())),
            ("reason", json::str(self.reason.as_str())),
            ("time", json::str(&self.time)),
            ("path", json::str(&self.path)),
            ("cluster_id", text(self.cluster_id.clone())),
            ("lineage", text(self.lineage.map(|l| hex(&l.0)))),
            ("incarnation", text(self.incarnation.map(|t| t.to_string()))),
            (
                "stream_incarnation",
                text(self.stream_incarnation.map(|t| t.to_string())),
            ),
            (
                "acknowledged_position",
                number(self.acknowledged.map(|a| a.position.0)),
            ),
            (
                "acknowledged_offset",
                number(self.acknowledged.map(|a| a.offset.get())),
            ),
            (
                "unacknowledged",
                number(self.acknowledged.map(|a| a.unacknowledged)),
            ),
            ("plain", self.plain.to_string()),
        ])
    }

    /// Reads a manifest. Unknown members are ignored, so later versions may add some.
    pub(crate) fn parse(text: &str) -> Result<Manifest, ManifestError> {
        let members = json::parse_object(text)?;
        let get = |name: &str| {
            members
                .iter()
                .find(|(k, _)| k == name)
                .map_or(&Value::Null, |(_, v)| v)
        };
        let field = |field: &'static str| ManifestError::Field { field };
        let text = |name: &'static str| match get(name) {
            Value::Text(t) => Ok(Some(t.clone())),
            Value::Null => Ok(None),
            Value::Bool(_) | Value::Number(_) => Err(field(name)),
        };
        let number = |name: &'static str| match get(name) {
            Value::Number(n) => json::whole(*n).map(Some).ok_or(field(name)),
            Value::Null => Ok(None),
            Value::Bool(_) | Value::Text(_) => Err(field(name)),
        };
        let token = |name: &'static str| {
            text(name)?
                .map(|t| {
                    t.parse::<u64>()
                        .ok()
                        .and_then(|n| Incarnation::try_from(n).ok())
                        .ok_or(field(name))
                })
                .transpose()
        };
        let format = number("format")?.ok_or(field("format"))?;
        if format != FORMAT {
            return Err(ManifestError::Format { found: format });
        }
        let state = match text("state")?.as_deref() {
            Some("moving") => State::Moving,
            Some("complete") => State::Complete,
            _ => return Err(field("state")),
        };
        let reason = text("reason")?
            .as_deref()
            .and_then(Reason::parse)
            .ok_or(field("reason"))?;
        let lineage = text("lineage")?
            .map(|t| parse_lineage(&t).ok_or(field("lineage")))
            .transpose()?;
        let acknowledged = match (
            number("acknowledged_position")?,
            number("acknowledged_offset")?,
            number("unacknowledged")?,
        ) {
            (Some(position), Some(offset), Some(unacknowledged)) => Some(Acknowledged {
                position: Position(position),
                offset: Offset::from_u64(offset),
                unacknowledged,
            }),
            (None, None, None) => None,
            _ => return Err(field("acknowledged_position")),
        };
        let plain = match get("plain") {
            Value::Bool(b) => *b,
            _ => return Err(field("plain")),
        };
        Ok(Manifest {
            reason,
            state,
            time: text("time")?.ok_or(field("time"))?,
            path: text("path")?.ok_or(field("path"))?,
            cluster_id: text("cluster_id")?,
            lineage,
            incarnation: token("incarnation")?,
            stream_incarnation: token("stream_incarnation")?,
            acknowledged,
            plain,
        })
    }

    /// The salvage directory's name without its collision suffix: the time with its separators
    /// dropped (`20261016T123456Z`), then the reason.
    fn dir_name(&self) -> String {
        let compact: String = self
            .time
            .chars()
            .filter(|c| !matches!(c, '-' | ':'))
            .collect();
        format!("{compact}-{}", self.reason.as_str())
    }
}

/// Now, as RFC 3339 in UTC to the second (`2026-10-16T12:34:56Z`).
pub(crate) fn utc_now() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    utc(secs)
}

/// `secs` after the Unix epoch as RFC 3339 in UTC.
fn utc(secs: u64) -> String {
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let (year, month, day) = civil(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// The proleptic Gregorian date `days` after 1970-01-01 (Howard Hinnant's `civil_from_days`).
fn civil(days: u64) -> (u64, u64, u64) {
    let z = days.saturating_add(719_468);
    let era = z / 146_097;
    let doe = z % 146_097;
    let yoe = doe
        .saturating_sub(doe / 1460)
        .saturating_add(doe / 36_524)
        .saturating_sub(doe / 146_096)
        / 365;
    let doy = doe.saturating_sub(
        yoe.saturating_mul(365)
            .saturating_add(yoe / 4)
            .saturating_sub(yoe / 100),
    );
    let mp = doy.saturating_mul(5).saturating_add(2) / 153;
    let day = doy
        .saturating_sub(mp.saturating_mul(153).saturating_add(2) / 5)
        .saturating_add(1);
    let month = if mp < 10 {
        mp.saturating_add(3)
    } else {
        mp.saturating_sub(9)
    };
    let year = yoe
        .saturating_add(era.saturating_mul(400))
        .saturating_add(u64::from(month <= 2));
    (year, month, day)
}

/// Writes `m` as `dir/manifest.json`: a temporary file fsynced and renamed over it, and `dir`
/// fsynced, so the manifest is either absent or whole.
pub(crate) fn write_manifest(dir: &Path, m: &Manifest) -> Result<(), SalvageError> {
    let tmp = dir.join(format!("{MANIFEST}.tmp"));
    let path = dir.join(MANIFEST);
    let mut file = File::create(&tmp).map_err(io("create", &tmp))?;
    file.write_all(m.render().as_bytes())
        .map_err(io("write", &tmp))?;
    file.sync_all().map_err(io("fsync", &tmp))?;
    drop(file);
    fs::rename(&tmp, &path).map_err(io("rename", &tmp))?;
    sync_dir(dir).map_err(io("fsync", dir))
}

/// The manifest in `dir`, if it has one.
pub(crate) fn read_manifest(dir: &Path) -> Result<Option<Manifest>, SalvageError> {
    let path = dir.join(MANIFEST);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io("read", &path)(e)),
    };
    Manifest::parse(&text)
        .map(Some)
        .map_err(|source| SalvageError::Manifest { path, source })
}

/// Deletes the manifest in `dir`, if any: a generation marked for salvage whose publish never
/// happened stays the current one.
pub(crate) fn unmark(dir: &Path) -> Result<(), SalvageError> {
    let path = dir.join(MANIFEST);
    match fs::remove_file(&path) {
        Ok(()) => sync_dir(dir).map_err(io("fsync", dir)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io("remove", &path)(e)),
    }
}

/// Moves the generation directory `from`, whose manifest is `m`, into `<root>/salvage/`, under a
/// name no salvage has, then fsyncs the salvage directory and the root. Returns where it went.
pub(crate) fn move_into(root: &Path, from: &Path, m: &Manifest) -> Result<PathBuf, SalvageError> {
    let dir = root.join(DIR);
    match fs::create_dir(&dir) {
        Ok(()) => sync_dir(root).map_err(io("fsync", root))?,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(io("create", &dir)(e)),
    }
    let base = m.dir_name();
    let mut target = dir.join(&base);
    let mut k = 2u32;
    while target.exists() {
        target = dir.join(format!("{base}-{k}"));
        k = k.saturating_add(1);
    }
    fs::rename(from, &target).map_err(io("rename", from))?;
    sync_dir(&dir).map_err(io("fsync", &dir))?;
    sync_dir(root).map_err(io("fsync", root))?;
    log::emit(Level::Warn, "salvaged", &[
        ("dir", &target.display()),
        ("reason", &m.reason.as_str()),
        ("path", &m.path),
    ]);
    Ok(target)
}

/// Completes the salvage in `dir`: SQLite's magic in place of the generation's marker (a db
/// file found replaced stays as it was), the db fsynced, then the manifest says `complete`.
pub(crate) fn complete(dir: &Path, mut m: Manifest) -> Result<(), SalvageError> {
    if let (Reason::HistoryChanged | Reason::Recreated, Some(lineage)) = (m.reason, m.lineage) {
        let path = dir.join("db");
        let db = File::options()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(io("open", &path))?;
        let mut head = [0u8; marker::LEN];
        // A file shorter than the marker cannot carry it: nothing to restore.
        if db.read_exact_at(&mut head, 0).is_ok() && marker::is_marked(&head, lineage) {
            db.write_all_at(&SQLITE_MAGIC, 0)
                .map_err(io("write", &path))?;
            db.sync_all().map_err(io("fsync", &path))?;
        }
    }
    m.state = State::Complete;
    write_manifest(dir, &m)
}

/// Completes every salvage of `root` still marked `moving`.
pub(crate) fn finish(root: &Path) -> Result<(), SalvageError> {
    for s in list(root)? {
        if s.manifest.state == State::Moving {
            complete(&s.dir, s.manifest)?;
        }
    }
    Ok(())
}

/// A salvage in a root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Salvaged {
    pub(crate) dir: PathBuf,
    pub(crate) manifest: Manifest,
}

/// The salvages of `root` with a readable manifest, by name.
pub(crate) fn list(root: &Path) -> Result<Vec<Salvaged>, SalvageError> {
    let dir = root.join(DIR);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(io("list", &dir)(e)),
    };
    let mut out = Vec::new();
    for entry in entries {
        let path = entry.map_err(io("list", &dir))?.path();
        if !path.is_dir() {
            continue;
        }
        match read_manifest(&path) {
            Ok(Some(manifest)) => out.push(Salvaged {
                dir: path,
                manifest,
            }),
            // Not one of ours, or written by a later version: never touched.
            Ok(None) | Err(SalvageError::Manifest { .. }) => {}
            Err(e) => return Err(e),
        }
    }
    out.sort_by(|a, b| a.dir.cmp(&b.dir));
    Ok(out)
}

#[cfg(test)]
mod tests;
