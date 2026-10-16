//! Materialize and catch up (design: Materialize): bring a trusted generation's db file to the end
//! of its log before any connection opens it.
//!
//! [`materialize`] runs at every open of a trusted generation: check page 1 (a db file that is not
//! this generation's, an empty one included, is refused before anything is written, and the open
//! salvages it as found), open the log (its torn tail cut), fsync it, delete the WAL and `-shm`
//! unread (and a `db-wal` or `db-journal` SQLite outside the extension may have left), re-apply
//! the COMMIT records after the latest BASE (final images per batch, cutting first, so a crash
//! midway replays again), fsync the db, append a BASE. By L3 the db holds every commit up to the
//! BASE and otherwise only pages from records after it, so the result is exactly the state at the
//! end of the valid log; the WAL plays no part.
//!
//! [`catch_up`] applies frames a takeover found on the stream after the log: each COMMIT is logged
//! with an ACK, the log fsynced, the frames applied, the db fsynced, a BASE appended.

use std::fs::File;
use std::fs::{self};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::path::PathBuf;

use ursula_headers::Offset;

use crate::applier::ApplyError;
use crate::applier::PageBatch;
use crate::codec::PAGE;
use crate::codec::frame::Decoded;
use crate::codec::frame::Frame;
use crate::codec::frame::FrameError;
use crate::codec::frame::RecordReader;
use crate::codec::frame::decode;
use crate::codec::frame::encode;
use crate::codec::marker;
use crate::codec::marker::SQLITE_MAGIC;
use crate::codec::segment::RecordBody;
use crate::generation::Generation;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::replog::Log;
use crate::replog::LogError;
use crate::replog::Recovered;
use crate::replog::sync_dir;

/// The page images a batch holds before it is written: 8 MiB (8 pages in tests, so batches split).
const BATCH_PAGES: usize = if cfg!(test) { 8 } else { 2048 };

#[cfg(test)]
thread_local! {
    /// Test hook: page writes left before replay fails as a crash would stop it.
    pub(crate) static CRASH_AFTER_PAGES: std::cell::Cell<Option<u64>> =
        const { std::cell::Cell::new(None) };
}

/// Counts a page written by replay; under the test hook, fails once its budget is spent.
fn page_written() -> Result<(), std::io::Error> {
    #[cfg(test)]
    {
        let left = CRASH_AFTER_PAGES.get();
        if let Some(n) = left {
            if n == 0 {
                return Err(std::io::Error::other("injected crash"));
            }
            CRASH_AFTER_PAGES.set(Some(n.saturating_sub(1)));
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LocalError {
    #[error(transparent)]
    Log(#[from] LogError),
    #[error("{op} {}: {source}", .path.display())]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("the COMMIT record at {position:?}: {source}")]
    Frame {
        position: Position,
        #[source]
        source: FrameError,
    },
    #[error("the COMMIT record at {position:?} holds no whole COMMIT frame")]
    NotACommit { position: Position },
    #[error("a PART frame ending at stream offset {end_offset} to catch up: runs are joined first")]
    Part { end_offset: Offset },
    #[error(
        "catching up with stream frames while this generation's commit at {first:?} is pending: \
         a takeover with pending commits rebuilds instead"
    )]
    #[cfg(test)]
    OwnPending { first: Position },
    #[error("encoding a frame to catch up: {0}")]
    Encode(#[source] FrameError),
    #[error(transparent)]
    Apply(#[from] ApplyError),
    /// Page 1 of the db file is not this generation's marker: the file was replaced (a copy, or
    /// another generation's), emptied or cut short.
    #[error(
        "{}: page 1 holds \"{}\", not this generation's marker: the db file was replaced, \
         emptied or cut short",
        .db.display(),
        .found.escape_ascii()
    )]
    Replaced { db: PathBuf, found: Vec<u8> },
    /// Page 1 holds SQLite's own magic: a plain SQLite file copied over the db, or a cache
    /// written before page 1 was marked.
    #[error(
        "{}: page 1 holds SQLite's own header, not the page-1 marker: a plain SQLite file, not \
         this generation's db",
        .db.display()
    )]
    Unmarked { db: PathBuf },
}

impl LocalError {
    /// The stream's content is at fault, not the local files: a commit or a snapshot whose page 1
    /// is not a SQLite database's.
    pub(crate) fn is_stream_content(&self) -> bool {
        matches!(self, LocalError::Apply(ApplyError::PageOne { .. }))
    }

    /// The db file is not the generation's: the open salvages the generation as `replaced`.
    pub(crate) fn is_replaced(&self) -> bool {
        matches!(
            self,
            LocalError::Replaced { .. } | LocalError::Unmarked { .. }
        )
    }
}

fn io(op: &'static str, path: &Path) -> impl FnOnce(std::io::Error) -> LocalError {
    let path = path.to_owned();
    move |source| LocalError::Io { op, path, source }
}

/// Checks page 1 of the db file `db` of a generation of `lineage`, before anything writes it: the
/// generation's marker. Every generation is published with page 1 written, so an empty file is
/// refused like any other.
fn check_page_one(db: &File, db_path: &Path, lineage: Lineage) -> Result<(), LocalError> {
    let len = db.metadata().map_err(io("stat", db_path))?.len();
    let mut head = [0u8; marker::LEN];
    let n = usize::try_from(len.min(marker::LEN as u64)).unwrap_or(marker::LEN);
    let read = head.get_mut(..n).unwrap_or_default();
    db.read_exact_at(read, 0).map_err(io("read", db_path))?;
    if marker::is_marked(read, lineage) {
        Ok(())
    } else if read == SQLITE_MAGIC {
        Err(LocalError::Unmarked {
            db: db_path.to_owned(),
        })
    } else {
        Err(LocalError::Replaced {
            db: db_path.to_owned(),
            found: read.to_vec(),
        })
    }
}

/// A materialized generation: its log open for appends and its db file.
#[derive(Debug)]
pub(crate) struct Materialized {
    pub(crate) generation: Generation,
    pub(crate) log: Log,
    pub(crate) db: File,
    /// What the log held at open.
    pub(crate) recovered: Recovered,
}

/// Whether a trusted generation holds data a missing stream or a history change would lose
/// (design: Open): its db file is longer than one page (every published db holds page 1), or its
/// valid log holds a COMMIT record. A generation without data is replaced with no salvage.
pub(crate) fn holds_data(m: &Materialized) -> bool {
    let longer =
        m.db.metadata()
            .map_or(true, |meta| meta.len() > PAGE as u64);
    longer || m.recovered.any_commit
}

/// A frame read from the stream after the log: a CLAIM, or a COMMIT (a PART run joined), and the
/// stream offset just after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForeignFrame {
    pub(crate) frame: Frame,
    pub(crate) end_offset: Offset,
}

/// Brings `g`'s db file to the end of its valid log.
pub(crate) fn materialize(g: &Generation) -> Result<Materialized, LocalError> {
    // Page 1 first, so a refused generation is left exactly as it was found.
    let db_path = g.db();
    let db = File::options()
        .read(true)
        .write(true)
        .open(&db_path)
        .map_err(io("open", &db_path))?;
    check_page_one(&db, &db_path, g.lineage)?;
    let (mut log, recovered) = Log::open(&g.log_dir())?;
    log.sync()?;
    let mut removed = false;
    let [stray_wal, journal] = g.strays();
    for path in [g.wal(), g.shm(), stray_wal, journal] {
        match fs::remove_file(&path) {
            Ok(()) => removed = true,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io("remove", &path)(e)),
        }
    }
    if removed {
        sync_dir(&g.dir).map_err(io("fsync", &g.dir))?;
    }
    let commits = recovered.after_base().filter_map(|r| match &r.body {
        RecordBody::Commit { frame } => Some((r.position, frame.as_slice())),
        _ => None,
    });
    let applied = apply(&db, &db_path, g.lineage, commits)?;
    let mut m = Materialized {
        generation: g.clone(),
        log,
        db,
        recovered,
    };
    if applied {
        base(&mut m)?;
    }
    Ok(m)
}

/// Logs the foreign `frames` (COMMITs, then one ACK at the last), fsyncs the log, applies them,
/// fsyncs the db and appends a BASE, so a crash midway replays, then trims the log (F3). Refused while this generation has
/// pending commits: commits after both the log's latest ACK and `acked`, the position the caller
/// knows acknowledged (a takeover's reconcile acknowledges commits before any ACK names them).
#[cfg(test)]
pub(crate) fn catch_up(
    m: &mut Materialized,
    acked: Position,
    frames: &[ForeignFrame],
) -> Result<(), LocalError> {
    if frames.is_empty() {
        return Ok(());
    }
    // Foreign frames go after our pending commits only by discarding them, which rebuilds.
    let acked = acked.max(m.log.acked());
    if let Some(&(first, _)) = m.recovered.pending().iter().find(|(p, _)| *p > acked) {
        return Err(LocalError::OwnPending { first });
    }
    catch_up_in_place(m, frames)
}

/// [`catch_up_files`] into a published generation, then the log trimmed by the one policy owners
/// trim by after an ACK ([`Log::trim`], F3: segments wholly at or below `min(base, acked)`), so a
/// reader that only ever catches up keeps one segment and the records since its base instead of
/// every frame it read. Never inside a build, as a second guard: every segment names the
/// generation a build supersedes, which a crash after the publish rename needs
/// ([`Building::publish`]), and a build's log is trimmed only once published
/// ([`trim_published`]).
///
/// [`Building::publish`]: crate::generation::Building::publish
pub(crate) fn catch_up_in_place(
    m: &mut Materialized,
    frames: &[ForeignFrame],
) -> Result<(), LocalError> {
    let db_path = m.generation.db();
    catch_up_files(&mut m.log, &m.db, &db_path, frames)?;
    trim_published(m)
}

/// Trims the log of a published generation (F3): the generation it superseded is gone, so its
/// first segment, which names it, is no longer needed.
pub(crate) fn trim_published(m: &mut Materialized) -> Result<(), LocalError> {
    m.log.trim()?;
    Ok(())
}

/// [`catch_up`] into a log and db file of any generation, a published one or one being built,
/// with no check of pending commits: the caller knows there are none. It trims nothing: a build's
/// log keeps its first segment until the build is published ([`catch_up_in_place`]).
pub(crate) fn catch_up_files(
    log: &mut Log,
    db: &File,
    db_path: &Path,
    frames: &[ForeignFrame],
) -> Result<(), LocalError> {
    let Some(last) = frames.last() else {
        return Ok(());
    };
    let lineage = log.lineage();
    let mut log_bytes = log.log_bytes();
    let mut logged = Vec::new();
    for f in frames {
        let bytes = match &f.frame {
            Frame::Part { .. } => {
                return Err(LocalError::Part {
                    end_offset: f.end_offset,
                });
            }
            frame => encode(frame).map_err(LocalError::Encode)?,
        };
        log_bytes = log_bytes.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        if matches!(f.frame, Frame::Commit { .. }) {
            let position = log.append(&RecordBody::Commit { frame: bytes })?;
            logged.push(position);
        }
    }
    let acked = logged.last().copied().unwrap_or_else(|| log.acked());
    log.append(&RecordBody::Ack {
        acked,
        acked_offset: last.end_offset,
        log_bytes,
    })?;
    log.sync()?;
    let commits = frames
        .iter()
        .filter_map(|f| match &f.frame {
            Frame::Commit { compressed, .. } => Some(compressed.as_slice()),
            _ => None,
        })
        .zip(logged)
        .map(|(compressed, position)| (position, compressed));
    if apply_compressed(db, db_path, lineage, commits)? {
        base_files(log, db, db_path)?;
    }
    Ok(())
}

/// fsyncs the db, then appends a BASE naming the last record. That is right only before binding,
/// when the db holds every logged commit; F2 in the switch PR must name the last confirmed
/// position instead, since a logged commit may be undecided then.
fn base(m: &mut Materialized) -> Result<(), LocalError> {
    let db_path = m.generation.db();
    base_files(&mut m.log, &m.db, &db_path)
}

fn base_files(log: &mut Log, db: &File, db_path: &Path) -> Result<(), LocalError> {
    db.sync_all().map_err(io("fsync", db_path))?;
    let last = Position(log.next_position().0.saturating_sub(1));
    log.append(&RecordBody::Base { position: last })?;
    Ok(())
}

/// Applies COMMIT records (whole `USQ2` frames) in order to the db file of a generation of
/// `lineage`. Returns whether there was any.
fn apply<'a>(
    db: &File,
    db_path: &Path,
    lineage: Lineage,
    commits: impl Iterator<Item = (Position, &'a [u8])>,
) -> Result<bool, LocalError> {
    let compressed = commits.map(|(position, frame)| {
        match decode(frame, u64::try_from(frame.len()).unwrap_or(u64::MAX)) {
            Ok(Decoded::Frame {
                frame: Frame::Commit { compressed, .. },
                len,
            }) if len == frame.len() => Ok((position, compressed)),
            Ok(_) => Err(LocalError::NotACommit { position }),
            Err(source) => Err(LocalError::Frame { position, source }),
        }
    });
    let mut batch = PageBatch::default();
    let mut any = false;
    for item in compressed {
        let (position, compressed) = item?;
        add(&mut batch, db, db_path, lineage, position, &compressed)?;
        any = true;
    }
    flush(&mut batch, db, db_path, lineage)?;
    Ok(any)
}

/// Applies compressed commit records in order. Returns whether there was any.
fn apply_compressed<'a>(
    db: &File,
    db_path: &Path,
    lineage: Lineage,
    commits: impl Iterator<Item = (Position, &'a [u8])>,
) -> Result<bool, LocalError> {
    let mut batch = PageBatch::default();
    let mut any = false;
    for (position, compressed) in commits {
        add(&mut batch, db, db_path, lineage, position, compressed)?;
        any = true;
    }
    flush(&mut batch, db, db_path, lineage)?;
    Ok(any)
}

/// Adds one commit to the batch, writing the batch out whenever it holds [`BATCH_PAGES`] pages.
fn add(
    batch: &mut PageBatch,
    db: &File,
    db_path: &Path,
    lineage: Lineage,
    position: Position,
    compressed: &[u8],
) -> Result<(), LocalError> {
    let frame_error = |source| LocalError::Frame { position, source };
    let mut reader = RecordReader::new(compressed).map_err(frame_error)?;
    batch.begin(reader.header().db_pages_after);
    loop {
        let mut image = Box::new([0u8; PAGE]);
        let Some(pgno) = reader.next_page(&mut image).map_err(frame_error)? else {
            return Ok(());
        };
        batch.page(pgno, image);
        if batch.len() >= BATCH_PAGES {
            flush(batch, db, db_path, lineage)?;
        }
    }
}

fn flush(
    batch: &mut PageBatch,
    db: &File,
    db_path: &Path,
    lineage: Lineage,
) -> Result<(), LocalError> {
    Ok(batch.flush(db, db_path, lineage, page_written)?)
}

#[cfg(test)]
mod tests;
