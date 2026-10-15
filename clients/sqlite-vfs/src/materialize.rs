//! Materialize and catch up (design §5.6): bring a trusted generation's db file to the end of its
//! log before any connection opens it.
//!
//! [`materialize`] runs at every open of a trusted generation: open the log (its torn tail cut),
//! fsync it, check page 1 ([`page_one`]: a db file that is not this generation's is refused before
//! anything is written), delete the WAL and `-shm` unread (and a `db-wal` or `db-journal` SQLite
//! outside the extension may have left), re-apply the COMMIT records after the latest BASE
//! (final images per batch, cutting first, so a crash midway replays again), fsync the db, append
//! a BASE. By L3 the db holds every commit up to the BASE and otherwise only pages from records
//! after it, so the result is exactly the state at the end of the valid log; the WAL plays no part.
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
use crate::codec::marker::MarkerError;
use crate::codec::marker::PageOne;
use crate::codec::marker::page_one;
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
    #[error("{}: {source}", .db.display())]
    PageOne {
        db: PathBuf,
        #[source]
        source: MarkerError,
    },
    /// Page 1 of the db file is not this generation's: the file was replaced (a copy, or SQLite
    /// outside the extension), or cut short.
    #[error(
        "{}: page 1 holds \"{}\", not this generation's marker: the db file was replaced",
        .db.display(),
        .found.escape_ascii()
    )]
    Replaced { db: PathBuf, found: Vec<u8> },
}

fn io(op: &'static str, path: &Path) -> impl FnOnce(std::io::Error) -> LocalError {
    let path = path.to_owned();
    move |source| LocalError::Io { op, path, source }
}

/// Checks page 1 of the db file `db` of a generation of `lineage`, before anything writes it: the
/// generation's marker, or an empty file whose log holds no commit the db should hold already
/// (`holds_commit`). Anything else is [`LocalError::Replaced`].
fn check_page_one(
    db: &File,
    db_path: &Path,
    lineage: Lineage,
    holds_commit: bool,
) -> Result<(), LocalError> {
    let len = db.metadata().map_err(io("stat", db_path))?.len();
    let mut head = [0u8; marker::LEN];
    let n = usize::try_from(len.min(marker::LEN as u64)).unwrap_or(marker::LEN);
    let read = head.get_mut(..n).unwrap_or_default();
    db.read_exact_at(read, 0).map_err(io("read", db_path))?;
    match page_one(read, len, lineage, holds_commit) {
        PageOne::Marked | PageOne::Uninitialized => Ok(()),
        PageOne::Foreign { found } => Err(LocalError::Replaced {
            db: db_path.to_owned(),
            found,
        }),
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

/// A frame read from the stream after the log: a CLAIM, or a COMMIT (a PART run joined), and the
/// stream offset just after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForeignFrame {
    pub(crate) frame: Frame,
    pub(crate) end_offset: Offset,
}

/// Brings `g`'s db file to the end of its valid log.
pub(crate) fn materialize(g: &Generation) -> Result<Materialized, LocalError> {
    let (mut log, recovered) = Log::open(&g.log_dir())?;
    log.sync()?;
    let db_path = g.db();
    let db = File::options()
        .read(true)
        .write(true)
        .open(&db_path)
        .map_err(io("open", &db_path))?;
    // A BASE record is written only once the db file holds a commit, and the latest one is never
    // trimmed (its position is above the base it names). A commit after it is applied below.
    let holds_commit = recovered.records.iter().any(|r| match r.body {
        RecordBody::Base { .. } => true,
        RecordBody::Commit { .. } => r.position <= recovered.base,
        RecordBody::Claim { .. } | RecordBody::Ack { .. } => false,
    });
    check_page_one(&db, &db_path, g.lineage, holds_commit)?;
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
/// fsyncs the db and appends a BASE, so a crash midway replays. Refused while this generation has
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
    let db_path = m.generation.db();
    catch_up_files(&mut m.log, &m.db, &db_path, frames)
}

/// [`catch_up`] into a log and db file of any generation, a published one or one being built,
/// with no check of pending commits: the caller knows there are none.
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
    batch.flush(db, lineage, page_written).map_err(|e| match e {
        ApplyError::Io(source) => io("replay into", db_path)(source),
        ApplyError::PageOne(source) => LocalError::PageOne {
            db: db_path.to_owned(),
            source,
        },
    })
}

#[cfg(test)]
mod tests;
