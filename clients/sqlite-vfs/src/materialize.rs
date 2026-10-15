//! Materialize and catch up (design §5.6): bring a trusted generation's db file to the end of its
//! log before any connection opens it.
//!
//! [`materialize`] runs at every open of a trusted generation: open the log (its torn tail cut),
//! fsync it, delete the WAL and `-shm` unread, re-apply the COMMIT records after the latest BASE
//! (final images per batch, cutting first, so a crash midway replays again), fsync the db, append
//! a BASE. By L3 the db holds every commit up to the BASE and otherwise only pages from records
//! after it, so the result is exactly the state at the end of the valid log; the WAL plays no part.
//!
//! [`catch_up`] applies frames a takeover found on the stream after the log: each COMMIT is logged
//! with an ACK, the log fsynced, the frames applied, the db fsynced, a BASE appended.

use std::fs::File;
use std::fs::{self};
use std::path::Path;
use std::path::PathBuf;

use ursula_headers::Offset;

use crate::applier::PageBatch;
use crate::codec::PAGE;
use crate::codec::frame::Decoded;
use crate::codec::frame::Frame;
use crate::codec::frame::FrameError;
use crate::codec::frame::RecordReader;
use crate::codec::frame::decode;
use crate::codec::frame::encode;
use crate::codec::segment::RecordBody;
use crate::generation::Generation;
use crate::ids::Position;
use crate::replog::Log;
use crate::replog::LogError;
use crate::replog::Recovered;
use crate::replog::sync_dir;

/// The page images a batch holds before it is written: 8 MiB.
const BATCH_PAGES: usize = 2048;

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
    #[error("encoding a frame to catch up: {0}")]
    Encode(#[source] FrameError),
}

fn io(op: &'static str, path: &Path) -> impl FnOnce(std::io::Error) -> LocalError {
    let path = path.to_owned();
    move |source| LocalError::Io { op, path, source }
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
    let mut removed = false;
    for path in [g.wal(), g.shm()] {
        match fs::remove_file(&path) {
            Ok(()) => removed = true,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io("remove", &path)(e)),
        }
    }
    if removed {
        sync_dir(&g.dir).map_err(io("fsync", &g.dir))?;
    }
    let db_path = g.db();
    let db = File::options()
        .read(true)
        .write(true)
        .open(&db_path)
        .map_err(io("open", &db_path))?;
    let commits = recovered.after_base().filter_map(|r| match &r.body {
        RecordBody::Commit { frame } => Some((r.position, frame.as_slice())),
        _ => None,
    });
    let applied = apply(&db, &db_path, commits)?;
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
/// fsyncs the db and appends a BASE, so a crash midway replays.
pub(crate) fn catch_up(m: &mut Materialized, frames: &[ForeignFrame]) -> Result<(), LocalError> {
    let Some(last) = frames.last() else {
        return Ok(());
    };
    let mut log_bytes = m.log.log_bytes();
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
            let position = m.log.append(&RecordBody::Commit { frame: bytes })?;
            logged.push(position);
        }
    }
    let acked = logged.last().copied().unwrap_or_else(|| m.log.acked());
    m.log.append(&RecordBody::Ack {
        acked,
        acked_offset: last.end_offset,
        log_bytes,
    })?;
    m.log.sync()?;
    let db_path = m.generation.db();
    let commits = frames
        .iter()
        .filter_map(|f| match &f.frame {
            Frame::Commit { compressed, .. } => Some(compressed.as_slice()),
            _ => None,
        })
        .zip(logged)
        .map(|(compressed, position)| (position, compressed));
    if apply_compressed(&m.db, &db_path, commits)? {
        base(m)?;
    }
    Ok(())
}

/// fsyncs the db, then appends a BASE naming the last record.
fn base(m: &mut Materialized) -> Result<(), LocalError> {
    let db_path = m.generation.db();
    m.db.sync_all().map_err(io("fsync", &db_path))?;
    let last = Position(m.log.next_position().0.saturating_sub(1));
    m.log.append(&RecordBody::Base { position: last })?;
    Ok(())
}

/// Applies COMMIT records (whole `USQ2` frames) in order. Returns whether there was any.
fn apply<'a>(
    db: &File,
    db_path: &Path,
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
    let mut batch: PageBatch<Box<[u8; PAGE]>> = PageBatch::default();
    let mut any = false;
    for item in compressed {
        let (position, compressed) = item?;
        add(&mut batch, db, db_path, position, &compressed)?;
        any = true;
    }
    flush(&mut batch, db, db_path)?;
    Ok(any)
}

/// Applies compressed commit records in order. Returns whether there was any.
fn apply_compressed<'a>(
    db: &File,
    db_path: &Path,
    commits: impl Iterator<Item = (Position, &'a [u8])>,
) -> Result<bool, LocalError> {
    let mut batch: PageBatch<Box<[u8; PAGE]>> = PageBatch::default();
    let mut any = false;
    for (position, compressed) in commits {
        add(&mut batch, db, db_path, position, compressed)?;
        any = true;
    }
    flush(&mut batch, db, db_path)?;
    Ok(any)
}

/// Adds one commit to the batch, writing the batch out whenever it holds [`BATCH_PAGES`] pages.
fn add(
    batch: &mut PageBatch<Box<[u8; PAGE]>>,
    db: &File,
    db_path: &Path,
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
            flush(batch, db, db_path)?;
        }
    }
}

fn flush(
    batch: &mut PageBatch<Box<[u8; PAGE]>>,
    db: &File,
    db_path: &Path,
) -> Result<(), LocalError> {
    batch.flush(db, || {}).map_err(io("replay into", db_path))
}

#[cfg(test)]
mod tests;
