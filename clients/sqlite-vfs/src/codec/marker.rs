//! The page-1 marker: bytes 0..16 of a generation's db file on disk.
//!
//! ```text
//! on disk      "Ursula08" | lineage[0..8]
//! to SQLite    "SQLite format 3\0"
//! ```
//!
//! The VFS translates the 16 bytes on every read and write of a bound db file, so SQLite in the
//! extension sees its own magic, and SQLite outside the extension (another process, a connection
//! opened before the extension loaded, `?vfs=unix`, the `sqlite3` shell) finds no database there
//! and fails with `SQLITE_NOTADB` before it reads or writes anything else. The marker names the
//! generation's lineage, so a db file copied in from another generation is not taken for this one.
//!
//! Every function here is pure: the VFS, the applier and the snapshot install call them on the
//! bytes they move.

use crate::ids::Lineage;

/// The marker's first 8 bytes.
pub const MARKER: [u8; 8] = *b"Ursula08";

/// What a SQLite database file starts with.
pub const SQLITE_MAGIC: [u8; 16] = *b"SQLite format 3\0";

/// How many bytes the marker replaces: SQLite's magic.
pub const LEN: usize = 16;

/// The marker of a generation of `lineage`.
pub fn mark(lineage: Lineage) -> [u8; LEN] {
    let mut out = [0u8; LEN];
    let (head, tail) = out.split_at_mut(MARKER.len());
    head.copy_from_slice(&MARKER);
    tail.copy_from_slice(
        lineage
            .0
            .get(..LEN.saturating_sub(MARKER.len()))
            .unwrap_or_default(),
    );
    out
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MarkerError {
    /// A read of page 1 found other bytes than this generation's marker.
    #[error(
        "page 1 holds \"{}\" at offset {at}, not this generation's marker",
        .found.escape_ascii()
    )]
    NotMarked { at: u64, found: Vec<u8> },
    /// A write of page 1 carries other bytes than SQLite's magic.
    #[error(
        "a write of page 1 carries \"{}\" at offset {at}, not SQLite's magic",
        .found.escape_ascii()
    )]
    NotSqlite { at: u64, found: Vec<u8> },
}

/// The part of `len` bytes at `off` that lies in bytes 0..16: where it starts in them (the buffer's
/// first byte), and how long it is.
fn overlap(off: u64, len: usize) -> Option<(usize, usize)> {
    let start = usize::try_from(off).ok().filter(|&s| s < LEN)?;
    let n = len.min(LEN.saturating_sub(start));
    (n > 0).then_some((start, n))
}

/// Replaces `from` with `to` where `buf` (at `off`) overlaps bytes 0..16. `Ok(false)` when it does
/// not overlap them; `Err` with the bytes found when they are not `from`'s, `buf` unchanged.
fn translate(
    buf: &mut [u8],
    off: u64,
    from: &[u8; LEN],
    to: &[u8; LEN],
) -> Result<bool, (u64, Vec<u8>)> {
    let Some((start, n)) = overlap(off, buf.len()) else {
        return Ok(false);
    };
    let end = start.saturating_add(n);
    let (Some(here), Some(want), Some(with)) =
        (buf.get_mut(..n), from.get(start..end), to.get(start..end))
    else {
        return Ok(false);
    };
    if here != want {
        return Err((off, here.to_vec()));
    }
    here.copy_from_slice(with);
    Ok(true)
}

/// A read of `buf.len()` bytes at `off` of a bound db file: the marker read from disk becomes
/// SQLite's magic. Anything else in bytes 0..16 is an error, `buf` unchanged.
pub fn on_read(buf: &mut [u8], off: u64, lineage: Lineage) -> Result<(), MarkerError> {
    translate(buf, off, &mark(lineage), &SQLITE_MAGIC)
        .map(drop)
        .map_err(|(at, found)| MarkerError::NotMarked { at, found })
}

/// A write of `buf` at `off` to a bound db file: `Some` bytes to write instead when it overlaps
/// bytes 0..16, SQLite's magic replaced by the marker. Anything else there is an error.
pub fn on_write(buf: &[u8], off: u64, lineage: Lineage) -> Result<Option<Vec<u8>>, MarkerError> {
    if overlap(off, buf.len()).is_none() {
        return Ok(None);
    }
    let mut out = buf.to_vec();
    translate(&mut out, off, &SQLITE_MAGIC, &mark(lineage))
        .map_err(|(at, found)| MarkerError::NotSqlite { at, found })?;
    Ok(Some(out))
}

/// Stamps a page-1 image about to be written to a db file outside the VFS (the applier, a snapshot
/// install, an empty db's initialization): SQLite's magic becomes the marker.
pub fn stamp(page: &mut [u8], lineage: Lineage) -> Result<(), MarkerError> {
    translate(page, 0, &SQLITE_MAGIC, &mark(lineage))
        .map(drop)
        .map_err(|(at, found)| MarkerError::NotSqlite { at, found })
}

/// What page 1 of a generation's db file holds, checked before anything opens or replays it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageOne {
    /// This generation's marker.
    Marked,
    /// An empty db file in a generation whose log holds no commit the db should hold already: it
    /// is initialized before it binds.
    Uninitialized,
    /// Anything else: a file SQLite outside the extension wrote, another generation's, or one cut
    /// short. `found` holds its first bytes, at most 16.
    Foreign { found: Vec<u8> },
}

/// Page 1 of a db file of `db_len` bytes whose first bytes are `head` (at most 16), in a generation
/// of `lineage` whose log holds a commit the db should hold already (`holds_commit`) or none.
pub fn page_one(head: &[u8], db_len: u64, lineage: Lineage, holds_commit: bool) -> PageOne {
    if db_len == 0 && !holds_commit {
        return PageOne::Uninitialized;
    }
    if head == mark(lineage) {
        return PageOne::Marked;
    }
    PageOne::Foreign {
        found: head.get(..LEN.min(head.len())).unwrap_or_default().to_vec(),
    }
}

#[cfg(test)]
mod tests;
