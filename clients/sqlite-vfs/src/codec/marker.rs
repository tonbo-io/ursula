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
//! Every function here is pure: the VFS, the applier, the snapshot install and a generation's
//! publish call them on the bytes they move.

use super::PAGE;
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
    if let Some(prefix) = lineage.0.first_chunk::<8>() {
        tail.copy_from_slice(prefix);
    }
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

/// Whether `head`, the first bytes of a generation's db file, is the marker of `lineage`. An
/// empty or shorter file is not: every generation is published with page 1 written.
pub fn is_marked(head: &[u8], lineage: Lineage) -> bool {
    head == mark(lineage)
}

/// Page 1 of an empty database in WAL mode, marked for a generation of `lineage`: what a
/// generation's db file holds when it is published without a commit (design D7), so no published
/// db file is ever empty or unmarked.
///
/// The bytes are those SQLite writes for a new database (`sqlite3BtreeNewDb`), then WAL mode: a
/// 4096-byte page, file format versions 2 and 2 (WAL), the default payload fractions, one page,
/// change counter 1, schema format 0 and text encoding 0 (SQLite sets both at the first `CREATE`),
/// and an empty table b-tree leaf for `sqlite_schema` at byte 100. It is not a commit: the first
/// commit writes page 1 again (every schema change does), so the stream holds it.
pub fn empty_page_one(lineage: Lineage) -> Box<[u8; PAGE]> {
    let mut page = Box::new([0u8; PAGE]);
    let mut put = |at: usize, bytes: &[u8]| {
        if let Some(to) = page.get_mut(at..at.saturating_add(bytes.len())) {
            to.copy_from_slice(bytes);
        }
    };
    put(0, &mark(lineage));
    // Page size, write and read versions (2: WAL), reserved bytes, payload fractions.
    put(16, &[0x10, 0x00, 2, 2, 0, 64, 32, 32]);
    // File change counter, and the size in pages it validates (bytes 92..96).
    put(24, &1u32.to_be_bytes());
    put(28, &1u32.to_be_bytes());
    put(92, &1u32.to_be_bytes());
    // The empty `sqlite_schema` b-tree: a table leaf (13), no freeblock, no cell, its content
    // area starting at the end of the page, no fragmented bytes.
    put(100, &[13, 0, 0, 0, 0, 0x10, 0x00, 0]);
    page
}

#[cfg(test)]
mod tests;
