//! Capture at the commit point (design: Capture): the WAL file's layout, the shape of every WAL
//! write a write transaction makes, and the transaction's final page images.
//!
//! [`classify`] names a write by its offset and length. [`WriteTxn`] follows one write transaction
//! from its WRITE lock: it records each frame's page number and the crc32c of the page this
//! process wrote, and reports the commit point, the page data of the frame whose header carries a
//! non-zero "database size after commit". [`WriteTxn::capture`] then builds the [`CommitRecord`]
//! from the commit frame's page in the buffer and the earlier frames read back from the WAL file,
//! each checked against the crc of what was written. Writes after the commit point (checksum
//! rewrites, sector padding) are the rest of a captured transaction and are not captured. Every
//! other write shape fails the write alone as unsupported. [`WriteTxn::check_size`] refuses a
//! commit of more page bytes than the commit limit before anything is read back.
//!
//! Pure: reading the WAL back is the caller's closure.

use std::collections::BTreeMap;

use crate::codec::PAGE;
use crate::codec::frame::CommitRecord;

/// The WAL header's size, a frame header's, and a frame's (header and page), as file offsets.
pub(crate) const WAL_HDR: i64 = WAL_HDR_LEN as i64;
pub(crate) const FRAME_HDR: i64 = FRAME_HDR_LEN as i64;
pub(crate) const FRAME: i64 = FRAME_LEN as i64;

/// Where frame 0's page starts in the WAL file, and a page's size as a file offset.
pub(crate) const FIRST_PAGE: i64 = WAL_HDR + FRAME_HDR;
pub(crate) const PAGE_BYTES: i64 = PAGE as i64;

/// The same sizes, as buffer lengths.
pub(crate) const WAL_HDR_LEN: usize = 32;
pub(crate) const FRAME_HDR_LEN: usize = 24;
pub(crate) const FRAME_LEN: usize = FRAME_HDR_LEN + PAGE;

/// The commit limit by default, in bytes of pages (`URSULA_VFS_MAX_COMMIT_BYTES`, design: Capture).
pub(crate) const DEFAULT_MAX_COMMIT_BYTES: u64 = 128 << 20;
/// The smallest commit limit: one page.
pub(crate) const MIN_COMMIT_BYTES: u64 = PAGE as u64;
/// The largest commit limit. Its compressed record stays well under the u32 a frame body's length
/// holds (zstd grows incompressible input by less than 1%).
pub(crate) const MAX_COMMIT_BYTES: u64 = 1 << 30;
const _: () = assert!(MAX_COMMIT_BYTES <= u32::MAX as u64 / 2);
const _: () = assert!(DEFAULT_MAX_COMMIT_BYTES <= MAX_COMMIT_BYTES);

/// Whether `off` is where a frame's page starts in the WAL file.
pub(crate) fn page_at(off: i64) -> bool {
    off >= FIRST_PAGE && off.saturating_sub(FIRST_PAGE).checked_rem(FRAME) == Some(0)
}

/// Whether `off` is where a frame's header starts in the WAL file.
fn frame_header_at(off: i64) -> bool {
    off >= WAL_HDR && off.saturating_sub(WAL_HDR).checked_rem(FRAME) == Some(0)
}

/// The db file offset of page `pgno` (1-based).
pub(crate) fn page_offset(pgno: u32) -> u64 {
    db_len(pgno.saturating_sub(1))
}

/// The length of a db file of `pages` pages.
pub(crate) fn db_len(pages: u32) -> u64 {
    u64::from(pages).saturating_mul(PAGE as u64)
}

/// The big-endian u32 at `at` in `b`.
pub(crate) fn be32(b: &[u8], at: usize) -> Option<u32> {
    let w = b.get(at..)?.first_chunk::<4>()?;
    Some(u32::from_be_bytes(*w))
}

/// A WAL write, by its offset and length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalWrite<'a> {
    /// The WAL header (a new WAL generation), with the page size it names.
    Header { page_size: Option<u32> },
    /// A frame header: the frame's page number and the database size after a commit (0: not a
    /// commit frame).
    FrameHeader {
        off: i64,
        pgno: u32,
        commit_size: u32,
    },
    /// A frame's page.
    Page { off: i64, data: &'a [u8] },
    /// Any other shape.
    Other { off: i64, len: usize },
}

/// Names the WAL write of `data` at `off`.
pub(crate) fn classify(off: i64, data: &[u8]) -> WalWrite<'_> {
    match data.len() {
        WAL_HDR_LEN if off == 0 => WalWrite::Header {
            page_size: be32(data, 8),
        },
        FRAME_HDR_LEN if frame_header_at(off) => match (be32(data, 0), be32(data, 4)) {
            (Some(pgno), Some(commit_size)) => WalWrite::FrameHeader {
                off,
                pgno,
                commit_size,
            },
            _ => WalWrite::Other {
                off,
                len: data.len(),
            },
        },
        PAGE if page_at(off) => WalWrite::Page { off, data },
        len => WalWrite::Other { off, len },
    }
}

/// The commit point: the commit frame's page is being written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommitPoint {
    /// The database size after the commit, in pages.
    pub(crate) db_pages_after: u32,
    /// The commit frame's page number.
    pub(crate) pgno: u32,
}

/// A write transaction's WAL write or commit that the extension does not capture.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CaptureError {
    #[error("a WAL write of {len} bytes at offset {off}: not a header, frame header or page")]
    Shape { off: i64, len: usize },
    #[error("a WAL header naming page size {page_size:?}: the extension requires 4096-byte pages")]
    PageSize { page_size: Option<u32> },
    #[error("a frame header at offset {off} while the header at {pending} has no page yet")]
    HeaderWithoutPage { off: i64, pending: i64 },
    #[error("a page at offset {off} that no frame header of the transaction precedes")]
    PageWithoutHeader { off: i64 },
    #[error("frame {pgno} of page 0 at offset {off}")]
    PageZero { off: i64, pgno: u32 },
    #[error(
        "the commit leaves WAL mode: page 1 names read version {read} and write version {write}"
    )]
    LeavesWal { read: u8, write: u8 },
    #[error("the commit changes the page size: page 1 names {found}")]
    Page1Size { found: u32 },
    #[error("page {pgno} at WAL offset {off} reads back other bytes than this process wrote")]
    LostWrite { pgno: u32, off: i64 },
    #[error("reading page {pgno} back from WAL offset {off}: {source}")]
    ReadBack {
        pgno: u32,
        off: i64,
        #[source]
        source: std::io::Error,
    },
    #[error("a commit frame page of {len} bytes")]
    CommitPage { len: usize },
}

/// A commit of more page bytes than the commit limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a commit of {bytes} bytes of pages, at most {max}")]
pub(crate) struct TooLarge {
    pub(crate) bytes: u64,
    pub(crate) max: u64,
}

/// A frame the transaction wrote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Written {
    pgno: u32,
    /// The crc32c of the page as this process last wrote it.
    crc: u32,
}

/// Where the transaction stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    /// Frames are being written. `pending` is a frame header whose page has not followed yet.
    Open { pending: Option<Pending> },
    /// The commit frame's page was reached: the rest is not captured.
    Committed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Pending {
    off: i64,
    pgno: u32,
    commit_size: u32,
}

/// One write transaction's frames, from its WRITE lock to its commit point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteTxn {
    /// Frame page offset → what was written there. A frame header written at or below the highest
    /// frame drops every frame from its offset on: `ROLLBACK TO` after a cache spill rewinds the
    /// WAL and writes over the frames it undid.
    frames: BTreeMap<i64, Written>,
    stage: Stage,
}

impl Default for WriteTxn {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteTxn {
    pub(crate) fn new() -> Self {
        WriteTxn {
            frames: BTreeMap::new(),
            stage: Stage::Open { pending: None },
        }
    }

    /// Takes the transaction's next WAL write. Returns the commit point when `w` is the commit
    /// frame's page, which the caller captures before forwarding it.
    pub(crate) fn on_write(
        &mut self,
        w: &WalWrite<'_>,
    ) -> Result<Option<CommitPoint>, CaptureError> {
        let Stage::Open { pending } = self.stage else {
            return Ok(None);
        };
        match *w {
            WalWrite::Header { page_size } => {
                if page_size != Some(PAGE as u32) {
                    return Err(CaptureError::PageSize { page_size });
                }
                if let Some(p) = pending {
                    return Err(CaptureError::HeaderWithoutPage {
                        off: 0,
                        pending: p.off,
                    });
                }
                // A restart, or a rewind to frame 0 that writes the header again.
                self.frames.clear();
                Ok(None)
            }
            WalWrite::FrameHeader {
                off,
                pgno,
                commit_size,
            } => {
                if let Some(p) = pending {
                    return Err(CaptureError::HeaderWithoutPage {
                        off,
                        pending: p.off,
                    });
                }
                if pgno == 0 {
                    return Err(CaptureError::PageZero { off, pgno });
                }
                // A rewind: the frames from here on were undone.
                drop(self.frames.split_off(&off.saturating_add(FRAME_HDR)));
                self.stage = Stage::Open {
                    pending: Some(Pending {
                        off,
                        pgno,
                        commit_size,
                    }),
                };
                Ok(None)
            }
            WalWrite::Page { off, data } => {
                let crc = crc32c::crc32c(data);
                match pending {
                    Some(p) if p.off.checked_add(FRAME_HDR) == Some(off) => {
                        self.frames.insert(off, Written { pgno: p.pgno, crc });
                        if p.commit_size == 0 {
                            self.stage = Stage::Open { pending: None };
                            return Ok(None);
                        }
                        self.stage = Stage::Committed;
                        Ok(Some(CommitPoint {
                            db_pages_after: p.commit_size,
                            pgno: p.pgno,
                        }))
                    }
                    Some(_) => Err(CaptureError::PageWithoutHeader { off }),
                    // A page the transaction already wrote, rewritten in place.
                    None => match self.frames.get_mut(&off) {
                        Some(written) => {
                            written.crc = crc;
                            Ok(None)
                        }
                        None => Err(CaptureError::PageWithoutHeader { off }),
                    },
                }
            }
            WalWrite::Other { off, len } => Err(CaptureError::Shape { off, len }),
        }
    }

    /// The final image of every page up to `size`, but the commit frame's page `commit_pgno`
    /// (which is in the caller's buffer): page number, the page's WAL offset and the crc32c of
    /// what this process wrote there, by ascending page number. The frames are walked in offset
    /// order, a later frame of a page winning; the commit frame is the last of them.
    pub(crate) fn final_frames(&self, size: u32, commit_pgno: u32) -> Vec<(u32, i64, u32)> {
        let mut latest: BTreeMap<u32, (i64, u32)> = BTreeMap::new();
        for (&off, w) in &self.frames {
            latest.insert(w.pgno, (off, w.crc));
        }
        latest
            .range(1..=size)
            .filter(|&(&pgno, _)| pgno != commit_pgno)
            .map(|(&pgno, &(off, crc))| (pgno, off, crc))
            .collect()
    }

    /// How many pages the commit's record holds: exactly the length of the record
    /// [`WriteTxn::capture`] builds, with nothing read back.
    pub(crate) fn pages(&self, point: CommitPoint) -> usize {
        let earlier = self.final_frames(point.db_pages_after, point.pgno).len();
        if point.pgno <= point.db_pages_after {
            earlier.saturating_add(1)
        } else {
            earlier
        }
    }

    /// Refuses a commit whose pages take more than `max` bytes (design: Capture).
    pub(crate) fn check_size(&self, point: CommitPoint, max: u64) -> Result<(), TooLarge> {
        let pages = u64::try_from(self.pages(point)).unwrap_or(u64::MAX);
        let bytes = pages.saturating_mul(PAGE as u64);
        if bytes > max {
            return Err(TooLarge { bytes, max });
        }
        Ok(())
    }

    /// The commit's record: the final image of every page the transaction wrote up to the
    /// commit's size, the earlier frames read back with `read_at` and checked against what this
    /// process wrote, the commit frame's page from `commit_page`. Page 1, when written, must keep
    /// WAL mode and 4 KiB pages.
    pub(crate) fn capture(
        &self,
        point: CommitPoint,
        commit_page: &[u8],
        mut read_at: impl FnMut(i64, &mut [u8; PAGE]) -> std::io::Result<()>,
    ) -> Result<CommitRecord, CaptureError> {
        let commit_image: &[u8; PAGE] =
            commit_page
                .try_into()
                .map_err(|_wrong_length| CaptureError::CommitPage {
                    len: commit_page.len(),
                })?;
        let mut pages = Vec::new();
        for (pgno, off, crc) in self.final_frames(point.db_pages_after, point.pgno) {
            let mut image = Box::new([0u8; PAGE]);
            read_at(off, &mut image).map_err(|source| CaptureError::ReadBack {
                pgno,
                off,
                source,
            })?;
            if crc32c::crc32c(image.as_slice()) != crc {
                return Err(CaptureError::LostWrite { pgno, off });
            }
            pages.push((pgno, image));
        }
        if point.pgno <= point.db_pages_after {
            let at = pages.partition_point(|&(p, _)| p < point.pgno);
            pages.insert(at, (point.pgno, Box::new(*commit_image)));
        }
        if let Some((_, page1)) = pages.first().filter(|(pgno, _)| *pgno == 1) {
            check_page1(page1.as_slice())?;
        }
        Ok(CommitRecord {
            db_pages_after: point.db_pages_after,
            pages,
        })
    }
}

/// Page 1 keeps WAL mode (file format read and write versions 2, bytes 18 and 19) and 4096-byte
/// pages (bytes 16 and 17, big-endian).
pub(crate) fn check_page1(p: &[u8]) -> Result<(), CaptureError> {
    let byte = |at: usize| p.get(at).copied().unwrap_or(0);
    let (write, read) = (byte(18), byte(19));
    if write != 2 || read != 2 {
        return Err(CaptureError::LeavesWal { read, write });
    }
    let size = u32::from(u16::from_be_bytes([byte(16), byte(17)]));
    // 1 stands for 65536.
    let found = if size == 1 { 65536 } else { size };
    if found != PAGE as u32 {
        return Err(CaptureError::Page1Size { found });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
