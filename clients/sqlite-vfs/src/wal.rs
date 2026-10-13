//! The local WAL file's format: frame layout, the recovery scan SQLite would do, folding it
//! into the db file, and the sidecar's claim on it ([`WalClaim`]).

use std::collections::BTreeMap;
use std::fs::{self};
use std::os::unix::fs::FileExt;

use crate::error::Error;
use crate::frame::PAGE;

/// The WAL header's size, a frame header's, and a frame's (header and page), as file offsets.
pub(crate) const WAL_HDR: i64 = WAL_HDR_LEN as i64;
pub(crate) const FRAME_HDR: i64 = FRAME_HDR_LEN as i64;
pub(crate) const FRAME: i64 = FRAME_LEN as i64;

/// Where frame 0's page starts in the WAL file, and a page's size as a file offset.
pub(crate) const FIRST_PAGE: i64 = WAL_HDR + FRAME_HDR;
pub(crate) const PAGE_BYTES: i64 = PAGE as i64;

/// Whether `off` is where a frame's page starts in the WAL file.
pub(crate) fn page_at(off: i64) -> bool {
    off >= FIRST_PAGE && off.saturating_sub(FIRST_PAGE).checked_rem(FRAME) == Some(0)
}

/// The same sizes, as buffer lengths.
pub(crate) const WAL_HDR_LEN: usize = 32;
pub(crate) const FRAME_HDR_LEN: usize = 24;
pub(crate) const FRAME_LEN: usize = FRAME_HDR_LEN + PAGE;
const FIRST_PAGE_AT: u64 = (WAL_HDR_LEN + FRAME_HDR_LEN) as u64;

/// The db file offset of page `pgno` (1-based).
pub(crate) fn page_offset(pgno: u32) -> u64 {
    db_len(pgno.saturating_sub(1))
}

/// The whole pages in a db file of `len` bytes.
pub(crate) fn pages_in(len: u64) -> u32 {
    const PAGE_LEN: u64 = PAGE as u64;
    u32::try_from(len / PAGE_LEN).unwrap_or(u32::MAX)
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

/// What a sidecar needs of the local WAL, so attach can check it against the files instead of
/// inferring it: the WAL's generation (the salts of its header, which SQLite changes at every
/// restart) and the frame number of the last acknowledged commit frame. Frame 0: the WAL holds no
/// commit, and the db file alone holds the state, fsynced before the sidecar was written. For a
/// frame 0 claim the salts field holds the db file's fingerprint instead (`local::db_fingerprint`,
/// 0 when none was recorded: a sidecar an attach in progress wrote, or an older extension's).
///
/// Why that suffices for any crash-consistent image of the files (a process crash, or a disk
/// snapshot restored without a reboot): SQLite starts a WAL generation only once every frame of
/// the previous one is in the db file, and the db file is fsynced before the new generation's
/// header can reach the disk (`commit`), so an image that shows the generation holds every page
/// from before it; the frames up to the claimed one hold every later commit up to the sidecar's
/// offset. Any page of the image that is newer (backfilled from a later commit) belongs to a
/// commit after the offset, which attach replays.
///
/// The sidecar advances at the commit point, before any frame of the commit reaches the local WAL
/// (`vfs::commit`), and nothing else of the VFS writes WAL frames but the copies of a commit frame
/// SQLite writes after it to pad a commit to a sector (`WalMatch::Covered`). So a commit frame past
/// the claim is no commit of the VFS: it came from a connection outside the extension.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WalClaim {
    pub(crate) salts: u64,
    pub(crate) frame: u32,
}

/// What the local WAL holds, measured against a sidecar's claim ([`WalClaim::check`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WalMatch {
    /// The claimed generation, its last commit at the claimed frame, or past it only by copies of
    /// the claimed commit frame (same page, same database size, same bytes), which SQLite writes to
    /// pad a commit to a sector when the volume does not promise power-safe overwrites; for a
    /// frame 0 claim, no commit at all. `end` is the last commit frame.
    Covered { end: u32 },
    /// The claimed generation, ending before the claimed commit: frames the sidecar counted on never
    /// reached the WAL (a crash between the sidecar and the commit's WAL writes, or a disk image
    /// whose sidecar reached the disk first). Every frame it holds is this VFS's own.
    Behind,
    /// A commit frame past the claim that is no padding copy: another writer's.
    PastClaim { last: u32 },
    /// Another generation, or no WAL, under a claim of frames: the WAL was restarted, truncated or
    /// deleted by something other than this VFS, which writes a frame 0 claim before it truncates.
    OtherGeneration,
    /// Commits under a frame 0 claim: written after the VFS last emptied the WAL.
    CommitsUnderNone { last: u32 },
}

impl WalClaim {
    pub(crate) const NONE: WalClaim = WalClaim { salts: 0, frame: 0 };

    /// A frame 0 claim with the db file's `fingerprint` (`local::db_fingerprint`).
    pub(crate) fn none_with(fingerprint: u64) -> WalClaim {
        WalClaim {
            salts: fingerprint,
            frame: 0,
        }
    }

    /// What the local WAL of the db file at `path`, read the way SQLite's recovery reads it, holds
    /// against this claim.
    pub(crate) fn check(self, path: &str) -> WalMatch {
        let scan = wal_recover(&format!("{path}-wal"));
        let last = scan.as_ref().map_or(0, |w| w.last);
        if self.frame == 0 {
            return if last == 0 {
                WalMatch::Covered { end: 0 }
            } else {
                WalMatch::CommitsUnderNone { last }
            };
        }
        let Some(scan) = scan.filter(|w| w.salts == self.salts) else {
            return WalMatch::OtherGeneration;
        };
        if last < self.frame {
            return WalMatch::Behind;
        }
        // Frames are numbered from 1: the claimed commit frame, and those after it to the last.
        let claimed = scan.frames.get(self.frame.saturating_sub(1) as usize);
        let past = scan
            .frames
            .get(self.frame as usize..last as usize)
            .unwrap_or_default();
        match claimed {
            Some(commit) if past.iter().all(|f| f == commit) => WalMatch::Covered { end: last },
            _ => WalMatch::PastClaim { last },
        }
    }
}

/// What SQLite's recovery (`walIndexRecover`) finds in a WAL file. The valid frames are read in
/// order while their salts match the header's and the cumulative checksum holds.
struct WalScan {
    /// The generation: the header's salts.
    pub(crate) salts: u64,
    /// The number of the last commit frame among the valid ones (0: none).
    pub(crate) last: u32,
    /// The db size (pages) after that commit.
    pub(crate) size: u32,
    /// The page number of each frame up to `last`, in order.
    pub(crate) pages: Vec<u32>,
    /// What each frame up to `last` holds, in order: a padding copy of a commit frame holds the
    /// same (`WalClaim::check`).
    pub(crate) frames: Vec<FrameId>,
}

/// A WAL frame's content, as far as telling a padding copy of a commit frame from it goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FrameId {
    pgno: u32,
    commit_size: u32,
    page_crc: u32,
}

/// A WAL frame's header.
struct FrameHeader {
    pgno: u32,
    /// The db size after a commit (0: not a commit frame).
    commit_size: u32,
    salts: [u8; 8],
    checksum: (u32, u32),
}

fn frame_header(h: &[u8; FRAME_HDR_LEN]) -> Option<FrameHeader> {
    let (salts, _) = h.get(8..)?.split_first_chunk::<8>()?;
    Some(FrameHeader {
        pgno: be32(h, 0)?,
        commit_size: be32(h, 4)?,
        salts: *salts,
        checksum: (be32(h, 16)?, be32(h, 20)?),
    })
}

/// `None` without a valid header (missing, short, torn, or not 4 KiB pages).
fn wal_recover(path: &str) -> Option<WalScan> {
    use std::io::Read;
    let mut r = std::io::BufReader::new(fs::File::open(path).ok()?);
    let mut hdr = [0u8; WAL_HDR_LEN];
    r.read_exact(&mut hdr).ok()?;
    let (summed, _) = hdr.split_first_chunk::<24>()?;
    let (salts, _) = hdr.get(16..)?.split_first_chunk::<8>()?;
    let magic = be32(&hdr, 0)?;
    if magic & !1 != 0x377f_0682 || be32(&hdr, 4)? != 3_007_000 || be32(&hdr, 8)? as usize != PAGE {
        return None;
    }
    // The checksum reads 32-bit words big-endian when the magic's low bit is set.
    let big_endian = magic & 1 == 1;
    let word = |w: [u8; 4]| {
        if big_endian {
            u32::from_be_bytes(w)
        } else {
            u32::from_le_bytes(w)
        }
    };
    let sum = |mut s: (u32, u32), b: &[u8]| {
        for c in b.chunks_exact(8) {
            if let (Some(w0), Some(w1)) = (c.first_chunk::<4>(), c.last_chunk::<4>()) {
                s.0 = s.0.wrapping_add(word(*w0)).wrapping_add(s.1);
                s.1 = s.1.wrapping_add(word(*w1)).wrapping_add(s.0);
            }
        }
        s
    };
    let mut s = sum((0, 0), summed);
    if s != (be32(&hdr, 24)?, be32(&hdr, 28)?) {
        return None;
    }
    let (mut last, mut size, mut pages, mut frames) = (0, 0, Vec::new(), Vec::new());
    let mut f = vec![0u8; FRAME_LEN];
    while r.read_exact(&mut f).is_ok() {
        let Some((h, page)) = f.split_first_chunk::<FRAME_HDR_LEN>() else {
            break;
        };
        let Some(h_summed) = h.first_chunk::<8>() else {
            break;
        };
        let Some(h) = frame_header(h) else {
            break;
        };
        if h.pgno == 0 || h.salts != *salts {
            break;
        }
        s = sum(sum(s, h_summed), page);
        if s != h.checksum {
            break;
        }
        pages.push(h.pgno);
        frames.push(FrameId {
            pgno: h.pgno,
            commit_size: h.commit_size,
            page_crc: crc32c::crc32c(page),
        });
        if h.commit_size != 0 {
            last = u32::try_from(pages.len()).ok()?;
            size = h.commit_size;
        }
    }
    pages.truncate(last as usize);
    frames.truncate(last as usize);
    Some(WalScan {
        salts: u64::from_be_bytes(*salts),
        last,
        size,
        pages,
        frames,
    })
}

/// Folds the local WAL into the db file `f` the way a complete checkpoint would (the valid frames
/// up to the last commit, a later frame winning, pages past that commit's db size dropped, the file
/// cut to it), without opening the file through SQLite: trust (`WalClaim`) only says every page
/// holds the state at the sidecar's offset or a later commit's, not that SQLite can read the file
/// (a disk image may hold a torn page 1 that replay rewrites). The caller holds `f` locked against
/// other processes (`lock_unused`).
pub(crate) fn fold_wal(path: &str, f: &fs::File) -> Result<(), Error> {
    let wal = format!("{path}-wal");
    let Some(scan) = wal_recover(&wal).filter(|w| w.last > 0) else {
        return Ok(());
    };
    let err = |source| Error::Io {
        op: "fold its WAL into",
        path: path.to_owned(),
        source,
    };
    let w = fs::File::open(&wal).map_err(err)?;
    // Frame index per page, a later frame winning.
    let latest: BTreeMap<u32, u64> = scan.pages.iter().copied().zip(0..).collect();
    let mut page = vec![0u8; PAGE];
    for (&pgno, &i) in latest.range(1..=scan.size) {
        // Within the WAL file: no overflow.
        let at = i
            .saturating_mul(FRAME_LEN as u64)
            .saturating_add(FIRST_PAGE_AT);
        w.read_exact_at(&mut page, at).map_err(err)?;
        f.write_all_at(&page, page_offset(pgno)).map_err(err)?;
    }
    f.set_len(db_len(scan.size)).map_err(err)
}
