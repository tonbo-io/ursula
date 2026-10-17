#![expect(
    clippy::arithmetic_side_effects,
    reason = "tests compute WAL offsets from small frame numbers"
)]

use super::CaptureError;
use super::CommitPoint;
use super::FIRST_PAGE;
use super::FRAME;
use super::TooLarge;
use super::WAL_HDR;
use super::WalWrite;
use super::WriteTxn;
use super::check_page1;
use super::classify;
use crate::codec::PAGE;

/// A WAL file in memory that forwards every write a transaction makes, the way the VFS does: the
/// commit frame's page is captured before it is written.
struct Wal {
    bytes: Vec<u8>,
    txn: WriteTxn,
}

impl Wal {
    fn new() -> Self {
        Wal {
            bytes: Vec::new(),
            txn: WriteTxn::new(),
        }
    }

    fn put(&mut self, off: i64, data: &[u8]) {
        let at = usize::try_from(off).unwrap();
        if self.bytes.len() < at + data.len() {
            self.bytes.resize(at + data.len(), 0);
        }
        self.bytes[at..at + data.len()].copy_from_slice(data);
    }

    /// Writes `data` at `off`; at the commit point returns the captured record, whose page count
    /// the commit limit's check counted without reading anything back.
    fn write(
        &mut self,
        off: i64,
        data: &[u8],
    ) -> Result<Option<crate::codec::frame::CommitRecord>, CaptureError> {
        let point = self.txn.on_write(&classify(off, data))?;
        let record = match point {
            Some(point) => {
                let record = self.txn.capture(point, data, |at, buf| {
                    let at = usize::try_from(at).unwrap();
                    buf.copy_from_slice(&self.bytes[at..at + PAGE]);
                    Ok(())
                })?;
                assert_eq!(self.txn.pages(point), record.pages.len());
                Some(record)
            }
            None => None,
        };
        self.put(off, data);
        Ok(record)
    }

    fn header(&mut self) {
        let mut h = [0u8; 32];
        h[8..12].copy_from_slice(&(PAGE as u32).to_be_bytes());
        assert!(self.write(0, &h).unwrap().is_none());
    }

    /// Writes frame `n`'s header and page; returns the record when it is a commit frame.
    fn frame(
        &mut self,
        n: i64,
        pgno: u32,
        commit_size: u32,
        page: &[u8; PAGE],
    ) -> Option<crate::codec::frame::CommitRecord> {
        let off = WAL_HDR + n * FRAME;
        let mut h = [0u8; 24];
        h[0..4].copy_from_slice(&pgno.to_be_bytes());
        h[4..8].copy_from_slice(&commit_size.to_be_bytes());
        assert!(self.write(off, &h).unwrap().is_none());
        self.write(off + 24, page).unwrap()
    }

    /// Rewrites frame `n`'s page in place.
    fn rewrite(&mut self, n: i64, page: &[u8; PAGE]) {
        assert!(self.write(FIRST_PAGE + n * FRAME, page).unwrap().is_none());
    }
}

fn page(fill: u8) -> [u8; PAGE] {
    [fill; PAGE]
}

/// A page 1 in WAL mode with 4 KiB pages.
fn page1(fill: u8) -> [u8; PAGE] {
    let mut p = page(fill);
    p[16..18].copy_from_slice(&4096u16.to_be_bytes());
    p[18] = 2;
    p[19] = 2;
    p
}

fn images(r: &crate::codec::frame::CommitRecord) -> Vec<(u32, u8)> {
    r.pages.iter().map(|(p, image)| (*p, image[100])).collect()
}

#[test]
fn classify_names_each_write_shape() {
    let mut h = [0u8; 32];
    h[8..12].copy_from_slice(&4096u32.to_be_bytes());
    assert_eq!(classify(0, &h), WalWrite::Header {
        page_size: Some(4096)
    });
    let mut fh = [0u8; 24];
    fh[0..4].copy_from_slice(&7u32.to_be_bytes());
    fh[4..8].copy_from_slice(&9u32.to_be_bytes());
    assert_eq!(classify(WAL_HDR + FRAME, &fh), WalWrite::FrameHeader {
        off: WAL_HDR + FRAME,
        pgno: 7,
        commit_size: 9
    });
    let p = page(1);
    assert!(matches!(
        classify(FIRST_PAGE + 2 * FRAME, &p),
        WalWrite::Page { off, .. } if off == FIRST_PAGE + 2 * FRAME
    ));
    // A page at a frame header's offset, a header at a page's, a partial page.
    assert_eq!(classify(WAL_HDR, &p), WalWrite::Other {
        off: WAL_HDR,
        len: PAGE
    });
    assert_eq!(classify(FIRST_PAGE, &fh), WalWrite::Other {
        off: FIRST_PAGE,
        len: 24
    });
    assert_eq!(classify(FIRST_PAGE, &p[..512]), WalWrite::Other {
        off: FIRST_PAGE,
        len: 512
    });
}

#[test]
fn one_commit_captures_every_page() {
    let mut wal = Wal::new();
    wal.header();
    assert!(wal.frame(0, 1, 0, &page1(1)).is_none());
    assert!(wal.frame(1, 2, 0, &page(2)).is_none());
    let record = wal.frame(2, 3, 3, &page(3)).unwrap();
    assert_eq!(record.db_pages_after, 3);
    assert_eq!(images(&record), vec![(1, 1), (2, 2), (3, 3)]);
}

#[test]
fn in_place_rewrites_inside_a_transaction_capture_the_newest_image() {
    let mut wal = Wal::new();
    wal.header();
    wal.frame(0, 2, 0, &page(2));
    wal.frame(1, 3, 0, &page(3));
    // The page cache spilled pages 2 and 3, then the transaction changed page 2 again: SQLite
    // overwrites the frame's page in place, without its header.
    wal.rewrite(0, &page(20));
    let record = wal.frame(2, 4, 4, &page(4)).unwrap();
    assert_eq!(images(&record), vec![(2, 20), (3, 3), (4, 4)]);
}

#[test]
fn a_commit_frame_after_spilled_frames_of_its_page_takes_the_buffer() {
    let mut wal = Wal::new();
    wal.header();
    wal.frame(0, 5, 0, &page(5));
    wal.frame(1, 6, 0, &page(6));
    // The commit frame is always appended, so its page may have an earlier frame.
    let record = wal.frame(2, 5, 6, &page(50)).unwrap();
    assert_eq!(images(&record), vec![(5, 50), (6, 6)]);
}

#[test]
fn pages_above_the_commit_size_are_dropped() {
    let mut wal = Wal::new();
    wal.header();
    wal.frame(0, 2, 0, &page(2));
    wal.frame(1, 9, 0, &page(9));
    let record = wal.frame(2, 3, 3, &page(3)).unwrap();
    assert_eq!(images(&record), vec![(2, 2), (3, 3)]);
}

#[test]
fn a_transaction_continues_a_wal_without_a_header() {
    let mut wal = Wal::new();
    let record = wal.frame(7, 2, 2, &page(2)).unwrap();
    assert_eq!(images(&record), vec![(2, 2)]);
}

#[test]
fn writes_after_the_commit_point_are_not_captured() {
    let mut wal = Wal::new();
    wal.header();
    wal.frame(0, 2, 0, &page(2));
    wal.rewrite(0, &page(21));
    assert!(wal.frame(1, 3, 3, &page(3)).is_some());
    // Checksum rewrites of earlier frame headers, then sector padding repeating the commit frame.
    let mut h = [0u8; 24];
    h[0..4].copy_from_slice(&2u32.to_be_bytes());
    assert!(wal.write(WAL_HDR, &h).unwrap().is_none());
    assert!(wal.frame(2, 3, 3, &page(3)).is_none());
    assert!(
        wal.write(FIRST_PAGE + 3 * FRAME, &[0u8; 100])
            .unwrap()
            .is_none()
    );
}

#[test]
fn a_read_back_that_differs_is_a_lost_write() {
    let mut wal = Wal::new();
    wal.header();
    wal.frame(0, 2, 0, &page(2));
    let at = usize::try_from(FIRST_PAGE).unwrap();
    wal.bytes[at + 7] ^= 1;
    let mut h = [0u8; 24];
    h[0..4].copy_from_slice(&3u32.to_be_bytes());
    h[4..8].copy_from_slice(&3u32.to_be_bytes());
    wal.write(WAL_HDR + FRAME, &h).unwrap();
    let err = wal.write(FIRST_PAGE + FRAME, &page(3)).unwrap_err();
    assert!(matches!(err, CaptureError::LostWrite { pgno: 2, off } if off == FIRST_PAGE));
}

#[test]
fn a_failed_read_back_names_the_page() {
    let mut txn = WriteTxn::new();
    let mut h = [0u8; 24];
    h[0..4].copy_from_slice(&2u32.to_be_bytes());
    txn.on_write(&classify(WAL_HDR, &h)).unwrap();
    txn.on_write(&classify(FIRST_PAGE, &page(2))).unwrap();
    let point = CommitPoint {
        db_pages_after: 3,
        pgno: 3,
    };
    let err = txn
        .capture(point, &page(3), |_, _| Err(std::io::Error::other("gone")))
        .unwrap_err();
    assert!(matches!(err, CaptureError::ReadBack { pgno: 2, .. }));
}

#[test]
fn other_write_shapes_are_unsupported() {
    let mut txn = WriteTxn::new();
    let err = txn
        .on_write(&classify(FIRST_PAGE + 100, &page(1)))
        .unwrap_err();
    assert!(matches!(err, CaptureError::Shape { len: PAGE, .. }));
    // A page whose frame header was never written.
    let err = txn.on_write(&classify(FIRST_PAGE, &page(1))).unwrap_err();
    assert!(matches!(err, CaptureError::PageWithoutHeader { off } if off == FIRST_PAGE));
    // Two headers in a row.
    let h = [
        0u8, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ];
    txn.on_write(&classify(WAL_HDR, &h)).unwrap();
    let err = txn.on_write(&classify(WAL_HDR + FRAME, &h)).unwrap_err();
    assert!(matches!(err, CaptureError::HeaderWithoutPage { .. }));
}

#[test]
fn a_wal_header_must_name_4096_byte_pages_and_come_first() {
    let mut txn = WriteTxn::new();
    let mut h = [0u8; 32];
    h[8..12].copy_from_slice(&1024u32.to_be_bytes());
    let err = txn.on_write(&classify(0, &h)).unwrap_err();
    assert!(matches!(err, CaptureError::PageSize {
        page_size: Some(1024)
    }));
}

#[test]
fn a_rewind_to_frame_0_that_writes_the_header_again_restarts_the_capture() {
    let mut wal = Wal::new();
    wal.header();
    wal.frame(0, 2, 0, &page(2));
    wal.frame(1, 9, 0, &page(9));
    wal.header();
    let record = wal.frame(0, 3, 3, &page(3)).unwrap();
    assert_eq!(images(&record), vec![(3, 3)]);
}

#[test]
fn a_savepoint_rollback_that_rewrites_a_frame_drops_what_it_undid() {
    // A cache spill wrote frames 0 (page 2) and 1 (page 7); ROLLBACK TO rewound the WAL to frame
    // 1, which now holds page 5.
    let mut wal = Wal::new();
    wal.header();
    wal.frame(0, 2, 0, &page(2));
    wal.frame(1, 7, 0, &page(7));
    wal.frame(1, 5, 0, &page(5));
    let record = wal.frame(2, 4, 7, &page(4)).unwrap();
    assert_eq!(images(&record), vec![(2, 2), (4, 4), (5, 5)]);
}

#[test]
fn a_commit_frame_written_over_undone_frames_drops_the_later_ones() {
    // Frames 0 to 2 (pages 2, 4, 8) were spilled, then rolled back to frame 1, where the commit
    // frame of page 4 goes: page 8 is no longer part of the transaction.
    let mut wal = Wal::new();
    wal.header();
    wal.frame(0, 2, 0, &page(2));
    wal.frame(1, 4, 0, &page(4));
    wal.frame(2, 8, 0, &page(8));
    let record = wal.frame(1, 4, 8, &page(40)).unwrap();
    assert_eq!(images(&record), vec![(2, 2), (4, 40)]);
}

#[test]
fn a_commit_leaving_wal_mode_or_4096_byte_pages_is_refused() {
    let mut wal = Wal::new();
    wal.header();
    let mut rollback = page1(1);
    rollback[18] = 1;
    rollback[19] = 1;
    let mut h = [0u8; 24];
    h[0..4].copy_from_slice(&1u32.to_be_bytes());
    h[4..8].copy_from_slice(&1u32.to_be_bytes());
    wal.write(WAL_HDR, &h).unwrap();
    let err = wal.write(FIRST_PAGE, &rollback).unwrap_err();
    assert!(matches!(err, CaptureError::LeavesWal { read: 1, write: 1 }));

    let mut big = page1(1);
    big[16..18].copy_from_slice(&1u16.to_be_bytes());
    assert!(matches!(
        check_page1(&big),
        Err(CaptureError::Page1Size { found: 65536 })
    ));
    check_page1(&page1(0)).unwrap();
}

// The commit limit counts the record's pages (a later frame of a page wins, pages above the
// commit size are dropped) before anything is read back: a commit of exactly the limit passes,
// one page more is refused.
#[test]
fn a_commit_above_the_limit_is_refused_before_any_page_is_read_back() {
    let mut txn = WriteTxn::new();
    let mut put = |n: i64, pgno: u32, commit_size: u32| {
        let off = WAL_HDR + n * FRAME;
        let mut h = [0u8; 24];
        h[0..4].copy_from_slice(&pgno.to_be_bytes());
        h[4..8].copy_from_slice(&commit_size.to_be_bytes());
        assert_eq!(txn.on_write(&classify(off, &h)).unwrap(), None);
        txn.on_write(&classify(off + 24, &page(1))).unwrap()
    };
    put(0, 2, 0);
    put(1, 2, 0);
    put(2, 9, 0);
    let point = put(3, 3, 3).unwrap();
    // Pages 2 and 3: page 2's later frame wins, page 9 is above the commit size.
    assert_eq!(txn.pages(point), 2);
    txn.check_size(point, 2 * PAGE as u64).unwrap();
    assert_eq!(
        txn.check_size(point, 2 * PAGE as u64 - 1),
        Err(TooLarge {
            bytes: 2 * PAGE as u64,
            max: 2 * PAGE as u64 - 1,
        })
    );
}
