#![expect(
    clippy::arithmetic_side_effects,
    reason = "tests compute offsets and lengths from small generated sizes"
)]

use proptest::collection::vec;
use proptest::prelude::any;
use proptest::prop_assert;
use proptest::prop_assert_eq;
use proptest::proptest;

use super::LEN;
use super::MarkerError;
use super::PageOne;
use super::SQLITE_MAGIC;
use super::mark;
use super::on_read;
use super::on_write;
use super::page_one;
use super::stamp;
use crate::ids::Lineage;

const LINEAGE: Lineage = Lineage([0xab; 16]);

/// A page 1 as SQLite writes it: its magic, then `fill`.
fn sqlite_page(len: usize, fill: u8) -> Vec<u8> {
    let mut page = vec![fill; len.max(LEN)];
    page[..LEN].copy_from_slice(&SQLITE_MAGIC);
    page
}

#[test]
fn the_marker_names_the_lineage() {
    let m = mark(Lineage(*b"0123456789abcdef"));
    assert_eq!(&m, b"Ursula0801234567");
    assert_ne!(mark(Lineage([1; 16])), mark(Lineage([2; 16])));
}

proptest! {
    // Any write overlapping bytes 0..16 that carries SQLite's magic there lands on disk with the
    // marker, and any read of what the disk holds gives SQLite's magic back. Bytes past 16 pass
    // unchanged, and a range that misses bytes 0..16 is not touched at all.
    #[test]
    fn magic_written_reads_back_as_magic(
        lineage in any::<[u8; 16]>(),
        off in 0usize..40,
        len in 1usize..64,
        roff in 0usize..40,
        rlen in 1usize..64,
        fill in any::<u8>(),
    ) {
        let lineage = Lineage(lineage);
        let sqlite = sqlite_page(128, fill);
        let mut disk = [0u8; 128];
        disk[..LEN].copy_from_slice(&mark(lineage));
        let write = &sqlite[off..off + len];
        match on_write(write, off as u64, lineage).unwrap() {
            Some(out) => {
                prop_assert!(off < LEN);
                prop_assert_eq!(out.len(), write.len());
                disk[off..off + len].copy_from_slice(&out);
            }
            None => {
                prop_assert!(off >= LEN);
                disk[off..off + len].copy_from_slice(write);
            }
        }
        let mut read = disk[roff..roff + rlen].to_vec();
        on_read(&mut read, roff as u64, lineage).unwrap();
        let end = (roff + rlen).min(LEN);
        if roff < LEN {
            prop_assert_eq!(&read[..end - roff], &SQLITE_MAGIC[roff..end]);
        }
        if roff + rlen > LEN {
            let from = roff.max(LEN);
            prop_assert_eq!(&read[from - roff..], &disk[from..roff + rlen]);
        }
    }

    // A write whose bytes 0..16 are not SQLite's magic is an error, and so is a read that finds
    // anything but this generation's marker there; the buffer read stays as it was.
    #[test]
    fn anything_else_is_an_error(
        bytes in vec(any::<u8>(), LEN),
        off in 0usize..LEN,
        len in 1usize..32,
    ) {
        let len = len.min(LEN - off);
        let range = off..off + len;
        if bytes[range.clone()] != SQLITE_MAGIC[range.clone()] {
            let refused = on_write(&bytes[range.clone()], off as u64, LINEAGE);
            let is_not_sqlite = matches!(refused, Err(MarkerError::NotSqlite { .. }));
            prop_assert!(is_not_sqlite);
        }
        if bytes[range.clone()] != mark(LINEAGE)[range.clone()] {
            let mut read = bytes[range.clone()].to_vec();
            let refused = on_read(&mut read, off as u64, LINEAGE);
            let is_not_marked = matches!(refused, Err(MarkerError::NotMarked { .. }));
            prop_assert!(is_not_marked);
            prop_assert_eq!(&read[..], &bytes[range]);
        }
    }
}

#[test]
fn another_lineage_is_not_marked() {
    let mut read = mark(Lineage([1; 16])).to_vec();
    assert!(matches!(
        on_read(&mut read, 0, LINEAGE),
        Err(MarkerError::NotMarked { at: 0, .. })
    ));
    let mut plain = SQLITE_MAGIC.to_vec();
    assert!(matches!(
        on_read(&mut plain, 0, LINEAGE),
        Err(MarkerError::NotMarked { .. })
    ));
}

#[test]
fn a_stamped_page_is_marked() {
    let mut page = sqlite_page(4096, 7);
    stamp(&mut page, LINEAGE).unwrap();
    assert_eq!(page[..LEN], mark(LINEAGE));
    assert!(page[LEN..].iter().all(|&b| b == 7));
    let mut foreign = vec![7u8; 4096];
    assert!(matches!(
        stamp(&mut foreign, LINEAGE),
        Err(MarkerError::NotSqlite { .. })
    ));
    assert!(
        foreign.iter().all(|&b| b == 7),
        "a refused stamp writes nothing"
    );
}

#[test]
fn page_one_is_marked_uninitialized_or_foreign() {
    let m = mark(LINEAGE);
    assert_eq!(page_one(&m, 4096, LINEAGE, true), PageOne::Marked);
    assert_eq!(page_one(&[], 0, LINEAGE, false), PageOne::Uninitialized);
    // An empty db in a generation whose log holds a commit the db should hold is not new.
    assert_eq!(page_one(&[], 0, LINEAGE, true), PageOne::Foreign {
        found: Vec::new()
    });
    assert_eq!(
        page_one(&SQLITE_MAGIC, 4096, LINEAGE, false),
        PageOne::Foreign {
            found: SQLITE_MAGIC.to_vec()
        }
    );
    assert_eq!(page_one(&m[..8], 8, LINEAGE, false), PageOne::Foreign {
        found: m[..8].to_vec()
    });
    assert_eq!(
        page_one(&mark(Lineage([1; 16])), 4096, LINEAGE, false),
        PageOne::Foreign {
            found: mark(Lineage([1; 16])).to_vec()
        }
    );
}
