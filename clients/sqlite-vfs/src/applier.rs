//! Applies commits to a db file in batches: the final image per page and the size. Materialization
//! and builds feed it `USQ2` commit records.
//!
//! A batch keeps, per page, the latest image among its commits, drops pages past a later shrink,
//! and remembers its smallest size and its last. [`PageBatch::flush`] cuts the file to the
//! smallest size, writes the final images and sets the last size, so applying a batch again gives
//! the same file, and applying the commits one batch after another gives the file the commits give
//! one by one.
//!
//! Page 1 is written stamped with the generation's marker ([`marker::stamp`]), as is the page 1 a
//! snapshot install writes ([`stamp_page_one`]): no db file of a generation holds SQLite's magic on
//! disk. A page 1 from the stream without SQLite's magic is refused ([`ApplyError::PageOne`]).

use std::collections::BTreeMap;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::path::PathBuf;

use crate::capture::db_len;
use crate::capture::page_offset;
use crate::codec::PAGE;
use crate::codec::marker;
use crate::codec::marker::MarkerError;
use crate::ids::Lineage;

/// A page image of a commit record.
pub(crate) type Image = Box<[u8; PAGE]>;

/// Why a batch, or a page-1 stamp, could not be written.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ApplyError {
    #[error("{op} {}: {source}", .path.display())]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// A page 1 the stream holds (a commit's or a snapshot's) that is not a SQLite database's.
    #[error("{}: page 1 from the stream: {source}", .path.display())]
    PageOne {
        path: PathBuf,
        #[source]
        source: MarkerError,
    },
}

fn io(op: &'static str, path: &Path) -> impl FnOnce(std::io::Error) -> ApplyError {
    let path = path.to_owned();
    move |source| ApplyError::Io { op, path, source }
}

/// One batch of commits.
#[derive(Debug, Default)]
pub(crate) struct PageBatch {
    /// Final image per page of the batch (pages past a later shrink removed).
    pages: BTreeMap<u32, Image>,
    /// Smallest db size within the batch, and the size after it.
    min_size: Option<u32>,
    size: Option<u32>,
}

impl PageBatch {
    /// Adds a commit: the db size after it, in pages, and the images it wrote.
    #[cfg(test)]
    pub(crate) fn commit(&mut self, size: u32, pages: impl IntoIterator<Item = (u32, Image)>) {
        self.begin(size);
        for (pgno, data) in pages {
            self.page(pgno, data);
        }
    }

    /// Starts a commit whose db size after it is `size` pages: pages past it are dropped.
    pub(crate) fn begin(&mut self, size: u32) {
        if size < self.size.unwrap_or(u32::MAX) {
            self.pages.retain(|&p, _| p <= size);
        }
        self.min_size = Some(self.min_size.map_or(size, |m| m.min(size)));
        self.size = Some(size);
    }

    /// Adds an image the current commit wrote. A batch flushed in the middle of a commit takes
    /// the rest of its pages in the next one.
    pub(crate) fn page(&mut self, pgno: u32, data: Image) {
        self.min_size = self.min_size.or(self.size);
        self.pages.insert(pgno, data);
    }

    /// The page images the batch holds.
    pub(crate) fn len(&self) -> usize {
        self.pages.len()
    }

    /// Writes the batch to `f`, the db file of a generation of `lineage`: cut to its smallest
    /// size, write the final page images (page 1 stamped with the marker; calling `written` after
    /// each, which may stop the flush), set the size. A batch without a commit writes nothing. A
    /// page 1 that does not start with SQLite's magic is refused before anything is written.
    pub(crate) fn flush(
        &mut self,
        f: &File,
        path: &Path,
        lineage: Lineage,
        mut written: impl FnMut() -> Result<(), std::io::Error>,
    ) -> Result<(), ApplyError> {
        let (Some(min), Some(size)) = (self.min_size.take(), self.size) else {
            return Ok(());
        };
        let mut pages = std::mem::take(&mut self.pages);
        if let Some(page) = pages.get_mut(&1) {
            marker::stamp(page.as_mut_slice(), lineage).map_err(|source| ApplyError::PageOne {
                path: path.to_owned(),
                source,
            })?;
        }
        if f.metadata().map_err(io("stat", path))?.len() > db_len(min) {
            f.set_len(db_len(min)).map_err(io("truncate", path))?;
        }
        for (pgno, data) in pages {
            f.write_all_at(data.as_slice(), page_offset(pgno))
                .and_then(|()| written())
                .map_err(io("replay into", path))?;
        }
        f.set_len(db_len(size)).map_err(io("resize", path))
    }
}

/// Stamps page 1 of the db file `f` (at `path`) of a generation of `lineage`, a snapshot's image
/// written outside the applier with SQLite's magic: the marker replaces the magic in place.
/// Anything else there is refused, nothing written.
pub(crate) fn stamp_page_one(f: &File, path: &Path, lineage: Lineage) -> Result<(), ApplyError> {
    let mut head = [0u8; marker::LEN];
    f.read_exact_at(&mut head, 0).map_err(io("read", path))?;
    marker::stamp(&mut head, lineage).map_err(|source| ApplyError::PageOne {
        path: path.to_owned(),
        source,
    })?;
    f.write_all_at(&head, 0).map_err(io("stamp", path))
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::os::unix::fs::FileExt;

    use super::ApplyError;
    use super::PageBatch;
    use super::stamp_page_one;
    use crate::codec::PAGE;
    use crate::codec::marker::MarkerError;
    use crate::codec::marker::SQLITE_MAGIC;
    use crate::codec::marker::mark;
    use crate::ids::Lineage;
    use crate::testdir::TempDir;

    const LINEAGE: Lineage = Lineage([3; 16]);

    /// A page image filled with `fill`, SQLite's magic in front when it is page 1.
    fn page(pgno: u32, fill: u8) -> Box<[u8; PAGE]> {
        let mut p = Box::new([fill; PAGE]);
        if pgno == 1 {
            p[..16].copy_from_slice(&SQLITE_MAGIC);
        }
        p
    }

    fn read(f: &File) -> Vec<u8> {
        let len = usize::try_from(f.metadata().unwrap().len()).unwrap();
        let mut buf = vec![0u8; len];
        f.read_exact_at(&mut buf, 0).unwrap();
        buf
    }

    #[test]
    fn a_batch_writes_final_images_and_its_last_size() {
        let dir = TempDir::new("applier");
        let path = dir.path().join("db");
        let f = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.write_all_at(&[9u8; 4 * PAGE], 0).unwrap();
        let mut batch = PageBatch::default();
        batch.commit(4, vec![(1, page(1, 1)), (4, page(4, 4))]);
        // A shrink drops page 4, a regrowth leaves page 3 zeroed.
        batch.commit(2, vec![(2, page(2, 2))]);
        batch.commit(3, Vec::new());
        assert_eq!(batch.len(), 2);
        let mut n = 0;
        batch
            .flush(&f, &dir.path().join("db"), LINEAGE, || {
                n += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(n, 2);
        let bytes = read(&f);
        assert_eq!(bytes.len(), 3 * PAGE);
        // Page 1 lands on disk marked.
        assert_eq!(bytes[..16], mark(LINEAGE));
        assert!(bytes[16..PAGE].iter().all(|&b| b == 1));
        assert!(bytes[PAGE..2 * PAGE].iter().all(|&b| b == 2));
        assert!(bytes[2 * PAGE..].iter().all(|&b| b == 0));
        // An empty batch writes nothing.
        batch
            .flush(&f, &dir.path().join("db"), LINEAGE, || {
                n += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(read(&f).len(), 3 * PAGE);
    }

    // A page 1 without SQLite's magic is refused before the batch writes anything.
    #[test]
    fn a_foreign_page_one_is_refused_before_anything_is_written() {
        let dir = TempDir::new("applier-foreign");
        let f = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("db"))
            .unwrap();
        f.write_all_at(&[9u8; 2 * PAGE], 0).unwrap();
        let mut batch = PageBatch::default();
        batch.commit(1, vec![(1, Box::new([1u8; PAGE]))]);
        let refused = batch.flush(&f, &dir.path().join("db"), LINEAGE, || Ok(()));
        assert!(matches!(
            refused,
            Err(ApplyError::PageOne {
                source: MarkerError::NotSqlite { .. },
                ..
            })
        ));
        assert_eq!(read(&f), vec![9u8; 2 * PAGE]);
    }

    // A snapshot's page 1 written in place is stamped; a foreign one is refused, unchanged.
    #[test]
    fn page_one_written_outside_the_applier_is_stamped() {
        let dir = TempDir::new("applier-stamp");
        let f = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("db"))
            .unwrap();
        f.write_all_at(page(1, 5).as_slice(), 0).unwrap();
        stamp_page_one(&f, &dir.path().join("db"), LINEAGE).unwrap();
        let bytes = read(&f);
        assert_eq!(bytes[..16], mark(LINEAGE));
        assert!(bytes[16..].iter().all(|&b| b == 5));
        assert!(matches!(
            stamp_page_one(&f, &dir.path().join("db"), LINEAGE),
            Err(ApplyError::PageOne {
                source: MarkerError::NotSqlite { .. },
                ..
            })
        ));
        assert_eq!(read(&f), bytes);
    }
}
