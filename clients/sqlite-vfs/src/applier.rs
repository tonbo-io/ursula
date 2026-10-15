//! Applies commits to a db file in batches: the final image per page and the size, whatever the
//! frame format they came in. 0.7's attach feeds it `USQ1` records, materialization `USQ2` commit
//! records.
//!
//! A batch keeps, per page, the latest image among its commits, drops pages past a later shrink,
//! and remembers its smallest size and its last. [`PageBatch::flush`] cuts the file to the
//! smallest size, writes the final images and sets the last size, so applying a batch again gives
//! the same file, and applying the commits one batch after another gives the file the commits give
//! one by one.

use std::collections::BTreeMap;
use std::fs::File;
use std::os::unix::fs::FileExt;

use crate::capture::db_len;
use crate::capture::page_offset;
use crate::codec::PAGE;

/// A page image as a format holds it.
pub(crate) trait Image {
    fn bytes(&self) -> &[u8];
}

/// 0.7's `USQ1` records.
impl Image for Vec<u8> {
    fn bytes(&self) -> &[u8] {
        self
    }
}

/// `USQ2` commit records.
impl Image for Box<[u8; PAGE]> {
    fn bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

/// One batch of commits, with page images of type `P`.
#[derive(Debug)]
pub(crate) struct PageBatch<P> {
    /// Final image per page of the batch (pages past a later shrink removed).
    pages: BTreeMap<u32, P>,
    /// Smallest db size within the batch, and the size after it.
    min_size: Option<u32>,
    size: Option<u32>,
}

impl<P> Default for PageBatch<P> {
    fn default() -> Self {
        PageBatch {
            pages: BTreeMap::new(),
            min_size: None,
            size: None,
        }
    }
}

impl<P: Image> PageBatch<P> {
    /// Adds a commit: the db size after it, in pages, and the images it wrote.
    pub(crate) fn commit(&mut self, size: u32, pages: impl IntoIterator<Item = (u32, P)>) {
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
    pub(crate) fn page(&mut self, pgno: u32, data: P) {
        self.min_size = self.min_size.or(self.size);
        self.pages.insert(pgno, data);
    }

    /// Starts over from a file of `size` pages written whole (a snapshot image): nothing to cut,
    /// nothing to write yet.
    pub(crate) fn restart(&mut self, size: u32) {
        self.pages.clear();
        self.min_size = None;
        self.size = Some(size);
    }

    /// Whether the batch holds a commit, so that [`PageBatch::flush`] writes.
    pub(crate) fn has_commit(&self) -> bool {
        self.min_size.is_some()
    }

    /// The page images the batch holds.
    pub(crate) fn len(&self) -> usize {
        self.pages.len()
    }

    /// Writes the batch to `f`: cut to its smallest size, write the final page images (calling
    /// `written` after each), set the size. A batch without a commit writes nothing.
    pub(crate) fn flush(
        &mut self,
        f: &File,
        mut written: impl FnMut(),
    ) -> Result<(), std::io::Error> {
        let (Some(min), Some(size)) = (self.min_size.take(), self.size) else {
            return Ok(());
        };
        let pages = std::mem::take(&mut self.pages);
        if f.metadata()?.len() > db_len(min) {
            f.set_len(db_len(min))?;
        }
        for (pgno, data) in pages {
            f.write_all_at(data.bytes(), page_offset(pgno))?;
            written();
        }
        f.set_len(db_len(size))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::os::unix::fs::FileExt;

    use super::PageBatch;
    use crate::codec::PAGE;
    use crate::testdir::TempDir;

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
        batch.commit(4, vec![(1, vec![1u8; PAGE]), (4, vec![4u8; PAGE])]);
        // A shrink drops page 4, a regrowth leaves page 3 zeroed.
        batch.commit(2, vec![(2, vec![2u8; PAGE])]);
        batch.commit(3, Vec::new());
        assert_eq!(batch.len(), 2);
        let mut n = 0;
        batch.flush(&f, || n += 1).unwrap();
        assert_eq!(n, 2);
        let bytes = read(&f);
        assert_eq!(bytes.len(), 3 * PAGE);
        assert!(bytes[..PAGE].iter().all(|&b| b == 1));
        assert!(bytes[PAGE..2 * PAGE].iter().all(|&b| b == 2));
        assert!(bytes[2 * PAGE..].iter().all(|&b| b == 0));
        // An empty batch writes nothing.
        batch.flush(&f, || n += 1).unwrap();
        assert_eq!(read(&f).len(), 3 * PAGE);
    }
}
