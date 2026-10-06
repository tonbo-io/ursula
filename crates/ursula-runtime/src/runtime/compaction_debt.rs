//! Compaction debt (bounded-stream-state F14d).
//!
//! The same-stream compactor used to discover work by recursively listing
//! the whole cold root every pass. Now the paths that create small exclusive
//! chunks record the cold-index pages they touched as debt, and the
//! compactor drains that set: it reads only those pages, by key, and issues
//! no LIST. The set is node-local and bounded. After a failover it restarts
//! empty; the next small flush or compaction output refills it, and the
//! cold-index repair cursor, which reads every page of every stream of the
//! group it leads, records the pages that still hold small chunks, so idle
//! streams are found too.

use std::collections::HashSet;
use std::collections::VecDeque;

use ursula_shard::BucketStreamId;

use crate::cold_index::ColdIndexPageKey;

/// Most pages held as debt; further pages are dropped until the compactor
/// drains some, and the repair cursor finds them again later.
pub(crate) const MAX_COMPACTION_DEBT_PAGES: usize = 65_536;

/// Pages that may hold compactable small chunks, in first-recorded order.
#[derive(Debug, Default)]
pub(crate) struct CompactionDebt {
    order: VecDeque<ColdIndexPageKey>,
    pages: HashSet<ColdIndexPageKey>,
}

impl CompactionDebt {
    /// Records one page; a page already held keeps its place.
    pub(crate) fn record_page(&mut self, key: ColdIndexPageKey) {
        if self.pages.len() >= MAX_COMPACTION_DEBT_PAGES || self.pages.contains(&key) {
            return;
        }
        self.pages.insert(key.clone());
        self.order.push_back(key);
    }

    /// Records every page that `[start_offset, end_offset)` of one stream
    /// incarnation touches.
    pub(crate) fn record_pages_of_range(
        &mut self,
        stream_id: &BucketStreamId,
        generation: u64,
        start_offset: u64,
        end_offset: u64,
    ) {
        if end_offset <= start_offset {
            return;
        }
        let span = ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES;
        for page_id in start_offset / span..=(end_offset - 1) / span {
            self.record_page(ColdIndexPageKey {
                stream_id: stream_id.clone(),
                generation,
                page_id,
            });
        }
    }

    /// Removes and returns up to `max` pages, oldest first.
    pub(crate) fn take(&mut self, max: usize) -> Vec<ColdIndexPageKey> {
        let count = max.min(self.order.len());
        let taken = self.order.drain(..count).collect::<Vec<_>>();
        for key in &taken {
            self.pages.remove(key);
        }
        if self.order.capacity() > self.order.len().saturating_mul(2).saturating_add(64) {
            self.order.shrink_to(self.order.len().saturating_mul(2));
        }
        if self.pages.capacity() > self.pages.len().saturating_mul(2).saturating_add(64) {
            self.pages.shrink_to(self.pages.len().saturating_mul(2));
        }
        taken
    }

    pub(crate) fn len(&self) -> usize {
        self.order.len()
    }

    pub(crate) fn remove_where(&mut self, discard: impl Fn(&ColdIndexPageKey) -> bool) {
        self.order.retain(|key| !discard(key));
        self.pages.retain(|key| !discard(key));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debt_records_each_touched_page_once_and_drains_in_order() {
        let stream = BucketStreamId::new("bkt", "s");
        let span = ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES;
        let mut debt = CompactionDebt::default();
        debt.record_pages_of_range(&stream, 3, 10, 20);
        debt.record_pages_of_range(&stream, 3, span - 5, span + 5);
        debt.record_pages_of_range(&stream, 3, 0, 0);
        assert_eq!(debt.len(), 2);
        let taken = debt.take(1);
        assert_eq!(taken[0].page_id, 0);
        assert_eq!(taken[0].generation, 3);
        assert_eq!(
            debt.take(10).iter().map(|k| k.page_id).collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(debt.len(), 0);
        // Once drained, a page can be recorded again.
        debt.record_pages_of_range(&stream, 3, 10, 20);
        assert_eq!(debt.len(), 1);
    }

    #[test]
    fn debt_is_bounded() {
        let mut debt = CompactionDebt::default();
        for index in 0..(MAX_COMPACTION_DEBT_PAGES as u64 + 10) {
            debt.record_page(ColdIndexPageKey {
                stream_id: BucketStreamId::new("bkt", "s"),
                generation: 0,
                page_id: index,
            });
        }
        assert_eq!(debt.len(), MAX_COMPACTION_DEBT_PAGES);
    }
}
