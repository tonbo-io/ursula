//! TTL expiry index: a min-heap of `(expires_at_ms, stream)` entries with deterministic ordering.

use super::BinaryHeap;
use super::BucketStreamId;
use super::Key;
use super::Ordering;
use super::Reverse;
use super::StreamKey;
use super::compare_stream_ids;

#[derive(Debug, Clone, Default)]
pub(super) struct TtlIndex {
    pub(super) entries: BinaryHeap<Reverse<TtlEntry>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TtlEntry {
    pub(super) expires_at_ms: u64,
    pub(super) stream_id: BucketStreamId,
    pub(super) key: StreamKey,
}

impl Ord for TtlEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.expires_at_ms
            .cmp(&other.expires_at_ms)
            .then_with(|| compare_stream_ids(&self.stream_id, &other.stream_id))
            .then_with(|| self.key.data().as_ffi().cmp(&other.key.data().as_ffi()))
    }
}

impl PartialOrd for TtlEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
