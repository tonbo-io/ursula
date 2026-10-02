//! Hot in-memory payload buffer split into append-ordered chunks.

use super::HotPayloadSegment;
use super::StreamReadSegment;
use super::VecDeque;

#[derive(Debug, Clone, Default)]
pub(super) struct HotBuffer {
    chunks: VecDeque<HotChunk>,
    /// Running sum of `chunk.bytes.len()` over `chunks` (F6a), so callers
    /// never rescan every hot chunk per append.
    payload_len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HotChunk {
    start_offset: u64,
    end_offset: u64,
    bytes: Vec<u8>,
}

impl HotBuffer {
    pub(super) fn from_payload(start_offset: u64, payload: Vec<u8>) -> Self {
        if payload.is_empty() {
            return Self::default();
        }
        let end_offset = start_offset
            .saturating_add(u64::try_from(payload.len()).expect("payload len fits u64"));
        let bytes = payload.len();
        let mut chunks = VecDeque::new();
        chunks.push_back(HotChunk {
            start_offset,
            end_offset,
            bytes: payload,
        });
        Self {
            chunks,
            payload_len: bytes,
        }
    }

    pub(super) fn from_snapshot(payload: Vec<u8>, segments: &[HotPayloadSegment]) -> Self {
        let mut chunks = VecDeque::with_capacity(segments.len());
        let mut bytes = 0usize;
        for segment in segments {
            let chunk = payload[segment.payload_start..segment.payload_end].to_vec();
            bytes = bytes.saturating_add(chunk.len());
            chunks.push_back(HotChunk {
                start_offset: segment.start_offset,
                end_offset: segment.end_offset,
                bytes: chunk,
            });
        }
        Self {
            chunks,
            payload_len: bytes,
        }
    }

    /// Hot payload bytes held, in O(1).
    pub(super) fn len(&self) -> usize {
        self.payload_len
    }

    pub(super) fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Number of append-ordered hot chunks (bounded-state gauge).
    pub(super) fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Per-chunk bookkeeping bytes beyond the payload itself (bounded-state
    /// gauge): one `HotChunk` header per chunk, ignoring allocator slack.
    pub(super) fn chunk_overhead_bytes(&self) -> usize {
        self.chunks
            .len()
            .saturating_mul(std::mem::size_of::<HotChunk>())
    }

    pub(super) fn hot_start_offset(&self) -> u64 {
        self.chunks
            .front()
            .map(|chunk| chunk.start_offset)
            .unwrap_or(0)
    }

    pub(super) fn first_start_offset(&self) -> Option<u64> {
        self.chunks.front().map(|chunk| chunk.start_offset)
    }

    /// End of the first hot append (chunk), if any.
    pub(super) fn first_end_offset(&self) -> Option<u64> {
        self.chunks.front().map(|chunk| chunk.end_offset)
    }

    pub(super) fn payload(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(self.len());
        for chunk in &self.chunks {
            payload.extend_from_slice(&chunk.bytes);
        }
        payload
    }

    pub(super) fn hot_segments(&self) -> Vec<HotPayloadSegment> {
        let mut payload_start = 0usize;
        self.chunks
            .iter()
            .map(|chunk| {
                let payload_end = payload_start + chunk.bytes.len();
                let segment = HotPayloadSegment {
                    start_offset: chunk.start_offset,
                    end_offset: chunk.end_offset,
                    payload_start,
                    payload_end,
                };
                payload_start = payload_end;
                segment
            })
            .collect()
    }

    pub(super) fn push(&mut self, start_offset: u64, end_offset: u64, payload: &[u8]) {
        if payload.is_empty() {
            return;
        }
        self.chunks.push_back(HotChunk {
            start_offset,
            end_offset,
            bytes: payload.to_vec(),
        });
        self.payload_len = self.payload_len.saturating_add(payload.len());
    }

    pub(super) fn append_checkpoint(&self) -> usize {
        self.chunks.len()
    }

    pub(super) fn rollback_appends(&mut self, checkpoint: usize) {
        while self.chunks.len() > checkpoint {
            if let Some(chunk) = self.chunks.pop_back() {
                self.payload_len = self.payload_len.saturating_sub(chunk.bytes.len());
            }
        }
    }

    pub(super) fn plan_cold_flush_from(
        &self,
        from_offset: u64,
        min_hot_bytes: usize,
        max_flush_bytes: usize,
    ) -> Option<(u64, u64, Vec<u8>)> {
        let mut payload = Vec::new();
        for chunk in &self.chunks {
            if chunk.end_offset <= from_offset {
                continue;
            }
            if chunk.start_offset
                > from_offset + u64::try_from(payload.len()).expect("payload len fits u64")
            {
                break;
            }
            if payload.len() >= max_flush_bytes {
                break;
            }
            let skip = if chunk.start_offset < from_offset {
                usize::try_from(from_offset - chunk.start_offset).expect("skip fits usize")
            } else {
                0
            };
            let remaining = max_flush_bytes - payload.len();
            let take = (chunk.bytes.len() - skip).min(remaining);
            payload.extend_from_slice(&chunk.bytes[skip..skip + take]);
            if take < chunk.bytes.len() - skip {
                break;
            }
        }
        if payload.len() < min_hot_bytes {
            return None;
        }
        let end_offset = from_offset + u64::try_from(payload.len()).expect("payload len fits u64");
        Some((from_offset, end_offset, payload))
    }

    pub(super) fn read_segments(
        &self,
        offset: u64,
        next_offset: u64,
    ) -> Vec<(u64, StreamReadSegment)> {
        let mut segments = Vec::new();
        for chunk in &self.chunks {
            let start = offset.max(chunk.start_offset);
            let end = next_offset.min(chunk.end_offset);
            if start < end {
                let payload_start =
                    usize::try_from(start - chunk.start_offset).expect("hot start fits usize");
                let payload_end =
                    usize::try_from(end - chunk.start_offset).expect("hot end fits usize");
                segments.push((
                    start,
                    StreamReadSegment::Hot(chunk.bytes[payload_start..payload_end].to_vec()),
                ));
            }
        }
        segments
    }

    pub(super) fn covers_prefix(&self, start_offset: u64, end_offset: u64) -> bool {
        let Some(first) = self.chunks.front() else {
            return false;
        };
        if first.start_offset != start_offset {
            return false;
        }
        let mut covered_offset = start_offset;
        for chunk in &self.chunks {
            if chunk.start_offset != covered_offset {
                return false;
            }
            if chunk.end_offset >= end_offset {
                return true;
            }
            covered_offset = chunk.end_offset;
        }
        false
    }

    pub(super) fn digest_prefix(&self, start_offset: u64, end_offset: u64) -> Option<String> {
        let len = usize::try_from(end_offset.checked_sub(start_offset)?).ok()?;
        let (_, planned_end, payload) = self.plan_cold_flush_from(start_offset, len, len)?;
        (planned_end == end_offset).then(|| blake3::hash(&payload).to_hex().to_string())
    }

    pub(super) fn flush_prefix(&mut self, end_offset: u64) {
        while self
            .chunks
            .front()
            .is_some_and(|chunk| chunk.end_offset <= end_offset)
        {
            if let Some(chunk) = self.chunks.pop_front() {
                self.payload_len = self.payload_len.saturating_sub(chunk.bytes.len());
            }
        }
        if let Some(front) = self.chunks.front_mut()
            && front.start_offset < end_offset
        {
            let drain_len =
                usize::try_from(end_offset - front.start_offset).expect("drain len fits usize");
            let drain_len = drain_len.min(front.bytes.len());
            front.bytes.drain(..drain_len);
            shrink_vec_if_slack(&mut front.bytes);
            front.start_offset = end_offset;
            self.payload_len = self.payload_len.saturating_sub(drain_len);
        }
        shrink_deque_if_slack(&mut self.chunks);
    }

    pub(super) fn discard_before(&mut self, retained_offset: u64) {
        self.flush_prefix(retained_offset);
    }
}

/// F7 capacity rule: after removing elements, a container whose capacity
/// exceeds `2 * len + 64` shrinks to `2 * len`.
pub(crate) fn capacity_has_slack(len: usize, capacity: usize) -> bool {
    capacity > len.saturating_mul(2).saturating_add(64)
}

pub(crate) fn shrink_vec_if_slack<T>(values: &mut Vec<T>) {
    if capacity_has_slack(values.len(), values.capacity()) {
        values.shrink_to(values.len().saturating_mul(2));
    }
}

pub(crate) fn shrink_deque_if_slack<T>(values: &mut VecDeque<T>) {
    if capacity_has_slack(values.len(), values.capacity()) {
        values.shrink_to(values.len().saturating_mul(2));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filled(appends: u64, bytes: usize) -> HotBuffer {
        let mut hot = HotBuffer::default();
        let payload = vec![7u8; bytes];
        let width = u64::try_from(bytes).unwrap();
        for index in 0..appends {
            hot.push(index * width, (index + 1) * width, &payload);
        }
        hot
    }

    fn scanned_len(hot: &HotBuffer) -> usize {
        hot.chunks.iter().map(|chunk| chunk.bytes.len()).sum()
    }

    #[test]
    fn payload_len_counter_tracks_push_flush_discard_and_rollback() {
        let mut hot = filled(10, 3);
        assert_eq!(hot.len(), 30);
        let checkpoint = hot.append_checkpoint();
        hot.push(30, 35, b"abcde");
        hot.push(35, 36, b"f");
        assert_eq!(hot.len(), 36);
        hot.rollback_appends(checkpoint);
        assert_eq!(hot.len(), 30);
        assert_eq!(scanned_len(&hot), 30);
        // Flush ends inside a chunk: partial drain of the front chunk.
        hot.flush_prefix(7);
        assert_eq!(hot.len(), 23);
        assert_eq!(scanned_len(&hot), 23);
        hot.discard_before(15);
        assert_eq!(hot.len(), 15);
        assert_eq!(scanned_len(&hot), 15);
        hot.flush_prefix(u64::MAX);
        assert_eq!(hot.len(), 0);
        assert_eq!(hot.hot_segments().len(), 0);
    }

    #[test]
    fn payload_len_counter_restores_from_snapshot_and_payload() {
        let hot = filled(4, 5);
        let restored = HotBuffer::from_snapshot(hot.payload(), &hot.hot_segments());
        assert_eq!(restored.len(), 20);
        assert_eq!(HotBuffer::from_payload(9, vec![1, 2, 3]).len(), 3);
        assert_eq!(HotBuffer::from_payload(9, Vec::new()).len(), 0);
    }

    #[test]
    fn flush_prefix_shrinks_chunk_deque_capacity() {
        // Measured before F7: a full flush window left the deque at its peak
        // capacity (2.6 MB after one window).
        let mut hot = filled(100_000, 1);
        assert!(hot.chunks.capacity() >= 100_000);
        hot.flush_prefix(99_990);
        assert_eq!(hot.chunks.len(), 10);
        assert!(
            hot.chunks.capacity() <= 2 * hot.chunks.len() + 64,
            "deque capacity {} not shrunk",
            hot.chunks.capacity()
        );
        hot.flush_prefix(100_000);
        assert!(hot.chunks.capacity() <= 64);
    }

    #[test]
    fn partial_flush_shrinks_front_chunk_bytes() {
        let mut hot = HotBuffer::default();
        hot.push(0, 1 << 20, &vec![1u8; 1 << 20]);
        hot.flush_prefix((1 << 20) - 10);
        let front = hot.chunks.front().unwrap();
        assert_eq!(front.bytes.len(), 10);
        assert!(front.bytes.capacity() <= 2 * 10 + 64);
        assert_eq!(hot.len(), 10);
    }
}
