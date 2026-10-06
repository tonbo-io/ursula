//! Hot in-memory payload buffer (bounded-stream-state F6b).
//!
//! Appends are coalesced into blocks of up to [`HOT_BLOCK_BYTES`], each with
//! its own start offset, so the buffer carries no per-append header. A block
//! ends where the next append is not contiguous (an external append above hot
//! bytes leaves a gap, F18) or where it is full. The buffer keeps nothing per
//! message; only streams with a record index have message boundaries, in
//! their dense offsets (F4b). Reads binary-search blocks and a flush drops
//! whole blocks and trims at most one. Snapshots emit one hot segment per block and restore
//! segments one-to-one, so every replica holds the same block layout after
//! the same history.

use super::HotPayloadSegment;
use super::StreamReadSegment;
use super::VecDeque;

/// Payload bytes one hot block holds before the next append starts a new
/// block (F6b).
pub(crate) const HOT_BLOCK_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Default)]
pub(super) struct HotBuffer {
    blocks: VecDeque<HotBlock>,
    /// Running sum of `block.bytes.len()` over `blocks` (F6a), so callers
    /// never rescan the buffer per append.
    payload_len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HotBlock {
    start_offset: u64,
    bytes: Vec<u8>,
}

impl HotBlock {
    fn end_offset(&self) -> u64 {
        self.start_offset.saturating_add(len_u64(self.bytes.len()))
    }
}

pub(super) fn len_u64(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

pub(super) fn offset_delta(from: u64, to: u64) -> usize {
    usize::try_from(to.saturating_sub(from)).unwrap_or(usize::MAX)
}

/// Grows `bytes` to hold `additional` more bytes: doubling, as `Vec` does,
/// but never past one block, so a full block holds exactly one block of
/// capacity.
fn reserve_in_block(bytes: &mut Vec<u8>, additional: usize) {
    let needed = bytes.len().saturating_add(additional);
    if needed <= bytes.capacity() {
        return;
    }
    let target = bytes
        .capacity()
        .saturating_mul(2)
        .max(needed)
        .min(HOT_BLOCK_BYTES.max(needed));
    bytes.reserve_exact(target.saturating_sub(bytes.len()));
}

impl HotBuffer {
    pub(super) fn from_payload(start_offset: u64, payload: Vec<u8>) -> Self {
        let mut buffer = Self::default();
        buffer.push_slice(start_offset, &payload);
        buffer
    }

    /// Restores the blocks a snapshot recorded, one block per segment, so a
    /// restored replica keeps the layout of the replica that built it.
    pub(super) fn from_snapshot(payload: Vec<u8>, segments: &[HotPayloadSegment]) -> Self {
        let mut blocks = VecDeque::with_capacity(segments.len());
        let mut bytes = 0usize;
        for segment in segments {
            let Some(chunk) = payload.get(segment.payload_start..segment.payload_end) else {
                continue;
            };
            if chunk.is_empty() {
                continue;
            }
            bytes = bytes.saturating_add(chunk.len());
            blocks.push_back(HotBlock {
                start_offset: segment.start_offset,
                bytes: chunk.to_vec(),
            });
        }
        Self {
            blocks,
            payload_len: bytes,
        }
    }

    /// Hot payload bytes held, in O(1).
    pub(super) fn len(&self) -> usize {
        self.payload_len
    }

    pub(super) fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    /// Number of hot blocks (bounded-state gauge).
    pub(super) fn chunk_count(&self) -> usize {
        self.blocks.len()
    }

    /// Bookkeeping bytes beyond the payload itself (bounded-state gauge):
    /// one block header per block, ignoring allocator slack. Blocks hold up
    /// to 64 KiB each, so this is per payload byte, not per record.
    pub(super) fn chunk_overhead_bytes(&self) -> usize {
        self.blocks
            .len()
            .saturating_mul(std::mem::size_of::<HotBlock>())
    }

    pub(super) fn hot_start_offset(&self) -> u64 {
        self.first_start_offset().unwrap_or(0)
    }

    pub(super) fn first_start_offset(&self) -> Option<u64> {
        self.blocks.front().map(|block| block.start_offset)
    }

    pub(super) fn payload(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(self.len());
        for block in &self.blocks {
            payload.extend_from_slice(&block.bytes);
        }
        payload
    }

    pub(super) fn hot_segments(&self) -> Vec<HotPayloadSegment> {
        let mut payload_start = 0usize;
        self.blocks
            .iter()
            .map(|block| {
                let payload_end = payload_start.saturating_add(block.bytes.len());
                let segment = HotPayloadSegment {
                    start_offset: block.start_offset,
                    end_offset: block.end_offset(),
                    payload_start,
                    payload_end,
                };
                payload_start = payload_end;
                segment
            })
            .collect()
    }

    pub(super) fn push(&mut self, start_offset: u64, end_offset: u64, payload: &[u8]) {
        debug_assert_eq!(
            end_offset.saturating_sub(start_offset),
            len_u64(payload.len())
        );
        self.push_slice(start_offset, payload);
    }

    fn push_slice(&mut self, start_offset: u64, mut payload: &[u8]) {
        if payload.is_empty() {
            return;
        }
        self.payload_len = self.payload_len.saturating_add(payload.len());
        let mut offset = start_offset;
        if let Some(last) = self.blocks.back_mut()
            && last.end_offset() == offset
            && last.bytes.len() < HOT_BLOCK_BYTES
        {
            // The branch condition keeps `last.bytes.len() < HOT_BLOCK_BYTES`.
            let take = payload
                .len()
                .min(HOT_BLOCK_BYTES.saturating_sub(last.bytes.len()));
            let (head, rest) = payload.split_at(take);
            reserve_in_block(&mut last.bytes, head.len());
            last.bytes.extend_from_slice(head);
            offset = offset.saturating_add(len_u64(take));
            payload = rest;
        } else if let Some(last) = self.blocks.back_mut() {
            // The previous block is closed for good (a gap or a full
            // block): give back any growth slack it still holds.
            last.bytes.shrink_to_fit();
        }
        for block in payload.chunks(HOT_BLOCK_BYTES) {
            let mut bytes = Vec::new();
            reserve_in_block(&mut bytes, block.len());
            bytes.extend_from_slice(block);
            self.blocks.push_back(HotBlock {
                start_offset: offset,
                bytes,
            });
            offset = offset.saturating_add(len_u64(block.len()));
        }
    }

    /// Index of the first block that ends after `offset`.
    fn first_block_ending_after(&self, offset: u64) -> usize {
        self.blocks
            .partition_point(|block| block.end_offset() <= offset)
    }

    pub(super) fn plan_cold_flush_from(
        &self,
        from_offset: u64,
        min_hot_bytes: usize,
        max_flush_bytes: usize,
    ) -> Option<(u64, u64, Vec<u8>)> {
        let mut payload = Vec::new();
        for block in self
            .blocks
            .range(self.first_block_ending_after(from_offset)..)
        {
            let planned_end = from_offset.saturating_add(len_u64(payload.len()));
            if block.start_offset > planned_end {
                break;
            }
            if payload.len() >= max_flush_bytes {
                break;
            }
            let skip = offset_delta(block.start_offset, from_offset).min(block.bytes.len());
            let Some(available) = block.bytes.get(skip..) else {
                break;
            };
            // `payload.len() < max_flush_bytes` was checked above.
            let take = available
                .len()
                .min(max_flush_bytes.saturating_sub(payload.len()));
            let Some(bytes) = available.get(..take) else {
                break;
            };
            payload.extend_from_slice(bytes);
            if take < available.len() {
                break;
            }
        }
        if payload.len() < min_hot_bytes {
            return None;
        }
        let end_offset = from_offset.saturating_add(len_u64(payload.len()));
        Some((from_offset, end_offset, payload))
    }

    pub(super) fn read_segments(
        &self,
        offset: u64,
        next_offset: u64,
    ) -> Vec<(u64, StreamReadSegment)> {
        let mut segments = Vec::new();
        for block in self.blocks.range(self.first_block_ending_after(offset)..) {
            if block.start_offset >= next_offset {
                break;
            }
            let start = offset.max(block.start_offset);
            let end = next_offset.min(block.end_offset());
            if start < end {
                let payload_start = offset_delta(block.start_offset, start);
                let payload_end = offset_delta(block.start_offset, end);
                if let Some(bytes) = block.bytes.get(payload_start..payload_end) {
                    segments.push((start, StreamReadSegment::Hot(bytes.to_vec())));
                }
            }
        }
        segments
    }

    /// The hot byte at `offset`, or `None` when that byte is not hot.
    pub(super) fn byte_at(&self, offset: u64) -> Option<u8> {
        let block = self
            .blocks
            .get(self.first_block_ending_after(offset))
            .filter(|block| block.start_offset <= offset)?;
        block
            .bytes
            .get(offset_delta(block.start_offset, offset))
            .copied()
    }

    /// End offsets (the offset after each LF) of the JSON messages that
    /// start at `start_offset`, scanning hot bytes up to `end_offset`. Stops
    /// at the last end within `max_bytes` of `start_offset`, except that a
    /// first message longer than `max_bytes` is returned whole. Stops early
    /// at a gap in the hot blocks.
    pub(super) fn lf_ends(&self, start_offset: u64, end_offset: u64, max_bytes: u64) -> Vec<u64> {
        let limit = start_offset.saturating_add(max_bytes);
        let mut ends = Vec::new();
        let mut cursor = start_offset;
        for block in self
            .blocks
            .range(self.first_block_ending_after(start_offset)..)
        {
            if block.start_offset > cursor || cursor >= end_offset {
                break;
            }
            let to = end_offset.min(block.end_offset());
            let Some(bytes) = block.bytes.get(
                offset_delta(block.start_offset, cursor)..offset_delta(block.start_offset, to),
            ) else {
                break;
            };
            for index in memchr::memchr_iter(b'\n', bytes) {
                let end = cursor.saturating_add(len_u64(index)).saturating_add(1);
                if end > limit && !ends.is_empty() {
                    return ends;
                }
                ends.push(end);
                if end >= limit {
                    return ends;
                }
            }
            cursor = to;
        }
        ends
    }

    /// Whether the hot blocks hold every byte of `[start_offset,
    /// end_offset)`, with no gap (an external append above hot bytes leaves
    /// one). An empty range is covered.
    pub(super) fn covers(&self, start_offset: u64, end_offset: u64) -> bool {
        if start_offset >= end_offset {
            return true;
        }
        let mut covered_offset = start_offset;
        for block in self
            .blocks
            .range(self.first_block_ending_after(start_offset)..)
        {
            if block.start_offset > covered_offset {
                return false;
            }
            covered_offset = block.end_offset();
            if covered_offset >= end_offset {
                return true;
            }
        }
        false
    }

    pub(super) fn covers_prefix(&self, start_offset: u64, end_offset: u64) -> bool {
        let Some(first) = self.blocks.front() else {
            return false;
        };
        if first.start_offset != start_offset {
            return false;
        }
        let mut covered_offset = start_offset;
        for block in &self.blocks {
            if block.start_offset != covered_offset {
                return false;
            }
            if block.end_offset() >= end_offset {
                return true;
            }
            covered_offset = block.end_offset();
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
            .blocks
            .front()
            .is_some_and(|block| block.end_offset() <= end_offset)
        {
            if let Some(block) = self.blocks.pop_front() {
                self.payload_len = self.payload_len.saturating_sub(block.bytes.len());
            }
        }
        if let Some(front) = self.blocks.front_mut()
            && front.start_offset < end_offset
        {
            let drain_len = offset_delta(front.start_offset, end_offset).min(front.bytes.len());
            front.bytes.drain(..drain_len);
            shrink_vec_if_slack(&mut front.bytes);
            front.start_offset = end_offset;
            self.payload_len = self.payload_len.saturating_sub(drain_len);
        }
        shrink_deque_if_slack(&mut self.blocks);
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
        let mut start = 0u64;
        for _ in 0..appends {
            let end = start.checked_add(width).unwrap();
            hot.push(start, end, &payload);
            start = end;
        }
        hot
    }

    fn scanned_len(hot: &HotBuffer) -> usize {
        hot.blocks.iter().map(|block| block.bytes.len()).sum()
    }

    #[test]
    fn payload_len_counter_tracks_push_flush_and_discard() {
        let mut hot = filled(10, 3);
        assert_eq!(hot.len(), 30);
        assert_eq!(scanned_len(&hot), 30);
        // Flush ends inside a block: partial drain of the front block.
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
    fn appends_coalesce_into_blocks_without_per_append_headers() {
        // Before F6b every append was its own 40-byte chunk header plus an
        // allocation: 100k appends of 200 B held 100k chunks.
        let hot = filled(100_000, 200);
        let expected_blocks = (100_000 * 200usize).div_ceil(HOT_BLOCK_BYTES);
        assert_eq!(hot.chunk_count(), expected_blocks);
        assert!(hot.chunk_overhead_bytes() * 1000 < hot.len() * 2);
        for block in &hot.blocks {
            assert!(block.bytes.len() <= HOT_BLOCK_BYTES);
            assert!(block.bytes.capacity() <= HOT_BLOCK_BYTES);
        }
        let segments = hot.hot_segments();
        assert_eq!(segments.len(), expected_blocks);
        assert_eq!(segments.last().unwrap().end_offset, 20_000_000);
    }

    #[test]
    fn a_gap_starts_a_new_block_and_restore_keeps_the_layout() {
        let mut hot = HotBuffer::default();
        hot.push(0, 10, &[1; 10]);
        hot.push(10, 20, &[2; 10]);
        // An external append covers [20, 50); hot bytes resume above it.
        hot.push(50, 60, &[3; 10]);
        hot.push(60, 70, &[4; 10]);
        let segments = hot.hot_segments();
        assert_eq!(segments.len(), 2);
        assert_eq!((segments[0].start_offset, segments[0].end_offset), (0, 20));
        assert_eq!((segments[1].start_offset, segments[1].end_offset), (50, 70));
        assert!(!hot.covers_prefix(0, 60));
        assert!(hot.covers_prefix(0, 20));
        let restored = HotBuffer::from_snapshot(hot.payload(), &segments);
        assert_eq!(restored.hot_segments(), segments);
        assert_eq!(restored.read_segments(0, 70), hot.read_segments(0, 70));
    }

    #[test]
    fn flush_prefix_shrinks_block_deque_capacity() {
        // Measured before F7: a full flush window left the deque at its peak
        // capacity (2.6 MB after one window).
        let mut hot = HotBuffer::default();
        for index in 0..10_000u64 {
            // Every append leaves a gap, so each one is its own block.
            hot.push(index * 2, index * 2 + 1, &[1]);
        }
        assert!(hot.blocks.capacity() >= 10_000);
        hot.flush_prefix(19_980);
        assert_eq!(hot.blocks.len(), 10);
        assert!(
            hot.blocks.capacity() <= 2 * hot.blocks.len() + 64,
            "deque capacity {} not shrunk",
            hot.blocks.capacity()
        );
        hot.flush_prefix(20_000);
        assert!(hot.blocks.capacity() <= 64);
    }

    #[test]
    fn partial_flush_shrinks_front_block_bytes() {
        let mut hot = HotBuffer::default();
        hot.push(0, 1 << 20, &vec![1u8; 1 << 20]);
        hot.flush_prefix((1 << 20) - 10);
        let front = hot.blocks.front().unwrap();
        assert_eq!(front.bytes.len(), 10);
        assert!(front.bytes.capacity() <= 2 * 10 + 64);
        assert_eq!(hot.len(), 10);
    }

    /// The pre-F6b representation, one entry per append, as the reference.
    #[derive(Default)]
    struct Model {
        appends: Vec<(u64, Vec<u8>)>,
    }

    impl Model {
        fn bytes_in(&self, start: u64, end: u64) -> Vec<(u64, u8)> {
            let mut out = Vec::new();
            for (offset, bytes) in &self.appends {
                for (index, byte) in bytes.iter().enumerate() {
                    let at = offset.checked_add(len_u64(index)).unwrap();
                    if at >= start && at < end {
                        out.push((at, *byte));
                    }
                }
            }
            out
        }

        fn flush_prefix(&mut self, end: u64) {
            let mut kept = Vec::new();
            for (offset, bytes) in self.appends.drain(..) {
                let finish = offset.checked_add(len_u64(bytes.len())).unwrap();
                if finish <= end {
                    continue;
                }
                if offset < end {
                    let skip = offset_delta(offset, end);
                    kept.push((end, bytes[skip..].to_vec()));
                } else {
                    kept.push((offset, bytes));
                }
            }
            self.appends = kept;
        }

        fn len(&self) -> usize {
            self.appends.iter().map(|(_, bytes)| bytes.len()).sum()
        }
    }

    fn flatten(segments: Vec<(u64, StreamReadSegment)>) -> Vec<(u64, u8)> {
        let mut out = Vec::new();
        for (start, segment) in segments {
            let StreamReadSegment::Hot(bytes) = segment else {
                panic!("hot buffer returned a non-hot segment");
            };
            for (index, byte) in bytes.into_iter().enumerate() {
                out.push((start.checked_add(len_u64(index)).unwrap(), byte));
            }
        }
        out
    }

    /// Small deterministic generator (xorshift), so the test needs no extra
    /// dependency and every failure reproduces from its seed.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0.wrapping_shl(13);
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0.wrapping_shl(17);
            self.0
        }

        fn below(&mut self, bound: u64) -> u64 {
            self.next().checked_rem(bound.max(1)).unwrap()
        }
    }

    #[test]
    fn blocks_match_per_append_chunks_under_random_workloads() {
        for seed in 1..=64u64 {
            let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
            let mut hot = HotBuffer::default();
            let mut model = Model::default();
            let mut tail = 0u64;
            let mut flushed = 0u64;
            for step in 0..400 {
                match rng.below(10) {
                    0..=5 => {
                        // Append: mostly tiny, sometimes larger than a block,
                        // sometimes above a gap left by an external append.
                        if rng.below(8) == 0 {
                            tail += 1 + rng.below(5_000);
                        }
                        let len = match rng.below(10) {
                            0 => 1 + rng.below(3 * HOT_BLOCK_BYTES as u64),
                            _ => 1 + rng.below(400),
                        } as usize;
                        let byte = u8::try_from(step % 251).unwrap();
                        let payload = vec![byte; len];
                        hot.push(tail, tail + len as u64, &payload);
                        model.appends.push((tail, payload));
                        tail += len as u64;
                    }
                    6 | 7 => {
                        // Flush a prefix, possibly ending mid-block.
                        let end = flushed + rng.below(tail - flushed + 1);
                        hot.flush_prefix(end);
                        model.flush_prefix(end);
                        flushed = end;
                    }
                    _ => {
                        // Snapshot round trip.
                        let segments = hot.hot_segments();
                        let restored = HotBuffer::from_snapshot(hot.payload(), &segments);
                        assert_eq!(restored.hot_segments(), segments, "seed {seed}");
                        hot = restored;
                    }
                }
                assert_eq!(hot.len(), model.len(), "seed {seed} step {step}");
                assert_eq!(scanned_len(&hot), model.len(), "seed {seed} step {step}");
                let lo = flushed.saturating_sub(10);
                let hi = tail + 10;
                let from = lo + rng.below(hi - lo);
                let to = from + rng.below(hi - from + 1);
                assert_eq!(
                    flatten(hot.read_segments(from, to)),
                    model.bytes_in(from, to),
                    "seed {seed} step {step} read [{from}, {to})"
                );
                if let Some(first) = model.appends.first() {
                    assert_eq!(hot.first_start_offset(), Some(first.0));
                    let contiguous_end = model
                        .appends
                        .iter()
                        .try_fold(first.0, |end, (offset, bytes)| {
                            (*offset == end).then_some(end + bytes.len() as u64)
                        });
                    let planned = hot.plan_cold_flush_from(first.0, 1, usize::MAX);
                    let expected_end = contiguous_end.unwrap_or_else(|| {
                        let mut end = first.0;
                        for (offset, bytes) in &model.appends {
                            if *offset != end {
                                break;
                            }
                            end += bytes.len() as u64;
                        }
                        end
                    });
                    assert_eq!(planned.map(|(_, end, _)| end), Some(expected_end));
                } else {
                    assert!(hot.is_empty(), "seed {seed} step {step}");
                }
                for block in &hot.blocks {
                    assert!(!block.bytes.is_empty());
                }
            }
        }
    }
}
