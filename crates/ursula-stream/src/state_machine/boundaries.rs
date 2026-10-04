//! F4b message boundaries without message records.
//!
//! Replicated state keeps no per-message records. Only streams with a record
//! index (JSON) have message boundaries: the dense record offsets. Every
//! record that starts at or above the seal point `p(s)` is dense (F1 seals
//! only records that end at or below it).
//!
//! Streams without a record index have no message boundaries at all. Any
//! offset in `[retained, tail]` is a valid snapshot or retention offset, and
//! bootstrap answers the hot range `[S, tail)` as one part.
//!
//! Message `i` spans from its start to the next start, or to the tail. Below
//! `p(s)` there are no per-message boundaries: snapshot alignment accepts any
//! offset there (F18), and bootstrap answers an honest partial when the
//! snapshot offset lies below the first exact boundary.

use std::collections::VecDeque;

use super::StreamMessageRecord;
use super::StreamSlot;

impl StreamSlot {
    /// The dense record offsets of a stream with a record index; `None`
    /// for every other stream. Only the entries at or above the seal point
    /// are message starts.
    fn derived_start_offsets(&self) -> Option<&VecDeque<u64>> {
        self.record_index
            .as_ref()
            .map(|index| index.dense_offsets())
    }

    /// Index into the dense offsets of the first start at or above
    /// `offset`, never below the seal point.
    fn derived_start_index(starts: &VecDeque<u64>, offset: u64, seal_point: u64) -> usize {
        let from = offset.max(seal_point);
        starts.partition_point(|start| *start < from)
    }

    /// Derived message starts in `[from, to)` (both at or above the seal
    /// point), counted in O(log n). Zero for a stream without a record
    /// index, which keeps no per-message bookkeeping.
    pub(super) fn derived_starts_between(&self, from: u64, to: u64) -> u64 {
        let Some(starts) = self.derived_start_offsets() else {
            return 0;
        };
        let first = Self::derived_start_index(starts, from, self.seal_point());
        let last = starts.partition_point(|start| *start < to);
        u64::try_from(last.saturating_sub(first)).unwrap_or(u64::MAX)
    }

    /// Derived messages that start at or above the seal point. Zero for a
    /// stream without a record index.
    pub(super) fn derived_hot_messages(&self) -> u64 {
        let Some(starts) = self.derived_start_offsets() else {
            return 0;
        };
        let first = Self::derived_start_index(starts, 0, self.seal_point());
        u64::try_from(starts.len().saturating_sub(first)).unwrap_or(u64::MAX)
    }

    /// Lowest offset from which bootstrap can answer exactly. With a record
    /// index: the first derived start at or above the seal point, or the
    /// tail. Without one: the seal point. Never below the retained offset.
    pub(super) fn derived_exact_frontier(&self) -> u64 {
        let seal_point = self.seal_point();
        let frontier = match self.derived_start_offsets() {
            Some(starts) => starts
                .get(Self::derived_start_index(starts, 0, seal_point))
                .copied()
                .unwrap_or(self.metadata.tail_offset),
            None => seal_point,
        };
        frontier.max(self.retained_offset)
    }

    /// Whether `offset` is a message boundary at or above the seal point:
    /// a derived start, or the tail. A stream without a record index has no
    /// message boundaries, so every offset qualifies.
    pub(super) fn derived_is_boundary(&self, offset: u64) -> bool {
        let Some(starts) = self.derived_start_offsets() else {
            return true;
        };
        if offset == self.metadata.tail_offset {
            return true;
        }
        let index = Self::derived_start_index(starts, offset, self.seal_point());
        starts.get(index) == Some(&offset)
    }

    /// Derived messages that start at or above `from` and the seal point,
    /// in order; `None` for a stream without a record index.
    pub(super) fn derived_messages_from(&self, from: u64) -> Option<DerivedMessages<'_>> {
        let starts = self.derived_start_offsets()?;
        Some(DerivedMessages {
            starts,
            next: Self::derived_start_index(starts, from, self.seal_point()),
            tail_offset: self.metadata.tail_offset,
        })
    }
}

/// Iterator over derived messages `[start, next start or tail)`.
pub(super) struct DerivedMessages<'a> {
    starts: &'a VecDeque<u64>,
    next: usize,
    tail_offset: u64,
}

impl Iterator for DerivedMessages<'_> {
    type Item = StreamMessageRecord;

    fn next(&mut self) -> Option<Self::Item> {
        let start_offset = *self.starts.get(self.next)?;
        self.next = self.next.saturating_add(1);
        let end_offset = self
            .starts
            .get(self.next)
            .copied()
            .unwrap_or(self.tail_offset);
        (end_offset > start_offset).then_some(StreamMessageRecord {
            start_offset,
            end_offset,
        })
    }
}
