//! F4b message boundaries without message records.
//!
//! Replicated state keeps no per-message records. A message boundary is only
//! needed at or above the seal point `p(s)`, where bootstrap answers one part
//! per message, and those boundaries already exist elsewhere:
//!
//! - streams with a record index (JSON) use the dense record offsets: every
//!   record that starts at or above `p(s)` is dense (F1 seals only records
//!   that end at or below it);
//! - every other stream keeps the start offset of each message at or above
//!   `p(s)` in the hot buffer (`HotBuffer::append_starts`), external appends
//!   above hot bytes included.
//!
//! Message `i` spans from its start to the next start, or to the tail. Below
//! `p(s)` there are no per-message boundaries: snapshot alignment accepts any
//! offset there (F18), and bootstrap answers an honest partial when the
//! snapshot offset lies below the first exact boundary.

use std::collections::VecDeque;

use super::StreamMessageRecord;
use super::StreamSlot;
use super::StreamStateMachine;

impl StreamSlot {
    /// Records the message boundaries of one non-empty append that
    /// occupies `[start_offset, end_offset)`. A stream with a record index
    /// needs nothing (its dense offsets are the boundaries); any other stream
    /// keeps the starts that lie at or above the seal point. Callers update
    /// the tail and the hot buffer first.
    pub(super) fn record_message_boundaries(
        &mut self,
        start_offset: u64,
        end_offset: u64,
        record_ends: &[u64],
    ) {
        if self.record_index.is_some() {
            return;
        }
        let records =
            StreamStateMachine::message_spans_for_append(start_offset, end_offset, record_ends);
        let seal_point = self.seal_point();
        for record in records {
            if record.start_offset >= seal_point {
                self.hot_buffer.push_append_start(record.start_offset);
            }
        }
    }

    /// The sorted offsets that hold the derived message starts: the dense
    /// record offsets with a record index, otherwise the hot append starts.
    /// Only the entries at or above the seal point are message starts.
    fn derived_start_offsets(&self) -> &VecDeque<u64> {
        match self.record_index.as_ref() {
            Some(index) => index.dense_offsets(),
            None => self.hot_buffer.append_starts(),
        }
    }

    /// Index into [`Self::derived_start_offsets`] of the first start at or
    /// above `offset`, never below the seal point.
    fn derived_start_index(&self, offset: u64) -> usize {
        let from = offset.max(self.seal_point());
        self.derived_start_offsets()
            .partition_point(|start| *start < from)
    }

    /// Derived message starts in `[from, to)` (both at or above the seal
    /// point), counted in O(log n).
    pub(super) fn derived_starts_between(&self, from: u64, to: u64) -> u64 {
        let first = self.derived_start_index(from);
        let last = self
            .derived_start_offsets()
            .partition_point(|start| *start < to);
        u64::try_from(last.saturating_sub(first)).unwrap_or(u64::MAX)
    }

    /// Derived messages that start at or above the seal point.
    pub(super) fn derived_hot_messages(&self) -> u64 {
        let first = self.derived_start_index(0);
        u64::try_from(self.derived_start_offsets().len().saturating_sub(first)).unwrap_or(u64::MAX)
    }

    /// Lowest offset from which every message is exact: the first
    /// derived start at or above the seal point, or the tail.
    pub(super) fn derived_exact_frontier(&self) -> u64 {
        let index = self.derived_start_index(0);
        self.derived_start_offsets()
            .get(index)
            .copied()
            .unwrap_or(self.metadata.tail_offset)
            .max(self.retained_offset)
    }

    /// Whether `offset` is a message boundary at or above the seal point:
    /// a derived start, or the tail.
    pub(super) fn derived_is_boundary(&self, offset: u64) -> bool {
        if offset == self.metadata.tail_offset {
            return true;
        }
        let index = self.derived_start_index(offset);
        self.derived_start_offsets().get(index) == Some(&offset)
    }

    /// Derived messages that start at or above `from` and the seal point,
    /// in order.
    pub(super) fn derived_messages_from(&self, from: u64) -> DerivedMessages<'_> {
        DerivedMessages {
            starts: self.derived_start_offsets(),
            next: self.derived_start_index(from),
            tail_offset: self.metadata.tail_offset,
        }
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

/// Restore check for the derived boundaries of one stream: append starts
/// strictly increase, lie in `[seal_point, tail)`, and exist only for
/// streams without a record index.
pub(super) fn append_starts_valid(
    starts: &[u64],
    seal_point: u64,
    tail_offset: u64,
    indexed: bool,
) -> bool {
    if starts.is_empty() {
        return true;
    }
    if indexed {
        return false;
    }
    let mut previous = None;
    for start in starts {
        if *start < seal_point || *start >= tail_offset || previous.is_some_and(|p| p >= *start) {
            return false;
        }
        previous = Some(*start);
    }
    true
}
