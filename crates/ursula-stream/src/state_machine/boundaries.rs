//! F4b message boundaries without message records (feature level 4).
//!
//! Below level 4 every stream keeps `message_records`: one `[start, end)`
//! entry per JSON record or per binary append, collapsed at cold transitions
//! (F4a). From level 4 the field is gone from live state. A message boundary
//! is only needed at or above the seal point `p(s)`, where bootstrap answers
//! one part per message, and those boundaries already exist elsewhere:
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
//!
//! A stream that still holds legacy message records after the raise converts
//! them on its next append, external append, flush, retention or
//! `TidyStream` ([`StreamStateMachine::migrate_message_records`]); until then
//! readers keep using the legacy records. The conversion is O(log n) plus the
//! records at or above the seal point, which the hot window bounds.

use std::collections::VecDeque;

use ursula_shard::BucketStreamId;

use super::StreamMessageRecord;
use super::StreamSlot;
use super::StreamStateMachine;

impl StreamStateMachine {
    /// Feature level 4 (F4b): message records are no longer kept.
    pub(super) fn message_records_removed(&self) -> bool {
        self.feature_level >= crate::feature::FEATURE_LEVEL_HOT_REPRESENTATION
    }

    /// Whether readers of `slot` use the derived boundaries: level 4 and no
    /// legacy message records left on the stream.
    pub(super) fn derived_boundaries(&self, slot: &StreamSlot) -> bool {
        self.message_records_removed() && slot.message_records.is_empty()
    }

    /// Converts legacy message records of `stream_id` into the level-4
    /// representation. No-op below level 4 or when nothing is left to
    /// convert. Deterministic: every replica converts at the same command.
    pub(super) fn migrate_message_records(&mut self, stream_id: &BucketStreamId) {
        if !self.message_records_removed() {
            return;
        }
        let Some(slot) = self.stream_slot_mut(stream_id) else {
            return;
        };
        if slot.message_records.is_empty() {
            if slot.message_records.capacity() > 0 {
                slot.message_records = Vec::new();
            }
            return;
        }
        let records = std::mem::take(&mut slot.message_records);
        if slot.record_index.is_some() {
            // JSON: the dense offsets already hold every record start at or
            // above the seal point.
            self.sync_hot_index(stream_id);
            return;
        }
        let seal_point = slot.seal_point();
        let retained_offset = slot.retained_offset;
        let first_run_end = slot.hot_buffer.first_end_offset();
        let from = records.partition_point(|record| record.start_offset < seal_point);
        for record in records.iter().skip(from) {
            if record.start_offset == seal_point
                && (seal_point > retained_offset
                    || first_run_end.is_some_and(|end| record.end_offset > end))
            {
                // A legacy record at the seal point may be the tail fragment
                // of a message a flush split, or a level-0 collapsed record
                // that folds several messages. Neither is a message start;
                // the exact frontier moves past it, as it did before.
                continue;
            }
            slot.hot_buffer.push_append_start(record.start_offset);
        }
        // The hot-record gauge now counts the derived boundaries.
        self.sync_hot_index(stream_id);
    }
}

impl StreamSlot {
    /// Records the message boundaries of one non-empty append that
    /// occupies `[start_offset, end_offset)`. Below level 4 this extends the
    /// message records; from level 4 a stream with a record index needs
    /// nothing (its dense offsets are the boundaries) and any other stream
    /// keeps the starts that lie at or above the seal point. Callers update
    /// the tail and the hot buffer first.
    pub(super) fn record_message_boundaries(
        &mut self,
        records_removed: bool,
        start_offset: u64,
        end_offset: u64,
        record_ends: &[u64],
    ) {
        let records =
            StreamStateMachine::message_records_for_append(start_offset, end_offset, record_ends);
        if !records_removed {
            self.message_records.extend(records);
            return;
        }
        if self.record_index.is_some() {
            return;
        }
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

    /// Lowest offset from which every message is exact (level 4): the first
    /// derived start at or above the seal point, or the tail.
    pub(super) fn derived_exact_frontier(&self) -> u64 {
        let index = self.derived_start_index(0);
        self.derived_start_offsets()
            .get(index)
            .copied()
            .unwrap_or(self.metadata.tail_offset)
            .max(self.retained_offset)
    }

    /// Whether `offset` is a message boundary at or above the seal point
    /// (level 4): a derived start, or the tail.
    pub(super) fn derived_is_boundary(&self, offset: u64) -> bool {
        if offset == self.metadata.tail_offset {
            return true;
        }
        let index = self.derived_start_index(offset);
        self.derived_start_offsets().get(index) == Some(&offset)
    }

    /// Derived messages that start at or above `from` and the seal point
    /// (level 4), in order.
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

/// Restore check for the level-4 representation of one stream: append
/// starts strictly increase, lie in `[seal_point, tail)`, and exist only
/// for streams without a record index at level 4.
pub(super) fn append_starts_valid(
    starts: &[u64],
    seal_point: u64,
    tail_offset: u64,
    indexed: bool,
    records_removed: bool,
) -> bool {
    if starts.is_empty() {
        return true;
    }
    if indexed || !records_removed {
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
