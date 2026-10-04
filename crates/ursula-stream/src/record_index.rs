//! Retained record-ordinal to canonical-offset boundaries for JSON streams.
//!
//! Records that are not yet flushed keep exact start offsets (the dense
//! part). From feature level 2 (bounded-stream-state F1) records whose bytes
//! are cold are *sealed*: the index keeps one [`RecordMark`] per 1 MiB block
//! of cold log that contains a record start, and a lookup of a sealed record
//! returns a [`RecordBracket`] that a reader resolves by counting LFs in at
//! most one block. Stored JSON records are one compact value plus one LF and
//! contain no other LF, so the dense offsets are exactly a cache of LF
//! positions, and a mark plus a bounded scan reproduces them.
//!
//! Invariants (design §5.2, checked by [`StreamRecordIndex::validate`]):
//!
//! - M1: `first_record <= dense_first_record <= next_record`.
//! - M2: sealed records exist exactly when `marks` is non-empty, and then
//!   `marks[0] == (first_record, retained_offset)`.
//! - M3: marks strictly increase in record and offset, consecutive marks lie
//!   in strictly increasing blocks, and the last mark's record is below
//!   `dense_first_record`.
//! - M4 (locality): every sealed record starts in the same block as the last
//!   mark at or below it (sealing emits a mark for each record that starts
//!   in a later block than the previous mark).
//! - M5: dense offsets strictly increase and lie below the tail; with marks
//!   present the first exceeds the last mark's offset, without marks it is
//!   the retained offset.

use std::collections::VecDeque;

use serde::Deserialize;
use serde::Serialize;

/// Log2 of the mark block size. Fixed by feature level 2.
pub const MARK_BLOCK_SHIFT: u32 = 20;
/// Mark block size: 1 MiB.
pub const MARK_BLOCK_BYTES: u64 = 1 << MARK_BLOCK_SHIFT;
/// Most records one command seals (`FlushCold`, `AppendExternal`, external
/// create or `TidyStream`), so a legacy stream converges over several
/// commands without stalling the group's apply.
pub const SEAL_BUDGET_RECORDS: u64 = 1_000_000;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "StreamRecordIndexWire", into = "StreamRecordIndexWire")]
pub struct StreamRecordIndex {
    first_record: u64,
    marks: Vec<RecordMark>,
    dense_first_record: u64,
    dense_offsets: VecDeque<u64>,
}

/// Serde form shared by backup export (#154) and `ImportSnapshot`. Absent
/// sparse fields mean all-dense, so payloads written before F1 decode
/// unchanged; `record_offsets` holds the dense offsets only.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StreamRecordIndexWire {
    first_record: u64,
    record_offsets: Vec<u64>,
    #[serde(default)]
    marks: Vec<RecordMark>,
    #[serde(default)]
    dense_first_record: Option<u64>,
}

impl From<StreamRecordIndexWire> for StreamRecordIndex {
    fn from(wire: StreamRecordIndexWire) -> Self {
        Self {
            first_record: wire.first_record,
            dense_first_record: wire.dense_first_record.unwrap_or(wire.first_record),
            marks: wire.marks,
            dense_offsets: wire.record_offsets.into(),
        }
    }
}

impl From<StreamRecordIndex> for StreamRecordIndexWire {
    fn from(index: StreamRecordIndex) -> Self {
        let sealed = !index.marks.is_empty();
        Self {
            first_record: index.first_record,
            record_offsets: index.dense_offsets.into(),
            marks: index.marks,
            dense_first_record: sealed.then_some(index.dense_first_record),
        }
    }
}

/// One sealed record whose exact start offset the index keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordMark {
    pub record: u64,
    pub offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRecordRange {
    pub first_record: u64,
    pub next_record: u64,
}

/// Where a sealed record or offset lies: record `from_record` starts exactly
/// at `from_offset`, and the target starts strictly between `from_offset`
/// and `limit`. `limit` never exceeds the end of `from_offset`'s 1 MiB block,
/// so resolving the target scans at most one block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordBracket {
    pub from_record: u64,
    pub from_offset: u64,
    pub limit: u64,
}

/// Result of [`StreamRecordIndex::offset_for`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOffset {
    Exact(u64),
    Bracket(RecordBracket),
}

impl RecordOffset {
    /// The exact offset, when the index holds it.
    pub fn exact(self) -> Option<u64> {
        match self {
            Self::Exact(offset) => Some(offset),
            Self::Bracket(_) => None,
        }
    }

    /// An offset at or above the record's start: the exact offset, or the
    /// bracket's exclusive limit.
    pub fn upper_bound(self) -> u64 {
        match self {
            Self::Exact(offset) => offset,
            Self::Bracket(bracket) => bracket.limit,
        }
    }
}

/// Result of [`StreamRecordIndex::locate_offset`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetLocation {
    /// The offset starts this record (or is the tail, for `next_record`).
    Exact(u64),
    /// A sealed offset: it is a record boundary exactly when the byte before
    /// it is LF, and then its record is `from_record` plus the LFs in
    /// `[from_offset, offset)`.
    Bracket(RecordBracket),
    /// Provably not a record boundary.
    NotBoundary,
}

#[derive(Debug)]
pub(crate) struct PreparedRecordAppend {
    range: StreamRecordRange,
    record_offsets: Vec<u64>,
}

impl PreparedRecordAppend {
    pub(crate) fn range(&self) -> StreamRecordRange {
        self.range
    }
}

/// A validated retention advance; see [`StreamRecordIndex::prepare_retain`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct PreparedRetain {
    kind: RetainKind,
    first_record: u64,
    effective_offset: u64,
}

impl PreparedRetain {
    /// The offset retention actually lands on: the target when it is dense
    /// or a mark, otherwise the mark at or below it (F1).
    pub(crate) fn effective_offset(&self) -> u64 {
        self.effective_offset
    }
}

#[derive(Debug, Clone, Copy)]
enum RetainKind {
    /// Drop every mark and this many dense offsets.
    Dense { removed: usize },
    /// Drop the marks below this one.
    Mark { index: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordIndexError {
    InvalidBoundaries,
    ArithmeticOverflow,
    RecordGone {
        first_record: u64,
        next_record: u64,
    },
    RecordBeyondTail {
        next_record: u64,
    },
    OffsetNotRecordBoundary,
    /// The record is sealed; its offset needs a bounded scan.
    RecordSealed,
}

pub fn is_json_record_content_type(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
}

pub fn canonical_json_record_ends(
    content_type: &str,
    payload: &[u8],
) -> Result<Vec<u64>, RecordIndexError> {
    if !is_json_record_content_type(content_type) {
        return Ok(Vec::new());
    }
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    if payload.last() != Some(&b'\n') {
        return Err(RecordIndexError::InvalidBoundaries);
    }
    memchr::memchr_iter(b'\n', payload)
        .map(|index| {
            u64::try_from(index.saturating_add(1)).map_err(|_| RecordIndexError::ArithmeticOverflow)
        })
        .collect()
}

/// First offset of the 1 MiB block after the one holding `offset`.
pub fn mark_block_end(offset: u64) -> u64 {
    (offset >> MARK_BLOCK_SHIFT)
        .saturating_add(1)
        .saturating_mul(MARK_BLOCK_BYTES)
}

fn mark_block(offset: u64) -> u64 {
    offset >> MARK_BLOCK_SHIFT
}

fn to_u64(value: usize) -> Result<u64, RecordIndexError> {
    u64::try_from(value).map_err(|_| RecordIndexError::ArithmeticOverflow)
}

fn to_usize(value: u64) -> Result<usize, RecordIndexError> {
    usize::try_from(value).map_err(|_| RecordIndexError::ArithmeticOverflow)
}

impl StreamRecordIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Restores an all-dense index (snapshots written before F1, or without
    /// sealed records).
    pub fn restore(
        first_record: u64,
        record_offsets: Vec<u64>,
        retained_offset: u64,
        tail_offset: u64,
    ) -> Result<Self, RecordIndexError> {
        Self::restore_sparse(
            first_record,
            Vec::new(),
            first_record,
            record_offsets,
            retained_offset,
            tail_offset,
        )
    }

    /// Restores an index with sealed records (snapshot fields 14, 15, 17-19).
    pub fn restore_sparse(
        first_record: u64,
        marks: Vec<RecordMark>,
        dense_first_record: u64,
        dense_offsets: Vec<u64>,
        retained_offset: u64,
        tail_offset: u64,
    ) -> Result<Self, RecordIndexError> {
        let index = Self {
            first_record,
            marks,
            dense_first_record,
            dense_offsets: dense_offsets.into(),
        };
        index.validate(retained_offset, tail_offset)?;
        Ok(index)
    }

    pub fn range(&self) -> Result<StreamRecordRange, RecordIndexError> {
        let next_record = self
            .dense_first_record
            .checked_add(to_u64(self.dense_offsets.len())?)
            .ok_or(RecordIndexError::ArithmeticOverflow)?;
        Ok(StreamRecordRange {
            first_record: self.first_record,
            next_record,
        })
    }

    /// Sealed-record marks, oldest first.
    pub fn marks(&self) -> &[RecordMark] {
        &self.marks
    }

    /// First record with an exact (dense) offset.
    pub fn dense_first_record(&self) -> u64 {
        self.dense_first_record
    }

    /// Exact start offsets of `[dense_first_record, next_record)`.
    pub fn dense_offsets(&self) -> &VecDeque<u64> {
        &self.dense_offsets
    }

    /// Number of dense offsets.
    pub fn dense_len(&self) -> usize {
        self.dense_offsets.len()
    }

    #[cfg(test)]
    pub(crate) fn record_offsets_capacity(&self) -> usize {
        self.dense_offsets.capacity()
    }

    #[cfg(test)]
    pub(crate) fn marks_capacity(&self) -> usize {
        self.marks.capacity()
    }

    pub fn append_relative_ends(
        &mut self,
        base_offset: u64,
        payload_len: u64,
        relative_ends: &[u64],
    ) -> Result<StreamRecordRange, RecordIndexError> {
        let prepared = self.prepare_append(base_offset, payload_len, relative_ends)?;
        Ok(self.commit_append(prepared))
    }

    pub(crate) fn prepare_append(
        &self,
        base_offset: u64,
        payload_len: u64,
        relative_ends: &[u64],
    ) -> Result<PreparedRecordAppend, RecordIndexError> {
        validate_relative_ends(payload_len, relative_ends)?;
        let record_start = self.range()?.next_record;
        let record_next = record_start
            .checked_add(to_u64(relative_ends.len())?)
            .ok_or(RecordIndexError::ArithmeticOverflow)?;
        let last_anchor = self
            .dense_offsets
            .back()
            .copied()
            .or_else(|| self.marks.last().map(|mark| mark.offset));
        if last_anchor.is_some_and(|last| last >= base_offset) {
            return Err(RecordIndexError::InvalidBoundaries);
        }
        let mut starts = Vec::with_capacity(relative_ends.len());
        let mut previous_end = 0;
        for end in relative_ends {
            let start_offset = base_offset
                .checked_add(previous_end)
                .ok_or(RecordIndexError::ArithmeticOverflow)?;
            starts.push(start_offset);
            previous_end = *end;
        }
        Ok(PreparedRecordAppend {
            range: StreamRecordRange {
                first_record: record_start,
                next_record: record_next,
            },
            record_offsets: starts,
        })
    }

    pub(crate) fn commit_append(&mut self, prepared: PreparedRecordAppend) -> StreamRecordRange {
        self.dense_offsets.extend(prepared.record_offsets);
        prepared.range
    }

    /// Index of the last mark whose record is at or below `record`.
    fn mark_index_le_record(&self, record: u64) -> Option<usize> {
        self.marks
            .partition_point(|mark| mark.record <= record)
            .checked_sub(1)
    }

    /// Index of the last mark whose offset is at or below `offset`.
    fn mark_index_le_offset(&self, offset: u64) -> Option<usize> {
        self.marks
            .partition_point(|mark| mark.offset <= offset)
            .checked_sub(1)
    }

    /// The last mark at or below `offset`.
    pub fn mark_le_offset(&self, offset: u64) -> Option<RecordMark> {
        self.mark_index_le_offset(offset)
            .and_then(|index| self.marks.get(index).copied())
    }

    /// The anchor after mark `index`: the next mark, else the first dense
    /// offset, else the tail.
    fn next_anchor_offset(&self, index: usize, tail_offset: u64) -> u64 {
        index
            .checked_add(1)
            .and_then(|next| self.marks.get(next))
            .map(|mark| mark.offset)
            .or_else(|| self.dense_offsets.front().copied())
            .unwrap_or(tail_offset)
    }

    fn bracket_at(&self, index: usize, tail_offset: u64) -> Option<RecordBracket> {
        let mark = self.marks.get(index)?;
        Some(RecordBracket {
            from_record: mark.record,
            from_offset: mark.offset,
            limit: mark_block_end(mark.offset).min(self.next_anchor_offset(index, tail_offset)),
        })
    }

    /// Start offset of `record`: exact for dense records, marks and
    /// `next_record` (the tail), a [`RecordBracket`] for other sealed
    /// records.
    pub fn offset_for(
        &self,
        record: u64,
        tail_offset: u64,
    ) -> Result<RecordOffset, RecordIndexError> {
        let range = self.range()?;
        if record < range.first_record {
            return Err(RecordIndexError::RecordGone {
                first_record: range.first_record,
                next_record: range.next_record,
            });
        }
        if record > range.next_record {
            return Err(RecordIndexError::RecordBeyondTail {
                next_record: range.next_record,
            });
        }
        if record == range.next_record {
            return Ok(RecordOffset::Exact(tail_offset));
        }
        if record >= self.dense_first_record {
            let index = to_usize(record - self.dense_first_record)?;
            return self
                .dense_offsets
                .get(index)
                .copied()
                .map(RecordOffset::Exact)
                .ok_or(RecordIndexError::InvalidBoundaries);
        }
        let index = self
            .mark_index_le_record(record)
            .ok_or(RecordIndexError::InvalidBoundaries)?;
        let bracket = self
            .bracket_at(index, tail_offset)
            .ok_or(RecordIndexError::InvalidBoundaries)?;
        if bracket.from_record == record {
            return Ok(RecordOffset::Exact(bracket.from_offset));
        }
        Ok(RecordOffset::Bracket(bracket))
    }

    /// Exact start offset of `record`, or [`RecordIndexError::RecordSealed`]
    /// when only a bracket is known.
    pub fn exact_offset_for(&self, record: u64, tail_offset: u64) -> Result<u64, RecordIndexError> {
        self.offset_for(record, tail_offset)?
            .exact()
            .ok_or(RecordIndexError::RecordSealed)
    }

    /// Where `offset` lies in record coordinates. Offsets below the first
    /// retained record or above the tail are not boundaries.
    pub fn locate_offset(
        &self,
        offset: u64,
        tail_offset: u64,
    ) -> Result<OffsetLocation, RecordIndexError> {
        let range = self.range()?;
        if offset == tail_offset {
            return Ok(OffsetLocation::Exact(range.next_record));
        }
        if offset > tail_offset {
            return Ok(OffsetLocation::NotBoundary);
        }
        if let Some(first_dense) = self.dense_offsets.front()
            && offset >= *first_dense
        {
            return Ok(match self.dense_offsets.binary_search(&offset) {
                Ok(index) => OffsetLocation::Exact(
                    self.dense_first_record
                        .checked_add(to_u64(index)?)
                        .ok_or(RecordIndexError::ArithmeticOverflow)?,
                ),
                Err(_) => OffsetLocation::NotBoundary,
            });
        }
        let Some(index) = self.mark_index_le_offset(offset) else {
            return Ok(OffsetLocation::NotBoundary);
        };
        let bracket = self
            .bracket_at(index, tail_offset)
            .ok_or(RecordIndexError::InvalidBoundaries)?;
        if offset == bracket.from_offset {
            return Ok(OffsetLocation::Exact(bracket.from_record));
        }
        if offset >= bracket.limit {
            // Inside the last record that starts in the mark's block (M4).
            return Ok(OffsetLocation::NotBoundary);
        }
        Ok(OffsetLocation::Bracket(bracket))
    }

    /// Anchors with exact offsets in `(after, through]`: marks and the first
    /// dense offset. A scan that crosses them verifies them (F19 step 3).
    pub fn anchors_between(&self, after: u64, through: u64) -> Vec<RecordMark> {
        let start = self.marks.partition_point(|mark| mark.offset <= after);
        let mut anchors = self
            .marks
            .iter()
            .skip(start)
            .take_while(|mark| mark.offset <= through)
            .copied()
            .collect::<Vec<_>>();
        if let Some(first_dense) = self.dense_offsets.front()
            && *first_dense > after
            && *first_dense <= through
        {
            anchors.push(RecordMark {
                record: self.dense_first_record,
                offset: *first_dense,
            });
        }
        anchors
    }

    /// Dense records whose bytes end at or below `seal_point`.
    pub fn sealable_records(&self, seal_point: u64, tail_offset: u64) -> u64 {
        let ends_below = self
            .dense_offsets
            .partition_point(|offset| *offset < seal_point);
        // Record i ends at dense[i + 1] (or the tail for the last one).
        let sealable = if seal_point >= tail_offset {
            ends_below
        } else {
            ends_below.saturating_sub(1)
        };
        u64::try_from(sealable).unwrap_or(u64::MAX)
    }

    /// Seals up to `budget` dense records whose end is at or below
    /// `seal_point`, oldest first, emitting a mark for each sealed record
    /// that starts in a later block than the previous mark (M4). Returns the
    /// number of records sealed. A record that straddles `seal_point` stays
    /// dense.
    pub fn seal_below(&mut self, seal_point: u64, tail_offset: u64, budget: u64) -> u64 {
        let mut sealed = 0_u64;
        while sealed < budget {
            let Some(start) = self.dense_offsets.front().copied() else {
                break;
            };
            let end = self.dense_offsets.get(1).copied().unwrap_or(tail_offset);
            if end > seal_point {
                break;
            }
            let needs_mark = self
                .marks
                .last()
                .is_none_or(|last| mark_block(start) > mark_block(last.offset));
            if needs_mark {
                self.marks.push(RecordMark {
                    record: self.dense_first_record,
                    offset: start,
                });
            }
            self.dense_offsets.pop_front();
            self.dense_first_record = self.dense_first_record.saturating_add(1);
            sealed = sealed.saturating_add(1);
        }
        // F7, bounded per command: releasing capacity copies the remaining
        // dense offsets, so a legacy stream mid-migration (more than one
        // budget of dense records left) shrinks once it gets below that.
        if sealed > 0 && to_u64(self.dense_offsets.len()).is_ok_and(|len| len <= budget) {
            shrink_deque(&mut self.dense_offsets);
        }
        sealed
    }

    pub fn retain_from_offset(
        &mut self,
        retained_offset: u64,
        tail_offset: u64,
    ) -> Result<u64, RecordIndexError> {
        let prepared = self.prepare_retain(retained_offset, tail_offset)?;
        Ok(self.commit_retain(prepared))
    }

    /// Validates a retention advance against the borrowed index without
    /// mutating it (F7: retention no longer clones the index to validate).
    /// A dense target or a mark retains exactly; any other sealed target
    /// lands on the last mark at or below it (F1), so apply never stores a
    /// record number it cannot recompute.
    pub(crate) fn prepare_retain(
        &self,
        retained_offset: u64,
        tail_offset: u64,
    ) -> Result<PreparedRetain, RecordIndexError> {
        let next_record = self.range()?.next_record;
        if retained_offset == tail_offset {
            return Ok(PreparedRetain {
                kind: RetainKind::Dense {
                    removed: self.dense_offsets.len(),
                },
                first_record: next_record,
                effective_offset: tail_offset,
            });
        }
        if let Some(first_dense) = self.dense_offsets.front()
            && retained_offset >= *first_dense
        {
            let removed = self
                .dense_offsets
                .binary_search(&retained_offset)
                .map_err(|_| RecordIndexError::OffsetNotRecordBoundary)?;
            return Ok(PreparedRetain {
                kind: RetainKind::Dense { removed },
                first_record: self
                    .dense_first_record
                    .checked_add(to_u64(removed)?)
                    .ok_or(RecordIndexError::ArithmeticOverflow)?,
                effective_offset: retained_offset,
            });
        }
        let index = self
            .mark_index_le_offset(retained_offset)
            .ok_or(RecordIndexError::OffsetNotRecordBoundary)?;
        let bracket = self
            .bracket_at(index, tail_offset)
            .ok_or(RecordIndexError::InvalidBoundaries)?;
        if retained_offset != bracket.from_offset && retained_offset >= bracket.limit {
            return Err(RecordIndexError::OffsetNotRecordBoundary);
        }
        Ok(PreparedRetain {
            kind: RetainKind::Mark { index },
            first_record: bracket.from_record,
            effective_offset: bracket.from_offset,
        })
    }

    /// Applies a prepared retention in place and releases dropped capacity
    /// once it exceeds `2 * len + 64`.
    pub(crate) fn commit_retain(&mut self, prepared: PreparedRetain) -> u64 {
        match prepared.kind {
            RetainKind::Dense { removed } => {
                let removed = removed.min(self.dense_offsets.len());
                self.marks.clear();
                self.marks.shrink_to_fit();
                self.dense_offsets.drain(..removed);
                self.first_record = prepared.first_record;
                self.dense_first_record = prepared.first_record;
                shrink_deque(&mut self.dense_offsets);
            }
            RetainKind::Mark { index } => {
                let index = index.min(self.marks.len());
                self.marks.drain(..index);
                self.first_record = prepared.first_record;
                let len = self.marks.len();
                if self.marks.capacity() > len.saturating_mul(2).saturating_add(64) {
                    self.marks.shrink_to(len.saturating_mul(2));
                }
            }
        }
        self.first_record
    }

    pub fn validate(&self, retained_offset: u64, tail_offset: u64) -> Result<(), RecordIndexError> {
        let range = self.range()?;
        if retained_offset > tail_offset || self.first_record > self.dense_first_record {
            return Err(RecordIndexError::InvalidBoundaries);
        }
        if self
            .dense_offsets
            .iter()
            .zip(self.dense_offsets.iter().skip(1))
            .any(|(left, right)| left >= right)
            || self
                .dense_offsets
                .back()
                .is_some_and(|last| *last >= tail_offset)
        {
            return Err(RecordIndexError::InvalidBoundaries);
        }
        let Some(first_mark) = self.marks.first() else {
            if self.first_record != self.dense_first_record {
                return Err(RecordIndexError::InvalidBoundaries);
            }
            return match self.dense_offsets.front() {
                None if retained_offset == tail_offset => Ok(()),
                Some(first) if *first == retained_offset => Ok(()),
                _ => Err(RecordIndexError::InvalidBoundaries),
            };
        };
        if *first_mark
            != (RecordMark {
                record: self.first_record,
                offset: retained_offset,
            })
        {
            return Err(RecordIndexError::InvalidBoundaries);
        }
        let marks_ordered =
            self.marks
                .iter()
                .zip(self.marks.iter().skip(1))
                .all(|(left, right)| {
                    left.record < right.record
                        && mark_block(left.offset) < mark_block(right.offset)
                        && right.offset - left.offset >= right.record - left.record
                });
        let Some(last_mark) = self.marks.last() else {
            return Err(RecordIndexError::InvalidBoundaries);
        };
        let next_anchor = self.dense_offsets.front().copied().unwrap_or(tail_offset);
        if !marks_ordered
            || last_mark.record >= self.dense_first_record
            || last_mark.offset >= next_anchor
            || next_anchor - last_mark.offset < self.dense_first_record - last_mark.record
            || range.next_record < self.dense_first_record
        {
            return Err(RecordIndexError::InvalidBoundaries);
        }
        Ok(())
    }
}

fn shrink_deque(deque: &mut VecDeque<u64>) {
    let len = deque.len();
    if deque.capacity() > len.saturating_mul(2).saturating_add(64) {
        deque.shrink_to(len.saturating_mul(2));
    }
}

fn validate_relative_ends(payload_len: u64, relative_ends: &[u64]) -> Result<(), RecordIndexError> {
    if payload_len == 0 {
        return relative_ends
            .is_empty()
            .then_some(())
            .ok_or(RecordIndexError::InvalidBoundaries);
    }
    if relative_ends.last().copied() != Some(payload_len)
        || relative_ends.first().copied() == Some(0)
        || relative_ends.windows(2).any(|pair| {
            let [left, right] = pair else {
                return true;
            };
            left >= right
        })
    {
        return Err(RecordIndexError::InvalidBoundaries);
    }
    Ok(())
}

/// How a bracketed record read turns its byte window into records (F1 read
/// path). The window starts at `window_offset`; when `leading_lf` is set its
/// first byte is the byte before the anchor `(window_record, window_offset +
/// 1)` and must be LF. The reader skips `skip` records, then takes up to
/// `take` complete records whose bytes fit in `max_bytes` (at least one).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordTrim {
    pub window_record: u64,
    pub leading_lf: bool,
    pub skip: u64,
    pub take: u64,
    pub max_bytes: u64,
    /// Exact anchors inside the window, verified while scanning (F19
    /// step 3).
    pub anchors: Vec<RecordMark>,
    pub tail_offset: u64,
    /// Whether a result that reaches the tail may claim `up_to_date`
    /// (false on followers, which forward or hold up-to-date reads).
    pub claim_up_to_date: bool,
}

/// The records a [`RecordTrim`] selected, as byte positions in the window
/// and stream coordinates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrimmedRecords {
    pub start: usize,
    pub end: usize,
    pub offset: u64,
    pub next_offset: u64,
    pub record_range: StreamRecordRange,
    pub up_to_date: bool,
}

/// A scan found bytes that disagree with the record index (RC-21).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("record coordinate corruption: {0}")]
pub struct RecordCorruption(pub String);

/// Resolves a bracketed record window by counting LFs. Never returns
/// shifted records: an anchor that is not preceded by LF, an LF count that
/// disagrees with an anchor's record, or a window without the requested
/// records is a [`RecordCorruption`].
pub fn trim_record_window(
    window: &[u8],
    window_offset: u64,
    trim: &RecordTrim,
) -> Result<TrimmedRecords, RecordCorruption> {
    let mut pos = 0_usize;
    if trim.leading_lf {
        if window.first() != Some(&b'\n') {
            return Err(RecordCorruption(format!(
                "anchor at offset {} is not preceded by LF",
                window_offset.saturating_add(1)
            )));
        }
        pos = 1;
    }
    let abs = |index: usize| window_offset.saturating_add(u64::try_from(index).unwrap_or(u64::MAX));
    let mut anchors = trim.anchors.clone();
    anchors.sort_unstable_by_key(|anchor| anchor.offset);
    let mut next_anchor = 0_usize;
    // Record `current` starts at window position `current_start`.
    let mut current = trim.window_record;
    let mut current_start = pos;
    let target = trim.window_record.saturating_add(trim.skip);
    let mut first_start = (trim.skip == 0).then_some(pos);
    let mut taken = 0_u64;
    let mut end = None;
    let mut exhausted = false;
    let mut lfs = memchr::memchr_iter(b'\n', window.get(pos..).unwrap_or_default());
    loop {
        // Verify anchors at or before the current record start.
        while let Some(anchor) = anchors.get(next_anchor) {
            let start_offset = abs(current_start);
            if anchor.offset > start_offset {
                break;
            }
            if anchor.offset == start_offset && anchor.record != current {
                return Err(RecordCorruption(format!(
                    "anchor ({}, {}) disagrees with {} LFs counted from record {}",
                    anchor.record,
                    anchor.offset,
                    current.saturating_sub(trim.window_record),
                    trim.window_record
                )));
            }
            if anchor.offset < start_offset {
                return Err(RecordCorruption(format!(
                    "anchor at offset {} is not preceded by LF",
                    anchor.offset
                )));
            }
            next_anchor += 1;
        }
        if first_start.is_some() && taken >= trim.take {
            break;
        }
        let Some(relative) = lfs.next() else {
            exhausted = true;
            break;
        };
        let record_end = pos.saturating_add(relative).saturating_add(1);
        if let Some(start) = first_start {
            let bytes = u64::try_from(record_end - start).unwrap_or(u64::MAX);
            if taken > 0 && bytes > trim.max_bytes {
                break;
            }
            taken += 1;
            end = Some(record_end);
        }
        current = current.saturating_add(1);
        current_start = record_end;
        if first_start.is_none() && current == target {
            first_start = Some(record_end);
        }
    }
    // Once every LF is counted, an anchor left inside the window lies in an
    // unterminated record: it is not preceded by LF.
    if exhausted
        && let Some(anchor) = anchors.get(next_anchor)
        && anchor.offset <= abs(window.len())
    {
        return Err(RecordCorruption(format!(
            "anchor at offset {} is not preceded by LF",
            anchor.offset
        )));
    }
    let (Some(start), Some(end)) = (first_start, end) else {
        return Err(RecordCorruption(format!(
            "window [{}, {}) holds fewer records than its bracket promises",
            window_offset,
            abs(window.len())
        )));
    };
    let offset = abs(start);
    let next_offset = abs(end);
    Ok(TrimmedRecords {
        start,
        end,
        offset,
        next_offset,
        record_range: StreamRecordRange {
            first_record: target,
            next_record: target.saturating_add(taken),
        },
        up_to_date: trim.claim_up_to_date && next_offset == trim.tail_offset,
    })
}

#[cfg(test)]
mod tests;
