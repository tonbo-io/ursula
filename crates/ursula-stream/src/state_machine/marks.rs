//! F1 sparse cold record marks (bounded-stream-state §5.2, feature level 2):
//! sealing at cold transitions, record lookups, and the record read plan both
//! engines share.

use super::BucketStreamId;
use super::StreamErrorCode;
use super::StreamReadPlan;
use super::StreamResponse;
use super::StreamStateMachine;
use crate::RecordIndexError;
use crate::RecordMark;
use crate::RecordOffset;
use crate::RecordTrim;
use crate::SEAL_BUDGET_RECORDS;
use crate::StreamRecordRange;

/// A record-aware read (`?record=r&max_records=k&max_bytes=b`).
#[derive(Debug, Clone, Copy)]
pub struct RecordReadRequest<'a> {
    pub stream_id: &'a BucketStreamId,
    pub record: u64,
    pub max_records: Option<u64>,
    /// Byte budget for complete records (P7); at least one record returns.
    pub max_bytes: usize,
    pub now_ms: u64,
    /// Server-side continuation anchor from the reader's previous response.
    pub anchor: Option<RecordReadAnchor>,
}

/// Where record `record` of incarnation `incarnation` starts, as a previous
/// read of the same session returned it (F1, RC-20).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordReadAnchor {
    pub incarnation: u64,
    pub record: u64,
    pub offset: u64,
}

/// Why [`StreamStateMachine::record_read_plan`] failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordPlanError {
    /// A protocol error with its stream error code.
    Response(StreamResponse),
    /// An internal record-index inconsistency.
    Index(String),
}

impl From<StreamResponse> for RecordPlanError {
    fn from(response: StreamResponse) -> Self {
        Self::Response(response)
    }
}

fn index_error(context: &str, err: RecordIndexError) -> RecordPlanError {
    RecordPlanError::Index(format!("{context}: {err:?}"))
}

impl StreamStateMachine {
    /// Whether F1 sparse marks are active (feature level 2).
    pub(super) fn sparse_marks_enabled(&self) -> bool {
        self.feature_level >= crate::feature::FEATURE_LEVEL_SPARSE_MARKS
    }

    /// Seals up to [`SEAL_BUDGET_RECORDS`] dense records whose bytes lie
    /// below the stream's seal point. Called at the end of `FlushCold`,
    /// `AppendExternal`, external creates and `TidyStream`; a no-op below
    /// level 2. Returns the number of records sealed.
    pub(super) fn seal_record_index(&mut self, stream_id: &BucketStreamId) -> u64 {
        if !self.sparse_marks_enabled() {
            return 0;
        }
        let Some(slot) = self.stream_slot_mut(stream_id) else {
            return 0;
        };
        let seal_point = slot.seal_point();
        let tail_offset = slot.metadata.tail_offset;
        slot.record_index.as_mut().map_or(0, |index| {
            index.seal_below(seal_point, tail_offset, SEAL_BUDGET_RECORDS)
        })
    }

    /// Whether the stream holds dense records that sealing would move (F1
    /// `TidyStream` debt, level 2).
    pub(super) fn stream_has_seal_debt(&self, stream_id: &BucketStreamId) -> bool {
        if !self.sparse_marks_enabled() {
            return false;
        }
        self.stream_slot(stream_id).is_some_and(|slot| {
            slot.record_index.as_ref().is_some_and(|index| {
                index.sealable_records(slot.seal_point(), slot.metadata.tail_offset) > 0
            })
        })
    }

    /// Start of `record`: exact, or a bracket to resolve by counting LFs.
    /// `Ok(None)` when the stream does not exist or has no record
    /// coordinates.
    pub fn locate_record(
        &self,
        stream_id: &BucketStreamId,
        record: u64,
    ) -> Result<Option<RecordOffset>, RecordIndexError> {
        let Some(slot) = self.stream_slot(stream_id) else {
            return Ok(None);
        };
        slot.record_index
            .as_ref()
            .map(|index| index.offset_for(record, slot.metadata.tail_offset))
            .transpose()
    }

    /// First record with an exact offset (records at or above it are dense).
    pub fn dense_first_record(&self, stream_id: &BucketStreamId) -> Option<u64> {
        self.stream_slot(stream_id)
            .and_then(|slot| slot.record_index.as_ref())
            .map(crate::StreamRecordIndex::dense_first_record)
    }

    /// Exact anchors with offsets in `(after, through]`, which a scan over
    /// that range verifies (F19 step 3).
    pub fn record_anchors_between(
        &self,
        stream_id: &BucketStreamId,
        after: u64,
        through: u64,
    ) -> Vec<RecordMark> {
        self.stream_slot(stream_id)
            .and_then(|slot| slot.record_index.as_ref())
            .map(|index| index.anchors_between(after, through))
            .unwrap_or_default()
    }

    /// The stream's incarnation (its `created_at_ms`), which continuation
    /// anchors carry.
    pub fn stream_incarnation(&self, stream_id: &BucketStreamId) -> Option<u64> {
        self.stream_metadata(stream_id)
            .map(|metadata| metadata.created_at_ms)
    }

    /// Plans a record read (shared by both engines). Records at or above
    /// the dense part resolve exactly, as before F1. A sealed first record
    /// gets a bracketed window: it starts at that record's mark (or at a
    /// validated continuation anchor), ends where the last requested record
    /// or the byte budget ends at the latest, and carries a [`RecordTrim`]
    /// that materialization applies by counting LFs.
    pub fn record_read_plan(
        &self,
        request: &RecordReadRequest<'_>,
    ) -> Result<StreamReadPlan, RecordPlanError> {
        let stream_id = request.stream_id;
        let record = request.record;
        let retained_record_range = self
            .record_range(stream_id)
            .map_err(|err| index_error("record range", err))?
            .ok_or_else(|| {
                StreamResponse::error(
                    StreamErrorCode::InvalidRecordBoundaries,
                    "record coordinates are inactive for this stream",
                )
            })?;
        if record < retained_record_range.first_record {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!(
                    "record {record} is older than first retained record {}",
                    retained_record_range.first_record
                ),
            )
            .into());
        }
        if record > retained_record_range.next_record {
            return Err(StreamResponse::error(
                StreamErrorCode::InvalidRecordBoundaries,
                format!(
                    "record {record} is beyond record tail {}",
                    retained_record_range.next_record
                ),
            )
            .into());
        }
        let window_end = request
            .max_records
            .map(|limit| record.saturating_add(limit))
            .unwrap_or(retained_record_range.next_record)
            .min(retained_record_range.next_record);
        let dense_first_record = self
            .dense_first_record(stream_id)
            .unwrap_or(retained_record_range.first_record);
        if record < dense_first_record {
            return self.bracketed_record_plan(request, window_end, retained_record_range);
        }
        let offset = self.exact_record_offset(stream_id, record)?;
        // P7 (extensions.md §6.6): `max_bytes` cuts the planned window at the
        // last record boundary within it, keeping at least one record.
        let (next_record, next_offset) =
            self.record_byte_cut(stream_id, record, offset, window_end, request.max_bytes)?;
        let max_len = usize::try_from(next_offset.saturating_sub(offset))
            .map_err(|_| RecordPlanError::Index("record read window exceeds usize".to_owned()))?;
        let mut plan = self.read_plan_at(stream_id, offset, max_len, request.now_ms)?;
        plan.retained_record_range = Some(retained_record_range);
        plan.record_range = Some(StreamRecordRange {
            first_record: record,
            next_record,
        });
        Ok(plan)
    }

    fn locate_record_or_index_error(
        &self,
        stream_id: &BucketStreamId,
        record: u64,
    ) -> Result<RecordOffset, RecordPlanError> {
        self.locate_record(stream_id, record)
            .map_err(|err| index_error("record offset", err))?
            .ok_or_else(|| RecordPlanError::Index("record stream disappeared".to_owned()))
    }

    fn exact_record_offset(
        &self,
        stream_id: &BucketStreamId,
        record: u64,
    ) -> Result<u64, RecordPlanError> {
        self.locate_record_or_index_error(stream_id, record)?
            .exact()
            .ok_or_else(|| index_error("record offset", RecordIndexError::RecordSealed))
    }

    /// Returns the end `(record, offset)` of the longest run of complete
    /// dense records in `[record, window_end)` whose stored bytes fit in
    /// `max_bytes`, and never fewer than one record when the window is not
    /// empty. Record boundaries are monotonic, so this binary-searches them.
    fn record_byte_cut(
        &self,
        stream_id: &BucketStreamId,
        record: u64,
        offset: u64,
        window_end: u64,
        max_bytes: usize,
    ) -> Result<(u64, u64), RecordPlanError> {
        let window_end_offset = self.exact_record_offset(stream_id, window_end)?;
        let budget = u64::try_from(max_bytes).unwrap_or(u64::MAX);
        if window_end <= record || window_end_offset.saturating_sub(offset) <= budget {
            return Ok((window_end, window_end_offset));
        }
        let limit = offset.saturating_add(budget);
        // Invariant: `low` fits (or is the mandatory first record); every
        // record end above `high` does not fit.
        let mut low = record.saturating_add(1);
        let mut high = window_end.saturating_sub(1);
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            if self.exact_record_offset(stream_id, mid)? <= limit {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        Ok((low, self.exact_record_offset(stream_id, low)?))
    }

    fn bracketed_record_plan(
        &self,
        request: &RecordReadRequest<'_>,
        window_end: u64,
        retained_record_range: StreamRecordRange,
    ) -> Result<StreamReadPlan, RecordPlanError> {
        let stream_id = request.stream_id;
        let record = request.record;
        let start = self.locate_record_or_index_error(stream_id, record)?;
        let (from_record, from_offset) = match start {
            RecordOffset::Exact(offset) => (record, offset),
            RecordOffset::Bracket(bracket) => match self.validated_anchor(request, bracket) {
                Some(offset) => (record, offset),
                None => (bracket.from_record, bracket.from_offset),
            },
        };
        let skip = record.saturating_sub(from_record);
        let start_upper = if skip == 0 {
            from_offset
        } else {
            start.upper_bound()
        };
        // Upper bounds of the first record's end and of the window's end.
        let next_upper = self
            .locate_record_or_index_error(stream_id, record.saturating_add(1).min(window_end))?
            .upper_bound();
        let end_upper = self
            .locate_record_or_index_error(stream_id, window_end)?
            .upper_bound();
        let budget = u64::try_from(request.max_bytes).unwrap_or(u64::MAX);
        let window_end_offset = end_upper.min(next_upper.max(start_upper.saturating_add(budget)));
        let retained_offset = self.earliest_retained_offset(stream_id);
        // A window that starts above the retained offset also reads the byte
        // before its anchor, which must be LF (F19 step 3).
        let leading_lf = from_offset > retained_offset;
        let window_start = if leading_lf {
            from_offset - 1
        } else {
            from_offset
        };
        let window_len = usize::try_from(window_end_offset.saturating_sub(window_start))
            .map_err(|_| RecordPlanError::Index("record read window exceeds usize".to_owned()))?;
        let mut plan = self.read_plan_at(stream_id, window_start, window_len, request.now_ms)?;
        let tail_offset = self
            .stream_metadata(stream_id)
            .map_or(window_end_offset, |metadata| metadata.tail_offset);
        plan.up_to_date = false;
        plan.retained_record_range = Some(retained_record_range);
        plan.record_range = Some(StreamRecordRange {
            first_record: record,
            next_record: window_end,
        });
        plan.record_trim = Some(Box::new(RecordTrim {
            window_record: from_record,
            leading_lf,
            skip,
            take: window_end.saturating_sub(record),
            max_bytes: budget,
            anchors: self.record_anchors_between(stream_id, from_offset, window_end_offset),
            tail_offset,
            claim_up_to_date: true,
        }));
        Ok(plan)
    }

    /// The continuation anchor's offset when it names the requested record
    /// of this incarnation and lies strictly inside the record's bracket
    /// (RC-20). The read still checks the LF before it.
    fn validated_anchor(
        &self,
        request: &RecordReadRequest<'_>,
        bracket: crate::RecordBracket,
    ) -> Option<u64> {
        let anchor = request.anchor?;
        let incarnation = self.stream_incarnation(request.stream_id)?;
        (anchor.incarnation == incarnation
            && anchor.record == request.record
            && anchor.offset > bracket.from_offset
            && anchor.offset < bracket.limit)
            .then_some(anchor.offset)
    }
}
