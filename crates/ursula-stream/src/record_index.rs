//! Exact retained record-ordinal to canonical-offset boundaries for JSON streams.

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRecordIndex {
    first_record: u64,
    record_offsets: Vec<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRecordRange {
    pub first_record: u64,
    pub next_record: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordIndexError {
    InvalidBoundaries,
    ArithmeticOverflow,
    RecordGone { first_record: u64, next_record: u64 },
    RecordBeyondTail { next_record: u64 },
    OffsetNotRecordBoundary,
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
    payload
        .iter()
        .enumerate()
        .filter_map(|(index, byte)| (*byte == b'\n').then_some(index + 1))
        .map(|end| u64::try_from(end).map_err(|_| RecordIndexError::ArithmeticOverflow))
        .collect()
}

impl StreamRecordIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn restore(
        first_record: u64,
        record_offsets: Vec<u64>,
        retained_offset: u64,
        tail_offset: u64,
    ) -> Result<Self, RecordIndexError> {
        let index = Self {
            first_record,
            record_offsets,
        };
        index.validate(retained_offset, tail_offset)?;
        Ok(index)
    }

    pub fn range(&self) -> Result<StreamRecordRange, RecordIndexError> {
        let retained = u64::try_from(self.record_offsets.len())
            .map_err(|_| RecordIndexError::ArithmeticOverflow)?;
        let next_record = self
            .first_record
            .checked_add(retained)
            .ok_or(RecordIndexError::ArithmeticOverflow)?;
        Ok(StreamRecordRange {
            first_record: self.first_record,
            next_record,
        })
    }

    pub fn record_offsets(&self) -> &[u64] {
        &self.record_offsets
    }

    pub fn append_relative_ends(
        &mut self,
        base_offset: u64,
        payload_len: u64,
        relative_ends: &[u64],
    ) -> Result<StreamRecordRange, RecordIndexError> {
        validate_relative_ends(payload_len, relative_ends)?;
        let record_start = self.range()?.next_record;
        let appended =
            u64::try_from(relative_ends.len()).map_err(|_| RecordIndexError::ArithmeticOverflow)?;
        let record_next = record_start
            .checked_add(appended)
            .ok_or(RecordIndexError::ArithmeticOverflow)?;
        if self
            .record_offsets
            .last()
            .is_some_and(|last| *last >= base_offset)
        {
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
        self.record_offsets.extend(starts);
        Ok(StreamRecordRange {
            first_record: record_start,
            next_record: record_next,
        })
    }

    pub fn offset_for(&self, record: u64, tail_offset: u64) -> Result<u64, RecordIndexError> {
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
            return Ok(tail_offset);
        }
        let relative = record
            .checked_sub(range.first_record)
            .ok_or(RecordIndexError::ArithmeticOverflow)?;
        let index = usize::try_from(relative).map_err(|_| RecordIndexError::ArithmeticOverflow)?;
        self.record_offsets
            .get(index)
            .copied()
            .ok_or(RecordIndexError::InvalidBoundaries)
    }

    pub fn record_for_offset(
        &self,
        offset: u64,
        tail_offset: u64,
    ) -> Result<u64, RecordIndexError> {
        let range = self.range()?;
        if offset == tail_offset {
            return Ok(range.next_record);
        }
        let relative = self
            .record_offsets
            .binary_search(&offset)
            .map_err(|_| RecordIndexError::OffsetNotRecordBoundary)?;
        range
            .first_record
            .checked_add(u64::try_from(relative).map_err(|_| RecordIndexError::ArithmeticOverflow)?)
            .ok_or(RecordIndexError::ArithmeticOverflow)
    }

    pub fn retain_from_offset(
        &mut self,
        retained_offset: u64,
        tail_offset: u64,
    ) -> Result<u64, RecordIndexError> {
        let removed = if retained_offset == tail_offset {
            self.record_offsets.len()
        } else {
            self.record_offsets
                .binary_search(&retained_offset)
                .map_err(|_| RecordIndexError::OffsetNotRecordBoundary)?
        };
        let removed_u64 =
            u64::try_from(removed).map_err(|_| RecordIndexError::ArithmeticOverflow)?;
        self.first_record = self
            .first_record
            .checked_add(removed_u64)
            .ok_or(RecordIndexError::ArithmeticOverflow)?;
        self.record_offsets.drain(..removed);
        Ok(self.first_record)
    }

    pub fn validate(&self, retained_offset: u64, tail_offset: u64) -> Result<(), RecordIndexError> {
        let _ = self.range()?;
        if retained_offset > tail_offset {
            return Err(RecordIndexError::InvalidBoundaries);
        }
        if self.record_offsets.is_empty() {
            return (retained_offset == tail_offset)
                .then_some(())
                .ok_or(RecordIndexError::InvalidBoundaries);
        }
        if self.record_offsets.first().copied() != Some(retained_offset)
            || self
                .record_offsets
                .iter()
                .any(|offset| *offset >= tail_offset)
            || self.record_offsets.windows(2).any(|pair| {
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

#[cfg(test)]
mod tests {
    use super::RecordIndexError;
    use super::StreamRecordIndex;
    use super::StreamRecordRange;
    use super::canonical_json_record_ends;

    #[test]
    fn append_maps_contiguous_ordinals_to_exact_offsets() {
        let mut index = StreamRecordIndex::new();
        assert_eq!(
            index.append_relative_ends(0, 18, &[9, 18]),
            Ok(StreamRecordRange {
                first_record: 0,
                next_record: 2,
            })
        );
        assert_eq!(index.record_offsets(), &[0, 9]);
        assert_eq!(index.offset_for(0, 18), Ok(0));
        assert_eq!(index.offset_for(1, 18), Ok(9));
        assert_eq!(index.offset_for(2, 18), Ok(18));

        assert_eq!(
            index.append_relative_ends(18, 9, &[9]),
            Ok(StreamRecordRange {
                first_record: 2,
                next_record: 3,
            })
        );
        assert_eq!(index.record_offsets(), &[0, 9, 18]);
        assert_eq!(index.offset_for(3, 27), Ok(27));
    }

    #[test]
    fn retention_drops_offsets_without_renumbering() {
        let mut index = StreamRecordIndex::new();
        index
            .append_relative_ends(0, 27, &[9, 18, 27])
            .expect("append boundaries");
        assert_eq!(index.retain_from_offset(18, 27), Ok(2));
        assert_eq!(index.record_offsets(), &[18]);
        assert_eq!(
            index.offset_for(1, 27),
            Err(RecordIndexError::RecordGone {
                first_record: 2,
                next_record: 3,
            })
        );
        assert_eq!(index.offset_for(2, 27), Ok(18));
    }

    #[test]
    fn restore_rejects_misaligned_or_non_monotonic_offsets() {
        assert_eq!(
            StreamRecordIndex::restore(0, vec![1, 9], 0, 18),
            Err(RecordIndexError::InvalidBoundaries)
        );
        assert_eq!(
            StreamRecordIndex::restore(0, vec![0, 0], 0, 18),
            Err(RecordIndexError::InvalidBoundaries)
        );
        assert!(StreamRecordIndex::restore(2, vec![18], 18, 27).is_ok());
    }

    #[test]
    fn relative_ends_must_cover_the_payload_exactly() {
        let mut index = StreamRecordIndex::new();
        assert_eq!(
            index.append_relative_ends(0, 18, &[9]),
            Err(RecordIndexError::InvalidBoundaries)
        );
        assert_eq!(
            index.append_relative_ends(0, 18, &[9, 9, 18]),
            Err(RecordIndexError::InvalidBoundaries)
        );
        assert_eq!(
            index.append_relative_ends(0, 0, &[0]),
            Err(RecordIndexError::InvalidBoundaries)
        );
    }

    #[test]
    fn canonical_json_payload_exposes_each_ndjson_boundary() {
        assert_eq!(
            canonical_json_record_ends(
                "application/json; charset=utf-8",
                b"{\"a\":1}\n{\"b\":2}\n"
            ),
            Ok(vec![8, 16])
        );
        assert_eq!(
            canonical_json_record_ends("application/octet-stream", b"x"),
            Ok(vec![])
        );
        assert_eq!(
            canonical_json_record_ends("application/json", b"{}"),
            Err(RecordIndexError::InvalidBoundaries)
        );
    }
}
