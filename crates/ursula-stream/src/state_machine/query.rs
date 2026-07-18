//! Read and query paths: heads, attrs, hot/cold accessors, read plans, snapshots, bootstrap.

use super::BucketStreamId;
use super::COLD_INDEX_PAGE_SPAN_BYTES;
use super::ColdChunkRef;
use super::HotPayloadSegment;
use super::ObjectPayloadRef;
use super::StreamAttrs;
use super::StreamBootstrapPlan;
use super::StreamErrorCode;
use super::StreamMetadata;
use super::StreamRead;
use super::StreamReadColdIndexSegment;
use super::StreamReadObjectSegment;
use super::StreamReadPlan;
use super::StreamReadSegment;
use super::StreamResponse;
use super::StreamStateMachine;
use super::StreamStatus;
use super::StreamVisibleSnapshot;
use super::stream_is_expired;
use crate::RecordIndexError;
use crate::StreamRecordIndex;
use crate::StreamRecordRange;

impl StreamStateMachine {
    pub fn head(&self, stream_id: &BucketStreamId) -> Option<&StreamMetadata> {
        self.stream_metadata(stream_id)
    }

    pub fn record_range(
        &self,
        stream_id: &BucketStreamId,
    ) -> Result<Option<StreamRecordRange>, RecordIndexError> {
        self.stream_slot(stream_id)
            .and_then(|slot| slot.record_index.as_ref())
            .map(StreamRecordIndex::range)
            .transpose()
    }

    pub fn offset_for_record(
        &self,
        stream_id: &BucketStreamId,
        record: u64,
    ) -> Result<Option<u64>, RecordIndexError> {
        let Some(slot) = self.stream_slot(stream_id) else {
            return Ok(None);
        };
        slot.record_index
            .as_ref()
            .map(|index| index.offset_for(record, slot.metadata.tail_offset))
            .transpose()
    }

    pub fn stream_attrs(&self, stream_id: &BucketStreamId) -> Option<&StreamAttrs> {
        self.stream_slot(stream_id)
            .and_then(|slot| slot.attrs.as_ref())
    }

    pub fn head_at(&mut self, stream_id: &BucketStreamId, now_ms: u64) -> Option<&StreamMetadata> {
        self.expire_stream_if_due(stream_id, now_ms);
        self.stream_metadata(stream_id)
    }

    pub fn access_requires_write(
        &self,
        stream_id: &BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
    ) -> Result<bool, StreamResponse> {
        self.validate_stream_scope(stream_id)?;
        let Some(stream) = self.stream_metadata(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        if stream_is_expired(stream, now_ms) {
            return Ok(true);
        }
        Ok(renew_ttl
            && stream.stream_ttl_seconds.is_some()
            && stream.last_ttl_touch_at_ms != now_ms)
    }

    pub fn hot_start_offset(&self, stream_id: &BucketStreamId) -> u64 {
        let Some(slot) = self.stream_slot(stream_id) else {
            return 0;
        };
        slot.hot_buffer
            .first_start_offset()
            .unwrap_or(slot.metadata.tail_offset)
    }

    pub fn cold_chunks(&self, stream_id: &BucketStreamId) -> &[ColdChunkRef] {
        self.stream_slot(stream_id)
            .map(|slot| slot.cold.cold_chunks())
            .unwrap_or(&[])
    }

    pub fn external_segments(&self, stream_id: &BucketStreamId) -> &[ObjectPayloadRef] {
        self.stream_slot(stream_id)
            .map(|slot| slot.cold.external_segments())
            .unwrap_or(&[])
    }

    pub fn hot_segments(&self, stream_id: &BucketStreamId) -> Vec<HotPayloadSegment> {
        self.stream_slot(stream_id)
            .map(|slot| slot.hot_buffer.hot_segments())
            .unwrap_or_default()
    }

    pub fn hot_payload_len(&self, stream_id: &BucketStreamId) -> Result<u64, StreamResponse> {
        let Some(slot) = self.stream_slot(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        Ok(u64::try_from(slot.hot_buffer.len()).expect("payload len fits u64"))
    }

    pub fn total_hot_payload_bytes(&self) -> u64 {
        self.registry
            .slots()
            .map(|slot| u64::try_from(slot.hot_buffer.len()).expect("payload len fits u64"))
            .sum()
    }

    pub fn bucket_exists(&self, bucket_id: &str) -> bool {
        self.buckets.contains(bucket_id)
    }

    pub fn read(
        &self,
        stream_id: &BucketStreamId,
        offset: u64,
        max_len: usize,
    ) -> Result<StreamRead, StreamResponse> {
        let plan = self.read_plan(stream_id, offset, max_len)?;
        if plan.segments.iter().any(|segment| {
            matches!(
                segment,
                StreamReadSegment::ColdIndex(_) | StreamReadSegment::Object(_)
            )
        }) {
            return Err(StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                format!("stream '{stream_id}' read requires object payload store"),
                plan.next_offset,
            ));
        }
        let payload = plan
            .segments
            .iter()
            .flat_map(|segment| match segment {
                StreamReadSegment::Hot(payload) => payload.as_slice(),
                StreamReadSegment::ColdIndex(_) | StreamReadSegment::Object(_) => {
                    unreachable!("object segments checked above")
                }
            })
            .copied()
            .collect();
        Ok(StreamRead {
            offset: plan.offset,
            next_offset: plan.next_offset,
            content_type: plan.content_type,
            payload,
            up_to_date: plan.up_to_date,
            closed: plan.closed,
        })
    }

    pub fn read_plan(
        &self,
        stream_id: &BucketStreamId,
        offset: u64,
        max_len: usize,
    ) -> Result<StreamReadPlan, StreamResponse> {
        self.read_plan_at(stream_id, offset, max_len, 0)
    }

    pub fn read_plan_at(
        &self,
        stream_id: &BucketStreamId,
        offset: u64,
        max_len: usize,
        now_ms: u64,
    ) -> Result<StreamReadPlan, StreamResponse> {
        let Some(slot) = self.stream_slot(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        let stream = &slot.metadata;
        if stream_is_expired(stream, now_ms) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        }
        if offset > stream.tail_offset {
            return Err(StreamResponse::error_with_next_offset(
                StreamErrorCode::OffsetOutOfRange,
                format!(
                    "offset {offset} is beyond stream '{}' tail {}",
                    stream_id, stream.tail_offset
                ),
                stream.tail_offset,
            ));
        }
        let retained_offset = self.earliest_retained_offset(stream_id);
        if offset < retained_offset {
            return Err(StreamResponse::error_with_next_offset(
                StreamErrorCode::StreamGone,
                format!(
                    "offset {offset} is older than stream '{}' retained offset {retained_offset}",
                    stream_id
                ),
                retained_offset,
            ));
        }

        let max_len_u64 = u64::try_from(max_len).unwrap_or(u64::MAX);
        let next_offset = stream.tail_offset.min(offset.saturating_add(max_len_u64));
        let mut segments = Vec::<(u64, StreamReadSegment)>::new();
        let hot_segments = slot.hot_buffer.read_segments(offset, next_offset);
        let cold_frontier = self.cold_frontier_offset(stream_id, retained_offset);
        let cold_index_end = next_offset.min(cold_frontier);
        let mut cursor = offset;
        for (hot_start, hot_segment) in &hot_segments {
            if cursor >= cold_index_end {
                break;
            }
            let Some(hot_end) = read_segment_end(*hot_start, hot_segment) else {
                continue;
            };
            if hot_end <= cursor {
                continue;
            }
            let gap_end = (*hot_start).min(cold_index_end);
            push_cold_index_segments(
                &mut segments,
                stream_id,
                slot.cold.cold_generation(),
                cursor,
                gap_end,
            );
            cursor = cursor.max(hot_end);
        }
        push_cold_index_segments(
            &mut segments,
            stream_id,
            slot.cold.cold_generation(),
            cursor,
            cold_index_end,
        );
        for chunk in self.cold_chunks(stream_id) {
            let start = offset.max(chunk.start_offset);
            let end = next_offset.min(chunk.end_offset);
            if start < end {
                segments.push((
                    start,
                    StreamReadSegment::Object(StreamReadObjectSegment {
                        object: ObjectPayloadRef::from(chunk),
                        read_start_offset: start,
                        len: usize::try_from(end - start).expect("object read len fits usize"),
                    }),
                ));
            }
        }
        for object in self.external_segments(stream_id) {
            let start = offset.max(object.start_offset);
            let end = next_offset.min(object.end_offset);
            if start < end {
                segments.push((
                    start,
                    StreamReadSegment::Object(StreamReadObjectSegment {
                        object: object.clone(),
                        read_start_offset: start,
                        len: usize::try_from(end - start).expect("object read len fits usize"),
                    }),
                ));
            }
        }
        segments.extend(hot_segments);
        segments.sort_by_key(|(start, _)| *start);
        if !segments_cover_range(&segments, offset, next_offset) {
            return Err(StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                format!("stream '{stream_id}' has missing payload segment metadata"),
                next_offset,
            ));
        }
        Ok(StreamReadPlan {
            offset,
            next_offset,
            content_type: stream.content_type.clone(),
            segments: segments.into_iter().map(|(_, segment)| segment).collect(),
            up_to_date: next_offset == stream.tail_offset,
            closed: stream.status == StreamStatus::Closed,
        })
    }

    pub fn latest_snapshot(
        &self,
        stream_id: &BucketStreamId,
    ) -> Result<Option<StreamVisibleSnapshot>, StreamResponse> {
        let Some(slot) = self.stream_slot(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        Ok(slot.visible_snapshot.clone())
    }

    pub fn read_snapshot(
        &self,
        stream_id: &BucketStreamId,
        snapshot_offset: u64,
    ) -> Result<StreamVisibleSnapshot, StreamResponse> {
        let snapshot = self.latest_snapshot(stream_id)?;
        match snapshot {
            Some(snapshot) if snapshot.offset == snapshot_offset => Ok(snapshot),
            _ => Err(StreamResponse::error(
                StreamErrorCode::SnapshotNotFound,
                format!("snapshot {snapshot_offset} for stream '{stream_id}' does not exist"),
            )),
        }
    }

    pub fn bootstrap_plan(
        &self,
        stream_id: &BucketStreamId,
    ) -> Result<StreamBootstrapPlan, StreamResponse> {
        let Some(slot) = self.stream_slot(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        let stream = &slot.metadata;
        let snapshot = slot.visible_snapshot.clone();
        let retained_offset = snapshot
            .as_ref()
            .map(|snapshot| snapshot.offset)
            .unwrap_or(0);
        let updates = slot
            .message_records
            .iter()
            .filter(|record| record.start_offset >= retained_offset)
            .cloned()
            .collect::<Vec<_>>();
        Ok(StreamBootstrapPlan {
            snapshot,
            updates,
            next_offset: stream.tail_offset,
            content_type: stream.content_type.clone(),
            up_to_date: true,
            closed: stream.status == StreamStatus::Closed,
        })
    }
}

fn push_cold_index_segments(
    segments: &mut Vec<(u64, StreamReadSegment)>,
    _stream_id: &BucketStreamId,
    generation: u64,
    start_offset: u64,
    end_offset: u64,
) {
    let mut cursor = start_offset;
    while cursor < end_offset {
        let page_id = cursor / COLD_INDEX_PAGE_SPAN_BYTES;
        let page_end = page_id
            .saturating_add(1)
            .saturating_mul(COLD_INDEX_PAGE_SPAN_BYTES);
        let segment_end = end_offset.min(page_end);
        segments.push((
            cursor,
            StreamReadSegment::ColdIndex(StreamReadColdIndexSegment {
                generation,
                page_id,
                read_start_offset: cursor,
                len: usize::try_from(segment_end - cursor).expect("cold index read len fits usize"),
            }),
        ));
        cursor = segment_end;
    }
}

fn segments_cover_range(
    segments: &[(u64, StreamReadSegment)],
    offset: u64,
    next_offset: u64,
) -> bool {
    if next_offset < offset {
        return false;
    }
    let mut expected_start = offset;
    for (segment_start, segment) in segments {
        let Some(segment_end) = read_segment_end(*segment_start, segment) else {
            return false;
        };
        if segment_end <= expected_start {
            continue;
        }
        if *segment_start > expected_start {
            return false;
        }
        expected_start = segment_end;
        if expected_start >= next_offset {
            return true;
        }
    }
    expected_start == next_offset
}

fn read_segment_end(segment_start: u64, segment: &StreamReadSegment) -> Option<u64> {
    match segment {
        StreamReadSegment::Object(object) => {
            if object.len == 0
                || object.read_start_offset != segment_start
                || object.read_start_offset < object.object.start_offset
            {
                return None;
            }
            let len = u64::try_from(object.len).ok()?;
            let segment_end = object.read_start_offset.checked_add(len)?;
            if segment_end > object.object.end_offset {
                return None;
            }
            Some(segment_end)
        }
        StreamReadSegment::ColdIndex(index) => {
            if index.len == 0 || index.read_start_offset != segment_start {
                return None;
            }
            let len = u64::try_from(index.len).ok()?;
            segment_start.checked_add(len)
        }
        StreamReadSegment::Hot(payload) => {
            if payload.is_empty() {
                return None;
            }
            let len = u64::try_from(payload.len()).ok()?;
            segment_start.checked_add(len)
        }
    }
}
