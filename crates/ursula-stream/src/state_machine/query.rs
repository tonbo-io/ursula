//! Read and query paths: heads, hot/cold accessors, read plans, snapshots, bootstrap.
#![expect(
    clippy::arithmetic_side_effects,
    reason = "pre-existing arithmetic debt; see Known debt in AGENTS.md"
)]

use super::BOOTSTRAP_MAX_UPDATE_BYTES;
use super::BucketStreamId;
use super::COLD_INDEX_PAGE_SPAN_BYTES;
use super::ColdChunkRef;
use super::HotPayloadSegment;
use super::ObjectPayloadRef;
use super::StreamBootstrapPlan;
use super::StreamErrorCode;
use super::StreamMessageRecord;
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
use super::hot_buffer::len_u64;
use super::hot_buffer::offset_delta;
use super::stream_is_expired;
use super::stream_ttl_renewal_due;
use crate::json_records::is_json_record_content_type;

impl StreamStateMachine {
    pub fn head(&self, stream_id: &BucketStreamId) -> Option<&StreamMetadata> {
        self.stream_metadata(stream_id)
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
        Ok(renew_ttl && stream_ttl_renewal_due(stream, now_ms))
    }

    /// Up to `max` stream ids strictly after `after`, in (bucket, stream)
    /// order. Leader-side cursors walk a group's streams with it.
    pub fn stream_ids_after(
        &self,
        after: Option<&BucketStreamId>,
        max: usize,
    ) -> Vec<BucketStreamId> {
        let mut ids = self
            .registry
            .slots()
            .map(|slot| &slot.metadata.stream_id)
            .filter(|id| after.is_none_or(|after| super::compare_stream_ids(id, after).is_gt()))
            .collect::<Vec<_>>();
        ids.sort_unstable_by(|left, right| super::compare_stream_ids(left, right));
        ids.truncate(max);
        ids.into_iter().cloned().collect()
    }

    pub fn hot_start_offset(&self, stream_id: &BucketStreamId) -> u64 {
        let Some(slot) = self.stream_slot(stream_id) else {
            return 0;
        };
        slot.hot_buffer
            .first_start_offset()
            .unwrap_or(slot.metadata.tail_offset)
    }

    pub fn retained_offset(&self, stream_id: &BucketStreamId) -> u64 {
        self.earliest_retained_offset(stream_id)
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
        Ok(len_u64(slot.hot_buffer.len()))
    }

    /// Whether the stream exists and has not expired at `now_ms`.
    pub fn stream_is_live(&self, stream_id: &BucketStreamId, now_ms: u64) -> bool {
        self.stream_metadata(stream_id)
            .is_some_and(|metadata| !super::stream_is_expired(metadata, now_ms))
    }

    /// Runtime append count of the stream's current incarnation (0 when the
    /// stream does not exist).
    pub fn stream_append_count(&self, stream_id: &BucketStreamId) -> u64 {
        self.stream_slot(stream_id)
            .map_or(0, |slot| slot.append_count)
    }

    /// Adds `appends` to the stream's runtime append count and returns the new
    /// value, or `None` when the stream does not exist.
    pub fn add_stream_append_count(
        &mut self,
        stream_id: &BucketStreamId,
        appends: u64,
    ) -> Option<u64> {
        let slot = self.stream_slot_mut(stream_id)?;
        slot.append_count = slot.append_count.saturating_add(appends);
        Some(slot.append_count)
    }

    /// Overwrites the stream's runtime append count (snapshot install and
    /// engine-level rollback). Returns `false` when the stream does not exist.
    pub fn set_stream_append_count(&mut self, stream_id: &BucketStreamId, count: u64) -> bool {
        let Some(slot) = self.stream_slot_mut(stream_id) else {
            return false;
        };
        slot.append_count = count;
        true
    }

    /// Non-zero runtime append counts of live streams, in arbitrary order.
    pub fn stream_append_counts(&self) -> impl Iterator<Item = (&BucketStreamId, u64)> {
        self.registry
            .slots()
            .filter(|slot| slot.append_count > 0)
            .map(|slot| (&slot.metadata.stream_id, slot.append_count))
    }

    /// Hot payload bytes across the group: what admission and the flush
    /// planner count (F6c).
    pub fn total_hot_payload_bytes(&self) -> u64 {
        self.hot_payload_bytes
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
        // F18 step 1: coverage is the complement of the hot buffer. Every
        // byte of `[retained, tail)` that no hot segment holds is cold, served
        // by state refs where they exist and by cold-index pages otherwise.
        // The replicated scalar frontier can lag below an external append
        // (bounded-state D1), so it no longer bounds cold-index lookups.
        let cold_index_end = next_offset;
        let mut direct_cold_ranges = self
            .cold_chunks(stream_id)
            .iter()
            .map(|chunk| (chunk.start_offset, chunk.end_offset))
            .chain(
                self.external_segments(stream_id)
                    .iter()
                    .map(|object| (object.start_offset, object.end_offset)),
            )
            .collect::<Vec<_>>();
        direct_cold_ranges.sort_unstable();
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
            push_cold_index_segments_excluding(
                &mut segments,
                stream_id,
                slot.cold.cold_generation(),
                cursor,
                gap_end,
                &direct_cold_ranges,
            );
            cursor = cursor.max(hot_end);
        }
        push_cold_index_segments_excluding(
            &mut segments,
            stream_id,
            slot.cold.cold_generation(),
            cursor,
            cold_index_end,
            &direct_cold_ranges,
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
                        len: offset_delta(start, end),
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
                        len: offset_delta(start, end),
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
            incarnation: stream.created_at_ms,
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
        self.bootstrap_plan_with_cap(stream_id, BOOTSTRAP_MAX_UPDATE_BYTES)
    }

    /// Plans `/bootstrap` with at most `max_update_bytes` of updates
    /// (bounded-stream-state F11).
    ///
    /// The updates answer the hot range `[S, tail)` from the snapshot
    /// offset `S`. They need `S` at or above the seal point and every byte of
    /// the range hot; otherwise the plan is snapshot-only (an honest
    /// partial).
    ///
    /// A JSON stream answers one part per message, split on LF. The updates
    /// stop at the last LF within the cap; a single message larger than the
    /// cap is returned whole. A capped plan is an honest partial:
    /// `next_offset` is the end of the last returned message and
    /// `up_to_date` is false. Any other stream answers the range as one
    /// part, or snapshot-only when it exceeds the cap.
    pub fn bootstrap_plan_with_cap(
        &self,
        stream_id: &BucketStreamId,
        max_update_bytes: u64,
    ) -> Result<StreamBootstrapPlan, StreamResponse> {
        let Some(slot) = self.stream_slot(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        let stream = &slot.metadata;
        let snapshot = slot.visible_snapshot.clone();
        let snapshot_offset = snapshot
            .as_ref()
            .map_or(slot.retained_offset, |snapshot| snapshot.offset);
        let exact_frontier = slot.seal_point().max(slot.retained_offset);
        let closed = stream.status == StreamStatus::Closed;
        // Honest partial: the bytes right after the snapshot are cold (or,
        // for a non-JSON stream, more than the cap), so bootstrap answers
        // the snapshot alone. The client continues with ordinary reads from
        // the snapshot, which also report closure once they reach the tail.
        // An external append above hot bytes leaves a cold gap that
        // bootstrap does not read.
        let honest_partial = |snapshot: Option<StreamVisibleSnapshot>| StreamBootstrapPlan {
            snapshot,
            updates: Vec::new(),
            next_offset: snapshot_offset,
            content_type: stream.content_type.clone(),
            up_to_date: false,
            closed: false,
        };
        if snapshot_offset < exact_frontier
            || !slot.hot_buffer.covers(snapshot_offset, stream.tail_offset)
        {
            return Ok(honest_partial(snapshot));
        }
        if !is_json_record_content_type(&stream.content_type) {
            let len = stream.tail_offset.saturating_sub(snapshot_offset);
            if len > max_update_bytes {
                return Ok(honest_partial(snapshot));
            }
            let updates = (len > 0)
                .then_some(StreamMessageRecord {
                    start_offset: snapshot_offset,
                    end_offset: stream.tail_offset,
                })
                .into_iter()
                .collect();
            return Ok(StreamBootstrapPlan {
                snapshot,
                updates,
                next_offset: stream.tail_offset,
                content_type: stream.content_type.clone(),
                up_to_date: true,
                closed,
            });
        }
        let ends = slot
            .hot_buffer
            .lf_ends(snapshot_offset, stream.tail_offset, max_update_bytes);
        let mut start_offset = snapshot_offset;
        let updates = ends
            .into_iter()
            .map(|end_offset| {
                let record = StreamMessageRecord {
                    start_offset,
                    end_offset,
                };
                start_offset = end_offset;
                record
            })
            .collect::<Vec<_>>();
        let next_offset = start_offset;
        let (up_to_date, closed) = if next_offset == stream.tail_offset {
            (true, closed)
        } else {
            (false, false)
        };
        Ok(StreamBootstrapPlan {
            snapshot,
            updates,
            next_offset,
            content_type: stream.content_type.clone(),
            up_to_date,
            closed,
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

fn push_cold_index_segments_excluding(
    segments: &mut Vec<(u64, StreamReadSegment)>,
    stream_id: &BucketStreamId,
    generation: u64,
    start_offset: u64,
    end_offset: u64,
    exclusions: &[(u64, u64)],
) {
    let mut cursor = start_offset;
    for (excluded_start, excluded_end) in exclusions {
        if *excluded_end <= cursor || *excluded_start >= end_offset {
            continue;
        }
        let gap_end = (*excluded_start).min(end_offset);
        push_cold_index_segments(segments, stream_id, generation, cursor, gap_end);
        cursor = cursor.max(*excluded_end);
        if cursor >= end_offset {
            return;
        }
    }
    push_cold_index_segments(segments, stream_id, generation, cursor, end_offset);
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
