//! Cold-tier flush planning, GC queue, retention compaction, and snapshot publishing.

use super::BucketStreamId;
use super::ColdChunkRef;
use super::ColdFlushCandidate;
use super::ColdGcEntry;
use super::ColdGcTarget;
use super::HashMap;
use super::StreamErrorCode;
use super::StreamErrorContext;
use super::StreamMessageRecord;
use super::StreamResponse;
use super::StreamStateMachine;
use super::StreamVisibleSnapshot;
use super::compare_stream_ids;
use super::stream_is_expired;

impl StreamStateMachine {
    pub fn plan_cold_flush(
        &self,
        stream_id: &BucketStreamId,
        min_hot_bytes: usize,
        max_flush_bytes: usize,
    ) -> Result<Option<ColdFlushCandidate>, StreamResponse> {
        let start_offset = self.hot_start_offset(stream_id);
        self.plan_cold_flush_with_start(stream_id, start_offset, min_hot_bytes, max_flush_bytes)
    }

    pub(super) fn plan_cold_flush_with_start(
        &self,
        stream_id: &BucketStreamId,
        start_offset: u64,
        min_hot_bytes: usize,
        max_flush_bytes: usize,
    ) -> Result<Option<ColdFlushCandidate>, StreamResponse> {
        if max_flush_bytes == 0 {
            return Ok(None);
        }
        let Some(slot) = self.stream_slot(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        let Some((start_offset, end_offset, payload)) =
            slot.hot_buffer
                .plan_cold_flush_from(start_offset, min_hot_bytes, max_flush_bytes)
        else {
            return Ok(None);
        };
        Ok(Some(ColdFlushCandidate {
            stream_id: stream_id.clone(),
            start_offset,
            end_offset,
            payload,
        }))
    }

    pub(super) fn plan_next_cold_flush_from_start(
        &self,
        mut start_fn: impl FnMut(&BucketStreamId) -> u64,
        min_hot_bytes: usize,
        max_flush_bytes: usize,
        group_hot_bytes: u64,
    ) -> Result<Option<ColdFlushCandidate>, StreamResponse> {
        if max_flush_bytes == 0 {
            return Ok(None);
        }
        let mut stream_ids = self.registry.stream_ids().cloned().collect::<Vec<_>>();
        stream_ids.sort_by(compare_stream_ids);
        for stream_id in &stream_ids {
            let start = start_fn(stream_id);
            match self.plan_cold_flush_with_start(stream_id, start, min_hot_bytes, max_flush_bytes)
            {
                Ok(Some(candidate)) => return Ok(Some(candidate)),
                Ok(None) => {}
                Err(StreamResponse::Error {
                    code: StreamErrorCode::StreamGone | StreamErrorCode::StreamNotFound,
                    ..
                }) => {}
                Err(err) => return Err(err),
            }
        }
        let group_min_hot_bytes = u64::try_from(min_hot_bytes).unwrap_or(u64::MAX);
        if group_hot_bytes < group_min_hot_bytes {
            return Ok(None);
        }
        for stream_id in stream_ids {
            let start = start_fn(&stream_id);
            match self.plan_cold_flush_with_start(&stream_id, start, 1, max_flush_bytes) {
                Ok(Some(candidate)) => return Ok(Some(candidate)),
                Ok(None) => {}
                Err(StreamResponse::Error {
                    code: StreamErrorCode::StreamGone | StreamErrorCode::StreamNotFound,
                    ..
                }) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(None)
    }

    pub fn plan_next_cold_flush_batch(
        &self,
        min_hot_bytes: usize,
        max_flush_bytes: usize,
        max_candidates: usize,
    ) -> Result<Vec<ColdFlushCandidate>, StreamResponse> {
        if max_candidates == 0 || max_flush_bytes == 0 {
            return Ok(Vec::new());
        }
        let mut planned_flush_offsets: HashMap<BucketStreamId, u64> = HashMap::new();
        let mut candidates = Vec::with_capacity(max_candidates);
        while candidates.len() < max_candidates {
            let start_for = |stream_id: &BucketStreamId| -> u64 {
                planned_flush_offsets
                    .get(stream_id)
                    .copied()
                    .unwrap_or_else(|| self.hot_start_offset(stream_id))
            };
            let group_hot_bytes: u64 = self
                .registry
                .stream_ids()
                .map(|stream_id| {
                    let start = start_for(stream_id);
                    self.stream_slot(stream_id)
                        .map(|slot| {
                            u64::try_from(slot.hot_buffer.remaining_len_from(start))
                                .expect("len fits u64")
                        })
                        .unwrap_or(0)
                })
                .sum();
            let candidate = self.plan_next_cold_flush_from_start(
                start_for,
                min_hot_bytes,
                max_flush_bytes,
                group_hot_bytes,
            )?;
            let Some(candidate) = candidate else {
                break;
            };
            planned_flush_offsets.insert(candidate.stream_id.clone(), candidate.end_offset);
            candidates.push(candidate);
        }
        Ok(candidates)
    }

    pub(super) fn publish_snapshot(
        &mut self,
        stream_id: BucketStreamId,
        snapshot_offset: u64,
        content_type: String,
        payload: Vec<u8>,
        now_ms: u64,
    ) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(&stream_id) {
            return response;
        }
        if content_type.trim().is_empty() {
            return StreamResponse::error(
                StreamErrorCode::InvalidSnapshot,
                "snapshot content type must not be empty",
            );
        }
        let Some(stream) = self.stream_metadata(&stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        if stream_is_expired(stream, now_ms) {
            self.remove_stream_state(&stream_id);
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        }
        let tail_offset = stream.tail_offset;
        let retained_offset = self.earliest_retained_offset(&stream_id);
        if snapshot_offset < retained_offset {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::StreamGone,
                format!(
                    "snapshot offset {snapshot_offset} is older than stream '{}' retained offset {retained_offset}",
                    stream_id
                ),
                retained_offset,
            );
        }
        if snapshot_offset > tail_offset {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::SnapshotConflict,
                format!(
                    "snapshot offset {snapshot_offset} is beyond stream '{}' tail {tail_offset}",
                    stream_id
                ),
                tail_offset,
            );
        }
        if !self.snapshot_offset_aligned(&stream_id, snapshot_offset, retained_offset) {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidSnapshot,
                format!(
                    "snapshot offset {snapshot_offset} is not aligned to a committed message boundary for stream '{stream_id}'"
                ),
                tail_offset,
            );
        }

        let mut retained_record_index = self
            .stream_slot(&stream_id)
            .expect("stream existence checked before snapshot publish")
            .record_index
            .clone();
        let record_range = if let Some(record_index) = retained_record_index.as_mut() {
            if record_index
                .retain_from_offset(snapshot_offset, tail_offset)
                .is_err()
            {
                return StreamResponse::error_with_next_offset(
                    StreamErrorCode::InvalidRecordBoundaries,
                    format!(
                        "snapshot offset {snapshot_offset} is not a retained record boundary for stream '{stream_id}'"
                    ),
                    tail_offset,
                );
            }
            match record_index.range() {
                Ok(range) => Some(range),
                Err(_) => {
                    return StreamResponse::error_with_next_offset(
                        StreamErrorCode::InvalidRecordBoundaries,
                        format!("stream '{stream_id}' has an invalid retained record index"),
                        tail_offset,
                    );
                }
            }
        } else {
            None
        };

        self.stream_slot_mut(&stream_id)
            .expect("stream existence checked before snapshot publish")
            .visible_snapshot = Some(StreamVisibleSnapshot {
            offset: snapshot_offset,
            content_type,
            payload,
        });
        self.compact_retained_prefix(&stream_id, snapshot_offset, retained_record_index);
        StreamResponse::SnapshotPublished {
            snapshot_offset,
            record_range,
        }
    }

    pub(super) fn flush_cold(
        &mut self,
        stream_id: BucketStreamId,
        chunk: ColdChunkRef,
    ) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(&stream_id) {
            return response;
        }
        if chunk.s3_path.trim().is_empty() {
            return StreamResponse::error(
                StreamErrorCode::InvalidColdFlush,
                "cold chunk S3 path must not be empty",
            );
        }
        if chunk.object_size == 0 {
            return StreamResponse::error(
                StreamErrorCode::InvalidColdFlush,
                "cold chunk object size must be greater than zero",
            );
        }
        let Some(slot) = self.stream_slot(&stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        let stream = &slot.metadata;
        if chunk.end_offset <= chunk.start_offset {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                "cold chunk must cover at least one byte",
                stream.tail_offset,
            );
        }
        if chunk.end_offset > stream.tail_offset {
            return StreamResponse::error_with_next_offset_and_context(
                StreamErrorCode::InvalidColdFlush,
                format!(
                    "cold chunk end {} is beyond stream '{}' tail {}",
                    chunk.end_offset, stream_id, stream.tail_offset
                ),
                stream.tail_offset,
                vec![StreamErrorContext::StaleColdFlushCandidate],
            );
        }
        let hot_buffer = &slot.hot_buffer;
        if hot_buffer.hot_start_offset() != chunk.start_offset {
            return StreamResponse::error_with_next_offset_and_context(
                StreamErrorCode::InvalidColdFlush,
                format!("cold chunk for stream '{stream_id}' must start at the hot prefix"),
                stream.tail_offset,
                vec![StreamErrorContext::StaleColdFlushCandidate],
            );
        }
        if !hot_buffer.covers_prefix(chunk.start_offset, chunk.end_offset) {
            return StreamResponse::error_with_next_offset_and_context(
                StreamErrorCode::InvalidColdFlush,
                format!(
                    "cold chunk for stream '{stream_id}' does not cover contiguous hot payload"
                ),
                stream.tail_offset,
                vec![StreamErrorContext::StaleColdFlushCandidate],
            );
        }
        let slot = self
            .stream_slot_mut(&stream_id)
            .expect("stream existence checked before cold flush mutation");
        slot.hot_buffer.flush_prefix(chunk.end_offset);
        slot.cold.push_cold_chunk(chunk.clone());
        self.compact_message_records_before(
            &stream_id,
            self.earliest_retained_offset(&stream_id),
            chunk.end_offset,
        );
        StreamResponse::ColdFlushed {
            hot_start_offset: self.hot_start_offset(&stream_id),
        }
    }

    pub fn delete_snapshot(
        &self,
        stream_id: &BucketStreamId,
        snapshot_offset: u64,
    ) -> StreamResponse {
        match self.latest_snapshot(stream_id) {
            Ok(Some(snapshot)) if snapshot.offset == snapshot_offset => StreamResponse::error(
                StreamErrorCode::SnapshotConflict,
                format!(
                    "snapshot {snapshot_offset} for stream '{stream_id}' is the latest visible snapshot"
                ),
            ),
            Ok(_) => StreamResponse::error(
                StreamErrorCode::SnapshotNotFound,
                format!("snapshot {snapshot_offset} for stream '{stream_id}' does not exist"),
            ),
            Err(err) => err,
        }
    }

    pub(super) fn ack_cold_gc(&mut self, up_to_seq: u64) -> StreamResponse {
        let removed = self.cold_gc.ack(up_to_seq);
        StreamResponse::ColdGcAcked { removed }
    }

    /// A bounded snapshot of the front of the GC queue for the leader's worker
    /// to reclaim. Read-only; draining is confirmed by a replicated `AckColdGc`.
    pub fn pending_cold_gc_batch(&self, max: usize) -> Vec<ColdGcEntry> {
        self.cold_gc.batch(max)
    }

    pub fn pending_cold_gc_len(&self) -> usize {
        self.cold_gc.len()
    }

    pub(super) fn earliest_retained_offset(&self, stream_id: &BucketStreamId) -> u64 {
        self.stream_slot(stream_id)
            .and_then(|slot| slot.visible_snapshot.as_ref())
            .map(|snapshot| snapshot.offset)
            .unwrap_or(0)
    }

    pub(super) fn snapshot_offset_aligned(
        &self,
        stream_id: &BucketStreamId,
        snapshot_offset: u64,
        retained_offset: u64,
    ) -> bool {
        snapshot_offset == retained_offset
            || snapshot_offset <= self.cold_frontier_offset(stream_id, retained_offset)
            || self
                .stream_slot(stream_id)
                .is_some_and(|slot| snapshot_offset <= slot.hot_buffer.hot_start_offset())
            || self.stream_slot(stream_id).is_some_and(|slot| {
                slot.message_records
                    .iter()
                    .any(|record| record.end_offset == snapshot_offset)
            })
    }

    pub(super) fn compact_retained_prefix(
        &mut self,
        stream_id: &BucketStreamId,
        retained_offset: u64,
        retained_record_index: Option<crate::StreamRecordIndex>,
    ) {
        let frontier = self.cold_frontier_offset(stream_id, retained_offset).max(
            self.stream_slot(stream_id)
                .map(|slot| slot.hot_buffer.hot_start_offset())
                .unwrap_or(retained_offset),
        );
        self.compact_message_records_before(stream_id, retained_offset, frontier);
        let slot = self
            .stream_slot_mut(stream_id)
            .expect("stream existence checked before retained-prefix compaction");
        slot.record_index = retained_record_index;
        slot.integrity.evict_before(retained_offset);
        let dropped_cold_paths = slot.cold.compact_before(retained_offset);
        if !dropped_cold_paths.is_empty() {
            self.cold_gc
                .enqueue(ColdGcTarget::Paths(dropped_cold_paths));
        }

        self.stream_slot_mut(stream_id)
            .expect("stream existence checked before hot compact")
            .hot_buffer
            .discard_before(retained_offset);
    }

    pub(super) fn compact_message_records_before(
        &mut self,
        stream_id: &BucketStreamId,
        retained_offset: u64,
        frontier: u64,
    ) {
        let slot = self
            .stream_slot_mut(stream_id)
            .expect("stream existence checked before message-record compaction");
        let records = std::mem::take(&mut slot.message_records);
        let frontier = frontier.max(retained_offset);
        let mut compacted = Vec::with_capacity(records.len());
        if frontier > retained_offset {
            compacted.push(StreamMessageRecord {
                start_offset: retained_offset,
                end_offset: frontier,
            });
        }
        compacted.extend(records.iter().filter_map(|record| {
            if record.end_offset <= frontier {
                return None;
            }
            let start_offset = record.start_offset.max(frontier).max(retained_offset);
            (record.end_offset > start_offset).then_some(StreamMessageRecord {
                start_offset,
                end_offset: record.end_offset,
            })
        }));
        if compacted.is_empty() {
            return;
        }
        self.stream_slot_mut(stream_id)
            .expect("stream existence checked before message record compact")
            .message_records = compacted;
    }

    pub(super) fn cold_frontier_offset(
        &self,
        stream_id: &BucketStreamId,
        retained_offset: u64,
    ) -> u64 {
        self.stream_slot(stream_id)
            .map(|slot| slot.cold.cold_frontier_offset(retained_offset))
            .unwrap_or(retained_offset)
    }
}
