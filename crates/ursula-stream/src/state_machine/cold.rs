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
        let payload_digest = blake3::hash(&payload).to_hex().to_string();
        Ok(Some(ColdFlushCandidate {
            stream_id: stream_id.clone(),
            start_offset,
            end_offset,
            payload,
            payload_digest,
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
        max_batch_bytes: usize,
        max_candidates: usize,
    ) -> Result<Vec<ColdFlushCandidate>, StreamResponse> {
        if max_candidates == 0 || max_flush_bytes == 0 || max_batch_bytes == 0 {
            return Ok(Vec::new());
        }
        let mut planned_flush_offsets: HashMap<BucketStreamId, u64> = HashMap::new();
        let mut candidates = Vec::with_capacity(max_candidates);
        let mut batch_bytes = 0usize;
        let initial_group_hot_bytes = self
            .registry
            .stream_ids()
            .map(|stream_id| {
                u64::try_from(
                    self.stream_slot(stream_id)
                        .map(|slot| {
                            slot.hot_buffer
                                .remaining_len_from(self.hot_start_offset(stream_id))
                        })
                        .unwrap_or(0),
                )
                .expect("len fits u64")
            })
            .sum::<u64>();
        let drain_group =
            initial_group_hot_bytes >= u64::try_from(min_hot_bytes).unwrap_or(u64::MAX);
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
                if drain_group { 1 } else { min_hot_bytes },
                max_flush_bytes,
                group_hot_bytes,
            )?;
            let Some(candidate) = candidate else {
                break;
            };
            let Some(next_batch_bytes) = batch_bytes.checked_add(candidate.payload.len()) else {
                break;
            };
            if !candidates.is_empty() && next_batch_bytes > max_batch_bytes {
                break;
            }
            planned_flush_offsets.insert(candidate.stream_id.clone(), candidate.end_offset);
            batch_bytes = next_batch_bytes;
            candidates.push(candidate);
            if batch_bytes >= max_batch_bytes {
                break;
            }
        }
        Ok(candidates)
    }

    pub(super) fn publish_snapshot(
        &mut self,
        stream_id: BucketStreamId,
        snapshot_offset: u64,
        content_type: String,
        payload: Vec<u8>,
        expected_digest: Option<String>,
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
        let digest = super::snapshot_digest(&content_type, &payload);
        let current_snapshot = self
            .stream_slot(&stream_id)
            .and_then(|slot| slot.visible_snapshot.as_ref());
        if let Some(expected_digest) = expected_digest.as_deref()
            && current_snapshot.map(|snapshot| snapshot.digest.as_str()) != Some(expected_digest)
        {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::SnapshotConflict,
                "current snapshot digest does not match Stream-Snapshot-Match",
                tail_offset,
            );
        }
        if let Some(current) = current_snapshot {
            if snapshot_offset < current.offset {
                return StreamResponse::error_with_next_offset(
                    StreamErrorCode::SnapshotConflict,
                    format!(
                        "snapshot offset {snapshot_offset} is older than latest snapshot offset {}",
                        current.offset
                    ),
                    tail_offset,
                );
            }
            if snapshot_offset == current.offset {
                if current.digest == digest {
                    return StreamResponse::SnapshotPublished {
                        snapshot_offset,
                        snapshot_digest: digest,
                        record_range: self.record_range(&stream_id).ok().flatten(),
                    };
                }
                return StreamResponse::error_with_next_offset(
                    StreamErrorCode::SnapshotConflict,
                    format!(
                        "snapshot offset {snapshot_offset} already has a different payload digest"
                    ),
                    tail_offset,
                );
            }
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

        let record_range = self.record_range(&stream_id).ok().flatten();

        self.stream_slot_mut(&stream_id)
            .expect("stream existence checked before snapshot publish")
            .visible_snapshot = Some(StreamVisibleSnapshot {
            offset: snapshot_offset,
            content_type,
            payload,
            digest: digest.clone(),
        });
        StreamResponse::SnapshotPublished {
            snapshot_offset,
            snapshot_digest: digest,
            record_range,
        }
    }

    pub(super) fn advance_retention(
        &mut self,
        stream_id: BucketStreamId,
        retained_offset: u64,
        now_ms: u64,
    ) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(&stream_id) {
            return response;
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
        let current = self.earliest_retained_offset(&stream_id);
        if retained_offset < current {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::SnapshotConflict,
                format!(
                    "retention offset {retained_offset} is older than current retained offset {current}"
                ),
                stream.tail_offset,
            );
        }
        let Some(snapshot) = self
            .stream_slot(&stream_id)
            .and_then(|slot| slot.visible_snapshot.as_ref())
        else {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::SnapshotConflict,
                "retention requires a published checkpoint",
                stream.tail_offset,
            );
        };
        if retained_offset > snapshot.offset {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::SnapshotConflict,
                format!(
                    "retention offset {retained_offset} is beyond latest checkpoint offset {}",
                    snapshot.offset
                ),
                stream.tail_offset,
            );
        }
        if retained_offset == current {
            return StreamResponse::RetentionAdvanced {
                retained_offset,
                record_range: self.record_range(&stream_id).ok().flatten(),
            };
        }
        if !self.snapshot_offset_aligned(&stream_id, retained_offset, current) {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidSnapshot,
                format!(
                    "retention offset {retained_offset} is not aligned to a committed message boundary for stream '{stream_id}'"
                ),
                stream.tail_offset,
            );
        }
        let mut retained_record_index = self
            .stream_slot(&stream_id)
            .expect("stream existence checked before retention")
            .record_index
            .clone();
        if let Some(record_index) = retained_record_index.as_mut()
            && record_index
                .retain_from_offset(retained_offset, stream.tail_offset)
                .is_err()
        {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidRecordBoundaries,
                format!(
                    "retention offset {retained_offset} is not a retained record boundary for stream '{stream_id}'"
                ),
                stream.tail_offset,
            );
        }
        let slot = self
            .stream_slot_mut(&stream_id)
            .expect("stream existence checked before retention mutation");
        let previous_retained_offset = slot.retained_offset;
        slot.retained_offset = retained_offset;
        self.usage_on_retention(
            &stream_id.bucket_id,
            retained_offset.saturating_sub(previous_retained_offset),
        );
        self.compact_retained_prefix(&stream_id, retained_offset, retained_record_index);
        StreamResponse::RetentionAdvanced {
            retained_offset,
            record_range: self.record_range(&stream_id).ok().flatten(),
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
        let logical_size = chunk.end_offset.saturating_sub(chunk.start_offset);
        if chunk
            .object_offset
            .checked_add(logical_size)
            .is_none_or(|end| end > chunk.object_size)
        {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                "cold chunk slice is outside the physical object",
                stream.tail_offset,
            );
        }
        if !chunk.shared_object && chunk.object_offset != 0 {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                "exclusive cold chunks must start at physical object offset zero",
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
        if !chunk.payload_digest.is_empty()
            && hot_buffer
                .digest_prefix(chunk.start_offset, chunk.end_offset)
                .as_deref()
                != Some(chunk.payload_digest.as_str())
        {
            return StreamResponse::error_with_next_offset_and_context(
                StreamErrorCode::InvalidColdFlush,
                format!("cold chunk payload for stream '{stream_id}' is stale"),
                stream.tail_offset,
                vec![StreamErrorContext::StaleColdFlushCandidate],
            );
        }
        let shared_path = chunk.shared_object.then(|| chunk.s3_path.clone());
        let slot = self
            .stream_slot_mut(&stream_id)
            .expect("stream existence checked before cold flush mutation");
        let hot_bytes_before = u64::try_from(slot.hot_buffer.len()).expect("payload len fits u64");
        slot.hot_buffer.flush_prefix(chunk.end_offset);
        let hot_bytes_after = u64::try_from(slot.hot_buffer.len()).expect("payload len fits u64");
        slot.cold.push_cold_chunk(chunk.clone());
        self.remove_hot_payload_bytes(hot_bytes_before.saturating_sub(hot_bytes_after));
        if let Some(path) = shared_path {
            self.retain_shared_cold_object(&path);
        }
        self.compact_message_records_before(
            &stream_id,
            self.earliest_retained_offset(&stream_id),
            chunk.end_offset,
        );
        StreamResponse::ColdFlushed {
            hot_start_offset: self.hot_start_offset(&stream_id),
        }
    }

    pub(super) fn compact_cold(
        &mut self,
        stream_id: BucketStreamId,
        old_chunks: Vec<ColdChunkRef>,
        replacement: ColdChunkRef,
        gc_not_before_ms: u64,
    ) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(&stream_id) {
            return response;
        }
        if self.stream_slot(&stream_id).is_none() {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        }
        if old_chunks.len() < 2 {
            return StreamResponse::error(
                StreamErrorCode::InvalidColdFlush,
                "cold compaction requires at least two input chunks",
            );
        }
        if replacement.s3_path.trim().is_empty() || replacement.object_size == 0 {
            return StreamResponse::error(
                StreamErrorCode::InvalidColdFlush,
                "cold compaction replacement must name a non-empty object",
            );
        }
        let mut expected_start = old_chunks
            .first()
            .map_or(replacement.start_offset, |chunk| chunk.start_offset);
        let mut compacted_bytes = 0_u64;
        for chunk in &old_chunks {
            if chunk.start_offset != expected_start
                || chunk.end_offset <= chunk.start_offset
                || chunk.object_size != chunk.end_offset.saturating_sub(chunk.start_offset)
            {
                return StreamResponse::error(
                    StreamErrorCode::InvalidColdFlush,
                    "cold compaction inputs must be contiguous raw chunks",
                );
            }
            expected_start = chunk.end_offset;
            compacted_bytes = compacted_bytes.saturating_add(chunk.object_size);
        }
        let first = old_chunks
            .first()
            .expect("cold compaction input count validated");
        if replacement.start_offset != first.start_offset
            || replacement.end_offset != expected_start
            || replacement.object_size != compacted_bytes
        {
            return StreamResponse::error(
                StreamErrorCode::InvalidColdFlush,
                "cold compaction replacement must cover the exact input range",
            );
        }
        let old_paths = old_chunks
            .into_iter()
            .map(|chunk| chunk.s3_path)
            .collect::<Vec<_>>();
        let compacted_chunks = u64::try_from(old_paths.len()).expect("chunk count fits u64");
        self.cold_gc
            .enqueue_after(ColdGcTarget::Paths(old_paths), gc_not_before_ms);
        StreamResponse::ColdCompacted {
            compacted_chunks,
            compacted_bytes,
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
            .map(|slot| slot.retained_offset)
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
        self.release_shared_cold_objects(dropped_cold_paths, 0);

        let slot = self
            .stream_slot_mut(stream_id)
            .expect("stream existence checked before hot compact");
        let hot_bytes_before = u64::try_from(slot.hot_buffer.len()).expect("payload len fits u64");
        slot.hot_buffer.discard_before(retained_offset);
        let hot_bytes_after = u64::try_from(slot.hot_buffer.len()).expect("payload len fits u64");
        self.remove_hot_payload_bytes(hot_bytes_before.saturating_sub(hot_bytes_after));
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
