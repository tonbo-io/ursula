//! Cold-tier flush planning, GC queue, retention compaction, and snapshot publishing.

use super::BucketStreamId;
use super::ColdChunkRef;
use super::ColdFlushCandidate;
use super::ColdGcEntry;
use super::ColdGcPlanEntry;
use super::ColdGcTarget;
use super::StreamErrorCode;
use super::StreamErrorContext;
use super::StreamMessageRecord;
use super::StreamResponse;
use super::StreamStateMachine;
use super::StreamVisibleSnapshot;
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
            cold_generation: slot.cold.cold_generation(),
            start_offset,
            end_offset,
            payload,
            payload_digest,
        }))
    }

    /// Plans one batch without moving the leader-local rotation cursor; see
    /// [`StreamStateMachine::plan_cold_flush_pass`] for the leader path.
    pub fn plan_next_cold_flush_batch(
        &self,
        min_hot_bytes: usize,
        max_flush_bytes: usize,
        max_batch_bytes: usize,
        max_candidates: usize,
    ) -> Result<Vec<ColdFlushCandidate>, StreamResponse> {
        let (pass, _) = self.plan_cold_flush_pass_from(
            super::ColdFlushPassRequest {
                min_hot_bytes,
                max_flush_bytes,
                max_batch_bytes,
                max_candidates,
                pressure: None,
            },
            None,
        )?;
        Ok(pass.candidates)
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
        let prepared_record_retain = match self
            .stream_slot(&stream_id)
            .expect("stream existence checked before retention")
            .record_index
            .as_ref()
            .map(|record_index| record_index.prepare_retain(retained_offset, stream.tail_offset))
            .transpose()
        {
            Ok(prepared) => prepared,
            Err(_) => {
                return StreamResponse::error_with_next_offset(
                    StreamErrorCode::InvalidRecordBoundaries,
                    format!(
                        "retention offset {retained_offset} is not a retained record boundary for stream '{stream_id}'"
                    ),
                    stream.tail_offset,
                );
            }
        };
        let slot = self
            .stream_slot_mut(&stream_id)
            .expect("stream existence checked before retention mutation");
        let previous_retained_offset = slot.retained_offset;
        slot.retained_offset = retained_offset;
        self.usage_on_retention(
            &stream_id.bucket_id,
            retained_offset.saturating_sub(previous_retained_offset),
        );
        self.compact_retained_prefix(&stream_id, retained_offset, prepared_record_retain);
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
        if let Err(response) = self.check_cold_flush(&stream_id, &chunk) {
            return response;
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
        self.sync_hot_index(&stream_id);
        if let Some(path) = shared_path {
            self.retain_shared_cold_object(&path, &stream_id.bucket_id);
        }
        self.compact_message_records_before(
            &stream_id,
            self.earliest_retained_offset(&stream_id),
            chunk.end_offset,
        );
        // F4a (level 1): external appends above the flushed hot prefix are
        // cold too; collapse everything below the seal point.
        self.collapse_sealed_message_records(&stream_id);
        StreamResponse::ColdFlushed {
            hot_start_offset: self.hot_start_offset(&stream_id),
        }
    }

    /// Read-only half of `FlushCold` apply: `Ok` exactly when applying
    /// `FlushCold { stream_id, chunk }` now would succeed. A leader checks it
    /// before writing the chunk's cold-index page entry, so the entry's range
    /// is proven to be hot, committed bytes when the write clips other
    /// entries (bounded-state F19 step 1), and a stale candidate writes no
    /// entry at all.
    pub fn check_cold_flush(
        &self,
        stream_id: &BucketStreamId,
        chunk: &ColdChunkRef,
    ) -> Result<(), StreamResponse> {
        self.validate_stream_scope(stream_id)?;
        if chunk.s3_path.trim().is_empty() {
            return Err(StreamResponse::error(
                StreamErrorCode::InvalidColdFlush,
                "cold chunk S3 path must not be empty",
            ));
        }
        if chunk.object_size == 0 {
            return Err(StreamResponse::error(
                StreamErrorCode::InvalidColdFlush,
                "cold chunk object size must be greater than zero",
            ));
        }
        let Some(slot) = self.stream_slot(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        let stream = &slot.metadata;
        if chunk.end_offset <= chunk.start_offset {
            return Err(StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                "cold chunk must cover at least one byte",
                stream.tail_offset,
            ));
        }
        let logical_size = chunk.end_offset.saturating_sub(chunk.start_offset);
        if chunk
            .object_offset
            .checked_add(logical_size)
            .is_none_or(|end| end > chunk.object_size)
        {
            return Err(StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                "cold chunk slice is outside the physical object",
                stream.tail_offset,
            ));
        }
        if !chunk.shared_object && chunk.object_offset != 0 {
            return Err(StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                "exclusive cold chunks must start at physical object offset zero",
                stream.tail_offset,
            ));
        }
        if chunk.end_offset > stream.tail_offset {
            return Err(StreamResponse::error_with_next_offset_and_context(
                StreamErrorCode::InvalidColdFlush,
                format!(
                    "cold chunk end {} is beyond stream '{}' tail {}",
                    chunk.end_offset, stream_id, stream.tail_offset
                ),
                stream.tail_offset,
                vec![StreamErrorContext::StaleColdFlushCandidate],
            ));
        }
        let hot_buffer = &slot.hot_buffer;
        if hot_buffer.hot_start_offset() != chunk.start_offset {
            return Err(StreamResponse::error_with_next_offset_and_context(
                StreamErrorCode::InvalidColdFlush,
                format!("cold chunk for stream '{stream_id}' must start at the hot prefix"),
                stream.tail_offset,
                vec![StreamErrorContext::StaleColdFlushCandidate],
            ));
        }
        if !hot_buffer.covers_prefix(chunk.start_offset, chunk.end_offset) {
            return Err(StreamResponse::error_with_next_offset_and_context(
                StreamErrorCode::InvalidColdFlush,
                format!(
                    "cold chunk for stream '{stream_id}' does not cover contiguous hot payload"
                ),
                stream.tail_offset,
                vec![StreamErrorContext::StaleColdFlushCandidate],
            ));
        }
        if !chunk.payload_digest.is_empty()
            && hot_buffer
                .digest_prefix(chunk.start_offset, chunk.end_offset)
                .as_deref()
                != Some(chunk.payload_digest.as_str())
        {
            return Err(StreamResponse::error_with_next_offset_and_context(
                StreamErrorCode::InvalidColdFlush,
                format!("cold chunk payload for stream '{stream_id}' is stale"),
                stream.tail_offset,
                vec![StreamErrorContext::StaleColdFlushCandidate],
            ));
        }
        Ok(())
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
        if old_chunks.is_empty()
            || (old_chunks.len() < 2 && old_chunks.iter().all(|chunk| !chunk.shared_object))
        {
            return StreamResponse::error(
                StreamErrorCode::InvalidColdFlush,
                "cold compaction requires two raw chunks or one legacy shared chunk",
            );
        }
        if replacement.s3_path.trim().is_empty() || replacement.object_size == 0 {
            return StreamResponse::error(
                StreamErrorCode::InvalidColdFlush,
                "cold compaction replacement must name a non-empty object",
            );
        }
        let rewriting_shared = old_chunks.iter().all(|chunk| chunk.shared_object);
        if old_chunks.iter().any(|chunk| chunk.shared_object) != rewriting_shared
            || replacement.shared_object
            || replacement.object_offset != 0
        {
            return StreamResponse::error(
                StreamErrorCode::InvalidColdFlush,
                "cold compaction cannot mix shared and raw inputs or publish a shared replacement",
            );
        }
        let mut expected_start = old_chunks
            .first()
            .map_or(replacement.start_offset, |chunk| chunk.start_offset);
        let mut compacted_bytes = 0_u64;
        for chunk in &old_chunks {
            let logical_bytes = chunk.end_offset.saturating_sub(chunk.start_offset);
            if chunk.start_offset != expected_start
                || chunk.end_offset <= chunk.start_offset
                || (!chunk.shared_object && chunk.object_size != logical_bytes)
            {
                return StreamResponse::error(
                    StreamErrorCode::InvalidColdFlush,
                    "cold compaction inputs must be contiguous raw chunks",
                );
            }
            expected_start = chunk.end_offset;
            compacted_bytes = compacted_bytes.saturating_add(logical_bytes);
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
        if rewriting_shared {
            let slot = self
                .stream_slot_mut(&stream_id)
                .expect("stream existence checked before cold compaction");
            if !slot.cold.remove_shared_chunks(&old_chunks) {
                return StreamResponse::error(
                    StreamErrorCode::InvalidColdFlush,
                    "legacy shared compaction input no longer matches the stream state",
                );
            }
        }
        let compacted_chunks = u64::try_from(old_chunks.len()).expect("chunk count fits u64");
        let mut exclusive_paths = Vec::new();
        let mut shared_paths = Vec::new();
        for chunk in old_chunks {
            if chunk.shared_object {
                shared_paths.push(chunk.s3_path);
            } else {
                exclusive_paths.push(chunk.s3_path);
            }
        }
        if !exclusive_paths.is_empty() {
            self.cold_gc.enqueue_after(
                stream_id.bucket_id.clone(),
                ColdGcTarget::Paths(exclusive_paths),
                gc_not_before_ms,
            );
        }
        self.release_shared_cold_objects(&stream_id.bucket_id, shared_paths, gc_not_before_ms);
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

    /// [`Self::pending_cold_gc_batch`] annotated for the GC worker with the
    /// cold generation of the live stream that now holds each stream entry's
    /// name (F14g step 1). Read-only and leader-local.
    pub fn plan_cold_gc_batch(&self, max: usize) -> Vec<ColdGcPlanEntry> {
        self.cold_gc
            .batch(max)
            .into_iter()
            .map(|entry| {
                let live_cold_generation = match &entry.target {
                    ColdGcTarget::Stream(stream_id) => self.cold_index_generation(stream_id),
                    ColdGcTarget::Paths(_) => None,
                };
                ColdGcPlanEntry {
                    entry,
                    live_cold_generation,
                }
            })
            .collect()
    }

    /// Cold-index page generation of the live stream `stream_id` (F14g):
    /// 0 for streams created below feature level 1, otherwise the stream's
    /// unique incarnation. Engines write the stream's pages under it.
    pub fn cold_index_generation(&self, stream_id: &BucketStreamId) -> Option<u64> {
        self.stream_slot(stream_id)
            .map(|slot| slot.cold.cold_generation())
    }

    pub fn pending_cold_gc_len(&self) -> usize {
        self.cold_gc.len()
    }

    pub fn pending_cold_gc_len_for_bucket(&self, bucket_id: &str) -> usize {
        self.cold_gc.len_for_bucket(bucket_id)
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
            // F4a collapses records below the seal point, so at level 1
            // every offset at or below it is accepted (F18).
            || (self.producer_bounds_enabled()
                && self
                    .stream_slot(stream_id)
                    .is_some_and(|slot| snapshot_offset <= slot.seal_point()))
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
        prepared_record_retain: Option<crate::record_index::PreparedRetain>,
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
        if let (Some(record_index), Some(prepared)) =
            (slot.record_index.as_mut(), prepared_record_retain)
        {
            record_index.commit_retain(prepared);
        }
        slot.integrity.evict_before(retained_offset);
        let dropped_cold_paths = slot.cold.compact_before(retained_offset);
        self.release_shared_cold_objects(&stream_id.bucket_id, dropped_cold_paths, 0);

        let slot = self
            .stream_slot_mut(stream_id)
            .expect("stream existence checked before hot compact");
        let hot_bytes_before = u64::try_from(slot.hot_buffer.len()).expect("payload len fits u64");
        slot.hot_buffer.discard_before(retained_offset);
        let hot_bytes_after = u64::try_from(slot.hot_buffer.len()).expect("payload len fits u64");
        self.remove_hot_payload_bytes(hot_bytes_before.saturating_sub(hot_bytes_after));
        self.sync_hot_index(stream_id);
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
        // F7: allocate the post-collapse size, not the pre-collapse length.
        let kept = records
            .iter()
            .filter(|record| {
                record.end_offset > frontier
                    && record.end_offset > record.start_offset.max(frontier).max(retained_offset)
            })
            .count();
        let mut compacted =
            Vec::with_capacity(kept.saturating_add(usize::from(frontier > retained_offset)));
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

    /// Lowest offset from which every retained message record is known to
    /// be one exact, whole message whose bytes are still hot.
    ///
    /// Cold flushes cut at byte offsets, then collapse the records below the
    /// flush frontier into one record and truncate a record that straddles
    /// it. The record that starts at (or crosses) the frontier may therefore
    /// be the tail fragment of a message whose head is cold, so the boundary
    /// is placed after that record. Without any cold coverage above the
    /// retained offset, every record is exact and the result is the retained
    /// offset.
    pub(super) fn exact_message_frontier(&self, stream_id: &BucketStreamId) -> u64 {
        let Some(slot) = self.stream_slot(stream_id) else {
            return 0;
        };
        let retained_offset = slot.retained_offset;
        // Every retained byte below the seal point is cold (F18), and F4a
        // collapses message records there, so boundaries are exact only
        // from the seal point on.
        let frontier = slot
            .cold
            .cold_frontier_offset(retained_offset)
            .max(slot.hot_buffer.hot_start_offset())
            .max(slot.seal_point());
        if frontier <= retained_offset {
            return retained_offset;
        }
        slot.message_records
            .iter()
            .find(|record| record.end_offset > frontier)
            .map_or(frontier, |record| record.end_offset)
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
