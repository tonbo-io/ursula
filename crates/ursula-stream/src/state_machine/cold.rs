//! Cold-tier flush planning, GC queue, retention compaction, and snapshot publishing.

use super::BucketStreamId;
use super::ColdChunkRef;
use super::ColdFlushCandidate;
use super::ColdGcEntry;
use super::ColdGcPlanEntry;
use super::ColdGcTarget;
use super::StreamErrorCode;
use super::StreamErrorContext;
use super::StreamResponse;
use super::StreamStateMachine;
use super::StreamVisibleSnapshot;
use super::stream_is_expired;
use crate::json_records::is_json_record_content_type;

/// Grace before the cold GC may delete pack slices that retention dropped
/// (bounded-state F14i). It matches the default
/// `storage.cold.compaction_gc_grace`; apply cannot read node configuration,
/// so the replicated rule uses this constant.
pub const RETENTION_COLD_GC_GRACE_MS: u64 = 300_000;

/// The body of a snapshot publish: inline bytes, or a staged cold-tier
/// object with the digest its proposer computed (F16).
pub(super) enum SnapshotBody {
    Inline(Vec<u8>),
    Object {
        object: super::ExternalPayloadRef,
        digest: String,
    },
}

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
                max_hot_age: None,
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
        body: SnapshotBody,
        now_ms: u64,
        expected_incarnation: Option<u64>,
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
        let (payload, object, digest) = match body {
            SnapshotBody::Inline(payload) => {
                let digest = super::snapshot_digest(&content_type, &payload);
                (payload, None, digest)
            }
            SnapshotBody::Object { object, digest } => (Vec::new(), Some(object), digest),
        };
        let current_snapshot = self
            .stream_slot(&stream_id)
            .and_then(|slot| slot.visible_snapshot.as_ref());
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
                    // An idempotent repeat: the body this command staged is
                    // referenced by nothing (F16).
                    let unreferenced = object.filter(|object| {
                        current.object.as_ref().map(|current| &current.s3_path)
                            != Some(&object.s3_path)
                    });
                    if let Some(object) = unreferenced {
                        self.cold_gc.enqueue_after(
                            stream_id.bucket_id.clone(),
                            ColdGcTarget::Paths(vec![object.s3_path]),
                            now_ms.saturating_add(RETENTION_COLD_GC_GRACE_MS),
                        );
                    }
                    return StreamResponse::SnapshotPublished {
                        snapshot_offset,
                        snapshot_digest: digest,
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
        if let Err(response) = self.check_json_boundary(
            &stream_id,
            snapshot_offset,
            retained_offset,
            expected_incarnation,
        ) {
            return response;
        }

        let superseded = self
            .stream_slot_mut(&stream_id)
            .expect("stream existence checked before snapshot publish")
            .visible_snapshot
            .replace(StreamVisibleSnapshot {
                offset: snapshot_offset,
                content_type,
                payload,
                digest: digest.clone(),
                object,
            });
        // A superseded cold body stays readable for the grace, so a read
        // planned before this publish still finds it (F16, as F14i).
        if let Some(superseded) = superseded.and_then(|snapshot| snapshot.object) {
            self.cold_gc.enqueue_after(
                stream_id.bucket_id.clone(),
                ColdGcTarget::Paths(vec![superseded.s3_path]),
                now_ms.saturating_add(RETENTION_COLD_GC_GRACE_MS),
            );
        }
        StreamResponse::SnapshotPublished {
            snapshot_offset,
            snapshot_digest: digest,
        }
    }

    pub(super) fn advance_retention(
        &mut self,
        stream_id: BucketStreamId,
        retained_offset: u64,
        now_ms: u64,
        expected_incarnation: Option<u64>,
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
            return StreamResponse::RetentionAdvanced { retained_offset };
        }
        if let Err(response) =
            self.check_json_boundary(&stream_id, retained_offset, current, expected_incarnation)
        {
            return response;
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
        // F14i: dropped pack slices stay readable for the compaction grace,
        // so a read planned before this retention still finds its bytes. The
        // not-before time derives from the command's `now_ms`, so every
        // replica enqueues the same entry.
        let gc_not_before_ms = now_ms.saturating_add(RETENTION_COLD_GC_GRACE_MS);
        self.compact_retained_prefix(&stream_id, retained_offset, gc_not_before_ms);
        StreamResponse::RetentionAdvanced { retained_offset }
    }

    pub(super) fn flush_cold(
        &mut self,
        stream_id: BucketStreamId,
        chunk: ColdChunkRef,
        cold_generation: u64,
    ) -> StreamResponse {
        if let Err(response) = self.check_cold_flush(&stream_id, &chunk) {
            return response;
        }
        // F14g follow-up: a flush planned from a removed incarnation must not
        // publish into the stream that replaced it, even when the new
        // incarnation's hot prefix holds the same bytes.
        if let Err(response) = self.check_cold_flush_generation(&stream_id, cold_generation) {
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
            self.retain_shared_cold_object(&path);
        }
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

    /// Read-only incarnation check of a cold flush: `Ok` when
    /// `planned` names the live stream's generation. Apply runs it, and
    /// leaders run it before writing any page entry, because the entry lands
    /// in the live stream's generation.
    pub fn check_cold_flush_generation(
        &self,
        stream_id: &BucketStreamId,
        planned: u64,
    ) -> Result<(), StreamResponse> {
        let Some(slot) = self.stream_slot(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        let live = slot.cold.cold_generation();
        if live == planned {
            return Ok(());
        }
        Err(StreamResponse::error_with_next_offset_and_context(
            StreamErrorCode::InvalidColdFlush,
            format!(
                "cold chunk for stream '{stream_id}' was planned from cold generation {planned}, \
                 but the live incarnation uses generation {live}"
            ),
            slot.metadata.tail_offset,
            vec![StreamErrorContext::StaleColdFlushCandidate],
        ))
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
                "cold compaction requires two raw chunks or one shared chunk",
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
                    "shared compaction input no longer matches the stream state",
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

    pub(super) fn ack_cold_gc(&mut self, up_to_seq: u64) -> StreamResponse {
        let removed = self.cold_gc.ack(up_to_seq);
        StreamResponse::ColdGcAcked { removed }
    }

    /// Applies [`crate::StreamCommand::DeferColdGc`] (F14b).
    pub(super) fn defer_cold_gc(&mut self, seq: u64, not_before_ms: u64) -> StreamResponse {
        StreamResponse::ColdGcDeferred {
            new_seq: self.cold_gc.defer(seq, not_before_ms),
        }
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

    /// Cold-index page generation of the live stream `stream_id` (F14g): the
    /// stream's unique incarnation. Engines write the stream's pages under it.
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

    /// The JSON LF obligation, apply side, for a snapshot or retention
    /// offset already inside `[floor, tail]` (`floor` is the retained
    /// offset). A JSON message boundary is an offset whose preceding byte is
    /// LF; the floor, the tail and offset 0 always are. When that byte is
    /// hot, apply checks it. Otherwise only the proposer can read it, so
    /// apply needs `expected_incarnation` naming the stream incarnation the
    /// proposer read it from, and refuses a missing or stale one with
    /// [`StreamErrorCode::JsonBoundaryUnverified`]. Other streams have no
    /// message boundaries: any offset in range is accepted.
    fn check_json_boundary(
        &self,
        stream_id: &BucketStreamId,
        offset: u64,
        floor: u64,
        expected_incarnation: Option<u64>,
    ) -> Result<(), StreamResponse> {
        let Some(slot) = self.stream_slot(stream_id) else {
            return Ok(());
        };
        let stream = &slot.metadata;
        if !is_json_record_content_type(&stream.content_type)
            || offset == 0
            || offset == floor
            || offset == stream.tail_offset
        {
            return Ok(());
        }
        match slot.hot_buffer.byte_at(offset - 1) {
            Some(b'\n') => Ok(()),
            Some(_) => Err(StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidSnapshot,
                format!("offset {offset} is not a JSON message boundary for stream '{stream_id}'"),
                stream.tail_offset,
            )),
            None if expected_incarnation == Some(stream.created_at_ms) => Ok(()),
            None => Err(StreamResponse::error_with_next_offset_and_context(
                StreamErrorCode::JsonBoundaryUnverified,
                format!(
                    "offset {offset} of stream '{stream_id}' needs a verified JSON message boundary"
                ),
                stream.tail_offset,
                vec![StreamErrorContext::StreamIncarnation {
                    incarnation: stream.created_at_ms,
                }],
            )),
        }
    }

    pub(super) fn compact_retained_prefix(
        &mut self,
        stream_id: &BucketStreamId,
        retained_offset: u64,
        gc_not_before_ms: u64,
    ) {
        let slot = self
            .stream_slot_mut(stream_id)
            .expect("stream existence checked before retained-prefix compaction");
        let dropped_cold_paths = slot.cold.compact_before(retained_offset);
        self.release_shared_cold_objects(
            &stream_id.bucket_id,
            dropped_cold_paths,
            gc_not_before_ms,
        );

        let slot = self
            .stream_slot_mut(stream_id)
            .expect("stream existence checked before hot compact");
        let hot_bytes_before = u64::try_from(slot.hot_buffer.len()).expect("payload len fits u64");
        slot.hot_buffer.discard_before(retained_offset);
        let hot_bytes_after = u64::try_from(slot.hot_buffer.len()).expect("payload len fits u64");
        self.remove_hot_payload_bytes(hot_bytes_before.saturating_sub(hot_bytes_after));
        self.sync_hot_index(stream_id);
    }
}
