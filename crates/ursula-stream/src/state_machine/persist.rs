//! Snapshot / restore serialization for the Raft state machine, and the
//! import that restores a backup as a history event (#581 section 4.4).

use std::num::NonZeroU64;

use super::BucketStreamId;
use super::ColdChunkRef;
use super::ColdGcQueue;
use super::HashMap;
use super::HistoryEvent;
use super::HotBuffer;
use super::HotPayloadSegment;
use super::ObjectPayloadRef;
use super::ProducerSnapshot;
use super::ProducerState;
use super::StreamColdState;
use super::StreamErrorCode;
use super::StreamResponse;
use super::StreamSlot;
use super::StreamSnapshot;
use super::StreamSnapshotEntry;
use super::StreamSnapshotError;
use super::StreamStateMachine;
use super::compare_stream_ids;

impl StreamStateMachine {
    pub fn snapshot(&self) -> StreamSnapshot {
        let mut buckets = self.buckets.iter().cloned().collect::<Vec<_>>();
        buckets.sort();
        let mut erased_buckets = self.erased_buckets.iter().cloned().collect::<Vec<_>>();
        erased_buckets.sort();

        let mut streams = self
            .registry
            .slots()
            .map(|slot| {
                let metadata = slot.metadata.clone();
                let stream_id = metadata.stream_id.clone();
                let payload = slot.hot_buffer.payload();
                let producer_states = producer_snapshot(&slot.producers);
                StreamSnapshotEntry {
                    metadata,
                    hot_start_offset: self.hot_start_offset(&stream_id),
                    payload,
                    hot_segments: slot.hot_buffer.hot_segments(),
                    cold_index_generation: slot.cold.cold_generation(),
                    cold_chunks: slot.cold.cold_chunks().to_vec(),
                    external_segments: slot.cold.external_segments().to_vec(),
                    retained_offset: slot.retained_offset,
                    visible_snapshot: slot.visible_snapshot.clone(),
                    producer_states,
                    retired_epoch_watermark: slot.retired_epoch_watermark,
                    history: slot.history,
                    inherited: slot.inherited,
                }
            })
            .collect::<Vec<_>>();
        streams.sort_by(|left, right| {
            compare_stream_ids(&left.metadata.stream_id, &right.metadata.stream_id)
        });

        StreamSnapshot {
            format_epoch: crate::FORMAT_EPOCH,
            buckets,
            erased_buckets,
            streams,
            pending_cold_gc: self.cold_gc.entries().cloned().collect(),
            next_cold_gc_seq: self.cold_gc.next_seq(),
            bucket_usage: self.bucket_usage_report(),
            last_created_at_ms: self.last_created_at_ms,
            history: self.history,
        }
    }

    /// Applies a whole-group state import (`StreamCommand::ImportSnapshot`)
    /// of the entry at `index`, a history event (#581 section 4.4).
    ///
    /// 1. A group that already holds the import of `backup_digest` answers
    ///    it again with `already`, so a repeat after an unknown outcome is
    ///    safe.
    /// 2. Restore-only by design: importing over live state would silently
    ///    merge two histories, so a non-empty group fails closed with
    ///    [`StreamErrorCode::ImportConflict`] and an invalid payload with
    ///    [`StreamErrorCode::ImportInvalid`]. A group that applied only its
    ///    genesis is empty.
    /// 3. The restore replaces the whole state. Producer records keep the
    ///    history they were claimed in, so each is history-fenced (G2).
    ///    Each stream names the source's history as the one its current
    ///    history began from, with its restored tail (W5).
    /// 4. The new history starts in the same entry, so no restored stream
    ///    is ever served under an earlier token.
    pub(crate) fn import_snapshot(
        &mut self,
        index: u64,
        snapshot: StreamSnapshot,
        history: NonZeroU64,
        backup_digest: crate::history::BackupDigest,
        now_ms: u64,
    ) -> StreamResponse {
        if let Some(current) = self.history
            && current.imported == Some(backup_digest)
            && !crate::sim_controls::off(crate::sim_controls::Defence::ImportKey)
        {
            return StreamResponse::SnapshotImported {
                buckets: u64::try_from(self.buckets.len()).unwrap_or(u64::MAX),
                streams: u64::try_from(self.registry.slots().count()).unwrap_or(u64::MAX),
                history: current.id,
                already: true,
            };
        }
        if !self.buckets.is_empty() || !self.erased_buckets.is_empty() || !self.registry.is_empty()
        {
            return StreamResponse::error(
                StreamErrorCode::ImportConflict,
                format!(
                    "group already holds {} bucket(s); snapshot import requires an empty group",
                    self.buckets.len()
                ),
            );
        }
        if snapshot.format_epoch != crate::FORMAT_EPOCH {
            return StreamResponse::error(
                StreamErrorCode::ImportInvalid,
                format!(
                    "snapshot import failed validation: {}",
                    StreamSnapshotError::FormatEpoch {
                        found: snapshot.format_epoch,
                    }
                ),
            );
        }
        let buckets = u64::try_from(snapshot.buckets.len()).unwrap_or(u64::MAX);
        let streams = u64::try_from(snapshot.streams.len()).unwrap_or(u64::MAX);
        let mut snapshot = snapshot;
        // A source stream not written since the source's own latest history
        // event still names the history before it. The source served its
        // bytes under its current token too, and that is the token a client
        // of the source holds, so the import names it.
        let source_history = snapshot.history.map_or(0, |source| source.id.get());
        for entry in &mut snapshot.streams {
            entry.history = source_history;
        }
        match Self::restore(snapshot) {
            Ok(mut restored) => {
                // C7: the counter never goes backwards, so a later create
                // never reuses an incarnation this group already assigned.
                restored.last_created_at_ms = restored
                    .last_created_at_ms
                    .max(self.last_created_at_ms)
                    .max(now_ms);
                restored.normalize_last_created_at_ms();
                restored.history = Some(HistoryEvent {
                    id: history,
                    event_index: index,
                    imported: Some(backup_digest),
                });
                *self = restored;
                StreamResponse::SnapshotImported {
                    buckets,
                    streams,
                    history,
                    already: false,
                }
            }
            Err(error) => StreamResponse::error(
                StreamErrorCode::ImportInvalid,
                format!("snapshot import failed validation: {error}"),
            ),
        }
    }

    /// Keeps C7's invariant after a restore or import: `last_created_at_ms`
    /// is at least every live stream's `created_at_ms`, so the next create is
    /// unique whatever the snapshot recorded.
    fn normalize_last_created_at_ms(&mut self) {
        self.last_created_at_ms = self.max_live_created_at_ms(self.last_created_at_ms);
    }

    pub fn restore(snapshot: StreamSnapshot) -> Result<Self, StreamSnapshotError> {
        if snapshot.format_epoch != crate::FORMAT_EPOCH {
            return Err(StreamSnapshotError::FormatEpoch {
                found: snapshot.format_epoch,
            });
        }
        let mut machine = Self {
            last_created_at_ms: snapshot.last_created_at_ms,
            history: snapshot.history,
            ..Self::default()
        };
        for bucket_id in &snapshot.buckets {
            if !machine.buckets.insert(bucket_id.clone()) {
                return Err(StreamSnapshotError::DuplicateBucket(bucket_id.clone()));
            }
        }
        for bucket_id in &snapshot.erased_buckets {
            if machine.buckets.contains(bucket_id) {
                return Err(StreamSnapshotError::ActiveBucketErased(bucket_id.clone()));
            }
            if !machine.erased_buckets.insert(bucket_id.clone()) {
                return Err(StreamSnapshotError::DuplicateErasedBucket(
                    bucket_id.clone(),
                ));
            }
        }

        for entry in snapshot.streams {
            let stream_id = entry.metadata.stream_id.clone();
            if !machine.buckets.contains(&stream_id.bucket_id) {
                return Err(StreamSnapshotError::MissingBucket(stream_id));
            }
            if let Some(snapshot) = entry.visible_snapshot.as_ref()
                && snapshot.offset > entry.metadata.tail_offset
            {
                return Err(StreamSnapshotError::SnapshotOffsetOutOfRange {
                    stream_id,
                    snapshot_offset: snapshot.offset,
                    tail_offset: entry.metadata.tail_offset,
                });
            }
            if entry.retained_offset > entry.metadata.tail_offset {
                return Err(StreamSnapshotError::SnapshotOffsetOutOfRange {
                    stream_id,
                    snapshot_offset: entry.retained_offset,
                    tail_offset: entry.metadata.tail_offset,
                });
            }
            if !hot_segments_match_payload(&entry.hot_segments, entry.payload.len())
                || !payload_sources_cover_retained_suffix(
                    &entry.cold_chunks,
                    &entry.external_segments,
                    &entry.hot_segments,
                    entry.retained_offset,
                    entry.metadata.tail_offset,
                )
            {
                return Err(StreamSnapshotError::PayloadLengthMismatch {
                    stream_id,
                    tail_offset: entry.metadata.tail_offset,
                    payload_len: entry.payload.len(),
                });
            }
            if machine.registry.contains_key(&stream_id) {
                return Err(StreamSnapshotError::DuplicateStream(stream_id));
            }
            let producer_states = restore_producer_states(&stream_id, entry.producer_states)?;
            let shared_cold_paths = entry
                .cold_chunks
                .iter()
                .filter(|chunk| chunk.shared_object)
                .map(|chunk| chunk.s3_path.clone())
                .collect::<Vec<_>>();
            let hot_buffer = HotBuffer::from_snapshot(entry.payload, &entry.hot_segments);
            let slot = StreamSlot {
                metadata: entry.metadata,
                hot_buffer,
                cold: StreamColdState::restore(
                    entry.cold_index_generation,
                    entry.cold_chunks,
                    entry.external_segments,
                ),
                retained_offset: entry.retained_offset,
                visible_snapshot: entry.visible_snapshot,
                producers: producer_states,
                retired_epoch_watermark: entry.retired_epoch_watermark,
                history: entry.history,
                inherited: entry.inherited,
                append_count: 0,
            };
            if machine.insert_stream_slot(slot).is_none() {
                return Err(StreamSnapshotError::DuplicateStream(stream_id));
            }
            for path in shared_cold_paths {
                machine.retain_shared_cold_object(&path);
            }
        }

        for entry in &snapshot.pending_cold_gc {
            if entry.bucket_id.is_empty() {
                return Err(StreamSnapshotError::UnattributedColdGc { seq: entry.seq });
            }
            if matches!(entry.target, super::ColdGcTarget::Stream(_))
                && entry.cold_generation.is_none()
            {
                return Err(StreamSnapshotError::ColdGcWithoutGeneration { seq: entry.seq });
            }
        }
        machine.cold_gc =
            ColdGcQueue::from_parts(snapshot.pending_cold_gc, snapshot.next_cold_gc_seq);
        machine.normalize_last_created_at_ms();

        // Usage restore: gauges are recomputed from the restored slots so a
        // snapshot can never carry gauge drift forward; only the monotonic
        // counters are taken from the snapshot.
        let mut recomputed: HashMap<String, super::BucketUsage> = HashMap::new();
        for slot in machine.registry.slots() {
            let usage = recomputed
                .entry(slot.metadata.stream_id.bucket_id.clone())
                .or_default();
            usage.stream_count = usage.stream_count.saturating_add(1);
            usage.retained_bytes = usage.retained_bytes.saturating_add(
                slot.metadata
                    .tail_offset
                    .saturating_sub(slot.retained_offset),
            );
        }
        for persisted in snapshot.bucket_usage {
            let usage = recomputed.entry(persisted.bucket_id).or_default();
            usage.committed_append_bytes = persisted.usage.committed_append_bytes;
            usage.committed_records = persisted.usage.committed_records;
            usage.committed_write_units = persisted.usage.committed_write_units;
        }
        machine.bucket_usage = recomputed;

        Ok(machine)
    }
}

fn producer_snapshot(states: &HashMap<String, ProducerState>) -> Vec<ProducerSnapshot> {
    let mut producer_states = states
        .iter()
        .map(|(producer_id, state)| ProducerSnapshot {
            producer_id: producer_id.clone(),
            epoch: state.epoch,
            last_seq: state.last_seq,
            claim_digest: state.claim_digest,
            newest: state.newest.clone(),
            closed_epoch: state.closed_epoch,
            opened: state.opened,
            history: state.history,
            last_seen_ms: state.last_seen_ms,
        })
        .collect::<Vec<_>>();
    producer_states.sort_by(|left, right| left.producer_id.cmp(&right.producer_id));
    producer_states
}

fn restore_producer_states(
    stream_id: &BucketStreamId,
    snapshots: Vec<ProducerSnapshot>,
) -> Result<HashMap<String, ProducerState>, StreamSnapshotError> {
    let mut states = HashMap::with_capacity(snapshots.len());
    for snapshot in snapshots {
        if states
            .insert(snapshot.producer_id.clone(), ProducerState {
                epoch: snapshot.epoch,
                last_seq: snapshot.last_seq,
                claim_digest: snapshot.claim_digest,
                newest: snapshot.newest,
                closed_epoch: snapshot.closed_epoch,
                opened: snapshot.opened,
                history: snapshot.history,
                last_seen_ms: snapshot.last_seen_ms,
            })
            .is_some()
        {
            return Err(StreamSnapshotError::DuplicateProducer {
                stream_id: stream_id.clone(),
                producer_id: snapshot.producer_id,
            });
        }
    }
    Ok(states)
}

fn valid_cold_chunk_ref(chunk: &ColdChunkRef) -> bool {
    let logical_len = chunk.end_offset.saturating_sub(chunk.start_offset);
    chunk.end_offset > chunk.start_offset
        && !chunk.s3_path.trim().is_empty()
        && chunk
            .object_offset
            .checked_add(logical_len)
            .is_some_and(|end| end <= chunk.object_size)
}

fn valid_object_payload_ref(object: &ObjectPayloadRef) -> bool {
    let logical_len = object.end_offset.saturating_sub(object.start_offset);
    object.end_offset > object.start_offset
        && !object.s3_path.trim().is_empty()
        && object
            .object_offset
            .checked_add(logical_len)
            .is_some_and(|end| end <= object.object_size)
}

fn hot_segments_match_payload(segments: &[HotPayloadSegment], payload_len: usize) -> bool {
    let mut expected_payload_start = 0;
    for segment in segments {
        if segment.end_offset <= segment.start_offset
            || segment.payload_start != expected_payload_start
            || segment.payload_end <= segment.payload_start
            || segment.payload_end > payload_len
        {
            return false;
        }
        let (Some(logical_span), Some(stored_len)) = (
            segment.end_offset.checked_sub(segment.start_offset),
            segment.payload_end.checked_sub(segment.payload_start),
        ) else {
            return false;
        };
        let Ok(logical_len) = usize::try_from(logical_span) else {
            return false;
        };
        if logical_len != stored_len {
            return false;
        }
        expected_payload_start = segment.payload_end;
    }
    expected_payload_start == payload_len
}

/// F18 step 1: coverage is the complement of the hot buffer, so every byte
/// of `[retained, tail)` that no hot segment holds is cold and served by
/// state refs or cold-index pages. Restore therefore no longer requires the
/// replicated scalar frontier to reach the hot buffer or the tail; snapshots
/// that carry a regressed frontier (bounded-state D1) restore and install.
/// What remains checked is that every source is well formed: refs are
/// valid, and hot segments are non-empty, ordered, disjoint and end at or
/// below the tail.
fn payload_sources_cover_retained_suffix(
    cold_chunks: &[ColdChunkRef],
    external_segments: &[ObjectPayloadRef],
    hot_segments: &[HotPayloadSegment],
    retained_offset: u64,
    tail_offset: u64,
) -> bool {
    if tail_offset < retained_offset {
        return false;
    }
    if !cold_chunks.iter().all(valid_cold_chunk_ref)
        || !external_segments.iter().all(valid_object_payload_ref)
    {
        return false;
    }
    let mut previous_end = 0;
    for segment in hot_segments {
        if segment.end_offset <= segment.start_offset
            || segment.start_offset < previous_end
            || segment.end_offset > tail_offset
        {
            return false;
        }
        previous_end = segment.end_offset;
    }
    true
}
