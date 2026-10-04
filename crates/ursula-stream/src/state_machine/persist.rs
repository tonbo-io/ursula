//! Snapshot / restore serialization for the Raft state machine.

use super::BucketStreamId;
use super::ColdChunkRef;
use super::ColdGcQueue;
use super::HashMap;
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
                    hot_append_starts: slot.hot_buffer.append_starts().iter().copied().collect(),
                    record_index: slot.record_index.clone(),
                    retained_offset: Some(slot.retained_offset),
                    visible_snapshot: slot.visible_snapshot.clone(),
                    producer_states,
                }
            })
            .collect::<Vec<_>>();
        streams.sort_by(|left, right| {
            compare_stream_ids(&left.metadata.stream_id, &right.metadata.stream_id)
        });

        let mut shared_cold_object_owners = self
            .shared_cold_object_owners
            .iter()
            .filter(|(path, _)| self.shared_cold_object_refs.contains_key(*path))
            .map(|(path, owners)| {
                let mut bucket_ids = owners.iter().cloned().collect::<Vec<_>>();
                bucket_ids.sort();
                crate::SharedColdObjectOwnersSnapshot {
                    s3_path: path.clone(),
                    bucket_ids,
                }
            })
            .collect::<Vec<_>>();
        shared_cold_object_owners.sort_by(|left, right| left.s3_path.cmp(&right.s3_path));

        StreamSnapshot {
            format_epoch: crate::FORMAT_EPOCH,
            buckets,
            erased_buckets,
            streams,
            pending_cold_gc: self.cold_gc.entries().cloned().collect(),
            next_cold_gc_seq: self.cold_gc.next_seq(),
            shared_cold_object_owners,
            bucket_usage: self.bucket_usage_report(),
            last_created_at_ms: self.last_created_at_ms,
        }
    }

    /// Applies a whole-group state import (`StreamCommand::ImportSnapshot`).
    ///
    /// Restore-only by design: importing over live state would silently merge
    /// two histories, so a non-empty group fails closed with
    /// [`StreamErrorCode::ImportConflict`] and an invalid payload with
    /// [`StreamErrorCode::ImportInvalid`].
    pub(crate) fn import_snapshot(&mut self, snapshot: StreamSnapshot) -> StreamResponse {
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
        match Self::restore(snapshot) {
            Ok(mut restored) => {
                // C7: the counter never goes backwards, so a later create
                // never reuses an incarnation this group already assigned.
                restored.last_created_at_ms =
                    restored.last_created_at_ms.max(self.last_created_at_ms);
                restored.normalize_last_created_at_ms();
                *self = restored;
                StreamResponse::SnapshotImported { buckets, streams }
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
            let retained_offset = entry.retained_offset.unwrap_or_else(|| {
                entry
                    .visible_snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.offset)
                    .unwrap_or(0)
            });
            if retained_offset > entry.metadata.tail_offset {
                return Err(StreamSnapshotError::SnapshotOffsetOutOfRange {
                    stream_id,
                    snapshot_offset: retained_offset,
                    tail_offset: entry.metadata.tail_offset,
                });
            }
            if let Some(record_index) = entry.record_index.as_ref()
                && record_index
                    .validate(retained_offset, entry.metadata.tail_offset)
                    .is_err()
            {
                return Err(StreamSnapshotError::RecordBoundaryMismatch { stream_id });
            }
            let hot_segments = if entry.hot_segments.is_empty() && !entry.payload.is_empty() {
                vec![HotPayloadSegment {
                    start_offset: entry.hot_start_offset,
                    end_offset: entry.metadata.tail_offset,
                    payload_start: 0,
                    payload_end: entry.payload.len(),
                }]
            } else {
                entry.hot_segments
            };
            if !hot_segments_match_payload(&hot_segments, entry.payload.len())
                || !payload_sources_cover_retained_suffix(
                    &entry.cold_chunks,
                    &entry.external_segments,
                    &hot_segments,
                    retained_offset,
                    entry.metadata.tail_offset,
                )
            {
                return Err(StreamSnapshotError::PayloadLengthMismatch {
                    stream_id,
                    tail_offset: entry.metadata.tail_offset,
                    payload_len: entry.payload.len(),
                });
            }
            // F4b: a stream's boundaries are the dense offsets or the append
            // starts, which must lie at or above the seal point.
            let seal_point = hot_segments
                .first()
                .map_or(entry.metadata.tail_offset, |segment| segment.start_offset);
            if !super::boundaries::append_starts_valid(
                &entry.hot_append_starts,
                seal_point,
                entry.metadata.tail_offset,
                entry.record_index.is_some(),
            ) {
                return Err(StreamSnapshotError::MessageBoundaryMismatch { stream_id });
            }
            if machine.registry.contains_key(&stream_id) {
                return Err(StreamSnapshotError::DuplicateStream(stream_id));
            }
            let producer_states = restore_producer_states(&stream_id, entry.producer_states)?;
            let visible_snapshot = entry.visible_snapshot.map(|mut snapshot| {
                if snapshot.digest.is_empty() {
                    snapshot.digest =
                        super::snapshot_digest(&snapshot.content_type, &snapshot.payload);
                }
                snapshot
            });
            let shared_cold_paths = entry
                .cold_chunks
                .iter()
                .filter(|chunk| chunk.shared_object)
                .map(|chunk| chunk.s3_path.clone())
                .collect::<Vec<_>>();
            let mut hot_buffer = HotBuffer::from_snapshot(entry.payload, &hot_segments);
            hot_buffer.restore_append_starts(entry.hot_append_starts);
            let slot = StreamSlot {
                metadata: entry.metadata,
                hot_buffer,
                cold: StreamColdState::restore(
                    entry.cold_index_generation,
                    entry.cold_chunks,
                    entry.external_segments,
                ),
                record_index: entry.record_index,
                retained_offset,
                visible_snapshot,
                receipt_window: super::producers::ReceiptWindow::rebuild(&producer_states),
                producers: producer_states,
                append_count: 0,
            };
            if machine.insert_stream_slot(slot).is_none() {
                return Err(StreamSnapshotError::DuplicateStream(stream_id));
            }
            for path in shared_cold_paths {
                machine.retain_shared_cold_object(&path, &stream_id.bucket_id);
            }
        }

        for record in snapshot.shared_cold_object_owners {
            if machine
                .shared_cold_object_refs
                .contains_key(&record.s3_path)
            {
                machine
                    .shared_cold_object_owners
                    .entry(record.s3_path)
                    .or_default()
                    .extend(record.bucket_ids);
            }
        }

        machine.cold_gc =
            ColdGcQueue::from_parts(snapshot.pending_cold_gc, snapshot.next_cold_gc_seq);
        machine.normalize_last_created_at_ms();

        // Usage restore: gauges are recomputed from the restored slots so a
        // snapshot can never carry gauge drift forward; only the monotonic
        // counters are taken from the snapshot. Legacy snapshots without the
        // field restart the monotonic counters from the recomputed gauges.
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
        for usage in recomputed.values_mut() {
            if usage.committed_append_bytes == 0 {
                usage.committed_append_bytes = usage.retained_bytes;
            }
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
            producer_epoch: state.producer_epoch,
            producer_seq: state.producer_seq,
            last_start_offset: state.last_start_offset,
            last_next_offset: state.last_next_offset,
            last_closed: state.last_closed,
            receipts: state.receipts.iter().cloned().collect(),
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
        let receipts = snapshot.receipts;
        if states
            .insert(snapshot.producer_id.clone(), ProducerState {
                producer_epoch: snapshot.producer_epoch,
                producer_seq: snapshot.producer_seq,
                last_start_offset: snapshot.last_start_offset,
                last_next_offset: snapshot.last_next_offset,
                last_closed: snapshot.last_closed,
                receipts: receipts.into(),
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
        let Ok(logical_len) = usize::try_from(segment.end_offset - segment.start_offset) else {
            return false;
        };
        if logical_len != segment.payload_end - segment.payload_start {
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
