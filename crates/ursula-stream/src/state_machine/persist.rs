//! Snapshot / restore / integrity serialization for the Raft state machine.

use super::BucketStreamId;
use super::ColdChunkRef;
use super::ColdGcQueue;
use super::HashMap;
use super::HotBuffer;
use super::HotPayloadSegment;
use super::ObjectPayloadRef;
use super::ProducerReceipt;
use super::ProducerSnapshot;
use super::ProducerState;
use super::StreamColdState;
use super::StreamErrorCode;
use super::StreamIntegrity;
use super::StreamMessageRecord;
use super::StreamResponse;
use super::StreamSlot;
use super::StreamSnapshot;
use super::StreamSnapshotEntry;
use super::StreamSnapshotError;
use super::StreamStateMachine;
use super::compare_stream_ids;
use super::normalize_stream_attrs;

impl StreamStateMachine {
    pub fn integrity_snapshot(
        &self,
        stream_id: &BucketStreamId,
    ) -> Result<crate::integrity::StreamIntegritySnapshot, StreamResponse> {
        let Some(slot) = self.stream_slot(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        Ok(slot.integrity.snapshot(
            self.earliest_retained_offset(stream_id),
            slot.metadata.tail_offset,
        ))
    }

    pub fn snapshot(&self) -> StreamSnapshot {
        let mut buckets = self.buckets.iter().cloned().collect::<Vec<_>>();
        buckets.sort();

        let mut streams = self
            .registry
            .slots()
            .map(|slot| {
                let metadata = slot.metadata.clone();
                let stream_id = metadata.stream_id.clone();
                let tail_offset = metadata.tail_offset;
                let payload = slot.hot_buffer.payload();
                let producer_states = producer_snapshot(&slot.producers);
                StreamSnapshotEntry {
                    metadata,
                    attrs: slot.attrs.clone(),
                    hot_start_offset: self.hot_start_offset(&stream_id),
                    payload,
                    hot_segments: slot.hot_buffer.hot_segments(),
                    cold_frontier_offset: self.cold_frontier_offset(
                        &stream_id,
                        self.earliest_retained_offset(&stream_id),
                    ),
                    cold_index_generation: slot.cold.cold_generation(),
                    cold_chunks: slot.cold.cold_chunks().to_vec(),
                    external_segments: slot.cold.external_segments().to_vec(),
                    message_records: slot.message_records.clone(),
                    record_index: slot.record_index.clone(),
                    integrity: slot
                        .integrity
                        .snapshot(self.earliest_retained_offset(&stream_id), tail_offset),
                    retained_offset: Some(slot.retained_offset),
                    visible_snapshot: slot.visible_snapshot.clone(),
                    producer_states,
                }
            })
            .collect::<Vec<_>>();
        streams.sort_by(|left, right| {
            compare_stream_ids(&left.metadata.stream_id, &right.metadata.stream_id)
        });

        StreamSnapshot {
            buckets,
            streams,
            pending_cold_gc: self.cold_gc.entries().cloned().collect(),
            next_cold_gc_seq: self.cold_gc.next_seq(),
            bucket_usage: self.bucket_usage_report(),
        }
    }

    pub fn restore(snapshot: StreamSnapshot) -> Result<Self, StreamSnapshotError> {
        let mut machine = Self::default();
        for bucket_id in snapshot.buckets {
            if !machine.buckets.insert(bucket_id.clone()) {
                return Err(StreamSnapshotError::DuplicateBucket(bucket_id));
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
                    entry.cold_frontier_offset,
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
            if !message_records_cover_retained_suffix(
                &entry.message_records,
                retained_offset,
                entry.metadata.tail_offset,
            ) {
                return Err(StreamSnapshotError::MessageBoundaryMismatch { stream_id });
            }
            let integrity = StreamIntegrity::restore(entry.integrity).ok_or_else(|| {
                StreamSnapshotError::IntegrityMismatch {
                    stream_id: stream_id.clone(),
                }
            })?;
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
            let slot = StreamSlot {
                metadata: entry.metadata,
                attrs: normalize_stream_attrs(entry.attrs),
                hot_buffer: HotBuffer::from_snapshot(entry.payload, &hot_segments),
                cold: StreamColdState::restore(
                    entry.cold_frontier_offset,
                    entry.cold_index_generation,
                    entry.cold_chunks,
                    entry.external_segments,
                ),
                message_records: entry.message_records,
                record_index: entry.record_index,
                integrity,
                retained_offset,
                visible_snapshot,
                producers: producer_states,
            };
            if machine.insert_stream_slot(slot).is_none() {
                return Err(StreamSnapshotError::DuplicateStream(stream_id));
            }
        }

        machine.cold_gc =
            ColdGcQueue::from_parts(snapshot.pending_cold_gc, snapshot.next_cold_gc_seq);

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
            last_items: state.last_items.clone(),
            receipts: state.receipts.clone(),
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
        let receipts = if snapshot.receipts.is_empty() {
            vec![ProducerReceipt {
                producer_seq: snapshot.producer_seq,
                start_offset: snapshot.last_start_offset,
                next_offset: snapshot.last_next_offset,
                closed: snapshot.last_closed,
                items: snapshot.last_items.clone(),
            }]
        } else {
            snapshot.receipts
        };
        if states
            .insert(snapshot.producer_id.clone(), ProducerState {
                producer_epoch: snapshot.producer_epoch,
                producer_seq: snapshot.producer_seq,
                last_start_offset: snapshot.last_start_offset,
                last_next_offset: snapshot.last_next_offset,
                last_closed: snapshot.last_closed,
                last_items: snapshot.last_items,
                receipts,
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
    chunk.end_offset > chunk.start_offset
        && !chunk.s3_path.trim().is_empty()
        && chunk.object_size >= chunk.end_offset - chunk.start_offset
}

fn valid_object_payload_ref(object: &ObjectPayloadRef) -> bool {
    object.end_offset > object.start_offset
        && !object.s3_path.trim().is_empty()
        && object.object_size >= object.end_offset - object.start_offset
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

fn payload_sources_cover_retained_suffix(
    cold_frontier_offset: u64,
    cold_chunks: &[ColdChunkRef],
    external_segments: &[ObjectPayloadRef],
    hot_segments: &[HotPayloadSegment],
    retained_offset: u64,
    tail_offset: u64,
) -> bool {
    if tail_offset < retained_offset {
        return false;
    }
    let mut ranges =
        Vec::with_capacity(1 + cold_chunks.len() + external_segments.len() + hot_segments.len());
    if cold_frontier_offset > retained_offset {
        ranges.push((retained_offset, cold_frontier_offset));
    }
    for chunk in cold_chunks {
        if !valid_cold_chunk_ref(chunk) {
            return false;
        }
        ranges.push((chunk.start_offset, chunk.end_offset));
    }
    for object in external_segments {
        if !valid_object_payload_ref(object) {
            return false;
        }
        ranges.push((object.start_offset, object.end_offset));
    }
    for segment in hot_segments {
        if segment.end_offset <= segment.start_offset {
            return false;
        }
        ranges.push((segment.start_offset, segment.end_offset));
    }
    ranges.sort_unstable();

    let mut expected_start = retained_offset;
    for (start_offset, end_offset) in ranges {
        if end_offset <= expected_start {
            continue;
        }
        if start_offset > expected_start {
            return false;
        }
        expected_start = end_offset;
        if expected_start >= tail_offset {
            return true;
        }
    }
    expected_start == tail_offset
}

pub(super) fn message_records_cover_retained_suffix(
    records: &[StreamMessageRecord],
    retained_offset: u64,
    tail_offset: u64,
) -> bool {
    let mut expected_start = retained_offset;
    for record in records {
        if record.start_offset != expected_start || record.end_offset <= record.start_offset {
            return false;
        }
        expected_start = record.end_offset;
    }
    expected_start == tail_offset
}
