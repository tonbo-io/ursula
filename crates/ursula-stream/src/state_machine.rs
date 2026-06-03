use std::collections::{HashMap, HashSet, VecDeque};

use ursula_shard::BucketStreamId;

use crate::command::StreamCommand;
use crate::integrity::StreamIntegrity;
use crate::model::{
    AppendExternalInput, AppendStreamInput, ColdChunkRef, ColdFlushCandidate, ColdGcEntry,
    ColdGcTarget, ExternalPayloadRef, HotPayloadSegment, ObjectPayloadRef, ProducerAppendRecord,
    ProducerRequest, ProducerSnapshot, ProducerState, StreamBatchAppend, StreamBatchAppendItem,
    StreamBootstrapPlan, StreamMessageRecord, StreamMetadata, StreamRead, StreamReadObjectSegment,
    StreamReadPlan, StreamReadSegment, StreamStatus, StreamVisibleSnapshot,
};
use crate::response::{StreamErrorCode, StreamResponse};
use crate::snapshot::{StreamSnapshot, StreamSnapshotEntry, StreamSnapshotError};
use crate::validate::{validate_bucket_id, validate_stream_id};

#[derive(Debug, Clone, Default)]
pub struct StreamStateMachine {
    buckets: HashSet<String>,
    streams: HashMap<BucketStreamId, StreamMetadata>,
    hot_buffers: HashMap<BucketStreamId, HotBuffer>,
    cold_chunks: HashMap<BucketStreamId, Vec<ColdChunkRef>>,
    external_segments: HashMap<BucketStreamId, Vec<ObjectPayloadRef>>,
    message_records: HashMap<BucketStreamId, Vec<StreamMessageRecord>>,
    integrities: HashMap<BucketStreamId, StreamIntegrity>,
    visible_snapshots: HashMap<BucketStreamId, StreamVisibleSnapshot>,
    producers: HashMap<BucketStreamId, HashMap<String, ProducerState>>,
    pending_cold_gc: VecDeque<ColdGcEntry>,
    next_cold_gc_seq: u64,
}

#[derive(Debug, Clone, Default)]
struct HotBuffer {
    chunks: VecDeque<HotChunk>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HotChunk {
    start_offset: u64,
    end_offset: u64,
    bytes: Vec<u8>,
}

impl HotBuffer {
    fn from_payload(start_offset: u64, payload: Vec<u8>) -> Self {
        if payload.is_empty() {
            return Self::default();
        }
        let end_offset = start_offset
            .saturating_add(u64::try_from(payload.len()).expect("payload len fits u64"));
        let mut chunks = VecDeque::new();
        chunks.push_back(HotChunk {
            start_offset,
            end_offset,
            bytes: payload,
        });
        Self { chunks }
    }

    fn from_snapshot(payload: Vec<u8>, segments: &[HotPayloadSegment]) -> Self {
        let mut chunks = VecDeque::with_capacity(segments.len());
        for segment in segments {
            chunks.push_back(HotChunk {
                start_offset: segment.start_offset,
                end_offset: segment.end_offset,
                bytes: payload[segment.payload_start..segment.payload_end].to_vec(),
            });
        }
        Self { chunks }
    }

    fn len(&self) -> usize {
        self.chunks.iter().map(|chunk| chunk.bytes.len()).sum()
    }

    fn hot_start_offset(&self) -> u64 {
        self.chunks
            .front()
            .map(|chunk| chunk.start_offset)
            .unwrap_or(0)
    }

    fn payload(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(self.len());
        for chunk in &self.chunks {
            payload.extend_from_slice(&chunk.bytes);
        }
        payload
    }

    fn hot_segments(&self) -> Vec<HotPayloadSegment> {
        let mut payload_start = 0usize;
        self.chunks
            .iter()
            .map(|chunk| {
                let payload_end = payload_start + chunk.bytes.len();
                let segment = HotPayloadSegment {
                    start_offset: chunk.start_offset,
                    end_offset: chunk.end_offset,
                    payload_start,
                    payload_end,
                };
                payload_start = payload_end;
                segment
            })
            .collect()
    }

    fn push(&mut self, start_offset: u64, end_offset: u64, payload: &[u8]) {
        if payload.is_empty() {
            return;
        }
        self.chunks.push_back(HotChunk {
            start_offset,
            end_offset,
            bytes: payload.to_vec(),
        });
    }

    fn plan_cold_flush(
        &self,
        min_hot_bytes: usize,
        max_flush_bytes: usize,
    ) -> Option<(u64, u64, Vec<u8>)> {
        let first = self.chunks.front()?;
        let mut payload = Vec::new();
        let mut end_offset = first.start_offset;
        for chunk in &self.chunks {
            if chunk.start_offset != end_offset || payload.len() >= max_flush_bytes {
                break;
            }
            let remaining = max_flush_bytes - payload.len();
            let take = chunk.bytes.len().min(remaining);
            payload.extend_from_slice(&chunk.bytes[..take]);
            end_offset = end_offset.saturating_add(u64::try_from(take).expect("take fits u64"));
            if take < chunk.bytes.len() {
                break;
            }
        }
        if payload.len() < min_hot_bytes {
            return None;
        }
        Some((first.start_offset, end_offset, payload))
    }

    fn read_segments(&self, offset: u64, next_offset: u64) -> Vec<(u64, StreamReadSegment)> {
        let mut segments = Vec::new();
        for chunk in &self.chunks {
            let start = offset.max(chunk.start_offset);
            let end = next_offset.min(chunk.end_offset);
            if start < end {
                let payload_start =
                    usize::try_from(start - chunk.start_offset).expect("hot start fits usize");
                let payload_end =
                    usize::try_from(end - chunk.start_offset).expect("hot end fits usize");
                segments.push((
                    start,
                    StreamReadSegment::Hot(chunk.bytes[payload_start..payload_end].to_vec()),
                ));
            }
        }
        segments
    }

    fn covers_prefix(&self, start_offset: u64, end_offset: u64) -> bool {
        let Some(first) = self.chunks.front() else {
            return false;
        };
        if first.start_offset != start_offset {
            return false;
        }
        let mut covered_offset = start_offset;
        for chunk in &self.chunks {
            if chunk.start_offset != covered_offset {
                return false;
            }
            if chunk.end_offset >= end_offset {
                return true;
            }
            covered_offset = chunk.end_offset;
        }
        false
    }

    fn flush_prefix(&mut self, end_offset: u64) {
        while self
            .chunks
            .front()
            .is_some_and(|chunk| chunk.end_offset <= end_offset)
        {
            self.chunks.pop_front();
        }
        if let Some(front) = self.chunks.front_mut()
            && front.start_offset < end_offset
        {
            let drain_len =
                usize::try_from(end_offset - front.start_offset).expect("drain len fits usize");
            front.bytes.drain(..drain_len);
            front.start_offset = end_offset;
        }
    }

    fn discard_before(&mut self, retained_offset: u64) {
        self.flush_prefix(retained_offset);
    }
}

impl StreamStateMachine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply(&mut self, command: StreamCommand) -> StreamResponse {
        match command {
            StreamCommand::CreateBucket { bucket_id } => self.create_bucket(bucket_id),
            StreamCommand::DeleteBucket { bucket_id } => self.delete_bucket(&bucket_id),
            StreamCommand::CreateStream {
                stream_id,
                content_type,
                initial_payload,
                close_after,
                stream_seq,
                producer,
                stream_ttl_seconds,
                stream_expires_at_ms,
                forked_from,
                fork_offset,
                now_ms,
            } => self.create_stream(CreateStreamInput {
                stream_id,
                content_type,
                initial_payload,
                close_after,
                stream_seq,
                producer,
                stream_ttl_seconds,
                stream_expires_at_ms,
                forked_from,
                fork_offset,
                now_ms,
            }),
            StreamCommand::CreateExternal {
                stream_id,
                content_type,
                initial_payload,
                close_after,
                stream_seq,
                producer,
                stream_ttl_seconds,
                stream_expires_at_ms,
                forked_from,
                fork_offset,
                now_ms,
            } => self.create_external_stream(CreateExternalStreamInput {
                stream_id,
                content_type,
                initial_payload,
                close_after,
                stream_seq,
                producer,
                stream_ttl_seconds,
                stream_expires_at_ms,
                forked_from,
                fork_offset,
                now_ms,
            }),
            StreamCommand::Append {
                stream_id,
                content_type,
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => self.append_borrowed(AppendStreamInput {
                stream_id,
                content_type: content_type.as_deref(),
                payload: &payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            }),
            StreamCommand::AppendExternal {
                stream_id,
                content_type,
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => self.append_external(AppendExternalInput {
                stream_id,
                content_type: content_type.as_deref(),
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            }),
            StreamCommand::AppendBatch {
                stream_id,
                content_type,
                payloads,
                producer,
                now_ms,
            } => match self.append_batch_borrowed(
                stream_id,
                content_type.as_deref(),
                &payloads.iter().map(Vec::as_slice).collect::<Vec<_>>(),
                producer,
                now_ms,
            ) {
                Ok(batch) => batch
                    .items
                    .last()
                    .map(|item| StreamResponse::Appended {
                        offset: item.offset,
                        next_offset: item.next_offset,
                        closed: item.closed,
                        deduplicated: item.deduplicated,
                        producer: None,
                    })
                    .unwrap_or_else(|| {
                        StreamResponse::error(
                            StreamErrorCode::EmptyAppend,
                            "append batch must contain at least one payload",
                        )
                    }),
                Err(response) => response,
            },
            StreamCommand::PublishSnapshot {
                stream_id,
                snapshot_offset,
                content_type,
                payload,
                now_ms,
            } => self.publish_snapshot(stream_id, snapshot_offset, content_type, payload, now_ms),
            StreamCommand::TouchStreamAccess {
                stream_id,
                now_ms,
                renew_ttl,
            } => self.touch_stream_access(&stream_id, now_ms, renew_ttl),
            StreamCommand::AddForkRef { stream_id, now_ms } => {
                self.add_fork_ref(&stream_id, now_ms)
            }
            StreamCommand::ReleaseForkRef { stream_id } => self.release_fork_ref(&stream_id),
            StreamCommand::FlushCold { stream_id, chunk } => self.flush_cold(stream_id, chunk),
            StreamCommand::Close {
                stream_id,
                stream_seq,
                producer,
                now_ms,
            } => self.close(stream_id, stream_seq, producer, now_ms),
            StreamCommand::DeleteStream { stream_id } => self.delete_stream(&stream_id),
            StreamCommand::AckColdGc { up_to_seq } => self.ack_cold_gc(up_to_seq),
        }
    }

    pub fn head(&self, stream_id: &BucketStreamId) -> Option<&StreamMetadata> {
        self.streams.get(stream_id)
    }

    pub fn head_at(&mut self, stream_id: &BucketStreamId, now_ms: u64) -> Option<&StreamMetadata> {
        self.expire_stream_if_due(stream_id, now_ms);
        self.streams.get(stream_id)
    }

    pub fn access_requires_write(
        &self,
        stream_id: &BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
    ) -> Result<bool, StreamResponse> {
        self.validate_stream_scope(stream_id)?;
        let Some(stream) = self.streams.get(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        if is_soft_deleted(stream) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
        if stream_is_expired(stream, now_ms) {
            return Ok(true);
        }
        Ok(renew_ttl
            && stream.stream_ttl_seconds.is_some()
            && stream.last_ttl_touch_at_ms != now_ms)
    }

    pub fn hot_start_offset(&self, stream_id: &BucketStreamId) -> u64 {
        let Some(stream) = self.streams.get(stream_id) else {
            return 0;
        };
        self.hot_buffers
            .get(stream_id)
            .and_then(|buffer| buffer.chunks.front().map(|chunk| chunk.start_offset))
            .unwrap_or(stream.tail_offset)
    }

    pub fn cold_chunks(&self, stream_id: &BucketStreamId) -> &[ColdChunkRef] {
        self.cold_chunks
            .get(stream_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn external_segments(&self, stream_id: &BucketStreamId) -> &[ObjectPayloadRef] {
        self.external_segments
            .get(stream_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn hot_segments(&self, stream_id: &BucketStreamId) -> Vec<HotPayloadSegment> {
        self.hot_buffers
            .get(stream_id)
            .map(HotBuffer::hot_segments)
            .unwrap_or_default()
    }

    pub fn hot_payload_len(&self, stream_id: &BucketStreamId) -> Result<u64, StreamResponse> {
        let Some(stream) = self.streams.get(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        if is_soft_deleted(stream) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
        let payload = self
            .hot_buffers
            .get(stream_id)
            .expect("hot buffer exists for stream metadata");
        Ok(u64::try_from(payload.len()).expect("payload len fits u64"))
    }

    pub fn total_hot_payload_bytes(&self) -> u64 {
        self.hot_buffers
            .values()
            .map(|payload| u64::try_from(payload.len()).expect("payload len fits u64"))
            .sum()
    }

    pub fn plan_cold_flush(
        &self,
        stream_id: &BucketStreamId,
        min_hot_bytes: usize,
        max_flush_bytes: usize,
    ) -> Result<Option<ColdFlushCandidate>, StreamResponse> {
        if max_flush_bytes == 0 {
            return Ok(None);
        }
        let Some(stream) = self.streams.get(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        if is_soft_deleted(stream) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
        let Some(hot_buffer) = self.hot_buffers.get(stream_id) else {
            return Ok(None);
        };
        let Some((start_offset, end_offset, payload)) =
            hot_buffer.plan_cold_flush(min_hot_bytes, max_flush_bytes)
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

    pub fn plan_next_cold_flush(
        &self,
        min_hot_bytes: usize,
        max_flush_bytes: usize,
    ) -> Result<Option<ColdFlushCandidate>, StreamResponse> {
        if max_flush_bytes == 0 {
            return Ok(None);
        }
        let mut stream_ids = self.streams.keys().cloned().collect::<Vec<_>>();
        stream_ids.sort_by(compare_stream_ids);
        for stream_id in &stream_ids {
            match self.plan_cold_flush(stream_id, min_hot_bytes, max_flush_bytes) {
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
        let group_hot_bytes = self.total_hot_payload_bytes();
        if group_hot_bytes < group_min_hot_bytes {
            return Ok(None);
        }
        for stream_id in stream_ids {
            match self.plan_cold_flush(&stream_id, 1, max_flush_bytes) {
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
        let mut preview = self.clone();
        let mut candidates = Vec::with_capacity(max_candidates);
        while candidates.len() < max_candidates {
            let Some(candidate) = preview.plan_next_cold_flush(min_hot_bytes, max_flush_bytes)?
            else {
                break;
            };
            let chunk = ColdChunkRef {
                start_offset: candidate.start_offset,
                end_offset: candidate.end_offset,
                s3_path: "planned-cold-flush-batch".to_owned(),
                object_size: u64::try_from(candidate.payload.len()).expect("payload len fits u64"),
            };
            match preview.flush_cold(candidate.stream_id.clone(), chunk) {
                StreamResponse::ColdFlushed { .. } => candidates.push(candidate),
                StreamResponse::Error { .. } => break,
                other => {
                    return Err(StreamResponse::error(
                        StreamErrorCode::InvalidColdFlush,
                        format!("unexpected cold flush planning response: {other:?}"),
                    ));
                }
            }
        }
        Ok(candidates)
    }

    pub fn bucket_exists(&self, bucket_id: &str) -> bool {
        self.buckets.contains(bucket_id)
    }

    pub fn integrity_snapshot(
        &self,
        stream_id: &BucketStreamId,
    ) -> Result<crate::integrity::StreamIntegritySnapshot, StreamResponse> {
        let Some(stream) = self.streams.get(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        if is_soft_deleted(stream) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
        Ok(self
            .integrities
            .get(stream_id)
            .expect("integrity exists for stream metadata")
            .snapshot(self.earliest_retained_offset(stream_id), stream.tail_offset))
    }

    pub fn snapshot(&self) -> StreamSnapshot {
        let mut buckets = self.buckets.iter().cloned().collect::<Vec<_>>();
        buckets.sort();

        let mut streams = self
            .streams
            .values()
            .cloned()
            .map(|metadata| {
                let stream_id = metadata.stream_id.clone();
                let tail_offset = metadata.tail_offset;
                let hot_buffer = self
                    .hot_buffers
                    .get(&stream_id)
                    .expect("hot buffer exists for stream metadata");
                let payload = hot_buffer.payload();
                let producer_states = self.producer_snapshot(&stream_id);
                StreamSnapshotEntry {
                    metadata,
                    hot_start_offset: hot_buffer.hot_start_offset(),
                    payload,
                    hot_segments: hot_buffer.hot_segments(),
                    cold_chunks: self
                        .cold_chunks
                        .get(&stream_id)
                        .cloned()
                        .unwrap_or_default(),
                    external_segments: self
                        .external_segments
                        .get(&stream_id)
                        .cloned()
                        .unwrap_or_default(),
                    message_records: self
                        .message_records
                        .get(&stream_id)
                        .cloned()
                        .unwrap_or_default(),
                    integrity: self
                        .integrities
                        .get(&stream_id)
                        .expect("integrity exists for stream metadata")
                        .snapshot(self.earliest_retained_offset(&stream_id), tail_offset),
                    visible_snapshot: self.visible_snapshots.get(&stream_id).cloned(),
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
            pending_cold_gc: self.pending_cold_gc.iter().cloned().collect(),
            next_cold_gc_seq: self.next_cold_gc_seq,
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
            let retained_offset = entry
                .visible_snapshot
                .as_ref()
                .map(|snapshot| snapshot.offset)
                .unwrap_or(0);
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
            if machine
                .streams
                .insert(entry.metadata.stream_id.clone(), entry.metadata)
                .is_some()
            {
                return Err(StreamSnapshotError::DuplicateStream(stream_id));
            }
            let producer_states = restore_producer_states(&stream_id, entry.producer_states)?;
            machine.hot_buffers.insert(
                stream_id.clone(),
                HotBuffer::from_snapshot(entry.payload, &hot_segments),
            );
            if !entry.cold_chunks.is_empty() {
                machine
                    .cold_chunks
                    .insert(stream_id.clone(), entry.cold_chunks);
            }
            if !entry.external_segments.is_empty() {
                machine
                    .external_segments
                    .insert(stream_id.clone(), entry.external_segments);
            }
            if !entry.message_records.is_empty() {
                machine
                    .message_records
                    .insert(stream_id.clone(), entry.message_records);
            }
            machine.integrities.insert(stream_id.clone(), integrity);
            if let Some(snapshot) = entry.visible_snapshot {
                machine
                    .visible_snapshots
                    .insert(stream_id.clone(), snapshot);
            }
            machine.producers.insert(stream_id.clone(), producer_states);
        }

        machine.pending_cold_gc = snapshot.pending_cold_gc.into_iter().collect();
        machine.next_cold_gc_seq = snapshot.next_cold_gc_seq;

        Ok(machine)
    }

    pub fn read(
        &self,
        stream_id: &BucketStreamId,
        offset: u64,
        max_len: usize,
    ) -> Result<StreamRead, StreamResponse> {
        let plan = self.read_plan(stream_id, offset, max_len)?;
        if plan
            .segments
            .iter()
            .any(|segment| matches!(segment, StreamReadSegment::Object(_)))
        {
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
                StreamReadSegment::Object(_) => unreachable!("object segments checked above"),
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
        let Some(stream) = self.streams.get(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        if is_soft_deleted(stream) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
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
        if let Some(hot_buffer) = self.hot_buffers.get(stream_id) {
            segments.extend(hot_buffer.read_segments(offset, next_offset));
        }
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
        let Some(stream) = self.streams.get(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        if is_soft_deleted(stream) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
        Ok(self.visible_snapshots.get(stream_id).cloned())
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

    pub fn bootstrap_plan(
        &self,
        stream_id: &BucketStreamId,
    ) -> Result<StreamBootstrapPlan, StreamResponse> {
        let Some(stream) = self.streams.get(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        if is_soft_deleted(stream) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
        let snapshot = self.visible_snapshots.get(stream_id).cloned();
        let retained_offset = snapshot
            .as_ref()
            .map(|snapshot| snapshot.offset)
            .unwrap_or(0);
        let updates = self
            .message_records
            .get(stream_id)
            .map(|records| {
                records
                    .iter()
                    .filter(|record| record.start_offset >= retained_offset)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Ok(StreamBootstrapPlan {
            snapshot,
            updates,
            next_offset: stream.tail_offset,
            content_type: stream.content_type.clone(),
            up_to_date: true,
            closed: stream.status == StreamStatus::Closed,
        })
    }

    fn publish_snapshot(
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
        let Some(stream) = self.streams.get(&stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        if is_soft_deleted(stream) {
            return StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            );
        }
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

        self.visible_snapshots.insert(
            stream_id.clone(),
            StreamVisibleSnapshot {
                offset: snapshot_offset,
                content_type,
                payload,
            },
        );
        self.compact_retained_prefix(&stream_id, snapshot_offset);
        StreamResponse::SnapshotPublished { snapshot_offset }
    }

    fn flush_cold(&mut self, stream_id: BucketStreamId, chunk: ColdChunkRef) -> StreamResponse {
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
        let Some(stream) = self.streams.get(&stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        if is_soft_deleted(stream) {
            return StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            );
        }
        if chunk.end_offset <= chunk.start_offset {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                "cold chunk must cover at least one byte",
                stream.tail_offset,
            );
        }
        if chunk.end_offset > stream.tail_offset {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                format!(
                    "cold chunk end {} is beyond stream '{}' tail {}",
                    chunk.end_offset, stream_id, stream.tail_offset
                ),
                stream.tail_offset,
            );
        }
        let Some(hot_buffer) = self.hot_buffers.get(&stream_id) else {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                format!("cold chunk for stream '{stream_id}' does not match hot payload"),
                stream.tail_offset,
            );
        };
        if hot_buffer.hot_start_offset() != chunk.start_offset {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                format!("cold chunk for stream '{stream_id}' must start at the hot prefix"),
                stream.tail_offset,
            );
        }
        if !hot_buffer.covers_prefix(chunk.start_offset, chunk.end_offset) {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::InvalidColdFlush,
                format!(
                    "cold chunk for stream '{stream_id}' does not cover contiguous hot payload"
                ),
                stream.tail_offset,
            );
        }
        self.hot_buffers
            .get_mut(&stream_id)
            .expect("hot buffer exists for stream metadata")
            .flush_prefix(chunk.end_offset);
        self.cold_chunks
            .entry(stream_id.clone())
            .or_default()
            .push(chunk.clone());
        StreamResponse::ColdFlushed {
            hot_start_offset: self.hot_start_offset(&stream_id),
        }
    }

    fn create_bucket(&mut self, bucket_id: String) -> StreamResponse {
        if let Err(message) = validate_bucket_id(&bucket_id) {
            return StreamResponse::error(StreamErrorCode::InvalidBucketId, message);
        }
        if !self.buckets.insert(bucket_id.clone()) {
            return StreamResponse::BucketAlreadyExists { bucket_id };
        }
        StreamResponse::BucketCreated { bucket_id }
    }

    fn delete_bucket(&mut self, bucket_id: &str) -> StreamResponse {
        if let Err(message) = validate_bucket_id(bucket_id) {
            return StreamResponse::error(StreamErrorCode::InvalidBucketId, message);
        }
        if !self.buckets.contains(bucket_id) {
            return StreamResponse::error(
                StreamErrorCode::BucketNotFound,
                format!("bucket '{bucket_id}' does not exist"),
            );
        }
        if self
            .streams
            .keys()
            .any(|stream_id| stream_id.bucket_id == bucket_id)
        {
            return StreamResponse::error(
                StreamErrorCode::BucketNotEmpty,
                format!("bucket '{bucket_id}' is not empty"),
            );
        }
        self.buckets.remove(bucket_id);
        StreamResponse::BucketDeleted {
            bucket_id: bucket_id.to_owned(),
        }
    }

    fn create_stream(&mut self, input: CreateStreamInput) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(&input.stream_id) {
            return response;
        }
        if let Err(response) =
            validate_retention(input.stream_ttl_seconds, input.stream_expires_at_ms)
        {
            return response;
        }
        if let Err(response) = validate_producer_request(input.producer.as_ref()) {
            return response;
        }
        if let Some(producer) = input.producer.as_ref()
            && producer.producer_seq != 0
        {
            return StreamResponse::error(
                StreamErrorCode::ProducerSeqConflict,
                format!(
                    "producer '{}' expected sequence 0, received {}",
                    producer.producer_id, producer.producer_seq
                ),
            );
        }
        if self
            .streams
            .get(&input.stream_id)
            .is_some_and(|existing| stream_is_expired(existing, input.now_ms))
        {
            self.remove_stream_state(&input.stream_id);
        }

        if let Some(existing) = self.streams.get(&input.stream_id) {
            if is_soft_deleted(existing) {
                return StreamResponse::error(
                    StreamErrorCode::StreamAlreadyExistsConflict,
                    format!(
                        "stream '{}' is gone and cannot be recreated yet",
                        input.stream_id
                    ),
                );
            }
            if existing.content_type == input.content_type
                && existing.status == status_from_closed(input.close_after)
                && existing.stream_ttl_seconds == input.stream_ttl_seconds
                && existing.stream_expires_at_ms == input.stream_expires_at_ms
                && existing.forked_from == input.forked_from
                && existing.fork_offset == input.fork_offset
            {
                return StreamResponse::AlreadyExists {
                    next_offset: existing.tail_offset,
                    closed: existing.status == StreamStatus::Closed,
                    content_type: existing.content_type.clone(),
                    stream_ttl_seconds: existing.stream_ttl_seconds,
                    stream_expires_at_ms: existing.stream_expires_at_ms,
                };
            }
            return StreamResponse::error(
                StreamErrorCode::StreamAlreadyExistsConflict,
                format!(
                    "stream '{}' already exists with different metadata",
                    input.stream_id
                ),
            );
        }

        let initial_len = input.initial_len();
        let metadata = StreamMetadata {
            stream_id: input.stream_id.clone(),
            content_type: input.content_type,
            status: status_from_closed(input.close_after),
            tail_offset: initial_len,
            last_stream_seq: input.stream_seq,
            stream_ttl_seconds: input.stream_ttl_seconds,
            stream_expires_at_ms: input.stream_expires_at_ms,
            created_at_ms: input.now_ms,
            last_ttl_touch_at_ms: input.now_ms,
            forked_from: input.forked_from,
            fork_offset: input.fork_offset,
            fork_ref_count: 0,
        };
        self.streams.insert(input.stream_id.clone(), metadata);
        self.hot_buffers.insert(
            input.stream_id.clone(),
            HotBuffer::from_payload(0, input.initial_payload),
        );
        let mut integrity = StreamIntegrity::default();
        if initial_len > 0 {
            let payload = self
                .hot_buffers
                .get(&input.stream_id)
                .expect("hot buffer exists for stream metadata")
                .payload();
            integrity.append_payload(&input.stream_id, 0, initial_len, &payload);
        }
        self.integrities.insert(input.stream_id.clone(), integrity);
        if initial_len > 0 {
            self.message_records.insert(
                input.stream_id.clone(),
                vec![StreamMessageRecord {
                    start_offset: 0,
                    end_offset: initial_len,
                }],
            );
        }
        let mut producer_states = HashMap::new();
        if let Some(producer) = input.producer {
            let last_item = ProducerAppendRecord {
                start_offset: 0,
                next_offset: initial_len,
                closed: input.close_after,
            };
            producer_states.insert(
                producer.producer_id,
                ProducerState {
                    producer_epoch: producer.producer_epoch,
                    producer_seq: producer.producer_seq,
                    last_start_offset: last_item.start_offset,
                    last_next_offset: last_item.next_offset,
                    last_closed: last_item.closed,
                    last_items: vec![last_item],
                },
            );
        }
        self.producers
            .insert(input.stream_id.clone(), producer_states);
        StreamResponse::Created {
            stream_id: input.stream_id,
            next_offset: initial_len,
            closed: input.close_after,
        }
    }

    fn create_external_stream(&mut self, input: CreateExternalStreamInput) -> StreamResponse {
        if let Err(response) = validate_external_payload_ref(&input.initial_payload) {
            return response;
        }
        if let Err(response) = self.validate_stream_scope(&input.stream_id) {
            return response;
        }
        if let Err(response) =
            validate_retention(input.stream_ttl_seconds, input.stream_expires_at_ms)
        {
            return response;
        }
        if let Err(response) = validate_producer_request(input.producer.as_ref()) {
            return response;
        }
        if let Some(producer) = input.producer.as_ref()
            && producer.producer_seq != 0
        {
            return StreamResponse::error(
                StreamErrorCode::ProducerSeqConflict,
                format!(
                    "producer '{}' expected sequence 0, received {}",
                    producer.producer_id, producer.producer_seq
                ),
            );
        }
        if self
            .streams
            .get(&input.stream_id)
            .is_some_and(|existing| stream_is_expired(existing, input.now_ms))
        {
            self.remove_stream_state(&input.stream_id);
        }

        if let Some(existing) = self.streams.get(&input.stream_id) {
            if is_soft_deleted(existing) {
                return StreamResponse::error(
                    StreamErrorCode::StreamAlreadyExistsConflict,
                    format!(
                        "stream '{}' is gone and cannot be recreated yet",
                        input.stream_id
                    ),
                );
            }
            if existing.content_type == input.content_type
                && existing.status == status_from_closed(input.close_after)
                && existing.stream_ttl_seconds == input.stream_ttl_seconds
                && existing.stream_expires_at_ms == input.stream_expires_at_ms
                && existing.forked_from == input.forked_from
                && existing.fork_offset == input.fork_offset
            {
                return StreamResponse::AlreadyExists {
                    next_offset: existing.tail_offset,
                    closed: existing.status == StreamStatus::Closed,
                    content_type: existing.content_type.clone(),
                    stream_ttl_seconds: existing.stream_ttl_seconds,
                    stream_expires_at_ms: existing.stream_expires_at_ms,
                };
            }
            return StreamResponse::error(
                StreamErrorCode::StreamAlreadyExistsConflict,
                format!(
                    "stream '{}' already exists with different metadata",
                    input.stream_id
                ),
            );
        }

        let initial_len = input.initial_payload.payload_len;
        let metadata = StreamMetadata {
            stream_id: input.stream_id.clone(),
            content_type: input.content_type,
            status: status_from_closed(input.close_after),
            tail_offset: initial_len,
            last_stream_seq: input.stream_seq,
            stream_ttl_seconds: input.stream_ttl_seconds,
            stream_expires_at_ms: input.stream_expires_at_ms,
            created_at_ms: input.now_ms,
            last_ttl_touch_at_ms: input.now_ms,
            forked_from: input.forked_from,
            fork_offset: input.fork_offset,
            fork_ref_count: 0,
        };
        self.streams.insert(input.stream_id.clone(), metadata);
        self.hot_buffers
            .insert(input.stream_id.clone(), HotBuffer::default());
        self.external_segments.insert(
            input.stream_id.clone(),
            vec![ObjectPayloadRef {
                start_offset: 0,
                end_offset: initial_len,
                s3_path: input.initial_payload.s3_path,
                object_size: input.initial_payload.object_size,
            }],
        );
        let mut integrity = StreamIntegrity::default();
        if initial_len > 0 {
            let object = self
                .external_segments
                .get(&input.stream_id)
                .and_then(|objects| objects.first())
                .expect("external segment exists for stream metadata");
            integrity.append_external(
                &input.stream_id,
                object.start_offset,
                object.end_offset,
                &object.s3_path,
                object.object_size,
            );
        }
        self.integrities.insert(input.stream_id.clone(), integrity);
        self.message_records.insert(
            input.stream_id.clone(),
            vec![StreamMessageRecord {
                start_offset: 0,
                end_offset: initial_len,
            }],
        );
        let mut producer_states = HashMap::new();
        if let Some(producer) = input.producer {
            let last_item = ProducerAppendRecord {
                start_offset: 0,
                next_offset: initial_len,
                closed: input.close_after,
            };
            producer_states.insert(
                producer.producer_id,
                ProducerState {
                    producer_epoch: producer.producer_epoch,
                    producer_seq: producer.producer_seq,
                    last_start_offset: last_item.start_offset,
                    last_next_offset: last_item.next_offset,
                    last_closed: last_item.closed,
                    last_items: vec![last_item],
                },
            );
        }
        self.producers
            .insert(input.stream_id.clone(), producer_states);
        StreamResponse::Created {
            stream_id: input.stream_id,
            next_offset: initial_len,
            closed: input.close_after,
        }
    }

    pub fn append_borrowed(&mut self, input: AppendStreamInput<'_>) -> StreamResponse {
        let AppendStreamInput {
            stream_id,
            content_type,
            payload,
            close_after,
            stream_seq,
            producer,
            now_ms,
        } = input;
        if let Err(response) = self.validate_stream_scope(&stream_id) {
            return response;
        }
        if let Err(response) = validate_producer_request(producer.as_ref()) {
            return response;
        }

        let Some(_) = self.streams.get(&stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        if self.expire_stream_if_due(&stream_id, now_ms) {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        }
        if self.streams.get(&stream_id).is_some_and(is_soft_deleted) {
            return StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            );
        }
        let producer_decision = match self.evaluate_producer(&stream_id, producer.as_ref()) {
            Ok(decision) => decision,
            Err(response) => return response,
        };
        if let ProducerDecision::Duplicate {
            offset,
            next_offset,
            closed,
            producer,
            ..
        } = producer_decision
        {
            if payload.is_empty() {
                return StreamResponse::Closed {
                    next_offset,
                    deduplicated: true,
                    producer: Some(producer),
                };
            }
            return StreamResponse::Appended {
                offset,
                next_offset,
                closed,
                deduplicated: true,
                producer: Some(producer),
            };
        }

        let Some(stream) = self.streams.get_mut(&stream_id) else {
            unreachable!("stream existence checked before producer evaluation");
        };

        if stream.status == StreamStatus::Closed {
            if close_after && payload.is_empty() {
                return StreamResponse::Closed {
                    next_offset: stream.tail_offset,
                    deduplicated: false,
                    producer: None,
                };
            }
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::StreamClosed,
                format!("stream '{stream_id}' is closed"),
                stream.tail_offset,
            );
        }

        if payload.is_empty() && !close_after {
            return StreamResponse::error(
                StreamErrorCode::EmptyAppend,
                "append payload must be non-empty unless closing the stream",
            );
        }

        if !payload.is_empty() {
            let Some(content_type) = content_type else {
                return StreamResponse::error(
                    StreamErrorCode::MissingContentType,
                    "append with a body must include content type",
                );
            };
            if content_type != stream.content_type {
                return StreamResponse::error_with_next_offset(
                    StreamErrorCode::ContentTypeMismatch,
                    format!(
                        "append content type '{content_type}' does not match stream content type '{}'",
                        stream.content_type
                    ),
                    stream.tail_offset,
                );
            }
        }

        if let Err(response) = check_stream_seq(stream, stream_seq.as_deref()) {
            return response;
        }

        let offset = stream.tail_offset;
        let payload_len = u64::try_from(payload.len()).expect("payload len fits u64");
        stream.tail_offset = stream.tail_offset.saturating_add(payload_len);
        if let Some(seq) = stream_seq {
            stream.last_stream_seq = Some(seq);
        }
        renew_stream_ttl(stream, now_ms);
        if close_after {
            stream.status = StreamStatus::Closed;
        }
        let closed = stream.status == StreamStatus::Closed;
        let next_offset = stream.tail_offset;
        let producer_ack = producer.clone();
        if let Some(producer) = producer {
            self.record_producer_success(
                stream_id.clone(),
                producer,
                ProducerAppendRecord {
                    start_offset: offset,
                    next_offset,
                    closed,
                },
                vec![ProducerAppendRecord {
                    start_offset: offset,
                    next_offset,
                    closed,
                }],
            );
        }

        if payload.is_empty() {
            StreamResponse::Closed {
                next_offset,
                deduplicated: false,
                producer: producer_ack,
            }
        } else {
            self.hot_buffers
                .get_mut(&stream_id)
                .expect("hot buffer exists for stream metadata")
                .push(offset, next_offset, payload);
            self.integrities
                .get_mut(&stream_id)
                .expect("integrity exists for stream metadata")
                .append_payload(&stream_id, offset, next_offset, payload);
            self.message_records
                .entry(stream_id.clone())
                .or_default()
                .push(StreamMessageRecord {
                    start_offset: offset,
                    end_offset: next_offset,
                });
            StreamResponse::Appended {
                offset,
                next_offset,
                closed: close_after,
                deduplicated: false,
                producer: producer_ack,
            }
        }
    }

    fn append_external(&mut self, input: AppendExternalInput<'_>) -> StreamResponse {
        let AppendExternalInput {
            stream_id,
            content_type,
            payload,
            close_after,
            stream_seq,
            producer,
            now_ms,
        } = input;
        if let Err(response) = validate_external_payload_ref(&payload) {
            return response;
        }
        if let Err(response) = self.validate_stream_scope(&stream_id) {
            return response;
        }
        if let Err(response) = validate_producer_request(producer.as_ref()) {
            return response;
        }
        let Some(_) = self.streams.get(&stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        if self.expire_stream_if_due(&stream_id, now_ms) {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        }
        if self.streams.get(&stream_id).is_some_and(is_soft_deleted) {
            return StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            );
        }
        let producer_decision = match self.evaluate_producer(&stream_id, producer.as_ref()) {
            Ok(decision) => decision,
            Err(response) => return response,
        };
        if let ProducerDecision::Duplicate {
            offset,
            next_offset,
            closed,
            producer,
            ..
        } = producer_decision
        {
            return StreamResponse::Appended {
                offset,
                next_offset,
                closed,
                deduplicated: true,
                producer: Some(producer),
            };
        }

        let Some(stream) = self.streams.get(&stream_id) else {
            unreachable!("stream existence checked before producer evaluation");
        };
        if stream.status == StreamStatus::Closed {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::StreamClosed,
                format!("stream '{stream_id}' is closed"),
                stream.tail_offset,
            );
        }
        let Some(content_type) = content_type else {
            return StreamResponse::error(
                StreamErrorCode::MissingContentType,
                "append with a body must include content type",
            );
        };
        if content_type != stream.content_type {
            return StreamResponse::error_with_next_offset(
                StreamErrorCode::ContentTypeMismatch,
                format!(
                    "append content type '{content_type}' does not match stream content type '{}'",
                    stream.content_type
                ),
                stream.tail_offset,
            );
        }
        if let Err(response) = check_stream_seq(stream, stream_seq.as_deref()) {
            return response;
        }
        let offset = stream.tail_offset;
        let next_offset = offset.saturating_add(payload.payload_len);
        let stream = self
            .streams
            .get_mut(&stream_id)
            .expect("stream existence checked before external append mutation");
        stream.tail_offset = next_offset;
        if let Some(seq) = stream_seq {
            stream.last_stream_seq = Some(seq);
        }
        renew_stream_ttl(stream, now_ms);
        if close_after {
            stream.status = StreamStatus::Closed;
        }
        let closed = stream.status == StreamStatus::Closed;
        let producer_ack = producer.clone();
        if let Some(producer) = producer {
            self.record_producer_success(
                stream_id.clone(),
                producer,
                ProducerAppendRecord {
                    start_offset: offset,
                    next_offset,
                    closed,
                },
                vec![ProducerAppendRecord {
                    start_offset: offset,
                    next_offset,
                    closed,
                }],
            );
        }
        self.external_segments
            .entry(stream_id.clone())
            .or_default()
            .push(ObjectPayloadRef {
                start_offset: offset,
                end_offset: next_offset,
                s3_path: payload.s3_path,
                object_size: payload.object_size,
            });
        let object = self
            .external_segments
            .get(&stream_id)
            .and_then(|segments| segments.last())
            .expect("external segment exists for stream metadata");
        self.integrities
            .get_mut(&stream_id)
            .expect("integrity exists for stream metadata")
            .append_external(
                &stream_id,
                object.start_offset,
                object.end_offset,
                &object.s3_path,
                object.object_size,
            );
        self.message_records
            .entry(stream_id.clone())
            .or_default()
            .push(StreamMessageRecord {
                start_offset: offset,
                end_offset: next_offset,
            });
        StreamResponse::Appended {
            offset,
            next_offset,
            closed: close_after,
            deduplicated: false,
            producer: producer_ack,
        }
    }

    pub fn append_batch_borrowed(
        &mut self,
        stream_id: BucketStreamId,
        content_type: Option<&str>,
        payloads: &[&[u8]],
        producer: Option<ProducerRequest>,
        now_ms: u64,
    ) -> Result<StreamBatchAppend, StreamResponse> {
        if payloads.is_empty() {
            return Err(StreamResponse::error(
                StreamErrorCode::EmptyAppend,
                "append batch must contain at least one payload",
            ));
        }
        self.validate_stream_scope(&stream_id)?;
        validate_producer_request(producer.as_ref())?;
        if self.expire_stream_if_due(&stream_id, now_ms) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        }
        if self.streams.get(&stream_id).is_some_and(is_soft_deleted) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
        let producer_decision = self.evaluate_producer(&stream_id, producer.as_ref())?;
        if let ProducerDecision::Duplicate { items, .. } = producer_decision {
            return Ok(StreamBatchAppend {
                items: items
                    .into_iter()
                    .map(|item| StreamBatchAppendItem {
                        offset: item.start_offset,
                        next_offset: item.next_offset,
                        closed: item.closed,
                        deduplicated: true,
                    })
                    .collect(),
                deduplicated: true,
            });
        }

        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        if stream.status == StreamStatus::Closed {
            return Err(StreamResponse::error_with_next_offset(
                StreamErrorCode::StreamClosed,
                format!("stream '{stream_id}' is closed"),
                stream.tail_offset,
            ));
        }
        let Some(content_type) = content_type else {
            return Err(StreamResponse::error(
                StreamErrorCode::MissingContentType,
                "append batch must include content type",
            ));
        };
        if content_type != stream.content_type {
            return Err(StreamResponse::error_with_next_offset(
                StreamErrorCode::ContentTypeMismatch,
                format!(
                    "append content type '{content_type}' does not match stream content type '{}'",
                    stream.content_type
                ),
                stream.tail_offset,
            ));
        }
        if payloads.iter().any(|payload| payload.is_empty()) {
            return Err(StreamResponse::error(
                StreamErrorCode::EmptyAppend,
                "append batch payloads must be non-empty",
            ));
        }

        let mut items = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let offset = stream.tail_offset;
            let payload_len = u64::try_from(payload.len()).expect("payload len fits u64");
            stream.tail_offset = stream.tail_offset.saturating_add(payload_len);
            items.push(ProducerAppendRecord {
                start_offset: offset,
                next_offset: stream.tail_offset,
                closed: false,
            });
        }
        let last = items
            .last()
            .expect("payloads checked non-empty before append")
            .clone();
        renew_stream_ttl(stream, now_ms);
        if let Some(producer) = producer {
            self.record_producer_success(stream_id.clone(), producer, last.clone(), items.clone());
        }
        let hot_buffer = self
            .hot_buffers
            .get_mut(&stream_id)
            .expect("hot buffer exists for stream metadata");
        for (item, payload) in items.iter().zip(payloads.iter()) {
            hot_buffer.push(item.start_offset, item.next_offset, payload);
        }
        let integrity = self
            .integrities
            .get_mut(&stream_id)
            .expect("integrity exists for stream metadata");
        for (item, payload) in items.iter().zip(payloads.iter()) {
            integrity.append_payload(&stream_id, item.start_offset, item.next_offset, payload);
        }
        self.message_records
            .entry(stream_id.clone())
            .or_default()
            .extend(items.iter().map(|item| StreamMessageRecord {
                start_offset: item.start_offset,
                end_offset: item.next_offset,
            }));
        Ok(StreamBatchAppend {
            items: items
                .into_iter()
                .map(|item| StreamBatchAppendItem {
                    offset: item.start_offset,
                    next_offset: item.next_offset,
                    closed: item.closed,
                    deduplicated: false,
                })
                .collect(),
            deduplicated: false,
        })
    }

    fn close(
        &mut self,
        stream_id: BucketStreamId,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    ) -> StreamResponse {
        self.append_borrowed(AppendStreamInput {
            stream_id,
            content_type: None,
            payload: &[],
            close_after: true,
            stream_seq,
            producer,
            now_ms,
        })
    }

    fn delete_stream(&mut self, stream_id: &BucketStreamId) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(stream_id) {
            return response;
        }
        let Some(stream) = self.streams.get_mut(stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        if is_soft_deleted(stream) {
            return StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            );
        }
        if stream.fork_ref_count > 0 {
            stream.status = StreamStatus::SoftDeleted;
            return StreamResponse::Deleted {
                hard_deleted: false,
                parent_to_release: None,
            };
        }
        let parent_to_release = stream.forked_from.clone();
        self.remove_stream_state(stream_id);
        StreamResponse::Deleted {
            hard_deleted: true,
            parent_to_release,
        }
    }

    fn add_fork_ref(&mut self, stream_id: &BucketStreamId, now_ms: u64) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(stream_id) {
            return response;
        }
        if self.expire_stream_if_due(stream_id, now_ms) {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        }
        let Some(stream) = self.streams.get_mut(stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        if is_soft_deleted(stream) {
            return StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            );
        }
        stream.fork_ref_count = stream.fork_ref_count.saturating_add(1);
        StreamResponse::ForkRefAdded {
            fork_ref_count: stream.fork_ref_count,
        }
    }

    fn release_fork_ref(&mut self, stream_id: &BucketStreamId) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(stream_id) {
            return response;
        }
        let Some(stream) = self.streams.get_mut(stream_id) else {
            return StreamResponse::ForkRefReleased {
                hard_deleted: false,
                fork_ref_count: 0,
                parent_to_release: None,
            };
        };
        if stream.fork_ref_count == 0 {
            return StreamResponse::error(
                StreamErrorCode::InvalidFork,
                format!("stream '{stream_id}' has no fork reference to release"),
            );
        }
        stream.fork_ref_count -= 1;
        if stream.fork_ref_count == 0 && is_soft_deleted(stream) {
            let parent_to_release = stream.forked_from.clone();
            self.remove_stream_state(stream_id);
            return StreamResponse::ForkRefReleased {
                hard_deleted: true,
                fork_ref_count: 0,
                parent_to_release,
            };
        }
        StreamResponse::ForkRefReleased {
            hard_deleted: false,
            fork_ref_count: stream.fork_ref_count,
            parent_to_release: None,
        }
    }

    fn touch_stream_access(
        &mut self,
        stream_id: &BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
    ) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(stream_id) {
            return response;
        }
        let Some(stream) = self.streams.get(stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        if is_soft_deleted(stream) {
            return StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            );
        }
        if stream_is_expired(stream, now_ms) {
            self.remove_stream_state(stream_id);
            return StreamResponse::Accessed {
                changed: true,
                expired: true,
            };
        }
        let changed = if renew_ttl && stream.stream_ttl_seconds.is_some() {
            let stream = self
                .streams
                .get_mut(stream_id)
                .expect("stream existence checked before TTL renewal");
            let previous = stream.last_ttl_touch_at_ms;
            renew_stream_ttl(stream, now_ms);
            stream.last_ttl_touch_at_ms != previous
        } else {
            false
        };
        StreamResponse::Accessed {
            changed,
            expired: false,
        }
    }

    fn expire_stream_if_due(&mut self, stream_id: &BucketStreamId, now_ms: u64) -> bool {
        if self
            .streams
            .get(stream_id)
            .is_some_and(|stream| stream_is_expired(stream, now_ms))
        {
            self.remove_stream_state(stream_id);
            return true;
        }
        false
    }

    fn remove_stream_state(&mut self, stream_id: &BucketStreamId) -> bool {
        if self.streams.remove(stream_id).is_some() {
            self.hot_buffers.remove(stream_id);
            let had_cold = self
                .cold_chunks
                .remove(stream_id)
                .is_some_and(|chunks| !chunks.is_empty());
            self.external_segments.remove(stream_id);
            self.message_records.remove(stream_id);
            self.integrities.remove(stream_id);
            self.visible_snapshots.remove(stream_id);
            self.producers.remove(stream_id);
            // The cold objects we wrote for this stream are now unreferenced.
            // Enqueue the whole prefix for the background GC worker to reclaim;
            // cold objects are stream-exclusive (forks copy, never share), so a
            // prefix sweep is safe and keeps the queue O(streams) not O(chunks).
            if had_cold {
                self.enqueue_cold_gc(ColdGcTarget::Stream(stream_id.clone()));
            }
            true
        } else {
            false
        }
    }

    fn enqueue_cold_gc(&mut self, target: ColdGcTarget) {
        let seq = self.next_cold_gc_seq;
        self.next_cold_gc_seq = self.next_cold_gc_seq.saturating_add(1);
        self.pending_cold_gc.push_back(ColdGcEntry { seq, target });
    }

    fn ack_cold_gc(&mut self, up_to_seq: u64) -> StreamResponse {
        let before = self.pending_cold_gc.len();
        while self
            .pending_cold_gc
            .front()
            .is_some_and(|entry| entry.seq <= up_to_seq)
        {
            self.pending_cold_gc.pop_front();
        }
        let removed = u64::try_from(before - self.pending_cold_gc.len()).expect("removed fits u64");
        StreamResponse::ColdGcAcked { removed }
    }

    /// A bounded snapshot of the front of the GC queue for the leader's worker
    /// to reclaim. Read-only; draining is confirmed by a replicated `AckColdGc`.
    pub fn pending_cold_gc_batch(&self, max: usize) -> Vec<ColdGcEntry> {
        self.pending_cold_gc.iter().take(max).cloned().collect()
    }

    pub fn pending_cold_gc_len(&self) -> usize {
        self.pending_cold_gc.len()
    }

    fn validate_stream_scope(&self, stream_id: &BucketStreamId) -> Result<(), StreamResponse> {
        if let Err(message) = validate_bucket_id(&stream_id.bucket_id) {
            return Err(StreamResponse::error(
                StreamErrorCode::InvalidBucketId,
                message,
            ));
        }
        if let Err(message) = validate_stream_id(stream_id) {
            return Err(StreamResponse::error(
                StreamErrorCode::InvalidStreamId,
                message,
            ));
        }
        if !self.buckets.contains(&stream_id.bucket_id) {
            return Err(StreamResponse::error(
                StreamErrorCode::BucketNotFound,
                format!("bucket '{}' does not exist", stream_id.bucket_id),
            ));
        }
        Ok(())
    }

    fn earliest_retained_offset(&self, stream_id: &BucketStreamId) -> u64 {
        self.visible_snapshots
            .get(stream_id)
            .map(|snapshot| snapshot.offset)
            .unwrap_or(0)
    }

    fn snapshot_offset_aligned(
        &self,
        stream_id: &BucketStreamId,
        snapshot_offset: u64,
        retained_offset: u64,
    ) -> bool {
        snapshot_offset == retained_offset
            || self.message_records.get(stream_id).is_some_and(|records| {
                records
                    .iter()
                    .any(|record| record.end_offset == snapshot_offset)
            })
    }

    fn compact_retained_prefix(&mut self, stream_id: &BucketStreamId, retained_offset: u64) {
        if let Some(records) = self.message_records.get_mut(stream_id) {
            records.retain(|record| record.end_offset > retained_offset);
            if records.is_empty() {
                self.message_records.remove(stream_id);
            }
        }
        if let Some(integrity) = self.integrities.get_mut(stream_id) {
            integrity.evict_before(retained_offset);
        }
        let mut dropped_cold_paths = Vec::new();
        if let Some(chunks) = self.cold_chunks.get_mut(stream_id) {
            chunks.retain(|chunk| {
                let retain = chunk.end_offset > retained_offset;
                if !retain {
                    dropped_cold_paths.push(chunk.s3_path.clone());
                }
                retain
            });
            if chunks.is_empty() {
                self.cold_chunks.remove(stream_id);
            }
        }
        if !dropped_cold_paths.is_empty() {
            self.enqueue_cold_gc(ColdGcTarget::Paths(dropped_cold_paths));
        }
        if let Some(objects) = self.external_segments.get_mut(stream_id) {
            objects.retain(|object| object.end_offset > retained_offset);
            if objects.is_empty() {
                self.external_segments.remove(stream_id);
            }
        }

        if let Some(hot_buffer) = self.hot_buffers.get_mut(stream_id) {
            hot_buffer.discard_before(retained_offset);
        }
    }

    fn producer_snapshot(&self, stream_id: &BucketStreamId) -> Vec<ProducerSnapshot> {
        let mut producer_states = self
            .producers
            .get(stream_id)
            .into_iter()
            .flat_map(|states| states.iter())
            .map(|(producer_id, state)| ProducerSnapshot {
                producer_id: producer_id.clone(),
                producer_epoch: state.producer_epoch,
                producer_seq: state.producer_seq,
                last_start_offset: state.last_start_offset,
                last_next_offset: state.last_next_offset,
                last_closed: state.last_closed,
                last_items: state.last_items.clone(),
            })
            .collect::<Vec<_>>();
        producer_states.sort_by(|left, right| left.producer_id.cmp(&right.producer_id));
        producer_states
    }

    fn evaluate_producer(
        &self,
        stream_id: &BucketStreamId,
        producer: Option<&ProducerRequest>,
    ) -> Result<ProducerDecision, StreamResponse> {
        let Some(producer) = producer else {
            return Ok(ProducerDecision::Accept);
        };
        let Some(states) = self.producers.get(stream_id) else {
            return Ok(ProducerDecision::Accept);
        };
        let Some(state) = states.get(&producer.producer_id) else {
            if producer.producer_seq == 0 {
                return Ok(ProducerDecision::Accept);
            }
            return Err(StreamResponse::error(
                StreamErrorCode::ProducerSeqConflict,
                format!(
                    "producer '{}' expected sequence 0, received {}",
                    producer.producer_id, producer.producer_seq
                ),
            ));
        };

        if producer.producer_epoch < state.producer_epoch {
            return Err(StreamResponse::error(
                StreamErrorCode::ProducerEpochStale,
                format!(
                    "producer '{}' epoch {} is stale; current epoch is {}",
                    producer.producer_id, producer.producer_epoch, state.producer_epoch
                ),
            ));
        }
        if producer.producer_epoch > state.producer_epoch {
            if producer.producer_seq == 0 {
                return Ok(ProducerDecision::Accept);
            }
            return Err(StreamResponse::error(
                StreamErrorCode::InvalidProducer,
                format!(
                    "producer '{}' new epoch {} must start at sequence 0",
                    producer.producer_id, producer.producer_epoch
                ),
            ));
        }

        if producer.producer_seq <= state.producer_seq {
            return Ok(ProducerDecision::Duplicate {
                offset: state.last_start_offset,
                next_offset: state.last_next_offset,
                closed: state.last_closed,
                producer: ProducerRequest {
                    producer_id: producer.producer_id.clone(),
                    producer_epoch: state.producer_epoch,
                    producer_seq: state.producer_seq,
                },
                items: state.last_items.clone(),
            });
        }
        if producer.producer_seq == state.producer_seq + 1 {
            return Ok(ProducerDecision::Accept);
        }
        Err(StreamResponse::error(
            StreamErrorCode::ProducerSeqConflict,
            format!(
                "producer '{}' expected sequence {}, received {}",
                producer.producer_id,
                state.producer_seq + 1,
                producer.producer_seq
            ),
        ))
    }

    fn record_producer_success(
        &mut self,
        stream_id: BucketStreamId,
        producer: ProducerRequest,
        last: ProducerAppendRecord,
        last_items: Vec<ProducerAppendRecord>,
    ) {
        self.producers.entry(stream_id).or_default().insert(
            producer.producer_id,
            ProducerState {
                producer_epoch: producer.producer_epoch,
                producer_seq: producer.producer_seq,
                last_start_offset: last.start_offset,
                last_next_offset: last.next_offset,
                last_closed: last.closed,
                last_items,
            },
        );
    }
}

#[derive(Debug)]
struct CreateStreamInput {
    stream_id: BucketStreamId,
    content_type: String,
    initial_payload: Vec<u8>,
    close_after: bool,
    stream_seq: Option<String>,
    producer: Option<ProducerRequest>,
    stream_ttl_seconds: Option<u64>,
    stream_expires_at_ms: Option<u64>,
    forked_from: Option<BucketStreamId>,
    fork_offset: Option<u64>,
    now_ms: u64,
}

#[derive(Debug)]
struct CreateExternalStreamInput {
    stream_id: BucketStreamId,
    content_type: String,
    initial_payload: ExternalPayloadRef,
    close_after: bool,
    stream_seq: Option<String>,
    producer: Option<ProducerRequest>,
    stream_ttl_seconds: Option<u64>,
    stream_expires_at_ms: Option<u64>,
    forked_from: Option<BucketStreamId>,
    fork_offset: Option<u64>,
    now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProducerDecision {
    Accept,
    Duplicate {
        offset: u64,
        next_offset: u64,
        closed: bool,
        producer: ProducerRequest,
        items: Vec<ProducerAppendRecord>,
    },
}

impl CreateStreamInput {
    fn initial_len(&self) -> u64 {
        u64::try_from(self.initial_payload.len()).expect("payload len fits u64")
    }
}

fn status_from_closed(closed: bool) -> StreamStatus {
    if closed {
        StreamStatus::Closed
    } else {
        StreamStatus::Open
    }
}

fn is_soft_deleted(stream: &StreamMetadata) -> bool {
    stream.status == StreamStatus::SoftDeleted
}

fn validate_retention(
    stream_ttl_seconds: Option<u64>,
    stream_expires_at_ms: Option<u64>,
) -> Result<(), StreamResponse> {
    if stream_ttl_seconds.is_some() && stream_expires_at_ms.is_some() {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidRetention,
            "stream ttl and expires-at cannot both be set",
        ));
    }
    if let Some(ttl_seconds) = stream_ttl_seconds
        && ttl_seconds.checked_mul(1000).is_none()
    {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidRetention,
            "stream ttl overflows millisecond range",
        ));
    }
    Ok(())
}

fn stream_expiry_at_ms(stream: &StreamMetadata) -> Option<u64> {
    if let Some(expires_at_ms) = stream.stream_expires_at_ms {
        return Some(expires_at_ms);
    }
    stream.stream_ttl_seconds.map(|ttl_seconds| {
        stream
            .last_ttl_touch_at_ms
            .saturating_add(ttl_seconds.saturating_mul(1000))
    })
}

fn stream_is_expired(stream: &StreamMetadata, now_ms: u64) -> bool {
    stream_expiry_at_ms(stream).is_some_and(|expires_at_ms| now_ms >= expires_at_ms)
}

fn renew_stream_ttl(stream: &mut StreamMetadata, now_ms: u64) {
    if stream.stream_ttl_seconds.is_some() && stream.stream_expires_at_ms.is_none() {
        stream.last_ttl_touch_at_ms = now_ms;
    }
}

fn check_stream_seq(stream: &StreamMetadata, incoming: Option<&str>) -> Result<(), StreamResponse> {
    let Some(incoming) = incoming else {
        return Ok(());
    };
    if let Some(last) = stream.last_stream_seq.as_deref()
        && incoming <= last
    {
        return Err(StreamResponse::error_with_next_offset(
            StreamErrorCode::StreamSeqConflict,
            format!("stream sequence '{incoming}' is not greater than last sequence '{last}'"),
            stream.tail_offset,
        ));
    }
    Ok(())
}

fn validate_producer_request(producer: Option<&ProducerRequest>) -> Result<(), StreamResponse> {
    let Some(producer) = producer else {
        return Ok(());
    };
    if producer.producer_id.trim().is_empty() {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidProducer,
            "producer id must not be empty",
        ));
    }
    const MAX_JS_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
    if producer.producer_epoch > MAX_JS_SAFE_INTEGER {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidProducer,
            format!(
                "producer epoch {} exceeds maximum {}",
                producer.producer_epoch, MAX_JS_SAFE_INTEGER
            ),
        ));
    }
    if producer.producer_seq > MAX_JS_SAFE_INTEGER {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidProducer,
            format!(
                "producer sequence {} exceeds maximum {}",
                producer.producer_seq, MAX_JS_SAFE_INTEGER
            ),
        ));
    }
    Ok(())
}

fn validate_external_payload_ref(payload: &ExternalPayloadRef) -> Result<(), StreamResponse> {
    if payload.s3_path.trim().is_empty() {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidColdFlush,
            "external payload S3 path must not be empty",
        ));
    }
    if payload.payload_len == 0 {
        return Err(StreamResponse::error(
            StreamErrorCode::EmptyAppend,
            "external payload length must be greater than zero",
        ));
    }
    if payload.object_size < payload.payload_len {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidColdFlush,
            "external payload object size must cover payload length",
        ));
    }
    Ok(())
}

fn restore_producer_states(
    stream_id: &BucketStreamId,
    snapshots: Vec<ProducerSnapshot>,
) -> Result<HashMap<String, ProducerState>, StreamSnapshotError> {
    let mut states = HashMap::with_capacity(snapshots.len());
    for snapshot in snapshots {
        if states
            .insert(
                snapshot.producer_id.clone(),
                ProducerState {
                    producer_epoch: snapshot.producer_epoch,
                    producer_seq: snapshot.producer_seq,
                    last_start_offset: snapshot.last_start_offset,
                    last_next_offset: snapshot.last_next_offset,
                    last_closed: snapshot.last_closed,
                    last_items: snapshot.last_items,
                },
            )
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
        Vec::with_capacity(cold_chunks.len() + external_segments.len() + hot_segments.len());
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
        StreamReadSegment::Hot(payload) => {
            if payload.is_empty() {
                return None;
            }
            let len = u64::try_from(payload.len()).ok()?;
            segment_start.checked_add(len)
        }
    }
}

fn message_records_cover_retained_suffix(
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

fn compare_stream_ids(left: &BucketStreamId, right: &BucketStreamId) -> std::cmp::Ordering {
    left.bucket_id
        .cmp(&right.bucket_id)
        .then_with(|| left.stream_id.cmp(&right.stream_id))
}

#[cfg(test)]
mod tests;
