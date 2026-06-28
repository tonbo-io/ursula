use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use slotmap::Key;
use slotmap::SlotMap;
use slotmap::new_key_type;
use ursula_shard::BucketStreamId;

use crate::command::StreamCommand;
use crate::integrity::StreamIntegrity;
use crate::model::AppendExternalInput;
use crate::model::AppendStreamInput;
use crate::model::COLD_INDEX_PAGE_SPAN_BYTES;
use crate::model::ColdChunkRef;
use crate::model::ColdFlushCandidate;
use crate::model::ColdGcEntry;
use crate::model::ColdGcTarget;
use crate::model::ExternalPayloadRef;
use crate::model::HotPayloadSegment;
use crate::model::MAX_STREAM_ATTRS_BYTES;
use crate::model::ObjectPayloadRef;
use crate::model::ProducerAppendRecord;
use crate::model::ProducerRequest;
use crate::model::ProducerSnapshot;
use crate::model::ProducerState;
use crate::model::StreamAttrs;
use crate::model::StreamBatchAppend;
use crate::model::StreamBatchAppendItem;
use crate::model::StreamBootstrapPlan;
use crate::model::StreamMessageRecord;
use crate::model::StreamMetadata;
use crate::model::StreamRead;
use crate::model::StreamReadColdIndexSegment;
use crate::model::StreamReadObjectSegment;
use crate::model::StreamReadPlan;
use crate::model::StreamReadSegment;
use crate::model::StreamStatus;
use crate::model::StreamVisibleSnapshot;
use crate::response::StreamErrorCode;
use crate::response::StreamErrorContext;
use crate::response::StreamResponse;
use crate::snapshot::StreamSnapshot;
use crate::snapshot::StreamSnapshotEntry;
use crate::snapshot::StreamSnapshotError;
use crate::validate::validate_bucket_id;
use crate::validate::validate_stream_id;

const TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE: usize = 256;

new_key_type! {
    struct StreamKey;
}

#[derive(Debug, Clone, Default)]
pub struct StreamStateMachine {
    buckets: HashSet<String>,
    stream_keys: HashMap<BucketStreamId, StreamKey>,
    streams: SlotMap<StreamKey, StreamSlot>,
    ttl_index: TtlIndex,
    pending_cold_gc: VecDeque<ColdGcEntry>,
    next_cold_gc_seq: u64,
}

#[derive(Debug, Clone)]
struct StreamSlot {
    metadata: StreamMetadata,
    attrs: Option<StreamAttrs>,
    hot_buffer: HotBuffer,
    cold: StreamColdState,
    message_records: Vec<StreamMessageRecord>,
    integrity: StreamIntegrity,
    visible_snapshot: Option<StreamVisibleSnapshot>,
    producers: HashMap<String, ProducerState>,
}

#[derive(Debug, Clone, Default)]
struct StreamColdState {
    cold_chunks: Vec<ColdChunkRef>,
    external_segments: Vec<ObjectPayloadRef>,
    cold_frontier: u64,
}

impl StreamColdState {
    fn cold_chunks(&self) -> &[ColdChunkRef] {
        &self.cold_chunks
    }

    fn external_segments(&self) -> &[ObjectPayloadRef] {
        &self.external_segments
    }

    fn cold_generation(&self) -> u64 {
        0
    }

    fn push_cold_chunk(&mut self, chunk: ColdChunkRef) {
        self.cold_frontier = chunk.end_offset;
    }

    fn push_external_segment(&mut self, object: ObjectPayloadRef) {
        let frontier = self.cold_frontier.max(object.end_offset);
        self.cold_frontier = frontier;
    }

    fn restore(
        cold_frontier_offset: u64,
        _cold_index_generation: u64,
        cold_chunks: Vec<ColdChunkRef>,
        external_segments: Vec<ObjectPayloadRef>,
    ) -> Self {
        Self {
            cold_chunks,
            external_segments,
            cold_frontier: cold_frontier_offset,
        }
    }

    fn has_cold_objects(&self) -> bool {
        self.cold_frontier > 0 || !self.cold_chunks.is_empty() || !self.external_segments.is_empty()
    }

    fn compact_before(&mut self, retained_offset: u64) -> Vec<String> {
        let mut dropped_cold_paths = Vec::new();
        self.cold_chunks.retain(|chunk| {
            let retain = chunk.end_offset > retained_offset;
            if !retain {
                dropped_cold_paths.push(chunk.s3_path.clone());
            }
            retain
        });
        self.external_segments
            .retain(|object| object.end_offset > retained_offset);
        dropped_cold_paths
    }

    fn cold_frontier_offset(&self, retained_offset: u64) -> u64 {
        let external_segments = self.external_segments();
        let cold_frontier = self.cold_frontier.max(retained_offset);
        let mut ranges = Vec::with_capacity(1 + external_segments.len());
        if cold_frontier > retained_offset {
            ranges.push((retained_offset, cold_frontier));
        }
        ranges.extend(
            external_segments
                .iter()
                .map(|object| (object.start_offset, object.end_offset)),
        );
        ranges.sort_unstable();

        let mut frontier = retained_offset;
        for (start_offset, end_offset) in ranges {
            if end_offset <= frontier {
                continue;
            }
            if start_offset > frontier {
                break;
            }
            frontier = end_offset;
        }
        frontier
    }
}

#[derive(Debug, Clone, Default)]
struct TtlIndex {
    entries: BinaryHeap<Reverse<TtlEntry>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TtlEntry {
    expires_at_ms: u64,
    stream_id: BucketStreamId,
    key: StreamKey,
}

impl Ord for TtlEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.expires_at_ms
            .cmp(&other.expires_at_ms)
            .then_with(|| compare_stream_ids(&self.stream_id, &other.stream_id))
            .then_with(|| self.key.data().as_ffi().cmp(&other.key.data().as_ffi()))
    }
}

impl PartialOrd for TtlEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
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

    fn plan_cold_flush_from(
        &self,
        from_offset: u64,
        min_hot_bytes: usize,
        max_flush_bytes: usize,
    ) -> Option<(u64, u64, Vec<u8>)> {
        let mut payload = Vec::new();
        for chunk in &self.chunks {
            if chunk.end_offset <= from_offset {
                continue;
            }
            if chunk.start_offset
                > from_offset + u64::try_from(payload.len()).expect("payload len fits u64")
            {
                break;
            }
            if payload.len() >= max_flush_bytes {
                break;
            }
            let skip = if chunk.start_offset < from_offset {
                usize::try_from(from_offset - chunk.start_offset).expect("skip fits usize")
            } else {
                0
            };
            let remaining = max_flush_bytes - payload.len();
            let take = (chunk.bytes.len() - skip).min(remaining);
            payload.extend_from_slice(&chunk.bytes[skip..skip + take]);
            if take < chunk.bytes.len() - skip {
                break;
            }
        }
        if payload.len() < min_hot_bytes {
            return None;
        }
        let end_offset = from_offset + u64::try_from(payload.len()).expect("payload len fits u64");
        Some((from_offset, end_offset, payload))
    }

    fn remaining_len_from(&self, from_offset: u64) -> usize {
        self.chunks
            .iter()
            .filter(|chunk| chunk.end_offset > from_offset)
            .map(|chunk| {
                let start = chunk.start_offset.max(from_offset);
                usize::try_from(chunk.end_offset - start).expect("remaining len fits usize")
            })
            .sum()
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

    fn stream_key(&self, stream_id: &BucketStreamId) -> Option<StreamKey> {
        self.stream_keys.get(stream_id).copied()
    }

    fn stream_slot(&self, stream_id: &BucketStreamId) -> Option<&StreamSlot> {
        let key = self.stream_key(stream_id)?;
        self.streams.get(key)
    }

    fn stream_slot_mut(&mut self, stream_id: &BucketStreamId) -> Option<&mut StreamSlot> {
        let key = self.stream_key(stream_id)?;
        self.streams.get_mut(key)
    }

    fn stream_metadata(&self, stream_id: &BucketStreamId) -> Option<&StreamMetadata> {
        self.stream_slot(stream_id).map(|slot| &slot.metadata)
    }

    fn stream_metadata_mut(&mut self, stream_id: &BucketStreamId) -> Option<&mut StreamMetadata> {
        self.stream_slot_mut(stream_id)
            .map(|slot| &mut slot.metadata)
    }

    fn insert_stream_slot(&mut self, slot: StreamSlot) -> Option<StreamKey> {
        let stream_id = slot.metadata.stream_id.clone();
        if self.stream_keys.contains_key(&stream_id) {
            return None;
        }
        let key = self.streams.insert(slot);
        self.stream_keys.insert(stream_id.clone(), key);
        self.push_ttl_entry(&stream_id, key);
        Some(key)
    }

    fn push_ttl_entry(&mut self, stream_id: &BucketStreamId, key: StreamKey) {
        let Some(slot) = self.streams.get(key) else {
            return;
        };
        let Some(expires_at_ms) = stream_expiry_at_ms(&slot.metadata) else {
            return;
        };
        self.ttl_index.entries.push(Reverse(TtlEntry {
            expires_at_ms,
            stream_id: stream_id.clone(),
            key,
        }));
    }

    fn refresh_ttl_entry(&mut self, stream_id: &BucketStreamId) {
        if let Some(key) = self.stream_key(stream_id) {
            self.push_ttl_entry(stream_id, key);
        }
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
                attrs,
                now_ms,
            } => {
                let response = self.create_stream(CreateStreamInput {
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
                    attrs,
                    now_ms,
                });
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
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
                attrs,
                now_ms,
            } => {
                let response = self.create_external_stream(CreateExternalStreamInput {
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
                    attrs,
                    now_ms,
                });
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::Append {
                stream_id,
                content_type,
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => {
                let response = self.append_borrowed(AppendStreamInput {
                    stream_id,
                    content_type: content_type.as_deref(),
                    payload: &payload,
                    close_after,
                    stream_seq,
                    producer,
                    now_ms,
                });
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::AppendExternal {
                stream_id,
                content_type,
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => {
                let response = self.append_external(AppendExternalInput {
                    stream_id,
                    content_type: content_type.as_deref(),
                    payload,
                    close_after,
                    stream_seq,
                    producer,
                    now_ms,
                });
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::AppendBatch {
                stream_id,
                content_type,
                payloads,
                producer,
                now_ms,
            } => {
                let response = match self.append_batch_borrowed(
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
                };
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::PublishSnapshot {
                stream_id,
                snapshot_offset,
                content_type,
                payload,
                now_ms,
            } => {
                let response = self.publish_snapshot(
                    stream_id,
                    snapshot_offset,
                    content_type,
                    payload,
                    now_ms,
                );
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::TouchStreamAccess {
                stream_id,
                now_ms,
                renew_ttl,
            } => {
                let response = self.touch_stream_access(&stream_id, now_ms, renew_ttl);
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::UpdateStreamAttrs {
                stream_id,
                attrs,
                now_ms,
            } => {
                let response = self.update_stream_attrs(&stream_id, attrs, now_ms);
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::AddForkRef { stream_id, now_ms } => {
                let response = self.add_fork_ref(&stream_id, now_ms);
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::ReleaseForkRef { stream_id } => self.release_fork_ref(&stream_id),
            StreamCommand::FlushCold { stream_id, chunk } => self.flush_cold(stream_id, chunk),
            StreamCommand::Close {
                stream_id,
                stream_seq,
                producer,
                now_ms,
            } => {
                let response = self.close(stream_id, stream_seq, producer, now_ms);
                self.sweep_expired_streams(now_ms, TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE);
                response
            }
            StreamCommand::DeleteStream { stream_id } => self.delete_stream(&stream_id),
            StreamCommand::AckColdGc { up_to_seq } => self.ack_cold_gc(up_to_seq),
        }
    }

    pub fn head(&self, stream_id: &BucketStreamId) -> Option<&StreamMetadata> {
        self.stream_metadata(stream_id)
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
        let Some(slot) = self.stream_slot(stream_id) else {
            return 0;
        };
        slot.hot_buffer
            .chunks
            .front()
            .map(|chunk| chunk.start_offset)
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
        if is_soft_deleted(&slot.metadata) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
        Ok(u64::try_from(slot.hot_buffer.len()).expect("payload len fits u64"))
    }

    pub fn total_hot_payload_bytes(&self) -> u64 {
        self.streams
            .values()
            .map(|slot| u64::try_from(slot.hot_buffer.len()).expect("payload len fits u64"))
            .sum()
    }

    pub fn plan_cold_flush(
        &self,
        stream_id: &BucketStreamId,
        min_hot_bytes: usize,
        max_flush_bytes: usize,
    ) -> Result<Option<ColdFlushCandidate>, StreamResponse> {
        let start_offset = self.hot_start_offset(stream_id);
        self.plan_cold_flush_with_start(stream_id, start_offset, min_hot_bytes, max_flush_bytes)
    }

    fn plan_cold_flush_with_start(
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
        if is_soft_deleted(&slot.metadata) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
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

    fn plan_next_cold_flush_from_start(
        &self,
        mut start_fn: impl FnMut(&BucketStreamId) -> u64,
        min_hot_bytes: usize,
        max_flush_bytes: usize,
        group_hot_bytes: u64,
    ) -> Result<Option<ColdFlushCandidate>, StreamResponse> {
        if max_flush_bytes == 0 {
            return Ok(None);
        }
        let mut stream_ids = self.stream_keys.keys().cloned().collect::<Vec<_>>();
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
                .stream_keys
                .keys()
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

    pub fn bucket_exists(&self, bucket_id: &str) -> bool {
        self.buckets.contains(bucket_id)
    }

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
        if is_soft_deleted(&slot.metadata) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
        Ok(slot.integrity.snapshot(
            self.earliest_retained_offset(stream_id),
            slot.metadata.tail_offset,
        ))
    }

    pub fn snapshot(&self) -> StreamSnapshot {
        let mut buckets = self.buckets.iter().cloned().collect::<Vec<_>>();
        buckets.sort();

        let mut streams = self
            .streams
            .values()
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
                    integrity: slot
                        .integrity
                        .snapshot(self.earliest_retained_offset(&stream_id), tail_offset),
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
            if machine.stream_keys.contains_key(&stream_id) {
                return Err(StreamSnapshotError::DuplicateStream(stream_id));
            }
            let producer_states = restore_producer_states(&stream_id, entry.producer_states)?;
            let visible_snapshot = entry.visible_snapshot;
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
                integrity,
                visible_snapshot,
                producers: producer_states,
            };
            if machine.insert_stream_slot(slot).is_none() {
                return Err(StreamSnapshotError::DuplicateStream(stream_id));
            }
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
        if is_soft_deleted(&slot.metadata) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
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
        let Some(slot) = self.stream_slot(stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        let stream = &slot.metadata;
        if is_soft_deleted(stream) {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            ));
        }
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
        let Some(stream) = self.stream_metadata(&stream_id) else {
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

        self.stream_slot_mut(&stream_id)
            .expect("stream existence checked before snapshot publish")
            .visible_snapshot = Some(StreamVisibleSnapshot {
            offset: snapshot_offset,
            content_type,
            payload,
        });
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
        let Some(slot) = self.stream_slot(&stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        let stream = &slot.metadata;
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
            .stream_keys
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
        let attrs = normalize_stream_attrs(input.attrs.clone());
        if let Err(response) = self.validate_stream_scope(&input.stream_id) {
            return response;
        }
        if let Err(response) = validate_stream_attrs(attrs.as_ref()) {
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
            return StreamResponse::error_with_context(
                StreamErrorCode::ProducerSeqConflict,
                format!(
                    "producer '{}' expected sequence 0, received {}",
                    producer.producer_id, producer.producer_seq
                ),
                vec![StreamErrorContext::ProducerSeqConflict {
                    expected_seq: 0,
                    received_seq: producer.producer_seq,
                }],
            );
        }
        if self
            .stream_metadata(&input.stream_id)
            .is_some_and(|existing| stream_is_expired(existing, input.now_ms))
        {
            self.remove_stream_state(&input.stream_id);
        }

        if let Some(existing_slot) = self.stream_slot(&input.stream_id) {
            let existing = &existing_slot.metadata;
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
                && existing_slot.attrs.as_ref() == attrs.as_ref()
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
        let hot_buffer = HotBuffer::from_payload(0, input.initial_payload);
        let mut integrity = StreamIntegrity::default();
        if initial_len > 0 {
            let payload = hot_buffer.payload();
            integrity.append_payload(&input.stream_id, 0, initial_len, &payload);
        }
        let message_records = if initial_len > 0 {
            vec![StreamMessageRecord {
                start_offset: 0,
                end_offset: initial_len,
            }]
        } else {
            Vec::new()
        };
        let mut producer_states = HashMap::new();
        if let Some(producer) = input.producer {
            let last_item = ProducerAppendRecord {
                start_offset: 0,
                next_offset: initial_len,
                closed: input.close_after,
            };
            producer_states.insert(producer.producer_id, ProducerState {
                producer_epoch: producer.producer_epoch,
                producer_seq: producer.producer_seq,
                last_start_offset: last_item.start_offset,
                last_next_offset: last_item.next_offset,
                last_closed: last_item.closed,
                last_items: vec![last_item],
            });
        }
        let stream_id = input.stream_id.clone();
        let slot = StreamSlot {
            metadata,
            attrs,
            hot_buffer,
            cold: StreamColdState::default(),
            message_records,
            integrity,
            visible_snapshot: None,
            producers: producer_states,
        };
        if self.insert_stream_slot(slot).is_none() {
            return StreamResponse::error(
                StreamErrorCode::StreamAlreadyExistsConflict,
                format!(
                    "stream '{}' already exists with different metadata",
                    stream_id
                ),
            );
        }
        StreamResponse::Created {
            stream_id: input.stream_id,
            next_offset: initial_len,
            closed: input.close_after,
        }
    }

    fn create_external_stream(&mut self, input: CreateExternalStreamInput) -> StreamResponse {
        let attrs = normalize_stream_attrs(input.attrs.clone());
        if let Err(response) = validate_external_payload_ref(&input.initial_payload) {
            return response;
        }
        if let Err(response) = self.validate_stream_scope(&input.stream_id) {
            return response;
        }
        if let Err(response) = validate_stream_attrs(attrs.as_ref()) {
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
            return StreamResponse::error_with_context(
                StreamErrorCode::ProducerSeqConflict,
                format!(
                    "producer '{}' expected sequence 0, received {}",
                    producer.producer_id, producer.producer_seq
                ),
                vec![StreamErrorContext::ProducerSeqConflict {
                    expected_seq: 0,
                    received_seq: producer.producer_seq,
                }],
            );
        }
        if self
            .stream_metadata(&input.stream_id)
            .is_some_and(|existing| stream_is_expired(existing, input.now_ms))
        {
            self.remove_stream_state(&input.stream_id);
        }

        if let Some(existing_slot) = self.stream_slot(&input.stream_id) {
            let existing = &existing_slot.metadata;
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
                && existing_slot.attrs.as_ref() == attrs.as_ref()
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
        let object = ObjectPayloadRef {
            start_offset: 0,
            end_offset: initial_len,
            s3_path: input.initial_payload.s3_path,
            object_size: input.initial_payload.object_size,
        };
        let mut cold = StreamColdState::default();
        cold.push_external_segment(object.clone());
        let mut integrity = StreamIntegrity::default();
        if initial_len > 0 {
            integrity.append_external(
                &input.stream_id,
                object.start_offset,
                object.end_offset,
                &object.s3_path,
                object.object_size,
            );
        }
        let message_records = vec![StreamMessageRecord {
            start_offset: 0,
            end_offset: initial_len,
        }];
        let mut producer_states = HashMap::new();
        if let Some(producer) = input.producer {
            let last_item = ProducerAppendRecord {
                start_offset: 0,
                next_offset: initial_len,
                closed: input.close_after,
            };
            producer_states.insert(producer.producer_id, ProducerState {
                producer_epoch: producer.producer_epoch,
                producer_seq: producer.producer_seq,
                last_start_offset: last_item.start_offset,
                last_next_offset: last_item.next_offset,
                last_closed: last_item.closed,
                last_items: vec![last_item],
            });
        }
        let stream_id = input.stream_id.clone();
        let slot = StreamSlot {
            metadata,
            attrs,
            hot_buffer: HotBuffer::default(),
            cold,
            message_records,
            integrity,
            visible_snapshot: None,
            producers: producer_states,
        };
        if self.insert_stream_slot(slot).is_none() {
            return StreamResponse::error(
                StreamErrorCode::StreamAlreadyExistsConflict,
                format!(
                    "stream '{}' already exists with different metadata",
                    stream_id
                ),
            );
        }
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

        let Some(_) = self.stream_metadata(&stream_id) else {
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
        if self
            .stream_metadata(&stream_id)
            .is_some_and(is_soft_deleted)
        {
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

        let Some(stream) = self.stream_metadata_mut(&stream_id) else {
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
            return StreamResponse::error_with_next_offset_and_context(
                StreamErrorCode::StreamClosed,
                format!("stream '{stream_id}' is closed"),
                stream.tail_offset,
                vec![StreamErrorContext::StreamClosed],
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
        self.refresh_ttl_entry(&stream_id);
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
            let slot = self
                .stream_slot_mut(&stream_id)
                .expect("stream existence checked before append mutation");
            slot.hot_buffer.push(offset, next_offset, payload);
            slot.integrity
                .append_payload(&stream_id, offset, next_offset, payload);
            slot.message_records.push(StreamMessageRecord {
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
        let Some(_) = self.stream_metadata(&stream_id) else {
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
        if self
            .stream_metadata(&stream_id)
            .is_some_and(is_soft_deleted)
        {
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

        let Some(stream) = self.stream_metadata(&stream_id) else {
            unreachable!("stream existence checked before producer evaluation");
        };
        if stream.status == StreamStatus::Closed {
            return StreamResponse::error_with_next_offset_and_context(
                StreamErrorCode::StreamClosed,
                format!("stream '{stream_id}' is closed"),
                stream.tail_offset,
                vec![StreamErrorContext::StreamClosed],
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
            .stream_metadata_mut(&stream_id)
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
        self.refresh_ttl_entry(&stream_id);
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
        let object = ObjectPayloadRef {
            start_offset: offset,
            end_offset: next_offset,
            s3_path: payload.s3_path,
            object_size: payload.object_size,
        };
        let slot = self
            .stream_slot_mut(&stream_id)
            .expect("stream existence checked before external append mutation");
        slot.cold.push_external_segment(object.clone());
        slot.integrity.append_external(
            &stream_id,
            object.start_offset,
            object.end_offset,
            &object.s3_path,
            object.object_size,
        );
        slot.message_records.push(StreamMessageRecord {
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
        if self
            .stream_metadata(&stream_id)
            .is_some_and(is_soft_deleted)
        {
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

        let Some(stream) = self.stream_metadata_mut(&stream_id) else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        if stream.status == StreamStatus::Closed {
            return Err(StreamResponse::error_with_next_offset_and_context(
                StreamErrorCode::StreamClosed,
                format!("stream '{stream_id}' is closed"),
                stream.tail_offset,
                vec![StreamErrorContext::StreamClosed],
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
        self.refresh_ttl_entry(&stream_id);
        if let Some(producer) = producer {
            self.record_producer_success(stream_id.clone(), producer, last.clone(), items.clone());
        }
        let slot = self
            .stream_slot_mut(&stream_id)
            .expect("stream existence checked before batch append mutation");
        for (item, payload) in items.iter().zip(payloads.iter()) {
            slot.hot_buffer
                .push(item.start_offset, item.next_offset, payload);
        }
        for (item, payload) in items.iter().zip(payloads.iter()) {
            slot.integrity
                .append_payload(&stream_id, item.start_offset, item.next_offset, payload);
        }
        slot.message_records
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
        let Some(stream) = self.stream_metadata_mut(stream_id) else {
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
        let Some(stream) = self.stream_metadata_mut(stream_id) else {
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

    fn update_stream_attrs(
        &mut self,
        stream_id: &BucketStreamId,
        attrs: Option<StreamAttrs>,
        now_ms: u64,
    ) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(stream_id) {
            return response;
        }
        if self.expire_stream_if_due(stream_id, now_ms) {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        }
        let Some(slot) = self.stream_slot(stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        let stream = &slot.metadata;
        if is_soft_deleted(stream) {
            return StreamResponse::error(
                StreamErrorCode::StreamGone,
                format!("stream '{stream_id}' is gone"),
            );
        }
        let attrs = normalize_stream_attrs(attrs);
        if let Err(response) = validate_stream_attrs(attrs.as_ref()) {
            return response;
        }
        if slot.attrs.as_ref() == attrs.as_ref() {
            return StreamResponse::AttrsUpdated { changed: false };
        }
        self.stream_slot_mut(stream_id)
            .expect("stream existence checked before attrs mutation")
            .attrs = attrs;
        StreamResponse::AttrsUpdated { changed: true }
    }

    fn release_fork_ref(&mut self, stream_id: &BucketStreamId) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(stream_id) {
            return response;
        }
        let Some(stream) = self.stream_metadata_mut(stream_id) else {
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
        let Some(stream) = self.stream_metadata(stream_id) else {
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
                .stream_metadata_mut(stream_id)
                .expect("stream existence checked before TTL renewal");
            let previous = stream.last_ttl_touch_at_ms;
            renew_stream_ttl(stream, now_ms);
            let changed = stream.last_ttl_touch_at_ms != previous;
            if changed {
                self.refresh_ttl_entry(stream_id);
            }
            changed
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
            .stream_metadata(stream_id)
            .is_some_and(|stream| stream_is_expired(stream, now_ms))
        {
            self.remove_stream_state(stream_id);
            return true;
        }
        false
    }

    fn sweep_expired_streams(&mut self, now_ms: u64, max_streams: usize) -> usize {
        if max_streams == 0 {
            return 0;
        }
        let mut removed = 0;
        while removed < max_streams {
            let Some(Reverse(entry)) = self.ttl_index.entries.peek().cloned() else {
                break;
            };
            if entry.expires_at_ms > now_ms {
                break;
            }
            self.ttl_index.entries.pop();
            let Some(current_key) = self.stream_key(&entry.stream_id) else {
                continue;
            };
            if current_key != entry.key {
                continue;
            }
            let Some(slot) = self.streams.get(entry.key) else {
                continue;
            };
            if stream_expiry_at_ms(&slot.metadata) != Some(entry.expires_at_ms) {
                continue;
            }
            if !stream_is_expired(&slot.metadata, now_ms) {
                continue;
            }
            if self.remove_stream_state(&entry.stream_id) {
                removed = removed.saturating_add(1);
            }
        }
        removed
    }

    fn remove_stream_state(&mut self, stream_id: &BucketStreamId) -> bool {
        if let Some(key) = self.stream_keys.remove(stream_id)
            && let Some(slot) = self.streams.remove(key)
        {
            let had_cold = slot.cold.has_cold_objects();
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
        self.stream_slot(stream_id)
            .and_then(|slot| slot.visible_snapshot.as_ref())
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

    fn compact_retained_prefix(&mut self, stream_id: &BucketStreamId, retained_offset: u64) {
        let frontier = self.cold_frontier_offset(stream_id, retained_offset).max(
            self.stream_slot(stream_id)
                .map(|slot| slot.hot_buffer.hot_start_offset())
                .unwrap_or(retained_offset),
        );
        self.compact_message_records_before(stream_id, retained_offset, frontier);
        let Some(slot) = self.stream_slot_mut(stream_id) else {
            return;
        };
        slot.integrity.evict_before(retained_offset);
        let dropped_cold_paths = slot.cold.compact_before(retained_offset);
        if !dropped_cold_paths.is_empty() {
            self.enqueue_cold_gc(ColdGcTarget::Paths(dropped_cold_paths));
        }

        self.stream_slot_mut(stream_id)
            .expect("stream existence checked before hot compact")
            .hot_buffer
            .discard_before(retained_offset);
    }

    fn compact_message_records_before(
        &mut self,
        stream_id: &BucketStreamId,
        retained_offset: u64,
        frontier: u64,
    ) {
        let Some(slot) = self.stream_slot_mut(stream_id) else {
            return;
        };
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

    fn cold_frontier_offset(&self, stream_id: &BucketStreamId, retained_offset: u64) -> u64 {
        self.stream_slot(stream_id)
            .map(|slot| slot.cold.cold_frontier_offset(retained_offset))
            .unwrap_or(retained_offset)
    }

    fn evaluate_producer(
        &self,
        stream_id: &BucketStreamId,
        producer: Option<&ProducerRequest>,
    ) -> Result<ProducerDecision, StreamResponse> {
        let Some(producer) = producer else {
            return Ok(ProducerDecision::Accept);
        };
        let Some(states) = self.stream_slot(stream_id).map(|slot| &slot.producers) else {
            return Ok(ProducerDecision::Accept);
        };
        let Some(state) = states.get(&producer.producer_id) else {
            if producer.producer_seq == 0 {
                return Ok(ProducerDecision::Accept);
            }
            return Err(StreamResponse::error_with_context(
                StreamErrorCode::ProducerSeqConflict,
                format!(
                    "producer '{}' expected sequence 0, received {}",
                    producer.producer_id, producer.producer_seq
                ),
                vec![StreamErrorContext::ProducerSeqConflict {
                    expected_seq: 0,
                    received_seq: producer.producer_seq,
                }],
            ));
        };

        if producer.producer_epoch < state.producer_epoch {
            return Err(StreamResponse::error_with_context(
                StreamErrorCode::ProducerEpochStale,
                format!(
                    "producer '{}' epoch {} is stale; current epoch is {}",
                    producer.producer_id, producer.producer_epoch, state.producer_epoch
                ),
                vec![StreamErrorContext::ProducerEpochStale {
                    current_epoch: state.producer_epoch,
                }],
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
        Err(StreamResponse::error_with_context(
            StreamErrorCode::ProducerSeqConflict,
            format!(
                "producer '{}' expected sequence {}, received {}",
                producer.producer_id,
                state.producer_seq + 1,
                producer.producer_seq
            ),
            vec![StreamErrorContext::ProducerSeqConflict {
                expected_seq: state.producer_seq + 1,
                received_seq: producer.producer_seq,
            }],
        ))
    }

    fn record_producer_success(
        &mut self,
        stream_id: BucketStreamId,
        producer: ProducerRequest,
        last: ProducerAppendRecord,
        last_items: Vec<ProducerAppendRecord>,
    ) {
        self.stream_slot_mut(&stream_id)
            .expect("stream existence checked before producer mutation")
            .producers
            .insert(producer.producer_id, ProducerState {
                producer_epoch: producer.producer_epoch,
                producer_seq: producer.producer_seq,
                last_start_offset: last.start_offset,
                last_next_offset: last.next_offset,
                last_closed: last.closed,
                last_items,
            });
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
        })
        .collect::<Vec<_>>();
    producer_states.sort_by(|left, right| left.producer_id.cmp(&right.producer_id));
    producer_states
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
    attrs: Option<StreamAttrs>,
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
    attrs: Option<StreamAttrs>,
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

fn normalize_stream_attrs(attrs: Option<StreamAttrs>) -> Option<StreamAttrs> {
    attrs.filter(|attrs| !attrs.is_empty())
}

fn validate_stream_attrs(attrs: Option<&StreamAttrs>) -> Result<(), StreamResponse> {
    let Some(attrs) = attrs else {
        return Ok(());
    };
    let encoded_len = serde_json::to_vec(attrs)
        .map_err(|err| {
            StreamResponse::error(
                StreamErrorCode::InvalidStreamAttrs,
                format!("encode stream attrs JSON: {err}"),
            )
        })?
        .len();
    if encoded_len > MAX_STREAM_ATTRS_BYTES {
        return Err(StreamResponse::error(
            StreamErrorCode::InvalidStreamAttrs,
            format!(
                "stream attrs JSON is {encoded_len} bytes; limit is {MAX_STREAM_ATTRS_BYTES} bytes"
            ),
        ));
    }
    Ok(())
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
            .insert(snapshot.producer_id.clone(), ProducerState {
                producer_epoch: snapshot.producer_epoch,
                producer_seq: snapshot.producer_seq,
                last_start_offset: snapshot.last_start_offset,
                last_next_offset: snapshot.last_next_offset,
                last_closed: snapshot.last_closed,
                last_items: snapshot.last_items,
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
