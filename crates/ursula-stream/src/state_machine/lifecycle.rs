//! Bucket/stream lifecycle: create, close, delete, attrs, and TTL expiry.

use super::AppendStreamInput;
use super::BucketStreamId;
use super::ColdGcTarget;
use super::CreateExternalStreamInput;
use super::CreateStreamInput;
use super::HashMap;
use super::HotBuffer;
use super::MAX_STREAM_ATTRS_BYTES;
use super::ObjectPayloadRef;
use super::ProducerAppendRecord;
use super::ProducerRequest;
use super::ProducerState;
use super::StreamAttrs;
use super::StreamColdState;
use super::StreamErrorCode;
use super::StreamErrorContext;
use super::StreamIntegrity;
use super::StreamMetadata;
use super::StreamResponse;
use super::StreamSlot;
use super::StreamStateMachine;
use super::StreamStatus;
use super::build_record_index;
use super::normalize_stream_attrs;
use super::renew_stream_ttl;
use super::stream_is_expired;
use super::validate_bucket_id;
use super::validate_external_payload_ref;
use super::validate_producer_request;
use super::validate_stream_id;

impl StreamStateMachine {
    pub(super) fn create_bucket(&mut self, bucket_id: String) -> StreamResponse {
        if let Err(message) = validate_bucket_id(&bucket_id) {
            return StreamResponse::error(StreamErrorCode::InvalidBucketId, message);
        }
        if !self.buckets.insert(bucket_id.clone()) {
            return StreamResponse::BucketAlreadyExists { bucket_id };
        }
        StreamResponse::BucketCreated { bucket_id }
    }

    pub(super) fn delete_bucket(&mut self, bucket_id: &str) -> StreamResponse {
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
            .registry
            .stream_ids()
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

    pub(super) fn create_stream(&mut self, input: CreateStreamInput) -> StreamResponse {
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
        let initial_len = input.initial_len();
        let record_index =
            match build_record_index(&input.content_type, initial_len, &input.record_ends) {
                Ok(index) => index,
                Err(response) => return response,
            };
        let record_range = record_index.as_ref().and_then(|index| index.range().ok());
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
            if existing.content_type == input.content_type
                && existing.status == status_from_closed(input.close_after)
                && existing.stream_ttl_seconds == input.stream_ttl_seconds
                && existing.stream_expires_at_ms == input.stream_expires_at_ms
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
        };
        let hot_buffer = HotBuffer::from_payload(0, input.initial_payload);
        let mut integrity = StreamIntegrity::default();
        if initial_len > 0 {
            let payload = hot_buffer.payload();
            integrity.append_payload(&input.stream_id, 0, initial_len, &payload);
        }
        let message_records = Self::message_records_for_append(0, initial_len, &input.record_ends);
        let mut producer_states = HashMap::new();
        if let Some(producer) = input.producer {
            let last_item = ProducerAppendRecord {
                start_offset: 0,
                next_offset: initial_len,
                closed: input.close_after,
                record_start: record_range.map(|range| range.first_record),
                record_next: record_range.map(|range| range.next_record),
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
            record_index,
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

    pub(super) fn create_external_stream(
        &mut self,
        input: CreateExternalStreamInput,
    ) -> StreamResponse {
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
        let record_index = match build_record_index(
            &input.content_type,
            input.initial_payload.payload_len,
            &input.record_ends,
        ) {
            Ok(index) => index,
            Err(response) => return response,
        };
        let record_range = record_index.as_ref().and_then(|index| index.range().ok());
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
            if existing.content_type == input.content_type
                && existing.status == status_from_closed(input.close_after)
                && existing.stream_ttl_seconds == input.stream_ttl_seconds
                && existing.stream_expires_at_ms == input.stream_expires_at_ms
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
        let message_records = Self::message_records_for_append(0, initial_len, &input.record_ends);
        let mut producer_states = HashMap::new();
        if let Some(producer) = input.producer {
            let last_item = ProducerAppendRecord {
                start_offset: 0,
                next_offset: initial_len,
                closed: input.close_after,
                record_start: record_range.map(|range| range.first_record),
                record_next: record_range.map(|range| range.next_record),
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
            record_index,
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

    pub(super) fn close(
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
            record_match: None,
        })
    }

    pub(super) fn delete_stream(&mut self, stream_id: &BucketStreamId) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(stream_id) {
            return response;
        }
        let Some(_) = self.stream_metadata(stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        self.remove_stream_state(stream_id);
        StreamResponse::Deleted
    }

    pub(super) fn update_stream_attrs(
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

    pub(super) fn touch_stream_access(
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

    pub(super) fn expire_stream_if_due(&mut self, stream_id: &BucketStreamId, now_ms: u64) -> bool {
        if self
            .stream_metadata(stream_id)
            .is_some_and(|stream| stream_is_expired(stream, now_ms))
        {
            self.remove_stream_state(stream_id);
            return true;
        }
        false
    }

    pub(super) fn sweep_expired_streams(&mut self, now_ms: u64, max_streams: usize) -> usize {
        if max_streams == 0 {
            return 0;
        }
        let mut removed = 0;
        while removed < max_streams {
            let Some(stream_id) = self.registry.pop_expired(now_ms) else {
                break;
            };
            if self.remove_stream_state(&stream_id) {
                removed = removed.saturating_add(1);
            }
        }
        removed
    }

    pub(super) fn remove_stream_state(&mut self, stream_id: &BucketStreamId) -> bool {
        let Some(slot) = self.registry.remove(stream_id) else {
            return false;
        };
        // The cold objects we wrote for this stream are now unreferenced.
        // Enqueue the whole prefix for the background GC worker to reclaim;
        // A prefix sweep is safe and keeps the queue O(streams), not O(chunks).
        if slot.cold.has_cold_objects() {
            self.cold_gc
                .enqueue(ColdGcTarget::Stream(stream_id.clone()));
        }
        true
    }

    pub(super) fn validate_stream_scope(
        &self,
        stream_id: &BucketStreamId,
    ) -> Result<(), StreamResponse> {
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
}

fn status_from_closed(closed: bool) -> StreamStatus {
    if closed {
        StreamStatus::Closed
    } else {
        StreamStatus::Open
    }
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
