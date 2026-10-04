//! Append paths (inline/external) and idempotent producer bookkeeping.

use super::AppendExternalInput;
use super::AppendStreamInput;
use super::BucketStreamId;
use super::BucketUsage;
use super::HashMap;
use super::ObjectPayloadRef;
use super::ProducerAppendRecord;
use super::ProducerReceipt;
use super::ProducerRequest;
use super::ProducerState;
use super::StreamCommand;
use super::StreamErrorCode;
use super::StreamErrorContext;
use super::StreamMetadata;
use super::StreamResponse;
use super::StreamStateMachine;
use super::StreamStatus;
use super::canonical_json_record_ends;
use super::prepare_record_append;
use super::renew_stream_ttl;
use super::validate_external_payload_ref;
use super::validate_producer_request;

struct StreamAppendUndo {
    metadata: StreamMetadata,
    hot_checkpoint: super::hot_buffer::HotCheckpoint,
    message_records_len: usize,
    record_checkpoint: Option<u64>,
    producers: HashMap<String, Option<ProducerState>>,
}

impl StreamStateMachine {
    pub fn append_transaction(
        &mut self,
        commands: Vec<StreamCommand>,
    ) -> Result<Vec<StreamResponse>, StreamResponse> {
        let Some(StreamCommand::Append {
            stream_id: first_stream,
            ..
        }) = commands.first()
        else {
            return Err(StreamResponse::error(
                StreamErrorCode::InvalidStreamId,
                "append transaction must contain at least one append command",
            ));
        };
        let Some(first_affinity) = first_stream.affinity_key.as_deref() else {
            return Err(StreamResponse::error(
                StreamErrorCode::InvalidStreamId,
                "append transaction streams must use path affinity",
            ));
        };
        let mut stream_undo = HashMap::<BucketStreamId, StreamAppendUndo>::new();
        let mut bucket_undo = HashMap::<String, Option<BucketUsage>>::new();
        for command in &commands {
            let StreamCommand::Append {
                stream_id,
                producer,
                now_ms,
                ..
            } = command
            else {
                return Err(StreamResponse::error(
                    StreamErrorCode::InvalidStreamId,
                    "append transaction contains a non-append command",
                ));
            };
            if stream_id.bucket_id != first_stream.bucket_id
                || stream_id.affinity_key.as_deref() != Some(first_affinity)
            {
                return Err(StreamResponse::error(
                    StreamErrorCode::InvalidStreamId,
                    "append transaction streams must share one bucket and affinity key",
                ));
            }
            // F4b: convert legacy message records before the undo
            // checkpoint, so a rollback never undoes the conversion.
            self.migrate_message_records(stream_id);
            let Some(slot) = self.stream_slot(stream_id) else {
                return Err(StreamResponse::error(
                    StreamErrorCode::StreamNotFound,
                    format!("stream '{stream_id}' does not exist"),
                ));
            };
            if super::stream_is_expired(&slot.metadata, *now_ms) {
                return Err(StreamResponse::error(
                    StreamErrorCode::StreamNotFound,
                    format!("stream '{stream_id}' does not exist"),
                ));
            }
            bucket_undo
                .entry(stream_id.bucket_id.clone())
                .or_insert_with(|| self.bucket_usage.get(&stream_id.bucket_id).copied());
            let undo = stream_undo
                .entry(stream_id.clone())
                .or_insert_with(|| StreamAppendUndo {
                    metadata: slot.metadata.clone(),
                    hot_checkpoint: slot.hot_buffer.append_checkpoint(),
                    message_records_len: slot.message_records.len(),
                    record_checkpoint: slot
                        .record_index
                        .as_ref()
                        .map(crate::StreamRecordIndex::append_checkpoint),
                    producers: HashMap::new(),
                });
            if let Some(producer) = producer {
                undo.producers
                    .entry(producer.producer_id.clone())
                    .or_insert_with(|| slot.producers.get(&producer.producer_id).cloned());
            }
        }

        let hot_payload_bytes = self.hot_payload_bytes;
        let enforce_now_ms = commands
            .iter()
            .filter_map(|command| match command {
                StreamCommand::Append { now_ms, .. } => Some(*now_ms),
                _ => None,
            })
            .max()
            .unwrap_or(0);
        let mut responses = Vec::with_capacity(commands.len());
        for command in commands {
            let StreamCommand::Append {
                stream_id,
                content_type,
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
                record_match,
            } = command
            else {
                unreachable!("transaction commands validated before mutation");
            };
            let response = self.append_borrowed_unenforced(AppendStreamInput {
                stream_id,
                content_type: content_type.as_deref(),
                payload: &payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
                record_match,
            });
            if matches!(response, StreamResponse::Error { .. }) {
                self.rollback_append_transaction(stream_undo, bucket_undo, hot_payload_bytes);
                return Err(response);
            }
            responses.push(response);
        }
        // F3: the window is enforced once, after the whole transaction
        // applied, so a failed transaction evicts nothing.
        for (stream_id, undo) in &stream_undo {
            if !undo.producers.is_empty() {
                self.enforce_producer_window(stream_id, enforce_now_ms);
            }
        }
        Ok(responses)
    }

    fn rollback_append_transaction(
        &mut self,
        stream_undo: HashMap<BucketStreamId, StreamAppendUndo>,
        bucket_undo: HashMap<String, Option<BucketUsage>>,
        hot_payload_bytes: u64,
    ) {
        self.hot_payload_bytes = hot_payload_bytes;
        for (bucket_id, usage) in bucket_undo {
            match usage {
                Some(usage) => {
                    self.bucket_usage.insert(bucket_id, usage);
                }
                None => {
                    self.bucket_usage.remove(&bucket_id);
                }
            }
        }
        for (stream_id, undo) in stream_undo {
            let Some(slot) = self.stream_slot_mut(&stream_id) else {
                continue;
            };
            slot.metadata = undo.metadata;
            slot.hot_buffer.rollback_appends(undo.hot_checkpoint);
            slot.message_records.truncate(undo.message_records_len);
            if let (Some(index), Some(checkpoint)) =
                (slot.record_index.as_mut(), undo.record_checkpoint)
            {
                index.rollback_appends(checkpoint);
            }
            let producers_touched = !undo.producers.is_empty();
            for (producer_id, producer) in undo.producers {
                match producer {
                    Some(producer) => {
                        slot.producers.insert(producer_id, producer);
                    }
                    None => {
                        slot.producers.remove(&producer_id);
                    }
                }
            }
            if producers_touched {
                slot.receipt_window = super::producers::ReceiptWindow::rebuild(&slot.producers);
            }
            self.sync_hot_index(&stream_id);
        }
    }

    /// Applies one append, then enforces the F3 receipt window once.
    pub fn append_borrowed(&mut self, input: AppendStreamInput<'_>) -> StreamResponse {
        let enforce_on = input.producer.as_ref().map(|_| input.stream_id.clone());
        let now_ms = input.now_ms;
        let response = self.append_borrowed_unenforced(input);
        if let Some(stream_id) = enforce_on {
            self.enforce_producer_window(&stream_id, now_ms);
        }
        response
    }

    /// One append without the F3 enforcement point, which a transaction
    /// runs once after all of its appends.
    fn append_borrowed_unenforced(&mut self, input: AppendStreamInput<'_>) -> StreamResponse {
        let AppendStreamInput {
            stream_id,
            content_type,
            payload,
            close_after,
            stream_seq,
            producer,
            now_ms,
            record_match,
        } = input;
        if let Err(response) = self.validate_stream_scope(&stream_id) {
            return response;
        }
        self.migrate_message_records(&stream_id);
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
        if let Some(producer) = producer.as_ref() {
            self.expire_idle_producer(&stream_id, &producer.producer_id, now_ms);
        }
        let producer_decision = match self.evaluate_producer(&stream_id, producer.as_ref(), now_ms)
        {
            Ok(decision) => decision,
            Err(response) => return response,
        };
        if let ProducerDecision::DuplicateEvicted { producer } = producer_decision {
            let (tail, closed) = self
                .stream_metadata(&stream_id)
                .map_or((0, false), |stream| {
                    (stream.tail_offset, stream.status == StreamStatus::Closed)
                });
            if payload.is_empty() {
                return StreamResponse::Closed {
                    next_offset: tail,
                    deduplicated: true,
                    producer: Some(producer),
                };
            }
            return StreamResponse::Appended {
                offset: tail,
                next_offset: tail,
                closed,
                deduplicated: true,
                producer: Some(producer),
                receipt_evicted: true,
                record_range: None,
            };
        }
        if let ProducerDecision::Duplicate {
            offset,
            next_offset,
            closed,
            producer,
            items,
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
                receipt_evicted: false,
                record_range: duplicate_record_range(&items, offset, next_offset),
            };
        }

        if let Err(response) = self.validate_record_match(&stream_id, record_match) {
            return response;
        }

        let payload_len = u64::try_from(payload.len()).expect("payload len fits u64");
        let record_ends = match content_type {
            Some(value) => match canonical_json_record_ends(value, payload) {
                Ok(record_ends) => record_ends,
                Err(_) => {
                    return StreamResponse::error(
                        StreamErrorCode::InvalidRecordBoundaries,
                        "application/json append payload must use canonical newline boundaries",
                    );
                }
            },
            None => Vec::new(),
        };
        let prepared_record_append = {
            let slot = self
                .stream_slot(&stream_id)
                .expect("stream existence checked before record validation");
            match prepare_record_append(
                slot.record_index.as_ref(),
                super::is_json_record_content_type(&slot.metadata.content_type),
                slot.metadata.tail_offset,
                payload_len,
                &record_ends,
            ) {
                Ok(prepared) => prepared,
                Err(response) => return response,
            }
        };
        let record_range = prepared_record_append
            .as_ref()
            .map(crate::PreparedRecordAppend::range);

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
                now_ms,
                ProducerAppendRecord {
                    start_offset: offset,
                    next_offset,
                    closed,
                    record_start: record_range.map(|range| range.first_record),
                    record_next: record_range.map(|range| range.next_record),
                },
            );
        }

        if payload.is_empty() {
            StreamResponse::Closed {
                next_offset,
                deduplicated: false,
                producer: producer_ack,
            }
        } else {
            let records_removed = self.message_records_removed();
            let slot = self
                .stream_slot_mut(&stream_id)
                .expect("stream existence checked before append mutation");
            if let (Some(index), Some(prepared)) =
                (slot.record_index.as_mut(), prepared_record_append)
            {
                let _range = index.commit_append(prepared);
            }
            slot.hot_buffer.push(offset, next_offset, payload);
            slot.record_message_boundaries(records_removed, offset, next_offset, &record_ends);
            self.add_hot_payload_bytes(payload_len);
            self.sync_hot_index(&stream_id);
            self.usage_on_append(
                &stream_id.bucket_id,
                payload_len,
                Self::appended_record_count(&record_ends, payload_len),
            );
            StreamResponse::Appended {
                offset,
                next_offset,
                closed: close_after,
                deduplicated: false,
                producer: producer_ack,
                receipt_evicted: false,
                record_range,
            }
        }
    }

    pub(super) fn append_external(&mut self, input: AppendExternalInput<'_>) -> StreamResponse {
        let enforce_on = input.producer.as_ref().map(|_| input.stream_id.clone());
        let now_ms = input.now_ms;
        let response = self.append_external_unenforced(input);
        if let Some(stream_id) = enforce_on {
            self.enforce_producer_window(&stream_id, now_ms);
        }
        response
    }

    fn append_external_unenforced(&mut self, input: AppendExternalInput<'_>) -> StreamResponse {
        let AppendExternalInput {
            stream_id,
            content_type,
            payload,
            record_ends,
            close_after,
            stream_seq,
            producer,
            now_ms,
            record_match,
        } = input;
        if let Err(response) = validate_external_payload_ref(&payload) {
            return response;
        }
        if let Err(response) = self.validate_stream_scope(&stream_id) {
            return response;
        }
        self.migrate_message_records(&stream_id);
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
        if let Some(producer) = producer.as_ref() {
            self.expire_idle_producer(&stream_id, &producer.producer_id, now_ms);
        }
        let producer_decision = match self.evaluate_producer(&stream_id, producer.as_ref(), now_ms)
        {
            Ok(decision) => decision,
            Err(response) => return response,
        };
        if let ProducerDecision::DuplicateEvicted { producer } = producer_decision {
            let (tail, closed) = self
                .stream_metadata(&stream_id)
                .map_or((0, false), |stream| {
                    (stream.tail_offset, stream.status == StreamStatus::Closed)
                });
            return StreamResponse::Appended {
                offset: tail,
                next_offset: tail,
                closed,
                deduplicated: true,
                producer: Some(producer),
                receipt_evicted: true,
                record_range: None,
            };
        }
        if let ProducerDecision::Duplicate {
            offset,
            next_offset,
            closed,
            producer,
            items,
        } = producer_decision
        {
            return StreamResponse::Appended {
                offset,
                next_offset,
                closed,
                deduplicated: true,
                producer: Some(producer),
                receipt_evicted: false,
                record_range: duplicate_record_range(&items, offset, next_offset),
            };
        }

        if let Err(response) = self.validate_record_match(&stream_id, record_match) {
            return response;
        }

        let prepared_record_append = {
            let slot = self
                .stream_slot(&stream_id)
                .expect("stream existence checked before record validation");
            match prepare_record_append(
                slot.record_index.as_ref(),
                super::is_json_record_content_type(&slot.metadata.content_type),
                slot.metadata.tail_offset,
                payload.payload_len,
                &record_ends,
            ) {
                Ok(prepared) => prepared,
                Err(response) => return response,
            }
        };
        let record_range = prepared_record_append
            .as_ref()
            .map(crate::PreparedRecordAppend::range);

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
                now_ms,
                ProducerAppendRecord {
                    start_offset: offset,
                    next_offset,
                    closed,
                    record_start: record_range.map(|range| range.first_record),
                    record_next: record_range.map(|range| range.next_record),
                },
            );
        }
        let external_locators_in_state = self.external_locators_in_state();
        let object = ObjectPayloadRef {
            start_offset: offset,
            end_offset: next_offset,
            s3_path: payload.s3_path,
            object_size: payload.object_size,
            object_offset: 0,
        };
        let records_removed = self.message_records_removed();
        let slot = self
            .stream_slot_mut(&stream_id)
            .expect("stream existence checked before external append mutation");
        if let (Some(index), Some(prepared)) = (slot.record_index.as_mut(), prepared_record_append)
        {
            let _range = index.commit_append(prepared);
        }
        if external_locators_in_state {
            // F5 (level 3): commit first, index after. State holds the
            // locator until the leader's offload pass writes its page entry.
            slot.cold.push_direct_external_segment(object.clone());
        } else {
            slot.cold.push_external_segment(object.clone());
        }
        slot.record_message_boundaries(records_removed, offset, next_offset, &record_ends);
        // F4a: an external append is a cold transition.
        self.collapse_sealed_message_records(&stream_id);
        self.sync_hot_index(&stream_id);
        // F1 (level 2): and it seals the records below the seal point, which
        // may include its own; the acknowledgement uses the range computed
        // above, never the index (RC-10).
        self.seal_record_index(&stream_id);
        let appended_bytes = next_offset.saturating_sub(offset);
        self.usage_on_append(
            &stream_id.bucket_id,
            appended_bytes,
            Self::appended_record_count(&record_ends, appended_bytes),
        );
        StreamResponse::Appended {
            offset,
            next_offset,
            closed: close_after,
            deduplicated: false,
            producer: producer_ack,
            receipt_evicted: false,
            record_range,
        }
    }

    fn validate_record_match(
        &self,
        stream_id: &BucketStreamId,
        expected: Option<u64>,
    ) -> Result<(), StreamResponse> {
        let Some(expected) = expected else {
            return Ok(());
        };
        let Some(slot) = self.stream_slot(stream_id) else {
            return Ok(());
        };
        let Some(index) = slot.record_index.as_ref() else {
            return Err(StreamResponse::error(
                StreamErrorCode::InvalidRecordBoundaries,
                "Stream-Record-Match requires active JSON record coordinates",
            ));
        };
        let current = index
            .range()
            .map_err(|_| {
                StreamResponse::error(
                    StreamErrorCode::InvalidRecordBoundaries,
                    "stream record index is invalid",
                )
            })?
            .next_record;
        if current == expected {
            return Ok(());
        }
        Err(StreamResponse::error_with_next_offset_and_context(
            StreamErrorCode::RecordPreconditionFailed,
            format!("record tail is {current}, expected {expected}"),
            slot.metadata.tail_offset,
            vec![StreamErrorContext::RecordTailMismatch {
                current_record: current,
            }],
        ))
    }

    /// Read-only: whether an append from `producer` would be
    /// answered as a duplicate without mutating the stream. Lets admission
    /// control bypass backpressure for idempotent retries without previewing
    /// the write on a copy of the group (F9).
    pub fn append_would_deduplicate(
        &self,
        stream_id: &BucketStreamId,
        producer: Option<&ProducerRequest>,
        now_ms: u64,
    ) -> bool {
        if producer.is_none()
            || self.validate_stream_scope(stream_id).is_err()
            || validate_producer_request(producer).is_err()
            || !self.stream_is_live(stream_id, now_ms)
        {
            return false;
        }
        matches!(
            self.evaluate_producer(stream_id, producer, now_ms),
            Ok(ProducerDecision::Duplicate { .. } | ProducerDecision::DuplicateEvicted { .. })
        )
    }

    fn evaluate_producer(
        &self,
        stream_id: &BucketStreamId,
        producer: Option<&ProducerRequest>,
        now_ms: u64,
    ) -> Result<ProducerDecision, StreamResponse> {
        let Some(producer) = producer else {
            return Ok(ProducerDecision::Accept);
        };
        let Some(states) = self.stream_slot(stream_id).map(|slot| &slot.producers) else {
            return Ok(ProducerDecision::Accept);
        };
        let bounded = self.producer_bounds_enabled();
        // F3: an idle producer is treated as absent at its own next write.
        let state = states
            .get(&producer.producer_id)
            .filter(|state| !(bounded && super::producers::producer_is_idle(state, now_ms)));
        let Some(state) = state else {
            if producer.producer_seq == 0 {
                // F3 producer cap: a new producer needs room, either below
                // the cap or by evicting producers idle for an hour.
                if bounded
                    && let Some(slot) = self.stream_slot(stream_id)
                    && !slot.producer_cap_admits(&producer.producer_id, now_ms)
                {
                    return Err(StreamResponse::error(
                        StreamErrorCode::ProducerLimit,
                        format!(
                            "producer_limit: stream '{stream_id}' already has {} producers active within the last hour",
                            super::producers::MAX_PRODUCERS_PER_STREAM
                        ),
                    ));
                }
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
            let Some(receipt) = find_receipt(&state.receipts, producer.producer_seq) else {
                if bounded {
                    // F3: beyond the receipt window a duplicate is still a
                    // duplicate (never accepted twice), answered without
                    // ranges.
                    return Ok(ProducerDecision::DuplicateEvicted {
                        producer: ProducerRequest {
                            producer_id: producer.producer_id.clone(),
                            producer_epoch: state.producer_epoch,
                            producer_seq: producer.producer_seq,
                        },
                    });
                }
                return Err(StreamResponse::error_with_context(
                    StreamErrorCode::ProducerSeqConflict,
                    format!(
                        "producer '{}' sequence {} is older than the retained receipt window ending at {}",
                        producer.producer_id, producer.producer_seq, state.producer_seq
                    ),
                    vec![StreamErrorContext::ProducerSeqConflict {
                        expected_seq: state.producer_seq.saturating_add(1),
                        received_seq: producer.producer_seq,
                    }],
                ));
            };
            return Ok(ProducerDecision::Duplicate {
                offset: receipt.start_offset,
                next_offset: receipt.next_offset,
                closed: receipt.closed,
                producer: ProducerRequest {
                    producer_id: producer.producer_id.clone(),
                    producer_epoch: state.producer_epoch,
                    producer_seq: receipt.producer_seq,
                },
                items: receipt.items.clone(),
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
        now_ms: u64,
        last: ProducerAppendRecord,
    ) {
        let bounded = self.producer_bounds_enabled();
        let receipt = ProducerReceipt {
            producer_seq: producer.producer_seq,
            start_offset: last.start_offset,
            next_offset: last.next_offset,
            closed: last.closed,
            items: vec![last.clone()],
        };
        // Level 1 answers from the newest receipt and keeps no copy (F3);
        // level 0 keeps `last_items` exactly as earlier releases do.
        let last_items = if bounded {
            Vec::new()
        } else {
            vec![last.clone()]
        };
        let last_seen_ms = bounded.then_some(now_ms);
        let Some(slot) = self.stream_slot_mut(&stream_id) else {
            return;
        };
        if let Some(state) = slot.producers.get_mut(&producer.producer_id)
            && state.producer_epoch == producer.producer_epoch
        {
            state.producer_seq = producer.producer_seq;
            state.last_start_offset = last.start_offset;
            state.last_next_offset = last.next_offset;
            state.last_closed = last.closed;
            state.last_items = last_items;
            // O(1) window update: the front is unchanged by a push and
            // becomes evictable once the producer holds two receipts.
            slot.receipt_window
                .push_receipt(&producer.producer_id, state, &receipt);
            state.receipts.push_back(receipt);
            if last_seen_ms.is_some() {
                state.last_seen_ms = last_seen_ms;
            }
            return;
        }
        // A new producer or a new epoch: the previous state (and its
        // receipts) is replaced.
        slot.remove_producer(&producer.producer_id);
        let state = ProducerState {
            producer_epoch: producer.producer_epoch,
            producer_seq: producer.producer_seq,
            last_start_offset: last.start_offset,
            last_next_offset: last.next_offset,
            last_closed: last.closed,
            last_items,
            receipts: std::collections::VecDeque::from([receipt]),
            last_seen_ms,
        };
        slot.receipt_window
            .add_producer(&producer.producer_id, &state);
        slot.producers.insert(producer.producer_id, state);
    }
}

/// The record range a producer receipt item stored at apply time.
fn item_record_range(item: &ProducerAppendRecord) -> Option<crate::StreamRecordRange> {
    match (item.record_start, item.record_next) {
        (Some(first_record), Some(next_record)) => Some(crate::StreamRecordRange {
            first_record,
            next_record,
        }),
        _ => None,
    }
}

/// A duplicate's acknowledgement range: the stored receipt item for its
/// byte range (RC-11), never one recomputed from the index.
fn duplicate_record_range(
    items: &[ProducerAppendRecord],
    offset: u64,
    next_offset: u64,
) -> Option<crate::StreamRecordRange> {
    items
        .iter()
        .find(|item| item.start_offset == offset && item.next_offset == next_offset)
        .and_then(item_record_range)
}

/// O(1) duplicate lookup (F3): receipts hold contiguous sequences, so the
/// receipt of `seq` sits at `seq - front.seq`. Falls back to a binary search
/// for receipt lists restored from older snapshots.
pub(super) fn find_receipt(
    receipts: &std::collections::VecDeque<ProducerReceipt>,
    seq: u64,
) -> Option<&ProducerReceipt> {
    let front = receipts.front()?;
    let direct = seq
        .checked_sub(front.producer_seq)
        .and_then(|index| usize::try_from(index).ok())
        .and_then(|index| receipts.get(index))
        .filter(|receipt| receipt.producer_seq == seq);
    if direct.is_some() {
        return direct;
    }
    receipts
        .binary_search_by_key(&seq, |receipt| receipt.producer_seq)
        .ok()
        .and_then(|index| receipts.get(index))
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
    /// A duplicate whose receipt the window evicted (F3, level 1).
    DuplicateEvicted {
        producer: ProducerRequest,
    },
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
