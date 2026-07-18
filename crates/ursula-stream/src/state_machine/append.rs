//! Append paths (inline/external/batch) and idempotent producer bookkeeping.

use super::AppendExternalInput;
use super::AppendStreamInput;
use super::BucketStreamId;
use super::ObjectPayloadRef;
use super::ProducerAppendRecord;
use super::ProducerRequest;
use super::ProducerState;
use super::StreamBatchAppend;
use super::StreamBatchAppendItem;
use super::StreamErrorCode;
use super::StreamErrorContext;
use super::StreamMessageRecord;
use super::StreamMetadata;
use super::StreamResponse;
use super::StreamStateMachine;
use super::StreamStatus;
use super::canonical_json_record_ends;
use super::prepare_record_append;
use super::renew_stream_ttl;
use super::validate_external_payload_ref;
use super::validate_producer_request;

impl StreamStateMachine {
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

        let payload_len = u64::try_from(payload.len()).expect("payload len fits u64");
        let record_ends = content_type
            .map(|value| canonical_json_record_ends(value, payload).unwrap_or_default())
            .unwrap_or_default();
        let prepared_record_index = {
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
                Ok(index) => index,
                Err(response) => return response,
            }
        };

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
            slot.record_index = prepared_record_index;
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

    pub(super) fn append_external(&mut self, input: AppendExternalInput<'_>) -> StreamResponse {
        let AppendExternalInput {
            stream_id,
            content_type,
            payload,
            record_ends,
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

        let prepared_record_index = {
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
                Ok(index) => index,
                Err(response) => return response,
            }
        };

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
        slot.record_index = prepared_record_index;
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

        let mut prepared_record_index = self
            .stream_slot(&stream_id)
            .and_then(|slot| slot.record_index.clone());

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

        let mut record_offset = stream.tail_offset;
        for payload in payloads {
            let payload_len = u64::try_from(payload.len()).expect("payload len fits u64");
            let record_ends = canonical_json_record_ends(content_type, payload).unwrap_or_default();
            prepared_record_index = prepare_record_append(
                prepared_record_index.as_ref(),
                super::is_json_record_content_type(content_type),
                record_offset,
                payload_len,
                &record_ends,
            )?;
            record_offset = record_offset.saturating_add(payload_len);
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
        slot.record_index = prepared_record_index;
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
