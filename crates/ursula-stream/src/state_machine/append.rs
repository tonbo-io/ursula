//! Append paths (inline/external) and idempotent producer bookkeeping.

use super::AppendExternalInput;
use super::AppendStreamInput;
use super::BucketStreamId;
use super::ObjectPayloadRef;
use super::ProducerReceipt;
use super::ProducerRequest;
use super::ProducerState;
use super::StreamErrorCode;
use super::StreamErrorContext;
use super::StreamMetadata;
use super::StreamResponse;
use super::StreamStateMachine;
use super::StreamStatus;
use super::canonical_json_record_ends;
use super::renew_stream_ttl;
use super::validate_external_payload_ref;
use super::validate_producer_request;
use super::validate_record_ends;

impl StreamStateMachine {
    /// Applies one append, then enforces the F3 receipt window once.
    pub fn append_borrowed(&mut self, input: AppendStreamInput<'_>) -> StreamResponse {
        let enforce_on = input.producer.as_ref().map(|_| input.stream_id.clone());
        let now_ms = input.now_ms;
        let response = 'append: {
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
                break 'append response;
            }
            if let Err(response) = validate_producer_request(producer.as_ref()) {
                break 'append response;
            }

            let Some(_) = self.stream_metadata(&stream_id) else {
                break 'append StreamResponse::error(
                    StreamErrorCode::StreamNotFound,
                    format!("stream '{stream_id}' does not exist"),
                );
            };
            if self.expire_stream_if_due(&stream_id, now_ms) {
                break 'append StreamResponse::error(
                    StreamErrorCode::StreamNotFound,
                    format!("stream '{stream_id}' does not exist"),
                );
            }
            if let Some(producer) = producer.as_ref() {
                self.expire_idle_producer(&stream_id, &producer.producer_id, now_ms);
            }
            let producer_decision =
                match self.evaluate_producer(&stream_id, producer.as_ref(), now_ms) {
                    Ok(decision) => decision,
                    Err(response) => break 'append response,
                };
            if let ProducerDecision::DuplicateEvicted { producer } = producer_decision {
                let (tail, closed) = self
                    .stream_metadata(&stream_id)
                    .map_or((0, false), |stream| {
                        (stream.tail_offset, stream.status == StreamStatus::Closed)
                    });
                if payload.is_empty() {
                    break 'append StreamResponse::Closed {
                        next_offset: tail,
                        deduplicated: true,
                        producer: Some(producer),
                    };
                }
                break 'append StreamResponse::Appended {
                    offset: tail,
                    next_offset: tail,
                    closed,
                    deduplicated: true,
                    producer: Some(producer),
                    receipt_evicted: true,
                };
            }
            if let ProducerDecision::Duplicate {
                offset,
                next_offset,
                closed,
                producer,
            } = producer_decision
            {
                if payload.is_empty() {
                    break 'append StreamResponse::Closed {
                        next_offset,
                        deduplicated: true,
                        producer: Some(producer),
                    };
                }
                break 'append StreamResponse::Appended {
                    offset,
                    next_offset,
                    closed,
                    deduplicated: true,
                    producer: Some(producer),
                    receipt_evicted: false,
                };
            }

            let payload_len = u64::try_from(payload.len()).expect("payload len fits u64");
            let record_ends = match content_type {
                Some(value) => match canonical_json_record_ends(value, payload) {
                    Ok(record_ends) => record_ends,
                    Err(_) => {
                        break 'append StreamResponse::error(
                            StreamErrorCode::InvalidRecordBoundaries,
                            "application/json append payload must use canonical newline boundaries",
                        );
                    }
                },
                None => Vec::new(),
            };

            let Some(stream) = self.stream_metadata_mut(&stream_id) else {
                unreachable!("stream existence checked before producer evaluation");
            };

            if stream.status == StreamStatus::Closed {
                if close_after && payload.is_empty() {
                    break 'append StreamResponse::Closed {
                        next_offset: stream.tail_offset,
                        deduplicated: false,
                        producer: None,
                    };
                }
                break 'append StreamResponse::error_with_next_offset_and_context(
                    StreamErrorCode::StreamClosed,
                    format!("stream '{stream_id}' is closed"),
                    stream.tail_offset,
                    vec![StreamErrorContext::StreamClosed],
                );
            }

            if payload.is_empty() && !close_after {
                break 'append StreamResponse::error(
                    StreamErrorCode::EmptyAppend,
                    "append payload must be non-empty unless closing the stream",
                );
            }

            if !payload.is_empty() {
                let Some(content_type) = content_type else {
                    break 'append StreamResponse::error(
                        StreamErrorCode::MissingContentType,
                        "append with a body must include content type",
                    );
                };
                if content_type != stream.content_type {
                    break 'append StreamResponse::error_with_next_offset(
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
                break 'append response;
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
                    ProducerReceiptRange {
                        start_offset: offset,
                        next_offset,
                        closed,
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
                let slot = self
                    .stream_slot_mut(&stream_id)
                    .expect("stream existence checked before append mutation");
                slot.hot_buffer.push(offset, next_offset, payload);
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
                }
            }
        };
        if let Some(stream_id) = enforce_on {
            self.enforce_producer_window(&stream_id, now_ms);
        }
        response
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
            };
        }
        if let ProducerDecision::Duplicate {
            offset,
            next_offset,
            closed,
            producer,
        } = producer_decision
        {
            return StreamResponse::Appended {
                offset,
                next_offset,
                closed,
                deduplicated: true,
                producer: Some(producer),
                receipt_evicted: false,
            };
        }

        let Some(stream) = self.stream_metadata(&stream_id) else {
            unreachable!("stream existence checked before producer evaluation");
        };
        if let Err(response) =
            validate_record_ends(&stream.content_type, payload.payload_len, &record_ends)
        {
            return response;
        }
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
                ProducerReceiptRange {
                    start_offset: offset,
                    next_offset,
                    closed,
                },
            );
        }
        let object = ObjectPayloadRef {
            start_offset: offset,
            end_offset: next_offset,
            s3_path: payload.s3_path,
            object_size: payload.object_size,
            object_offset: 0,
        };
        let slot = self
            .stream_slot_mut(&stream_id)
            .expect("stream existence checked before external append mutation");
        // F5: commit first, index after. State holds the locator until the
        // leader's offload pass writes its page entry.
        slot.cold.push_direct_external_segment(object.clone());
        self.sync_hot_index(&stream_id);
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
        }
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
        // F3: an idle producer is treated as absent at its own next write.
        let state = states
            .get(&producer.producer_id)
            .filter(|state| !super::producers::producer_is_idle(state, now_ms));
        let Some(state) = state else {
            if producer.producer_seq == 0 {
                // F3 producer cap: a new producer needs room, either below
                // the cap or by evicting producers idle for an hour.
                if let Some(slot) = self.stream_slot(stream_id)
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
                // F3: beyond the receipt window a duplicate is still a
                // duplicate (never accepted twice), answered without ranges.
                return Ok(ProducerDecision::DuplicateEvicted {
                    producer: ProducerRequest {
                        producer_id: producer.producer_id.clone(),
                        producer_epoch: state.producer_epoch,
                        producer_seq: producer.producer_seq,
                    },
                });
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
            });
        }
        // The duplicate branch above returned for `producer_seq <=
        // state.producer_seq`, so `state.producer_seq < u64::MAX` here and the
        // saturating add never clamps.
        let expected_seq = state.producer_seq.saturating_add(1);
        if producer.producer_seq == expected_seq {
            return Ok(ProducerDecision::Accept);
        }
        Err(StreamResponse::error_with_context(
            StreamErrorCode::ProducerSeqConflict,
            format!(
                "producer '{}' expected sequence {expected_seq}, received {}",
                producer.producer_id, producer.producer_seq
            ),
            vec![StreamErrorContext::ProducerSeqConflict {
                expected_seq,
                received_seq: producer.producer_seq,
            }],
        ))
    }

    fn record_producer_success(
        &mut self,
        stream_id: BucketStreamId,
        producer: ProducerRequest,
        now_ms: u64,
        last: ProducerReceiptRange,
    ) {
        let receipt = ProducerReceipt {
            producer_seq: producer.producer_seq,
            start_offset: last.start_offset,
            next_offset: last.next_offset,
            closed: last.closed,
        };
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
            // O(1) window update: the front is unchanged by a push and
            // becomes evictable once the producer holds two receipts.
            slot.receipt_window
                .push_receipt(&producer.producer_id, state);
            state.receipts.push_back(receipt);
            state.last_seen_ms = now_ms;
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
            receipts: std::collections::VecDeque::from([receipt]),
            last_seen_ms: now_ms,
        };
        slot.receipt_window
            .add_producer(&producer.producer_id, &state);
        slot.producers.insert(producer.producer_id, state);
    }
}

/// The byte range and closure an accepted producer write committed.
struct ProducerReceiptRange {
    start_offset: u64,
    next_offset: u64,
    closed: bool,
}

/// O(1) duplicate lookup (F3): within an epoch a producer's sequences are
/// accepted only as `0, 1, 2, ...` and receipts are evicted from the front,
/// so the receipt of `seq` sits at `seq - front.seq`.
pub(super) fn find_receipt(
    receipts: &std::collections::VecDeque<ProducerReceipt>,
    seq: u64,
) -> Option<&ProducerReceipt> {
    let front = receipts.front()?;
    seq.checked_sub(front.producer_seq)
        .and_then(|index| usize::try_from(index).ok())
        .and_then(|index| receipts.get(index))
        .filter(|receipt| receipt.producer_seq == seq)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProducerDecision {
    Accept,
    Duplicate {
        offset: u64,
        next_offset: u64,
        closed: bool,
        producer: ProducerRequest,
    },
    /// A duplicate whose receipt the window evicted (F3).
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
