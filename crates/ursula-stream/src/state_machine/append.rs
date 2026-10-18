//! Append paths (inline/external) and idempotent producer bookkeeping.

use super::AppendExternalInput;
use super::AppendStreamInput;
use super::BucketStreamId;
use super::ClosedEpoch;
use super::IncarnationCheck;
use super::NonCanonicalJsonPayload;
use super::ObjectPayloadRef;
use super::Opened;
use super::ProducerAck;
use super::ProducerDelivery;
use super::ProducerEarlyAnswer;
use super::ProducerRefusal;
use super::ProducerRequest;
use super::ProducerState;
use super::StreamErrorCode;
use super::StreamErrorContext;
use super::StreamMetadata;
use super::StreamResponse;
use super::StreamStateMachine;
use super::StreamStatus;
use super::WriteKind;
use super::WriteReceipt;
use super::canonical_json_message_count;
use super::content_fault_response;
use super::producer_content_fault;
use super::producer_record::Evaluation;
use super::producer_record::Refusal;
use super::producer_record::WriteKey;
use super::producer_record::evaluate;
use super::producer_write_key;
use super::renew_stream_ttl;
use super::validate_external_payload_ref;
use super::validate_message_count;

impl StreamStateMachine {
    /// Applies one append, then enforces the F3 producer cap once.
    pub(super) fn append_borrowed(&mut self, input: AppendStreamInput<'_>) -> StreamResponse {
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
                kind,
                check,
            } = input;
            if let Err(response) = self.validate_stream_scope(&stream_id) {
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
            let gate = match self.producer_gate(&stream_id, producer.as_ref(), check, now_ms) {
                Ok(gate) => gate,
                Err(response) => break 'append response,
            };
            let gate = match gate {
                ProducerGate::Repeat(ack) => {
                    break 'append self.repeat_response(
                        &stream_id,
                        ack,
                        kind == WriteKind::CloseOnly,
                    );
                }
                ProducerGate::Unkeyed => None,
                ProducerGate::Apply(key, advance) => Some((key, advance)),
            };
            let fault = match producer.as_ref().map(producer_content_fault).transpose() {
                Ok(fault) => fault.flatten(),
                Err(response) => break 'append response,
            };

            let payload_len = u64::try_from(payload.len()).expect("payload len fits u64");
            let Some(stream) = self.stream_metadata_mut(&stream_id) else {
                unreachable!("stream existence checked before producer evaluation");
            };

            if stream.status == StreamStatus::Closed {
                // An unkeyed close of a closed stream is idempotent. A keyed
                // one that would apply gets `409` like any other write: its
                // repeat was answered by the gate above (#581 addendum 3).
                if kind == WriteKind::CloseOnly && gate.is_none() {
                    break 'append StreamResponse::Closed {
                        next_offset: stream.tail_offset,
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

            // Step 5 (#581 section 5.3): content checks, only for a write
            // that would apply.
            if let Some(fault) = fault {
                break 'append content_fault_response(fault);
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

            let message_count = match content_type {
                Some(value) => match canonical_json_message_count(value, payload) {
                    Ok(message_count) => message_count,
                    Err(NonCanonicalJsonPayload) => {
                        break 'append StreamResponse::error(
                            StreamErrorCode::InvalidRecordBoundaries,
                            "application/json append payload must use canonical newline boundaries",
                        );
                    }
                },
                None => 0,
            };

            if let Err(response) = check_stream_seq(stream, stream_seq.as_deref()) {
                break 'append response;
            }

            // W5: the write applies; record what the stream's current
            // history began from before its bytes or closure change.
            self.inherit_history(&stream_id);
            let Some(stream) = self.stream_metadata_mut(&stream_id) else {
                unreachable!("stream existence checked before producer evaluation");
            };
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
            let producer_ack = match (producer, gate) {
                (Some(producer), Some((key, advance))) => Some(self.record_producer_success(
                    &stream_id,
                    producer.producer_id,
                    key,
                    advance,
                    WriteReceipt {
                        start_offset: offset,
                        next_offset,
                        closed,
                    },
                    now_ms,
                )),
                _ => None,
            };

            if kind == WriteKind::CloseOnly {
                StreamResponse::Closed {
                    next_offset,
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
                    Self::appended_record_count(message_count, payload_len),
                );
                StreamResponse::Appended {
                    offset,
                    next_offset,
                    closed: close_after,
                    producer: producer_ack,
                }
            }
        };
        if let Some(stream_id) = enforce_on {
            self.enforce_producer_cap(&stream_id, now_ms);
        }
        response
    }

    pub(super) fn append_external(&mut self, input: AppendExternalInput<'_>) -> StreamResponse {
        let enforce_on = input.producer.as_ref().map(|_| input.stream_id.clone());
        let now_ms = input.now_ms;
        let response = self.append_external_unenforced(input);
        if let Some(stream_id) = enforce_on {
            self.enforce_producer_cap(&stream_id, now_ms);
        }
        response
    }

    fn append_external_unenforced(&mut self, input: AppendExternalInput<'_>) -> StreamResponse {
        let AppendExternalInput {
            stream_id,
            content_type,
            payload,
            message_count,
            close_after,
            stream_seq,
            producer,
            now_ms,
            check,
        } = input;
        if let Err(response) = validate_external_payload_ref(&payload) {
            return response;
        }
        if let Err(response) = self.validate_stream_scope(&stream_id) {
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
        let gate = match self.producer_gate(&stream_id, producer.as_ref(), check, now_ms) {
            Ok(ProducerGate::Repeat(ack)) => return self.repeat_response(&stream_id, ack, false),
            Ok(ProducerGate::Unkeyed) => None,
            Ok(ProducerGate::Apply(key, advance)) => Some((key, advance)),
            Err(response) => return response,
        };
        let fault = match producer.as_ref().map(producer_content_fault).transpose() {
            Ok(fault) => fault.flatten(),
            Err(response) => return response,
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
        // Step 5 (#581 section 5.3): content checks, only for a write that
        // would apply.
        if let Some(fault) = fault {
            return content_fault_response(fault);
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
        if let Err(response) =
            validate_message_count(&stream.content_type, payload.payload_len, message_count)
        {
            return response;
        }
        if let Err(response) = check_stream_seq(stream, stream_seq.as_deref()) {
            return response;
        }
        let offset = stream.tail_offset;
        let next_offset = offset.saturating_add(payload.payload_len);
        // W5, as for an inline append.
        self.inherit_history(&stream_id);
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
        let producer_ack = match (producer, gate) {
            (Some(producer), Some((key, advance))) => Some(self.record_producer_success(
                &stream_id,
                producer.producer_id,
                key,
                advance,
                WriteReceipt {
                    start_offset: offset,
                    next_offset,
                    closed,
                },
                now_ms,
            )),
            _ => None,
        };
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
            Self::appended_record_count(message_count, appended_bytes),
        );
        StreamResponse::Appended {
            offset,
            next_offset,
            closed: close_after,
            producer: producer_ack,
        }
    }

    /// Read-only: whether a write from `producer` would be answered as a
    /// repeat of a write that applied before, without mutating the stream.
    /// Lets admission control bypass backpressure for idempotent retries
    /// without previewing the write on a copy of the group (F9).
    pub fn append_would_deduplicate(
        &self,
        stream_id: &BucketStreamId,
        producer: Option<&ProducerRequest>,
        check: IncarnationCheck,
        now_ms: u64,
    ) -> bool {
        self.stream_is_live(stream_id, now_ms)
            && self.validate_stream_scope(stream_id).is_ok()
            && matches!(
                self.producer_gate(stream_id, producer, check, now_ms),
                Ok(ProducerGate::Repeat(_))
            )
    }

    /// Read-only: the answer to a tokened producer write that does not
    /// depend on its body (#581 section 2.5, W7): a repeat of a write that
    /// applied under `incarnation` (rows 9 and 10), or a key conflict
    /// (row 8). `None` when the write must be read and proposed: the stream
    /// is another incarnation or gone, or any other row decides.
    pub fn producer_early_answer(
        &self,
        stream_id: &BucketStreamId,
        producer: &ProducerRequest,
        incarnation: u64,
        now_ms: u64,
    ) -> Option<ProducerEarlyAnswer> {
        if !self.stream_is_live(stream_id, now_ms)
            || self.validate_stream_scope(stream_id).is_err()
            || self.stream_incarnation(stream_id) != Some(incarnation)
        {
            return None;
        }
        let key = producer_write_key(producer).ok()?;
        match self.evaluate_write(
            stream_id,
            &producer.producer_id,
            key,
            IncarnationCheck::Matched,
            now_ms,
        )? {
            Evaluation::Repeat(ack) => Some(ProducerEarlyAnswer::Repeat(ack)),
            Evaluation::Refuse(Refusal::Producer(ProducerRefusal::KeyConflict { epoch })) => {
                Some(ProducerEarlyAnswer::KeyConflict { epoch })
            }
            Evaluation::Claim { .. } | Evaluation::Next | Evaluation::Refuse(_) => None,
        }
    }

    /// Evaluates the write `key` of `producer_id` (#581 section 2.3.1). An
    /// idle record counts as retired, so a read-only caller sees the same
    /// floor that apply's retirement raises. `None` when the stream is gone.
    fn evaluate_write(
        &self,
        stream_id: &BucketStreamId,
        producer_id: &str,
        key: WriteKey,
        check: IncarnationCheck,
        now_ms: u64,
    ) -> Option<Evaluation> {
        let slot = self.stream_slot(stream_id)?;
        let record = slot.producers.get(producer_id);
        let (record, floor) = match record {
            Some(record) if super::producers::producer_is_idle(record, now_ms) => (
                None,
                Some(
                    slot.retired_epoch_watermark
                        .map_or(record.epoch, |floor| floor.max(record.epoch)),
                ),
            ),
            record => (record, slot.retired_epoch_watermark),
        };
        Some(evaluate(record, floor, self.history_id(), key, check))
    }

    /// Evaluates a write's producer key before it applies. A refusal is the
    /// `Err` response; nothing changed.
    fn producer_gate(
        &self,
        stream_id: &BucketStreamId,
        producer: Option<&ProducerRequest>,
        check: IncarnationCheck,
        now_ms: u64,
    ) -> Result<ProducerGate, StreamResponse> {
        let Some(producer) = producer else {
            return Ok(ProducerGate::Unkeyed);
        };
        let key = producer_write_key(producer)?;
        let Some(evaluation) =
            self.evaluate_write(stream_id, &producer.producer_id, key, check, now_ms)
        else {
            return Err(StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        };
        match evaluation {
            Evaluation::Claim { closes, opened } => {
                // F3 producer cap: a new producer needs room, either below the
                // cap or by evicting producers idle for an hour.
                if closes.is_none()
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
                Ok(ProducerGate::Apply(key, Advance::Claim { closes, opened }))
            }
            Evaluation::Next => Ok(ProducerGate::Apply(key, Advance::Next)),
            Evaluation::Repeat(ack) => Ok(ProducerGate::Repeat(ack)),
            Evaluation::Refuse(refusal) => {
                Err(refusal_response(&producer.producer_id, key, refusal))
            }
        }
    }

    /// The answer to a repeat of a producer write that applied before: an
    /// append's receipt, or a close's when the request is close-only.
    fn repeat_response(
        &self,
        stream_id: &BucketStreamId,
        ack: ProducerAck,
        close_only: bool,
    ) -> StreamResponse {
        let receipt = match ack.delivery {
            ProducerDelivery::Applied(receipt) | ProducerDelivery::Repeat(receipt) => receipt,
            // No range is kept: the tail stands in, and renders nothing.
            ProducerDelivery::Evicted => {
                let (tail, closed) = self
                    .stream_metadata(stream_id)
                    .map_or((0, false), |stream| {
                        (stream.tail_offset, stream.status == StreamStatus::Closed)
                    });
                WriteReceipt {
                    start_offset: tail,
                    next_offset: tail,
                    closed,
                }
            }
        };
        if close_only {
            return StreamResponse::Closed {
                next_offset: receipt.next_offset,
                producer: Some(ack),
            };
        }
        StreamResponse::Appended {
            offset: receipt.start_offset,
            next_offset: receipt.next_offset,
            closed: receipt.closed,
            producer: Some(ack),
        }
    }

    /// Records an applied producer write (rows 2, 6 and 12) and returns its
    /// answer.
    pub(super) fn record_producer_success(
        &mut self,
        stream_id: &BucketStreamId,
        producer_id: String,
        key: WriteKey,
        advance: Advance,
        receipt: WriteReceipt,
        now_ms: u64,
    ) -> ProducerAck {
        let history = self.history_id();
        let slot = self
            .stream_slot_mut(stream_id)
            .expect("stream existence checked before producer evaluation");
        match advance {
            Advance::Claim { closes, opened } => {
                let state = ProducerState::claimed(key, closes, opened, history, receipt, now_ms);
                let ack = state.applied_ack();
                slot.producers.insert(producer_id, state);
                ack
            }
            Advance::Next => {
                let state = slot
                    .producers
                    .get_mut(&producer_id)
                    .expect("row 12 evaluated against this record");
                state.advance(key, receipt, now_ms);
                state.applied_ack()
            }
        }
    }
}

/// How an applied producer write changes its record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Advance {
    /// Rows 2 and 6.
    Claim {
        closes: Option<ClosedEpoch>,
        opened: Opened,
    },
    /// Row 12.
    Next,
}

/// The outcome of evaluating a write's producer key before it applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProducerGate {
    /// A write without producer headers.
    Unkeyed,
    /// The write applies.
    Apply(WriteKey, Advance),
    /// The key applied before (rows 9 and 10).
    Repeat(ProducerAck),
}

/// The response to a refused producer write.
fn refusal_response(producer_id: &str, key: WriteKey, refusal: Refusal) -> StreamResponse {
    let refusal = match refusal {
        Refusal::SeqGap { expected, received } => {
            return StreamResponse::error_with_context(
                StreamErrorCode::ProducerSeqConflict,
                format!(
                    "producer '{producer_id}' expected sequence {expected}, received {received}"
                ),
                vec![StreamErrorContext::ProducerSeqConflict {
                    expected_seq: expected,
                    received_seq: received,
                }],
            );
        }
        Refusal::Producer(refusal) => refusal,
    };
    let message = match refusal {
        ProducerRefusal::EpochClosed { epoch, closed, .. } => format!(
            "producer '{producer_id}' epoch {} is closed after sequence {}; current epoch is {epoch}",
            closed.epoch, closed.seq
        ),
        ProducerRefusal::Expired { epoch } => format!(
            "producer '{producer_id}' epoch {} is not remembered; claim an epoch above {epoch}",
            key.epoch
        ),
        ProducerRefusal::StaleEpoch { epoch } => format!(
            "producer '{producer_id}' epoch {} is stale; current epoch is {epoch}",
            key.epoch
        ),
        ProducerRefusal::KeyConflict { epoch } => format!(
            "producer '{producer_id}' epoch {epoch} sequence {} holds a write with another identity",
            key.seq
        ),
        ProducerRefusal::EpochNotClaimed => format!(
            "producer '{producer_id}' new epoch {} must start at sequence 0",
            key.epoch
        ),
    };
    StreamResponse::error_with_context(refusal.code(), message, vec![StreamErrorContext::Producer(
        refusal,
    )])
}

fn check_stream_seq(stream: &StreamMetadata, incoming: Option<&str>) -> Result<(), StreamResponse> {
    let Some(incoming) = incoming else {
        return Ok(());
    };
    if let Some(last) = stream.last_stream_seq.as_deref()
        && incoming <= last
    {
        return Err(StreamResponse::error_with_next_offset_and_context(
            StreamErrorCode::StreamSeqConflict,
            format!("stream sequence '{incoming}' is not greater than last sequence '{last}'"),
            stream.tail_offset,
            vec![StreamErrorContext::LastStreamSeq {
                seq: last.to_owned(),
            }],
        ));
    }
    Ok(())
}
