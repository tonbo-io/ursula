//! Bucket/stream lifecycle: create, close, delete, and TTL expiry.

use std::num::NonZeroU32;

use super::AppendStreamInput;
use super::BucketStreamId;
use super::ColdGcTarget;
use super::CreateExternalStreamInput;
use super::CreateStreamInput;
use super::HashMap;
use super::HotBuffer;
use super::IncarnationCheck;
use super::ObjectPayloadRef;
use super::ProducerRequest;
use super::StreamColdState;
use super::StreamCommand;
use super::StreamErrorCode;
use super::StreamErrorContext;
use super::StreamMetadata;
use super::StreamResponse;
use super::StreamSlot;
use super::StreamStateMachine;
use super::StreamStatus;
use super::WriteKind;
use super::compare_stream_ids;
use super::renew_stream_ttl;
use super::stream_is_expired;
use super::validate_bucket_id;
use super::validate_external_payload_ref;
use super::validate_message_count;
use super::validate_stream_id;
use crate::usage::StreamEnd;
use crate::usage::StreamUsage;

impl StreamStateMachine {
    pub(super) fn create_bucket(&mut self, bucket_id: String) -> StreamResponse {
        if let Err(message) = validate_bucket_id(&bucket_id) {
            return StreamResponse::error(StreamErrorCode::InvalidBucketId, message);
        }
        if self.erased_buckets.contains(&bucket_id) {
            return StreamResponse::error(
                StreamErrorCode::BucketErased,
                format!("bucket '{bucket_id}' was permanently erased"),
            );
        }
        if !self.buckets.insert(bucket_id.clone()) {
            return StreamResponse::BucketAlreadyExists { bucket_id };
        }
        StreamResponse::BucketCreated { bucket_id }
    }

    /// Tenant offboarding: removes every stream the bucket owns in this
    /// group (each removal enqueues its cold-object prefixes for the GC
    /// worker), then the bucket, and installs a permanent erasure fence. The
    /// aggregate usage ledger is retained with zero gauges until an external
    /// accounting system has had a chance to observe its monotonic counters.
    /// Purging an absent bucket succeeds with zero removals so a crashed
    /// purge can be re-run to convergence.
    pub(super) fn purge_bucket(&mut self, bucket_id: &str) -> StreamResponse {
        if let Err(message) = validate_bucket_id(bucket_id) {
            return StreamResponse::error(StreamErrorCode::InvalidBucketId, message);
        }
        // In stream order, not the registry's: every replica removes them,
        // and gives their ends sequence numbers (U-B) and their objects
        // cold-GC entries, in the same order.
        let mut stream_ids = self
            .registry
            .stream_ids()
            .filter(|stream_id| stream_id.bucket_id == bucket_id)
            .cloned()
            .collect::<Vec<_>>();
        stream_ids.sort_by(compare_stream_ids);
        let mut removed_streams = 0u64;
        for stream_id in stream_ids {
            if self.remove_stream_state(&stream_id, StreamEnd::Purged) {
                removed_streams = removed_streams.saturating_add(1);
            }
        }
        self.buckets.remove(bucket_id);
        self.erased_buckets.insert(bucket_id.to_owned());
        StreamResponse::BucketPurged {
            bucket_id: bucket_id.to_owned(),
            removed_streams,
            pending_cold_gc_entries: u64::try_from(self.pending_cold_gc_len_for_bucket(bucket_id))
                .unwrap_or(u64::MAX),
        }
    }

    pub(super) fn create_stream(&mut self, input: CreateStreamInput) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(&input.stream_id) {
            return response;
        }
        if let Err(response) =
            validate_retention(input.stream_ttl_seconds, input.stream_expires_at_ms)
        {
            return response;
        }
        let initial_len = input.initial_len();
        if self
            .stream_metadata(&input.stream_id)
            .is_some_and(|existing| stream_is_expired(existing, input.now_ms))
        {
            self.remove_stream_state(&input.stream_id, StreamEnd::Expired);
        }

        if let Some(existing_slot) = self.stream_slot(&input.stream_id) {
            let existing = &existing_slot.metadata;
            if existing.content_type == input.content_type
                && existing.status == status_from_closed(input.close_after)
                && existing.stream_ttl_seconds == input.stream_ttl_seconds
                && existing.stream_expires_at_ms == input.stream_expires_at_ms
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

        let created_at_ms = match self.next_created_at_ms(input.now_ms) {
            Ok(created_at_ms) => created_at_ms,
            Err(response) => return response,
        };
        let metadata = StreamMetadata {
            stream_id: input.stream_id.clone(),
            content_type: input.content_type,
            status: status_from_closed(input.close_after),
            tail_offset: initial_len,
            last_stream_seq: input.stream_seq,
            stream_ttl_seconds: input.stream_ttl_seconds,
            stream_expires_at_ms: input.stream_expires_at_ms,
            created_at_ms,
            last_ttl_touch_at_ms: input.now_ms,
        };
        let hot_buffer = HotBuffer::from_payload(0, input.initial_payload);
        let stream_id = input.stream_id.clone();
        let slot = StreamSlot {
            metadata,
            hot_buffer,
            cold: StreamColdState::with_generation(created_at_ms),
            retained_offset: 0,
            visible_snapshot: None,
            producers: HashMap::new(),
            retired_epoch_watermark: None,
            history: self.history_id(),
            inherited: None,
            append_count: 0,
            usage: StreamUsage::default(),
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
        self.record_created_at_ms(created_at_ms);
        self.usage_on_write(
            &stream_id,
            initial_len,
            Self::appended_record_count(input.message_count, initial_len),
            input.now_ms,
        );
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
        if let Err(response) = validate_message_count(
            &input.content_type,
            input.initial_payload.payload_len,
            input.message_count,
        ) {
            return response;
        }
        if self
            .stream_metadata(&input.stream_id)
            .is_some_and(|existing| stream_is_expired(existing, input.now_ms))
        {
            self.remove_stream_state(&input.stream_id, StreamEnd::Expired);
        }

        if let Some(existing_slot) = self.stream_slot(&input.stream_id) {
            let existing = &existing_slot.metadata;
            if existing.content_type == input.content_type
                && existing.status == status_from_closed(input.close_after)
                && existing.stream_ttl_seconds == input.stream_ttl_seconds
                && existing.stream_expires_at_ms == input.stream_expires_at_ms
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
        let created_at_ms = match self.next_created_at_ms(input.now_ms) {
            Ok(created_at_ms) => created_at_ms,
            Err(response) => return response,
        };
        let metadata = StreamMetadata {
            stream_id: input.stream_id.clone(),
            content_type: input.content_type,
            status: status_from_closed(input.close_after),
            tail_offset: initial_len,
            last_stream_seq: input.stream_seq,
            stream_ttl_seconds: input.stream_ttl_seconds,
            stream_expires_at_ms: input.stream_expires_at_ms,
            created_at_ms,
            last_ttl_touch_at_ms: input.now_ms,
        };
        let object = ObjectPayloadRef {
            start_offset: 0,
            end_offset: initial_len,
            s3_path: input.initial_payload.s3_path,
            object_size: input.initial_payload.object_size,
            object_offset: 0,
        };
        let mut cold = StreamColdState::with_generation(created_at_ms);
        // The payload was staged before apply chose the incarnation, so no
        // page of this generation can reference it; keep it in state.
        cold.push_direct_external_segment(object.clone());
        let stream_id = input.stream_id.clone();
        let slot = StreamSlot {
            metadata,
            hot_buffer: HotBuffer::default(),
            cold,
            retained_offset: 0,
            visible_snapshot: None,
            producers: HashMap::new(),
            retired_epoch_watermark: None,
            history: self.history_id(),
            inherited: None,
            append_count: 0,
            usage: StreamUsage::default(),
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
        self.record_created_at_ms(created_at_ms);
        self.usage_on_write(
            &stream_id,
            initial_len,
            Self::appended_record_count(input.message_count, initial_len),
            input.now_ms,
        );
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
        check: IncarnationCheck,
    ) -> StreamResponse {
        self.append_borrowed(AppendStreamInput {
            stream_id,
            content_type: None,
            payload: &[],
            close_after: true,
            stream_seq,
            producer,
            now_ms,
            kind: WriteKind::CloseOnly,
            check,
        })
    }

    /// Deletes a stream. A stream expired at `now_ms` is already gone to
    /// every reader: the delete removes it all the same and answers
    /// `Deleted { expired: true }`, which a client sees as 404, as for a
    /// missing stream, so the answer does not depend on whether the expiry
    /// sweep removed it first.
    pub(super) fn delete_stream(
        &mut self,
        stream_id: &BucketStreamId,
        now_ms: u64,
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
        let expired = stream_is_expired(stream, now_ms);
        let end = if expired {
            StreamEnd::Expired
        } else {
            StreamEnd::Deleted
        };
        self.remove_stream_state(stream_id, end);
        StreamResponse::Deleted { expired }
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
            self.remove_stream_state(stream_id, StreamEnd::Expired);
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
            self.remove_stream_state(stream_id, StreamEnd::Expired);
            return true;
        }
        false
    }

    /// Applies [`StreamCommand::ExpireStreams`]: removes up to `limit`
    /// streams expired at `now_ms`, in the TTL index's (expiry, bucket,
    /// stream) order. Every side effect of a removal (cold GC, usage, the
    /// removal tally) happens here, as for any removal.
    pub(super) fn expire_streams(&mut self, now_ms: u64, limit: NonZeroU32) -> StreamResponse {
        let limit = u64::from(limit.get());
        let mut removed = 0u64;
        while removed < limit {
            let Some(stream_id) = self.registry.pop_expired(now_ms) else {
                break;
            };
            if self.remove_stream_state(&stream_id, StreamEnd::Expired) {
                removed = removed.saturating_add(1);
            }
        }
        StreamResponse::StreamsExpired { removed }
    }

    /// Removes a stream, which ended as `end`: the one path of every
    /// removal, so each one queues its cold objects, moves its final usage
    /// to the ledger's ended streams (U-B) and counts in the removal tally.
    pub(super) fn remove_stream_state(
        &mut self,
        stream_id: &BucketStreamId,
        end: StreamEnd,
    ) -> bool {
        let Some(slot) = self.registry.remove(stream_id) else {
            return false;
        };
        self.record_removed(&slot);
        self.usage_on_end(&slot, end);
        self.flush_planner.unmark_hot(stream_id);
        self.remove_hot_payload_bytes(
            u64::try_from(slot.hot_buffer.len()).expect("payload len fits u64"),
        );
        let shared_paths = slot
            .cold
            .shared_object_paths()
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        // The cold objects we wrote for this stream are now unreferenced.
        // Enqueue the whole prefix for the background GC worker to reclaim;
        // A prefix sweep is safe and keeps the queue O(streams), not O(chunks).
        // F18 step 2: any byte of `[0, tail)` the hot buffer does not hold
        // may have been written to cold storage.
        let has_cold_objects = u64::try_from(slot.hot_buffer.len()).unwrap_or(u64::MAX)
            < slot.metadata.tail_offset
            || slot.cold.has_state_refs();
        if has_cold_objects {
            // The entry names the removed incarnation's generation, so the
            // worker deletes only that incarnation's objects, its external
            // payloads included (F14a, F14g step 2).
            self.cold_gc.enqueue_stream(
                stream_id.bucket_id.clone(),
                stream_id.clone(),
                slot.cold.cold_generation(),
            );
            let direct_external_paths = slot
                .cold
                .external_segments()
                .iter()
                .map(|object| object.s3_path.clone())
                .collect::<Vec<_>>();
            if !direct_external_paths.is_empty() {
                self.cold_gc.enqueue(
                    stream_id.bucket_id.clone(),
                    ColdGcTarget::Paths(direct_external_paths),
                );
            }
        }
        // A cold snapshot body is referenced only by state (F16).
        if let Some(object) = slot
            .visible_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.object.as_ref())
        {
            self.cold_gc.enqueue(
                stream_id.bucket_id.clone(),
                ColdGcTarget::Paths(vec![object.s3_path.clone()]),
            );
        }
        self.release_shared_cold_objects(&stream_id.bucket_id, shared_paths, 0);
        true
    }

    /// Counts a removed stream and what it held in a group snapshot (#669).
    fn record_removed(&mut self, slot: &StreamSlot) {
        let hot = u64::try_from(slot.hot_buffer.len()).unwrap_or(u64::MAX);
        let inline_snapshot = slot
            .visible_snapshot
            .as_ref()
            .filter(|snapshot| snapshot.object.is_none())
            .map_or(0, |snapshot| {
                u64::try_from(snapshot.payload.len()).unwrap_or(u64::MAX)
            });
        self.removed.streams = self.removed.streams.saturating_add(1);
        self.removed.bytes = self
            .removed
            .bytes
            .saturating_add(hot)
            .saturating_add(inline_snapshot)
            .saturating_add(crate::COMMAND_LOG_OVERHEAD_BYTES);
    }

    /// The `Stream-Incarnation` precondition (D12) of `command` at apply
    /// time: `None` when it may apply, else the refusal, with nothing
    /// changed. It applies when the stream's token in the current history
    /// is `expected`, so a write that names a token of an earlier history
    /// (a recreate, a restore, an accepted loss) never applies.
    /// Evaluation follows RFC 9110 §13.2: a command on a missing (or
    /// expired) stream applies and answers its usual 404, except a create,
    /// which is refused so that it creates nothing; a stream of any other
    /// incarnation refuses it, naming the current one. A create whose ids
    /// are invalid, or whose bucket was erased, keeps its own error.
    /// Commands that never carry the precondition apply unchanged.
    pub fn incarnation_precondition(
        &self,
        command: &StreamCommand,
        expected: u64,
    ) -> Option<StreamResponse> {
        if crate::sim_controls::off(crate::sim_controls::Defence::IncarnationCheck) {
            return None;
        }
        let (stream_id, now_ms, create) = match command {
            StreamCommand::CreateStream {
                stream_id, now_ms, ..
            }
            | StreamCommand::CreateExternal {
                stream_id, now_ms, ..
            } => (stream_id, *now_ms, true),
            StreamCommand::Append {
                stream_id, now_ms, ..
            }
            | StreamCommand::AppendExternal {
                stream_id, now_ms, ..
            }
            | StreamCommand::PublishSnapshot {
                stream_id, now_ms, ..
            }
            | StreamCommand::PublishSnapshotExternal {
                stream_id, now_ms, ..
            }
            | StreamCommand::AdvanceRetention {
                stream_id, now_ms, ..
            }
            | StreamCommand::Close {
                stream_id, now_ms, ..
            }
            | StreamCommand::DeleteStream { stream_id, now_ms } => (stream_id, *now_ms, false),
            _ => return None,
        };
        let current = self
            .stream_metadata(stream_id)
            .filter(|stream| !stream_is_expired(stream, now_ms))
            .map(|stream| self.token_of(stream));
        match current {
            Some(current) if current == expected => None,
            Some(current) => Some(StreamResponse::error_with_context(
                StreamErrorCode::IncarnationMismatch,
                format!("stream '{stream_id}' is incarnation {current}, not {expected}"),
                vec![StreamErrorContext::StreamIncarnation {
                    incarnation: current,
                }],
            )),
            None if create
                && validate_bucket_id(&stream_id.bucket_id).is_ok()
                && validate_stream_id(stream_id).is_ok()
                && !self.erased_buckets.contains(&stream_id.bucket_id) =>
            {
                Some(StreamResponse::error(
                    StreamErrorCode::IncarnationMismatch,
                    format!(
                        "stream '{stream_id}' does not exist, so it is not incarnation {expected}"
                    ),
                ))
            }
            None => None,
        }
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
            if self.erased_buckets.contains(&stream_id.bucket_id) {
                return Err(StreamResponse::error(
                    StreamErrorCode::BucketErased,
                    format!("bucket '{}' was permanently erased", stream_id.bucket_id),
                ));
            }
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
