#![expect(
    clippy::arithmetic_side_effects,
    reason = "pre-existing arithmetic debt; see Known debt in AGENTS.md"
)]
use std::sync::Arc;

use ursula_shard::BucketStreamId;
use ursula_shard::ShardPlacement;
use ursula_stream::AppendStreamInput;
use ursula_stream::ColdFlushPassRequest;
use ursula_stream::ObjectPayloadRef;
use ursula_stream::ProducerRequest;
use ursula_stream::SharedRefCandidate;
use ursula_stream::SharedRefCompactionRequest;
use ursula_stream::SharedRefIdleTracker;
use ursula_stream::StreamCommand;
use ursula_stream::StreamErrorCode;
use ursula_stream::StreamMessageRecord;
use ursula_stream::StreamReadColdIndexSegment;
use ursula_stream::StreamReadPlan;
use ursula_stream::StreamReadSegment;
use ursula_stream::StreamResponse;
use ursula_stream::StreamSnapshot;
use ursula_stream::StreamStateMachine;

use super::GroupAckColdGcFuture;
use super::GroupAdvanceRetentionFuture;
use super::GroupAppendFuture;
use super::GroupBootstrapStreamFuture;
use super::GroupBucketUsageFuture;
use super::GroupCloseStreamFuture;
use super::GroupColdHotBacklogFuture;
use super::GroupCompactColdFuture;
use super::GroupCreateStreamFuture;
use super::GroupDeferColdGcFuture;
use super::GroupDeleteStreamFuture;
use super::GroupEngine;
use super::GroupEngineCreateFuture;
use super::GroupEngineError;
use super::GroupEngineFactory;
use super::GroupEngineMetrics;
use super::GroupFlushColdFuture;
use super::GroupHeadStreamFuture;
use super::GroupInstallSnapshotFuture;
use super::GroupPlanColdFlushFuture;
use super::GroupPlanColdGcFuture;
use super::GroupPlanColdOrphanSweepFuture;
use super::GroupPlanNextColdFlushBatchFuture;
use super::GroupPlanSharedRefCompactionFuture;
use super::GroupPublishSnapshotFuture;
use super::GroupPurgeBucketFuture;
use super::GroupReadSnapshotFuture;
use super::GroupReadStreamFuture;
use super::GroupReadStreamPartsFuture;
use super::GroupRepairColdIndexFuture;
use super::GroupSnapshotFuture;
use super::GroupStateGaugesFuture;
use super::GroupTidyStreamFuture;
use super::GroupTidyStreamsFuture;
use super::GroupTouchStreamAccessFuture;
use super::GroupWriteResponse;
use crate::cold_index::ColdIndexPageCache;
use crate::cold_index::ColdIndexPageKey;
use crate::cold_index::ColdIndexRepairInput;
use crate::cold_index::ColdIndexRepairReport;
use crate::cold_index::ColdStoreColdIndexPageStore;
use crate::cold_index::RepairColdIndexRequest;
use crate::cold_index::RepairColdIndexResponse;
use crate::cold_index::clipped_entries;
use crate::cold_index::repair_cold_index_streams;
use crate::cold_index::replace_cold_chunk_index_pages_with_rollback_in_generation;
use crate::cold_index::rollback_cold_index_pages;
use crate::cold_index::write_cold_chunk_index_pages_with_rollback_in_generation;
use crate::cold_refs::ColdOrphanSweepPlan;
use crate::cold_refs::ColdOrphanSweepRequest;
use crate::cold_refs::ColdOrphanSweepStream;
use crate::cold_store::ColdStoreHandle;
use crate::cold_store::DEFAULT_CONTENT_TYPE;
use crate::cold_store::cold_pack_dir;
use crate::command::GroupSnapshot;
use crate::command::GroupWriteCommand;
use crate::request::AckColdGcResponse;
use crate::request::AdvanceRetentionRequest;
use crate::request::AdvanceRetentionResponse;
use crate::request::AppendExternalRequest;
use crate::request::AppendRequest;
use crate::request::AppendResponse;
use crate::request::BootstrapStreamRequest;
use crate::request::BootstrapStreamResponse;
use crate::request::BootstrapUpdate;
use crate::request::CloseStreamRequest;
use crate::request::CloseStreamResponse;
use crate::request::ColdHotBacklog;
use crate::request::ColdWriteAdmission;
use crate::request::CompactColdRequest;
use crate::request::CompactColdResponse;
use crate::request::CreateStreamExternalRequest;
use crate::request::CreateStreamRequest;
use crate::request::CreateStreamResponse;
use crate::request::DeferColdGcResponse;
use crate::request::DeleteStreamRequest;
use crate::request::DeleteStreamResponse;
use crate::request::FlushColdRequest;
use crate::request::FlushColdResponse;
use crate::request::GroupReadStreamParts;
use crate::request::HeadStreamRequest;
use crate::request::HeadStreamResponse;
use crate::request::ImportGroupStateRequest;
use crate::request::ImportGroupStateResponse;
use crate::request::PlanColdFlushRequest;
use crate::request::PlanGroupColdFlushRequest;
use crate::request::PublishSnapshotRequest;
use crate::request::PublishSnapshotResponse;
use crate::request::PurgeBucketResponse;
use crate::request::ReadSnapshotRequest;
use crate::request::ReadSnapshotResponse;
use crate::request::ReadStreamRequest;
use crate::request::StreamAppendCount;
use crate::request::TidyStreamsRequest;
use crate::request::TidyStreamsResponse;
use crate::request::TouchStreamAccessResponse;
use crate::request::WriteHotBacklog;
use crate::retention_gc::RetentionGcTarget;
use crate::retention_gc::RetentionGcTracker;
use crate::retention_gc::collect_retained_cold_objects;

pub(crate) struct AppendPayloadInput<'a> {
    stream_id: BucketStreamId,
    content_type: Option<&'a str>,
    payload: &'a [u8],
    close_after: bool,
    stream_seq: Option<String>,
    producer: Option<ProducerRequest>,
    now_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryGroupEngine {
    pub(crate) commit_index: u64,
    pub(crate) state_machine: StreamStateMachine,
    pub(crate) cold_store: Option<ColdStoreHandle>,
    pub(crate) cold_index_cache: Option<Arc<ColdIndexPageCache<ColdStoreColdIndexPageStore>>>,
    /// Read plans issued while serving `/bootstrap`; one per request
    /// (bounded-stream-state F11). Node-local and not replicated.
    pub(crate) bootstrap_read_plans: u64,
    /// Leader-local tail tracker of the shared-ref compaction driver (F2).
    /// Not replicated; a new leader starts it empty.
    pub(crate) shared_ref_idle: SharedRefIdleTracker,
    /// Leader-local grace clock of retention GC (F14f). Not replicated; a
    /// new leader starts it empty.
    pub(crate) retention_gc: RetentionGcTracker,
}

impl InMemoryGroupEngine {
    pub fn with_cold_store(cold_store: ColdStoreHandle) -> Self {
        let mut engine = Self::default();
        engine.set_cold_store(Some(cold_store));
        engine
    }

    pub fn cold_store(&self) -> Option<ColdStoreHandle> {
        self.cold_store.clone()
    }

    /// The cold-index page cache this engine invalidates when replicated
    /// cold commands apply. Read paths outside the engine (the Raft group
    /// handle) share it, so apply-time invalidation reaches their reads on
    /// every replica.
    pub fn cold_index_cache(&self) -> Option<Arc<ColdIndexPageCache<ColdStoreColdIndexPageStore>>> {
        self.cold_index_cache.clone()
    }

    pub(crate) fn set_cold_store(&mut self, cold_store: Option<ColdStoreHandle>) {
        self.cold_index_cache = cold_store.as_ref().map(|cold_store| {
            Arc::new(ColdIndexPageCache::new(
                Arc::new(ColdStoreColdIndexPageStore::new(cold_store.clone())),
                1024,
            ))
        });
        self.cold_store = cold_store;
    }

    pub fn apply_committed_write(
        &mut self,
        command: GroupWriteCommand,
        placement: ShardPlacement,
    ) -> Result<GroupWriteResponse, GroupEngineError> {
        match command {
            GroupWriteCommand::Stream(command) => self.apply_stream_command(command, placement),
        }
    }

    /// Applies one canonical [`StreamCommand`] to the deterministic state
    /// machine and lifts its [`StreamResponse`] into the group-level response,
    /// maintaining the group commit index and per-stream append counts.
    pub fn apply_stream_command(
        &mut self,
        command: StreamCommand,
        placement: ShardPlacement,
    ) -> Result<GroupWriteResponse, GroupEngineError> {
        match command {
            // The `Stream-Incarnation` precondition (D12) is checked before
            // anything the wrapped command does here (a create's implicit
            // bucket included), so a refusal changes nothing.
            StreamCommand::IfIncarnation {
                incarnation,
                command,
            } => {
                if let Some(refusal) = self
                    .state_machine
                    .incarnation_precondition(&command, incarnation)
                {
                    return self.group_response_from_stream(refusal, None, placement);
                }
                self.apply_stream_command(*command, placement)
            }
            // Appends skip `StreamStateMachine::apply` to keep the exact
            // borrowed fast path (no TTL sweep on the append hot path).
            StreamCommand::Append {
                stream_id,
                content_type,
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => self
                .append_payload(
                    AppendPayloadInput {
                        stream_id,
                        content_type: content_type.as_deref(),
                        payload: &payload,
                        close_after,
                        stream_seq,
                        producer,
                        now_ms,
                    },
                    placement,
                )
                .map(GroupWriteResponse::Append),
            command => {
                let stream_id = command_stream_id(&command);
                // Commands whose pages the leader rewrote before proposing:
                // compaction and F5 offloads (which may clip entries), plus
                // external appends and creates (bounded-state F13). Every
                // replica drops the stream's cached pages, so a page cached
                // earlier (possibly holding a stale entry over the same
                // offsets) is reloaded.
                let compacted_stream_id = match &command {
                    StreamCommand::CompactCold { stream_id, .. }
                    | StreamCommand::AppendExternal { stream_id, .. }
                    | StreamCommand::CreateExternal { stream_id, .. }
                    | StreamCommand::OffloadColdRefs { stream_id, .. } => Some(stream_id.clone()),
                    _ => None,
                };
                // Pages an exclusive cold flush rewrote (and possibly clipped)
                // on the leader; every replica drops its cached copies.
                let flushed_range = match &command {
                    StreamCommand::FlushCold {
                        stream_id, chunk, ..
                    } if !chunk.shared_object => {
                        Some((stream_id.clone(), chunk.start_offset, chunk.end_offset))
                    }
                    _ => None,
                };
                if let StreamCommand::CreateStream { stream_id, .. }
                | StreamCommand::CreateExternal { stream_id, .. } = &command
                {
                    ensure_bucket_exists(&mut self.state_machine, stream_id)?;
                }
                let response = self.state_machine.apply(command);
                let response = self.group_response_from_stream(response, stream_id, placement);
                if response.is_ok()
                    && let (Some(cache), Some(stream_id)) =
                        (self.cold_index_cache.as_ref(), compacted_stream_id.as_ref())
                {
                    cache.invalidate_stream(stream_id);
                }
                if response.is_ok()
                    && let (Some(cache), Some((stream_id, start_offset, end_offset))) =
                        (self.cold_index_cache.as_ref(), flushed_range.as_ref())
                    && let Some(generation) = self.state_machine.cold_index_generation(stream_id)
                {
                    cache.invalidate_range(stream_id, generation, *start_offset, *end_offset);
                }
                response
            }
        }
    }

    /// The stream's and the group's hot bytes after a write (F6a); a missing
    /// or deleted stream holds none.
    fn write_hot_backlog(&self, stream_id: Option<&BucketStreamId>) -> WriteHotBacklog {
        WriteHotBacklog {
            stream_hot_bytes: stream_id
                .and_then(|stream_id| self.state_machine.hot_payload_len(stream_id).ok())
                .unwrap_or(0),
            group_hot_bytes: self.state_machine.total_hot_payload_bytes(),
        }
    }

    /// Lifts a [`StreamResponse`] into the matching [`GroupWriteResponse`],
    /// advancing the group commit index for every mutating outcome.
    fn group_response_from_stream(
        &mut self,
        response: StreamResponse,
        stream_id: Option<BucketStreamId>,
        placement: ShardPlacement,
    ) -> Result<GroupWriteResponse, GroupEngineError> {
        match response {
            StreamResponse::Created {
                next_offset,
                closed,
                ..
            } => {
                let stream_id = require_response_stream_id(stream_id, "created")?;
                self.commit_index += 1;
                Ok(GroupWriteResponse::CreateStream(CreateStreamResponse {
                    placement,
                    next_offset,
                    closed,
                    already_exists: false,
                    group_commit_index: self.commit_index,
                    hot_backlog: Some(self.write_hot_backlog(Some(&stream_id))),
                    incarnation: response_incarnation(&self.state_machine, Some(&stream_id)),
                }))
            }
            StreamResponse::AlreadyExists {
                next_offset,
                closed,
                ..
            } => Ok(GroupWriteResponse::CreateStream(CreateStreamResponse {
                placement,
                next_offset,
                closed,
                already_exists: true,
                group_commit_index: self.commit_index,
                hot_backlog: Some(self.write_hot_backlog(stream_id.as_ref())),
                incarnation: response_incarnation(&self.state_machine, stream_id.as_ref()),
            })),
            StreamResponse::Appended {
                offset,
                next_offset,
                closed,
                deduplicated,
                producer,
                receipt_evicted,
            } => {
                let stream_id = require_response_stream_id(stream_id, "appended")?;
                let stream_hot_bytes = self.state_machine.hot_payload_len(&stream_id).unwrap_or(0);
                let group_hot_bytes = self.state_machine.total_hot_payload_bytes();
                if !deduplicated {
                    self.commit_index += 1;
                    self.state_machine.add_stream_append_count(&stream_id, 1);
                }
                let stream_append_count = self.state_machine.stream_append_count(&stream_id);
                Ok(GroupWriteResponse::Append(AppendResponse {
                    placement,
                    start_offset: offset,
                    next_offset,
                    stream_append_count,
                    group_commit_index: self.commit_index,
                    closed,
                    deduplicated,
                    producer,
                    stream_hot_bytes,
                    group_hot_bytes,
                    receipt_evicted,
                    incarnation: response_incarnation(&self.state_machine, Some(&stream_id)),
                }))
            }
            StreamResponse::SnapshotPublished {
                snapshot_offset,
                snapshot_digest,
            } => {
                self.commit_index += 1;
                Ok(GroupWriteResponse::PublishSnapshot(
                    PublishSnapshotResponse {
                        placement,
                        snapshot_offset,
                        snapshot_digest,
                        group_commit_index: self.commit_index,
                        hot_backlog: Some(self.write_hot_backlog(stream_id.as_ref())),
                        incarnation: response_incarnation(&self.state_machine, stream_id.as_ref()),
                    },
                ))
            }
            StreamResponse::StreamTidied { debt_remaining } => {
                require_response_stream_id(stream_id, "tidied")?;
                self.commit_index += 1;
                Ok(GroupWriteResponse::TidyStream(
                    crate::request::TidyStreamResponse {
                        placement,
                        debt_remaining,
                        group_commit_index: self.commit_index,
                    },
                ))
            }
            StreamResponse::RetentionAdvanced { retained_offset } => {
                self.commit_index += 1;
                Ok(GroupWriteResponse::AdvanceRetention(
                    AdvanceRetentionResponse {
                        placement,
                        retained_offset,
                        group_commit_index: self.commit_index,
                        hot_backlog: Some(self.write_hot_backlog(stream_id.as_ref())),
                        incarnation: response_incarnation(&self.state_machine, stream_id.as_ref()),
                    },
                ))
            }
            StreamResponse::Accessed { changed, expired } => {
                if changed || expired {
                    self.commit_index += 1;
                }
                Ok(GroupWriteResponse::TouchStreamAccess(
                    TouchStreamAccessResponse {
                        placement,
                        changed,
                        expired,
                        group_commit_index: self.commit_index,
                    },
                ))
            }
            StreamResponse::SnapshotImported { buckets, streams } => {
                self.commit_index += 1;
                Ok(GroupWriteResponse::ImportGroupState(
                    ImportGroupStateResponse {
                        placement,
                        buckets,
                        streams,
                        group_commit_index: self.commit_index,
                    },
                ))
            }
            StreamResponse::ColdFlushed { hot_start_offset } => {
                self.commit_index += 1;
                Ok(GroupWriteResponse::FlushCold(FlushColdResponse {
                    placement,
                    hot_start_offset,
                    group_commit_index: self.commit_index,
                    hot_backlog: Some(self.write_hot_backlog(stream_id.as_ref())),
                }))
            }
            StreamResponse::ColdCompacted {
                compacted_chunks,
                compacted_bytes,
            } => {
                self.commit_index += 1;
                Ok(GroupWriteResponse::CompactCold(CompactColdResponse {
                    placement,
                    compacted_chunks,
                    compacted_bytes,
                    group_commit_index: self.commit_index,
                }))
            }
            StreamResponse::Closed {
                next_offset,
                deduplicated,
                ..
            } => {
                let stream_id = require_response_stream_id(stream_id, "closed")?;
                if !deduplicated {
                    self.commit_index += 1;
                }
                Ok(GroupWriteResponse::CloseStream(CloseStreamResponse {
                    placement,
                    next_offset,
                    group_commit_index: self.commit_index,
                    deduplicated,
                    incarnation: response_incarnation(&self.state_machine, Some(&stream_id)),
                }))
            }
            StreamResponse::Deleted => {
                let stream_id = require_response_stream_id(stream_id, "deleted")?;
                self.commit_index += 1;
                Ok(GroupWriteResponse::DeleteStream(DeleteStreamResponse {
                    placement,
                    group_commit_index: self.commit_index,
                    hot_backlog: Some(self.write_hot_backlog(Some(&stream_id))),
                }))
            }
            StreamResponse::ColdGcAcked { removed } => {
                self.commit_index += 1;
                Ok(GroupWriteResponse::AckColdGc(AckColdGcResponse {
                    placement,
                    removed,
                    group_commit_index: self.commit_index,
                }))
            }
            StreamResponse::ColdRefsOffloaded { removed, remaining } => {
                require_response_stream_id(stream_id, "cold refs offloaded")?;
                self.commit_index += 1;
                Ok(GroupWriteResponse::OffloadColdRefs(
                    crate::cold_refs::OffloadStreamColdRefsResponse {
                        placement,
                        removed,
                        remaining,
                        group_commit_index: self.commit_index,
                    },
                ))
            }
            StreamResponse::ColdGcDeferred { new_seq } => {
                self.commit_index += 1;
                Ok(GroupWriteResponse::DeferColdGc(DeferColdGcResponse {
                    placement,
                    new_seq,
                    group_commit_index: self.commit_index,
                }))
            }
            StreamResponse::BucketPurged {
                bucket_id: _,
                removed_streams,
                pending_cold_gc_entries,
            } => {
                self.commit_index += 1;
                Ok(GroupWriteResponse::PurgeBucket(PurgeBucketResponse {
                    placement,
                    removed_streams,
                    pending_cold_gc_entries,
                    group_commit_index: self.commit_index,
                }))
            }
            StreamResponse::Error {
                code,
                message,
                next_offset,
                context,
            } => Err(GroupEngineError::stream_with_context(
                code,
                message,
                next_offset,
                context,
            )),
            other @ (StreamResponse::BucketCreated { .. }
            | StreamResponse::BucketAlreadyExists { .. }) => Err(GroupEngineError::new(format!(
                "unexpected group write response: {other:?}"
            ))),
        }
    }

    pub(crate) fn cold_hot_backlog_for(
        &self,
        stream_id: BucketStreamId,
    ) -> Result<ColdHotBacklog, GroupEngineError> {
        let stream_hot_bytes = self.state_machine.hot_payload_len(&stream_id).unwrap_or(0);
        Ok(ColdHotBacklog {
            stream_id,
            stream_hot_bytes,
            group_hot_bytes: self.state_machine.total_hot_payload_bytes(),
        })
    }

    /// Cold admission (F6c): the group's hot payload plus the incoming
    /// payload must stay within the group cap. The hot window keeps no
    /// per-message bookkeeping, so a write is charged its payload only.
    pub fn check_cold_write_admission(
        &self,
        stream_id: &BucketStreamId,
        admission: ColdWriteAdmission,
        incoming_bytes: u64,
    ) -> Result<(), GroupEngineError> {
        let Some(limit) = admission.max_hot_bytes_per_group else {
            return Ok(());
        };
        if incoming_bytes == 0 {
            return Ok(());
        }
        let before = self.state_machine.total_hot_payload_bytes();
        let after = before.saturating_add(incoming_bytes);
        if after <= limit {
            return Ok(());
        }
        Err(GroupEngineError::cold_backpressure(
            stream_id.clone(),
            before,
            after,
            limit,
        ))
    }

    pub(crate) fn create_stream_with_admission_inner(
        &mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<CreateStreamResponse, GroupEngineError> {
        // F9: O(1) admission instead of previewing the write on a copy of the
        // group. A create of a live stream is answered without adding hot
        // bytes (already-exists or a conflict), so it bypasses admission.
        if admission.is_enabled()
            && !self
                .state_machine
                .stream_is_live(&request.stream_id, request.now_ms)
        {
            self.check_cold_write_admission(
                &request.stream_id,
                admission,
                u64::try_from(request.initial_payload.len()).unwrap_or(u64::MAX),
            )?;
        }
        let response =
            match self.apply_committed_write(GroupWriteCommand::from(request), placement)? {
                GroupWriteResponse::CreateStream(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected create stream write response: {other:?}"
                    )));
                }
            };
        Ok(response)
    }

    pub(crate) fn append_with_admission_inner(
        &mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<AppendResponse, GroupEngineError> {
        // F9: O(1) admission; a deduplicated producer retry adds no hot bytes
        // and bypasses it, decided read-only.
        if admission.is_enabled()
            && !self.state_machine.append_would_deduplicate(
                &request.stream_id,
                request.producer.as_ref(),
                request.now_ms,
            )
        {
            self.check_cold_write_admission(
                &request.stream_id,
                admission,
                u64::try_from(request.payload.len()).unwrap_or(u64::MAX),
            )?;
        }
        let response =
            match self.apply_committed_write(GroupWriteCommand::from(request), placement)? {
                GroupWriteResponse::Append(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected append write response: {other:?}"
                    )));
                }
            };
        Ok(response)
    }

    pub fn access_requires_write(
        &self,
        stream_id: &BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
    ) -> Result<bool, GroupEngineError> {
        self.state_machine
            .access_requires_write(stream_id, now_ms, renew_ttl)
            .map_err(stream_response_error)
    }

    pub(crate) fn apply_access_command(
        &mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> Result<TouchStreamAccessResponse, GroupEngineError> {
        match self.apply_committed_write(
            GroupWriteCommand::Stream(StreamCommand::TouchStreamAccess {
                stream_id,
                now_ms,
                renew_ttl,
            }),
            placement,
        )? {
            GroupWriteResponse::TouchStreamAccess(response) => Ok(response),
            other => Err(GroupEngineError::new(format!(
                "unexpected touch stream access write response: {other:?}"
            ))),
        }
    }

    pub(crate) fn ensure_stream_access(
        &mut self,
        stream_id: &BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> Result<Option<TouchStreamAccessResponse>, GroupEngineError> {
        if !self.access_requires_write(stream_id, now_ms, renew_ttl)? {
            return Ok(None);
        }
        let response =
            self.apply_access_command(stream_id.clone(), now_ms, renew_ttl, placement)?;
        if response.expired {
            return Err(GroupEngineError::stream(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        }
        Ok(Some(response))
    }

    pub(crate) fn append_payload(
        &mut self,
        input: AppendPayloadInput<'_>,
        placement: ShardPlacement,
    ) -> Result<AppendResponse, GroupEngineError> {
        let AppendPayloadInput {
            stream_id,
            content_type,
            payload,
            close_after,
            stream_seq,
            producer,
            now_ms,
        } = input;
        let stream_count_key = stream_id.clone();
        let response = self.state_machine.append_borrowed(AppendStreamInput {
            stream_id,
            content_type,
            payload,
            close_after,
            stream_seq,
            producer,
            now_ms,
        });
        self.append_response_from_stream(stream_count_key, response, placement)
    }

    fn append_response_from_stream(
        &mut self,
        stream_id: BucketStreamId,
        response: StreamResponse,
        placement: ShardPlacement,
    ) -> Result<AppendResponse, GroupEngineError> {
        match response {
            StreamResponse::Appended {
                offset,
                next_offset,
                closed,
                deduplicated,
                producer,
                receipt_evicted,
            } => {
                let stream_hot_bytes = self.state_machine.hot_payload_len(&stream_id).unwrap_or(0);
                let group_hot_bytes = self.state_machine.total_hot_payload_bytes();
                if !deduplicated {
                    self.commit_index += 1;
                    self.state_machine.add_stream_append_count(&stream_id, 1);
                }
                let stream_append_count = self.state_machine.stream_append_count(&stream_id);
                Ok(AppendResponse {
                    placement,
                    start_offset: offset,
                    next_offset,
                    stream_append_count,
                    group_commit_index: self.commit_index,
                    closed,
                    deduplicated,
                    producer,
                    stream_hot_bytes,
                    group_hot_bytes,
                    receipt_evicted,
                    incarnation: response_incarnation(&self.state_machine, Some(&stream_id)),
                })
            }
            StreamResponse::Error {
                code,
                message,
                next_offset,
                context,
            } => Err(GroupEngineError::stream_with_context(
                code,
                message,
                next_offset,
                context,
            )),
            other => Err(GroupEngineError::new(format!(
                "unexpected append response: {other:?}"
            ))),
        }
    }

    pub fn read_stream_plan(
        &mut self,
        request: &ReadStreamRequest,
        placement: ShardPlacement,
    ) -> Result<StreamReadPlan, GroupEngineError> {
        self.ensure_stream_access(&request.stream_id, request.now_ms, true, placement)?;
        self.read_stream_plan_after_access(request)
    }

    pub fn read_stream_plan_after_access(
        &self,
        request: &ReadStreamRequest,
    ) -> Result<StreamReadPlan, GroupEngineError> {
        self.state_machine
            .read_plan_at(
                &request.stream_id,
                request.offset,
                request.max_len,
                request.now_ms,
            )
            .map_err(stream_response_error)
    }

    /// Per-bucket usage held by this group's state machine. Public so the
    /// Raft engine can serve usage reads from its applied state machine.
    pub fn bucket_usage_report(&self) -> Vec<ursula_stream::BucketUsageSnapshot> {
        self.state_machine.bucket_usage_report()
    }

    /// Whether any client write ever changed this group's applied state.
    /// Public so the Raft engine can read it.
    pub fn holds_client_state(&self) -> bool {
        self.state_machine.holds_client_state()
    }

    /// Bounded-state gauges of the applied state (§7.5 of
    /// `bounded-stream-state.md`). Public so the Raft engine can serve them.
    pub fn state_gauges(&self) -> ursula_stream::GroupStateGauges {
        self.state_machine.state_gauges()
    }

    /// Streams with `TidyStream` debt (bounded-state F0), for the leader's
    /// tidy driver. Public so the Raft engine can serve it.
    pub fn tidy_candidates(&self, now_ms: u64, limit: usize) -> Vec<BucketStreamId> {
        self.state_machine.tidy_candidates(now_ms, limit)
    }

    pub fn head_stream_after_access(
        &mut self,
        request: &HeadStreamRequest,
        placement: ShardPlacement,
    ) -> Result<HeadStreamResponse, GroupEngineError> {
        let Some(metadata) = self
            .state_machine
            .head_at(&request.stream_id, request.now_ms)
        else {
            return Err(GroupEngineError::stream(
                StreamErrorCode::StreamNotFound,
                format!("stream '{}' does not exist", request.stream_id),
            ));
        };
        let content_type = metadata.content_type.clone();
        let tail_offset = metadata.tail_offset;
        let closed = metadata.status == ursula_stream::StreamStatus::Closed;
        let stream_ttl_seconds = metadata.stream_ttl_seconds;
        let stream_expires_at_ms = metadata.stream_expires_at_ms;
        let created_at_ms = metadata.created_at_ms;
        let _ = metadata;
        let snapshot = self
            .state_machine
            .latest_snapshot(&request.stream_id)
            .map_err(stream_response_error)?;
        Ok(HeadStreamResponse {
            placement,
            content_type,
            tail_offset,
            cold_hot_start_offset: self.state_machine.hot_start_offset(&request.stream_id),
            closed,
            stream_ttl_seconds,
            stream_expires_at_ms,
            snapshot_offset: snapshot.as_ref().map(|snapshot| snapshot.offset),
            snapshot_digest: snapshot.map(|snapshot| snapshot.digest),
            retained_offset: self.state_machine.retained_offset(&request.stream_id),
            created_at_ms,
        })
    }

    /// Reads one cold-index read segment through the page cache.
    async fn read_cold_index_segment(
        cold_store: &ColdStoreHandle,
        cache: &ColdIndexPageCache<ColdStoreColdIndexPageStore>,
        stream_id: &BucketStreamId,
        segment: &StreamReadColdIndexSegment,
    ) -> std::io::Result<Vec<u8>> {
        let objects = cache.object_segments_for_read(stream_id, segment).await?;
        let segment_end = segment
            .read_start_offset
            .saturating_add(u64::try_from(segment.len).expect("read len fits u64"));
        let mut payload = Vec::new();
        let mut cursor = segment.read_start_offset;
        for object in objects {
            // A retried cold flush can leave overlapping objects in an index
            // page. They describe the same byte range, so materialize each
            // byte once while still using the original object start for range
            // addressing.
            let start = object
                .start_offset
                .max(segment.read_start_offset)
                .max(cursor);
            let end = object.end_offset.min(segment_end);
            if start >= end {
                continue;
            }
            let bytes = cold_store
                .read_object_range_for_stream(
                    stream_id,
                    &object,
                    start,
                    usize::try_from(end - start).expect("object read len fits usize"),
                )
                .await?;
            payload.extend_from_slice(&bytes);
            cursor = end;
        }
        Ok(payload)
    }

    pub async fn read_payload_from_plan(
        cold_store: Option<&ColdStoreHandle>,
        cold_index_cache: Option<&Arc<ColdIndexPageCache<ColdStoreColdIndexPageStore>>>,
        stream_id: &BucketStreamId,
        plan: &StreamReadPlan,
    ) -> Result<Vec<u8>, GroupEngineError> {
        let mut payload = Vec::new();
        for segment in &plan.segments {
            match segment {
                StreamReadSegment::Hot(bytes) => payload.extend_from_slice(bytes),
                StreamReadSegment::ColdIndex(segment) => {
                    let Some(cold_store) = cold_store else {
                        return Err(GroupEngineError::stream_with_next_offset(
                            StreamErrorCode::InvalidColdFlush,
                            format!("stream '{stream_id}' read requires object payload store"),
                            Some(plan.next_offset),
                        ));
                    };
                    let Some(cache) = cold_index_cache else {
                        return Err(GroupEngineError::stream_with_next_offset(
                            StreamErrorCode::InvalidColdFlush,
                            format!("stream '{stream_id}' read requires cold index page cache"),
                            Some(plan.next_offset),
                        ));
                    };
                    let bytes =
                        match Self::read_cold_index_segment(cold_store, cache, stream_id, segment)
                            .await
                        {
                            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                                // RT2: the cached page may name an object that a
                                // compaction already deleted. Refresh the page
                                // once and retry before failing the read.
                                cache
                                    .refresh_page(&ColdIndexPageKey {
                                        stream_id: stream_id.clone(),
                                        generation: segment.generation,
                                        page_id: segment.page_id,
                                    })
                                    .await
                                    .map_err(|err| GroupEngineError::new(err.to_string()))?;
                                Self::read_cold_index_segment(cold_store, cache, stream_id, segment)
                                    .await
                            }
                            other => other,
                        }
                        .map_err(|err| GroupEngineError::new(err.to_string()))?;
                    payload.extend_from_slice(&bytes);
                }
                StreamReadSegment::Object(segment) => {
                    let Some(cold_store) = cold_store else {
                        return Err(GroupEngineError::stream_with_next_offset(
                            StreamErrorCode::InvalidColdFlush,
                            format!("stream '{stream_id}' read requires object payload store"),
                            Some(plan.next_offset),
                        ));
                    };
                    let bytes = cold_store
                        .read_object_range_for_stream(
                            stream_id,
                            &segment.object,
                            segment.read_start_offset,
                            segment.len,
                        )
                        .await
                        .map_err(|err| GroupEngineError::new(err.to_string()))?;
                    payload.extend_from_slice(&bytes);
                }
            }
        }
        Ok(payload)
    }

    pub(crate) async fn read_own_payload_from_plan(
        &self,
        stream_id: &BucketStreamId,
        plan: &StreamReadPlan,
    ) -> Result<Vec<u8>, GroupEngineError> {
        Self::read_payload_from_plan(
            self.cold_store.as_ref(),
            self.cold_index_cache.as_ref(),
            stream_id,
            plan,
        )
        .await
    }

    /// Materializes bootstrap updates from ONE read plan covering every
    /// update, then cuts the window into the planned parts (one per JSON
    /// record, or one in all for any other stream).
    pub(crate) async fn bootstrap_updates(
        &mut self,
        stream_id: &BucketStreamId,
        records: &[StreamMessageRecord],
        content_type: &str,
        now_ms: u64,
    ) -> Result<Vec<BootstrapUpdate>, GroupEngineError> {
        let (Some(first), Some(last)) = (records.first(), records.last()) else {
            return Ok(Vec::new());
        };
        let window_start = first.start_offset;
        let window_end = last.end_offset;
        let window_error = |message: &str| {
            GroupEngineError::stream(
                StreamErrorCode::InvalidSnapshot,
                format!(
                    "bootstrap window [{window_start}..{window_end}) for stream '{stream_id}' {message}"
                ),
            )
        };
        let window_len = window_end
            .checked_sub(window_start)
            .and_then(|len| usize::try_from(len).ok())
            .ok_or_else(|| window_error("is invalid"))?;
        self.bootstrap_read_plans = self.bootstrap_read_plans.saturating_add(1);
        let plan = self
            .state_machine
            .read_plan_at(stream_id, window_start, window_len, now_ms)
            .map_err(stream_response_error)?;
        // Bootstrap never reads cold storage: the plan only covers bytes at
        // or above the exact frontier, which are hot.
        if plan
            .segments
            .iter()
            .any(|segment| !matches!(segment, StreamReadSegment::Hot(_)))
        {
            return Err(GroupEngineError::new(format!(
                "bootstrap window [{window_start}..{window_end}) for stream '{stream_id}' is not hot"
            )));
        }
        let payload = self.read_own_payload_from_plan(stream_id, &plan).await?;
        if payload.len() != window_len {
            return Err(window_error("was not fully materialized"));
        }
        let mut updates = Vec::with_capacity(records.len());
        for record in records {
            let part = record
                .start_offset
                .checked_sub(window_start)
                .zip(record.end_offset.checked_sub(window_start))
                .and_then(|(start, end)| {
                    let start = usize::try_from(start).ok()?;
                    let end = usize::try_from(end).ok()?;
                    payload.get(start..end)
                })
                .ok_or_else(|| window_error("does not contain every message"))?;
            updates.push(BootstrapUpdate {
                start_offset: record.start_offset,
                next_offset: record.end_offset,
                content_type: content_type.to_owned(),
                payload: part.to_vec(),
            });
        }
        Ok(updates)
    }

    pub(crate) fn build_snapshot(&self, placement: ShardPlacement) -> GroupSnapshot {
        let stream_snapshot = self.state_machine.snapshot();
        let stream_append_counts = self.stream_append_counts_snapshot();
        GroupSnapshot {
            placement,
            group_commit_index: self.commit_index,
            stream_snapshot,
            stream_append_counts,
        }
    }

    pub(crate) fn stream_append_counts_snapshot(&self) -> Vec<StreamAppendCount> {
        // Counts live in the stream slots, so only live streams carry one.
        let mut counts = self
            .state_machine
            .stream_append_counts()
            .map(|(stream_id, append_count)| StreamAppendCount {
                stream_id: stream_id.clone(),
                append_count,
            })
            .collect::<Vec<_>>();
        counts.sort_by(|left, right| compare_stream_ids(&left.stream_id, &right.stream_id));
        counts
    }

    /// Cold-index generation of the live stream `stream_id` (F14g), which
    /// pre-proposal cold-index page writes use.
    pub fn cold_index_generation(&self, stream_id: &BucketStreamId) -> Option<u64> {
        self.state_machine.cold_index_generation(stream_id)
    }

    #[cfg(test)]
    pub(crate) fn tracked_append_count_entries(&self) -> usize {
        self.state_machine.stream_append_counts().count()
    }

    pub fn stream_tail_offset(&self, stream_id: &BucketStreamId) -> Option<u64> {
        self.state_machine
            .head(stream_id)
            .map(|metadata| metadata.tail_offset)
    }

    /// Leader-side pre-check before a cold flush writes its page entry: `Ok`
    /// exactly when applying the flush now would succeed, so the entry's
    /// range is proven hot and the clip rule (bounded-state F19 step 1) may
    /// remove whatever else overlaps it. A stale candidate is rejected with
    /// the same typed error apply would return, before any page write.
    pub fn check_cold_flush(&self, request: &FlushColdRequest) -> Result<(), GroupEngineError> {
        self.state_machine
            .check_cold_flush(&request.stream_id, &request.chunk)
            .and_then(|()| {
                self.state_machine
                    .check_cold_flush_generation(&request.stream_id, request.cold_generation)
            })
            .map_err(stream_response_error)
    }

    /// What applied state proves about up to `max_streams` streams after
    /// `after`, for cold-index page repair (bounded-state F19 step 2).
    pub fn cold_index_repair_inputs(
        &self,
        after: Option<&BucketStreamId>,
        max_streams: usize,
    ) -> Vec<ColdIndexRepairInput> {
        self.state_machine
            .stream_ids_after(after, max_streams)
            .into_iter()
            .filter_map(|stream_id| self.cold_index_repair_input(stream_id))
            .collect()
    }

    /// F2 discovery on this replica, tracking idle tails leader-locally.
    pub fn plan_shared_ref_compaction_candidates(
        &mut self,
        request: &SharedRefCompactionRequest,
    ) -> Vec<SharedRefCandidate> {
        self.state_machine
            .shared_ref_candidates(request, &mut self.shared_ref_idle)
    }

    /// F5 offload discovery on this replica: streams whose state-held
    /// external refs `request` makes due.
    pub fn staged_external_ref_candidates(
        &self,
        request: &crate::cold_refs::OffloadColdRefsRequest,
    ) -> Vec<ursula_stream::StagedExternalRefCandidate> {
        self.state_machine.staged_external_ref_candidates(
            request.max_staged_refs,
            &|object| request.is_due(object),
            request.max_streams.max(1),
        )
    }

    /// What applied state references for one orphan-sweep step (F14h): the
    /// group's pack directories at the start of a cycle, the group-wide
    /// referenced paths, and up to `max_streams` live streams after the
    /// cursor with the paths their state references.
    pub fn cold_orphan_sweep_plan(
        &self,
        request: &ColdOrphanSweepRequest,
        raft_group_id: u32,
    ) -> ColdOrphanSweepPlan {
        let max_streams = request.max_streams.max(1);
        let stream_ids = self
            .state_machine
            .stream_ids_after(request.after.as_ref(), max_streams);
        let next_after = (stream_ids.len() >= max_streams)
            .then(|| stream_ids.last().cloned())
            .flatten();
        let pack_dirs = if request.after.is_none() {
            self.state_machine
                .bucket_ids()
                .into_iter()
                .map(|bucket_id| cold_pack_dir(&bucket_id, raft_group_id))
                .collect()
        } else {
            Vec::new()
        };
        let streams = stream_ids
            .into_iter()
            .map(|stream_id| ColdOrphanSweepStream {
                generation: self
                    .state_machine
                    .cold_index_generation(&stream_id)
                    .unwrap_or(0),
                referenced: self.state_machine.stream_referenced_cold_paths(&stream_id),
                cold_range: (
                    self.state_machine.retained_offset(&stream_id),
                    self.state_machine.hot_start_offset(&stream_id),
                ),
                referenced_ranges: self
                    .state_machine
                    .cold_chunks(&stream_id)
                    .iter()
                    .map(|chunk| (chunk.start_offset, chunk.end_offset))
                    .chain(
                        self.state_machine
                            .external_segments(&stream_id)
                            .iter()
                            .map(|object| (object.start_offset, object.end_offset)),
                    )
                    .collect(),
                retained_range: (
                    self.state_machine.retained_offset(&stream_id),
                    self.stream_tail_offset(&stream_id).unwrap_or(0),
                ),
                hot_ranges: self
                    .state_machine
                    .hot_segments(&stream_id)
                    .iter()
                    .map(|segment| (segment.start_offset, segment.end_offset))
                    .collect(),
                stream_id,
            })
            .collect();
        ColdOrphanSweepPlan {
            leader: true,
            pack_dirs,
            group_referenced: self.state_machine.group_referenced_cold_paths(),
            streams,
            next_after,
        }
    }

    /// The repair inputs one [`RepairColdIndexRequest`] covers: the one
    /// stream it names (F2 repairs a stream right before compacting it), or
    /// the next cursor step.
    pub fn cold_index_repair_inputs_for(
        &self,
        request: &RepairColdIndexRequest,
    ) -> Vec<ColdIndexRepairInput> {
        match &request.stream {
            Some(stream_id) => self
                .cold_index_repair_input(stream_id.clone())
                .into_iter()
                .collect(),
            None => {
                self.cold_index_repair_inputs(request.after.as_ref(), request.max_streams.max(1))
            }
        }
    }

    /// Retention GC due for the streams one repair cursor step visits
    /// (bounded-state F14f). A step that starts a cycle first forgets the
    /// incarnations that no longer exist.
    pub fn retention_gc_targets(
        &mut self,
        request: &RepairColdIndexRequest,
        inputs: &[ColdIndexRepairInput],
    ) -> Vec<RetentionGcTarget> {
        let Some(now_ms) = request.retention_gc_now_ms else {
            return Vec::new();
        };
        if request.after.is_none() && request.stream.is_none() {
            let state_machine = &self.state_machine;
            self.retention_gc.retain(|stream_id, created_at_ms| {
                state_machine
                    .head(stream_id)
                    .is_some_and(|metadata| metadata.created_at_ms == created_at_ms)
            });
        }
        inputs
            .iter()
            .filter_map(|input| {
                self.retention_gc
                    .observe(input, now_ms, ursula_stream::RETENTION_COLD_GC_GRACE_MS)
            })
            .collect()
    }

    /// Records the retention GC targets that completed.
    pub fn retention_gc_collected(&mut self, targets: &[RetentionGcTarget]) {
        for target in targets {
            self.retention_gc.collected(target);
        }
    }

    /// What applied state proves about one stream, for cold-index page repair.
    pub fn cold_index_repair_input(
        &self,
        stream_id: BucketStreamId,
    ) -> Option<ColdIndexRepairInput> {
        let metadata = self.state_machine.head(&stream_id)?;
        let hot_ranges = self
            .state_machine
            .hot_segments(&stream_id)
            .iter()
            .map(|segment| (segment.start_offset, segment.end_offset))
            .collect();
        let state_refs = self
            .state_machine
            .cold_chunks(&stream_id)
            .iter()
            .map(ObjectPayloadRef::from)
            .chain(
                self.state_machine
                    .external_segments(&stream_id)
                    .iter()
                    .cloned(),
            )
            .collect();
        Some(ColdIndexRepairInput {
            generation: self
                .state_machine
                .cold_index_generation(&stream_id)
                .unwrap_or(0),
            retained_offset: self.state_machine.retained_offset(&stream_id),
            tail_offset: metadata.tail_offset,
            created_at_ms: metadata.created_at_ms,
            hot_ranges,
            state_refs,
            stream_id,
        })
    }

    pub(crate) fn install_snapshot_inner(
        &mut self,
        snapshot: GroupSnapshot,
    ) -> Result<(), GroupEngineError> {
        let GroupSnapshot {
            placement: _,
            group_commit_index,
            stream_snapshot,
            stream_append_counts,
        } = snapshot;
        self.install_snapshot_parts(group_commit_index, stream_snapshot, stream_append_counts)
    }

    pub(crate) fn install_snapshot_parts(
        &mut self,
        group_commit_index: u64,
        stream_snapshot: StreamSnapshot,
        stream_append_counts: Vec<StreamAppendCount>,
    ) -> Result<(), GroupEngineError> {
        let mut state_machine = StreamStateMachine::restore(stream_snapshot)
            .map_err(|err| GroupEngineError::new(format!("restore stream snapshot: {err}")))?;
        // A count for a stream the snapshot does not hold has nothing to
        // attach to and is dropped (F9: counts die with their slot).
        for count in stream_append_counts {
            state_machine.set_stream_append_count(&count.stream_id, count.append_count);
        }

        self.commit_index = group_commit_index;
        self.state_machine = state_machine;
        // The installed state may follow page rewrites (clips, compaction)
        // this replica never applied; cached pages could predate them.
        if let Some(cache) = self.cold_index_cache.as_ref() {
            cache.clear();
        }
        Ok(())
    }
}

impl GroupEngine for InMemoryGroupEngine {
    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupCreateStreamFuture<'a> {
        if admission.is_enabled() {
            return Box::pin(async move {
                self.create_stream_with_admission_inner(request, placement, admission)
            });
        }
        let command = GroupWriteCommand::from(request);
        Box::pin(async move {
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::CreateStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected create stream write response: {other:?}"
                ))),
            }
        })
    }

    fn create_stream_external<'a>(
        &'a mut self,
        request: CreateStreamExternalRequest,
        placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        Box::pin(async move {
            // The state keeps the initial payload as a direct reference
            // (F14g), so the engine writes no page entry before proposing.
            let command = GroupWriteCommand::from(request);
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::CreateStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected external create stream write response: {other:?}"
                ))),
            }
        })
    }

    fn read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a> {
        Box::pin(async move {
            self.read_stream_parts(request, placement)
                .await?
                .into_response()
                .await
        })
    }

    fn read_stream_parts<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamPartsFuture<'a> {
        Box::pin(async move {
            let stream_id = request.stream_id.clone();
            let plan = self.read_stream_plan(&request, placement)?;
            Ok(GroupReadStreamParts::from_plan(
                placement,
                stream_id,
                plan,
                self.cold_store(),
                self.cold_index_cache.clone(),
            ))
        })
    }

    fn publish_snapshot<'a>(
        &'a mut self,
        request: PublishSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupPublishSnapshotFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request);
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::PublishSnapshot(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected publish snapshot write response: {other:?}"
                ))),
            }
        })
    }

    fn advance_retention<'a>(
        &'a mut self,
        request: AdvanceRetentionRequest,
        placement: ShardPlacement,
    ) -> GroupAdvanceRetentionFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request);
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::AdvanceRetention(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected advance retention write response: {other:?}"
                ))),
            }
        })
    }

    fn import_group_state<'a>(
        &'a mut self,
        request: ImportGroupStateRequest,
        placement: ShardPlacement,
    ) -> crate::GroupImportGroupStateFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(StreamCommand::from(request));
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::ImportGroupState(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected group state import response: {other:?}"
                ))),
            }
        })
    }

    fn state_gauges<'a>(&'a mut self, _placement: ShardPlacement) -> GroupStateGaugesFuture<'a> {
        Box::pin(async move { Ok(self.state_machine.state_gauges()) })
    }

    fn tidy_stream<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        placement: ShardPlacement,
    ) -> GroupTidyStreamFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(StreamCommand::TidyStream { stream_id, now_ms });
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::TidyStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected tidy stream write response: {other:?}"
                ))),
            }
        })
    }

    fn offload_cold_refs<'a>(
        &'a mut self,
        request: crate::cold_refs::OffloadColdRefsRequest,
        placement: ShardPlacement,
    ) -> super::GroupOffloadColdRefsFuture<'a> {
        Box::pin(async move {
            let mut report = crate::cold_refs::OffloadColdRefsResponse::default();
            let Some(cold_store) = self.cold_store.clone() else {
                return Ok(report);
            };
            let store = ColdStoreColdIndexPageStore::new(cold_store);
            for candidate in self.staged_external_ref_candidates(&request) {
                // Index after commit: every ref is committed, so its entries
                // are correct whatever happens to the proposal below.
                for object in &candidate.refs {
                    let clipped = crate::cold_index::write_proven_external_index_pages(
                        &store,
                        &candidate.stream_id,
                        candidate.cold_generation,
                        object,
                    )
                    .await
                    .map_err(|err| GroupEngineError::new(err.to_string()))?;
                    report.page_entries_clipped =
                        report.page_entries_clipped.saturating_add(clipped);
                }
                if let Some(cache) = self.cold_index_cache.as_ref() {
                    cache.invalidate_stream(&candidate.stream_id);
                }
                let command = GroupWriteCommand::from(StreamCommand::OffloadColdRefs {
                    stream_id: candidate.stream_id,
                    refs: candidate.refs,
                });
                match self.apply_committed_write(command, placement) {
                    Ok(GroupWriteResponse::OffloadColdRefs(response)) => {
                        report.streams = report.streams.saturating_add(1);
                        report.refs_offloaded =
                            report.refs_offloaded.saturating_add(response.removed);
                    }
                    Ok(other) => {
                        return Err(GroupEngineError::new(format!(
                            "unexpected offload cold refs write response: {other:?}"
                        )));
                    }
                    Err(err) if err.code().is_some() => {
                        report.rejected = report.rejected.saturating_add(1);
                    }
                    Err(err) => return Err(err),
                }
            }
            Ok(report)
        })
    }

    fn tidy_streams<'a>(
        &'a mut self,
        request: TidyStreamsRequest,
        placement: ShardPlacement,
    ) -> GroupTidyStreamsFuture<'a> {
        Box::pin(async move {
            let mut report = TidyStreamsResponse::default();
            for stream_id in self.tidy_candidates(request.now_ms, request.max_streams) {
                let response = self
                    .tidy_stream(stream_id, request.now_ms, placement)
                    .await?;
                report.tidied = report.tidied.saturating_add(1);
                if response.debt_remaining {
                    report.debt_remaining = report.debt_remaining.saturating_add(1);
                }
            }
            Ok(report)
        })
    }

    fn read_snapshot<'a>(
        &'a mut self,
        request: ReadSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupReadSnapshotFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, true, placement)?;
            let snapshot = match request.snapshot_offset {
                Some(offset) => self
                    .state_machine
                    .read_snapshot(&request.stream_id, offset)
                    .map_err(stream_response_error)?,
                None => self
                    .state_machine
                    .latest_snapshot(&request.stream_id)
                    .map_err(stream_response_error)?
                    .ok_or_else(|| {
                        GroupEngineError::stream(
                            StreamErrorCode::SnapshotNotFound,
                            format!("stream '{}' has no visible snapshot", request.stream_id),
                        )
                    })?,
            };
            let tail_offset = self
                .state_machine
                .head_at(&request.stream_id, request.now_ms)
                .map(|metadata| metadata.tail_offset)
                .unwrap_or(snapshot.offset);
            let incarnation = response_incarnation(&self.state_machine, Some(&request.stream_id));
            Ok(ReadSnapshotResponse {
                placement,
                snapshot_offset: snapshot.offset,
                next_offset: snapshot.offset,
                content_type: snapshot.content_type,
                snapshot_digest: snapshot.digest,
                payload: snapshot.payload,
                object: snapshot.object,
                up_to_date: snapshot.offset == tail_offset,
                incarnation,
            })
        })
    }

    fn bootstrap_stream<'a>(
        &'a mut self,
        request: BootstrapStreamRequest,
        placement: ShardPlacement,
    ) -> GroupBootstrapStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, true, placement)?;
            let plan = self
                .state_machine
                .bootstrap_plan(&request.stream_id)
                .map_err(stream_response_error)?;
            // Read before the cold reads below; `&mut self` keeps the
            // state from changing until the response is built.
            let incarnation = response_incarnation(&self.state_machine, Some(&request.stream_id));
            let snapshot_offset = plan.snapshot.as_ref().map(|snapshot| snapshot.offset);
            let snapshot_content_type = plan
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.content_type.clone())
                .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_owned());
            let snapshot_payload = plan
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.payload.clone())
                .unwrap_or_default();
            let snapshot_object = plan
                .snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.object.clone());
            let updates = self
                .bootstrap_updates(
                    &request.stream_id,
                    &plan.updates,
                    &plan.content_type,
                    request.now_ms,
                )
                .await?;
            Ok(BootstrapStreamResponse {
                placement,
                snapshot_offset,
                snapshot_content_type,
                snapshot_payload,
                snapshot_object,
                updates,
                next_offset: plan.next_offset,
                up_to_date: plan.up_to_date,
                closed: plan.closed,
                incarnation,
            })
        })
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a> {
        Box::pin(async move { self.apply_access_command(stream_id, now_ms, renew_ttl, placement) })
    }

    fn head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            self.head_stream_after_access(&request, placement)
        })
    }

    fn bucket_usage<'a>(&'a mut self, _placement: ShardPlacement) -> GroupBucketUsageFuture<'a> {
        Box::pin(async move { Ok(self.state_machine.bucket_usage_report()) })
    }

    fn close_stream<'a>(
        &'a mut self,
        request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request);
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::CloseStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected close stream write response: {other:?}"
                ))),
            }
        })
    }

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a> {
        let command = GroupWriteCommand::from(request);
        Box::pin(async move {
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::DeleteStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected delete stream write response: {other:?}"
                ))),
            }
        })
    }

    fn purge_bucket<'a>(
        &'a mut self,
        bucket_id: String,
        placement: ShardPlacement,
    ) -> GroupPurgeBucketFuture<'a> {
        Box::pin(async move {
            match self.apply_committed_write(
                GroupWriteCommand::Stream(StreamCommand::PurgeBucket { bucket_id }),
                placement,
            )? {
                GroupWriteResponse::PurgeBucket(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected purge bucket write response: {other:?}"
                ))),
            }
        })
    }

    fn ack_cold_gc<'a>(
        &'a mut self,
        up_to_seq: u64,
        placement: ShardPlacement,
    ) -> GroupAckColdGcFuture<'a> {
        Box::pin(async move {
            match self.apply_committed_write(
                GroupWriteCommand::Stream(StreamCommand::AckColdGc { up_to_seq }),
                placement,
            )? {
                GroupWriteResponse::AckColdGc(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected ack cold gc write response: {other:?}"
                ))),
            }
        })
    }

    fn defer_cold_gc<'a>(
        &'a mut self,
        seq: u64,
        not_before_ms: u64,
        placement: ShardPlacement,
    ) -> GroupDeferColdGcFuture<'a> {
        Box::pin(async move {
            match self.apply_committed_write(
                GroupWriteCommand::Stream(StreamCommand::DeferColdGc { seq, not_before_ms }),
                placement,
            )? {
                GroupWriteResponse::DeferColdGc(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected defer cold gc write response: {other:?}"
                ))),
            }
        })
    }

    fn plan_cold_gc<'a>(
        &'a mut self,
        max: usize,
        _placement: ShardPlacement,
    ) -> GroupPlanColdGcFuture<'a> {
        let entries = self.state_machine.plan_cold_gc_batch(max);
        Box::pin(async move { Ok(entries) })
    }

    fn plan_shared_ref_compaction<'a>(
        &'a mut self,
        request: SharedRefCompactionRequest,
        _placement: ShardPlacement,
    ) -> GroupPlanSharedRefCompactionFuture<'a> {
        let candidates = self.plan_shared_ref_compaction_candidates(&request);
        Box::pin(async move { Ok(candidates) })
    }

    fn plan_cold_orphan_sweep<'a>(
        &'a mut self,
        request: ColdOrphanSweepRequest,
        placement: ShardPlacement,
    ) -> GroupPlanColdOrphanSweepFuture<'a> {
        let plan = self.cold_orphan_sweep_plan(&request, placement.raft_group_id.0);
        Box::pin(async move { Ok(plan) })
    }

    fn repair_cold_index<'a>(
        &'a mut self,
        request: RepairColdIndexRequest,
        _placement: ShardPlacement,
    ) -> GroupRepairColdIndexFuture<'a> {
        Box::pin(async move {
            let Some(cold_store) = self.cold_store.clone() else {
                return Ok(RepairColdIndexResponse::default());
            };
            let inputs = self.cold_index_repair_inputs_for(&request);
            let retention_targets = self.retention_gc_targets(&request, &inputs);
            let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
            let (report, compaction_pages) =
                repair_cold_index_streams(&store, self.cold_index_cache.as_deref(), &inputs)
                    .await
                    .map_err(|err| GroupEngineError::new(err.to_string()))?;
            if !retention_targets.is_empty() {
                let (_, completed) = collect_retained_cold_objects(
                    &cold_store,
                    self.cold_index_cache.as_deref(),
                    &retention_targets,
                )
                .await;
                self.retention_gc_collected(&completed);
            }
            Ok(repair_cold_index_response(
                &request,
                &inputs,
                report,
                compaction_pages,
            ))
        })
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendFuture<'a> {
        if admission.is_enabled() {
            return Box::pin(async move {
                self.append_with_admission_inner(request, placement, admission)
            });
        }
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request);
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::Append(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected append write response: {other:?}"
                ))),
            }
        })
    }

    fn append_external<'a>(
        &'a mut self,
        request: AppendExternalRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            // F5: commit first, index after. Apply keeps the locator in
            // state; the offload pass writes the page entry once the append
            // committed.
            let command = GroupWriteCommand::from(request);
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::Append(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected external append write response: {other:?}"
                ))),
            }
        })
    }

    fn flush_cold<'a>(
        &'a mut self,
        request: FlushColdRequest,
        placement: ShardPlacement,
    ) -> GroupFlushColdFuture<'a> {
        Box::pin(async move {
            let mut index_rollback = None;
            if !request.chunk.shared_object
                && let Some(cold_store) = self.cold_store.as_ref()
            {
                self.check_cold_flush(&request)?;
                let generation = self
                    .state_machine
                    .cold_index_generation(&request.stream_id)
                    .unwrap_or(0);
                let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
                let rollback = write_cold_chunk_index_pages_with_rollback_in_generation(
                    &store,
                    &request.stream_id,
                    generation,
                    &request.chunk,
                )
                .await
                .map_err(|err| GroupEngineError::new(err.to_string()))?;
                // The clip rule may have removed stale entries that a cached
                // page still holds.
                if clipped_entries(&rollback) > 0
                    && let Some(cache) = self.cold_index_cache.as_ref()
                {
                    cache.invalidate_stream(&request.stream_id);
                }
                index_rollback = Some((store, rollback));
            }
            let command = GroupWriteCommand::from(request);
            match self.apply_committed_write(command, placement) {
                Ok(GroupWriteResponse::FlushCold(response)) => Ok(response),
                Ok(other) => {
                    if let Some((store, rollback)) = index_rollback {
                        rollback_cold_index_pages(&store, rollback)
                            .await
                            .map_err(|err| GroupEngineError::new(err.to_string()))?;
                    }
                    Err(GroupEngineError::new(format!(
                        "unexpected flush cold write response: {other:?}"
                    )))
                }
                Err(err) => {
                    if let Some((store, rollback)) = index_rollback {
                        rollback_cold_index_pages(&store, rollback).await.map_err(
                            |rollback_err| {
                                GroupEngineError::new(format!(
                                    "rollback cold index after flush failure: {rollback_err}"
                                ))
                            },
                        )?;
                    }
                    Err(err)
                }
            }
        })
    }

    fn compact_cold<'a>(
        &'a mut self,
        request: CompactColdRequest,
        placement: ShardPlacement,
    ) -> GroupCompactColdFuture<'a> {
        Box::pin(async move {
            let mut index_rollback = None;
            if let Some(cold_store) = self.cold_store.as_ref() {
                let generation = self
                    .state_machine
                    .cold_index_generation(&request.stream_id)
                    .unwrap_or(0);
                let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
                let rollback = if request.old_chunks.iter().all(|chunk| chunk.shared_object) {
                    write_cold_chunk_index_pages_with_rollback_in_generation(
                        &store,
                        &request.stream_id,
                        generation,
                        &request.replacement,
                    )
                    .await
                    .map_err(|err| GroupEngineError::new(err.to_string()))?
                } else {
                    let Some(rollback) =
                        replace_cold_chunk_index_pages_with_rollback_in_generation(
                            &store,
                            &request.stream_id,
                            generation,
                            &request.old_chunks,
                            &request.replacement,
                        )
                        .await
                        .map_err(|err| GroupEngineError::new(err.to_string()))?
                    else {
                        return Err(GroupEngineError::new(
                            "cold compaction input no longer matches the cold index",
                        ));
                    };
                    rollback
                };
                index_rollback = Some((store, rollback));
            }
            let command = GroupWriteCommand::from(request);
            let result = match self.apply_committed_write(command, placement) {
                Ok(GroupWriteResponse::CompactCold(response)) => Ok(response),
                Ok(other) => Err(GroupEngineError::new(format!(
                    "unexpected compact cold write response: {other:?}"
                ))),
                Err(err) => Err(err),
            };
            if result.is_err()
                && let Some((store, rollback)) = index_rollback
            {
                rollback_cold_index_pages(&store, rollback)
                    .await
                    .map_err(|err| {
                        GroupEngineError::new(format!(
                            "rollback cold index after compaction failure: {err}"
                        ))
                    })?;
            }
            result
        })
    }

    fn plan_cold_flush<'a>(
        &'a mut self,
        request: PlanColdFlushRequest,
        _placement: ShardPlacement,
    ) -> GroupPlanColdFlushFuture<'a> {
        Box::pin(async move {
            self.state_machine
                .plan_cold_flush(
                    &request.stream_id,
                    request.min_hot_bytes,
                    request.max_flush_bytes,
                )
                .map_err(stream_response_error)
        })
    }

    fn plan_next_cold_flush_batch<'a>(
        &'a mut self,
        request: PlanGroupColdFlushRequest,
        _placement: ShardPlacement,
        max_candidates: usize,
    ) -> GroupPlanNextColdFlushBatchFuture<'a> {
        Box::pin(async move {
            self.state_machine
                .plan_cold_flush_pass(ColdFlushPassRequest {
                    min_hot_bytes: request.min_hot_bytes,
                    max_flush_bytes: request.max_flush_bytes,
                    max_batch_bytes: request.max_batch_bytes,
                    max_candidates,
                    pressure: request.pressure,
                    max_hot_age: request
                        .max_hot_age
                        .map(|age| ursula_stream::ColdFlushHotAge {
                            now_ms: crate::runtime::unix_time_ms(),
                            max_age_ms: u64::try_from(age.as_millis()).unwrap_or(u64::MAX),
                        }),
                })
                .map(|pass| pass.candidates)
                .map_err(stream_response_error)
        })
    }

    fn cold_hot_backlog<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        _placement: ShardPlacement,
    ) -> GroupColdHotBacklogFuture<'a> {
        Box::pin(async move { self.cold_hot_backlog_for(stream_id) })
    }

    fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
        Box::pin(async move { Ok(self.build_snapshot(placement)) })
    }

    fn install_snapshot<'a>(
        &'a mut self,
        snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a> {
        Box::pin(async move { self.install_snapshot_inner(snapshot) })
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryGroupEngineFactory {
    cold_store: Option<ColdStoreHandle>,
}

impl InMemoryGroupEngineFactory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_cold_store(cold_store: Option<ColdStoreHandle>) -> Self {
        Self { cold_store }
    }
}

impl GroupEngineFactory for InMemoryGroupEngineFactory {
    fn create<'a>(
        &'a self,
        _placement: ShardPlacement,
        _metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let mut engine = InMemoryGroupEngine::default();
            engine.set_cold_store(self.cold_store.clone());
            let engine: Box<dyn GroupEngine> = Box::new(engine);
            Ok(engine)
        })
    }
}

pub(crate) fn compare_stream_ids(
    left: &BucketStreamId,
    right: &BucketStreamId,
) -> std::cmp::Ordering {
    left.bucket_id
        .cmp(&right.bucket_id)
        .then_with(|| left.stream_id.cmp(&right.stream_id))
}
pub(crate) fn ensure_bucket_exists(
    state_machine: &mut StreamStateMachine,
    stream_id: &BucketStreamId,
) -> Result<(), GroupEngineError> {
    if state_machine.bucket_exists(&stream_id.bucket_id) {
        return Ok(());
    }

    match state_machine.apply(StreamCommand::CreateBucket {
        bucket_id: stream_id.bucket_id.clone(),
    }) {
        StreamResponse::BucketCreated { .. } | StreamResponse::BucketAlreadyExists { .. } => Ok(()),
        StreamResponse::Error {
            code,
            message,
            next_offset,
            context,
        } => Err(GroupEngineError::stream_with_context(
            code,
            message,
            next_offset,
            context,
        )),
        other => Err(GroupEngineError::new(format!(
            "unexpected create bucket response: {other:?}"
        ))),
    }
}

/// Stream id a command targets, if any (bucket and GC commands have none).
fn command_stream_id(command: &StreamCommand) -> Option<BucketStreamId> {
    match command {
        StreamCommand::CreateBucket { .. }
        | StreamCommand::PurgeBucket { .. }
        | StreamCommand::AckColdGc { .. }
        | StreamCommand::DeferColdGc { .. }
        | StreamCommand::ImportSnapshot { .. } => None,
        StreamCommand::CreateStream { stream_id, .. }
        | StreamCommand::CreateExternal { stream_id, .. }
        | StreamCommand::Append { stream_id, .. }
        | StreamCommand::AppendExternal { stream_id, .. }
        | StreamCommand::PublishSnapshot { stream_id, .. }
        | StreamCommand::PublishSnapshotExternal { stream_id, .. }
        | StreamCommand::AdvanceRetention { stream_id, .. }
        | StreamCommand::TouchStreamAccess { stream_id, .. }
        | StreamCommand::FlushCold { stream_id, .. }
        | StreamCommand::CompactCold { stream_id, .. }
        | StreamCommand::Close { stream_id, .. }
        | StreamCommand::DeleteStream { stream_id }
        | StreamCommand::TidyStream { stream_id, .. }
        | StreamCommand::OffloadColdRefs { stream_id, .. } => Some(stream_id.clone()),
        StreamCommand::IfIncarnation { command, .. } => command_stream_id(command),
    }
}

/// [`StreamStateMachine::stream_incarnation`] for a response, read from the
/// state that just served the write or read; `0` for a stream that no
/// longer exists.
fn response_incarnation(
    state_machine: &StreamStateMachine,
    stream_id: Option<&BucketStreamId>,
) -> u64 {
    stream_id
        .and_then(|stream_id| state_machine.stream_incarnation(stream_id))
        .unwrap_or(0)
}

fn require_response_stream_id(
    stream_id: Option<BucketStreamId>,
    response: &str,
) -> Result<BucketStreamId, GroupEngineError> {
    stream_id.ok_or_else(|| {
        GroupEngineError::new(format!(
            "{response} response for a command without a stream id"
        ))
    })
}

/// Where the next repair step resumes: after the last stream of a full
/// batch, or `None` once a short batch ends the cycle.
pub fn next_repair_cursor(
    inputs: &[ColdIndexRepairInput],
    max_streams: usize,
) -> Option<BucketStreamId> {
    if inputs.len() < max_streams {
        return None;
    }
    inputs.last().map(|input| input.stream_id.clone())
}

/// The response to one repair step over `inputs`. A step that names one
/// stream leaves the cursor alone and never completes a cycle.
pub fn repair_cold_index_response(
    request: &RepairColdIndexRequest,
    inputs: &[ColdIndexRepairInput],
    report: ColdIndexRepairReport,
    compaction_pages: Vec<ColdIndexPageKey>,
) -> RepairColdIndexResponse {
    if request.stream.is_some() {
        return RepairColdIndexResponse {
            report,
            compaction_pages,
            next_after: None,
            cycle_completed: false,
        };
    }
    let next_after = next_repair_cursor(inputs, request.max_streams.max(1));
    RepairColdIndexResponse {
        report,
        compaction_pages,
        cycle_completed: next_after.is_none(),
        next_after,
    }
}

pub(crate) fn stream_response_error(response: StreamResponse) -> GroupEngineError {
    match response {
        StreamResponse::Error {
            code,
            message,
            next_offset,
            context,
        } => GroupEngineError::stream_with_context(code, message, next_offset, context),
        other => GroupEngineError::new(format!("unexpected stream response error: {other:?}")),
    }
}
