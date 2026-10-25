use std::collections::BTreeMap;
use std::sync::Arc;

use ursula_shard::BucketStreamId;
use ursula_shard::ShardPlacement;
use ursula_stream::ColdFlushPassRequest;
use ursula_stream::IncarnationCheck;
use ursula_stream::ObjectPayloadRef;
use ursula_stream::ProducerAck;
use ursula_stream::ProducerEarlyAnswer;
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

use super::ColdOperation;
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
use super::GroupInfraError;
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
use super::GroupRouteWriteFuture;
use super::GroupSnapshotError;
use super::GroupSnapshotFuture;
use super::GroupStateGaugesFuture;
use super::GroupTidyStreamFuture;
use super::GroupTidyStreamsFuture;
use super::GroupTouchStreamAccessFuture;
use super::GroupWriteResponse;
use super::ResponseKind;
use super::cold_store_failure;
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
use crate::cold_uploads::UploadFence;
use crate::command::GroupSnapshot;
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
use crate::request::ProducerEarlyAnswerRequest;
use crate::request::PublishSnapshotRequest;
use crate::request::PublishSnapshotResponse;
use crate::request::PurgeBucketRequest;
use crate::request::PurgeBucketResponse;
use crate::request::ReadSnapshotRequest;
use crate::request::ReadSnapshotResponse;
use crate::request::ReadStreamRequest;
use crate::request::RouteWriteRequest;
use crate::request::SweepResponse;
use crate::request::TidyStreamsPass;
use crate::request::TidyStreamsRequest;
use crate::request::TidyStreamsResponse;
use crate::request::TouchStreamAccessResponse;
use crate::request::WriteHotBacklog;
use crate::request::WriteRoute;
use crate::retention_gc::RetentionGcTarget;
use crate::retention_gc::RetentionGcTracker;
use crate::retention_gc::collect_retained_cold_objects;

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
    /// The applied history, published after every command and snapshot
    /// install (#581 section 4.7).
    pub(crate) history: super::HistoryPublisher,
    /// When this replica's end records are due (F20): node-local, sampled
    /// on every tidy pass, on every replica, and reset by a snapshot
    /// install.
    pub(crate) retirement: crate::expiry_sweep::RetirementClock,
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

    /// Advances the in-memory commit counter. It is a monotonic gauge of
    /// applied writes, so it saturates instead of wrapping.
    fn advance_commit_index(&mut self) {
        self.commit_index = self.commit_index.saturating_add(1);
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

    /// Applies the committed command of the log entry at `index`. The Raft
    /// state machine passes each entry's log index, which a history event
    /// records (#581 section 4.3).
    pub fn apply_committed_entry(
        &mut self,
        command: StreamCommand,
        index: u64,
        placement: ShardPlacement,
    ) -> Result<GroupWriteResponse, GroupEngineError> {
        self.apply_checked_stream_command(command, index, placement, IncarnationCheck::Absent)
    }

    /// [`Self::apply_committed_entry`] for an engine without a log: the
    /// entry's index is the next commit index.
    pub fn apply_committed_write(
        &mut self,
        command: StreamCommand,
        placement: ShardPlacement,
    ) -> Result<GroupWriteResponse, GroupEngineError> {
        let index = self.commit_index.saturating_add(1);
        self.apply_committed_entry(command, index, placement)
    }

    /// Applies one canonical [`StreamCommand`] of an engine without a log
    /// to the deterministic state machine and lifts its [`StreamResponse`]
    /// into the group-level response, maintaining the group commit index.
    /// The entry's index is the next commit index.
    pub fn apply_stream_command(
        &mut self,
        command: StreamCommand,
        placement: ShardPlacement,
    ) -> Result<GroupWriteResponse, GroupEngineError> {
        let index = self.commit_index.saturating_add(1);
        self.apply_checked_stream_command(command, index, placement, IncarnationCheck::Absent)
    }

    /// The applied history of this engine's group, which changes at every
    /// history event it applies (#581 section 4.7).
    pub fn subscribe_history(&self) -> super::HistoryWatch {
        self.history.subscribe()
    }

    /// Starts a group of an engine without Raft: it draws its genesis
    /// history id when it creates the group (#581 section 4.3, W2).
    pub fn start_genesis(&mut self, placement: ShardPlacement, now_ms: u64) {
        if self.state_machine.history().is_none() {
            let started = self.apply_stream_command(
                StreamCommand::NewHistory {
                    history: ursula_stream::draw_history_id(),
                    now_ms,
                    // No replicas: nothing to keep from a history before.
                    previous: 0,
                    kept: 0,
                    leader: ursula_stream::HistoryLeader {
                        term: 0,
                        node_id: 0,
                    },
                },
                placement,
            );
            debug_assert!(started.is_ok(), "a genesis event always applies");
        }
    }

    /// [`Self::apply_stream_command`] for a command whose
    /// `Stream-Incarnation` precondition `check` says whether it matched.
    fn apply_checked_stream_command(
        &mut self,
        command: StreamCommand,
        index: u64,
        placement: ShardPlacement,
        check: IncarnationCheck,
    ) -> Result<GroupWriteResponse, GroupEngineError> {
        // Minor 1 (#581 review): before its genesis a group applies nothing
        // but the genesis, whatever a leader's fence let through.
        if self.state_machine.history().is_none()
            && !matches!(command, StreamCommand::NewHistory { .. })
        {
            return Err(GroupEngineError::stream(
                StreamErrorCode::HistoryPending,
                "the group's genesis has not applied; it serves nothing before it",
            ));
        }
        let history_event = matches!(
            command,
            StreamCommand::NewHistory { .. } | StreamCommand::ImportSnapshot { .. }
        );
        let response = self.apply_unpublished(command, index, placement, check);
        if history_event {
            self.history.publish(self.state_machine.history());
        }
        response
    }

    fn apply_unpublished(
        &mut self,
        command: StreamCommand,
        index: u64,
        placement: ShardPlacement,
        check: IncarnationCheck,
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
                    return self.group_response_from_stream(
                        refusal,
                        ResponseKind::answering(&command),
                        None,
                        placement,
                    );
                }
                self.apply_checked_stream_command(
                    *command,
                    index,
                    placement,
                    IncarnationCheck::Matched,
                )
            }
            command => {
                let stream_id = command_stream_id(&command);
                let expected = ResponseKind::answering(&command);
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
                let response = self.state_machine.apply_checked(index, command, check);
                let response =
                    self.group_response_from_stream(response, expected, stream_id, placement);
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
        expected: ResponseKind,
        stream_id: Option<BucketStreamId>,
        placement: ShardPlacement,
    ) -> Result<GroupWriteResponse, GroupEngineError> {
        match response {
            StreamResponse::Created {
                next_offset,
                closed,
                ..
            } => {
                let stream_id = require_response_stream_id(stream_id, ResponseKind::CreateStream)?;
                self.advance_commit_index();
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
                producer,
            } => {
                let stream_id = require_response_stream_id(stream_id, ResponseKind::Append)?;
                let stream_hot_bytes = self.state_machine.hot_payload_len(&stream_id).unwrap_or(0);
                let group_hot_bytes = self.state_machine.total_hot_payload_bytes();
                if !producer.as_ref().is_some_and(ProducerAck::is_repeat) {
                    self.advance_commit_index();
                }
                Ok(GroupWriteResponse::Append(AppendResponse {
                    placement,
                    start_offset: offset,
                    next_offset,
                    group_commit_index: self.commit_index,
                    closed,
                    producer,
                    stream_hot_bytes,
                    group_hot_bytes,
                    incarnation: response_incarnation(&self.state_machine, Some(&stream_id)),
                }))
            }
            StreamResponse::SnapshotPublished {
                snapshot_offset,
                snapshot_digest,
            } => {
                self.advance_commit_index();
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
                require_response_stream_id(stream_id, ResponseKind::TidyStream)?;
                self.advance_commit_index();
                Ok(GroupWriteResponse::TidyStream(
                    crate::request::TidyStreamResponse {
                        placement,
                        debt_remaining,
                        group_commit_index: self.commit_index,
                    },
                ))
            }
            StreamResponse::RetentionAdvanced { retained_offset } => {
                self.advance_commit_index();
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
                    self.advance_commit_index();
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
            StreamResponse::SnapshotImported {
                buckets,
                streams,
                history,
                already,
            } => {
                if !already {
                    self.advance_commit_index();
                }
                Ok(GroupWriteResponse::ImportGroupState(
                    ImportGroupStateResponse {
                        placement,
                        buckets,
                        streams,
                        group_commit_index: self.commit_index,
                        history,
                        already,
                    },
                ))
            }
            StreamResponse::HistoryStarted { history } => {
                self.advance_commit_index();
                Ok(GroupWriteResponse::NewHistory(
                    crate::request::NewHistoryResponse {
                        placement,
                        history,
                        group_commit_index: self.commit_index,
                    },
                ))
            }
            StreamResponse::ColdFlushed { hot_start_offset } => {
                self.advance_commit_index();
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
                self.advance_commit_index();
                Ok(GroupWriteResponse::CompactCold(CompactColdResponse {
                    placement,
                    compacted_chunks,
                    compacted_bytes,
                    group_commit_index: self.commit_index,
                }))
            }
            StreamResponse::Closed {
                next_offset,
                producer,
            } => {
                let stream_id = require_response_stream_id(stream_id, ResponseKind::CloseStream)?;
                if !producer.as_ref().is_some_and(ProducerAck::is_repeat) {
                    self.advance_commit_index();
                }
                Ok(GroupWriteResponse::CloseStream(CloseStreamResponse {
                    placement,
                    next_offset,
                    group_commit_index: self.commit_index,
                    producer,
                    incarnation: response_incarnation(&self.state_machine, Some(&stream_id)),
                }))
            }
            StreamResponse::Deleted { expired } => {
                let stream_id = require_response_stream_id(stream_id, ResponseKind::DeleteStream)?;
                self.advance_commit_index();
                Ok(GroupWriteResponse::DeleteStream(DeleteStreamResponse {
                    placement,
                    group_commit_index: self.commit_index,
                    expired,
                    hot_backlog: Some(self.write_hot_backlog(Some(&stream_id))),
                }))
            }
            StreamResponse::ColdGcAcked { removed } => {
                self.advance_commit_index();
                Ok(GroupWriteResponse::AckColdGc(AckColdGcResponse {
                    placement,
                    removed,
                    group_commit_index: self.commit_index,
                }))
            }
            StreamResponse::Swept { expired, retired } => {
                if expired > 0 || retired > 0 {
                    self.advance_commit_index();
                }
                Ok(GroupWriteResponse::Sweep(SweepResponse {
                    placement,
                    expired,
                    retired,
                    group_commit_index: self.commit_index,
                }))
            }
            StreamResponse::ColdRefsOffloaded { removed, remaining } => {
                require_response_stream_id(stream_id, ResponseKind::OffloadColdRefs)?;
                self.advance_commit_index();
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
                self.advance_commit_index();
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
                self.advance_commit_index();
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
            | StreamResponse::BucketAlreadyExists { .. }) => Err(GroupEngineError::Infra(
                GroupInfraError::UnexpectedResponse {
                    expected,
                    actual: ResponseKind::from(&other),
                },
            )),
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
        CreateStreamResponse::try_from(
            self.apply_committed_write(StreamCommand::from(request), placement)?,
        )
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
                match request.if_incarnation {
                    Some(incarnation)
                        if self.state_machine.stream_incarnation(&request.stream_id)
                            == Some(incarnation) =>
                    {
                        IncarnationCheck::Matched
                    }
                    _ => IncarnationCheck::Absent,
                },
                request.now_ms,
            )
        {
            self.check_cold_write_admission(
                &request.stream_id,
                admission,
                u64::try_from(request.payload.len()).unwrap_or(u64::MAX),
            )?;
        }
        AppendResponse::try_from(
            self.apply_committed_write(StreamCommand::from(request), placement)?,
        )
    }

    /// The early answer of [`GroupEngine::route_write`], from this
    /// engine's applied state.
    pub fn producer_early_answer_now(
        &self,
        request: &ProducerEarlyAnswerRequest,
    ) -> Option<ProducerEarlyAnswer> {
        self.state_machine.producer_early_answer(
            &request.stream_id,
            &request.producer,
            request.incarnation,
            request.now_ms,
        )
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
        TouchStreamAccessResponse::try_from(self.apply_committed_write(
            StreamCommand::TouchStreamAccess {
                stream_id,
                now_ms,
                renew_ttl,
            },
            placement,
        )?)
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

    /// Per-bucket bytes of the latest visible snapshot bodies in this
    /// group's state machine. Public so the Raft engine can serve usage
    /// reads from its applied state machine.
    pub fn snapshot_bytes_by_bucket(&self) -> BTreeMap<String, u64> {
        self.state_machine.snapshot_bytes_by_bucket()
    }

    /// Bounded-state gauges of the applied state (Gauges in
    /// `bounded-stream-state.md`). Public so the Raft engine can serve them.
    pub fn state_gauges(&self) -> ursula_stream::GroupStateGauges {
        self.state_machine.state_gauges()
    }

    /// What the applied state removed since it was built or restored
    /// (#669): the Raft state machine reads it around each entry to learn
    /// that the entry removed a stream.
    pub fn removed_state(&self) -> ursula_stream::RemovedState {
        self.state_machine.removed_state()
    }

    /// Streams with `TidyStream` debt (bounded-state F0), for the leader's
    /// tidy driver. Public so the Raft engine can serve it.
    pub fn tidy_candidates(&self, now_ms: u64, limit: usize) -> Vec<BucketStreamId> {
        self.state_machine.tidy_candidates(now_ms, limit)
    }

    /// Whether the leader's group sweep proposes `Sweep` at `now_ms` and
    /// `retire_through` ([`crate::expiry_sweep`]): a stream may have
    /// expired, or an end record is due. `false` is exact.
    pub fn sweep_due(&self, now_ms: u64, retire_through: u64) -> bool {
        self.state_machine.sweep_due(now_ms, retire_through)
    }

    /// Samples the group's usage seq on this replica's retirement clock at
    /// `now`, and returns the usage seq through which its end records are
    /// due, which the leader's sweep folds through (F20,
    /// [`crate::expiry_sweep::RetirementClock`]). Every replica samples, so
    /// a new leader retires on time. `now_ms`, the pass's wall clock less
    /// `retain`, dates the seed after a start, an install or a history
    /// change.
    pub fn sample_retirement(
        &mut self,
        now: crate::rt::time::Instant,
        now_ms: u64,
        retain: std::time::Duration,
    ) -> u64 {
        let sample = crate::expiry_sweep::RetirementSample {
            history: self.state_machine.history_id(),
            usage_seq: self.state_machine.usage_seq(),
        };
        let state_machine = &self.state_machine;
        self.retirement.sample(now, sample, retain, || {
            let retain_ms = u64::try_from(retain.as_millis()).unwrap_or(u64::MAX);
            state_machine.end_records_ended_by(now_ms.saturating_sub(retain_ms))
        })
    }

    pub fn head_stream_after_access(
        &self,
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
        let incarnation = self
            .state_machine
            .stream_incarnation(&request.stream_id)
            .unwrap_or(0);
        let inherited = self.state_machine.stream_inherited(&request.stream_id);
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
            incarnation,
            inherited,
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
            let Some(read_len) = end.checked_sub(start).filter(|len| *len > 0) else {
                continue;
            };
            let bytes = cold_store
                .read_object_range_for_stream(
                    stream_id,
                    &object,
                    start,
                    usize::try_from(read_len).expect("object read len fits usize"),
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
                                    .map_err(|err| {
                                        cold_store_failure(
                                            ColdOperation::RefreshIndexPage {
                                                stream_id: stream_id.clone(),
                                            },
                                            err,
                                        )
                                    })?;
                                Self::read_cold_index_segment(cold_store, cache, stream_id, segment)
                                    .await
                            }
                            other => other,
                        }
                        .map_err(|err| {
                            cold_store_failure(
                                ColdOperation::ReadPayload {
                                    stream_id: stream_id.clone(),
                                },
                                err,
                            )
                        })?;
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
                        .map_err(|err| {
                            cold_store_failure(
                                ColdOperation::ReadPayload {
                                    stream_id: stream_id.clone(),
                                },
                                err,
                            )
                        })?;
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
            return Err(GroupEngineError::Infra(
                GroupInfraError::BootstrapWindowNotHot {
                    stream_id: stream_id.clone(),
                    start: window_start,
                    end: window_end,
                },
            ));
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
        GroupSnapshot {
            placement,
            group_commit_index: self.commit_index,
            stream_snapshot: self.state_machine.snapshot(),
            usage: self.state_machine.usage_snapshot(),
        }
    }

    /// Cold-index generation of the live stream `stream_id` (F14g), which
    /// pre-proposal cold-index page writes use.
    pub fn cold_index_generation(&self, stream_id: &BucketStreamId) -> Option<u64> {
        self.state_machine.cold_index_generation(stream_id)
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
        let buckets = if request.after.is_none() {
            self.state_machine.bucket_ids()
        } else {
            Vec::new()
        };
        let pack_dirs = buckets
            .iter()
            .map(|bucket_id| cold_pack_dir(bucket_id, raft_group_id))
            .collect();
        let part_dirs = buckets
            .iter()
            .map(|bucket_id| crate::cold_store::snapshot_part_dir(bucket_id, raft_group_id))
            .collect();
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
            part_dirs,
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
            usage,
        } = snapshot;
        self.install_snapshot_parts(group_commit_index, stream_snapshot, usage)
    }

    /// The one install path of both engines, a start's restore included:
    /// the group snapshot's state with its usage ledger, checked exactly
    /// against its bucket totals (F20).
    pub(crate) fn install_snapshot_parts(
        &mut self,
        group_commit_index: u64,
        stream_snapshot: StreamSnapshot,
        usage: ursula_stream::UsageSnapshot,
    ) -> Result<(), GroupEngineError> {
        let state_machine = StreamStateMachine::restore_group(stream_snapshot, usage)
            .map_err(|err| GroupInfraError::Snapshot(GroupSnapshotError::InvalidContent(err)))?;

        self.commit_index = group_commit_index;
        self.state_machine = state_machine;
        // The installed state is another lineage's as far as the clock
        // knows: it seeds again from the end records' times.
        self.retirement = crate::expiry_sweep::RetirementClock::default();
        // The installed state may follow page rewrites (clips, compaction)
        // this replica never applied; cached pages could predate them.
        if let Some(cache) = self.cold_index_cache.as_ref() {
            cache.clear();
        }
        self.history.publish(self.state_machine.history());
        Ok(())
    }
}

impl GroupEngine for InMemoryGroupEngine {
    /// Without replication the engine leads in one leadership for as long
    /// as it runs.
    fn upload_fence(&self) -> Option<UploadFence> {
        Some(UploadFence::Unreplicated)
    }

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
        let command = StreamCommand::from(request);
        Box::pin(async move {
            CreateStreamResponse::try_from(self.apply_committed_write(command, placement)?)
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
            let command = StreamCommand::from(request);
            CreateStreamResponse::try_from(self.apply_committed_write(command, placement)?)
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
            let command = StreamCommand::from(request);
            PublishSnapshotResponse::try_from(self.apply_committed_write(command, placement)?)
        })
    }

    fn advance_retention<'a>(
        &'a mut self,
        request: AdvanceRetentionRequest,
        placement: ShardPlacement,
    ) -> GroupAdvanceRetentionFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            let command = StreamCommand::from(request);
            AdvanceRetentionResponse::try_from(self.apply_committed_write(command, placement)?)
        })
    }

    fn import_group_state<'a>(
        &'a mut self,
        request: ImportGroupStateRequest,
        placement: ShardPlacement,
    ) -> crate::GroupImportGroupStateFuture<'a> {
        Box::pin(async move {
            ImportGroupStateResponse::try_from(
                self.apply_stream_command(StreamCommand::from(request), placement)?,
            )
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
            let command = StreamCommand::TidyStream { stream_id, now_ms };
            crate::request::TidyStreamResponse::try_from(
                self.apply_committed_write(command, placement)?,
            )
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
                    .map_err(|err| {
                        cold_store_failure(
                            ColdOperation::IndexExternalObject {
                                stream_id: candidate.stream_id.clone(),
                            },
                            err,
                        )
                    })?;
                    report.page_entries_clipped =
                        report.page_entries_clipped.saturating_add(clipped);
                }
                if let Some(cache) = self.cold_index_cache.as_ref() {
                    cache.invalidate_stream(&candidate.stream_id);
                }
                let command = StreamCommand::OffloadColdRefs {
                    stream_id: candidate.stream_id,
                    refs: candidate.refs,
                };
                match self.apply_committed_write(command, placement) {
                    Ok(GroupWriteResponse::OffloadColdRefs(response)) => {
                        report.streams = report.streams.saturating_add(1);
                        report.refs_offloaded =
                            report.refs_offloaded.saturating_add(response.removed);
                    }
                    Ok(other) => {
                        return Err(GroupEngineError::Infra(
                            GroupInfraError::UnexpectedResponse {
                                expected: ResponseKind::OffloadColdRefs,
                                actual: other.kind(),
                            },
                        ));
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
            let retire_through = self.sample_retirement(
                crate::rt::time::Instant::now(),
                request.now_ms,
                request.end_record_retain,
            );
            if request.max_expiry_sweeps > 0 && crate::expiry_sweep::enabled() {
                let now_ms = crate::expiry_sweep::sweep_now_ms(request.now_ms);
                let max = u64::try_from(request.max_expiry_sweeps).unwrap_or(u64::MAX);
                while self.sweep_due(now_ms, retire_through) {
                    if report.expiry_sweeps >= max {
                        crate::expiry_sweep::capped(&mut report);
                        break;
                    }
                    let swept = match crate::expiry_sweep::settle(self.apply_committed_write(
                        crate::expiry_sweep::command(now_ms, retire_through),
                        placement,
                    )) {
                        Ok(swept) => swept,
                        Err(err) => {
                            crate::expiry_sweep::interrupted(
                                placement.raft_group_id,
                                &err,
                                &mut report,
                            );
                            break;
                        }
                    };
                    crate::expiry_sweep::applied(&mut report, &swept);
                }
            }
            let mut error = None;
            for stream_id in self.tidy_candidates(request.now_ms, request.max_streams) {
                match self.tidy_stream(stream_id, request.now_ms, placement).await {
                    Ok(response) => {
                        report.tidied = report.tidied.saturating_add(1);
                        if response.debt_remaining {
                            report.debt_remaining = report.debt_remaining.saturating_add(1);
                        }
                    }
                    Err(err) => {
                        error = Some(err);
                        break;
                    }
                }
            }
            TidyStreamsPass { report, error }
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

    fn route_write<'a>(
        &'a mut self,
        request: RouteWriteRequest,
        _placement: ShardPlacement,
    ) -> GroupRouteWriteFuture<'a> {
        Box::pin(async move {
            Ok(WriteRoute::Leader {
                early_answer: request
                    .early_answer_request()
                    .and_then(|request| self.producer_early_answer_now(&request)),
                fence: UploadFence::Unreplicated,
            })
        })
    }

    fn bucket_usage<'a>(&'a mut self, _placement: ShardPlacement) -> GroupBucketUsageFuture<'a> {
        Box::pin(async move {
            Ok(super::GroupBucketUsage {
                applied_index: self.commit_index,
                buckets: self.state_machine.bucket_usage_report(),
                snapshot_bytes: self.state_machine.snapshot_bytes_by_bucket(),
            })
        })
    }

    fn close_stream<'a>(
        &'a mut self,
        request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            let command = StreamCommand::from(request);
            CloseStreamResponse::try_from(self.apply_committed_write(command, placement)?)
        })
    }

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a> {
        let command = StreamCommand::from(request);
        Box::pin(async move {
            DeleteStreamResponse::try_from(self.apply_committed_write(command, placement)?)
        })
    }

    fn purge_bucket<'a>(
        &'a mut self,
        request: PurgeBucketRequest,
        placement: ShardPlacement,
    ) -> GroupPurgeBucketFuture<'a> {
        Box::pin(async move {
            PurgeBucketResponse::try_from(
                self.apply_committed_write(StreamCommand::from(request), placement)?,
            )
        })
    }

    fn ack_cold_gc<'a>(
        &'a mut self,
        up_to_seq: u64,
        placement: ShardPlacement,
    ) -> GroupAckColdGcFuture<'a> {
        Box::pin(async move {
            AckColdGcResponse::try_from(
                self.apply_committed_write(StreamCommand::AckColdGc { up_to_seq }, placement)?,
            )
        })
    }

    fn defer_cold_gc<'a>(
        &'a mut self,
        seq: u64,
        not_before_ms: u64,
        placement: ShardPlacement,
    ) -> GroupDeferColdGcFuture<'a> {
        Box::pin(async move {
            DeferColdGcResponse::try_from(self.apply_committed_write(
                StreamCommand::DeferColdGc { seq, not_before_ms },
                placement,
            )?)
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
                    .map_err(|err| cold_store_failure(ColdOperation::RepairIndex, err))?;
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
            let command = StreamCommand::from(request);
            AppendResponse::try_from(self.apply_committed_write(command, placement)?)
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
            let command = StreamCommand::from(request);
            AppendResponse::try_from(self.apply_committed_write(command, placement)?)
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
                .map_err(|err| {
                    cold_store_failure(
                        ColdOperation::IndexFlushedChunk {
                            stream_id: request.stream_id.clone(),
                        },
                        err,
                    )
                })?;
                // The clip rule may have removed stale entries that a cached
                // page still holds.
                if clipped_entries(&rollback) > 0
                    && let Some(cache) = self.cold_index_cache.as_ref()
                {
                    cache.invalidate_stream(&request.stream_id);
                }
                index_rollback = Some((store, rollback, request.stream_id.clone()));
            }
            let command = StreamCommand::from(request);
            let result = self
                .apply_committed_write(command, placement)
                .and_then(FlushColdResponse::try_from);
            if result.is_err()
                && let Some((store, rollback, stream_id)) = index_rollback
            {
                rollback_cold_index_pages(&store, rollback)
                    .await
                    .map_err(|err| {
                        cold_store_failure(ColdOperation::RollbackIndex { stream_id }, err)
                    })?;
            }
            result
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
                let index_compacted_chunk = || ColdOperation::IndexCompactedChunk {
                    stream_id: request.stream_id.clone(),
                };
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
                    .map_err(|err| cold_store_failure(index_compacted_chunk(), err))?
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
                        .map_err(|err| cold_store_failure(index_compacted_chunk(), err))?
                    else {
                        return Err(GroupEngineError::Infra(
                            GroupInfraError::ColdCompactionStale {
                                stream_id: request.stream_id.clone(),
                            },
                        ));
                    };
                    rollback
                };
                index_rollback = Some((store, rollback, request.stream_id.clone()));
            }
            let command = StreamCommand::from(request);
            let result = self
                .apply_committed_write(command, placement)
                .and_then(CompactColdResponse::try_from);
            if result.is_err()
                && let Some((store, rollback, stream_id)) = index_rollback
            {
                rollback_cold_index_pages(&store, rollback)
                    .await
                    .map_err(|err| {
                        cold_store_failure(ColdOperation::RollbackIndex { stream_id }, err)
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

    fn history_watch(&self) -> Option<super::HistoryWatch> {
        Some(self.subscribe_history())
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

#[cfg(test)]
impl InMemoryGroupEngine {
    /// An engine whose group applied its genesis, for tests that apply
    /// commands to it directly: a group applies nothing before its genesis.
    pub(crate) fn started() -> Self {
        let mut engine = Self::default();
        engine.start_genesis(
            ShardPlacement {
                core_id: ursula_shard::CoreId(0),
                shard_id: ursula_shard::ShardId(0),
                raft_group_id: ursula_shard::RaftGroupId(0),
            },
            0,
        );
        engine
    }

    /// [`Self::started`] over `cold_store`.
    pub(crate) fn started_with_cold_store(cold_store: ColdStoreHandle) -> Self {
        let mut engine = Self::started();
        engine.set_cold_store(Some(cold_store));
        engine
    }
}

impl GroupEngineFactory for InMemoryGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        _metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let mut engine = InMemoryGroupEngine::default();
            engine.set_cold_store(self.cold_store.clone());
            engine.start_genesis(placement, 0);
            let engine: Box<dyn GroupEngine> = Box::new(engine);
            Ok(engine)
        })
    }
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
        other => Err(GroupEngineError::Infra(
            GroupInfraError::UnexpectedResponse {
                expected: ResponseKind::BucketCreated,
                actual: ResponseKind::from(&other),
            },
        )),
    }
}

/// Stream id a command targets, if any (bucket and GC commands have none).
fn command_stream_id(command: &StreamCommand) -> Option<BucketStreamId> {
    match command {
        StreamCommand::CreateBucket { .. }
        | StreamCommand::PurgeBucket { .. }
        | StreamCommand::AckColdGc { .. }
        | StreamCommand::DeferColdGc { .. }
        | StreamCommand::ImportSnapshot { .. }
        | StreamCommand::NewHistory { .. }
        | StreamCommand::Sweep { .. } => None,
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
        | StreamCommand::DeleteStream { stream_id, .. }
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
    response: ResponseKind,
) -> Result<BucketStreamId, GroupEngineError> {
    stream_id.ok_or(GroupEngineError::Infra(GroupInfraError::MissingStreamId {
        response,
    }))
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
        other => GroupEngineError::Infra(GroupInfraError::UnexpectedResponse {
            expected: ResponseKind::StreamError,
            actual: ResponseKind::from(&other),
        }),
    }
}
