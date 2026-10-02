use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use ursula_shard::BucketStreamId;
use ursula_shard::ShardPlacement;
use ursula_stream::AppendStreamInput;
use ursula_stream::ColdFlushPassRequest;
use ursula_stream::FEATURE_LEVEL_EXTERNAL_LOCATORS;
use ursula_stream::FEATURE_LEVEL_KEYED_STREAMS;
use ursula_stream::ObjectPayloadRef;
use ursula_stream::ProducerRequest;
use ursula_stream::RecordPlanError;
use ursula_stream::RecordReadAnchor;
use ursula_stream::RecordReadRequest;
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
use super::GroupAppendBatchFuture;
use super::GroupAppendBatchResponse;
use super::GroupAppendFuture;
use super::GroupAppendTransactionFuture;
use super::GroupBootstrapStreamFuture;
use super::GroupBucketUsageFuture;
use super::GroupCloseStreamFuture;
use super::GroupColdHotBacklogFuture;
use super::GroupCompactColdFuture;
use super::GroupCreateStreamFuture;
use super::GroupDeferColdGcFuture;
use super::GroupDeleteSnapshotFuture;
use super::GroupDeleteStreamFuture;
use super::GroupEngine;
use super::GroupEngineCreateFuture;
use super::GroupEngineError;
use super::GroupEngineFactory;
use super::GroupEngineMetrics;
use super::GroupFeatureLevelFuture;
use super::GroupFlushColdFuture;
use super::GroupGetStreamAttrsFuture;
use super::GroupHeadStreamFuture;
use super::GroupInstallSnapshotFuture;
use super::GroupListBucketStreamsFuture;
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
use super::GroupSetBucketQuotaFuture;
use super::GroupSetFeatureLevelFuture;
use super::GroupSnapshotFuture;
use super::GroupStateGaugesFuture;
use super::GroupTidyStreamFuture;
use super::GroupTidyStreamsFuture;
use super::GroupTouchStreamAccessFuture;
use super::GroupUpdateStreamAttrsFuture;
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
use crate::cold_index::write_external_segment_index_pages;
use crate::cold_index::write_external_segment_index_pages_in_generation;
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
use crate::request::AppendBatchRequest;
use crate::request::AppendExternalRequest;
use crate::request::AppendRequest;
use crate::request::AppendResponse;
use crate::request::AppendTransactionRequest;
use crate::request::AppendTransactionResponse;
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
use crate::request::DeleteSnapshotRequest;
use crate::request::DeleteStreamRequest;
use crate::request::DeleteStreamResponse;
use crate::request::FlushColdRequest;
use crate::request::FlushColdResponse;
use crate::request::GetStreamAttrsRequest;
use crate::request::GetStreamAttrsResponse;
use crate::request::GroupReadStreamParts;
use crate::request::HeadStreamRequest;
use crate::request::HeadStreamResponse;
use crate::request::ImportGroupStateRequest;
use crate::request::ImportGroupStateResponse;
use crate::request::ListBucketStreamsRequest;
use crate::request::PlanColdFlushRequest;
use crate::request::PlanGroupColdFlushRequest;
use crate::request::PublishSnapshotRequest;
use crate::request::PublishSnapshotResponse;
use crate::request::PurgeBucketResponse;
use crate::request::ReadSnapshotRequest;
use crate::request::ReadSnapshotResponse;
use crate::request::ReadStreamRequest;
use crate::request::SetBucketQuotaRequest;
use crate::request::SetBucketQuotaResponse;
use crate::request::SetFeatureLevelRequest;
use crate::request::SetFeatureLevelResponse;
use crate::request::StreamAppendCount;
use crate::request::TidyStreamsRequest;
use crate::request::TidyStreamsResponse;
use crate::request::TouchStreamAccessResponse;
use crate::request::UpdateStreamAttrsRequest;
use crate::request::UpdateStreamAttrsResponse;
use crate::request::WriteHotBacklog;

pub(crate) struct AppendPayloadInput<'a> {
    stream_id: BucketStreamId,
    content_type: Option<&'a str>,
    payload: &'a [u8],
    close_after: bool,
    stream_seq: Option<String>,
    producer: Option<ProducerRequest>,
    now_ms: u64,
    record_match: Option<u64>,
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
            GroupWriteCommand::Batch { commands } => Ok(GroupWriteResponse::Batch(
                commands
                    .into_iter()
                    .map(|command| self.apply_stream_command(command, placement))
                    .collect(),
            )),
            GroupWriteCommand::Transaction { commands } => {
                self.apply_append_transaction(commands, placement)
            }
        }
    }

    fn apply_append_transaction(
        &mut self,
        commands: Vec<StreamCommand>,
        placement: ShardPlacement,
    ) -> Result<GroupWriteResponse, GroupEngineError> {
        let commit_index = self.commit_index;
        let mut append_counts = HashMap::new();
        let mut stream_ids = Vec::with_capacity(commands.len());
        for command in &commands {
            let Some(stream_id) = command_stream_id(command) else {
                return Err(GroupEngineError::new(
                    "append transaction contains a command without a stream",
                ));
            };
            append_counts
                .entry(stream_id.clone())
                .or_insert_with(|| self.state_machine.stream_append_count(&stream_id));
            stream_ids.push(stream_id);
        }
        let responses = match self.state_machine.append_transaction(commands) {
            Ok(responses) => responses,
            Err(response) => return Err(stream_response_error(response)),
        };
        let mut group_responses = Vec::with_capacity(responses.len());
        for (stream_id, response) in stream_ids.into_iter().zip(responses) {
            match self.append_response_from_stream(stream_id, response, placement) {
                Ok(response) => group_responses.push(Ok(GroupWriteResponse::Append(response))),
                Err(err) => {
                    self.commit_index = commit_index;
                    for (stream_id, count) in append_counts {
                        self.state_machine
                            .set_stream_append_count(&stream_id, count);
                    }
                    return Err(err);
                }
            }
        }
        Ok(GroupWriteResponse::Batch(group_responses))
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
                record_match,
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
                        record_match,
                    },
                    placement,
                )
                .map(GroupWriteResponse::Append),
            StreamCommand::AppendBatch {
                stream_id,
                content_type,
                payloads,
                producer,
                now_ms,
            } => self.apply_append_batch(
                stream_id,
                content_type,
                payloads,
                producer,
                now_ms,
                placement,
            ),
            command => {
                let stream_id = command_stream_id(&command);
                let command_producer = command_producer(&command);
                // Commands whose pages the leader rewrote before proposing:
                // compaction, F5 offloads (which may clip entries), and
                // external appends and creates, whose entries the leader
                // writes straight to the page store below level 3 (bounded-
                // state F13). Every replica drops the stream's cached pages,
                // so a page cached earlier (possibly holding a stale entry over
                // the same offsets) is reloaded.
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
                let response = self.group_response_from_stream(
                    response,
                    stream_id,
                    command_producer,
                    placement,
                );
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

    fn apply_append_batch(
        &mut self,
        stream_id: BucketStreamId,
        content_type: Option<String>,
        payloads: Vec<Bytes>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
        placement: ShardPlacement,
    ) -> Result<GroupWriteResponse, GroupEngineError> {
        if let Some(producer) = producer {
            let payload_refs = payloads.iter().map(Bytes::as_ref).collect::<Vec<_>>();
            let batch = self
                .state_machine
                .append_batch_borrowed(
                    stream_id.clone(),
                    content_type.as_deref(),
                    &payload_refs,
                    Some(producer.clone()),
                    now_ms,
                )
                .map_err(stream_response_error)?;
            let old_commit_index = self.commit_index;
            let old_append_count = self.state_machine.stream_append_count(&stream_id);
            if batch.receipt_evicted {
                // F3: a duplicate beyond the receipt window, answered once
                // for the whole batch and without ranges.
                let tail = self
                    .state_machine
                    .head(&stream_id)
                    .map_or(0, |head| head.tail_offset);
                return Ok(GroupWriteResponse::AppendBatch(GroupAppendBatchResponse {
                    placement,
                    items: vec![Ok(AppendResponse {
                        placement,
                        start_offset: tail,
                        next_offset: tail,
                        stream_append_count: old_append_count,
                        group_commit_index: old_commit_index,
                        closed: false,
                        deduplicated: true,
                        producer: None,
                        record_range: None,
                        stream_hot_bytes: self.state_machine.hot_real_len(&stream_id).unwrap_or(0),
                        group_hot_bytes: self.state_machine.total_hot_real_bytes(),
                        receipt_evicted: true,
                    })],
                }));
            }
            if !batch.deduplicated {
                let count = u64::try_from(batch.items.len()).expect("item count fits u64");
                self.commit_index += count;
                self.state_machine
                    .add_stream_append_count(&stream_id, count);
            }
            let stream_hot_bytes = self.state_machine.hot_real_len(&stream_id).unwrap_or(0);
            let group_hot_bytes = self.state_machine.total_hot_real_bytes();
            let items = batch
                .items
                .into_iter()
                .enumerate()
                .map(|(index, item)| {
                    let item_index = u64::try_from(index + 1).expect("item index fits u64");
                    Ok(AppendResponse {
                        placement,
                        start_offset: item.offset,
                        next_offset: item.next_offset,
                        stream_append_count: if item.deduplicated {
                            old_append_count
                        } else {
                            old_append_count + item_index
                        },
                        group_commit_index: if item.deduplicated {
                            old_commit_index
                        } else {
                            old_commit_index + item_index
                        },
                        closed: item.closed,
                        deduplicated: item.deduplicated,
                        producer: None,
                        // F1 (RC-10, RC-11): the range apply computed or the
                        // stored receipt's, never one derived from the index.
                        record_range: item.record_range,
                        stream_hot_bytes,
                        group_hot_bytes,
                        receipt_evicted: false,
                    })
                })
                .collect();
            return Ok(GroupWriteResponse::AppendBatch(GroupAppendBatchResponse {
                placement,
                items,
            }));
        }

        let mut items = Vec::with_capacity(payloads.len());
        for payload in payloads {
            if payload.is_empty() {
                items.push(Err(GroupEngineError::stream(
                    StreamErrorCode::EmptyAppend,
                    "append payload must be non-empty",
                )));
                continue;
            }
            items.push(self.append_payload(
                AppendPayloadInput {
                    stream_id: stream_id.clone(),
                    content_type: content_type.as_deref(),
                    payload: &payload,
                    close_after: false,
                    stream_seq: None,
                    producer: None,
                    now_ms,
                    record_match: None,
                },
                placement,
            ));
        }
        Ok(GroupWriteResponse::AppendBatch(GroupAppendBatchResponse {
            placement,
            items,
        }))
    }

    /// The stream's and the group's hot bytes after a write (F6a); a missing
    /// or deleted stream holds none.
    fn write_hot_backlog(&self, stream_id: Option<&BucketStreamId>) -> WriteHotBacklog {
        WriteHotBacklog {
            stream_hot_bytes: stream_id
                .and_then(|stream_id| self.state_machine.hot_real_len(stream_id))
                .unwrap_or(0),
            group_hot_bytes: self.state_machine.total_hot_real_bytes(),
        }
    }

    /// Lifts a [`StreamResponse`] into the matching [`GroupWriteResponse`],
    /// advancing the group commit index for every mutating outcome.
    fn group_response_from_stream(
        &mut self,
        response: StreamResponse,
        stream_id: Option<BucketStreamId>,
        command_producer: Option<ProducerRequest>,
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
                    record_range: self
                        .state_machine
                        .record_range(&stream_id)
                        .map_err(|err| GroupEngineError::new(format!("record range: {err:?}")))?,
                    hot_backlog: Some(self.write_hot_backlog(Some(&stream_id))),
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
                record_range: None,
                hot_backlog: Some(self.write_hot_backlog(stream_id.as_ref())),
            })),
            StreamResponse::Appended {
                offset,
                next_offset,
                closed,
                deduplicated,
                producer,
                receipt_evicted,
                record_range,
            } => {
                let stream_id = require_response_stream_id(stream_id, "appended")?;
                // F1 (RC-10, RC-11): apply computed the range (or kept the
                // receipt's); F3: an evicted duplicate carries none.
                let stream_hot_bytes = self.state_machine.hot_real_len(&stream_id).unwrap_or(0);
                let group_hot_bytes = self.state_machine.total_hot_real_bytes();
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
                    record_range,
                    stream_hot_bytes,
                    group_hot_bytes,
                    receipt_evicted,
                }))
            }
            StreamResponse::SnapshotPublished {
                snapshot_offset,
                snapshot_digest,
                record_range,
            } => {
                self.commit_index += 1;
                Ok(GroupWriteResponse::PublishSnapshot(
                    PublishSnapshotResponse {
                        placement,
                        snapshot_offset,
                        snapshot_digest,
                        group_commit_index: self.commit_index,
                        record_range,
                        hot_backlog: Some(self.write_hot_backlog(stream_id.as_ref())),
                    },
                ))
            }
            StreamResponse::BucketQuotaSet { .. } => {
                self.commit_index += 1;
                Ok(GroupWriteResponse::SetBucketQuota(SetBucketQuotaResponse {
                    placement,
                    group_commit_index: self.commit_index,
                }))
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
            StreamResponse::FeatureLevelSet {
                level,
                previous_level,
            } => {
                self.commit_index += 1;
                Ok(GroupWriteResponse::SetFeatureLevel(
                    SetFeatureLevelResponse {
                        placement,
                        level,
                        previous_level,
                        group_commit_index: self.commit_index,
                    },
                ))
            }
            StreamResponse::RetentionAdvanced {
                retained_offset,
                record_range,
            } => {
                self.commit_index += 1;
                Ok(GroupWriteResponse::AdvanceRetention(
                    AdvanceRetentionResponse {
                        placement,
                        retained_offset,
                        group_commit_index: self.commit_index,
                        record_range,
                        hot_backlog: Some(self.write_hot_backlog(stream_id.as_ref())),
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
            StreamResponse::AttrsUpdated { changed } => {
                if changed {
                    self.commit_index += 1;
                }
                Ok(GroupWriteResponse::UpdateStreamAttrs(
                    UpdateStreamAttrsResponse {
                        placement,
                        changed,
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
                let record_range = self
                    .state_machine
                    .record_range_for_append(
                        &stream_id,
                        next_offset,
                        next_offset,
                        command_producer.as_ref(),
                    )
                    .map_err(|err| GroupEngineError::new(format!("record range: {err:?}")))?;
                if !deduplicated {
                    self.commit_index += 1;
                }
                Ok(GroupWriteResponse::CloseStream(CloseStreamResponse {
                    placement,
                    next_offset,
                    group_commit_index: self.commit_index,
                    deduplicated,
                    record_range,
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
            | StreamResponse::BucketAlreadyExists { .. }
            | StreamResponse::BucketDeleted { .. }) => Err(GroupEngineError::new(format!(
                "unexpected group write response: {other:?}"
            ))),
        }
    }

    pub(crate) fn cold_hot_backlog_for(
        &self,
        stream_id: BucketStreamId,
    ) -> Result<ColdHotBacklog, GroupEngineError> {
        let stream_hot_bytes = self.state_machine.hot_real_len(&stream_id).unwrap_or(0);
        Ok(ColdHotBacklog {
            stream_id,
            stream_hot_bytes,
            group_hot_bytes: self.state_machine.total_hot_real_bytes(),
        })
    }

    /// Admission for one payload of `incoming_bytes`.
    pub fn check_cold_write_admission_bytes(
        &self,
        stream_id: &BucketStreamId,
        admission: ColdWriteAdmission,
        incoming_bytes: u64,
    ) -> Result<(), GroupEngineError> {
        self.check_cold_write_admission(stream_id, admission, incoming_bytes, 1)
    }

    /// Cold admission (F6c): the group's real hot size (payload plus
    /// per-record overhead) plus the incoming payload, charged as at least
    /// `incoming_records` records, must stay within the group cap.
    pub fn check_cold_write_admission(
        &self,
        stream_id: &BucketStreamId,
        admission: ColdWriteAdmission,
        incoming_bytes: u64,
        incoming_records: u64,
    ) -> Result<(), GroupEngineError> {
        let Some(limit) = admission.max_hot_bytes_per_group else {
            return Ok(());
        };
        if incoming_bytes == 0 {
            return Ok(());
        }
        let before = self.state_machine.total_hot_real_bytes();
        let after = before.saturating_add(
            self.state_machine
                .hot_real_bytes(incoming_bytes, incoming_records.max(1)),
        );
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
            self.check_cold_write_admission_bytes(
                &request.stream_id,
                admission,
                u64::try_from(request.initial_payload.len()).expect("payload len fits u64"),
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
            self.check_cold_write_admission_bytes(
                &request.stream_id,
                admission,
                u64::try_from(request.payload.len()).expect("payload len fits u64"),
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

    pub(crate) fn append_batch_with_admission_inner(
        &mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<GroupAppendBatchResponse, GroupEngineError> {
        // F9: O(1) admission; see `append_with_admission_inner`. A producer
        // batch is deduplicated as a whole or not at all.
        if admission.is_enabled()
            && !self.state_machine.append_would_deduplicate(
                &request.stream_id,
                request.producer.as_ref(),
                request.now_ms,
            )
        {
            let incoming_bytes = request
                .payloads
                .iter()
                .map(|payload| u64::try_from(payload.len()).expect("payload len fits u64"))
                .sum();
            self.check_cold_write_admission(
                &request.stream_id,
                admission,
                incoming_bytes,
                u64::try_from(request.payloads.len()).unwrap_or(u64::MAX),
            )?;
        }
        let response =
            match self.apply_committed_write(GroupWriteCommand::from(request), placement)? {
                GroupWriteResponse::AppendBatch(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected append batch write response: {other:?}"
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
            record_match,
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
            record_match,
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
                record_range,
            } => {
                let stream_hot_bytes = self.state_machine.hot_real_len(&stream_id).unwrap_or(0);
                let group_hot_bytes = self.state_machine.total_hot_real_bytes();
                // F1 (RC-10, RC-11): apply computed the range (or kept the
                // receipt's); F3: an evicted duplicate carries none.
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
                    record_range,
                    stream_hot_bytes,
                    group_hot_bytes,
                    receipt_evicted,
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
        let Some(record) = request.record else {
            let mut plan = self
                .state_machine
                .read_plan_at(
                    &request.stream_id,
                    request.offset,
                    request.max_len,
                    request.now_ms,
                )
                .map_err(stream_response_error)?;
            plan.retained_record_range = self
                .state_machine
                .record_range(&request.stream_id)
                .map_err(|err| GroupEngineError::new(format!("record range: {err:?}")))?;
            return Ok(plan);
        };
        self.state_machine
            .record_read_plan(&RecordReadRequest {
                stream_id: &request.stream_id,
                record,
                max_records: request.max_records,
                max_bytes: request.max_len,
                now_ms: request.now_ms,
                anchor: request.record_anchor.map(|anchor| RecordReadAnchor {
                    incarnation: anchor.incarnation,
                    record: anchor.record,
                    offset: anchor.offset,
                }),
            })
            .map_err(|err| match err {
                RecordPlanError::Response(response) => stream_response_error(response),
                RecordPlanError::Index(message) => GroupEngineError::new(message),
            })
    }

    /// Per-bucket usage held by this group's state machine. Public so the
    /// Raft engine can serve usage reads from its applied state machine.
    pub fn bucket_usage_report(&self) -> Vec<ursula_stream::BucketUsageSnapshot> {
        self.state_machine.bucket_usage_report()
    }

    /// This group's share of a bucket listing; see
    /// [`ursula_stream::StreamStateMachine::list_bucket_streams`]. Public so
    /// the Raft engine can serve it from its applied state machine.
    pub fn list_bucket_streams_report(
        &self,
        request: &ListBucketStreamsRequest,
    ) -> Option<Vec<ursula_stream::BucketStreamListing>> {
        self.state_machine.list_bucket_streams(
            &request.bucket_id,
            &request.prefix,
            request.after.as_deref(),
            request.limit,
            request.now_ms,
        )
    }

    /// Replicated group feature level (C0) of the applied state.
    pub fn feature_level(&self) -> u32 {
        self.state_machine.feature_level()
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
            integrity: self
                .state_machine
                .integrity_snapshot(&request.stream_id)
                .map_err(stream_response_error)?,
            record_range: self
                .state_machine
                .record_range(&request.stream_id)
                .map_err(|err| GroupEngineError::new(format!("record range: {err:?}")))?,
            created_at_ms: Some(created_at_ms),
        })
    }

    pub fn get_stream_attrs_after_access(
        &mut self,
        request: &GetStreamAttrsRequest,
        placement: ShardPlacement,
    ) -> Result<GetStreamAttrsResponse, GroupEngineError> {
        if self
            .state_machine
            .head_at(&request.stream_id, request.now_ms)
            .is_none()
        {
            return Err(GroupEngineError::stream(
                StreamErrorCode::StreamNotFound,
                format!("stream '{}' does not exist", request.stream_id),
            ));
        }
        Ok(GetStreamAttrsResponse {
            placement,
            attrs: self.state_machine.stream_attrs(&request.stream_id).cloned(),
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
    /// update, then cuts the window into one part per message record.
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
        // Bootstrap never reads cold storage: the plan only covers messages
        // at or above the exact-message frontier, which are hot.
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
    /// external refs `request` makes due (empty below feature level 3).
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

    /// Whether external appends keep their locator in replicated state at
    /// this replica's applied level (F5, feature level 3). Levels only rise,
    /// so a proposal applied later sees at least this level: when this holds
    /// the engine must not write a page entry before proposing.
    pub fn external_locators_in_state(&self) -> bool {
        self.state_machine.feature_level() >= FEATURE_LEVEL_EXTERNAL_LOCATORS
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

    /// Whether `stream_id` exists and has not expired at `now_ms`. A create
    /// of a live stream never applies its initial payload (it answers
    /// already-exists or a conflict), so the external create path must not
    /// write a page entry for it: the entry would land at offset 0 of the
    /// existing stream.
    pub fn stream_is_live(&self, stream_id: &BucketStreamId, now_ms: u64) -> bool {
        matches!(
            self.state_machine
                .access_requires_write(stream_id, now_ms, false),
            Ok(false)
        )
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
            // From feature level 1 the state keeps the initial payload as a
            // direct reference (F14g); the level never drops between this
            // check and apply, so skipping the page is always safe. A create
            // of a live stream never writes a page either, so a conflicting
            // or retried create cannot replace the live stream's entry.
            if let Some(cold_store) = self.cold_store.as_ref()
                && self.state_machine.feature_level() < FEATURE_LEVEL_KEYED_STREAMS
                && !self.stream_is_live(&request.stream_id, request.now_ms)
            {
                let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
                write_external_segment_index_pages(
                    &store,
                    &request.stream_id,
                    0,
                    &request.initial_payload,
                )
                .await
                .map_err(|err| GroupEngineError::new(err.to_string()))?;
            }
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

    fn feature_level<'a>(&'a mut self, _placement: ShardPlacement) -> GroupFeatureLevelFuture<'a> {
        Box::pin(async move { Ok(self.state_machine.feature_level()) })
    }

    fn list_bucket_streams<'a>(
        &'a mut self,
        request: ListBucketStreamsRequest,
        _placement: ShardPlacement,
    ) -> GroupListBucketStreamsFuture<'a> {
        Box::pin(async move { Ok(self.list_bucket_streams_report(&request)) })
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

    fn set_feature_level<'a>(
        &'a mut self,
        request: SetFeatureLevelRequest,
        placement: ShardPlacement,
    ) -> GroupSetFeatureLevelFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(request);
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::SetFeatureLevel(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected set feature level write response: {other:?}"
                ))),
            }
        })
    }

    fn set_bucket_quota<'a>(
        &'a mut self,
        request: SetBucketQuotaRequest,
        placement: ShardPlacement,
    ) -> GroupSetBucketQuotaFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(request);
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::SetBucketQuota(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected set bucket quota write response: {other:?}"
                ))),
            }
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
            Ok(ReadSnapshotResponse {
                placement,
                snapshot_offset: snapshot.offset,
                next_offset: snapshot.offset,
                content_type: snapshot.content_type,
                snapshot_digest: snapshot.digest,
                payload: snapshot.payload,
                up_to_date: snapshot.offset == tail_offset,
                record_range: self
                    .state_machine
                    .record_range(&request.stream_id)
                    .map_err(|err| GroupEngineError::new(format!("record range: {err:?}")))?,
            })
        })
    }

    fn delete_snapshot<'a>(
        &'a mut self,
        request: DeleteSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteSnapshotFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            match self
                .state_machine
                .delete_snapshot(&request.stream_id, request.snapshot_offset)
            {
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
                    "unexpected delete snapshot response: {other:?}"
                ))),
            }
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
                updates,
                next_offset: plan.next_offset,
                up_to_date: plan.up_to_date,
                closed: plan.closed,
                record_range: self
                    .state_machine
                    .record_range(&request.stream_id)
                    .map_err(|err| GroupEngineError::new(format!("record range: {err:?}")))?,
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

    fn get_stream_attrs<'a>(
        &'a mut self,
        request: GetStreamAttrsRequest,
        placement: ShardPlacement,
    ) -> GroupGetStreamAttrsFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            self.get_stream_attrs_after_access(&request, placement)
        })
    }

    fn update_stream_attrs<'a>(
        &'a mut self,
        request: UpdateStreamAttrsRequest,
        placement: ShardPlacement,
    ) -> GroupUpdateStreamAttrsFuture<'a> {
        Box::pin(async move {
            match self.apply_committed_write(GroupWriteCommand::from(request), placement)? {
                GroupWriteResponse::UpdateStreamAttrs(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected update stream attrs write response: {other:?}"
                ))),
            }
        })
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
            let Some(cold_store) = self.cold_store.as_ref() else {
                return Ok(RepairColdIndexResponse::default());
            };
            let inputs = self.cold_index_repair_inputs_for(&request);
            let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
            let (report, compaction_pages) =
                repair_cold_index_streams(&store, self.cold_index_cache.as_deref(), &inputs)
                    .await
                    .map_err(|err| GroupEngineError::new(err.to_string()))?;
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

    fn append_transaction<'a>(
        &'a mut self,
        request: AppendTransactionRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendTransactionFuture<'a> {
        Box::pin(async move {
            let Some(first) = request.operations.first() else {
                return Err(GroupEngineError::new(
                    "append transaction must contain at least one operation",
                ));
            };
            self.check_cold_write_admission(
                &first.stream_id,
                admission,
                request.payload_bytes(),
                u64::try_from(request.operations.len()).unwrap_or(u64::MAX),
            )?;
            let command = GroupWriteCommand::Transaction {
                commands: request
                    .operations
                    .into_iter()
                    .map(StreamCommand::from)
                    .collect(),
            };
            let GroupWriteResponse::Batch(items) =
                self.apply_committed_write(command, placement)?
            else {
                return Err(GroupEngineError::new(
                    "unexpected append transaction write response",
                ));
            };
            let items = items
                .into_iter()
                .map(|item| match item? {
                    GroupWriteResponse::Append(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected append transaction item response: {other:?}"
                    ))),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(AppendTransactionResponse { placement, items })
        })
    }

    fn append_external<'a>(
        &'a mut self,
        request: AppendExternalRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            // F5 (level 3): commit first, index after. Apply keeps the
            // locator in state; the offload pass writes the page entry once
            // the append committed. Below level 3 the page entry written
            // here, before proposing, is the only locator.
            if let Some(cold_store) = self
                .cold_store
                .as_ref()
                .filter(|_| !self.external_locators_in_state())
            {
                let start_offset = self
                    .state_machine
                    .head(&request.stream_id)
                    .map(|metadata| metadata.tail_offset)
                    .ok_or_else(|| {
                        GroupEngineError::stream(
                            ursula_stream::StreamErrorCode::StreamNotFound,
                            format!("stream '{}' does not exist", request.stream_id),
                        )
                    })?;
                let generation = self
                    .state_machine
                    .cold_index_generation(&request.stream_id)
                    .unwrap_or(0);
                let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
                write_external_segment_index_pages_in_generation(
                    &store,
                    &request.stream_id,
                    generation,
                    start_offset,
                    &request.payload,
                )
                .await
                .map_err(|err| GroupEngineError::new(err.to_string()))?;
            }
            let command = GroupWriteCommand::from(request);
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::Append(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected external append write response: {other:?}"
                ))),
            }
        })
    }

    fn append_batch<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendBatchFuture<'a> {
        if admission.is_enabled() {
            return Box::pin(async move {
                self.append_batch_with_admission_inner(request, placement, admission)
            });
        }
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request);
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::AppendBatch(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected append batch write response: {other:?}"
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
        | StreamCommand::DeleteBucket { .. }
        | StreamCommand::PurgeBucket { .. }
        | StreamCommand::AckColdGc { .. }
        | StreamCommand::DeferColdGc { .. }
        | StreamCommand::ImportSnapshot { .. }
        | StreamCommand::SetBucketQuota { .. }
        | StreamCommand::SetFeatureLevel { .. } => None,
        StreamCommand::CreateStream { stream_id, .. }
        | StreamCommand::CreateExternal { stream_id, .. }
        | StreamCommand::Append { stream_id, .. }
        | StreamCommand::AppendExternal { stream_id, .. }
        | StreamCommand::AppendBatch { stream_id, .. }
        | StreamCommand::PublishSnapshot { stream_id, .. }
        | StreamCommand::AdvanceRetention { stream_id, .. }
        | StreamCommand::TouchStreamAccess { stream_id, .. }
        | StreamCommand::UpdateStreamAttrs { stream_id, .. }
        | StreamCommand::FlushCold { stream_id, .. }
        | StreamCommand::CompactCold { stream_id, .. }
        | StreamCommand::Close { stream_id, .. }
        | StreamCommand::DeleteStream { stream_id }
        | StreamCommand::TidyStream { stream_id, .. }
        | StreamCommand::OffloadColdRefs { stream_id, .. } => Some(stream_id.clone()),
    }
}

fn command_producer(command: &StreamCommand) -> Option<ProducerRequest> {
    match command {
        StreamCommand::Close { producer, .. } => producer.clone(),
        _ => None,
    }
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
